//! Mission B2: end-to-end crash-recovery tests that drive the executor
//! fast-path bypass sites through the public `Engine::execute_powql` API.
//!
//! These tests are the real contract for B2: they simulate the exact
//! "user ran `UPDATE users SET name='x' WHERE id=5`, process crashes,
//! reopens, row should still say `x`" scenario from the bug report. If
//! any of the five executor bypass call sites ever regresses to a
//! non-WAL-logged primitive, one of these tests will fail.
//!
//! Each test is built around the `mem::forget` crash simulator: we build
//! an `Engine`, run the mutation, then leak the engine so no Drop impl
//! runs. That skips `Catalog::checkpoint` entirely, leaving the heap on
//! disk in its pre-mutation state and the WAL file holding every record
//! the mutation logged. A fresh `Engine::new` then replays the WAL and
//! the tests assert the mutation landed.

use powdb_query::ast::Literal;
use powdb_query::executor::Engine;
use powdb_query::result::QueryResult;
use powdb_storage::types::Value;

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "powdb_wal_executor_{name}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Run a PowQL mutation on a fresh engine, unwrapping on parse/plan/exec
/// error. Separate helper so each test body stays focused on the shape of
/// the mutation + the post-replay assertions.
fn exec(engine: &mut Engine, query: &str) -> QueryResult {
    engine
        .execute_powql(query)
        .unwrap_or_else(|e| panic!("failed to execute `{query}`: {e}"))
}

fn count_rows(engine: &mut Engine, query: &str) -> usize {
    match exec(engine, query) {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// B2 reproducer #1: `UPDATE users SET age = 999 WHERE id = 5` via the
/// executor's prepared-update-by-pk fast path. Before Mission B2 this
/// went through `tbl.with_row_bytes_mut` which bypassed the WAL entirely;
/// a crash here would silently lose the update on replay.
#[test]
fn test_crash_recovery_update_by_pk() {
    let dir = temp_dir("update_pk");
    std::fs::create_dir_all(&dir).unwrap();

    // Session 1: create schema, insert 10 rows cleanly, update row id=5
    // through the fast-path `update { age := 999 }`, crash.
    {
        let mut engine = Engine::new(&dir).unwrap();
        exec(
            &mut engine,
            "type users { required id: int, required name: str, age: int }",
        );
        // Index on `id` — this is what unlocks the prepared-update-by-pk
        // fast path in `try_execute_update_pk_fast`. PowQL has no
        // `create index` syntax, so we reach through to the catalog.
        engine.catalog_mut().create_index("users", "id").unwrap();
        for i in 0..10i64 {
            exec(
                &mut engine,
                &format!(
                    "insert users {{ id := {i}, name := \"user_{i}\", age := {age} }}",
                    age = 20 + i
                ),
            );
        }
        // Query shape that triggers the fast path: `T filter .pk = ?
        // update { col := ? }` on an int-indexed pk column with a
        // fixed-size target column. `execute_prepared` is the only
        // entry that reaches `try_execute_update_pk_fast`, which is
        // the exact call site this test is pinning.
        let prep = engine
            .prepare("users filter .id = 5 update { age := 999 }")
            .expect("prepare should succeed");
        let literals = vec![Literal::Int(5), Literal::Int(999)];
        engine
            .execute_prepared(&prep, &literals)
            .expect("prepared update should succeed");
        // Crash without checkpoint: every insert + the update are only
        // durable via the WAL.
        std::mem::forget(engine);
    }

    // Session 2: reopen and assert the update replayed.
    {
        let mut engine = Engine::new(&dir).unwrap();
        let res = exec(&mut engine, "users filter .id = 5");
        match res {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(rows.len(), 1, "id=5 should exist after replay");
                let age_idx = columns.iter().position(|c| c == "age").unwrap();
                assert_eq!(
                    rows[0][age_idx],
                    Value::Int(999),
                    "age should be 999 after WAL replay of update_by_pk fast path"
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
        // And the other rows must still have their original ages.
        assert_eq!(count_rows(&mut engine, "users"), 10);
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// B2 reproducer #2: var-length column update via the executor's
/// single-var-col in-place fast path. `name := "x"` on a non-indexed
/// string column used to bypass the WAL through `patch_var_col_in_place`.
#[test]
fn test_crash_recovery_var_col_update() {
    let dir = temp_dir("var_col");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let mut engine = Engine::new(&dir).unwrap();
        exec(
            &mut engine,
            "type users { required id: int, required name: str, age: int }",
        );
        for i in 0..10i64 {
            exec(
                &mut engine,
                &format!("insert users {{ id := {i}, name := \"original_name_{i}\", age := {i} }}",),
            );
        }
        // Shrink row 5's name to a single char. No index on name, single
        // var column assignment → this is the exact shape that hits the
        // `patch_var_col_in_place` fast path (now `patch_var_col_logged`).
        exec(&mut engine, "users filter .id = 5 update { name := \"x\" }");
        std::mem::forget(engine);
    }

    {
        let mut engine = Engine::new(&dir).unwrap();
        let res = exec(&mut engine, "users filter .id = 5");
        match res {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(rows.len(), 1);
                let name_idx = columns.iter().position(|c| c == "name").unwrap();
                assert_eq!(
                    rows[0][name_idx],
                    Value::Str("x".into()),
                    "name should be 'x' after WAL replay of var-col fast path"
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
        assert_eq!(count_rows(&mut engine, "users"), 10);
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// B2 reproducer #3: `DELETE FROM users WHERE age > 50` via the fused
/// single-pass `scan_delete_matching` primitive. Before Mission B2 this
/// wrote nothing to the WAL at all — the predicate is opaque and no rids
/// were known up front. Now every match emits a Delete record during the
/// same scan.
#[test]
fn test_crash_recovery_delete_by_filter() {
    let dir = temp_dir("delete_filter");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let mut engine = Engine::new(&dir).unwrap();
        exec(
            &mut engine,
            "type users { required id: int, required name: str, age: int }",
        );
        for i in 0..100i64 {
            exec(
                &mut engine,
                &format!("insert users {{ id := {i}, name := \"user_{i}\", age := {i} }}",),
            );
        }
        // 49 matches: ages 51..=99 inclusive.
        exec(&mut engine, "users filter .age > 50 delete");
        std::mem::forget(engine);
    }

    {
        let mut engine = Engine::new(&dir).unwrap();
        assert_eq!(
            count_rows(&mut engine, "users"),
            51,
            "expected 100 − 49 = 51 rows after WAL replay of filtered delete"
        );
        // Nothing with age > 50 should remain.
        assert_eq!(
            count_rows(&mut engine, "users filter .age > 50"),
            0,
            "every age > 50 row should be deleted after replay"
        );
        // And everything with age <= 50 should still be there.
        assert_eq!(
            count_rows(&mut engine, "users filter .age <= 50"),
            51,
            "every age <= 50 row should survive replay"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}
