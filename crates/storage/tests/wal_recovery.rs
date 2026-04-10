//! Mission 2 (infra audit): crash-and-recover integration tests.
//!
//! These tests verify that the WAL wired into [`powdb_storage::catalog::Catalog`]
//! actually recovers data after a simulated crash. The "crash" is simulated
//! by calling [`std::mem::forget`] on the Catalog, which skips every Drop
//! impl underneath it — including the [`powdb_storage::heap::HeapFile`]
//! Drop that would otherwise flush dirty pages. After `mem::forget`, the
//! in-memory hot-page cache is leaked, the heap file on disk has only the
//! zeroed pages allocated during insert (no row data), and the WAL file
//! holds every record that was `wal.flush()`ed before the crash.
//!
//! The recovery path is then [`Catalog::open`] → [`Catalog::replay_wal`],
//! which reads every WAL record and re-applies it to the freshly-reopened
//! (empty) heap, restoring every row.

use powdb_storage::catalog::Catalog;
use powdb_storage::types::{ColumnDef, RowId, Schema, TypeId, Value};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "powdb_wal_recovery_{name}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn user_schema() -> Schema {
    Schema {
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            },
            ColumnDef {
                name: "name".into(),
                type_id: TypeId::Str,
                required: true,
                position: 1,
            },
        ],
    }
}

/// Primary test: insert 100 rows, simulate a crash by `mem::forget`-ing
/// the catalog (so no heap flush, no WAL truncate), reopen, and assert
/// every row comes back via WAL replay.
#[test]
fn test_crash_recovery_100_rows() {
    let dir = temp_dir("crash_100");
    std::fs::create_dir_all(&dir).unwrap();

    // ── Session 1: create, insert 100 rows, simulate crash ─────────────
    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema()).unwrap();
        for i in 0..100i64 {
            cat.insert(
                "users",
                &vec![Value::Int(i), Value::Str(format!("user_{i}"))],
            )
            .unwrap();
        }
        // Crash simulation: skip every Drop underneath the Catalog so
        // the in-memory hot-page cache is lost. `wal.flush` already
        // ran inside every `insert` call, so the WAL file on disk
        // holds all 100 records.
        std::mem::forget(cat);
    }

    // ── Session 2: reopen and expect replay to restore all 100 rows ────
    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(
            rows.len(),
            100,
            "expected 100 rows after WAL replay, got {}",
            rows.len()
        );
        // Spot-check the full id range — order after replay is
        // determined by the insert order in the WAL.
        let mut ids: Vec<i64> = rows
            .iter()
            .map(|(_, r)| match &r[0] {
                Value::Int(i) => *i,
                _ => panic!("expected Int id"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids.first(), Some(&0));
        assert_eq!(ids.last(), Some(&99));
        // And make sure every id 0..100 is present (no gaps, no dupes).
        let expected: Vec<i64> = (0..100).collect();
        assert_eq!(ids, expected);
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Clean-shutdown control: if the catalog drops normally (calling its
/// own checkpoint), the WAL is truncated and replay on the next open is
/// a no-op. Verifies that normal-close paths don't re-apply records on
/// top of the already-persistent heap.
#[test]
fn test_clean_shutdown_no_replay() {
    let dir = temp_dir("clean");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema()).unwrap();
        for i in 0..50i64 {
            cat.insert(
                "users",
                &vec![Value::Int(i), Value::Str(format!("clean_{i}"))],
            )
            .unwrap();
        }
        // Drop runs Catalog::checkpoint which flushes the heap and
        // truncates the WAL.
    }

    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(
            rows.len(),
            50,
            "clean shutdown should persist exactly 50 rows, got {}",
            rows.len()
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Delete recovery: insert some rows cleanly, then delete a few, crash.
/// Replay should reapply the deletes on the freshly-opened heap. This is
/// an explicit check that deletes are idempotent on replay — a "double
/// delete" of an already-missing slot is a no-op at the heap layer.
#[test]
fn test_crash_recovery_deletes_idempotent() {
    let dir = temp_dir("delete_idem");
    std::fs::create_dir_all(&dir).unwrap();

    // Session 1: insert cleanly (will be checkpointed on drop).
    let rids_to_keep;
    let rids_to_delete;
    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema()).unwrap();
        let mut all = Vec::new();
        for i in 0..20i64 {
            let rid = cat
                .insert("users", &vec![Value::Int(i), Value::Str(format!("row_{i}"))])
                .unwrap();
            all.push(rid);
        }
        rids_to_delete = all[0..5].to_vec();
        rids_to_keep = all[5..].to_vec();
        // Clean drop: heap flushed, WAL truncated.
    }

    // Session 2: delete 5 rows, then simulate a crash. The WAL now
    // contains exactly 5 Delete records. The heap on disk still has
    // all 20 rows (the deletes weren't checkpointed).
    {
        let mut cat = Catalog::open(&dir).unwrap();
        for rid in &rids_to_delete {
            cat.delete("users", *rid).unwrap();
        }
        std::mem::forget(cat);
    }

    // Session 3: reopen → replay the 5 Delete records → should end up
    // with 15 rows, and replay must be safely a no-op for any delete
    // whose slot is already gone.
    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(
            rows.len(),
            15,
            "after delete replay expected 15 rows, got {}",
            rows.len()
        );
        for rid in &rids_to_keep {
            assert!(
                cat.get("users", *rid).is_some(),
                "kept rid {rid:?} should still exist"
            );
        }
        for rid in &rids_to_delete {
            assert!(
                cat.get("users", *rid).is_none(),
                "deleted rid {rid:?} must be gone after replay"
            );
        }
    }

    // Session 4 (idempotence check): reopen the now-clean catalog
    // again. The previous open truncated the WAL at the end of replay,
    // so this open should be a pure no-op and the row count stays 15.
    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(rows.len(), 15);
    }

    std::fs::remove_dir_all(&dir).ok();
}

// ─── Mission B2: executor fast-path bypass recovery ────────────────────────
//
// These tests cover the WAL-logged wrappers (`update_row_bytes_logged`,
// `patch_var_col_logged`, `scan_delete_matching_logged`) that the query
// executor now routes its hot-path variants through. Before Mission B2,
// all three bypassed the WAL entirely — a crash between the mutation and
// the next checkpoint would silently lose the write on replay. Each test
// simulates exactly that crash and asserts the mutation survives.

fn user_schema_with_age() -> Schema {
    Schema {
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            },
            ColumnDef {
                name: "name".into(),
                type_id: TypeId::Str,
                required: true,
                position: 1,
            },
            ColumnDef {
                name: "age".into(),
                type_id: TypeId::Int,
                required: false,
                position: 2,
            },
        ],
    }
}

/// Fixed-width in-place patch via `update_row_bytes_logged`. Simulates the
/// executor's `update_by_pk` fast path: insert cleanly, patch `age` through
/// the byte-level closure, crash, reopen, verify the patch replayed.
#[test]
fn test_crash_recovery_update_by_pk_fast_path() {
    let dir = temp_dir("update_pk_fast");
    std::fs::create_dir_all(&dir).unwrap();

    let target_rid: RowId;
    // Session 1: insert cleanly, then the real work — one logged update
    // via the fast-path wrapper, then crash without checkpoint.
    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema_with_age()).unwrap();
        let mut all = Vec::new();
        for i in 0..10i64 {
            let rid = cat
                .insert(
                    "users",
                    &vec![
                        Value::Int(i),
                        Value::Str(format!("user_{i}")),
                        Value::Int(20 + i),
                    ],
                )
                .unwrap();
            all.push(rid);
        }
        // Clean-checkpoint the inserts so the heap has all 10 rows on
        // disk and the WAL is empty going into the fast-path update.
        cat.checkpoint().unwrap();

        // Row 5 is our target: patch age from 25 → 999 via the logged
        // byte-mutation wrapper. This is the exact API the executor's
        // `try_execute_update_pk_fast` now routes through.
        target_rid = all[5];
        let schema = cat.schema("users").unwrap().clone();
        let layout = powdb_storage::row::RowLayout::new(&schema);
        let bitmap_size = layout.bitmap_size();
        // age is column index 2 — fixed Int, 8 bytes.
        let age_off = 2 + bitmap_size + layout.fixed_offset(2).unwrap();
        let age_bitmap_byte = 2 + (2 / 8);
        let age_bit_mask = 1u8 << (2 % 8);
        let new_age: i64 = 999;
        let ok = cat
            .update_row_bytes_logged("users", target_rid, |row| {
                row[age_bitmap_byte] &= !age_bit_mask;
                row[age_off..age_off + 8].copy_from_slice(&new_age.to_le_bytes());
            })
            .unwrap();
        assert!(ok, "update_row_bytes_logged should find the row");

        // Crash — skip every Drop, leaving the WAL with exactly one
        // Update record and the heap on disk still showing age=25.
        std::mem::forget(cat);
    }

    // Session 2: reopen, replay, confirm the update landed.
    {
        let cat = Catalog::open(&dir).unwrap();
        let row = cat
            .get("users", target_rid)
            .expect("row 5 should exist after replay");
        assert_eq!(row[0], Value::Int(5));
        assert_eq!(
            row[2],
            Value::Int(999),
            "update_row_bytes_logged should replay the age patch"
        );
        // And a crude sanity check: every other row kept its original age.
        let mut saw_999 = 0;
        for (_, r) in cat.scan("users").unwrap() {
            if let Value::Int(999) = r[2] {
                saw_999 += 1;
            }
        }
        assert_eq!(saw_999, 1, "exactly one row should have age=999");
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Var-column shrink via `patch_var_col_logged`. Writes a longer name
/// cleanly, then shrinks it through the logged wrapper, crash, reopen,
/// verify the shorter name replayed.
#[test]
fn test_crash_recovery_var_col_update() {
    let dir = temp_dir("var_col_update");
    std::fs::create_dir_all(&dir).unwrap();

    let target_rid: RowId;
    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema_with_age()).unwrap();
        let mut all = Vec::new();
        for i in 0..10i64 {
            let rid = cat
                .insert(
                    "users",
                    &vec![
                        Value::Int(i),
                        // Pad the name so we have room to shrink.
                        Value::Str(format!("original_user_name_{i}")),
                        Value::Int(20 + i),
                    ],
                )
                .unwrap();
            all.push(rid);
        }
        cat.checkpoint().unwrap();

        target_rid = all[3];
        // Shrink the name column (index 1) from 21 chars to 1 char via
        // the var-col fast path. Strictly smaller, so `patch_var_col_logged`
        // stays on the in-place shrink path.
        let ok = cat
            .patch_var_col_logged("users", target_rid, 1, Some(b"x"))
            .unwrap();
        assert!(ok, "patch_var_col_logged should succeed on shrink");

        std::mem::forget(cat);
    }

    {
        let cat = Catalog::open(&dir).unwrap();
        let row = cat
            .get("users", target_rid)
            .expect("row 3 should exist after replay");
        assert_eq!(row[0], Value::Int(3));
        assert_eq!(
            row[1],
            Value::Str("x".into()),
            "patch_var_col_logged should replay the var-col shrink"
        );
        // Other rows should still have their original names.
        let cnt_short = cat
            .scan("users")
            .unwrap()
            .filter(|(_, r)| matches!(&r[1], Value::Str(s) if s == "x"))
            .count();
        assert_eq!(cnt_short, 1, "exactly one row should have the shrunk name");
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Single-pass filtered delete via `scan_delete_matching_logged`. Inserts
/// cleanly, deletes every row where age > 50 through the logged variant,
/// crash, reopen, verify only the matching rows are gone.
#[test]
fn test_crash_recovery_delete_by_filter() {
    let dir = temp_dir("delete_filter");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema_with_age()).unwrap();
        for i in 0..100i64 {
            cat.insert(
                "users",
                &vec![
                    Value::Int(i),
                    Value::Str(format!("user_{i}")),
                    Value::Int(i), // age = id for a clean age > 50 filter
                ],
            )
            .unwrap();
        }
        cat.checkpoint().unwrap();

        // Delete every row whose age > 50 via the raw-bytes predicate.
        // This is the exact shape the executor's `Delete(Filter(SeqScan))`
        // fast path now routes through.
        let schema = cat.schema("users").unwrap().clone();
        let layout = powdb_storage::row::RowLayout::new(&schema);
        let count = cat
            .scan_delete_matching_logged("users", |data| {
                match powdb_storage::row::decode_column(&schema, &layout, data, 2) {
                    Value::Int(v) => v > 50,
                    _ => false,
                }
            })
            .unwrap();
        assert_eq!(count, 49, "ages 51..=99 inclusive is 49 rows");

        std::mem::forget(cat);
    }

    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(
            rows.len(),
            51,
            "expected 100 − 49 = 51 rows after replay of filtered delete"
        );
        // Every surviving row must have age <= 50.
        for (_, r) in &rows {
            if let Value::Int(v) = &r[2] {
                assert!(*v <= 50, "surviving row has age {v} > 50");
            } else {
                panic!("expected Int age");
            }
        }
        // And every id from 0..=50 must still be present.
        let mut ids: Vec<i64> = rows
            .iter()
            .map(|(_, r)| match &r[0] {
                Value::Int(i) => *i,
                _ => panic!(),
            })
            .collect();
        ids.sort();
        let expected: Vec<i64> = (0..=50).collect();
        assert_eq!(ids, expected);
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Explicit checkpoint then additional inserts, then crash. After
/// reopen, the post-checkpoint rows must be recovered via replay, and
/// the pre-checkpoint rows (which are fully on disk) must NOT be
/// duplicated. This works because `Catalog::checkpoint` truncates the
/// WAL, so the replay only sees the post-checkpoint inserts.
#[test]
fn test_checkpoint_then_crash_no_duplicates() {
    let dir = temp_dir("checkpoint_crash");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let mut cat = Catalog::create(&dir).unwrap();
        cat.create_table(user_schema()).unwrap();
        for i in 0..30i64 {
            cat.insert("users", &vec![Value::Int(i), Value::Str(format!("a_{i}"))])
                .unwrap();
        }
        // Explicit checkpoint: heap flushed, WAL truncated.
        cat.checkpoint().unwrap();
        // Now log 10 more inserts. These go into the WAL but the
        // hot-page cache still holds them.
        for i in 30..40i64 {
            cat.insert("users", &vec![Value::Int(i), Value::Str(format!("b_{i}"))])
                .unwrap();
        }
        // Crash — the post-checkpoint 10 rows are only in the WAL.
        std::mem::forget(cat);
    }

    {
        let cat = Catalog::open(&dir).unwrap();
        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(
            rows.len(),
            40,
            "expected 30 pre-checkpoint + 10 replayed = 40 rows, got {}",
            rows.len()
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}
