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
use powdb_storage::types::{ColumnDef, Schema, TypeId, Value};

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
