//! Mission 3: index persistence integration test.
//!
//! Verifies that a b-tree index built via `Catalog::create_index` survives
//! a `Catalog::open` round-trip without re-running CREATE INDEX. Also
//! checks that rows inserted *after* the reopen land in the rehydrated
//! index, so subsequent lookups still hit the fast path.

use powdb_storage::catalog::Catalog;
use powdb_storage::types::{ColumnDef, Row, RowId, Schema, TypeId, Value};
use std::path::PathBuf;

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "powdb_index_persist_{name}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
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

fn row(id: i64, name: &str) -> Row {
    vec![Value::Int(id), Value::Str(name.into())]
}

#[test]
fn test_index_survives_catalog_reopen() {
    let dir = fresh_dir("survives_reopen");

    // Phase 1: create the catalog, build the table, build an index, insert
    // some rows, sanity-check the index works.
    {
        let mut cat = Catalog::create(&dir).expect("create catalog");
        cat.create_table(user_schema()).expect("create table");
        cat.create_index("users", "id").expect("create index");

        for i in 0..100i64 {
            cat.insert("users", &row(i, &format!("user_{i}")))
                .unwrap_or_else(|e| panic!("insert row {i}: {e}"));
        }

        // Hit the index for every key — confirms the in-memory tree is
        // populated end-to-end before we restart.
        for i in 0..100i64 {
            let result = cat
                .index_lookup("users", "id", &Value::Int(i))
                .expect("index_lookup")
                .unwrap_or_else(|| panic!("missing key {i} pre-restart"));
            assert_eq!(result[0], Value::Int(i));
            assert_eq!(result[1], Value::Str(format!("user_{i}")));
        }

        // Confirm the index file actually landed on disk — without this,
        // the rest of the test would silently rebuild the index from the
        // heap on every reopen and pass for the wrong reason.
        let idx_path = dir.join("users_id.idx");
        assert!(
            idx_path.exists(),
            "expected index file at {}",
            idx_path.display()
        );
    }

    // Phase 2: drop the catalog, reopen, and verify the index is still
    // there with no manual CREATE INDEX needed.
    {
        let mut cat = Catalog::open(&dir).expect("reopen catalog");

        // The catalog must still know about the indexed column. We can
        // observe this two ways: index_lookup must succeed for an
        // existing key, AND inserting a new key must hit the index too.
        for i in 0..100i64 {
            let result = cat
                .index_lookup("users", "id", &Value::Int(i))
                .expect("index_lookup post-reopen")
                .unwrap_or_else(|| panic!("missing key {i} post-restart"));
            assert_eq!(result[0], Value::Int(i));
            assert_eq!(result[1], Value::Str(format!("user_{i}")));
        }

        // Missing key should still miss (i.e. we're hitting a real index,
        // not silently scanning the heap).
        assert!(
            cat.index_lookup("users", "id", &Value::Int(9999))
                .unwrap()
                .is_none(),
            "phantom index hit on missing key"
        );

        // Insert a new row after reopen — the rehydrated index must
        // accept the write and be queryable.
        cat.insert("users", &row(424242, "post_restart"))
            .expect("insert post-restart");
        let after = cat
            .index_lookup("users", "id", &Value::Int(424242))
            .unwrap()
            .unwrap_or_else(|| panic!("post-restart insert not in index"));
        assert_eq!(after[1], Value::Str("post_restart".into()));
    }

    // Phase 3: reopen one more time to confirm the post-restart insert
    // was also persisted (i.e. saves on insert really do flush).
    {
        let cat = Catalog::open(&dir).expect("third reopen");
        let after = cat
            .index_lookup("users", "id", &Value::Int(424242))
            .unwrap()
            .unwrap_or_else(|| panic!("post-restart insert lost across second reopen"));
        assert_eq!(after[1], Value::Str("post_restart".into()));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_index_rehydrates_via_rebuild_when_idx_file_missing() {
    // Mission 3: pre-existing catalogs (e.g. anything created before this
    // mission landed) won't have a `.idx` file on disk even if the
    // catalog metadata lists the column as indexed. The Table::open path
    // must rebuild from the heap and re-save in that case so the next
    // open is fast.
    let dir = fresh_dir("rebuild_on_missing");

    {
        let mut cat = Catalog::create(&dir).expect("create catalog");
        cat.create_table(user_schema()).expect("create table");
        cat.create_index("users", "id").expect("create index");
        for i in 0..50i64 {
            cat.insert("users", &row(i, &format!("user_{i}"))).unwrap();
        }
    }

    // Simulate the upgrade scenario: catalog still claims an index on
    // `id`, but the .idx file is gone.
    let idx_path = dir.join("users_id.idx");
    std::fs::remove_file(&idx_path).expect("remove .idx file");
    assert!(!idx_path.exists());

    {
        let cat = Catalog::open(&dir).expect("reopen with missing idx");
        // The rebuild path should have repopulated the in-memory tree
        // from the heap.
        for i in 0..50i64 {
            let result = cat
                .index_lookup("users", "id", &Value::Int(i))
                .unwrap()
                .unwrap_or_else(|| panic!("missing key {i} after rebuild"));
            assert_eq!(result[0], Value::Int(i));
        }
        // And the rebuild should have written a fresh `.idx` file so
        // the next open hits the fast path.
        assert!(
            idx_path.exists(),
            "expected rebuilt index to be written back to {}",
            idx_path.display()
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_index_crash_before_save_rebuilds_from_heap() {
    // Blocker B3: under the deferred-save model, `Table::insert` no
    // longer fsyncs `.idx` files per row — it marks the tree dirty in
    // memory and defers the save to `Catalog::checkpoint` / `Drop`.
    //
    // That change is only correct if WAL replay + post-replay
    // rebuild put the index back in sync with the heap after a
    // crash. Simulate the crash by `mem::forget`ing the catalog so
    // its `Drop` (which normally calls `checkpoint`) never runs, then
    // reopen and confirm every inserted key is still findable via
    // the index fast path.
    let dir = fresh_dir("crash_before_save");

    {
        let mut cat = Catalog::create(&dir).expect("create catalog");
        cat.create_table(user_schema()).expect("create table");
        cat.create_index("users", "id").expect("create index");
        // Force one clean checkpoint here so the catalog.bin and the
        // `.idx` file land on disk — otherwise `Catalog::open` has
        // nothing to rehydrate from besides the WAL.
        cat.checkpoint().expect("initial checkpoint");

        for i in 0..100i64 {
            cat.insert("users", &row(i, &format!("user_{i}"))).unwrap();
        }
        // Intentionally skip `cat.checkpoint()`. The indexes are now
        // dirty in memory — if we drop cleanly they would be saved,
        // so bypass Drop with `mem::forget` to emulate a crash
        // between the last insert and the next checkpoint.
        std::mem::forget(cat);
    }

    {
        let cat = Catalog::open(&dir).expect("reopen after crash");
        for i in 0..100i64 {
            let result = cat
                .index_lookup("users", "id", &Value::Int(i))
                .unwrap()
                .unwrap_or_else(|| panic!("key {i} missing post-crash: index not rebuilt"));
            assert_eq!(result[0], Value::Int(i));
            assert_eq!(result[1], Value::Str(format!("user_{i}")));
        }
        // Missing keys should still miss — confirms we're hitting a
        // real (rebuilt) btree, not falling through to a heap scan.
        assert!(cat
            .index_lookup("users", "id", &Value::Int(9999))
            .unwrap()
            .is_none());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_index_persists_deletes_across_reopen() {
    // Confirms the delete path saves the mutated btree, so a deleted key
    // stays deleted after restart (rather than being silently
    // resurrected from a stale .idx file).
    let dir = fresh_dir("persists_deletes");

    let rid_to_delete: RowId;
    {
        let mut cat = Catalog::create(&dir).expect("create catalog");
        cat.create_table(user_schema()).expect("create table");
        cat.create_index("users", "id").expect("create index");
        let mut deleted_rid = None;
        for i in 0..20i64 {
            let r = cat.insert("users", &row(i, &format!("user_{i}"))).unwrap();
            if i == 7 {
                deleted_rid = Some(r);
            }
        }
        rid_to_delete = deleted_rid.unwrap();
        cat.delete("users", rid_to_delete).expect("delete row 7");
        // Pre-restart: deleted key must be gone from the index.
        assert!(
            cat.index_lookup("users", "id", &Value::Int(7))
                .unwrap()
                .is_none(),
            "deleted key still present pre-restart"
        );
    }

    {
        let cat = Catalog::open(&dir).expect("reopen");
        // Post-restart: deleted key still gone (delete path persisted
        // its mutation to the .idx file).
        assert!(
            cat.index_lookup("users", "id", &Value::Int(7))
                .unwrap()
                .is_none(),
            "deleted key resurrected after restart"
        );
        // Other keys still there.
        for i in [0, 5, 10, 19] {
            assert!(
                cat.index_lookup("users", "id", &Value::Int(i))
                    .unwrap()
                    .is_some(),
                "key {i} missing after restart"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
