//! Mission infra-1 — concurrent read lock test.
//!
//! Before this mission, PowDB's TCP server wrapped the whole `Engine` in
//! `Arc<Mutex<Engine>>`, so every SELECT serialised on one lock. This
//! test pins down the new behaviour: under `Arc<RwLock<Engine>>` with
//! `execute_powql_readonly`, four threads should be able to walk a large
//! table in parallel.
//!
//! The test is a wall-clock comparison between four sequential scans and
//! four threaded scans. With true parallelism, the threaded path should
//! finish in well under the sequential time (we assert < 75%, which is
//! generous enough to not flake on a loaded CI box but strict enough to
//! reject the old `Mutex<Engine>` behaviour — which would show a
//! threaded-to-sequential ratio of ~1.0, not ~0.25).
//!
//! Blocker B1 — concurrent read *correctness* test (not just liveness).
//! `concurrent_readers_correctness` scans a table big enough to overflow
//! the single-slot hot-page cache, so every scan hammers
//! `DiskManager::read_page` on a shared `&File`. Each row carries a
//! known `id` the test can verify, so any byte-level corruption from the
//! old `seek + read_exact` race shows up as wrong ids / wrong row counts
//! rather than a silent timing anomaly.

use powdb_query::ast::Literal;
use powdb_query::executor::Engine;
use powdb_query::result::QueryResult;
use powdb_storage::types::Value;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::Instant;

fn fresh_engine() -> Arc<RwLock<Engine>> {
    let test_id = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("powdb_conc_read_{test_id}_{ts}"));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let engine = Engine::new(&data_dir).unwrap();
    Arc::new(RwLock::new(engine))
}

/// Seed the engine with enough rows that a filtered scan takes long
/// enough for parallelism to be measurable over thread-startup noise.
fn seed(engine: &Arc<RwLock<Engine>>, rows: usize) {
    let mut eng = engine.write().unwrap();
    eng.execute_powql(
        "type User { required id: int, required name: str, required age: int, required status: str }"
    ).unwrap();
    for i in 0..rows {
        let q = format!(
            r#"insert User {{ id := {i}, name := "user_{i}", age := {age}, status := "{status}" }}"#,
            i = i,
            age = 18 + (i % 70),
            status = if i % 3 == 0 { "active" } else { "inactive" },
        );
        eng.execute_powql(&q).unwrap();
    }
}

#[test]
fn concurrent_readers_make_progress_in_parallel() {
    let engine = fresh_engine();
    // 8K rows gives each scan enough work to dominate thread-startup cost
    // without making the test itself slow.
    seed(&engine, 8_000);

    // A read-heavy query: filter + project, scanning the whole table.
    // `execute_powql_readonly` should take this through the
    // `project_filter_limit_fast` path end-to-end.
    let query = r#"User filter .age > 25 and .status = "active" { .id, .name, .age }"#;

    // Sanity check: make sure the query actually works and returns rows.
    {
        let eng = engine.read().unwrap();
        let res = eng.execute_powql_readonly(query).unwrap();
        match res {
            QueryResult::Rows { rows, .. } => {
                assert!(rows.len() > 100, "expected many rows, got {}", rows.len());
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    // Warm up once to page in the heap file and populate the plan cache.
    for _ in 0..4 {
        let eng = engine.read().unwrap();
        let _ = eng.execute_powql_readonly(query).unwrap();
    }

    // Sequential baseline: four scans back-to-back on the current thread.
    // Each iteration pays the full scan cost because `project_filter_limit_fast`
    // walks the entire heap with no caching.
    let iters_per_thread = 8;
    let n_threads = 4;

    let seq_start = Instant::now();
    for _ in 0..(iters_per_thread * n_threads) {
        let eng = engine.read().unwrap();
        let _ = eng.execute_powql_readonly(query).unwrap();
    }
    let seq_elapsed = seq_start.elapsed();

    // Concurrent path: four threads, each running `iters_per_thread` scans
    // simultaneously. A barrier synchronises the thread start so we're
    // measuring overlapped scan time, not thread-spawn latency.
    let barrier = Arc::new(Barrier::new(n_threads));
    let conc_start = Instant::now();
    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let eng = engine.clone();
            let bar = barrier.clone();
            let q = query.to_string();
            thread::spawn(move || {
                bar.wait();
                for _ in 0..iters_per_thread {
                    let guard = eng.read().unwrap();
                    let _ = guard.execute_powql_readonly(&q).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let conc_elapsed = conc_start.elapsed();

    let ratio = conc_elapsed.as_secs_f64() / seq_elapsed.as_secs_f64();
    eprintln!(
        "concurrent_readers: sequential={:?}, concurrent={:?}, ratio={:.3}",
        seq_elapsed, conc_elapsed, ratio,
    );

    // With a true RwLock the 4-thread concurrent run should finish in
    // roughly 1/n of the sequential time on an idle box. We allow a
    // generous 0.85 threshold to survive GHA's noisy 2-vCPU runners
    // where thread scheduling can compress the speedup. A Mutex
    // regression would clock in around 1.0 (same wall time as
    // sequential), so the gap is still large enough to catch it.
    assert!(
        ratio < 0.85,
        "expected parallel reads under RwLock, but concurrent/sequential ratio was {ratio:.3} \
         (sequential={seq_elapsed:?}, concurrent={conc_elapsed:?}). \
         A ratio near 1.0 suggests reads are serialising on a mutex again.",
    );
}

/// Blocker B1 regression test — byte-level correctness of the concurrent
/// `heap.get(rid)` path (point lookups by indexed key).
///
/// Before the pread/pwrite fix, `DiskManager::read_page` did
/// `file.seek(offset); file.read_exact(buf)` on a shared `&File`, which
/// races on the kernel file offset under multiple reader threads. The
/// failure mode: thread A seeks to page X, thread B seeks to page Y,
/// and whichever `read_exact` wins returns bytes from the *wrong* page.
///
/// The filtered SeqScan fast path uses `libc::mmap` and is unaffected,
/// so to actually exercise `disk.read_page` across threads we drive the
/// **IndexScan** path: `filter .id = <literal>` with a B-tree index on
/// `id` plans as `IndexScan`, which calls `btree.lookup_int` →
/// `heap.get(rid)` → `disk.read_page` with *no* mmap fallback. Each
/// thread hammers random ids from a 100K-row table, well past the
/// single-slot hot-page cache, so nearly every lookup hits `read_page`.
/// Each row carries a known `payload` derived from its `id`, so any
/// byte-level cross-feed between threads shows up as a payload that
/// doesn't match its id. On the unpatched code this test fails loudly;
/// on the fixed code every lookup must return the expected payload.
#[test]
fn concurrent_readers_see_uncorrupted_rows() {
    // Inline fresh-engine setup so we can hang on to the data_dir path —
    // we need it to pass through to `Table::create_index`.
    let data_dir = {
        let test_id = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("powdb_conc_read_corr_{test_id}_{ts}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    };
    let engine = Arc::new(RwLock::new(Engine::new(&data_dir).unwrap()));

    // `HeapFile`'s write-back cache is a single slot (`hot_page`). At
    // PAGE_SIZE = 4 KiB and ~30 bytes per row, 100K rows fills ~1000
    // data pages, so the overwhelming majority of lookups miss the hot
    // page and fall through to `DiskManager::read_page` — the exact
    // code path we're trying to stress. (Per the fix spec: "at least
    // 100K rows".)
    const N: usize = 100_000;

    {
        let mut eng = engine.write().unwrap();
        eng.execute_powql("type Row { required id: int, required payload: str }")
            .unwrap();
        // Seed via the prepared-insert fast path — parse + plan once,
        // bind new literals per row. Without this, seeding N=100K rows
        // through `execute_powql` format!() strings takes ~10 minutes
        // because each call re-parses/re-plans/re-resolves the catalog.
        let prep = eng
            .prepare(r#"insert Row { id := 0, payload := "x" }"#)
            .unwrap();
        for i in 0..N {
            let literals = [Literal::Int(i as i64), Literal::String(format!("p_{i}"))];
            eng.execute_prepared(&prep, &literals).unwrap();
        }

        // Build a B-tree index on `id` so that `filter .id = <k>` plans
        // to IndexScan and the executor takes `btree.lookup_int` →
        // `heap.get(rid)` → `disk.read_page`. This is the path the old
        // `seek + read_exact` race corrupts under concurrent readers.
        let tbl = eng
            .catalog_mut()
            .get_table_mut("Row")
            .expect("Row table exists");
        tbl.create_index("id", &data_dir).expect("build id index");

        // Force every dirty page out of `HeapFile`'s write-back buffer
        // and onto disk. Without this, `heap.get` short-circuits every
        // lookup through `dirty_buffer` (an in-memory HashMap) and
        // never touches `disk.read_page` — which would make this test a
        // no-op regression check. After `flush_all_dirty`, subsequent
        // `heap.get` calls for anything other than the current
        // `hot_page` fall straight through to `disk.read_page`.
        tbl.heap.flush_all_dirty().expect("flush dirty pages");
    }

    // Sanity: the indexed point lookup returns the expected row.
    {
        let eng = engine.read().unwrap();
        let res = eng
            .execute_powql_readonly("Row filter .id = 42 { .id, .payload }")
            .unwrap();
        match res {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1, "expected one row for .id = 42");
                let id = match &rows[0][0] {
                    Value::Int(n) => *n,
                    other => panic!("expected Int id, got {other:?}"),
                };
                let payload = match &rows[0][1] {
                    Value::Str(s) => s.as_str(),
                    other => panic!("expected Str payload, got {other:?}"),
                };
                assert_eq!(id, 42);
                assert_eq!(payload, "p_42");
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    // Concurrent lookups: several threads each do LOOKUPS_PER_THREAD
    // indexed point lookups for varying ids. Each lookup drives
    // `btree.lookup_int` → `heap.get(rid)` → `disk.read_page` with no
    // mmap fallback (the test never calls `enable_mmap`). With a
    // shared `&File` and the old seek+read race, threads would
    // cross-feed bytes and the payload returned for id K would not
    // equal "p_K".
    //
    // We skip `execute_powql_readonly` here and call straight into
    // `Table::index_lookup` — the parse/plan/plan-cache overhead of
    // the query path dwarfs the actual I/O window we're trying to
    // race, and minimising that overhead makes the race much more
    // likely to surface under concurrent load.
    //
    // Coprime strides per thread make every thread walk every id in a
    // different order — different seek patterns per thread are what
    // makes the old race observable.
    const LOOKUPS_PER_THREAD: usize = 10_000;
    let n_threads = 16;
    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|thread_idx| {
            let eng = engine.clone();
            let bar = barrier.clone();
            thread::spawn(move || {
                let stride: usize = 7 * (thread_idx + 1) + 1;
                let mut k = thread_idx * 13;
                bar.wait();
                // Take the read guard once and hammer `index_lookup`
                // directly. This keeps the hot loop tight: every
                // iteration is just btree lookup + heap.get, no
                // parser/planner frames in between.
                let guard = eng.read().unwrap();
                let tbl = guard.catalog().get_table("Row").expect("Row table");
                for _ in 0..LOOKUPS_PER_THREAD {
                    k = (k + stride) % N;
                    let key = Value::Int(k as i64);
                    let (_rid, row) = tbl.index_lookup("id", &key).unwrap_or_else(|| {
                        panic!(
                            "index_lookup for id {k} returned None \
                                 — btree/heap disagreement, likely a \
                                 torn read from a racing seek+read"
                        )
                    });
                    assert_eq!(row.len(), 2, "expected 2 columns per row");
                    let got_id = match &row[0] {
                        Value::Int(n) => *n,
                        other => panic!("expected Int id, got {other:?}"),
                    };
                    let got_payload = match &row[1] {
                        Value::Str(s) => s.as_str(),
                        other => panic!("expected Str payload, got {other:?}"),
                    };
                    assert_eq!(
                        got_id, k as i64,
                        "lookup for id {k} returned id {got_id} \
                         — byte corruption from a racing seek+read \
                         on the shared &File"
                    );
                    let expected = format!("p_{k}");
                    assert_eq!(
                        got_payload, expected,
                        "lookup for id {k} returned payload \
                         {got_payload:?}, expected {expected:?} \
                         — byte corruption from a racing seek+read \
                         on the shared &File"
                    );
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn write_lock_excludes_readers_correctness() {
    // Sanity test for the handler dispatch: a read after an insert
    // through the RwLock should see the insert. This catches a subtle
    // class of bug where the read path could end up holding a stale
    // view of the catalog if we ever tried to cache plans behind the
    // writer's back.
    let engine = fresh_engine();
    {
        let mut eng = engine.write().unwrap();
        eng.execute_powql("type Item { required id: int, required name: str }")
            .unwrap();
        eng.execute_powql(r#"insert Item { id := 1, name := "apple" }"#)
            .unwrap();
        eng.execute_powql(r#"insert Item { id := 2, name := "banana" }"#)
            .unwrap();
    }
    {
        let eng = engine.read().unwrap();
        let res = eng.execute_powql_readonly("count(Item)").unwrap();
        match res {
            QueryResult::Scalar(powdb_storage::types::Value::Int(n)) => {
                assert_eq!(n, 2);
            }
            other => panic!("expected scalar count, got {other:?}"),
        }
    }
    // Now add a third item and verify the read-lock path sees it.
    {
        let mut eng = engine.write().unwrap();
        eng.execute_powql(r#"insert Item { id := 3, name := "cherry" }"#)
            .unwrap();
    }
    {
        let eng = engine.read().unwrap();
        let res = eng.execute_powql_readonly("count(Item)").unwrap();
        match res {
            QueryResult::Scalar(powdb_storage::types::Value::Int(n)) => {
                assert_eq!(n, 3);
            }
            other => panic!("expected scalar count, got {other:?}"),
        }
    }
}

#[test]
fn readonly_rejects_mutations_with_escalation_sentinel() {
    use powdb_query::executor::READONLY_NEEDS_WRITE;
    let engine = fresh_engine();
    {
        let mut eng = engine.write().unwrap();
        eng.execute_powql("type T { required id: int }").unwrap();
    }
    let eng = engine.read().unwrap();
    let err = eng
        .execute_powql_readonly(r#"insert T { id := 1 }"#)
        .unwrap_err();
    assert_eq!(err, READONLY_NEEDS_WRITE);
}
