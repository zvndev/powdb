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

use powdb_query::executor::Engine;
use powdb_query::result::QueryResult;
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
    let handles: Vec<_> = (0..n_threads).map(|_| {
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
    }).collect();
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
    // generous 0.75 threshold to survive CI noise. A Mutex regression
    // would clock in around 1.0 (same wall time as sequential), so this
    // gap is large enough to catch it reliably.
    assert!(
        ratio < 0.75,
        "expected parallel reads under RwLock, but concurrent/sequential ratio was {ratio:.3} \
         (sequential={seq_elapsed:?}, concurrent={conc_elapsed:?}). \
         A ratio near 1.0 suggests reads are serialising on a mutex again.",
    );
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
        eng.execute_powql("type Item { required id: int, required name: str }").unwrap();
        eng.execute_powql(r#"insert Item { id := 1, name := "apple" }"#).unwrap();
        eng.execute_powql(r#"insert Item { id := 2, name := "banana" }"#).unwrap();
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
        eng.execute_powql(r#"insert Item { id := 3, name := "cherry" }"#).unwrap();
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
    let err = eng.execute_powql_readonly(r#"insert T { id := 1 }"#).unwrap_err();
    assert_eq!(err, READONLY_NEEDS_WRITE);
}
