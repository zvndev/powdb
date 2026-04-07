//! Smoke benchmark — NOT a publishable measurement.
//!
//! This is a sanity check to answer one question: "is the native engine in
//! the right order of magnitude?" If these numbers come out within a factor
//! of 2-3x of the JS scaffolding numbers (1.06M point lookups/sec, 1.02M
//! B-tree lookups/sec on 500K rows), the architecture ported cleanly. If
//! they come out 10x slower, we have a Rust-specific regression to hunt.
//!
//! This deliberately does NOT use criterion. No statistical analysis, no
//! warm-up, no per-op overhead measurement. Just wall clock over N iterations.
//! The formal spec (coming next) will use criterion with proper methodology.

use powdb_query::executor::Engine;
use powdb_storage::types::*;
use std::path::PathBuf;
use std::time::Instant;

const N_ROWS: usize = 50_000;
const N_LOOKUPS: usize = 200_000;
const N_SCANS: usize = 100;
const N_POWQL: usize = 2_000;

fn main() {
    let data_dir = std::env::temp_dir().join("powdb_bench_smoke");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();

    println!("PowDB smoke benchmark");
    println!("data dir: {}", data_dir.display());
    println!("rows:     {N_ROWS}");
    println!();

    let mut engine = Engine::new(&data_dir).unwrap();
    bench_insert(&mut engine, &data_dir);
    bench_index_lookup(&mut engine);
    bench_seq_scan(&mut engine);
    bench_powql_parsed(&mut engine);

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ───── 1. Insert throughput (direct path, no PowQL parse) ──────────────────

fn bench_insert(engine: &mut Engine, data_dir: &PathBuf) {
    // Create schema via the engine so the catalog persists it.
    engine
        .execute_powql(
            "type User { required id: int, required name: str, required age: int }",
        )
        .expect("create type");

    let table = engine.catalog_mut().get_table_mut("User").unwrap();

    let start = Instant::now();
    for i in 0..N_ROWS {
        let row = vec![
            Value::Int(i as i64),
            Value::Str(format!("user_{i}")),
            Value::Int((18 + (i % 60)) as i64),
        ];
        table.insert(&row).unwrap();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / N_ROWS as u128;
    let ops_per_sec = (N_ROWS as f64) / elapsed.as_secs_f64();

    println!("[1] direct insert ({N_ROWS} rows)");
    println!("    total:   {:>10.3} ms", elapsed.as_secs_f64() * 1000.0);
    println!("    per op:  {:>10} ns", per_op);
    println!("    ops/sec: {:>10.0}", ops_per_sec);
    println!();

    // Build an index on .id so the next benchmark has something to use.
    let build_start = Instant::now();
    table.create_index("id", data_dir).unwrap();
    println!(
        "    built B-tree index on .id in {:.2} ms",
        build_start.elapsed().as_secs_f64() * 1000.0,
    );
    println!();
}

// ───── 2. Point lookup via B-tree (the 42x path) ────────────────────────────

fn bench_index_lookup(engine: &mut Engine) {
    let table = engine.catalog().get_table("User").unwrap();

    // Warm-up. Not statistically rigorous — just gets caches into a
    // consistent-ish state.
    for i in 0..1_000 {
        let _ = table.index_lookup("id", &Value::Int((i % N_ROWS as i64) as i64));
    }

    let start = Instant::now();
    let mut hits = 0usize;
    for i in 0..N_LOOKUPS {
        let key = Value::Int((i % N_ROWS) as i64);
        if table.index_lookup("id", &key).is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / N_LOOKUPS as u128;
    let ops_per_sec = (N_LOOKUPS as f64) / elapsed.as_secs_f64();

    println!("[2] B-tree point lookup ({N_LOOKUPS} ops, {hits} hits)");
    println!("    total:   {:>10.3} ms", elapsed.as_secs_f64() * 1000.0);
    println!("    per op:  {:>10} ns", per_op);
    println!("    ops/sec: {:>10.0}", ops_per_sec);
    println!("    JS ref:  1_020_000 ops/sec (500K rows, same B-tree order)");
    println!();
}

// ───── 3. Sequential scan with predicate ────────────────────────────────────

fn bench_seq_scan(engine: &mut Engine) {
    let table = engine.catalog().get_table("User").unwrap();

    // Warm-up
    for _ in 0..2 {
        let _ = table.scan().count();
    }

    let start = Instant::now();
    let mut total_matches = 0usize;
    for _ in 0..N_SCANS {
        let matches = table
            .scan()
            .filter(|(_, row)| match &row[2] {
                Value::Int(age) => *age > 30,
                _ => false,
            })
            .count();
        total_matches += matches;
    }
    let elapsed = start.elapsed();
    let per_scan = elapsed.as_secs_f64() * 1000.0 / N_SCANS as f64;
    let rows_per_sec = (N_ROWS as f64 * N_SCANS as f64) / elapsed.as_secs_f64();

    println!("[3] sequential scan + filter ({N_SCANS} scans of {N_ROWS} rows)");
    println!("    per scan:  {:>10.3} ms", per_scan);
    println!("    rows/sec:  {:>10.0}", rows_per_sec);
    println!("    matches:   {} per scan", total_matches / N_SCANS);
    println!();
}

// ───── 4. Full PowQL-parsed query path ─────────────────────────────────────

fn bench_powql_parsed(engine: &mut Engine) {
    // Warm-up parse cache behavior
    for _ in 0..10 {
        let _ = engine.execute_powql("User filter .id = 42 { .id, .name }");
    }

    let start = Instant::now();
    let mut hits = 0usize;
    for i in 0..N_POWQL {
        let q = format!("User filter .id = {} {{ .id, .name }}", i % N_ROWS);
        match engine.execute_powql(&q) {
            Ok(_) => hits += 1,
            Err(e) => {
                eprintln!("query failed at i={i}: {e}");
                return;
            }
        }
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / N_POWQL as u128;
    let ops_per_sec = (N_POWQL as f64) / elapsed.as_secs_f64();

    println!("[4] full PowQL parse + plan + execute ({N_POWQL} queries, {hits} hits)");
    println!("    per op:  {:>10} ns ({:.3} ms)", per_op, per_op as f64 / 1_000_000.0);
    println!("    ops/sec: {:>10.0}", ops_per_sec);
    println!();

    println!("Ratio check: direct [2] vs parsed [4] should be ~5-10x gap for PowDB.");
    println!("The JS scaffolding showed ~42x for SQL; PowQL keeps the parser simple.");
}
