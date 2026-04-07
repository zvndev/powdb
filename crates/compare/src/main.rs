//! Apples-to-apples comparison bench: PowDB vs SQLite vs Postgres.
//!
//! Runs the same four workloads against each engine and prints a comparison
//! table. This is the thesis proof — "removing SQL translation" should show
//! PowDB winning on point queries by a wide margin.
//!
//! Usage:
//!   cargo run -p powdb-compare --release
//!
//! Postgres is optional — if no server is reachable, it's skipped.
//! Set POWDB_BENCH_PG_URL to override the default connection string.

mod engines;

use engines::postgres::PostgresEngine;
use engines::powdb::PowdbEngine;
use engines::sqlite::SqliteEngine;
use engines::BenchEngine;

use std::time::Instant;

const N_ROWS: usize = 50_000;
const N_OPS: usize = 10_000;

struct BenchResult {
    engine: String,
    point_lookup_ns: f64,
    scan_filter_ns: f64,
    count_filter_ns: f64,
}

fn bench_engine(engine: &mut dyn BenchEngine) -> BenchResult {
    let name = engine.name().to_string();

    // ── Setup (not timed) ─────────────────────────────────────────────
    println!("  [{name}] populating {N_ROWS} rows...");
    engine.setup(N_ROWS);

    // ── Warm up ───────────────────────────────────────────────────────
    for i in 0..100 {
        let _ = engine.point_lookup((i * 491) % N_ROWS as i64);
    }
    let _ = engine.scan_filter_count(30);
    let _ = engine.count_filter(30);

    // ── Point lookup ──────────────────────────────────────────────────
    let start = Instant::now();
    for i in 0..N_OPS {
        let id = ((i * 491) % N_ROWS) as i64;
        let _ = engine.point_lookup(id);
    }
    let point_lookup_ns = start.elapsed().as_nanos() as f64 / N_OPS as f64;

    // ── Scan + filter count ───────────────────────────────────────────
    let n_scans = 100;
    let start = Instant::now();
    for i in 0..n_scans {
        let threshold = (20 + (i % 40)) as i64;
        let _ = engine.scan_filter_count(threshold);
    }
    let scan_filter_ns = start.elapsed().as_nanos() as f64 / n_scans as f64;

    // ── Count with filter ─────────────────────────────────────────────
    let start = Instant::now();
    for i in 0..n_scans {
        let threshold = (20 + (i % 40)) as i64;
        let _ = engine.count_filter(threshold);
    }
    let count_filter_ns = start.elapsed().as_nanos() as f64 / n_scans as f64;

    BenchResult {
        engine: name,
        point_lookup_ns,
        scan_filter_ns,
        count_filter_ns,
    }
}

fn fmt_ns(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{:.0} ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.1} us", ns / 1_000.0)
    } else {
        format!("{:.2} ms", ns / 1_000_000.0)
    }
}

fn main() {
    println!("PowDB comparison bench — {N_ROWS} rows, {N_OPS} point lookups\n");

    let mut results: Vec<BenchResult> = Vec::new();

    // ── PowDB ─────────────────────────────────────────────────────────
    {
        let mut engine = PowdbEngine::new();
        results.push(bench_engine(&mut engine));
    }

    // ── SQLite ────────────────────────────────────────────────────────
    {
        let mut engine = SqliteEngine::new();
        results.push(bench_engine(&mut engine));
    }

    // ── Postgres (optional) ───────────────────────────────────────────
    match PostgresEngine::try_new() {
        Some(mut engine) => {
            results.push(bench_engine(&mut engine));
        }
        None => {
            println!("  [Postgres] skipped — no server reachable");
            println!("    (set POWDB_BENCH_PG_URL or start postgres on localhost:5432)\n");
        }
    }

    // ── Results table ─────────────────────────────────────────────────
    println!();
    println!(
        "{:<12} {:>16} {:>16} {:>16}",
        "engine", "point lookup", "scan+filter", "count+filter"
    );
    println!("{}", "─".repeat(64));

    for r in &results {
        println!(
            "{:<12} {:>16} {:>16} {:>16}",
            r.engine,
            fmt_ns(r.point_lookup_ns),
            fmt_ns(r.scan_filter_ns),
            fmt_ns(r.count_filter_ns),
        );
    }

    // ── Ratios vs PowDB ──────────────────────────────────────────────
    if results.len() > 1 {
        let powdb = &results[0];
        println!();
        println!(
            "{:<12} {:>16} {:>16} {:>16}",
            "ratio", "point lookup", "scan+filter", "count+filter"
        );
        println!("{}", "─".repeat(64));
        for r in results.iter().skip(1) {
            let pl_ratio = r.point_lookup_ns / powdb.point_lookup_ns;
            let sf_ratio = r.scan_filter_ns / powdb.scan_filter_ns;
            let cf_ratio = r.count_filter_ns / powdb.count_filter_ns;
            println!(
                "{:<12} {:>15.1}x {:>15.1}x {:>15.1}x",
                format!("{}/powdb", r.engine),
                pl_ratio,
                sf_ratio,
                cf_ratio,
            );
        }
    }

    println!();
}
