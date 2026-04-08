//! Mission A wide bench runner — apples-to-apples comparison across
//! PowDB, SQLite, (optionally) Postgres, and (optionally) MySQL over
//! 15 canonical Mission A workloads.
//!
//! Usage:
//!   cargo run -p powdb-compare --release
//!
//! Environment variables:
//!   BENCH_N_ROWS       — override the fixture size (default 100_000).
//!   POWDB_BENCH_PG_URL — override Postgres connection URL, or `skip`.
//!   POWDB_BENCH_MYSQL_URL — override MySQL connection URL.
//!
//! Postgres and MySQL are optional — unreachable servers are skipped
//! with a `[skipped]` line rather than failing the run.

mod engines;

use engines::postgres::PostgresEngine;
use engines::powdb::PowdbEngine;
use engines::sqlite::SqliteEngine;
use engines::{gen_row, BenchEngine, STATUSES};

use std::io::Write;
use std::time::Instant;

/// All 15 Mission A workload names, in canonical order.
/// These strings are the headers in the printed table and the `workload`
/// column in `results.csv`, and must match exactly the names used by the
/// criterion bench (see PLAN-MISSION-A.md §4 BENCH → CRITERION contract).
const WORKLOADS: &[&str] = &[
    "point_lookup_indexed",
    "point_lookup_nonindexed",
    "scan_filter_count",
    "scan_filter_project_top100",
    "scan_filter_sort_limit10",
    "agg_sum",
    "agg_avg",
    "agg_min",
    "agg_max",
    "multi_col_and_filter",
    "insert_single",
    "insert_batch_1k",
    "update_by_pk",
    "update_by_filter",
    "delete_by_filter",
];

/// Per-(engine, workload) timing result in nanoseconds-per-op.
struct Cell {
    ns_per_op: f64,
}

struct EngineResults {
    name: String,
    cells: Vec<Option<Cell>>, // index matches WORKLOADS
}

/// Number of ops per read workload. Kept modest so 15 workloads × 4
/// engines still finishes in a few minutes.
const READ_OPS: usize = 2_000;
/// Number of ops per write workload.
const WRITE_OPS: usize = 1_000;
/// Number of rebuild-and-delete iterations for the destructive
/// `delete_by_filter` workload.
const DELETE_ITERS: usize = 3;

fn time_iter<F: FnMut()>(n: usize, mut f: F) -> f64 {
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    start.elapsed().as_nanos() as f64 / n as f64
}

/// Parse the `BENCH_N_ROWS` env var (defaults to 100_000).
fn parse_n_rows() -> usize {
    std::env::var("BENCH_N_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000)
}

/// Parse an optional `--workloads name1,name2` CLI argument.
///
/// Returns `None` if the flag is not set (run all workloads).
fn parse_workload_filter() -> Option<Vec<String>> {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--workloads" {
            let value = args.next()?;
            return Some(
                value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        } else if let Some(rest) = a.strip_prefix("--workloads=") {
            return Some(
                rest.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
    }
    None
}

fn selected_workloads(filter: &Option<Vec<String>>) -> Vec<&'static str> {
    match filter {
        Some(names) => WORKLOADS
            .iter()
            .copied()
            .filter(|w| names.iter().any(|n| n == w))
            .collect(),
        None => WORKLOADS.to_vec(),
    }
}

/// Run every selected workload against a single engine. Returns a sparse
/// cell vector aligned with `WORKLOADS` (indices missing from
/// `selected` are left as `None`).
fn bench_engine(
    engine: &mut dyn BenchEngine,
    n_rows: usize,
    selected: &[&'static str],
) -> EngineResults {
    let name = engine.name().to_string();
    println!("  [{name}] populating {n_rows} rows...");
    engine.setup(n_rows);

    // Warm up: one call per read workload to prime caches, JIT
    // plan-cache entries, and so on.
    warmup(engine, n_rows, selected);

    let mut cells: Vec<Option<Cell>> = WORKLOADS.iter().map(|_| None).collect();

    // Precomputed helper inputs reused across iterations.
    let mut next_insert_id: i64 = n_rows as i64 + 1;

    // Prebuild the batch for `insert_batch_1k` — keeps format!()
    // allocation out of the timed loop per §2 of the plan. Ids are
    // allocated lazily at the start of the workload so they don't collide
    // with the `insert_single` workload's IDs.
    let batch_size: usize = 1_000;

    for (idx, workload) in WORKLOADS.iter().enumerate() {
        if !selected.contains(workload) {
            continue;
        }
        println!("  [{name}] running {workload}...");
        let ns = match *workload {
            "point_lookup_indexed" => time_iter(READ_OPS, || {
                let id = ((fastrand_xor(idx as u64) as usize) % n_rows) as i64;
                let _ = engine.point_lookup_indexed(id);
            }),
            "point_lookup_nonindexed" => time_iter(READ_OPS, || {
                let id = (fastrand_xor(idx as u64) as usize) % n_rows;
                let target = 1_700_000_000 + id as i64;
                let _ = engine.point_lookup_nonindexed(target);
            }),
            "scan_filter_count" => time_iter(READ_OPS, || {
                let _ = engine.scan_filter_count(30);
            }),
            "scan_filter_project_top100" => time_iter(READ_OPS, || {
                let _ = engine.scan_filter_project_top100(30);
            }),
            "scan_filter_sort_limit10" => time_iter(READ_OPS, || {
                let _ = engine.scan_filter_sort_limit10(30);
            }),
            "agg_sum" => time_iter(READ_OPS, || {
                let _ = engine.agg_sum();
            }),
            "agg_avg" => time_iter(READ_OPS, || {
                let _ = engine.agg_avg(30);
            }),
            "agg_min" => time_iter(READ_OPS, || {
                let _ = engine.agg_min();
            }),
            "agg_max" => time_iter(READ_OPS, || {
                let _ = engine.agg_max();
            }),
            "multi_col_and_filter" => time_iter(READ_OPS, || {
                let _ = engine.multi_col_and_filter(30, "active");
            }),
            "insert_single" => {
                // Each iteration inserts a brand-new row with a fresh id
                // outside the existing populated range. We assign ids
                // sequentially starting at next_insert_id.
                let start_id = next_insert_id;
                let start = Instant::now();
                for i in 0..WRITE_OPS {
                    let id = start_id + i as i64;
                    engine.insert_single(
                        id,
                        "new",
                        30,
                        "active",
                        "new@ex.com",
                        1_700_100_000 + id,
                    );
                }
                next_insert_id = start_id + WRITE_OPS as i64;
                start.elapsed().as_nanos() as f64 / WRITE_OPS as f64
            }
            "insert_batch_1k" => {
                // Build the batch once, outside the timed region, so we
                // measure only insert throughput (not format!() cost).
                // Each iteration uses a fresh id range to avoid PK
                // collisions with prior runs.
                let iters: usize = 3;
                let mut total_ns: f64 = 0.0;
                for _ in 0..iters {
                    let base = next_insert_id;
                    let mut rows: Vec<(i64, String, i64, String, String, i64)> =
                        Vec::with_capacity(batch_size);
                    for k in 0..batch_size {
                        let id = base + k as i64;
                        rows.push((
                            id,
                            format!("batch_{id}"),
                            30,
                            "active".to_string(),
                            format!("batch_{id}@ex.com"),
                            1_700_200_000 + id,
                        ));
                    }
                    let start = Instant::now();
                    engine.insert_batch(&rows);
                    total_ns += start.elapsed().as_nanos() as f64;
                    next_insert_id = base + batch_size as i64;
                }
                // ns per *single insert* (not per batch call) so the
                // number is comparable to `insert_single`.
                total_ns / (iters * batch_size) as f64
            }
            "update_by_pk" => time_iter(WRITE_OPS, || {
                // Pick an id inside the original populated range.
                let id = (fastrand_xor(idx as u64) as usize % n_rows) as i64;
                let _ = engine.update_by_pk(id, 42);
            }),
            "update_by_filter" => {
                // `update_by_filter` is idempotent (setting status to the
                // same value is a no-op for content), so we can loop.
                // Note: reduced from 20 iters → 3 because PowDB's current
                // Update(Filter(SeqScan)) path does a per-row catalog.update
                // that dominates the timing. Expected to drop once the
                // fused in-place update fast path lands (tracked as
                // Mission A followup).
                let iters: usize = 3;
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = engine.update_by_filter(50, "senior");
                }
                start.elapsed().as_nanos() as f64 / iters as f64
            }
            "delete_by_filter" => {
                // Destructive — requires rebuild between iterations.
                let mut total_ns: f64 = 0.0;
                for _ in 0..DELETE_ITERS {
                    engine.setup(n_rows);
                    let start = Instant::now();
                    let _ = engine.delete_by_filter(20);
                    total_ns += start.elapsed().as_nanos() as f64;
                }
                // Re-populate the fixture so downstream runs (if any)
                // aren't surprised by a half-empty table.
                engine.setup(n_rows);
                next_insert_id = n_rows as i64 + 1;
                total_ns / DELETE_ITERS as f64
            }
            other => panic!("unknown workload: {other}"),
        };
        cells[idx] = Some(Cell { ns_per_op: ns });
    }

    EngineResults { name, cells }
}

fn warmup(engine: &mut dyn BenchEngine, n_rows: usize, selected: &[&'static str]) {
    // Read warmup: 5 calls per read workload. Writes are skipped to avoid
    // mutating the fixture before the timed run.
    let sample_id = (n_rows / 2) as i64;
    for _ in 0..5 {
        if selected.contains(&"point_lookup_indexed") {
            let _ = engine.point_lookup_indexed(sample_id);
        }
        if selected.contains(&"point_lookup_nonindexed") {
            let _ = engine.point_lookup_nonindexed(1_700_000_000 + sample_id);
        }
        if selected.contains(&"scan_filter_count") {
            let _ = engine.scan_filter_count(30);
        }
        if selected.contains(&"scan_filter_project_top100") {
            let _ = engine.scan_filter_project_top100(30);
        }
        if selected.contains(&"scan_filter_sort_limit10") {
            let _ = engine.scan_filter_sort_limit10(30);
        }
        if selected.contains(&"agg_sum") {
            let _ = engine.agg_sum();
        }
        if selected.contains(&"agg_avg") {
            let _ = engine.agg_avg(30);
        }
        if selected.contains(&"agg_min") {
            let _ = engine.agg_min();
        }
        if selected.contains(&"agg_max") {
            let _ = engine.agg_max();
        }
        if selected.contains(&"multi_col_and_filter") {
            let _ = engine.multi_col_and_filter(30, "active");
        }
    }
}

/// Tiny deterministic PRNG so point-lookup iterations stride around the
/// fixture without clustering. Not cryptographically meaningful.
fn fastrand_xor(seed: u64) -> u64 {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

fn fmt_ns(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{:.0}ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.1}us", ns / 1_000.0)
    } else {
        format!("{:.2}ms", ns / 1_000_000.0)
    }
}

/// Print the comparison matrix: rows = workloads, cols = engines. Values
/// are per-op times, formatted with a sensible unit.
fn print_table(results: &[EngineResults], selected: &[&'static str]) {
    let col_w = 14;
    print!("{:<30}", "workload");
    for r in results {
        print!(" {:>col_w$}", r.name, col_w = col_w);
    }
    println!();
    println!("{}", "─".repeat(30 + (col_w + 1) * results.len()));

    for (idx, workload) in WORKLOADS.iter().enumerate() {
        if !selected.contains(workload) {
            continue;
        }
        print!("{:<30}", workload);
        for r in results {
            let s = match &r.cells[idx] {
                Some(c) => fmt_ns(c.ns_per_op),
                None => "-".to_string(),
            };
            print!(" {:>col_w$}", s, col_w = col_w);
        }
        println!();
    }
}

/// Print the ratio table: for each non-PowDB engine, show (engine / powdb)
/// for every selected workload. >1 means PowDB is faster.
fn print_ratio_table(results: &[EngineResults], selected: &[&'static str]) {
    let powdb_idx = match results.iter().position(|r| r.name == "powdb") {
        Some(i) => i,
        None => return,
    };

    let col_w = 14;
    println!();
    print!("{:<30}", "ratio (x / powdb)");
    for r in results.iter().enumerate().filter(|(i, _)| *i != powdb_idx) {
        print!(" {:>col_w$}", r.1.name, col_w = col_w);
    }
    println!();
    println!(
        "{}",
        "─".repeat(30 + (col_w + 1) * results.len().saturating_sub(1))
    );

    let powdb = &results[powdb_idx];
    for (idx, workload) in WORKLOADS.iter().enumerate() {
        if !selected.contains(workload) {
            continue;
        }
        print!("{:<30}", workload);
        for (i, r) in results.iter().enumerate() {
            if i == powdb_idx {
                continue;
            }
            let s = match (&r.cells[idx], &powdb.cells[idx]) {
                (Some(rc), Some(pc)) if pc.ns_per_op > 0.0 => {
                    format!("{:.1}x", rc.ns_per_op / pc.ns_per_op)
                }
                _ => "-".to_string(),
            };
            print!(" {:>col_w$}", s, col_w = col_w);
        }
        println!();
    }
}

/// Write `crates/compare/results.csv` with one row per (engine, workload).
///
/// Columns: `engine,workload,ns_per_op,ops_per_sec`.
fn write_csv(results: &[EngineResults], selected: &[&'static str]) -> std::io::Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("results.csv");
    let mut file = std::fs::File::create(&path)?;
    writeln!(file, "engine,workload,ns_per_op,ops_per_sec")?;
    for r in results {
        for (idx, workload) in WORKLOADS.iter().enumerate() {
            if !selected.contains(workload) {
                continue;
            }
            if let Some(cell) = &r.cells[idx] {
                let ops_per_sec = if cell.ns_per_op > 0.0 {
                    1e9 / cell.ns_per_op
                } else {
                    0.0
                };
                writeln!(
                    file,
                    "{},{},{:.1},{:.1}",
                    r.name, workload, cell.ns_per_op, ops_per_sec
                )?;
            }
        }
    }
    println!("\nwrote results to {}", path.display());
    Ok(())
}

fn main() {
    let n_rows = parse_n_rows();
    let filter = parse_workload_filter();
    let selected = selected_workloads(&filter);

    // Silence example-row generator warning when compiled without the
    // `gen_row` consumer above. (kept live because write workloads use it).
    let _ = gen_row(0);
    let _ = STATUSES;

    println!(
        "PowDB Mission A wide bench — {} rows, {} workloads\n",
        n_rows,
        selected.len()
    );

    let mut results: Vec<EngineResults> = Vec::new();

    // ── PowDB ─────────────────────────────────────────────────────────
    {
        let mut engine = PowdbEngine::new();
        results.push(bench_engine(&mut engine, n_rows, &selected));
    }

    // ── SQLite ────────────────────────────────────────────────────────
    {
        let mut engine = SqliteEngine::new();
        results.push(bench_engine(&mut engine, n_rows, &selected));
    }

    // ── Postgres (optional) ───────────────────────────────────────────
    match PostgresEngine::try_new() {
        Some(mut engine) => {
            results.push(bench_engine(&mut engine, n_rows, &selected));
        }
        None => {
            println!("  [postgres] skipped — no server reachable (set POWDB_BENCH_PG_URL or start docker compose up -d)");
        }
    }

    // ── MySQL (optional, feature-gated) ───────────────────────────────
    #[cfg(feature = "mysql")]
    {
        match engines::mysql::MysqlEngine::try_new() {
            Some(mut engine) => {
                results.push(bench_engine(&mut engine, n_rows, &selected));
            }
            None => {
                println!("  [mysql] skipped — no server reachable");
            }
        }
    }
    #[cfg(not(feature = "mysql"))]
    {
        // MYSQL worker owns `engines/mysql.rs`; build this crate with
        // `--features mysql` once that file has landed to enable the
        // engine.
        println!("  [mysql] skipped — build without the `mysql` feature");
    }

    // ── Results ───────────────────────────────────────────────────────
    println!();
    print_table(&results, &selected);
    print_ratio_table(&results, &selected);

    if let Err(e) = write_csv(&results, &selected) {
        eprintln!("failed to write results.csv: {e}");
    }

    println!();
}
