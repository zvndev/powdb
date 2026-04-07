//! PowQL parse + plan + execute criterion benches for the regression gate.
//!
//! Workloads 4–6 from docs/superpowers/specs/2026-04-07-bench-regression-gate-design.md
//!
//! 4.  `powql_point`              — `User filter .id = N { .name }` (the 3,020x path, IndexScan fold)
//! 5a. `powql_filter_only`        — `User filter .age > N` (no projection)
//! 5b. `powql_filter_projection`  — `User filter .age > N { .name, .email }`
//! 6.  `powql_aggregation`        — `count(User filter .age > N)`
//!
//! The thesis lives at workload 4. The ratio guard
//! `powql_point.median / btree_lookup.median ≤ 2.5` enforces it.
//!
//! Each bench sets up a single 50K-row fixture once, then runs the timed loop
//! against pre-generated query strings so format!() allocation isn't on the
//! hot path.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powdb_query::executor::Engine;
use powdb_storage::types::*;
use tempfile::TempDir;

const N_ROWS: usize = 50_000;
const N_QUERIES: usize = 100;

/// Build a fresh User table with `N_ROWS` rows and an index on `id`. Same
/// fixture used by every PowQL bench in this file. Returns the engine and
/// the tempdir guard.
fn setup_user_fixture() -> (Engine, TempDir) {
    let tmp = TempDir::new().expect("create tempdir");
    let mut engine = Engine::new(tmp.path()).expect("engine init");

    engine
        .execute_powql(
            "type User { required id: int, required name: str, required age: int, required email: str }",
        )
        .expect("create type");

    let data_dir: std::path::PathBuf = tmp.path().to_path_buf();
    {
        let table = engine
            .catalog_mut()
            .get_table_mut("User")
            .expect("get User table");
        for i in 0..N_ROWS {
            let row = vec![
                Value::Int(i as i64),
                Value::Str(format!("user_{i}")),
                Value::Int((18 + (i % 60)) as i64),
                Value::Str(format!("user_{i}@example.com")),
            ];
            table.insert(&row).expect("insert row");
        }
        table
            .create_index("id", &data_dir)
            .expect("build id index");
    }

    (engine, tmp)
}

/// Pre-generate `N_QUERIES` query strings so format!() allocation doesn't
/// dominate the timed loop.
fn gen_queries<F: Fn(usize) -> String>(f: F) -> Vec<String> {
    (0..N_QUERIES).map(f).collect()
}

/// Run a fixed number of warm-up queries to settle the plan cache before
/// criterion's own warm-up phase begins.
fn warm_plan_cache(engine: &mut Engine, queries: &[String]) {
    for q in queries.iter().take(10) {
        let _ = engine.execute_powql(q);
    }
}

// ───── 4. Full PowQL point query — the 3,020x path ─────────────────────────
//
// This is the thesis. Numerator of the ratio guard. If this regresses, the
// gate fires before the merge. The whole point of the bench suite.

fn bench_powql_point(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!("User filter .id = {} {{ .name }}", (i * 491) % N_ROWS)
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_point", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q))
        });
    });
}

// ───── 5a. PowQL filter only ───────────────────────────────────────────────
//
// Non-index filter, no projection. Tests the executor's filter path without
// the projection layer on top. A regression here vs 5b tells you projection
// is fine but filter+executor slowed down.

fn bench_powql_filter_only(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!("User filter .age > {}", 20 + (i % 40))
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_filter_only", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q))
        });
    });
}

// ───── 5b. PowQL filter + projection ───────────────────────────────────────
//
// Non-index filter with projection. Diff against 5a isolates projection
// overhead. A regression here without 5a regressing means projection broke.

fn bench_powql_filter_projection(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!(
            "User filter .age > {} {{ .name, .email }}",
            20 + (i % 40)
        )
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_filter_projection", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q))
        });
    });
}

// ───── 6. PowQL aggregation ────────────────────────────────────────────────
//
// count() aggregate over a filter. Guards the aggregation path; a regression
// here without scan/filter regressing means the aggregate dispatch broke.

fn bench_powql_aggregation(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!("count(User filter .age > {})", 20 + (i % 40))
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_aggregation", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q))
        });
    });
}

criterion_group! {
    name = powql_benches;
    config = Criterion::default().noise_threshold(0.04);
    targets =
        bench_powql_point,
        bench_powql_filter_only,
        bench_powql_filter_projection,
        bench_powql_aggregation
}
criterion_main!(powql_benches);
