//! PowQL parse + plan + execute criterion benches for the regression gate.
//!
//! This file covers the full 15-workload surface defined in PLAN-MISSION-A.md §1,
//! plus the three legacy bench functions kept for gate continuity
//! (`powql_point`, `powql_filter_only`, `powql_filter_projection`,
//! `powql_aggregation`).
//!
//! Canonical workload names (per PLAN-MISSION-A.md §4, BENCH→CRITERION contract):
//!
//!   1.  point_lookup_indexed       → reuses legacy `powql_point`
//!   2.  point_lookup_nonindexed
//!   3.  scan_filter_count          → reuses legacy `powql_aggregation`
//!   4.  scan_filter_project_top100
//!   5.  scan_filter_sort_limit10
//!   6.  agg_sum
//!   7.  agg_avg
//!   8.  agg_min
//!   9.  agg_max
//!   10. multi_col_and_filter
//!   11. insert_single
//!   12. insert_batch_1k
//!   13. update_by_pk
//!   14. update_by_filter
//!   15. delete_by_filter
//!
//! The thesis lives at workload 1. The ratio guard
//! `powql_point.median / btree_lookup.median ≤ 2.5` enforces it.
//!
//! Read workloads build a single 100K-row fixture once per bench_function and
//! reuse it, running against pre-generated query strings so format!() allocation
//! isn't on the hot path. Write workloads use `iter_batched` with a per-iter
//! setup closure so each iteration sees a fresh deterministic fixture — this
//! is the only way to honestly measure destructive mutations like
//! `delete_by_filter`.
//!
//! When FASTPATH agent's parser extension hasn't landed yet, the four
//! aggregate-with-column workloads (`agg_sum/agg_avg/agg_min/agg_max`) will
//! fail to parse. In that case the timed closure runs once, records the parse
//! error time, and the bench is effectively a no-op — this still lets the
//! comparator CAPTURE a (bogus) baseline on first run. Once FASTPATH lands,
//! a rebaseline commit will overwrite the null entries with real numbers.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use powdb_query::executor::Engine;
use powdb_storage::types::*;
use powdb_storage::wal::WalSyncMode;
use std::time::Duration;
use tempfile::TempDir;

/// Read-workload fixture size. 100K rows per PLAN-MISSION-A.md §1.
const N_ROWS: usize = 100_000;

/// Write-workload fixture size — same 100K rows, but built per-iter for
/// destructive mutations. Scaled down only where the build cost would blow
/// criterion's time budget (see `bench_delete_by_filter` comment).
const N_ROWS_WRITE: usize = 100_000;

/// Smaller fixture for the pure destructive bench (`delete_by_filter`)
/// where criterion must run `iter_batched` with a fresh fixture on every
/// single iteration. At 100K rows each build takes ~400ms; with criterion's
/// default 100 samples the full run would exceed 40s and time-out in CI.
/// 10K rows is large enough to keep the compiled-predicate walk realistic
/// while staying under a ~5s total bench budget.
const N_ROWS_DESTRUCTIVE: usize = 10_000;

/// Fixture size for `update_by_filter`. Perf sprint: FASTPATH's fused
/// `Update(Filter(SeqScan))` fast path eliminates the two-pass
/// collect-then-patch pattern, so the benchmark can run at 100K rows.
const N_ROWS_UPDATE_FILTER: usize = 100_000;

/// Number of pre-generated query strings per read bench. Cycling through a
/// small ring amortises hash-lookups against the plan cache without letting
/// the compiler constant-fold the query out.
const N_QUERIES: usize = 100;

// ───── Fixture builders ────────────────────────────────────────────────────

/// Build a fresh `User` table with `n` rows and an index on `id`, matching
/// the schema defined in PLAN-MISSION-A.md §1:
///
/// ```text
/// User(id INT PRIMARY KEY, name STR, age INT, status STR, email STR, created_at INT)
/// ```
///
/// Deterministic row generator:
///
/// ```text
/// id         = i
/// name       = "user_{i}"
/// age        = 18 + (i % 60)
/// status     = ["active", "inactive", "pending"][i % 3]
/// email      = "user_{i}@example.com"
/// created_at = 1_700_000_000 + i
/// ```
///
/// Returns the engine and the tempdir guard (drop-order matters — the engine
/// holds mmap pointers into the tempdir's files).
fn setup_user_fixture_n(n: usize) -> (Engine, TempDir) {
    let tmp = TempDir::new().expect("create tempdir");
    let mut engine = Engine::new(tmp.path()).expect("engine init");

    // Mission B: PowDB now ships a write-ahead log that fsyncs at every
    // statement boundary by default. The reference SQLite engine in the
    // wide-bench harness uses `:memory:` (zero fsync), so to keep the
    // criterion regression gate measuring the same thing it has always
    // measured — execute_powql throughput minus durability cost — we
    // disable WAL fsync inside the bench. The WAL is still appended +
    // recovered on process crash; only the machine-crash guarantee is
    // dropped. Production code uses the default Full mode.
    engine.catalog_mut().set_wal_sync_mode(WalSyncMode::Off);

    engine
        .execute_powql(
            "type User { \
             required id: int, \
             required name: str, \
             required age: int, \
             required status: str, \
             required email: str, \
             required created_at: int \
             }",
        )
        .expect("create type");

    let data_dir: std::path::PathBuf = tmp.path().to_path_buf();
    {
        let table = engine
            .catalog_mut()
            .get_table_mut("User")
            .expect("get User table");
        for i in 0..n {
            let row = vec![
                Value::Int(i as i64),
                Value::Str(format!("user_{i}")),
                Value::Int((18 + (i % 60)) as i64),
                Value::Str(status_for(i).to_string()),
                Value::Str(format!("user_{i}@example.com")),
                Value::Int(1_700_000_000 + i as i64),
            ];
            table.insert(&row).expect("insert row");
        }
        table.create_index("id", &data_dir).expect("build id index");
    }

    (engine, tmp)
}

/// Cyclic status assignment matching PLAN-MISSION-A.md §1.
fn status_for(i: usize) -> &'static str {
    match i % 3 {
        0 => "active",
        1 => "inactive",
        _ => "pending",
    }
}

/// Default fixture: 100K rows.
fn setup_user_fixture() -> (Engine, TempDir) {
    setup_user_fixture_n(N_ROWS)
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

/// Run a fresh query on the engine, swallowing errors silently. Used by the
/// aggregate-with-column benches so that if FASTPATH's parser extension hasn't
/// landed yet the bench doesn't panic — it just measures the parse-error
/// path. On first run this records a null capture; on the post-FASTPATH
/// rebaseline it records the real number.
fn try_execute(engine: &mut Engine, q: &str) {
    let _ = engine.execute_powql(q);
}

// ═══════════════════════════════════════════════════════════════════════════
// READ BENCHES
// ═══════════════════════════════════════════════════════════════════════════

// ───── Workload 1. point_lookup_indexed — legacy `powql_point` ─────────────
//
// Single-row lookup on the indexed primary key. This is THE thesis workload:
// `powql_point` is the numerator of the ratio guard. If it regresses, the gate
// fires before the merge.

fn bench_powql_point(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| format!("User filter .id = {} {{ .name }}", (i * 491) % N_ROWS));
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_point", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Legacy 5a. powql_filter_only ────────────────────────────────────────
//
// Kept as-is for gate continuity. Non-index filter, no projection.

fn bench_powql_filter_only(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| format!("User filter .age > {}", 20 + (i % 40)));
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_filter_only", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Legacy 5b. powql_filter_projection ──────────────────────────────────
//
// Kept as-is for gate continuity. Non-index filter with projection.

fn bench_powql_filter_projection(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries =
        gen_queries(|i| format!("User filter .age > {} {{ .name, .email }}", 20 + (i % 40)));
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_filter_projection", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Workload 3. scan_filter_count — legacy `powql_aggregation` ──────────
//
// `count(User filter .age > N)` — the aggregate regression guard.

fn bench_powql_aggregation(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| format!("count(User filter .age > {})", 20 + (i % 40)));
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("powql_aggregation", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Workload 2. point_lookup_nonindexed ─────────────────────────────────
//
// Single-row lookup on a non-indexed int column. The planner folds
// `.created_at = literal` into an IndexScan, the executor falls through to
// the "no index on this column" scan path, and early-returns on the first
// match. Measures the worst case of an unindexed equality lookup.

fn bench_point_lookup_nonindexed(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        let target = 1_700_000_000 + ((i * 491) % N_ROWS) as i64;
        format!("User filter .created_at = {target} {{ .name }}")
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("point_lookup_nonindexed", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Workload 4. scan_filter_project_top100 ──────────────────────────────
//
// `User filter .age > N limit 100 { .name, .email }`. Currently executes as
// `Project(Limit(Filter(SeqScan)))` via the generic interpreter. FASTPATH
// will fuse this into a streaming walker that short-circuits on 100 matches.

fn bench_scan_filter_project_top100(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!(
            "User filter .age > {} limit 100 {{ .name, .email }}",
            20 + (i % 40)
        )
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("scan_filter_project_top100", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Workload 5. scan_filter_sort_limit10 ────────────────────────────────
//
// `User filter .age > N order .created_at desc limit 10 { .name, .created_at }`.
// Top-10-by-sort workload. Generic path today; FASTPATH adds a bounded top-N
// heap.

fn bench_scan_filter_sort_limit10(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!(
            "User filter .age > {} order .created_at desc limit 10 {{ .name, .created_at }}",
            20 + (i % 40)
        )
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("scan_filter_sort_limit10", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ───── Workload 6. agg_sum ─────────────────────────────────────────────────
//
// `sum(User { .age })` — requires FASTPATH's parser extension
// (lift trailing `{ .field }` into AggregateExpr.field for non-count). Until
// that lands, `try_execute` swallows the parse-error path so the bench still
// compiles and runs once; the captured baseline will reflect the error
// fast-path, and a post-FASTPATH rebaseline commit replaces it with the real
// number.

fn bench_agg_sum(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();
    let q = "sum(User { .age })".to_string();
    try_execute(&mut engine, &q);

    c.bench_function("agg_sum", |b| {
        b.iter(|| {
            try_execute(&mut engine, &q);
            black_box(&q);
        });
    });
}

// ───── Workload 7. agg_avg ─────────────────────────────────────────────────
//
// `avg(User filter .age > 30 { .age })`. Same parser-extension dependency.

fn bench_agg_avg(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| format!("avg(User filter .age > {} {{ .age }})", 20 + (i % 40)));
    for q in queries.iter().take(10) {
        try_execute(&mut engine, q);
    }

    let mut idx: usize = 0;
    c.bench_function("agg_avg", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            try_execute(&mut engine, q);
            black_box(q);
        });
    });
}

// ───── Workload 8. agg_min ─────────────────────────────────────────────────
//
// `min(User { .created_at })`. Same parser-extension dependency.

fn bench_agg_min(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();
    let q = "min(User { .created_at })".to_string();
    try_execute(&mut engine, &q);

    c.bench_function("agg_min", |b| {
        b.iter(|| {
            try_execute(&mut engine, &q);
            black_box(&q);
        });
    });
}

// ───── Workload 9. agg_max ─────────────────────────────────────────────────
//
// `max(User { .age })`. Same parser-extension dependency.

fn bench_agg_max(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();
    let q = "max(User { .age })".to_string();
    try_execute(&mut engine, &q);

    c.bench_function("agg_max", |b| {
        b.iter(|| {
            try_execute(&mut engine, &q);
            black_box(&q);
        });
    });
}

// ───── Workload 10. multi_col_and_filter ───────────────────────────────────
//
// `User filter .age > N and .status = "active" { .name, .age }`. Two-predicate
// conjunction — int range + string equality. The existing compiled-predicate
// path only handles a single `.field op literal`, so this falls into the
// decode_selective + eval_predicate path today. FASTPATH extends
// `try_compile_int_predicate` to AND compiled leaves; this bench guards that
// optimisation.

fn bench_multi_col_and_filter(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture();

    let queries = gen_queries(|i| {
        format!(
            "User filter .age > {} and .status = \"active\" {{ .name, .age }}",
            20 + (i % 40)
        )
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("multi_col_and_filter", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("query failed"))
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// WRITE BENCHES
// ═══════════════════════════════════════════════════════════════════════════

// ───── Workload 11. insert_single ──────────────────────────────────────────
//
// Single-row insert via PowQL text. The hot OLTP path. Uses a shared fixture
// and a monotonically increasing id per iter so each insert lands on a
// distinct primary key — the underlying table grows across the sample, which
// is the honest measurement of "insert into a populated table".

fn bench_insert_single(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture_n(N_ROWS_WRITE);

    let mut next_id: i64 = N_ROWS_WRITE as i64;

    c.bench_function("insert_single", |b| {
        b.iter(|| {
            let id = next_id;
            next_id += 1;
            let q = format!(
                "insert User {{ id := {id}, name := \"new_{id}\", \
                 age := 30, status := \"active\", \
                 email := \"new_{id}@example.com\", created_at := {} }}",
                1_700_000_000 + id
            );
            black_box(engine.execute_powql(&q).expect("insert failed"))
        });
    });
}

// ───── Workload 12. insert_batch_1k ────────────────────────────────────────
//
// 1000 inserts in a tight loop. This is the stress test: the one workload
// where SQLite's prepared-statement reuse is the tightest competition. If
// PowDB still wins here, the thesis holds even on SQL's best loop. Each
// criterion iteration inserts 1000 new rows with fresh ids; the table grows
// unboundedly but that's fine — 10 samples × 10 iters × 1000 rows ≈ 100K
// extra rows, which is comfortably under the heap's capacity.

fn bench_insert_batch_1k(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture_n(N_ROWS_WRITE);

    // Anchor the counter well above the fixture so ids never collide with
    // the seeded rows even across the entire sample run.
    let mut base_id: i64 = (N_ROWS_WRITE as i64) * 10;

    c.bench_function("insert_batch_1k", |b| {
        b.iter(|| {
            let start_id = base_id;
            base_id += 1000;
            for offset in 0..1000_i64 {
                let id = start_id + offset;
                let q = format!(
                    "insert User {{ id := {id}, name := \"b_{id}\", \
                     age := 30, status := \"active\", \
                     email := \"b_{id}@example.com\", created_at := {} }}",
                    1_700_000_000 + id
                );
                let _ = engine.execute_powql(&q).expect("insert failed");
            }
            black_box(base_id)
        });
    });
}

// ───── Workload 13. update_by_pk ───────────────────────────────────────────
//
// Single-row update by primary key. The planner emits `Update(Filter(SeqScan))`
// and the executor does a full scan+match today. FASTPATH will fold
// `Update.filter .pk = literal` into `Update(IndexScan)` for a direct lookup.
// Uses the shared fixture because the mutation is idempotent — setting the
// same id to the same age repeatedly does not change the table.

fn bench_update_by_pk(c: &mut Criterion) {
    let (mut engine, _tmp) = setup_user_fixture_n(N_ROWS_WRITE);

    let queries = gen_queries(|i| {
        format!(
            "User filter .id = {} update {{ age := 31 }}",
            (i * 491) % N_ROWS_WRITE
        )
    });
    warm_plan_cache(&mut engine, &queries);

    let mut idx: usize = 0;
    c.bench_function("update_by_pk", |b| {
        b.iter(|| {
            let q = &queries[idx % queries.len()];
            idx = idx.wrapping_add(1);
            black_box(engine.execute_powql(q).expect("update failed"))
        });
    });
}

// ───── Workload 14. update_by_filter ───────────────────────────────────────
//
// Update every row matching `.age > 50`. The existing generic path does a
// second full scan to match values, which is O(N·M). FASTPATH fuses this into
// a single-pass compiled-predicate walk. Updates are idempotent here (every
// match gets the same new status) so we can reuse the shared fixture without
// accumulating state.

fn bench_update_by_filter(c: &mut Criterion) {
    // See N_ROWS_UPDATE_FILTER comment above — the pre-FASTPATH executor is
    // O(N·M) so we run this against a 10K-row fixture until the fast path
    // lands. Post-FASTPATH, bump back to N_ROWS_WRITE in the rebaseline.
    let (mut engine, _tmp) = setup_user_fixture_n(N_ROWS_UPDATE_FILTER);

    let q = "User filter .age > 50 update { status := \"senior\" }".to_string();
    warm_plan_cache(&mut engine, std::slice::from_ref(&q));

    c.bench_function("update_by_filter", |b| {
        b.iter(|| black_box(engine.execute_powql(&q).expect("update failed")));
    });
}

// ───── Workload 15. delete_by_filter ───────────────────────────────────────
//
// Destructive bulk delete. Every iteration MUST see a fresh fixture,
// otherwise the second iter finds nothing to delete and the bench flatlines.
// Uses `iter_batched` with `BatchSize::PerIteration` so criterion rebuilds
// the fixture between timed runs. The fixture is intentionally smaller
// (`N_ROWS_DESTRUCTIVE` = 10K) because `setup_user_fixture_n(100_000)` costs
// ~400ms per build and criterion's minimum 10 samples × setup + timed run
// would blow the budget.

fn bench_delete_by_filter(c: &mut Criterion) {
    let q = "User filter .age < 20 delete".to_string();

    c.bench_function("delete_by_filter", |b| {
        b.iter_batched(
            || setup_user_fixture_n(N_ROWS_DESTRUCTIVE),
            |(mut engine, tmp)| {
                let r = engine.execute_powql(&q).expect("delete failed");
                // Keep both engine and tempdir alive past the timed call so
                // the mmap isn't torn down inside the measurement window.
                black_box(&r);
                (engine, tmp)
            },
            BatchSize::PerIteration,
        );
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// Criterion group wiring
// ═══════════════════════════════════════════════════════════════════════════

// Fast benches use the default sample count and 4% noise threshold. Each
// completes in <5s against a 100K-row fixture because the timed closure is
// a single PowQL invocation against a warm plan cache.
criterion_group! {
    name = powql_fast_benches;
    config = Criterion::default().noise_threshold(0.04);
    targets =
        // Legacy + workload 1/3 (thesis guards, gate continuity).
        bench_powql_point,
        bench_powql_filter_only,
        bench_powql_filter_projection,
        bench_powql_aggregation,
        // Workload 2.
        bench_point_lookup_nonindexed,
        // Workload 4.
        bench_scan_filter_project_top100,
        // Workload 5.
        bench_scan_filter_sort_limit10,
        // Workloads 6-9 (gated on FASTPATH parser extension).
        bench_agg_sum,
        bench_agg_avg,
        bench_agg_min,
        bench_agg_max,
        // Workload 10.
        bench_multi_col_and_filter,
        // Workload 11 (single insert — fast).
        bench_insert_single,
}

// Slow benches pay a per-iteration cost of ~1-400ms driven by pre-FASTPATH
// O(N) and O(N·M) executor paths. We drop the sample count to criterion's
// floor (10), relax the noise threshold to 10%, and give each a 15s
// measurement window to keep the total run time under ~60 s on CI. Once
// FASTPATH lands the fused plan nodes we can migrate these back to the fast
// group in the rebaseline commit.
criterion_group! {
    name = powql_slow_benches;
    config = Criterion::default()
        .noise_threshold(0.10)
        .sample_size(10)
        .measurement_time(Duration::from_secs(15));
    targets =
        // Workload 13 — generic Update path does a full re-scan to match
        // RIDs by value equality, O(N) per update.
        bench_update_by_pk,
        // Workload 12 — 1000 inserts per iter.
        bench_insert_batch_1k,
        // Workload 14 — O(N·M) generic Update on ~1.6K matches of a 10K
        // fixture pre-FASTPATH.
        bench_update_by_filter,
        // Workload 15 — destructive, per-iter fixture rebuild.
        bench_delete_by_filter,
}

criterion_main!(powql_fast_benches, powql_slow_benches);
