//! Storage-layer criterion benches for the regression gate.
//!
//! Workloads 1–3 from docs/superpowers/specs/2026-04-07-bench-regression-gate-design.md
//!
//! 1. `insert_10k`       — Table::insert loop, 10K rows into a fresh User table
//! 2. `btree_lookup`     — Table::index_lookup on a 50K-row table, 100 random keys per iter
//! 3. `seq_scan_filter`  — Table::scan().filter(age > 30).count() on 50K rows
//!
//! Each bench creates its own tempdir and populates its own fixture. The
//! timed loop only touches the operation under test.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powdb_query::executor::Engine;
use powdb_storage::types::*;
use powdb_storage::wal::WalSyncMode;
use std::path::Path;
use tempfile::TempDir;

const N_ROWS: usize = 50_000;
const N_INSERT: usize = 10_000;

/// Build a fresh User table with `n` rows and an index on `id`. Returns the
/// engine (so the caller can run queries) and the tempdir guard (so the data
/// directory lives as long as the engine does).
fn setup_user_table(n: usize) -> (Engine, TempDir) {
    let tmp = TempDir::new().expect("create tempdir");
    let mut engine = Engine::new(tmp.path()).expect("engine init");
    // Mission B: bench in `:memory:`-equivalent mode (no WAL append, no
    // fsync). The reference SQLite engine uses `:memory:`; matching that
    // is the only way the comparison stays apples-to-apples.
    engine.catalog_mut().set_wal_sync_mode(WalSyncMode::Off);

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
        for i in 0..n {
            let row = vec![
                Value::Int(i as i64),
                Value::Str(format!("user_{i}")),
                Value::Int((18 + (i % 60)) as i64),
                Value::Str(format!("user_{i}@example.com")),
            ];
            table.insert(&row).expect("insert row");
        }
        table.create_index("id", &data_dir).expect("build id index");
    }

    (engine, tmp)
}

// ───── 1. Insert throughput ────────────────────────────────────────────────
//
// Measures the raw heap-insert path + single-column B-tree update (no index
// here — we build a fresh table each iter so the index isn't in play). This
// guards the WAL + page write + heap append path.

fn bench_insert_10k(c: &mut Criterion) {
    c.bench_function("insert_10k", |b| {
        b.iter_with_setup(
            || {
                // Fresh tempdir + schema per sample, not per iter, is too
                // expensive for criterion (each sample is 3s of loops).
                // iter_with_setup runs this per iter — that's what we want
                // for a measurement that must not share state across iters.
                // Insert throughput is a closed batch; this is the honest way.
                let tmp = TempDir::new().expect("create tempdir");
                let mut engine = Engine::new(tmp.path()).expect("engine init");
                // Mission B: match the bench-mode contract — no WAL.
                engine.catalog_mut().set_wal_sync_mode(WalSyncMode::Off);
                engine
                    .execute_powql(
                        "type User { required id: int, required name: str, required age: int, required email: str }",
                    )
                    .expect("create type");
                (engine, tmp)
            },
            |(mut engine, tmp)| {
                let data_dir: std::path::PathBuf = tmp.path().to_path_buf();
                let table = engine
                    .catalog_mut()
                    .get_table_mut("User")
                    .expect("get User table");
                for i in 0..N_INSERT {
                    let row = vec![
                        Value::Int(i as i64),
                        Value::Str(format!("user_{i}")),
                        Value::Int((18 + (i % 60)) as i64),
                        Value::Str(format!("user_{i}@example.com")),
                    ];
                    table.insert(&row).expect("insert row");
                }
                // Keep tempdir alive until the end of the timed closure.
                black_box(&data_dir);
                black_box((engine, tmp))
            },
        );
    });
}

// ───── 2. B-tree point lookup ──────────────────────────────────────────────
//
// The denominator of the thesis ratio. A regression here means the B-tree
// or the heap's random-get path got slower. This is the raw floor.

fn bench_btree_lookup(c: &mut Criterion) {
    let (engine, _tmp) = setup_user_table(N_ROWS);
    let table = engine.catalog().get_table("User").expect("get User");

    // Pre-generate 100 keys so the timed loop isn't measuring Int() construction.
    let keys: Vec<Value> = (0..100)
        .map(|i| Value::Int(i * 491 % N_ROWS as i64))
        .collect();

    let mut idx: usize = 0;
    c.bench_function("btree_lookup", |b| {
        b.iter(|| {
            let key = &keys[idx % keys.len()];
            idx = idx.wrapping_add(1);
            black_box(table.index_lookup("id", key))
        });
    });
}

// ───── 3. Sequential scan with Rust predicate ──────────────────────────────
//
// Full table scan + count of rows matching age > 30. Measures iterator
// throughput and the cost of decoding rows on the scan path.

fn bench_seq_scan_filter(c: &mut Criterion) {
    let (engine, _tmp) = setup_user_table(N_ROWS);
    let table = engine.catalog().get_table("User").expect("get User");

    c.bench_function("seq_scan_filter", |b| {
        b.iter(|| {
            let count = table
                .scan()
                .filter(|(_, row)| matches!(&row[2], Value::Int(age) if *age > 30))
                .count();
            black_box(count)
        });
    });
}

// Unused helper to pacify unused-import warnings if Path gets pruned.
#[allow(dead_code)]
fn _touch_path(p: &Path) -> &Path {
    p
}

criterion_group! {
    name = storage_benches;
    config = Criterion::default().noise_threshold(0.04);
    targets = bench_insert_10k, bench_btree_lookup, bench_seq_scan_filter
}
criterion_main!(storage_benches);
