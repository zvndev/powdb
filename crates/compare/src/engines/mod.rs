//! Engine abstraction for the Mission A wide comparison bench.
//!
//! Every engine (PowDB, SQLite, Postgres, MySQL) implements [`BenchEngine`]
//! against the same `User` table schema so the 15 Mission A workloads are
//! directly comparable.
//!
//! Schema (all engines):
//!
//! ```text
//! User(id INT PRIMARY KEY, name STR, age INT, status STR, email STR, created_at INT)
//! ```
//!
//! Deterministic row generator (populated in `setup`):
//!
//! ```text
//! id         = i
//! name       = "user_{i}"
//! age        = 18 + (i % 60)
//! status     = ["active", "inactive", "pending"][i % 3]
//! email      = "user_{i}@example.com"
//! created_at = 1_700_000_000 + i
//! ```

pub mod postgres;
pub mod powdb;
pub mod sqlite;

// MYSQL agent owns `engines/mysql.rs` in a separate worktree. Gate behind
// a feature so this worktree builds cleanly without the file. Once the
// MYSQL branch merges, the feature can be flipped on by default.
#[cfg(feature = "mysql")]
pub mod mysql;

/// Deterministic `status` cycle used by both the row generator and the
/// `multi_col_and_filter` workload to pick a realistic predicate value.
pub const STATUSES: [&str; 3] = ["active", "inactive", "pending"];

/// Deterministically generate the `i`-th row of the `User` fixture.
///
/// Returned as owned strings so callers can pass the values straight into
/// prepared-statement binders without extra allocation.
pub fn gen_row(i: usize) -> (i64, String, i64, String, String, i64) {
    (
        i as i64,
        format!("user_{i}"),
        (18 + (i % 60)) as i64,
        STATUSES[i % 3].to_string(),
        format!("user_{i}@example.com"),
        1_700_000_000 + i as i64,
    )
}

/// Uniform interface every comparison-bench engine implements. All 15
/// methods map one-to-one with the Mission A workloads in §1 of
/// `PLAN-MISSION-A.md`.
pub trait BenchEngine {
    /// Human-readable engine name (e.g. `"powdb"`, `"sqlite"`).
    fn name(&self) -> &str;

    /// Populate the `User` table with `n_rows` deterministic rows.
    fn setup(&mut self, n_rows: usize);

    // ── Reads ─────────────────────────────────────────────────────────

    /// 1. `point_lookup_indexed` — single-row lookup by indexed primary key.
    fn point_lookup_indexed(&self, id: i64) -> Option<String>;

    /// 2. `point_lookup_nonindexed` — single-row lookup on a non-indexed
    ///    int column (forces a sequential scan).
    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String>;

    /// 3. `scan_filter_count` — count rows matching an int-range predicate.
    fn scan_filter_count(&self, age_threshold: i64) -> usize;

    /// 4. `scan_filter_project_top100` — first 100 rows matching a predicate.
    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)>;

    /// 5. `scan_filter_sort_limit10` — top-10 by `created_at desc`.
    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)>;

    /// 6. `agg_sum` — sum of all `age` values.
    fn agg_sum(&self) -> i64;

    /// 7. `agg_avg` — average `age` across rows matching a predicate.
    fn agg_avg(&self, age_threshold: i64) -> f64;

    /// 8. `agg_min` — minimum `created_at` across all rows.
    fn agg_min(&self) -> i64;

    /// 9. `agg_max` — maximum `age`.
    fn agg_max(&self) -> i64;

    /// 10. `multi_col_and_filter` — two-predicate conjunction.
    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)>;

    // ── Writes ────────────────────────────────────────────────────────

    /// 11. `insert_single` — insert a single row.
    fn insert_single(
        &mut self,
        id: i64,
        name: &str,
        age: i64,
        status: &str,
        email: &str,
        created_at: i64,
    );

    /// 12. `insert_batch` — insert a prebuilt batch of rows inside one
    ///     transaction (the harness supplies 1_000 rows per call).
    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]);

    /// 13. `update_by_pk` — update a single row by primary key.
    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64;

    /// 14. `update_by_filter` — update every row matching a range predicate.
    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64;

    /// 15. `delete_by_filter` — delete every row matching a range predicate.
    fn delete_by_filter(&mut self, age_threshold: i64) -> u64;
}
