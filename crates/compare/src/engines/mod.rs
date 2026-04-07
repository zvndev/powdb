//! Engine abstraction for apples-to-apples comparison benchmarks.
//!
//! Each engine implements [`BenchEngine`] against the same User-table schema
//! so that insert, lookup, and scan workloads are directly comparable.

pub mod powdb;
pub mod postgres;
pub mod sqlite;

/// Uniform interface that every comparison-bench engine must implement.
///
/// Schema assumed by all engines:
///
/// ```text
/// User(id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)
/// ```
pub trait BenchEngine {
    /// Human-readable engine name (e.g. "sqlite", "powdb").
    fn name(&self) -> &str;

    /// Populate the User table with `n_rows` rows.
    ///
    /// Row pattern:
    /// - id:    `i` (0..n_rows)
    /// - name:  `user_{i}`
    /// - age:   `18 + (i % 60)`
    /// - email: `user_{i}@example.com`
    fn setup(&mut self, n_rows: usize);

    /// Look up a single row by primary key, returning the `name` column.
    fn point_lookup(&self, id: i64) -> Option<String>;

    /// `SELECT COUNT(*) FROM user_table WHERE age > ?`
    fn scan_filter_count(&self, age_threshold: i64) -> usize;

    /// Identical to [`scan_filter_count`] for SQL engines; custom engines may
    /// implement a dedicated count-with-filter path.
    fn count_filter(&self, age_threshold: i64) -> usize;
}
