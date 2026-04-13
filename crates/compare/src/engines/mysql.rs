//! MySQL implementation of [`BenchEngine`] for Mission A comparison benchmarks.
//!
//! Uses the synchronous `mysql` crate (blocking API) so the engine plays
//! nicely with the rest of the bench harness. If the server cannot be reached,
//! [`MysqlEngine::try_new`] returns `None` and the harness skips the engine.

use std::cell::RefCell;

use mysql::prelude::*;
use mysql::{Conn, Opts, Params, TxOpts};

use super::BenchEngine;

/// MySQL-backed bench engine.
///
/// Construct via [`MysqlEngine::try_new`]. The engine owns a single blocking
/// [`Conn`] wrapped in a [`RefCell`] so the read-only trait methods can still
/// issue queries through `&self`.
pub struct MysqlEngine {
    conn: RefCell<Conn>,
}

impl MysqlEngine {
    /// Attempt to connect to MySQL and prepare the `bench_users` table.
    ///
    /// Connection URL resolution order:
    /// 1. `POWDB_BENCH_MYSQL_URL` environment variable
    /// 2. `mysql://root:powdb@localhost:3306/powdb_bench`
    ///
    /// Returns `None` if the server is unreachable or table setup fails —
    /// never panics.
    pub fn try_new() -> Option<Self> {
        let url = std::env::var("POWDB_BENCH_MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:powdb@localhost:3306/powdb_bench".to_string());

        let opts = Opts::from_url(&url).ok()?;
        let mut conn = Conn::new(opts).ok()?;

        conn.query_drop("DROP TABLE IF EXISTS bench_users").ok()?;
        conn.query_drop(
            "CREATE TABLE bench_users (
                id         BIGINT PRIMARY KEY,
                name       TEXT   NOT NULL,
                age        BIGINT NOT NULL,
                status     TEXT   NOT NULL,
                email      TEXT   NOT NULL,
                created_at BIGINT NOT NULL
            )",
        )
        .ok()?;

        Some(Self {
            conn: RefCell::new(conn),
        })
    }
}

impl BenchEngine for MysqlEngine {
    fn name(&self) -> &str {
        "mysql"
    }

    fn setup(&mut self, n_rows: usize) {
        // Wipe any previous data and reload deterministically in a single
        // transaction. Prepared statement + `exec_batch` keeps this fair vs
        // the Postgres COPY path — both are "single round-trip per batch".
        let conn = self.conn.get_mut();

        conn.query_drop("TRUNCATE TABLE bench_users")
            .expect("truncate bench_users");

        let mut tx = conn
            .start_transaction(TxOpts::default())
            .expect("start transaction");

        let rows = (0..n_rows).map(|i| {
            let id = i as i64;
            let name = format!("user_{i}");
            let age = 18 + (i % 60) as i64;
            let status = match i % 3 {
                0 => "active",
                1 => "inactive",
                _ => "pending",
            };
            let email = format!("user_{i}@example.com");
            let created_at = 1_700_000_000i64 + id;
            (id, name, age, status.to_string(), email, created_at)
        });

        tx.exec_batch(
            "INSERT INTO bench_users (id, name, age, status, email, created_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
            rows.map(|(id, name, age, status, email, created_at)| {
                Params::Positional(vec![
                    id.into(),
                    name.into(),
                    age.into(),
                    status.into(),
                    email.into(),
                    created_at.into(),
                ])
            }),
        )
        .expect("insert bench_users batch");

        tx.commit().expect("commit setup transaction");
    }

    // ── Reads ─────────────────────────────────────────────────────────

    fn point_lookup_indexed(&self, id: i64) -> Option<String> {
        self.conn
            .borrow_mut()
            .exec_first::<String, _, _>("SELECT name FROM bench_users WHERE id = ?", (id,))
            .expect("point_lookup_indexed query failed")
    }

    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String> {
        self.conn
            .borrow_mut()
            .exec_first::<String, _, _>(
                "SELECT name FROM bench_users WHERE created_at = ?",
                (created_at,),
            )
            .expect("point_lookup_nonindexed query failed")
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let count: i64 = self
            .conn
            .borrow_mut()
            .exec_first(
                "SELECT COUNT(*) FROM bench_users WHERE age > ?",
                (age_threshold,),
            )
            .expect("scan_filter_count query failed")
            .expect("scan_filter_count returned no row");
        count as usize
    }

    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)> {
        self.conn
            .borrow_mut()
            .exec_map(
                "SELECT name, email FROM bench_users WHERE age > ? LIMIT 100",
                (age_threshold,),
                |(name, email): (String, String)| (name, email),
            )
            .expect("scan_filter_project_top100 query failed")
    }

    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)> {
        self.conn
            .borrow_mut()
            .exec_map(
                "SELECT name, created_at FROM bench_users \
                 WHERE age > ? ORDER BY created_at DESC LIMIT 10",
                (age_threshold,),
                |(name, created_at): (String, i64)| (name, created_at),
            )
            .expect("scan_filter_sort_limit10 query failed")
    }

    fn agg_sum(&self) -> i64 {
        // SUM over BIGINT yields DECIMAL in MySQL; the driver exposes it as a
        // signed integer for our magnitude. Fetch as i64 directly.
        self.conn
            .borrow_mut()
            .query_first::<i64, _>("SELECT SUM(age) FROM bench_users")
            .expect("agg_sum query failed")
            .expect("agg_sum returned no row")
    }

    fn agg_avg(&self, age_threshold: i64) -> f64 {
        self.conn
            .borrow_mut()
            .exec_first::<f64, _, _>(
                "SELECT AVG(age) FROM bench_users WHERE age > ?",
                (age_threshold,),
            )
            .expect("agg_avg query failed")
            .expect("agg_avg returned no row")
    }

    fn agg_min(&self) -> i64 {
        self.conn
            .borrow_mut()
            .query_first::<i64, _>("SELECT MIN(created_at) FROM bench_users")
            .expect("agg_min query failed")
            .expect("agg_min returned no row")
    }

    fn agg_max(&self) -> i64 {
        self.conn
            .borrow_mut()
            .query_first::<i64, _>("SELECT MAX(age) FROM bench_users")
            .expect("agg_max query failed")
            .expect("agg_max returned no row")
    }

    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)> {
        self.conn
            .borrow_mut()
            .exec_map(
                "SELECT name, age FROM bench_users WHERE age > ? AND status = ?",
                (age_threshold, status),
                |(name, age): (String, i64)| (name, age),
            )
            .expect("multi_col_and_filter query failed")
    }

    // ── Writes ────────────────────────────────────────────────────────

    fn insert_single(
        &mut self,
        id: i64,
        name: &str,
        age: i64,
        status: &str,
        email: &str,
        created_at: i64,
    ) {
        self.conn
            .get_mut()
            .exec_drop(
                "INSERT INTO bench_users (id, name, age, status, email, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                (id, name, age, status, email, created_at),
            )
            .expect("insert_single query failed");
    }

    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]) {
        let conn = self.conn.get_mut();
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .expect("start insert_batch transaction");

        tx.exec_batch(
            "INSERT INTO bench_users (id, name, age, status, email, created_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
            rows.iter()
                .map(|(id, name, age, status, email, created_at)| {
                    Params::Positional(vec![
                        (*id).into(),
                        name.as_str().into(),
                        (*age).into(),
                        status.as_str().into(),
                        email.as_str().into(),
                        (*created_at).into(),
                    ])
                }),
        )
        .expect("insert_batch exec_batch failed");

        tx.commit().expect("commit insert_batch transaction");
    }

    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64 {
        let conn = self.conn.get_mut();
        conn.exec_drop("UPDATE bench_users SET age = ? WHERE id = ?", (new_age, id))
            .expect("update_by_pk query failed");
        conn.affected_rows()
    }

    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64 {
        let conn = self.conn.get_mut();
        conn.exec_drop(
            "UPDATE bench_users SET status = ? WHERE age > ?",
            (new_status, age_threshold),
        )
        .expect("update_by_filter query failed");
        conn.affected_rows()
    }

    fn delete_by_filter(&mut self, age_threshold: i64) -> u64 {
        let conn = self.conn.get_mut();
        conn.exec_drop("DELETE FROM bench_users WHERE age < ?", (age_threshold,))
            .expect("delete_by_filter query failed");
        conn.affected_rows()
    }
}
