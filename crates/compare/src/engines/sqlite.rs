//! SQLite implementation of [`BenchEngine`] for the Mission A wide bench.
//!
//! Uses an in-memory database (`:memory:`) for fairness — PowDB benches use
//! tmpfs, so both engines avoid real disk I/O. All read workloads go through
//! `Connection::prepare_cached` so repeated calls pay the prepare cost
//! exactly once (the optimistic SQL-engine case). Writes are batched inside
//! a single transaction.

use rusqlite::{params, Connection};

use super::{gen_row, BenchEngine};

/// SQLite-backed bench engine operating on an in-memory database.
pub struct SqliteEngine {
    conn: Connection,
}

impl SqliteEngine {
    /// Create a new in-memory SQLite engine with the Mission A schema.
    ///
    /// Call [`BenchEngine::setup`] to populate rows.
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("open :memory: SQLite database");

        // Match PowDB's Mission A schema. `INTEGER PRIMARY KEY` is
        // implicitly indexed in SQLite (it aliases the rowid), which
        // mirrors PowDB's explicit `create_index("id", ...)`. No additional
        // index on `created_at` — the non-indexed workload relies on that.
        conn.execute_batch(
            "CREATE TABLE user_table (
                id         INTEGER PRIMARY KEY,
                name       TEXT    NOT NULL,
                age        INTEGER NOT NULL,
                status     TEXT    NOT NULL,
                email      TEXT    NOT NULL,
                created_at INTEGER NOT NULL
            );",
        )
        .expect("create user_table");

        Self { conn }
    }
}

impl Default for SqliteEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchEngine for SqliteEngine {
    fn name(&self) -> &str {
        "sqlite"
    }

    fn setup(&mut self, n_rows: usize) {
        // Idempotent: truncate first so repeated calls (used by the
        // destructive `delete_by_filter` workload to rebuild the fixture)
        // always start from a clean slate.
        self.conn
            .execute_batch("DELETE FROM user_table")
            .expect("truncate user_table");

        // Wrap all inserts in a single transaction for speed.
        let tx = self.conn.transaction().expect("begin transaction");
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO user_table (id, name, age, status, email, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .expect("prepare insert");

            for i in 0..n_rows {
                let (id, name, age, status, email, created_at) = gen_row(i);
                stmt.execute(params![id, name, age, status, email, created_at])
                    .expect("insert row");
            }
        }
        tx.commit().expect("commit transaction");
    }

    // ── Reads ─────────────────────────────────────────────────────────

    fn point_lookup_indexed(&self, id: i64) -> Option<String> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM user_table WHERE id = ?1")
            .expect("prepare point_lookup_indexed");
        stmt.query_row(params![id], |row| row.get::<_, String>(0))
            .ok()
    }

    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT name FROM user_table WHERE created_at = ?1 LIMIT 1")
            .expect("prepare point_lookup_nonindexed");
        stmt.query_row(params![created_at], |row| row.get::<_, String>(0))
            .ok()
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT COUNT(*) FROM user_table WHERE age > ?1")
            .expect("prepare scan_filter_count");
        stmt.query_row(params![age_threshold], |row| row.get::<_, i64>(0))
            .expect("scan_filter_count query") as usize
    }

    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT name, email FROM user_table WHERE age > ?1 LIMIT 100",
            )
            .expect("prepare scan_filter_project_top100");
        let rows = stmt
            .query_map(params![age_threshold], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query scan_filter_project_top100");
        rows.filter_map(|r| r.ok()).collect()
    }

    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT name, created_at FROM user_table \
                 WHERE age > ?1 ORDER BY created_at DESC LIMIT 10",
            )
            .expect("prepare scan_filter_sort_limit10");
        let rows = stmt
            .query_map(params![age_threshold], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .expect("query scan_filter_sort_limit10");
        rows.filter_map(|r| r.ok()).collect()
    }

    fn agg_sum(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT SUM(age) FROM user_table")
            .expect("prepare agg_sum");
        stmt.query_row([], |row| row.get::<_, i64>(0))
            .expect("agg_sum query")
    }

    fn agg_avg(&self, age_threshold: i64) -> f64 {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT AVG(age) FROM user_table WHERE age > ?1")
            .expect("prepare agg_avg");
        stmt.query_row(params![age_threshold], |row| row.get::<_, f64>(0))
            .unwrap_or(0.0)
    }

    fn agg_min(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT MIN(created_at) FROM user_table")
            .expect("prepare agg_min");
        stmt.query_row([], |row| row.get::<_, i64>(0))
            .expect("agg_min query")
    }

    fn agg_max(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT MAX(age) FROM user_table")
            .expect("prepare agg_max");
        stmt.query_row([], |row| row.get::<_, i64>(0))
            .expect("agg_max query")
    }

    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT name, age FROM user_table WHERE age > ?1 AND status = ?2",
            )
            .expect("prepare multi_col_and_filter");
        let rows = stmt
            .query_map(params![age_threshold, status], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .expect("query multi_col_and_filter");
        rows.filter_map(|r| r.ok()).collect()
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
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO user_table (id, name, age, status, email, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .expect("prepare insert_single");
        stmt.execute(params![id, name, age, status, email, created_at])
            .expect("insert_single execute");
    }

    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]) {
        let tx = self.conn.transaction().expect("begin transaction");
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO user_table (id, name, age, status, email, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .expect("prepare insert_batch");
            for (id, name, age, status, email, created_at) in rows {
                stmt.execute(params![id, name, age, status, email, created_at])
                    .expect("insert_batch execute");
            }
        }
        tx.commit().expect("commit insert_batch");
    }

    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64 {
        let mut stmt = self
            .conn
            .prepare_cached("UPDATE user_table SET age = ?1 WHERE id = ?2")
            .expect("prepare update_by_pk");
        stmt.execute(params![new_age, id]).expect("update_by_pk") as u64
    }

    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64 {
        let mut stmt = self
            .conn
            .prepare_cached("UPDATE user_table SET status = ?1 WHERE age > ?2")
            .expect("prepare update_by_filter");
        stmt.execute(params![new_status, age_threshold])
            .expect("update_by_filter") as u64
    }

    fn delete_by_filter(&mut self, age_threshold: i64) -> u64 {
        let mut stmt = self
            .conn
            .prepare_cached("DELETE FROM user_table WHERE age < ?1")
            .expect("prepare delete_by_filter");
        stmt.execute(params![age_threshold])
            .expect("delete_by_filter") as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test() {
        let mut engine = SqliteEngine::new();
        engine.setup(1_000);

        assert_eq!(
            engine.point_lookup_indexed(42),
            Some("user_42".to_string())
        );
        assert_eq!(engine.point_lookup_indexed(9999), None);

        // Rows with age > 30: (i % 60) > 12, so 47 out of every 60.
        let count = engine.scan_filter_count(30);
        assert!(count > 0);
    }
}
