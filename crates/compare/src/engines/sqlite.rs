//! SQLite implementation of [`BenchEngine`] for comparison benchmarks.
//!
//! Uses an in-memory database (`:memory:`) for fairness — PowDB benches use
//! tmpfs, so both engines avoid real disk I/O.

use rusqlite::{params, Connection};

use super::BenchEngine;

/// SQLite-backed bench engine operating on an in-memory database.
pub struct SqliteEngine {
    conn: Connection,
}

impl SqliteEngine {
    /// Create a new in-memory SQLite engine.
    ///
    /// The User table schema is created immediately; call [`BenchEngine::setup`]
    /// to populate rows.
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("open :memory: SQLite database");

        // Match PowDB's schema: id is INTEGER PRIMARY KEY (implicitly indexed).
        conn.execute_batch(
            "CREATE TABLE user_table (
                id    INTEGER PRIMARY KEY,
                name  TEXT    NOT NULL,
                age   INTEGER NOT NULL,
                email TEXT    NOT NULL
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
        // Wrap all inserts in a single transaction for speed.
        let tx = self.conn.transaction().expect("begin transaction");
        {
            let mut stmt = tx
                .prepare("INSERT INTO user_table (id, name, age, email) VALUES (?1, ?2, ?3, ?4)")
                .expect("prepare insert");

            for i in 0..n_rows {
                let id = i as i64;
                let name = format!("user_{i}");
                let age = (18 + (i % 60)) as i64;
                let email = format!("user_{i}@example.com");
                stmt.execute(params![id, name, age, email])
                    .expect("insert row");
            }
        }
        tx.commit().expect("commit transaction");
    }

    fn point_lookup(&self, id: i64) -> Option<String> {
        self.conn
            .query_row(
                "SELECT name FROM user_table WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .ok()
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM user_table WHERE age > ?1",
                params![age_threshold],
                |row| row.get::<_, i64>(0),
            )
            .expect("scan_filter_count query") as usize
    }

    fn count_filter(&self, age_threshold: i64) -> usize {
        // For SQL engines, COUNT(*) with a WHERE clause is the canonical path —
        // identical to scan_filter_count.
        self.scan_filter_count(age_threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test() {
        let mut engine = SqliteEngine::new();
        engine.setup(1_000);

        // Point lookup: user_42 should exist.
        assert_eq!(engine.point_lookup(42), Some("user_42".to_string()));

        // Non-existent id returns None.
        assert_eq!(engine.point_lookup(9999), None);

        // age ranges from 18..77 (18 + 0..59). Rows with age > 30 are those
        // where (i % 60) > 12, i.e. 47 out of every 60. With 1000 rows:
        // 16 full groups of 60 (=752 matching) + partial group of 40 rows
        // (i=960..999, i%60 = 0..39, matching = 27). Total = 779.
        let count = engine.scan_filter_count(30);
        assert_eq!(count, 779);

        // count_filter should agree.
        assert_eq!(engine.count_filter(30), count);
    }
}
