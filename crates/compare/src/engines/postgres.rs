use std::cell::RefCell;

use postgres::{Client, NoTls};

use super::BenchEngine;

/// Postgres-backed benchmark engine for apples-to-apples comparison with PowDB.
///
/// Use [`PostgresEngine::try_new`] to obtain an instance. If the database is
/// unreachable the constructor returns `None` so the harness can skip Postgres
/// benchmarks gracefully.
pub struct PostgresEngine {
    client: RefCell<Client>,
}

impl PostgresEngine {
    /// Attempt to connect to Postgres and prepare the `bench_users` table.
    ///
    /// Connection URL resolution order:
    /// 1. `POWDB_BENCH_PG_URL` environment variable
    /// 2. `postgresql://localhost:5432/powdb_bench`
    ///
    /// Returns `None` when the connection cannot be established.
    pub fn try_new() -> Option<Self> {
        let url = std::env::var("POWDB_BENCH_PG_URL")
            .unwrap_or_else(|_| "postgresql://localhost:5432/powdb_bench".to_string());

        let mut client = Client::connect(&url, NoTls).ok()?;

        client
            .batch_execute("DROP TABLE IF EXISTS bench_users")
            .ok()?;

        client
            .batch_execute(
                "CREATE TABLE bench_users (
                    id    BIGINT PRIMARY KEY,
                    name  TEXT   NOT NULL,
                    age   BIGINT NOT NULL,
                    email TEXT   NOT NULL
                )",
            )
            .ok()?;

        // Explicit index on id (PRIMARY KEY already creates one, but the spec
        // asks for it to be stated explicitly).
        client
            .batch_execute("CREATE INDEX IF NOT EXISTS idx_bench_users_id ON bench_users (id)")
            .ok()?;

        Some(Self { client: RefCell::new(client) })
    }
}

impl BenchEngine for PostgresEngine {
    fn name(&self) -> &str {
        "Postgres"
    }

    fn setup(&mut self, n_rows: usize) {
        // Use COPY for fast bulk-loading inside a single transaction.
        let mut client = self.client.borrow_mut();
        let mut writer = client
            .copy_in("COPY bench_users (id, name, age, email) FROM STDIN WITH (FORMAT text)")
            .expect("COPY IN failed to start");

        use std::io::Write;
        // Write tab-separated rows; text-mode COPY uses \t as delimiter and \n
        // as row terminator by default.
        let mut buf = Vec::with_capacity(n_rows * 64);
        for i in 0..n_rows {
            let id = i as i64;
            let age = 18 + (i % 60) as i64;
            write!(
                buf,
                "{}\tuser_{}\t{}\tuser_{}@example.com\n",
                id, i, age, i
            )
            .unwrap();

            // Flush in ~8 MiB chunks to keep memory bounded.
            if buf.len() >= 8 * 1024 * 1024 {
                writer.write_all(&buf).expect("COPY write failed");
                buf.clear();
            }
        }

        if !buf.is_empty() {
            writer.write_all(&buf).expect("COPY write failed");
        }

        writer.finish().expect("COPY finish failed");
    }

    fn point_lookup(&self, id: i64) -> Option<String> {
        self.client
            .borrow_mut()
            .query_opt("SELECT name FROM bench_users WHERE id = $1", &[&id])
            .expect("point_lookup query failed")
            .map(|row| row.get::<_, String>(0))
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let row = self
            .client
            .borrow_mut()
            .query_one(
                "SELECT COUNT(*) FROM bench_users WHERE age > $1",
                &[&age_threshold],
            )
            .expect("scan_filter_count query failed");

        row.get::<_, i64>(0) as usize
    }

    fn count_filter(&self, age_threshold: i64) -> usize {
        let row = self
            .client
            .borrow_mut()
            .query_one(
                "SELECT COUNT(*) FROM bench_users WHERE age > $1",
                &[&age_threshold],
            )
            .expect("count_filter query failed");

        row.get::<_, i64>(0) as usize
    }
}
