//! Postgres implementation of [`BenchEngine`] for the Mission A wide bench.
//!
//! Returns `None` from `try_new()` if the server is unreachable so the
//! harness can skip the engine gracefully. All reads use prepared
//! statements; writes are batched in transactions.

use std::cell::RefCell;

use postgres::{Client, NoTls, Statement};

use super::{gen_row, BenchEngine};

/// Postgres-backed benchmark engine.
pub struct PostgresEngine {
    client: RefCell<Client>,
    /// Cached prepared statements keyed by workload. Lazily populated by
    /// each read method on first call. We don't pre-prepare in `try_new`
    /// to keep the constructor's error-handling simple.
    prepared: RefCell<Prepared>,
}

#[derive(Default)]
struct Prepared {
    point_lookup_indexed: Option<Statement>,
    point_lookup_nonindexed: Option<Statement>,
    scan_filter_count: Option<Statement>,
    scan_filter_project_top100: Option<Statement>,
    scan_filter_sort_limit10: Option<Statement>,
    agg_sum: Option<Statement>,
    agg_avg: Option<Statement>,
    agg_min: Option<Statement>,
    agg_max: Option<Statement>,
    multi_col_and_filter: Option<Statement>,
    insert_single: Option<Statement>,
    update_by_pk: Option<Statement>,
    update_by_filter: Option<Statement>,
    delete_by_filter: Option<Statement>,
}

impl PostgresEngine {
    /// Attempt to connect to Postgres and prepare the `bench_users` table.
    ///
    /// Connection URL resolution order:
    /// 1. `POWDB_BENCH_PG_URL` environment variable (set to `skip` to
    ///    deliberately bypass Postgres entirely).
    /// 2. `postgresql://postgres:powdb@localhost:5432/powdb_bench`
    /// 3. `postgresql://localhost:5432/powdb_bench` (legacy fallback)
    ///
    /// Returns `None` when the connection cannot be established.
    pub fn try_new() -> Option<Self> {
        let url = match std::env::var("POWDB_BENCH_PG_URL") {
            Ok(v) if v == "skip" => return None,
            Ok(v) => v,
            Err(_) => "postgresql://postgres:powdb@localhost:5432/powdb_bench".to_string(),
        };

        let mut client = Client::connect(&url, NoTls)
            .or_else(|_| Client::connect("postgresql://localhost:5432/powdb_bench", NoTls))
            .ok()?;

        client
            .batch_execute("DROP TABLE IF EXISTS bench_users")
            .ok()?;

        client
            .batch_execute(
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

        // Explicit index on id (PRIMARY KEY already creates one, but the
        // spec asks for it to be stated explicitly).
        client
            .batch_execute("CREATE INDEX IF NOT EXISTS idx_bench_users_id ON bench_users (id)")
            .ok()?;

        Some(Self {
            client: RefCell::new(client),
            prepared: RefCell::new(Prepared::default()),
        })
    }
}

macro_rules! prep {
    ($self:expr, $field:ident, $sql:expr) => {{
        let mut cache = $self.prepared.borrow_mut();
        if cache.$field.is_none() {
            let s = $self
                .client
                .borrow_mut()
                .prepare($sql)
                .expect("prepare statement");
            cache.$field = Some(s);
        }
        cache.$field.as_ref().unwrap().clone()
    }};
}

impl BenchEngine for PostgresEngine {
    fn name(&self) -> &str {
        "postgres"
    }

    fn setup(&mut self, n_rows: usize) {
        // Use COPY for fast bulk-loading inside a single transaction.
        let mut client = self.client.borrow_mut();
        client
            .batch_execute("TRUNCATE bench_users")
            .expect("truncate bench_users");
        let mut writer = client
            .copy_in(
                "COPY bench_users (id, name, age, status, email, created_at) FROM STDIN WITH (FORMAT text)",
            )
            .expect("COPY IN failed to start");

        use std::io::Write;
        let mut buf = Vec::with_capacity(n_rows * 96);
        for i in 0..n_rows {
            let (id, name, age, status, email, created_at) = gen_row(i);
            writeln!(
                buf,
                "{}\t{}\t{}\t{}\t{}\t{}",
                id, name, age, status, email, created_at
            )
            .unwrap();

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

    // ── Reads ─────────────────────────────────────────────────────────

    fn point_lookup_indexed(&self, id: i64) -> Option<String> {
        let stmt = prep!(
            self,
            point_lookup_indexed,
            "SELECT name FROM bench_users WHERE id = $1"
        );
        self.client
            .borrow_mut()
            .query_opt(&stmt, &[&id])
            .expect("point_lookup_indexed")
            .map(|row| row.get::<_, String>(0))
    }

    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String> {
        let stmt = prep!(
            self,
            point_lookup_nonindexed,
            "SELECT name FROM bench_users WHERE created_at = $1 LIMIT 1"
        );
        self.client
            .borrow_mut()
            .query_opt(&stmt, &[&created_at])
            .expect("point_lookup_nonindexed")
            .map(|row| row.get::<_, String>(0))
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let stmt = prep!(
            self,
            scan_filter_count,
            "SELECT COUNT(*) FROM bench_users WHERE age > $1"
        );
        let row = self
            .client
            .borrow_mut()
            .query_one(&stmt, &[&age_threshold])
            .expect("scan_filter_count");
        row.get::<_, i64>(0) as usize
    }

    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)> {
        let stmt = prep!(
            self,
            scan_filter_project_top100,
            "SELECT name, email FROM bench_users WHERE age > $1 LIMIT 100"
        );
        self.client
            .borrow_mut()
            .query(&stmt, &[&age_threshold])
            .expect("scan_filter_project_top100")
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect()
    }

    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)> {
        let stmt = prep!(
            self,
            scan_filter_sort_limit10,
            "SELECT name, created_at FROM bench_users WHERE age > $1 ORDER BY created_at DESC LIMIT 10"
        );
        self.client
            .borrow_mut()
            .query(&stmt, &[&age_threshold])
            .expect("scan_filter_sort_limit10")
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect()
    }

    fn agg_sum(&self) -> i64 {
        let stmt = prep!(self, agg_sum, "SELECT SUM(age) FROM bench_users");
        let row = self
            .client
            .borrow_mut()
            .query_one(&stmt, &[])
            .expect("agg_sum");
        row.get::<_, i64>(0)
    }

    fn agg_avg(&self, age_threshold: i64) -> f64 {
        let stmt = prep!(
            self,
            agg_avg,
            "SELECT AVG(age)::float8 FROM bench_users WHERE age > $1"
        );
        let row = self
            .client
            .borrow_mut()
            .query_one(&stmt, &[&age_threshold])
            .expect("agg_avg");
        row.get::<_, f64>(0)
    }

    fn agg_min(&self) -> i64 {
        let stmt = prep!(self, agg_min, "SELECT MIN(created_at) FROM bench_users");
        let row = self
            .client
            .borrow_mut()
            .query_one(&stmt, &[])
            .expect("agg_min");
        row.get::<_, i64>(0)
    }

    fn agg_max(&self) -> i64 {
        let stmt = prep!(self, agg_max, "SELECT MAX(age) FROM bench_users");
        let row = self
            .client
            .borrow_mut()
            .query_one(&stmt, &[])
            .expect("agg_max");
        row.get::<_, i64>(0)
    }

    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)> {
        let stmt = prep!(
            self,
            multi_col_and_filter,
            "SELECT name, age FROM bench_users WHERE age > $1 AND status = $2"
        );
        self.client
            .borrow_mut()
            .query(&stmt, &[&age_threshold, &status])
            .expect("multi_col_and_filter")
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect()
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
        let stmt = prep!(
            self,
            insert_single,
            "INSERT INTO bench_users (id, name, age, status, email, created_at) VALUES ($1, $2, $3, $4, $5, $6)"
        );
        self.client
            .borrow_mut()
            .execute(&stmt, &[&id, &name, &age, &status, &email, &created_at])
            .expect("insert_single");
    }

    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]) {
        // Wrap the batch in a single transaction, re-preparing the
        // insert statement on the transaction because `Statement`
        // handles are bound to their originating connection/transaction.
        let mut client = self.client.borrow_mut();
        let mut tx = client.transaction().expect("begin transaction");
        let stmt = tx
            .prepare(
                "INSERT INTO bench_users (id, name, age, status, email, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .expect("prepare insert_batch");
        for (id, name, age, status, email, created_at) in rows {
            tx.execute(
                &stmt,
                &[
                    id,
                    &name.as_str(),
                    age,
                    &status.as_str(),
                    &email.as_str(),
                    created_at,
                ],
            )
            .expect("insert_batch row");
        }
        tx.commit().expect("commit insert_batch");
    }

    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64 {
        let stmt = prep!(
            self,
            update_by_pk,
            "UPDATE bench_users SET age = $1 WHERE id = $2"
        );
        self.client
            .borrow_mut()
            .execute(&stmt, &[&new_age, &id])
            .expect("update_by_pk")
    }

    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64 {
        let stmt = prep!(
            self,
            update_by_filter,
            "UPDATE bench_users SET status = $1 WHERE age > $2"
        );
        self.client
            .borrow_mut()
            .execute(&stmt, &[&new_status, &age_threshold])
            .expect("update_by_filter")
    }

    fn delete_by_filter(&mut self, age_threshold: i64) -> u64 {
        let stmt = prep!(
            self,
            delete_by_filter,
            "DELETE FROM bench_users WHERE age < $1"
        );
        self.client
            .borrow_mut()
            .execute(&stmt, &[&age_threshold])
            .expect("delete_by_filter")
    }
}
