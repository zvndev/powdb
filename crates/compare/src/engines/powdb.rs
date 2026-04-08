//! PowDB [`BenchEngine`] adapter for the Mission A wide bench.
//!
//! Wraps `powdb_query::executor::Engine` behind the uniform comparison
//! interface so all 15 Mission A workloads can run head-to-head against
//! SQLite / Postgres / MySQL.
//!
//! ## Design notes
//!
//! - The inner [`Engine`] lives inside a [`RefCell`] because `execute_powql`
//!   requires `&mut self` while the read methods on [`BenchEngine`] take
//!   `&self`. This is safe for single-threaded bench harnesses.
//! - `point_lookup_indexed` bypasses PowQL parsing and walks the B-tree
//!   directly; this mirrors the existing fast-path pattern established in
//!   PR #1 and what a real client SDK would do with a cached plan.
//! - All other reads go through `execute_powql()` so we measure the full
//!   parse → plan → execute pipeline end-to-end. For the four non-count
//!   aggregates we temporarily bypass the parser and build a `PlanNode`
//!   by hand (see §1 fallback clause in PLAN-MISSION-A.md) because the
//!   parser extension for `sum(User { .col })` has not landed in this
//!   worktree.
//! - Writes go through `execute_powql()` with one query string per call,
//!   with batched writes wrapped in a tight loop (PowQL has no batch-insert
//!   syntax today).

use std::cell::RefCell;

use powdb_query::ast::{AggFunc, Expr, Literal};
use powdb_query::executor::Engine;
use powdb_query::plan::PlanNode;
use powdb_query::result::QueryResult;
use powdb_storage::row::RowLayout;
use powdb_storage::types::Value;
use tempfile::TempDir;

use super::{gen_row, BenchEngine};

/// Comparison-bench wrapper around the PowDB query engine.
pub struct PowdbEngine {
    engine: RefCell<Engine>,
    /// Cached layout for zero-alloc column decode on point lookups.
    layout: Option<RowLayout>,
    /// Keeps the temp directory alive for the lifetime of the engine.
    _tmp: TempDir,
}

impl PowdbEngine {
    pub fn new() -> Self {
        let tmp = TempDir::new().expect("create tempdir");
        let engine = Engine::new(tmp.path()).expect("engine init");
        PowdbEngine {
            engine: RefCell::new(engine),
            layout: None,
            _tmp: tmp,
        }
    }

    /// Run a PowQL read query and return the first row's first column as an
    /// `Option<String>`. Used by the two point-lookup workloads.
    fn powql_first_string(&self, query: &str) -> Option<String> {
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(query)
            .expect("powql read failed");
        match result {
            QueryResult::Rows { mut rows, .. } => {
                if rows.is_empty() {
                    None
                } else {
                    match rows.remove(0).into_iter().next() {
                        Some(Value::Str(s)) => Some(s),
                        _ => None,
                    }
                }
            }
            _ => None,
        }
    }

    /// Build and execute an `Aggregate(function, ...)` plan directly.
    ///
    /// The PowQL parser currently hard-codes `AggregateExpr.field` to `None`
    /// (see PLAN-MISSION-A.md §1 "How we resolve the aggregate-column gap"),
    /// so until FASTPATH lands the parser extension we hand-build the plan
    /// node and feed it to `execute_plan` directly.
    ///
    /// This bypasses the parser and planner entirely but still exercises the
    /// full executor — a fair representation of a "compiled plan" access
    /// pattern until the parser catches up.
    fn exec_agg_with_field(
        &self,
        func: AggFunc,
        field: &str,
        filter: Option<Expr>,
    ) -> QueryResult {
        let mut input = PlanNode::SeqScan {
            table: "User".to_string(),
        };
        if let Some(pred) = filter {
            input = PlanNode::Filter {
                input: Box::new(input),
                predicate: pred,
            };
        }
        let plan = PlanNode::Aggregate {
            input: Box::new(input),
            function: func,
            field: Some(field.to_string()),
        };
        self.engine
            .borrow_mut()
            .execute_plan(&plan)
            .expect("aggregate plan execution failed")
    }

    fn scalar_int(r: QueryResult) -> i64 {
        match r {
            QueryResult::Scalar(Value::Int(n)) => n,
            other => panic!("expected scalar int, got {other:?}"),
        }
    }

    fn scalar_float(r: QueryResult) -> f64 {
        match r {
            QueryResult::Scalar(Value::Float(f)) => f,
            QueryResult::Scalar(Value::Int(n)) => n as f64,
            other => panic!("expected scalar number, got {other:?}"),
        }
    }
}

impl Default for PowdbEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchEngine for PowdbEngine {
    fn name(&self) -> &str {
        "powdb"
    }

    fn setup(&mut self, n_rows: usize) {
        // Idempotent: destructive workloads (`delete_by_filter`) call
        // `setup` repeatedly to rebuild the fixture. Because PowDB's heap
        // holds a write-through mmap that is frozen at `enable_mmap()`
        // time, the cleanest way to reset the fixture is to wipe the
        // entire engine + tempdir and start over. That also guarantees
        // we don't accidentally inherit stale indexes or half-deleted
        // rows from a prior iteration.
        if self.engine.get_mut().catalog().get_table("User").is_some() {
            let fresh_tmp = TempDir::new().expect("create tempdir");
            let fresh_engine = Engine::new(fresh_tmp.path()).expect("engine reset");
            self.engine = RefCell::new(fresh_engine);
            self.layout = None;
            self._tmp = fresh_tmp;
        }

        let engine = self.engine.get_mut();
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

        let data_dir = self._tmp.path().to_path_buf();
        {
            let table = engine
                .catalog_mut()
                .get_table_mut("User")
                .expect("get User table");
            for i in 0..n_rows {
                let (id, name, age, status, email, created_at) = gen_row(i);
                let row = vec![
                    Value::Int(id),
                    Value::Str(name),
                    Value::Int(age),
                    Value::Str(status),
                    Value::Str(email),
                    Value::Int(created_at),
                ];
                table.insert(&row).expect("insert row");
            }

            // Preserve index on `id` — this is the only index for the
            // 15-workload run. `point_lookup_indexed` depends on it being
            // present; losing it silently turns the Project(IndexScan) fast
            // path into a linear scan and tanks the headline ratio.
            // `create_index` is safe to call again; the table rebuilds the
            // index from current row contents.
            table
                .create_index("id", &data_dir)
                .expect("build id index");

            self.layout = Some(RowLayout::new(&table.schema));

            // Activate mmap for zero-syscall reads.
            table.heap.enable_mmap();
        }
    }

    // ── Reads ─────────────────────────────────────────────────────────

    fn point_lookup_indexed(&self, id: i64) -> Option<String> {
        // Direct B-tree lookup — bypasses the PowQL parser entirely. This
        // is the "cached plan / direct API" path a real client SDK would
        // use. See §4 FASTPATH→BENCH in PLAN-MISSION-A.md.
        let engine = self.engine.borrow();
        let tbl = engine.catalog().get_table("User")?;
        let rid = tbl.indexes.get("id")?.lookup(&Value::Int(id))?;
        let data = tbl.heap.get(rid)?;
        // Columns: id=0, name=1, age=2, status=3, email=4, created_at=5
        let layout = self.layout.as_ref().unwrap();
        match powdb_storage::row::decode_column(&tbl.schema, layout, &data, 1) {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String> {
        let query = format!("User filter .created_at = {created_at} {{ .name }}");
        self.powql_first_string(&query)
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let query = format!("count(User filter .age > {age_threshold})");
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("scan_filter_count query failed");
        match result {
            QueryResult::Scalar(Value::Int(n)) => n as usize,
            _ => 0,
        }
    }

    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)> {
        let query = format!(
            "User filter .age > {age_threshold} limit 100 {{ .name, .email }}"
        );
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("scan_filter_project_top100 query failed");
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|mut r| {
                    let email = match r.pop() {
                        Some(Value::Str(s)) => s,
                        _ => String::new(),
                    };
                    let name = match r.pop() {
                        Some(Value::Str(s)) => s,
                        _ => String::new(),
                    };
                    (name, email)
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)> {
        let query = format!(
            "User filter .age > {age_threshold} order .created_at desc limit 10 {{ .name, .created_at }}"
        );
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("scan_filter_sort_limit10 query failed");
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|mut r| {
                    let created_at = match r.pop() {
                        Some(Value::Int(n)) => n,
                        _ => 0,
                    };
                    let name = match r.pop() {
                        Some(Value::Str(s)) => s,
                        _ => String::new(),
                    };
                    (name, created_at)
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn agg_sum(&self) -> i64 {
        // FALLBACK path: parser cannot attach a column to a non-count
        // aggregate until FASTPATH lands. Hand-build the plan instead.
        Self::scalar_int(self.exec_agg_with_field(AggFunc::Sum, "age", None))
    }

    fn agg_avg(&self, age_threshold: i64) -> f64 {
        let filter = Expr::BinaryOp(
            Box::new(Expr::Field("age".to_string())),
            powdb_query::ast::BinOp::Gt,
            Box::new(Expr::Literal(Literal::Int(age_threshold))),
        );
        Self::scalar_float(self.exec_agg_with_field(AggFunc::Avg, "age", Some(filter)))
    }

    fn agg_min(&self) -> i64 {
        Self::scalar_int(self.exec_agg_with_field(AggFunc::Min, "created_at", None))
    }

    fn agg_max(&self) -> i64 {
        Self::scalar_int(self.exec_agg_with_field(AggFunc::Max, "age", None))
    }

    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)> {
        // Note: PowQL string literals use double quotes. We rely on the
        // caller passing a status with no embedded quotes (always one of
        // the canonical `STATUSES`); no escaping is needed.
        let query = format!(
            "User filter .age > {age_threshold} and .status = \"{status}\" {{ .name, .age }}"
        );
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("multi_col_and_filter query failed");
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|mut r| {
                    let age = match r.pop() {
                        Some(Value::Int(n)) => n,
                        _ => 0,
                    };
                    let name = match r.pop() {
                        Some(Value::Str(s)) => s,
                        _ => String::new(),
                    };
                    (name, age)
                })
                .collect(),
            _ => Vec::new(),
        }
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
        let query = format!(
            "insert User {{ id := {id}, name := \"{name}\", age := {age}, status := \"{status}\", email := \"{email}\", created_at := {created_at} }}"
        );
        let _ = self
            .engine
            .get_mut()
            .execute_powql(&query)
            .expect("insert_single failed");
    }

    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]) {
        // PowQL has no batch-insert syntax; loop one INSERT per row, all
        // going through `execute_powql` like the SQL engines' prepared
        // statements. Each call is independent — PowDB has no transaction
        // at the query-text layer (writes are already ACID per call).
        let engine = self.engine.get_mut();
        for (id, name, age, status, email, created_at) in rows {
            let query = format!(
                "insert User {{ id := {id}, name := \"{name}\", age := {age}, status := \"{status}\", email := \"{email}\", created_at := {created_at} }}"
            );
            engine
                .execute_powql(&query)
                .expect("insert_batch row failed");
        }
    }

    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64 {
        let query = format!("User filter .id = {id} update {{ age := {new_age} }}");
        let result = self
            .engine
            .get_mut()
            .execute_powql(&query)
            .expect("update_by_pk failed");
        match result {
            QueryResult::Modified(n) => n,
            _ => 0,
        }
    }

    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64 {
        let query = format!(
            "User filter .age > {age_threshold} update {{ status := \"{new_status}\" }}"
        );
        let result = self
            .engine
            .get_mut()
            .execute_powql(&query)
            .expect("update_by_filter failed");
        match result {
            QueryResult::Modified(n) => n,
            _ => 0,
        }
    }

    fn delete_by_filter(&mut self, age_threshold: i64) -> u64 {
        let query = format!("User filter .age < {age_threshold} delete");
        let result = self
            .engine
            .get_mut()
            .execute_powql(&query)
            .expect("delete_by_filter failed");
        match result {
            QueryResult::Modified(n) => n,
            _ => 0,
        }
    }
}
