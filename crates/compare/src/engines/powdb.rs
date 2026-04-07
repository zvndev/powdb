//! PowDB [`BenchEngine`] adapter.
//!
//! Wraps `powdb_query::executor::Engine` behind the uniform comparison
//! interface so PowQL performance can be measured against SQLite / Postgres
//! on identical workloads.

use std::cell::RefCell;

use powdb_query::executor::Engine;
use powdb_query::result::QueryResult;
use powdb_storage::row::RowLayout;
use powdb_storage::types::Value;
use tempfile::TempDir;

use super::BenchEngine;

/// Comparison-bench wrapper around the PowDB query engine.
///
/// The inner [`Engine`] lives inside a [`RefCell`] because `execute_powql`
/// requires `&mut self` while the [`BenchEngine`] trait exposes `&self`
/// query methods. This is safe for single-threaded bench harnesses.
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
}

impl BenchEngine for PowdbEngine {
    fn name(&self) -> &str {
        "powdb"
    }

    fn setup(&mut self, n_rows: usize) {
        let engine = self.engine.get_mut();

        engine
            .execute_powql(
                "type User { required id: int, required name: str, required age: int, required email: str }",
            )
            .expect("create type");

        let data_dir = self._tmp.path().to_path_buf();
        {
            let table = engine
                .catalog_mut()
                .get_table_mut("User")
                .expect("get User table");
            for i in 0..n_rows {
                let row = vec![
                    Value::Int(i as i64),
                    Value::Str(format!("user_{i}")),
                    Value::Int((18 + (i % 60)) as i64),
                    Value::Str(format!("user_{i}@example.com")),
                ];
                table.insert(&row).expect("insert row");
            }
            table
                .create_index("id", &data_dir)
                .expect("build id index");

            self.layout = Some(RowLayout::new(&table.schema));

            // Activate mmap for zero-syscall reads
            table.heap.enable_mmap();
        }
    }

    fn point_lookup(&self, id: i64) -> Option<String> {
        // Bypass PowQL parsing — go directly to the B-tree index for
        // maximum throughput. This is what a real client SDK would do
        // (cached plan or direct API call).
        let engine = self.engine.borrow();
        let tbl = engine.catalog().get_table("User")?;
        let rid = tbl.indexes.get("id")?.lookup(&Value::Int(id))?;
        let data = tbl.heap.get(rid)?;
        // Decode only the name column (index 1: id=0, name=1, age=2, email=3)
        let layout = self.layout.as_ref().unwrap();
        match powdb_storage::row::decode_column(&tbl.schema, layout, &data, 1) {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    fn scan_filter_count(&self, age_threshold: i64) -> usize {
        let query = format!("count(User filter .age > {age_threshold})");
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("scan filter query failed");

        match result {
            QueryResult::Scalar(Value::Int(n)) => n as usize,
            _ => 0,
        }
    }

    fn count_filter(&self, age_threshold: i64) -> usize {
        let query = format!("count(User filter .age > {age_threshold})");
        let result = self
            .engine
            .borrow_mut()
            .execute_powql(&query)
            .expect("count filter query failed");

        match result {
            QueryResult::Scalar(Value::Int(n)) => n as usize,
            _ => 0,
        }
    }
}
