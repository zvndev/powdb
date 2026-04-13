use crate::ast::*;
use crate::canonicalize::canonicalize;
use crate::plan::*;
use crate::plan_cache::PlanCache;
use crate::planner;
use crate::result::QueryResult;
use powdb_storage::catalog::Catalog;
use powdb_storage::row::{RowLayout, decode_column, decode_row, patch_var_column_in_place};
use powdb_storage::types::*;
use powdb_storage::view::{ViewDef, ViewRegistry};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{info, error, Level};

/// Sentinel error returned by `Engine::execute_powql_readonly` when the
/// query touches a materialized view whose backing table is dirty. The
/// read path holds only `&self`, so it can't refresh the view — the caller
/// is expected to recognise this prefix and retry with the write lock.
///
/// Mission infra-1: this is the escalation hook between the RwLock reader
/// fast path and the generic write path. Handlers match on it verbatim.
pub const READONLY_NEEDS_WRITE: &str = "__POWDB_READONLY_NEEDS_WRITE__";

/// Plan cache capacity. Bench workloads fill ~15 slots; real apps will sit
/// comfortably in 256. Lookup is O(1), collisions clear the cache (see
/// `plan_cache::PlanCache::insert`).
const PLAN_CACHE_CAPACITY: usize = 256;

// ─── Mission D11 Phase 1: scalar hot-loop helpers ─────────────────────────
//
// These macros expand into the scan body of `agg_single_col_fast` and sit
// inside the `for_each_row_raw` closure. They exist to:
//
//   1. Split the loop on presence of a predicate *outside* the hot body,
//      so the no-predicate path (agg_sum/agg_min/agg_max bench workloads)
//      never pays the `Option<CompiledPredicate>` branch per row.
//   2. Drop two bounds checks per row by reading the null bitmap byte
//      and the 8-byte value via raw pointer casts.
//
// SAFETY (shared across every call site below):
//
//   - `$bmp_byte` is `col_idx / 8` where `col_idx < n_cols`, and the row
//     encoding stores `bitmap_size = n_cols.div_ceil(8)` bytes of bitmap
//     starting at offset 2. So `2 + $bmp_byte < 2 + bitmap_size ≤ row_len`
//     and `get_unchecked(2 + $bmp_byte)` is inside the row slice.
//   - `$off = 2 + bitmap_size + fixed_offsets[col_idx]` for a fixed-size
//     column. Every fixed-size column contributes `fixed_size(type_id)`
//     bytes to the fixed region, so the row always has `[$off .. $off+8]`
//     available for any i64/f64 column — enforced by the row encoder
//     (`storage/src/row.rs`) and the schema invariant that a row with a
//     given schema has `row_len ≥ 2 + bitmap_size + fixed_region_size`.
//   - Both macros are only invoked from `agg_single_col_fast`, which
//     early-returns if the column isn't Int/Float (8-byte fixed) and
//     early-returns if `fast.fixed_offsets[col_idx]` is `None`.
macro_rules! agg_int_loop {
    (
        $self:expr, $table:expr, $pred:expr,
        $bmp_byte:expr, $bmp_bit:expr, $off:expr,
        |$v:ident : i64| $body:block
    ) => {{
        let bmp_byte = $bmp_byte;
        let bmp_bit = $bmp_bit;
        let off = $off;
        if let Some(pred) = &$pred {
            $self.catalog.for_each_row_raw($table, |_rid, data| {
                if !pred(data) { return; }
                // SAFETY: see module-level comment on agg_int_loop!.
                let bmp = unsafe { *data.get_unchecked(2 + bmp_byte) };
                if (bmp >> bmp_bit) & 1 == 1 { return; }
                let $v: i64 = unsafe {
                    i64::from_le_bytes(*(data.as_ptr().add(off) as *const [u8; 8]))
                };
                $body
            }).map_err(|e| e.to_string())?;
        } else {
            $self.catalog.for_each_row_raw($table, |_rid, data| {
                // SAFETY: see module-level comment on agg_int_loop!.
                let bmp = unsafe { *data.get_unchecked(2 + bmp_byte) };
                if (bmp >> bmp_bit) & 1 == 1 { return; }
                let $v: i64 = unsafe {
                    i64::from_le_bytes(*(data.as_ptr().add(off) as *const [u8; 8]))
                };
                $body
            }).map_err(|e| e.to_string())?;
        }
    }};
}

macro_rules! agg_float_loop {
    (
        $self:expr, $table:expr, $pred:expr,
        $bmp_byte:expr, $bmp_bit:expr, $off:expr,
        |$v:ident : f64| $body:block
    ) => {{
        let bmp_byte = $bmp_byte;
        let bmp_bit = $bmp_bit;
        let off = $off;
        if let Some(pred) = &$pred {
            $self.catalog.for_each_row_raw($table, |_rid, data| {
                if !pred(data) { return; }
                // SAFETY: see module-level comment on agg_float_loop!.
                let bmp = unsafe { *data.get_unchecked(2 + bmp_byte) };
                if (bmp >> bmp_bit) & 1 == 1 { return; }
                let $v: f64 = unsafe {
                    f64::from_le_bytes(*(data.as_ptr().add(off) as *const [u8; 8]))
                };
                $body
            }).map_err(|e| e.to_string())?;
        } else {
            $self.catalog.for_each_row_raw($table, |_rid, data| {
                // SAFETY: see module-level comment on agg_float_loop!.
                let bmp = unsafe { *data.get_unchecked(2 + bmp_byte) };
                if (bmp >> bmp_bit) & 1 == 1 { return; }
                let $v: f64 = unsafe {
                    f64::from_le_bytes(*(data.as_ptr().add(off) as *const [u8; 8]))
                };
                $body
            }).map_err(|e| e.to_string())?;
        }
    }};
}

/// Mission infra-1: classify a parsed statement as read-only vs. mutating.
/// Used by [`Engine::execute_powql_readonly`] and by the server handler
/// to decide between the RwLock reader and writer sides. `Union` recurses
/// because each side can independently be read/write (though in practice
/// both sides are reads — the parser only builds Union from query shapes).
pub fn is_read_only_statement(stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(_) => true,
        Statement::Union(u) => {
            is_read_only_statement(&u.left) && is_read_only_statement(&u.right)
        }
        Statement::Insert(_)
        | Statement::Upsert(_)
        | Statement::UpdateQuery(_)
        | Statement::DeleteQuery(_)
        | Statement::CreateType(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateView(_)
        | Statement::RefreshView(_)
        | Statement::DropView(_) => false,
        Statement::Explain(inner) => is_read_only_statement(inner),
    }
}

pub struct Engine {
    catalog: Catalog,
    /// Mission D9 — cached parsed+planned query trees keyed by canonical
    /// hash. Saves the ~3μs parse+plan cost on repeat queries that differ
    /// only in literal values.
    ///
    /// Mission infra-1: wrapped in `Mutex` so the read path can be driven
    /// by `&self`. The critical section is extremely short — a single
    /// hashmap lookup + plan clone on a hit, or a single insert on a miss.
    /// A full `RwLock` would be over-engineered here; the contention window
    /// is smaller than the read-path scan work it gates.
    plan_cache: Mutex<PlanCache>,
    /// Mission C Phase 13: reusable `Vec<Value>` scratch buffer for the
    /// prepared-insert fast path. `execute_prepared` used to allocate a
    /// fresh `vec![Value::Empty; n_cols]` on every insert; recycling this
    /// buffer shaves one heap alloc per row on `insert_batch_1k`.
    insert_values_scratch: Vec<Value>,
    /// Materialized view registry: tracks view definitions, dependencies,
    /// and dirty state. Views are backed by regular catalog tables; this
    /// registry adds the lifecycle metadata.
    view_registry: ViewRegistry,
}

/// Mission C Phase 5: a pre-parsed, pre-planned query. The caller holds
/// one of these and repeatedly executes it with fresh literal values via
/// [`Engine::execute_prepared`]. This is PowDB's equivalent of SQLite's
/// `prepare_cached` — the parse + plan cost is paid exactly once, and
/// every subsequent execution skips the lexer, the canonicalise hash,
/// and the plan-cache hashmap lookup.
///
/// The template plan still contains the literal values from the original
/// query string. They're overwritten on every call. See `execute_prepared`
/// for the substitution walk order.
///
/// For `PlanNode::Insert` templates whose assignment values are all plain
/// literals (the common case — `insert T { id := 1, name := "a" }`), we
/// additionally resolve the column indices at prepare time and stash them
/// in `insert_col_indices`. That lets `execute_prepared` skip the
/// plan-clone + substitute walk entirely and build the row directly from
/// the caller's literal slice — the fastest possible insert through the
/// query layer.
#[derive(Clone)]
pub struct PreparedQuery {
    plan_template: PlanNode,
    /// Total number of `Expr::Literal` slots reachable from the plan.
    /// Callers must supply exactly this many literals per execution.
    pub param_count: usize,
    /// Fast-path metadata for `PlanNode::Insert`. `Some` when:
    ///   * the template is an Insert, and
    ///   * every assignment RHS is `Expr::Literal(_)` (no computed exprs),
    ///     which means param_count == assignments.len() and the caller's
    ///     literal slice maps 1:1 to schema column indices.
    ///
    /// Mission C Phase 15: upgraded from a bare `Vec<usize>` to a
    /// dedicated [`InsertFast`] struct so the execute path can skip the
    /// second `catalog.schema(table)` HashMap lookup just to read
    /// `n_cols`, and can dispatch through `get_table_mut` + `tbl.insert`
    /// instead of going via the generic `catalog.insert` wrapper.
    insert_fast: Option<InsertFast>,
    /// Mission C Phase 14: fast-path metadata for point updates by primary
    /// key — `T filter .pk = <lit> update { col := <lit> }` where `pk` is
    /// an indexed column and `col` is fixed-size and not indexed. At
    /// execute time we skip plan clone, substitute walk, schema re-lookup,
    /// `resolved_assignments` + `FastPatch` + `matching_rids` Vec allocs,
    /// and the whole `PlanNode::Update` arm. Just a btree lookup and a
    /// byte patch.
    update_pk_fast: Option<UpdatePkFast>,
}

/// Mission C Phase 15: precomputed insert fast-path metadata. Built once
/// in [`Engine::prepare`] from a `PlanNode::Insert` template whose every
/// assignment RHS is a raw literal. The execute path reads `n_cols` and
/// `col_indices` directly — no catalog schema lookup needed.
#[derive(Clone)]
struct InsertFast {
    /// Mission C Phase 18: cached slot index into `Catalog::tables`.
    /// Resolved once at `prepare` time and stable for the lifetime of
    /// the catalog (PowDB has no DROP TABLE). Lets the hot path dispatch
    /// through `catalog.table_by_slot_mut(slot)` — a pure Vec index,
    /// no hash, no bucket walk, no string compare.
    table_slot: usize,
    /// Schema column index for each positional literal, in the order the
    /// caller passes them.
    col_indices: Vec<usize>,
    /// Total number of schema columns — the size `insert_values_scratch`
    /// must be resized to before filling positions via `col_indices`.
    /// Cached here so the hot loop skips `catalog.schema(table)` entirely.
    n_cols: usize,
}

/// Mission C Phase 14: precomputed fast-path for `update_by_pk` shaped
/// prepared queries. Built once in [`Engine::prepare`] and reused on every
/// `execute_prepared` call.
#[derive(Clone)]
struct UpdatePkFast {
    /// Mission C Phase 18: cached slot index into `Catalog::tables`.
    /// Resolved once at `prepare` time and stable for the lifetime of
    /// the catalog. At a 52ns total budget the swap from FxHashMap
    /// probe to a Vec index is measurable.
    table_slot: usize,
    /// Name of the key column (the `.id = ?` side). We look this up in
    /// the owning table's `indexed_cols` at execute time rather than
    /// caching a raw `&BTree` — the engine owns the catalog and can't
    /// hand out long-lived borrows anyway, and the n≤5 linear scan is
    /// a handful of ns.
    key_col: String,
    /// Byte offset of the target fixed column in the row encoding:
    /// `2 + bitmap_size + layout.fixed_offsets[target_col]`.
    field_off: usize,
    /// Byte offset of the bitmap byte containing the target column's null
    /// bit (`2 + target_col / 8`).
    bitmap_byte_off: usize,
    /// Bit mask for the target column's null bit.
    bit_mask: u8,
    /// Type of the target fixed column — drives the literal-to-bytes
    /// encoding at execute time.
    target_type: TypeId,
    /// Index into the caller's `literals` slice that holds the filter key.
    /// Always 0 today (filter literal is visited before the assignment
    /// RHS), but stored explicitly so the contract is obvious.
    key_literal_idx: usize,
    /// Index into the caller's `literals` slice that holds the new value.
    value_literal_idx: usize,
}

impl Engine {
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        // Try to reopen an existing database first; only create a fresh
        // catalog when there isn't one already on disk.
        let catalog = match Catalog::open(data_dir) {
            Ok(c) => {
                info!(data_dir = %data_dir.display(), "engine reopened existing database");
                c
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                info!(data_dir = %data_dir.display(), "engine initialized fresh database");
                Catalog::create(data_dir)?
            }
            Err(e) => return Err(e),
        };
        let view_registry = ViewRegistry::open(data_dir)
            .unwrap_or_else(|_| ViewRegistry::new(data_dir));
        Ok(Engine {
            catalog,
            plan_cache: Mutex::new(PlanCache::new(PLAN_CACHE_CAPACITY)),
            insert_values_scratch: Vec::new(),
            view_registry,
        })
    }

    /// Parse + plan + execute a PowQL query.
    ///
    /// Mission D6 — tracing collapse: the previous implementation ran 4
    /// `Instant::now()` + 3 `elapsed().as_micros()` calls + formatted an
    /// `info!` span on every query, even when tracing was disabled. On a
    /// sub-microsecond `point_lookup_indexed` call that overhead was
    /// 100-200ns — 20%+ of the whole query. We now measure time only when
    /// INFO is actually enabled via `tracing::enabled!`, and we moved the
    /// noisy `debug!(?plan)` line behind the same gate so the Debug
    /// formatter can't run unconditionally either.
    ///
    /// Mission D9 — plan cache: on the hot path we canonicalise the query
    /// text (lex + FNV-1a hash with literal values stripped), check the
    /// cache, and on a hit substitute the new literals into a clone of the
    /// cached plan. This skips re-lexing, re-parsing, and re-planning —
    /// around 3μs per call on bench workloads. On a miss we plan as before
    /// and insert the plan under its canonical hash.
    pub fn execute_powql(&mut self, input: &str) -> Result<QueryResult, String> {
        // Hot path: tracing disabled. Zero syscalls, zero formatting.
        if !tracing::enabled!(Level::INFO) {
            // D9: try the plan cache first. Canonicalisation lexes the
            // query once; on a hit we skip the parser and planner entirely.
            if let Ok((hash, literals)) = canonicalize(input) {
                let cached = self.plan_cache.lock().unwrap()
                    .get_with_substitution(hash, &literals);
                if let Some(plan) = cached {
                    let plan = lower_unindexed_range_scans(&self.catalog, &plan);
                    let result = self.execute_plan(&plan);
                    // Mission B (post-review): statement-boundary WAL
                    // group commit. Catalog::wal_log now only appends;
                    // the fsync happens here exactly once per statement.
                    // `sync_wal` is a no-op when nothing was buffered
                    // (pure reads pay zero fsync).
                    self.catalog.sync_wal().map_err(|e| e.to_string())?;
                    return result;
                }
                // Miss — plan, insert, execute.
                return match planner::plan(input) {
                    Ok(plan) => {
                        self.plan_cache.lock().unwrap().insert(hash, plan.clone());
                        let plan = lower_unindexed_range_scans(&self.catalog, &plan);
                        let result = self.execute_plan(&plan);
                        self.catalog.sync_wal().map_err(|e| e.to_string())?;
                        result
                    }
                    Err(e) => Err(e.message),
                };
            }
            // Lex error — fall through to the planner so the caller gets a
            // consistent error shape.
            return match planner::plan(input) {
                Ok(plan) => {
                    let plan = lower_unindexed_range_scans(&self.catalog, &plan);
                    let result = self.execute_plan(&plan);
                    self.catalog.sync_wal().map_err(|e| e.to_string())?;
                    result
                }
                Err(e) => Err(e.message),
            };
        }

        // Instrumented path — only taken under explicit tracing subscribers.
        let total_start = Instant::now();
        let plan_start = Instant::now();
        let plan = planner::plan(input).map_err(|e| {
            error!(query = %input, error = %e.message, "query plan failed");
            e.message
        })?;
        let plan_us = plan_start.elapsed().as_micros();

        let exec_start = Instant::now();
        let plan = lower_unindexed_range_scans(&self.catalog, &plan);
        let result = self.execute_plan(&plan);
        // Mission B (post-review): statement-boundary WAL flush.
        let _ = self.catalog.sync_wal();
        let exec_us = exec_start.elapsed().as_micros();

        let total_us = total_start.elapsed().as_micros();
        match &result {
            Ok(r) => {
                info!(
                    query = %input,
                    plan_us = plan_us,
                    exec_us = exec_us,
                    total_us = total_us,
                    rows = r.row_count(),
                    "query ok"
                );
            }
            Err(e) => {
                error!(
                    query = %input,
                    plan_us = plan_us,
                    exec_us = exec_us,
                    error = %e,
                    "query failed"
                );
            }
        }
        result
    }

    /// Plan cache stats — useful for benches and debugging.
    pub fn plan_cache_stats(&self) -> (u64, u64, usize) {
        let cache = self.plan_cache.lock().unwrap();
        (cache.hits, cache.misses, cache.len())
    }

    /// Mission infra-1: read-only entry point.
    ///
    /// Parses + plans + executes a PowQL query using only a shared borrow
    /// on the engine. Rejects any statement that would mutate state
    /// (Insert/Update/Delete/CreateTable/AlterTable/DropTable/CreateView/
    /// RefreshView/DropView) by returning [`READONLY_NEEDS_WRITE`] so the
    /// caller can escalate to the write lock.
    ///
    /// Also returns [`READONLY_NEEDS_WRITE`] if a materialized view in the
    /// query is dirty — refreshing one requires `&mut self`, so the caller
    /// must retake the write lock for the first refresh.
    ///
    /// This method is the concurrent-read fast path behind
    /// `Arc<RwLock<Engine>>`: multiple threads can call it simultaneously
    /// under a shared `.read()` lock and each will scan independently.
    pub fn execute_powql_readonly(&self, input: &str) -> Result<QueryResult, String> {
        // Parse the statement first so we can classify read vs. write
        // without touching the catalog. This is the same lex+parse cost
        // the hot path would pay anyway.
        let stmt = crate::parser::parse(input).map_err(|e| e.message)?;
        if !is_read_only_statement(&stmt) {
            return Err(READONLY_NEEDS_WRITE.to_string());
        }

        // Try the plan cache first — identical hash scheme to
        // `execute_powql` so both paths share cache state. The mutex
        // section is just a hashmap lookup + plan clone.
        if let Ok((hash, literals)) = canonicalize(input) {
            let cached = self.plan_cache.lock().unwrap()
                .get_with_substitution(hash, &literals);
            if let Some(plan) = cached {
                let plan = lower_unindexed_range_scans(&self.catalog, &plan);
                return self.execute_plan_readonly(&plan);
            }
            // Miss: plan + insert + execute. The planner is pure, so this
            // is safe from `&self`.
            let plan = crate::planner::plan_statement(stmt).map_err(|e| e.message)?;
            self.plan_cache.lock().unwrap().insert(hash, plan.clone());
            let plan = lower_unindexed_range_scans(&self.catalog, &plan);
            return self.execute_plan_readonly(&plan);
        }
        // Lex error — fall through to the planner for a consistent error
        // shape (though `parse` above would usually have caught it).
        let plan = crate::planner::plan_statement(stmt).map_err(|e| e.message)?;
        let plan = lower_unindexed_range_scans(&self.catalog, &plan);
        self.execute_plan_readonly(&plan)
    }

    /// Read-only version of [`Engine::execute_plan`]. Dispatches the
    /// read-path plan variants by calling `&self` helpers and errors with
    /// [`READONLY_NEEDS_WRITE`] on any write variant. This is the
    /// recursion target for composite read plans under the RwLock reader.
    ///
    /// The dispatch mirrors `execute_plan` for the read branches but does
    /// not carry any of the fast-paths that need `&mut self` (e.g. plan-
    /// cache mutation on inner subqueries is handled via the shared mutex
    /// in [`Engine::execute_powql_readonly`]; in-flight subquery
    /// materialisation uses [`Engine::materialize_subqueries_readonly`]).
    fn execute_plan_readonly(&self, plan: &PlanNode) -> Result<QueryResult, String> {
        match plan {
            PlanNode::SeqScan { table } => {
                // Dirty view means we'd need to refresh it — can't do that
                // under `&self`. Escalate to the write path.
                if self.view_registry.is_dirty(table) {
                    return Err(READONLY_NEEDS_WRITE.to_string());
                }
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows: Vec<Vec<Value>> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::AliasScan { table, alias } => {
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter()
                    .map(|c| format!("{alias}.{}", c.name))
                    .collect();
                let rows: Vec<Vec<Value>> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::IndexScan { table, column, key } => {
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let key_value = literal_to_value(key)?;
                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;

                if let Some(btree) = tbl.index(column) {
                    let hit = match &key_value {
                        Value::Int(k) => btree.lookup_int(*k),
                        other => btree.lookup(other),
                    };
                    let rows = match hit {
                        Some(rid) => match tbl.heap.get(rid) {
                            Some(data) => vec![decode_row(&tbl.schema, &data)],
                            None => Vec::new(),
                        },
                        None => Vec::new(),
                    };
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // No index: synthetic eq predicate + compiled scan.
                let fast = FastLayout::new(&schema);
                let synth_pred = Expr::BinaryOp(
                    Box::new(Expr::Field(column.clone())),
                    BinOp::Eq,
                    Box::new(key.clone()),
                );
                if let Some(compiled) = compile_predicate(&synth_pred, &columns, &fast, &schema) {
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |_rid, data| {
                        if compiled(data) {
                            rows.push(decode_row(&schema, data));
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Last resort: slow eq-check.
                let col_idx = schema.column_index(column)
                    .ok_or_else(|| format!("column '{column}' not found"))?;
                let rows: Vec<Vec<Value>> = tbl.scan()
                    .filter_map(|(_, row)| {
                        if row[col_idx] == key_value { Some(row) } else { None }
                    })
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::RangeScan { table, column, start, end } => {
                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;
                let columns: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();
                let schema = tbl.schema.clone();

                let start_val = match start {
                    Some((expr, _)) => Some(literal_to_value(expr)?),
                    None => None,
                };
                let end_val = match end {
                    Some((expr, _)) => Some(literal_to_value(expr)?),
                    None => None,
                };
                let start_inclusive = start.as_ref().map(|(_, inc)| *inc).unwrap_or(true);
                let end_inclusive = end.as_ref().map(|(_, inc)| *inc).unwrap_or(true);

                if let Some(btree) = tbl.index(column) {
                    let hits: Vec<(Value, RowId)> = match (&start_val, &end_val) {
                        (Some(s), Some(e)) => btree.range(s, e).collect(),
                        (Some(s), None) => btree.range_from(s),
                        (None, Some(e)) => btree.range_to(e),
                        (None, None) => {
                            // Unbounded both sides — equivalent to seq scan.
                            let rows: Vec<Vec<Value>> = tbl.scan()
                                .map(|(_, row)| row)
                                .collect();
                            return Ok(QueryResult::Rows { columns, rows });
                        }
                    };
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(hits.len());
                    for (key, rid) in hits {
                        // Filter for exclusive bounds.
                        if !start_inclusive {
                            if let Some(ref s) = start_val {
                                if &key == s { continue; }
                            }
                        }
                        if !end_inclusive {
                            if let Some(ref e) = end_val {
                                if &key == e { continue; }
                            }
                        }
                        if let Some(data) = tbl.heap.get(rid) {
                            rows.push(decode_row(&schema, &data));
                        }
                    }
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Fallback: no index — synthesize the range predicate and scan.
                let fast = FastLayout::new(&schema);
                let synth = synthesize_range_predicate(column, start, end);
                if let Some(compiled) = compile_predicate(&synth, &columns, &fast, &schema) {
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |_rid, data| {
                        if compiled(data) {
                            rows.push(decode_row(&schema, data));
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Last resort: decoded row eval.
                let col_idx = schema.column_index(column)
                    .ok_or_else(|| format!("column '{column}' not found"))?;
                let rows: Vec<Vec<Value>> = tbl.scan()
                    .filter(|(_, row)| range_matches(&row[col_idx], &start_val, start_inclusive, &end_val, end_inclusive))
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::Filter { input, predicate } => {
                // Materialise subqueries using the `&self` variant.
                // Uncorrelated subqueries are replaced with InList/Bool;
                // correlated ones are left as InSubquery/ExistsSubquery
                // for per-row materialisation below.
                let materialized;
                let predicate = if contains_subquery(predicate) {
                    materialized = self.materialize_subqueries_readonly(predicate)?;
                    &materialized
                } else {
                    predicate
                };

                // Correlated subquery path: per-row materialisation.
                if contains_subquery(predicate) {
                    let result = self.execute_plan_readonly(input)?;
                    return match result {
                        QueryResult::Rows { columns, rows } => {
                            let mut filtered = Vec::new();
                            for row in rows {
                                let row_pred = self.materialize_correlated_for_row_readonly(
                                    predicate, &row, &columns,
                                )?;
                                if eval_predicate(&row_pred, &row, &columns) {
                                    filtered.push(row);
                                }
                            }
                            Ok(QueryResult::Rows { columns, rows: filtered })
                        }
                        _ => Err("filter requires row input".into()),
                    };
                }

                // Fused Filter+SeqScan fast path.
                if let PlanNode::SeqScan { table } = input.as_ref() {
                    if self.view_registry.is_dirty(table) {
                        return Err(READONLY_NEEDS_WRITE.to_string());
                    }
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?
                        .clone();
                    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                    let fast = FastLayout::new(&schema);
                    let row_layout = RowLayout::new(&schema);
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);

                    if let Some(compiled) = compile_predicate(predicate, &columns, &fast, &schema) {
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            if compiled(data) {
                                rows.push(decode_row(&schema, data));
                            }
                        }).map_err(|e| e.to_string())?;
                    } else {
                        let pred_cols = predicate_column_indices(predicate, &columns);
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            let pred_row = decode_selective(&schema, &row_layout, data, &pred_cols);
                            if eval_predicate(predicate, &pred_row, &columns) {
                                rows.push(decode_row(&schema, data));
                            }
                        }).map_err(|e| e.to_string())?;
                    }

                    return Ok(QueryResult::Rows { columns, rows });
                }

                // General path.
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let filtered: Vec<Vec<Value>> = rows.into_iter()
                            .filter(|row| eval_predicate(predicate, row, &columns))
                            .collect();
                        Ok(QueryResult::Rows { columns, rows: filtered })
                    }
                    _ => Err("filter requires row input".into()),
                }
            }

            PlanNode::Project { input, fields } => {
                // Fast path: Project over IndexScan. Avoids full-row decode
                // by calling decode_column only for projected fields.
                if let PlanNode::IndexScan { table, column, key } = input.as_ref() {
                    let key_value = literal_to_value(key)?;
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let schema = &tbl.schema;
                    let layout = tbl.row_layout();

                    let proj_columns: Vec<String> = fields.iter().map(|f| {
                        f.alias.clone().unwrap_or_else(|| match &f.expr {
                            Expr::Field(name) => name.clone(),
                            _ => "?".into(),
                        })
                    }).collect();

                    let proj_indices: Vec<usize> = fields.iter().filter_map(|f| {
                        if let Expr::Field(name) = &f.expr {
                            schema.column_index(name)
                        } else {
                            None
                        }
                    }).collect();

                    if let Some(btree) = tbl.index(column) {
                        let lookup_result = match &key_value {
                            Value::Int(k) => btree.lookup_int(*k),
                            other => btree.lookup(other),
                        };
                        let rows = match lookup_result {
                            Some(rid) => match tbl.heap.get(rid) {
                                Some(data) => {
                                    let row: Vec<Value> = proj_indices.iter()
                                        .map(|&ci| decode_column(schema, layout, &data, ci))
                                        .collect();
                                    vec![row]
                                }
                                None => Vec::new(),
                            },
                            None => Vec::new(),
                        };
                        return Ok(QueryResult::Rows { columns: proj_columns, rows });
                    }
                }

                // Fast paths over Limit(Sort(...)) / Limit(Filter(...)) / Limit(SeqScan).
                if let PlanNode::Limit { input: inner, count: limit_expr } = input.as_ref() {
                    if let PlanNode::Sort { input: sort_input, keys } = inner.as_ref() {
                        if keys.len() == 1 {
                            let sort_field = &keys[0].field;
                            let descending = keys[0].descending;
                            let limit = match limit_expr {
                                Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                                _ => usize::MAX,
                            };
                            let (table_opt, pred_opt): (Option<&str>, Option<&Expr>) = match sort_input.as_ref() {
                                PlanNode::SeqScan { table } => (Some(table.as_str()), None),
                                PlanNode::Filter { input: fi, predicate } => {
                                    if let PlanNode::SeqScan { table } = fi.as_ref() {
                                        (Some(table.as_str()), Some(predicate))
                                    } else {
                                        (None, None)
                                    }
                                }
                                _ => (None, None),
                            };
                            if let Some(table) = table_opt {
                                if let Some(result) = self.project_filter_sort_limit_fast(
                                    table, fields, sort_field, descending, limit, pred_opt,
                                )? {
                                    return Ok(result);
                                }
                            }
                        }
                    }
                    if let PlanNode::Filter { input: fi, predicate } = inner.as_ref() {
                        if let PlanNode::SeqScan { table } = fi.as_ref() {
                            let limit = match limit_expr {
                                Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                                _ => usize::MAX,
                            };
                            if let Some(result) = self.project_filter_limit_fast(
                                table, fields, limit, Some(predicate),
                            )? {
                                return Ok(result);
                            }
                        }
                    }
                    if let PlanNode::SeqScan { table } = inner.as_ref() {
                        let limit = match limit_expr {
                            Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                            _ => usize::MAX,
                        };
                        if let Some(result) = self.project_filter_limit_fast(
                            table, fields, limit, None,
                        )? {
                            return Ok(result);
                        }
                    }
                }

                // Project(Filter(SeqScan)) without Limit.
                if let PlanNode::Filter { input: fi, predicate } = input.as_ref() {
                    if let PlanNode::SeqScan { table } = fi.as_ref() {
                        if let Some(result) = self.project_filter_limit_fast(
                            table, fields, usize::MAX, Some(predicate),
                        )? {
                            return Ok(result);
                        }
                    }
                }

                // Project(SeqScan) without Filter or Limit.
                if let PlanNode::SeqScan { table } = input.as_ref() {
                    if let Some(result) = self.project_filter_limit_fast(
                        table, fields, usize::MAX, None,
                    )? {
                        return Ok(result);
                    }
                }

                // Generic path.
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let proj_columns: Vec<String> = fields.iter().map(|f| {
                            f.alias.clone().unwrap_or_else(|| match &f.expr {
                                Expr::Field(name) => name.clone(),
                                Expr::QualifiedField { qualifier, field } => {
                                    format!("{qualifier}.{field}")
                                }
                                _ => "?".into(),
                            })
                        }).collect();
                        let proj_rows: Vec<Vec<Value>> = rows.iter().map(|row| {
                            fields.iter().map(|f| eval_expr(&f.expr, row, &columns)).collect()
                        }).collect();
                        Ok(QueryResult::Rows { columns: proj_columns, rows: proj_rows })
                    }
                    _ => Err("project requires row input".into()),
                }
            }

            PlanNode::Sort { input, keys } => {
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, mut rows } => {
                        let key_indices: Vec<(usize, bool)> = keys.iter().map(|k| {
                            let idx = columns.iter().position(|c| c == &k.field)
                                .unwrap_or_else(|| panic!("column '{}' not found", k.field));
                            (idx, k.descending)
                        }).collect();
                        rows.sort_by(|a, b| {
                            for &(col_idx, descending) in &key_indices {
                                let cmp = a[col_idx].cmp(&b[col_idx]);
                                let cmp = if descending { cmp.reverse() } else { cmp };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                        Ok(QueryResult::Rows { columns, rows })
                    }
                    _ => Err("sort requires row input".into()),
                }
            }

            PlanNode::Limit { input, count } => {
                let result = self.execute_plan_readonly(input)?;
                let n = match count {
                    Expr::Literal(Literal::Int(v)) => *v as usize,
                    _ => return Err("limit must be integer literal".into()),
                };
                match result {
                    QueryResult::Rows { columns, rows } => {
                        Ok(QueryResult::Rows { columns, rows: rows.into_iter().take(n).collect() })
                    }
                    _ => Err("limit requires row input".into()),
                }
            }

            PlanNode::Offset { input, count } => {
                let result = self.execute_plan_readonly(input)?;
                let n = match count {
                    Expr::Literal(Literal::Int(v)) => *v as usize,
                    _ => return Err("offset must be integer literal".into()),
                };
                match result {
                    QueryResult::Rows { columns, rows } => {
                        Ok(QueryResult::Rows { columns, rows: rows.into_iter().skip(n).collect() })
                    }
                    _ => Err("offset requires row input".into()),
                }
            }

            PlanNode::Aggregate { input, function, field } => {
                // Fast path: count() over SeqScan.
                if *function == AggFunc::Count {
                    if let PlanNode::SeqScan { table } = input.as_ref() {
                        let mut count: i64 = 0;
                        self.catalog.for_each_row_raw(table, |_rid, _data| {
                            count += 1;
                        }).map_err(|e| e.to_string())?;
                        return Ok(QueryResult::Scalar(Value::Int(count)));
                    }
                    if let PlanNode::Filter { input: inner, predicate } = input.as_ref() {
                        if let PlanNode::SeqScan { table } = inner.as_ref() {
                            let schema = self.catalog.schema(table)
                                .ok_or_else(|| format!("table '{table}' not found"))?
                                .clone();
                            let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                            let fast = FastLayout::new(&schema);
                            let row_layout = RowLayout::new(&schema);

                            if let Some(compiled) = compile_predicate(predicate, &columns, &fast, &schema) {
                                let mut count: i64 = 0;
                                self.catalog.for_each_row_raw(table, |_rid, data| {
                                    if compiled(data) {
                                        count += 1;
                                    }
                                }).map_err(|e| e.to_string())?;
                                return Ok(QueryResult::Scalar(Value::Int(count)));
                            }

                            let pred_cols = predicate_column_indices(predicate, &columns);
                            let mut count: i64 = 0;
                            self.catalog.for_each_row_raw(table, |_rid, data| {
                                let pred_row = decode_selective(&schema, &row_layout, data, &pred_cols);
                                if eval_predicate(predicate, &pred_row, &columns) {
                                    count += 1;
                                }
                            }).map_err(|e| e.to_string())?;
                            return Ok(QueryResult::Scalar(Value::Int(count)));
                        }
                    }
                }

                // Fast path: sum/avg/min/max over single fixed-size numeric.
                if matches!(function, AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max | AggFunc::CountDistinct) {
                    if let Some(col) = field.as_ref() {
                        let (table_opt, pred_opt): (Option<&str>, Option<&Expr>) = match input.as_ref() {
                            PlanNode::SeqScan { table } => (Some(table.as_str()), None),
                            PlanNode::Filter { input: inner, predicate } => {
                                if let PlanNode::SeqScan { table } = inner.as_ref() {
                                    (Some(table.as_str()), Some(predicate))
                                } else {
                                    (None, None)
                                }
                            }
                            _ => (None, None),
                        };
                        if let Some(table) = table_opt {
                            if let Some(result) = self.agg_single_col_fast(table, col, *function, pred_opt)? {
                                return Ok(result);
                            }
                        }
                    }
                }

                // Generic path.
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        match function {
                            AggFunc::Count => Ok(QueryResult::Scalar(Value::Int(rows.len() as i64))),
                            AggFunc::CountDistinct => {
                                let col = field.as_ref().ok_or("count distinct requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let mut seen = std::collections::HashSet::new();
                                for row in &rows {
                                    let v = &row[idx];
                                    if !v.is_empty() { seen.insert(v.clone()); }
                                }
                                Ok(QueryResult::Scalar(Value::Int(seen.len() as i64)))
                            }
                            AggFunc::Avg => {
                                let col = field.as_ref().ok_or("avg requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let sum: f64 = rows.iter().filter_map(|r| match &r[idx] {
                                    Value::Int(v) => Some(*v as f64),
                                    Value::Float(v) => Some(*v),
                                    _ => None,
                                }).sum();
                                let count = rows.len() as f64;
                                Ok(QueryResult::Scalar(Value::Float(sum / count)))
                            }
                            AggFunc::Sum => {
                                let col = field.as_ref().ok_or("sum requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let mut int_sum: i64 = 0;
                                let mut float_sum: f64 = 0.0;
                                let mut saw_float = false;
                                for r in &rows {
                                    match &r[idx] {
                                        Value::Int(v)   => int_sum += *v,
                                        Value::Float(v) => { float_sum += *v; saw_float = true; }
                                        _ => {}
                                    }
                                }
                                let result = if saw_float {
                                    Value::Float(float_sum + int_sum as f64)
                                } else {
                                    Value::Int(int_sum)
                                };
                                Ok(QueryResult::Scalar(result))
                            }
                            AggFunc::Min | AggFunc::Max => {
                                let col = field.as_ref().ok_or("min/max requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let vals: Vec<&Value> = rows.iter().map(|r| &r[idx]).collect();
                                let result = if *function == AggFunc::Min {
                                    vals.into_iter().min().cloned()
                                } else {
                                    vals.into_iter().max().cloned()
                                };
                                Ok(QueryResult::Scalar(result.unwrap_or(Value::Empty)))
                            }
                        }
                    }
                    _ => Err("aggregate requires row input".into()),
                }
            }

            PlanNode::Distinct { input } => {
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let mut seen = std::collections::HashSet::new();
                        let mut unique_rows = Vec::new();
                        for row in rows {
                            if seen.insert(row.clone()) {
                                unique_rows.push(row);
                            }
                        }
                        Ok(QueryResult::Rows { columns, rows: unique_rows })
                    }
                    other => Ok(other),
                }
            }

            PlanNode::GroupBy { input, keys, aggregates, having } => {
                let result = self.execute_plan_readonly(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let key_indices: Vec<usize> = keys.iter().map(|k| {
                            columns.iter().position(|c| c == k)
                                .ok_or_else(|| format!("group-by column '{k}' not found"))
                        }).collect::<Result<Vec<_>, _>>()?;

                        let agg_field_indices: Vec<usize> = aggregates.iter().map(|a| {
                            if a.field == "*" {
                                Ok(usize::MAX)
                            } else {
                                columns.iter().position(|c| c == &a.field)
                                    .ok_or_else(|| format!("aggregate column '{}' not found", a.field))
                            }
                        }).collect::<Result<Vec<_>, _>>()?;

                        let mut group_map: rustc_hash::FxHashMap<Vec<Value>, usize> =
                            rustc_hash::FxHashMap::default();
                        let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
                        for (ri, row) in rows.iter().enumerate() {
                            let key: Vec<Value> = key_indices.iter()
                                .map(|&i| row[i].clone()).collect();
                            match group_map.get(&key) {
                                Some(&idx) => groups[idx].1.push(ri),
                                None => {
                                    let idx = groups.len();
                                    group_map.insert(key.clone(), idx);
                                    groups.push((key, vec![ri]));
                                }
                            }
                        }

                        let mut out_columns: Vec<String> = keys.clone();
                        for agg in aggregates.iter() {
                            out_columns.push(agg.output_name.clone());
                        }

                        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
                        for (key_vals, row_indices) in &groups {
                            let mut row = key_vals.clone();
                            for (ai, agg) in aggregates.iter().enumerate() {
                                let col_idx = agg_field_indices[ai];
                                let val = compute_group_aggregate(
                                    agg.function, &rows, row_indices, col_idx,
                                );
                                row.push(val);
                            }
                            out_rows.push(row);
                        }

                        if let Some(having_expr) = having {
                            out_rows.retain(|row| eval_predicate(having_expr, row, &out_columns));
                        }

                        Ok(QueryResult::Rows { columns: out_columns, rows: out_rows })
                    }
                    _ => Err("group by requires row input".into()),
                }
            }

            PlanNode::NestedLoopJoin { left, right, on, kind } => {
                let left_result = self.execute_plan_readonly(left)?;
                let right_result = self.execute_plan_readonly(right)?;
                let (left_columns, left_rows) = match left_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("join left side must produce rows".into()),
                };
                let (right_columns, right_rows) = match right_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("join right side must produce rows".into()),
                };

                if !matches!(kind, JoinKind::Cross) {
                    if let Some(pred) = on {
                        if let Some((l_idx, r_idx)) = try_extract_equi_join_keys(
                            pred, &left_columns, &right_columns,
                        ) {
                            return Ok(hash_join(
                                left_columns, left_rows,
                                right_columns, right_rows,
                                l_idx, r_idx,
                                *kind,
                            ));
                        }
                    }
                }

                let n_left = left_columns.len();
                let n_right = right_columns.len();
                let mut columns = Vec::with_capacity(n_left + n_right);
                columns.extend(left_columns);
                columns.extend(right_columns);

                let mut rows: Vec<Vec<Value>> = Vec::with_capacity(left_rows.len());
                let mut combined: Vec<Value> = Vec::with_capacity(n_left + n_right);

                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        combined.clear();
                        combined.extend_from_slice(left_row);
                        combined.extend_from_slice(right_row);
                        let keep = match kind {
                            JoinKind::Cross => true,
                            JoinKind::Inner | JoinKind::LeftOuter => match on {
                                Some(pred) => eval_predicate(pred, &combined, &columns),
                                None => true,
                            },
                            JoinKind::RightOuter => unreachable!(
                                "planner rewrites RightOuter to LeftOuter"
                            ),
                        };
                        if keep {
                            rows.push(combined.clone());
                            matched = true;
                        }
                    }
                    if !matched && matches!(kind, JoinKind::LeftOuter) {
                        let mut row = Vec::with_capacity(n_left + n_right);
                        row.extend_from_slice(left_row);
                        row.resize(n_left + n_right, Value::Empty);
                        rows.push(row);
                    }
                }

                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::Window { input, windows } => {
                let result = self.execute_plan_readonly(input)?;
                execute_window(result, windows)
            }

            PlanNode::Union { left, right, all } => {
                let left_result = self.execute_plan_readonly(left)?;
                let right_result = self.execute_plan_readonly(right)?;
                let (left_cols, left_rows) = match left_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("UNION requires query results on left side".into()),
                };
                let (_, right_rows) = match right_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("UNION requires query results on right side".into()),
                };
                let mut combined = left_rows;
                if *all {
                    combined.extend(right_rows);
                } else {
                    let mut seen = std::collections::HashSet::new();
                    for row in &combined {
                        seen.insert(row.clone());
                    }
                    for row in right_rows {
                        if seen.insert(row.clone()) {
                            combined.push(row);
                        }
                    }
                }
                Ok(QueryResult::Rows { columns: left_cols, rows: combined })
            }

            PlanNode::Explain { input } => {
                let text = format_plan_tree(input, 0);
                Ok(QueryResult::Rows {
                    columns: vec!["plan".to_string()],
                    rows: text.lines()
                        .map(|line| vec![Value::Str(line.to_string())])
                        .collect(),
                })
            }

            // All write variants — caller must escalate to the write lock.
            PlanNode::Insert { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Upsert { .. }
            | PlanNode::CreateTable { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::DropTable { .. }
            | PlanNode::CreateView { .. }
            | PlanNode::RefreshView { .. }
            | PlanNode::DropView { .. } => Err(READONLY_NEEDS_WRITE.to_string()),
        }
    }

    /// `&self` variant of [`Engine::materialize_subqueries`]. Used by the
    /// read path so `Filter` predicates with `InSubquery`/`ExistsSubquery`
    /// children can evaluate their inner queries without taking the write
    /// lock. Inner queries that would themselves need a write (e.g. dirty
    /// view) escalate via [`READONLY_NEEDS_WRITE`] just like the top-level
    /// read path does.
    fn materialize_subqueries_readonly(&self, expr: &Expr) -> Result<Expr, String> {
        match expr {
            Expr::InSubquery { expr: inner, subquery, negated } => {
                if is_correlated_subquery(subquery, &self.catalog) {
                    // Pass through — will be materialized per-row in the
                    // Filter handler's correlated subquery path.
                    let inner = self.materialize_subqueries_readonly(inner)?;
                    return Ok(Expr::InSubquery {
                        expr: Box::new(inner),
                        subquery: subquery.clone(),
                        negated: *negated,
                    });
                }
                let inner = self.materialize_subqueries_readonly(inner)?;
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(*subquery.clone()),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan_readonly(&sub_plan)?;
                let values = match result {
                    QueryResult::Rows { rows, .. } => {
                        rows.into_iter()
                            .filter_map(|mut row| {
                                if row.is_empty() { None }
                                else { Some(value_to_expr(row.swap_remove(0))) }
                            })
                            .collect()
                    }
                    _ => Vec::new(),
                };
                Ok(Expr::InList {
                    expr: Box::new(inner),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::ExistsSubquery { subquery, negated } => {
                if is_correlated_subquery(subquery, &self.catalog) {
                    return Ok(expr.clone());
                }
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(*subquery.clone()),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan_readonly(&sub_plan)?;
                let has_rows = match result {
                    QueryResult::Rows { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                let truth = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(Literal::Bool(truth)))
            }
            Expr::BinaryOp(l, op, r) => {
                let l = self.materialize_subqueries_readonly(l)?;
                let r = self.materialize_subqueries_readonly(r)?;
                Ok(Expr::BinaryOp(Box::new(l), *op, Box::new(r)))
            }
            Expr::UnaryOp(op, inner) => {
                let inner = self.materialize_subqueries_readonly(inner)?;
                Ok(Expr::UnaryOp(*op, Box::new(inner)))
            }
            Expr::Case { whens, else_expr } => {
                let whens = whens.iter().map(|(c, r)| {
                    let c = self.materialize_subqueries_readonly(c)?;
                    let r = self.materialize_subqueries_readonly(r)?;
                    Ok((Box::new(c), Box::new(r)))
                }).collect::<Result<Vec<_>, String>>()?;
                let else_expr = match else_expr {
                    Some(e) => Some(Box::new(self.materialize_subqueries_readonly(e)?)),
                    None => None,
                };
                Ok(Expr::Case { whens, else_expr })
            }
            other => Ok(other.clone()),
        }
    }

    /// Per-row materialisation of correlated subqueries. For each row in the
    /// outer query, substitute outer column references in the subquery's
    /// filter with the current row's literal values, execute the modified
    /// subquery, and return the result as an InList or Bool literal.
    fn materialize_correlated_for_row_readonly(
        &self,
        expr: &Expr,
        outer_row: &[Value],
        outer_columns: &[String],
    ) -> Result<Expr, String> {
        match expr {
            Expr::InSubquery { expr: inner, subquery, negated } => {
                let inner = self.materialize_correlated_for_row_readonly(
                    inner, outer_row, outer_columns,
                )?;
                let mut sub = *subquery.clone();
                if let Some(ref filter) = sub.filter {
                    sub.filter = Some(substitute_outer_refs(
                        filter, &sub.source, &self.catalog, outer_row, outer_columns,
                    ));
                }
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(sub),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan_readonly(&sub_plan)?;
                let values = match result {
                    QueryResult::Rows { rows, .. } => {
                        rows.into_iter()
                            .filter_map(|mut row| {
                                if row.is_empty() { None }
                                else { Some(value_to_expr(row.swap_remove(0))) }
                            })
                            .collect()
                    }
                    _ => Vec::new(),
                };
                Ok(Expr::InList {
                    expr: Box::new(inner),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::ExistsSubquery { subquery, negated } => {
                let mut sub = *subquery.clone();
                if let Some(ref filter) = sub.filter {
                    sub.filter = Some(substitute_outer_refs(
                        filter, &sub.source, &self.catalog, outer_row, outer_columns,
                    ));
                }
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(sub),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan_readonly(&sub_plan)?;
                let has_rows = match result {
                    QueryResult::Rows { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                let truth = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(Literal::Bool(truth)))
            }
            Expr::BinaryOp(l, op, r) => {
                let l = self.materialize_correlated_for_row_readonly(l, outer_row, outer_columns)?;
                let r = self.materialize_correlated_for_row_readonly(r, outer_row, outer_columns)?;
                Ok(Expr::BinaryOp(Box::new(l), *op, Box::new(r)))
            }
            Expr::UnaryOp(op, inner) => {
                let inner = self.materialize_correlated_for_row_readonly(inner, outer_row, outer_columns)?;
                Ok(Expr::UnaryOp(*op, Box::new(inner)))
            }
            other => Ok(other.clone()),
        }
    }

    /// Parse and plan a query once, returning a [`PreparedQuery`] handle
    /// the caller can execute repeatedly with fresh literal values.
    ///
    /// Mission C Phase 5: the plan cache already short-circuits repeat
    /// queries that share a shape, but every call still pays for
    /// `canonicalize` (lex + FNV hash) and a hashmap lookup. For a tight
    /// insert loop that's ~500-800ns of pure overhead per call on top of
    /// the caller's `format!()` cost. Prepared statements skip the lex,
    /// skip the hash, skip the format, and skip the cache lookup — the
    /// caller holds the plan template directly and hands us the new
    /// literals as a slice.
    ///
    /// The plan template holds whatever literal values the original query
    /// string contained; those are overwritten on every `execute_prepared`
    /// call, same way the plan cache does on a cache hit.
    ///
    /// The returned `param_count` matches the total number of
    /// `Expr::Literal` slots reachable from the plan, in the deterministic
    /// walk order used by `canonicalize` and the cache. Callers must pass
    /// exactly that many literals to `execute_prepared`, in the same order
    /// they appear in the source text.
    pub fn prepare(&mut self, query: &str) -> Result<PreparedQuery, String> {
        let plan = planner::plan(query).map_err(|e| e.message)?;
        let param_count = crate::plan_cache::count_literal_slots(&plan);

        // Insert fast path: if the template is Insert and every assignment
        // RHS is a literal, resolve column indices once here and store
        // them. execute_prepared will skip the plan-clone + substitute
        // walk on this path.
        //
        // Mission C Phase 15: also cache `n_cols` and the target table
        // name so execute_prepared doesn't need a second HashMap lookup
        // on `self.catalog.schema(table)` just to size the scratch Vec.
        let insert_fast = match &plan {
            PlanNode::Insert { table, assignments }
                if assignments.iter().all(|a| matches!(a.value, Expr::Literal(_)))
                   && param_count == assignments.len() =>
            {
                let table_slot = self.catalog.table_slot(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;
                let schema = &self.catalog.table_by_slot(table_slot).schema;
                let n_cols = schema.columns.len();
                let indices: Result<Vec<usize>, String> = assignments.iter()
                    .map(|a| schema.column_index(&a.field)
                        .ok_or_else(|| format!("column '{}' not found", a.field)))
                    .collect();
                Some(InsertFast {
                    table_slot,
                    col_indices: indices?,
                    n_cols,
                })
            }
            _ => None,
        };

        // Mission C Phase 14: update-by-pk fast path. Match on the shape
        // planner::plan_update builds for `T filter .pk = ? update
        // { col := ? }` — `Update { input: IndexScan(pk), assignments:
        // [{col, Literal}] }` — and only if every precondition holds:
        //   * `pk` is an indexed column (so the executor would take the
        //     btree.lookup path at run time regardless)
        //   * there's exactly one assignment
        //   * the assigned column is fixed-size and *not* indexed (so we
        //     don't have to maintain any secondary index on write)
        //   * both literal slots are already `Expr::Literal` (no computed
        //     expressions)
        // If any of these fail we fall through to the standard substitute
        // + execute path.
        let update_pk_fast = Self::try_build_update_pk_fast(&self.catalog, &plan);

        Ok(PreparedQuery {
            plan_template: plan,
            param_count,
            insert_fast,
            update_pk_fast,
        })
    }

    /// Mission C Phase 14: inspect a planned tree and, if it matches the
    /// `update_by_pk` fast-path shape, return the precomputed byte-patch
    /// metadata. Returns `None` on any mismatch — the caller falls through
    /// to the substitute-and-execute path, which is always correct.
    fn try_build_update_pk_fast(
        catalog: &Catalog,
        plan: &PlanNode,
    ) -> Option<UpdatePkFast> {
        // Top level must be `Update { input: IndexScan(...), ... }`.
        let (table, input, assignments) = match plan {
            PlanNode::Update { table, input, assignments } => (table, input.as_ref(), assignments),
            _ => return None,
        };
        // Exactly one assignment — the bench hot path and the only case
        // where a single byte-patch covers the whole mutation.
        if assignments.len() != 1 {
            return None;
        }
        let assn = &assignments[0];
        // Assignment RHS must be a raw literal, not a computed expr.
        if !matches!(assn.value, Expr::Literal(_)) {
            return None;
        }
        // Input must be an IndexScan on the same table with a literal key.
        let (key_col, key_table) = match input {
            PlanNode::IndexScan { table: t, column, key: Expr::Literal(_) } => {
                (column.clone(), t.clone())
            }
            _ => return None,
        };
        if &key_table != table {
            return None;
        }

        // Look up schema + index state from the live catalog, caching
        // the slot so the execute path skips the name probe.
        let table_slot = catalog.table_slot(table)?;
        let tbl = catalog.table_by_slot(table_slot);
        let schema = &tbl.schema;

        // Key column must have an index (the btree.lookup path is what
        // makes the fast path worth building).
        if !tbl.has_index(&key_col) {
            return None;
        }

        // Target column must exist, be fixed-size, and NOT be indexed (so
        // we don't have to maintain any secondary index here).
        let target_col_idx = schema.column_index(&assn.field)?;
        let target_type = schema.columns[target_col_idx].type_id;
        if !is_fixed_size(target_type) {
            return None;
        }
        if tbl.has_indexed_col(target_col_idx) {
            return None;
        }

        // Precompute byte offsets from the cached row layout.
        let layout = tbl.row_layout();
        let fixed_off = layout.fixed_offset(target_col_idx)?;
        let bitmap_size = layout.bitmap_size();
        let field_off = 2 + bitmap_size + fixed_off;
        let bitmap_byte_off = 2 + target_col_idx / 8;
        let bit_mask = 1u8 << (target_col_idx % 8);

        // Literal walk order for `Update { IndexScan(key), [{value}] }`
        // (see `plan_cache::substitute_plan` — input first, then the
        // assignments). The filter key is literal 0, the assignment RHS
        // is literal 1.
        Some(UpdatePkFast {
            table_slot,
            key_col,
            field_off,
            bitmap_byte_off,
            bit_mask,
            target_type,
            key_literal_idx: 0,
            value_literal_idx: 1,
        })
    }

    /// Execute a [`PreparedQuery`] with the given literal values.
    ///
    /// The literals are substituted into a clone of the template plan in
    /// the same deterministic walk order that [`crate::canonicalize`]
    /// produces (filter predicate first, then projection, then assignment
    /// RHS, and so on). Substitution errors here mean the caller passed
    /// the wrong number of literals for this query shape.
    pub fn execute_prepared(
        &mut self,
        prep: &PreparedQuery,
        literals: &[Literal],
    ) -> Result<QueryResult, String> {
        if literals.len() != prep.param_count {
            return Err(format!(
                "prepared query expects {} literal(s), got {}",
                prep.param_count,
                literals.len(),
            ));
        }

        // Mission C Phase 14: update-by-pk fast path. Skip plan clone,
        // substitute walk, resolved_assignments, FastPatch, Vec<RowId>,
        // RowLayout::new — straight to btree.lookup_int + byte patch.
        // On rare mismatches (wrong literal type, index dropped after
        // prepare) the helper returns `Ok(None)` and we fall through to
        // the generic substitute-and-execute path below.
        if let Some(fast) = &prep.update_pk_fast {
            if let Some(result) = self.try_execute_update_pk_fast(fast, literals)? {
                // Mark dependent views dirty for prepared update fast path.
                if let PlanNode::Update { table, .. } = &prep.plan_template {
                    self.view_registry.mark_dependents_dirty(table);
                }
                // Mission B (post-review): statement-boundary WAL group
                // commit. The fast path appended an Update record but did
                // not flush — flush it now so the executor's contract is
                // "WAL is on disk before this returns".
                self.catalog.sync_wal().map_err(|e| e.to_string())?;
                return Ok(result);
            }
        }

        // Insert fast path: skip plan-clone + substitute walk + PlanNode::Insert
        // arm's column-index resolution. Build the Row directly from the
        // caller's literal slice using indices we resolved at prepare time.
        // Saves ~300-500ns per insert on the bench.
        //
        // Mission C Phase 13: the scratch `Vec<Value>` is reused across
        // calls — no fresh allocation per insert. We split the borrow
        // between `self.catalog` and `self.insert_values_scratch` by
        // moving the scratch into a local, filling it, passing to the
        // catalog, and putting it back.
        //
        // Mission C Phase 15: the cached `InsertFast` carries `n_cols`
        // and the table name, so the hot path makes exactly one catalog
        // HashMap lookup (`get_table_mut`) and dispatches straight into
        // `tbl.insert` — no intermediate schema lookup, no generic
        // `Catalog::insert` wrapper.
        if let Some(fast) = &prep.insert_fast {
            let mut values = std::mem::take(&mut self.insert_values_scratch);
            values.clear();
            values.resize(fast.n_cols, Value::Empty);
            for (pos, lit) in literals.iter().enumerate() {
                values[fast.col_indices[pos]] = literal_value_from(lit);
            }
            // Mission C Phase 18: direct O(1) slot index — no
            // catalog hash probe. Slot was resolved at prepare time.
            let tbl = self.catalog.table_by_slot_mut(fast.table_slot);
            let res = tbl.insert(&values).map_err(|e| e.to_string());
            // Clear strings before returning the scratch — don't keep
            // dangling allocations from the previous row alive across
            // calls. `clear()` drops the Value::Str entries.
            values.clear();
            self.insert_values_scratch = values;
            res?;
            // Mark dependent views dirty for prepared insert fast path.
            if let PlanNode::Insert { table, .. } = &prep.plan_template {
                self.view_registry.mark_dependents_dirty(table);
            }
            // Mission B (post-review): statement-boundary WAL group commit.
            self.catalog.sync_wal().map_err(|e| e.to_string())?;
            return Ok(QueryResult::Modified(1));
        }

        let mut plan = prep.plan_template.clone();
        let mut idx = 0usize;
        crate::plan_cache::substitute_plan(&mut plan, literals, &mut idx);
        debug_assert_eq!(idx, literals.len());
        let result = self.execute_plan(&plan);
        // Mission B (post-review): statement-boundary WAL group commit.
        // No-op when nothing was buffered (read-only plans).
        self.catalog.sync_wal().map_err(|e| e.to_string())?;
        result
    }

    /// Mission C Phase 14: point-update fast path for prepared
    /// `T filter .pk = ? update { col := ? }` queries. The caller has
    /// already verified this is an int-indexed pk with a fixed-size,
    /// non-indexed target column; all we do here is pluck the two
    /// literals out of the caller's slice, run one `btree.lookup_int`,
    /// and patch 1–8 bytes of the row. No plan clone, no allocations.
    ///
    /// Returns:
    ///   * `Ok(Some(result))` — fast path took the mutation.
    ///   * `Ok(None)` — can't take the fast path this call (wrong
    ///     literal type, index dropped since prepare, etc.). Caller
    ///     falls through to the generic substitute-and-execute path.
    ///   * `Err(_)` — real error (table gone, I/O, etc.).
    #[inline]
    fn try_execute_update_pk_fast(
        &mut self,
        fast: &UpdatePkFast,
        literals: &[Literal],
    ) -> Result<Option<QueryResult>, String> {
        // 1) Extract the key literal. The fast path is only built for
        //    int key columns; any other literal type means the caller
        //    is violating the prepared-query contract or the schema
        //    changed — either way, fall back.
        let key_int = match &literals[fast.key_literal_idx] {
            Literal::Int(v) => *v,
            _ => return Ok(None),
        };

        // 2) Encode the new value as little-endian bytes matching the
        //    target column's fixed encoding.
        let bytes: FixedBytes = match (fast.target_type, &literals[fast.value_literal_idx]) {
            (TypeId::Int, Literal::Int(v))         => FixedBytes::I64(v.to_le_bytes()),
            (TypeId::DateTime, Literal::Int(v))    => FixedBytes::I64(v.to_le_bytes()),
            (TypeId::Float, Literal::Float(v))     => FixedBytes::F64(v.to_le_bytes()),
            (TypeId::Bool, Literal::Bool(v))       => FixedBytes::Bool(if *v { 1 } else { 0 }),
            // Type mismatch — fall back to the generic path for a
            // consistent error shape.
            _ => return Ok(None),
        };

        // 3) Look up the table + btree, do the int lookup, patch the row
        //    in place. Phase 18: table dispatch is a direct slot index;
        //    the btree lookup is the linear scan over `indexed_cols`.
        //    Single btree.lookup_int + one `with_row_bytes_mut` call.
        //    No Vec allocations at all.
        //
        // Mission B2: route the in-place patch through the catalog's
        // WAL-logged wrapper so crash recovery sees the update. The
        // extra cost is one WAL append + fsync per query — the hot
        // loop structure is unchanged.
        let tbl = self.catalog.table_by_slot_mut(fast.table_slot);
        let Some(btree) = tbl.index(&fast.key_col) else {
            // Index dropped since prepare — bail to the generic path.
            return Ok(None);
        };
        let Some(rid) = btree.lookup_int(key_int) else {
            return Ok(Some(QueryResult::Modified(0)));
        };

        let fast_table_slot = fast.table_slot;
        let bitmap_byte_off = fast.bitmap_byte_off;
        let bit_mask = fast.bit_mask;
        let field_off = fast.field_off;
        let ok = self
            .catalog
            .update_row_bytes_logged_by_slot(fast_table_slot, rid, |row| {
                // Idempotent null-bit clear — safe even when the column was
                // already non-null (the overwhelmingly common case).
                row[bitmap_byte_off] &= !bit_mask;
                let field_bytes = bytes.as_slice();
                row[field_off..field_off + field_bytes.len()]
                    .copy_from_slice(field_bytes);
            })
            .map_err(|e| e.to_string())?;

        Ok(Some(QueryResult::Modified(if ok { 1 } else { 0 })))
    }

    /// Mission C Phase 13: moving variant of [`Engine::execute_prepared`]
    /// for the insert fast path. Takes `literals` by mutable reference
    /// so that each `Literal::String` can be consumed via `mem::take`
    /// instead of cloned into a `Value::Str`. On `insert_batch_1k` that
    /// removes three per-row heap allocations (name, status, email),
    /// bringing the workload over the line vs SQLite's amortized
    /// prepare+execute loop.
    ///
    /// The caller's `Literal::String` entries are replaced with empty
    /// strings on successful inserts — the `literals` slice is *not*
    /// left in a valid-for-reuse state except for `Int`/`Float`/`Bool`
    /// values. Non-insert templates fall through to the standard
    /// substitute-and-execute path.
    pub fn execute_prepared_take(
        &mut self,
        prep: &PreparedQuery,
        literals: &mut [Literal],
    ) -> Result<QueryResult, String> {
        if literals.len() != prep.param_count {
            return Err(format!(
                "prepared query expects {} literal(s), got {}",
                prep.param_count,
                literals.len(),
            ));
        }

        if let Some(fast) = &prep.insert_fast {
            let mut values = std::mem::take(&mut self.insert_values_scratch);
            values.clear();
            values.resize(fast.n_cols, Value::Empty);
            for (pos, lit) in literals.iter_mut().enumerate() {
                values[fast.col_indices[pos]] = literal_value_take(lit);
            }
            // Mission C Phase 18: direct O(1) slot index — see
            // `execute_prepared` for rationale. This is the hot path
            // for `insert_batch_1k`.
            let tbl = self.catalog.table_by_slot_mut(fast.table_slot);
            let res = tbl.insert(&values).map_err(|e| e.to_string());
            values.clear();
            self.insert_values_scratch = values;
            res?;
            // Mission B (post-review): statement-boundary WAL group commit.
            self.catalog.sync_wal().map_err(|e| e.to_string())?;
            return Ok(QueryResult::Modified(1));
        }

        // Non-insert templates — fall back to the standard path. We
        // can't usefully move the literals because `substitute_plan`
        // still expects an immutable slice, and the non-insert hot
        // paths are dominated by plan walks anyway.
        self.execute_prepared(prep, literals)
    }

    /// Walk an expression tree and replace every `InSubquery` node with
    /// an `InList` by executing the subquery and collecting its first
    /// column as literal values. This must be called before entering
    /// the row-by-row scan loop because the scan closure can't call back
    /// into the engine.
    fn materialize_subqueries(&mut self, expr: &Expr) -> Result<Expr, String> {
        match expr {
            Expr::InSubquery { expr: inner, subquery, negated } => {
                if is_correlated_subquery(subquery, &self.catalog) {
                    let inner = self.materialize_subqueries(inner)?;
                    return Ok(Expr::InSubquery {
                        expr: Box::new(inner),
                        subquery: subquery.clone(),
                        negated: *negated,
                    });
                }
                let inner = self.materialize_subqueries(inner)?;
                // Plan and execute the subquery.
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(*subquery.clone()),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan(&sub_plan)?;
                let values = match result {
                    QueryResult::Rows { rows, .. } => {
                        rows.into_iter()
                            .filter_map(|mut row| {
                                if row.is_empty() { None }
                                else { Some(value_to_expr(row.swap_remove(0))) }
                            })
                            .collect()
                    }
                    _ => Vec::new(),
                };
                Ok(Expr::InList {
                    expr: Box::new(inner),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::ExistsSubquery { subquery, negated } => {
                if is_correlated_subquery(subquery, &self.catalog) {
                    return Ok(expr.clone());
                }
                // Uncorrelated EXISTS: run the subquery once and collapse
                // into a Bool literal.
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(*subquery.clone()),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan(&sub_plan)?;
                let has_rows = match result {
                    QueryResult::Rows { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                let truth = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(Literal::Bool(truth)))
            }
            Expr::BinaryOp(l, op, r) => {
                let l = self.materialize_subqueries(l)?;
                let r = self.materialize_subqueries(r)?;
                Ok(Expr::BinaryOp(Box::new(l), *op, Box::new(r)))
            }
            Expr::UnaryOp(op, inner) => {
                let inner = self.materialize_subqueries(inner)?;
                Ok(Expr::UnaryOp(*op, Box::new(inner)))
            }
            Expr::Case { whens, else_expr } => {
                let whens = whens.iter().map(|(c, r)| {
                    let c = self.materialize_subqueries(c)?;
                    let r = self.materialize_subqueries(r)?;
                    Ok((Box::new(c), Box::new(r)))
                }).collect::<Result<Vec<_>, String>>()?;
                let else_expr = match else_expr {
                    Some(e) => Some(Box::new(self.materialize_subqueries(e)?)),
                    None => None,
                };
                Ok(Expr::Case { whens, else_expr })
            }
            // Leaf nodes: no subqueries possible.
            other => Ok(other.clone()),
        }
    }

    /// Write-path per-row materialisation of correlated subqueries.
    fn materialize_correlated_for_row(
        &mut self,
        expr: &Expr,
        outer_row: &[Value],
        outer_columns: &[String],
    ) -> Result<Expr, String> {
        match expr {
            Expr::InSubquery { expr: inner, subquery, negated } => {
                let inner = self.materialize_correlated_for_row(
                    inner, outer_row, outer_columns,
                )?;
                let mut sub = *subquery.clone();
                if let Some(ref filter) = sub.filter {
                    sub.filter = Some(substitute_outer_refs(
                        filter, &sub.source, &self.catalog, outer_row, outer_columns,
                    ));
                }
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(sub),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan(&sub_plan)?;
                let values = match result {
                    QueryResult::Rows { rows, .. } => {
                        rows.into_iter()
                            .filter_map(|mut row| {
                                if row.is_empty() { None }
                                else { Some(value_to_expr(row.swap_remove(0))) }
                            })
                            .collect()
                    }
                    _ => Vec::new(),
                };
                Ok(Expr::InList {
                    expr: Box::new(inner),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::ExistsSubquery { subquery, negated } => {
                let mut sub = *subquery.clone();
                if let Some(ref filter) = sub.filter {
                    sub.filter = Some(substitute_outer_refs(
                        filter, &sub.source, &self.catalog, outer_row, outer_columns,
                    ));
                }
                let sub_plan = crate::planner::plan_statement(
                    Statement::Query(sub),
                ).map_err(|e| e.message)?;
                let result = self.execute_plan(&sub_plan)?;
                let has_rows = match result {
                    QueryResult::Rows { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                let truth = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(Literal::Bool(truth)))
            }
            Expr::BinaryOp(l, op, r) => {
                let l = self.materialize_correlated_for_row(l, outer_row, outer_columns)?;
                let r = self.materialize_correlated_for_row(r, outer_row, outer_columns)?;
                Ok(Expr::BinaryOp(Box::new(l), *op, Box::new(r)))
            }
            Expr::UnaryOp(op, inner) => {
                let inner = self.materialize_correlated_for_row(inner, outer_row, outer_columns)?;
                Ok(Expr::UnaryOp(*op, Box::new(inner)))
            }
            other => Ok(other.clone()),
        }
    }

    pub fn execute_plan(&mut self, plan: &PlanNode) -> Result<QueryResult, String> {
        match plan {
            PlanNode::SeqScan { table } => {
                // Auto-refresh dirty materialized views on read.
                if self.view_registry.is_dirty(table) {
                    self.refresh_view(table)?;
                }
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows: Vec<Vec<Value>> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::Filter { input, predicate } => {
                // Materialize any IN-subqueries in the predicate before the
                // scan loop — the closure can't call back into the engine.
                // Correlated subqueries are left in place for per-row eval.
                let materialized;
                let predicate = if contains_subquery(predicate) {
                    materialized = self.materialize_subqueries(predicate)?;
                    &materialized
                } else {
                    predicate
                };

                // Correlated subquery path: per-row materialisation.
                if contains_subquery(predicate) {
                    let result = self.execute_plan(input)?;
                    return match result {
                        QueryResult::Rows { columns, rows } => {
                            let mut filtered = Vec::new();
                            for row in rows {
                                let row_pred = self.materialize_correlated_for_row(
                                    predicate, &row, &columns,
                                )?;
                                if eval_predicate(&row_pred, &row, &columns) {
                                    filtered.push(row);
                                }
                            }
                            Ok(QueryResult::Rows { columns, rows: filtered })
                        }
                        _ => Err("filter requires row input".into()),
                    };
                }

                // Fast path: fuse Filter + SeqScan into a zero-copy streaming
                // loop. Uses decode_column() to evaluate the predicate on only
                // the columns it references, avoiding heap allocations for
                // String/Bytes columns that aren't part of the filter.
                if let PlanNode::SeqScan { table } = input.as_ref() {
                    // Auto-refresh dirty materialized views.
                    if self.view_registry.is_dirty(table) {
                        self.refresh_view(table)?;
                    }
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?
                        .clone();
                    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                    let fast = FastLayout::new(&schema);
                    let row_layout = RowLayout::new(&schema);
                    // Mission F: pre-size to skip the first 4 Vec doublings
                    // (4 → 8 → 16 → 32 → 64). On a 100K-row scan with 30%
                    // selectivity that's ~4 fewer reallocations + memcpys.
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);

                    // Try compiled predicate for the filter check (handles
                    // int leaves, string-eq leaves, and And conjunctions).
                    if let Some(compiled) = compile_predicate(predicate, &columns, &fast, &schema) {
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            if compiled(data) {
                                rows.push(decode_row(&schema, data));
                            }
                        }).map_err(|e| e.to_string())?;
                    } else {
                        let pred_cols = predicate_column_indices(predicate, &columns);
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            let pred_row = decode_selective(&schema, &row_layout, data, &pred_cols);
                            if eval_predicate(predicate, &pred_row, &columns) {
                                rows.push(decode_row(&schema, data));
                            }
                        }).map_err(|e| e.to_string())?;
                    }

                    return Ok(QueryResult::Rows { columns, rows });
                }

                // General path: materialise then filter.
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let filtered: Vec<Vec<Value>> = rows.into_iter()
                            .filter(|row| eval_predicate(predicate, row, &columns))
                            .collect();
                        Ok(QueryResult::Rows { columns, rows: filtered })
                    }
                    _ => Err("filter requires row input".into()),
                }
            }

            PlanNode::Project { input, fields } => {
                // Fast path: Project over IndexScan — decode only projected
                // columns from raw bytes instead of full decode_row.
                if let PlanNode::IndexScan { table, column, key } = input.as_ref() {
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?
                        .clone();
                    let all_columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                    let key_value = literal_to_value(key)?;
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;

                    let proj_columns: Vec<String> = fields.iter().map(|f| {
                        f.alias.clone().unwrap_or_else(|| match &f.expr {
                            Expr::Field(name) => name.clone(),
                            _ => "?".into(),
                        })
                    }).collect();

                    // Determine which column indices the projection needs
                    let proj_indices: Vec<usize> = fields.iter().filter_map(|f| {
                        if let Expr::Field(name) = &f.expr {
                            all_columns.iter().position(|c| c == name)
                        } else {
                            None
                        }
                    }).collect();

                    if let Some(btree) = tbl.index(column) {
                        let layout = RowLayout::new(&schema);
                        // Mission D7: int-specialized lookup skips the
                        // `<Value as Ord>::cmp` discriminant dispatch on
                        // int-keyed indexes (the vast majority).
                        let lookup_result = match &key_value {
                            Value::Int(k) => btree.lookup_int(*k),
                            other => btree.lookup(other),
                        };
                        let rows = match lookup_result {
                            Some(rid) => {
                                match tbl.heap.get(rid) {
                                    Some(data) => {
                                        let row: Vec<Value> = proj_indices.iter()
                                            .map(|&ci| decode_column(&schema, &layout, &data, ci))
                                            .collect();
                                        vec![row]
                                    }
                                    None => Vec::new(),
                                }
                            }
                            None => Vec::new(),
                        };
                        return Ok(QueryResult::Rows { columns: proj_columns, rows });
                    }
                }

                // Fast path: Project(Limit(Sort(Filter(SeqScan)))) — bounded
                // top-N heap. Decodes only the sort key + projected columns,
                // keeps at most `limit` rows in a heap. Also handles the
                // Project(Limit(Sort(SeqScan))) variant (no filter).
                if let PlanNode::Limit { input: inner, count: limit_expr } = input.as_ref() {
                    if let PlanNode::Sort { input: sort_input, keys } = inner.as_ref() {
                        // Fast path only for single-key sorts
                        if keys.len() == 1 {
                            let sort_field = &keys[0].field;
                            let descending = keys[0].descending;
                            let limit = match limit_expr {
                                Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                                _ => usize::MAX,
                            };
                            let (table_opt, pred_opt): (Option<&str>, Option<&Expr>) = match sort_input.as_ref() {
                                PlanNode::SeqScan { table } => (Some(table.as_str()), None),
                                PlanNode::Filter { input: fi, predicate } => {
                                    if let PlanNode::SeqScan { table } = fi.as_ref() {
                                        (Some(table.as_str()), Some(predicate))
                                    } else {
                                        (None, None)
                                    }
                                }
                                _ => (None, None),
                            };
                            if let Some(table) = table_opt {
                                if let Some(result) = self.project_filter_sort_limit_fast(
                                    table, fields, sort_field, descending, limit, pred_opt,
                                )? {
                                    return Ok(result);
                                }
                            }
                        }
                    }
                    // Fast path: Project(Limit(Filter(SeqScan))) — stream,
                    // decode only projected columns, stop at limit.
                    if let PlanNode::Filter { input: fi, predicate } = inner.as_ref() {
                        if let PlanNode::SeqScan { table } = fi.as_ref() {
                            let limit = match limit_expr {
                                Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                                _ => usize::MAX,
                            };
                            if let Some(result) = self.project_filter_limit_fast(
                                table, fields, limit, Some(predicate),
                            )? {
                                return Ok(result);
                            }
                        }
                    }
                    // Fast path: Project(Limit(SeqScan)) — stream, no filter.
                    if let PlanNode::SeqScan { table } = inner.as_ref() {
                        let limit = match limit_expr {
                            Expr::Literal(Literal::Int(v)) if *v >= 0 => *v as usize,
                            _ => usize::MAX,
                        };
                        if let Some(result) = self.project_filter_limit_fast(
                            table, fields, limit, None,
                        )? {
                            return Ok(result);
                        }
                    }
                }

                // Mission D4: Project(Filter(SeqScan)) without Limit. Reuses
                // `project_filter_limit_fast` with limit = usize::MAX so the
                // hot loop decodes only projected columns and uses the
                // compiled predicate. Previously this fell through to the
                // generic Filter branch which materialised every column via
                // `decode_row` then re-projected — quadratic work.
                //
                // multi_col_and_filter (`U filter .age > 30 and .status =
                // "active" { .name, .age }`) was 6.18ms (0.7x SQLite) and
                // is the load-bearing workload for this fast path.
                if let PlanNode::Filter { input: fi, predicate } = input.as_ref() {
                    if let PlanNode::SeqScan { table } = fi.as_ref() {
                        if let Some(result) = self.project_filter_limit_fast(
                            table, fields, usize::MAX, Some(predicate),
                        )? {
                            return Ok(result);
                        }
                    }
                }

                // Mission D4: Project(SeqScan) without Filter or Limit.
                // Decode only projected columns; the previous fall-through
                // built full Vec<Value> rows then re-projected.
                if let PlanNode::SeqScan { table } = input.as_ref() {
                    if let Some(result) = self.project_filter_limit_fast(
                        table, fields, usize::MAX, None,
                    )? {
                        return Ok(result);
                    }
                }

                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let proj_columns: Vec<String> = fields.iter().map(|f| {
                            f.alias.clone().unwrap_or_else(|| match &f.expr {
                                Expr::Field(name) => name.clone(),
                                // Mission E1.2: `{ u.name }` projects as the
                                // qualified column name so callers can still
                                // disambiguate across the join output.
                                Expr::QualifiedField { qualifier, field } => {
                                    format!("{qualifier}.{field}")
                                }
                                _ => "?".into(),
                            })
                        }).collect();
                        let proj_rows: Vec<Vec<Value>> = rows.iter().map(|row| {
                            fields.iter().map(|f| eval_expr(&f.expr, row, &columns)).collect()
                        }).collect();
                        Ok(QueryResult::Rows { columns: proj_columns, rows: proj_rows })
                    }
                    _ => Err("project requires row input".into()),
                }
            }

            PlanNode::Sort { input, keys } => {
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, mut rows } => {
                        let key_indices: Vec<(usize, bool)> = keys.iter().map(|k| {
                            let idx = columns.iter().position(|c| c == &k.field)
                                .unwrap_or_else(|| panic!("column '{}' not found", k.field));
                            (idx, k.descending)
                        }).collect();
                        rows.sort_by(|a, b| {
                            for &(col_idx, descending) in &key_indices {
                                let cmp = a[col_idx].cmp(&b[col_idx]);
                                let cmp = if descending { cmp.reverse() } else { cmp };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                        Ok(QueryResult::Rows { columns, rows })
                    }
                    _ => Err("sort requires row input".into()),
                }
            }

            PlanNode::Limit { input, count } => {
                let result = self.execute_plan(input)?;
                let n = match count {
                    Expr::Literal(Literal::Int(v)) => *v as usize,
                    _ => return Err("limit must be integer literal".into()),
                };
                match result {
                    QueryResult::Rows { columns, rows } => {
                        Ok(QueryResult::Rows { columns, rows: rows.into_iter().take(n).collect() })
                    }
                    _ => Err("limit requires row input".into()),
                }
            }

            PlanNode::Offset { input, count } => {
                let result = self.execute_plan(input)?;
                let n = match count {
                    Expr::Literal(Literal::Int(v)) => *v as usize,
                    _ => return Err("offset must be integer literal".into()),
                };
                match result {
                    QueryResult::Rows { columns, rows } => {
                        Ok(QueryResult::Rows { columns, rows: rows.into_iter().skip(n).collect() })
                    }
                    _ => Err("offset requires row input".into()),
                }
            }

            PlanNode::Aggregate { input, function, field } => {
                // Fast path: count() over SeqScan — count rows without any decode
                if *function == AggFunc::Count {
                    if let PlanNode::SeqScan { table } = input.as_ref() {
                        let mut count: i64 = 0;
                        self.catalog.for_each_row_raw(table, |_rid, _data| {
                            count += 1;
                        }).map_err(|e| e.to_string())?;
                        return Ok(QueryResult::Scalar(Value::Int(count)));
                    }
                    // Fast path: count() over Filter(SeqScan) — try compiled
                    // predicate first, fall back to decode_column path.
                    if let PlanNode::Filter { input: inner, predicate } = input.as_ref() {
                        if let PlanNode::SeqScan { table } = inner.as_ref() {
                            let schema = self.catalog.schema(table)
                                .ok_or_else(|| format!("table '{table}' not found"))?
                                .clone();
                            let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                            let fast = FastLayout::new(&schema);
                            let row_layout = RowLayout::new(&schema);

                            // Try compiled predicate (zero-allocation hot path).
                            // Handles int leaves, string-eq leaves, AND conjunctions.
                            if let Some(compiled) = compile_predicate(predicate, &columns, &fast, &schema) {
                                let mut count: i64 = 0;
                                self.catalog.for_each_row_raw(table, |_rid, data| {
                                    if compiled(data) {
                                        count += 1;
                                    }
                                }).map_err(|e| e.to_string())?;
                                return Ok(QueryResult::Scalar(Value::Int(count)));
                            }

                            // Fallback: decode predicate columns
                            let pred_cols = predicate_column_indices(predicate, &columns);
                            let mut count: i64 = 0;
                            self.catalog.for_each_row_raw(table, |_rid, data| {
                                let pred_row = decode_selective(&schema, &row_layout, data, &pred_cols);
                                if eval_predicate(predicate, &pred_row, &columns) {
                                    count += 1;
                                }
                            }).map_err(|e| e.to_string())?;

                            return Ok(QueryResult::Scalar(Value::Int(count)));
                        }
                    }
                }

                // Fast path: sum/avg/min/max over a single fixed-size int
                // column with an optional compiled filter predicate. Walks
                // raw row bytes, zero allocation per row.
                if matches!(function, AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max | AggFunc::CountDistinct) {
                    if let Some(col) = field.as_ref() {
                        // Shape: Aggregate(SeqScan) or Aggregate(Filter(SeqScan))
                        let (table_opt, pred_opt): (Option<&str>, Option<&Expr>) = match input.as_ref() {
                            PlanNode::SeqScan { table } => (Some(table.as_str()), None),
                            PlanNode::Filter { input: inner, predicate } => {
                                if let PlanNode::SeqScan { table } = inner.as_ref() {
                                    (Some(table.as_str()), Some(predicate))
                                } else {
                                    (None, None)
                                }
                            }
                            _ => (None, None),
                        };
                        if let Some(table) = table_opt {
                            if let Some(result) = self.agg_single_col_fast(table, col, *function, pred_opt)? {
                                return Ok(result);
                            }
                        }
                    }
                }

                // Fast path: Project(Limit(Filter(SeqScan))) — stream, decode
                // only projected columns, stop once we hit the limit.
                // (Handled in the Project branch; this branch only fires when
                // the aggregate is the outer node.)
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        match function {
                            AggFunc::Count => Ok(QueryResult::Scalar(Value::Int(rows.len() as i64))),
                            AggFunc::CountDistinct => {
                                let col = field.as_ref().ok_or("count distinct requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let mut seen = std::collections::HashSet::new();
                                for row in &rows {
                                    let v = &row[idx];
                                    if !v.is_empty() { seen.insert(v.clone()); }
                                }
                                Ok(QueryResult::Scalar(Value::Int(seen.len() as i64)))
                            }
                            AggFunc::Avg => {
                                let col = field.as_ref().ok_or("avg requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let sum: f64 = rows.iter().filter_map(|r| match &r[idx] {
                                    Value::Int(v) => Some(*v as f64),
                                    Value::Float(v) => Some(*v),
                                    _ => None,
                                }).sum();
                                let count = rows.len() as f64;
                                Ok(QueryResult::Scalar(Value::Float(sum / count)))
                            }
                            AggFunc::Sum => {
                                let col = field.as_ref().ok_or("sum requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                // Track int and float contributions separately so
                                // Float columns (and mixed Int/Float rows) don't get
                                // silently dropped as they did in the Int-only
                                // version. If any Float is present, the whole sum
                                // promotes to Float — matching Avg's semantics.
                                let mut int_sum: i64 = 0;
                                let mut float_sum: f64 = 0.0;
                                let mut saw_float = false;
                                for r in &rows {
                                    match &r[idx] {
                                        Value::Int(v)   => int_sum += *v,
                                        Value::Float(v) => { float_sum += *v; saw_float = true; }
                                        _ => {}
                                    }
                                }
                                let result = if saw_float {
                                    Value::Float(float_sum + int_sum as f64)
                                } else {
                                    Value::Int(int_sum)
                                };
                                Ok(QueryResult::Scalar(result))
                            }
                            AggFunc::Min | AggFunc::Max => {
                                let col = field.as_ref().ok_or("min/max requires field")?;
                                let idx = columns.iter().position(|c| c == col).ok_or("col not found")?;
                                let vals: Vec<&Value> = rows.iter().map(|r| &r[idx]).collect();
                                let result = if *function == AggFunc::Min {
                                    vals.into_iter().min().cloned()
                                } else {
                                    vals.into_iter().max().cloned()
                                };
                                Ok(QueryResult::Scalar(result.unwrap_or(Value::Empty)))
                            }
                        }
                    }
                    _ => Err("aggregate requires row input".into()),
                }
            }

            PlanNode::Insert { table, assignments } => {
                // Mission C Phase 3: resolve column indices + literals under
                // a short-lived shared borrow on the catalog, then release
                // it before calling insert(). The previous code cloned the
                // full Schema (6+ String allocations on User) just to dodge
                // the borrow checker — a measurable 200-400ns on every
                // insert_single call in the bench.
                let values = {
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let mut values = vec![Value::Empty; schema.columns.len()];
                    for a in assignments {
                        let idx = schema.column_index(&a.field)
                            .ok_or_else(|| format!("column '{}' not found", a.field))?;
                        values[idx] = literal_to_value(&a.value)?;
                    }
                    values
                };
                self.catalog.insert(table, &values).map_err(|e| e.to_string())?;
                self.view_registry.mark_dependents_dirty(table);
                Ok(QueryResult::Modified(1))
            }

            PlanNode::Upsert { table, key_column, assignments, on_conflict } => {
                // Build the insert values from assignments.
                let (values, key_idx) = {
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let mut values = vec![Value::Empty; schema.columns.len()];
                    for a in assignments {
                        let idx = schema.column_index(&a.field)
                            .ok_or_else(|| format!("column '{}' not found", a.field))?;
                        values[idx] = literal_to_value(&a.value)?;
                    }
                    let key_idx = schema.column_index(key_column)
                        .ok_or_else(|| format!("key column '{key_column}' not found"))?;
                    (values, key_idx)
                };

                let key_value = values[key_idx].clone();

                // Probe the index for a conflict.
                let existing = {
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    if let Some(btree) = tbl.index(key_column) {
                        let hit = match &key_value {
                            Value::Int(k) => btree.lookup_int(*k),
                            other => btree.lookup(other),
                        };
                        hit.and_then(|rid| {
                            tbl.heap.get(rid).map(|data| {
                                (rid, decode_row(&tbl.schema, &data))
                            })
                        })
                    } else {
                        // No index — linear scan for the key.
                        let mut found = None;
                        for (rid, row) in tbl.scan() {
                            if row[key_idx] == key_value {
                                found = Some((rid, row));
                                break;
                            }
                        }
                        found
                    }
                };

                if let Some((rid, mut existing_row)) = existing {
                    // Conflict: apply on_conflict assignments (or all non-key if empty).
                    let update_assignments = if on_conflict.is_empty() {
                        assignments
                    } else {
                        on_conflict
                    };
                    let changed_cols: Vec<usize> = {
                        let schema = self.catalog.schema(table)
                            .ok_or_else(|| format!("table '{table}' not found"))?;
                        let mut indices = Vec::new();
                        for a in update_assignments {
                            let idx = schema.column_index(&a.field)
                                .ok_or_else(|| format!("column '{}' not found", a.field))?;
                            if idx != key_idx {
                                existing_row[idx] = literal_to_value(&a.value)?;
                                indices.push(idx);
                            }
                        }
                        indices
                    };
                    self.catalog.update_hinted(table, rid, &existing_row, Some(&changed_cols))
                        .map_err(|e| e.to_string())?;
                    self.view_registry.mark_dependents_dirty(table);
                    Ok(QueryResult::Modified(1))
                } else {
                    // No conflict: insert.
                    self.catalog.insert(table, &values).map_err(|e| e.to_string())?;
                    self.view_registry.mark_dependents_dirty(table);
                    Ok(QueryResult::Modified(1))
                }
            }

            PlanNode::Update { input, table, assignments } => {
                // Mission C Phase 3: resolve assignments against a borrowed
                // schema, then drop the borrow before the mutation loop.
                // Try literal-only path first; fall back to per-row expression
                // evaluation if any assignment contains a non-literal expression
                // (e.g., `age := .age + 1`).
                let (col_indices, literal_vals): (Vec<usize>, Option<Vec<Value>>) = {
                    let schema_ref = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let indices: Vec<usize> = assignments.iter()
                        .map(|a| schema_ref.column_index(&a.field)
                            .ok_or_else(|| format!("column '{}' not found", a.field)))
                        .collect::<Result<_, _>>()?;
                    let vals: Result<Vec<Value>, _> = assignments.iter()
                        .map(|a| literal_to_value(&a.value))
                        .collect();
                    (indices, vals.ok())
                };
                let resolved_assignments: Option<Vec<(usize, Value)>> = literal_vals.map(|vals|
                    col_indices.iter().copied().zip(vals).collect()
                );

                // Mission C Phase 2: the hint Table::update_hinted needs to
                // decide whether to read the old row for index diff.
                let changed_cols: Vec<usize> = col_indices.clone();

                // ── Fused scan+update for Update(Filter(SeqScan)) ────────
                // Perf sprint: instead of the two-pass collect-RIDs-then-loop
                // pattern (which pays one ensure_hot per matched row on the
                // second pass), fuse the predicate evaluation and in-place
                // byte-level mutation into a single heap walk. Same idea as
                // the fused scan_delete_matching path for deletes.
                if let Some(ref resolved_assignments) = resolved_assignments {
                    if let PlanNode::Filter { input: inner, predicate } = input.as_ref() {
                        if let PlanNode::SeqScan { table: t } = inner.as_ref() {
                            if t == table {
                                let fused_result = self.try_fused_scan_update(
                                    table, predicate, resolved_assignments, &changed_cols,
                                );
                                if let Some(result) = fused_result {
                                    return result;
                                }
                            }
                        }
                    }
                }

                // Collect matching RowIds in a single pass.
                let matching_rids = self.collect_rids_for_mutation(input, table)?;

                // ── Literal-only fast paths ─────────────────────────────
                if let Some(ref resolved_assignments) = resolved_assignments {

                // Mission C Phase 4: in-place byte-patch fast path. If every
                // assignment targets a fixed-size non-null column AND none of
                // them is indexed, we can skip decode_row / Vec<Value> /
                // encode_row_into entirely and patch the row's raw bytes on
                // the hot page.
                let fast_patch: Option<Vec<FastPatch>> = {
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let schema = &tbl.schema;
                    let all_fixed_nonnull = resolved_assignments.iter().all(|(idx, val)| {
                        is_fixed_size(schema.columns[*idx].type_id) && !val.is_empty()
                    });
                    let no_indexed = !resolved_assignments.iter()
                        .any(|(idx, _)| tbl.has_indexed_col(*idx));

                    if all_fixed_nonnull && no_indexed {
                        let layout = RowLayout::new(schema);
                        let bitmap_size = layout.bitmap_size();
                        let patches: Vec<FastPatch> = resolved_assignments.iter().map(|(idx, val)| {
                            let fixed_off = layout.fixed_offset(*idx)
                                .expect("is_fixed_size already checked");
                            let field_off = 2 + bitmap_size + fixed_off;
                            let bytes: FixedBytes = match val {
                                Value::Int(v)      => FixedBytes::I64(v.to_le_bytes()),
                                Value::Float(v)    => FixedBytes::F64(v.to_le_bytes()),
                                Value::Bool(v)     => FixedBytes::Bool(if *v { 1 } else { 0 }),
                                Value::DateTime(v) => FixedBytes::I64(v.to_le_bytes()),
                                Value::Uuid(v)     => FixedBytes::Uuid(*v),
                                _ => unreachable!("all_fixed_nonnull guard lied"),
                            };
                            FastPatch {
                                field_off,
                                bitmap_byte_off: 2 + idx / 8,
                                bit_mask: 1u8 << (idx % 8),
                                bytes,
                            }
                        }).collect();
                        Some(patches)
                    } else {
                        None
                    }
                };

                if let Some(patches) = fast_patch {
                    let mut count = 0u64;
                    for rid in matching_rids {
                        // Mission B2: WAL-log every patch so crash
                        // recovery replays the update. Same mutation
                        // closure as before — the wrapper just sandwiches
                        // it between a hot-page read and a WAL append.
                        let ok = self.catalog.update_row_bytes_logged(table, rid, |row| {
                            for p in &patches {
                                row[p.bitmap_byte_off] &= !p.bit_mask;
                                let field_bytes = p.bytes.as_slice();
                                row[p.field_off..p.field_off + field_bytes.len()]
                                    .copy_from_slice(field_bytes);
                            }
                        }).map_err(|e| e.to_string())?;
                        if ok {
                            count += 1;
                        }
                    }
                    self.view_registry.mark_dependents_dirty(table);
                    return Ok(QueryResult::Modified(count));
                }

                // Mission C Phase 10: var-column in-place shrink fast path.
                let var_fast: Option<(usize, Option<Vec<u8>>)> = {
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let schema = &tbl.schema;
                    let is_single = resolved_assignments.len() == 1;
                    let is_var_col = is_single
                        && !is_fixed_size(schema.columns[resolved_assignments[0].0].type_id);
                    let no_indexed = !resolved_assignments.iter()
                        .any(|(idx, _)| tbl.has_indexed_col(*idx));

                    if is_single && is_var_col && no_indexed {
                        let (idx, val) = &resolved_assignments[0];
                        let bytes_opt: Option<Vec<u8>> = match val {
                            Value::Str(s) => Some(s.as_bytes().to_vec()),
                            Value::Bytes(b) => Some(b.clone()),
                            Value::Empty => None,
                            _ => return Err(format!(
                                "type mismatch: cannot assign non-var value to var column '{}'",
                                schema.columns[*idx].name
                            )),
                        };
                        Some((*idx, bytes_opt))
                    } else {
                        None
                    }
                };

                if let Some((col_idx, new_bytes_opt)) = var_fast {
                    let new_bytes_ref: Option<&[u8]> = new_bytes_opt.as_deref();
                    let mut count = 0u64;
                    let mut fallback_rids: Vec<RowId> = Vec::new();
                    for rid in &matching_rids {
                        // Mission B2: logged variant so crash recovery
                        // replays the shrink. On a false return (row
                        // would have to grow), the rid is pushed to
                        // `fallback_rids` and the slower `update_hinted`
                        // path — which is already WAL-logged — picks it up.
                        let ok = self.catalog
                            .patch_var_col_logged(table, *rid, col_idx, new_bytes_ref)
                            .map_err(|e| e.to_string())?;
                        if ok {
                            count += 1;
                        } else {
                            fallback_rids.push(*rid);
                        }
                    }
                    for rid in fallback_rids {
                        let mut row = match self.catalog.get(table, rid) {
                            Some(r) => r,
                            None => continue,
                        };
                        for (idx, val) in resolved_assignments.iter() {
                            row[*idx] = val.clone();
                        }
                        self.catalog
                            .update_hinted(table, rid, &row, Some(&changed_cols))
                            .map_err(|e| e.to_string())?;
                        count += 1;
                    }
                    self.view_registry.mark_dependents_dirty(table);
                    return Ok(QueryResult::Modified(count));
                }

                // Generic literal path: decode row, apply literal values.
                let mut count = 0u64;
                for rid in matching_rids {
                    let mut row = match self.catalog.get(table, rid) {
                        Some(r) => r,
                        None => continue,
                    };
                    for (idx, val) in resolved_assignments.iter() {
                        row[*idx] = val.clone();
                    }
                    self.catalog
                        .update_hinted(table, rid, &row, Some(&changed_cols))
                        .map_err(|e| e.to_string())?;
                    count += 1;
                }
                self.view_registry.mark_dependents_dirty(table);
                return Ok(QueryResult::Modified(count));

                } // end if let Some(resolved_assignments)

                // ── Expression-based update path ────────────────────────
                // At least one assignment contains a non-literal expression
                // (e.g., `age := .age + 1`). Evaluate per-row.
                let col_names: Vec<String> = {
                    let schema_ref = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    schema_ref.columns.iter().map(|c| c.name.clone()).collect()
                };
                let mut count = 0u64;
                for rid in matching_rids {
                    let mut row = match self.catalog.get(table, rid) {
                        Some(r) => r,
                        None => continue,
                    };
                    for (i, asgn) in assignments.iter().enumerate() {
                        let val = eval_expr(&asgn.value, &row, &col_names);
                        row[col_indices[i]] = val;
                    }
                    self.catalog
                        .update_hinted(table, rid, &row, Some(&changed_cols))
                        .map_err(|e| e.to_string())?;
                    count += 1;
                }
                self.view_registry.mark_dependents_dirty(table);
                Ok(QueryResult::Modified(count))
            }

            PlanNode::Delete { input, table } => {
                // Mission C Phase 3: no schema clone — collect_rids_for_mutation
                // looks up schema internally when it needs one, and the mutation
                // loop doesn't need the schema at all.
                //
                // Mission C Phase 12: route bulk deletes through
                // `Catalog::delete_many`, which batches the btree leaf
                // compaction and shares one `ensure_hot` per row between
                // the index-key extraction and the slot delete. On
                // `delete_by_filter` (100K fixture, ~20K matches) that
                // removes ~4ms of pure `Vec::remove` memmove from the btree
                // maintenance phase.
                //
                // Mission C Phase 16: for the common `delete where ...`
                // shape (Filter(SeqScan)) — and the rarer "delete
                // everything" shape (SeqScan) — skip the two-pass
                // `collect_rids_for_mutation` + `delete_many` flow entirely.
                // The fused `scan_delete_matching` primitive walks the
                // heap exactly once, paying one `ensure_hot` per page
                // instead of per-row. That closes the last major gap on
                // the bench's `delete_by_filter` workload.
                if let PlanNode::Filter { input: inner, predicate } = input.as_ref() {
                    if let PlanNode::SeqScan { table: t } = inner.as_ref() {
                        if t == table {
                            let schema = self.catalog.schema(table)
                                .ok_or_else(|| format!("table '{table}' not found"))?;
                            let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                            let fast = FastLayout::new(schema);
                            if let Some(compiled) = compile_predicate(predicate, &columns, &fast, schema) {
                                // Mission B2: logged variant so every
                                // matched rid hits the WAL during the
                                // single-pass scan. Structure of the
                                // fused scan is unchanged — only the
                                // hook closure now also appends.
                                let count = self.catalog
                                    .scan_delete_matching_logged(table, |data| compiled(data))
                                    .map_err(|e| e.to_string())?;
                                self.view_registry.mark_dependents_dirty(table);
                                return Ok(QueryResult::Modified(count));
                            }
                        }
                    }
                } else if let PlanNode::SeqScan { table: t } = input.as_ref() {
                    if t == table {
                        // `delete from T` with no predicate — every live
                        // row matches. One pass is still the right shape.
                        // Mission B2: logged variant — see above.
                        let count = self.catalog
                            .scan_delete_matching_logged(table, |_| true)
                            .map_err(|e| e.to_string())?;
                        self.view_registry.mark_dependents_dirty(table);
                        return Ok(QueryResult::Modified(count));
                    }
                }

                let matching_rids = self.collect_rids_for_mutation(input, table)?;
                let count = self
                    .catalog
                    .delete_many(table, &matching_rids)
                    .map_err(|e| e.to_string())?;
                self.view_registry.mark_dependents_dirty(table);
                Ok(QueryResult::Modified(count))
            }

            PlanNode::AliasScan { table, alias } => {
                // Mission E1.2: scan `table` and rename every output column
                // to `alias.field`. Used as a join leaf so downstream
                // NestedLoopJoin + Filter + Project nodes can resolve
                // `Expr::QualifiedField` lookups by direct column-name match.
                //
                // We don't bother with a fused zero-copy loop here yet — the
                // whole join path is nested-loop and correctness-first
                // (Phase E1.3 will introduce hash join and at that point we
                // can revisit whether to specialise AliasScan).
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter()
                    .map(|c| format!("{alias}.{}", c.name))
                    .collect();
                let rows: Vec<Vec<Value>> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::NestedLoopJoin { left, right, on, kind } => {
                // Materialise both sides. The executor ships two strategies:
                //   1. Hash join (E1.3) — when the `on` predicate is a
                //      simple equi-predicate `left_col = right_col`, build a
                //      FxHashMap<Value, Vec<row_idx>> over the right side
                //      and probe with the left side. O(L + R) instead of
                //      O(L × R). Handles Inner and LeftOuter.
                //   2. Nested loop (E1.2) — fallback for Cross, non-equi
                //      predicates, or `on` expressions that reference
                //      either side with something more complex than a
                //      QualifiedField.
                let left_result = self.execute_plan(left)?;
                let right_result = self.execute_plan(right)?;
                let (left_columns, left_rows) = match left_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("join left side must produce rows".into()),
                };
                let (right_columns, right_rows) = match right_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("join right side must produce rows".into()),
                };

                // Hash-join fast path.
                if !matches!(kind, JoinKind::Cross) {
                    if let Some(pred) = on {
                        if let Some((l_idx, r_idx)) = try_extract_equi_join_keys(
                            pred, &left_columns, &right_columns,
                        ) {
                            return Ok(hash_join(
                                left_columns, left_rows,
                                right_columns, right_rows,
                                l_idx, r_idx,
                                *kind,
                            ));
                        }
                    }
                }

                // Nested-loop fallback.
                let n_left = left_columns.len();
                let n_right = right_columns.len();
                let mut columns = Vec::with_capacity(n_left + n_right);
                columns.extend(left_columns);
                columns.extend(right_columns);

                let mut rows: Vec<Vec<Value>> = Vec::with_capacity(left_rows.len());
                let mut combined: Vec<Value> = Vec::with_capacity(n_left + n_right);

                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        combined.clear();
                        combined.extend_from_slice(left_row);
                        combined.extend_from_slice(right_row);
                        let keep = match kind {
                            JoinKind::Cross => true,
                            JoinKind::Inner | JoinKind::LeftOuter => match on {
                                Some(pred) => eval_predicate(pred, &combined, &columns),
                                // Missing `on` for non-cross joins is a
                                // parser error, but if it slips through we
                                // treat it as "match everything".
                                None => true,
                            },
                            // RightOuter is rewritten to LeftOuter by the
                            // planner, so we never see it here.
                            JoinKind::RightOuter => unreachable!(
                                "planner rewrites RightOuter to LeftOuter"
                            ),
                        };
                        if keep {
                            rows.push(combined.clone());
                            matched = true;
                        }
                    }
                    if !matched && matches!(kind, JoinKind::LeftOuter) {
                        let mut row = Vec::with_capacity(n_left + n_right);
                        row.extend_from_slice(left_row);
                        row.resize(n_left + n_right, Value::Empty);
                        rows.push(row);
                    }
                }

                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::Distinct { input } => {
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        let mut seen = std::collections::HashSet::new();
                        let mut unique_rows = Vec::new();
                        for row in rows {
                            if seen.insert(row.clone()) {
                                unique_rows.push(row);
                            }
                        }
                        Ok(QueryResult::Rows { columns, rows: unique_rows })
                    }
                    other => Ok(other),
                }
            }

            PlanNode::GroupBy { input, keys, aggregates, having } => {
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, rows } => {
                        // Resolve key column indices.
                        let key_indices: Vec<usize> = keys.iter().map(|k| {
                            columns.iter().position(|c| c == k)
                                .ok_or_else(|| format!("group-by column '{k}' not found"))
                        }).collect::<Result<Vec<_>, _>>()?;

                        // Resolve aggregate field indices. count(*) uses
                        // sentinel usize::MAX — compute_group_aggregate
                        // treats it as "count all rows in the group".
                        let agg_field_indices: Vec<usize> = aggregates.iter().map(|a| {
                            if a.field == "*" {
                                Ok(usize::MAX)
                            } else {
                                columns.iter().position(|c| c == &a.field)
                                    .ok_or_else(|| format!("aggregate column '{}' not found", a.field))
                            }
                        }).collect::<Result<Vec<_>, _>>()?;

                        // Group rows by key values (preserving insertion order).
                        let mut group_map: rustc_hash::FxHashMap<Vec<Value>, usize> =
                            rustc_hash::FxHashMap::default();
                        let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
                        for (ri, row) in rows.iter().enumerate() {
                            let key: Vec<Value> = key_indices.iter()
                                .map(|&i| row[i].clone()).collect();
                            match group_map.get(&key) {
                                Some(&idx) => groups[idx].1.push(ri),
                                None => {
                                    let idx = groups.len();
                                    group_map.insert(key.clone(), idx);
                                    groups.push((key, vec![ri]));
                                }
                            }
                        }

                        // Build output column names: keys ++ aggregate output names.
                        let mut out_columns: Vec<String> = keys.clone();
                        for agg in aggregates.iter() {
                            out_columns.push(agg.output_name.clone());
                        }

                        // Compute aggregates per group.
                        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
                        for (key_vals, row_indices) in &groups {
                            let mut row = key_vals.clone();
                            for (ai, agg) in aggregates.iter().enumerate() {
                                let col_idx = agg_field_indices[ai];
                                let val = compute_group_aggregate(
                                    agg.function, &rows, row_indices, col_idx,
                                );
                                row.push(val);
                            }
                            out_rows.push(row);
                        }

                        // Apply HAVING filter.
                        if let Some(having_expr) = having {
                            out_rows.retain(|row| eval_predicate(having_expr, row, &out_columns));
                        }

                        Ok(QueryResult::Rows { columns: out_columns, rows: out_rows })
                    }
                    _ => Err("group by requires row input".into()),
                }
            }

            PlanNode::CreateTable { name, fields } => {
                let columns: Vec<ColumnDef> = fields.iter().enumerate().map(|(i, (fname, tname, req))| {
                    ColumnDef {
                        name: fname.clone(),
                        type_id: type_name_to_id(tname),
                        required: *req,
                        position: i as u16,
                    }
                }).collect();
                let schema = Schema { table_name: name.clone(), columns };
                self.catalog.create_table(schema).map_err(|e| e.to_string())?;
                Ok(QueryResult::Created(name.clone()))
            }

            PlanNode::AlterTable { table, action } => {
                match action {
                    AlterAction::AddColumn { name, type_name, required } => {
                        let position = self.catalog.schema(table)
                            .ok_or_else(|| format!("table '{table}' not found"))?
                            .columns.len() as u16;
                        let col = ColumnDef {
                            name: name.clone(),
                            type_id: type_name_to_id(type_name),
                            required: *required,
                            position,
                        };
                        self.catalog.alter_table_add_column(table, col)
                            .map_err(|e| e.to_string())?;
                        Ok(QueryResult::Executed {
                            message: format!("column '{name}' added to '{table}'"),
                        })
                    }
                    AlterAction::DropColumn { name } => {
                        self.catalog.alter_table_drop_column(table, name)
                            .map_err(|e| e.to_string())?;
                        Ok(QueryResult::Executed {
                            message: format!("column '{name}' dropped from '{table}'"),
                        })
                    }
                }
            }

            PlanNode::DropTable { name } => {
                self.catalog.drop_table(name).map_err(|e| e.to_string())?;
                Ok(QueryResult::Executed {
                    message: format!("table '{name}' dropped"),
                })
            }

            PlanNode::CreateView { name, query_text } => {
                self.create_view(name, query_text)?;
                Ok(QueryResult::Executed {
                    message: format!("materialized view '{name}' created"),
                })
            }

            PlanNode::RefreshView { name } => {
                self.refresh_view(name)?;
                Ok(QueryResult::Executed {
                    message: format!("materialized view '{name}' refreshed"),
                })
            }

            PlanNode::DropView { name } => {
                self.drop_view(name)?;
                Ok(QueryResult::Executed {
                    message: format!("materialized view '{name}' dropped"),
                })
            }

            PlanNode::Window { input, windows } => {
                let result = self.execute_plan(input)?;
                execute_window(result, windows)
            }

            PlanNode::Union { left, right, all } => {
                let left_result = self.execute_plan(left)?;
                let right_result = self.execute_plan(right)?;
                let (left_cols, left_rows) = match left_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("UNION requires query results on left side".into()),
                };
                let (_, right_rows) = match right_result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("UNION requires query results on right side".into()),
                };
                let mut combined = left_rows;
                if *all {
                    // UNION ALL — just concatenate.
                    combined.extend(right_rows);
                } else {
                    // UNION — deduplicate using the same HashSet approach
                    // as DISTINCT. Value already implements Hash + Eq.
                    let mut seen = std::collections::HashSet::new();
                    for row in &combined {
                        seen.insert(row.clone());
                    }
                    for row in right_rows {
                        if seen.insert(row.clone()) {
                            combined.push(row);
                        }
                    }
                }
                Ok(QueryResult::Rows { columns: left_cols, rows: combined })
            }

            PlanNode::Explain { input } => {
                let text = format_plan_tree(input, 0);
                Ok(QueryResult::Rows {
                    columns: vec!["plan".to_string()],
                    rows: text.lines()
                        .map(|line| vec![Value::Str(line.to_string())])
                        .collect(),
                })
            }

            PlanNode::IndexScan { table, column, key } => {
                let key_value = literal_to_value(key)?;
                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;
                let columns: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();

                // Fast path: the table has a B-tree on this column. A single
                // point lookup returns 0 or 1 rows — this is the whole reason
                // the planner bothers emitting IndexScan.
                //
                // Mission D7: use `lookup_int` on int-keyed indexes to skip
                // the Value enum dispatch in the inner binary search. The
                // generic `tbl.index_lookup` helper can't do this without
                // lying about the key type, so we inline the index+heap
                // touch here.
                if let Some(btree) = tbl.index(column) {
                    let hit = match &key_value {
                        Value::Int(k) => btree.lookup_int(*k),
                        other => btree.lookup(other),
                    };
                    let rows = match hit {
                        Some(rid) => match tbl.heap.get(rid) {
                            Some(data) => vec![decode_row(&tbl.schema, &data)],
                            None => Vec::new(),
                        },
                        None => Vec::new(),
                    };
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Fallback: no index on this column. The planner emits IndexScan
                // eagerly (it has no visibility into which columns are indexed
                // at plan time), so here we must behave like SeqScan+Filter on
                // `.col = literal`: return *all* matching rows, not just the
                // first one. A non-indexed column isn't necessarily unique.
                // We compile the eq predicate once and stream without any
                // per-row decode for non-matching rows.
                let schema = &tbl.schema;
                let fast = FastLayout::new(schema);
                let synth_pred = Expr::BinaryOp(
                    Box::new(Expr::Field(column.clone())),
                    BinOp::Eq,
                    Box::new(key.clone()),
                );
                if let Some(compiled) = compile_predicate(&synth_pred, &columns, &fast, schema) {
                    // Mission F: skip the first 4 Vec doublings.
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |_rid, data| {
                        if compiled(data) {
                            rows.push(decode_row(schema, data));
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Last resort: slow eq-check on materialised rows.
                let col_idx = schema.column_index(column)
                    .ok_or_else(|| format!("column '{column}' not found"))?;
                let rows: Vec<Vec<Value>> = tbl.scan()
                    .filter_map(|(_, row)| {
                        if row[col_idx] == key_value { Some(row) } else { None }
                    })
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }

            PlanNode::RangeScan { table, column, start, end } => {
                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;
                let columns: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();
                let schema = &tbl.schema;

                let start_val = match start {
                    Some((expr, _)) => Some(literal_to_value(expr)?),
                    None => None,
                };
                let end_val = match end {
                    Some((expr, _)) => Some(literal_to_value(expr)?),
                    None => None,
                };
                let start_inclusive = start.as_ref().map(|(_, inc)| *inc).unwrap_or(true);
                let end_inclusive = end.as_ref().map(|(_, inc)| *inc).unwrap_or(true);

                if let Some(btree) = tbl.index(column) {
                    let hits: Vec<(Value, RowId)> = match (&start_val, &end_val) {
                        (Some(s), Some(e)) => btree.range(s, e).collect(),
                        (Some(s), None) => btree.range_from(s),
                        (None, Some(e)) => btree.range_to(e),
                        (None, None) => {
                            let rows: Vec<Vec<Value>> = tbl.scan()
                                .map(|(_, row)| row)
                                .collect();
                            return Ok(QueryResult::Rows { columns, rows });
                        }
                    };
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(hits.len());
                    for (key, rid) in hits {
                        if !start_inclusive {
                            if let Some(ref s) = start_val {
                                if &key == s { continue; }
                            }
                        }
                        if !end_inclusive {
                            if let Some(ref e) = end_val {
                                if &key == e { continue; }
                            }
                        }
                        if let Some(data) = tbl.heap.get(rid) {
                            rows.push(decode_row(schema, &data));
                        }
                    }
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Fallback: no index — synthesize range predicate and scan.
                let fast = FastLayout::new(schema);
                let synth = synthesize_range_predicate(column, start, end);
                if let Some(compiled) = compile_predicate(&synth, &columns, &fast, schema) {
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |_rid, data| {
                        if compiled(data) {
                            rows.push(decode_row(schema, data));
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(QueryResult::Rows { columns, rows });
                }

                let col_idx = schema.column_index(column)
                    .ok_or_else(|| format!("column '{column}' not found"))?;
                let rows: Vec<Vec<Value>> = tbl.scan()
                    .filter(|(_, row)| range_matches(&row[col_idx], &start_val, start_inclusive, &end_val, end_inclusive))
                    .map(|(_, row)| row)
                    .collect();
                Ok(QueryResult::Rows { columns, rows })
            }
        }
    }

    // ─── Materialized view operations ──────────────────────────────────────

    /// Create a materialized view: execute the source query, store results
    /// in a new backing table, and register the view.
    fn create_view(&mut self, name: &str, query_text: &str) -> Result<(), String> {
        if self.view_registry.is_view(name) {
            return Err(format!("materialized view '{name}' already exists"));
        }
        // Execute the source query to get the result set.
        let result = self.execute_powql(query_text)?;
        let (columns, rows) = match result {
            QueryResult::Rows { columns, rows } => (columns, rows),
            _ => return Err("view source query must be a SELECT".into()),
        };
        // Derive a schema for the backing table from the query result columns.
        let schema = self.derive_view_schema(name, &columns, &rows);
        // Create the backing table and insert the result rows.
        self.catalog.create_table(schema).map_err(|e| e.to_string())?;
        for row in &rows {
            self.catalog.insert(name, row).map_err(|e| e.to_string())?;
        }
        // Determine which base tables this view depends on by parsing the query.
        let depends_on = self.extract_view_deps(query_text);
        self.view_registry.register(ViewDef {
            name: name.to_string(),
            query: query_text.to_string(),
            depends_on,
            dirty: false,
        }).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Refresh a materialized view: re-execute its source query and replace
    /// the backing table's contents.
    fn refresh_view(&mut self, name: &str) -> Result<(), String> {
        let def = self.view_registry.get(name)
            .ok_or_else(|| format!("materialized view '{name}' not found"))?;
        let query_text = def.query.clone();
        // Execute the source query.
        let result = self.execute_powql(&query_text)?;
        let (_columns, rows) = match result {
            QueryResult::Rows { columns, rows } => (columns, rows),
            _ => return Err("view source query must be a SELECT".into()),
        };
        // Clear old data and insert fresh results. Mission B2: logged
        // variant — view refreshes are a mutation and crash recovery
        // must see them.
        self.catalog
            .scan_delete_matching_logged(name, |_| true)
            .map_err(|e| e.to_string())?;
        for row in &rows {
            self.catalog.insert(name, row).map_err(|e| e.to_string())?;
        }
        self.view_registry.mark_clean(name);
        Ok(())
    }

    /// Drop a materialized view: remove the backing table and unregister.
    fn drop_view(&mut self, name: &str) -> Result<(), String> {
        if !self.view_registry.is_view(name) {
            return Err(format!("materialized view '{name}' not found"));
        }
        self.view_registry.unregister(name).map_err(|e| e.to_string())?;
        self.catalog.drop_table(name).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Derive a storage `Schema` for a view's backing table from query
    /// result column names and the first row's types.
    fn derive_view_schema(&self, name: &str, columns: &[String], rows: &[Vec<Value>]) -> Schema {
        use powdb_storage::types::{ColumnDef, TypeId};
        let cols: Vec<ColumnDef> = columns.iter().enumerate().map(|(i, col_name)| {
            let type_id = rows.first()
                .and_then(|row| row.get(i))
                .map(|v| v.type_id())
                .unwrap_or(TypeId::Str);
            ColumnDef {
                name: col_name.clone(),
                type_id,
                required: false,
                position: i as u16,
            }
        }).collect();
        Schema {
            table_name: name.to_string(),
            columns: cols,
        }
    }

    /// Extract base table dependencies from a view's source query by
    /// parsing it and collecting the source table name.
    fn extract_view_deps(&self, query_text: &str) -> Vec<String> {
        use crate::parser::parse;
        match parse(query_text) {
            Ok(Statement::Query(q)) => {
                let mut deps = vec![q.source.clone()];
                for j in &q.joins {
                    deps.push(j.source.clone());
                }
                deps
            }
            _ => Vec::new(),
        }
    }

    // ─── Specialized fast paths ─────────────────────────────────────────────
    //
    // These methods are helpers for the `execute_plan` match arms above.
    // Each returns `Ok(Some(result))` when the fast path fires, `Ok(None)`
    // when the shape isn't supported (caller falls back to generic code).

    /// Aggregate sum/avg/min/max over a single fixed-size i64 column, with
    /// an optional compiled filter predicate. Walks raw row bytes — zero
    /// per-row allocation. Uses i128 accumulator for sum/avg overflow safety.
    fn agg_single_col_fast(
        &self,
        table: &str,
        col: &str,
        function: AggFunc,
        predicate: Option<&Expr>,
    ) -> Result<Option<QueryResult>, String> {
        let schema = self.catalog.schema(table)
            .ok_or_else(|| format!("table '{table}' not found"))?
            .clone();
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let col_idx = match schema.column_index(col) {
            Some(i) => i,
            None => return Ok(None),
        };
        // Only fast-path fixed-size numeric columns (Int/Float) for
        // sum/avg/min/max/count. Mission D10: Float parity — prior version
        // bailed on Float columns, forcing them through the generic row-
        // decoding path that allocated a Vec<Value> per row and dispatched
        // on Value::cmp for every compare. f64 decode is structurally the
        // same as i64 (load 8 bytes, cast), so the fast path handles both.
        let col_type = schema.columns[col_idx].type_id;
        if col_type != TypeId::Int && col_type != TypeId::Float {
            return Ok(None);
        }

        let fast = FastLayout::new(&schema);
        // Mission C Phase 20b: inline the numeric-column reader instead of
        // building a `Box<dyn Fn>`. Eliminates 100K vtable dispatches per
        // 100K-row agg scan — every reader call folds directly into the
        // hot loop below.
        let byte_offset = match fast.fixed_offsets[col_idx] {
            Some(o) => o,
            None => return Ok(None),
        };
        let bitmap_byte = col_idx / 8;
        let bitmap_bit = (col_idx % 8) as u32;
        let data_offset = 2 + fast.bitmap_size + byte_offset;

        // Optional compiled filter.
        let compiled_pred: Option<CompiledPredicate> = match predicate {
            Some(pred) => match compile_predicate(pred, &columns, &fast, &schema) {
                Some(c) => Some(c),
                None => return Ok(None), // let generic path handle it
            },
            None => None,
        };

        // Mission C Phase 20b: specialize the inner loop per aggregate
        // function. The previous version ran a `match function { ... }`
        // *inside* the closure, which kept LLVM from producing optimal
        // scalar code for each variant (agg_max regressed ~23% vs the
        // baseline Box<dyn Fn> version even though per-row vtable cost
        // should have been strictly lower). Pushing the match out of the
        // hot loop lets each specialized body fold cleanly into
        // `for_each_row_raw` and removes a captured `AggFunc` + match
        // dispatch per row.
        //
        // Mission D10: same specialisation applies to the Float branch.
        // For Min/Max we use `f64::total_cmp` so the result matches
        // `Value::Ord` — this is the same ordering ORDER BY and the
        // top-N sort fast path use, keeping semantics consistent across
        // read paths (NaN compares as greatest, -0.0 < +0.0 for
        // deterministic tie-breaking).
        //
        // Mission D11 Phase 1: each inner loop now splits on presence of
        // a predicate (`if let Some(pred) = &compiled_pred`) so the hot
        // body never re-tests `Option` per row, and reads column bytes
        // via `read_i64_unchecked` / `read_f64_unchecked` helpers that
        // drop two bounds checks per row (null bitmap byte + value
        // slice). Safety is carried by the `FastLayout` invariant that
        // `data_offset + 8 <= row_len` for any fixed-size column; see
        // the helper doc comments. Hot loops are macro-generated so the
        // with-pred / no-pred split can't drift between variants.
        let result = match col_type {
            TypeId::Int => match function {
                AggFunc::Sum | AggFunc::Avg => {
                    let mut sum_i128: i128 = 0;
                    let mut count: i64 = 0;
                    agg_int_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: i64| {
                            count += 1;
                            sum_i128 += v as i128;
                        }
                    );
                    if matches!(function, AggFunc::Sum) {
                        let clamped = sum_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
                        QueryResult::Scalar(Value::Int(clamped))
                    } else if count == 0 {
                        QueryResult::Scalar(Value::Empty)
                    } else {
                        let avg = (sum_i128 as f64) / (count as f64);
                        QueryResult::Scalar(Value::Float(avg))
                    }
                }
                AggFunc::Min => {
                    let mut min_v: Option<i64> = None;
                    agg_int_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: i64| {
                            min_v = Some(match min_v { Some(m) => m.min(v), None => v });
                        }
                    );
                    QueryResult::Scalar(min_v.map(Value::Int).unwrap_or(Value::Empty))
                }
                AggFunc::Max => {
                    let mut max_v: Option<i64> = None;
                    agg_int_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: i64| {
                            max_v = Some(match max_v { Some(m) => m.max(v), None => v });
                        }
                    );
                    QueryResult::Scalar(max_v.map(Value::Int).unwrap_or(Value::Empty))
                }
                AggFunc::Count => {
                    let mut count: i64 = 0;
                    agg_int_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |_v: i64| { count += 1; }
                    );
                    QueryResult::Scalar(Value::Int(count))
                }
                AggFunc::CountDistinct => {
                    let mut seen = rustc_hash::FxHashSet::default();
                    agg_int_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: i64| { seen.insert(v); }
                    );
                    QueryResult::Scalar(Value::Int(seen.len() as i64))
                }
            }
            TypeId::Float => match function {
                AggFunc::Sum => {
                    // Use a single f64 accumulator. Naive summation is
                    // sufficient for MVP parity; if precision becomes an
                    // issue on long scans we can upgrade to Kahan–Neumaier
                    // compensated sum (~2x scalar cost, zero error growth).
                    let mut sum: f64 = 0.0;
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: f64| { sum += v; }
                    );
                    QueryResult::Scalar(Value::Float(sum))
                }
                AggFunc::Avg => {
                    let mut sum: f64 = 0.0;
                    let mut count: i64 = 0;
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: f64| {
                            sum += v;
                            count += 1;
                        }
                    );
                    if count == 0 {
                        QueryResult::Scalar(Value::Empty)
                    } else {
                        QueryResult::Scalar(Value::Float(sum / count as f64))
                    }
                }
                AggFunc::Min => {
                    // `total_cmp` for deterministic NaN handling (matches
                    // Value::Ord). NaN compares greatest, so Min will
                    // correctly ignore it in favour of any finite value.
                    let mut min_v: Option<f64> = None;
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: f64| {
                            min_v = Some(match min_v {
                                Some(m) => if v.total_cmp(&m).is_lt() { v } else { m },
                                None => v,
                            });
                        }
                    );
                    QueryResult::Scalar(min_v.map(Value::Float).unwrap_or(Value::Empty))
                }
                AggFunc::Max => {
                    let mut max_v: Option<f64> = None;
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: f64| {
                            max_v = Some(match max_v {
                                Some(m) => if v.total_cmp(&m).is_gt() { v } else { m },
                                None => v,
                            });
                        }
                    );
                    QueryResult::Scalar(max_v.map(Value::Float).unwrap_or(Value::Empty))
                }
                AggFunc::Count => {
                    let mut count: i64 = 0;
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |_v: f64| { count += 1; }
                    );
                    QueryResult::Scalar(Value::Int(count))
                }
                AggFunc::CountDistinct => {
                    // Hash on `f64::to_bits` — matches `Value::Hash`, so
                    // distinct NaN bit patterns count as distinct and
                    // -0.0/+0.0 count as distinct. Consistent with how
                    // Float values are hashed in every other DISTINCT /
                    // GROUP BY path.
                    let mut seen = rustc_hash::FxHashSet::default();
                    agg_float_loop!(
                        self, table, compiled_pred, bitmap_byte, bitmap_bit, data_offset,
                        |v: f64| { seen.insert(v.to_bits()); }
                    );
                    QueryResult::Scalar(Value::Int(seen.len() as i64))
                }
            }
            _ => unreachable!("type guard above restricts to Int/Float"),
        };
        Ok(Some(result))
    }

    /// `Project(Limit(Filter(SeqScan)))` and `Project(Limit(SeqScan))`.
    /// Streams rows, decodes only projected columns, stops at the limit.
    fn project_filter_limit_fast(
        &self,
        table: &str,
        fields: &[ProjectField],
        limit: usize,
        predicate: Option<&Expr>,
    ) -> Result<Option<QueryResult>, String> {
        let schema = self.catalog.schema(table)
            .ok_or_else(|| format!("table '{table}' not found"))?
            .clone();
        let all_columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        // Each projection field must be a simple `.field` reference for this
        // fast path. Aliased or computed fields fall through.
        let mut proj_indices: Vec<usize> = Vec::with_capacity(fields.len());
        let mut proj_columns: Vec<String> = Vec::with_capacity(fields.len());
        for f in fields {
            let name = match &f.expr {
                Expr::Field(n) => n.clone(),
                _ => return Ok(None),
            };
            let idx = match all_columns.iter().position(|c| c == &name) {
                Some(i) => i,
                None => return Ok(None),
            };
            proj_indices.push(idx);
            proj_columns.push(f.alias.clone().unwrap_or(name));
        }

        let fast = FastLayout::new(&schema);
        let row_layout = RowLayout::new(&schema);

        let compiled_pred: Option<CompiledPredicate> = match predicate {
            Some(pred) => match compile_predicate(pred, &all_columns, &fast, &schema) {
                Some(c) => Some(c),
                None => return Ok(None),
            },
            None => None,
        };

        let mut out: Vec<Vec<Value>> = Vec::with_capacity(limit.min(1024));
        // Mission D2: use try_for_each_row_raw to actually stop iterating
        // once the limit is reached. The previous `done` flag only short-
        // circuited the closure body, so a `limit 100` over 100K rows still
        // walked all 100K slots — burning ~30x SQLite on scan_filter_project_top100.
        self.catalog.try_for_each_row_raw(table, |_rid, data| {
            use std::ops::ControlFlow;
            if let Some(ref pred) = compiled_pred {
                if !pred(data) { return ControlFlow::Continue(()); }
            }
            let row: Vec<Value> = proj_indices.iter()
                .map(|&ci| decode_column(&schema, &row_layout, data, ci))
                .collect();
            out.push(row);
            if out.len() >= limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }).map_err(|e| e.to_string())?;

        Ok(Some(QueryResult::Rows { columns: proj_columns, rows: out }))
    }

    /// `Project(Limit(Sort(Filter(SeqScan))))` and `Project(Limit(Sort(SeqScan)))`.
    /// Bounded top-N heap over the sort key. Only the sort key needs to be
    /// read per row; projected columns are decoded only for the final
    /// winning rows when the heap drains.
    fn project_filter_sort_limit_fast(
        &self,
        table: &str,
        fields: &[ProjectField],
        sort_field: &str,
        descending: bool,
        limit: usize,
        predicate: Option<&Expr>,
    ) -> Result<Option<QueryResult>, String> {
        if limit == 0 {
            // Degenerate case — empty result. Let the generic path handle it
            // for proper column naming.
            return Ok(None);
        }
        let schema = self.catalog.schema(table)
            .ok_or_else(|| format!("table '{table}' not found"))?
            .clone();
        let all_columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        // Sort key must be a fixed-size numeric column (Int or Float).
        // Mission D10: extended from Int-only. Float sort keys use a
        // sortable-u64 transform (see `f64_to_sortable_u64`) so the heap
        // path stays keyed on `u64` and the whole branch shape is
        // identical to the Int case — no new heap types, no `total_cmp`
        // closures in the hot loop.
        let sort_idx = match schema.column_index(sort_field) {
            Some(i) => i,
            None => return Ok(None),
        };
        let sort_col_type = schema.columns[sort_idx].type_id;
        if sort_col_type != TypeId::Int && sort_col_type != TypeId::Float {
            return Ok(None);
        }

        // Each projection field must be a simple `.field`.
        let mut proj_indices: Vec<usize> = Vec::with_capacity(fields.len());
        let mut proj_columns: Vec<String> = Vec::with_capacity(fields.len());
        for f in fields {
            let name = match &f.expr {
                Expr::Field(n) => n.clone(),
                _ => return Ok(None),
            };
            let idx = match all_columns.iter().position(|c| c == &name) {
                Some(i) => i,
                None => return Ok(None),
            };
            proj_indices.push(idx);
            proj_columns.push(f.alias.clone().unwrap_or(name));
        }

        let fast = FastLayout::new(&schema);
        let row_layout = RowLayout::new(&schema);
        // Mission C Phase 20b: inline numeric-column reader (no Box<dyn Fn>).
        let sort_byte_offset = match fast.fixed_offsets[sort_idx] {
            Some(o) => o,
            None => return Ok(None),
        };
        let sort_bitmap_byte = sort_idx / 8;
        let sort_bitmap_bit = (sort_idx % 8) as u32;
        let sort_data_offset = 2 + fast.bitmap_size + sort_byte_offset;

        let compiled_pred: Option<CompiledPredicate> = match predicate {
            Some(pred) => match compile_predicate(pred, &all_columns, &fast, &schema) {
                Some(c) => Some(c),
                None => return Ok(None),
            },
            None => None,
        };

        // Bounded top-N heap. For `order .x desc limit N`, we want the N
        // largest values — use a min-heap so the smallest is at the top and
        // can be popped when a better candidate arrives. For ascending, use
        // a max-heap. We tie-break with a monotonic `seq` counter so the
        // result is deterministic and stable.
        //
        // To keep this simple we maintain two typed heaps and pick by
        // direction.
        let drained: Vec<Vec<u8>> = match sort_col_type {
            TypeId::Int => {
                let mut seq: u64 = 0;
                let mut heap_desc: BinaryHeap<Reverse<(i64, u64, Vec<u8>)>> = BinaryHeap::with_capacity(limit);
                let mut heap_asc: BinaryHeap<(i64, u64, Vec<u8>)> = BinaryHeap::with_capacity(limit);

                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    // Inlined int-column reader: null check + i64 decode.
                    let is_null = (data[2 + sort_bitmap_byte] >> sort_bitmap_bit) & 1 == 1;
                    if is_null {
                        return;
                    }
                    let key = i64::from_le_bytes(
                        data[sort_data_offset..sort_data_offset + 8].try_into().unwrap(),
                    );
                    let id = seq;
                    seq += 1;

                    if descending {
                        if heap_desc.len() < limit {
                            heap_desc.push(Reverse((key, id, data.to_vec())));
                        } else if let Some(Reverse((top_key, _, _))) = heap_desc.peek() {
                            if key > *top_key {
                                heap_desc.pop();
                                heap_desc.push(Reverse((key, id, data.to_vec())));
                            }
                        }
                    } else if heap_asc.len() < limit {
                        heap_asc.push((key, id, data.to_vec()));
                    } else if let Some((top_key, _, _)) = heap_asc.peek() {
                        if key < *top_key {
                            heap_asc.pop();
                            heap_asc.push((key, id, data.to_vec()));
                        }
                    }
                }).map_err(|e| e.to_string())?;

                let mut drained: Vec<(i64, u64, Vec<u8>)> = if descending {
                    heap_desc.into_iter().map(|Reverse(t)| t).collect()
                } else {
                    heap_asc.into_iter().collect()
                };
                if descending {
                    drained.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                } else {
                    drained.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                }
                drained.into_iter().map(|(_, _, d)| d).collect()
            }
            TypeId::Float => {
                // Novel angle: rather than introducing a `TotalF64` newtype
                // with `Ord via total_cmp`, transform the f64 bit pattern
                // into a sortable `u64` so `BinaryHeap<u64>` orders exactly
                // like `f64::total_cmp` would. Classic trick: flip the sign
                // bit on positives, flip all bits on negatives. Result:
                // - NaN (sign=0) stays greatest, matching total_cmp
                // - -0.0 sorts before +0.0, matching total_cmp
                // - Hot loop is branch-cheap (one compare + one xor)
                let mut seq: u64 = 0;
                let mut heap_desc: BinaryHeap<Reverse<(u64, u64, Vec<u8>)>> = BinaryHeap::with_capacity(limit);
                let mut heap_asc: BinaryHeap<(u64, u64, Vec<u8>)> = BinaryHeap::with_capacity(limit);

                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + sort_bitmap_byte] >> sort_bitmap_bit) & 1 == 1;
                    if is_null {
                        return;
                    }
                    let bits = u64::from_le_bytes(
                        data[sort_data_offset..sort_data_offset + 8].try_into().unwrap(),
                    );
                    let key = f64_bits_to_sortable_u64(bits);
                    let id = seq;
                    seq += 1;

                    if descending {
                        if heap_desc.len() < limit {
                            heap_desc.push(Reverse((key, id, data.to_vec())));
                        } else if let Some(Reverse((top_key, _, _))) = heap_desc.peek() {
                            if key > *top_key {
                                heap_desc.pop();
                                heap_desc.push(Reverse((key, id, data.to_vec())));
                            }
                        }
                    } else if heap_asc.len() < limit {
                        heap_asc.push((key, id, data.to_vec()));
                    } else if let Some((top_key, _, _)) = heap_asc.peek() {
                        if key < *top_key {
                            heap_asc.pop();
                            heap_asc.push((key, id, data.to_vec()));
                        }
                    }
                }).map_err(|e| e.to_string())?;

                let mut drained: Vec<(u64, u64, Vec<u8>)> = if descending {
                    heap_desc.into_iter().map(|Reverse(t)| t).collect()
                } else {
                    heap_asc.into_iter().collect()
                };
                if descending {
                    drained.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                } else {
                    drained.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                }
                drained.into_iter().map(|(_, _, d)| d).collect()
            }
            _ => unreachable!("type guard above restricts to Int/Float"),
        };

        let rows: Vec<Vec<Value>> = drained.into_iter().map(|data| {
            proj_indices.iter()
                .map(|&ci| decode_column(&schema, &row_layout, &data, ci))
                .collect()
        }).collect();

        Ok(Some(QueryResult::Rows { columns: proj_columns, rows }))
    }

    /// Gather the RowIds that a mutation should operate on, without
    /// materialising the full row set. Handles the shapes the planner emits
    /// for update/delete: SeqScan, IndexScan, and Filter(SeqScan). Other
    /// shapes fall back to `generic_rid_match`.
    ///
    /// Perf sprint: try to fuse the predicate evaluation and in-place
    /// byte-level mutation into a single heap walk. Returns `Some(result)`
    /// if the fused path fired, `None` to fall through to the generic
    /// two-pass code.
    ///
    /// Covers two shapes:
    /// 1. Fixed-width non-null literal assignments on non-indexed columns
    ///    → byte-patch every matched row in place (row length unchanged).
    /// 2. Single var-col literal assignment on a non-indexed column
    ///    → `patch_var_column_in_place` on every matched row (may shrink);
    ///    rows that can't be patched in place are collected for fallback.
    fn try_fused_scan_update(
        &mut self,
        table: &str,
        predicate: &Expr,
        resolved: &[(usize, Value)],
        changed_cols: &[usize],
    ) -> Option<Result<QueryResult, String>> {
        // Build compiled predicate. Requires a schema borrow that must be
        // dropped before we call scan_patch_matching_logged.
        let compiled = {
            let schema = self.catalog.schema(table)?;
            let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let fast = FastLayout::new(schema);
            compile_predicate(predicate, &columns, &fast, schema)?
        };

        // ── Path 1: fixed-width fast patch ──────────────────────────
        let fixed_patches: Option<Vec<FastPatch>> = {
            let tbl = self.catalog.get_table(table)?;
            let schema = &tbl.schema;
            let all_fixed_nonnull = resolved.iter().all(|(idx, val)| {
                is_fixed_size(schema.columns[*idx].type_id) && !val.is_empty()
            });
            let no_indexed = !resolved.iter().any(|(idx, _)| tbl.has_indexed_col(*idx));
            if all_fixed_nonnull && no_indexed {
                let layout = RowLayout::new(schema);
                let bitmap_size = layout.bitmap_size();
                Some(resolved.iter().map(|(idx, val)| {
                    let fixed_off = layout.fixed_offset(*idx)
                        .expect("is_fixed_size already checked");
                    let field_off = 2 + bitmap_size + fixed_off;
                    let bytes: FixedBytes = match val {
                        Value::Int(v)      => FixedBytes::I64(v.to_le_bytes()),
                        Value::Float(v)    => FixedBytes::F64(v.to_le_bytes()),
                        Value::Bool(v)     => FixedBytes::Bool(if *v { 1 } else { 0 }),
                        Value::DateTime(v) => FixedBytes::I64(v.to_le_bytes()),
                        Value::Uuid(v)     => FixedBytes::Uuid(*v),
                        _ => unreachable!("all_fixed_nonnull guard"),
                    };
                    FastPatch {
                        field_off,
                        bitmap_byte_off: 2 + idx / 8,
                        bit_mask: 1u8 << (idx % 8),
                        bytes,
                    }
                }).collect())
            } else {
                None
            }
        };
        if let Some(patches) = fixed_patches {
            let result = self.catalog.scan_patch_matching_logged(
                table,
                compiled,
                |row| {
                    for p in &patches {
                        row[p.bitmap_byte_off] &= !p.bit_mask;
                        let field_bytes = p.bytes.as_slice();
                        row[p.field_off..p.field_off + field_bytes.len()]
                            .copy_from_slice(field_bytes);
                    }
                    Some(row.len() as u16)
                },
            ).map_err(|e| e.to_string());
            match result {
                Ok((count, _)) => {
                    self.view_registry.mark_dependents_dirty(table);
                    return Some(Ok(QueryResult::Modified(count)));
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // ── Path 2: single var-col shrink fast patch ────────────────
        let var_patch: Option<(usize, Option<Vec<u8>>)> = {
            let tbl = self.catalog.get_table(table)?;
            let schema = &tbl.schema;
            let is_single = resolved.len() == 1;
            let is_var = is_single
                && !is_fixed_size(schema.columns[resolved[0].0].type_id);
            let no_indexed = !resolved.iter().any(|(idx, _)| tbl.has_indexed_col(*idx));
            if is_single && is_var && no_indexed {
                let (idx, val) = &resolved[0];
                let bytes_opt = match val {
                    Value::Str(s)   => Some(s.as_bytes().to_vec()),
                    Value::Bytes(b) => Some(b.clone()),
                    Value::Empty    => None,
                    _               => return None, // type mismatch, fall through
                };
                Some((*idx, bytes_opt))
            } else {
                None
            }
        };
        if let Some((col_idx, ref new_bytes_opt)) = var_patch {
            // Build a fresh RowLayout before the mutable borrow.
            let layout = {
                let schema = self.catalog.schema(table)?;
                RowLayout::new(schema)
            };
            let new_bytes_ref: Option<&[u8]> = new_bytes_opt.as_deref();
            let result = self.catalog.scan_patch_matching_logged(
                table,
                compiled,
                |row| patch_var_column_in_place(row, &layout, col_idx, new_bytes_ref),
            ).map_err(|e| e.to_string());
            match result {
                Ok((mut count, fallback_rids)) => {
                    // Handle rows where in-place patch failed (new > old).
                    for rid in fallback_rids {
                        let mut row = match self.catalog.get(table, rid) {
                            Some(r) => r,
                            None => continue,
                        };
                        for (idx, val) in resolved.iter() {
                            row[*idx] = val.clone();
                        }
                        self.catalog
                            .update_hinted(table, rid, &row, Some(changed_cols))
                            .map_err(|e| e.to_string())
                            .ok();
                        count += 1;
                    }
                    self.view_registry.mark_dependents_dirty(table);
                    return Some(Ok(QueryResult::Modified(count)));
                }
                Err(e) => return Some(Err(e)),
            }
        }

        None // no fused path applicable — fall through
    }

    /// Mission C Phase 3: schema is looked up via `self.catalog.schema(table)`
    /// inside the branches that actually need it. Previously the caller had
    /// to clone the full Schema (6+ String allocs) before every mutation just
    /// so this function could borrow it — a cost the update/delete hot path
    /// did not need.
    fn collect_rids_for_mutation(
        &mut self,
        input: &PlanNode,
        table: &str,
    ) -> Result<Vec<RowId>, String> {
        match input {
            PlanNode::SeqScan { table: t } if t == table => {
                // "Update/delete everything" — rare but legal.
                let rids: Vec<RowId> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .map(|(rid, _)| rid)
                    .collect();
                Ok(rids)
            }
            PlanNode::IndexScan { table: t, column, key } if t == table => {
                let key_value = literal_to_value(key)?;

                // Indexed case: single lookup, 0 or 1 rows.
                // Mission D7: int-specialized fast path on int-keyed indexes
                // (primary keys, created_at, etc.) — the common case for
                // `update_by_pk` / `delete where id = ?`.
                //
                // Scope the `tbl` borrow so it's released before we fall
                // through to the scan-based paths below (which reborrow
                // `self.catalog`).
                {
                    let tbl = self.catalog.get_table(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    if let Some(btree) = tbl.index(column) {
                        let hit = match &key_value {
                            Value::Int(k) => btree.lookup_int(*k),
                            other => btree.lookup(other),
                        };
                        return Ok(match hit {
                            Some(rid) => vec![rid],
                            None => Vec::new(),
                        });
                    }
                }

                // No index: the planner folds `.col = literal` to IndexScan
                // regardless of whether the column is actually unique. When
                // there's no index we must behave like Filter(SeqScan) and
                // return *all* matching RIDs — not just the first one.
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let fast = FastLayout::new(schema);
                let synth = Expr::BinaryOp(
                    Box::new(Expr::Field(column.clone())),
                    BinOp::Eq,
                    Box::new(key.clone()),
                );
                if let Some(compiled) = compile_predicate(&synth, &columns, &fast, schema) {
                    // Mission F: skip the first 4 Vec doublings.
                    let mut rids: Vec<RowId> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |rid, data| {
                        if compiled(data) {
                            rids.push(rid);
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(rids);
                }

                // Fallback: decode each row, compare values.
                let col_idx = schema.column_index(column)
                    .ok_or_else(|| format!("column '{column}' not found"))?;
                let rids: Vec<RowId> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .filter_map(|(rid, row)| if row[col_idx] == key_value { Some(rid) } else { None })
                    .collect();
                Ok(rids)
            }
            PlanNode::Filter { input: inner, predicate } => {
                if let PlanNode::SeqScan { table: t } = inner.as_ref() {
                    if t != table {
                        return self.generic_rid_match(input, table);
                    }
                    let schema = self.catalog.schema(table)
                        .ok_or_else(|| format!("table '{table}' not found"))?;
                    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                    let fast = FastLayout::new(schema);
                    let row_layout = RowLayout::new(schema);

                    // Try compiled predicate first.
                    if let Some(compiled) = compile_predicate(predicate, &columns, &fast, schema) {
                        // Mission F: skip the first 4 Vec doublings.
                        let mut rids: Vec<RowId> = Vec::with_capacity(64);
                        self.catalog.for_each_row_raw(table, |rid, data| {
                            if compiled(data) {
                                rids.push(rid);
                            }
                        }).map_err(|e| e.to_string())?;
                        return Ok(rids);
                    }

                    // Fallback: selective decode + eval.
                    let pred_cols = predicate_column_indices(predicate, &columns);
                    let mut rids: Vec<RowId> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |rid, data| {
                        let pred_row = decode_selective(schema, &row_layout, data, &pred_cols);
                        if eval_predicate(predicate, &pred_row, &columns) {
                            rids.push(rid);
                        }
                    }).map_err(|e| e.to_string())?;
                    return Ok(rids);
                }
                self.generic_rid_match(input, table)
            }
            _ => self.generic_rid_match(input, table),
        }
    }

    /// Last-ditch generic match: execute the plan, collect matching rows,
    /// then find corresponding RowIds by value equality. This is the old
    /// O(N*M) code path; only used when the plan shape is something exotic.
    fn generic_rid_match(
        &mut self,
        input: &PlanNode,
        table: &str,
    ) -> Result<Vec<RowId>, String> {
        let result = self.execute_plan(input)?;
        let rows = match result {
            QueryResult::Rows { rows, .. } => rows,
            _ => return Err("mutation source must be rows".into()),
        };
        let matching: Vec<RowId> = self.catalog.scan(table)
            .map_err(|e| e.to_string())?
            .filter(|(_, row)| rows.iter().any(|r| r == row))
            .map(|(rid, _)| rid)
            .collect();
        Ok(matching)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}

/// Mission C Phase 4: precomputed byte-patch for the in-place update fast
/// path. Built once per `Update` query (outside the rid loop) and reused on
/// every matching row.
#[derive(Clone, Copy)]
struct FastPatch {
    /// Byte offset of the fixed column within the row encoding:
    /// `2 + bitmap_size + layout.fixed_offsets[col]`.
    field_off: usize,
    /// Byte offset of the bitmap byte containing this column's null bit
    /// (`2 + col/8`). We read-modify-write this byte to force the column
    /// non-null, so the idempotent clear is safe for already-non-null rows.
    bitmap_byte_off: usize,
    /// Bit mask for this column's null bit within `bitmap_byte_off`.
    bit_mask: u8,
    /// The new fixed-width value encoded as little-endian bytes.
    bytes: FixedBytes,
}

#[derive(Clone, Copy)]
enum FixedBytes {
    I64([u8; 8]),
    F64([u8; 8]),
    Bool(u8),
    Uuid([u8; 16]),
}

impl FixedBytes {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            FixedBytes::I64(b)  => b.as_slice(),
            FixedBytes::F64(b)  => b.as_slice(),
            FixedBytes::Bool(b) => std::slice::from_ref(b),
            FixedBytes::Uuid(b) => b.as_slice(),
        }
    }
}

fn type_name_to_id(name: &str) -> TypeId {
    match name {
        "str"      => TypeId::Str,
        "int"      => TypeId::Int,
        "float"    => TypeId::Float,
        "bool"     => TypeId::Bool,
        "datetime" => TypeId::DateTime,
        "uuid"     => TypeId::Uuid,
        "bytes"    => TypeId::Bytes,
        _          => TypeId::Str,
    }
}

/// Convert a runtime `Value` back into an `Expr::Literal` for InSubquery
/// materialization. Non-literal-representable values become `Literal::Int(0)`
/// (shouldn't happen in practice — subqueries return primitive columns).
/// Check if an expression tree contains any `InSubquery` nodes.
/// Collect all `Expr::Field` names referenced by an expression tree.
fn collect_field_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Field(name) => out.push(name.clone()),
        Expr::QualifiedField { qualifier, field } => {
            out.push(format!("{qualifier}.{field}"));
        }
        Expr::BinaryOp(l, _, r) => { collect_field_refs(l, out); collect_field_refs(r, out); }
        Expr::UnaryOp(_, inner) => collect_field_refs(inner, out),
        Expr::FunctionCall(_, inner) => collect_field_refs(inner, out),
        Expr::Coalesce(l, r) => { collect_field_refs(l, out); collect_field_refs(r, out); }
        Expr::InList { expr, list, .. } => {
            collect_field_refs(expr, out);
            for item in list { collect_field_refs(item, out); }
        }
        Expr::ScalarFunc(_, args) => { for a in args { collect_field_refs(a, out); } }
        Expr::Cast(inner, _) => { collect_field_refs(inner, out); }
        Expr::Case { whens, else_expr } => {
            for (c, r) in whens { collect_field_refs(c, out); collect_field_refs(r, out); }
            if let Some(e) = else_expr { collect_field_refs(e, out); }
        }
        _ => {}
    }
}

/// Detect whether a subquery is correlated: any `Expr::Field` reference in
/// the subquery's filter that doesn't match a column in the subquery's
/// source table indicates a reference to an outer scope.
/// Replace outer-scope field references in a correlated subquery's filter
/// with literal values from the current outer row. Fields that belong to
/// the subquery's own source table are left unchanged.
fn substitute_outer_refs(
    expr: &Expr,
    subquery_source: &str,
    catalog: &Catalog,
    outer_row: &[Value],
    outer_columns: &[String],
) -> Expr {
    let sub_cols: Vec<String> = catalog.schema(subquery_source)
        .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    substitute_outer_refs_inner(expr, &sub_cols, outer_row, outer_columns)
}

fn substitute_outer_refs_inner(
    expr: &Expr,
    sub_cols: &[String],
    outer_row: &[Value],
    outer_columns: &[String],
) -> Expr {
    match expr {
        Expr::Field(name) => {
            if sub_cols.iter().any(|c| c == name) {
                expr.clone()
            } else if let Some(i) = outer_columns.iter().position(|c| c == name) {
                value_to_expr(outer_row[i].clone())
            } else {
                expr.clone()
            }
        }
        Expr::BinaryOp(l, op, r) => {
            let l = substitute_outer_refs_inner(l, sub_cols, outer_row, outer_columns);
            let r = substitute_outer_refs_inner(r, sub_cols, outer_row, outer_columns);
            Expr::BinaryOp(Box::new(l), *op, Box::new(r))
        }
        Expr::UnaryOp(op, inner) => {
            let inner = substitute_outer_refs_inner(inner, sub_cols, outer_row, outer_columns);
            Expr::UnaryOp(*op, Box::new(inner))
        }
        Expr::InList { expr: e, list, negated } => {
            let e = substitute_outer_refs_inner(e, sub_cols, outer_row, outer_columns);
            let list = list.iter()
                .map(|item| substitute_outer_refs_inner(item, sub_cols, outer_row, outer_columns))
                .collect();
            Expr::InList { expr: Box::new(e), list, negated: *negated }
        }
        Expr::Coalesce(l, r) => {
            let l = substitute_outer_refs_inner(l, sub_cols, outer_row, outer_columns);
            let r = substitute_outer_refs_inner(r, sub_cols, outer_row, outer_columns);
            Expr::Coalesce(Box::new(l), Box::new(r))
        }
        other => other.clone(),
    }
}

fn is_correlated_subquery(subquery: &QueryExpr, catalog: &Catalog) -> bool {
    let filter = match &subquery.filter {
        Some(f) => f,
        None => return false,
    };
    let schema = match catalog.schema(&subquery.source) {
        Some(s) => s,
        None => return false, // table not found — not correlation, just an error
    };
    let table_cols: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut refs = Vec::new();
    collect_field_refs(filter, &mut refs);
    // If any referenced field doesn't exist in the subquery's source table,
    // it's (probably) a reference to an outer scope — i.e., correlated.
    refs.iter().any(|r| {
        // Skip qualified references (alias.field) — they unambiguously
        // target a specific source and will only match the subquery's own
        // source if they share the alias.
        if r.contains('.') {
            let alias = subquery.alias.as_deref().unwrap_or(&subquery.source);
            !r.starts_with(alias)
        } else {
            !table_cols.iter().any(|c| c == r)
        }
    })
}

fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } => true,
        Expr::ExistsSubquery { .. } => true,
        Expr::BinaryOp(l, _, r) => contains_subquery(l) || contains_subquery(r),
        Expr::UnaryOp(_, inner) => contains_subquery(inner),
        Expr::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
        }
        Expr::Case { whens, else_expr } => {
            whens.iter().any(|(c, r)| contains_subquery(c) || contains_subquery(r))
                || else_expr.as_ref().is_some_and(|e| contains_subquery(e))
        }
        Expr::ScalarFunc(_, args) => args.iter().any(contains_subquery),
        Expr::Cast(inner, _) => contains_subquery(inner),
        Expr::FunctionCall(_, inner) => contains_subquery(inner),
        Expr::Coalesce(l, r) => contains_subquery(l) || contains_subquery(r),
        _ => false,
    }
}

fn value_to_expr(val: Value) -> Expr {
    match val {
        Value::Int(v)    => Expr::Literal(Literal::Int(v)),
        Value::Float(v)  => Expr::Literal(Literal::Float(v)),
        Value::Str(v)    => Expr::Literal(Literal::String(v)),
        Value::Bool(v)   => Expr::Literal(Literal::Bool(v)),
        _ => Expr::Literal(Literal::Int(0)),
    }
}

fn literal_to_value(expr: &Expr) -> Result<Value, String> {
    match expr {
        Expr::Literal(Literal::Int(v))    => Ok(Value::Int(*v)),
        Expr::Literal(Literal::Float(v))  => Ok(Value::Float(*v)),
        Expr::Literal(Literal::String(v)) => Ok(Value::Str(v.clone())),
        Expr::Literal(Literal::Bool(v))   => Ok(Value::Bool(*v)),
        _ => Err("expected literal value".into()),
    }
}

/// Mission C Phase 5: direct Literal→Value conversion used by the
/// prepared-statement Insert fast path. Skips the `Expr::Literal` unwrap
/// and the `Result` plumbing of [`literal_to_value`]. String literals
/// still clone because the row needs an owned `Value::Str`.
#[inline]
fn literal_value_from(lit: &Literal) -> Value {
    match lit {
        Literal::Int(v)    => Value::Int(*v),
        Literal::Float(v)  => Value::Float(*v),
        Literal::String(v) => Value::Str(v.clone()),
        Literal::Bool(v)   => Value::Bool(*v),
    }
}

/// Mission C Phase 13: moving companion to [`literal_value_from`] used
/// by [`Engine::execute_prepared_take`]. Pulls the `String` out of a
/// `Literal::String` via `mem::take`, leaving an empty string behind
/// so the caller's slice remains valid (but with blanked-out strings).
/// On the insert fast path this removes one heap alloc per string
/// column per row.
#[inline]
fn literal_value_take(lit: &mut Literal) -> Value {
    match lit {
        Literal::Int(v)    => Value::Int(*v),
        Literal::Float(v)  => Value::Float(*v),
        Literal::String(v) => Value::Str(std::mem::take(v)),
        Literal::Bool(v)   => Value::Bool(*v),
    }
}

fn eval_expr(expr: &Expr, row: &[Value], columns: &[String]) -> Value {
    match expr {
        Expr::Field(name) => {
            columns.iter().position(|c| c == name)
                .map(|i| row[i].clone())
                .unwrap_or(Value::Empty)
        }
        Expr::QualifiedField { qualifier, field } => {
            // Mission E1.2: join queries emit columns named `alias.field`,
            // so the lookup is a direct prefix+tail match. We compare in
            // pieces to avoid allocating a fresh `format!("{q}.{f}")` on
            // every row — the join loop can evaluate this tens of thousands
            // of times per query.
            let q = qualifier.as_bytes();
            let f = field.as_bytes();
            let idx = columns.iter().position(|c| {
                let b = c.as_bytes();
                b.len() == q.len() + 1 + f.len()
                    && b[..q.len()] == *q
                    && b[q.len()] == b'.'
                    && b[q.len() + 1..] == *f
            });
            idx.map(|i| row[i].clone()).unwrap_or(Value::Empty)
        }
        Expr::Literal(lit) => match lit {
            Literal::Int(v) => Value::Int(*v),
            Literal::Float(v) => Value::Float(*v),
            Literal::String(v) => Value::Str(v.clone()),
            Literal::Bool(v) => Value::Bool(*v),
        },
        Expr::BinaryOp(left, op, right) => {
            let l = eval_expr(left, row, columns);
            let r = eval_expr(right, row, columns);
            eval_binop(&l, *op, &r)
        }
        Expr::Coalesce(left, right) => {
            let l = eval_expr(left, row, columns);
            if l.is_empty() { eval_expr(right, row, columns) } else { l }
        }
        Expr::InList { expr, list, negated } => {
            let val = eval_expr(expr, row, columns);
            let found = list.iter().any(|item| {
                let iv = eval_expr(item, row, columns);
                val == iv
            });
            Value::Bool(if *negated { !found } else { found })
        }
        Expr::InSubquery { .. } => {
            // Should have been materialized into InList before eval_expr.
            Value::Empty
        }
        Expr::ExistsSubquery { .. } => {
            // Should have been materialized into a Bool literal before
            // eval_expr (see materialize_subqueries).
            Value::Empty
        }
        Expr::UnaryOp(op, inner) => {
            let v = eval_expr(inner, row, columns);
            match op {
                UnaryOp::Not => match v {
                    Value::Bool(b) => Value::Bool(!b),
                    _ => Value::Empty,
                },
                UnaryOp::Exists => Value::Bool(!v.is_empty()),
                UnaryOp::NotExists => Value::Bool(v.is_empty()),
                UnaryOp::IsNull => Value::Bool(v.is_empty()),
                UnaryOp::IsNotNull => Value::Bool(!v.is_empty()),
            }
        }
        Expr::ScalarFunc(func, args) => {
            let vals: Vec<Value> = args.iter().map(|a| eval_expr(a, row, columns)).collect();
            eval_scalar_func(*func, &vals)
        }
        Expr::Case { whens, else_expr } => {
            for (condition, result) in whens {
                if eval_predicate(condition, row, columns) {
                    return eval_expr(result, row, columns);
                }
            }
            match else_expr {
                Some(e) => eval_expr(e, row, columns),
                None => Value::Empty,
            }
        }
        Expr::Cast(inner, cast_type) => {
            let val = eval_expr(inner, row, columns);
            eval_cast(val, *cast_type)
        }
        Expr::FunctionCall(_, _) | Expr::Param(_) | Expr::Window { .. } => Value::Empty,
    }
}

fn eval_predicate(expr: &Expr, row: &[Value], columns: &[String]) -> bool {
    match eval_expr(expr, row, columns) {
        Value::Bool(b) => b,
        _ => false,
    }
}

fn eval_scalar_func(func: ScalarFn, args: &[Value]) -> Value {
    match func {
        ScalarFn::Upper => match args.first() {
            Some(Value::Str(s)) => Value::Str(s.to_uppercase()),
            _ => Value::Empty,
        },
        ScalarFn::Lower => match args.first() {
            Some(Value::Str(s)) => Value::Str(s.to_lowercase()),
            _ => Value::Empty,
        },
        ScalarFn::Length => match args.first() {
            Some(Value::Str(s)) => Value::Int(s.len() as i64),
            _ => Value::Empty,
        },
        ScalarFn::Trim => match args.first() {
            Some(Value::Str(s)) => Value::Str(s.trim().to_string()),
            _ => Value::Empty,
        },
        ScalarFn::Substring => {
            if args.len() < 3 { return Value::Empty; }
            match (&args[0], &args[1], &args[2]) {
                (Value::Str(s), Value::Int(start), Value::Int(len)) => {
                    let start = (*start as usize).saturating_sub(1); // 1-indexed
                    let len = *len as usize;
                    let sub: String = s.chars().skip(start).take(len).collect();
                    Value::Str(sub)
                }
                _ => Value::Empty,
            }
        }
        ScalarFn::Concat => {
            let mut result = String::new();
            for v in args {
                match v {
                    Value::Str(s) => result.push_str(s),
                    Value::Int(n) => result.push_str(&n.to_string()),
                    Value::Float(f) => result.push_str(&f.to_string()),
                    Value::Bool(b) => result.push_str(if *b { "true" } else { "false" }),
                    _ => {}
                }
            }
            Value::Str(result)
        }
        // Math functions
        ScalarFn::Abs => match args.first() {
            Some(Value::Int(n)) => Value::Int(n.abs()),
            Some(Value::Float(f)) => Value::Float(f.abs()),
            _ => Value::Empty,
        },
        ScalarFn::Round => {
            let decimals = match args.get(1) {
                Some(Value::Int(d)) => *d as i32,
                _ => 0,
            };
            match args.first() {
                Some(Value::Float(f)) => {
                    let factor = 10_f64.powi(decimals);
                    Value::Float((f * factor).round() / factor)
                }
                Some(Value::Int(n)) => Value::Int(*n),
                _ => Value::Empty,
            }
        }
        ScalarFn::Ceil => match args.first() {
            Some(Value::Float(f)) => Value::Float(f.ceil()),
            Some(Value::Int(n)) => Value::Int(*n),
            _ => Value::Empty,
        },
        ScalarFn::Floor => match args.first() {
            Some(Value::Float(f)) => Value::Float(f.floor()),
            Some(Value::Int(n)) => Value::Int(*n),
            _ => Value::Empty,
        },
        ScalarFn::Sqrt => match args.first() {
            Some(Value::Float(f)) if *f >= 0.0 => Value::Float(f.sqrt()),
            Some(Value::Int(n)) if *n >= 0 => Value::Float((*n as f64).sqrt()),
            _ => Value::Empty,
        },
        ScalarFn::Pow => {
            match (args.first(), args.get(1)) {
                (Some(Value::Float(base)), Some(Value::Float(exp))) => Value::Float(base.powf(*exp)),
                (Some(Value::Float(base)), Some(Value::Int(exp))) => Value::Float(base.powi(*exp as i32)),
                (Some(Value::Int(base)), Some(Value::Int(exp))) => {
                    if *exp >= 0 && *exp <= u32::MAX as i64 {
                        match base.checked_pow(*exp as u32) {
                            Some(v) => Value::Int(v),
                            None => Value::Float((*base as f64).powi(*exp as i32)),
                        }
                    } else {
                        Value::Float((*base as f64).powi(*exp as i32))
                    }
                }
                (Some(Value::Int(base)), Some(Value::Float(exp))) => Value::Float((*base as f64).powf(*exp)),
                _ => Value::Empty,
            }
        }
        // Date/time functions
        ScalarFn::Now => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            Value::DateTime(micros)
        }
        ScalarFn::Extract => {
            // extract("part", datetime_expr)
            let part = match args.first() {
                Some(Value::Str(s)) => s.as_str(),
                _ => return Value::Empty,
            };
            let micros = match args.get(1) {
                Some(Value::DateTime(m)) => *m,
                Some(Value::Int(m)) => *m, // treat raw int as micros
                _ => return Value::Empty,
            };
            datetime_extract(part, micros)
        }
        ScalarFn::DateAdd => {
            // date_add(datetime_expr, amount, "unit")
            let micros = match args.first() {
                Some(Value::DateTime(m)) => *m,
                Some(Value::Int(m)) => *m,
                _ => return Value::Empty,
            };
            let amount = match args.get(1) {
                Some(Value::Int(n)) => *n,
                _ => return Value::Empty,
            };
            let unit = match args.get(2) {
                Some(Value::Str(s)) => s.as_str(),
                _ => return Value::Empty,
            };
            let delta_micros = match unit {
                "microsecond" | "microseconds" | "us" => amount,
                "millisecond" | "milliseconds" | "ms" => amount * 1_000,
                "second" | "seconds" | "s" => amount * 1_000_000,
                "minute" | "minutes" | "m" => amount * 60_000_000,
                "hour" | "hours" | "h" => amount * 3_600_000_000,
                "day" | "days" | "d" => amount * 86_400_000_000,
                _ => return Value::Empty,
            };
            Value::DateTime(micros + delta_micros)
        }
        ScalarFn::DateDiff => {
            // date_diff(dt1, dt2, "unit")
            let m1 = match args.first() {
                Some(Value::DateTime(m)) => *m,
                Some(Value::Int(m)) => *m,
                _ => return Value::Empty,
            };
            let m2 = match args.get(1) {
                Some(Value::DateTime(m)) => *m,
                Some(Value::Int(m)) => *m,
                _ => return Value::Empty,
            };
            let unit = match args.get(2) {
                Some(Value::Str(s)) => s.as_str(),
                _ => return Value::Empty,
            };
            let diff = m1 - m2;
            let result = match unit {
                "microsecond" | "microseconds" | "us" => diff,
                "millisecond" | "milliseconds" | "ms" => diff / 1_000,
                "second" | "seconds" | "s" => diff / 1_000_000,
                "minute" | "minutes" | "m" => diff / 60_000_000,
                "hour" | "hours" | "h" => diff / 3_600_000_000,
                "day" | "days" | "d" => diff / 86_400_000_000,
                _ => return Value::Empty,
            };
            Value::Int(result)
        }
    }
}

/// Extract a component from a DateTime value (microseconds since epoch).
fn datetime_extract(part: &str, micros: i64) -> Value {
    // Convert micros to seconds + remainder for calendar calculations
    let total_secs = micros / 1_000_000;
    let micro_rem = micros % 1_000_000;

    // Simple civil calendar from Unix timestamp (no TZ — UTC assumed)
    let days_since_epoch = if total_secs >= 0 {
        total_secs / 86400
    } else {
        (total_secs - 86399) / 86400
    };
    let secs_of_day = total_secs - days_since_epoch * 86400;

    match part {
        "hour" => Value::Int(secs_of_day / 3600),
        "minute" => Value::Int((secs_of_day % 3600) / 60),
        "second" => Value::Int(secs_of_day % 60),
        "millisecond" => Value::Int(micro_rem / 1000),
        "microsecond" => Value::Int(micro_rem),
        "epoch" => Value::Int(total_secs),
        "year" | "month" | "day" => {
            // Civil date from days since 1970-01-01 (algorithm from Howard Hinnant)
            let z = days_since_epoch + 719468;
            let era = if z >= 0 { z } else { z - 146096 } / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = (yoe as i64) + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };
            match part {
                "year" => Value::Int(y),
                "month" => Value::Int(m as i64),
                "day" => Value::Int(d as i64),
                _ => unreachable!(),
            }
        }
        _ => Value::Empty,
    }
}

/// Evaluate a CAST expression.
fn eval_cast(val: Value, target: CastType) -> Value {
    match target {
        CastType::Int => match val {
            Value::Int(n) => Value::Int(n),
            Value::Float(f) => Value::Int(f as i64),
            Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
            Value::Str(s) => s.parse::<i64>().map(Value::Int).unwrap_or(Value::Empty),
            Value::DateTime(m) => Value::Int(m),
            _ => Value::Empty,
        },
        CastType::Float => match val {
            Value::Float(f) => Value::Float(f),
            Value::Int(n) => Value::Float(n as f64),
            Value::Str(s) => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Empty),
            Value::Bool(b) => Value::Float(if b { 1.0 } else { 0.0 }),
            _ => Value::Empty,
        },
        CastType::Str => match val {
            Value::Str(s) => Value::Str(s),
            Value::Int(n) => Value::Str(n.to_string()),
            Value::Float(f) => Value::Str(f.to_string()),
            Value::Bool(b) => Value::Str(b.to_string()),
            Value::DateTime(m) => Value::Str(m.to_string()),
            _ => Value::Empty,
        },
        CastType::Bool => match val {
            Value::Bool(b) => Value::Bool(b),
            Value::Int(n) => Value::Bool(n != 0),
            Value::Str(s) => match s.as_str() {
                "true" | "1" | "yes" => Value::Bool(true),
                "false" | "0" | "no" => Value::Bool(false),
                _ => Value::Empty,
            },
            _ => Value::Empty,
        },
        CastType::DateTime => match val {
            Value::DateTime(m) => Value::DateTime(m),
            Value::Int(m) => Value::DateTime(m),
            _ => Value::Empty,
        },
    }
}

/// Execute window function computations. Shared by both read and write paths.
///
/// For each `WindowDef`:
///   1. Sort rows by (partition_by keys, order_by keys).
///   2. Walk sorted rows, detecting partition boundaries.
///   3. Compute the window value per row (running aggregates reset at
///      partition boundaries).
///   4. Append the computed column to each row and register the column name.
///
/// All computed columns are appended to the original row data; the
/// downstream `Project` node plucks the ones the user asked for.
fn execute_window(result: QueryResult, windows: &[WindowDef]) -> Result<QueryResult, String> {
    let (mut columns, mut rows) = match result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => return Err("window function requires row input".into()),
    };

    for wdef in windows {
        // Resolve partition/order column indices against current columns.
        let part_indices: Vec<usize> = wdef.partition_by.iter().map(|name| {
            columns.iter().position(|c| c == name)
                .ok_or_else(|| format!("window partition column '{name}' not found"))
        }).collect::<Result<Vec<_>, _>>()?;

        let ord_indices: Vec<(usize, bool)> = wdef.order_by.iter().map(|sk| {
            columns.iter().position(|c| c == &sk.field)
                .map(|i| (i, sk.descending))
                .ok_or_else(|| format!("window order column '{}' not found", sk.field))
        }).collect::<Result<Vec<_>, _>>()?;

        // Resolve the argument column index (for aggregate windows).
        let arg_col_idx: Option<usize> = if let Some(arg) = wdef.args.first() {
            match arg {
                Expr::Field(name) => {
                    if name == "*" {
                        None // count(*) style — no specific column
                    } else {
                        Some(columns.iter().position(|c| c == name)
                            .ok_or_else(|| format!("window arg column '{name}' not found"))?)
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        // Build a sort-index to sort rows by partition_by then order_by
        // without actually reordering the original Vec (we need original
        // order to write results back).
        let n = rows.len();
        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_by(|&a, &b| {
            // Compare partition keys first.
            for &pi in &part_indices {
                let cmp = rows[a][pi].cmp(&rows[b][pi]);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            // Then order keys.
            for &(oi, desc) in &ord_indices {
                let cmp = rows[a][oi].cmp(&rows[b][oi]);
                if cmp != std::cmp::Ordering::Equal {
                    return if desc { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        });

        // Compute window values in sorted order, tracking partition boundaries.
        let mut win_values: Vec<Value> = vec![Value::Empty; n];
        let mut partition_start = 0usize;
        // Running state for aggregate windows:
        let mut running_count: i64 = 0;
        let mut running_int_sum: i64 = 0;
        let mut running_float_sum: f64 = 0.0;
        let mut running_saw_float = false;
        let mut running_min: Option<Value> = None;
        let mut running_max: Option<Value> = None;
        let mut rank_counter: i64 = 0;
        let mut dense_rank_counter: i64 = 0;
        let mut prev_order_key: Option<Vec<Value>> = None;
        let mut same_rank_count: i64 = 0;

        for sorted_pos in 0..n {
            let row_idx = indices[sorted_pos];

            // Detect partition boundary.
            let new_partition = if sorted_pos == 0 {
                true
            } else {
                let prev_row_idx = indices[sorted_pos - 1];
                part_indices.iter().any(|&pi| rows[row_idx][pi] != rows[prev_row_idx][pi])
            };

            if new_partition {
                partition_start = sorted_pos;
                running_count = 0;
                running_int_sum = 0;
                running_float_sum = 0.0;
                running_saw_float = false;
                running_min = None;
                running_max = None;
                rank_counter = 0;
                dense_rank_counter = 0;
                prev_order_key = None;
                same_rank_count = 0;
            }

            // Extract current order key for rank tracking.
            let current_order_key: Vec<Value> = ord_indices.iter()
                .map(|&(oi, _)| rows[row_idx][oi].clone())
                .collect();
            let same_as_prev = prev_order_key.as_ref() == Some(&current_order_key);

            let value = match wdef.function {
                WindowFunc::RowNumber => {
                    Value::Int((sorted_pos - partition_start + 1) as i64)
                }
                WindowFunc::Rank => {
                    if same_as_prev {
                        same_rank_count += 1;
                    } else {
                        rank_counter += same_rank_count + 1;
                        same_rank_count = 0;
                        if rank_counter == 0 { rank_counter = 1; }
                    }
                    Value::Int(rank_counter)
                }
                WindowFunc::DenseRank => {
                    if !same_as_prev {
                        dense_rank_counter += 1;
                    }
                    Value::Int(dense_rank_counter)
                }
                WindowFunc::Sum => {
                    if let Some(ci) = arg_col_idx {
                        match &rows[row_idx][ci] {
                            Value::Int(v) => running_int_sum += v,
                            Value::Float(v) => { running_float_sum += v; running_saw_float = true; }
                            _ => {}
                        }
                    }
                    if running_saw_float {
                        Value::Float(running_float_sum + running_int_sum as f64)
                    } else {
                        Value::Int(running_int_sum)
                    }
                }
                WindowFunc::Avg => {
                    if let Some(ci) = arg_col_idx {
                        match &rows[row_idx][ci] {
                            Value::Int(v) => { running_float_sum += *v as f64; running_count += 1; }
                            Value::Float(v) => { running_float_sum += v; running_count += 1; }
                            _ => {}
                        }
                    }
                    if running_count == 0 { Value::Empty } else {
                        Value::Float(running_float_sum / running_count as f64)
                    }
                }
                WindowFunc::Count => {
                    if let Some(ci) = arg_col_idx {
                        if !rows[row_idx][ci].is_empty() {
                            running_count += 1;
                        }
                    } else {
                        // count(*) — count all rows
                        running_count += 1;
                    }
                    Value::Int(running_count)
                }
                WindowFunc::Min => {
                    if let Some(ci) = arg_col_idx {
                        let v = &rows[row_idx][ci];
                        if !v.is_empty() {
                            running_min = Some(match &running_min {
                                None => v.clone(),
                                Some(cur) => if v < cur { v.clone() } else { cur.clone() },
                            });
                        }
                    }
                    running_min.clone().unwrap_or(Value::Empty)
                }
                WindowFunc::Max => {
                    if let Some(ci) = arg_col_idx {
                        let v = &rows[row_idx][ci];
                        if !v.is_empty() {
                            running_max = Some(match &running_max {
                                None => v.clone(),
                                Some(cur) => if v > cur { v.clone() } else { cur.clone() },
                            });
                        }
                    }
                    running_max.clone().unwrap_or(Value::Empty)
                }
            };

            prev_order_key = Some(current_order_key);
            win_values[row_idx] = value;
        }

        // Append the computed window column to each row.
        for (ri, row) in rows.iter_mut().enumerate() {
            row.push(win_values[ri].clone());
        }
        columns.push(wdef.output_name.clone());
    }

    Ok(QueryResult::Rows { columns, rows })
}

/// Mission E2b: compute one aggregate over a set of rows in a group.
fn compute_group_aggregate(
    func: AggFunc,
    all_rows: &[Vec<Value>],
    row_indices: &[usize],
    col_idx: usize,
) -> Value {
    match func {
        AggFunc::Count => {
            if col_idx == usize::MAX {
                // count(*) — count all rows in the group.
                return Value::Int(row_indices.len() as i64);
            }
            let count = row_indices.iter()
                .filter(|&&ri| !all_rows[ri][col_idx].is_empty())
                .count();
            Value::Int(count as i64)
        }
        AggFunc::CountDistinct => {
            let mut seen = std::collections::HashSet::new();
            for &ri in row_indices {
                let v = &all_rows[ri][col_idx];
                if !v.is_empty() { seen.insert(v.clone()); }
            }
            Value::Int(seen.len() as i64)
        }
        AggFunc::Sum => {
            // Mirror the scalar Sum path: accumulate int and float
            // contributions separately and promote the final result to
            // Float if any Float row was observed. Prevents silent
            // drop of Float columns in GROUP BY aggregates.
            let mut int_sum: i64 = 0;
            let mut float_sum: f64 = 0.0;
            let mut saw_float = false;
            for &ri in row_indices {
                match &all_rows[ri][col_idx] {
                    Value::Int(v)   => int_sum += v,
                    Value::Float(v) => { float_sum += *v; saw_float = true; }
                    _ => {}
                }
            }
            if saw_float {
                Value::Float(float_sum + int_sum as f64)
            } else {
                Value::Int(int_sum)
            }
        }
        AggFunc::Avg => {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for &ri in row_indices {
                match &all_rows[ri][col_idx] {
                    Value::Int(v)   => { sum += *v as f64; count += 1; }
                    Value::Float(v) => { sum += *v;        count += 1; }
                    _ => {}
                }
            }
            if count == 0 { Value::Empty } else { Value::Float(sum / count as f64) }
        }
        AggFunc::Min => {
            row_indices.iter()
                .map(|&ri| &all_rows[ri][col_idx])
                .filter(|v| !v.is_empty())
                .min().cloned()
                .unwrap_or(Value::Empty)
        }
        AggFunc::Max => {
            row_indices.iter()
                .map(|&ri| &all_rows[ri][col_idx])
                .filter(|v| !v.is_empty())
                .max().cloned()
                .unwrap_or(Value::Empty)
        }
    }
}

/// Mission E1.3: try to extract equi-join key indices from a join `on`
/// predicate. Returns `Some((left_col_idx, right_col_idx))` when the
/// predicate is exactly `L = R` (or `R = L`) and both sides resolve
/// cleanly — `L` to the left subtree's column list and `R` to the right
/// subtree's column list.
///
/// This is deliberately narrow. We only recognise the two shapes:
///   * `QualifiedField = QualifiedField`  (`u.id = o.user_id`)
///   * `Field = Field`                    (`.id = .user_id`, unqualified)
///
/// Anything else — conjunctions, constants, function calls, or predicates
/// that touch the same side on both halves — falls through to the
/// nested-loop path unchanged.
fn try_extract_equi_join_keys(
    pred: &Expr,
    left_columns: &[String],
    right_columns: &[String],
) -> Option<(usize, usize)> {
    let (lhs, op, rhs) = match pred {
        Expr::BinaryOp(l, op, r) => (l.as_ref(), *op, r.as_ref()),
        _ => return None,
    };
    if op != BinOp::Eq {
        return None;
    }
    // Normal orientation: lhs in left, rhs in right.
    if let (Some(li), Some(ri)) = (
        resolve_side_column(lhs, left_columns),
        resolve_side_column(rhs, right_columns),
    ) {
        return Some((li, ri));
    }
    // Swapped: rhs in left, lhs in right. Both sides of `=` are
    // commutative so this is safe.
    if let (Some(li), Some(ri)) = (
        resolve_side_column(rhs, left_columns),
        resolve_side_column(lhs, right_columns),
    ) {
        return Some((li, ri));
    }
    None
}

fn resolve_side_column(expr: &Expr, columns: &[String]) -> Option<usize> {
    match expr {
        Expr::QualifiedField { qualifier, field } => {
            // Byte-level match so we don't allocate a fresh `format!` on
            // every call — this runs once per plan, so allocation would be
            // cheap, but the match is trivial enough to keep inline with
            // the eval_expr version.
            let q = qualifier.as_bytes();
            let f = field.as_bytes();
            columns.iter().position(|c| {
                let b = c.as_bytes();
                b.len() == q.len() + 1 + f.len()
                    && b[..q.len()] == *q
                    && b[q.len()] == b'.'
                    && b[q.len() + 1..] == *f
            })
        }
        Expr::Field(name) => columns.iter().position(|c| c == name),
        _ => None,
    }
}

/// Mission E1.3: O(L + R) hash join. Builds a `FxHashMap<Value, Vec<usize>>`
/// over the right (inner) side's join keys, then streams the left (outer)
/// side and for each probe row emits every combined row whose right-side
/// key matches. For `JoinKind::LeftOuter`, unmatched left rows are emitted
/// padded with `Value::Empty` on the right side.
///
/// The right side is always the build side. That choice is forced for
/// LeftOuter (the left side must stream so we can detect orphans), and
/// for Inner it's a reasonable default — left-deep plans tend to grow the
/// left side with each join, so the un-joined right leaf is often the
/// smaller of the two at each level.
fn hash_join(
    left_columns: Vec<String>,
    left_rows: Vec<Vec<Value>>,
    right_columns: Vec<String>,
    right_rows: Vec<Vec<Value>>,
    left_key_idx: usize,
    right_key_idx: usize,
    kind: JoinKind,
) -> QueryResult {
    use rustc_hash::FxHashMap;

    let n_left = left_columns.len();
    let n_right = right_columns.len();
    let mut columns = Vec::with_capacity(n_left + n_right);
    columns.extend(left_columns);
    columns.extend(right_columns);

    // Build: right_key -> list of right-row indices. Pre-size to the row
    // count so the map doesn't rehash mid-build.
    let mut build: FxHashMap<Value, Vec<usize>> =
        FxHashMap::with_capacity_and_hasher(right_rows.len(), Default::default());
    for (i, row) in right_rows.iter().enumerate() {
        // Skip Empty keys on the build side — they can never match under
        // SQL semantics (NULL ≠ NULL) and would collapse all nullables to
        // one bucket.
        if matches!(row[right_key_idx], Value::Empty) {
            continue;
        }
        build.entry(row[right_key_idx].clone()).or_default().push(i);
    }

    // Reasonable starting capacity — inner joins produce ≥ left_rows.len()
    // rows in the common 1:1 case, left-outer always emits ≥ left_rows.len().
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(left_rows.len());

    for left_row in &left_rows {
        let key = &left_row[left_key_idx];
        let matched = if matches!(key, Value::Empty) {
            None
        } else {
            build.get(key)
        };
        match matched {
            Some(matches) if !matches.is_empty() => {
                for &ri in matches {
                    let right_row = &right_rows[ri];
                    let mut combined = Vec::with_capacity(n_left + n_right);
                    combined.extend_from_slice(left_row);
                    combined.extend_from_slice(right_row);
                    rows.push(combined);
                }
            }
            _ => {
                if matches!(kind, JoinKind::LeftOuter) {
                    let mut row = Vec::with_capacity(n_left + n_right);
                    row.extend_from_slice(left_row);
                    row.resize(n_left + n_right, Value::Empty);
                    rows.push(row);
                }
            }
        }
    }

    QueryResult::Rows { columns, rows }
}

/// Lower unindexed `RangeScan` nodes to `Filter(SeqScan)` so that all
/// downstream fast paths (count, project+limit, sort+limit, agg, update,
/// delete) continue to fire.
///
/// The planner emits `RangeScan` speculatively for every range inequality
/// (`.age > 30`) because it has no catalog access. When the column has a
/// B-tree index, `RangeScan` is the correct plan. When it doesn't, the
/// executor's `RangeScan` fallback materialises every matching row with
/// full `decode_row` — bypassing the compiled-predicate fast paths that
/// `Filter(SeqScan)` would trigger.
///
/// This pass runs once per query, before execution.
fn lower_unindexed_range_scans(catalog: &Catalog, plan: &PlanNode) -> PlanNode {
    match plan {
        PlanNode::RangeScan { table, column, start, end } => {
            if let Some(tbl) = catalog.get_table(table) {
                if tbl.index(column).is_some() {
                    return plan.clone();
                }
            }
            let pred = synthesize_range_predicate(column, start, end);
            PlanNode::Filter {
                input: Box::new(PlanNode::SeqScan { table: table.clone() }),
                predicate: pred,
            }
        }
        PlanNode::Filter { input, predicate } => PlanNode::Filter {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            predicate: predicate.clone(),
        },
        PlanNode::Project { input, fields } => PlanNode::Project {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            fields: fields.clone(),
        },
        PlanNode::Sort { input, keys } => PlanNode::Sort {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            keys: keys.clone(),
        },
        PlanNode::Limit { input, count } => PlanNode::Limit {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            count: count.clone(),
        },
        PlanNode::Offset { input, count } => PlanNode::Offset {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            count: count.clone(),
        },
        PlanNode::Aggregate { input, function, field } => PlanNode::Aggregate {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            function: *function,
            field: field.clone(),
        },
        PlanNode::Distinct { input } => PlanNode::Distinct {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
        },
        PlanNode::GroupBy { input, keys, aggregates, having } => PlanNode::GroupBy {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            keys: keys.clone(),
            aggregates: aggregates.clone(),
            having: having.clone(),
        },
        PlanNode::Update { input, table, assignments } => PlanNode::Update {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            table: table.clone(),
            assignments: assignments.clone(),
        },
        PlanNode::Delete { input, table } => PlanNode::Delete {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            table: table.clone(),
        },
        PlanNode::Window { input, windows } => PlanNode::Window {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
            windows: windows.clone(),
        },
        PlanNode::Union { left, right, all } => PlanNode::Union {
            left: Box::new(lower_unindexed_range_scans(catalog, left)),
            right: Box::new(lower_unindexed_range_scans(catalog, right)),
            all: *all,
        },
        PlanNode::Explain { input } => PlanNode::Explain {
            input: Box::new(lower_unindexed_range_scans(catalog, input)),
        },
        PlanNode::NestedLoopJoin { left, right, on, kind } => PlanNode::NestedLoopJoin {
            left: Box::new(lower_unindexed_range_scans(catalog, left)),
            right: Box::new(lower_unindexed_range_scans(catalog, right)),
            on: on.clone(),
            kind: *kind,
        },
        // Leaf nodes: no children to recurse into.
        _ => plan.clone(),
    }
}

/// Synthesize a range predicate from RangeScan bounds for the fallback path.
fn synthesize_range_predicate(column: &str, start: &Option<(Expr, bool)>, end: &Option<(Expr, bool)>) -> Expr {
    let lower = start.as_ref().map(|(expr, inclusive)| {
        let op = if *inclusive { BinOp::Gte } else { BinOp::Gt };
        Expr::BinaryOp(
            Box::new(Expr::Field(column.to_string())),
            op,
            Box::new(expr.clone()),
        )
    });
    let upper = end.as_ref().map(|(expr, inclusive)| {
        let op = if *inclusive { BinOp::Lte } else { BinOp::Lt };
        Expr::BinaryOp(
            Box::new(Expr::Field(column.to_string())),
            op,
            Box::new(expr.clone()),
        )
    });
    match (lower, upper) {
        (Some(l), Some(u)) => Expr::BinaryOp(Box::new(l), BinOp::And, Box::new(u)),
        (Some(l), None) => l,
        (None, Some(u)) => u,
        (None, None) => Expr::Literal(Literal::Bool(true)),
    }
}

/// Check if a value falls within a range (used in last-resort decoded-row eval).
fn range_matches(val: &Value, start: &Option<Value>, start_inc: bool, end: &Option<Value>, end_inc: bool) -> bool {
    if let Some(ref s) = start {
        if start_inc { if val < s { return false; } }
        else if val <= s { return false; }
    }
    if let Some(ref e) = end {
        if end_inc { if val > e { return false; } }
        else if val >= e { return false; }
    }
    true
}

/// Format a `PlanNode` tree as a human-readable, indented text
/// representation. Used by the `EXPLAIN` command.
fn format_plan_tree(plan: &PlanNode, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    match plan {
        PlanNode::SeqScan { table } => format!("{indent}SeqScan table={table}"),
        PlanNode::AliasScan { table, alias } => {
            format!("{indent}AliasScan table={table} alias={alias}")
        }
        PlanNode::IndexScan { table, column, key } => {
            format!("{indent}IndexScan table={table} column={column} key={key:?}")
        }
        PlanNode::RangeScan { table, column, start, end } => {
            let s = match start {
                Some((expr, inc)) => {
                    let op = if *inc { ">=" } else { ">" };
                    format!("{op}{expr:?}")
                }
                None => "unbounded".to_string(),
            };
            let e = match end {
                Some((expr, inc)) => {
                    let op = if *inc { "<=" } else { "<" };
                    format!("{op}{expr:?}")
                }
                None => "unbounded".to_string(),
            };
            format!("{indent}RangeScan table={table} column={column} [{s}, {e}]")
        }
        PlanNode::Filter { input, predicate } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Filter predicate={predicate:?}\n{child}")
        }
        PlanNode::Project { input, fields } => {
            let names: Vec<String> = fields.iter().map(|f| {
                match &f.alias {
                    Some(a) => format!("{a}: {:?}", f.expr),
                    None => format!("{:?}", f.expr),
                }
            }).collect();
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Project fields=[{}]\n{child}", names.join(", "))
        }
        PlanNode::Sort { input, keys } => {
            let ks: Vec<String> = keys.iter().map(|k| {
                if k.descending { format!("{} desc", k.field) } else { k.field.clone() }
            }).collect();
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Sort keys=[{}]\n{child}", ks.join(", "))
        }
        PlanNode::Limit { input, count } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Limit count={count:?}\n{child}")
        }
        PlanNode::Offset { input, count } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Offset count={count:?}\n{child}")
        }
        PlanNode::Aggregate { input, function, field } => {
            let f = field.as_deref().unwrap_or("*");
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Aggregate fn={function:?} field={f}\n{child}")
        }
        PlanNode::NestedLoopJoin { left, right, on, kind } => {
            let left_child = format_plan_tree(left, depth + 1);
            let right_child = format_plan_tree(right, depth + 1);
            let on_str = match on {
                Some(pred) => format!("{pred:?}"),
                None => "none".to_string(),
            };
            format!(
                "{indent}NestedLoopJoin kind={kind:?} on={on_str}\n{left_child}\n{right_child}"
            )
        }
        PlanNode::Distinct { input } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Distinct\n{child}")
        }
        PlanNode::GroupBy { input, keys, aggregates, having } => {
            let agg_strs: Vec<String> = aggregates.iter().map(|a| {
                format!("{:?}({}) as {}", a.function, a.field, a.output_name)
            }).collect();
            let having_str = match having {
                Some(h) => format!(" having={h:?}"),
                None => String::new(),
            };
            let child = format_plan_tree(input, depth + 1);
            format!(
                "{indent}GroupBy keys=[{}] aggs=[{}]{having_str}\n{child}",
                keys.join(", "),
                agg_strs.join(", "),
            )
        }
        PlanNode::Insert { table, assignments } => {
            let cols: Vec<&str> = assignments.iter().map(|a| a.field.as_str()).collect();
            format!("{indent}Insert table={table} cols=[{}]", cols.join(", "))
        }
        PlanNode::Upsert { table, key_column, assignments, on_conflict } => {
            let cols: Vec<&str> = assignments.iter().map(|a| a.field.as_str()).collect();
            let conflict_cols: Vec<&str> = on_conflict.iter().map(|a| a.field.as_str()).collect();
            if conflict_cols.is_empty() {
                format!("{indent}Upsert table={table} key={key_column} cols=[{}]", cols.join(", "))
            } else {
                format!("{indent}Upsert table={table} key={key_column} cols=[{}] on_conflict=[{}]",
                    cols.join(", "), conflict_cols.join(", "))
            }
        }
        PlanNode::Update { input, table, assignments } => {
            let cols: Vec<&str> = assignments.iter().map(|a| a.field.as_str()).collect();
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Update table={table} set=[{}]\n{child}", cols.join(", "))
        }
        PlanNode::Delete { input, table } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Delete table={table}\n{child}")
        }
        PlanNode::CreateTable { name, fields } => {
            let fs: Vec<String> = fields.iter().map(|(n, t, r)| {
                if *r { format!("{n}: {t} required") } else { format!("{n}: {t}") }
            }).collect();
            format!("{indent}CreateTable name={name} fields=[{}]", fs.join(", "))
        }
        PlanNode::AlterTable { table, action } => {
            format!("{indent}AlterTable table={table} action={action:?}")
        }
        PlanNode::DropTable { name } => format!("{indent}DropTable name={name}"),
        PlanNode::CreateView { name, .. } => format!("{indent}CreateView name={name}"),
        PlanNode::RefreshView { name } => format!("{indent}RefreshView name={name}"),
        PlanNode::DropView { name } => format!("{indent}DropView name={name}"),
        PlanNode::Window { input, windows } => {
            let ws: Vec<String> = windows.iter().map(|w| {
                format!("{:?} as {}", w.function, w.output_name)
            }).collect();
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Window fns=[{}]\n{child}", ws.join(", "))
        }
        PlanNode::Union { left, right, all } => {
            let kind = if *all { "UNION ALL" } else { "UNION" };
            let left_child = format_plan_tree(left, depth + 1);
            let right_child = format_plan_tree(right, depth + 1);
            format!("{indent}{kind}\n{left_child}\n{right_child}")
        }
        PlanNode::Explain { input } => {
            let child = format_plan_tree(input, depth + 1);
            format!("{indent}Explain\n{child}")
        }
    }
}

/// Executor-local row layout — computes the layout facts the compiled
/// predicates and column readers need without touching the storage crate's
/// private `RowLayout` internals.
///
/// The row format is:
///   [length: u16][null_bitmap][fixed cols packed][var offset table: (n_var+1) u16s][var data]
struct FastLayout {
    /// Null bitmap size in bytes.
    bitmap_size: usize,
    /// Byte offset within the fixed region for each column (None = var-length).
    fixed_offsets: Vec<Option<usize>>,
    /// Size of the fixed region in bytes.
    fixed_region_size: usize,
    /// For each column: its slot index in the var-offset table (None = fixed).
    var_indices: Vec<Option<usize>>,
    /// Total number of variable-length columns.
    n_var: usize,
}

impl FastLayout {
    fn new(schema: &Schema) -> Self {
        let n_cols = schema.columns.len();
        let bitmap_size = n_cols.div_ceil(8);
        let mut fixed_offsets = vec![None; n_cols];
        let mut var_indices = vec![None; n_cols];
        let mut fixed_pos: usize = 0;
        let mut var_count: usize = 0;

        for (i, col) in schema.columns.iter().enumerate() {
            if is_fixed_size(col.type_id) {
                fixed_offsets[i] = Some(fixed_pos);
                fixed_pos += fixed_size(col.type_id).unwrap();
            } else {
                var_indices[i] = Some(var_count);
                var_count += 1;
            }
        }

        FastLayout {
            bitmap_size,
            fixed_offsets,
            fixed_region_size: fixed_pos,
            var_indices,
            n_var: var_count,
        }
    }

    /// Where the var-offset table starts within `data`.
    #[inline]
    fn var_offset_table_start(&self) -> usize {
        2 + self.bitmap_size + self.fixed_region_size
    }

    /// Where the var-data region starts within `data`.
    #[inline]
    fn var_data_start(&self) -> usize {
        self.var_offset_table_start() + (self.n_var + 1) * 2
    }
}

type CompiledPredicate = Box<dyn Fn(&[u8]) -> bool>;

/// Map an f64 bit pattern to a u64 that orders under unsigned integer
/// comparison the same way `f64::total_cmp` orders the floats. Classic
/// sortable-float transform:
///   - Positive floats (sign bit 0): flip the sign bit. This maps
///     [+0, +∞, +NaN] to [0x8000…, 0xFFF0…, 0xFFF8…] — increasing as u64.
///   - Negative floats (sign bit 1): flip every bit. This maps
///     [-∞, -0] to [0x000F…, 0x7FFF…] — increasing as u64, and placed
///     *below* the positive range so negatives < positives.
///
/// Used by Mission D10 Float fast paths so we can key heaps on `u64`
/// (branch-cheap, folds into LLVM xor/sar/xor) instead of a `TotalF64`
/// newtype with `Ord::cmp` calling `total_cmp`.
#[inline]
fn f64_bits_to_sortable_u64(bits: u64) -> u64 {
    // `((bits >> 63) as i64 * -1) as u64 | 0x8000_0000_0000_0000`
    // would also work; the branchless form below is equally good on
    // modern CPUs and easier to read.
    if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    }
}

/// A single flattened predicate leaf — pure data, no closures, no allocation
/// per call. Mission D3: replaces recursive Box<dyn Fn> conjunctions with a
/// `Vec<CompiledLeaf>` so the inner scan loop becomes a tight match instead
/// of N+1 vtable indirect calls per row.
enum CompiledLeaf {
    /// `.field <op> literal_int` (or reversed)
    Int {
        data_offset: usize,
        bitmap_byte: usize,
        bitmap_bit: u8,
        op: BinOp,
        literal: i64,
    },
    /// `.field <op> literal_float` (or reversed), where `.field` is a
    /// Float column. Int literals that bound a Float column (e.g.
    /// `.price > 100` on `price: float`) are also routed here, promoted
    /// to `f64` at compile time so the hot loop only sees one shape.
    /// Comparisons use `f64::total_cmp` so NaN handling is deterministic
    /// and consistent with `Value::Ord` across every read path.
    Float {
        data_offset: usize,
        bitmap_byte: usize,
        bitmap_bit: u8,
        op: BinOp,
        literal: f64,
    },
    /// `.field is null` or `.field is not null`
    IsNull {
        bitmap_byte: usize,
        bitmap_bit: u8,
        want_null: bool,
    },
    /// `.field = string_literal` or `.field != string_literal`
    StrEq {
        var_offset_table_start: usize,
        var_data_start: usize,
        var_idx: usize,
        bitmap_byte: usize,
        bitmap_bit: u8,
        negate: bool,
        needle: Vec<u8>,
    },
}

impl CompiledLeaf {
    /// Evaluate this leaf against a row's raw bytes. `#[inline]` so the
    /// match folds into the caller's tight loop with LTO.
    #[inline]
    fn eval(&self, data: &[u8]) -> bool {
        match self {
            CompiledLeaf::Int {
                data_offset,
                bitmap_byte,
                bitmap_bit,
                op,
                literal,
            } => {
                let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                if is_null {
                    return false;
                }
                let val = i64::from_le_bytes(
                    data[*data_offset..*data_offset + 8].try_into().unwrap(),
                );
                match op {
                    BinOp::Eq => val == *literal,
                    BinOp::Neq => val != *literal,
                    BinOp::Lt => val < *literal,
                    BinOp::Gt => val > *literal,
                    BinOp::Lte => val <= *literal,
                    BinOp::Gte => val >= *literal,
                    _ => false,
                }
            }
            CompiledLeaf::Float {
                data_offset,
                bitmap_byte,
                bitmap_bit,
                op,
                literal,
            } => {
                let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                if is_null {
                    return false;
                }
                let val = f64::from_le_bytes(
                    data[*data_offset..*data_offset + 8].try_into().unwrap(),
                );
                // `total_cmp` matches Value::Ord: NaN > everything,
                // -0.0 < +0.0, finite order as expected. Keeps compiled
                // WHERE identical in semantics to the generic row-decode
                // path (which calls Value::cmp directly).
                let ord = val.total_cmp(literal);
                match op {
                    BinOp::Eq => ord.is_eq(),
                    BinOp::Neq => !ord.is_eq(),
                    BinOp::Lt => ord.is_lt(),
                    BinOp::Gt => ord.is_gt(),
                    BinOp::Lte => !ord.is_gt(),
                    BinOp::Gte => !ord.is_lt(),
                    _ => false,
                }
            }
            CompiledLeaf::IsNull { bitmap_byte, bitmap_bit, want_null } => {
                let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                if *want_null { is_null } else { !is_null }
            }
            CompiledLeaf::StrEq {
                var_offset_table_start,
                var_data_start,
                var_idx,
                bitmap_byte,
                bitmap_bit,
                negate,
                needle,
            } => {
                let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                if is_null {
                    return false;
                }
                let off_pos = var_offset_table_start + var_idx * 2;
                let next_pos = var_offset_table_start + (var_idx + 1) * 2;
                let start = u16::from_le_bytes(data[off_pos..off_pos + 2].try_into().unwrap()) as usize;
                let end = u16::from_le_bytes(data[next_pos..next_pos + 2].try_into().unwrap()) as usize;
                let slice = &data[var_data_start + start..var_data_start + end];
                let eq = slice == needle.as_slice();
                if *negate { !eq } else { eq }
            }
        }
    }
}

/// Attempt to compile a predicate expression into a closure over raw row
/// bytes. Returns None if the predicate contains shapes we don't handle
/// (arithmetic, Or, Coalesce, non-literal comparands, etc.). Supported:
///   - `.field <op> literal_int` and its reversed form
///   - `.field = string_literal` / `string_literal = .field`
///   - `And` conjunctions of any number of the above
///
/// Mission D3: AND chains are flattened into a single `Vec<CompiledLeaf>`
/// closed over by ONE outer closure. The previous implementation built a
/// recursive `Box<Fn>` per AND combinator, costing N+1 indirect vtable
/// calls per row for an N-leaf conjunction. The flat version dispatches
/// each leaf via match (predictable branch, fully inlinable with LTO),
/// short-circuiting on the first failing leaf.
fn compile_predicate(
    expr: &Expr,
    columns: &[String],
    layout: &FastLayout,
    schema: &Schema,
) -> Option<CompiledPredicate> {
    let mut leaves: Vec<CompiledLeaf> = Vec::new();
    flatten_and_compile(expr, columns, layout, schema, &mut leaves)?;
    if leaves.is_empty() {
        return None;
    }
    if leaves.len() == 1 {
        // Single-leaf fast path: skip the Vec iteration entirely.
        let leaf = leaves.into_iter().next().unwrap();
        return Some(Box::new(move |data: &[u8]| leaf.eval(data)));
    }
    Some(Box::new(move |data: &[u8]| {
        // Tight short-circuit AND loop. With CompiledLeaf::eval marked
        // #[inline], LTO can fold the match arms into this loop body.
        for leaf in &leaves {
            if !leaf.eval(data) {
                return false;
            }
        }
        true
    }))
}

/// Recursively walk an AND chain and push each leaf into `out`. Returns
/// `None` if any sub-expression isn't a supported leaf shape.
fn flatten_and_compile(
    expr: &Expr,
    columns: &[String],
    layout: &FastLayout,
    schema: &Schema,
    out: &mut Vec<CompiledLeaf>,
) -> Option<()> {
    match expr {
        Expr::BinaryOp(left, BinOp::And, right) => {
            flatten_and_compile(left, columns, layout, schema, out)?;
            flatten_and_compile(right, columns, layout, schema, out)?;
            Some(())
        }
        Expr::BinaryOp(left, op, right) => {
            if let Some(leaf) = build_int_leaf(left, *op, right, columns, layout, schema) {
                out.push(leaf);
                return Some(());
            }
            if let Some(leaf) = build_float_leaf(left, *op, right, columns, layout, schema) {
                out.push(leaf);
                return Some(());
            }
            if let Some(leaf) = build_str_eq_leaf(left, *op, right, columns, layout, schema) {
                out.push(leaf);
                return Some(());
            }
            None
        }
        Expr::UnaryOp(op, inner) if *op == UnaryOp::IsNull || *op == UnaryOp::IsNotNull => {
            if let Expr::Field(name) = inner.as_ref() {
                let col_idx = columns.iter().position(|c| c == name)?;
                let bitmap_byte = col_idx / 8;
                let bitmap_bit = (col_idx % 8) as u8;
                let want_null = *op == UnaryOp::IsNull;
                out.push(CompiledLeaf::IsNull { bitmap_byte, bitmap_bit, want_null });
                Some(())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Build an `Int` leaf from `.field <op> literal_int` (or reversed).
///
/// Only fires for columns whose declared type is `TypeId::Int`. If the
/// column is a different numeric type (Float, DateTime) we return `None`
/// so the caller falls back to the generic `Value::cmp` evaluation path,
/// which correctly handles cross-type numeric comparison (e.g. Int literal
/// vs Float column in `BETWEEN 100 AND 500` on a `price: float` column).
/// Previously this function read 8 bytes of a Float column as little-endian
/// i64, producing nonsense comparisons.
fn build_int_leaf(
    left: &Expr,
    op: BinOp,
    right: &Expr,
    columns: &[String],
    layout: &FastLayout,
    schema: &Schema,
) -> Option<CompiledLeaf> {
    let (field_name, literal_val, op) = match (left, right) {
        (Expr::Field(name), Expr::Literal(Literal::Int(v))) => (name, *v, op),
        (Expr::Literal(Literal::Int(v)), Expr::Field(name)) => {
            let flipped = match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Gt => BinOp::Lt,
                BinOp::Lte => BinOp::Gte,
                BinOp::Gte => BinOp::Lte,
                other => other, // Eq, Neq are symmetric
            };
            (name, *v, flipped)
        }
        _ => return None,
    };

    let col_idx = columns.iter().position(|c| c == field_name)?;
    // Guard: the compiled Int leaf reads the column's 8 bytes as i64.
    // Only valid when the column is actually an Int column.
    if schema.columns[col_idx].type_id != TypeId::Int {
        return None;
    }
    let byte_offset = layout.fixed_offsets[col_idx]?;
    let bitmap_byte = col_idx / 8;
    let bitmap_bit = (col_idx % 8) as u8;
    let data_offset = 2 + layout.bitmap_size + byte_offset;

    Some(CompiledLeaf::Int {
        data_offset,
        bitmap_byte,
        bitmap_bit,
        op,
        literal: literal_val,
    })
}

/// Build a `Float` leaf from `.field <op> literal` where `.field` is a
/// Float column and `literal` is numeric (Float or Int — Int literals are
/// promoted to `f64` at compile time so the hot loop only sees one shape).
///
/// Mission D10: adds the Float fast-path counterpart to `build_int_leaf`.
/// Without this, `WHERE .price > 100.0` on a `price: float` column falls
/// through `compile_predicate`, forcing the whole query to the generic
/// `decode_row → Value::cmp` path which allocates a `Vec<Value>` per row.
fn build_float_leaf(
    left: &Expr,
    op: BinOp,
    right: &Expr,
    columns: &[String],
    layout: &FastLayout,
    schema: &Schema,
) -> Option<CompiledLeaf> {
    // Accept either direction: field-op-literal or literal-op-field.
    // When the literal is on the left, flip the operator so the hot-loop
    // eval can assume the field is always the LHS.
    let (field_name, literal_val, op) = match (left, right) {
        (Expr::Field(name), Expr::Literal(Literal::Float(v))) => (name, *v, op),
        (Expr::Field(name), Expr::Literal(Literal::Int(v)))   => (name, *v as f64, op),
        (Expr::Literal(Literal::Float(v)), Expr::Field(name)) => {
            let flipped = match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Gt => BinOp::Lt,
                BinOp::Lte => BinOp::Gte,
                BinOp::Gte => BinOp::Lte,
                other => other,
            };
            (name, *v, flipped)
        }
        (Expr::Literal(Literal::Int(v)), Expr::Field(name)) => {
            let flipped = match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Gt => BinOp::Lt,
                BinOp::Lte => BinOp::Gte,
                BinOp::Gte => BinOp::Lte,
                other => other,
            };
            (name, *v as f64, flipped)
        }
        _ => return None,
    };

    let col_idx = columns.iter().position(|c| c == field_name)?;
    // Symmetric guard to build_int_leaf: only fire on Float columns. If
    // the column is Int but the literal was Float, we want the generic
    // path (which promotes Int → f64 via Value::cmp) — compiling a
    // Float leaf would read the i64 bytes as f64 and produce nonsense.
    if schema.columns[col_idx].type_id != TypeId::Float {
        return None;
    }
    let byte_offset = layout.fixed_offsets[col_idx]?;
    let bitmap_byte = col_idx / 8;
    let bitmap_bit = (col_idx % 8) as u8;
    let data_offset = 2 + layout.bitmap_size + byte_offset;

    Some(CompiledLeaf::Float {
        data_offset,
        bitmap_byte,
        bitmap_bit,
        op,
        literal: literal_val,
    })
}

/// Build a `StrEq` leaf from `.field = string_literal` (or reversed).
fn build_str_eq_leaf(
    left: &Expr,
    op: BinOp,
    right: &Expr,
    columns: &[String],
    layout: &FastLayout,
    schema: &Schema,
) -> Option<CompiledLeaf> {
    if op != BinOp::Eq && op != BinOp::Neq {
        return None;
    }
    let (field_name, literal_str) = match (left, right) {
        (Expr::Field(name), Expr::Literal(Literal::String(s))) => (name, s.clone()),
        (Expr::Literal(Literal::String(s)), Expr::Field(name)) => (name, s.clone()),
        _ => return None,
    };

    let col_idx = columns.iter().position(|c| c == field_name)?;
    if schema.columns[col_idx].type_id != TypeId::Str {
        return None;
    }
    let var_idx = layout.var_indices[col_idx]?;
    let var_offset_table_start = layout.var_offset_table_start();
    let var_data_start = layout.var_data_start();
    let bitmap_byte = col_idx / 8;
    let bitmap_bit = (col_idx % 8) as u8;
    let negate = op == BinOp::Neq;

    Some(CompiledLeaf::StrEq {
        var_offset_table_start,
        var_data_start,
        var_idx,
        bitmap_byte,
        bitmap_bit,
        negate,
        needle: literal_str.into_bytes(),
    })
}

/// Collect the column indices referenced by a predicate expression.
fn predicate_column_indices(expr: &Expr, columns: &[String]) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_field_indices(expr, columns, &mut indices);
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn collect_field_indices(expr: &Expr, columns: &[String], out: &mut Vec<usize>) {
    match expr {
        Expr::Field(name) => {
            if let Some(idx) = columns.iter().position(|c| c == name) {
                out.push(idx);
            }
        }
        Expr::BinaryOp(left, _, right) => {
            collect_field_indices(left, columns, out);
            collect_field_indices(right, columns, out);
        }
        Expr::Coalesce(left, right) => {
            collect_field_indices(left, columns, out);
            collect_field_indices(right, columns, out);
        }
        Expr::UnaryOp(_, inner) => {
            collect_field_indices(inner, columns, out);
        }
        Expr::FunctionCall(_, inner) => {
            collect_field_indices(inner, columns, out);
        }
        Expr::ScalarFunc(_, args) => {
            for arg in args {
                collect_field_indices(arg, columns, out);
            }
        }
        Expr::Cast(inner, _) => {
            collect_field_indices(inner, columns, out);
        }
        Expr::Case { whens, else_expr } => {
            for (cond, result) in whens {
                collect_field_indices(cond, columns, out);
                collect_field_indices(result, columns, out);
            }
            if let Some(e) = else_expr {
                collect_field_indices(e, columns, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_field_indices(expr, columns, out);
            for item in list {
                collect_field_indices(item, columns, out);
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_field_indices(expr, columns, out);
        }
        _ => {}
    }
}

/// Decode only the specified columns from raw row bytes, filling the rest
/// with `Value::Empty`. This avoids heap allocations for String/Bytes
/// columns that the predicate doesn't reference.
fn decode_selective(schema: &Schema, layout: &RowLayout, data: &[u8], col_indices: &[usize]) -> Vec<Value> {
    let n_cols = schema.columns.len();
    let mut values = vec![Value::Empty; n_cols];
    for &ci in col_indices {
        values[ci] = decode_column(schema, layout, data, ci);
    }
    values
}

fn eval_binop(left: &Value, op: BinOp, right: &Value) -> Value {
    match op {
        BinOp::Eq  => Value::Bool(left == right),
        BinOp::Neq => Value::Bool(left != right),
        BinOp::Lt  => Value::Bool(left < right),
        BinOp::Gt  => Value::Bool(left > right),
        BinOp::Lte => Value::Bool(left <= right),
        BinOp::Gte => Value::Bool(left >= right),
        BinOp::And => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
            _ => Value::Bool(false),
        },
        BinOp::Or => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
            _ => Value::Bool(false),
        },
        BinOp::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.saturating_add(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + *b as f64),
            _ => Value::Empty,
        },
        BinOp::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.saturating_sub(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 - b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a - *b as f64),
            _ => Value::Empty,
        },
        BinOp::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.saturating_mul(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 * b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a * *b as f64),
            _ => Value::Empty,
        },
        BinOp::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a / b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a / b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 / b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a / *b as f64),
            _ => Value::Empty,
        },
        BinOp::Like => match (left, right) {
            (Value::Str(text), Value::Str(pattern)) => Value::Bool(like_match(text, pattern)),
            _ => Value::Bool(false),
        },
    }
}

/// SQL LIKE pattern match. `%` matches any sequence (including empty),
/// `_` matches exactly one character. No escape character for now.
fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_dp(&t, &p, 0, 0)
}

fn like_dp(t: &[char], p: &[char], ti: usize, pi: usize) -> bool {
    if pi == p.len() {
        return ti == t.len();
    }
    if p[pi] == '%' {
        // '%' can match zero or more characters — try both.
        // Skip consecutive '%' to avoid exponential blowup.
        let mut pi2 = pi;
        while pi2 < p.len() && p[pi2] == '%' {
            pi2 += 1;
        }
        for i in ti..=t.len() {
            if like_dp(t, p, i, pi2) {
                return true;
            }
        }
        false
    } else if ti < t.len() && (p[pi] == '_' || p[pi] == t[ti]) {
        like_dp(t, p, ti + 1, pi + 1)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn test_engine() -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_exec_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql("type User { required name: str, required email: str, age: int }").unwrap();
        engine.execute_powql(r#"insert User { name := "Alice", email := "alice@ex.com", age := 30 }"#).unwrap();
        engine.execute_powql(r#"insert User { name := "Bob", email := "bob@ex.com", age := 25 }"#).unwrap();
        engine.execute_powql(r#"insert User { name := "Charlie", email := "charlie@ex.com", age := 35 }"#).unwrap();
        engine
    }

    #[test]
    fn test_scan_all() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User filter .age > 28").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // Alice (30) and Charlie (35)
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User { name }").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name"]);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_insert_and_count() {
        let mut engine = test_engine();
        let result = engine.execute_powql("count(User)").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 3),
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_update() {
        let mut engine = test_engine();
        engine.execute_powql(r#"User filter .name = "Alice" update { age := 31 }"#).unwrap();
        let result = engine.execute_powql(r#"User filter .name = "Alice" { name, age }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][1], Value::Int(31));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_delete() {
        let mut engine = test_engine();
        engine.execute_powql(r#"User filter .name = "Bob" delete"#).unwrap();
        let result = engine.execute_powql("count(User)").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 2),
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_order_limit() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User order .age desc limit 2 { name, age }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("Charlie".into())); // age 35
                assert_eq!(rows[1][0], Value::Str("Alice".into()));   // age 30
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── LIMIT / OFFSET combined semantics ──────────────────────────────────
    //
    // SQL/PowQL semantics: offset skips M rows first, then limit takes N rows.
    // `limit 3 offset 1` on 5 rows must return rows 1..4 (three rows), not
    // `N - M` rows. These regression tests pin the plan-shape ordering that
    // previously had Offset wrapping Limit (so Limit capped at N rows and
    // Offset then skipped M of those, yielding N - M).

    /// 5-row Product fixture with an `id` column we can order on.
    fn product_engine() -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_limit_offset_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql("type Product { required id: int, required name: str }").unwrap();
        for i in 0..5i64 {
            let q = format!(r#"insert Product {{ id := {i}, name := "p{i}" }}"#);
            engine.execute_powql(&q).unwrap();
        }
        engine
    }

    #[test]
    fn test_limit_offset_combined() {
        // 5 rows, `limit 3 offset 1` → exactly 3 rows, ids [1, 2, 3] when
        // ordered by id. We order by id to pin the row identity; without
        // an order by, insertion order is implementation-defined.
        let mut engine = product_engine();
        let result = engine
            .execute_powql("Product order .id limit 3 offset 1 { .id }")
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3, "limit 3 offset 1 on 5 rows must return 3 rows");
                assert_eq!(rows[0][0], Value::Int(1));
                assert_eq!(rows[1][0], Value::Int(2));
                assert_eq!(rows[2][0], Value::Int(3));
            }
            _ => panic!("expected rows"),
        }

        // `limit 2 offset 1` → exactly 2 rows, ids [1, 2].
        let result = engine
            .execute_powql("Product order .id limit 2 offset 1 { .id }")
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "limit 2 offset 1 on 5 rows must return 2 rows");
                assert_eq!(rows[0][0], Value::Int(1));
                assert_eq!(rows[1][0], Value::Int(2));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_limit_offset_combined_with_order() {
        // Same semantics but ordering on a string column. Names are p0..p4,
        // so sort order is identical to id order.
        let mut engine = product_engine();
        let result = engine
            .execute_powql("Product order .name limit 3 offset 1 { .name }")
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::Str("p1".into()));
                assert_eq!(rows[1][0], Value::Str("p2".into()));
                assert_eq!(rows[2][0], Value::Str("p3".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_offset_then_limit_keyword_order() {
        // Parser accepts limit/offset in either order — verify the plan
        // semantics are identical regardless of keyword order.
        let mut engine = product_engine();
        let result = engine
            .execute_powql("Product order .id offset 1 limit 3 { .id }")
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::Int(1));
                assert_eq!(rows[1][0], Value::Int(2));
                assert_eq!(rows[2][0], Value::Int(3));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Mission A fast-path tests ──────────────────────────────────────────
    //
    // Fixture: Mission A workload schema — the same User shape used by
    // crates/compare. Deterministic generator so expected values are
    // computable directly in the test without reimplementing the interpreter.

    /// Build a Mission A User table with `n` rows and an index on id.
    /// Row i (0-indexed, id = i):
    ///   id        = i
    ///   name      = format!("user_{i}")
    ///   age       = 18 + (i % 60)
    ///   status    = ["active","inactive","pending"][i % 3]
    ///   email     = format!("user_{i}@example.com")
    ///   created_at= 1_700_000_000 + i
    fn mission_a_engine(n: i64) -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_mission_a_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type User { required id: int, required name: str, required age: int, \
             required status: str, required email: str, required created_at: int }"
        ).unwrap();
        engine.catalog_mut().create_index("User", "id").unwrap();
        let statuses = ["active", "inactive", "pending"];
        for i in 0..n {
            let age = 18 + (i % 60);
            let status = statuses[(i as usize) % 3];
            let created_at = 1_700_000_000_i64 + i;
            let q = format!(
                r#"insert User {{ id := {i}, name := "user_{i}", age := {age}, status := "{status}", email := "user_{i}@example.com", created_at := {created_at} }}"#
            );
            engine.execute_powql(&q).unwrap();
        }
        engine
    }

    #[test]
    fn test_fastpath_point_lookup_nonindexed() {
        // `.email = literal` has no index — must short-circuit via compiled
        // predicate on the first match.
        let mut engine = mission_a_engine(50);
        let result = engine.execute_powql(r#"User filter .email = "user_17@example.com""#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                // id column is position 0
                assert_eq!(rows[0][0], Value::Int(17));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_fastpath_scan_filter_project_top100() {
        // Project(Limit(Filter(SeqScan))) — stream, stop at 100.
        let mut engine = mission_a_engine(1000);
        let result = engine.execute_powql(
            "User filter .age > 30 limit 100 { .id, .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 100);
                // All rows must have age > 30 (age = 18 + (id % 60))
                // Verify via id: 18 + (id % 60) > 30  <=>  id % 60 > 12
                for row in &rows {
                    if let Value::Int(id) = row[0] {
                        assert!(18 + (id % 60) > 30, "id={id} has age={}", 18 + (id % 60));
                    } else {
                        panic!("expected int id");
                    }
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_fastpath_scan_filter_sort_limit10_desc() {
        // Project(Limit(Sort(Filter(SeqScan)))) — bounded top-N heap desc.
        let mut engine = mission_a_engine(500);
        let result = engine.execute_powql(
            "User filter .age > 20 order .created_at desc limit 10 { .id, .created_at }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 10);
                // Must be monotonically non-increasing in created_at.
                let keys: Vec<i64> = rows.iter().map(|r| {
                    if let Value::Int(v) = r[1] { v } else { panic!("expected int"); }
                }).collect();
                for w in keys.windows(2) {
                    assert!(w[0] >= w[1], "not desc sorted: {keys:?}");
                }
                // Highest created_at is id=499 (created_at=1_700_000_499),
                // age=18+(499%60)=37 which is > 20, so id=499 must be first.
                assert_eq!(rows[0][0], Value::Int(499));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_fastpath_scan_filter_sort_limit10_asc() {
        let mut engine = mission_a_engine(500);
        let result = engine.execute_powql(
            "User filter .age > 20 order .created_at limit 10 { .id, .created_at }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 10);
                let keys: Vec<i64> = rows.iter().map(|r| {
                    if let Value::Int(v) = r[1] { v } else { panic!("expected int"); }
                }).collect();
                for w in keys.windows(2) {
                    assert!(w[0] <= w[1], "not asc sorted: {keys:?}");
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_fastpath_agg_sum() {
        // sum over all rows of the age column. Deterministic expected value.
        let n: i64 = 300;
        let mut engine = mission_a_engine(n);
        let result = engine.execute_powql("sum(User { .age })").unwrap();
        let expected: i64 = (0..n).map(|i| 18 + (i % 60)).sum();
        match result {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, expected),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn test_fastpath_agg_sum_with_filter() {
        let n: i64 = 300;
        let mut engine = mission_a_engine(n);
        let result = engine.execute_powql("sum(User filter .age > 30 { .age })").unwrap();
        let expected: i64 = (0..n).map(|i| 18 + (i % 60)).filter(|a| *a > 30).sum();
        match result {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, expected),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn test_fastpath_agg_avg() {
        let n: i64 = 300;
        let mut engine = mission_a_engine(n);
        let result = engine.execute_powql("avg(User { .age })").unwrap();
        let total: f64 = (0..n).map(|i| (18 + (i % 60)) as f64).sum();
        let expected = total / n as f64;
        match result {
            QueryResult::Scalar(Value::Float(v)) => {
                assert!((v - expected).abs() < 1e-9, "expected {expected}, got {v}");
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn test_fastpath_agg_min_max() {
        let n: i64 = 300;
        let mut engine = mission_a_engine(n);
        // age = 18 + (i % 60), so min=18 and max=77 (18+59)
        let result_min = engine.execute_powql("min(User { .age })").unwrap();
        match result_min {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, 18),
            other => panic!("expected Int, got {other:?}"),
        }
        let result_max = engine.execute_powql("max(User { .age })").unwrap();
        match result_max {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, 77),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn test_fastpath_multi_col_and_filter() {
        // AND of int > and string = — both must be compiled into one closure.
        let n: i64 = 300;
        let mut engine = mission_a_engine(n);
        let result = engine.execute_powql(
            r#"count(User filter .age > 30 and .status = "active")"#
        ).unwrap();
        // Expected count via the same deterministic generator.
        let statuses = ["active", "inactive", "pending"];
        let expected = (0..n).filter(|i| {
            let age = 18 + (i % 60);
            let status = statuses[(*i as usize) % 3];
            age > 30 && status == "active"
        }).count() as i64;
        match result {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, expected),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn test_fastpath_update_by_pk() {
        // Update(IndexScan) — single-row mutation via B-tree lookup.
        let mut engine = mission_a_engine(50);
        let result = engine.execute_powql(
            "User filter .id = 25 update { age := 99 }"
        ).unwrap();
        match result {
            QueryResult::Modified(n) => assert_eq!(n, 1),
            _ => panic!("expected Modified"),
        }
        // Verify the row has the new age.
        let lookup = engine.execute_powql("User filter .id = 25 { .age }").unwrap();
        match lookup {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int(99));
            }
            _ => panic!("expected rows"),
        }
        // Verify no neighbouring rows were touched.
        let neighbour = engine.execute_powql("User filter .id = 24 { .age }").unwrap();
        if let QueryResult::Rows { rows, .. } = neighbour {
            assert_eq!(rows[0][0], Value::Int(42));
        }
    }

    #[test]
    fn test_fastpath_update_by_filter_single_pass() {
        // Regression test for the O(N*M) bug: update by a range filter must
        // not take quadratic time. We can't directly assert timing, but we
        // can assert correctness and that the call completes for a
        // reasonably-sized table (the old path at N=2000 was ~40M row-eq
        // comparisons; the new path is O(N)).
        let n: i64 = 2000;
        let mut engine = mission_a_engine(n);
        let result = engine.execute_powql(
            "User filter .age > 50 update { age := 5 }"
        ).unwrap();
        let expected = (0..n).filter(|i| 18 + (i % 60) > 50).count() as u64;
        match result {
            QueryResult::Modified(nn) => assert_eq!(nn, expected),
            _ => panic!("expected Modified"),
        }
        // Every row that matched the filter now has age=5. We verify both
        // directions:
        //   (a) no rows remain with age > 50 (the filter predicate)
        //   (b) count(age = 5) equals the number of rows we updated
        // Note: the original generator never produces age=5, so count(age=5)
        // is exactly the number of updated rows.
        let check_zero = engine.execute_powql(r#"count(User filter .age > 50)"#).unwrap();
        match check_zero {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, 0, "some rows still have age > 50"),
            _ => panic!("expected Int"),
        }
        let check_five = engine.execute_powql(r#"count(User filter .age = 5)"#).unwrap();
        match check_five {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v as u64, expected),
            _ => panic!("expected Int"),
        }
        // Total row count unchanged.
        let total = engine.execute_powql("count(User)").unwrap();
        match total {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v, n),
            _ => panic!("expected Int"),
        }
    }

    #[test]
    fn test_fastpath_delete_by_filter_single_pass() {
        let n: i64 = 2000;
        let mut engine = mission_a_engine(n);
        let to_delete = (0..n).filter(|i| 18 + (i % 60) > 60).count() as u64;
        let result = engine.execute_powql(
            "User filter .age > 60 delete"
        ).unwrap();
        match result {
            QueryResult::Modified(nn) => assert_eq!(nn, to_delete),
            _ => panic!("expected Modified"),
        }
        let count = engine.execute_powql("count(User)").unwrap();
        match count {
            QueryResult::Scalar(Value::Int(v)) => assert_eq!(v as u64, n as u64 - to_delete),
            _ => panic!("expected Int"),
        }
    }

    #[test]
    fn test_fastpath_delete_by_pk() {
        let mut engine = mission_a_engine(30);
        let result = engine.execute_powql("User filter .id = 7 delete").unwrap();
        match result {
            QueryResult::Modified(n) => assert_eq!(n, 1),
            _ => panic!("expected Modified"),
        }
        // The deleted row must be gone.
        let lookup = engine.execute_powql("User filter .id = 7").unwrap();
        match lookup {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected rows"),
        }
        // Neighbours still present.
        let other = engine.execute_powql("User filter .id = 8 { .id }").unwrap();
        match other {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int(8));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_fastpath_update_by_filter_matches_generic() {
        // Cross-check: running the fast-path update and counting the
        // modified rows must agree with counting matching rows via a
        // separate query. This catches off-by-one bugs in rid collection.
        let n: i64 = 500;
        let mut engine = mission_a_engine(n);
        let count_before = engine.execute_powql(
            r#"count(User filter .status = "active")"#
        ).unwrap();
        let expected_count = match count_before {
            QueryResult::Scalar(Value::Int(v)) => v as u64,
            _ => panic!("expected Int"),
        };

        let upd = engine.execute_powql(
            r#"User filter .status = "active" update { age := 42 }"#
        ).unwrap();
        match upd {
            QueryResult::Modified(n) => assert_eq!(n, expected_count),
            _ => panic!("expected Modified"),
        }

        // All "active" rows now have age = 42.
        let count_after = engine.execute_powql(
            r#"count(User filter .age = 42)"#
        ).unwrap();
        match count_after {
            QueryResult::Scalar(Value::Int(v)) => {
                // Some non-active rows may also happen to have age = 42 from
                // the original schedule (age = 18 + (i % 60) == 42 when
                // i % 60 == 24). So we assert >= expected_count.
                assert!(v as u64 >= expected_count);
            }
            _ => panic!("expected Int"),
        }
    }

    // ── Mission C Phase 5: prepared statements ────────────────────

    #[test]
    fn test_prepared_insert_reuses_template() {
        let mut engine = test_engine();
        let prep = engine.prepare(
            r#"insert User { name := "seed", email := "seed@ex.com", age := 0 }"#
        ).expect("prepare");
        // The template has 3 literal slots: name, email, age.
        assert_eq!(prep.param_count, 3);

        for i in 0..5 {
            engine.execute_prepared(&prep, &[
                Literal::String(format!("user{i}")),
                Literal::String(format!("u{i}@ex.com")),
                Literal::Int(20 + i as i64),
            ]).expect("execute_prepared");
        }

        // 3 seeded + 5 prepared inserts = 8 rows.
        let count = engine.execute_powql("count(User)").unwrap();
        match count {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 8),
            _ => panic!("expected scalar"),
        }
    }

    #[test]
    fn test_prepared_update_by_pk() {
        let mut engine = test_engine();
        let prep = engine.prepare(
            r#"User filter .name = "seed" update { age := 0 }"#
        ).expect("prepare");
        // Two slots: filter literal "seed" + assignment literal 0.
        assert_eq!(prep.param_count, 2);

        engine.execute_prepared(&prep, &[
            Literal::String("Alice".into()),
            Literal::Int(99),
        ]).expect("execute_prepared");

        let result = engine.execute_powql(
            r#"User filter .name = "Alice" { age }"#
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(99));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_prepared_wrong_arity_errors() {
        let mut engine = test_engine();
        let prep = engine.prepare(
            r#"User filter .age > 0 { name }"#
        ).expect("prepare");
        assert_eq!(prep.param_count, 1);
        let err = engine.execute_prepared(&prep, &[]).unwrap_err();
        assert!(err.contains("expects 1 literal"));
    }

    // ─── Mission E1.2 join executor tests ───────────────────────────────────
    //
    // Fixture: two-table User + Order schema. User has 3 rows; Order has 4
    // rows referencing users 1 and 2 (plus one orphan user_id 99 so we can
    // probe LEFT OUTER semantics). Charlie (user 3) has no orders.

    fn join_engine() -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_join_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type User { required id: int, required name: str }"
        ).unwrap();
        engine.execute_powql(
            "type Order { required id: int, required user_id: int, required total: int }"
        ).unwrap();
        engine.execute_powql(r#"insert User { id := 1, name := "Alice" }"#).unwrap();
        engine.execute_powql(r#"insert User { id := 2, name := "Bob" }"#).unwrap();
        engine.execute_powql(r#"insert User { id := 3, name := "Charlie" }"#).unwrap();
        engine.execute_powql(r#"insert Order { id := 10, user_id := 1, total := 100 }"#).unwrap();
        engine.execute_powql(r#"insert Order { id := 11, user_id := 1, total := 200 }"#).unwrap();
        engine.execute_powql(r#"insert Order { id := 12, user_id := 2, total := 50  }"#).unwrap();
        engine.execute_powql(r#"insert Order { id := 13, user_id := 99, total := 999 }"#).unwrap();
        engine
    }

    #[test]
    fn test_inner_join_matches_rows() {
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u join Order as o on u.id = o.user_id",
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                // 3 matches: Alice has 2 orders, Bob has 1. Charlie + orphan
                // are dropped under INNER semantics.
                assert_eq!(rows.len(), 3);
                // Columns are concatenated alias.field for both sides.
                assert!(columns.contains(&"u.id".to_string()));
                assert!(columns.contains(&"u.name".to_string()));
                assert!(columns.contains(&"o.id".to_string()));
                assert!(columns.contains(&"o.user_id".to_string()));
                assert!(columns.contains(&"o.total".to_string()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_inner_join_with_qualified_projection_and_filter() {
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u join Order as o on u.id = o.user_id \
             filter o.total > 75 { u.name, o.total }",
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["u.name", "o.total"]);
                // Alice/100, Alice/200 (Bob's 50 filtered out).
                assert_eq!(rows.len(), 2);
                let names: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
                assert!(names.iter().all(|v| matches!(v, Value::Str(s) if s == "Alice")));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_left_outer_join_emits_orphan_left_rows() {
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u left join Order as o on u.id = o.user_id",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                // Alice(2) + Bob(1) + Charlie(padding) = 4 rows.
                assert_eq!(rows.len(), 4);
                // Find Charlie's row and verify the right-side columns are Empty.
                let u_name_idx = columns.iter().position(|c| c == "u.name").unwrap();
                let o_total_idx = columns.iter().position(|c| c == "o.total").unwrap();
                let charlie = rows.iter().find(|r| {
                    matches!(&r[u_name_idx], Value::Str(s) if s == "Charlie")
                }).expect("Charlie row present");
                assert_eq!(charlie[o_total_idx], Value::Empty);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_right_outer_join_emits_orphan_right_rows() {
        let mut engine = join_engine();
        // The orphan order (user_id = 99) has no matching User; RIGHT OUTER
        // should still emit it with the left-side (User) columns as Empty.
        let result = engine.execute_powql(
            "User as u right join Order as o on u.id = o.user_id",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                // All 4 orders appear (3 matched + 1 orphan).
                assert_eq!(rows.len(), 4);
                let u_name_idx = columns.iter().position(|c| c == "u.name").unwrap();
                let o_total_idx = columns.iter().position(|c| c == "o.total").unwrap();
                let orphan = rows.iter().find(|r| r[o_total_idx] == Value::Int(999))
                    .expect("orphan order row present");
                assert_eq!(orphan[u_name_idx], Value::Empty);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_cross_join_emits_full_product() {
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u cross join Order as o",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3 * 4);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_hash_join_handles_swapped_predicate_orientation() {
        // `on o.user_id = u.id` should resolve the same as `u.id = o.user_id`
        // — exercises the swapped-orientation branch in
        // `try_extract_equi_join_keys`.
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u join Order as o on o.user_id = u.id { u.name, o.total }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                assert_eq!(columns, vec!["u.name", "o.total"]);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_non_equi_join_falls_back_to_nested_loop() {
        // `u.id < o.user_id` isn't an equi-join, so the executor must
        // drop into the nested-loop path and still return correct rows.
        let mut engine = join_engine();
        let result = engine.execute_powql(
            "User as u join Order as o on u.id < o.user_id",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                // Pairs where u.id < o.user_id:
                //   User 1 < orders 2,99 = 2 rows (o.user_id=2 twice? no, only one order for user 2)
                //   Actually: orders have user_ids [1,1,2,99].
                //   User 1 (id=1): 1<1 no, 1<1 no, 1<2 yes, 1<99 yes → 2
                //   User 2 (id=2): 2<1 no, 2<1 no, 2<2 no, 2<99 yes → 1
                //   User 3 (id=3): 3<1 no, 3<1 no, 3<2 no, 3<99 yes → 1
                // Total 4.
                assert_eq!(rows.len(), 4);
                let u_id_idx = columns.iter().position(|c| c == "u.id").unwrap();
                let o_uid_idx = columns.iter().position(|c| c == "o.user_id").unwrap();
                for row in &rows {
                    match (&row[u_id_idx], &row[o_uid_idx]) {
                        (Value::Int(u), Value::Int(o)) => assert!(u < o),
                        _ => panic!("expected int columns"),
                    }
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_hash_join_with_string_key() {
        // Exercise the Value::Str hash path — plus verifies Hash impl for
        // Value works end to end via FxHashMap.
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir()
            .join(format!("powdb_strjoin_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type A { required code: str, required label: str }"
        ).unwrap();
        engine.execute_powql(
            "type B { required code: str, required score: int }"
        ).unwrap();
        engine.execute_powql(r#"insert A { code := "x", label := "X-label" }"#).unwrap();
        engine.execute_powql(r#"insert A { code := "y", label := "Y-label" }"#).unwrap();
        engine.execute_powql(r#"insert B { code := "x", score := 100 }"#).unwrap();
        engine.execute_powql(r#"insert B { code := "y", score := 200 }"#).unwrap();
        engine.execute_powql(r#"insert B { code := "z", score := 300 }"#).unwrap();

        let result = engine.execute_powql(
            "A as a join B as b on a.code = b.code { a.label, b.score }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // x→100, y→200. z has no matching A.
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_multi_join_chain() {
        // Third source — verify left-deep chains compose correctly.
        let mut engine = join_engine();
        engine.execute_powql(
            "type Product { required id: int, required name: str }"
        ).unwrap();
        engine.execute_powql(r#"insert Product { id := 100, name := "Widget" }"#).unwrap();
        engine.execute_powql(r#"insert Product { id := 200, name := "Gadget" }"#).unwrap();
        // Re-create Orders with a product_id column wouldn't work without
        // table alter; instead we pick a test that exercises the shape only.
        let result = engine.execute_powql(
            "User as u join Order as o on u.id = o.user_id \
             cross join Product as p",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                // 3 inner matches × 2 products = 6 rows.
                assert_eq!(rows.len(), 6);
                assert!(columns.contains(&"u.name".to_string()));
                assert!(columns.contains(&"o.total".to_string()));
                assert!(columns.contains(&"p.name".to_string()));
            }
            _ => panic!("expected rows"),
        }
    }

    // ---- Mission E2a: DISTINCT + IN-list + BETWEEN + LIKE -----------------

    #[test]
    fn test_distinct_deduplicates_rows() {
        let mut engine = test_engine();
        // Insert a second Alice to create a duplicate name.
        engine.execute_powql(
            r#"insert User { name := "Alice", email := "alice2@ex.com", age := 25 }"#,
        ).unwrap();
        let result = engine.execute_powql("User distinct { .name }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let names: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
                // 4 rows in table (Alice×2, Bob, Charlie) but 3 distinct names.
                assert_eq!(names.len(), 3);
                let alice_count = names.iter()
                    .filter(|v| matches!(v, Value::Str(s) if s == "Alice"))
                    .count();
                assert_eq!(alice_count, 1);
                assert!(names.iter().any(|v| matches!(v, Value::Str(s) if s == "Bob")));
                assert!(names.iter().any(|v| matches!(v, Value::Str(s) if s == "Charlie")));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_in_list_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter .name in ("Alice", "Bob") { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_not_in_list_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter .name not in ("Alice") { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Bob and Charlie survive.
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_between_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            "User filter .age between 25 and 30 { .name, .age }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Alice is 30 (inclusive), Bob is 25 (inclusive).
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_between_filter_float_column_int_literals() {
        // Regression for Value::Ord cross-type bug: BETWEEN on a Float column
        // with Int literals previously returned zero rows because Ord fell
        // through to TypeId discriminant comparison instead of promoting Int
        // to f64. Verifies the fix end-to-end through the query engine.
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_exec_between_float_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql("type Product { required name: str, required price: float }").unwrap();
        engine.execute_powql(r#"insert Product { name := "Cable",   price := 29.0 }"#).unwrap();
        engine.execute_powql(r#"insert Product { name := "Speaker", price := 175.5 }"#).unwrap();
        engine.execute_powql(r#"insert Product { name := "Monitor", price := 450.0 }"#).unwrap();
        engine.execute_powql(r#"insert Product { name := "Laptop",  price := 1299.0 }"#).unwrap();

        let result = engine.execute_powql(
            "Product filter .price between 100 and 500 { .name, .price }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "expected 2 rows in [100, 500] range, got {}: {:?}", rows.len(), rows);
                // Sorted by insert order: Speaker (175.5), Monitor (450.0).
                let names: Vec<&str> = rows.iter().map(|r| match &r[0] {
                    Value::Str(s) => s.as_str(),
                    _ => panic!("expected string name"),
                }).collect();
                assert!(names.contains(&"Speaker"));
                assert!(names.contains(&"Monitor"));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_not_between_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            "User filter .age not between 26 and 29 { .name }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Alice (30), Bob (25), Charlie (35) all outside [26,29].
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_like_prefix_match() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter .name like "Ali%" { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(matches!(&rows[0][0], Value::Str(s) if s == "Alice"));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_like_wildcard_underscore() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter .name like "_ob" { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(matches!(&rows[0][0], Value::Str(s) if s == "Bob"));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_not_like_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter .name not like "A%" { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Bob and Charlie survive (don't start with A).
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_in_list_with_integers() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            "User filter .age in (25, 30) { .name }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_like_full_match() {
        let mut engine = test_engine();
        // Exact match (no wildcards).
        let result = engine.execute_powql(
            r#"User filter .name like "Alice" { .name }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Mission E2b: GROUP BY + HAVING ────────────────────────────────────

    #[test]
    fn test_group_by_count() {
        // All 3 users share the same "age bucket" when we group by a
        // derived column, but we can at least group by a column with
        // distinct values. test_engine has 3 distinct names.
        let mut engine = test_engine();
        let result = engine.execute_powql(
            "User group .name { .name, n: count(.name) }",
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "n"]);
                assert_eq!(rows.len(), 3); // 3 distinct names
                // Each group has 1 row.
                for row in &rows {
                    assert_eq!(row[1], Value::Int(1));
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_sum_avg() {
        // Group all rows into one bucket by a constant column.
        // We'll use the mission_a_engine with a known shape.
        let mut engine = test_engine();
        // All 3 users: ages 30, 25, 35 → sum=90, avg=30.0
        let result = engine.execute_powql(
            "User group .email { .email, total_age: sum(.age) }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Each email is unique → 3 groups, each with sum of one age.
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_with_filter() {
        let mut engine = test_engine();
        // Filter first, then group.
        let result = engine.execute_powql(
            "User filter .age >= 30 group .name { .name, n: count(.name) }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Alice (30) and Charlie (35) survive filter.
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_having() {
        // Use mission_a_engine so we have multiple rows per group.
        let mut engine = mission_a_engine(30);
        // 30 rows: statuses cycle active/inactive/pending → 10 each.
        // Group by status, HAVING count > 5.
        let result = engine.execute_powql(
            "User group .status having count(.name) > 5 { .status, n: count(.name) }",
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["status", "n"]);
                // All 3 groups have 10 rows each, all > 5.
                assert_eq!(rows.len(), 3);
                for row in &rows {
                    assert_eq!(row[1], Value::Int(10));
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_having_filters_groups() {
        let mut engine = mission_a_engine(30);
        // HAVING count > 100 → no groups survive.
        let result = engine.execute_powql(
            "User group .status having count(.name) > 100 { .status }",
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_min_max() {
        let mut engine = mission_a_engine(30);
        // 30 rows, ages = 18 + (i % 60) for i in 0..30, so ages 18..47.
        // Group by status (3 groups of 10 each).
        // status=active: i=0,3,6,9,12,15,18,21,24,27 → ages 18,21,24,27,30,33,36,39,42,45
        // min=18, max=45
        let result = engine.execute_powql(
            r#"User filter .status = "active" group .status { .status, lo: min(.age), hi: max(.age) }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["status", "lo", "hi"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("active".into()));
                assert_eq!(rows[0][1], Value::Int(18));
                assert_eq!(rows[0][2], Value::Int(45));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_group_by_avg() {
        let mut engine = mission_a_engine(6);
        // 6 rows: i=0..5
        // active (i=0,3): ages 18,21 → avg=19.5
        // inactive (i=1,4): ages 19,22 → avg=20.5
        // pending (i=2,5): ages 20,23 → avg=21.5
        let result = engine.execute_powql(
            r#"User filter .status = "active" group .status { .status, a: avg(.age) }"#,
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0][1] {
                    Value::Float(v) => assert!((v - 19.5).abs() < 0.001),
                    other => panic!("expected float, got {other:?}"),
                }
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────

    #[test]
    fn test_is_null_filter() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Diana", email := "diana@ex.com" }"#).unwrap();
        let result = engine.execute_powql("User filter .age is null { .name }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Diana".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_is_not_null_filter() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Diana", email := "diana@ex.com" }"#).unwrap();
        let result = engine.execute_powql("User filter .age is not null { .name }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_is_null_count() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Diana", email := "diana@ex.com" }"#).unwrap();
        let result = engine.execute_powql("count(User filter .age is null)").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 1),
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_is_null_combined_with_and() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Diana", email := "diana@ex.com" }"#).unwrap();
        engine.execute_powql(r#"insert User { name := "Eve", email := "eve@ex.com" }"#).unwrap();
        let result = engine.execute_powql(
            r#"User filter .age is null and .name = "Diana" { .name }"#
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Diana".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── String function tests ─────────────────────────────────────────────

    #[test]
    fn test_upper_in_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User filter upper(.name) = "ALICE""#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Alice".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_lower_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User { low: lower(.email) }").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["low"]);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::Str("alice@ex.com".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_length_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User { .name, len: length(.name) }").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "len"]);
                assert_eq!(rows[0][1], Value::Int(5));
                assert_eq!(rows[1][1], Value::Int(3));
                assert_eq!(rows[2][1], Value::Int(7));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_substring_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User { sub: substring(.name, 1, 3) }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Str("Ali".into()));
                assert_eq!(rows[1][0], Value::Str("Bob".into()));
                assert_eq!(rows[2][0], Value::Str("Cha".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_concat_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User { full: concat(.name, " - ", .email) }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Str("Alice - alice@ex.com".into()));
                assert_eq!(rows[1][0], Value::Str("Bob - bob@ex.com".into()));
                assert_eq!(rows[2][0], Value::Str("Charlie - charlie@ex.com".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_concat_coerces_int() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User { info: concat(.name, " age=", .age) }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Str("Alice age=30".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── CASE WHEN tests ───────────────────────────────────────────────

    #[test]
    fn test_case_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User { .name, label: case when .age > 30 then "senior" when .age >= 30 then "exactly30" else "young" end }"#
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "label"]);
                assert_eq!(rows.len(), 3);
                for row in &rows {
                    let name = &row[0];
                    let label = &row[1];
                    match name {
                        Value::Str(n) if n == "Alice" => assert_eq!(label, &Value::Str("exactly30".into())),
                        Value::Str(n) if n == "Bob" => assert_eq!(label, &Value::Str("young".into())),
                        Value::Str(n) if n == "Charlie" => assert_eq!(label, &Value::Str("senior".into())),
                        _ => panic!("unexpected name: {name:?}"),
                    }
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_case_in_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User filter case when .age > 30 then true else false end"#
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Charlie".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_case_without_else_returns_empty() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"User { .name, label: case when .age > 100 then "old" end }"#
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                for row in &rows {
                    assert_eq!(row[1], Value::Empty);
                }
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Mul/Div expression tests (E2f) ───────────────────────────────

    #[test]
    fn test_mul_in_projection() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User { .name, double_age: .age * 2 }").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "double_age"]);
                // Alice age=30 → 60, Bob age=25 → 50, Charlie age=35 → 70
                let ages: Vec<_> = rows.iter().map(|r| &r[1]).collect();
                assert!(ages.contains(&&Value::Int(60)));
                assert!(ages.contains(&&Value::Int(50)));
                assert!(ages.contains(&&Value::Int(70)));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_div_in_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql("User filter .age / 10 > 2").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // 30/10=3>2 ✓, 25/10=2 ✗, 35/10=3>2 ✓
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Multi-column ORDER BY tests (E2f) ────────────────────────────

    #[test]
    fn test_multi_order_by() {
        let mut engine = test_engine();
        // Insert another 30-year-old so we can test tiebreaker
        engine.execute_powql(r#"insert User { name := "Dave", email := "dave@ex.com", age := 30 }"#).unwrap();
        let result = engine.execute_powql("User order .age asc, .name asc { .name, .age }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Expected: Bob(25), Alice(30), Dave(30), Charlie(35)
                assert_eq!(rows[0][0], Value::Str("Bob".into()));
                assert_eq!(rows[1][0], Value::Str("Alice".into()));
                assert_eq!(rows[2][0], Value::Str("Dave".into()));
                assert_eq!(rows[3][0], Value::Str("Charlie".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_multi_order_mixed_direction() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Dave", email := "dave@ex.com", age := 30 }"#).unwrap();
        let result = engine.execute_powql("User order .age asc, .name desc { .name, .age }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Expected: Bob(25), Dave(30), Alice(30), Charlie(35)
                assert_eq!(rows[0][0], Value::Str("Bob".into()));
                assert_eq!(rows[1][0], Value::Str("Dave".into()));
                assert_eq!(rows[2][0], Value::Str("Alice".into()));
                assert_eq!(rows[3][0], Value::Str("Charlie".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── ALTER TABLE / DROP TABLE tests (E2g) ─────────────────────────

    #[test]
    fn test_alter_add_column() {
        let mut engine = test_engine();
        let result = engine.execute_powql("alter User add column status: str").unwrap();
        match result {
            QueryResult::Executed { message } => {
                assert!(message.contains("status"));
                assert!(message.contains("User"));
            }
            other => panic!("expected Executed, got {other:?}"),
        }
        // Verify schema was updated — new inserts can use the new column
        engine.execute_powql(r#"insert User { name := "Eve", email := "eve@ex.com", age := 22, status := "active" }"#).unwrap();
        let result = engine.execute_powql(r#"User filter .name = "Eve" { .name, .status }"#).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "status"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Str("active".into()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn test_alter_add_column_reads_old_rows() {
        // Regression: before the catalog rewrite path existed, rows
        // inserted before `alter ... add column` were left on disk
        // with the pre-alter variable-offset-table layout. A bare
        // `Type` scan then walked `decode_row` which read
        // `n_var + 1` offsets using the NEW schema and panicked with
        // "range end index X out of range for slice of length Y".
        //
        // This test reproduces that exactly: insert, alter, bare scan.
        // Any panic or wrong row count means the rewrite regressed.
        let mut engine = test_engine();
        engine
            .execute_powql("alter User add column country: str")
            .unwrap();
        // Bare scan: NO filter, so the planner cannot skip old rows.
        let result = engine.execute_powql("User").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert!(columns.contains(&"country".to_string()));
                assert_eq!(rows.len(), 3, "three old rows must still be readable");
                let country_idx = columns
                    .iter()
                    .position(|c| c == "country")
                    .expect("country column");
                for row in &rows {
                    assert_eq!(
                        row[country_idx],
                        Value::Empty,
                        "backfilled column must be Empty"
                    );
                }
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn test_alter_add_required_column_fails() {
        // Adding a required column to a non-empty table has no
        // default value to backfill with, so storing `Empty` would
        // silently violate the required invariant. The catalog must
        // reject it.
        let mut engine = test_engine();
        let err = engine
            .execute_powql("alter User add column required country: str")
            .expect_err("required-column add on non-empty table must fail");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("required") || msg.contains("backfill"),
            "error should mention required/backfill, got: {err}"
        );
        // And the schema must NOT have silently gained the column.
        let result = engine.execute_powql("User").unwrap();
        if let QueryResult::Rows { columns, .. } = result {
            assert!(
                !columns.contains(&"country".to_string()),
                "failed alter must not mutate the schema"
            );
        }
    }

    #[test]
    fn test_alter_add_column_then_update_old_row() {
        // Regression-plus: after the rewrite path backfills Empty, an
        // UPDATE against an old row's new column must round-trip.
        // This exercises encode/decode with the new schema shape on a
        // row that was originally written with the old shape.
        let mut engine = test_engine();
        engine
            .execute_powql("alter User add column country: str")
            .unwrap();
        engine
            .execute_powql(r#"User filter .name = "Alice" update { country := "US" }"#)
            .unwrap();

        let result = engine
            .execute_powql(r#"User filter .name = "Alice" { .name, .country }"#)
            .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Alice".into()));
                assert_eq!(rows[0][1], Value::Str("US".into()));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // The other two rows should still decode cleanly with Empty.
        let result = engine.execute_powql("User").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(rows.len(), 3);
                let country_idx = columns
                    .iter()
                    .position(|c| c == "country")
                    .expect("country column");
                let empties = rows
                    .iter()
                    .filter(|r| r[country_idx] == Value::Empty)
                    .count();
                assert_eq!(
                    empties, 2,
                    "two unchanged old rows must still read as Empty"
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn test_alter_drop_column() {
        let mut engine = test_engine();
        engine.execute_powql("alter User drop column email").unwrap();
        let result = engine.execute_powql("User { .name, .age }").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "age"]);
                assert_eq!(rows.len(), 3);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn test_drop_table() {
        let mut engine = test_engine();
        let result = engine.execute_powql("drop User").unwrap();
        match result {
            QueryResult::Executed { message } => {
                assert!(message.contains("User"));
                assert!(message.contains("dropped"));
            }
            other => panic!("expected Executed, got {other:?}"),
        }
        // Querying the dropped table should fail
        assert!(engine.execute_powql("User").is_err());
    }

    #[test]
    fn test_drop_nonexistent_table_errors() {
        let mut engine = test_engine();
        assert!(engine.execute_powql("drop NonExistent").is_err());
    }

    #[test]
    fn test_alter_add_duplicate_column_errors() {
        let mut engine = test_engine();
        assert!(engine.execute_powql("alter User add name: str").is_err());
    }

    #[test]
    fn test_alter_drop_nonexistent_column_errors() {
        let mut engine = test_engine();
        assert!(engine.execute_powql("alter User drop column nonexistent").is_err());
    }

    // ─── IN subquery tests (E2h) ─────────────────────────────────────

    #[test]
    fn test_in_subquery_basic() {
        let mut engine = test_engine();
        // Create a second table with a subset of user names
        engine.execute_powql("type VIP { required name: str }").unwrap();
        engine.execute_powql(r#"insert VIP { name := "Alice" }"#).unwrap();
        engine.execute_powql(r#"insert VIP { name := "Charlie" }"#).unwrap();

        let result = engine.execute_powql(
            "User filter .name in (VIP { .name }) { .name, .age }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Str("Alice".into())));
                assert!(names.contains(&&Value::Str("Charlie".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_not_in_subquery() {
        let mut engine = test_engine();
        engine.execute_powql("type VIP { required name: str }").unwrap();
        engine.execute_powql(r#"insert VIP { name := "Alice" }"#).unwrap();
        engine.execute_powql(r#"insert VIP { name := "Charlie" }"#).unwrap();

        let result = engine.execute_powql(
            "User filter .name not in (VIP { .name }) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("Bob".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_in_subquery_with_filter() {
        let mut engine = test_engine();
        engine.execute_powql("type Score { required name: str, required points: int }").unwrap();
        engine.execute_powql(r#"insert Score { name := "Alice", points := 100 }"#).unwrap();
        engine.execute_powql(r#"insert Score { name := "Bob", points := 50 }"#).unwrap();
        engine.execute_powql(r#"insert Score { name := "Charlie", points := 80 }"#).unwrap();

        // Find users whose names are in the high-scorers list (points > 70)
        let result = engine.execute_powql(
            "User filter .name in (Score filter .points > 70 { .name }) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Str("Alice".into())));
                assert!(names.contains(&&Value::Str("Charlie".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── EXISTS subquery tests (uncorrelated) ───────────────────────────

    #[test]
    fn test_exists_subquery_uncorrelated_true() {
        let mut engine = test_engine();
        // A side table with at least one row → EXISTS(...) = true, so the
        // filter passes every User row through.
        engine.execute_powql("type VIP { required name: str }").unwrap();
        engine.execute_powql(r#"insert VIP { name := "Alice" }"#).unwrap();

        let result = engine.execute_powql(
            "User filter exists (VIP) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3, "all users should pass when EXISTS is true");
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_exists_subquery_uncorrelated_false() {
        let mut engine = test_engine();
        // An empty side table → EXISTS(...) = false, so no User rows pass.
        engine.execute_powql("type VIP { required name: str }").unwrap();

        let result = engine.execute_powql(
            "User filter exists (VIP) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0, "no rows should pass when EXISTS is false");
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_not_exists_subquery() {
        let mut engine = test_engine();
        // NOT EXISTS over an empty table → true → all rows pass.
        engine.execute_powql("type VIP { required name: str }").unwrap();

        let result = engine.execute_powql(
            "User filter not exists (VIP) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }

        // Now add a row — NOT EXISTS becomes false → no rows pass.
        engine.execute_powql(r#"insert VIP { name := "Alice" }"#).unwrap();
        let result = engine.execute_powql(
            "User filter not exists (VIP) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_exists_subquery_with_inner_filter() {
        let mut engine = test_engine();
        // Subquery with its own filter: only rows matching the inner
        // predicate count toward EXISTS.
        engine.execute_powql("type Score { required name: str, required points: int }").unwrap();
        engine.execute_powql(r#"insert Score { name := "Alice", points := 100 }"#).unwrap();

        // Inner filter matches → EXISTS true → all users pass.
        let result = engine.execute_powql(
            "User filter exists (Score filter .points > 50) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_exists_subquery_with_inner_filter_no_match() {
        // Fresh engine so the plan cache doesn't collide with the
        // `> 50` shape from the sibling test.
        let mut engine = test_engine();
        engine.execute_powql("type Score { required name: str, required points: int }").unwrap();
        engine.execute_powql(r#"insert Score { name := "Alice", points := 100 }"#).unwrap();

        // Inner filter matches nothing → EXISTS false → no users pass.
        let result = engine.execute_powql(
            "User filter exists (Score filter .points > 1000) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected rows"),
        }
    }

    // ─── Materialized view tests ────────────────────────────────────────────

    #[test]
    fn test_create_materialized_view() {
        let mut engine = test_engine();
        let result = engine.execute_powql(
            r#"materialize OldUsers as User filter .age > 28"#,
        ).unwrap();
        match result {
            QueryResult::Executed { message } => {
                assert!(message.contains("OldUsers"));
            }
            _ => panic!("expected Executed"),
        }
        // Query the view like a table.
        let result = engine.execute_powql("OldUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // Alice (30) and Charlie (35)
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_view_auto_refresh_on_insert() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize OldUsers as User filter .age > 28"#).unwrap();
        // Insert a new qualifying row.
        engine.execute_powql(r#"insert User { name := "Dave", email := "dave@ex.com", age := 40 }"#).unwrap();
        // The view should auto-refresh and include Dave.
        let result = engine.execute_powql("OldUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3); // Alice, Charlie, Dave
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_view_auto_refresh_on_delete() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize OldUsers as User filter .age > 28"#).unwrap();
        // Delete Alice (age 30) from the base table.
        engine.execute_powql(r#"User filter .name = "Alice" delete"#).unwrap();
        // View should auto-refresh: only Charlie remains.
        let result = engine.execute_powql("OldUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_view_auto_refresh_on_update() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize OldUsers as User filter .age > 28"#).unwrap();
        // Update Bob's age to make him qualify.
        engine.execute_powql(r#"User filter .name = "Bob" update { age := 50 }"#).unwrap();
        let result = engine.execute_powql("OldUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3); // Alice, Charlie, Bob
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_explicit_refresh() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize OldUsers as User filter .age > 28"#).unwrap();
        engine.execute_powql(r#"insert User { name := "Eve", email := "eve@ex.com", age := 55 }"#).unwrap();
        // Explicit refresh.
        let result = engine.execute_powql("refresh OldUsers").unwrap();
        match result {
            QueryResult::Executed { message } => {
                assert!(message.contains("refreshed"));
            }
            _ => panic!("expected Executed"),
        }
        // Now query — should include Eve.
        let result = engine.execute_powql("OldUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_drop_view() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize OldUsers as User filter .age > 28"#).unwrap();
        let result = engine.execute_powql("drop view OldUsers").unwrap();
        match result {
            QueryResult::Executed { message } => {
                assert!(message.contains("dropped"));
            }
            _ => panic!("expected Executed"),
        }
        // Querying the dropped view should fail.
        let err = engine.execute_powql("OldUsers").unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn test_view_with_projection() {
        let mut engine = test_engine();
        engine.execute_powql(
            r#"materialize UserNames as User { .name }"#,
        ).unwrap();
        let result = engine.execute_powql("UserNames").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name".to_string()]);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_view_no_stale_reads() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize AllUsers as User"#).unwrap();
        // Verify initial state.
        let result = engine.execute_powql("AllUsers").unwrap();
        match &result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!("expected rows"),
        }
        // Insert two more.
        engine.execute_powql(r#"insert User { name := "D", email := "d@ex.com", age := 1 }"#).unwrap();
        engine.execute_powql(r#"insert User { name := "E", email := "e@ex.com", age := 2 }"#).unwrap();
        // First insert marks dirty, second stays dirty. Auto-refresh fires on read.
        let result = engine.execute_powql("AllUsers").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 5),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_duplicate_view_creation_fails() {
        let mut engine = test_engine();
        engine.execute_powql(r#"materialize V as User"#).unwrap();
        let err = engine.execute_powql(r#"materialize V as User"#).unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn test_drop_nonexistent_view_fails() {
        let mut engine = test_engine();
        let err = engine.execute_powql("drop view NoSuchView").unwrap_err();
        assert!(err.contains("not found"));
    }

    // ── UNION / UNION ALL tests ────────────────────────────────

    #[test]
    fn test_union_deduplicates() {
        let mut engine = test_engine();
        engine.execute_powql("type A { name: str }").unwrap();
        engine.execute_powql("type B { name: str }").unwrap();
        engine.execute_powql(r#"insert A { name := "alice" }"#).unwrap();
        engine.execute_powql(r#"insert A { name := "bob" }"#).unwrap();
        engine.execute_powql(r#"insert B { name := "bob" }"#).unwrap();
        engine.execute_powql(r#"insert B { name := "carol" }"#).unwrap();
        let result = engine.execute_powql("A union B").unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        // alice, bob, carol — bob deduped
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_union_all_keeps_duplicates() {
        let mut engine = test_engine();
        engine.execute_powql("type X { val: int }").unwrap();
        engine.execute_powql("type Y { val: int }").unwrap();
        engine.execute_powql("insert X { val := 1 }").unwrap();
        engine.execute_powql("insert X { val := 2 }").unwrap();
        engine.execute_powql("insert Y { val := 2 }").unwrap();
        engine.execute_powql("insert Y { val := 3 }").unwrap();
        let result = engine.execute_powql("X union all Y").unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        // 1, 2, 2, 3 — no dedup
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn test_union_with_filters() {
        let mut engine = test_engine();
        engine.execute_powql("type Emp { name: str, dept: str }").unwrap();
        engine.execute_powql(r#"insert Emp { name := "alice", dept := "eng" }"#).unwrap();
        engine.execute_powql(r#"insert Emp { name := "bob", dept := "sales" }"#).unwrap();
        engine.execute_powql(r#"insert Emp { name := "carol", dept := "eng" }"#).unwrap();
        let result = engine.execute_powql(
            r#"Emp filter .dept = "eng" union Emp filter .dept = "sales""#
        ).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_union_chain_three_tables() {
        let mut engine = test_engine();
        engine.execute_powql("type T1 { v: int }").unwrap();
        engine.execute_powql("type T2 { v: int }").unwrap();
        engine.execute_powql("type T3 { v: int }").unwrap();
        engine.execute_powql("insert T1 { v := 1 }").unwrap();
        engine.execute_powql("insert T2 { v := 2 }").unwrap();
        engine.execute_powql("insert T3 { v := 3 }").unwrap();
        let result = engine.execute_powql("T1 union T2 union T3").unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_union_uses_left_side_columns() {
        let mut engine = test_engine();
        engine.execute_powql("type L { name: str }").unwrap();
        engine.execute_powql("type R { name: str }").unwrap();
        engine.execute_powql(r#"insert L { name := "a" }"#).unwrap();
        engine.execute_powql(r#"insert R { name := "b" }"#).unwrap();
        let result = engine.execute_powql("L union R").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name".to_string()]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected rows"),
        }
    }

    // ── COUNT DISTINCT tests ───────────────────────────────────

    #[test]
    fn test_count_distinct_standalone() {
        let mut engine = test_engine();
        engine.execute_powql("type Color { name: str }").unwrap();
        engine.execute_powql(r#"insert Color { name := "red" }"#).unwrap();
        engine.execute_powql(r#"insert Color { name := "blue" }"#).unwrap();
        engine.execute_powql(r#"insert Color { name := "red" }"#).unwrap();
        engine.execute_powql(r#"insert Color { name := "green" }"#).unwrap();
        let result = engine.execute_powql("count(distinct Color { .name })").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 3), // red, blue, green
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_count_distinct_in_group_by() {
        let mut engine = test_engine();
        engine.execute_powql("type Sale { dept: str, item: str }").unwrap();
        engine.execute_powql(r#"insert Sale { dept := "eng", item := "laptop" }"#).unwrap();
        engine.execute_powql(r#"insert Sale { dept := "eng", item := "laptop" }"#).unwrap();
        engine.execute_powql(r#"insert Sale { dept := "eng", item := "monitor" }"#).unwrap();
        engine.execute_powql(r#"insert Sale { dept := "sales", item := "phone" }"#).unwrap();
        let result = engine.execute_powql(
            "Sale group .dept { .dept, count(distinct .item) }"
        ).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        // eng: 2 distinct items (laptop, monitor), sales: 1 (phone)
        let eng_row = rows.iter().find(|r| r[0] == Value::Str("eng".into())).unwrap();
        let sales_row = rows.iter().find(|r| r[0] == Value::Str("sales".into())).unwrap();
        assert_eq!(eng_row[1], Value::Int(2));
        assert_eq!(sales_row[1], Value::Int(1));
    }

    #[test]
    fn test_count_distinct_with_filter() {
        let mut engine = test_engine();
        // Use test_engine which creates User with name, email, age
        engine.execute_powql(r#"insert User { name := "Dave", email := "d@e.com", age := 30 }"#).unwrap();
        let result = engine.execute_powql("count(distinct User { .age })").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => {
                // 30(alice), 25(bob), 35(charlie), 30(dave) → 3 distinct
                assert_eq!(n, 3);
            }
            _ => panic!("expected scalar int"),
        }
    }

    // ── UPDATE with expressions tests ──────────────────────────

    #[test]
    fn test_update_with_arithmetic_expression() {
        let mut engine = test_engine();
        // Alice starts at age 30
        engine.execute_powql(
            r#"User filter .name = "Alice" update { age := .age + 5 }"#
        ).unwrap();
        let result = engine.execute_powql(r#"User filter .name = "Alice""#).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows[0][2], Value::Int(35)); // 30 + 5 = 35
    }

    #[test]
    fn test_update_with_multiply_expression() {
        let mut engine = test_engine();
        // Double everyone's age
        engine.execute_powql("User update { age := .age * 2 }").unwrap();
        let result = engine.execute_powql("User").unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        let ages: Vec<i64> = rows.iter().map(|r| match &r[2] { Value::Int(v) => *v, _ => 0 }).collect();
        assert!(ages.contains(&60));  // Alice: 30*2
        assert!(ages.contains(&50));  // Bob: 25*2
        assert!(ages.contains(&70));  // Charlie: 35*2
    }

    #[test]
    fn test_update_expression_with_filter() {
        let mut engine = test_engine();
        // Increment age only for people over 28
        engine.execute_powql("User filter .age > 28 update { age := .age + 1 }").unwrap();
        let result = engine.execute_powql(r#"User filter .name = "Alice""#).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows[0][2], Value::Int(31)); // Alice was 30, now 31
        let result = engine.execute_powql(r#"User filter .name = "Bob""#).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows[0][2], Value::Int(25)); // Bob was 25, unchanged
    }

    #[test]
    fn test_update_literal_still_uses_fast_path() {
        // Verify the literal path still works after the refactor
        let mut engine = test_engine();
        engine.execute_powql(
            r#"User filter .name = "Alice" update { age := 99 }"#
        ).unwrap();
        let result = engine.execute_powql(r#"User filter .name = "Alice""#).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows[0][2], Value::Int(99));
    }

    // ── COUNT(*) in GROUP BY tests ─────────────────────────────

    #[test]
    fn test_group_by_count_star() {
        let mut engine = test_engine();
        // test_engine has 3 users: Alice(30), Bob(25), Charlie(35)
        // Add another user with same age as Alice
        engine.execute_powql(r#"insert User { name := "Dave", email := "d@e.com", age := 30 }"#).unwrap();
        let result = engine.execute_powql(
            "User group .age { .age, count(*) }"
        ).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        let age30 = rows.iter().find(|r| r[0] == Value::Int(30)).unwrap();
        assert_eq!(age30[1], Value::Int(2)); // Alice + Dave
        let age25 = rows.iter().find(|r| r[0] == Value::Int(25)).unwrap();
        assert_eq!(age25[1], Value::Int(1)); // Bob only
    }

    #[test]
    fn test_group_by_count_star_with_having() {
        let mut engine = test_engine();
        engine.execute_powql(r#"insert User { name := "Dave", email := "d@e.com", age := 30 }"#).unwrap();
        let result = engine.execute_powql(
            "User group .age having count(*) > 1 { .age, count(*) }"
        ).unwrap();
        let rows = match result { QueryResult::Rows { rows, .. } => rows, _ => panic!() };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(30)); // only age=30 has count > 1
    }

    // ── Mixed-type arithmetic (Int <-> Float) regression tests ─────────

    /// Engine with a Product type containing price:float + stock:int.
    /// Exercises mixed numeric promotion in `eval_binop`.
    fn product_mix_engine() -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_product_mix_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type Product { required name: str, required price: float, required stock: int }"
        ).unwrap();
        engine.execute_powql(
            r#"insert Product { name := "Apple",  price := 1.5, stock := 10 }"#
        ).unwrap();
        engine.execute_powql(
            r#"insert Product { name := "Banana", price := 0.25, stock := 4 }"#
        ).unwrap();
        engine.execute_powql(
            r#"insert Product { name := "Cherry", price := 2.0, stock := 3 }"#
        ).unwrap();
        engine
    }

    fn as_float(v: &Value) -> f64 {
        match v {
            Value::Float(f) => *f,
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn test_arith_float_times_int() {
        let mut engine = product_mix_engine();
        let result = engine.execute_powql(
            "Product { .name, total: .price * .stock }"
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "total"]);
                let mut by_name: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in &rows {
                    let name = match &row[0] { Value::Str(s) => s.clone(), _ => panic!() };
                    by_name.insert(name, as_float(&row[1]));
                }
                assert!((by_name["Apple"]  - 15.0).abs() < 1e-9);
                assert!((by_name["Banana"] -  1.0).abs() < 1e-9);
                assert!((by_name["Cherry"] -  6.0).abs() < 1e-9);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_arith_int_plus_float() {
        let mut engine = product_mix_engine();
        // stock:int + price:float → should promote to float
        let result = engine.execute_powql(
            "Product { .name, bumped: .stock + .price }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let mut by_name: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in &rows {
                    let name = match &row[0] { Value::Str(s) => s.clone(), _ => panic!() };
                    by_name.insert(name, as_float(&row[1]));
                }
                assert!((by_name["Apple"]  - 11.5).abs() < 1e-9);
                assert!((by_name["Banana"] -  4.25).abs() < 1e-9);
                assert!((by_name["Cherry"] -  5.0).abs() < 1e-9);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_arith_float_div_int() {
        let mut engine = product_mix_engine();
        let result = engine.execute_powql(
            "Product { .name, unit: .price / .stock }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let mut by_name: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in &rows {
                    let name = match &row[0] { Value::Str(s) => s.clone(), _ => panic!() };
                    by_name.insert(name, as_float(&row[1]));
                }
                assert!((by_name["Apple"]  - 0.15).abs() < 1e-9);
                assert!((by_name["Banana"] - 0.0625).abs() < 1e-9);
                assert!((by_name["Cherry"] - (2.0 / 3.0)).abs() < 1e-9);
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_arith_int_minus_float() {
        let mut engine = product_mix_engine();
        let result = engine.execute_powql(
            "Product { .name, delta: .stock - .price }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let mut by_name: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in &rows {
                    let name = match &row[0] { Value::Str(s) => s.clone(), _ => panic!() };
                    by_name.insert(name, as_float(&row[1]));
                }
                assert!((by_name["Apple"]  -  8.5).abs() < 1e-9);
                assert!((by_name["Banana"] -  3.75).abs() < 1e-9);
                assert!((by_name["Cherry"] -  1.0).abs() < 1e-9);
            }
            _ => panic!("expected rows"),
        }
    }

    // Regression: sum() on a Float column must return the actual
    // floating-point sum, not Int(0). The old slow-path loops filtered
    // out Value::Float and only summed Ints, silently dropping every
    // value in a Float column.
    #[test]
    fn test_sum_float_scalar() {
        let mut engine = product_mix_engine();
        let result = engine.execute_powql("sum(Product { .price })").unwrap();
        match result {
            QueryResult::Scalar(v) => {
                // 1.5 + 0.25 + 2.0 = 3.75
                assert!((as_float(&v) - 3.75).abs() < 1e-9,
                        "expected 3.75, got {v:?}");
            }
            _ => panic!("expected scalar result, got {result:?}"),
        }
    }

    // Regression: sum() of a Float column inside a GROUP BY must work
    // the same way. compute_group_aggregate had the identical Int-only
    // bug as the scalar path.
    #[test]
    fn test_sum_float_group_by() {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_sum_float_gb_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type Sale { required region: str, required amount: float }"
        ).unwrap();
        engine.execute_powql(r#"insert Sale { region := "E", amount := 1.5 }"#).unwrap();
        engine.execute_powql(r#"insert Sale { region := "E", amount := 2.25 }"#).unwrap();
        engine.execute_powql(r#"insert Sale { region := "W", amount := 4.0 }"#).unwrap();
        engine.execute_powql(r#"insert Sale { region := "W", amount := 0.5 }"#).unwrap();

        let result = engine.execute_powql(
            "Sale group .region { .region, total: sum(.amount) }"
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["region", "total"]);
                let mut by_region: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in &rows {
                    let region = match &row[0] { Value::Str(s) => s.clone(), _ => panic!() };
                    by_region.insert(region, as_float(&row[1]));
                }
                assert!((by_region["E"] - 3.75).abs() < 1e-9, "E: {:?}", by_region.get("E"));
                assert!((by_region["W"] - 4.5).abs()  < 1e-9, "W: {:?}", by_region.get("W"));
            }
            _ => panic!("expected rows, got {result:?}"),
        }
    }

    // ─── Mission D10: Float fast-path parity ─────────────────────────────
    //
    // Prior to D10, three hot paths in the executor bailed on Float columns:
    //   1. `agg_single_col_fast` — sum/avg/min/max/count fell through to the
    //      generic row-decoding path (allocates Vec<Value> per row).
    //   2. `project_filter_sort_limit_fast` — top-N by Float column fell
    //      through the generic sort path.
    //   3. `compile_predicate` / `build_int_leaf` — WHERE on Float columns
    //      couldn't compile, so the whole filter walked Value::cmp.
    //
    // These tests exercise each Float fast path end-to-end, including NaN
    // handling via `total_cmp` (which matches `Value::Ord` so semantics are
    // identical between fast-path and generic-path reads).

    /// Engine with a Price table: price:float, qty:int. Eight rows with a
    /// deliberate spread of values, a NaN, a negative, -0.0, and a null.
    /// The null exercises the bitmap-skip branch; NaN and -0.0 exercise
    /// the `total_cmp` invariant.
    fn float_fast_engine() -> Engine {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("powdb_float_fast_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_powql(
            "type Price { required name: str, price: float, required qty: int }"
        ).unwrap();
        // Insertion order deliberately scrambled so top-N doesn't trivially
        // match insertion order.
        let rows = [
            ("a",  "price := 1.5",   "qty := 1"),
            ("b",  "price := 0.25",  "qty := 2"),
            ("c",  "price := 2.0",   "qty := 3"),
            ("d",  "price := -3.5",  "qty := 4"),
            ("e",  "price := 10.0",  "qty := 5"),
            ("f",  "price := 0.5",   "qty := 6"),
            ("g",  "price := 100.0", "qty := 7"),
            ("h",  "price := -0.0",  "qty := 8"),
        ];
        for (name, price, qty) in rows {
            engine.execute_powql(
                &format!(r#"insert Price {{ name := "{name}", {price}, {qty} }}"#)
            ).unwrap();
        }
        engine
    }

    #[test]
    fn test_d10_agg_sum_float_fast_path() {
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("sum(Price { .price })").unwrap();
        // 1.5 + 0.25 + 2.0 + -3.5 + 10.0 + 0.5 + 100.0 + -0.0 = 110.75
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - 110.75).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_avg_float_fast_path() {
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("avg(Price { .price })").unwrap();
        // 110.75 / 8 = 13.84375
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - 13.84375).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_min_float_fast_path() {
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("min(Price { .price })").unwrap();
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - (-3.5)).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_max_float_fast_path() {
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("max(Price { .price })").unwrap();
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - 100.0).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_count_distinct_float_fast_path() {
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("count(distinct Price { .price })").unwrap();
        // All 8 prices are distinct (+0.0 isn't present; -0.0 is, and
        // distinct from every other value). Hash via to_bits so -0.0 and
        // +0.0 would count separately — matches Value::Hash.
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 8, "got {n}"),
            _ => panic!("expected scalar int, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_float_with_compiled_where() {
        // Exercises `build_float_leaf` — WHERE .price > 1.0 must compile,
        // and the Float fast path must use it to short-circuit rows.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("sum(Price filter .price > 1.0 { .price })").unwrap();
        // Rows > 1.0: 1.5, 2.0, 10.0, 100.0 → sum = 113.5
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - 113.5).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_float_with_compiled_where_int_literal() {
        // Novel cross-type: WHERE .price > 1 (Int literal on Float column)
        // must still compile via build_float_leaf — the Int literal is
        // promoted to f64 at compile time so the hot loop only sees f64.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("sum(Price filter .price > 1 { .price })").unwrap();
        match result {
            QueryResult::Scalar(v) => {
                assert!((as_float(&v) - 113.5).abs() < 1e-9, "got {v:?}");
            }
            _ => panic!("expected scalar, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_agg_float_with_reversed_literal() {
        // `100.0 > .price` (literal on LHS) must also compile. The
        // build_float_leaf flips the operator so the field is always LHS.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql("count(Price filter 1.0 < .price { .price })").unwrap();
        // Rows where 1.0 < .price: 1.5, 2.0, 10.0, 100.0 → count = 4
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 4, "got {n}"),
            _ => panic!("expected scalar int, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_sort_float_desc_limit_fast_path() {
        // Top-3 by price descending — exercises the Float branch of
        // project_filter_sort_limit_fast with the sortable-u64 transform.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql(
            "Price order .price desc limit 3 { .name, .price }"
        ).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "price"]);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::Str("g".into())); // 100.0
                assert!((as_float(&rows[0][1]) - 100.0).abs() < 1e-9);
                assert_eq!(rows[1][0], Value::Str("e".into())); // 10.0
                assert!((as_float(&rows[1][1]) - 10.0).abs() < 1e-9);
                assert_eq!(rows[2][0], Value::Str("c".into())); // 2.0
                assert!((as_float(&rows[2][1]) - 2.0).abs() < 1e-9);
            }
            _ => panic!("expected rows, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_sort_float_asc_limit_fast_path() {
        // Bottom-3 by price — negative and -0.0 must order correctly.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql(
            "Price order .price limit 3 { .name, .price }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::Str("d".into())); // -3.5
                // -0.0 must come before +0.25 under total_cmp ordering.
                assert_eq!(rows[1][0], Value::Str("h".into())); // -0.0
                assert_eq!(rows[2][0], Value::Str("b".into())); // 0.25
            }
            _ => panic!("expected rows, got {result:?}"),
        }
    }

    #[test]
    fn test_d10_sort_float_with_compiled_filter() {
        // Filter + sort + limit all on Float column — every fast path
        // fires on the same query.
        let mut engine = float_fast_engine();
        let result = engine.execute_powql(
            "Price filter .price > 0.0 order .price desc limit 2 { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("g".into())); // 100.0
                assert_eq!(rows[1][0], Value::Str("e".into())); // 10.0
            }
            _ => panic!("expected rows, got {result:?}"),
        }
    }

    #[test]
    fn test_f64_sortable_transform_monotonic() {
        // The sortable-u64 transform must preserve total_cmp ordering.
        // Regression guard against accidentally breaking the clever
        // sign-flip trick in `f64_bits_to_sortable_u64`.
        let samples: [f64; 11] = [
            f64::NEG_INFINITY,
            -1e100,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1e100,
            f64::INFINITY,
            f64::NAN, // total_cmp says NaN > +∞
        ];
        let mut sorted = samples;
        sorted.sort_by(|a, b| a.total_cmp(b));

        let as_sortable: Vec<u64> = sorted.iter()
            .map(|f| f64_bits_to_sortable_u64(f.to_bits()))
            .collect();

        // Each u64 must be strictly greater than its predecessor, because
        // `total_cmp` places every sample at a distinct total-order slot.
        for pair in as_sortable.windows(2) {
            assert!(pair[0] < pair[1],
                "sortable u64 not monotonic: {:#x} >= {:#x}", pair[0], pair[1]);
        }
    }

    // ─── EXPLAIN tests ─────────────────────────────────────────────────

    #[test]
    fn test_explain_simple_scan() {
        let mut engine = test_engine();
        let result = engine.execute_powql("explain User").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["plan"]);
                assert!(!rows.is_empty());
                assert!(matches!(&rows[0][0], Value::Str(s) if s.contains("SeqScan")));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_explain_filter() {
        let mut engine = test_engine();
        let result = engine.execute_powql("explain User filter .age > 30").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let plan_text: String = rows.iter()
                    .map(|r| match &r[0] { Value::Str(s) => s.as_str(), _ => "" })
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(plan_text.contains("Filter"), "plan should show Filter(SeqScan) after lowering unindexed RangeScan");
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_explain_does_not_execute() {
        let mut engine = test_engine();
        // EXPLAIN should NOT actually insert a row.
        let result = engine.execute_powql(r#"explain insert User { name := "Zara", age := 99 }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                let plan_text: String = rows.iter()
                    .map(|r| match &r[0] { Value::Str(s) => s.as_str(), _ => "" })
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(plan_text.contains("Insert"));
            }
            _ => panic!("expected rows"),
        }
        // Verify no row was actually inserted.
        let result = engine.execute_powql("User { .name }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3, "should still have original 3 users");
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Correlated subquery tests ──────────────────────────────────────

    #[test]
    fn test_correlated_in_subquery() {
        let mut engine = test_engine();
        // Create an orders table with user_name to correlate on.
        engine.execute_powql("type UserOrder { required user_name: str, required total: int }").unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Alice", total := 100 }"#).unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Alice", total := 200 }"#).unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Bob", total := 50 }"#).unwrap();

        // Correlated: for each User row, find orders where user_name = outer .name
        // The subquery references .name which is a User column, not a UserOrder column.
        let result = engine.execute_powql(
            "User filter .name in (UserOrder filter .user_name = .name { .user_name }) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "Alice and Bob have orders");
                let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Str("Alice".into())));
                assert!(names.contains(&&Value::Str("Bob".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_correlated_exists_subquery() {
        let mut engine = test_engine();
        engine.execute_powql("type UserOrder { required user_name: str, required total: int }").unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Alice", total := 100 }"#).unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Bob", total := 50 }"#).unwrap();

        // Correlated EXISTS: only Users who have at least one order.
        // .name in the subquery filter refers to the outer User's name column.
        let result = engine.execute_powql(
            "User filter exists (UserOrder filter .user_name = .name) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "Alice and Bob have orders");
                let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Str("Alice".into())));
                assert!(names.contains(&&Value::Str("Bob".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_correlated_not_exists_subquery() {
        let mut engine = test_engine();
        engine.execute_powql("type UserOrder { required user_name: str, required total: int }").unwrap();
        engine.execute_powql(r#"insert UserOrder { user_name := "Alice", total := 100 }"#).unwrap();

        // NOT EXISTS: Users without orders (Bob and Charlie).
        let result = engine.execute_powql(
            "User filter not exists (UserOrder filter .user_name = .name) { .name }"
        ).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "Bob and Charlie have no orders");
                let names: Vec<_> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Str("Bob".into())));
                assert!(names.contains(&&Value::Str("Charlie".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── CAST tests ───────────────────────────────────────────────────

    #[test]
    fn test_cast_int_to_str() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User { s: cast(.age, "str") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Str("30".into()));
                assert_eq!(rows[1][0], Value::Str("25".into()));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_cast_str_to_int() {
        let mut engine = test_engine();
        engine.execute_powql(r#"type Numbers { required val: str }"#).unwrap();
        engine.execute_powql(r#"insert Numbers { val := "42" }"#).unwrap();
        let result = engine.execute_powql(r#"Numbers { n: cast(.val, "int") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(42));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_cast_float_to_int() {
        let mut engine = test_engine();
        engine.execute_powql("type Floats { required val: float }").unwrap();
        engine.execute_powql("insert Floats { val := 3.7 }").unwrap();
        let result = engine.execute_powql(r#"Floats { n: cast(.val, "int") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(3));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_cast_int_to_float() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User { f: cast(.age, "float") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Float(30.0));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_cast_int_to_bool() {
        let mut engine = test_engine();
        let result = engine.execute_powql(r#"User { b: cast(.age, "bool") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // age=30 -> true (non-zero)
                assert_eq!(rows[0][0], Value::Bool(true));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Math function tests ──────────────────────────────────────────

    #[test]
    fn test_abs() {
        let mut engine = test_engine();
        engine.execute_powql("type Nums { required val: int }").unwrap();
        engine.execute_powql("insert Nums { val := -42 }").unwrap();
        let result = engine.execute_powql("Nums { a: abs(.val) }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(42));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_round() {
        let mut engine = test_engine();
        engine.execute_powql("type Floats { required val: float }").unwrap();
        engine.execute_powql("insert Floats { val := 3.14159 }").unwrap();
        let result = engine.execute_powql("Floats { r: round(.val, 2) }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Float(3.14));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_ceil_floor() {
        let mut engine = test_engine();
        engine.execute_powql("type Floats { required val: float }").unwrap();
        engine.execute_powql("insert Floats { val := 3.2 }").unwrap();
        let c = engine.execute_powql("Floats { c: ceil(.val) }").unwrap();
        let f = engine.execute_powql("Floats { f: floor(.val) }").unwrap();
        match (c, f) {
            (QueryResult::Rows { rows: cr, .. }, QueryResult::Rows { rows: fr, .. }) => {
                assert_eq!(cr[0][0], Value::Float(4.0));
                assert_eq!(fr[0][0], Value::Float(3.0));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_sqrt() {
        let mut engine = test_engine();
        engine.execute_powql("type Nums { required val: int }").unwrap();
        engine.execute_powql("insert Nums { val := 144 }").unwrap();
        let result = engine.execute_powql("Nums { s: sqrt(.val) }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Float(12.0));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_pow() {
        let mut engine = test_engine();
        engine.execute_powql("type Nums { required val: int }").unwrap();
        engine.execute_powql("insert Nums { val := 3 }").unwrap();
        let result = engine.execute_powql("Nums { p: pow(.val, 4) }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(81));
            }
            _ => panic!("expected rows"),
        }
    }

    // ─── Date/time function tests ─────────────────────────────────────

    #[test]
    fn test_now_returns_datetime() {
        let mut engine = test_engine();
        engine.execute_powql("type Events { required name: str }").unwrap();
        engine.execute_powql(r#"insert Events { name := "test" }"#).unwrap();
        let result = engine.execute_powql("Events { ts: now() }").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                match &rows[0][0] {
                    Value::DateTime(m) => assert!(*m > 0, "now() should return positive timestamp"),
                    other => panic!("expected DateTime, got {other:?}"),
                }
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_extract_from_datetime() {
        let mut engine = test_engine();
        engine.execute_powql("type Events { required ts: datetime }").unwrap();
        // 2024-01-15 12:30:45 UTC in microseconds
        // 2024-01-15 = 19737 days since epoch
        // 19737 * 86400 = 1705276800 seconds + 12*3600 + 30*60 + 45 = 1705321845
        // * 1_000_000 = 1705321845000000
        engine.execute_powql("insert Events { ts := 1705321845000000 }").unwrap();
        let result = engine.execute_powql(r#"Events { y: extract("year", .ts), m: extract("month", .ts), d: extract("day", .ts), h: extract("hour", .ts) }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(2024));
                assert_eq!(rows[0][1], Value::Int(1));
                assert_eq!(rows[0][2], Value::Int(15));
                assert_eq!(rows[0][3], Value::Int(12));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_date_add() {
        let mut engine = test_engine();
        engine.execute_powql("type Events { required ts: datetime }").unwrap();
        let base = 1705321845000000_i64; // 2024-01-15 12:30:45 UTC
        engine.execute_powql(&format!("insert Events {{ ts := {base} }}")).unwrap();
        let result = engine.execute_powql(r#"Events { later: date_add(.ts, 2, "hours") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::DateTime(base + 2 * 3_600_000_000));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_date_diff() {
        let mut engine = test_engine();
        engine.execute_powql("type Events { required start_ts: datetime, required end_ts: datetime }").unwrap();
        let t1 = 1705321845000000_i64; // 2024-01-15 12:30:45 UTC
        let t2 = t1 + 3 * 86_400_000_000; // +3 days
        engine.execute_powql(&format!("insert Events {{ start_ts := {t1}, end_ts := {t2} }}")).unwrap();
        let result = engine.execute_powql(r#"Events { diff: date_diff(.end_ts, .start_ts, "days") }"#).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int(3));
            }
            _ => panic!("expected rows"),
        }
    }
}
