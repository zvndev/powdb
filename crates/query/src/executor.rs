use crate::ast::*;
use crate::canonicalize::canonicalize;
use crate::plan::*;
use crate::plan_cache::PlanCache;
use crate::planner;
use crate::result::QueryResult;
use powdb_storage::catalog::Catalog;
use powdb_storage::row::{RowLayout, decode_column, decode_row};
use powdb_storage::types::*;
use powdb_storage::view::{ViewDef, ViewRegistry};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::path::Path;
use std::time::Instant;
use tracing::{info, error, Level};

/// Plan cache capacity. Bench workloads fill ~15 slots; real apps will sit
/// comfortably in 256. Lookup is O(1), collisions clear the cache (see
/// `plan_cache::PlanCache::insert`).
const PLAN_CACHE_CAPACITY: usize = 256;

pub struct Engine {
    catalog: Catalog,
    /// Mission D9 — cached parsed+planned query trees keyed by canonical
    /// hash. Saves the ~3μs parse+plan cost on repeat queries that differ
    /// only in literal values.
    plan_cache: PlanCache,
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
            plan_cache: PlanCache::new(PLAN_CACHE_CAPACITY),
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
                if let Some(plan) = self.plan_cache.get_with_substitution(hash, &literals) {
                    return self.execute_plan(&plan);
                }
                // Miss — plan, insert, execute.
                return match planner::plan(input) {
                    Ok(plan) => {
                        self.plan_cache.insert(hash, plan.clone());
                        self.execute_plan(&plan)
                    }
                    Err(e) => Err(e.message),
                };
            }
            // Lex error — fall through to the planner so the caller gets a
            // consistent error shape.
            return match planner::plan(input) {
                Ok(plan) => self.execute_plan(&plan),
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
        let result = self.execute_plan(&plan);
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
        (self.plan_cache.hits, self.plan_cache.misses, self.plan_cache.len())
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
            return Ok(QueryResult::Modified(1));
        }

        let mut plan = prep.plan_template.clone();
        let mut idx = 0usize;
        crate::plan_cache::substitute_plan(&mut plan, literals, &mut idx);
        debug_assert_eq!(idx, literals.len());
        self.execute_plan(&plan)
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
        let tbl = self.catalog.table_by_slot_mut(fast.table_slot);
        let Some(btree) = tbl.index(&fast.key_col) else {
            // Index dropped since prepare — bail to the generic path.
            return Ok(None);
        };
        let Some(rid) = btree.lookup_int(key_int) else {
            return Ok(Some(QueryResult::Modified(0)));
        };

        let ok = tbl.with_row_bytes_mut(rid, |row| {
            // Idempotent null-bit clear — safe even when the column was
            // already non-null (the overwhelmingly common case).
            row[fast.bitmap_byte_off] &= !fast.bit_mask;
            let field_bytes = bytes.as_slice();
            row[fast.field_off..fast.field_off + field_bytes.len()]
                .copy_from_slice(field_bytes);
        }).map_err(|e| e.to_string())?;

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
                let materialized;
                let predicate = if contains_subquery(predicate) {
                    materialized = self.materialize_subqueries(predicate)?;
                    &materialized
                } else {
                    predicate
                };

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
                                .expect(&format!("column '{}' not found", k.field));
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
                                let sum: i64 = rows.iter().filter_map(|r| match &r[idx] {
                                    Value::Int(v) => Some(*v),
                                    _ => None,
                                }).sum();
                                Ok(QueryResult::Scalar(Value::Int(sum)))
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
                        let ok = self.catalog.with_row_bytes_mut(table, rid, |row| {
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
                        let ok = self.catalog
                            .patch_var_col_in_place(table, *rid, col_idx, new_bytes_ref)
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
                                let count = self.catalog
                                    .scan_delete_matching(table, |data| compiled(data))
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
                        let count = self.catalog
                            .scan_delete_matching(table, |_| true)
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
                let result = self.execute_plan(&input)?;
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

            PlanNode::IndexScan { table, column, key } => {
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let key_value = literal_to_value(key)?;

                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;

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
                let fast = FastLayout::new(&schema);
                let synth_pred = Expr::BinaryOp(
                    Box::new(Expr::Field(column.clone())),
                    BinOp::Eq,
                    Box::new(key.clone()),
                );
                if let Some(compiled) = compile_predicate(&synth_pred, &columns, &fast, &schema) {
                    // Mission F: skip the first 4 Vec doublings.
                    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(64);
                    self.catalog.for_each_row_raw(table, |_rid, data| {
                        if compiled(data) {
                            rows.push(decode_row(&schema, data));
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
        // Clear old data and insert fresh results.
        self.catalog
            .scan_delete_matching(name, |_| true)
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
        // Only fast-path fixed-size int columns for sum/avg/min/max.
        if schema.columns[col_idx].type_id != TypeId::Int {
            return Ok(None);
        }

        let fast = FastLayout::new(&schema);
        // Mission C Phase 20b: inline the int-column reader instead of
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
        let result = match function {
            AggFunc::Sum | AggFunc::Avg => {
                let mut sum_i128: i128 = 0;
                let mut count: i64 = 0;
                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                    if is_null { return; }
                    let v = i64::from_le_bytes(
                        data[data_offset..data_offset + 8].try_into().unwrap(),
                    );
                    count += 1;
                    sum_i128 += v as i128;
                }).map_err(|e| e.to_string())?;
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
                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                    if is_null { return; }
                    let v = i64::from_le_bytes(
                        data[data_offset..data_offset + 8].try_into().unwrap(),
                    );
                    min_v = Some(match min_v { Some(m) => m.min(v), None => v });
                }).map_err(|e| e.to_string())?;
                QueryResult::Scalar(min_v.map(Value::Int).unwrap_or(Value::Empty))
            }
            AggFunc::Max => {
                let mut max_v: Option<i64> = None;
                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                    if is_null { return; }
                    let v = i64::from_le_bytes(
                        data[data_offset..data_offset + 8].try_into().unwrap(),
                    );
                    max_v = Some(match max_v { Some(m) => m.max(v), None => v });
                }).map_err(|e| e.to_string())?;
                QueryResult::Scalar(max_v.map(Value::Int).unwrap_or(Value::Empty))
            }
            AggFunc::Count => {
                let mut count: i64 = 0;
                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                    if is_null { return; }
                    count += 1;
                }).map_err(|e| e.to_string())?;
                QueryResult::Scalar(Value::Int(count))
            }
            AggFunc::CountDistinct => {
                let mut seen = rustc_hash::FxHashSet::default();
                self.catalog.for_each_row_raw(table, |_rid, data| {
                    if let Some(ref pred) = compiled_pred {
                        if !pred(data) { return; }
                    }
                    let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
                    if is_null { return; }
                    let v = i64::from_le_bytes(
                        data[data_offset..data_offset + 8].try_into().unwrap(),
                    );
                    seen.insert(v);
                }).map_err(|e| e.to_string())?;
                QueryResult::Scalar(Value::Int(seen.len() as i64))
            }
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

        // Sort key must be a fixed-size Int column.
        let sort_idx = match schema.column_index(sort_field) {
            Some(i) => i,
            None => return Ok(None),
        };
        if schema.columns[sort_idx].type_id != TypeId::Int {
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
        // Mission C Phase 20b: inline int-column reader (no Box<dyn Fn>).
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
                    // top of min-heap is the smallest currently-kept key;
                    // replace it if the new key is larger.
                    if key > *top_key {
                        heap_desc.pop();
                        heap_desc.push(Reverse((key, id, data.to_vec())));
                    }
                }
            } else if heap_asc.len() < limit {
                heap_asc.push((key, id, data.to_vec()));
            } else if let Some((top_key, _, _)) = heap_asc.peek() {
                // top of max-heap is the largest currently-kept key;
                // replace it if the new key is smaller.
                if key < *top_key {
                    heap_asc.pop();
                    heap_asc.push((key, id, data.to_vec()));
                }
            }
        }).map_err(|e| e.to_string())?;

        // Drain into a sorted vec (ascending by key, then by seq for stability).
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

        let rows: Vec<Vec<Value>> = drained.into_iter().map(|(_, _, data)| {
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
fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } => true,
        Expr::BinaryOp(l, _, r) => contains_subquery(l) || contains_subquery(r),
        Expr::UnaryOp(_, inner) => contains_subquery(inner),
        Expr::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
        }
        Expr::Case { whens, else_expr } => {
            whens.iter().any(|(c, r)| contains_subquery(c) || contains_subquery(r))
                || else_expr.as_ref().map_or(false, |e| contains_subquery(e))
        }
        Expr::ScalarFunc(_, args) => args.iter().any(contains_subquery),
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
        Expr::FunctionCall(_, _) | Expr::Param(_) => Value::Empty,
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
    }
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
            let mut sum: i64 = 0;
            for &ri in row_indices {
                if let Value::Int(v) = &all_rows[ri][col_idx] {
                    sum += v;
                }
            }
            Value::Int(sum)
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
            (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            _ => Value::Empty,
        },
        BinOp::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            _ => Value::Empty,
        },
        BinOp::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
            _ => Value::Empty,
        },
        BinOp::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a / b),
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
}
