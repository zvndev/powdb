use crate::ast::*;
use crate::plan::*;
use crate::planner;
use crate::result::QueryResult;
use powdb_storage::catalog::Catalog;
use powdb_storage::row::{RowLayout, decode_column, decode_row};
use powdb_storage::types::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::path::Path;
use std::time::Instant;
use tracing::{info, debug, error};

pub struct Engine {
    catalog: Catalog,
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
        Ok(Engine { catalog })
    }

    pub fn execute_powql(&mut self, input: &str) -> Result<QueryResult, String> {
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

        debug!(plan = ?plan, "executed plan");
        result
    }

    pub fn execute_plan(&mut self, plan: &PlanNode) -> Result<QueryResult, String> {
        match plan {
            PlanNode::SeqScan { table } => {
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
                // Fast path: fuse Filter + SeqScan into a zero-copy streaming
                // loop. Uses decode_column() to evaluate the predicate on only
                // the columns it references, avoiding heap allocations for
                // String/Bytes columns that aren't part of the filter.
                if let PlanNode::SeqScan { table } = input.as_ref() {
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

                    if tbl.indexes.contains_key(column) {
                        let layout = RowLayout::new(&schema);
                        // Mission D7: int-specialized lookup skips the
                        // `<Value as Ord>::cmp` discriminant dispatch on
                        // int-keyed indexes (the vast majority).
                        let btree = tbl.indexes.get(column).unwrap();
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
                    if let PlanNode::Sort { input: sort_input, field: sort_field, descending } = inner.as_ref() {
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
                                table, fields, sort_field, *descending, limit, pred_opt,
                            )? {
                                return Ok(result);
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

            PlanNode::Sort { input, field, descending } => {
                let result = self.execute_plan(input)?;
                match result {
                    QueryResult::Rows { columns, mut rows } => {
                        let col_idx = columns.iter().position(|c| c == field)
                            .ok_or_else(|| format!("column '{field}' not found"))?;
                        rows.sort_by(|a, b| {
                            let cmp = a[col_idx].cmp(&b[col_idx]);
                            if *descending { cmp.reverse() } else { cmp }
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
                if matches!(function, AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max) {
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
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();
                let mut values = vec![Value::Empty; schema.columns.len()];
                for a in assignments {
                    let idx = schema.column_index(&a.field)
                        .ok_or_else(|| format!("column '{}' not found", a.field))?;
                    values[idx] = literal_to_value(&a.value)?;
                }
                self.catalog.insert(table, &values).map_err(|e| e.to_string())?;
                Ok(QueryResult::Modified(1))
            }

            PlanNode::Update { input, table, assignments } => {
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();

                // Resolve assignment column indices and values once.
                let resolved_assignments: Vec<(usize, Value)> = assignments.iter()
                    .map(|a| {
                        let idx = schema.column_index(&a.field)
                            .ok_or_else(|| format!("column '{}' not found", a.field))?;
                        let val = literal_to_value(&a.value)?;
                        Ok::<_, String>((idx, val))
                    })
                    .collect::<Result<_, _>>()?;

                // Collect matching RowIds in a single pass (fixes the old
                // O(N*M) value-equality join against a materialised row set).
                let matching_rids = self.collect_rids_for_mutation(input, table, &schema)?;

                let mut count = 0u64;
                for rid in matching_rids {
                    let mut row = match self.catalog.get(table, rid) {
                        Some(r) => r,
                        None => continue, // concurrently gone
                    };
                    for (idx, val) in &resolved_assignments {
                        row[*idx] = val.clone();
                    }
                    self.catalog.update(table, rid, &row).map_err(|e| e.to_string())?;
                    count += 1;
                }
                Ok(QueryResult::Modified(count))
            }

            PlanNode::Delete { input, table } => {
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();

                let matching_rids = self.collect_rids_for_mutation(input, table, &schema)?;
                let count = matching_rids.len() as u64;
                for rid in matching_rids {
                    self.catalog.delete(table, rid).map_err(|e| e.to_string())?;
                }
                Ok(QueryResult::Modified(count))
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
                if let Some(btree) = tbl.indexes.get(column) {
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
        let reader = match compile_int_reader(col_idx, &fast) {
            Some(r) => r,
            None => return Ok(None),
        };

        // Optional compiled filter.
        let compiled_pred: Option<CompiledPredicate> = match predicate {
            Some(pred) => match compile_predicate(pred, &columns, &fast, &schema) {
                Some(c) => Some(c),
                None => return Ok(None), // let generic path handle it
            },
            None => None,
        };

        let mut sum_i128: i128 = 0;
        let mut count: i64 = 0;
        let mut min_v: Option<i64> = None;
        let mut max_v: Option<i64> = None;

        self.catalog.for_each_row_raw(table, |_rid, data| {
            if let Some(ref pred) = compiled_pred {
                if !pred(data) { return; }
            }
            if let Some(v) = reader(data) {
                count += 1;
                match function {
                    AggFunc::Sum | AggFunc::Avg => {
                        sum_i128 += v as i128;
                    }
                    AggFunc::Min => {
                        min_v = Some(match min_v { Some(m) => m.min(v), None => v });
                    }
                    AggFunc::Max => {
                        max_v = Some(match max_v { Some(m) => m.max(v), None => v });
                    }
                    AggFunc::Count => {}
                }
            }
        }).map_err(|e| e.to_string())?;

        let result = match function {
            AggFunc::Sum => {
                // Saturating clamp to i64 range.
                let clamped = sum_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
                QueryResult::Scalar(Value::Int(clamped))
            }
            AggFunc::Avg => {
                if count == 0 {
                    QueryResult::Scalar(Value::Empty)
                } else {
                    let avg = (sum_i128 as f64) / (count as f64);
                    QueryResult::Scalar(Value::Float(avg))
                }
            }
            AggFunc::Min => QueryResult::Scalar(min_v.map(Value::Int).unwrap_or(Value::Empty)),
            AggFunc::Max => QueryResult::Scalar(max_v.map(Value::Int).unwrap_or(Value::Empty)),
            AggFunc::Count => QueryResult::Scalar(Value::Int(count)),
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
        let reader = match compile_int_reader(sort_idx, &fast) {
            Some(r) => r,
            None => return Ok(None),
        };

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
            let key = match reader(data) {
                Some(k) => k,
                None => return, // null sort key: skip
            };
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
    fn collect_rids_for_mutation(
        &mut self,
        input: &PlanNode,
        table: &str,
        schema: &Schema,
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
                let tbl = self.catalog.get_table(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?;

                // Indexed case: single lookup, 0 or 1 rows.
                // Mission D7: int-specialized fast path on int-keyed indexes
                // (primary keys, created_at, etc.) — the common case for
                // `update_by_pk` / `delete where id = ?`.
                if let Some(btree) = tbl.indexes.get(column) {
                    let hit = match &key_value {
                        Value::Int(k) => btree.lookup_int(*k),
                        other => btree.lookup(other),
                    };
                    return Ok(match hit {
                        Some(rid) => vec![rid],
                        None => Vec::new(),
                    });
                }

                // No index: the planner folds `.col = literal` to IndexScan
                // regardless of whether the column is actually unique. When
                // there's no index we must behave like Filter(SeqScan) and
                // return *all* matching RIDs — not just the first one.
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

fn literal_to_value(expr: &Expr) -> Result<Value, String> {
    match expr {
        Expr::Literal(Literal::Int(v))    => Ok(Value::Int(*v)),
        Expr::Literal(Literal::Float(v))  => Ok(Value::Float(*v)),
        Expr::Literal(Literal::String(v)) => Ok(Value::Str(v.clone())),
        Expr::Literal(Literal::Bool(v))   => Ok(Value::Bool(*v)),
        _ => Err("expected literal value".into()),
    }
}

fn eval_expr(expr: &Expr, row: &[Value], columns: &[String]) -> Value {
    match expr {
        Expr::Field(name) => {
            columns.iter().position(|c| c == name)
                .map(|i| row[i].clone())
                .unwrap_or(Value::Empty)
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
        _ => Value::Empty,
    }
}

fn eval_predicate(expr: &Expr, row: &[Value], columns: &[String]) -> bool {
    match eval_expr(expr, row, columns) {
        Value::Bool(b) => b,
        _ => false,
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
type IntReader = Box<dyn Fn(&[u8]) -> Option<i64>>;

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
            if let Some(leaf) = build_int_leaf(left, *op, right, columns, layout) {
                out.push(leaf);
                return Some(());
            }
            if let Some(leaf) = build_str_eq_leaf(left, *op, right, columns, layout, schema) {
                out.push(leaf);
                return Some(());
            }
            None
        }
        _ => None,
    }
}

/// Build an `Int` leaf from `.field <op> literal_int` (or reversed).
fn build_int_leaf(
    left: &Expr,
    op: BinOp,
    right: &Expr,
    columns: &[String],
    layout: &FastLayout,
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

/// Build a closure that reads the value of a single fixed-size int column
/// straight from row bytes. Returns None for nulls (caller decides what to do).
fn compile_int_reader(
    col_idx: usize,
    layout: &FastLayout,
) -> Option<IntReader> {
    let byte_offset = layout.fixed_offsets[col_idx]?;
    let bitmap_byte = col_idx / 8;
    let bitmap_bit = col_idx % 8;
    let data_offset = 2 + layout.bitmap_size + byte_offset;
    Some(Box::new(move |data: &[u8]| {
        let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
        if is_null {
            return None;
        }
        Some(i64::from_le_bytes(
            data[data_offset..data_offset + 8].try_into().unwrap(),
        ))
    }))
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
}
