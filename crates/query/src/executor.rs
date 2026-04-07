use crate::ast::*;
use crate::plan::*;
use crate::planner;
use crate::result::QueryResult;
use powdb_storage::catalog::Catalog;
use powdb_storage::row::{RowLayout, decode_column, decode_row};
use powdb_storage::types::*;
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
                    let layout = RowLayout::new(&schema);
                    let mut rows: Vec<Vec<Value>> = Vec::new();

                    // Try compiled predicate for the filter check
                    if let Some(compiled) = try_compile_int_predicate(predicate, &columns, &layout) {
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            if compiled(data) {
                                rows.push(decode_row(&schema, data));
                            }
                        }).map_err(|e| e.to_string())?;
                    } else {
                        let pred_cols = predicate_column_indices(predicate, &columns);
                        self.catalog.for_each_row_raw(table, |_rid, data| {
                            let pred_row = decode_selective(&schema, &layout, data, &pred_cols);
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
                        let rows = match tbl.indexes.get(column).unwrap().lookup(&key_value) {
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
                            let layout = RowLayout::new(&schema);

                            // Try compiled predicate (zero-allocation hot path)
                            if let Some(compiled) = try_compile_int_predicate(predicate, &columns, &layout) {
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
                                let pred_row = decode_selective(&schema, &layout, data, &pred_cols);
                                if eval_predicate(predicate, &pred_row, &columns) {
                                    count += 1;
                                }
                            }).map_err(|e| e.to_string())?;

                            return Ok(QueryResult::Scalar(Value::Int(count)));
                        }
                    }
                }

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
                let result = self.execute_plan(input)?;
                let (_columns, rows) = match result {
                    QueryResult::Rows { columns, rows } => (columns, rows),
                    _ => return Err("update source must be rows".into()),
                };
                let schema = self.catalog.schema(table)
                    .ok_or_else(|| format!("table '{table}' not found"))?
                    .clone();

                let matching: Vec<(RowId, Row)> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .filter(|(_, row)| rows.iter().any(|r| r == row))
                    .collect();

                let mut count = 0u64;
                for (rid, mut row) in matching {
                    for a in assignments {
                        let idx = schema.column_index(&a.field)
                            .ok_or_else(|| format!("column '{}' not found", a.field))?;
                        row[idx] = literal_to_value(&a.value)?;
                    }
                    self.catalog.update(table, rid, &row).map_err(|e| e.to_string())?;
                    count += 1;
                }
                Ok(QueryResult::Modified(count))
            }

            PlanNode::Delete { input, table } => {
                let result = self.execute_plan(input)?;
                let rows = match result {
                    QueryResult::Rows { rows, .. } => rows,
                    _ => return Err("delete source must be rows".into()),
                };

                let matching: Vec<RowId> = self.catalog.scan(table)
                    .map_err(|e| e.to_string())?
                    .filter(|(_, row)| rows.iter().any(|r| r == row))
                    .map(|(rid, _)| rid)
                    .collect();

                let count = matching.len() as u64;
                for rid in matching {
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
                if tbl.indexes.contains_key(column) {
                    let rows = match tbl.index_lookup(column, &key_value) {
                        Some((_, row)) => vec![row],
                        None => Vec::new(),
                    };
                    return Ok(QueryResult::Rows { columns, rows });
                }

                // Fallback: no index on this column. The planner emits IndexScan
                // eagerly (it has no visibility into which columns are indexed),
                // so we do the equality filter ourselves here instead of erroring.
                // This preserves the previous SeqScan+Filter behavior.
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

/// Try to compile a simple predicate (field op literal) into a closure that
/// operates directly on raw row bytes, bypassing Value allocation entirely.
/// Returns None if the predicate is too complex to compile.
fn try_compile_int_predicate(
    expr: &Expr,
    columns: &[String],
    layout: &RowLayout,
) -> Option<Box<dyn Fn(&[u8]) -> bool>> {
    if let Expr::BinaryOp(left, op, right) = expr {
        // Pattern: .field op literal_int
        let (field_name, literal_val, op) = match (left.as_ref(), right.as_ref()) {
            (Expr::Field(name), Expr::Literal(Literal::Int(v))) => (name, *v, *op),
            (Expr::Literal(Literal::Int(v)), Expr::Field(name)) => {
                // Flip: literal op field → field flipped_op literal
                let flipped = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::Lte => BinOp::Gte,
                    BinOp::Gte => BinOp::Lte,
                    other => *other, // Eq, Neq are symmetric
                };
                (name, *v, flipped)
            }
            _ => return None,
        };

        let col_idx = columns.iter().position(|c| c == field_name)?;
        let byte_offset = layout.fixed_offset(col_idx)?; // Must be fixed-size
        let bitmap_byte = col_idx / 8;
        let bitmap_bit = col_idx % 8;
        let data_offset = 2 + layout.bitmap_size() + byte_offset; // 2B length prefix + bitmap + fixed offset

        Some(Box::new(move |data: &[u8]| {
            // Check null
            let is_null = (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1;
            if is_null {
                return false;
            }
            let val = i64::from_le_bytes(
                data[data_offset..data_offset + 8].try_into().unwrap(),
            );
            match op {
                BinOp::Eq => val == literal_val,
                BinOp::Neq => val != literal_val,
                BinOp::Lt => val < literal_val,
                BinOp::Gt => val > literal_val,
                BinOp::Lte => val <= literal_val,
                BinOp::Gte => val >= literal_val,
                _ => false,
            }
        }))
    } else {
        None
    }
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
}
