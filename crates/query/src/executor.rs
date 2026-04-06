use crate::ast::*;
use crate::plan::*;
use crate::planner;
use crate::result::QueryResult;
use batadb_storage::catalog::Catalog;
use batadb_storage::types::*;
use std::io;
use std::path::Path;

pub struct Engine {
    catalog: Catalog,
}

impl Engine {
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        Ok(Engine {
            catalog: Catalog::create(data_dir)?,
        })
    }

    pub fn execute_bataql(&mut self, input: &str) -> Result<QueryResult, String> {
        let plan = planner::plan(input).map_err(|e| e.message)?;
        self.execute_plan(&plan)
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

            PlanNode::IndexScan { .. } => Err("index scan not yet implemented".into()),
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
        let dir = std::env::temp_dir().join(format!("batadb_exec_{}_{}", std::process::id(), id));
        let mut engine = Engine::new(&dir).unwrap();
        engine.execute_bataql("type User { required name: str, required email: str, age: int }").unwrap();
        engine.execute_bataql(r#"insert User { name := "Alice", email := "alice@ex.com", age := 30 }"#).unwrap();
        engine.execute_bataql(r#"insert User { name := "Bob", email := "bob@ex.com", age := 25 }"#).unwrap();
        engine.execute_bataql(r#"insert User { name := "Charlie", email := "charlie@ex.com", age := 35 }"#).unwrap();
        engine
    }

    #[test]
    fn test_scan_all() {
        let mut engine = test_engine();
        let result = engine.execute_bataql("User").unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn test_filter() {
        let mut engine = test_engine();
        let result = engine.execute_bataql("User filter .age > 28").unwrap();
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
        let result = engine.execute_bataql("User { name }").unwrap();
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
        let result = engine.execute_bataql("count(User)").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 3),
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_update() {
        let mut engine = test_engine();
        engine.execute_bataql(r#"User filter .name = "Alice" update { age := 31 }"#).unwrap();
        let result = engine.execute_bataql(r#"User filter .name = "Alice" { name, age }"#).unwrap();
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
        engine.execute_bataql(r#"User filter .name = "Bob" delete"#).unwrap();
        let result = engine.execute_bataql("count(User)").unwrap();
        match result {
            QueryResult::Scalar(Value::Int(n)) => assert_eq!(n, 2),
            _ => panic!("expected scalar int"),
        }
    }

    #[test]
    fn test_order_limit() {
        let mut engine = test_engine();
        let result = engine.execute_bataql("User order .age desc limit 2 { name, age }").unwrap();
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
