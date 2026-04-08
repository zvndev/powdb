use crate::ast::*;
use crate::parser::{parse, ParseError};
use crate::plan::*;

#[derive(Debug)]
pub struct PlanError {
    pub message: String,
}

impl From<ParseError> for PlanError {
    fn from(e: ParseError) -> Self {
        PlanError { message: e.message }
    }
}

pub fn plan(input: &str) -> Result<PlanNode, PlanError> {
    let stmt = parse(input)?;
    plan_statement(stmt)
}

pub fn plan_statement(stmt: Statement) -> Result<PlanNode, PlanError> {
    match stmt {
        Statement::Query(q) => plan_query(q),
        Statement::Insert(ins) => plan_insert(ins),
        Statement::UpdateQuery(upd) => plan_update(upd),
        Statement::DeleteQuery(del) => plan_delete(del),
        Statement::CreateType(ct) => plan_create_type(ct),
    }
}

fn plan_query(q: QueryExpr) -> Result<PlanNode, PlanError> {
    // Try to fold `filter .col = literal` into an IndexScan. The executor
    // decides at run time whether the column actually has an index — if not,
    // it transparently falls back to a sequential scan with the same predicate,
    // so this rewrite is always safe.
    //
    // We only rewrite the *simple* eq case: `filter .col = literal`. Conjunctions
    // like `filter .col = 1 and .other > 5` fall through to SeqScan + Filter.
    // Extending this to split conjunctions is a future optimization.
    let (source, filter) = match q.filter {
        Some(pred) => match try_extract_eq_index_key(&q.source, &pred) {
            Some(index_scan) => (index_scan, None),
            None => (PlanNode::SeqScan { table: q.source.clone() }, Some(pred)),
        },
        None => (PlanNode::SeqScan { table: q.source.clone() }, None),
    };
    let mut node = source;

    if let Some(pred) = filter {
        node = PlanNode::Filter { input: Box::new(node), predicate: pred };
    }

    if let Some(order) = q.order {
        node = PlanNode::Sort {
            input: Box::new(node),
            field: order.field,
            descending: order.descending,
        };
    }

    if let Some(lim) = q.limit {
        node = PlanNode::Limit { input: Box::new(node), count: lim };
    }

    if let Some(off) = q.offset {
        node = PlanNode::Offset { input: Box::new(node), count: off };
    }

    if let Some(proj) = q.projection {
        let fields = proj.into_iter().map(|pf| ProjectField {
            alias: pf.alias,
            expr: pf.expr,
        }).collect();
        node = PlanNode::Project { input: Box::new(node), fields };
    }

    if let Some(agg) = q.aggregation {
        node = PlanNode::Aggregate {
            input: Box::new(node),
            function: agg.function,
            field: agg.field,
        };
    }

    Ok(node)
}

fn plan_insert(ins: InsertExpr) -> Result<PlanNode, PlanError> {
    Ok(PlanNode::Insert {
        table: ins.target,
        assignments: ins.assignments,
    })
}

fn plan_update(upd: UpdateExpr) -> Result<PlanNode, PlanError> {
    // Mirror the read-side IndexScan fold: when the update filter is a simple
    // `.col = literal`, emit `Update(IndexScan)` so the executor's index-lookup
    // mutation fast path fires. The executor falls back to a scan if the
    // column happens to lack an index, so this is always safe.
    let source = match upd.filter {
        Some(pred) => match try_extract_eq_index_key(&upd.source, &pred) {
            Some(index_scan) => index_scan,
            None => PlanNode::Filter {
                input: Box::new(PlanNode::SeqScan { table: upd.source.clone() }),
                predicate: pred,
            },
        },
        None => PlanNode::SeqScan { table: upd.source.clone() },
    };
    Ok(PlanNode::Update {
        input: Box::new(source),
        table: upd.source,
        assignments: upd.assignments,
    })
}

fn plan_delete(del: DeleteExpr) -> Result<PlanNode, PlanError> {
    let source = match del.filter {
        Some(pred) => match try_extract_eq_index_key(&del.source, &pred) {
            Some(index_scan) => index_scan,
            None => PlanNode::Filter {
                input: Box::new(PlanNode::SeqScan { table: del.source.clone() }),
                predicate: pred,
            },
        },
        None => PlanNode::SeqScan { table: del.source.clone() },
    };
    Ok(PlanNode::Delete {
        input: Box::new(source),
        table: del.source,
    })
}

fn plan_create_type(ct: CreateTypeExpr) -> Result<PlanNode, PlanError> {
    let fields = ct.fields.into_iter().map(|f| (f.name, f.type_name, f.required)).collect();
    Ok(PlanNode::CreateTable { name: ct.name, fields })
}

/// If the predicate is a simple `.field = literal` (or `literal = .field`),
/// return a corresponding IndexScan plan node. Otherwise return None so the
/// caller can fall through to SeqScan + Filter.
///
/// The executor decides at run time whether the named column actually has a
/// B-tree index — if not, IndexScan transparently falls back to a scan +
/// equality filter on that column. That means this rewrite is always safe
/// regardless of schema/index state; it just unlocks the fast path when an
/// index happens to exist.
fn try_extract_eq_index_key(table: &str, pred: &Expr) -> Option<PlanNode> {
    let (lhs, op, rhs) = match pred {
        Expr::BinaryOp(lhs, op, rhs) => (lhs.as_ref(), *op, rhs.as_ref()),
        _ => return None,
    };
    if op != BinOp::Eq {
        return None;
    }
    let (column, key) = match (lhs, rhs) {
        (Expr::Field(name), Expr::Literal(_)) => (name.clone(), rhs.clone()),
        (Expr::Literal(_), Expr::Field(name)) => (name.clone(), lhs.clone()),
        _ => return None,
    };
    Some(PlanNode::IndexScan {
        table: table.to_string(),
        column,
        key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanNode;

    #[test]
    fn test_plan_simple_scan() {
        let plan = plan("User").unwrap();
        assert!(matches!(plan, PlanNode::SeqScan { table } if table == "User"));
    }

    #[test]
    fn test_plan_filter() {
        let plan = plan("User filter .age > 30").unwrap();
        assert!(matches!(plan, PlanNode::Filter { .. }));
    }

    #[test]
    fn test_plan_filter_with_projection() {
        let plan = plan("User filter .age > 30 { name, email }").unwrap();
        assert!(matches!(plan, PlanNode::Project { .. }));
    }

    #[test]
    fn test_plan_insert() {
        let plan = plan(r#"insert User { name := "Alice", age := 30 }"#).unwrap();
        assert!(matches!(plan, PlanNode::Insert { .. }));
    }

    #[test]
    fn test_plan_order_limit() {
        let plan = plan("User order .name limit 10").unwrap();
        match plan {
            PlanNode::Limit { input, .. } => {
                assert!(matches!(*input, PlanNode::Sort { .. }));
            }
            _ => panic!("expected Limit(Sort(SeqScan))"),
        }
    }

    #[test]
    fn test_plan_count() {
        let plan = plan("count(User)").unwrap();
        assert!(matches!(plan, PlanNode::Aggregate { .. }));
    }

    #[test]
    fn test_plan_eq_becomes_index_scan() {
        // `filter .col = literal` should fold into an IndexScan — the executor
        // falls back to a scan if the column happens to lack an index.
        let plan = plan("User filter .id = 42").unwrap();
        match plan {
            PlanNode::IndexScan { table, column, key } => {
                assert_eq!(table, "User");
                assert_eq!(column, "id");
                assert!(matches!(key, Expr::Literal(Literal::Int(42))));
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_eq_reversed_becomes_index_scan() {
        // Literal-on-the-left form should fold the same way.
        let plan = plan(r#"User filter "NYC" = .city"#).unwrap();
        assert!(matches!(plan, PlanNode::IndexScan { .. }));
    }

    #[test]
    fn test_plan_non_eq_stays_filter() {
        // `>` isn't index-eligible under this simple rewrite. Stays SeqScan+Filter.
        let plan = plan("User filter .age > 30").unwrap();
        match plan {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::SeqScan { .. }));
            }
            other => panic!("expected Filter(SeqScan), got {other:?}"),
        }
    }

    #[test]
    fn test_plan_index_scan_with_projection() {
        // Projection on top of an IndexScan should layer correctly.
        let plan = plan("User filter .id = 1 { .name }").unwrap();
        match plan {
            PlanNode::Project { input, .. } => {
                assert!(matches!(*input, PlanNode::IndexScan { .. }));
            }
            other => panic!("expected Project(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn test_plan_update_by_pk_becomes_index_scan() {
        // `.id = literal` update should fold to Update(IndexScan), not
        // Update(Filter(SeqScan)).
        let plan = plan("User filter .id = 42 update { age := 31 }").unwrap();
        match plan {
            PlanNode::Update { input, .. } => {
                assert!(matches!(*input, PlanNode::IndexScan { .. }),
                    "expected Update(IndexScan), got {input:?}");
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_update_range_stays_filter() {
        let plan = plan("User filter .age > 30 update { age := 31 }").unwrap();
        match plan {
            PlanNode::Update { input, .. } => {
                assert!(matches!(*input, PlanNode::Filter { .. }));
            }
            other => panic!("expected Update(Filter), got {other:?}"),
        }
    }

    #[test]
    fn test_plan_delete_by_pk_becomes_index_scan() {
        let plan = plan("User filter .id = 7 delete").unwrap();
        match plan {
            PlanNode::Delete { input, .. } => {
                assert!(matches!(*input, PlanNode::IndexScan { .. }));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }
}
