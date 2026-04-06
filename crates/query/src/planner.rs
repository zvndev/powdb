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
    let mut node = PlanNode::SeqScan { table: q.source.clone() };

    if let Some(pred) = q.filter {
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
    let mut source = PlanNode::SeqScan { table: upd.source.clone() };
    if let Some(pred) = upd.filter {
        source = PlanNode::Filter { input: Box::new(source), predicate: pred };
    }
    Ok(PlanNode::Update {
        input: Box::new(source),
        table: upd.source,
        assignments: upd.assignments,
    })
}

fn plan_delete(del: DeleteExpr) -> Result<PlanNode, PlanError> {
    let mut source = PlanNode::SeqScan { table: del.source.clone() };
    if let Some(pred) = del.filter {
        source = PlanNode::Filter { input: Box::new(source), predicate: pred };
    }
    Ok(PlanNode::Delete {
        input: Box::new(source),
        table: del.source,
    })
}

fn plan_create_type(ct: CreateTypeExpr) -> Result<PlanNode, PlanError> {
    let fields = ct.fields.into_iter().map(|f| (f.name, f.type_name, f.required)).collect();
    Ok(PlanNode::CreateTable { name: ct.name, fields })
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
}
