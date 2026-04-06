use crate::ast::{AggFunc, Expr, Assignment};

/// Physical plan nodes — what the executor actually runs.
#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan { table: String },
    IndexScan { table: String, column: String, key: Expr },
    Filter { input: Box<PlanNode>, predicate: Expr },
    Project { input: Box<PlanNode>, fields: Vec<ProjectField> },
    Sort { input: Box<PlanNode>, field: String, descending: bool },
    Limit { input: Box<PlanNode>, count: Expr },
    Offset { input: Box<PlanNode>, count: Expr },
    Aggregate { input: Box<PlanNode>, function: AggFunc, field: Option<String> },
    Insert { table: String, assignments: Vec<Assignment> },
    Update { input: Box<PlanNode>, table: String, assignments: Vec<Assignment> },
    Delete { input: Box<PlanNode>, table: String },
    CreateTable { name: String, fields: Vec<(String, String, bool)> },
}

#[derive(Debug, Clone)]
pub struct ProjectField {
    pub alias: Option<String>,
    pub expr: Expr,
}
