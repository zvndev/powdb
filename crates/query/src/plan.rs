use crate::ast::{AggFunc, Expr, Assignment, JoinKind};

/// Physical plan nodes — what the executor actually runs.
#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan { table: String },
    /// Mission E1.2: sequential scan that renames output columns to
    /// `alias.field`. Used exclusively as the leaves of a join plan so
    /// downstream `NestedLoopJoin` + `Filter` + `Project` nodes can resolve
    /// `Expr::QualifiedField` lookups by direct column-name match. Kept
    /// separate from `SeqScan` so the single-table fast paths (which match
    /// on `PlanNode::SeqScan { .. }` in many places) stay untouched.
    AliasScan { table: String, alias: String },
    IndexScan { table: String, column: String, key: Expr },
    Filter { input: Box<PlanNode>, predicate: Expr },
    Project { input: Box<PlanNode>, fields: Vec<ProjectField> },
    Sort { input: Box<PlanNode>, field: String, descending: bool },
    Limit { input: Box<PlanNode>, count: Expr },
    Offset { input: Box<PlanNode>, count: Expr },
    Aggregate { input: Box<PlanNode>, function: AggFunc, field: Option<String> },
    /// Mission E1.2: nested-loop join. Correctness-first implementation —
    /// O(L × R) scan for every join. E1.3 will add a hash-join fast path
    /// for equijoins (the common case). The executor handles `Inner`,
    /// `Cross`, and `LeftOuter`; `RightOuter` is rewritten by the planner
    /// into a `LeftOuter` with swapped inputs.
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        /// Join predicate. `None` for `Cross` joins (emit every pair).
        on: Option<Expr>,
        kind: JoinKind,
    },
    Distinct { input: Box<PlanNode> },
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
