use crate::ast::{AggFunc, AlterAction, Expr, Assignment, JoinKind, WindowFunc};

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
    Sort { input: Box<PlanNode>, keys: Vec<SortKey> },
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
    /// Mission E2b: grouped aggregation. Output columns are
    /// `keys ++ [agg.output_name for agg in aggregates]`. The optional
    /// `having` predicate is evaluated against each output row *after*
    /// aggregation — it can reference both key columns and aggregate
    /// output names (the planner rewrites `FunctionCall` nodes in the
    /// HAVING expression into `Field("__agg_N")` references).
    GroupBy {
        input: Box<PlanNode>,
        keys: Vec<String>,
        aggregates: Vec<GroupAgg>,
        having: Option<Expr>,
    },
    AlterTable { table: String, action: AlterAction },
    DropTable { name: String },
    Insert { table: String, assignments: Vec<Assignment> },
    Update { input: Box<PlanNode>, table: String, assignments: Vec<Assignment> },
    Delete { input: Box<PlanNode>, table: String },
    CreateTable { name: String, fields: Vec<(String, String, bool)> },
    /// Create a materialized view: execute query, store results, register.
    CreateView { name: String, query_text: String },
    /// Explicitly refresh a materialized view.
    RefreshView { name: String },
    /// Drop a materialized view (backing table + registry entry).
    DropView { name: String },
    /// Window function computation layer.
    Window {
        input: Box<PlanNode>,
        windows: Vec<WindowDef>,
    },
    /// UNION [ALL]: execute both sides, concatenate (ALL) or deduplicate.
    Union { left: Box<PlanNode>, right: Box<PlanNode>, all: bool },
    /// EXPLAIN: format the inner plan tree as a text result without executing.
    Explain { input: Box<PlanNode> },
}

#[derive(Debug, Clone)]
pub struct ProjectField {
    pub alias: Option<String>,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub field: String,
    pub descending: bool,
}

/// One aggregate computation inside a `PlanNode::GroupBy`.
#[derive(Debug, Clone)]
pub struct GroupAgg {
    pub function: AggFunc,
    /// Source column name to aggregate over.
    pub field: String,
    /// Synthetic output column name (`__agg_0`, `__agg_1`, …).
    pub output_name: String,
}

/// One window function definition inside a `PlanNode::Window`.
#[derive(Debug, Clone)]
pub struct WindowDef {
    pub function: WindowFunc,
    pub args: Vec<Expr>,
    pub partition_by: Vec<String>,
    pub order_by: Vec<SortKey>,
    pub output_name: String,
}
