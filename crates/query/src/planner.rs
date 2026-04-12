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
        Statement::AlterTable(at) => Ok(PlanNode::AlterTable { table: at.table, action: at.action }),
        Statement::DropTable(dt) => Ok(PlanNode::DropTable { name: dt.table }),
        Statement::CreateView(cv) => Ok(PlanNode::CreateView {
            name: cv.name,
            query_text: cv.query_text,
        }),
        Statement::RefreshView(rv) => Ok(PlanNode::RefreshView { name: rv.name }),
        Statement::DropView(dv) => Ok(PlanNode::DropView { name: dv.name }),
        Statement::Union(u) => {
            let left = plan_statement(*u.left)?;
            let right = plan_statement(*u.right)?;
            Ok(PlanNode::Union {
                left: Box::new(left),
                right: Box::new(right),
                all: u.all,
            })
        }
    }
}

fn plan_query(q: QueryExpr) -> Result<PlanNode, PlanError> {
    // Mission E1.2: if the query has joins, build a left-deep nested-loop
    // plan. Correctness first — hash-join optimization is E1.3. We also
    // don't try to fold an IndexScan under a joined query yet (the
    // leaf-level fast paths all match on `PlanNode::SeqScan { .. }`
    // literally, so mixing them into a join plan would silently break).
    if !q.joins.is_empty() {
        return plan_joined_query(q);
    }
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

    // Mission E2b: GROUP BY path — insert GroupBy + Project before
    // order/limit/offset/distinct.
    if let Some(group) = q.group_by {
        let mut proj_fields: Vec<ProjectField> = q.projection
            .map(|proj| proj.into_iter().map(|pf| ProjectField { alias: pf.alias, expr: pf.expr }).collect())
            .unwrap_or_default();
        let mut having = group.having;
        let aggregates = extract_aggregates(&mut proj_fields, &mut having);

        node = PlanNode::GroupBy {
            input: Box::new(node),
            keys: group.keys,
            aggregates,
            having,
        };

        if !proj_fields.is_empty() {
            node = PlanNode::Project { input: Box::new(node), fields: proj_fields };
        }

        if let Some(order) = q.order {
            node = PlanNode::Sort { input: Box::new(node), keys: order.keys.into_iter().map(|k| SortKey { field: k.field, descending: k.descending }).collect() };
        }
        // Offset must be applied *before* Limit: skip M rows, then take N.
        // Plan shape is Limit(Offset(...)), so Offset is built first (inner)
        // and Limit wraps it (outer).
        if let Some(off) = q.offset {
            node = PlanNode::Offset { input: Box::new(node), count: off };
        }
        if let Some(lim) = q.limit {
            node = PlanNode::Limit { input: Box::new(node), count: lim };
        }
        if q.distinct {
            node = PlanNode::Distinct { input: Box::new(node) };
        }
        return Ok(node);
    }

    if let Some(order) = q.order {
        node = PlanNode::Sort {
            input: Box::new(node),
            keys: order.keys.into_iter().map(|k| SortKey { field: k.field, descending: k.descending }).collect(),
        };
    }

    // Offset must be applied *before* Limit: skip M rows, then take N.
    // Plan shape is Limit(Offset(...)), so Offset is built first (inner)
    // and Limit wraps it (outer).
    if let Some(off) = q.offset {
        node = PlanNode::Offset { input: Box::new(node), count: off };
    }

    if let Some(lim) = q.limit {
        node = PlanNode::Limit { input: Box::new(node), count: lim };
    }

    if let Some(proj) = q.projection {
        let mut fields: Vec<ProjectField> = proj.into_iter().map(|pf| ProjectField {
            alias: pf.alias,
            expr: pf.expr,
        }).collect();
        let windows = extract_windows(&mut fields);
        if !windows.is_empty() {
            node = PlanNode::Window { input: Box::new(node), windows };
        }
        node = PlanNode::Project { input: Box::new(node), fields };
    }

    if q.distinct {
        node = PlanNode::Distinct { input: Box::new(node) };
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

/// Build a left-deep nested-loop join plan for a query with 1+ join clauses.
///
/// The plan shape for `T1 as a [inner|left|cross] join T2 as b on <pred> ...` is:
///
///   Project? (optional, from q.projection)
///   └─ Offset? / Limit? / Sort?
///      └─ Filter? (the top-level q.filter, using qualified columns)
///         └─ NestedLoopJoin { kind, on }
///            ├─ AliasScan { T1, a }
///            └─ AliasScan { T2, b }
///
/// Multi-join chains extend left-deep: a third join adds a second
/// `NestedLoopJoin` on top, with the first join's output as its `left`.
///
/// Aliases default to the source table name when the query didn't write
/// `as <name>` explicitly — that way users can always write `T.field`
/// without being forced to alias every source.
///
/// RightOuter is rewritten into LeftOuter with inputs swapped — the two
/// differ only in which side survives non-matching rows, and swapping
/// inputs lets the executor ship a single LeftOuter path.
fn plan_joined_query(q: QueryExpr) -> Result<PlanNode, PlanError> {
    let primary_alias = q.alias.clone().unwrap_or_else(|| q.source.clone());
    let mut node = PlanNode::AliasScan {
        table: q.source.clone(),
        alias: primary_alias,
    };

    for join in q.joins {
        let right_alias = join.alias.unwrap_or_else(|| join.source.clone());
        let right = PlanNode::AliasScan {
            table: join.source,
            alias: right_alias,
        };
        match join.kind {
            JoinKind::Inner | JoinKind::LeftOuter | JoinKind::Cross => {
                node = PlanNode::NestedLoopJoin {
                    left: Box::new(node),
                    right: Box::new(right),
                    on: join.on,
                    kind: join.kind,
                };
            }
            JoinKind::RightOuter => {
                // `a RIGHT OUTER JOIN b ON <p>` ≡ `b LEFT OUTER JOIN a ON <p>`.
                node = PlanNode::NestedLoopJoin {
                    left: Box::new(right),
                    right: Box::new(node),
                    on: join.on,
                    kind: JoinKind::LeftOuter,
                };
            }
        }
    }

    if let Some(pred) = q.filter {
        node = PlanNode::Filter {
            input: Box::new(node),
            predicate: pred,
        };
    }

    if let Some(order) = q.order {
        node = PlanNode::Sort {
            input: Box::new(node),
            keys: order.keys.into_iter().map(|k| SortKey { field: k.field, descending: k.descending }).collect(),
        };
    }

    // Offset must be applied *before* Limit: skip M rows, then take N.
    // Plan shape is Limit(Offset(...)), so Offset is built first (inner)
    // and Limit wraps it (outer).
    if let Some(off) = q.offset {
        node = PlanNode::Offset { input: Box::new(node), count: off };
    }

    if let Some(lim) = q.limit {
        node = PlanNode::Limit { input: Box::new(node), count: lim };
    }

    // Mission E2b: GROUP BY path for joined queries.
    if let Some(group) = q.group_by {
        let mut proj_fields: Vec<ProjectField> = q.projection
            .map(|proj| proj.into_iter().map(|pf| ProjectField { alias: pf.alias, expr: pf.expr }).collect())
            .unwrap_or_default();
        let mut having = group.having;
        let aggregates = extract_aggregates(&mut proj_fields, &mut having);

        node = PlanNode::GroupBy {
            input: Box::new(node),
            keys: group.keys,
            aggregates,
            having,
        };

        if !proj_fields.is_empty() {
            node = PlanNode::Project { input: Box::new(node), fields: proj_fields };
        }
        if q.distinct {
            node = PlanNode::Distinct { input: Box::new(node) };
        }
        return Ok(node);
    }

    if let Some(proj) = q.projection {
        let mut fields: Vec<ProjectField> = proj.into_iter().map(|pf| ProjectField {
            alias: pf.alias,
            expr: pf.expr,
        }).collect();
        let windows = extract_windows(&mut fields);
        if !windows.is_empty() {
            node = PlanNode::Window { input: Box::new(node), windows };
        }
        node = PlanNode::Project { input: Box::new(node), fields };
    }

    if q.distinct {
        node = PlanNode::Distinct { input: Box::new(node) };
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

/// Walk projection fields, replacing every `Expr::Window { .. }` with
/// `Expr::Field("__win_N")` and collecting the corresponding `WindowDef`
/// descriptors. Returns the list of window definitions to insert as a
/// `PlanNode::Window` before the `Project` node.
fn extract_windows(proj_fields: &mut [ProjectField]) -> Vec<WindowDef> {
    let mut defs = Vec::new();
    let mut counter = 0usize;
    for f in proj_fields.iter_mut() {
        if let Expr::Window { function, args, partition_by, order_by } = &f.expr {
            let output_name = format!("__win_{counter}");
            defs.push(WindowDef {
                function: *function,
                args: args.clone(),
                partition_by: partition_by.clone(),
                order_by: order_by.iter().map(|k| SortKey { field: k.field.clone(), descending: k.descending }).collect(),
                output_name: output_name.clone(),
            });
            f.expr = Expr::Field(output_name);
            counter += 1;
        }
    }
    defs
}

/// Walk projection fields and HAVING expression, replacing every
/// `Expr::FunctionCall(func, Field(col))` with `Expr::Field("__agg_N")`
/// and collecting the corresponding `GroupAgg` descriptors. Deduplicates:
/// if the same (func, field) pair appears in both projection and HAVING,
/// they share a single `GroupAgg` entry.
fn extract_aggregates(
    proj_fields: &mut [ProjectField],
    having: &mut Option<Expr>,
) -> Vec<GroupAgg> {
    let mut aggs: Vec<GroupAgg> = Vec::new();
    let mut counter = 0usize;
    for f in proj_fields.iter_mut() {
        rewrite_agg_expr(&mut f.expr, &mut aggs, &mut counter);
    }
    if let Some(h) = having {
        rewrite_agg_expr(h, &mut aggs, &mut counter);
    }
    aggs
}

fn rewrite_agg_expr(expr: &mut Expr, aggs: &mut Vec<GroupAgg>, counter: &mut usize) {
    match expr {
        Expr::FunctionCall(func, inner) => {
            if let Expr::Field(name) = inner.as_ref() {
                let output = find_or_insert_agg(aggs, *func, name, counter);
                *expr = Expr::Field(output);
            }
        }
        Expr::BinaryOp(l, _, r) => {
            rewrite_agg_expr(l, aggs, counter);
            rewrite_agg_expr(r, aggs, counter);
        }
        Expr::UnaryOp(_, inner) => rewrite_agg_expr(inner, aggs, counter),
        Expr::Coalesce(l, r) => {
            rewrite_agg_expr(l, aggs, counter);
            rewrite_agg_expr(r, aggs, counter);
        }
        Expr::InList { expr: e, list, .. } => {
            rewrite_agg_expr(e, aggs, counter);
            for item in list {
                rewrite_agg_expr(item, aggs, counter);
            }
        }
        Expr::InSubquery { expr: e, .. } => {
            rewrite_agg_expr(e, aggs, counter);
        }
        _ => {}
    }
}

fn find_or_insert_agg(
    aggs: &mut Vec<GroupAgg>,
    func: AggFunc,
    field: &str,
    counter: &mut usize,
) -> String {
    for existing in aggs.iter() {
        if existing.function == func && existing.field == field {
            return existing.output_name.clone();
        }
    }
    let output_name = format!("__agg_{counter}");
    aggs.push(GroupAgg {
        function: func,
        field: field.to_string(),
        output_name: output_name.clone(),
    });
    *counter += 1;
    output_name
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

    #[test]
    fn test_plan_inner_join_builds_nested_loop() {
        // Mission E1.2: a join query should plan to NestedLoopJoin with
        // AliasScan leaves on both sides.
        let plan = plan("User as u join Order as o on u.id = o.user_id").unwrap();
        match plan {
            PlanNode::NestedLoopJoin { left, right, on, kind } => {
                assert_eq!(kind, JoinKind::Inner);
                assert!(on.is_some());
                assert!(matches!(*left, PlanNode::AliasScan { .. }));
                assert!(matches!(*right, PlanNode::AliasScan { .. }));
            }
            other => panic!("expected NestedLoopJoin, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_right_join_rewritten_as_left_with_swapped_inputs() {
        let plan = plan("User as u right join Order as o on u.id = o.user_id").unwrap();
        match plan {
            PlanNode::NestedLoopJoin { left, right, kind, .. } => {
                assert_eq!(kind, JoinKind::LeftOuter);
                // Swapped: Order is now on the left, User on the right.
                match *left {
                    PlanNode::AliasScan { table, .. } => assert_eq!(table, "Order"),
                    other => panic!("expected AliasScan(Order), got {other:?}"),
                }
                match *right {
                    PlanNode::AliasScan { table, .. } => assert_eq!(table, "User"),
                    other => panic!("expected AliasScan(User), got {other:?}"),
                }
            }
            other => panic!("expected NestedLoopJoin, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_multi_join_is_left_deep() {
        // Three sources → two NestedLoopJoins, left-deep.
        let plan = plan(
            "User as u join Order as o on u.id = o.user_id \
             join Product as p on o.product_id = p.id",
        )
        .unwrap();
        match plan {
            PlanNode::NestedLoopJoin { left, right, .. } => {
                // Outer (Product) join: right is AliasScan(Product)
                match *right {
                    PlanNode::AliasScan { table, .. } => assert_eq!(table, "Product"),
                    other => panic!("expected AliasScan(Product), got {other:?}"),
                }
                // Outer.left is inner (Order) NestedLoopJoin
                assert!(matches!(*left, PlanNode::NestedLoopJoin { .. }));
            }
            other => panic!("expected NestedLoopJoin, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_join_with_filter_tail_wraps_filter_on_top() {
        let plan = plan(
            "User as u join Order as o on u.id = o.user_id filter o.total > 100",
        )
        .unwrap();
        match plan {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(*input, PlanNode::NestedLoopJoin { .. }));
            }
            other => panic!("expected Filter(NestedLoopJoin), got {other:?}"),
        }
    }

    #[test]
    fn test_plan_group_by_builds_groupby_node() {
        let plan = plan("User group .status { .status, n: count(.name) }").unwrap();
        // Should be Project(GroupBy(SeqScan)).
        match plan {
            PlanNode::Project { input, fields } => {
                assert_eq!(fields.len(), 2);
                match *input {
                    PlanNode::GroupBy { input: inner, keys, aggregates, having } => {
                        assert!(matches!(*inner, PlanNode::SeqScan { .. }));
                        assert_eq!(keys, vec!["status"]);
                        assert_eq!(aggregates.len(), 1);
                        assert_eq!(aggregates[0].function, AggFunc::Count);
                        assert_eq!(aggregates[0].field, "name");
                        assert!(having.is_none());
                    }
                    other => panic!("expected GroupBy, got {other:?}"),
                }
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_group_by_having_rewrites_agg_in_having() {
        let plan = plan("User group .status having count(.name) > 1 { .status }").unwrap();
        match plan {
            PlanNode::Project { input, .. } => {
                match *input {
                    PlanNode::GroupBy { having, aggregates, .. } => {
                        // The planner should have extracted count(.name) into
                        // aggregates and rewritten the HAVING to reference __agg_0.
                        assert_eq!(aggregates.len(), 1);
                        assert_eq!(aggregates[0].output_name, "__agg_0");
                        let h = having.expect("having should be Some");
                        match h {
                            Expr::BinaryOp(l, BinOp::Gt, _) => {
                                assert!(matches!(*l, Expr::Field(ref name) if name == "__agg_0"),
                                    "expected Field(__agg_0), got {l:?}");
                            }
                            other => panic!("expected BinaryOp, got {other:?}"),
                        }
                    }
                    other => panic!("expected GroupBy, got {other:?}"),
                }
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_window_inserts_window_node_before_project() {
        let plan = plan("User { .name, rn: row_number() over (order .age) }").unwrap();
        // Expected shape: Project(Window(SeqScan))
        match plan {
            PlanNode::Project { input, fields } => {
                assert_eq!(fields.len(), 2);
                // The window expr should have been replaced with Field("__win_0")
                assert!(matches!(&fields[1].expr, Expr::Field(name) if name == "__win_0"),
                    "expected Field(__win_0), got {:?}", fields[1].expr);
                match *input {
                    PlanNode::Window { input: inner, windows } => {
                        assert_eq!(windows.len(), 1);
                        assert_eq!(windows[0].output_name, "__win_0");
                        assert!(matches!(*inner, PlanNode::SeqScan { .. }));
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_multiple_windows() {
        let plan = plan(
            "User { .name, rn: row_number() over (order .age), s: sum(.salary) over (partition .dept order .salary) }"
        ).unwrap();
        match plan {
            PlanNode::Project { input, fields } => {
                assert_eq!(fields.len(), 3);
                assert!(matches!(&fields[1].expr, Expr::Field(name) if name == "__win_0"));
                assert!(matches!(&fields[2].expr, Expr::Field(name) if name == "__win_1"));
                match *input {
                    PlanNode::Window { windows, .. } => {
                        assert_eq!(windows.len(), 2);
                        assert_eq!(windows[0].output_name, "__win_0");
                        assert_eq!(windows[1].output_name, "__win_1");
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_no_window_without_over() {
        // Plain aggregate in projection should not create a Window node.
        let plan = plan("User group .dept { .dept, total: sum(.salary) }").unwrap();
        match plan {
            PlanNode::Project { input, .. } => {
                // Input should be GroupBy, not Window.
                assert!(matches!(*input, PlanNode::GroupBy { .. }),
                    "expected GroupBy under Project, got {:?}", input);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }
}
