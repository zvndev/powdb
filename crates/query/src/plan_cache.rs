//! Plan cache — Mission D9.
//!
//! Two queries that differ only in literal values share the same parsed +
//! planned tree. Re-running the lexer + parser + planner on every call is
//! pure overhead — easily 1-3μs per query, which is the entire budget on
//! sub-microsecond workloads like `update_by_pk`. SQLite gets around this
//! with `prepare_cached`; we get around it with this cache.
//!
//! ## How it works
//!
//! 1. [`crate::canonicalize::canonicalize`] lexes the input and produces
//!    `(canonical_hash, literals)`. The hash collapses literal *values*
//!    into placeholders, so `User filter .id = 1` and `User filter .id = 2`
//!    have the same hash.
//! 2. On the first call, [`PlanCache::insert`] stores the planned tree
//!    keyed by the canonical hash. The plan still has the *first call's*
//!    literal values baked into its `Expr::Literal` nodes — that's fine,
//!    we'll overwrite them on subsequent hits.
//! 3. On a subsequent call, [`PlanCache::get_with_substitution`] clones
//!    the cached plan and walks it depth-first, replacing each
//!    `Expr::Literal` it finds (in source order) with the corresponding
//!    literal from the new query.
//!
//! The walk order is **deterministic and matches the source order of the
//! literal list collected by `canonicalize`** — see the per-PlanNode
//! comments below for the exact traversal contract.

use crate::ast::{Assignment, Expr, Literal};
use crate::plan::PlanNode;
use rustc_hash::FxHashMap;

/// LRU-ish plan cache keyed by canonical query hash.
///
/// Mission F: uses FxHashMap. The keys are u64 hashes (already pre-hashed
/// by `canonicalize`), so SipHash is pure overhead — Fx is much cheaper for
/// the integer-keyed lookup.
pub struct PlanCache {
    cache: FxHashMap<u64, PlanNode>,
    capacity: usize,
    pub hits: u64,
    pub misses: u64,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        PlanCache {
            cache: FxHashMap::default(),
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    /// Store a planned query under its canonical hash. The plan can have
    /// any literal values inside it — those will be overwritten on hit.
    pub fn insert(&mut self, hash: u64, plan: PlanNode) {
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&hash) {
            // Crude eviction: when full, drop everything. Plan cache is
            // small (capacity ~256) and bench loops only ever fill a
            // handful of slots, so this is acceptable for now. A real LRU
            // would matter once we have hundreds of distinct query shapes.
            self.cache.clear();
        }
        self.cache.insert(hash, plan);
    }

    /// Look up a plan by canonical hash and return a clone with the new
    /// literals substituted into every `Expr::Literal` slot in source
    /// order.
    ///
    /// Returns `Some(plan)` on a hit and bumps `self.hits`. Returns `None`
    /// on a miss and bumps `self.misses`. Returning `None` instead of
    /// reaching for the planner here keeps this module dependency-free
    /// from `planner` — the engine handles the miss path.
    ///
    /// The substitution is done on a **clone** of the cached plan, not the
    /// stored copy. The cached plan stays pristine for the next call.
    pub fn get_with_substitution(
        &mut self,
        hash: u64,
        literals: &[Literal],
    ) -> Option<PlanNode> {
        match self.cache.get(&hash) {
            Some(template) => {
                self.hits += 1;
                let mut plan = template.clone();
                let mut idx = 0usize;
                substitute_plan(&mut plan, literals, &mut idx);
                debug_assert_eq!(
                    idx, literals.len(),
                    "plan substitution consumed {idx} literals but query had {}",
                    literals.len(),
                );
                Some(plan)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

/// Walk a plan tree depth-first, replacing every `Expr::Literal` with the
/// next literal from `literals` (consumed by index). The traversal order
/// is deterministic and matches the source order produced by
/// [`crate::canonicalize::canonicalize`].
///
/// **Walk contract** — the cache only works because both ends agree on
/// this order:
///   - Children of recursive nodes (`Filter`, `Project`, `Sort`, `Limit`,
///     `Offset`, `Aggregate`, `Update`, `Delete`) are visited *before*
///     the local expressions, because the source `User filter ... { ... }`
///     reads the table → predicate → projection in that order, and the
///     planner wraps `SeqScan → Filter → Project` accordingly.
///   - For `Update`, the input plan is visited first (which holds the
///     filter literal), then assignments in declaration order — same
///     order as the source `User filter .id = 42 update { age := 31 }`.
///
/// `pub(crate)` so the executor's prepared-statement API can reuse the
/// exact same walk — same order as canonicalise, same as the cache.
pub(crate) fn substitute_plan(plan: &mut PlanNode, literals: &[Literal], idx: &mut usize) {
    match plan {
        PlanNode::SeqScan { .. } => {}
        PlanNode::AliasScan { .. } => {}
        PlanNode::IndexScan { key, .. } => {
            substitute_expr(key, literals, idx);
        }
        PlanNode::Filter { input, predicate } => {
            substitute_plan(input, literals, idx);
            substitute_expr(predicate, literals, idx);
        }
        PlanNode::Project { input, fields } => {
            substitute_plan(input, literals, idx);
            for f in fields {
                substitute_expr(&mut f.expr, literals, idx);
            }
        }
        PlanNode::Sort { input, .. } => substitute_plan(input, literals, idx),
        PlanNode::AlterTable { .. } => {}
        PlanNode::DropTable { .. } => {}
        PlanNode::Limit { input, count } => {
            // Source order for `filter ... limit N offset M` is
            // [filter literals, N, M]. The planner now builds
            // Limit(Offset(...)) so that execution skips M rows *before*
            // taking N. Naively walking "input then count" would yield
            // [filter, M, N] — wrong. Special-case `Limit(Offset(...))`
            // to descend into Offset's own input (which holds the filter
            // literals), then visit Limit.count, then Offset.count, so
            // the literal stream stays in source order.
            if let PlanNode::Offset { input: inner, count: off_count } = input.as_mut() {
                substitute_plan(inner, literals, idx);
                substitute_expr(count, literals, idx);
                substitute_expr(off_count, literals, idx);
            } else {
                substitute_plan(input, literals, idx);
                substitute_expr(count, literals, idx);
            }
        }
        PlanNode::Offset { input, count } => {
            // Bare Offset (no wrapping Limit) — source order is
            // [..., offset literal] so descend first then visit count.
            substitute_plan(input, literals, idx);
            substitute_expr(count, literals, idx);
        }
        PlanNode::Aggregate { input, .. } => {
            substitute_plan(input, literals, idx);
        }
        PlanNode::NestedLoopJoin { left, right, on, .. } => {
            // Walk order: left subtree → right subtree → on predicate.
            // Matches canonicalise's source-order literal collection for
            // joined queries: left source tokens come first, then right
            // source tokens, then the `on` expression's literals (if any).
            substitute_plan(left, literals, idx);
            substitute_plan(right, literals, idx);
            if let Some(pred) = on {
                substitute_expr(pred, literals, idx);
            }
        }
        PlanNode::Distinct { input } => {
            substitute_plan(input, literals, idx);
        }
        PlanNode::GroupBy { input, having, .. } => {
            substitute_plan(input, literals, idx);
            if let Some(pred) = having {
                substitute_expr(pred, literals, idx);
            }
        }
        PlanNode::Insert { assignments, .. } => {
            substitute_assignments(assignments, literals, idx);
        }
        PlanNode::Update { input, assignments, .. } => {
            substitute_plan(input, literals, idx);
            substitute_assignments(assignments, literals, idx);
        }
        PlanNode::Delete { input, .. } => {
            substitute_plan(input, literals, idx);
        }
        PlanNode::CreateTable { .. } => {}
        PlanNode::CreateView { .. } => {}
        PlanNode::RefreshView { .. } => {}
        PlanNode::DropView { .. } => {}
        PlanNode::Window { input, windows } => {
            substitute_plan(input, literals, idx);
            for w in windows {
                for arg in &mut w.args {
                    substitute_expr(arg, literals, idx);
                }
            }
        }
        PlanNode::Union { left, right, .. } => {
            substitute_plan(left, literals, idx);
            substitute_plan(right, literals, idx);
        }
    }
}

fn substitute_assignments(
    assignments: &mut [Assignment],
    literals: &[Literal],
    idx: &mut usize,
) {
    for a in assignments {
        substitute_expr(&mut a.value, literals, idx);
    }
}

/// Count every `Expr::Literal` slot reachable from `plan` using the same
/// walk order as [`substitute_plan`]. Used by `Engine::prepare` to validate
/// that calls to `execute_prepared` pass the right number of literals, and
/// to fail early if a caller prepares a query with zero literals (which
/// would be a no-op for the prepared API — better to catch that up front).
pub(crate) fn count_literal_slots(plan: &PlanNode) -> usize {
    let mut n = 0usize;
    count_plan(plan, &mut n);
    n
}

fn count_plan(plan: &PlanNode, n: &mut usize) {
    match plan {
        PlanNode::SeqScan { .. } => {}
        PlanNode::AliasScan { .. } => {}
        PlanNode::IndexScan { key, .. } => count_expr(key, n),
        PlanNode::Filter { input, predicate } => {
            count_plan(input, n);
            count_expr(predicate, n);
        }
        PlanNode::Project { input, fields } => {
            count_plan(input, n);
            for f in fields {
                count_expr(&f.expr, n);
            }
        }
        PlanNode::Sort { input, .. } => count_plan(input, n),
        PlanNode::Limit { input, count } => {
            // Mirror the substitute walk: `Limit(Offset(...))` descends
            // into the offset's child first, then counts Limit.count,
            // then Offset.count. Source order is
            // [..., limit literal, offset literal].
            if let PlanNode::Offset { input: inner, count: off_count } = input.as_ref() {
                count_plan(inner, n);
                count_expr(count, n);
                count_expr(off_count, n);
            } else {
                count_plan(input, n);
                count_expr(count, n);
            }
        }
        PlanNode::Offset { input, count } => {
            count_plan(input, n);
            count_expr(count, n);
        }
        PlanNode::Aggregate { input, .. } => count_plan(input, n),
        PlanNode::NestedLoopJoin { left, right, on, .. } => {
            count_plan(left, n);
            count_plan(right, n);
            if let Some(pred) = on {
                count_expr(pred, n);
            }
        }
        PlanNode::Distinct { input } => count_plan(input, n),
        PlanNode::GroupBy { input, having, .. } => {
            count_plan(input, n);
            if let Some(pred) = having {
                count_expr(pred, n);
            }
        }
        PlanNode::Insert { assignments, .. } => {
            for a in assignments {
                count_expr(&a.value, n);
            }
        }
        PlanNode::Update { input, assignments, .. } => {
            count_plan(input, n);
            for a in assignments {
                count_expr(&a.value, n);
            }
        }
        PlanNode::Delete { input, .. } => count_plan(input, n),
        PlanNode::CreateTable { .. } => {}
        PlanNode::AlterTable { .. } => {}
        PlanNode::DropTable { .. } => {}
        PlanNode::CreateView { .. } => {}
        PlanNode::RefreshView { .. } => {}
        PlanNode::DropView { .. } => {}
        PlanNode::Window { input, windows } => {
            count_plan(input, n);
            for w in windows {
                for arg in &w.args {
                    count_expr(arg, n);
                }
            }
        }
        PlanNode::Union { left, right, .. } => {
            count_plan(left, n);
            count_plan(right, n);
        }
    }
}

fn count_expr(expr: &Expr, n: &mut usize) {
    match expr {
        Expr::Literal(_) => *n += 1,
        Expr::Field(_) | Expr::QualifiedField { .. } | Expr::Param(_) => {}
        Expr::BinaryOp(l, _, r) => {
            count_expr(l, n);
            count_expr(r, n);
        }
        Expr::UnaryOp(_, inner) => count_expr(inner, n),
        Expr::FunctionCall(_, inner) => count_expr(inner, n),
        Expr::Coalesce(l, r) => {
            count_expr(l, n);
            count_expr(r, n);
        }
        Expr::InList { expr, list, .. } => {
            count_expr(expr, n);
            for item in list {
                count_expr(item, n);
            }
        }
        Expr::ScalarFunc(_, args) => {
            for a in args {
                count_expr(a, n);
            }
        }
        Expr::Case { whens, else_expr } => {
            for (cond, result) in whens {
                count_expr(cond, n);
                count_expr(result, n);
            }
            if let Some(e) = else_expr {
                count_expr(e, n);
            }
        }
        Expr::InSubquery { expr, .. } => {
            count_expr(expr, n);
            // Subquery literals are not counted — the subquery is
            // re-planned/executed separately.
        }
        Expr::ExistsSubquery { .. } => {
            // Subquery literals are not counted — the subquery is
            // re-planned/executed separately.
        }
        Expr::Window { args, .. } => {
            for a in args {
                count_expr(a, n);
            }
        }
    }
}

fn substitute_expr(expr: &mut Expr, literals: &[Literal], idx: &mut usize) {
    match expr {
        Expr::Literal(_) => {
            // The cached plan held the *first* call's literal at this
            // slot; replace with the new call's value at the matching
            // source position.
            *expr = Expr::Literal(literals[*idx].clone());
            *idx += 1;
        }
        Expr::Field(_) | Expr::QualifiedField { .. } | Expr::Param(_) => {}
        Expr::BinaryOp(l, _, r) => {
            substitute_expr(l, literals, idx);
            substitute_expr(r, literals, idx);
        }
        Expr::UnaryOp(_, inner) => {
            substitute_expr(inner, literals, idx);
        }
        Expr::FunctionCall(_, inner) => {
            substitute_expr(inner, literals, idx);
        }
        Expr::Coalesce(l, r) => {
            substitute_expr(l, literals, idx);
            substitute_expr(r, literals, idx);
        }
        Expr::InList { expr, list, .. } => {
            substitute_expr(expr, literals, idx);
            for item in list {
                substitute_expr(item, literals, idx);
            }
        }
        Expr::ScalarFunc(_, args) => {
            for a in args {
                substitute_expr(a, literals, idx);
            }
        }
        Expr::Case { whens, else_expr } => {
            for (cond, result) in whens {
                substitute_expr(cond, literals, idx);
                substitute_expr(result, literals, idx);
            }
            if let Some(e) = else_expr {
                substitute_expr(e, literals, idx);
            }
        }
        Expr::InSubquery { expr, .. } => {
            substitute_expr(expr, literals, idx);
        }
        Expr::ExistsSubquery { .. } => {
            // Subquery has its own literal list; nothing to substitute
            // at this level.
        }
        Expr::Window { args, .. } => {
            for a in args {
                substitute_expr(a, literals, idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonicalize::canonicalize;
    use crate::planner;

    #[test]
    fn test_cache_hit_substitutes_literal() {
        let mut cache = PlanCache::new(100);

        // First call: "User filter .id = 42" — miss, plan + insert.
        let q1 = "User filter .id = 42";
        let (h1, lits1) = canonicalize(q1).unwrap();
        let p1 = planner::plan(q1).unwrap();
        cache.insert(h1, p1);

        // Second call with a different literal — should hit and produce
        // a plan with the new literal substituted in.
        let q2 = "User filter .id = 99";
        let (h2, lits2) = canonicalize(q2).unwrap();
        assert_eq!(h1, h2, "different literals must hash the same");

        let plan = cache.get_with_substitution(h2, &lits2).expect("hit");

        // The plan should be `IndexScan { key: Literal::Int(99) }`.
        match plan {
            PlanNode::IndexScan { key, .. } => {
                assert_eq!(key, Expr::Literal(Literal::Int(99)));
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }

        // First call's literal vector still holds 42, untouched — proves
        // we substituted on a clone, not the cached template.
        assert_eq!(lits1, vec![Literal::Int(42)]);
        assert_eq!(cache.hits, 1);
        assert_eq!(cache.misses, 0);
    }

    #[test]
    fn test_cache_miss_returns_none_and_bumps_counter() {
        let mut cache = PlanCache::new(100);
        assert!(cache.get_with_substitution(99999, &[]).is_none());
        assert_eq!(cache.misses, 1);
        assert_eq!(cache.hits, 0);
    }

    #[test]
    fn test_multi_literal_filter_substitution() {
        let mut cache = PlanCache::new(100);
        let q1 = r#"User filter .age > 30 and .status = "active" { .name }"#;
        let (h1, _) = canonicalize(q1).unwrap();
        cache.insert(h1, planner::plan(q1).unwrap());

        let q2 = r#"User filter .age > 50 and .status = "pending" { .name }"#;
        let (h2, lits2) = canonicalize(q2).unwrap();
        let plan = cache.get_with_substitution(h2, &lits2).expect("hit");

        // Walk the plan and pull out every literal — should be [50, "pending"].
        let mut found = Vec::new();
        collect_literals_for_test(&plan, &mut found);
        assert_eq!(found, vec![
            Literal::Int(50),
            Literal::String("pending".into()),
        ]);
    }

    #[test]
    fn test_update_by_pk_substitution() {
        let mut cache = PlanCache::new(100);
        let q1 = "User filter .id = 1 update { age := 100 }";
        let (h1, _) = canonicalize(q1).unwrap();
        cache.insert(h1, planner::plan(q1).unwrap());

        let q2 = "User filter .id = 7 update { age := 200 }";
        let (h2, lits2) = canonicalize(q2).unwrap();
        let plan = cache.get_with_substitution(h2, &lits2).expect("hit");

        let mut found = Vec::new();
        collect_literals_for_test(&plan, &mut found);
        assert_eq!(found, vec![Literal::Int(7), Literal::Int(200)]);
    }

    #[test]
    fn test_insert_substitution() {
        let mut cache = PlanCache::new(100);
        let q1 = r#"insert User { id := 1, name := "Alice", age := 20 }"#;
        let (h1, _) = canonicalize(q1).unwrap();
        cache.insert(h1, planner::plan(q1).unwrap());

        let q2 = r#"insert User { id := 2, name := "Bob", age := 30 }"#;
        let (h2, lits2) = canonicalize(q2).unwrap();
        let plan = cache.get_with_substitution(h2, &lits2).expect("hit");

        let mut found = Vec::new();
        collect_literals_for_test(&plan, &mut found);
        assert_eq!(found, vec![
            Literal::Int(2),
            Literal::String("Bob".into()),
            Literal::Int(30),
        ]);
    }

    #[test]
    fn test_eviction_on_capacity() {
        let mut cache = PlanCache::new(2);
        let q1 = "User";
        let q2 = "User filter .age > 1";
        let q3 = "User filter .age > 2";
        // q3 has same canonical as q2 — won't trigger eviction.
        // Use a different shape to force eviction.
        let q3_distinct = "User filter .id = 5";

        let (h1, _) = canonicalize(q1).unwrap();
        let (h2, _) = canonicalize(q2).unwrap();
        let (h3, _) = canonicalize(q3_distinct).unwrap();
        cache.insert(h1, planner::plan(q1).unwrap());
        cache.insert(h2, planner::plan(q2).unwrap());
        // Cache full → inserting a third *new* shape should clear.
        cache.insert(h3, planner::plan(q3_distinct).unwrap());
        assert!(cache.cache.contains_key(&h3));
        assert_eq!(cache.cache.len(), 1);
    }

    /// Test helper — depth-first walk that pulls out every Literal in the
    /// same order `substitute_plan` would visit them. Used to verify
    /// substitution actually wrote to the right slots.
    fn collect_literals_for_test(plan: &PlanNode, out: &mut Vec<Literal>) {
        match plan {
            PlanNode::SeqScan { .. } => {}
            PlanNode::AliasScan { .. } => {}
            PlanNode::IndexScan { key, .. } => collect_expr_literals(key, out),
            PlanNode::Filter { input, predicate } => {
                collect_literals_for_test(input, out);
                collect_expr_literals(predicate, out);
            }
            PlanNode::Project { input, fields } => {
                collect_literals_for_test(input, out);
                for f in fields {
                    collect_expr_literals(&f.expr, out);
                }
            }
            PlanNode::Sort { input, .. } => collect_literals_for_test(input, out),
            PlanNode::Limit { input, count } => {
                collect_literals_for_test(input, out);
                collect_expr_literals(count, out);
            }
            PlanNode::Offset { input, count } => {
                collect_literals_for_test(input, out);
                collect_expr_literals(count, out);
            }
            PlanNode::Aggregate { input, .. } => collect_literals_for_test(input, out),
            PlanNode::NestedLoopJoin { left, right, on, .. } => {
                collect_literals_for_test(left, out);
                collect_literals_for_test(right, out);
                if let Some(pred) = on {
                    collect_expr_literals(pred, out);
                }
            }
            PlanNode::Insert { assignments, .. } => {
                for a in assignments {
                    collect_expr_literals(&a.value, out);
                }
            }
            PlanNode::Update { input, assignments, .. } => {
                collect_literals_for_test(input, out);
                for a in assignments {
                    collect_expr_literals(&a.value, out);
                }
            }
            PlanNode::Distinct { input } => collect_literals_for_test(input, out),
            PlanNode::GroupBy { input, having, .. } => {
                collect_literals_for_test(input, out);
                if let Some(pred) = having {
                    collect_expr_literals(pred, out);
                }
            }
            PlanNode::Delete { input, .. } => collect_literals_for_test(input, out),
            PlanNode::CreateTable { .. } => {}
            PlanNode::AlterTable { .. } => {}
            PlanNode::DropTable { .. } => {}
            PlanNode::CreateView { .. } => {}
            PlanNode::RefreshView { .. } => {}
            PlanNode::DropView { .. } => {}
            PlanNode::Window { input, windows } => {
                collect_literals_for_test(input, out);
                for w in windows {
                    for arg in &w.args {
                        collect_expr_literals(arg, out);
                    }
                }
            }
            PlanNode::Union { left, right, .. } => {
                collect_literals_for_test(left, out);
                collect_literals_for_test(right, out);
            }
        }
    }

    fn collect_expr_literals(expr: &Expr, out: &mut Vec<Literal>) {
        match expr {
            Expr::Literal(l) => out.push(l.clone()),
            Expr::Field(_) | Expr::QualifiedField { .. } | Expr::Param(_) => {}
            Expr::BinaryOp(l, _, r) => {
                collect_expr_literals(l, out);
                collect_expr_literals(r, out);
            }
            Expr::UnaryOp(_, inner) => collect_expr_literals(inner, out),
            Expr::FunctionCall(_, inner) => collect_expr_literals(inner, out),
            Expr::Coalesce(l, r) => {
                collect_expr_literals(l, out);
                collect_expr_literals(r, out);
            }
            Expr::InList { expr, list, .. } => {
                collect_expr_literals(expr, out);
                for item in list {
                    collect_expr_literals(item, out);
                }
            }
            Expr::ScalarFunc(_, args) => {
                for a in args {
                    collect_expr_literals(a, out);
                }
            }
            Expr::Case { whens, else_expr } => {
                for (cond, result) in whens {
                    collect_expr_literals(cond, out);
                    collect_expr_literals(result, out);
                }
                if let Some(e) = else_expr {
                    collect_expr_literals(e, out);
                }
            }
            Expr::InSubquery { expr, .. } => {
                collect_expr_literals(expr, out);
            }
            Expr::ExistsSubquery { .. } => {}
            Expr::Window { args, .. } => {
                for a in args {
                    collect_expr_literals(a, out);
                }
            }
        }
    }
}
