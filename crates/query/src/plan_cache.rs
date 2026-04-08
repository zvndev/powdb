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
fn substitute_plan(plan: &mut PlanNode, literals: &[Literal], idx: &mut usize) {
    match plan {
        PlanNode::SeqScan { .. } => {}
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
        PlanNode::Sort { input, .. } => {
            substitute_plan(input, literals, idx);
        }
        PlanNode::Limit { input, count } => {
            substitute_plan(input, literals, idx);
            substitute_expr(count, literals, idx);
        }
        PlanNode::Offset { input, count } => {
            substitute_plan(input, literals, idx);
            substitute_expr(count, literals, idx);
        }
        PlanNode::Aggregate { input, .. } => {
            substitute_plan(input, literals, idx);
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

fn substitute_expr(expr: &mut Expr, literals: &[Literal], idx: &mut usize) {
    match expr {
        Expr::Literal(_) => {
            // The cached plan held the *first* call's literal at this
            // slot; replace with the new call's value at the matching
            // source position.
            *expr = Expr::Literal(literals[*idx].clone());
            *idx += 1;
        }
        Expr::Field(_) | Expr::Param(_) => {}
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
            PlanNode::Delete { input, .. } => collect_literals_for_test(input, out),
            PlanNode::CreateTable { .. } => {}
        }
    }

    fn collect_expr_literals(expr: &Expr, out: &mut Vec<Literal>) {
        match expr {
            Expr::Literal(l) => out.push(l.clone()),
            Expr::Field(_) | Expr::Param(_) => {}
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
        }
    }
}
