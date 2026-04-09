# Mission E — Language features (perf-first)

**Status:** design draft (2026-04-07)
**Depends on:** Missions B, C, D (foundations + perf)
**Goal:** Close the language gap to "credible competitor" without sacrificing the
hot-path wins from Mission A/C/D.

## Framing

PowDB's thesis is "removing SQL translation saves 22-42x on the raw-bytes
walker hot path." Every language feature added has the potential to undo
that win by introducing planner overhead, generic dispatch, or per-row
allocations. Mission E plans the language additions with one rule:

> Every feature must compile to a plan that can EITHER fall through to a
> Mission C/D fast path OR justify its slower path with a fast-path
> optimisation of its own.

No feature ships if it would force the existing fast paths to be removed
or to "check if this feature is in use" on every row.

## Current language inventory (verified against `crates/query/src/ast.rs`)

**Statements:** Query, Insert, UpdateQuery, DeleteQuery, CreateType.

**Query clauses:** filter, order, limit, offset, projection, single
top-level aggregate (count/avg/sum/min/max).

**Expressions:** field, literal, param, binary op (eq/neq/lt/gt/lte/gte/and/or/+/-/\*//), unary op (not/exists/notexists), function call (agg only), coalesce.

**Operators:** comparison + logical + arithmetic. No LIKE, no IN, no
BETWEEN, no IS NULL.

**DDL:** CreateType only. No ALTER, no DROP, no CREATE INDEX (auto?
or never?), no DROP INDEX, no schema migration.

**Reserved tokens that are NOT yet wired (lexer accepts, parser rejects):**
`Upsert`, `Multi`, `Link`, `Index`, `On`, `Match`, `Group`,
`Transaction`, `View`, `Materialized`, `Let`, `As`, `Pipe (|)`,
`Arrow (->)`. The lexer at `crates/query/src/token.rs` is ahead of the
parser — Mission E mostly fills in the parser/planner/executor gap for
tokens that already exist.

## Feature gap (vs SQLite, prioritised)

Each feature gets a priority and a perf rationale:

| Pri | Feature | Perf risk | Why |
| --: | --- | --- | --- |
| **P0** | Multi-row INSERT | none — pure win | Mission C #10. Fixes insert_batch_1k 20x loss. |
| **P0** | Transactions (BEGIN/COMMIT/ROLLBACK) | none with WAL | Mission B exit criterion already implies it. Group-commits N inserts in one fsync. |
| **P0** | INNER JOIN (single key) | high if naive | Hash join on indexed key, raw-bytes probe. Without this we're not a database. |
| **P0** | EXPLAIN / EXPLAIN ANALYZE | none | Diagnostic-only — we need this to debug perf regressions. Should print fast-path tags. |
| **P1** | GROUP BY + multi-aggregate | medium | Hash group-by over raw bytes; pre-resolved aggregator slots. |
| **P1** | IS NULL / IS NOT NULL | low | Null bitmap is in the row layout — compile_predicate adds two leaf forms. |
| **P1** | LIKE prefix-match (`'foo%'`) | low | Prefix compare on raw bytes. Full LIKE deferred. |
| **P1** | IN (literal list) | low | Sorted-array binary search per row, or hash if list > 16. |
| **P2** | LEFT JOIN | medium | Same plan as INNER JOIN but emit nulls for unmatched build side. |
| **P2** | UPSERT (insert ... on conflict update) | medium | One index probe + branch. Worth it for write workloads. |
| **P2** | DROP TYPE / ALTER TYPE add column | low | DDL only — no hot-path change. |
| **P2** | Multi-field ORDER BY | low | Composite comparator; same heap structure. |
| **P2** | DISTINCT | medium | Hash-set over projection result; cost is O(N) hash. |
| **P3** | Subqueries (scalar) | high | Nested execution. Defer until we have query rewrite. |
| **P3** | Subqueries (correlated) | very high | Defer indefinitely. Most use cases are hash join in disguise. |
| **P3** | Window functions | very high | Defer. Not in thesis scope. |
| **P3** | Views (non-materialised) | low | Plan-time substitution. Cheap. |
| **P3** | Materialised views | high | Storage + maintenance. Defer to a separate mission. |
| **NO** | Stored procedures | n/a | Out of scope forever. |
| **NO** | Triggers | n/a | Out of scope forever. |
| **NO** | Server-side cursors | n/a | The result-streaming protocol can replace these later. |

## Detailed designs (P0 + P1)

### E1. Multi-row INSERT

**Syntax:**
```powql
insert User [
  { name := "Alice", age := 30 },
  { name := "Bob",   age := 25 },
  { name := "Carol", age := 35 },
]
```

**AST:** extend `InsertExpr.assignments` to `Vec<Vec<Assignment>>` (one
per row), or add a sibling `InsertBatch { target, rows: Vec<Vec<Assignment>> }`.
The latter is cleaner — the existing single-row Insert path stays
fast-path-clean.

**Plan:** new `PlanNode::InsertBatch { table, rows }`.

**Executor:**
1. Resolve schema once.
2. Encode all rows in a tight loop, accumulating heap-page mutations.
3. Single `wal.append + wal.flush` covering all N records.
4. Bulk-update each affected index after the heap inserts.

**Perf:** parse once, plan once, single WAL flush. Drops
`insert_batch_1k` from 6274ns/row to ~1200ns/row. Already documented in
Mission C #10.

**Edge cases:**
- Different rows can have different assignment ordering — resolve each
  individually.
- Batch fails as a unit (atomic insert) — wrap in implicit transaction.

### E2. Transactions

**Syntax:**
```powql
begin
insert User { name := "Alice", age := 30 }
update User filter .id = 1 { age := 31 }
commit
```

Or one-line: `transaction { ... ; ... ; ... }` (matches the reserved
`Transaction` token + braces).

**AST:** new `Statement::Begin`, `Statement::Commit`, `Statement::Rollback`,
`Statement::Transaction(Vec<Statement>)`.

**Plan:** transactions are not a plan node — they wrap the executor's
execution context. The Engine gains `current_tx: Option<TxContext>` and
all WAL appends within a transaction share a `tx_id`. `commit` flushes
the WAL once.

**Perf:** the structural win — group-fsync. SQLite's "wrap inserts in a
single transaction" pattern is what gives them the 20x lead on
`insert_batch_1k` in their own setup; we match it via E1, but E2 lets
the user do the same by hand for cross-statement batches.

**Concurrency interaction (Mission B):** with the RwLock on Catalog,
transactions hold a write lock for the duration. That's a coarse lock
but acceptable for now — proper MVCC is a separate mission.

**Recovery (Mission B):** WAL records have `tx_id`. On replay, only
records with a `Commit` for their `tx_id` are applied. Records from
uncommitted transactions are dropped.

### E3. INNER JOIN

**Syntax:**
```powql
User join Order on .id = .user_id { User.name, Order.total }
```

Or, using the `link` token (already reserved):
```powql
type User { id: int, name: str }
type Order { user_id: link User, total: int }
# now joins are implicit:
Order { .user.name, .total }   # auto-resolves the link
```

The `link` form is the elegant PowQL move — the schema records the
foreign-key relationship and joins are syntactic sugar over it. But the
explicit `join ... on` is necessary for ad-hoc queries against
non-linked tables.

**Plan:**
```rust
PlanNode::HashJoin {
    build: Box<PlanNode>,    // smaller side
    probe: Box<PlanNode>,    // larger side
    build_key: String,       // field name on build side
    probe_key: String,       // field name on probe side
    join_type: JoinType,     // Inner, Left, Right
}
```

The planner picks build vs probe based on cardinality estimates. With
no statistics yet, default heuristic: build = the side with an
equality predicate against an indexed column.

**Executor:**
1. Run the build side, populate a hash map: `key_value → Vec<RowBytes>`.
   (Store raw row bytes to defer decode.)
2. Walk the probe side via `for_each_row_raw`. For each row:
   - Read the probe-key bytes at the known offset
   - Hash and probe the build map
   - For each match, emit a joined row (concatenate raw byte slices,
     decode only projected columns)

**Perf rules:**
- Build side decodes only `(key_field, all_required_fields)`. No full
  decode.
- Probe side stays raw-bytes — predicate on the probe table runs as
  compile_predicate (Mission D D3).
- Joined-row emit only allocates for the projected columns.
- Hash table is `FxHashMap<i64, SmallVec<[u32; 4]>>` for int keys —
  no Value enum. Mission D D7 (lookup_int specialisation) extends to
  the join hash table.

**Expected impact:** A `User join Order on .id = .user_id` over 100K x
10K rows should run at 5-10ms — competitive with SQLite's hash join.

### E4. EXPLAIN / EXPLAIN ANALYZE

**Syntax:**
```powql
explain User filter .age > 50 { name, age } limit 10
```

Returns a tree representation of the plan with fast-path annotations:
```
Project [name, age]                       (fast-path: project_filter_limit_fast)
└── Limit 10
    └── Filter .age > 50                  (compiled: int_leaf, no AND)
        └── SeqScan User                  (mmap: enabled, early-term: yes)
```

`explain analyze` runs the query and adds per-node timing and row counts:
```
Project [name, age]                       (fast-path: project_filter_limit_fast)
   actual time: 1.2ms, rows: 10
└── Limit 10
    └── Filter .age > 50                  (compiled: int_leaf)
       actual time: 1.0ms, rows: 10 (out of 247 scanned)
        └── SeqScan User
           actual time: 0.8ms, rows: 247
```

**Perf:** EXPLAIN is a no-cost diagnostic. EXPLAIN ANALYZE has the
overhead of inserting per-node timing — gated behind the explicit
`analyze` keyword so the normal hot path is untouched. Use the same
`tracing::enabled!` guard pattern from Mission D D6.

**Why P0:** without EXPLAIN we cannot diagnose plan regressions. The
fast-path annotation is critical — it tells us at a glance whether
the executor took the slow generic path or the optimised path. **This
is how we catch perf regressions in the wild before they hit the
bench gate.**

### E5. GROUP BY + multi-aggregate

**Syntax:**
```powql
User group .status { status := .status, n := count(*), avg_age := avg(.age) }
```

**AST:** add `group_by: Option<Vec<String>>` to `QueryExpr`. Projection
fields can be aggregate expressions referencing input columns.

**Plan:**
```rust
PlanNode::HashAggregate {
    input: Box<PlanNode>,
    group_keys: Vec<String>,
    aggregates: Vec<(String, AggFunc, Option<String>)>,  // (alias, func, field)
}
```

**Executor:**
1. Hash map keyed by `(group_key_values...)` → `(state_per_aggregate...)`
2. Walk input via `for_each_row_raw`. For each row:
   - Read group key bytes, build a hash key.
   - Look up or insert state vector.
   - For each aggregate, update its state from the row's column bytes.
3. After scan, emit one row per group with finalised aggregate values.

**Perf rules:**
- Group key reads are direct byte slice → hash. No decode.
- Aggregate state updates are fixed-offset reads + arithmetic. No
  Value boxing.
- The hash map is `FxHashMap<SmallVec<[u8; 16]>, AggState>` — group
  key serialised as bytes for arbitrary types. For single-int group
  keys, specialise to `FxHashMap<i64, AggState>`.

**Expected impact:** A `group by status` over 100K rows with 5 distinct
statuses should run at ~600μs (matching count + sum + avg in one pass
vs SQLite's ~800μs).

### E6. IS NULL / IS NOT NULL

**Syntax:** standard. `filter .field is null`.

**Compile path:** new leaf in `compile_predicate`:
```rust
fn compile_null_leaf(...) -> Option<CompiledPredicate> {
    let bitmap_byte = col_idx / 8;
    let bitmap_bit = col_idx % 8;
    Some(Box::new(move |data| {
        (data[2 + bitmap_byte] >> bitmap_bit) & 1 == 1   // is_null
    }))
}
```

One byte read + bit test. Fastest predicate in the system.

### E7. LIKE prefix-match

**Syntax:** `filter .name like "alice%"`.

**Restriction:** ONLY suffix-`%` patterns in the first version. `'%foo'`
and `'%foo%'` require either a reverse index or a full scan with
substring search — defer.

**Compile path:** new leaf for prefix-eq:
```rust
Some(Box::new(move |data| {
    let slice = read_var_field(...);
    slice.starts_with(needle.as_bytes())
}))
```

`std::slice::starts_with` is SIMD-friendly.

### E8. IN (literal list)

**Syntax:** `filter .status in ("active", "pending")` or `.id in (1, 2, 3)`.

**Compile path:** if list ≤ 16 entries, linear scan inside the closure.
If > 16, build a `FxHashSet` once and probe per row.

For int IN-lists, sort and binary-search inside the closure.

## Detailed designs (P2)

### E9. UPSERT

**Syntax:**
```powql
upsert User on .email
  { name := "Alice", email := "alice@x.com", age := 30 }
  on conflict { age := excluded.age }
```

**Plan:** `PlanNode::Upsert { table, key_column, values, on_conflict }`.

**Executor:**
1. Probe the index on `key_column` for the key value.
2. If miss: do the insert path (Mission C fast insert).
3. If hit: do the in-place update fast path (Mission C #6) on the
   matched RID.

**Perf:** one extra index probe per row. With binary-search BTree
(Mission D D1) the probe is ~150ns — the upsert overhead is dominated
by the actual write, not the lookup.

### E10. LEFT JOIN

Same plan as INNER JOIN but the executor emits a row with NULLs for
the build side when the probe row doesn't match.

**Perf:** one extra branch per probe row. Negligible.

### E11. DROP TYPE / ALTER TYPE add column

**DDL only.** No hot-path interaction. Adding a column rewrites the
table's row layout — for an empty table this is a metadata change; for
a populated table it's a background rewrite (defer).

### E12. Multi-field ORDER BY

Composite comparator, same heap structure. The bounded heap fast path
(`project_filter_sort_limit_fast`) currently only supports single int
fields — extend to a tuple of (field, direction) pairs with a
generic comparator.

## Architectural rules to preserve

1. **AST IS the plan tree.** Mission E features must not introduce a
   second IR. The planner maps `Statement` → `PlanNode` directly.

2. **Fast paths are pattern matches in the executor.** Each new plan
   node gets a fast path arm. Generic dispatch is the fallback only.

3. **`for_each_row_raw` stays the inner loop.** No feature is allowed
   to require row materialisation as the only path. If a feature can
   only run on `Vec<Value>`, it goes in the slow path.

4. **No new closure boxes per row.** All compile_predicate closures
   are built once per query. New leaf types add new compile_*_leaf
   functions, not runtime branches.

5. **Decoder stays per-column.** No "decode entire row to
   `Vec<Value>`" inside any new feature's hot path.

## What to NOT add (ever)

- **Stored procedures.** SQL's worst pattern. Inline the logic on the client.
- **Triggers.** Hidden control flow. Catastrophic for perf reasoning.
- **Server-side functions in non-PowQL languages.** UDFs in Rust modules
  loaded at startup are fine; loadable .so files are not.
- **Implicit type conversion.** `1 = "1"` should be a parse error, not
  truthy.
- **NULL is not a value.** Every comparison with a null returns false,
  not null. The bench grade depends on this — three-valued logic adds
  a branch per comparison and we are not shipping that.
- **Schema-less mode.** PowDB is schema-first. Period.

## Implementation order

Phase 1 — language survival kit (1 week):
1. Multi-row INSERT (E1) — ships with Mission C #10
2. Transactions (E2) — ships with Mission B
3. EXPLAIN / EXPLAIN ANALYZE (E4) — required to debug everything below

Phase 2 — joins and grouping (1-2 weeks):
4. INNER JOIN with hash join + raw-bytes probe (E3)
5. GROUP BY + multi-aggregate hash aggregator (E5)

Phase 3 — predicate richness (3-5 days):
6. IS NULL / IS NOT NULL (E6)
7. LIKE prefix-match (E7)
8. IN (literal list) (E8)

Phase 4 — write ergonomics (3-5 days):
9. UPSERT (E9)
10. LEFT JOIN (E10)
11. Multi-field ORDER BY (E12)

Phase 5 — DDL polish (3 days):
12. DROP TYPE
13. ALTER TYPE add column (empty tables only initially)
14. Explicit CREATE INDEX / DROP INDEX

## Exit criteria

Mission E is done when:

- [ ] PowDB can run the TPC-H Q1 query (group by + multi-aggregate)
      against a generated 100MB dataset
- [ ] PowDB can run a 2-table inner join (`User × Order`) and beat
      SQLite by ≥1.5x on a 100K × 100K bench
- [ ] EXPLAIN ANALYZE output for every wide-bench workload shows the
      fast-path tag for the plan node it took
- [ ] Multi-row INSERT works and is the recommended bulk-load pattern
- [ ] BEGIN/COMMIT/ROLLBACK works and survives the recovery tests
- [ ] No existing wide-bench workload regresses
- [ ] PowQL grammar documented in `docs/powql-grammar.md`

## Open questions

- **Should the `link` keyword become the canonical join syntax?** It's
  the elegant PowQL move but it ties joins to schema declarations. The
  explicit `join ... on` form is more portable for users coming from
  SQL. Probably we ship both and let users pick.
- **Do we need a real type system for expressions?** Currently
  `eval_binop` returns `Value::Empty` on type mismatch. Should it be
  a parse-time type error instead? Type-checking the AST after parse
  would catch bugs early but adds a pass. Defer until users complain.
- **Wire protocol prepared statements.** Mission C #9 wires
  `PlanCache` internally. A real `Prepare`/`Execute` protocol message
  pair would let clients skip the canonicalisation step. Defer until
  the canonicalisation cost is measured to be the bottleneck.
- **JSON / JSONB?** Not in thesis scope. If we add it, do it as a
  proper opaque blob with extraction operators (`->`, `->>`) — never
  expand inline. Defer.

## Related missions

- **Mission B** is the foundation: WAL + concurrency + index persistence.
  Mission E's transactions ride on top of B's WAL.
- **Mission C** brings the write fast paths that E's INSERT/UPDATE
  features depend on.
- **Mission D** brings the read fast paths (early termination, mmap
  reuse, plan cache, binary search) that E's JOIN/GROUP BY rely on.
- **Future Mission F** (un-scoped): cost-based planner, statistics,
  multi-table optimisation. E's hash join uses heuristics; F would
  add real cost estimation.
