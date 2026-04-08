# Mission A Plan Review

**Verdict:** PASS

**Summary:** The plan is on-thesis, scope-disciplined, and correctly grounded against the actual Rust code. All 15 workloads measure either the translation-tier overhead or a plan shape that protects the fast path. The single authorized parser extension is genuinely minimal and matches the design-doc intent.

---

## Thesis alignment

- **PASS** — Every workload in §1 either (a) compares PowDB end-to-end against SQLite/Postgres/MySQL on a query the SQL engines must parse+plan+execute, or (b) protects a plan shape via a regression gate. None of the 15 workloads measures network round-trips at scale, distributed throughput, or any IO-bound regime the thesis explicitly disclaims (CONTEXT.md lines 121-136).
- **PASS** — The thesis claim "removing SQL translation saves 22-42x" is honoured in workloads #1, #3, #11 (already-fast paths) and protected in workloads #2, #4-#10, #12-#15 (need FASTPATH expansion). The §1 classification of "GENERIC vs already-fast" matches what `executor.rs` actually does today (verified line-by-line below).
- **PASS** — §1 line 5 frames the mission as "PowDB is faster than SQLite/Postgres/MySQL on every PowQL-expressible workload because it removes the SQL translation tier." This is the same one-sentence thesis as `CONTEXT.md` line 12. No drift.
- **PASS** — Workload #12 (`insert_batch_1k`) is correctly framed as the "stress test" — the one path where SQLite's prepared-statement reuse is competitive — with an explicit fallback to §6 if PowDB doesn't win, instead of weakening the fast path to chase the number. That's the right framing.

## Scope creep check

- **PASS** — Read workloads #1-#10 use only PowQL features that already parse: `filter .field op literal`, `and`, `order .field desc`, `limit N`, projection `{ .field }`, and the four `count|sum|avg|min|max` aggregate forms. The only feature that doesn't parse today (`sum/avg/min/max` with a column) is explicitly addressed via the authorized parser extension (see next section).
- **PASS** — Write workloads #11-#15 use only `insert User { ... }`, `User filter ... update { ... }`, and `User filter ... delete`. All three parse today (verified `parser.rs` lines 161-188 for insert, lines 89-97 for update/delete).
- **PASS** — None of the workloads smuggle in links, group by, having, distinct, joins, subqueries, `match`, let bindings, union, like, in-list, between, or upsert. All of those are explicitly parked in §6.
- **PASS** — FASTPATH agent's scope (§3) is exactly 8 fast-path additions, one per GENERIC classification in §1. No "while-we're-in-here" optimisations. The parser extension is explicitly the minimum necessary to run workloads #6-#9.
- **PASS** — INFRA agent's scope is `docker-compose.yml` + `AGENTS.md` only — no `crates/**` files.
- **PASS** — CRITERION agent's scope is `crates/bench/benches/powql.rs` + `baseline/main.json` + `baseline/thesis-ratios.json`. No reach into `crates/query` or `crates/compare`.

## Parser-extension authorization

- **PASS** — The plan authorises *exactly* the minimum parser change needed: in `parse_aggregate_query` (parser.rs line 233), after `parse_query_tail` runs, if `query.projection` is `Some(single_field)` and the aggregate is non-count, lift that field into `AggregateExpr.field` and clear `query.projection`. That is approximately 6-8 lines of code in one function.
- **PASS** — Verified in `parser.rs:252`: the parser currently hard-codes `field: None` on every `AggregateExpr`, which is exactly what the plan calls out. The fix is narrow and isolated.
- **PASS** — Design-doc intent confirmed at `powql-language-design.md:137`: `User filter .age > 30 | avg(.age)`. The design has always intended aggregates-over-columns. The plan's authorised form (`avg(User filter .age > 30 { .age })`) is a strict subset using the trailing-projection syntax that already parses inside `parse_query_tail`. This is bug-shaped, not feature-shaped.
- **PASS** — The plan explicitly defers the pipe form `| avg(.age)`, group by, multi-field aggregates, and aggregate-with-alias to §6. Scope is bounded.
- **PASS** — Fallback path is documented at PLAN line 55-56: if FASTPATH declines the parser change, BENCH calls `engine.execute_plan(&hand_built_plan)` directly for the 4 aggregate workloads. That sacrifices the parse-tier cost in the comparison but still measures planner+executor — an acceptable degradation.

## Cross-agent contract check

- **PASS — disjoint file ownership.** I cross-checked every file claimed by every agent in §3:
  - BENCH: `crates/compare/src/main.rs`, `engines/mod.rs`, `engines/powdb.rs`, `engines/sqlite.rs`, `engines/postgres.rs`, `Cargo.toml`. Plus *imports* (does not modify) `engines/mysql.rs`.
  - MYSQL: `crates/compare/src/engines/mysql.rs` only.
  - INFRA: `docker-compose.yml`, `AGENTS.md` (`## Wide bench` section append).
  - CRITERION: `crates/bench/benches/powql.rs`, `crates/bench/baseline/main.json`, `crates/bench/baseline/thesis-ratios.json`.
  - FASTPATH: `crates/query/src/{executor,planner,plan,parser,ast}.rs`.
  - **No file appears in two agents' ownership lists.** The one shared file (`engines/mod.rs`) is BENCH-owned and just declares `pub mod mysql;` so MYSQL's file is reachable — that's a one-line dependency, not shared editing.

- **PASS — BENCH→MYSQL contract sufficient.** §3 MYSQL specifies the exact module surface (`MysqlEngine`, `try_new() -> Option<Self>`, `impl BenchEngine`), the engine name (`"mysql"` lowercase), the connection-URL resolution order, and the schema. BENCH can write its `main.rs` import code without coordinating with MYSQL mid-run.

- **PASS — CRITERION→FASTPATH race handled.** §4 explicitly addresses both orderings:
  - FASTPATH lands first → null captures reflect fast-path numbers (good).
  - CRITERION lands first → null captures reflect generic-interpreter numbers; the rebaseline commit after FASTPATH lands will explicitly state `"bench: rebaseline after Mission A fast paths"`.
  - Either way, the ratio guards at §3 CRITERION (e.g., `update_by_pk_over_powql_point ≤ 4.0`) tighten in the right direction: they're set as ceilings *after* FASTPATH lands, so they catch any subsequent un-landing of the optimisations.

- **PASS — schema drift across SQL engines handled.** All three SQL-engine schemas (sqlite, postgres, mysql) are governed by code BENCH or MYSQL writes. BENCH owns `sqlite.rs` and `postgres.rs`. MYSQL owns `mysql.rs` but §3 MYSQL specifies "same as Postgres" with the exact column list (`id BIGINT PRIMARY KEY, name TEXT NOT NULL, age BIGINT NOT NULL, status TEXT NOT NULL, email TEXT NOT NULL, created_at BIGINT NOT NULL`). PowDB's schema is updated by BENCH in `engines/powdb.rs::setup`. All four engines get the same logical schema.

## Success criteria check

- **PASS** — §5 has 8 criteria, all measurable and automated:
  1. `cargo build --release` clean — boolean.
  2. **`cargo test --workspace` green, no skipped/ignored tests** — boolean. *This is the minimum non-regression bar Kirby asked about. Present.*
  3. `docker compose up -d` succeeds — boolean.
  4. `cargo run -p powdb-compare --release` runs all 15 workloads, prints comparison + ratio tables, writes CSV, exits 0 — boolean + file existence.
  5. `cargo bench -p powdb-bench` runs and the comparator exits 0 (null baselines captured, existing baselines stay within ±7%, all thesis ratios under ceiling) — boolean.
  6. **PowDB ≥2x faster than SQLite per workload** — measurable. Explicit fallback documented: "move to §6 with a specific reason, OR block on further FASTPATH work." This protects the thesis.
  7. Regression gate runs in CI — boolean.
  8. `AGENTS.md` has `## Wide bench` section — file inspection.

- **PASS** — §5 implicitly requires explicit rebaselining commits via §4's "rebaseline commit will explicitly state 'bench: rebaseline after Mission A fast paths'" and references AGENTS.md's existing rebaseline workflow at lines 220-235. Not a vague "we'll figure it out later."

## Issues requiring changes

None blocking. Minor nitpicks below — none should block parallel agent dispatch.

### Nit 1 — wording in §3 BENCH ownership of `mysql.rs`

PLAN-MISSION-A.md line 351 says: "`crates/compare/src/engines/mysql.rs` — **MAY import and wire** from `main.rs` (`match MysqlEngine::try_new() { ... }`) but **MUST NOT create or modify the file itself.** MYSQL agent owns the file."

The phrase "MAY import and wire from main.rs" is mildly confusing — read literally it sounds like BENCH is importing from `main.rs`. The intent is "BENCH may import `MysqlEngine` *into* `main.rs`." Worth a one-word clarification but not blocking; the surrounding text makes intent unambiguous.

### Nit 2 — §3 BENCH `engines/powdb.rs` schema update is implicit

PLAN line 347 says BENCH should "implement every new method on `PowdbEngine`, preserving the mmap + RowLayout-caching pattern." This implicitly requires updating `setup()` to add `status` and `created_at` columns to the `User` schema and the row generator. The plan doesn't call this out explicitly but it's the only way the workloads can run, and BENCH owns the file exclusively, so there's no contract risk. Worth a one-line note in §3 BENCH; not blocking.

### Nit 3 — `point_lookup_indexed` requires explicit `create_index("id", ...)`

The existing `engines/powdb.rs::setup()` calls `create_index("id", ...)` (verified line 72). Since the schema changes (adding `status`, `created_at`), BENCH must remember to keep the index-creation call after the rewritten setup. This is implicit and BENCH owns the file, so no contract violation, but it's the kind of thing that, if forgotten, would silently turn workload #1 from "Project(IndexScan) fast path" into "no-index fallback" and tank the headline ratio. Worth a sentence in §3 BENCH explicitly stating "preserve `create_index('id', ...)` after the row insert loop."

## Backlog hygiene

- **PASS** — §6 tracks 17 distinct items across language/parser, schema/catalog, planner, executor, bench infrastructure, and a parser-bug observation. None of these are required to run the 15 workloads in §1.
- **PASS** — Items I'd expect to see in §6 that are present: pipe-into-aggregate, group by, link traversal, distinct/in/between/like/union/subqueries, `??` coalesce, multi-field aggregate aliases, type-name footgun, batch-insert syntax, multi-column index, conjunction splitting, cost-based planner, cached-plan API, RowLayout caching, MySQL warmup, CPU pinning, JSON output, parser bug.
- **PASS** — Nothing I noticed during code verification is missing. The §6 list comprehensively captures the scope-adjacent debt the PM saw while writing the workloads.
- **PASS** — Nothing in §6 belongs in §1. Every §6 item is genuinely a different mission's concern.

## Final notes

### Code-grounded verification

I verified the following plan claims against the actual Rust source. All check out:

1. **`parser.rs:252`** — `query.aggregation = Some(AggregateExpr { function: func, field: None });` — confirms the plan's claim that aggregates can't attach a column today. The parser-extension authorisation is grounded.

2. **`planner.rs:132-150`** — `try_extract_eq_index_key` folds *any* `.field = literal` into an `IndexScan` plan node, regardless of whether the column actually has an index. The executor decides at runtime. The plan's classification of `point_lookup_nonindexed` (#2) as "folds to `IndexScan` at plan time, then falls into the executor's 'index not present' fallback" is **correct**.

   *Minor terminology nit:* the plan's hard-check question asked whether `try_extract_eq_index_key` "really excludes non-PK columns for point_lookup_nonindexed as described" — that's not actually what the function does. It includes *all* eq-against-literal columns; the executor is what differentiates index vs no-index. The plan's *workload classification* is correct, but the framing in the hard-check question is slightly off. Just noting in case it matters to the orchestrator.

3. **`executor.rs:137-183`** — `Project(IndexScan)` fast path. Only fires if `tbl.indexes.contains_key(column)`. Falls through to generic when no index. Confirms #1 (indexed) is fast and #2 (non-indexed) is generic.

4. **`executor.rs:91-122`** — `Filter(SeqScan)` fused fast path with `try_compile_int_predicate`. Confirms #3 (count over filter) is already fast.

5. **`executor.rs:247-291`** — `Aggregate(Count, ...)` fast path. Confirms #3 already fast and that other aggregates (`Sum/Avg/Min/Max`) fall through to the generic branch (line 293) which materialises rows and errors with "sum requires field" if `field` is None. Plan classification of #6-#9 as GENERIC is correct.

6. **`executor.rs:516-567`** — `try_compile_int_predicate` only handles `field op literal_int`, returns None for `BinaryOp(And, ...)`. Confirms #10 (`multi_col_and_filter`) classification.

7. **`executor.rs:349-395`** — `Update` and `Delete` paths. Both call `execute_plan(input)` first (materialising `Vec<Vec<Value>>`), then do a second `scan()` and `rows.iter().any(|r| r == row)` value-equality check. **This is the O(N·M) bug the plan flags at workload #14 as "the worst current-day gap" and #15.** Verified.

8. **`plan.rs`** — `PlanNode` enum has all 12 variants the plan references (`SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Limit`, `Offset`, `Aggregate`, `Insert`, `Update`, `Delete`, `CreateTable`). No new plan-node variant needed. The plan correctly notes FASTPATH should "prefer reusing existing variants."

### Recommendation

Approve the plan and dispatch the 5 worker agents (BENCH, MYSQL, INFRA, CRITERION, FASTPATH) in parallel. The three nits above can be addressed by the workers in flight without re-planning — they're not contract violations, just clarifications BENCH should keep in mind.

The plan is the strongest pre-implementation document I've seen on this codebase. It does the hard verification work upfront (the "PowQL parser verification notes" preamble is excellent), grounds every workload claim against actual line numbers in the code, makes the scope expansion (the parser change) explicit and minimal, and keeps the file ownership lists genuinely disjoint. Ship it.

_End of review._
