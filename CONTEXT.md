# PowDB — the bigger picture

> Read this *before* `HANDOFF.md`. HANDOFF tells you what just shipped and what's
> next tactically. This file tells you **why PowDB exists**, how we got here,
> and what "success" looks like. Without this you'll make good-looking decisions
> that miss the point.

---

## The one-sentence thesis

**Most databases spend 22–42x more work translating SQL than actually doing the query. PowDB removes the translation tier.**

That's it. That's the whole reason this project exists. Every architectural
decision traces back to this claim, and every benchmark exists to keep that
claim honest.

---

## Why that thesis, and how we know the number

Before the Rust work, there was a **JavaScript scaffolding project** (now
parked under `turbodb-experiments/` inside this repo — keep it, don't delete).
It wasn't a product. It was an instrumentation rig built to answer a single
question: *when SQLite or Postgres serves a point lookup, where does the time
actually go?*

The JS scaffolding measured:
- **1.06M point lookups/sec** via a raw B-tree (direct API)
- **~25K queries/sec** when the same lookup went through a full SQL parse + plan + execute

That's a **42x gap**. And it wasn't I/O, wasn't disk, wasn't cache. It was the
SQL translation tier: lexing, parsing into a tree matching an ancient grammar,
name resolution against a catalog, query rewriting, cost-based planning,
bytecode emission, VDBE interpretation. All for a point lookup that the
underlying storage engine could serve in ~1µs.

**The conclusion:** if you control the query language and keep it close enough
to the physical plan that no rewriting is needed, you can serve the same point
lookup at ~2–3µs instead of ~40µs. Not by making the B-tree faster — by
deleting the translation tier.

That's what PowQL is. It's not a "better SQL." It's a language designed so the
parser's AST **is already a plan tree**. `User filter .id = 42 { .name }`
lexes in ~200ns, parses in ~400ns, plans in ~100ns, executes in ~1,200ns.
Total: ~2,600ns. We just measured it.

---

## Where we are in the roadmap

PowDB is being built in **layers**, each with a checkpoint:

| Layer | What | Status |
|-------|------|--------|
| **1. Storage engine** | heap, B-tree, WAL, MVCC, catalog, page cache | **Shipped.** 64 passing tests, reviewed, hardened. |
| **2. Query engine** | lexer, parser, planner, executor, plan cache | **Shipped.** 36 passing tests. Planner IndexScan fold just landed. |
| **3. Wire protocol + server** | binary protocol, Tokio TCP server, auth | **Shipped.** Deployed to Fly at `zvndev-powdb`. |
| **4. Clients** | TypeScript client with codec | **Shipped.** `@zvndev/powdb-client` (not published). |
| **5. Regression gate** | formal criterion bench with CI blocking | **← you are here.** |
| **6. CLI polish** | REPL, error messages, schema inspection | Partial. |
| **7. Migrations + DDL** | `alter type`, index management, schema versioning | Not started. |
| **8. Replication / backup** | point-in-time restore, logical streaming | Design only. |
| **9. Multi-tenancy / isolation** | namespaces, row-level auth | Design only. |

**The planner fix that landed yesterday is the last load-bearing piece of the
thesis.** Before it, the full PowQL query path was 7.5ms (133 ops/sec) because
`.id = 42` was running as SeqScan+Filter over 50K rows. Now it's 2.6µs (378K
ops/sec). That's the **3,020x speedup** you keep seeing mentioned — it's not
a microbenchmark trick, it's "the planner was correctly using the index" vs
"the planner was ignoring the index."

With the fold in place, the ratio of full-query to raw-B-tree is **~2.1x**
(2,641ns ÷ 1,237ns). That's the number the thesis predicted: single-digit-x,
not 40x. The thesis is validated in Rust.

---

## Why the regression gate matters NOW

This is the question the new session needs to understand before choosing
workloads, thresholds, or CI strategies.

**It's not about shipping a benchmark artifact.** Kirby doesn't need a chart
for a blog post, a pitch deck, or a marketing page. This isn't "PowDB vs
Postgres" content.

**It's about protecting the thesis during future work.** Layers 6–9 will touch
the planner, the executor, the parser, the type system. Any of them can
silently regress the fast path. A 2x regression at layer [4] (full PowQL)
would quietly destroy the thesis without failing any functional test — queries
would still return correct results, just slower. The gate exists to **notice
that, immediately, and block the merge**.

The gate's job: *"if a commit makes the full PowQL point-query path slower by
more than the noise floor, CI fails and someone has to decide — on purpose —
whether the regression is worth it."*

That framing shapes every downstream decision:

- **Workloads** must include the exact paths the thesis rests on. The raw
  B-tree floor (so we can see if the floor itself moved), the full PowQL path
  (so we can see the ratio), and enough coverage of other paths (insert, scan,
  aggregation) to catch collateral damage.
- **Thresholds** should be set where *real* regressions trip them but noise
  doesn't. Criterion's default noise threshold (2%) is probably too tight;
  10% is probably too loose. 5% is a reasonable starting point.
- **Baseline update workflow** must be *deliberate* — regressions shouldn't
  quietly rebaseline themselves. A human commits the new baseline JSON with
  a commit message explaining why the slowdown is acceptable.
- **CI integration** should block merges, not just warn. A warning that nobody
  looks at is worse than no gate at all.
- **Machine variance** matters because we care about ratios more than absolute
  numbers. Pin to one GHA runner type, accept ~10% absolute variance across
  runs, rely on the ratio (e.g., "full PowQL ≤ 2.5x raw B-tree") as the real
  guard.

---

## What PowDB is NOT trying to be

This is as important as what it is. The new session will drift if it forgets
this list.

- **Not a Postgres replacement.** No joins (yet), no subqueries, no stored
  procedures, no extensions ecosystem, no `EXPLAIN ANALYZE`.
- **Not a SQLite replacement.** Smaller feature surface, different query
  language, needs a server (embedded mode is future).
- **Not distributed.** Single node, single process. Replication is in the
  design doc but not planned for this year.
- **Not a public product yet.** Private repo, no landing page, no public
  benchmarks, no comparison charts. Kirby is validating the thesis first.
- **Not SQL-compatible.** PowQL is the language. Zero effort is being spent
  on a SQL front-end. The whole point is to *not* have a SQL front-end.
- **Not "a database but AI-native" or any LLM tie-in.** It's just a database.

---

## The aspirational vs the shipped

There are several design docs in the repo root (`powdb-implementation-brief.md`,
`powdb-wire-protocol.md`, `powql-language-design.md`) that describe the full
long-term vision. **These include things that are not built yet:**

- C FFI API (`pow_conn`, `pow_compile`, `pow_execute`, etc.)
- Link traversal in PowQL (`User { posts: .posts { title } }`)
- Group/let bindings
- `??` default operator
- Embedded mode (`.pow` file open)
- Cursor/pagination in the wire protocol

If you read those docs, **verify against the actual code** before quoting them
as fact. When the design doc and the code disagree, AGENTS.md at the repo root
is the source of truth for what actually works. The design docs are
north-star, not spec.

---

## How Kirby works (the human)

- **Solo builder across many projects.** PowDB is one of several parallel
  tracks (turbine-orm, Bevrly, Parcelle, Compose). Context is expensive.
- **Bias toward action.** "LETS GO - BUILD BEAUTIFUL THINGS PEOPLE LOVE" is
  the literal vibe. Don't stall on ceremony.
- **Less A/B/C/D, more decisive recommendations.** When you offer four
  options, Kirby has to do four option-evaluations. When you offer one
  recommendation with a one-sentence rationale and "push back if wrong,"
  Kirby either says "yep" or gives you a tight correction. The second shape
  is ~10x faster.
- **Go further than you think.** Default Claude behavior is to stop at the
  obvious first implementation. Kirby's consistent feedback: push past it.
  Build the thing more completely before asking.
- **Check in at real decision points.** Schema shape, API surface, UI
  direction — yes, surface it. Internal mechanics (which HashMap impl, which
  error type) — just pick and go.
- **Private repo, ZVN DEV git identity.** Don't override git config.

---

## What "done" looks like for the current task

After brainstorming → spec → plan → implementation:

1. `cargo bench -p powdb-bench` runs a criterion suite covering the critical
   paths in ~60s total.
2. `crates/bench/baseline/main.json` (or equivalent) is checked in.
3. A script / cargo target compares current results against the baseline and
   exits non-zero if any workload regresses beyond a set threshold.
4. GitHub Actions runs the bench on PRs targeting `main`, and blocks merge if
   the comparator exits non-zero.
5. Updating the baseline is a deliberate, documented human action (not
   automated).
6. `AGENTS.md` at the repo root documents the workflow so future agents /
   contributors know how to interpret failures and when to rebaseline.
7. The existing `smoke-bench` binary stays in place as the cheap sanity
   check — it's useful for local iteration without the criterion wall-clock
   overhead.

That's the finish line. Everything between here and there is mechanics.
