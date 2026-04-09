# PowDB Session Handoff — 2026-04-06

> **You are a fresh Claude Code session.** The previous session's cwd was pinned to
> `/Users/macbookpro-kirby/Desktop/Coding/ZVN/BataQL` which no longer exists after
> the rename. This file exists so we don't lose state across the boundary.
>
> **Read this end to end before doing anything.** Then see "Your next task" at the bottom.

---

## What just shipped

### 1. Planner fix — fold `.field = literal` into `IndexScan`
**Before:** `User filter .id = 42` planned as `SeqScan + Filter` → 7,497,821 ns/op (133 ops/sec).
**After:**  Same query plans as `IndexScan { table: User, column: id, key: Int(42) }` → 2,484 ns/op (402K ops/sec).
That's a **~3,020x improvement** on the full BataQL→PowQL point-query path.

- `crates/query/src/planner.rs`: added `try_extract_eq_index_key` helper, rewrites simple eq in `plan_query`.
  Handles both `.col = literal` and `literal = .col`. Conjunctions (`and`, `or`) fall through to `SeqScan+Filter`.
- `crates/query/src/executor.rs`: replaced the `"index scan not yet implemented"` stub with a real branch that:
  - Uses `Table::index_lookup` when `tbl.indexes.contains_key(column)` → single B-tree probe.
  - Falls back to `scan() + equality filter on col_idx` when the column has no index (so the planner rewrite is always safe regardless of schema state).
- 4 new planner tests (eq folds, reversed eq folds, non-eq stays Filter, projection layers on top of IndexScan).
- 105 workspace tests passing.

### 2. Full rename: bata* → pow*
Avoided collision with existing `zvndev/batadata` (separate Neon clone) and Fly resources prefixed `bata-`.
- Crates: `powdb-{storage,query,server,cli,bench}`
- Rust modules: `powdb_storage::`, `execute_powql`
- Env vars: `POWDB_{DATA,PORT,PASSWORD,HOST,DB}`
- Fly app + volume: `zvndev-powdb` / `powdb_data`
- TS client: `@zvndev/powdb-client`
- Directory: `BataQL/` → `PowDB/`
- 5 design doc files renamed via `git mv` (history preserved)

### 3. Private repo created + pushed
**`zvndev/powdb`** (private) — https://github.com/zvndev/powdb
- commit `aacfa9a` — the rename
- commit `077b960` — planner IndexScan fold + smoke bench
- commit `c6b0e18` — Fly deploy, TS client, hardening, JS experiments snapshot

---

## Smoke bench — current numbers (post-rename, release mode)

```
rows:     50000

[1] direct insert (50000 rows)
    per op:  2218 ns      ops/sec: 450,853

[2] B-tree point lookup (200000 ops)
    per op:  1237 ns      ops/sec: 808,039
    JS ref:  1_020_000 ops/sec (500K rows, same B-tree order)

[3] sequential scan + filter (100 scans of 50000 rows)
    per scan: 6.229 ms    rows/sec: 8,026,795

[4] full PowQL parse + plan + execute (2000 point queries)
    per op:  2641 ns      ops/sec: 378,638
```

**Ratio check:** raw B-tree lookup [2] = 1,237 ns, full PowQL query [4] = 2,641 ns → parser+planner+executor overhead is **~1,404 ns (~2.1x)**. The strategic thesis (removing SQL translation saves 22-42x) is validated at the full-query level: we shed the entire translation tier that the JS scaffolding measured.

The smoke bench is **not a formal measurement** — no criterion, no statistical rigor, no warm-up protocol. It's a sanity check. The next task replaces it with a proper regression gate.

---

## Your next task — brainstorming #4: the formal bench spec

The user wants a formal, criterion-based benchmark suite that serves as an **internal regression gate** (their explicit choice — not a publishable marketing artifact).

### Decisions already made
- **Audience:** (a) Internal regression gate. Not marketing, not competitive comparison, not blog-post charts. The goal is "if a commit regresses any workload by >X%, CI fails."
- **Approach:** #1 — criterion + checked-in baseline JSON (user implicitly agreed via "Yep sounds good - 1-3 first then report back before #4"). The other approaches considered were (2) external TCP harness and (3) extending smoke.rs — both rejected.
- **Tracking:** baseline JSON lives in-repo, updated deliberately by humans with `cargo bench -- --save-baseline main` and a dedicated commit. Regressions block merges.
- **Crate:** extend the existing `powdb-bench` crate. Keep `smoke-bench` as a cheap sanity check; add a real `cargo bench` target alongside.

### What you need to brainstorm + design

Use the **superpowers:brainstorming** skill. Walk the user through design sections, get approval per section, then write the spec to:

```
docs/superpowers/specs/2026-04-06-bench-regression-gate-design.md
```

Then invoke **superpowers:writing-plans** to produce an implementation plan at:

```
docs/superpowers/plans/2026-04-06-bench-regression-gate.md
```

### Proposed workloads (7) — confirm or revise with user

These came out of pre-compaction brainstorming. Open for changes based on the user's priorities.

1. **Insert throughput** — `Table::insert` direct API, single-row, no WAL flush tuning. Already in smoke [1].
2. **B-tree point lookup** — `Table::index_lookup(col, key)` direct API. Already in smoke [2].
3. **Sequential scan + predicate** — `Table::scan().filter(...)`. Already in smoke [3].
4. **Full PowQL point query** — parse + plan + execute `User filter .id = N { .id, .name }`. Already in smoke [4]. This is the critical path that just got 3,020x faster.
5. **PowQL projection query** — parse + plan + execute `User filter .age > N { .name, .email }` — forces the filter+projection path, no index fold.
6. **PowQL aggregation** — parse + plan + execute `count(User filter .age > N)`.
7. **Wire protocol round-trip** — real TCP client → server → response. Measures the end-to-end latency including Tokio/protocol overhead. Uses the TS client or a Rust test client against a spawned server.

### Design questions the spec must answer

- **Regression threshold.** 5%? 10%? 15%? Criterion has built-in noise threshold (`--noise-threshold`) — should we use it, or a custom comparator?
- **Baseline update workflow.** Who runs the update? Is it a dedicated script (`scripts/update-bench-baseline.sh`) that runs the bench, saves to `benches/baseline.json`, and stages the file? Or a justfile target? Or `cargo xtask bench-update`?
- **CI integration.** Does CI run the bench on every PR? Or only on merges to main? Release-mode benches take time — budget trade-off.
- **Criterion output format.** Criterion writes to `target/criterion/`. We want a stable JSON format checked in, so we need a post-processing step that extracts the mean/median and writes the baseline file.
- **Machine variance.** Bench results depend on the host. Options: (a) pin CI to a specific GHA runner type, (b) use ratios not absolute values, (c) accept variance and tune thresholds. For internal gate, (a)+(c) is probably fine.
- **Warm-up protocol.** Criterion handles this but we should document it.
- **Dataset size.** Smoke uses 50K rows. Formal bench should probably use a mix: small (1K) for parse overhead, medium (50K) for cache-hot lookups, large (500K) for realistic page-faulting scans.

### Files you'll touch in the implementation plan

- `crates/bench/Cargo.toml` — add `criterion` dev-dep, `[[bench]]` targets
- `crates/bench/benches/point_lookup.rs` (new)
- `crates/bench/benches/powql_parse.rs` (new)
- `crates/bench/benches/scan_filter.rs` (new)
- `crates/bench/benches/wire_protocol.rs` (new) — spawns server
- `crates/bench/baseline/main.json` (new) — the checked-in baseline
- `crates/bench/src/compare.rs` or similar — reads criterion output, compares, exits non-zero on regression
- `scripts/update-bench-baseline.sh` (new)
- `.github/workflows/bench.yml` (new) — CI gate
- `AGENTS.md` — document the bench workflow for future agents

---

## Kirby context you should know

- **Works across many projects** — Bevrly, Parcelle, Compose, PowDB, turbine-orm, headless-subrise. Context-switches constantly. Keep responses tight.
- **Bias toward action** — "LETS GO - BUILD BEAUTIFUL THINGS PEOPLE LOVE" is the literal tone. Don't over-plan.
- **Check in at design decisions** — schema, API shape, UI direction. Don't silently take weird approaches.
- **Go further than you think** — Claude's consistent feedback: the first implementation isn't far enough. Push past the obvious.
- **Git identity for this dir:** `~/.gitconfig` includeIf resolves to ZVN DEV (`78920650+zvndev@users.noreply.github.com`). Do not override.
- **Fly resources:** PowDB is deployed at `zvndev-powdb` / `213.188.194.202:5433`. Don't touch without asking.

---

## How to start the new session

```bash
cd /Users/macbookpro-kirby/Desktop/Coding/ZVN/PowDB
claude
```

Then paste the prompt in `PROMPT-FOR-NEW-SESSION.md` (next file).
