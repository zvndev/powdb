# PowDB bench regression gate — design

**Date:** 2026-04-07
**Status:** Approved for implementation planning
**Scope:** Internal regression gate only. Not a marketing artifact, not a competitive benchmark, not public.

---

## 1. Purpose

Catch silent performance regressions in the load-bearing PowDB query paths during future layer 6–9 work (CLI polish, migrations, replication, multi-tenancy). Block PR merges when the regression is real. Keep the feedback loop tight enough to fail fast without burning CI time.

The gate exists because the thesis — *"removing SQL translation saves 22–42x on the point-query path"* — currently lives in a single commit (`077b960`, the planner IndexScan fold). Any future change that silently un-folds the eq-to-IndexScan rewrite, slows the B-tree, or inflates parser/planner/executor overhead would quietly destroy the validation of the thesis. Functional tests would still pass. The numbers would rot.

The gate's job: *"if a commit makes the full PowQL point-query path slower beyond noise, CI fails and someone decides — on purpose — whether the regression is worth it."*

---

## 2. What's in scope and what's not

**In scope:**
- A criterion-based benchmark suite in `powdb-bench` covering 7 workloads.
- A hand-rolled baseline JSON file (not criterion's native baseline format) checked in under `crates/bench/baseline/`.
- A second baseline file for thesis ratios (hand-edited only).
- A comparator binary that reads the last criterion run and both baselines, exits non-zero on regression.
- A GitHub Actions workflow that runs the suite on PRs to `main` and sets a required status check.
- Documentation of the workflow in `AGENTS.md`.

**Out of scope:**
- Wire-protocol benchmarking (Tokio TCP round-trip). Belongs in a separate integration test, later PR — thesis lives at layer [4], not at the wire.
- Cold-cache / large-dataset characterization (500K+ rows). Different measurement category, different purpose.
- Cross-machine comparison or historical trend charts. Internal gate, not observatory.
- Marketing charts, blog posts, or competitive comparisons.

---

## 3. Workloads

Seven criterion benchmarks. Single 50K-row fixture shared across all query benches so numbers compare directly against `smoke-bench`. Insert uses 10K rows — throughput doesn't need more.

| # | Name | API | Dataset | Purpose |
|---|---|---|---|---|
| 1 | `insert_10k` | `Table::insert` loop | 10K rows, fresh `User` table | Guards WAL + page write + heap append path |
| 2 | `btree_lookup` | `Table::index_lookup("id", key)` | 50K rows, random keys | Raw B-tree floor; the denominator of the thesis ratio |
| 3 | `seq_scan_filter` | `Table::scan().filter(age > 30).count()` | 50K rows | Scan iterator throughput, cache behavior |
| 4 | `powql_point` | `execute_powql("User filter .id = N { .name }")` | 50K rows | **The 3,020x path.** The numerator of the thesis ratio. |
| 5a | `powql_filter_only` | `execute_powql("User filter .age > N")` | 50K rows | Non-index filter, no projection |
| 5b | `powql_filter_projection` | `execute_powql("User filter .age > N { .name, .email }")` | 50K rows | Isolates projection overhead from 5a |
| 6 | `powql_aggregation` | `execute_powql("count(User filter .age > N)")` | 50K rows | Aggregate path |

Workloads 5a and 5b are intentionally split so a regression in one vs the other tells you which layer broke (filter+executor vs projection).

Schema used across query benches:
```powql
type User {
  required id: int,
  required name: str,
  required age: int,
  required email: str,
}
```

Each bench's `setup` closure creates a fresh temp data dir, builds the schema, inserts the fixture rows, builds the `id` B-tree index, and returns a ready-to-query `Engine`. The timed loop touches only the measured operation.

---

## 4. Two independent gates

A PR fails the gate if **either** guard fires.

### 4.1 Per-workload absolute gate (±7% vs baseline)

For each of the 7 workloads, criterion's median-over-samples (ns/iter) is compared to the checked-in baseline. A current value exceeding the baseline by more than **7%** is a regression.

**Why 7%:** Tighter than 10% (catches real regressions before they compound), looser than 5% (5% false-positives on cold-cache jitter on a shared-tenancy GHA runner, which wastes rebaseline cycles and erodes trust in the gate). 7% is the smallest number where a fail almost certainly means something real.

**Symmetry:** Only one-sided. Values dropping more than 7% below baseline are *improvements* and don't fail — they just mean the baseline is stale and should be refreshed.

**Criterion config:** The criterion `noise_threshold` is set to **4%** so criterion's own change-detection doesn't flag sub-gate noise. The comparator's 7% is the source of truth; criterion's internal flag is informational only.

### 4.2 Ratio guard (`thesis-ratios.json`)

Currently one ratio: `powql_point.median / btree_lookup.median ≤ 2.5`.

Computed from each run's medians. The ceiling is checked into `thesis-ratios.json` and hand-edited only.

**Why a ratio and not an absolute:** Absolute numbers drift with hardware. The thesis is about the *ratio* — how much overhead the parser/planner/executor add on top of the raw B-tree probe. If hardware changes, both values move together and the ratio is preserved. A planner regression (e.g., someone breaks the `.field = literal` IndexScan fold) shows up immediately as the numerator exploding while the denominator stays flat.

**Current observed:** ~2.14 (smoke-bench: 2,641ns full PowQL / 1,237ns raw B-tree). Ceiling is set 17% above current to leave headroom for incidental overhead but not enough to hide a real regression.

**Why only one ratio initially:** YAGNI. We can add more ratios later (e.g., `powql_aggregation / seq_scan`) if a failure mode emerges that isn't caught by this one plus the absolute gate. Starting small keeps the thesis explicit.

### 4.3 Why both guards

- **Absolute** catches storage / executor / parser slowdowns (B-tree getting slower, insert getting slower, `execute_powql` dispatch getting slower).
- **Ratio** catches planner / translation regressions — the thesis-specific failure mode where both paths move but the translation overhead grows.

Either guard alone misses half the failure classes.

---

## 5. Baseline files and update friction

Two files with *deliberately different* update friction.

### 5.1 `crates/bench/baseline/main.json` — low friction, scripted

Per-workload absolute means. Updated via script when an intentional change shifts the numbers.

Shape:
```json
{
  "schema": 1,
  "runner": "ubuntu-24.04",
  "rustc": "1.87.0",
  "updated": "2026-04-07",
  "commit": "<git sha at time of capture>",
  "workloads": {
    "insert_10k":              { "ns_per_iter": 2218, "ops_per_sec": 450853 },
    "btree_lookup":            { "ns_per_iter": 1237, "ops_per_sec": 808039 },
    "seq_scan_filter":         { "ns_per_iter": 6229000, "ops_per_sec": 161 },
    "powql_point":             { "ns_per_iter": 2641, "ops_per_sec": 378638 },
    "powql_filter_only":       { "ns_per_iter": null, "ops_per_sec": null },
    "powql_filter_projection": { "ns_per_iter": null, "ops_per_sec": null },
    "powql_aggregation":       { "ns_per_iter": null, "ops_per_sec": null }
  }
}
```

(Values for workloads that don't exist in smoke yet are left `null` — the first run of the gate will populate them during the initial baseline capture.)

**Update workflow:**
1. Make the intentional code change.
2. Run `./scripts/update-bench-baseline.sh`.
3. Script runs `cargo bench -p powdb-bench`, reads each workload's `mean.point_estimate` from `target/criterion/**/new/estimates.json`, writes the updated `main.json` with the current rustc version and git sha, and stages it.
4. Commit the baseline alongside the code change. Convention: `bench: rebaseline after <change> (<workload>: <delta>)`.
5. PR reviewer verifies the baseline diff matches the claimed change. If the claimed change is "planner: fold in-list into IndexScan" but the diff shows `insert_10k` regressed 20%, that's a red flag.

### 5.2 `crates/bench/baseline/thesis-ratios.json` — high friction, hand-edited

Ratio ceilings. Never touched by any script. Changed only by manual edit in a dedicated commit.

Shape:
```json
{
  "schema": 1,
  "updated": "2026-04-07",
  "ratios": {
    "powql_point_over_btree_lookup": {
      "numerator": "powql_point",
      "denominator": "btree_lookup",
      "ceiling": 2.5,
      "observed_at_last_edit": 2.14,
      "meaning": "Full PowQL parser+planner+executor overhead over raw B-tree probe. This is the thesis metric. Raising this ceiling means the thesis gave ground — justify in the commit message."
    }
  }
}
```

**Update workflow:**
1. Hand-edit the file.
2. Commit in isolation. Convention: `bench: relax <ratio_name> <old> → <new> (<why>)`.
3. Social contract — we're a one-person team; no CI guard needed to prevent drive-by updates. The filename, the header comment, and the commit convention are enough.

### 5.3 Why hand-rolled JSON and not criterion's `--save-baseline`

Criterion's native baseline lives in `target/criterion/` and uses nested directories keyed on criterion-version-specific layout. Checking it in ties the repo to a specific criterion version and doesn't survive version bumps cleanly. The hand-rolled format is stable across criterion versions, diff-friendly in PRs, and separates "the number we promise to hold" from "criterion's internal bookkeeping."

---

## 6. Comparator — `crates/bench/src/bin/compare.rs`

A small binary, not a library. Run after `cargo bench`.

**Flow:**
1. For each of the 7 workloads, read `target/criterion/<group>/<workload>/new/estimates.json`. Extract `median.point_estimate` (ns).
2. Read `crates/bench/baseline/main.json`. For each workload, compute `(current - baseline) / baseline`. If > 0.07, record as absolute regression.
3. Read `crates/bench/baseline/thesis-ratios.json`. For each ratio, compute `current[numerator] / current[denominator]` (both medians). If > ceiling, record as ratio regression.
4. Print a table:
   ```
   workload                   baseline     current      delta    gate
   ────────────────────────── ─────────── ─────────── ──────── ──────
   insert_10k                    2218 ns     2301 ns    +3.7%   PASS
   btree_lookup                  1237 ns     1250 ns    +1.1%   PASS
   powql_point                   2641 ns     2880 ns    +9.1%   FAIL
   ...
   ratio                      ceiling      current    status
   ────────────────────────── ─────────── ─────────── ──────
   powql_point / btree_lookup    2.5x         2.30x    PASS
   ```
5. Exit `0` if all pass, `1` if any fail.

**Local usage:**
```bash
cargo bench -p powdb-bench
cargo run -p powdb-bench --bin compare
```

**CI usage:** Same two steps. The workflow file runs them in sequence.

**What it does NOT do:**
- No historical storage. Each run reads the last criterion run against the checked-in baseline and that's it. No database, no charts, no trend analysis.
- No partial-run support initially. All 7 workloads must run. If criterion skipped one, the comparator fails loudly rather than pretending the missing workload is fine.
- No rebaseline logic. Updating the baseline is the job of `update-bench-baseline.sh`, not the comparator.

---

## 7. Update script — `scripts/update-bench-baseline.sh`

Bash script. One purpose: refresh `main.json` after an intentional change.

**Flow:**
1. `cargo bench -p powdb-bench` (full suite, release mode).
2. For each workload, `jq` out `median.point_estimate` from `target/criterion/<group>/<workload>/new/estimates.json`.
3. Compose a new `main.json` with the extracted values, current rustc version (`rustc --version`), current git commit (`git rev-parse HEAD`), and today's date.
4. Write to `crates/bench/baseline/main.json`.
5. `git add crates/bench/baseline/main.json`.
6. Print a summary diff (old value → new value per workload) so the human can sanity-check before committing.

The script does NOT commit. The human commits, because the commit message needs to explain *why* the baseline moved.

**Dependencies:** `jq` (commonly available on dev machines and GHA Ubuntu runners).

---

## 8. CI workflow — `.github/workflows/bench.yml`

**Trigger:**
- `pull_request` targeting `main`.
- `push` to `main` (so a failure on merge is recorded even if branch protection slipped).

**Runner:** `ubuntu-24.04`. Explicit version, not `ubuntu-latest`. When Ubuntu 26.04 becomes the new `-latest`, that can silently shift the numbers by ~5–15% and burn a full rebaseline cycle.

**Steps:**
1. `actions/checkout@v4`.
2. Rust toolchain setup (stable, pinned to the workspace's current MSRV once we have one — for now, latest stable).
3. `actions/cache` on `~/.cargo/registry`, `~/.cargo/git`, `target/`.
4. `cargo bench -p powdb-bench --quiet`.
5. `cargo run -p powdb-bench --bin compare`.

**Expected wall-clock:** ~2 minutes end-to-end on a warm cache (40–90s of benches + compile/restore overhead). Acceptable PR friction for a solo project.

**Branch protection:** `main` must be configured to require the `bench` job as a status check. **This is a manual admin action** on the GitHub repo settings page — it's not a file in the repo. It's documented in `AGENTS.md` as a setup step and flagged in the implementation plan so it isn't forgotten.

**No self-hosted runner.** Adds ops burden and hides real regressions behind "my machine vs yours" debugging. The 7% gate + ratio guard are designed to tolerate GHA shared-tenancy noise.

---

## 9. Machine variance strategy

- **Runner pinning:** `ubuntu-24.04`, not `-latest`.
- **Criterion settings:** 100 samples per benchmark, 3s warm-up (criterion defaults). **Median** (`median.point_estimate` from `estimates.json`) used for comparison, not mean — median is robust to occasional outliers from GHA runner hiccups, and mean would let a single 10x-slower sample swing the gate unfairly.
- **Gates tuned for runner noise:** 7% per-workload absolute gate is wide enough to absorb ~5% typical GHA shared-tenancy jitter without eating real regressions.
- **Ratio guard is hardware-proof:** both numerator and denominator move together under hardware changes, so the ratio survives cross-machine comparison.
- **Escalation path:** if the absolute gate starts false-positiving more than ~once a month on noise alone, widen to 10% before introducing a self-hosted runner. Document the decision point and the new threshold in `AGENTS.md`. Adding a self-hosted runner is a last resort because it adds ops burden.

---

## 10. Warm-up protocol

- **Criterion default warm-up:** 3 seconds per benchmark, discarded. Handles JIT/cache settling.
- **Per-bench fixture priming:** each bench's `setup` closure (run once per benchmark, not per sample) creates a fresh temp data dir under `std::env::temp_dir()`, builds the schema, inserts the fixture rows, builds indexes. The timed loop starts with a warm catalog and a populated page cache.
- **Query benches** additionally run 10 warm-up queries before the timed loop kicks in to settle the plan cache.
- **Temp data dir cleanup:** `Drop` impl on the fixture struct removes the directory. If the bench is killed mid-run, the OS cleans up `temp_dir` on reboot; we don't track it.

---

## 11. Relationship to `smoke-bench`

`smoke-bench` stays. It has a different purpose, cost, and audience.

| | `smoke-bench` | criterion suite |
|---|---|---|
| Audience | Developer during iteration | CI gate |
| Runtime | ~3 seconds total | ~60 seconds total |
| Statistical rigor | None (single wall-clock loop) | Criterion methodology |
| When to run | Manually, after a change you care about | Automatically, on every PR |
| When it fails | Never — it just prints numbers | On regression beyond gate |
| Purpose | "Am I still in the right order of magnitude?" | "Did this commit regress the thesis?" |

Removing `smoke-bench` would cost ~10 seconds of local iteration friction per check and add nothing. Keep it.

---

## 12. Documentation deliverables

1. A new section in `AGENTS.md`: "Bench regression gate." Covers:
   - How to run the suite locally.
   - How to interpret a failure (which workload, how much, which gate).
   - How to rebaseline `main.json` (when it's appropriate, how to run the script, what to put in the commit message).
   - How to raise a ratio ceiling in `thesis-ratios.json` (when it's appropriate, the commit convention, who to tag for review if there's more than one person).
   - The manual GitHub branch-protection setup step.
   - The escalation path if the gate gets noisy.

2. A header comment block in `thesis-ratios.json` explaining the "hand-edit only, justify in commit" rule directly in the file.

3. A header comment block in `scripts/update-bench-baseline.sh` explaining that the script stages but does not commit, and the human is responsible for the commit message.

---

## 13. What "done" looks like

- `cargo bench -p powdb-bench` runs the 7-workload criterion suite in ~60 seconds.
- `crates/bench/baseline/main.json` and `crates/bench/baseline/thesis-ratios.json` checked in with current numbers from a clean run on the pinned GHA runner.
- `cargo run -p powdb-bench --bin compare` exits 0 on current `main`.
- `.github/workflows/bench.yml` runs on PRs to `main` and on pushes to `main`, and a required status check is configured in branch protection.
- `AGENTS.md` has the "Bench regression gate" section.
- `smoke-bench` still works unchanged.
- All existing workspace tests (currently 105) still pass.

---

## 14. Open questions (answered during brainstorming, recorded for the plan)

- **Threshold:** 7% per-workload absolute, 2.5x ratio ceiling. Two independent guards, both must pass.
- **Baseline format:** hand-rolled JSON, not criterion's native `--save-baseline`.
- **Baseline update friction:** low (scripted) for absolute, high (hand-edited, isolated commit) for ratios.
- **CI trigger:** every PR to `main`, and pushes to `main`. Blocks merge via required status check.
- **Runner:** pinned `ubuntu-24.04`, no self-hosted.
- **Dataset size:** single 50K-row fixture for query benches, 10K for insert throughput. No cold-cache / 500K-row category.
- **Warm-up:** criterion default (3s) plus per-bench fixture priming. Query benches run 10 warm-up queries before the timed loop.
- **Wire protocol bench:** out of scope for the gate. Separate integration test in a later PR.

---

## 15. Non-goals (explicit)

- Not a marketing artifact. No "PowDB vs Postgres" numbers.
- Not a characterization suite. One fixture size, no cold-cache runs, no pathological-workload stress tests.
- Not a benchmarking framework. The comparator is ~200 lines of Rust; it's not extensible and doesn't need to be.
- Not a historical tracking system. No database of past runs, no charts, no flamegraphs.
- Not a replacement for profiling. When the gate fires, the developer drops into `perf` / `cargo-flamegraph` locally. The gate tells you *that* something regressed, not *what* to fix.
