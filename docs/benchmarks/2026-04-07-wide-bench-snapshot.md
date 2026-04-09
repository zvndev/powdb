# Wide-bench snapshot — 2026-04-07

Captured from `crates/compare/results.csv` (Apr 7 17:54 local, M1). This file
preserves the Mission A result set because `results.csv` is in `.gitignore`
(intentionally: it's a bench artifact, rewritten on every run).

## Methodology

- Harness: `cargo run -p powdb-compare --release`
- Fixture: 100,000 rows on each engine, identical schema
  (`id int, name str, age int, status str, email str, created_at datetime`)
- Engines: PowDB (in-process) + SQLite (in-process). Postgres and MySQL were
  skipped this run (docker not required for this capture).
- Each workload: 10+ iterations, ns/op reported as median.
- Commit: `d37d1ae` (Mission A: wide bench + read-side fast paths)

## Raw results (ns/op)

| workload                     |         PowDB |        SQLite | ratio (SQLite÷PowDB) | verdict |
| ---                          |           ---:|           ---:|                  ---:| :---:   |
| point_lookup_indexed         |         354.8 |         262.6 |               **0.74x** | **LOSS** |
| point_lookup_nonindexed      |        52,070 |        47,709 |               **0.92x** | **LOSS** |
| scan_filter_count            |        52,511 |       190,358 |               3.63x   | WIN     |
| scan_filter_project_top100   |        44,927 |        12,309 |               **0.27x** | **LOSS (3.65x slower)** |
| scan_filter_sort_limit10     |       290,850 |       923,738 |               3.18x   | WIN     |
| agg_sum                      |        51,435 |       177,745 |               3.46x   | WIN     |
| agg_avg                      |        64,579 |       222,622 |               3.45x   | WIN     |
| agg_min                      |        51,897 |       225,624 |               4.35x   | WIN     |
| agg_max                      |        51,436 |       202,512 |               3.94x   | WIN     |
| multi_col_and_filter         |       646,239 |       463,459 |               **0.72x** | **LOSS** |
| insert_single                |         6,143 |           906 |               **0.15x** | **LOSS (6.8x slower)** |
| insert_batch_1k              |         6,274 |           301 |               **0.05x** | **LOSS (20.8x slower)** |
| update_by_pk                 |        18,096 |           393 |               **0.02x** | **LOSS (46x slower)** |
| update_by_filter             |   943,179,014 |       744,319 |               **0.0008x** | **LOSS (1267x slower — 0.94s/op)** |
| delete_by_filter             |     1,388,125 |       234,944 |               **0.17x** | **LOSS (5.9x slower)** |

**Score: 5 wins, 10 losses.**

## Honest framing

The commit messages `d37d1ae` and `882d968` describe PowDB as "3-4x faster than
SQLite on all bench workloads". That claim is **only true for the 6 aggregate
and sort-heavy read workloads** (scan_filter_count, scan_filter_sort_limit10,
agg_sum/avg/min/max). PowDB loses 10 of 15 workloads vs SQLite on this fixture:

1. **Point-lookup gap.** SQLite's indexed point lookup is 1.35x faster than
   ours. Non-indexed is 1.09x faster. This is the single most surprising
   result: SQLite has a full SQL parser + prepared statement + VDBE in the hot
   path and still beats us. Root cause unknown; suspect SQLite's B-tree is
   more cache-friendly than ours.

2. **Projection regression.** `scan_filter_project_top100` is 3.65x slower
   than SQLite. We are doing more work to produce a small projection than
   SQLite does to run the full query. Suspect: result-materialisation path
   allocates per-row when it could stream.

3. **multi_col_and_filter.** 1.39x slower. The executor handles multiple AND
   predicates in the filter chain without vectorising the check.

4. **Write-side collapse.** Every write workload is dramatically slower:
   - `insert_single`: 6.8x slower
   - `insert_batch_1k`: 20.8x slower
   - `update_by_pk`: 46x slower
   - `update_by_filter`: **1267x slower** (0.94 seconds per op on 100K rows)
   - `delete_by_filter`: 5.9x slower

   The update_by_filter number is particularly damning: nearly one full second
   to update rows matching a predicate. This is the "pre-FASTPATH generic path
   is O(N) per update" problem noted in `crates/bench/baseline/thesis-ratios.json`
   — the current impl does a full scan then re-matches RIDs by value equality
   per update. 100K rows × 100K rows = 10B comparisons.

5. **Write path has no WAL and no buffer pool flush ordering.** The write
   numbers above are *without* durability. Once we wire the WAL (Mission B),
   insert hot path grows by at least one fsync per commit — probably making
   the gap worse unless we batch. SQLite uses WAL mode + mmap + group commit
   by default, which is what we need to match.

## What holds up from the thesis

The thesis was: "removing SQL translation saves 22-42x on the raw-bytes walker
hot path". Measured against SQLite:

- **Aggregates (4 workloads)**: 3.45x - 4.35x faster. Consistent with a lean
  raw-bytes walker beating SQLite's VDBE inner loop. **Thesis holds.**
- **scan_filter_count**: 3.63x faster. Compiled predicate over zero-copy scan
  is winning. **Thesis holds.**
- **scan_filter_sort_limit10**: 3.18x faster. Top-N heap on raw bytes beats
  SQLite's sort. **Thesis holds on sort.**
- **Point lookups**: Thesis predicts we'd crush SQLite here. We don't. **Thesis
  does not hold** — investigation needed (see Mission D).
- **Projection**: Thesis predicts minimal overhead. 3.65x slower. **Thesis
  does not hold**.
- **Writes**: Thesis never claimed write-side wins. SQLite WAL mode is
  extremely tuned. But 20-1267x gaps are structural problems, not
  measurement artifacts.

## Next-step missions

- **Mission B** (docs/superpowers/specs/2026-04-08-mission-b-durability-and-concurrency.md):
  WAL wiring + concurrency + index persistence. Correctness first.
- **Mission C** (docs/superpowers/specs/2026-04-08-mission-c-write-path-perf.md):
  Attack the write-side gap. update_by_filter O(N²) → O(1) indexed. Batch
  commit with group fsync. Insert hot path allocation audit.
- **Mission D** (docs/superpowers/specs/2026-04-08-mission-d-read-path-perf.md):
  Close the remaining 4 read losses. Point-lookup profiling. Projection
  streaming.
- **Mission E** (docs/superpowers/specs/2026-04-08-mission-e-language-features.md):
  Language gaps (joins, group by, EXPLAIN, prepared statements) planned with
  a perf-first lens so they don't add planner overhead to the already-tight
  hot paths.

## Reproducibility

```bash
# Ensure docker compose is NOT running (or start it for a full 4-engine run)
docker compose ps     # expect: no postgres or mysql

cargo run -p powdb-compare --release
# rewrites crates/compare/results.csv
```

Note that re-running will produce slightly different absolute numbers
(±10-15% on M1 idle) but the verdicts above are stable: the wins are
consistently wins and the losses are consistently losses.
