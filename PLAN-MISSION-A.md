# PLAN — Mission A: Wide PowQL Bench + Fast-Path Completeness

> **Status:** PM-authored. Not yet reviewed. Not yet executing.
> **Branch target:** `perf/mission-a-wide-bench` (off current `perf/end-to-end-fast-path`).
> **Thesis:** PowDB is faster than SQLite/Postgres/MySQL on every PowQL-expressible workload because it removes the SQL translation tier. Mission A proves that for 15 workloads and installs a regression gate so the property can't silently erode.

---

## Context and scope

PR #1 (squash commit `882d968`) landed an end-to-end fast path covering exactly four plan shapes:

1. `Filter(SeqScan)` with `int-col op int-literal` predicate
2. `Aggregate(Count, Filter(SeqScan))` with the same compiled predicate
3. `Aggregate(Count, SeqScan)` (no filter, raw row count)
4. `Project(IndexScan)` with a fixed-width column projection

Everything else currently falls through to the generic node-tree interpreter. That interpreter materialises `Vec<Vec<Value>>` between nodes, which is where the remaining perf risk lives. Mission A:

1. Expands the `powdb-compare` crate from 3 workloads to 15 workloads spanning reads and writes.
2. Adds a MySQL engine alongside SQLite and Postgres so the comparison covers all three mainstream SQL engines.
3. Adds criterion benches for every new workload so the regression gate protects them.
4. Expands the executor fast paths so every Mission A workload hits a compiled/zero-alloc code path instead of the generic interpreter.

**Non-goals** (explicitly out of scope, tracked in §6): links, group by, distinct, joins (even `match`), let bindings, in-list, between, like, subqueries, upsert, transactions in PowQL text, migrations, CLI polish, multi-tenancy, replication.

---

## PowQL parser verification notes (read this before §1)

I verified each workload string against `crates/query/src/parser.rs`, `lexer.rs`, and `plan.rs`. Findings:

- `and`, `or`, `=`, `!=`, `<`, `<=`, `>`, `>=` all parse and nest through `parse_and_expr` / `parse_or_expr` / `parse_comparison`, so `filter .age > 30 and .status = "active"` is fine.
- `order .field [asc|desc] limit N` parses.
- `insert User { field := value, ... }` parses one row at a time. **There is no batch-insert syntax.** `insert_batch_1k` must loop.
- `User filter ... update { field := value }` and `User filter ... delete` both parse.
- **`count(User filter ...)` parses.** That's the only aggregate that currently round-trips through parser + executor cleanly, because the parser hard-codes `field: None` on the `AggregateExpr` and the executor's generic branch rejects `None` for `sum/avg/min/max`.
- **`sum(.age of User ...)` / `avg(User | .age)` do NOT parse.** There is zero syntax today for attaching a column to a non-count aggregate. This is a hard blocker for 4 of the 10 planned read workloads (`agg_sum`, `agg_avg`, `agg_min`, `agg_max`).

### How we resolve the aggregate-column gap

FASTPATH agent will make a small, targeted parser + planner + executor change: accept an optional trailing `{ .field }` single-column projection inside an aggregate call so that

```
sum(User filter .age > 0 { .age })
avg(User filter .age > 0 { .age })
min(User filter .age > 0 { .age })
max(User filter .age > 0 { .age })
```

all parse. The parser already handles `{ .field }` projection tails via `parse_query_tail`, so the change is: after `parse_query_tail`, if `query.projection` is `Some(single_field)` and the aggregate is non-count, lift that field into `AggregateExpr.field` and clear `query.projection` before returning. Executor side, add a fast path for `Aggregate(Sum/Avg/Min/Max, Project(SeqScan))` and `Aggregate(..., Project(Filter(SeqScan)))` that walks raw row bytes and pulls a single i64 column directly.

This is a strict subset of the design-doc `sum(User filter X | .age)` pipe syntax and is 100% on-thesis — no rewriting, no cost-based planning, one new plan shape.

**If FASTPATH agent declines to land the parser piece**, BENCH agent falls back to calling `engine.execute_plan(&hand_built_plan)` directly for these 4 workloads. That still measures planner + executor, but skips parsing — a minor thesis dilution we accept as a fallback.

---

## §1. Workload spec

Every workload uses the `User` table with 100_000 rows unless noted. Schema across engines:

```text
User(id INT PRIMARY KEY, name STR, age INT, status STR, email STR, created_at INT)
```

Rows are generated deterministically:

```text
id         = i
name       = "user_{i}"
age        = 18 + (i % 60)
status     = ["active", "inactive", "pending"][i % 3]
email      = "user_{i}@example.com"
created_at = 1_700_000_000 + i
```

`status` is added over the PR #1 schema so we can hit a string-equality predicate inside an `and` without inflating the row count math. `created_at` is added so sort/limit has a monotonically meaningful ordering column that isn't the primary key.

### Reads (10)

#### 1. `point_lookup_indexed`
- **description:** Single-row lookup by indexed primary key. This is the thesis workload.
- **powql:** `User filter .id = 42 { .name }`
- **sqlite_sql:** `SELECT name FROM user_table WHERE id = ?`
- **postgres_sql:** `SELECT name FROM bench_users WHERE id = $1`
- **mysql_sql:** `SELECT name FROM bench_users WHERE id = ?`
- **bench_method:** `point_lookup_indexed(id) -> Option<String>`
- **expected_fast_path:** `Project(IndexScan)` (shape 4, already covered)
- **expected_powdb_win:** yes — this is the 2.055x-ratio path validated in `main.json`. No SQL engine avoids parse+plan+optimise.
- **data_size:** 100_000

#### 2. `point_lookup_nonindexed`
- **description:** Single-row lookup on a non-indexed int column (forces seq scan + early return).
- **powql:** `User filter .created_at = 1700042042 { .name }`
- **sqlite_sql:** `SELECT name FROM user_table WHERE created_at = ?`
- **postgres_sql:** `SELECT name FROM bench_users WHERE created_at = $1`
- **mysql_sql:** `SELECT name FROM bench_users WHERE created_at = ?`
- **bench_method:** `point_lookup_nonindexed(created_at) -> Option<String>`
- **expected_fast_path:** currently folds to `IndexScan` at plan time, then falls into the executor's "index not present" fallback which does a full scan through `tbl.scan()`. That fallback materialises `Vec<Value>` — **GENERIC.**
- **expected_powdb_win:** needs fast-path expansion — FASTPATH agent makes the `IndexScan`-fallback branch use the compiled-predicate + `for_each_row_raw` path so no-index scans are as fast as filter scans. After that, yes, because we still skip SQL parse/plan on every iteration.
- **data_size:** 100_000

#### 3. `scan_filter_count`
- **description:** Count rows matching an int-range predicate.
- **powql:** `count(User filter .age > 30)`
- **sqlite_sql:** `SELECT COUNT(*) FROM user_table WHERE age > ?`
- **postgres_sql:** `SELECT COUNT(*) FROM bench_users WHERE age > $1`
- **mysql_sql:** `SELECT COUNT(*) FROM bench_users WHERE age > ?`
- **bench_method:** `scan_filter_count(age) -> usize` (already exists, keep as-is)
- **expected_fast_path:** `Aggregate(Count, Filter(SeqScan))` (shape 2, already covered)
- **expected_powdb_win:** yes — validated 3–4x in PR #1.
- **data_size:** 100_000

#### 4. `scan_filter_project_top100`
- **description:** Filter + project with a LIMIT cap. No ordering — just take the first 100 matches.
- **powql:** `User filter .age > 30 limit 100 { .name, .email }`
- **sqlite_sql:** `SELECT name, email FROM user_table WHERE age > ? LIMIT 100`
- **postgres_sql:** `SELECT name, email FROM bench_users WHERE age > $1 LIMIT 100`
- **mysql_sql:** `SELECT name, email FROM bench_users WHERE age > ? LIMIT 100`
- **bench_method:** `scan_filter_project_top100(age) -> Vec<(String, String)>`
- **expected_fast_path:** `Project(Limit(Filter(SeqScan)))` — **GENERIC.** Currently hits the materialising branches in `executor.rs`.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH adds a fused `Project(Limit(Filter(SeqScan)))` shape that streams up to N matching rows, decoding only the projected columns, then stops the scan early.
- **data_size:** 100_000

#### 5. `scan_filter_sort_limit10`
- **description:** Top-10 by `created_at desc` among filtered rows.
- **powql:** `User filter .age > 30 order .created_at desc limit 10 { .name, .created_at }`
- **sqlite_sql:** `SELECT name, created_at FROM user_table WHERE age > ? ORDER BY created_at DESC LIMIT 10`
- **postgres_sql:** `SELECT name, created_at FROM bench_users WHERE age > $1 ORDER BY created_at DESC LIMIT 10`
- **mysql_sql:** `SELECT name, created_at FROM bench_users WHERE age > ? ORDER BY created_at DESC LIMIT 10`
- **bench_method:** `scan_filter_sort_limit10(age) -> Vec<(String, i64)>`
- **expected_fast_path:** `Project(Limit(Sort(Filter(SeqScan))))` — **GENERIC.** Sort materialises the full filtered result.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH adds a top-N heap path: scan once, maintain a bounded heap of size 10 keyed on `created_at`, emit the heap sorted. SQLite will either full-sort or use an index; we have no index on `created_at` so the comparison is apples-to-apples.
- **data_size:** 100_000

#### 6. `agg_sum`
- **description:** Sum of all `age` values (no filter, sums the whole table).
- **powql:** `sum(User { .age })` (relies on FASTPATH agent's aggregate-column parser extension)
- **sqlite_sql:** `SELECT SUM(age) FROM user_table`
- **postgres_sql:** `SELECT SUM(age) FROM bench_users`
- **mysql_sql:** `SELECT SUM(age) FROM bench_users`
- **bench_method:** `agg_sum() -> i64`
- **expected_fast_path:** `Aggregate(Sum, Project(SeqScan))` — **GENERIC** today. Needs parser extension + new fast path.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH adds a compiled single-column-sum walker that pulls a single i64 field from each row's fixed-offset slot and accumulates. One branch-free inner loop, no allocation.
- **data_size:** 100_000

#### 7. `agg_avg`
- **description:** Average age across rows matching a predicate.
- **powql:** `avg(User filter .age > 30 { .age })`
- **sqlite_sql:** `SELECT AVG(age) FROM user_table WHERE age > ?`
- **postgres_sql:** `SELECT AVG(age) FROM bench_users WHERE age > $1`
- **mysql_sql:** `SELECT AVG(age) FROM bench_users WHERE age > ?`
- **bench_method:** `agg_avg(age) -> f64`
- **expected_fast_path:** `Aggregate(Avg, Project(Filter(SeqScan)))` — **GENERIC.** Needs the same parser extension + new fast path, keyed off the existing `try_compile_int_predicate` + a sum+count accumulator.
- **expected_powdb_win:** needs fast-path expansion (FASTPATH).
- **data_size:** 100_000

#### 8. `agg_min`
- **description:** Minimum `created_at` across all rows (seeds a time-range query).
- **powql:** `min(User { .created_at })`
- **sqlite_sql:** `SELECT MIN(created_at) FROM user_table`
- **postgres_sql:** `SELECT MIN(created_at) FROM bench_users`
- **mysql_sql:** `SELECT MIN(created_at) FROM bench_users`
- **bench_method:** `agg_min() -> i64`
- **expected_fast_path:** `Aggregate(Min, Project(SeqScan))` — **GENERIC.**
- **expected_powdb_win:** needs fast-path expansion — same single-column walker, keep running `min`.
- **data_size:** 100_000

#### 9. `agg_max`
- **description:** Maximum `age`.
- **powql:** `max(User { .age })`
- **sqlite_sql:** `SELECT MAX(age) FROM user_table`
- **postgres_sql:** `SELECT MAX(age) FROM bench_users`
- **mysql_sql:** `SELECT MAX(age) FROM bench_users`
- **bench_method:** `agg_max() -> i64`
- **expected_fast_path:** `Aggregate(Max, Project(SeqScan))` — **GENERIC.**
- **expected_powdb_win:** needs fast-path expansion — same walker with running `max`.
- **data_size:** 100_000

#### 10. `multi_col_and_filter`
- **description:** Two-predicate conjunction, one int range + one string equality.
- **powql:** `User filter .age > 30 and .status = "active" { .name, .age }`
- **sqlite_sql:** `SELECT name, age FROM user_table WHERE age > ? AND status = ?`
- **postgres_sql:** `SELECT name, age FROM bench_users WHERE age > $1 AND status = $2`
- **mysql_sql:** `SELECT name, age FROM bench_users WHERE age > ? AND status = ?`
- **bench_method:** `multi_col_and_filter(age, status) -> Vec<(String, i64)>`
- **expected_fast_path:** `Project(Filter(SeqScan))` where the predicate is a `BinaryOp(And, ...)` — the current `try_compile_int_predicate` only handles a single `field op literal` pattern and returns `None` for `And`, so it falls into `decode_selective` + `eval_predicate`. Partial fast-path. Classify as **GENERIC** for this plan.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH extends `try_compile_int_predicate` to handle `And(int_pred, str_eq_pred)` by compiling each side to a raw-bytes check and ANDing them.
- **data_size:** 100_000

### Writes (5)

#### 11. `insert_single`
- **description:** Insert a single row by PowQL text (the hottest OLTP path for any SQL engine — parse/plan/bind + write).
- **powql:** `insert User { id := 100001, name := "new", age := 30, status := "active", email := "new@ex.com", created_at := 1700100001 }`
- **sqlite_sql:** `INSERT INTO user_table (id, name, age, status, email, created_at) VALUES (?, ?, ?, ?, ?, ?)`
- **postgres_sql:** `INSERT INTO bench_users (id, name, age, status, email, created_at) VALUES ($1, $2, $3, $4, $5, $6)`
- **mysql_sql:** `INSERT INTO bench_users (id, name, age, status, email, created_at) VALUES (?, ?, ?, ?, ?, ?)`
- **bench_method:** `insert_single(id, name, age, status, email, created_at) -> ()`
- **expected_fast_path:** `Insert` plan node — **GENERIC** but the generic path is already cheap because `Engine::execute_plan` on `Insert` decodes the assignments directly. Parse + plan dominates for SQL engines. Keep as-is; no FASTPATH work.
- **expected_powdb_win:** yes — SQL engines pay full parse+plan per call even with prepared statements (the protocol round-trip still dominates in-process). PowDB's parse+plan is ~500ns.
- **data_size:** 100_000 pre-loaded rows; bench inserts new rows into a growing table.

#### 12. `insert_batch_1k`
- **description:** Insert 1_000 rows in a tight loop from the host. SQL engines use a transaction (prepared statement); PowDB loops `execute_powql`. This is the one workload where we **expect SQLite to compete** because its prepared-statement reuse is very efficient — if PowDB still wins, the thesis holds even on the tightest SQL loop.
- **powql:** `insert User { id := $i, ... }` × 1_000 in a host-side loop; PowDB engine constructs plan text per call.
- **sqlite_sql:** `BEGIN; INSERT ... (prepared) × 1000; COMMIT`
- **postgres_sql:** `BEGIN; prepared INSERT × 1000; COMMIT` (or `COPY`, but we deliberately use INSERT to measure translation cost)
- **mysql_sql:** `START TRANSACTION; prepared INSERT × 1000; COMMIT`
- **bench_method:** `insert_batch(rows: &[(i64, &str, i64, &str, &str, i64)]) -> ()`
- **expected_fast_path:** `Insert` plan node × 1_000 — GENERIC, but cheap.
- **expected_powdb_win:** yes — the `Insert` plan node bypasses all query-pipeline overhead, and PowDB's parse+plan is faster than SQLite's even-optimised prepare-cache lookup. If this one comes out a wash we add it to §6 rather than weaken the fast path.
- **data_size:** start from empty; measure 1_000 inserts per iteration.

#### 13. `update_by_pk`
- **description:** Update a single row by primary key.
- **powql:** `User filter .id = 42 update { age := 31 }`
- **sqlite_sql:** `UPDATE user_table SET age = ? WHERE id = ?`
- **postgres_sql:** `UPDATE bench_users SET age = $1 WHERE id = $2`
- **mysql_sql:** `UPDATE bench_users SET age = ? WHERE id = ?`
- **bench_method:** `update_by_pk(id, new_age) -> u64`
- **expected_fast_path:** `Update` plan node. The planner builds `Update(Filter(SeqScan))` even for a PK equality, and the executor uses `scan()` + linear match to find the row, then calls `catalog.update`. **GENERIC and slow.** This is the worst current-day gap.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH teaches the planner to produce `Update(IndexScan)` when the filter is `.pk_col = literal`, then adds an executor fast path that does a direct index lookup + in-place row replace without the `scan().filter().collect()` round trip.
- **data_size:** 100_000

#### 14. `update_by_filter`
- **description:** Update every row matching a range predicate (rewrite `status` for a subset).
- **powql:** `User filter .age > 50 update { status := "senior" }`
- **sqlite_sql:** `UPDATE user_table SET status = 'senior' WHERE age > ?`
- **postgres_sql:** `UPDATE bench_users SET status = 'senior' WHERE age > $1`
- **mysql_sql:** `UPDATE bench_users SET status = 'senior' WHERE age > ?`
- **bench_method:** `update_by_filter(age_threshold, new_status) -> u64`
- **expected_fast_path:** `Update(Filter(SeqScan))` — **GENERIC.** Same pattern as `update_by_pk` but bulk.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH adds a fused `Update(Filter(SeqScan))` path using the compiled predicate to select row IDs, then a single pass of `catalog.update` calls. Note: the current executor does a second full `scan()` to locate matching rows by value equality after planning — this is O(N·M). Fixing that alone will likely give a >10x speedup even before compiled-predicate work.
- **data_size:** 100_000 (matches ~16_000 rows)

#### 15. `delete_by_filter`
- **description:** Delete every row matching a range predicate.
- **powql:** `User filter .age < 20 delete`
- **sqlite_sql:** `DELETE FROM user_table WHERE age < ?`
- **postgres_sql:** `DELETE FROM bench_users WHERE age < $1`
- **mysql_sql:** `DELETE FROM bench_users WHERE age < ?`
- **bench_method:** `delete_by_filter(age_threshold) -> u64`
- **expected_fast_path:** `Delete(Filter(SeqScan))` — **GENERIC.** Same O(N·M) pattern as `update_by_filter`.
- **expected_powdb_win:** needs fast-path expansion — FASTPATH adds a fused `Delete(Filter(SeqScan))` path: compiled-predicate walk collects RIDs in a `Vec<RowId>`, then a single batched delete pass. Note: benches must re-populate between iterations because `delete` is destructive — budget for fixture rebuild cost outside the timed loop.
- **data_size:** 100_000 per iteration (rebuild fixture between iterations)

### Summary

- **Read workloads:** 10 (1 already fast, 9 need FASTPATH work)
- **Write workloads:** 5 (1 already fine, 4 need FASTPATH work)
- **Total:** 15
- **On current fast path:** 3 (`point_lookup_indexed`, `scan_filter_count`, `insert_single`)
- **Need FASTPATH expansion:** 12

---

## §2. BenchEngine trait additions

New `crates/compare/src/engines/mod.rs` trait (keeps everything object-safe — all inputs are owned primitives, all outputs are owned simple types). **Rename the existing `point_lookup` → `point_lookup_indexed`** so the workload name is unambiguous; both the old `scan_filter_count` and `count_filter` stay as-is.

```rust
pub trait BenchEngine {
    fn name(&self) -> &str;

    /// Populate User table with n_rows deterministic rows. See §1 schema.
    fn setup(&mut self, n_rows: usize);

    // ── Reads ─────────────────────────────────────────────────────────

    /// 1. point_lookup_indexed
    fn point_lookup_indexed(&self, id: i64) -> Option<String>;

    /// 2. point_lookup_nonindexed
    fn point_lookup_nonindexed(&self, created_at: i64) -> Option<String>;

    /// 3. scan_filter_count  (existing, keep signature)
    fn scan_filter_count(&self, age_threshold: i64) -> usize;

    /// 4. scan_filter_project_top100
    fn scan_filter_project_top100(&self, age_threshold: i64) -> Vec<(String, String)>;

    /// 5. scan_filter_sort_limit10
    fn scan_filter_sort_limit10(&self, age_threshold: i64) -> Vec<(String, i64)>;

    /// 6. agg_sum
    fn agg_sum(&self) -> i64;

    /// 7. agg_avg
    fn agg_avg(&self, age_threshold: i64) -> f64;

    /// 8. agg_min
    fn agg_min(&self) -> i64;

    /// 9. agg_max
    fn agg_max(&self) -> i64;

    /// 10. multi_col_and_filter
    fn multi_col_and_filter(&self, age_threshold: i64, status: &str) -> Vec<(String, i64)>;

    // ── Writes ────────────────────────────────────────────────────────

    /// 11. insert_single
    fn insert_single(
        &mut self,
        id: i64,
        name: &str,
        age: i64,
        status: &str,
        email: &str,
        created_at: i64,
    );

    /// 12. insert_batch (caller passes the row set; 1_000 in the bench)
    fn insert_batch(&mut self, rows: &[(i64, String, i64, String, String, i64)]);

    /// 13. update_by_pk
    fn update_by_pk(&mut self, id: i64, new_age: i64) -> u64;

    /// 14. update_by_filter
    fn update_by_filter(&mut self, age_threshold: i64, new_status: &str) -> u64;

    /// 15. delete_by_filter
    fn delete_by_filter(&mut self, age_threshold: i64) -> u64;
}
```

Notes:

- Write methods take `&mut self` so we can keep SQLite's `Connection::transaction` usage ergonomic.
- `insert_batch` owns strings (`String`, not `&str`) in the tuple so the caller can pre-build the batch once and reuse it. This keeps format!() allocation out of the timed loop.
- `scan_filter_project_top100` returns `Vec<(String, String)>`. We pay the allocation but it's constant (100 rows), so it won't dominate.
- `scan_filter_sort_limit10` returns `Vec<(String, i64)>` — also constant (10 rows).
- All read methods that return `Vec` cap their return size, so allocation cost is bounded and comparable across engines.
- The old `count_filter` method is **removed** — it duplicated `scan_filter_count`. Any workload that used it now calls `scan_filter_count` directly.

---

## §3. File ownership map for 5 worker agents

### Agent BENCH — wide workload runner + engine plumbing

**Owns exclusively (no other agent touches these):**
- `crates/compare/src/main.rs` — expand from 3 workloads to 15; add CSV output at `crates/compare/results.csv`; add a `--workloads` CLI filter so you can run a subset; keep the printed comparison table.
- `crates/compare/src/engines/mod.rs` — replace the trait with §2.
- `crates/compare/src/engines/powdb.rs` — implement every new method on `PowdbEngine`, preserving the mmap + RowLayout-caching pattern and bypassing PowQL parsing (calling the executor's plan path) where it gives PowDB a fair representation of "cached plan" behaviour. Where FASTPATH lands direct-API shortcuts (B-tree bypass for point lookups etc.), keep calling the same helpers — do not duplicate.
- `crates/compare/src/engines/sqlite.rs` — implement every new method using prepared statements. Writes use a single transaction per batch.
- `crates/compare/src/engines/postgres.rs` — implement every new method. Writes use prepared statements in a transaction. Schema changes: add `status TEXT NOT NULL` and `created_at BIGINT NOT NULL` to `bench_users` in `try_new()`.
- `crates/compare/Cargo.toml` — add `mysql = "25"` (or the current stable `mysql_async` blocking build, whichever works with the workspace) behind an always-on dep so the build is deterministic when docker is up.
- `crates/compare/src/engines/mysql.rs` — **MAY import and wire** from `main.rs` (`match MysqlEngine::try_new() { ... }`) but **MUST NOT create or modify the file itself.** MYSQL agent owns the file.

**Must not touch:**
- `crates/query/**` — that's FASTPATH's crate.
- `crates/bench/**` — that's CRITERION's crate.
- `docker-compose.yml` / `AGENTS.md` — INFRA's.

**Deliverable:** `cargo run -p powdb-compare --release` runs all 15 workloads against PowDB + SQLite, adds Postgres if reachable, adds MySQL if reachable, prints the comparison table and ratio table, and writes `crates/compare/results.csv`.

### Agent MYSQL — MySQL engine implementation

**Owns exclusively:**
- `crates/compare/src/engines/mysql.rs` — new file. Implements `BenchEngine` from §2 against a MySQL 8 server on `localhost:3306`.

**Exact module surface that BENCH agent will import:**

```rust
// crates/compare/src/engines/mysql.rs
use super::BenchEngine;

pub struct MysqlEngine {
    // private
}

impl MysqlEngine {
    /// Connection URL resolution order:
    /// 1. POWDB_BENCH_MYSQL_URL env var
    /// 2. mysql://root:powdb@localhost:3306/powdb_bench
    /// Returns None if connect fails, the bench runner then skips the engine.
    pub fn try_new() -> Option<Self> { /* ... */ }
}

impl BenchEngine for MysqlEngine { /* all 15 methods */ }
```

- Schema: same as Postgres (`id BIGINT PRIMARY KEY, name TEXT NOT NULL, age BIGINT NOT NULL, status TEXT NOT NULL, email TEXT NOT NULL, created_at BIGINT NOT NULL`).
- Use `LOAD DATA LOCAL INFILE` or a prepared `INSERT ... VALUES (?,?,?,?,?,?)` in a transaction for `setup`. Match Postgres' batching strategy for fairness.
- `name()` returns `"mysql"` (lowercase, so output lines up with `"sqlite"` / `"powdb"`).

**Must not touch:** anything else.

**Deliverable:** `MysqlEngine::try_new()` returns `Some(_)` when `docker compose up -d` is running, `None` otherwise. All 15 trait methods pass the smoke test documented in §5.

### Agent INFRA — docker infra + doc

**Owns exclusively:**
- `docker-compose.yml` at repo root. Two services:
  - `postgres` (image `postgres:16`, env `POSTGRES_PASSWORD=powdb POSTGRES_DB=powdb_bench`, port `5432:5432`, volume `powdb_pg_data:/var/lib/postgresql/data`).
  - `mysql` (image `mysql:8`, env `MYSQL_ROOT_PASSWORD=powdb MYSQL_DATABASE=powdb_bench`, port `3306:3306`, volume `powdb_mysql_data:/var/lib/mysql`, command including `--local-infile=1` so LOAD DATA LOCAL INFILE works).
- `AGENTS.md` — add a new top-level section `## Wide bench` after the existing `## Bench regression gate` section:
  - How to start the two databases: `docker compose up -d`.
  - How to run: `cargo run -p powdb-compare --release`.
  - How to find the CSV: `crates/compare/results.csv`.
  - Expected output shape and how to interpret the ratio columns.
  - How to skip Postgres/MySQL deliberately (`POWDB_BENCH_PG_URL=skip` or `docker compose down`).
  - One sentence noting the bench is **not gated by CI**; it's the human-run wide proof, separate from the criterion gate.

**Must not touch:** any `crates/**` file.

**Deliverable:** `docker compose up -d` brings both servers up; both are reachable from the bench harness; AGENTS.md documents the flow.

### Agent CRITERION — criterion bench suite expansion

**Owns exclusively:**
- `crates/bench/benches/powql.rs` — add one `bench_<workload_name>` function per new workload (except the 4 already-covered ones that have equivalents: `powql_point` covers #1, `powql_aggregation` covers #3; keep those fn names for gate continuity and add new fns only for the 13 newly-introduced names). **Every new fn uses the exact `name` from §1.** Use the existing `setup_user_fixture` pattern; extend it to include `status` + `created_at` columns.
- `crates/bench/baseline/main.json` — add a `null` entry for every new workload name so the first gate run lands in CAPTURE mode and writes the live number. **Do not invent numbers.** Existing entries stay exactly as-is.
- `crates/bench/baseline/thesis-ratios.json` — add meaningful ratios where the structural relationship is clear:
  - `scan_filter_count_over_btree_lookup ≤ 8_000` — aggregate over 100K rows vs single probe; primarily a sanity ceiling.
  - `update_by_pk_over_powql_point ≤ 4.0` — an indexed update should be within 4x of an indexed read after FASTPATH.
  - `insert_single_over_btree_lookup ≤ 5.0` — insert hot path should stay within 5x of a B-tree probe.
  - All ratios are starting values; comment each one with the "observed_at_last_edit" pending first CI capture.

**Must not touch:** `crates/query/**`, `crates/compare/**`, `docker-compose.yml`, `AGENTS.md`.

**Deliverable:** `cargo bench -p powdb-bench` runs all new workloads, the comparator script exits 0 on first run by entering CAPTURE mode for null baselines.

### Agent FASTPATH — executor fast-path expansion

**Owns exclusively:**
- `crates/query/src/executor.rs` — add new fast-path branches listed below.
- `crates/query/src/planner.rs` — update the planner to emit the new plan shapes that executor's fast paths match on (the `Update(IndexScan)` fold in particular).
- `crates/query/src/plan.rs` — only if a new plan-node variant is required. Prefer reusing existing variants.
- `crates/query/src/parser.rs` — add the aggregate-column parser extension described in the "PowQL parser verification notes" section above. This is the minimum parser change the plan authorises.
- `crates/query/src/ast.rs` — only if a new AST node is required for the parser extension. Prefer reusing `AggregateExpr.field` + `ProjectionField` lifting.
- Unit tests in the same files (`#[cfg(test)]` modules).

**Fast paths to add (one per `GENERIC` classification in §1):**

1. `point_lookup_nonindexed` — make `PlanNode::IndexScan`'s "no index present" fallback use `try_compile_int_predicate` + `for_each_row_raw` instead of `tbl.scan()`. Short-circuit on first match.
2. `scan_filter_project_top100` — add a fused `Project(Limit(Filter(SeqScan)))` fast path in `executor.rs` that streams rows through the compiled predicate, decodes only the projected columns, and stops after `limit` matches.
3. `scan_filter_sort_limit10` — add a fused `Project(Limit(Sort(Filter(SeqScan))))` fast path using a top-N heap (use `std::collections::BinaryHeap` bounded at `limit`).
4. `agg_sum` / `agg_avg` / `agg_min` / `agg_max` — parser extension (see above) + new fused `Aggregate(*, Project(SeqScan|Filter(SeqScan)))` fast path that walks one fixed-size i64 column per row, no allocation.
5. `multi_col_and_filter` — extend `try_compile_int_predicate` to accept `BinaryOp(And, int_pred, str_eq_pred)` and ANDing compiled leaves. String eq uses `data[offset..offset+len]` direct byte compare, with `len` from the fixed-offset table.
6. `update_by_pk` — planner fold: when `Update.input` is `Filter(SeqScan)` with a simple `.col = literal` predicate, emit `Update(IndexScan)` directly (parallel to the existing read-side fold). Executor side, add `Update(IndexScan)` fast path: single B-tree lookup → `catalog.update` → done.
7. `update_by_filter` — fused `Update(Filter(SeqScan))` fast path: compiled predicate identifies matching RIDs in one pass, then one `catalog.update` per RID. Crucially, **replace the current O(N·M) value-equality match** with direct RID tracking.
8. `delete_by_filter` — fused `Delete(Filter(SeqScan))` fast path: same shape as update_by_filter but calls `catalog.delete`.

**Must not touch:** `crates/compare/**`, `crates/bench/**`, `docker-compose.yml`, `AGENTS.md`.

**Deliverable:** every workload classified `needs fast-path expansion` in §1 is ≥2x faster than SQLite in the BENCH agent's output. `cargo test --workspace` stays green. No existing criterion workload regresses beyond the 7% gate (if one does, rebaseline deliberately and document why).

---

## §4. Integration dependencies (cross-agent contracts)

These are the hard interfaces between agents. Breaking any of these breaks the mission.

### BENCH → MYSQL (file + type contract)

- **Module path:** `engines::mysql::MysqlEngine`
- **Constructor:** `pub fn try_new() -> Option<MysqlEngine>`
- **Trait:** `impl BenchEngine for MysqlEngine` — all 15 methods from §2.
- **Engine name:** `"mysql"` (lowercase).
- **When BENCH imports it:** `crates/compare/src/engines/mod.rs` declares `pub mod mysql;`, `crates/compare/src/main.rs` calls `engines::mysql::MysqlEngine::try_new()` and pushes into results if `Some`.

### BENCH → CRITERION (workload name contract)

Both sides must spell the workload identifiers **exactly** the same way so the baseline JSON can correlate. The canonical names (lowercase, snake_case, no prefix):

```
point_lookup_indexed
point_lookup_nonindexed
scan_filter_count
scan_filter_project_top100
scan_filter_sort_limit10
agg_sum
agg_avg
agg_min
agg_max
multi_col_and_filter
insert_single
insert_batch_1k
update_by_pk
update_by_filter
delete_by_filter
```

- BENCH uses these as column headers and CSV row keys.
- CRITERION uses these as criterion bench fn names via `c.bench_function("name", ...)`. **Exception:** to keep gate continuity, workloads 1 and 3 in §1 map to the existing `powql_point` and `powql_aggregation` bench fn names. The `main.json` baseline already has those entries; do not rename them. BENCH's CSV, however, still uses the §1 names (`point_lookup_indexed`, `scan_filter_count`) — CRITERION just keeps its own legacy internal names for the gate.

### CRITERION → FASTPATH (which workloads are expected to speed up)

CRITERION's baseline JSON will have `null` values for the 12 new workloads at first run. FASTPATH agent's work determines whether those null captures become "PowDB wins" or "PowDB is embarrassed by SQLite." The agents run in parallel, so CRITERION cannot wait — instead, CRITERION sets reasonable first-run expectations by adding these ratio guards (see §3 CRITERION section) so that once FASTPATH lands, a subsequent merge doesn't silently un-land the improvement.

If FASTPATH lands first and CRITERION lands second, the null captures reflect the fast-path numbers — good.
If CRITERION lands first and FASTPATH lands second, the null captures reflect generic-interpreter numbers — also fine because the ratio guards will *tighten* after FASTPATH, and the rebaseline commit will explicitly state "bench: rebaseline after Mission A fast paths".

### FASTPATH → BENCH (direct-API access for compiled plans)

BENCH's `PowdbEngine` is allowed to bypass `execute_powql()` and call the executor's `execute_plan()` directly with a hand-built `PlanNode` when doing so represents the realistic "compiled-plan" access pattern a client SDK would use. For Mission A, the specific cases are:

- `point_lookup_indexed` — already bypasses via direct `tbl.indexes.get("id").lookup()` in the existing impl. Keep this.
- `point_lookup_nonindexed` — do NOT bypass; this one must measure `execute_powql` so we can compare end-to-end.
- All other workloads — go through `execute_powql` unless FASTPATH explicitly documents a compiled-plan helper on `Engine` (e.g., a `prepared(powql) -> PreparedQuery` handle).

FASTPATH does **not** owe BENCH any new `Engine` API beyond what's needed for its own fast-path work. If FASTPATH adds a `Engine::prepare(&str) -> PlanNode` helper as a natural byproduct, BENCH may use it; otherwise BENCH calls `execute_powql` every iteration and lets the plan cache do its job.

---

## §5. Success criteria

The mission is done when all of the following hold on `perf/mission-a-wide-bench` merged into `main`:

1. `cargo build --release` is clean across the workspace.
2. `cargo test --workspace` is green. No test is skipped, ignored, or made conditional.
3. `docker compose up -d` starts Postgres 16 + MySQL 8 locally.
4. `cargo run -p powdb-compare --release` runs all 15 workloads against PowDB + SQLite unconditionally, and against Postgres + MySQL if reachable. It:
   - Prints a comparison table in the existing format (engine × workload).
   - Prints a ratio table (engine/powdb) for every non-PowDB engine.
   - Writes `crates/compare/results.csv` with one row per (engine, workload) pair and columns `engine,workload,ns_per_op,ops_per_sec,matches_expected`.
   - Exits 0.
   - When Postgres or MySQL is unreachable, prints a `[skipped]` line and continues.
5. `cargo bench -p powdb-bench` runs the expanded criterion suite (including all new per-workload fns). The comparator script exits 0 — null baselines get captured, existing non-null baselines stay within ±7%, all thesis ratios stay under ceiling.
6. For **every** workload in §1, PowDB is at least **2x faster** than SQLite in the `compare-engines` output. If a workload fails to meet 2x, it is:
   - (a) moved to §6 with a specific reason, OR
   - (b) blocked on further FASTPATH work (create a dedicated follow-up task, do not merge Mission A claiming it's done).
7. The regression gate (`.github/workflows/bench.yml`) runs on the Mission A PR and enters CAPTURE mode for all null baselines, producing a diff that a reviewer can sanity-check before merging.
8. `AGENTS.md` has a `## Wide bench` section with the commands from §3 INFRA.

---

## §6. Backlog — out of scope for Mission A

Things I noticed while reading the codebase or verifying workload strings. These are **not Mission A**; track them as separate follow-ups.

### Language / parser
- **Pipe-into-aggregate** (`User filter X | avg(.age)`) from the design doc. The minimal `{ .field }` trailing form Mission A authorises covers the bench needs; the pipe form is ergonomic and belongs to a later PowQL polish pass.
- **Group by / having.** Mission B territory. Requires a `group` plan node and a hash-group executor.
- **Link traversal.** Mission C. Requires schema-level link resolution and an index-driven join executor.
- **`distinct`, `in`, `between`, `like`, `union`, subqueries.** Design-doc aspirational. None are in Mission A.
- **`??` coalesce in projections.** Parser already tokenises `??` and the AST has `Expr::Coalesce`, but I didn't verify executor coverage in projection context. Track separately.
- **Multiple-field projection with aliases in aggregates**, e.g., `sum(User { total: .age })`. Out of scope — the parser extension for §1 assumes a single unaliased `.field`.

### Schema / catalog
- **Type name footgun** documented in `AGENTS.md`: unknown type names silently fall back to `TypeId::Str`. Mission A deliberately doesn't fix this because it's orthogonal.
- **No batch-insert syntax in PowQL.** Workload #12 (`insert_batch_1k`) uses a host-side loop. A future mission could add `insert User [{ ... }, { ... }, ...]` or similar; track separately.
- **No composite / multi-column index.** Mission A workload #10 (`multi_col_and_filter`) doesn't use one; the compiled-predicate AND path is sufficient. But multi-column indexes are on the mission B roadmap.

### Planner
- **Conjunction splitting.** Planner's `try_extract_eq_index_key` comments explicitly call this out: "Extending this to split conjunctions is a future optimization." Mission A works around it by compiling ANDs in the predicate directly; a real split-conjunction optimiser is follow-up.
- **Cost-based plan selection.** Not today, not Mission A. The existing rewrite-then-fallback pattern is fine while the plan shapes are small.

### Executor
- **`PowdbEngine` in `compare/` bypasses parsing for `point_lookup_indexed`.** This is the "direct API" simulation. A real client SDK would do this via a cached plan handle. Mission A does not formalise the cached-plan API; that's a deliverable of the wire-protocol polish mission.
- **`RowLayout::new` on every aggregate call.** We could cache layouts per table on the executor to shave nanoseconds. Defer — the Mission A fast paths cache layouts inside their own closures already.

### Bench infrastructure
- **No MySQL warmup scheme.** MySQL's first query is always slower due to thread/buffer-pool warmup. BENCH harness does a warmup loop for all engines, which should cover it, but if MySQL's first bench iteration looks anomalous, add a larger warmup pass.
- **`compare-engines` doesn't pin CPU or set thread priority.** We rely on machine determinism. A future polish pass could lock to `SCHED_FIFO` or similar.
- **No JSON output.** CSV is enough for Mission A. JSON export can be added later if the `compare-engines` output needs to feed a dashboard.

### Observed parser bug worth flagging (separate fix)
- The parser in `parse_aggregate_query` unconditionally sets `AggregateExpr.field = None`. This means even `count(User)` passes `field: None` — fine for count (executor ignores it) but it's the reason `avg/sum/min/max` can't attach a column today. The §1 parser extension addresses this narrowly; a more complete fix would be to make the AST more expressive. Track as a "parser cleanup" follow-up.

---

_End of plan. FASTPATH, BENCH, MYSQL, INFRA, and CRITERION agents can now begin in parallel — all five have disjoint file ownership and clear contracts in §4._
