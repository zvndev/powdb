# AGENTS.md — PowDB & PowQL for AI assistants and humans

This file exists so that an agent (or a person) who has never seen PowDB can walk into a task and write correct code on the first try. If you update the language or the wire protocol, update this file in the same commit.

**Authoritative references:** [`docs/POWQL.md`](docs/POWQL.md) is the full language reference. [`docs/getting-started.md`](docs/getting-started.md) is the tutorial. This file is the 5-minute version — opinionated, with the footguns called out.

---

## What PowDB is

PowDB is an embeddable database engine written from scratch in Rust. It speaks its own query language (PowQL), not SQL. The core thesis:

> Most of what a SQL engine does is *translate your query* into something executable. We remove that tier. PowQL is designed so the parser's AST **is already a plan tree** — no rewriting, no cost-based planning, no bytecode VM.

The measurable result: 3–9× faster than SQLite on every workload we benchmark, with a 200ns parse + 100ns plan + 1200ns execute budget for a point lookup.

### When PowDB is the right choice

- **Embedded / edge / serverless workloads** where query latency is a tight budget and you don't want SQLite's quirks.
- **Single-node analytics** over tables that fit on disk. The scan path is zero-syscall (mmap) and filters are compiled to byte-level predicates.
- **You control both sides** (the DB and the app). PowDB has no Postgres wire protocol, no ODBC, no legacy compatibility. The client is a TCP binary protocol or an in-process Engine.
- **You want to read the code.** Four crates, ~20K lines, no generated parsers, no plan-language IR.

### When it's *not* the right choice

- You need a drop-in Postgres/MySQL replacement. PowQL is a different language; there is **no SQL compatibility layer**, and that is a deliberate design decision (the translation tier is the thing we're removing).
- You need multi-node replication, sharding, or Raft-style consensus. Single-node only today.
- You need fine-grained ACLs, row-level security, or multi-tenant isolation. Auth is a single shared password.
- You need user-defined functions, stored procedures, or triggers.

---

## PowQL in 60 seconds

PowQL reads **left-to-right in execution order**: source → filter → order → limit → projection.

```powql
User filter .age > 25 order .age desc limit 10 { .name, .age }
```

That's:
1. Start with the `User` table.
2. Keep rows where `age > 25`.
3. Sort descending by `age`.
4. Take the first 10.
5. Project `name` and `age`.

Compare SQL: `SELECT name, age FROM User WHERE age > 25 ORDER BY age DESC LIMIT 10`. Same result, inside-out reading order.

### The five things that trip people up

1. **Equality is `=`, not `==`.** Assignment in insert/update is `:=`.
2. **Field refs inside operators have a leading dot:** `.age`, `.city`. Naked identifiers are tables or aliases.
3. **The statement keyword for CREATE TABLE is `type`**. There is no `create table`.
4. **Projection is trailing braces**, not `SELECT`. `User { .name, .age }` is a projection, not row construction.
5. **DDL uses `alter`**, not pseudo-verbs. `alter User add column`, `alter User drop column`, `alter User add index .col`.

---

## PowQL ↔ SQL cheat sheet

| Task | PowQL | SQL |
|---|---|---|
| Define a table | `type User { required name: str, age: int }` | `CREATE TABLE User (name TEXT NOT NULL, age INT)` |
| Drop a table | `drop User` | `DROP TABLE User` |
| Add a column | `alter User add column status: str` | `ALTER TABLE User ADD COLUMN status TEXT` |
| Drop a column | `alter User drop column status` | `ALTER TABLE User DROP COLUMN status` |
| Create an index | `alter User add index .email` | `CREATE INDEX ON User (email)` |
| Insert | `insert User { name := "Alice", age := 30 }` | `INSERT INTO User (name, age) VALUES ('Alice', 30)` |
| Scan a table | `User` | `SELECT * FROM User` |
| Filter | `User filter .age > 30` | `SELECT * FROM User WHERE age > 30` |
| Project | `User { .name, .age }` | `SELECT name, age FROM User` |
| Alias a column | `User { n: .name }` | `SELECT name AS n FROM User` |
| Order | `User order .age desc` | `SELECT * FROM User ORDER BY age DESC` |
| Limit / Offset | `User limit 10 offset 20` | `SELECT * FROM User LIMIT 10 OFFSET 20` |
| Distinct | `User distinct { .city }` | `SELECT DISTINCT city FROM User` |
| Count all | `count(User)` | `SELECT COUNT(*) FROM User` |
| Filtered aggregate | `count(User filter .age > 30)` | `SELECT COUNT(*) FROM User WHERE age > 30` |
| Sum a column | `sum(User { .age })` | `SELECT SUM(age) FROM User` |
| Group + aggregate | `User group .city { .city, n: count(.name) }` | `SELECT city, COUNT(name) n FROM User GROUP BY city` |
| HAVING | `User group .city { .city, n: count(.name) } having n >= 2` | `SELECT city, COUNT(*) n FROM User GROUP BY city HAVING n >= 2` |
| Inner join | `User as u inner join Order as o on u.id = o.user_id { u.name, o.total }` | `SELECT u.name, o.total FROM User u JOIN Order o ON u.id = o.user_id` |
| Left join | `User as u left join Order as o on u.id = o.user_id` | `SELECT ... FROM User u LEFT JOIN Order o ON ...` |
| IN subquery | `User filter .id in (Order filter .total > 100 { .user_id })` | `SELECT * FROM User WHERE id IN (SELECT user_id FROM Order WHERE total > 100)` |
| EXISTS | `User filter exists (Order filter .user_id = User.id)` | `SELECT * FROM User WHERE EXISTS (SELECT 1 FROM Order o WHERE o.user_id = User.id)` |
| UNION | `(A filter ...) union (B filter ...)` | `SELECT ... UNION SELECT ...` |
| NULL check | `User filter .age = null` / `.age != null` | `WHERE age IS NULL` / `IS NOT NULL` |
| Update | `User filter .id = 1 update { age := 31 }` | `UPDATE User SET age = 31 WHERE id = 1` |
| Update with expr | `User update { age := .age + 1 }` | `UPDATE User SET age = age + 1` |
| Delete | `User filter .age < 18 delete` | `DELETE FROM User WHERE age < 18` |
| Upsert | `upsert User on .id { id := 1, name := "Alice" }` | `INSERT ... ON CONFLICT (id) DO UPDATE ...` |
| CASE | `case when .age > 30 then "old" else "young" end` | `CASE WHEN age > 30 THEN 'old' ELSE 'young' END` |
| Materialized view | `materialize OldUsers as User filter .age > 28` | `CREATE MATERIALIZED VIEW OldUsers AS ...` |

### Things that look right but do **not** parse

| Don't write | Write instead |
|---|---|
| `create table T { ... }` | `type T { ... }` |
| `insert into T { ... }` | `insert T { ... }` |
| `name: string!` | `required name: str` |
| `name = "Alice"` (in insert) | `name := "Alice"` |
| `.city == "NYC"` | `.city = "NYC"` |
| `string`, `varchar`, `text` | `str` *(unknown names silently coerce to `str` — footgun)* |
| `User match T on ...` | `User inner join T on ...` (*`match` is not a keyword*) |
| `User create_index .col` | `alter User add index .col` |
| `User add_column x: int` | `alter User add column x: int` |
| `NULL` | `null` (lowercase) |
| `AND`, `OR`, `NOT` | `and`, `or`, `not` (lowercase) |
| `User.posts` (link navigation) | not yet implemented |
| `let x := ...` | not yet implemented |

---

## Type system

Canonical type names: `str`, `int`, `float`, `bool`, `datetime`, `uuid`, `bytes`.

**Footgun:** the executor's type resolver falls back to `TypeId::Str` for any unknown name (`crates/query/src/executor.rs`), so `string`, `varchar`, or a typo silently produces a Str column with no error. Always use the canonical names above.

`required` is a prefix keyword on the field, not a `!` suffix: `required name: str`, never `name: str!`.

---

## Why PowDB is fast (the short version)

These are the design moves that buy the speedup. Understanding them keeps you from accidentally undoing them:

1. **Planner is a pure function.** It does not touch the catalog — it emits `RangeScan` speculatively, and the executor lowers to `Filter(SeqScan)` at runtime if no index exists. This keeps the parser → plan pipeline allocation-free for cache hits.
2. **Plan cache hashes canonical PowQL.** Literals are substituted at lookup time (FNV-1a hash, `crates/query/src/plan_cache.rs`). A repeated `User filter .id = <N>` reuses the same plan for all N.
3. **Compiled integer predicates.** `Filter(SeqScan)` on simple numeric predicates compiles into a branch-free byte-level check that skips full row decoding. See `execute_plan` fast paths in `crates/query/src/executor.rs`.
4. **mmap-based scans.** The storage layer exposes `try_for_each_row_raw` over memory-mapped heap files. Early termination is a `return ControlFlow::Break`.
5. **Slotted 4KB pages + persistent B+tree indexes.** Standard, but the index format (BIDX, binary) is crash-safe and survives restart with no rebuild.
6. **WAL with group commit at statement boundaries.** Writes are durable by default; throughput is maintained by batching.

If you're changing a hot path, run the regression gate locally: `cargo bench -p powdb-bench && cargo run -p powdb-bench --bin compare`.

---

## Talking to PowDB

### Embedded (in-process)

```rust
use powdb_query::executor::Engine;

let mut engine = Engine::new("./powdb_data")?;
engine.execute_powql("type User { required name: str, age: int }")?;
engine.execute_powql(r#"insert User { name := "Alice", age := 30 }"#)?;
let result = engine.execute_powql("User filter .age > 25 { .name, .age }")?;
```

### CLI / REPL

```bash
cargo run --release -p powdb-cli                      # embedded REPL
cargo run --release -p powdb-cli -- --remote host:5433 --password <pw>
```

### TCP server

```bash
cargo run --release -p powdb-server -- --port 5433 --data-dir ./powdb_data
```

Binary length-prefixed framing. **Don't use `nc` or `telnet`** — the server will hang on its `read_exact`.

### TypeScript client

```bash
npm install @zvndev/powdb-client
```

```ts
import { Client } from "@zvndev/powdb-client";

const client = await Client.connect({ host: "localhost", port: 5433 });
const r = await client.query("User filter .age > 25 { .name, .age }");
if (r.kind === "rows") console.table(r.rows);
await client.close();
```

**No parameter binding yet.** If your input is untrusted, escape it yourself; we don't have prepared-statement placeholders over the wire.

Return shapes:
- `{ kind: "rows", columns: string[], rows: string[][] }` — SELECT-like queries
- `{ kind: "scalar", value: string }` — aggregates
- `{ kind: "ok", affected: bigint }` — mutations and DDL

---

## Writing queries that perform

- **Point lookup on an indexed column** is the fast path: `User filter .email = "alice@example.com" { .name }` — ~200ns parse, ~100ns plan, ~800ns execute with a warm cache.
- **Sort+limit without an index** uses a top-k heap in the executor, not a full sort. `User order .age desc limit 10` is O(N log K).
- **Joins** use hash join when `on` is an equi-predicate (`u.id = o.user_id`), nested loop otherwise. Put the smaller table on the **right** — the hash table is built over the right side.
- **Projections before aggregates save work.** `sum(User filter .active = true { .amount })` is cheaper than decoding the whole row.
- **`count(*)` is free** — it reads the live-row count from the heap header, no scan.

---

## What's shipped vs. what's planned

Shipped: joins (inner/left/right/cross, nested-loop + hash), GROUP BY + HAVING, DISTINCT, UNION / UNION ALL, subqueries (IN, EXISTS, correlated), CASE, LIKE, BETWEEN, IN-list, window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX over partition), arithmetic, string/math/datetime scalars, CAST, COALESCE, materialized views with auto-refresh, upsert, prepared queries with literal substitution, password auth, WAL + crash recovery, persistent indexes.

Planned (design doc only — don't use): link navigation (`User.posts`), `let` bindings, default operator (`??`), UDFs, TLS, per-row permissions, replication.

---

## For contributors

Build: `cargo build --workspace`. Test: `cargo test --workspace`. Lint: `cargo clippy --workspace --all-targets -- -D warnings`. Format: `cargo fmt --all`.

CI gates on `main`:
- `clippy + fmt + test` — `.github/workflows/ci.yml`
- `criterion + regression gate` — `.github/workflows/bench.yml` (blocks merges if any of 7 load-bearing workloads regress >7% against the checked-in baseline)

Internal docs:
- `CLAUDE.md` — codebase guide for Claude Code (architecture, crate graph, common patterns)
- `CONTRIBUTING.md` — contribution workflow
- `SECURITY.md` — vulnerability reporting + threat model
- `docs/design/` — long-form language / engine design docs
- `docs/superpowers/specs/` — implementation specs for shipped features

When in doubt about what the parser accepts, **run it** against `cargo run --release -p powdb-cli`. This file is the 5-minute version; `docs/POWQL.md` is the reference; the parser is the truth.
