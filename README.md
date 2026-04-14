# PowDB

A database engine that removes the SQL translation tier. **3--9x faster than SQLite** on every workload we measure.

Most databases spend 22--42x more work *translating your query* than actually doing it. PowDB eliminates that overhead with PowQL -- a query language designed so the parser's AST **is already a plan tree**. No rewriting, no cost-based planning, no bytecode interpreter. A point lookup parses in ~200ns, plans in ~100ns, and executes in ~1,200ns.

## Benchmark: PowDB vs SQLite (50K rows, M1)

| Workload | PowDB | SQLite | Speedup |
|---|---|---|---|
| Indexed point lookup | 90 ns | 293 ns | **3.2x** |
| Scan + filter + count | 311 us | 1,964 us | **6.3x** |
| Scan + filter + sort + limit 10 | 2.5 ms | 9.9 ms | **4.0x** |
| Aggregate SUM | 218 us | 1,884 us | **8.6x** |
| Aggregate MIN | 250 us | 2,352 us | **9.4x** |
| Multi-column AND filter | 1.8 ms | 4.7 ms | **2.5x** |
| Single-row insert | 346 ns | 901 ns | **2.6x** |
| Update by primary key | 50 ns | 416 ns | **8.3x** |
| Update by filter (10K rows) | 2.4 ms | 7.1 ms | **3.0x** |

Both engines use in-memory mode (PowDB: `WalSyncMode::Off`, SQLite: `:memory:`). Full results in `crates/compare/results.csv`.

## PowQL

PowQL reads left to right. You name the table, apply operations, and project fields -- all in one pipeline.

```
-- Define a schema
type User {
  required name: str,
  required email: str,
  age: int
}

-- Insert
insert User { name := "Alice", email := "alice@example.com", age := 30 }

-- Query pipeline: source -> filter -> order -> limit -> projection
User filter .age > 25 order .age desc limit 10 { .name, .age }

-- Aggregates
count(User filter .age > 25)
sum(User { .age })
avg(User filter .city = "NYC" { .age })

-- Joins
User as u inner join Team as t on u.team_id = t.id { u.name, team_name: t.name }

-- GROUP BY + HAVING
User group .city { .city, avg_age: avg(.age) } having avg_age > 30

-- Subqueries
User filter .id in (Order filter .total > 100 { .user_id })

-- Set operations
(User filter .age > 30) union (User filter .city = "NYC")

-- Mutations
User filter .age < 18 delete
User filter .id = 1 update { age := 31 }

-- DDL
alter User add column score: int
alter User drop column score
alter User add index .email
drop User
```

Full reference: [docs/POWQL.md](docs/POWQL.md) | Getting started: [docs/getting-started.md](docs/getting-started.md)

## Build from source

```bash
# Requires Rust stable (1.80+)
cargo build --release
```

This builds all crates: the storage engine, query engine, TCP server, CLI, and benchmarks.

## Run

### Embedded (CLI / REPL)

```bash
cargo run --release -p powdb-cli
```

Opens an interactive REPL. Data is stored in `./powdb_data/` by default.

### Server mode

```bash
cargo run --release -p powdb-server -- --port 5433 --data-dir ./powdb_data
```

Listens on TCP with a binary wire protocol. Connect via the CLI:

```bash
cargo run --release -p powdb-cli --release -- --remote localhost:5433
```

Or the TypeScript client:

```typescript
import { Client } from "@zvndev/powdb-client";

const client = await Client.connect({ host: "localhost", port: 5433 });
const result = await client.query("User filter .age > 25 { .name, .age }");
console.table(result.rows);
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `POWDB_PORT` | `5433` | TCP port for the server |
| `POWDB_DATA` | `./powdb_data` | Data directory (heap files, WAL, catalog, indexes) |
| `POWDB_PASSWORD` | *(none)* | Require this password on connect |
| `RUST_LOG` | `info` | Log level (`debug`, `trace` for per-query timings) |

## Features

**Storage engine**
- Slotted-page heap with 4KB pages
- B+tree indexes with crash-safe persistence (BIDX binary format)
- Write-ahead log with statement-boundary group commit
- Crash recovery (WAL replay + page-zero recovery + index rebuild)
- Memory-mapped reads (zero-syscall scan path)
- Compiled integer predicates (branch-free filter at the byte level)
- Thread-safe concurrent reads via pread(2)/pwrite(2)

**Query engine**
- PowQL parser + planner + executor with plan cache
- Joins (nested-loop + hash join for equi-joins)
- GROUP BY, HAVING, DISTINCT
- UNION / UNION ALL
- Subqueries (IN, EXISTS)
- Expressions in SELECT and WHERE (arithmetic, string ops, BETWEEN, LIKE, IN-list)
- COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT
- ORDER BY (multi-column), LIMIT, OFFSET
- Materialized views with automatic dirty tracking
- Prepared queries with literal substitution

**DDL**
- `type` (create table), `drop` (drop table)
- `alter <T> add column`, `alter <T> drop column` (with full heap rewrite)
- `alter <T> add index` (B+tree, persisted)

**Server**
- Tokio async TCP with `Arc<RwLock<Engine>>` for parallel readers
- Binary wire protocol (length-prefixed framing)
- Optional password auth

## Architecture

```
crates/
  storage/   Heap files, B+tree, WAL, catalog, page cache, row encoding
  query/     Lexer, parser, planner, executor (Engine), plan cache
  server/    Tokio TCP server + binary wire protocol
  cli/       Interactive REPL (embedded + remote modes)
  bench/     Criterion benchmarks + regression gate
  compare/   PowDB vs SQLite wide-bench harness
```

The engine is `powdb_query::executor::Engine`. It owns a `Catalog` (which owns `Table`s, each backed by a `HeapFile` + optional `BTree` indexes) and a `Wal`. The server wraps it in `Arc<RwLock<Engine>>` for concurrent access.

## Benchmarks

PowDB has a CI-enforced regression gate that blocks PRs to `main` if any workload regresses beyond its threshold. Run locally:

```bash
cargo bench -p powdb-bench              # criterion suite (~60s)
cargo run --release -p powdb-bench --bin compare   # regression gate
```

Run the PowDB vs SQLite comparison bench:

```bash
cargo run --release -p powdb-compare    # prints table + writes results.csv
```

## Tests

```bash
cargo test --workspace
```

444 tests across storage, query, server, bench, and compare crates.

## License

MIT License. See [LICENSE](LICENSE) for details.
