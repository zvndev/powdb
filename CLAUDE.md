# PowDB — Claude Code Guide

## Quick Start

```bash
cargo build --workspace            # build everything
cargo test --workspace             # run all tests (~30s)
cargo run --release -p powdb-compare  # benchmark vs SQLite (100K rows)
cargo bench -p powdb-bench         # criterion benchmarks (~60s)
```

## Architecture

PowDB is a from-scratch database engine with its own query language (PowQL). No SQL, no translation layer — the thesis is that removing the SQL parsing/planning overhead makes queries faster.

### Crate Dependency Graph

```
powdb-cli ──→ powdb-server ──→ powdb-query ──→ powdb-storage
                                    ↑                ↑
                              powdb-bench      powdb-compare
```

### Query Pipeline

```
PowQL text → Lexer (token stream) → Parser (AST) → Planner (PlanNode tree) → Executor (results)
```

- **Lexer** (`crates/query/src/lexer.rs`): Tokenizes PowQL input
- **Parser** (`crates/query/src/parser.rs`): Recursive descent, produces `Statement` AST
- **Planner** (`crates/query/src/planner.rs`): Pure function (no catalog access), produces `PlanNode` tree. Speculatively emits `RangeScan` for range inequalities
- **Executor** (`crates/query/src/executor.rs`): Runs plans against the storage engine. Has fast paths for common patterns (count, project+limit, sort+limit, agg, update, delete). Lowers `RangeScan` → `Filter(SeqScan)` at runtime when no index exists
- **Plan Cache** (`crates/query/src/plan_cache.rs`): FNV-1a hash, stores canonical plans, substitutes literals at lookup time

### Storage Engine

- **Slotted Pages** (`crates/storage/src/page.rs`): 4KB pages with slot directory
- **Heap Files** (`crates/storage/src/heap.rs`): Variable-length row storage, mmap-based scanning
- **B+ Tree** (`crates/storage/src/btree.rs`): Disk-persisted indexes, created per-column
- **WAL** (`crates/storage/src/wal.rs`): Write-ahead log with group commit
- **Catalog** (`crates/storage/src/catalog.rs`): Schema registry, table/index management

## Key Design Decisions

1. **Planner is pure** — no catalog access. This means `RangeScan` is emitted speculatively; the executor does plan lowering at runtime based on actual index availability
2. **Compiled predicates** — `Filter(SeqScan)` fast paths compile filter expressions into byte-level operations that skip full row decoding
3. **PowQL, not SQL** — the query language is purpose-built. Never suggest SQL compatibility layers or Postgres wire protocol
4. **Zero-copy scanning** — mmap-based heap scans with `try_for_each_row_raw` for early termination

## Test Commands

```bash
cargo test --workspace                    # all tests
cargo test -p powdb-query                 # query crate only
cargo test -p powdb-query -- executor     # executor tests only
cargo test -p powdb-storage               # storage crate only
```

## Benchmark Commands

```bash
# Wide comparison (PowDB vs SQLite, 100K rows, 15 workloads)
cargo run --release -p powdb-compare

# Criterion regression suite (20 workloads)
cargo bench -p powdb-bench

# Check against regression baselines
cargo run -p powdb-bench --bin compare

# Reset baselines after intentional changes
./scripts/update-bench-baseline.sh
```

## Common Patterns

### Adding a new PowQL keyword
1. Add token variant to `crates/query/src/token.rs`
2. Add lexer rule to `crates/query/src/lexer.rs`
3. Add parser production to `crates/query/src/parser.rs`
4. Add plan node (if needed) to `crates/query/src/plan.rs`
5. Add planner case to `crates/query/src/planner.rs`
6. Add executor case to `crates/query/src/executor.rs`

### Adding an executor fast path
Fast paths match on specific `PlanNode` shapes in `execute_plan()`. Pattern-match the plan tree and handle it before the generic recursive executor. Always verify with benchmarks.

### The executor fast paths pattern-match on `Filter(SeqScan)`
If the planner emits a different shape for the same logical operation, the fast paths won't fire. Use `lower_unindexed_range_scans` as a template for plan lowering.

## CI

Two workflow files:
- `.github/workflows/ci.yml` — clippy + fmt + test
- `.github/workflows/bench.yml` — criterion regression gate

Both are required status checks on `main`.
