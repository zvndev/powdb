# Contributing to PowDB

## Prerequisites

- Rust stable (latest)
- Docker + Docker Compose (optional, for running wide benchmarks against Postgres/MySQL)

## Quick Start

```bash
git clone https://github.com/zvndev/powdb.git
cd powdb
cargo build --workspace
cargo test --workspace
```

## Build Commands

```bash
cargo build --workspace           # debug build
cargo build --release --workspace # release build
cargo test --workspace            # run all tests
cargo bench -p powdb-bench        # criterion benchmarks (~60s)
cargo run --release -p powdb-compare  # wide bench vs SQLite/PG/MySQL
```

## Project Structure

```
crates/storage   # slotted pages, B+ tree, WAL, buffer pool, catalog
crates/query     # lexer, parser, planner, executor (Engine)
crates/server    # Tokio TCP server + binary wire protocol
crates/cli       # rustyline REPL (embedded + remote mode)
crates/bench     # criterion benchmarks + regression gate
crates/compare   # wide benchmark comparisons vs other databases
clients/ts       # TypeScript client + demo
```

## Development Workflow

1. Create a branch from `main`
2. Make changes, run `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings`
3. Run `cargo test --workspace` — all tests must pass
4. Run `cargo run --release -p powdb-compare` to check for performance regressions
5. Open a PR against `main`

## CI Checks

PRs must pass:
- **clippy + fmt + test** — lints, formatting, and all workspace tests
- **criterion + regression gate** — benchmark must not regress beyond thresholds

## Benchmark Regression Gate

The criterion gate compares each workload's median against baselines in `crates/bench/baseline/main.json`. Thresholds vary by workload (7-20%).

If you intentionally change performance characteristics:
```bash
./scripts/update-bench-baseline.sh
git add crates/bench/baseline/main.json
git commit -m "bench: rebaseline after <change> (<workload>: <delta>)"
```

## Code Style

- Standard `rustfmt` formatting (enforced by CI)
- All clippy warnings are errors in CI
- Prefer `?` for error propagation over manual matching
- No `unwrap()` in new code — use proper error handling

## Architecture Notes

PowDB uses PowQL, a custom query language (not SQL). The query pipeline is:

```
Input → Lexer → Parser → Planner → Executor → Result
                                      ↓
                              Storage Engine (B+ tree, heap files, WAL)
```

The planner has no catalog access (it's a pure function). Plan lowering (e.g., `RangeScan` → `Filter(SeqScan)` for unindexed columns) happens at execution time in the executor.

## License

MIT
