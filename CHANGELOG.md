# Changelog

All notable changes to PowDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-04-16

Hardening release: all known fuzz-reachable panics in the query layer are now
errors, and the CI gate has been tightened with cargo-audit and blocking fuzz
smoke runs.

### Fixed

- **Lexer**: integer literals wider than `i64::MAX` now return `LexError`
  instead of panicking (#25, closes #24)
- **Parser**: unterminated projection/assignment/argument/type-decl bodies at
  EOF (`nn{`, `z{`, etc.) now return `ParseError` instead of panicking via
  out-of-bounds indexing (#25, closes #26)
- **Executor**: `ORDER BY` on an unknown column now returns an error instead
  of panicking (#22)

### Security

- Bumped `rustls-webpki` 0.103.10 → 0.103.12 to pick up fixes for
  RUSTSEC-2026-0098 / RUSTSEC-2026-0099 (name-constraint bypass) (#25)

### CI

- New `cargo audit` job on every PR — blocks merges on known advisories (#23)
- New fuzz smoke workflow: `fuzz_lexer`, `fuzz_parser`, `fuzz_roundtrip` each
  run 60s on PRs that touch the query front-end, and nightly at 07:00 UTC.
  Blocking on failure (#23, #25)

## [0.1.1] - 2026-04-14

Post-launch polish: TS client test coverage, engine bug fixes surfaced by
end-to-end testing, and documentation sync.

### Added

- **TS client**: 53 end-to-end tests covering DDL, insert, filter, projection,
  aggregates, joins, GROUP BY/HAVING, subqueries, updates, deletes, and error
  paths (#18)
- **AGENTS.md**: user-facing primer with PowQL-vs-SQL cheat sheet, footgun
  table, and performance notes for AI assistants and new users (#20)

### Fixed

- **Parser**: `= null` and `!= null` now desugar to `IS NULL` / `IS NOT NULL`
  instead of being rejected (#19)
- **Executor**: `HAVING` on post-projection group queries now filters groups
  correctly (#19)
- **Parser**: statements with trailing tokens (e.g. `User match T on ...`,
  `User create_index .col`) now error cleanly instead of silently parsing as a
  bare-source query and dropping the rest (#19)
- **Executor**: DDL statements (`alter ... add index`, `alter ... add column`,
  `alter ... drop column`) now return an affected-count result instead of an
  empty row set (#19)

### Changed

- **Docs**: `README.md`, `docs/getting-started.md`, and `docs/POWQL.md` updated
  to use current syntax everywhere — `alter T add index .col` (not
  `create_index`), `alter T add column` (not `add_column`), `sum(T { .x })`
  (not `sum(T | .x)`), and `T1 as a inner join T2 as b on ...` (not `match`) (#20)

## [0.1.0] - 2026-04-12

Initial release of PowDB — a from-scratch database engine with PowQL query language.

### Added

- **Storage engine**: slotted 4KB pages, heap files, B+ tree indexes (disk-persisted), WAL with group commit, buffer pool with clock-sweep eviction, mmap-based scanning
- **Row format**: compact binary encoding with 1-byte type tags, variable-length strings, support for Int, Float, Str, Bool, DateTime, UUID, Bytes types
- **Query language (PowQL)**: lexer, recursive-descent parser, pure-function planner, executor with compiled predicates
  - Schema: `type T { required field: type, ... }`
  - Insert: `insert T { field := value, ... }`
  - Query pipeline: `T filter .field > val order .field desc limit N { .f1, .f2 }`
  - Aggregates: `count()`, `sum()`, `avg()`, `min()`, `max()` with GROUP BY / HAVING
  - Joins: INNER, LEFT OUTER, RIGHT OUTER (rewritten to LEFT), CROSS
  - Subqueries: IN, EXISTS, NOT IN, NOT EXISTS (correlated and uncorrelated)
  - Window functions: ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/MIN/MAX OVER
  - Set operations: UNION, UNION ALL
  - EXPLAIN for query plan inspection
  - CAST, CASE/WHEN, BETWEEN, LIKE, IS NULL/IS NOT NULL
  - Scalar functions: UPPER, LOWER, LENGTH, TRIM, SUBSTRING, CONCAT, ABS, ROUND, CEIL, FLOOR, SQRT, POW, NOW, EXTRACT, DATE_ADD, DATE_DIFF
  - UPSERT with ON CONFLICT
  - ALTER TABLE ADD/DROP COLUMN
  - Materialized views (CREATE VIEW, REFRESH VIEW, DROP VIEW)
- **Plan cache**: FNV-1a canonical hashing, literal substitution at lookup time
- **Executor fast paths**: compiled predicates for zero-decode filtering, fused scan+update, fused scan+delete, sort+limit, project+limit, aggregation fast paths
- **Plan lowering**: `RangeScan` → `Filter(SeqScan)` for unindexed columns at execution time
- **TCP server**: Tokio-based, binary wire protocol, password authentication, connection limits, graceful shutdown
- **CLI**: rustyline REPL with embedded and remote modes
- **TypeScript client**: `@zvndev/powdb-client` with full wire protocol support
- **Benchmarks**: criterion regression gate (20 workloads, per-workload thresholds), wide comparison suite vs SQLite/Postgres/MySQL
- **CI**: clippy + fmt + test workflow, criterion regression gate workflow
- **Performance**: 1.3x–10.8x faster than SQLite across all 15 comparison workloads at 100K rows

### Performance (100K rows, vs SQLite)

| Workload | Ratio |
|---|---|
| point_lookup_indexed | 3.8x faster |
| scan_filter_count | 6.7x faster |
| agg_min | 10.8x faster |
| agg_sum | 9.2x faster |
| update_by_filter | 3.2x faster |
| delete_by_filter | 1.3x faster |
