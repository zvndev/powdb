# Sprint Plan — PowDB Hardening + Features
Generated: 2026-04-11
Based on: Product review findings from conversation (security audit, engineering quality, competitive landscape)

## Sprint Goal
Fix all CRITICAL and HIGH security vulnerabilities, add missing query features (EXPLAIN, hash joins, window functions), harden CI, add license, and add property-based + fuzz tests.

## Success Criteria
- [x] All 3 CRITICAL protocol vulnerabilities fixed
- [x] All 3 HIGH storage/protocol vulnerabilities fixed
- [x] EXPLAIN command working
- [x] Hash joins for equi-joins working
- [x] Window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT OVER) working
- [x] Correlated subqueries working
- [x] CI runs clippy + rustfmt on every PR
- [x] MIT LICENSE file exists
- [x] Property-based tests for row encoding round-trips
- [x] Fuzz targets for PowQL parser
- [x] `cargo test --workspace` passes
- [x] `cargo build --release` succeeds

## Dev Tracks

### Track 1: Server Security Hardening
**Files touched:** `crates/server/src/protocol.rs`, `crates/server/src/main.rs`, `crates/server/src/handler.rs`, `crates/server/Cargo.toml`
**Tasks:**
- [x] TASK-01 (P0): Add MAX_PAYLOAD_SIZE (64MB) check in `read_from` before allocation (protocol.rs:153)
- [x] TASK-02 (P0): Add bounds validation in `decode` — check `6 + payload_len <= data.len()` (protocol.rs:78)
- [x] TASK-03 (P0): Add bounds checks before all slice accesses in MSG_RESULT_ROWS/MSG_RESULT_OK decode (protocol.rs:104,110,127)
- [x] TASK-04 (P1): Add MAX_COLUMNS (4096) and MAX_ROWS (10M) limits in MSG_RESULT_ROWS decode (protocol.rs:104-119)
- [x] TASK-05 (P1): Change default bind address from "0.0.0.0" to "127.0.0.1" (main.rs:88), add --bind flag
- [x] TASK-06 (P1): Add connection rate limiting via tokio::sync::Semaphore (MAX_CONNECTIONS=1024) (main.rs:94-108)
- [x] TASK-07 (P2): Use constant-time password comparison (handler.rs:63)
- [x] TASK-08 (P2): Sanitize error messages before sending to clients — only pass known-safe errors, redact internals (handler.rs:109)

### Track 2: Storage Security & Durability
**Files touched:** `crates/storage/src/wal.rs`, `crates/storage/src/heap.rs`
**Tasks:**
- [x] TASK-09 (P0): Fix integer underflow in WAL `read_all` — use `checked_sub` for `total_len - WAL_HEADER_SIZE` (wal.rs:194)
- [x] TASK-10 (P1): Cap WAL record size before allocation — add MAX_WAL_RECORD_SIZE (256MB) check before `vec![0u8; data_len]` (wal.rs:195)
- [x] TASK-11 (P1): Fix CRC validation order — validate a size limit before allocating, not just after (wal.rs:194-209)
- [x] TASK-12 (P1): Fix mmap pointer lifetime — invalidate mmap_ptr when file extends via insert (heap.rs enable_mmap/insert)
- [x] TASK-13 (P2): Add `disable_mmap` method and call it at start of insert/delete/update paths that extend the file

### Track 3: Query Engine Features
**Files touched:** `crates/query/src/token.rs`, `crates/query/src/lexer.rs`, `crates/query/src/parser.rs`, `crates/query/src/ast.rs`, `crates/query/src/plan.rs`, `crates/query/src/planner.rs`, `crates/query/src/executor.rs`, `crates/query/src/result.rs`, `crates/query/src/lib.rs`
**Tasks:**
- [x] TASK-14 (P2): EXPLAIN command — parse `explain <query>`, plan inner query, format plan tree as text result
- [x] TASK-15 (P2): Hash joins — detect equi-join predicates in planner, add HashJoin plan node, implement hash-build/probe in executor
- [x] TASK-16 (P2): Window functions — ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX OVER (PARTITION BY ... ORDER BY ...)
- [x] TASK-17 (P2): Correlated subqueries — allow outer column references in IN/EXISTS subqueries, re-execute per outer row

### Track 4: CI, License & Test Infrastructure
**Files touched:** `.github/workflows/ci.yml` (new), `LICENSE` (new), `crates/storage/Cargo.toml`, `crates/query/Cargo.toml`, new test files in `crates/storage/tests/`, `crates/query/tests/`
**Tasks:**
- [x] TASK-18 (P1): Create MIT LICENSE file in repo root
- [x] TASK-19 (P1): Add `ci.yml` workflow with cargo clippy + cargo fmt --check + cargo test
- [x] TASK-20 (P2): Add proptest round-trip tests for row encoding/decoding (storage crate)
- [x] TASK-21 (P2): Add proptest round-trip tests for PowQL parse → format → reparse (query crate)
