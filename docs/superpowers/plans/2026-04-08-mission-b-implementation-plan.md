# Mission B — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-04-08-mission-b-durability-and-concurrency.md`
**Status:** ready to dispatch (2026-04-07)
**Strategy:** 3 parallel agents in git worktrees, sequential merge

This document is the *executable* sister of the Mission B spec. The spec
locks the architectural decisions; this plan turns them into a step-by-step
runbook the agents and the coordinator can execute mechanically.

## Pre-flight

Coordinator (the main session) does these BEFORE dispatching any agent:

1. **Branch naming.** All Mission B branches share the prefix `mission-b/`:
   - `mission-b/agent-1-wal-wiring`
   - `mission-b/agent-2-concurrency`
   - `mission-b/agent-3-index-persistence`
2. **Worktrees.** Each agent runs in an isolated git worktree:
   ```bash
   git worktree add -b mission-b/agent-1-wal-wiring ../powdb-mission-b-agent1 main
   git worktree add -b mission-b/agent-3-index-persistence ../powdb-mission-b-agent3 main
   # Agent 2 worktree is created AFTER Agent 1 merges (it builds on Agent 1's seam)
   ```
3. **Baseline capture.** Before any code changes, capture the pre-Mission-B
   bench snapshot to `docs/benchmarks/2026-04-07-pre-mission-b.md` for
   later comparison. (Already done — that's the wide-bench snapshot from
   2026-04-07.)
4. **Mission F merges first** (build profile quick wins). Mission B agents
   should be branched from a `main` that already includes Mission F's
   profile.release + target-cpu + FxHashMap + #[inline] commits, so the
   bench numbers they generate are against the optimised build.

## Agent 1 — WAL wiring + recovery (CRITICAL PATH)

**Worktree:** `../powdb-mission-b-agent1`
**Branch:** `mission-b/agent-1-wal-wiring`
**Estimated wall time:** 4-6 hours
**Owns:** `wal.rs`, `catalog.rs`, `lib.rs`, deletes `tx.rs`/`mvcc.rs`/`buffer.rs`

### Step-by-step

**A1.1 — WAL payload encoding** (30 min)
- Add `pub fn encode_write_record(table: &str, op: WriteOp, rid: Option<RowId>, row_bytes: &[u8]) -> Vec<u8>` to `wal.rs`.
- Add the matching `decode_write_record(data: &[u8]) -> Result<...>`.
- Format: `[u16 table_len][table utf8][u8 op_kind][u32 page_id][u16 slot_idx][u32 row_len][row_bytes ...]`
- 5 round-trip tests covering: insert, update, delete, empty row, max-len table name.

**A1.2 — DDL record type** (15 min)
- Add `WalRecordType::CreateTable = 6` and `DropTable = 7` (DropTable is reserved for Mission E but the type stays).
- Add `encode_ddl_record` / `decode_ddl_record` for the schema blob.
- Round-trip test.

**A1.3 — Catalog owns the WAL** (1 hour)
- Change `Catalog` struct: `wal: Wal, next_lsn: AtomicU64`.
- `Catalog::create(data_dir)`: open `<data_dir>/powdb.wal` (creates new file).
- `Catalog::open(data_dir)`: opens existing or creates.
- Engine constructor passes the catalog through unchanged.

**A1.4 — Write-path intercept** (1 hour)
- `Catalog::insert(table, row)`: encode row → `encode_write_record` → `wal.append` (no flush) → `table.heap.insert(row)` → return rid. The WAL append happens BEFORE the heap mutation so a crash mid-heap-write is recoverable.
- `Catalog::update(table, rid, row)`: similar, with `WriteOp::Update`.
- `Catalog::delete(table, rid)`: similar, with `WriteOp::Delete` and an empty row payload.
- Add `skip_wal: bool` parameter (default false) used by replay path so replayed records don't re-emit themselves.

**A1.5 — Commit seam** (30 min)
- `Catalog::commit(&mut self) -> io::Result<()>`: emit `WalRecordType::Commit` for the current implicit tx, then `wal.flush()` (which fsyncs).
- Engine's write path calls `catalog.commit()` after each successful write statement.
- For multi-statement tx (Mission E), commit will be called explicitly.

**A1.6 — DDL durability** (30 min)
- `Catalog::create_table(name, schema)`:
  1. `wal.append(WalRecordType::CreateTable, encode_ddl_record(...))`
  2. `wal.flush()`
  3. `self.tables.insert(name, table)`
  4. `self.persist()` (writes catalog.bin via tmp+rename)
- Order matters: WAL fsync first, then catalog.bin update. If catalog.bin write fails, the next open will see the CreateTable in the WAL and replay it.

**A1.7 — Recovery (the load-bearing piece)** (1.5 hours)
- In `Catalog::open` after the catalog.bin load and the per-table heap open:
  1. **Pre-replay index load**: scan `<data_dir>/<table>_<col>.idx.blob` files and call `BTree::open(path)`. Land this as `let _ = path;` plus `todo!("agent 3 hook")` for now — Agent 3 fills it in. The replay loop must run AFTER the indexes are loaded so replayed inserts update them.
  2. Call `wal.read_all()` to drain all records into `Vec<WalRecord>`.
  3. Group by `tx_id`. A tx is committed iff its set contains a `WalRecordType::Commit` record.
  4. For uncommitted txs: drop their records.
  5. For committed txs, in tx_id order:
     - Replay `CreateTable` records first (idempotent — skip if table already exists in catalog.bin).
     - Then replay Insert/Update/Delete via `Catalog::insert(skip_wal=true)` etc.
  6. After replay succeeds, `wal.truncate()`.
- Add `Catalog::replay_record(record: &WalRecord) -> io::Result<()>` as the replay primitive.

**A1.8 — Delete dead modules** (15 min)
- `git rm crates/storage/src/tx.rs` (157 LOC)
- `git rm crates/storage/src/mvcc.rs` (49 LOC)
- `git rm crates/storage/src/buffer.rs` (292 LOC)
- Remove `pub mod tx; pub mod mvcc; pub mod buffer;` from `crates/storage/src/lib.rs`.
- Remove any `use crate::tx::*;` in test files.

**A1.9 — Recovery test matrix** (1.5 hours)
Write all 8 tests from spec D6:
- `recover_single_insert`
- `recover_multi_insert` (100 rows)
- `recover_uncommitted_dropped` (no flush)
- `recover_mixed_commit_incomplete`
- `recover_with_index` (depends on Agent 3 minimum-hook in A1.7 step 1; tests can use `#[ignore]` until Agent 3 lands and then re-enabled in A1.10)
- `recover_with_update_then_crash`
- `recover_delete_then_crash`
- `recover_torn_wal` (truncate file mid-record, verify clean recovery up to last good record)

Each test follows the pattern: open engine → run statements → drop engine → reopen engine → assert state.

**A1.10 — Bench rebaseline** (30 min)
- Run `cargo bench -p powdb-bench`.
- The criterion gate may fail because writes are now ~15-50µs slower due to fsync.
- Run `scripts/update-bench-baseline.sh` (or rebuild the baseline JSON manually).
- Run `cargo run -p powdb-compare --release` and capture the new wide-bench numbers.
- Document the delta in the commit message and in `docs/benchmarks/2026-04-07-after-mission-b.md`.

**A1.11 — API surface for Agents 2 and 3** (15 min)
Document in PR description and `crates/storage/src/lib.rs` doc comment:
- `Engine::new(data_dir) -> Result<Arc<Engine>>`
- `Engine::execute_powql(&self, &str) -> Result<QueryResult, String>`
- `Engine::close(&self) -> io::Result<()>`
- `Catalog::write()` / `Catalog::read()` (Agent 2 needs)
- `Catalog::for_each_index_file(&self, f)` or equivalent enumeration helper (Agent 3 needs)

### Agent 1 acceptance criteria
- [ ] `cargo test --workspace` green (excluding the Agent 3 stub tests if marked `#[ignore]`)
- [ ] All 7 implementable recovery tests from D6 pass
- [ ] `tx.rs`, `mvcc.rs`, `buffer.rs` deleted (498 LOC removed)
- [ ] `cargo bench -p powdb-bench` passes the (rebaselined) gate
- [ ] PR description documents the API for Agents 2 + 3
- [ ] Coordinator merges to `main` before Agents 2 + 3 start

## Agent 3 — Index persistence (parallel with Agent 2 once Agent 1 lands)

**Worktree:** `../powdb-mission-b-agent3`
**Branch:** `mission-b/agent-3-index-persistence` (rebased onto Agent 1's merge)
**Estimated wall time:** 2-3 hours
**Owns:** `btree.rs`, the index-load hook in `catalog.rs` Agent 1 left as `todo!()`, minor `table.rs`

### Step-by-step

**A3.1 — Serialisation format** (1 hour)
- Implement `BTree::serialize_to_file(&self, path: &Path) -> io::Result<()>`.
- Format from spec D4: `BIDX` magic + version + node_count + root + per-node tagged dump.
- Use `crates/storage/src/row.rs::encode_value` for keys (existing helper).
- Write to `path.tmp`, then rename to `path`, then fsync the parent dir for atomicity.
- 4 tests: empty tree, single-key tree, multi-level tree (>256 keys), tree with String keys.

**A3.2 — Deserialisation** (45 min)
- `BTree::open(path: &Path) -> io::Result<Self>`. Verify magic + version, rebuild `nodes: Vec<Node>`, set `root` and `path`.
- Test: serialize then open, assert lookup correctness.

**A3.3 — Dirty tracking** (15 min)
- Add `dirty: bool` to `BTree`. Set true on every `insert`/`delete`. Reset by the persistence call.
- Setter via `BTree::take_dirty(&mut self) -> bool`.

**A3.4 — Wire into commit path** (30 min)
- In `Catalog::commit` (Agent 1's seam): after `wal.flush()` succeeds, walk all tables → all indexes; for any dirty index, `btree.serialize_to_file(&btree.path) + btree.take_dirty()`.
- Order matters: WAL fsync first (durability for the row data), then index blob (durability for the index structure). If the index blob write fails, the next open replays the WAL inserts and rebuilds the dirty parts of the index.

**A3.5 — Load at open** (30 min)
- Replace Agent 1's `todo!("agent 3 hook")` with the real load:
  ```rust
  for entry in std::fs::read_dir(data_dir)? {
      let entry = entry?;
      let path = entry.path();
      if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
          if let Some(stripped) = name.strip_suffix(".idx.blob") {
              if let Some((table, col)) = stripped.rsplit_once('_') {
                  let btree = BTree::open(&path)?;
                  if let Some(t) = self.tables.get_mut(table) {
                      t.indexes.insert(col.to_string(), btree);
                  }
              }
          }
      }
  }
  ```

**A3.6 — Tests** (45 min)
Three tests from spec phase 3 §6:
- `idx_persists_across_restart`
- `idx_incremental_updates`
- `idx_delete_persisted`
- Plus: enable Agent 1's `recover_with_index` which was `#[ignore]`d in A1.9.

### Agent 3 acceptance criteria
- [ ] All BTree serialization tests pass
- [ ] All 3 index persistence tests pass + Agent 1's `recover_with_index`
- [ ] `cargo test --workspace` green
- [ ] PR rebased on Agent 1's merge cleanly

## Agent 2 — Concurrency (depends on Agent 1)

**Worktree:** `../powdb-mission-b-agent2` (created AFTER Agent 1 merges)
**Branch:** `mission-b/agent-2-concurrency` (branched from `main` after Agent 1 + Agent 3 land)
**Estimated wall time:** 3-4 hours
**Owns:** `handler.rs`, `executor.rs` (signature changes), `plan.rs` (is_write helper)

### Step-by-step

**A2.1 — `Catalog` becomes `RwLock`-able** (30 min)
- Add `pub struct Catalog { ... }` (already exists). The Engine wrapper gets an `Arc<RwLock<Catalog>>`.
- Add helper methods on the Engine that take internal locks: `engine.read()`, `engine.write()`. These are NOT exposed publicly — execute_powql dispatches internally.

**A2.2 — `is_write()` on PlanNode** (15 min)
- Add to `crates/query/src/plan.rs`:
  ```rust
  impl PlanNode {
      pub fn is_write(&self) -> bool {
          matches!(self,
              PlanNode::Insert { .. } |
              PlanNode::Update { .. } |
              PlanNode::Delete { .. } |
              PlanNode::CreateTable { .. })
      }
  }
  ```
- Test for each variant.

**A2.3 — Split execute_powql** (1.5 hours)
- Refactor `Executor::execute_plan` to two flavours:
  - `execute_read_plan(&self, plan: &PlanNode, catalog: &Catalog) -> Result<...>`
  - `execute_write_plan(&self, plan: &PlanNode, catalog: &mut Catalog) -> Result<...>`
- The read variant takes `&self` and `&Catalog` only — no mutation.
- The write variant takes `&self` (engine state) and `&mut Catalog`.
- Most arms are read; only `Insert/Update/Delete/CreateTable` go into the write side.
- Engine's `execute_powql`:
  ```rust
  pub fn execute_powql(&self, input: &str) -> Result<QueryResult, String> {
      let plan = self.plan_query(input)?;
      if plan.is_write() {
          let mut cat = self.catalog.write().unwrap();
          let result = self.execute_write_plan(&plan, &mut cat)?;
          cat.commit().map_err(|e| e.to_string())?;
          Ok(result)
      } else {
          let cat = self.catalog.read().unwrap();
          self.execute_read_plan(&plan, &cat)
      }
  }
  ```

**A2.4 — Handler rewrite** (30 min)
- `crates/server/src/handler.rs:65` goes from:
  ```rust
  let mut eng = engine.lock().unwrap();
  match eng.execute_powql(&query) { ... }
  ```
  to:
  ```rust
  let engine = engine.clone();   // Arc clone
  let result = tokio::task::spawn_blocking(move || {
      engine.execute_powql(&query)
  }).await.unwrap();
  ```
- The handler's `Arc<Engine>` no longer needs `Mutex` — remove the wrapping.
- Run server tests.

**A2.5 — Concurrency stress test** (45 min)
- Spawn 16 reader tokio tasks doing `point_lookup_indexed` against a shared Engine.
- Spawn 1 writer task doing inserts.
- Run for 1 second.
- Assert: no deadlock, no panic, readers' observed row counts are monotonic, all writer commits visible to subsequent reads.

**A2.6 — Throughput micro-bench** (45 min)
- New bench `bench_concurrent_reads.rs`: N=1, N=4, N=16, N=64 concurrent point lookups.
- Report p50, p99, total throughput.
- Verify N=16 throughput ≥ 10× N=1 throughput. (The pre-mission-B baseline is N=16 == N=1 due to the global mutex.)

### Agent 2 acceptance criteria
- [ ] `cargo test --workspace` green
- [ ] Concurrency stress test passes
- [ ] `bench_concurrent_reads` shows ≥10× speedup at N=16 vs N=1
- [ ] `handler.rs` no longer holds `Arc<Mutex<Engine>>`
- [ ] Wide bench numbers no worse than +10% vs the post-Agent-1 baseline

## Coordinator merge order

```
main
 ├── Mission F merged
 │
 ├── Agent 1 PR (mission-b/agent-1-wal-wiring)
 │     ↓ merge
 │
 ├── Agent 3 PR (rebased onto Agent 1's merge)
 │     ↓ merge
 │
 └── Agent 2 PR (rebased onto Agent 1 + Agent 3)
       ↓ merge
       Mission B complete
```

**Why this order:** Agent 2 touches executor signatures which ripple
through the read path. Landing it last minimises rebase pain for Agents 1
and 3, both of which need a stable executor surface.

**Rebase strategy:** when Agent 3 rebases onto Agent 1, the only conflict
should be the `todo!("agent 3 hook")` line in `catalog.rs::open`, which
Agent 3 replaces with the real index load.

## Post-merge validation

After all three merges land on `main`:

1. `cargo test --workspace` — full suite green
2. `cargo bench -p powdb-bench` — no regression beyond rebaselined floor
3. `cargo run -p powdb-compare --release` — capture new wide-bench snapshot
4. `docs/benchmarks/2026-04-NN-after-mission-b.md` — write the snapshot
5. Smoke test: `cargo run --bin powdb -- --data-dir /tmp/powdb-smoke` — start the server, insert 10K rows, kill -9, restart, verify 10K rows present
6. Update the four mission specs' "Status" line to reflect Mission B is done

## Failure modes and rollback

If Agent 1's recovery tests reveal a structural problem in the WAL layer:
- The branch is still in a worktree, not merged.
- Discard the worktree: `git worktree remove ../powdb-mission-b-agent1 --force`
- Recreate from `main`, restart the agent with a refined task description.

If Agent 1 lands but Agent 2 reveals the read/write split breaks something:
- Revert Agent 2's merge with `git revert <merge-commit>`
- Mission B is still partially done — durability is in, concurrency isn't.
- Re-attempt Agent 2 with the lessons learned.

If post-merge benches show >2x write regression vs pre-Mission-B baseline:
- This is BEYOND the spec exit criteria's "no worse than 2x".
- Profile with `cargo flamegraph`, find the hot syscall, and either tune
  `Wal::batch_size` or defer to Mission C.

## Coordinator checklist

Before dispatching agents:

- [ ] Mission F has merged into `main`
- [ ] `docs/benchmarks/2026-04-07-pre-mission-b.md` snapshot saved
- [ ] Worktrees for Agents 1 and 3 created (Agent 2 deferred)
- [ ] Mission B spec linked from each agent task description
- [ ] Each agent has the spec's relevant Decision section (D1-D6) in their prompt
- [ ] Each agent knows it CANNOT re-litigate the locked decisions
- [ ] Each agent knows the file ownership and merge order

After Agent 1 PR opens:

- [ ] Read recovery test output, verify 7 of 8 pass (the 8th depends on Agent 3)
- [ ] Code review for: skip_wal flag correctness, replay idempotency, fsync ordering
- [ ] Merge to main
- [ ] Notify Agent 3 they can rebase

After Agent 3 PR opens:

- [ ] Verify all 4 BTree serialization tests pass
- [ ] Verify Agent 1's `recover_with_index` now passes
- [ ] Code review for: serialize/deserialize round-trip, dirty flag correctness
- [ ] Merge to main
- [ ] Create Agent 2 worktree from updated main, dispatch Agent 2

After Agent 2 PR opens:

- [ ] Verify concurrent read bench shows ≥10x improvement
- [ ] Verify no read-path regressions in wide bench
- [ ] Code review for: lock ordering (catalog before WAL or vice versa, document the choice), handler `spawn_blocking` correctness
- [ ] Merge to main
- [ ] Run post-merge validation checklist

## Open coordination questions

- **When should Agent 2 rebase?** After Agent 1 OR after Agent 3? Spec
  says "after both" but if Agent 3 takes longer than Agent 1, Agent 2
  could land its work against Agent 1's branch and let Agent 3 rebase
  on top. Coordinator's call based on actual completion times.
- **Who updates `thesis-ratios.json` if Mission B's WAL fsync widens
  the insert-vs-lookup ratio?** Agent 1, in their PR, with explicit
  justification in the commit message.
- **Should Agent 1 bench in the worktree's data dir or `/tmp`?** `/tmp`
  for tmpfs speed; the recovery tests can use a per-test temp dir.
