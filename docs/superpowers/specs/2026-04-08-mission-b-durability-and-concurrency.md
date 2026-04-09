# Mission B — Durability + Concurrency

**Status:** design approved (2026-04-07), implementation pending
**Owner:** multi-agent dispatch (3 agents in git worktrees)
**Depends on:** nothing (new code path)
**Blocks:** Mission C (write perf), Mission D (read perf), Mission E (language)

## Problem

The audit on 2026-04-07 found three concurrent correctness gaps any one of
which disqualifies PowDB from being called a "real database":

1. **No durability.** `crates/storage/src/heap.rs:67-92` calls
   `self.disk.write_page(...)` directly without WAL. A crash mid-insert
   silently loses the insert — the page is either half-written or atomic
   at the OS level (no guarantee), and there is no recovery log to replay.
2. **Global engine mutex.** `crates/server/src/handler.rs:65` acquires
   `engine.lock().unwrap()` for every query. Reads serialize behind writes
   and behind each other. Zero read concurrency.
3. **Indexes evaporate on restart.** `crates/storage/src/table.rs:25-30`
   explicitly opens tables with `indexes: HashMap::new()`. The `BTree`
   struct has a `path` field marked `#[allow(dead_code)]`
   (`crates/storage/src/btree.rs`) — it was designed for persistence but
   the code never wrote to it.

Underneath these there is a fourth gap:

4. **Dead buffer pool code.** `crates/storage/src/buffer.rs` implements a
   full clock-sweep buffer pool that is not referenced from anywhere else
   in the codebase. Reads bypass it via mmap, writes bypass it via direct
   `DiskManager::write_page`. 292 lines of unused code. Either wire it in
   or delete it.

## Non-goals

This mission fixes correctness and enables multi-reader concurrency. It is
NOT the write-performance mission (that's Mission C). It is NOT the
read-performance mission (that's Mission D). It is NOT the language mission
(that's Mission E). If a task looks like it belongs in those missions,
defer it.

Specifically out of scope for Mission B:
- Fused in-place update fast path — Mission C.
- Group-commit tuning beyond the existing `Wal::batch_size` parameter — Mission C.
- Proper paged B+ tree (Option B for index persistence) — deferred to
  post-Mission C when write perf is tuned.
- Multi-statement transactions (`begin; ... commit;`) — Mission E. Mission B
  only needs implicit per-statement transactions.
- BEGIN/COMMIT/ROLLBACK PowQL keywords — parser work, Mission E.
- Backup/restore and pg_dump equivalents — separate ops mission.
- Replication, streaming WAL, hot standbys — explicitly out of scope for
  PowDB's thesis (single-node).

## Architectural decisions (locked)

These were chosen during the 2026-04-07 audit with the Mission B scope
approval. Agents MUST NOT re-litigate.

### D1. WAL style: logical record + page invariant, not physiological

`crates/storage/src/wal.rs` already implements a logical WAL with record
types `Insert/Update/Delete/Commit/Rollback`, CRC32 per record, group
commit via `batch_size`, replay via `read_all()`, and `truncate()`. It
passes 6 tests. **We wire this up, we do not rewrite it.**

Each record's `data` payload will be a length-prefixed tuple of
`(table_name_len u16, table_name utf8, rid page_id u32, rid slot_index u16,
row_bytes_len u32, row_bytes ...)` for Insert, Update, and Delete. Commit
has an empty payload. Rollback is unused in Mission B (no multi-statement
tx yet) but the record type stays for Mission E.

**Why logical not physiological:**
- Physiological (page-level byte diffs) requires full-page writes after
  checkpoint to protect against torn-page hazards. That's a much bigger
  implementation.
- We already have the row-encoder in `crates/storage/src/row.rs`. Logging
  encoded row bytes is a one-copy operation.
- Recovery becomes "replay the insert/update/delete against the table"
  which is idempotent if we track the last durable LSN per table.
- SQLite's WAL mode is physiological because they support rollback inside
  a tx. We don't support multi-statement tx in Mission B so we don't need
  it.

### D2. Concurrency model: single-writer + snapshot readers via WAL frame LSN

Replace `Arc<Mutex<Engine>>` with:

```rust
pub struct Engine {
    catalog: Arc<RwLock<Catalog>>,    // reads take read-lock, writes take write-lock
    wal: Arc<Mutex<Wal>>,              // serialised behind a single writer
    next_lsn: AtomicU64,
    planner_cache: Arc<PlanCache>,     // already lock-free
}
```

- Reads take `catalog.read()` — N readers run concurrently on the mmap.
- Writes take `catalog.write()` — exclusive. Only one writer at a time.
  This matches SQLite WAL mode and Postgres serial write path on a single
  table, and is the right tradeoff for a single-node store.
- `execute_powql` classifies the query as read or write by inspecting the
  top-level `PlanNode` variant (`Insert | Update | Delete | CreateTable`
  → write; everything else → read).
- On the handler (`crates/server/src/handler.rs:65`), remove
  `Arc<Mutex<Engine>>` entirely. Pass `Arc<Engine>` and let the engine
  internally handle the read/write dispatch.

**Why not full MVCC with per-row version chains:** `crates/storage/src/mvcc.rs`
has 49 lines of `UndoLog` primitives that nothing calls. Real row-version
MVCC is a 2000+ LOC undertaking and requires GC/purge. We don't need it for
the single-writer model.

### D3. MVCC: delete `mvcc.rs` and `tx.rs` as dead code

`crates/storage/src/tx.rs` (157 LOC) and `crates/storage/src/mvcc.rs` (49 LOC)
are tested but never referenced outside their own `#[cfg(test)]` blocks.
The Mission B write path is single-writer, so there is no MVCC snapshot
logic to implement. Delete both files. If Mission E brings back
multi-statement tx, we'll reintroduce tx state from the WAL's tx_id field.

**Rationale:** dead code that passes tests is still dead code, and it
provides a false sense of "we have MVCC" when we don't. Agent 1 deletes
these files as part of WAL wiring.

### D4. Index persistence: single-blob-per-index, Option A

For Mission B, each `BTree` serialises to a single file
`<data_dir>/<table>_<column>.idx.blob` with format:

```
magic      [4]   = "BIDX"
version    u16
node_count u32
root_index u32
for each node:
    node_type u8    (0 = Internal, 1 = Leaf)
    key_count u16
    keys      (length-prefixed Value encoding, reuse row.rs encoder)
    children  (u32 × key_count + 1 for Internal)
    or values (RowId × key_count for Leaf)
    next_leaf u32 (0xFFFFFFFF = None, for Leaf only)
```

- On `create_index`: build the BTree in memory from a scan (existing
  behaviour), then call `btree.serialize_to_file(&self.path)` and fsync.
- On `Catalog::open`: for each index file matching `<table>_<col>.idx.blob`,
  call `BTree::deserialize(&path)` and install into `table.indexes`.
  This runs BEFORE the WAL replay so that replayed inserts update the
  indexes.
- On `BTree::insert/delete`: after mutation, mark the BTree dirty. On WAL
  commit (flush), if any index is dirty, write the whole blob + fsync.

**Why single-blob not paged:** the full B+ tree lives in `Vec<Node>` with
Order 256 today. A 100K-row index fits in <10KB of serialised form.
Single-blob is O(n) to write on commit, which is acceptable for the
write volumes PowDB targets and is dramatically simpler than a paged B+
tree with free lists and COW root pointers. The paged design stays on
the Mission-C-or-later menu.

**Correctness note:** the blob write is protected by the WAL. If a crash
happens after WAL fsync but before the index blob is written, recovery
replays the WAL inserts/updates/deletes against the in-memory index and
re-dumps. This is the reason the rebuild runs *before* the replay.

### D5. Transaction model: implicit per-statement transactions

Every `execute_powql` call starts an implicit transaction, emits WAL
records, appends a Commit record, flushes the WAL, and returns success.
A crash between the Commit record and the WAL flush means the work is
lost — that's the implicit-tx semantics SQLite/MySQL have without
`BEGIN`.

Structure:

```
fn execute_powql(&self, input: &str) -> Result<QueryResult, String> {
    let plan = parse_and_plan(input)?;
    if plan.is_write() {
        let mut cat = self.catalog.write();      // blocks concurrent writers
        let mut wal = self.wal.lock().unwrap();
        let tx_id = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let result = execute_write(&mut cat, &mut wal, tx_id, &plan)?;
        wal.append(tx_id, WalRecordType::Commit, &[])?;
        wal.flush()?;                            // fsync here
        Ok(result)
    } else {
        let cat = self.catalog.read();           // shared with other readers
        execute_read(&cat, &plan)
    }
}
```

Mission E (language) will add `BEGIN/COMMIT` PowQL statements that hold
the write-lock across multiple statements. For now: one statement =
one transaction = one fsync.

### D6. Testing strategy: instrumented crash injection + restart

Agent 1 adds a `wal_crash_after_fsync()` test helper gated by `cfg(test)`
that:

1. Runs a sequence of PowQL statements through a fresh engine.
2. Calls `engine.wal_sync()` to flush and fsync.
3. Simulates crash by drop-and-reopen of the engine (closes the WAL, closes
   the heap, closes the catalog, then re-opens fresh from the same data dir).
4. Verifies the committed state is recovered and the uncommitted state is
   not.

We deliberately do NOT use subprocess fork + SIGKILL. It's flakier, harder
to debug in CI, and the drop-and-reopen path exercises exactly the same
recovery code.

**Required test cases (Agent 1 writes all of these):**

- `recover_single_insert`: one insert, flush, restart, verify row exists.
- `recover_multi_insert`: 100 inserts, flush, restart, verify all 100.
- `recover_uncommitted_dropped`: 10 inserts with NO flush, restart, verify
  0 rows present.
- `recover_mixed_commit_incomplete`: 5 inserts flushed, then 5 more with
  no flush, restart, verify exactly 5 rows present.
- `recover_with_index`: create_index, insert rows, flush, restart, verify
  index point-lookup still works.
- `recover_with_update_then_crash`: insert + update + commit + crash +
  restart + verify updated value visible.
- `recover_delete_then_crash`: insert + delete + commit + crash + restart +
  verify row is absent.
- `recover_torn_wal`: corrupt the last N bytes of the WAL file, open,
  verify the pre-corruption records recover cleanly (the existing
  `read_all` already stops at CRC mismatch).

## Implementation phases

### Phase 1: Agent 1 — WAL wiring + recovery (critical path)

Owns: `crates/storage/src/wal.rs`, `crates/storage/src/heap.rs`,
`crates/storage/src/catalog.rs`, `crates/storage/src/lib.rs`,
`crates/storage/src/table.rs`, plus `crates/storage/src/mvcc.rs` +
`crates/storage/src/tx.rs` (deletion), plus `crates/storage/src/buffer.rs`
(delete or note dead).

1. **Record payload encoding.** Add `pub fn encode_write_record(table_name:
   &str, op: WriteOp, rid: Option<RowId>, row_bytes: &[u8]) -> Vec<u8>`
   and the matching `pub fn decode_write_record(data: &[u8]) -> Result<...>`
   to `wal.rs`. Test round-trips.

2. **Catalog holds WAL.** Change `Catalog` struct to own an `Option<Wal>`.
   `Catalog::create(data_dir)` opens `<data_dir>/powdb.wal`. `Catalog::open`
   opens the existing WAL if present, else creates.

3. **Write-path intercept.** In `Catalog::insert/update/delete`:
   - Encode the record payload.
   - Call `self.wal.append(self.next_lsn(), WalRecordType::Insert|..., &data)`.
   - Perform the heap mutation.
   - Return the rid.
   `Catalog` owns an `AtomicU64` for the LSN counter.

4. **Commit seam.** Add `pub fn commit(&mut self) -> io::Result<()>` to
   `Catalog` that appends a `Commit` record for the current implicit tx
   and calls `self.wal.flush()`. The engine calls `catalog.commit()` after
   each successful write statement.

5. **Recovery.** On `Catalog::open`, after opening each table:
   a. Load any `.idx.blob` files into the table's `indexes` map (Agent 3
      work — land a minimal stub here that `todo!()`s if we see an .idx.blob
      but Agent 3 will land the real load).
   b. Call `self.wal.read_all()` to get all records.
   c. Group by `tx_id`. A tx is "committed" iff it has a `Commit` record.
   d. For each committed tx, replay Insert/Update/Delete records against
      the correct table. Use `heap.insert/update/delete` directly (bypass
      the WAL on replay — the `Catalog::replay` path takes a `skip_wal` flag).
   e. Uncommitted txs are dropped.
   f. After replay succeeds, truncate the WAL.

6. **Catalog persistence protected.** The `persist()` call in `create_table`
   writes the schema blob to `catalog.bin.tmp` and renames. Add a WAL record
   type `WalRecordType::CreateTable = 6` for DDL durability. If the rename
   succeeds before WAL flush, recovery needs to see the table on restart.
   Strategy: DDL is ALWAYS `wal.append(CreateTable) + wal.flush() + persist()`
   in that order. Recovery replays CreateTable records before any DML
   records so the table exists when we try to replay inserts into it.

7. **Delete dead modules.** Remove `tx.rs`, `mvcc.rs`, `buffer.rs`. Update
   `lib.rs` `pub mod` declarations. Delete the one test in `tx.rs` that
   imports from `mvcc.rs`. Total deletion: 498 LOC.

8. **Test matrix.** Write the 8 recovery tests from D6.

9. **Benches still pass.** Run `cargo bench -p powdb-bench` and verify the
   criterion regression gate. The WAL adds one fsync per write which will
   move `insert_single` numbers — rebaseline via
   `scripts/update-bench-baseline.sh` and note the delta in the commit
   message. Do NOT change `thesis-ratios.json` without explicit
   justification (the `insert_single_over_btree_lookup` ratio of 10.0
   should still hold since the btree lookup denominator doesn't fsync).

10. **API surface for Agents 2 and 3.** Document (in a PR description and
    in `crates/storage/src/lib.rs` doc comment):
    - `Engine::new(data_dir) -> Result<Arc<Engine>>`
    - `Engine::execute_powql(&self, input) -> Result<QueryResult, String>`
    - `Engine::close(&self) -> io::Result<()>` (flushes WAL)
    - `Catalog::write()` / `Catalog::read()` guards (Agent 2 depends on this)
    - `Catalog::for_each_index_file(&self, f)` (Agent 3 depends on this)

### Phase 2: Agent 2 — concurrency (depends on Phase 1 API)

Owns: `crates/server/src/handler.rs`, `crates/server/src/lib.rs`,
`crates/server/src/main.rs`, plus the `Engine` struct's concurrency seams.

1. **Remove `Arc<Mutex<Engine>>` from handler.** Pass `Arc<Engine>` (not
   mutex-wrapped). The engine internally handles the single-writer /
   multi-reader split via `RwLock<Catalog>`.

2. **Classify queries.** Add `PlanNode::is_write(&self) -> bool` in
   `crates/query/src/plan.rs`. Variants `Insert | Update | Delete |
   CreateTable` return true; everything else false.

3. **Split execute.** Split `execute_powql` into `execute_read` and
   `execute_write`. The top-level dispatch:
   ```rust
   pub fn execute_powql(&self, input: &str) -> Result<QueryResult, String> {
       let plan = ...;
       if plan.is_write() {
           self.execute_write(&plan)  // takes catalog.write() + wal.lock()
       } else {
           self.execute_read(&plan)   // takes catalog.read()
       }
   }
   ```
   Change the executor's `&mut self` on reads to `&self` + shared catalog
   borrow. The read paths already don't mutate engine state.

4. **Plan cache stays.** The plan cache (`crates/query/src/plan_cache.rs`)
   is already shared — Agent 2 just needs to verify it's still
   `Arc<PlanCache>` not behind the engine mutex.

5. **Concurrency tests.** Spawn 16 reader tokio tasks + 1 writer task
   against the same `Engine`. Run for 1 second. Assert: all readers see
   monotonically-increasing row counts, no deadlock, no panics, writer
   commits are visible immediately after it returns.

6. **Handler rewrite.** `handler.rs:65` goes from:
   ```rust
   let mut eng = engine.lock().unwrap();
   match eng.execute_powql(&query) { ... }
   ```
   to:
   ```rust
   let engine = engine.clone();       // Arc clone
   let result = tokio::task::spawn_blocking(move || {
       engine.execute_powql(&query)
   }).await.unwrap();
   ```
   Reads happen on the tokio blocking pool so the async task doesn't
   block on fsync during a write. This is worth ~100x throughput on
   read-heavy workloads.

7. **Bench the improvement.** Write a micro-bench that runs N=1/4/16/64
   concurrent `point_lookup_indexed` queries against a shared engine and
   reports p50/p99 latency and total throughput. Verify N=16 is > 10x
   the throughput of N=1 (the old behaviour was N=16 == N=1 due to the
   mutex).

### Phase 3: Agent 3 — index persistence (parallel with Phase 2)

Owns: `crates/storage/src/btree.rs`, plus a small hook into
`crates/storage/src/catalog.rs::open` that Agent 1 will leave as a
`todo!()` for Agent 3 to fill in.

1. **Serialisation format.** Implement `BTree::serialize_to_file(&self,
   path: &Path) -> io::Result<()>` matching the format in D4. Use
   `row::encode_value` for keys. fsync after write.

2. **Deserialisation.** `BTree::open(path: &Path) -> io::Result<Self>`.
   Verify magic + version. Rebuild `nodes: Vec<Node>` from the blob. No
   validation beyond CRC at file boundary (the replay in Agent 1 handles
   staleness).

3. **Dirty tracking.** Add `dirty: bool` to `BTree`. Flipped true on every
   `insert/delete`, flipped false after successful serialisation.

4. **Hook into write path.** In `Table::insert` after the index update,
   if `btree.dirty`, the `Catalog::commit()` path (Agent 1's seam) calls
   `btree.serialize_to_file(&btree.path)` + `btree.dirty = false`. This is
   a simple path-through — Agent 3 lands the BTree methods, Agent 1 wires
   the call.

5. **Load at open.** In `Catalog::open`, for each table, scan the data
   directory for `<table>_<col>.idx.blob` files and call `BTree::open`.
   Install into `table.indexes`. This is the hook Agent 1 leaves as
   `todo!()`.

6. **Tests.** Three tests:
   - `idx_persists_across_restart`: create_index, insert 100 rows, close,
     reopen, point-lookup returns the right rid.
   - `idx_incremental_updates`: insert 100 rows, close, reopen, insert 50
     more, close, reopen, verify all 150 visible through index.
   - `idx_delete_persisted`: insert 100, delete 50, close, reopen, verify
     the 50 are gone from the index.

## Criterion regression gate interaction

The WAL adds ~15-50 µs per write (one fsync). The criterion gate in
`crates/bench/baseline/main.json` has:

```
insert_10k, insert_single, insert_batch_1k, update_by_pk, update_by_filter,
delete_by_filter
```

all currently `null` (pending first CI capture). After Mission B lands, the
first CI run becomes the new capture point and `insert_single_over_btree_lookup`
ratio might widen from 5.51x to 10-20x on M1 depending on fsync cost.

**Acceptable:** the ratio ceiling is currently 10.0 with an explicit note
that post-WAL it will move. Agent 1 updates the ratio to 30.0 **only if**
the observed post-WAL number exceeds 10.0, and documents the reason in a
hand-editing commit to `thesis-ratios.json` per the rebaseline protocol.

## Exit criteria

Mission B is complete when all of:

- [ ] All 8 recovery tests (D6) pass
- [ ] `cargo test --workspace` green
- [ ] `cargo bench -p powdb-bench && cargo run -p powdb-bench --bin compare`
      passes the gate (possibly with rebaseline commit for writes)
- [ ] Concurrent read bench (Phase 2 §7) shows ≥10x speedup at N=16 vs
      N=1 on `point_lookup_indexed`
- [ ] `tx.rs`, `mvcc.rs`, `buffer.rs` deleted
- [ ] Crash + restart on a real Fly machine (smoke test, not CI) recovers
      a 10K-row fixture losslessly
- [ ] Wide bench (`cargo run -p powdb-compare --release`) shows write
      numbers NO WORSE than 2x the pre-WAL numbers from
      `docs/benchmarks/2026-04-07-wide-bench-snapshot.md` (the fsync cost
      is acceptable but the per-row decode path must NOT regress further
      — that's a Mission C job)

## Open questions (NOT blockers)

- **Per-tx fsync vs group fsync on the handler side.** The current
  `Wal::batch_size` autoflushes every N records. With single-statement
  implicit tx, the single-writer model ends up calling flush after every
  `Commit` record, so `batch_size` is effectively 1. A proper group-commit
  pattern needs multiple writers sharing one fsync — comes in Mission C.
- **Checkpoint frequency.** Recovery currently replays the entire WAL from
  byte 0. After N hours of writes this is slow. Add a "checkpoint" = "WAL
  truncated, catalog fully flushed" sometime in Mission B §Phase1 step 5f
  ("After replay succeeds, truncate the WAL") OR run a checkpoint every
  16MB of WAL. TBD per Agent 1's judgement.
- **Catalog bin vs WAL replay race.** If `catalog.bin` lags the WAL by one
  DDL record, recovery sees a CreateTable record for a table that already
  exists in catalog.bin. Agent 1 makes this idempotent: if replay hits
  CreateTable and the table is already present, skip (don't error).

## File touches by agent (for worktree partitioning)

### Agent 1
```
crates/storage/src/wal.rs        (+payload encode/decode, +replay)
crates/storage/src/heap.rs       (no change — Catalog owns WAL)
crates/storage/src/catalog.rs    (+wal field, +commit, +replay, +DDL record)
crates/storage/src/table.rs      (minor — catalog passes WAL)
crates/storage/src/lib.rs        (-pub mod tx, -pub mod mvcc, -pub mod buffer)
crates/storage/src/tx.rs         (DELETE)
crates/storage/src/mvcc.rs       (DELETE)
crates/storage/src/buffer.rs     (DELETE)
crates/storage/Cargo.toml        (no change)
```

### Agent 2
```
crates/server/src/handler.rs     (remove Mutex, add spawn_blocking)
crates/server/src/lib.rs         (signature changes)
crates/server/src/main.rs        (signature changes)
crates/query/src/executor.rs     (split read/write, relax &mut self to &self on reads)
crates/query/src/plan.rs         (add is_write())
```

### Agent 3
```
crates/storage/src/btree.rs      (serialize/deserialize)
crates/storage/src/catalog.rs    (the index-load hook Agent 1 leaves)
crates/storage/src/table.rs      (minor — dirty flag forwarding)
```

**Merge order:** Agent 1 merges first → Agent 3 merges second (against
Agent 1's branch) → Agent 2 merges last (against both). Rationale: Agent 2
touches executor signatures which ripple everywhere, so landing it last
minimises rebase pain.

## Related missions

- `docs/superpowers/specs/2026-04-08-mission-c-write-path-perf.md` — the
  write-side perf fixes that depend on Mission B having wired the WAL.
- `docs/superpowers/specs/2026-04-08-mission-d-read-path-perf.md` — the
  read-side gap closure (mmap persistence, projection streaming, point-lookup
  profile).
- `docs/superpowers/specs/2026-04-08-mission-e-language-features.md` —
  joins, group by, prepared statements, BEGIN/COMMIT, EXPLAIN. Multi-statement
  tx requires the Mission B plumbing.
