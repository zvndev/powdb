# Mission C — Write-path performance

**Status:** design draft (2026-04-07)
**Depends on:** Mission B (WAL wired, concurrency seam, index persistence)
**Blocks:** thesis credibility — current write numbers are embarrassing

## Problem

Per the 2026-04-07 wide bench
(`docs/benchmarks/2026-04-07-wide-bench-snapshot.md`), PowDB is dramatically
slower than SQLite on every write workload:

| workload         |  PowDB |  SQLite | PowDB is |
| ---              |   ---: |    ---: | ---:     |
| insert_single    |  6143  |    906  |  6.8x slower  |
| insert_batch_1k  |  6274  |    301  | 20.8x slower  |
| update_by_pk     | 18096  |    393  | **46x slower** |
| update_by_filter | 943179014 | 744319 | **1267x slower (~1 second)** |
| delete_by_filter | 1388125 | 234944 |  5.9x slower  |

These are not WAL-fsync losses (PowDB currently has NO WAL — Mission B adds
it). These are pure per-op CPU costs in the insert/update/delete path.

## Root causes identified by the 2026-04-07 audit

### C1. `Table::update` = `delete + insert` pattern

`crates/storage/src/table.rs:69-72`:
```rust
pub fn update(&mut self, rid: RowId, values: &Row) -> io::Result<RowId> {
    self.delete(rid)?;
    self.insert(values)
}
```

Every update does:
1. `heap.get(rid)` to read old row (decode overhead)
2. `decode_row` to get old index keys
3. remove entries from each index
4. `heap.delete` (page read + page write)
5. `encode_row` the new values
6. `heap.insert` (page read + page write)
7. insert into each index

That's **two full page writes and a full row decode+encode per update**.
`heap.update` at `crates/storage/src/heap.rs:138` already tries in-place
update first (faster slot rewrite), but `Table::update` bypasses it
entirely.

### C2. `collect_rids_for_mutation` collects, then the update loop re-decodes

`crates/query/src/executor.rs:442-473` (`PlanNode::Update`):
```rust
let matching_rids = self.collect_rids_for_mutation(input, table, &schema)?;
for rid in matching_rids {
    let mut row = match self.catalog.get(table, rid) { ... };  // decode
    for (idx, val) in &resolved_assignments { row[*idx] = val.clone(); }
    self.catalog.update(table, rid, &row).map_err(...)?;       // re-encode
}
```

Every matched row is:
- Walked as raw bytes in `collect_rids_for_mutation` (fast)
- Decoded again in `catalog.get(rid)` (slow)
- Re-encoded in `catalog.update` (slow)
- Decoded AGAIN inside `Table::update → Table::delete → heap.get → decode_row`
  for the index-removal step

Three decodes per update. None of them are needed — the raw bytes from
`for_each_row_raw` already have everything we need.

### C3. Fixed-size column updates don't use in-place byte patching

For the common update `status := "senior"` or `age := 42`:
- Int / DateTime / Float / Bool / UUID columns have known byte offsets in
  the encoded row (from `FastLayout`). Updating an int is a single 8-byte
  `ptr.write_unaligned` at the known offset.
- Variable-length columns (Str / Bytes) can do in-place ONLY if the new
  value ≤ old value in length (the slot layout supports overwrite), else
  falls back to delete+insert.

`crates/storage/src/page.rs` has a slotted-page layout and `update` method
on `Page` — let Agent verify it supports fixed-offset in-place rewrite.

### C4. Insert batch = parse+plan+execute per row

`crates/compare/src/engines/powdb.rs:380-389` shows the bench harness
forming a new SQL-style string per row and calling `execute_powql` 1000
times. At ~5000ns of parse+plan overhead per call, a 1000-row batch pays
5ms just on the front end.

SQLite uses a prepared statement pattern: parse once, bind values, execute
N times. We need the same. Our `PlanCache` struct (`crates/query/src/plan_cache.rs`)
is a 100% lazy dead module — it's defined, it's tested, it's never called
from anywhere in the codebase. Wiring it is a one-day task.

### C5. PlanCache is dead code

Verified with grep: `plan_cache|PlanCache` has zero call sites in
`crates/query/src/executor.rs`. Every single PowQL query re-runs the full
lexer + parser + planner. For the plan-cache-friendly workloads
(repeated identical-structure queries), this is wasted.

A canonicalization step that replaces literals with `$1, $2, ...` before
hashing would give us SQLite-style prepared statements for free.

### C6. Per-query tracing overhead

`crates/query/src/executor.rs:38-77`:
```rust
let total_start = Instant::now();
let plan_start = Instant::now();
let plan = planner::plan(input)?;
let plan_us = plan_start.elapsed().as_micros();
let exec_start = Instant::now();
let result = self.execute_plan(&plan);
let exec_us = exec_start.elapsed().as_micros();
let total_us = total_start.elapsed().as_micros();
info!(query = %input, plan_us, exec_us, total_us, rows, "query ok");
debug!(plan = ?plan, "executed plan");
```

That's 4 `Instant::now()` calls + 3 `elapsed().as_micros()` + an `info!`
format cost + a `debug!` cost (which still runs the `?plan` Debug
formatter even if the level is disabled — tracing's field recording).
At ~355ns per point lookup, instrumentation is ~30-50% of measured time.

Fix: collapse to one `Instant::now()` (or zero, using a feature flag), emit
a single trace at the end, only format the plan if the level is enabled
via `tracing::enabled!(Level::DEBUG)`.

### C7. Insert path does not batch WAL writes

Even with Mission B's WAL wired, every insert is a separate `append` +
`flush`. For `insert_batch_1k` that's 1000 fsyncs. With
`Wal::batch_size = 64` the autoflush kicks in more reasonably, but the
hot path still does the full parse+plan per row.

The proper fix is a PowQL batch-insert syntax:
```powql
insert User [
  { name := "a", age := 30 },
  { name := "b", age := 31 },
]
```

Parse once, plan once, execute N inserts against the same table handle,
one WAL flush at the end.

### C8. `for_each_row_raw` sets up a fresh mmap per call

`crates/storage/src/heap.rs:179-203` shows `for_each_row` calling
`libc::mmap` + `libc::munmap` on every scan invocation, even when the
`HeapFile` already has a persistent `mmap_ptr` from `enable_mmap()`. The
persistent mmap is ONLY used by `get()` (point lookups), not by scans.

Every scan pays:
- mmap syscall (~5000ns)
- cold page cache (kernel has to re-populate)
- munmap syscall (~3000ns)

For bench workloads that scan the same table repeatedly, this is pure
waste. Reads should reuse the persistent `mmap_ptr`. Writes that
invalidate it should null it and let the next read re-map.

### C9. BTree leaf scan is linear, not binary search

`crates/storage/src/btree.rs:134`:
```rust
return keys.iter().position(|k| k == key).map(|i| values[i]);
```

`ORDER = 256` means each leaf has up to 256 keys. A linear scan averages
128 comparisons per leaf. A binary search is 8 comparisons. **This is
the per-row cost on every index update.** Insert/update/delete on an
indexed table pays this cost for every key touched.

Fix: `keys.binary_search(key)`.

Same on the internal nodes at line 138.

This is a READ-path fix (Mission D), but the write-path bench
(`update_by_pk`, `insert_single`) also goes through `btree.insert` and
`btree.delete` which both call similar linear scans — so fixing it
benefits writes too.

### C10. `Value` comparison is enum-dispatched

Every key comparison in the BTree goes through `PartialOrd for Value`
which matches on the enum discriminant + forwards to the inner type's
cmp. For an int-keyed index this is a 5-10ns cost per comparison × N
comparisons per lookup = real time.

Fix (long-form): specialize `BTree<K>` on key type so the hot path is a
raw `i64::cmp`. Short-form (Mission C): an `impl BTree` method
`lookup_int(&self, key: i64)` that skips the enum dispatch for the
common int case.

## Mission C work plan

Not all of these ship at once. The execution order is tuned to land the
biggest-ROI fixes first.

### Phase 1 — "stop the bleeding" (2-3 days)

The low-hanging perf fruit that requires NO architectural change:

1. **Binary search in BTree** (C9). `keys.binary_search(key)` replaces
   `keys.iter().position`. Expected impact: `point_lookup_indexed` drops
   from 354ns to ~150ns on M1 (beats SQLite). `update_by_pk` drops from
   18096ns to ~14000ns (not huge because the index is just a fraction of
   the per-op cost). Also Mission D.

2. **Delete buffer.rs + tx.rs + mvcc.rs** (292 + 157 + 49 = 498 LOC).
   Mission B also wants these gone. No perf impact, but reduces surface
   area for all the subsequent work.

3. **Collapse per-query tracing overhead** (C6). Single `Instant::now()`
   guarded by `tracing::enabled!(Level::INFO)`. Expected impact:
   `point_lookup_indexed` drops by ~50-100ns (20% improvement on the hot
   path for that workload).

4. **Fix `for_each_row_raw` to reuse persistent mmap** (C8). The heap
   should keep one mmap alive from `enable_mmap()` and scan from that
   pointer without re-mapping. The per-call mmap path becomes a fallback
   when `enable_mmap` hasn't been called. Expected impact: scans drop by
   a few microseconds of syscall overhead, biggest win on fast
   repetitive bench workloads.

5. **Add early-termination to `for_each_row_raw`** — signature changes to
   `FnMut(RowId, &[u8]) -> bool` where false stops the scan. Update all
   call sites to return true (no behavior change) except the LIMIT fast
   paths that return false after hitting their limit. Expected impact:
   `scan_filter_project_top100` drops from 44926ns to ~1000ns (45x
   speedup — the bench matches 100 rows out of a 100K fixture after only
   ~200 rows of scanning). Also Mission D.

### Phase 2 — "fused mutation" (3-5 days)

The structural fixes that rewrite the update/delete hot path. Depends on
Phase 1 landing so we have a clean slate.

6. **Fused in-place update fast path** (C1, C2, C3). New
   `PlanNode::UpdateFast { table, predicate, assignments }` that the
   planner folds from `Update(Filter(SeqScan))` when all assignment
   targets are fixed-size columns AND no assignment column is indexed.
   The executor:
   - Walks raw row bytes with `for_each_row_raw`
   - For each matching row:
     - Writes new values directly into the page buffer at fixed offsets
     - Marks the page dirty
     - Emits one WAL `Update` record with `(rid, [(col_idx, new_value)])`
   - Calls `disk.write_page` ONCE per modified page (not per row)
   - Calls `wal.flush` ONCE at the end of the statement

   Expected impact: `update_by_pk` drops from 18096ns to ~1500ns.
   `update_by_filter` drops from 943ms to ~30ms (a 30x improvement, which
   would put us within 40x of SQLite — not winning but no longer the
   laughable 1267x gap).

7. **Fused delete fast path**. Same pattern but for `Delete(Filter(SeqScan))`:
   walk raw bytes, mark matching slots as deleted in the page bitmap
   (no decode), batch one WAL delete per matched row, one page write
   per touched page. Expected impact: `delete_by_filter` drops from
   1388125ns to ~50000ns (28x improvement → within 4x of SQLite).

8. **Index key avoidance on unaffected columns**. When the updated row's
   indexed columns don't change, skip index work entirely. The
   generic path already needs this (it does full delete+insert from
   the index for every update) and the fast path needs to preserve it.

### Phase 3 — "prepared statements" (2-3 days)

The parse+plan overhead fix. Depends on Phase 1 + 2 landing (they change
the plan surface).

9. **Wire PlanCache into Engine** (C5). Add `Engine.plan_cache:
   Arc<RwLock<PlanCache>>`. `execute_powql` canonicalises the query text
   (strips literals), hashes, checks cache, uses cached plan if present.
   The cache stores the parameterised plan; at execute time the values
   are substituted from the literal list in the original query. Expected
   impact: `insert_single` drops from 6143ns to ~2500ns (parse+plan cost
   was ~3500ns of the hot path).

10. **PowQL batch-insert syntax** (C7). Parser addition:
    ```powql
    insert User [ { ... }, { ... }, { ... } ]
    ```
    Single parse, single plan, single WAL flush at the end of the
    statement, N heap insertions. Expected impact: `insert_batch_1k`
    drops from 6274ns/row to ~1200ns/row (5x improvement — within 4x
    of SQLite).

11. **Insert-path WAL grouping**. Inside a batch insert, append N insert
    records to the WAL buffer and flush once at commit. Set
    `Wal::batch_size` high enough that autoflush doesn't fire mid-batch.

### Phase 4 — "polish" (1-2 days)

12. **Int-specialised BTree lookup** (C10). `lookup_int(&self, k: i64)`
    that bypasses the Value enum dispatch for the common case. Minor
    win but cheap to land.

13. **Value::Int special-case in compile_predicate**. Predicates like
    `.age > 50` should become a direct `i64` comparison without any
    `Value` indirection. Partially done for the filter path; make sure
    the in-place update fast path uses the same compiled predicate.

## Exit criteria

Mission C is done when, on the wide bench:

- [ ] `insert_single` ≤ 3000ns (within 4x of SQLite 906ns, beating our 6143)
- [ ] `insert_batch_1k` ≤ 1500ns (within 5x of SQLite 301ns)
- [ ] `update_by_pk` ≤ 2000ns (within 5x of SQLite 393ns, vs our 18096)
- [ ] `update_by_filter` ≤ 50ms (within 70x of SQLite 0.74ms, vs our 0.94s)
- [ ] `delete_by_filter` ≤ 400000ns (within 2x of SQLite 234944ns, vs 1.39M)
- [ ] No read-side workload regresses more than 5% from the Mission B
      baseline
- [ ] `cargo bench -p powdb-bench && cargo run -p powdb-bench --bin compare`
      gate passes with rebaselined numbers
- [ ] Commit message trail explains each phase's before/after

**We don't promise to beat SQLite on writes.** SQLite's WAL mode is
15 years of tuning. Getting within 5x on single-op writes and within 70x
on update_by_filter would be a reasonable floor for a 3000-line database
— the claim then becomes "fast enough to not embarrass the thesis."

## Open questions

- **Do we need a real prepared-statement API at the wire protocol?** The
  PlanCache canonicalisation gives us 80% of the win without a protocol
  change. A real `Prepare`/`Execute` message pair would remove the
  canonicalisation CPU cost but adds protocol surface. Defer to Mission E.
- **Should Mission C add a real buffer pool?** No. mmap + OS page cache
  is simpler and as fast for our access patterns. `buffer.rs` goes in
  the trash.
- **How do we measure update_by_filter's per-row cost?** The compare
  bench reports full-op time. For profiling we need `criterion`
  measurements on smaller fixtures or a flamegraph on the 100K run.

## Second-pass audit findings (2026-04-07 storage deep-dive)

These were discovered after the original C1-C10 list. They join Mission
C's scope as C11-C18.

### C11. `Catalog::persist()` rewrites the entire catalog file per change

`crates/storage/src/catalog.rs:65-72,187`. Every `create_table()` calls
`persist()` which truncates and rewrites the full catalog file then
fsyncs. With N tables, this is O(N) work per table add. Insert hot
path doesn't trigger this directly, but Mission B's index persistence
does — it'd be tempting to call `catalog.persist()` after every
indexed insert. Don't.

**Fix:** append-only schema log, OR batch DDL into a single persist.
For Mission C the immediate fix is "don't call persist on the insert
hot path". Document the gap; full append-log is its own mission.

### C12. `HeapFile::allocate_page` extends the file 4KB at a time

`crates/storage/src/heap.rs` (search for `allocate_page`). Each new
page is a separate file extension. For an `insert_batch_1k` writing
into a fresh table, the heap allocates ~250 pages (4-row average per
page); that's 250 file-extend syscalls.

**Fix:** preallocate in 1MB chunks via `ftruncate`. Track high-water
mark separately from file size. Compounds with C7 (WAL grouping) for
the bulk-load path.

**Expected impact:** `insert_batch_1k` -200ns/row.

### C13. `disk.write_page` is unbuffered pwrite per call

`crates/storage/src/disk.rs:41-46`. Every `write_page` is a syscall.
Mission C #6 (fused in-place update) writes once per modified page
(not per row), but the per-page write is still a raw syscall.

**Fix:** add a write coalescer. Maintain a small dirty-page buffer in
DiskManager (16 entries); flush on overflow or on `flush_all()`. WAL
flushes (Mission B) become `flush_all + wal.flush + disk.fsync`.

**Expected impact:** combines with C7 — bulk loads pay one fsync
instead of N.

### C14. Index insert clones the key Value twice

`crates/storage/src/table.rs:32-44` calls `btree.insert(values[idx].clone(), rid)`
for every indexed column. The BTree's split logic at `btree.rs:83,99`
clones the mid_key TWICE during a node split. For a String-keyed
index, that's 3+ heap allocations per insert.

**Fix:**
1. `Table::insert` should pass an owned Value (move, not clone) when it
   knows the input row is being consumed.
2. `BTree::insert` split logic should `mem::take` instead of cloning.

**Expected impact:** `insert_single` on a String-indexed table -300ns.
Compounds with the binary-search BTree fix.

### C15. `Value` enum is 32 bytes; row encoding allocates 4-6 intermediate Vecs

`crates/storage/src/types.rs` and `crates/storage/src/row.rs:54`. Per-row
encode allocates separate Vecs for `fixed_buf, var_data, null_bitmap,
var_offsets`. For `insert_batch_1k` that's 4-6K malloc calls. Also,
`Value` is 32 bytes; a 10-column row materialised as `Vec<Value>` is
320 bytes — three M1 cache lines per row.

**Fix:**
- Single shared `RowEncodeBuf` reused across inserts in a batch.
  Stack-allocate the 4 buffers as fixed-size arrays for the common case
  (≤16 columns) with a Vec fallback.
- For `var_offsets` specifically, use `SmallVec<[u16; 16]>` so the
  common case never touches the heap.

**Expected impact:** `insert_batch_1k` -800ns/row. Biggest single win
in the second-pass audit.

### C16. `decode_column` for Str/Bytes uses `from_utf8_lossy().into_owned()`

`crates/storage/src/row.rs:201,289`. Every String column decode allocates
a fresh `String` via lossy UTF-8, even when the column might be discarded
(e.g., the predicate column in a Filter that never gets projected). The
fast path D4 (Project(Filter(SeqScan)) without limit) avoids this for
projected columns, but the slow paths still hit it.

**Fix:**
1. For predicate evaluation that already uses `compile_str_eq_leaf`, the
   raw byte slice never goes through `from_utf8_lossy` — that's already
   fast. The leak is in fallback paths that hit `decode_selective`.
2. Make `decode_column` return `Cow<'static, str>` or accept a
   `&mut String` reused across rows.
3. Do NOT validate UTF-8 unless the consumer needs a `&str`. The
   slot data is byte-oriented; UTF-8 validation can be deferred to
   the wire layer.

**Expected impact:** filter+project workloads with String columns
-30% on the slow path. Mostly a hardening fix for non-fast-path queries.

### C17. `for_each_row_raw` walks slot bitmap one byte at a time

Already noted in Mission D D8. Listed here for write context: when a
fused update fast path lands (Mission C #6), it walks the same slot
iterator. The bitmap-skipping optimisation accelerates BOTH read and
write fast paths.

### C18. `generic_rid_match` is O(N×M)

`crates/query/src/executor.rs:954-957`. The fallback mutation path
materialises the query result (N rows), then scans the table (M rows),
doing `rows.iter().any(|r| r == row)` per matched candidate. This is
PROBABLY the smoking gun for `update_by_filter` at 1267x slower:
the executor falls into the generic path for the bench's
`User filter .age > 50 update { status := "senior" }` shape.

Mission C #6 (fused update fast path) replaces this entirely for the
bench shape. But the generic fallback should still get a fix:

**Fix:**
1. The generic mutation path should use a `HashSet<RowId>` instead of
   pairwise row equality.
2. Even better: when the input contains a SeqScan + arbitrary filter,
   walk the raw bytes once with a (possibly slow) decoded predicate,
   collecting RIDs directly. No double walk.

**Expected impact:** the fused fast path ships in C #6 and skips this
entirely. The generic fallback fix is "defense in depth" so future
language features that don't have a fast path don't get this O(N²)
disaster.

## Related missions

- Mission B wired the WAL and the concurrency seam — Mission C lives on
  top of that.
- Mission D closes the remaining 4 read losses — some of the fixes
  overlap (BTree binary search, mmap reuse, early termination).
- Mission E adds language features — batch insert, prepared statements,
  BEGIN/COMMIT are partially covered here but the parser/protocol surface
  belongs to E.
- Mission F (build profile) lands BEFORE Mission C so the LTO and
  inlining gains compound with the structural fixes.
