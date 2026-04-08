# Mission D — Read-path performance

**Status:** design draft (2026-04-07)
**Depends on:** Mission B (no architectural depend, but lands after for clean diff)
**Overlaps with:** Mission C (BTree fix, mmap reuse, tracing collapse, PlanCache wiring)
**Goal:** close the 4 read losses and convert read wins into "decisive wins"

## Problem

Per `docs/benchmarks/2026-04-07-wide-bench-snapshot.md`, PowDB loses 4 read
workloads to SQLite despite the thesis claim of 22-42x speedups from
removing SQL translation. The losses are not large in absolute time but
they undermine the central claim:

| workload                     | PowDB | SQLite | ratio | verdict |
| ---                          | ---:  | ---:   | ---:  | ---     |
| point_lookup_indexed         |   355 |    263 |  0.74x | LOSS (1.35x slower) |
| point_lookup_nonindexed      |  52070|  47709 |  0.92x | LOSS (1.09x slower) |
| scan_filter_project_top100   | 44927 |  12309 |  0.27x | **LOSS (3.65x slower)** |
| multi_col_and_filter         | 646239| 463459 |  0.72x | LOSS (1.39x slower) |

The `scan_filter_project_top100` regression is the loud one — three and a
half times slower than SQLite on a workload where we should be winning by
3-4x like every other scan. The other three are tighter but they share a
common cause: SQLite's `prepare_cached` amortises away the parse cost, and
PowDB has nothing equivalent.

## Root causes (verified against current code)

### D1. BTree leaf and internal nodes do linear scan

`crates/storage/src/btree.rs:134, 138`:
```rust
return keys.iter().position(|k| k == key).map(|i| values[i]);
//                ^^^^^^^^^ linear scan over up to 256 keys

let pos = keys.iter().position(|k| key < k).unwrap_or(keys.len());
//                ^^^^^^^^^ linear scan over up to 256 keys per internal node
```

`ORDER = 256`. Average leaf comparison count is 128. A 100K-row table
with `ORDER=256` is a tree of ~3 levels (256³ = 16M). That's 3 internal
+ 1 leaf = 4 linear scans averaging ~512 comparisons total per lookup.
Each comparison is `Value::cmp` which dispatches on the enum
discriminant.

A binary search is 8 comparisons per node, 32 total. The savings
compound with the enum-dispatch fix in D7.

**Fix:** `keys.binary_search(key)` for both internal and leaf. The
`Value` ordering already implements `Ord` so `binary_search` works
out of the box.

**Expected impact:** `point_lookup_indexed` drops from 355ns to ~150ns
(beats SQLite's 263ns by ~1.7x). Index-mediated update/delete also
benefits (Mission C secondary effect).

### D2. `for_each_row_raw` cannot terminate early

`crates/storage/src/heap.rs:169-218`:
```rust
pub fn for_each_row<F: FnMut(RowId, &[u8])>(&self, mut callback: F)
    -> io::Result<()>
{
    // ... mmap setup ...
    for slot_idx in 0..n_slots {
        // ... offset/len decoding ...
        callback(slot_idx as RowId, row_data);
    }
    // ... munmap ...
}
```

The callback returns `()`. `project_filter_limit_fast` uses a `done`
flag to early-return inside the closure (`crates/query/src/executor.rs:695`):
```rust
self.catalog.for_each_row_raw(table, |_rid, data| {
    if done { return; }
    // ...
    if out.len() >= limit { done = true; }
}).map_err(|e| e.to_string())?;
```

But `for_each_row` keeps iterating every remaining slot, calling the
no-op closure 100K times. **The `done` flag is a no-op for the loop.**

For `scan_filter_project_top100` on the bench (100 matching rows out of
100K, hit at row ~200): we do **99,800 wasted closure invocations** plus
all the slot bitmap reads.

**Fix:** Change the callback signature to `FnMut(RowId, &[u8]) -> bool`
where `false` stops the loop. Update all call sites:
- Filter+SeqScan callers: always return `true`
- LIMIT-bearing fast paths: return `false` when limit hit

For ergonomic compatibility, keep the existing name as a wrapper that
ignores the bool, and add a new `for_each_row_until` (or just bite the
bullet and do the breaking change — there are <20 call sites).

**Expected impact:** `scan_filter_project_top100` drops from 44,927ns to
~1,200ns (37x improvement). This is the single biggest unforced error
in the read path.

### D3. `compile_predicate` AND chains are nested `Box<dyn Fn>`

`crates/query/src/executor.rs:1102-1106`:
```rust
Expr::BinaryOp(left, BinOp::And, right) => {
    let l = compile_predicate(left, columns, layout, schema)?;
    let r = compile_predicate(right, columns, layout, schema)?;
    Some(Box::new(move |data| l(data) && r(data)))
}
```

For `multi_col_and_filter` (`age > 50 AND status = "senior"`), the
compiled predicate is `Box<dyn Fn>` wrapping two more `Box<dyn Fn>`.
Each call dispatches three virtual calls and three closure activations
per row. Over 100K rows that's 300K virtual calls.

A 3-clause AND nests four levels deep. For pathological filter chains
the dispatch cost dominates the actual byte comparisons.

**Fix:** Add a flat AND form. `compile_predicate` collects all leaves
of an AND tree into a `Vec<CompiledLeaf>` and emits one closure that
loops:
```rust
Some(Box::new(move |data| {
    for leaf in &leaves {
        if !leaf(data) { return false; }
    }
    true
}))
```

A `Vec<Box<dyn Fn>>` still has dynamic dispatch but only one level —
no closure-of-closure boxing. Bonus: branch prediction sees the same
loop body every iteration, and short-circuiting still works.

**Expected impact:** `multi_col_and_filter` drops from 646,239ns to
~400,000ns (matching SQLite's 463,459ns — going from a 1.39x loss to a
~1.15x win).

### D4. Filter+SeqScan decodes the FULL row even when caller will project

`crates/query/src/executor.rs:109-114`:
```rust
if let Some(compiled) = compile_predicate(predicate, &columns, &fast, &schema) {
    self.catalog.for_each_row_raw(table, |_rid, data| {
        if compiled(data) {
            rows.push(decode_row(&schema, data));   // ← full decode
        }
    })...
}
```

`decode_row` allocates a `Vec<Value>` with one entry per column,
including String columns which require UTF-8 validation and a heap
allocation per string. For `multi_col_and_filter` which selects
`name, age` but keeps the predicate-matching rows AS FULL ROWS until
the consuming `Project` pass strips them, we're allocating
`status, email, created_at` on every match for nothing.

**Fix:** When `Filter(SeqScan)` is wrapped in a `Project`, call the
fused `project_filter_limit_fast` with `limit = usize::MAX`. The
existing fast path already handles this — but the planner currently
only triggers it when there's a `Limit`. The check can extend to
`Project(Filter(SeqScan))` (no limit). Pass `usize::MAX` as the limit
so the early-termination optimisation is a no-op but the per-row
projection-only decode kicks in.

**Expected impact:** `multi_col_and_filter` drops further (combines
with D3) — likely to ~300,000ns, beating SQLite on this workload by
~1.5x.

### D5. `for_each_row_raw` re-mmaps per call (also Mission C C8)

`crates/storage/src/heap.rs:179-203` calls `libc::mmap` and `libc::munmap`
on every scan, even though `enable_mmap()` already set up a persistent
`mmap_ptr` field. The persistent mmap is only consumed by `get()`.

**Fix:** Reuse `self.mmap_ptr` if non-null. Remap on:
- File grew since last enable_mmap (track with `mmap_len < file_len`)
- Insert/delete invalidated the layout (set `mmap_ptr = ptr::null_mut()`,
  next read re-enables)

**Expected impact:** ~5-10μs saved per scan (mmap+munmap syscall cost).
For workloads that scan repeatedly (point_lookup_nonindexed, every read
bench), this reclaims a measurable chunk. `point_lookup_nonindexed`
could drop from 52070ns to ~46000ns (within 1% of SQLite).

### D6. Per-query tracing overhead (also Mission C C6)

`crates/query/src/executor.rs:38-77` runs 4 `Instant::now()` and 3
`elapsed().as_micros()` calls per query, then formats an `info!` line
and a `debug!` with `?plan` (Debug formatter runs even at info level
because tracing's field recording).

For `point_lookup_indexed` at 355ns total, instrumentation is 100ns+.

**Fix:** Single `Instant::now()` guarded by
`tracing::enabled!(Level::INFO)`. Remove the `debug!(?plan)` (or guard
with `enabled!(Level::DEBUG)` and switch to `tracing::field::display`
to defer formatting).

**Expected impact:** `point_lookup_indexed` drops by ~50-100ns (combines
with D1 to put us comfortably under SQLite).

### D7. `Value::cmp` enum dispatch in BTree key compare

Even after binary search lands, every comparison is
`<Value as Ord>::cmp` which matches on the discriminant and forwards.
For an int-keyed index the dispatch eats 5-10ns per comparison. With
binary search (32 comparisons per lookup) that's still 160-320ns of
pure dispatch.

**Fix:** Specialised `BTree::lookup_int(&self, key: i64) -> Option<RowId>`
that bypasses Value at the leaf level. The internal nodes can stay
generic since they're hit only ~3 times per lookup, but the leaf scan
becomes a raw `i64::cmp`.

Alternative: a `KeyKind` enum check at lookup-start that picks a
specialised loop. Rust's enum-of-functions optimisation is good enough
that the dispatch becomes a single branch.

Mission C #12 lists this as a polish item; Mission D wants it earlier
because it's the difference between "we beat SQLite" and "we tie".

**Expected impact:** `point_lookup_indexed` drops by ~50ns more
(combined with D1+D6 → ~120-150ns total, **2x faster** than SQLite).

### D8. Slot iteration in `for_each_row` reads bitmap byte by byte

`crates/storage/src/heap.rs` walks every slot and reads each slot's
header to decide whether to call the callback. For a table with many
deleted slots this is wasted.

The slotted-page layout has a delete bitmap. We can iterate populated
slots only by walking the bitmap and skipping zero bytes (each byte
covers 8 slots; `u64::from_le_bytes` covers 64 slots).

**Fix:** `for_each_row_raw` iterates the page slot bitmap with
`u64::trailing_zeros` to skip dead slots in groups. The cost saving is
proportional to deletion fraction; for a fresh fixture it's a wash, but
the bench harness deletes 50% of rows in `delete_by_filter` and then
re-runs reads — there the saving is real.

**Expected impact:** marginal on the current bench (no deletes between
fixture and reads), but it's a correctness-friendly optimisation that
becomes valuable as soon as workloads include real DELETE traffic.

### D9. PlanCache wiring (also Mission C C5)

Already documented. PowDB re-parses and re-plans every single query,
including `point_lookup_indexed` which the bench harness executes
thousands of times in a row. SQLite's `prepare_cached` makes its
parse-cost ~zero across iterations.

**Fix:** Wire `PlanCache` (`crates/query/src/plan_cache.rs`) into
`Engine`. Canonicalise query text by replacing literals with `$1, $2, ...`
before hashing. On cache hit, substitute the literal list at execute
time without re-running the lexer/parser/planner.

**Expected impact:** `point_lookup_indexed` drops by ~3000ns (the
parse+plan cost we measured when removing tracing). Once D1+D6+D7+D9
all land, point_lookup_indexed sits at ~150ns and we own this workload.

### D10. `point_lookup_nonindexed` is a full SeqScan with no early-return

`compare/src/engines/powdb.rs` for non-indexed lookup:
```rust
let q = format!("user_table filter .created_at = {ts} project { name } limit 1");
```

That's `Project(Limit(Filter(SeqScan)))`. After D2 lands the early
termination, it stops at the first match. Without D2 the scan walks
all 100K rows even though only one matches.

**Fix:** D2 is the fix. No new work.

**Expected impact:** point_lookup_nonindexed drops from 52,070ns to
~25,000ns (the bench's matching row is ~halfway through the table on
average). This converts the loss into a 2x WIN over SQLite.

## Mission D work plan

The fixes are tightly interleaved with Mission C — most of Phase 1 is
shared with Mission C Phase 1. The order here optimises for *commits
that move bench numbers*, not for theoretical purity.

### Phase 1 — share with Mission C Phase 1 (1-2 days)

These ship in the same diffs as Mission C and are credited to both
missions in the commit messages.

1. **D1: Binary search in BTree.** `keys.binary_search(key)` at line
   134 and 138. Add tests for empty leaf, single-key leaf, and
   key-not-present. **Impact:** point_lookup_indexed -200ns,
   update_by_pk -1500ns, all index updates faster.

2. **D2: Early termination in `for_each_row_raw`.** Signature change
   from `FnMut(RowId, &[u8])` to `FnMut(RowId, &[u8]) -> bool`.
   Update all call sites; compile errors will guide. The LIMIT fast
   paths return `false` when their counter is hit.
   **Impact:** scan_filter_project_top100 -43000ns,
   point_lookup_nonindexed -25000ns.

3. **D5: Persistent mmap reuse.** `for_each_row_raw` checks
   `self.mmap_ptr.is_null()` first; if non-null, reuse. Writes that
   modify file size set `self.mmap_ptr = null_mut()` to force re-enable
   on next read.
   **Impact:** all scans -5000ns syscall cost.

4. **D6: Collapse per-query tracing.** Single `Instant::now()` guarded
   by `tracing::enabled!(Level::INFO)`. Remove `debug!(?plan)` line.
   **Impact:** point_lookup_indexed -100ns.

### Phase 2 — Mission D specific (1-2 days)

5. **D3: Flat AND in `compile_predicate`.** Walk the BinaryOp(And)
   tree, collect leaves into a `Vec<CompiledLeaf>`, emit one closure.
   **Impact:** multi_col_and_filter -200000ns.

6. **D4: Project(Filter(SeqScan)) without Limit fast path.** Extend
   the `project_filter_limit_fast` trigger to fire on `Project(Filter(SeqScan))`
   as well as `Project(Limit(Filter(SeqScan)))`, with `limit =
   usize::MAX`. The early-termination check becomes a no-op but the
   projected-decode kicks in.
   **Impact:** multi_col_and_filter -100000ns.

7. **D9: Wire PlanCache.** Canonicalisation + hash + cache lookup +
   plan substitution. Same task as Mission C #9.
   **Impact:** point_lookup_indexed -3000ns (relative to parse cost
   of ~3500ns/query). Note: this is the BIG win for the read path.

### Phase 3 — read-side polish (1 day)

8. **D7: `lookup_int` BTree specialisation.** Bypass Value enum at the
   leaf scan. Reuse the existing tree walk; only the comparison
   changes.
   **Impact:** point_lookup_indexed -50ns. Pushes us clearly past
   SQLite.

9. **D8: Bitmap-skipping slot iteration.** `u64::trailing_zeros` walk
   over the slot bitmap. Adds a code path for "dense slot range" that
   the existing scan still hits when the bitmap is all-ones.
   **Impact:** marginal on current fixtures but correctness-friendly
   for delete-heavy workloads.

## Exit criteria

Mission D is done when, on the wide bench:

- [ ] `point_lookup_indexed` ≤ 200ns (was 355ns; SQLite 263ns) — **WIN**
- [ ] `point_lookup_nonindexed` ≤ 30000ns (was 52070ns; SQLite 47709ns) — **WIN**
- [ ] `scan_filter_project_top100` ≤ 2000ns (was 44927ns; SQLite 12309ns) — **WIN by 6x**
- [ ] `multi_col_and_filter` ≤ 350000ns (was 646239ns; SQLite 463459ns) — **WIN**
- [ ] All previously-winning read workloads stay at or above their
      Mission A baseline (no read regressions)
- [ ] `cargo bench -p powdb-bench && cargo run -p powdb-compare --release`
      gate passes with rebaselined numbers
- [ ] BTree binary-search test coverage: empty leaf, single-key,
      key-not-present, key-at-min, key-at-max, key-just-below-min,
      key-just-above-max
- [ ] PlanCache hit-rate metric exposed (Engine.plan_cache_hits /
      plan_cache_misses) so we can verify the hot path is actually
      using the cache

## Read-path cleanups (out of scope but documented)

These don't move bench numbers but reduce surface area:

- **Drop the `done` flag from `project_filter_limit_fast`** once D2
  lands. The `return false` from the callback handles termination
  cleanly.
- **Inline `decode_column` for fixed-size int/datetime/float types**
  via `#[inline(always)]`. The current decode path branches on
  `TypeId` per call; the planner could generate a per-column
  decoder closure at plan time and we'd skip the branch.
- **Reduce `scan` allocations.** `Catalog::scan` currently materialises
  `Vec<Value>` rows even for callers that immediately re-encode (the
  generic `PlanNode::SeqScan` arm at executor.rs:84-91). The fast
  paths bypass it but the generic path stays as a fallback for shapes
  the planner doesn't fold.

## Open questions

- **Should `project_filter_limit_fast` also handle expression
  projections?** Currently it bails on anything that isn't a simple
  `Expr::Field`. Adding `Expr::Literal` and `Expr::BinaryOp` (constant
  fold) would let `count(*)` and similar through. Defer to Mission E
  (language features).
- **Do we need a column statistics layer?** SQLite's planner uses
  table statistics to pick join order and index usage. PowDB has no
  joins yet (Mission E), but `multi_col_and_filter` would benefit
  from picking the more selective predicate first. Defer.
- **Is `compile_predicate` worth a real codegen pass?** With cranelift
  or LLVM we could JIT the predicate to native code and skip closure
  dispatch entirely. The current bench gap doesn't justify the
  complexity, but if we ever try to claim "fastest predicate eval on
  earth" this is the next step.

## Second-pass audit findings (2026-04-07 query deep-dive)

These join Mission D as D11-D18, discovered after the original D1-D10
list.

### D11. `Aggregate` min/max materialises `Vec<&Value>` then clones

`crates/query/src/executor.rs:414-419`. The min/max aggregate path
allocates a `Vec<&Value>` of references then `.cloned()` the winner. For
`agg_min` / `agg_max` over a String column this is one wasted alloc per
result, plus a clone of the winning string.

**Fix:** `rows.iter().min_by(|a, b| a[idx].cmp(&b[idx])).cloned()` —
no intermediate Vec.

**Expected impact:** -50ns on min/max workloads. We win these by 4x
already; this widens the lead.

### D12. `Offset` arm always materialises and skips

`crates/query/src/executor.rs:295-307`. `OFFSET 1000000 LIMIT 10` walks
all 1M rows into a `Vec<Vec<Value>>`, skips them, then takes 10. No
wide-bench workload uses Offset, but real-world pagination does.

**Fix:** add a slot-counting fast path that uses
`for_each_row_raw` + a counter, only decoding rows after the offset is
hit. Combined with D2 (early termination), this becomes the same
machinery the LIMIT fast path already uses.

**Expected impact:** future-proof — the bench doesn't measure this but
real apps will.

### D13. Unbounded `Sort` always materialises

`crates/query/src/executor.rs:265-279`. `Sort` without a wrapping
`Limit` runs `execute_plan(input)` and `rows.sort_by`. The bounded-heap
fast path only triggers for `Sort + Limit + Project`. Standalone
`Sort` is dead-naive.

**Fix:** for `Sort` without limit, the result IS the full result, so
materialisation is mandatory. But the fast path can still be
column-selective: read only the sort key from raw bytes, sort indices,
then decode the projected columns at the end. This avoids decoding
columns that aren't projected.

**Expected impact:** moderate on `User order .age { name }` patterns.

### D14. Schema lookups + column-name vec rebuilt 12+ times per query

`crates/query/src/executor.rs:82-84,99-101,145-147,323-325` (pattern
repeats throughout). Each fast path arm clones the schema and rebuilds
`columns: Vec<String>` via `schema.columns.iter().map(|c| c.name.clone()).collect()`.
For complex queries like `Project(Limit(Sort(Filter(SeqScan))))` this
runs 4-5 times per query.

**Fix:** Cache the column-name `Vec<String>` and `FastLayout` per
`Schema` inside the catalog. They're immutable once a table exists.
Hand them out as `Arc<Vec<String>>` and `Arc<FastLayout>` so the
fast paths don't allocate.

**Expected impact:** `point_lookup_indexed` -50ns. Compounds with D9
(PlanCache) — once the plan is cached, the cached plan can carry
references to the cached layouts.

### D15. `decode_selective` allocates `vec![Value::Empty; n_cols]`

`crates/query/src/executor.rs:1272-1280`. For a 20-column table with a
2-column filter, allocates a 20-slot Vec of `Value::Empty` per row, then
fills only the 2 needed slots. The other 18 slots are just dead memory.

**Fix:** Return `SmallVec<[(usize, Value); 4]>` of (col_idx, value)
pairs. Or better, since the predicate evaluator already knows which
columns it needs, pass it the bytes directly and skip the intermediate
representation entirely.

**Expected impact:** -10% on the slow path (when compile_predicate
fails). Hardening; the fast path never goes here after Mission D D3.

### D16. `generic_rid_match` is O(N×M) — same as Mission C C18

The mutation fallback path materialises the query result then scans
the table doing per-row equality. Mission D doesn't write so this
doesn't directly affect read perf, but `collect_rids_for_mutation`
calls into it for unsupported plan shapes — and a malformed plan can
silently fall into the slow path. The fix is in Mission C C18.

### D17. `QueryResult::Rows` always materialises — no streaming variant

`crates/query/src/result.rs:6-13`. All results are `Vec<Vec<Value>>`
collected in memory. For a 100K-row scan returning to a server client,
the server allocates 100K rows before sending the first byte. The
wire layer at `crates/server/src/handler.rs:62-79` then transcodes
them to the protocol format with another full allocation pass.

**Fix:** Add `QueryResult::RowStream { columns, iter: Box<dyn Iterator<Item=Row>> }`.
The wire layer drains the iterator and writes rows in chunks. The
in-process bench keeps using `Vec<Vec<Value>>` for compatibility.

**Expected impact:** unblocks future server-side streaming workloads.
Doesn't help current bench numbers because the bench is in-process.
But it's a structural cleanup that prevents the wire-format perf
problems documented in F2's audit (per-message frame allocation,
per-cell `to_string`).

### D18. The wire layer calls `.to_string()` per cell

Documented in the second-pass server audit at
`crates/server/src/handler.rs:105-118`. For a 1000-row result with 6
columns, `value_to_display()` allocates 6000 strings. Combined with
D17 (no streaming), the server materialises everything twice.

**Fix:** ships with D17. The streaming variant emits raw bytes per
column directly into the wire protocol's buffer; no per-cell `String`.

**Expected impact:** the wire layer is not on the current bench critical
path (the bench is in-process). But for any real client this is the
biggest wall between PowDB-the-engine and PowDB-the-server.

## Related missions

- **Mission B** wires the WAL — Mission D lands after B so the read
  path doesn't change while durability is being added.
- **Mission C** is the write-path twin of D. They share Phase 1 (BTree
  binary search, mmap reuse, tracing collapse, PlanCache). Land them
  in a coordinated set of commits.
- **Mission E** adds language features (joins, GROUP BY, prepared
  statements at the wire) — many of these surface new read patterns
  that Mission D's optimisations should still cover.
- **Mission F** (build profile) lands BEFORE Mission D so the LTO and
  inlining gains compound with D1 (binary search), D5 (mmap reuse),
  and D7 (int-specialised lookup).
