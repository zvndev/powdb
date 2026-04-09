# Mission F — Build profile and dependency quick wins

**Status:** design draft (2026-04-07)
**Depends on:** nothing
**Blocks:** nothing — but should land FIRST (before Missions B/C/D) so the
gains compound with every other optimisation
**Estimated effort:** 1-2 hours
**Estimated impact:** 17-39% across-the-board speedup, zero behavioural risk

## Why this is its own mission

Mission F is the "free money" mission. Six independent fixes in the build
config and dependency choices, each touching <30 lines of code, each
delivering measurable perf, NONE of them changing observable behaviour
beyond "it goes faster".

These fixes were missed in Mission A because they live in `Cargo.toml`
files that nobody profiled. The 2026-04-07 second-pass audit found them
all in one parallel investigation.

The reason to ship Mission F before B/C/D is **measurement validity**. If
we land Missions B-D's structural fixes against the current default-build
binary, then enable LTO at the end, the gains attributed to each mission
will be muddled — some of B's "improvement" will actually be LTO. Land
Mission F first, rebaseline the bench, then measure B/C/D against the
optimised baseline.

## The findings

### F1. No `[profile.release]` config in workspace `Cargo.toml`

The workspace `Cargo.toml` has zero release-profile overrides. Defaults:
- LTO = `false`
- codegen-units = 16
- panic = `unwind`
- opt-level = 3

**Fix:** Add to workspace `Cargo.toml`:
```toml
[profile.release]
lto = true
codegen-units = 1
panic = "abort"
opt-level = 3
debug = false
strip = "symbols"
```

LTO is the load-bearing change. Without LTO, cross-crate calls (executor →
storage, executor → query AST) cannot inline. With ORDER=256 binary
search, mmap reads, and compile_predicate closures all crossing crate
boundaries, this is a 5-15% sitting-duck loss.

`codegen-units = 1` further serialises codegen but trades compile time
for ~2-4% extra runtime perf.

`panic = "abort"` removes unwinding tables — smaller binary, faster
hot-path branches because the compiler doesn't need to maintain
landing pads.

**Risk:** with `panic = "abort"`, panics terminate the process. The
server's tokio task-level panic recovery (if any) becomes
moot — investigate `crates/server/src/handler.rs` to confirm there's
no `catch_unwind` we'd be breaking. (Spoiler from the audit: there
isn't.)

### F2. No `.cargo/config.toml` with `target-cpu=native`

M1/Apple Silicon binaries don't use NEON or M1-specific instructions
without explicit `target-cpu`. The `crc32fast` crate (used by WAL
records in Mission B) ships SIMD acceleration that's gated on the
target CPU.

**Fix:** Create `.cargo/config.toml`:
```toml
[build]
rustflags = ["-C", "target-cpu=native"]
```

**Risk:** the produced binary won't run on CPUs that lack the features
the build machine has. For a thesis project running on M1 → M1, zero
risk. For CI: the GitHub Actions runner is x86_64 — make sure CI uses
its own profile (or omit target-cpu in CI). Add a comment to
`.cargo/config.toml` explaining this.

**Estimated impact:** 3-8% on tight loops; bigger on the WAL CRC path
because crc32fast actually uses SIMD when allowed.

### F3. `tokio = "1", features = ["full"]` in powdb-server

`crates/server/Cargo.toml:9` pulls in all ~30 tokio features. The server
uses a small subset.

**Fix:** Replace with:
```toml
tokio = { version = "1", features = [
  "rt-multi-thread", "net", "io-util", "macros", "sync", "time"
] }
```

**Risk:** if the handler uses something like `tokio::process` or
`tokio::signal`, the build breaks. Easy to find by adding features
back as compile errors guide. Re-run tests after.

**Estimated impact:** smaller binary, faster compile. Negligible runtime
benefit (the unused features sit cold). Worth it for hygiene + cleaner
linker output.

### F4. `std::HashMap` everywhere — should be `FxHashMap` for hot maps

`SipHash` is the std default — DoS-resistant but 2-3x slower than FxHash
for non-adversarial keys. PowDB hot-path maps are NEVER adversarial:

- `crates/storage/src/catalog.rs:3` — `HashMap<String, Table>` (table
  lookups on every query)
- `crates/storage/src/table.rs` — `HashMap<String, BTree>` (index
  lookups on every insert/update)
- `crates/query/src/plan_cache.rs:5` — `HashMap<u64, PlanNode>` (every
  cached query lookup, once Mission C #9 wires it)

**Fix:** Add `rustc-hash = "2"` to the workspace deps and:
```rust
use rustc_hash::FxHashMap;
// HashMap → FxHashMap, HashMap::new() → FxHashMap::default()
```

**Risk:** none. FxHash is a drop-in replacement.

**Estimated impact:** 2-4% on point lookups (where the per-query table+index
name lookup is a measurable fraction of total time). Compounds with
Mission D D9 (PlanCache wiring) since the cache itself uses HashMap.

### F5. Hot functions lack `#[inline]` hints

The audit found 0 `#[inline]` annotations in `crates/storage/src/` and
only 2 in `crates/query/src/executor.rs` (1768 LOC). Without LTO,
cross-crate calls can't inline — and even WITH LTO, marking small hot
functions explicitly helps.

**Fix:** Add `#[inline]` (or `#[inline(always)]` for the tightest paths)
to:
- `decode_row`, `decode_column`, `decode_selective` in `crates/query/src/executor.rs`
  (or wherever the row decoder lives)
- `compile_predicate` leaf closures' inner read functions
- `BTree::lookup` and the new `binary_search` path
- `Heap::get` (mmap point lookup)
- Page slot reading helpers in `crates/storage/src/page.rs`
- `Value::cmp` impl (or replace with the int-specialised lookup from
  Mission D D7)

**Risk:** binary size grows slightly. With `lto = true` and
`codegen-units = 1` the linker drops cold copies anyway.

**Estimated impact:** 5-8% on point lookups and tight scans. Compounds
with F1 (LTO). Mission C/D fast paths benefit the most.

### F6. `Vec::new()` instead of `Vec::with_capacity()` in scan loops

`crates/query/src/executor.rs:105` (and similar):
```rust
let mut rows: Vec<Vec<Value>> = Vec::new();
self.catalog.for_each_row_raw(table, |_rid, data| {
    if compiled(data) { rows.push(decode_row(...)); }
});
```

For a 100K-row scan that returns 30K matches, `rows` resizes ~17 times
(doubling from 0 → 1 → 2 → 4 → 8 → ... → 32768), each resize allocates
+ memcpys.

**Fix:** Pre-allocate with a heuristic:
```rust
let estimated = self.catalog.row_count(table).unwrap_or(1024);
let mut rows: Vec<Vec<Value>> = Vec::with_capacity(estimated.min(16384));
```

For LIMIT-bearing fast paths, use the limit directly:
`Vec::with_capacity(limit.min(1024))` (already done in
`project_filter_limit_fast` at executor.rs:693, but missing in the
generic Filter+SeqScan arm).

**Risk:** zero.

**Estimated impact:** 1-2% on scans, more on big result sets.

## Implementation order

1. **F1 first** (workspace profile.release). Touches one file. Run
   `cargo bench -p powdb-bench` immediately after to get the new
   baseline.
2. **F2 second** (.cargo/config.toml). Touches one new file. Bench again.
3. **F4 + F6 batch** (FxHashMap + with_capacity). Touches a handful of
   files. Bench.
4. **F3** (tokio features). Touches one file. Run server tests.
5. **F5** (#[inline] hints). Touches the most files but is purely
   additive. Bench last to attribute the gain to inlining specifically.

After all 6, run the wide bench (`cargo run -p powdb-compare --release`)
and capture the new snapshot to `docs/benchmarks/2026-04-NN-after-mission-f.md`.
Update `crates/bench/baseline/main.json` to reflect the new floor —
otherwise the regression gate will reject every subsequent change.

## Exit criteria

- [ ] All 6 fixes landed
- [ ] `cargo bench -p powdb-bench` baseline rewritten
- [ ] `cargo run -p powdb-compare --release` snapshot captured
- [ ] Wide bench shows ≥5% improvement on at least 8 of 15 workloads
      (sanity check that the build flags actually moved numbers)
- [ ] CI build still passes (verify target-cpu=native is gated for
      non-host builds)
- [ ] Server tests still pass after tokio feature trim
- [ ] No behavioural test failures

## Why these aren't in Missions B/C/D

The B/C/D missions are *structural* — they change algorithms, data
structures, and execution paths. Mission F is *configurational* — it
changes nothing about the code's logic, only how it's compiled.

Mixing them in the same commit set would muddy the perf attribution. By
landing F first as its own commit set, every subsequent mission's bench
delta is clearly attributable to that mission's structural change.

## Related missions

- **Mission B**: WAL CRC path benefits from F2 (target-cpu enables
  crc32fast SIMD).
- **Mission C**: write-path fast paths benefit from F1 (cross-crate
  inlining of heap.insert + index.insert).
- **Mission D**: read-path fast paths benefit from F5 (inline hints on
  decode_column and BTree binary_search).
- **Mission E**: language features inherit all gains automatically.
