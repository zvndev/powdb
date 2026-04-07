# PowDB / PowQL — agent notes

Quick reference for AI assistants and humans writing client code or queries
against PowDB. This is the **as-implemented** truth, not the design doc — when
they disagree, this file wins. If you change the language or the wire protocol,
update this file in the same commit.

The design doc (`powql-language-design.md`) is the long-form vision and still
has things the parser doesn't yet accept (links, group, let bindings, `??`
defaults, etc.). Don't paste examples from there into a client without first
checking they parse.

---

## PowQL syntax — what the parser actually accepts

### Schema definition

```powql
type User {
  required name: str,
  required age:  int,
  city: str,
}
```

- Statement keyword is `type`. There is **no** `create table`.
- `required` is a **prefix keyword** on the field, not a `!` suffix. Optional
  fields just omit it.
- Commas between fields are allowed and conventional. Trailing comma is fine.
- Canonical type names are: `str`, `int`, `float`, `bool`, `datetime`, `uuid`,
  `bytes`. **Footgun:** `crates/query/src/executor.rs:307` falls back to
  `TypeId::Str` for any unknown name, so `string`, `varchar`, `text`, or a typo
  silently produces a Str column with no error. Always use the canonical names
  above until that fallback is fixed.

### Insert

```powql
insert User { name := "Alice", age := 30, city := "NYC" }
```

- Keyword is `insert <Type>` — **no `into`**.
- Field assignment uses `:=`, not `:` or `=`.
- String literals use double quotes.

### Query pipeline

```powql
User
User filter .age > 27
User filter .age > 27 order .age desc limit 10 { .name, .age }
User filter .city = "NYC" { .name, .age }
```

- Pipeline order is execution order: source → filter → order → limit → projection.
- Field references inside filter / order use a **leading dot**: `.age`, `.city`.
- Equality is **single `=`**, not `==`. The lexer doesn't have a `==` token
  (`crates/query/src/lexer.rs:161`).
- Other comparisons: `!=`, `<`, `<=`, `>`, `>=`.
- `order .field [asc|desc]` — default is asc.
- Projection braces: both `{ .name, .age }` and `{ name, age }` parse, but the
  dotted form is consistent with filter/order so prefer it.

### Aggregates

```powql
count(User)
count(User filter .age > 27)
avg(User filter .age > 27 | .age)   # not yet — pipe-into-agg is design only
```

`count`, `avg`, `sum`, `min`, `max` exist as call-form aggregates. Anything more
elaborate (group by, having, let bindings) is design-only — check the parser
before using.

### Things that look right but **don't parse**

| Don't write              | Write instead                          |
| ------------------------ | -------------------------------------- |
| `create table T { ... }` | `type T { ... }`                       |
| `insert into T { ... }`  | `insert T { ... }`                     |
| `name: string!`          | `required name: str`                   |
| `age: int!`              | `required age: int`                    |
| `name = "Alice"` (insert)| `name := "Alice"`                      |
| `.city == "NYC"`         | `.city = "NYC"`                        |
| `string`, `varchar`      | `str` (silently coerced — bug)         |
| `User.posts` (link)      | not yet implemented                    |
| `let x := ...`           | not yet implemented                    |

---

## Connecting to the hosted Fly deployment

Public endpoint (zvn-dev org, region `iad`, dedicated IPv4):

```
host:     213.188.194.202
port:     5433
db:       default
password: see `fly secrets list -a zvndev-powdb`  (POWDB_PASSWORD)
```

The local copy is at `/tmp/powdb_fly_password.txt` on this machine. Don't
commit it.

### From the local CLI (interactive REPL)

```bash
# build once
cargo build --release -p powdb-cli

# connect to the Fly server
./target/release/powdb-cli \
  --remote 213.188.194.202:5433 \
  --password "$(cat /tmp/powdb_fly_password.txt)"
```

You can also `cargo run -p powdb-cli --release -- --remote ... --password ...`
during development. `--db` defaults to `main`; the Fly server uses `default` if
you want to be explicit, but any name works since the server has a single
catalog.

### From the TypeScript client

```bash
cd clients/ts
pnpm install                                      # first time only
POWDB_PASSWORD="$(cat /tmp/powdb_fly_password.txt)" pnpm demo
```

That runs `clients/ts/demo/demo.ts` against the Fly endpoint. To use the client
in your own code:

```ts
import { Client } from "@zvndev/powdb-client";

const client = await Client.connect({
  host: "213.188.194.202",
  port: 5433,
  dbName: "default",
  password: process.env.POWDB_PASSWORD,
});

const result = await client.query("User filter .age > 27 { .name, .age }");
if (result.kind === "rows") {
  console.table(result.rows);
}
await client.close();
```

### Plain `nc` won't work

The wire protocol is binary length-prefixed framing
(`crates/server/src/protocol.rs`), not line-oriented. Use the CLI or a real
client — `telnet`/`nc` will just hang the server's `read_exact`.

---

## Server / deploy quick facts

- App: `zvndev-powdb` on Fly.io, single machine, `shared-cpu-1x` 256mb.
- Persistent volume `powdb_data` mounted at `/data`. `POWDB_DATA=/data` so
  the catalog (`catalog.bin`) and per-table heap files live there.
- Catalog is persisted on every `type` statement via atomic `temp + rename`
  (`crates/storage/src/catalog.rs`). If you blow away `/data`, you lose
  everything; if you stop and start the machine, data survives.
- Logs: `fly logs -a zvndev-powdb`. Per-query timings are emitted at info
  level via `tracing`.
- Restart: `fly machine restart -a zvndev-powdb`.
- Auth: `POWDB_PASSWORD` is a Fly secret; the server compares it to the
  `Connect` message's password field. Empty password disables auth.

## Bench regression gate

PowDB has a criterion-based regression gate that runs on every PR to `main`.
Its job is to catch silent perf regressions in the load-bearing query paths —
especially the `powql_point` workload (the 3,020x IndexScan-fold path that
validates the thesis). When the gate fires, **don't blindly rebaseline** —
investigate first.

Spec: `docs/superpowers/specs/2026-04-07-bench-regression-gate-design.md`.

### Run locally

```bash
cargo bench -p powdb-bench           # ~60s, runs all 7 workloads
cargo run -p powdb-bench --bin compare
```

The comparator reads `target/criterion/<workload>/new/estimates.json` and
compares against two checked-in baselines:

- `crates/bench/baseline/main.json` — per-workload absolute (±7% gate)
- `crates/bench/baseline/thesis-ratios.json` — ratio ceilings (currently
  `powql_point / btree_lookup ≤ 2.5x`)

Either guard fires → exit non-zero → CI blocks merge.

The cheap `smoke-bench` binary (`cargo run --release -p powdb-bench --bin
smoke-bench`) is still around for fast local iteration. It does not gate
anything — it just prints numbers.

### Interpreting a failure

The comparator output names the workload, the baseline value, the current
value, and the delta. Two failure modes:

1. **One workload exceeds 7% absolute.** Storage / executor / parser
   slowdown. Run `cargo bench -p powdb-bench` locally on `main` to confirm
   it isn't transient runner noise. If it reproduces, profile the workload
   (`cargo flamegraph --bench storage` etc.) and fix the regression.

2. **A thesis ratio exceeds its ceiling.** Planner / translation regression
   — the parser+planner+executor overhead grew relative to the raw B-tree.
   This is the failure mode the gate exists for. Almost always means
   someone broke a planner rewrite (e.g., the `.field = literal` →
   `IndexScan` fold from commit `077b960`). Look at the planner first.

### Rebaseline (intentional change)

Use this when you legitimately moved the numbers and the new floor is the
new truth.

```bash
./scripts/update-bench-baseline.sh
git diff --cached crates/bench/baseline/main.json     # sanity-check the diff
git commit -m "bench: rebaseline after <change> (<workload>: <delta>)"
```

The script runs the bench suite, extracts each workload's median, writes a
new `main.json` with the current rustc version + git sha + date, and stages
it. **It does not commit** — you commit, with a message that explains *why*
the baseline moved. The PR reviewer will check that the baseline diff
matches the claimed change.

### Raise a thesis ratio ceiling (rare, requires justification)

**Hand-edit only.** No script touches `thesis-ratios.json`. Raising a
ceiling means the thesis gave ground, so the commit must explain the
tradeoff. Convention:

```
bench: relax powql_point_over_btree_lookup 2.5 -> 2.8
       (cost-model planner adds ~400ns to plan phase, acceptable for join support)
```

Commit it in isolation — do not bundle with code changes. There is no CI
guard preventing a drive-by edit; the file format and the social contract
are the guard.

### Branch protection (manual setup, one time)

The bench job has to be a **required status check** to actually block
merges. This is a GitHub repo settings change, not a file in the repo.

1. Go to `Settings → Branches → Branch protection rules → main → Edit`.
2. Tick "Require status checks to pass before merging."
3. Tick "Require branches to be up to date before merging."
4. In the status checks search box, find `criterion + regression gate` (the
   job name from `.github/workflows/bench.yml`) and add it.
5. Save.

If you fork or re-clone the repo, redo this — it's not version controlled.

### When the gate gets noisy

GHA shared-tenancy noise on `ubuntu-24.04` is normally <5% on the workloads
in this suite, so the 7% gate should rarely false-positive. If it starts
flapping (~once a month or more on noise alone):

1. First, widen the absolute threshold to 10% in
   `crates/bench/src/bin/compare.rs` (`ABSOLUTE_THRESHOLD`). Cheap fix.
2. Only after that, consider a self-hosted runner. Adds ops burden — last
   resort.

The ratio guard is hardware-proof (both numerator and denominator move
together) so it doesn't need tuning when noise widens.

---

## Repo layout

```
crates/storage   slotted pages, B+ tree, WAL, buffer pool, catalog persistence
crates/query     lexer, parser, planner, executor (Engine)
crates/server    Tokio TCP server + binary wire protocol
crates/cli       rustyline REPL — embedded mode and remote mode
clients/ts       TypeScript client + demo app
Dockerfile       multi-stage build, rust:1.89-slim-bookworm builder
fly.toml         single machine, dedicated v4, persistent volume
```
