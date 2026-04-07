# BataDB / BataQL — agent notes

Quick reference for AI assistants and humans writing client code or queries
against BataDB. This is the **as-implemented** truth, not the design doc — when
they disagree, this file wins. If you change the language or the wire protocol,
update this file in the same commit.

The design doc (`bataql-language-design.md`) is the long-form vision and still
has things the parser doesn't yet accept (links, group, let bindings, `??`
defaults, etc.). Don't paste examples from there into a client without first
checking they parse.

---

## BataQL syntax — what the parser actually accepts

### Schema definition

```bataql
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

```bataql
insert User { name := "Alice", age := 30, city := "NYC" }
```

- Keyword is `insert <Type>` — **no `into`**.
- Field assignment uses `:=`, not `:` or `=`.
- String literals use double quotes.

### Query pipeline

```bataql
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

```bataql
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
password: see `fly secrets list -a zvndev-batadb`  (BATADB_PASSWORD)
```

The local copy is at `/tmp/batadb_fly_password.txt` on this machine. Don't
commit it.

### From the local CLI (interactive REPL)

```bash
# build once
cargo build --release -p batadb-cli

# connect to the Fly server
./target/release/batadb-cli \
  --remote 213.188.194.202:5433 \
  --password "$(cat /tmp/batadb_fly_password.txt)"
```

You can also `cargo run -p batadb-cli --release -- --remote ... --password ...`
during development. `--db` defaults to `main`; the Fly server uses `default` if
you want to be explicit, but any name works since the server has a single
catalog.

### From the TypeScript client

```bash
cd clients/ts
pnpm install                                      # first time only
BATADB_PASSWORD="$(cat /tmp/batadb_fly_password.txt)" pnpm demo
```

That runs `clients/ts/demo/demo.ts` against the Fly endpoint. To use the client
in your own code:

```ts
import { Client } from "@zvndev/batadb-client";

const client = await Client.connect({
  host: "213.188.194.202",
  port: 5433,
  dbName: "default",
  password: process.env.BATADB_PASSWORD,
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

- App: `zvndev-batadb` on Fly.io, single machine, `shared-cpu-1x` 256mb.
- Persistent volume `batadb_data` mounted at `/data`. `BATADB_DATA=/data` so
  the catalog (`catalog.bin`) and per-table heap files live there.
- Catalog is persisted on every `type` statement via atomic `temp + rename`
  (`crates/storage/src/catalog.rs`). If you blow away `/data`, you lose
  everything; if you stop and start the machine, data survives.
- Logs: `fly logs -a zvndev-batadb`. Per-query timings are emitted at info
  level via `tracing`.
- Restart: `fly machine restart -a zvndev-batadb`.
- Auth: `BATADB_PASSWORD` is a Fly secret; the server compares it to the
  `Connect` message's password field. Empty password disables auth.

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
