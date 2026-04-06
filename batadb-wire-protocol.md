# BataDB: Wire protocol and engine architecture

## Engine architecture

BataDB is a library first, server second. The core engine is a single library
with a C ABI that any language can load. The server wraps that library with a
network listener.

```
┌─────────────────────────────────────────────────────┐
│                   BataDB Engine (library)            │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ BataQL   │  │ Query    │  │ Storage engine     │  │
│  │ compiler │→ │ executor │→ │                    │  │
│  │          │  │ (vector) │  │  B-tree indexes    │  │
│  └──────────┘  └──────────┘  │  Columnar segments │  │
│                              │  WAL + group commit│  │
│  ┌──────────┐               │  Undo-log MVCC     │  │
│  │ SQL →    │               │  Buffer pool       │  │
│  │ BataQL   │               └───────────────────┘  │
│  │ compat   │                                      │
│  └──────────┘                                      │
└─────────────────────────────────────────────────────┘
         │                              │
    C ABI (embedded)           Network listener (server)
         │                              │
  ┌──────┴──────┐              ┌────────┴────────┐
  │ TurboLang   │              │ Wire protocol   │
  │ Rust, Go    │              │ (TCP + TLS)     │
  │ Python, etc │              │                 │
  └─────────────┘              │ Mode 1: native  │
                               │ Mode 2: prepared│
                               │ Mode 3: PG wire │
                               └─────────────────┘
```

### Engine API (C ABI)

The engine exposes a typed function interface. This is what drivers call
directly in embedded mode, and what the network listener calls after
deserializing wire protocol messages.

```c
// Connection and transaction management
bata_conn*    bata_open(const char* path, bata_options* opts);
void          bata_close(bata_conn* conn);
bata_tx*      bata_begin(bata_conn* conn, bata_isolation level);
int           bata_commit(bata_tx* tx);
int           bata_rollback(bata_tx* tx);

// Query execution
bata_plan*    bata_compile(bata_conn* conn, const char* bataql, size_t len);
bata_result*  bata_execute(bata_tx* tx, bata_plan* plan, bata_params* params);
void          bata_plan_free(bata_plan* plan);

// Prepared plan cache — compile once, execute many
uint64_t      bata_plan_hash(bata_plan* plan);
bata_plan*    bata_plan_lookup(bata_conn* conn, uint64_t hash);
void          bata_plan_cache(bata_conn* conn, bata_plan* plan);

// Result consumption (streaming)
int           bata_result_next(bata_result* res);   // advance cursor
int           bata_result_columns(bata_result* res); // column count
bata_type     bata_result_type(bata_result* res, int col);
int64_t       bata_result_int(bata_result* res, int col);
double        bata_result_float(bata_result* res, int col);
bata_str      bata_result_str(bata_result* res, int col);
int           bata_result_is_empty(bata_result* res, int col); // set-based null
void          bata_result_free(bata_result* res);

// Direct operations (bypass query compiler — the 42x path)
bata_result*  bata_scan(bata_tx* tx, bata_table_id table);
bata_result*  bata_index_lookup(bata_tx* tx, bata_index_id idx, bata_value* key);
bata_rowid    bata_insert(bata_tx* tx, bata_table_id table, bata_row* row);
int           bata_update(bata_tx* tx, bata_table_id table, bata_rowid id, bata_row* changes);
int           bata_delete(bata_tx* tx, bata_table_id table, bata_rowid id);

// Schema
bata_schema*  bata_schema_current(bata_conn* conn);
bata_plan*    bata_migrate_plan(bata_conn* conn, const char* new_schema, size_t len);
int           bata_migrate_apply(bata_conn* conn, bata_plan* plan);
```

The key split: `bata_compile` + `bata_execute` is the normal path (Mode 1).
The `bata_scan` / `bata_index_lookup` / `bata_insert` functions are the direct
path (Mode 2) that compiled languages use to skip the query compiler entirely.

## Wire protocol

### Packet format

All messages use a simple binary framing:

```
┌──────────┬──────────┬──────────┬─────────────────┐
│ type (1) │ flags (1)│ len (4)  │ payload (len)   │
└──────────┴──────────┴──────────┴─────────────────┘
```

- type: message type (0x01 = query, 0x02 = result, 0x03 = error, etc.)
- flags: compression (bit 0), streaming (bit 1), transaction (bit 2)
- len: payload length in bytes (uint32, little-endian)
- payload: type-specific binary data

Maximum message size: 16MB. Larger results stream as multiple messages.

### Mode 1: Native BataQL protocol

The primary protocol for all non-compiled language drivers.

**Query flow:**

```
Client                           Server
  │                                │
  ├─── QUERY(bataql_text, params) ─→│  compile + execute
  │                                │
  │←── RESULT_HEADER(columns, types)│  column metadata
  │←── RESULT_BATCH(rows)         ─│  binary row data (batched)
  │←── RESULT_BATCH(rows)         ─│  ... streaming
  │←── RESULT_COMPLETE(stats)     ─│  done
  │                                │
```

**Prepared query flow (cached compilation):**

```
Client                           Server
  │                                │
  ├─── PREPARE(bataql_text)       ─→│  compile + cache
  │←── PREPARED(plan_hash)        ─│  returns plan handle
  │                                │
  ├─── EXECUTE(plan_hash, params) ─→│  skip compile, just execute
  │←── RESULT_HEADER ...          ─│
  │←── RESULT_BATCH ...           ─│
  │←── RESULT_COMPLETE ...        ─│
  │                                │
```

Prepared queries skip the BataQL compiler on subsequent calls. The server caches
the compiled plan by hash. This is analogous to PostgreSQL's prepared statements
but the compiled plan is a physical execution plan, not just a parsed AST.

**Result format — column-oriented binary:**

Results are sent in column-major order within each batch. This means all values
for column 0 come first, then all values for column 1, etc. This is the opposite
of PostgreSQL's row-major result format.

Why column-major results:
- Client-side analytics can process columns without row-by-row iteration
- Better compression (same-type values cluster together)
- Matches the engine's internal columnar format — no row-to-column conversion
- Typed arrays on the client side (Int32Array, Float64Array) map directly

```
RESULT_BATCH payload:
  row_count:  uint32
  column 0:   [null_bitmap] [values...]
  column 1:   [null_bitmap] [values...]
  ...

  Where [values] is a packed array of the column's native type:
    int:      int64[]  (8 bytes each)
    float:    float64[] (8 bytes each)
    str:      [offset_table: uint32[]] [string_data: bytes]
    bool:     bit-packed (1 bit per row)
    datetime: int64[] (microseconds since epoch)
    uuid:     bytes[] (16 bytes each)
```

The null bitmap uses set-based nullability: a 1-bit means "empty set" (no value),
0-bit means "has value." This maps directly to BataQL's `exists` / `not exists`.

### Mode 2: Prepared operations (compiled languages)

For TurboLang and other compiled languages that compile BataQL at build time.

The compiler emits a plan hash + parameter slots. At runtime, the client sends
the hash + bound parameters. The server looks up the cached plan and executes
directly — no parsing, no compilation.

```
Client (compiled)                Server
  │                                │
  │  [build time: compile BataQL   │
  │   to plan, compute hash]       │
  │                                │
  ├─── REGISTER_PLAN(hash, plan)  ─→│  cache the plan
  │←── OK                         ─│
  │                                │
  ├─── EXEC_PLAN(hash, params)    ─→│  lookup + execute (no compile)
  │←── RESULT_HEADER ...          ─│
  │←── RESULT_BATCH ...           ─│
  │                                │
```

In embedded mode (no network), this becomes a direct function call:

```c
// At build time, the compiler emits:
static bata_plan* plan_get_user_by_email = NULL;
static uint64_t plan_hash_get_user_by_email = 0xA3F2...;

// At runtime:
if (!plan_get_user_by_email) {
    plan_get_user_by_email = bata_plan_lookup(conn, plan_hash_get_user_by_email);
}
bata_params params = { .values = { email_str } };
bata_result* res = bata_execute(tx, plan_get_user_by_email, &params);
```

This is the 42x path. No parsing, no compilation, no planning at runtime.
Just parameter binding and execution.

### Mode 3: PostgreSQL wire protocol (compatibility)

BataDB speaks the PostgreSQL v3 wire protocol on a configurable port. This enables:
- psql, pgAdmin, pgcli (command-line tools)
- Grafana, Metabase, Tableau (BI tools)
- Any PostgreSQL driver (psycopg2, node-postgres, JDBC)
- Existing ORMs (including TurbineORM during migration)

The translation path:

```
SQL text → PostgreSQL parser → SQL AST → BataQL AST → compile → execute
```

This adds the parse + translate overhead (~20μs per query based on our benchmarks)
but the engine underneath is still BataDB's. So users get:
- BataDB's compact storage format (not PostgreSQL's bloated heap)
- BataDB's undo-log MVCC (no VACUUM)
- BataDB's vectorized executor for analytical queries
- BataDB's columnar segments for scan-heavy workloads

Limitations of the PG compatibility layer:
- Not all PostgreSQL SQL extensions are supported (PL/pgSQL, custom types,
  extensions like PostGIS are not available)
- Some edge cases in NULL handling differ (BataDB uses set-based nullability
  internally but translates to SQL NULL semantics for PG wire)
- System catalogs (pg_catalog) are emulated for basic tool compatibility
  but not all tables/views are present

## Driver architecture

Each language driver wraps the wire protocol (for server mode) or the C ABI
(for embedded mode) and exposes an idiomatic interface.

### TypeScript / JavaScript driver

```typescript
import { BataDB } from '@batadb/client';

// Connect (server mode)
const db = await BataDB.connect('bata://localhost:5433/mydb');

// Or embedded mode (loads the native library)
const db = await BataDB.open('./mydata.bata');

// Query with BataQL — returns typed results
const users = await db.query<User>(
  `User filter .age > $age order .name limit $limit`,
  { age: 30, limit: 10 }
);
// users is User[] with full type inference

// Prepared queries (compiled once, executed many)
const getUser = await db.prepare<User>(
  `User filter .email = $email`
);
const alice = await getUser.execute({ email: 'alice@example.com' });
const bob = await getUser.execute({ email: 'bob@example.com' });
// Second call skips compilation — just sends params

// Streaming large results
const stream = db.stream<User>(`User filter .age > 30`);
for await (const batch of stream) {
  // batch is User[] — arrives in chunks of ~1000 rows
  processBatch(batch);
}

// Transactions
await db.transaction(async (tx) => {
  const user = await tx.query(`insert User { name := $name, ... }`, { name: 'Alice' });
  await tx.query(`insert Post { author := $user, ... }`, { user: user.id });
});

// Migrations
await db.migrate('./schema.bataql'); // declarative diff + apply

// Schema introspection
const schema = await db.schema();
// schema.types, schema.views, schema.indexes
```

### TurboLang driver (compile-time integration)

```turbolang
import bata from "batadb"

// Schema is known at compile time — type errors are build errors
let users = bata.query(User filter .age > 30 { name, email })
// ↑ This BataQL is parsed and compiled at build time.
// At runtime it's a pre-compiled plan execution — the 42x path.

// Type-safe: if User doesn't have a .age field, this is a compile error.
// If you rename .age to .years_old, every query referencing .age breaks
// at build time, not at 3am in production.

// Link traversal compiles to index lookups
let user_posts = bata.query(
  User filter .email = email { name, posts: .posts { title } }
)

// Computed views are just types
let active = bata.query(ActiveUser filter .age > 50)
```

### Python driver

```python
from batadb import connect

db = connect("bata://localhost:5433/mydb")

# BataQL queries
users = db.query("User filter .age > $age", age=30)
for user in users:
    print(user.name, user.email)

# Streaming
for batch in db.stream("User", batch_size=5000):
    process(batch)

# Also supports raw SQL via PG compat (for pandas/jupyter users)
df = db.sql("SELECT name, age FROM users WHERE age > 30")
```

## Server configuration

BataDB server is a single binary that wraps the engine library:

```toml
# batadb.toml

[storage]
data_dir = "/var/lib/batadb/data"
wal_dir = "/var/lib/batadb/wal"       # separate disk recommended
page_size = 4096
wal_batch_size = 128                   # group commit batch
direct_io = true                       # bypass OS page cache (Linux only)

[server]
listen_addr = "0.0.0.0"
bata_port = 5433                       # native BataQL protocol
pg_port = 5432                         # PostgreSQL compatibility
max_connections = 200

[mvcc]
isolation = "snapshot"                 # snapshot isolation (default)
undo_retention = "10m"                 # keep undo entries for 10 min
auto_purge = true                      # background undo cleanup

[columnar]
segment_size = 65536                   # rows per columnar segment
merge_threshold = 0.3                  # merge to columnar when 30% of rows are cold
background_merge = true                # async merge hot rows → columnar

[indexes]
auto_suggest = true                    # track query patterns for index suggestions
suggest_threshold = 100                # suggest after 100 queries use the pattern

[views]
materialized_refresh = "async"         # async refresh on data change
max_materialized_views = 50
```

## Connection lifecycle

```
Client                              Server
  │                                    │
  ├─── CONNECT(db, user, auth)       ─→│  authenticate
  │←── CONNECT_OK(server_version)    ─│
  │                                    │
  ├─── BEGIN(isolation)              ─→│  start transaction
  │←── TX_OK(tx_id)                  ─│
  │                                    │
  ├─── QUERY(bataql, params)         ─→│  compile + execute
  │←── RESULT_HEADER(cols, types)    ─│
  │←── RESULT_BATCH(rows)           ─│
  │←── RESULT_COMPLETE(stats)        ─│
  │                                    │
  ├─── COMMIT                        ─→│  commit transaction
  │←── COMMIT_OK                     ─│
  │                                    │
  ├─── DISCONNECT                    ─→│
  │←── BYE                           ─│
```

## What this means for the platform

The three-product stack:

```
┌─────────────────────────────────────────────────┐
│  TurboLang application code                      │
│  (compile-time BataQL, zero-copy types)          │
├─────────────────────────────────────────────────┤
│  BataQL (query language)                         │
│  - Native protocol (all languages)               │
│  - PostgreSQL wire protocol (ecosystem tools)    │
│  - Computed views, streaming, migrations          │
├─────────────────────────────────────────────────┤
│  BataDB (storage engine)                         │
│  - Hybrid row-column storage                     │
│  - Undo-log MVCC (no VACUUM)                     │
│  - Vectorized executor                           │
│  - B-tree indexes, WAL group commit              │
│  - Embedded library or server process             │
├─────────────────────────────────────────────────┤
│  TurbineORM (existing Postgres ORM)              │
│  → migrates to BataDB via PG wire protocol       │
│  → eventually uses native BataQL protocol         │
└─────────────────────────────────────────────────┘
```

Migration path for existing TurbineORM users:
1. Replace PostgreSQL with BataDB (PG wire protocol — drop-in replacement)
2. Existing queries work immediately, faster storage engine underneath
3. Gradually adopt BataQL for new queries (5-10x improvement over SQL)
4. Adopt TurboLang for critical paths (42x improvement)
