# PowDB: Wire protocol and engine architecture

## Engine architecture

PowDB is a library first, server second. The core engine is a single library
with a C ABI that any language can load. The server wraps that library with a
network listener.

```
┌─────────────────────────────────────────────────────┐
│                   PowDB Engine (library)            │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ PowQL   │  │ Query    │  │ Storage engine     │  │
│  │ compiler │→ │ executor │→ │                    │  │
│  │          │  │ (vector) │  │  B-tree indexes    │  │
│  └──────────┘  └──────────┘  │  Columnar segments │  │
│                              │  WAL + group commit│  │
│  ┌──────────┐               │  Undo-log MVCC     │  │
│  │ SQL →    │               │  Buffer pool       │  │
│  │ PowQL   │               └───────────────────┘  │
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
pow_conn*    pow_open(const char* path, pow_options* opts);
void          pow_close(pow_conn* conn);
pow_tx*      pow_begin(pow_conn* conn, pow_isolation level);
int           pow_commit(pow_tx* tx);
int           pow_rollback(pow_tx* tx);

// Query execution
pow_plan*    pow_compile(pow_conn* conn, const char* powql, size_t len);
pow_result*  pow_execute(pow_tx* tx, pow_plan* plan, pow_params* params);
void          pow_plan_free(pow_plan* plan);

// Prepared plan cache — compile once, execute many
uint64_t      pow_plan_hash(pow_plan* plan);
pow_plan*    pow_plan_lookup(pow_conn* conn, uint64_t hash);
void          pow_plan_cache(pow_conn* conn, pow_plan* plan);

// Result consumption (streaming)
int           pow_result_next(pow_result* res);   // advance cursor
int           pow_result_columns(pow_result* res); // column count
pow_type     pow_result_type(pow_result* res, int col);
int64_t       pow_result_int(pow_result* res, int col);
double        pow_result_float(pow_result* res, int col);
pow_str      pow_result_str(pow_result* res, int col);
int           pow_result_is_empty(pow_result* res, int col); // set-based null
void          pow_result_free(pow_result* res);

// Direct operations (bypass query compiler — the 42x path)
pow_result*  pow_scan(pow_tx* tx, pow_table_id table);
pow_result*  pow_index_lookup(pow_tx* tx, pow_index_id idx, pow_value* key);
pow_rowid    pow_insert(pow_tx* tx, pow_table_id table, pow_row* row);
int           pow_update(pow_tx* tx, pow_table_id table, pow_rowid id, pow_row* changes);
int           pow_delete(pow_tx* tx, pow_table_id table, pow_rowid id);

// Schema
pow_schema*  pow_schema_current(pow_conn* conn);
pow_plan*    pow_migrate_plan(pow_conn* conn, const char* new_schema, size_t len);
int           pow_migrate_apply(pow_conn* conn, pow_plan* plan);
```

The key split: `pow_compile` + `pow_execute` is the normal path (Mode 1).
The `pow_scan` / `pow_index_lookup` / `pow_insert` functions are the direct
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

### Mode 1: Native PowQL protocol

The primary protocol for all non-compiled language drivers.

**Query flow:**

```
Client                           Server
  │                                │
  ├─── QUERY(powql_text, params) ─→│  compile + execute
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
  ├─── PREPARE(powql_text)       ─→│  compile + cache
  │←── PREPARED(plan_hash)        ─│  returns plan handle
  │                                │
  ├─── EXECUTE(plan_hash, params) ─→│  skip compile, just execute
  │←── RESULT_HEADER ...          ─│
  │←── RESULT_BATCH ...           ─│
  │←── RESULT_COMPLETE ...        ─│
  │                                │
```

Prepared queries skip the PowQL compiler on subsequent calls. The server caches
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
0-bit means "has value." This maps directly to PowQL's `exists` / `not exists`.

### Mode 2: Prepared operations (compiled languages)

For TurboLang and other compiled languages that compile PowQL at build time.

The compiler emits a plan hash + parameter slots. At runtime, the client sends
the hash + bound parameters. The server looks up the cached plan and executes
directly — no parsing, no compilation.

```
Client (compiled)                Server
  │                                │
  │  [build time: compile PowQL   │
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
static pow_plan* plan_get_user_by_email = NULL;
static uint64_t plan_hash_get_user_by_email = 0xA3F2...;

// At runtime:
if (!plan_get_user_by_email) {
    plan_get_user_by_email = pow_plan_lookup(conn, plan_hash_get_user_by_email);
}
pow_params params = { .values = { email_str } };
pow_result* res = pow_execute(tx, plan_get_user_by_email, &params);
```

This is the 42x path. No parsing, no compilation, no planning at runtime.
Just parameter binding and execution.

### Mode 3: PostgreSQL wire protocol (compatibility)

PowDB speaks the PostgreSQL v3 wire protocol on a configurable port. This enables:
- psql, pgAdmin, pgcli (command-line tools)
- Grafana, Metabase, Tableau (BI tools)
- Any PostgreSQL driver (psycopg2, node-postgres, JDBC)
- Existing ORMs (including TurbineORM during migration)

The translation path:

```
SQL text → PostgreSQL parser → SQL AST → PowQL AST → compile → execute
```

This adds the parse + translate overhead (~20μs per query based on our benchmarks)
but the engine underneath is still PowDB's. So users get:
- PowDB's compact storage format (not PostgreSQL's bloated heap)
- PowDB's undo-log MVCC (no VACUUM)
- PowDB's vectorized executor for analytical queries
- PowDB's columnar segments for scan-heavy workloads

Limitations of the PG compatibility layer:
- Not all PostgreSQL SQL extensions are supported (PL/pgSQL, custom types,
  extensions like PostGIS are not available)
- Some edge cases in NULL handling differ (PowDB uses set-based nullability
  internally but translates to SQL NULL semantics for PG wire)
- System catalogs (pg_catalog) are emulated for basic tool compatibility
  but not all tables/views are present

## Driver architecture

Each language driver wraps the wire protocol (for server mode) or the C ABI
(for embedded mode) and exposes an idiomatic interface.

### TypeScript / JavaScript driver

```typescript
import { PowDB } from '@powdb/client';

// Connect (server mode)
const db = await PowDB.connect('pow://localhost:5433/mydb');

// Or embedded mode (loads the native library)
const db = await PowDB.open('./mydata.pow');

// Query with PowQL — returns typed results
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
await db.migrate('./schema.powql'); // declarative diff + apply

// Schema introspection
const schema = await db.schema();
// schema.types, schema.views, schema.indexes
```

### TurboLang driver (compile-time integration)

```turbolang
import pow from "powdb"

// Schema is known at compile time — type errors are build errors
let users = pow.query(User filter .age > 30 { name, email })
// ↑ This PowQL is parsed and compiled at build time.
// At runtime it's a pre-compiled plan execution — the 42x path.

// Type-safe: if User doesn't have a .age field, this is a compile error.
// If you rename .age to .years_old, every query referencing .age breaks
// at build time, not at 3am in production.

// Link traversal compiles to index lookups
let user_posts = pow.query(
  User filter .email = email { name, posts: .posts { title } }
)

// Computed views are just types
let active = pow.query(ActiveUser filter .age > 50)
```

### Python driver

```python
from powdb import connect

db = connect("pow://localhost:5433/mydb")

# PowQL queries
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

PowDB server is a single binary that wraps the engine library:

```toml
# powdb.toml

[storage]
data_dir = "/var/lib/powdb/data"
wal_dir = "/var/lib/powdb/wal"       # separate disk recommended
page_size = 4096
wal_batch_size = 128                   # group commit batch
direct_io = true                       # bypass OS page cache (Linux only)

[server]
listen_addr = "0.0.0.0"
pow_port = 5433                       # native PowQL protocol
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
  ├─── QUERY(powql, params)         ─→│  compile + execute
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
│  (compile-time PowQL, zero-copy types)          │
├─────────────────────────────────────────────────┤
│  PowQL (query language)                         │
│  - Native protocol (all languages)               │
│  - PostgreSQL wire protocol (ecosystem tools)    │
│  - Computed views, streaming, migrations          │
├─────────────────────────────────────────────────┤
│  PowDB (storage engine)                         │
│  - Hybrid row-column storage                     │
│  - Undo-log MVCC (no VACUUM)                     │
│  - Vectorized executor                           │
│  - B-tree indexes, WAL group commit              │
│  - Embedded library or server process             │
├─────────────────────────────────────────────────┤
│  TurbineORM (existing Postgres ORM)              │
│  → migrates to PowDB via PG wire protocol       │
│  → eventually uses native PowQL protocol         │
└─────────────────────────────────────────────────┘
```

Migration path for existing TurbineORM users:
1. Replace PostgreSQL with PowDB (PG wire protocol — drop-in replacement)
2. Existing queries work immediately, faster storage engine underneath
3. Gradually adopt PowQL for new queries (5-10x improvement over SQL)
4. Adopt TurboLang for critical paths (42x improvement)
