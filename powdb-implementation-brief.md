# PowDB: Complete implementation brief

This document contains everything needed to implement PowDB from scratch.
It is the single source of truth — all architectural decisions are backed by
production benchmarks and explained with rationale.

---

## Table of contents

1. What PowDB is
2. Production benchmark evidence (numbers that drive every decision)
3. Storage engine specification
4. PowQL query language specification
5. Wire protocol specification
6. Engine C ABI specification
7. Driver specifications
8. Server configuration
9. Implementation phases (what to build and in what order)
10. Open decisions (things not yet finalized)

---

## 1. What PowDB is

A database engine built for modern hardware, designed to eliminate the
translation layers that make traditional databases slow.

Three products form the stack:
- **PowDB** — storage engine (embedded library with C ABI + optional server)
- **PowQL** — standalone query language DSL (not tied to any host language)
- **TurboLang integration** — optional first-class client with compile-time query planning

Existing product context:
- **TurbineORM** (turbineorm.dev) — Postgres ORM for TypeScript, already in production
- **TurboLang** (turbolang.dev) — new general-purpose programming language, in development
- PowDB must support PostgreSQL wire protocol so TurbineORM users can migrate incrementally

The core thesis, validated by benchmarks: SQL translation layers (parsing, planning,
type marshaling) account for 20-42x of query latency. Removing them while keeping
a better storage engine produces 5-100x improvements depending on workload.

---

## 2. Production benchmark evidence

All numbers from Railway deployment: Intel Xeon Icelake (shared host), ZFS storage,
Node.js v22. Benchmarks were written in JavaScript as scaffolding to validate patterns —
the implementation language is Rust or Zig. Ratios (not absolute numbers) are what matter,
and they held constant across sandbox and production environments, confirming they're
CPU-bound, not I/O-bound.

### 2.1 Storage format overhead

| Format | Bytes/row | Overhead/row | Overhead % |
|--------|-----------|-------------|------------|
| PostgreSQL heap (8KB pages) | 77 | 28B (23B tuple header + 4B line pointer + padding) | 37% |
| Compact row (4KB pages) | 45 | 2B (length prefix) | 5% |
| Columnar | 48 | 8B (amortized column headers) | 17% |
| SQLite | 47 | N/A (includes B-tree + WAL overhead) | ~10% |

**Decision: compact row format (2B/row overhead) for OLTP hot path. Columnar segments
for analytical cold path. Background merge converts hot rows to columnar.**

### 2.2 Point lookup performance (50K rows)

| Method | p50 latency | ops/sec | vs SQLite prepared |
|--------|------------|---------|-------------------|
| SQLite raw SQL | 27μs | 28K | 0.7x |
| SQLite prepared statement | 20μs | 41K | 1.0x |
| Compact row direct read | <1μs | 1.06M | 42x |
| Columnar direct read | <1μs | 8.8M | 293x |

**The 42x gap is entirely SQL parsing + query planning + type marshaling.
This gap held constant across environments (sandbox: 39x, production: 42x).**

### 2.3 Scan and aggregation performance (50K rows)

| Operation | SQLite | Columnar | Ratio |
|-----------|--------|----------|-------|
| Single column scan (age) | 24,369μs | 326μs | 75x |
| AVG(age) aggregation | 5,341μs | 51μs | 105x |
| Full table scan (all cols) | 126,683μs | 1,658μs | 76x |

**Columnar format reads only the columns needed. Row stores read everything.**

### 2.4 Index performance (500K rows)

| Method | p50 | ops/sec | vs sequential scan |
|--------|-----|---------|-------------------|
| Sequential scan | 30μs | 9K | 1x |
| B-tree order=64 (height 4) | <1μs | 1.07M | 30x |
| B-tree order=256 (height 3) | <1μs | 1.02M | 30x |
| B-tree order=1024 (height 2) | <1μs | 954K | 30x |
| Hash index | <1μs | 640K | 30x |
| Direct known location | <1μs | 1.37M | 30x |
| SQLite B-tree | 18μs | 45K | 2x |

**B-tree order 256: height 3 for 500K rows (3 page reads per lookup).
Our B-tree at 1.02M ops/sec vs SQLite's at 45K = 22x gap, same data structure,
pure SQL overhead.**

Range scan (1000 rows): our B-tree 419μs vs SQLite 4,893μs = 12x.

### 2.5 WAL / durability (production fsync = 221μs)

| Batch size | Durable writes/sec | μs per record |
|-----------|-------------------|---------------|
| 1 (fsync each) | 465 | 2,151 |
| 8 | 4,806 | 208 |
| 32 | 23,757 | 42 |
| 128 | 49,200 | 20 |
| 256 | 87,122 | 11 |
| 512 | 114,593 | 9 |
| No WAL (unsafe) | 7,524,454 | 0.1 |

**Decision: group commit at batch 128-256. Plateau begins at 128 (~49K writes/sec).
On bare-metal NVMe (10-50μs fsync vs our 221μs), expect 200K+ writes/sec at batch=32.**

### 2.6 MVCC concurrency cost (50K rows, 30% update churn)

| Engine | Update rate | Scan after churn | Bloat | Point read |
|--------|------------|-----------------|-------|------------|
| No MVCC (baseline) | 679K/s | 331μs | 0% | 12.3M ops/s |
| Append-only (PostgreSQL) | 122K/s | 2,041μs (6.2x worse) | 21% | 8.8M ops/s |
| Undo-log (InnoDB) | 739K/s | 1,130μs (3.4x worse) | 0% | 10.9M ops/s |

**Decision: undo-log MVCC.**
- Updates are faster than no-MVCC (739K vs 679K — in-place modification has better cache behavior)
- Zero bloat on main table (vs 21% for append-only)
- No VACUUM ever needed
- Scans degrade 3.4x (vs 6.2x for append-only) because main table is clean
- Undo entries are purged by advancing a pointer, not rewriting the table

### 2.7 Vectorized execution (500K rows)

| Query | Volcano (row-at-a-time) | Vectorized (typed arrays) | Ratio |
|-------|------------------------|--------------------------|-------|
| Filter + count | 4,321μs | 1,038μs | 4.2x |
| Filter + count (unrolled 4x) | 4,321μs | 624μs | 6.9x |
| AVG(age) where age > 30 | 4,311μs | 1,318μs | 3.3x |
| GROUP BY age, COUNT(*) | 11,326μs | 859μs | 13.2x |
| Multi-column filter + project | 7,943μs | 4,199μs | 1.9x |
| Raw iteration (objects vs Int16Array) | 5,062μs | 628μs | 8.1x |

**These are JavaScript numbers. In native code with SIMD (AVX2/AVX-512),
expect 10-50x for vectorized operations. The 13.2x group-by used a flat
Int32Array instead of a hash map — a standard vectorized optimization for
bounded-domain keys.**

---

## 3. Storage engine specification

### 3.1 Page format

4KB pages (4096 bytes). Each page has a header:

```
Page layout (4096 bytes):
┌────────────────────────────────────┐
│ Page header (8 bytes)              │
│   page_id:    uint32               │
│   page_type:  uint8 (data/index/   │
│               overflow/columnar)    │
│   flags:      uint8                │
│   free_start: uint16               │
├────────────────────────────────────┤
│ Row/entry data                     │
│ (grows downward from header)       │
│                                    │
│                                    │
│                                    │
│ Free space                         │
│                                    │
│                                    │
│ (slot directory grows upward)      │
├────────────────────────────────────┤
│ Slot directory (grows from bottom) │
│   slot[0]: uint16 (offset to row)  │
│   slot[1]: uint16                  │
│   ...                              │
│   slot_count: uint16               │
└────────────────────────────────────┘
```

### 3.2 Compact row format

Each row within a data page:

```
Row layout:
┌──────────────────────────────────────┐
│ Row header (2 bytes)                 │
│   length: uint16 (total row bytes)   │
├──────────────────────────────────────┤
│ Null bitmap (ceil(n_cols / 8) bytes) │
│   bit = 1 means "empty set" (no val)│
├──────────────────────────────────────┤
│ Fixed-length columns (packed)        │
│   int64, float64, bool, datetime...  │
├──────────────────────────────────────┤
│ Variable-length offset table         │
│   offsets to each varchar/blob       │
├──────────────────────────────────────┤
│ Variable-length data                 │
│   string bytes, blob bytes           │
└──────────────────────────────────────┘
```

Total overhead: 2 bytes (length prefix) + null bitmap. For a 4-column table:
2 + 1 = 3 bytes overhead per row vs PostgreSQL's 28 bytes.

### 3.3 Columnar segment format

Cold data is stored in columnar segments for analytical query performance.
Each segment contains N rows (configurable, default 65,536) stored column-by-column:

```
Columnar segment:
┌──────────────────────────────────────┐
│ Segment header                       │
│   row_count: uint32                  │
│   column_count: uint16               │
│   column_directory: [offset, type][] │
├──────────────────────────────────────┤
│ Column 0: packed int64[] or similar  │
│   (with optional run-length encoding)│
├──────────────────────────────────────┤
│ Column 1: packed values              │
├──────────────────────────────────────┤
│ Column 2: ...                        │
└──────────────────────────────────────┘
```

### 3.4 B-tree index

B+ tree with configurable order (default 256 for integer keys, 128 for string keys).

- Internal nodes: [key₀ | ptr₀ | key₁ | ptr₁ | ... | ptr_n]
- Leaf nodes: [key₀ | rowid₀ | key₁ | rowid₁ | ...] + next_leaf pointer
- Leaf nodes are linked for range scans
- RowIDs are (page_id, slot_index) pairs pointing into the data pages

For 500K rows with order 256: height 3, meaning 3 page reads per point lookup.
For 1 billion rows: height 4 (4 page reads = ~40μs on NVMe).

### 3.5 WAL (Write-Ahead Log)

Every mutation writes to the WAL before modifying data pages.

Record format:
```
WAL record:
┌──────────┬──────────┬──────────┬──────────┐
│ len (4)  │ crc32 (4)│ tx_id (8)│ data ... │
└──────────┴──────────┴──────────┴──────────┘
```

Group commit: records buffer until batch_size is reached (default 128),
then flush + single fsync. This amortizes the fsync cost across many writes.

WAL is append-only. Periodic checkpoints flush dirty pages to data files,
then the WAL can be truncated.

### 3.6 Undo-log MVCC

Each row has a hidden `_xmin` (creating transaction ID) and `_undo_ptr`
(pointer to previous version in the undo log).

**Write path (UPDATE):**
1. Copy current row to undo log (push old version)
2. Modify row in-place in the data page
3. Set _xmin = current_tx, _undo_ptr = undo_log_offset

**Read path (snapshot isolation):**
1. Read current row from data page
2. If _xmin > snapshot_id, follow _undo_ptr to find visible version
3. Walk undo chain until finding version where _xmin < snapshot_id

**Undo purge:**
Background thread advances the "oldest active transaction" watermark.
Undo entries older than this are safe to reclaim. This is a pointer
advance, not a table rewrite (unlike PostgreSQL's VACUUM).

### 3.7 Buffer pool

In-memory cache of recently accessed pages. Pages are loaded from disk
on first access and evicted using clock-sweep (like PostgreSQL) when
memory pressure is high.

For direct I/O mode (Linux): the buffer pool IS the only page cache.
The OS page cache is bypassed. This gives the engine full control over
eviction policy and prevents double-caching.

---

## 4. PowQL query language specification

### 4.1 Schema definition

```powql
type User {
  required name: str
  required email: str
  age: int                        # optional — returns {} if missing

  multi link posts -> Post        # one-to-many
  link company -> Company         # optional one-to-one

  index on .email                 # unique index
  index on .age                   # range index
}

type Post {
  required title: str
  required body: str
  required created_at: datetime
  required link author -> User    # required one-to-one

  index on .created_at
}

type Company {
  required name: str
  multi link employees -> User
}
```

### 4.2 Query syntax

**Basic operations:**
```powql
User                                          # all users (sequential scan)
User filter .age > 30                         # filter
User { name, email }                          # projection
User filter .age > 30 { name, email, age }    # filter + project
User order .name limit 10                     # order + limit
User filter .age > 30 order .name desc limit 10 { name, email }  # full pipeline
```

**Link traversal (not joins):**
```powql
User { name, posts: .posts { title, created_at } }     # nested results
User filter count(.posts) > 0                            # filter by link
User { name, company_name: .company.name }               # deep traversal
```

**Aggregations:**
```powql
count(User)                                                 # count
User filter .age > 30 | avg(.age)                          # pipe to aggregate
User group .company.name {                                  # group by
  company: .key, headcount: count(.), avg_age: avg(.age)
}
User group .company.name filter count(.) > 5 {             # group + having
  company: .key, headcount: count(.)
}
```

**Set-based nullability (no NULL, no three-valued logic):**
```powql
User { name, age: .age ?? 0 }       # default value for empty set
User filter exists .age              # only users who have an age
User filter not exists .age          # users without an age
# .age > 30 naturally excludes empty sets — no NULL surprise
```

**Ad-hoc match (escape hatch for non-link joins):**
```powql
User as u match Employee as e on u.email = e.personal_email
  { user_name: u.name, employee_name: e.name }
```

**Mutations:**
```powql
insert User { name := "Alice", email := "alice@example.com", age := 30 }

User filter .email = "alice@example.com" update { age := 31 }
User filter .age > 0 update { age := .age + 1 }
User filter .last_login < datetime('2020-01-01') delete

User upsert on .email = "alice@example.com" {
  name := "Alice", email := "alice@example.com", age := 31
}
```

**Transactions:**
```powql
transaction {
  let alice := insert User { name := "Alice", email := "alice@ex.com" }
  insert Post { title := "First post", author := alice, ... }
}
```

**Computed views:**
```powql
view ActiveUser := User filter .last_login > now() - duration('30d')
view TeamSummary := User group .company.name {
  company: .key, headcount: count(.), avg_age: avg(.age)
}

materialized view DailyStats := Post group .created_at.date {
  date: .key, post_count: count(.), unique_authors: count(distinct .author)
}

# Views compose like types
ActiveUser filter .age > 50 { name, email }
TeamSummary filter .headcount > 10
```

**Let bindings:**
```powql
let active := User filter .last_login > datetime('2024-01-01')
active { name, email }
count(active)
```

### 4.3 Type system

Scalar types:
- `str` — UTF-8 string
- `int` — 64-bit signed integer
- `float` — 64-bit IEEE 754
- `bool` — true/false
- `datetime` — microsecond precision timestamp
- `uuid` — 128-bit UUID
- `bytes` — binary blob
- `duration` — time interval
- `json` — arbitrary JSON (escape hatch for schemaless data)

Modifiers:
- `required` — field must have a value (set is never empty)
- `multi` — set contains zero or more values (on links: one-to-many)
- No modifier = optional (set contains zero or one value)

### 4.4 Migrations

Declarative schema diffing. Developer maintains a `schema.powql` file:

```bash
powdb migrate --plan     # show diff (dry run)
powdb migrate --apply    # apply transactionally
```

Features:
- Rename detection (field disappears + similar field appears = rename prompt)
- Data migration steps for type changes (shown for review)
- Destructive change warnings (drop column/type requires confirmation)
- Fully transactional (rollback on any failure)
- Migration history stored in database

### 4.5 Index management

Automatic indexes for: primary keys, link targets (foreign keys), frequently
queried fields.

Manual declaration in schema:
```powql
type User {
  ...
  index on .email
  index on .age
  index on (.name, .age)   # composite
}
```

Suggestion engine:
```bash
powdb suggest-indexes     # analyzes query patterns, recommends indexes
```

---

## 5. Wire protocol specification

### 5.1 Packet framing

```
┌──────────┬──────────┬──────────┬─────────────────┐
│ type (1) │ flags (1)│ len (4)  │ payload (len)   │
└──────────┴──────────┴──────────┴─────────────────┘
```

- type: message type byte
- flags: bit 0 = compressed, bit 1 = streaming, bit 2 = in-transaction
- len: uint32 little-endian payload length
- Max message: 16MB. Larger results stream as multiple RESULT_BATCH messages.

### 5.2 Message types

```
0x01 CONNECT         client → server  (db name, auth)
0x02 CONNECT_OK      server → client  (server version)
0x03 QUERY           client → server  (powql text + params)
0x04 PREPARE         client → server  (powql text)
0x05 PREPARED        server → client  (plan hash)
0x06 EXECUTE         client → server  (plan hash + params)
0x07 RESULT_HEADER   server → client  (column names + types)
0x08 RESULT_BATCH    server → client  (column-oriented row data)
0x09 RESULT_COMPLETE server → client  (row count, timing stats)
0x0A ERROR           server → client  (error code + message)
0x0B BEGIN           client → server  (isolation level)
0x0C COMMIT          client → server
0x0D ROLLBACK        client → server
0x0E TX_OK           server → client  (tx id)
0x0F REGISTER_PLAN   client → server  (plan hash + compiled plan)
0x10 DISCONNECT      client → server
0x11 BYE             server → client
```

### 5.3 Result format (column-oriented binary)

RESULT_BATCH payload:
```
row_count: uint32
For each column:
  null_bitmap: ceil(row_count / 8) bytes  (1 = empty set, 0 = has value)
  values: packed array of column's native type
    int:      int64[] (8 bytes each, little-endian)
    float:    float64[] (8 bytes each)
    str:      uint32[] offset table + concatenated string bytes
    bool:     bit-packed
    datetime: int64[] (microseconds since epoch)
    uuid:     16 bytes each
```

Column-major results because:
- Matches engine's internal columnar format (no conversion)
- Better compression (same-type values cluster)
- Client analytics can process columns directly as typed arrays

### 5.4 Mode 1: Native PowQL (all language drivers)

```
Client → QUERY(powql_text, params) → Server
Server: compile PowQL → execute → stream results
Server → RESULT_HEADER → RESULT_BATCH(es) → RESULT_COMPLETE → Client
```

Prepared variant:
```
Client → PREPARE(powql_text) → Server → PREPARED(hash) → Client
Client → EXECUTE(hash, params) → Server → results (skips compile)
```

### 5.5 Mode 2: Pre-compiled plans (TurboLang, compiled languages)

```
Client → REGISTER_PLAN(hash, compiled_plan) → Server → OK
Client → EXECUTE(hash, params) → Server → results (no compile, no parse)
```

The compiler emits the plan at build time. At runtime, only parameter
binding and execution happen. This is the 42x path.

### 5.6 Mode 3: PostgreSQL wire protocol (compatibility)

Standard PostgreSQL v3 wire protocol on a separate port. Enables psql,
pgAdmin, Grafana, Metabase, existing ORMs (including TurbineORM).

Translation path: SQL → PG parser → SQL AST → PowQL AST → compile → execute

Limitations:
- No PL/pgSQL, no PostgreSQL extensions
- System catalogs (pg_catalog) partially emulated
- NULL semantics translated (set-based internally → SQL NULL externally)

---

## 6. Engine C ABI specification

The engine is a native library (.so / .dylib / .dll) with a C ABI.
Every higher-level interface (drivers, server) calls through this API.

```c
// === Connection ===
pow_conn*    pow_open(const char* path, pow_options* opts);
void          pow_close(pow_conn* conn);

// === Transactions ===
pow_tx*      pow_begin(pow_conn* conn, pow_isolation level);
int           pow_commit(pow_tx* tx);
int           pow_rollback(pow_tx* tx);

// === Query compilation + execution ===
pow_plan*    pow_compile(pow_conn* conn, const char* powql, size_t len);
pow_result*  pow_execute(pow_tx* tx, pow_plan* plan, pow_params* params);
void          pow_plan_free(pow_plan* plan);

// === Plan cache ===
uint64_t      pow_plan_hash(pow_plan* plan);
pow_plan*    pow_plan_lookup(pow_conn* conn, uint64_t hash);
void          pow_plan_cache(pow_conn* conn, pow_plan* plan);

// === Result consumption ===
int           pow_result_next(pow_result* res);
int           pow_result_columns(pow_result* res);
pow_type     pow_result_type(pow_result* res, int col);
int64_t       pow_result_int(pow_result* res, int col);
double        pow_result_float(pow_result* res, int col);
pow_str      pow_result_str(pow_result* res, int col);
int           pow_result_is_empty(pow_result* res, int col);
void          pow_result_free(pow_result* res);

// === Direct operations (bypass compiler — the 42x path) ===
pow_result*  pow_scan(pow_tx* tx, pow_table_id table);
pow_result*  pow_index_lookup(pow_tx* tx, pow_index_id idx, pow_value* key);
pow_rowid    pow_insert(pow_tx* tx, pow_table_id table, pow_row* row);
int           pow_update(pow_tx* tx, pow_table_id table, pow_rowid id, pow_row* changes);
int           pow_delete(pow_tx* tx, pow_table_id table, pow_rowid id);

// === Schema + migrations ===
pow_schema*  pow_schema_current(pow_conn* conn);
pow_plan*    pow_migrate_plan(pow_conn* conn, const char* new_schema, size_t len);
int           pow_migrate_apply(pow_conn* conn, pow_plan* plan);
```

---

## 7. Driver specifications

### 7.1 TypeScript driver (@powdb/client)

```typescript
import { PowDB } from '@powdb/client';

const db = await PowDB.connect('pow://localhost:5433/mydb');  // server
const db = await PowDB.open('./mydata.pow');                  // embedded

const users = await db.query<User>(
  `User filter .age > $age order .name limit $limit`,
  { age: 30, limit: 10 }
);

const getUser = await db.prepare<User>(`User filter .email = $email`);
const alice = await getUser.execute({ email: 'alice@example.com' });

const stream = db.stream<User>(`User filter .age > 30`);
for await (const batch of stream) { processBatch(batch); }

await db.transaction(async (tx) => {
  const user = await tx.query(`insert User { ... }`, params);
  await tx.query(`insert Post { author := $user, ... }`, { user: user.id });
});

await db.migrate('./schema.powql');
```

### 7.2 Python driver

```python
from powdb import connect

db = connect("pow://localhost:5433/mydb")
users = db.query("User filter .age > $age", age=30)
for batch in db.stream("User", batch_size=5000): process(batch)
df = db.sql("SELECT name, age FROM users WHERE age > 30")  # PG compat
```

### 7.3 TurboLang (compile-time integration)

```turbolang
import pow from "powdb"

// Parsed and compiled at build time — runtime is just plan execution
let users = pow.query(User filter .age > 30 { name, email })

// Type errors are compile errors, not runtime errors
let posts = pow.query(User filter .email = email { name, posts: .posts { title } })
```

---

## 8. Server configuration

```toml
# powdb.toml

[storage]
data_dir = "/var/lib/powdb/data"
wal_dir = "/var/lib/powdb/wal"
page_size = 4096
wal_batch_size = 128
direct_io = true

[server]
listen_addr = "0.0.0.0"
pow_port = 5433
pg_port = 5432
max_connections = 200

[mvcc]
isolation = "snapshot"
undo_retention = "10m"
auto_purge = true

[columnar]
segment_size = 65536
merge_threshold = 0.3
background_merge = true

[indexes]
auto_suggest = true
suggest_threshold = 100

[views]
materialized_refresh = "async"
max_materialized_views = 50
```

---

## 9. Implementation phases

### Phase 1: Core storage engine

Build in Rust (preferred for ecosystem) or Zig (preferred for comptime + simplicity).

Deliverables:
1. **Page manager** — 4KB page allocation, read, write, free list
2. **Compact row format** — encode/decode rows with 2B header + null bitmap
3. **B-tree index** — B+ tree with order 256, insert, lookup, range scan, leaf linking
4. **WAL** — append records, group commit with configurable batch size, checkpoint
5. **Undo-log MVCC** — begin/commit/rollback transactions, snapshot isolation,
   in-place updates with undo chain, background undo purge
6. **Buffer pool** — LRU/clock-sweep page cache, dirty page tracking, flush to disk
7. **C ABI** — expose all of the above through the C function interface

Validation: re-run the 7 benchmark experiments against the native implementation.
The compact row format should match or beat the JavaScript scaffolding numbers.
B-tree should achieve >1M lookups/sec. WAL batch=128 should achieve >50K writes/sec
on production NVMe.

### Phase 2: PowQL compiler

Deliverables:
1. **Lexer + parser** — PowQL text → AST
2. **Type checker** — resolve types against schema, catch errors at compile time
3. **Planner** — AST → logical plan → physical plan (choose index scan vs seq scan,
   choose vectorized vs row-oriented based on data shape)
4. **Plan cache** — hash-based cache of compiled plans
5. **Computed views** — named query storage, view composition
6. **Materialized views** — cached result sets with change tracking for auto-refresh
7. **Migration engine** — schema diffing, plan generation, transactional apply

### Phase 3: Vectorized executor

Deliverables:
1. **Vectorized filter** — batch evaluation of predicates on columnar data
2. **Vectorized aggregation** — SUM, AVG, COUNT, MIN, MAX on typed arrays
3. **Vectorized group-by** — flat array for bounded keys, hash for unbounded
4. **Selection vectors** — filter → gather pattern for multi-column predicates
5. **SIMD kernels** — AVX2/AVX-512 for integer comparisons, aggregations (Linux x86_64)

### Phase 4: Wire protocols + drivers

Deliverables:
1. **Network listener** — TCP + TLS, connection management
2. **Native protocol** — PowQL binary protocol implementation
3. **PostgreSQL wire protocol** — v3 protocol, SQL → PowQL translation layer
4. **TypeScript driver** — npm package @powdb/client
5. **Python driver** — pip package powdb

### Phase 5: TurboLang integration + production hardening

Deliverables:
1. **TurboLang compiler plugin** — parse PowQL at build time, emit plan hashes
2. **Connection pooling** — server-side connection management
3. **Replication** — leader-follower for read scaling
4. **Background columnar merge** — async conversion of hot rows → columnar segments
5. **Monitoring** — query stats, slow query log, resource utilization

---

## 10. Open decisions

These items need to be resolved during implementation but don't block starting:

1. **Implementation language** — Rust vs Zig. Rust has better ecosystem (tokio, io_uring
   crates, great tooling). Zig has comptime (TigerBeetle uses it to great effect) and
   simpler memory model. Recommendation: Rust, unless TurboLang's compiler is already
   in Zig, in which case shared tooling matters.

2. **Embedded mode memory management** — how much RAM does the buffer pool get by
   default? Should it auto-tune based on available system memory?

3. **Replication protocol** — leader-follower with WAL shipping? Or something more
   sophisticated like Raft consensus for multi-leader?

4. **Text search** — should PowDB have built-in full-text search, or leave that to
   external systems (Elasticsearch, Meilisearch)?

5. **Geospatial** — should PowDB support geometric types and spatial indexes (R-tree)?
   Or is PostGIS compatibility through the PG wire protocol sufficient?

6. **JSON support** — the `json` type is an escape hatch for schemaless data. How deep
   should JSON querying go? Path expressions? Indexing into JSON fields?
