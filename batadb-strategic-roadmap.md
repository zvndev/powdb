# BataDB: Strategic roadmap

## What BataDB is

A new database engine built from validated first principles. Every architectural
decision is backed by production benchmarks (Railway, Intel Xeon Icelake, ZFS).

Three products form the stack:
- **BataDB** — the storage engine (embedded library + server)
- **BataQL** — the query language (standalone DSL, not tied to any host language)
- **TurboLang** — first-class client with compile-time superpowers (optional)

TurbineORM (existing Postgres ORM) migrates seamlessly via PostgreSQL wire
protocol compatibility.

## Validated architecture decisions

### Storage engine
| Decision | Choice | Evidence |
|----------|--------|----------|
| Page size | 4KB | Best random read throughput (445K pages/sec production) |
| Row format | Compact (2B overhead/row) | 37% less overhead than PostgreSQL's heap |
| Columnar | Hybrid hot-row + cold-columnar | 45-105x faster for scans and aggregations |
| Indexes | B-tree order 256-512 | Height 3 for 500K rows, 1M lookups/sec |
| MVCC | Undo-log (InnoDB-style) | Zero bloat, 739K updates/sec, no VACUUM |
| WAL | Group commit, batch 128-256 | 49-115K durable writes/sec on production |
| Execution | Vectorized (batch processing) | 4-13x in benchmarks, expect 10-50x native |
| I/O | Direct I/O + io_uring (Linux) | Bypass OS page cache for predictable latency |

### Query language (BataQL)
| Decision | Choice | Rationale |
|----------|--------|-----------|
| Paradigm | Pipeline (filter → shape → aggregate) | Reads in execution order, not SQL's scrambled order |
| Relationships | Links (first-class) + match (escape hatch) | Links use indexes automatically; match is explicit about cost |
| Nullability | Set-based ({value} or {}) | Eliminates three-valued logic, no NULL surprises |
| Composability | Every expression returns a set | Any expression can be input to any other |
| Type safety | Schema-aware at parse time | Renamed columns break at compile time, not runtime |
| Views | First-class computed + materialized | Named queries that compose like types |
| Migrations | Declarative schema diffing | Edit schema file → diff → review → apply (transactional) |
| Indexes | Automatic + manual override + suggestions | Auto for PKs and links, manual for tuning, suggest from patterns |
| Streaming | Cursor-based, auto-materialized for small results | Large results stream in batches |

### Wire protocol
| Mode | Path | Performance |
|------|------|-------------|
| Mode 1: Native BataQL | BataQL text → compile → execute → binary columns | 5-10x over SQL |
| Mode 2: Prepared plans | Pre-compiled plan hash + params → execute | 42x over SQL |
| Mode 3: PG wire compat | SQL → translate → BataQL → compile → execute | ~1x (compat) |

### Engine deployment
| Mode | Description | Use case |
|------|-------------|----------|
| Embedded | C ABI library loaded by application | Development, edge, single-process apps |
| Server | Library + network listener | Multi-client production, managed hosting |

### Driver tiers
| Tier | Language | Integration depth |
|------|----------|-------------------|
| 1 | TurboLang | Compile-time plans, zero-copy types, inline syntax |
| 2 | TypeScript, Python, Rust, Go | BataQL over native protocol, typed drivers |
| 3 | Any PG-compatible tool | SQL via PostgreSQL wire protocol |

## Production benchmark results (Railway)

Environment: Intel Xeon Icelake (shared), ZFS storage, Node.js v22

| Metric | Value | Comparison |
|--------|-------|------------|
| Point lookup (compact format) | 1.06M ops/sec | 42x faster than SQLite prepared |
| Point lookup (columnar) | 8.8M ops/sec | 292x faster than SQLite |
| Single column scan (columnar vs SQLite) | 326μs vs 24,369μs | 75x faster |
| Aggregation AVG (columnar vs SQLite) | 51μs vs 5,341μs | 105x faster |
| B-tree lookup (500K rows) | 1.02M ops/sec | 22x faster than SQLite B-tree |
| Range scan (1000 rows) | 419μs vs 4,893μs | 12x faster than SQLite |
| fsync latency | 221μs | Real production SSD |
| WAL batch=128 | 49,200 writes/sec | Viable for OLTP |
| WAL batch=512 | 114,593 writes/sec | High-throughput writes |
| MVCC updates (undo-log) | 739K/sec | Faster than no-MVCC baseline |
| MVCC bloat (undo-log) | 0% | vs 21% for append-only |
| Vectorized group-by | 859μs vs 11,326μs | 13.2x faster than row-at-a-time |
| Storage overhead (compact) | 5% (2B/row) | vs 37% (28B/row) for PostgreSQL |

## Implementation plan

### Phase 1: Core engine (Rust or Zig)
- Page manager with 4KB pages
- Compact row format + columnar segment format
- B-tree index (order 256)
- WAL with group commit
- Undo-log MVCC
- C ABI for embedding
- Basic vectorized executor (filter, project, aggregate)

### Phase 2: BataQL
- Parser (BataQL text → AST)
- Type checker (schema-aware)
- Compiler (AST → physical plan → engine API calls)
- Plan cache (hash-based)
- Computed views + materialized views
- Declarative migration engine

### Phase 3: Wire protocols
- Native binary protocol (column-oriented results)
- PostgreSQL wire protocol (v3, compatibility)
- TypeScript driver (@batadb/client)
- Python driver

### Phase 4: TurboLang integration
- Compile-time BataQL parsing and plan generation
- Zero-copy type mapping (TurboLang structs = BataDB row format)
- Inline BataQL syntax in TurboLang source files

### Phase 5: Production hardening
- Connection pooling
- Replication (leader-follower)
- Background columnar merge (hot rows → cold columnar segments)
- Index suggestion engine
- Monitoring and observability

## Documents

- [Layer 1 checkpoint](batadb-checkpoint-layer1.md) — benchmark findings and storage decisions
- [BataQL language design](bataql-language-design.md) — query language syntax and semantics
- [Wire protocol design](batadb-wire-protocol.md) — protocol, engine API, driver architecture
- [Experiment code](turbodb-experiments.tar.gz) — 7 benchmark experiments (v0.3)
