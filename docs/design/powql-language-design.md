# PowQL: Query language design

## Why not just use SQL?

SQL was designed in 1974 for non-programmers to query relational data.
50 years later, it has real problems that can't be fixed without breaking compatibility:

1. **String-based** — queries are text that gets parsed at runtime. Every query pays
   the parse → analyze → plan → optimize pipeline. Our benchmarks showed this costs 20-42x.

2. **Not composable** — you can't take a SQL expression and use it as part of another
   expression naturally. Subqueries exist but they're awkward. There's no `let` binding,
   no piping, no function composition.

3. **NULL is broken** — SQL uses three-valued logic (true/false/unknown). NULL propagates
   silently through expressions. `WHERE age > 30` silently drops rows where age is NULL.
   `NULL = NULL` is not true. This causes more bugs than any other SQL feature.

4. **Joins are inside-out** — you describe the Cartesian product you want filtered, not
   the traversal you want to make. `SELECT * FROM users JOIN posts ON users.id = posts.user_id`
   when what you mean is "give me users and their posts."

5. **No type safety** — column names are strings, types are checked at runtime. Rename a
   column and queries break silently at runtime, not at compile time.

6. **Ordering is wrong** — `SELECT` comes first but executes last. The actual execution
   order is FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT. This confuses
   every beginner and makes autocomplete harder.

## Design principles

PowQL should be:

1. **Composable** — every expression returns a set. Any expression can be the input to
   any other expression. No special cases.

2. **Links, not joins** — relationships are first-class. `user.posts` traverses a link.
   No ON clauses, no Cartesian products.

3. **Typed** — every expression has a known type at parse time. The schema is part of the
   language. Column references that don't exist are compile errors, not runtime errors.

4. **Set-based nullability** — a missing value is an empty set `{}`, not NULL. An optional
   field returns `{value}` or `{}`. This eliminates three-valued logic entirely. You test
   for missing values with `exists` or provide defaults with `??`.

5. **Pipeline order** — expressions read left to right in execution order.
   Filter first, then shape, then aggregate. Not SELECT...FROM...WHERE.

6. **Compiles to typed operations** — PowQL doesn't get interpreted. It compiles to a
   physical plan that maps directly to storage engine operations. When used from TurboLang,
   this compilation happens at build time.

## Syntax proposal

### Schema definition

```powql
type User {
  required name: str
  required email: str
  age: int              # optional — returns {} if missing
  
  multi link posts -> Post     # one-to-many
  link company -> Company      # one-to-one (optional)
}

type Post {
  required title: str
  required body: str
  required created_at: datetime
  required link author -> User  # required link
}

type Company {
  required name: str
  multi link employees -> User
}
```

### Basic queries

```powql
# Get all users
User

# Filter
User filter .age > 30

# Select specific fields (projection)
User { name, email }

# Filter + project
User filter .age > 30 { name, email, age }

# Order + limit
User order .name limit 10

# Full pipeline
User 
  filter .age > 30
  order .name desc
  limit 10
  { name, email, age }
```

### Links (not joins)

```powql
# Get users with their posts
User { name, posts: .posts { title, created_at } }

# Get users who have posts
User filter count(.posts) > 0

# Get users with their company name
User { name, company_name: .company.name }

# Deep traversal
User { 
  name, 
  posts: .posts { 
    title, 
    # reverse link — who else works at the same company?
    coworkers: .author.company.employees { name }
  }
}
```

### Aggregations

```powql
# Count all users
count(User)

# Average age of users over 30
User filter .age > 30 | avg(.age)

# Group by
User 
  group .company.name {
    company: .key,
    headcount: count(.),
    avg_age: avg(.age),
  }

# Group by with having
User 
  group .company.name 
  filter count(.) > 5
  {
    company: .key,
    headcount: count(.),
  }
```

### Set-based nullability

```powql
# .age returns {} if not set, {value} if set

# Provide default
User { name, age: .age ?? 0 }

# Filter only users who HAVE an age
User filter exists .age

# Filter users who DON'T have an age
User filter not exists .age

# This is NEVER ambiguous. No three-valued logic.
# "User filter .age > 30" returns users where age exists AND is > 30.
# Users with no age are simply not in the result set — no NULL surprise.
```

### Computed fields and let bindings

```powql
# Computed field
User {
  name,
  age,
  is_senior: .age > 65,
}

# Let binding for reuse
let active_users := User filter .last_login > datetime('2024-01-01')

active_users { name, email }
count(active_users)
active_users filter .age > 30
```

### Mutations

```powql
# Insert
insert User { name := "Alice", email := "alice@example.com", age := 30 }

# Insert with link
insert Post { 
  title := "Hello", 
  body := "World",
  created_at := now(),
  author := (User filter .email = "alice@example.com"),
}

# Update
User filter .email = "alice@example.com"
  update { age := 31 }

# Update with computation
User filter .age > 0
  update { age := .age + 1 }

# Delete
User filter .last_login < datetime('2020-01-01')
  delete

# Upsert (insert or update)
User upsert on .email = "alice@example.com" {
  name := "Alice Updated",
  email := "alice@example.com",
  age := 31,
}
```

### Transactions

```powql
transaction {
  let alice := insert User { name := "Alice", email := "alice@ex.com" }
  insert Post { title := "First post", author := alice, ... }
}
```

## How this maps to the storage engine

Each PowQL operation maps directly to engine operations:

| PowQL | Engine operation |
|--------|-----------------|
| `User` | Sequential scan of User table |
| `filter .age > 30` | Filter operator (vectorized on columnar segment) |
| `{ name, email }` | Projection (columnar: read only name + email columns) |
| `.posts` | Index lookup on posts.author_id → user.id |
| `order .name` | Sort operator (or index scan if name is indexed) |
| `limit 10` | Early termination |
| `count(User)` | Aggregation (vectorized on columnar) |
| `group .company.name` | Hash-group or sort-group (vectorized) |
| `insert User {...}` | B-tree insert + WAL append |
| `update { age := 31 }` | In-place update + undo-log entry |

The key insight: **there's no parse → plan → optimize at runtime for known queries.**

When PowQL is compiled (especially from TurboLang), the compiler:
1. Resolves all type references at build time
2. Chooses index scan vs seq scan based on statistics
3. Emits a direct execution plan — a sequence of engine API calls
4. The "query" at runtime is just function calls into the storage engine

This is the 42x gap we measured — eliminating the SQL translation layers entirely.

## Comparison with prior art

| Feature | SQL | EdgeQL | GraphQL | PowQL |
|---------|-----|--------|---------|--------|
| Composable | No (subqueries) | Yes | Partial | Yes |
| Links vs joins | Joins | Links | Links | Links |
| NULL handling | Three-valued | Sets | Nullable types | Sets |
| Type safe | No | Yes | Yes (schema) | Yes |
| Execution order | Scrambled | Left to right | Declarative | Left to right |
| Compiles to plan | No | Partially | No | Yes |
| Mutations | Separate syntax | Integrated | Separate | Integrated |

PowQL is most similar to EdgeQL (from EdgeDB/Gel) in philosophy but diverges on:
- Pipeline syntax (`filter` / `order` / `limit` as chained operators, not keywords)
- Explicit set-based nullability with `??` and `exists`
- First-class compilation to physical plans
- No runtime query parser needed when used from a compiled language

## Wire protocol

Three access modes:

### Mode 1: Native binary protocol (fastest)
PowQL text → server-side compile → execute → binary result
The compiled plan is cached. Subsequent calls with same query shape skip compilation.
Result format: column-oriented binary (no JSON parsing overhead).

### Mode 2: Prepared operations (compiled languages)
TurboLang/Rust/C compile PowQL at build time → send plan hash + parameters → execute.
No parsing or compilation on the server. Just parameter binding + execute.
This is the 42x path.

### Mode 3: PostgreSQL wire protocol (compatibility)
SQL text → translate to PowQL internally → execute → PostgreSQL result format.
Slowest path but enables Grafana, pgAdmin, BI tools, existing ORMs.
The translation layer adds the overhead we measured (20-42x) but that's the price
of compatibility.

## What this means for PowDB's implementation

The engine needs to expose a typed API, not a text API:

```
// Pseudocode — the engine's internal interface
fn scan(table: TableId) -> RowIterator
fn filter(input: RowIterator, predicate: CompiledExpr) -> RowIterator
fn project(input: RowIterator, columns: &[ColumnId]) -> RowIterator
fn aggregate(input: RowIterator, agg: AggOp, column: ColumnId) -> Scalar
fn index_lookup(index: IndexId, key: Value) -> RowIterator
fn insert(table: TableId, row: TypedRow) -> RowId
fn update(table: TableId, rowId: RowId, changes: TypedRow) -> void
```

PowQL compiles down to calls against this API. The SQL compatibility layer
also compiles down to this API but through a longer path (parse SQL → analyze →
map to PowQL → compile → engine API calls).

## Design decisions

### 1. Ad-hoc joins: the `match` operator

Links are the default and fast path. For ad-hoc cross-table comparisons that
aren't predeclared relationships, use `match`:

```powql
# Predeclared link (fast — engine knows the index)
User { name, posts: .posts { title } }

# Ad-hoc match (explicit about cost — may or may not have an index)
User as u match Employee as e on u.email = e.personal_email
  { user_name: u.name, employee_name: e.name }

# Self-match (find users who share a birthday)
User as a match User as b on a.birthday = b.birthday and a.id != b.id
  { a.name, b.name }
```

`match` makes the cost visible. The engine will use indexes when available but
the developer knows this is an expensive operation, unlike SQL where every JOIN
looks identical regardless of whether it's indexed.

### 2. Migrations: declarative schema diffing

The developer maintains a single `schema.powql` file that IS the schema. To migrate,
you change the file and PowDB diffs the current state against the desired state,
generates a migration plan, and asks for confirmation before applying.

```bash
# Developer edits schema.powql — adds a field, removes a type, etc.
powdb migrate --plan          # shows what would change (dry run)
powdb migrate --apply         # applies the changes

# Under the hood:
# 1. Read current schema from the database
# 2. Read desired schema from schema.powql
# 3. Diff them — compute ADD COLUMN, DROP TYPE, CREATE INDEX, etc.
# 4. Generate a reversible migration plan
# 5. Show the plan to the developer for review
# 6. Apply within a transaction (rollback if anything fails)
```

Why declarative diffing (not manual migration files):
- **Lowest risk**: the database always knows its current state. No drift between
  migration files and reality. No "migration 47 was applied but 48 failed halfway."
- **Best DX**: you never write ALTER TABLE by hand. Just change the schema file.
- **Sturdy**: every migration is a transaction. If step 3 of 5 fails, everything
  rolls back. The database is never in a half-migrated state.
- **Reviewable**: the plan is shown before applying. Destructive changes (drop column,
  drop type) require explicit confirmation.

Edge cases handled:
- **Rename detection**: if a field disappears and a new one appears with the same type,
  ask "did you rename X to Y?" instead of dropping + creating.
- **Data migrations**: for changes that need data transformation (change a string column
  to an int), the tool generates a data migration step and shows it for review.
- **Irreversible changes**: dropping data is flagged clearly. The plan shows exactly
  what data would be lost.

Migration history is stored in the database itself, so you can always see what
changed and when:

```powql
# Built-in migration history
select PowDB.migrations order .applied_at desc
```

### 3. Indexes: automatic with manual override

The engine automatically creates indexes for:
- Primary keys (always)
- Link targets (the foreign key side of every `link` declaration)
- Fields used in `filter` expressions that appear frequently in compiled queries

Developers can declare additional indexes explicitly:

```powql
type User {
  required name: str
  required email: str
  age: int

  index on .email          # unique index
  index on .age            # range index for filter .age > 30
  index on (.name, .age)   # composite index
}
```

The engine also tracks query patterns and suggests indexes:

```bash
powdb suggest-indexes    # analyzes recent query patterns
# Output:
# Suggested: index on User.age  (used in 847 queries, avg scan 12ms → est. 0.3ms)
# Suggested: index on Post(.created_at, .author)  (used in 234 queries)
```

Why this approach: automatic handles the common cases (you should never manually
index a primary key or a link target — that's just noise). Manual override lets
experts tune for their specific workload. Suggestions bridge the gap for developers
who aren't sure.

### 4. Streaming: cursor-based with automatic materialization

Large result sets stream by default. Small result sets materialize.

```powql
# Returns a cursor — streams rows as you consume them
User filter .age > 30 { name, email }

# Explicit materialization (loads everything into memory)
User filter .age > 30 { name, email } | collect

# Streaming with batch size control
User filter .age > 30 { name, email } | batch 1000
```

The wire protocol supports streaming natively — the server sends result batches
as they're produced, the client consumes them as they arrive. This matters for
large analytical queries where materializing millions of rows would blow memory.

For the common case (small OLTP results under 10K rows), the engine automatically
materializes — the overhead of cursor management isn't worth it for small results.

### 5. Computed views: first-class named queries

Views are named PowQL expressions that behave like types:

```powql
# Define a view
view ActiveUser := User filter .last_login > now() - duration('30d')

view TeamSummary := User
  group .company.name
  { company: .key,
    headcount: count(.),
    avg_age: avg(.age) }

# Use views like types
ActiveUser filter .age > 30 { name, email }
ActiveUser | count
TeamSummary filter .headcount > 10 order .avg_age desc
```

Views compose — a view can reference other views:

```powql
view SeniorActiveUser := ActiveUser filter .age > 50
```

Views can be materialized for performance:

```powql
# Materialized view — engine maintains a cached result set
# Updated automatically when underlying data changes
materialized view DailyStats := Post
  group .created_at.date
  { date: .key,
    post_count: count(.),
    unique_authors: count(distinct .author),
  }
```

Why this is a game changer: views let you build a vocabulary specific to your
domain. Instead of repeating the same filter/group/project pattern everywhere,
you name it once. Other queries compose on top of it. Materialized views give
you precomputed dashboards that stay in sync automatically — no cron jobs,
no stale caches.
