# PowQL Language Reference

PowQL is the query language for PowDB, a high-performance embedded database written in Rust. PowQL is designed to be modern, concise, and pipeline-oriented while remaining immediately familiar to anyone who knows SQL.

PowDB beats SQLite on all 15 benchmark workloads (1.2x--10x faster). PowQL is at SQL parity for core operations.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Schema Definition](#schema-definition)
3. [Queries](#queries)
4. [Expressions](#expressions)
5. [Aggregates](#aggregates)
6. [GROUP BY and HAVING](#group-by-and-having)
7. [Joins](#joins)
8. [Set Operations](#set-operations)
9. [Subqueries](#subqueries)
10. [Functions](#functions)
11. [Mutations](#mutations)
12. [DDL](#ddl)
13. [Materialized Views](#materialized-views)
14. [Prepared Queries](#prepared-queries)
15. [Type System](#type-system)
16. [PowQL vs SQL Cheat Sheet](#powql-vs-sql-cheat-sheet)

---

## Quick Start

PowQL reads left to right. You name the table, apply operations, and project fields -- all in one pipeline.

```
-- Define a schema
type User {
  required name: str,
  required email: str,
  age: int
}

-- Insert a row
insert User { name := "Alice", email := "alice@example.com", age := 30 }

-- Scan all users
User

-- Filter, order, limit, project -- one pipeline
User filter .age > 25 order .age desc limit 10 { .name, .age }

-- Count rows matching a condition
count(User filter .age > 30)

-- Group and aggregate
User group .status having count(.name) > 5 { .status, n: count(.name) }
```

**PowQL vs SQL at a glance:**

| PowQL | SQL |
|---|---|
| `User filter .age > 30 { .name }` | `SELECT name FROM User WHERE age > 30` |
| `count(User filter .active = true)` | `SELECT COUNT(*) FROM User WHERE active = true` |
| `User order .age desc limit 5` | `SELECT * FROM User ORDER BY age DESC LIMIT 5` |
| `insert User { name := "Alice" }` | `INSERT INTO User (name) VALUES ('Alice')` |
| `User filter .id = 1 update { age := 31 }` | `UPDATE User SET age = 31 WHERE id = 1` |
| `User filter .id = 1 delete` | `DELETE FROM User WHERE id = 1` |

---

## Schema Definition

Tables are defined using the `type` keyword. Each field has a name and a type, optionally prefixed with `required` to enforce non-null values.

### Syntax

```
type <TableName> {
  [required] <field>: <type>,
  [required] <field>: <type>,
  ...
}
```

### Examples

```
-- A simple user table
type User {
  required name: str,
  required email: str,
  age: int
}

-- A table with all supported types
type Record {
  required id: int,
  required title: str,
  score: float,
  active: bool,
  created_at: datetime,
  ref_id: uuid,
  payload: bytes
}
```

Fields without `required` are nullable -- they can hold empty/null values.

### Supported Types

| Type | Description | Storage |
|---|---|---|
| `str` | UTF-8 text | Variable-length |
| `int` | 64-bit signed integer | 8 bytes fixed |
| `float` | 64-bit floating point (IEEE 754) | 8 bytes fixed |
| `bool` | Boolean (true/false) | 1 byte fixed |
| `datetime` | Timestamp as 64-bit integer (epoch) | 8 bytes fixed |
| `uuid` | 128-bit UUID | 16 bytes fixed |
| `bytes` | Raw binary data | Variable-length |

---

## Queries

Queries in PowQL are pipeline-oriented: start with a table name, then chain operations left to right.

### Full Scan

Read every row from a table:

```
User
```

### Filter

Apply a predicate to keep only matching rows:

```
User filter .age > 30
User filter .name = "Alice"
User filter .age > 25 and .status = "active"
User filter .age < 20 or .age > 60
```

### Projection

Select specific fields using `{ }` braces. Reference fields with the `.field` dot syntax:

```
User { .name, .email }
User filter .age > 30 { .name, .age }
```

Projections can include aliases:

```
User { full_name: .name, years: .age }
```

Projections can include computed expressions:

```
User { .name, double_age: .age * 2 }
User { .name, info: concat(.name, " age=", .age) }
```

### Ordering

Sort results using `order` with one or more fields. Default direction is ascending. Use `asc` or `desc` explicitly:

```
User order .age
User order .age desc
User order .name asc
User order .age asc, .name desc
```

### Limit and Offset

Restrict the number of returned rows and skip rows:

```
User limit 10
User order .age desc limit 5
User order .age offset 20 limit 10
```

### Distinct

Remove duplicate rows from the result:

```
User distinct { .name }
User filter .age > 20 distinct { .status }
```

### Pipeline Composition

Operations compose naturally left to right. The full pipeline order is:

```
<Table> [distinct] [filter <expr>] [group <keys> [having <expr>]] [order <keys>] [limit <n>] [offset <n>] { <projection> }
```

A complete example:

```
User filter .age > 18 order .name asc limit 100 offset 20 { .name, .email, .age }
```

SQL equivalent:

```sql
SELECT name, email, age FROM User WHERE age > 18 ORDER BY name ASC LIMIT 100 OFFSET 20
```

---

## Expressions

PowQL supports a full expression language for filters, projections, and assignments.

### Field References

Fields are referenced with a dot prefix:

```
.name
.age
.email
```

In join queries, use qualified references with the alias:

```
u.name
o.total
```

### Literals

| Type | Examples |
|---|---|
| Integer | `42`, `-7`, `0` |
| Float | `3.14`, `-0.5` |
| String | `"hello"`, `"Alice"` |
| Boolean | `true`, `false` |

### Parameters

Query parameters are prefixed with `$` and bound at execution time:

```
User filter .age > $min_age
User filter .name = $target
insert User { name := $name, email := $email, age := $age }
```

Parameters enable safe, reusable queries without string interpolation. See [Prepared Queries](#prepared-queries) for the execution API.

### Comparison Operators

| Operator | Meaning |
|---|---|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

```
User filter .age > 30
User filter .name = "Alice"
User filter .score != 0
```

### Arithmetic Operators

| Operator | Meaning | Precedence |
|---|---|---|
| `*` | Multiply | Higher |
| `/` | Divide | Higher |
| `+` | Add | Lower |
| `-` | Subtract | Lower |

Standard precedence applies -- `*` and `/` bind tighter than `+` and `-`:

```
User { .name, double_age: .age * 2 }
User filter .age / 10 > 2
User filter .price * .quantity > 100
User filter .a + .b * .c > 0   -- parsed as .a + (.b * .c)
```

Use parentheses to override precedence:

```
User filter (.a + .b) * .c > 0
```

### Logical Operators

| Operator | Meaning |
|---|---|
| `and` | Logical AND |
| `or` | Logical OR |
| `not` | Logical NOT |

```
User filter .age > 25 and .status = "active"
User filter .age < 20 or .age > 60
User filter not .active
```

### NULL Checks

```
User filter .age is null
User filter .age is not null
User filter .age is null and .name = "Diana"
```

### IN Lists

Check if a value is in a set of literals:

```
User filter .name in ("Alice", "Bob")
User filter .age in (25, 30, 35)
User filter .name not in ("Alice")
User filter .age not in (1, 2, 3)
```

### BETWEEN

Range check (inclusive on both ends). Desugars to `>= low AND <= high`:

```
User filter .age between 25 and 35
User filter .age not between 10 and 20
```

### LIKE

Pattern matching with `%` (any sequence) and `_` (single character):

```
User filter .name like "Ali%"        -- starts with "Ali"
User filter .name like "_ob"         -- 3 chars ending in "ob"
User filter .name like "Alice"       -- exact match
User filter .name not like "A%"      -- does NOT start with "A"
```

### Coalesce

The `??` operator returns the left operand if non-null, otherwise the right:

```
User { .name, display_age: .age ?? 0 }
```

### Operator Precedence

From highest to lowest binding:

| Precedence | Operators |
|---|---|
| 1 (tightest) | `*`, `/` |
| 2 | `+`, `-`, `??` |
| 3 | `=`, `!=`, `<`, `>`, `<=`, `>=`, `like`, `in`, `between`, `is null`, `is not null` |
| 4 | `not` |
| 5 | `and` |
| 6 (loosest) | `or` |

Use parentheses to override: `(.age > 25 or .role = "admin") and .active = true`

---

## Aggregates

PowQL supports five aggregate functions. They wrap a query in a function-call syntax.

### Standalone Aggregates

```
count(User)                              -- count all rows
count(User filter .age > 30)             -- count with filter
sum(User { .age })                       -- sum a column
sum(User filter .age > 30 { .age })      -- sum with filter
avg(User { .age })                       -- average
min(User { .age })                       -- minimum
max(User { .age })                       -- maximum
```

### count(distinct ...)

Count unique values in a column:

```
count(distinct User { .name })
count(distinct User { .age })
```

### Aggregate Functions Reference

| Function | Description | Syntax |
|---|---|---|
| `count` | Number of rows | `count(Table [filter ...])` |
| `count(distinct ...)` | Number of unique values | `count(distinct Table { .field })` |
| `sum` | Sum of numeric column | `sum(Table { .field })` |
| `avg` | Average of numeric column | `avg(Table { .field })` |
| `min` | Minimum value | `min(Table { .field })` |
| `max` | Maximum value | `max(Table { .field })` |

For `sum`, `avg`, `min`, and `max`, the target column is specified via the projection `{ .field }`. For `count`, the projection is optional.

---

## GROUP BY and HAVING

Group rows by one or more keys and compute aggregate values per group.

### Syntax

```
<Table> [filter ...] group <.key1>, <.key2> [having <expr>] { <.key>, <agg(...)> }
```

### Basic Grouping

```
-- Count users per name
User group .name { .name, n: count(.name) }

-- Group by multiple keys
User group .status, .age { .status, .age }
```

### Aggregates in GROUP BY Projections

Inside a GROUP BY projection, you can use any aggregate function:

```
User group .status {
  .status,
  total: count(.name),
  avg_age: avg(.age),
  youngest: min(.age),
  oldest: max(.age),
  total_age: sum(.age)
}
```

### count(*) in GROUP BY

Count all rows per group including nulls:

```
User group .age { .age, count(*) }
```

### count(distinct) in GROUP BY

Count distinct values within each group:

```
Sale group .dept { .dept, count(distinct .item) }
```

### HAVING

Filter groups after aggregation:

```
User group .status having count(.name) > 5 { .status, n: count(.name) }
User group .age having count(*) > 1 { .age, count(*) }
```

### Filter + Group

Filter rows before grouping:

```
User filter .age >= 30 group .name { .name, n: count(.name) }
```

SQL equivalent:

```sql
SELECT name, COUNT(name) AS n FROM User WHERE age >= 30 GROUP BY name
```

---

## Joins

PowQL supports inner, left outer, right outer, and cross joins. Aliases are used to disambiguate fields from different tables.

### Syntax

```
<Table1> as <alias1> [inner|left|right|cross] join <Table2> as <alias2> on <expr>
```

### Inner Join

Returns only rows that match in both tables. `join` without a modifier defaults to inner:

```
User as u join Order as o on u.id = o.user_id
User as u inner join Order as o on u.id = o.user_id
```

### Left Outer Join

Returns all rows from the left table. Unmatched right-side columns are null:

```
User as u left join Order as o on u.id = o.user_id
User as u left outer join Order as o on u.id = o.user_id
```

### Right Outer Join

Returns all rows from the right table. Unmatched left-side columns are null:

```
User as u right join Order as o on u.id = o.user_id
```

### Cross Join

Produces the Cartesian product of both tables. No `on` clause:

```
User as u cross join Product as p
```

### Qualified Field References

In join queries, reference fields with the alias prefix:

```
User as u join Order as o on u.id = o.user_id { u.name, o.total }
```

### Filter and Projection on Joins

Joins compose with the full query pipeline:

```
User as u join Order as o on u.id = o.user_id
  filter o.total > 75 { u.name, o.total }
```

### Multi-Table Joins

Chain multiple joins left to right:

```
User as u join Order as o on u.id = o.user_id
  join Product as p on o.product_id = p.id
```

```
User as u join Order as o on u.id = o.user_id
  cross join Product as p
```

### Hash Join vs Nested Loop

PowQL automatically selects the best join strategy:

- **Hash join** (O(L + R)) -- used for equi-joins (`a.col = b.col`)
- **Nested loop** (O(L x R)) -- fallback for non-equi predicates or cross joins

No hint syntax is needed; the engine detects the optimal path.

---

## Set Operations

### UNION

Combine results from two queries, removing duplicates:

```
User filter .dept = "eng" union User filter .dept = "sales"
A union B
```

### UNION ALL

Combine results keeping all duplicates:

```
X union all Y
```

### Chaining

UNION is left-associative and can be chained:

```
T1 union T2 union T3
```

### With Filters

Each side of a UNION can have its own filter/projection pipeline:

```
User filter .age > 50 union User filter .status = "vip"
```

---

## Subqueries

### IN Subquery

Filter rows where a field's value exists in the result of another query:

```
User filter .name in (VIP { .name })
User filter .name in (VIP filter .active = true { .name })
```

### NOT IN Subquery

Exclude rows where a field's value exists in another query's result:

```
User filter .name not in (VIP { .name })
User filter .id not in (Order { .user_id })
```

### Subquery with Filter

The subquery can include its own pipeline:

```
User filter .name in (Score filter .points > 70 { .name }) { .name }
```

SQL equivalent:

```sql
SELECT name FROM User WHERE name IN (SELECT name FROM Score WHERE points > 70)
```

### EXISTS / NOT EXISTS

Check whether a subquery returns any rows:

```
User filter exists (Order filter .user_id = 1)
User filter not exists (Order filter .status = "pending")
```

`exists` evaluates to true if the inner query matches at least one row. `not exists` is the negation.

---

## Functions

### Scalar Functions

Scalar functions operate on individual values and can be used in projections and filters.

#### upper / lower

Convert string to upper or lower case:

```
User filter upper(.name) = "ALICE"
User { low: lower(.email) }
```

#### length

Return the character length of a string:

```
User { .name, len: length(.name) }
```

#### trim

Remove leading and trailing whitespace:

```
User { clean: trim(.name) }
```

#### substring

Extract a substring. Arguments: `(expr, start, length)` -- 1-indexed:

```
User { sub: substring(.name, 1, 3) }
-- Alice -> "Ali", Bob -> "Bob", Charlie -> "Cha"
```

#### concat

Concatenate multiple values. Non-string types are coerced to strings:

```
User { full: concat(.name, " - ", .email) }
-- "Alice - alice@example.com"

User { info: concat(.name, " age=", .age) }
-- "Alice age=30"
```

### CASE WHEN

Conditional expression with multiple branches:

```
User {
  .name,
  label: case
    when .age > 30 then "senior"
    when .age >= 30 then "exactly30"
    else "young"
  end
}
```

CASE in a filter:

```
User filter case when .age > 30 then true else false end
```

CASE without ELSE returns null (Empty) when no branch matches:

```
User { .name, label: case when .age > 100 then "old" end }
-- all labels will be null since no one is over 100
```

---

## Mutations

### INSERT

Insert a single row. Fields are assigned with `:=`:

```
insert User { name := "Alice", email := "alice@example.com", age := 30 }
insert User { name := "Bob", email := "bob@example.com" }
```

Omitted fields default to null. Required fields must be provided.

### UPDATE

Update rows matching an optional filter. Supports both literal values and expressions:

```
-- Set a literal value
User filter .name = "Alice" update { age := 31 }

-- Update with an expression referencing the current row
User filter .name = "Alice" update { age := .age + 5 }

-- Update all rows
User update { age := .age * 2 }

-- Arithmetic in update
User filter .age > 28 update { age := .age + 1 }
```

### DELETE

Delete rows matching an optional filter:

```
User filter .name = "Bob" delete
User filter .age < 18 delete
User filter .age > 60 delete
```

Delete all rows (use with care):

```
User delete
```

---

## DDL

### CREATE TABLE (type)

Create a new table. See [Schema Definition](#schema-definition):

```
type User {
  required name: str,
  required email: str,
  age: int
}
```

### ALTER TABLE

Add or drop columns on an existing table.

#### Add Column

```
alter User add column status: str
alter User add required active: bool
alter User add status: str                 -- "column" keyword is optional
```

#### Drop Column

```
alter User drop column email
alter User drop email                      -- "column" keyword is optional
```

### DROP TABLE

Remove a table entirely:

```
drop User
```

---

## Materialized Views

Materialized views store the result of a query as a physical table. PowDB automatically refreshes views when underlying data changes.

### Create

Define a view with `materialize ... as`:

```
materialize OldUsers as User filter .age > 28
materialize UserNames as User { .name }
materialize ActiveUsers as User filter .status = "active" { .name, .email }
```

### Query

Query a materialized view exactly like a table:

```
OldUsers
OldUsers filter .name = "Alice"
count(OldUsers)
```

### Auto-Refresh

When the underlying table changes (insert, update, or delete), PowDB marks dependent views as dirty. The next time you query a dirty view, it is automatically refreshed before returning results. No stale reads.

### Manual Refresh

Force a refresh explicitly:

```
refresh OldUsers
```

### Drop

Remove a materialized view:

```
drop view OldUsers
```

Note: `drop view` removes the view. Plain `drop` (without `view`) drops a table.

---

## Prepared Queries

PowDB supports prepared queries for high-performance repeated operations. The query is parsed and planned once, then executed repeatedly with different literal values.

### How It Works

1. **Prepare** -- Parse and plan the query once. The engine counts the literal slots.
2. **Execute** -- Supply new literal values for each execution. The engine substitutes them into the cached plan.

Prepared queries skip the lexer, parser, planner, and plan-cache lookup on every execution after the first. This is PowDB's equivalent of SQLite's `prepare_cached`.

### Literal Slot Order

Literals are substituted in the order they appear in the source query, left to right. For example:

```
insert User { name := "seed", email := "seed@ex.com", age := 0 }
-- 3 literal slots: [0] = name, [1] = email, [2] = age
```

```
User filter .name = "seed" update { age := 0 }
-- 2 literal slots: [0] = filter value, [1] = assignment value
```

### Fast Paths

PowDB detects common prepared-query shapes and optimizes them:

- **Insert fast path** -- When all assignment values are plain literals, column indices are resolved once at prepare time. Each execution builds the row directly from the literal slice with zero plan cloning.
- **Point update fast path** -- When the query is `T filter .pk = ? update { col := ? }` with an indexed primary key and a fixed-size target column, the engine performs a single B-tree lookup and patches raw bytes in place. No plan clone, no allocations.

### API Usage (Rust)

```rust
let prep = engine.prepare(
    r#"insert User { name := "x", email := "x@e.com", age := 0 }"#
)?;

for i in 0..1000 {
    engine.execute_prepared(&prep, &[
        Literal::String(format!("user{i}")),
        Literal::String(format!("u{i}@ex.com")),
        Literal::Int(20 + i),
    ])?;
}
```

---

## Type System

PowQL has seven data types plus a null representation.

| Type | PowQL Name | Rust Mapping | Size | Description |
|---|---|---|---|---|
| Integer | `int` | `i64` | 8 bytes | 64-bit signed integer |
| Float | `float` | `f64` | 8 bytes | IEEE 754 double precision |
| Boolean | `bool` | `bool` | 1 byte | `true` or `false` |
| String | `str` | `String` | Variable | UTF-8 text |
| DateTime | `datetime` | `i64` (epoch) | 8 bytes | Unix timestamp |
| UUID | `uuid` | `[u8; 16]` | 16 bytes | 128-bit identifier |
| Bytes | `bytes` | `Vec<u8>` | Variable | Raw binary data |
| Null | (empty) | `Value::Empty` | 0 bytes | Absence of a value |

### Nullability

- Fields marked `required` in the schema cannot be null.
- All other fields are nullable by default.
- Use `is null` / `is not null` to check for null values.
- The `??` coalesce operator provides a fallback for null values.
- Aggregate functions skip null values (except `count(*)` which counts all rows).

### Type Coercion

- `concat` coerces all arguments to strings: `concat(.name, " age=", .age)` produces `"Alice age=30"`.
- Arithmetic on mixed int/float promotes to float.
- Comparisons between incompatible types evaluate to false.

---

## PowQL vs SQL Cheat Sheet

| Operation | PowQL | SQL |
|---|---|---|
| **Select all** | `User` | `SELECT * FROM User` |
| **Select columns** | `User { .name, .age }` | `SELECT name, age FROM User` |
| **Alias** | `User { full_name: .name }` | `SELECT name AS full_name FROM User` |
| **Where** | `User filter .age > 30` | `SELECT * FROM User WHERE age > 30` |
| **AND / OR** | `User filter .a > 1 and .b < 5` | `... WHERE a > 1 AND b < 5` |
| **Order** | `User order .age desc` | `... ORDER BY age DESC` |
| **Multi-sort** | `User order .age asc, .name desc` | `... ORDER BY age ASC, name DESC` |
| **Limit** | `User limit 10` | `... LIMIT 10` |
| **Offset** | `User offset 20 limit 10` | `... LIMIT 10 OFFSET 20` |
| **Distinct** | `User distinct { .name }` | `SELECT DISTINCT name FROM User` |
| **Count** | `count(User)` | `SELECT COUNT(*) FROM User` |
| **Count where** | `count(User filter .age > 30)` | `SELECT COUNT(*) FROM User WHERE age > 30` |
| **Count distinct** | `count(distinct User { .name })` | `SELECT COUNT(DISTINCT name) FROM User` |
| **Sum** | `sum(User { .age })` | `SELECT SUM(age) FROM User` |
| **Avg** | `avg(User { .age })` | `SELECT AVG(age) FROM User` |
| **Min / Max** | `min(User { .age })` | `SELECT MIN(age) FROM User` |
| **Group By** | `User group .status { .status, count(.name) }` | `SELECT status, COUNT(name) FROM User GROUP BY status` |
| **Having** | `User group .status having count(.name) > 5 { .status }` | `... GROUP BY status HAVING COUNT(name) > 5` |
| **Inner Join** | `User as u join Order as o on u.id = o.user_id` | `SELECT * FROM User u JOIN Order o ON u.id = o.user_id` |
| **Left Join** | `User as u left join Order as o on u.id = o.user_id` | `... LEFT JOIN Order o ON u.id = o.user_id` |
| **Right Join** | `User as u right join Order as o on u.id = o.user_id` | `... RIGHT JOIN Order o ON u.id = o.user_id` |
| **Cross Join** | `User as u cross join Product as p` | `... CROSS JOIN Product p` |
| **IN list** | `User filter .age in (25, 30)` | `... WHERE age IN (25, 30)` |
| **NOT IN** | `User filter .name not in ("Alice")` | `... WHERE name NOT IN ('Alice')` |
| **IN subquery** | `User filter .name in (VIP { .name })` | `... WHERE name IN (SELECT name FROM VIP)` |
| **BETWEEN** | `User filter .age between 20 and 30` | `... WHERE age BETWEEN 20 AND 30` |
| **LIKE** | `User filter .name like "A%"` | `... WHERE name LIKE 'A%'` |
| **IS NULL** | `User filter .age is null` | `... WHERE age IS NULL` |
| **IS NOT NULL** | `User filter .age is not null` | `... WHERE age IS NOT NULL` |
| **Coalesce** | `.age ?? 0` | `COALESCE(age, 0)` |
| **CASE WHEN** | `case when .age > 30 then "old" else "young" end` | `CASE WHEN age > 30 THEN 'old' ELSE 'young' END` |
| **UNION** | `A union B` | `A UNION B` |
| **UNION ALL** | `A union all B` | `A UNION ALL B` |
| **Insert** | `insert User { name := "Alice", age := 30 }` | `INSERT INTO User (name, age) VALUES ('Alice', 30)` |
| **Update** | `User filter .id = 1 update { age := 31 }` | `UPDATE User SET age = 31 WHERE id = 1` |
| **Update expr** | `User update { age := .age + 1 }` | `UPDATE User SET age = age + 1` |
| **Delete** | `User filter .id = 1 delete` | `DELETE FROM User WHERE id = 1` |
| **Create table** | `type User { required name: str }` | `CREATE TABLE User (name TEXT NOT NULL)` |
| **Drop table** | `drop User` | `DROP TABLE User` |
| **Alter add** | `alter User add column status: str` | `ALTER TABLE User ADD COLUMN status TEXT` |
| **Alter drop** | `alter User drop column status` | `ALTER TABLE User DROP COLUMN status` |
| **Create view** | `materialize V as User filter .active = true` | `CREATE MATERIALIZED VIEW V AS SELECT * FROM User WHERE active` |
| **Refresh view** | `refresh V` | `REFRESH MATERIALIZED VIEW V` |
| **Drop view** | `drop view V` | `DROP MATERIALIZED VIEW V` |
| **Upper** | `upper(.name)` | `UPPER(name)` |
| **Lower** | `lower(.email)` | `LOWER(email)` |
| **Length** | `length(.name)` | `LENGTH(name)` |
| **Trim** | `trim(.name)` | `TRIM(name)` |
| **Substring** | `substring(.name, 1, 3)` | `SUBSTRING(name, 1, 3)` |
| **Concat** | `concat(.name, " ", .email)` | `CONCAT(name, ' ', email)` |

### Key Syntactic Differences

| Concept | PowQL | SQL |
|---|---|---|
| Field reference | `.field` (dot prefix) | `field` (bare identifier) |
| Assignment | `:=` | `=` or `SET col = val` |
| Table definition | `type Name { ... }` | `CREATE TABLE Name (...)` |
| Required/NOT NULL | `required field: type` | `field TYPE NOT NULL` |
| String literals | `"double quotes"` | `'single quotes'` |
| Query shape | Pipeline: `Table verb verb { proj }` | Clausal: `SELECT proj FROM Table WHERE ... ORDER BY ...` |
| Aggregates | Wrapping: `count(Table filter ...)` | Inline: `SELECT COUNT(*) FROM Table WHERE ...` |
| Materialized views | `materialize Name as Query` | `CREATE MATERIALIZED VIEW Name AS Query` |
