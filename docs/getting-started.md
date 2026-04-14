# Getting Started with PowDB

PowDB is a high-performance database engine with its own query language called PowQL. This tutorial walks you through installing PowDB, creating a table, inserting data, querying it, and running in server mode. It takes about 10 minutes.

---

## 1. Install from Source

PowDB is written in Rust. You need Rust 1.80 or newer. If you don't have it, install it from [rustup.rs](https://rustup.rs/).

```bash
git clone https://github.com/zvndev/powdb.git
cd powdb
cargo build --release
```

This builds the CLI, server, query engine, and storage engine.

---

## 2. Start the REPL

Launch the interactive PowQL shell:

```bash
cargo run --release -p powdb-cli
```

You should see:

```
PowDB v0.1.0 — embedded mode
Data directory: ./powdb_data
Type PowQL queries. Use Ctrl-D to exit.

powql>
```

Data is stored in `./powdb_data/` by default. You can change it with `--data-dir`:

```bash
cargo run --release -p powdb-cli -- --data-dir ./my_project_data
```

---

## 3. Create a Table

PowDB uses the `type` keyword to define tables. Fields prefixed with `required` cannot be null.

```
powql> type User { required name: str, required email: str, age: int }
```

Output:

```
type User created
```

That's it. No `CREATE TABLE`, no column types in parentheses. Fields have a name and a type, separated by a colon.

Supported types: `str`, `int`, `float`, `bool`, `datetime`, `uuid`, `bytes`.

---

## 4. Insert Data

Insert rows with the `insert` keyword. Fields are assigned with `:=`.

```
powql> insert User { name := "Alice", email := "alice@example.com", age := 30 }
```

Output:

```
1 row affected
```

Let's add a few more people:

```
powql> insert User { name := "Bob", email := "bob@example.com", age := 25 }
1 row affected

powql> insert User { name := "Charlie", email := "charlie@example.com", age := 35 }
1 row affected

powql> insert User { name := "Diana", email := "diana@example.com", age := 28 }
1 row affected

powql> insert User { name := "Eve", email := "eve@example.com", age := 22 }
1 row affected

powql> insert User { name := "Frank", email := "frank@example.com", age := 40 }
1 row affected
```

Fields without `required` can be omitted -- they default to null:

```
powql> insert User { name := "Grace", email := "grace@example.com" }
1 row affected
```

---

## 5. Query Basics

### Select all rows

Just type the table name:

```
powql> User
```

Output:

```
 name    | email                | age
---------+----------------------+----
 Alice   | alice@example.com    | 30
 Bob     | bob@example.com      | 25
 Charlie | charlie@example.com  | 35
 Diana   | diana@example.com    | 28
 Eve     | eve@example.com      | 22
 Frank   | frank@example.com    | 40
 Grace   | grace@example.com    | {}
(7 rows)
```

The `{}` for Grace's age means null (she was inserted without an age).

### Filter rows

Use `filter` with a condition. Fields are referenced with a dot prefix:

```
powql> User filter .age > 25
```

Output:

```
 name    | email                | age
---------+----------------------+----
 Alice   | alice@example.com    | 30
 Charlie | charlie@example.com  | 35
 Diana   | diana@example.com    | 28
 Frank   | frank@example.com    | 40
(4 rows)
```

### Project specific fields

Use `{ }` braces to select which fields to return:

```
powql> User { .name, .age }
```

Output:

```
 name    | age
---------+----
 Alice   | 30
 Bob     | 25
 Charlie | 35
 Diana   | 28
 Eve     | 22
 Frank   | 40
 Grace   | {}
(7 rows)
```

You can combine filter and projection:

```
powql> User filter .age > 25 { .name, .age }
```

Output:

```
 name    | age
---------+----
 Alice   | 30
 Charlie | 35
 Diana   | 28
 Frank   | 40
(4 rows)
```

---

## 6. Sorting and Limiting

PowQL operations chain left to right in a pipeline. Add `order` and `limit` to sort and cap the results:

```
powql> User order .age desc limit 3 { .name, .age }
```

Output:

```
 name    | age
---------+----
 Frank   | 40
 Charlie | 35
 Alice   | 30
(3 rows)
```

You can sort ascending (the default) or descending:

```
powql> User order .age asc limit 3 { .name, .age }
```

Output:

```
 name  | age
-------+----
 Eve   | 22
 Bob   | 25
 Diana | 28
(3 rows)
```

---

## 7. Aggregations

PowQL wraps aggregate functions around the query pipeline.

### Count

```
powql> count(User)
```

Output:

```
7
```

Count with a filter:

```
powql> count(User filter .age > 25)
```

Output:

```
4
```

### Average

```
powql> avg(User { .age })
```

Output:

```
30
```

### Sum

```
powql> sum(User filter .age > 25 { .age })
```

Output:

```
133
```

Other aggregate functions: `min()`, `max()`.

---

## 8. Updates

Use `update` after an optional `filter` to modify rows. Assignments use `:=`:

```
powql> User filter .name = "Alice" update { age := 31 }
```

Output:

```
1 row affected
```

Verify it worked:

```
powql> User filter .name = "Alice" { .name, .age }
```

Output:

```
 name  | age
-------+----
 Alice | 31
(1 row)
```

You can also use expressions that reference the current row value:

```
powql> User filter .name = "Bob" update { age := .age + 1 }
```

Output:

```
1 row affected
```

---

## 9. Create an Index

Indexes speed up lookups on frequently queried columns. Use `create_index` to build a B+tree index:

```
powql> User create_index .email
```

Output:

```
index created on User.email
```

Indexed columns are used automatically for point lookups and range scans -- no query hints needed.

---

## 10. Delete

Use `delete` after a `filter` to remove matching rows:

```
powql> User filter .age < 25 delete
```

Output:

```
1 row affected
```

Verify Eve was removed:

```
powql> User { .name, .age }
```

Output:

```
 name    | age
---------+----
 Alice   | 31
 Bob     | 26
 Charlie | 35
 Diana   | 28
 Frank   | 40
 Grace   | {}
(6 rows)
```

To delete all rows in a table (use with care):

```
User delete
```

---

## 11. Server Mode

So far we've been running PowDB in embedded mode -- the CLI talks directly to the storage engine. For multi-client access, run PowDB as a server.

### Start the server

In one terminal:

```bash
cargo run --release -p powdb-server -- --port 5433 --data-dir ./powdb_data
```

Output:

```
powdb server listening addr=127.0.0.1:5433 data_dir=./powdb_data auth=false ...
```

### Connect from a client

In another terminal, connect with the CLI in remote mode:

```bash
cargo run --release -p powdb-cli -- --remote localhost:5433
```

Output:

```
PowDB v0.1.0 — remote mode
Connecting to localhost:5433 ...
Connected to db `main` (server v0.1.0)
Type PowQL queries. Use Ctrl-D to exit.

powql>
```

From here, everything works exactly the same as embedded mode. The server handles concurrent readers and uses a write-ahead log for durability.

### Password authentication

To require a password:

```bash
# Start the server with a password
cargo run --release -p powdb-server -- --password mysecret

# Connect with the password
cargo run --release -p powdb-cli -- --remote localhost:5433 --password mysecret
```

You can also set the password via the `POWDB_PASSWORD` environment variable.

---

## What's Next

This tutorial covered the basics: tables, inserts, queries, aggregates, updates, indexes, and deletes. PowDB supports much more, including joins, group by, subqueries, materialized views, and set operations.

See the full language reference: [PowQL Reference](POWQL.md)
