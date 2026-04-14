/**
 * Comprehensive end-to-end tests for the PowDB TypeScript client.
 *
 * Requires a running PowDB server:
 *   POWDB_HOST=127.0.0.1 POWDB_PORT=15433 npx tsx test/client.test.ts
 */

import { Client, type QueryResult } from "../src/index.js";
import { strict as assert } from "node:assert";

const HOST = process.env.POWDB_HOST ?? "127.0.0.1";
const PORT = Number(process.env.POWDB_PORT ?? "15433");

let client: Client;
let passed = 0;
let failed = 0;
const failures: string[] = [];

// Unique table prefix so tests don't collide across runs
const T = `T${Date.now().toString(36)}`;
const tbl = (name: string) => `${T}_${name}`;

async function test(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ✓ ${name}`);
  } catch (err: any) {
    failed++;
    failures.push(`${name}: ${err.message}`);
    console.log(`  ✗ ${name}`);
    console.log(`    ${err.message}`);
  }
}

function assertRows(r: QueryResult, expected: { cols: string[]; rows: string[][] }) {
  assert.equal(r.kind, "rows", `expected rows, got ${r.kind}`);
  if (r.kind !== "rows") return;
  assert.deepStrictEqual(r.columns, expected.cols);
  assert.deepStrictEqual(r.rows, expected.rows);
}

function assertScalar(r: QueryResult, value: string) {
  assert.equal(r.kind, "scalar", `expected scalar, got ${r.kind}`);
  if (r.kind !== "scalar") return;
  assert.equal(r.value, value);
}

function assertOk(r: QueryResult, affected?: number) {
  assert.equal(r.kind, "ok", `expected ok, got ${r.kind}`);
  if (r.kind !== "ok") return;
  if (affected !== undefined) {
    assert.equal(Number(r.affected), affected);
  }
}

async function main() {
  console.log(`\nConnecting to ${HOST}:${PORT}...`);
  client = await Client.connect({ host: HOST, port: PORT });
  console.log(`Connected — server v${client.serverVersion}\n`);

  // ──────────────────────────────────────────────────────────
  console.log("DDL — type creation");
  // ──────────────────────────────────────────────────────────

  await test("create table with all types", async () => {
    const t = tbl("AllTypes");
    const r = await client.query(
      `type ${t} { required name: str, age: int, score: float, active: bool }`
    );
    assertOk(r, 0);
  });

  await test("create table with required fields", async () => {
    const t = tbl("Users");
    const r = await client.query(
      `type ${t} { required name: str, required email: str, age: int, city: str }`
    );
    assertOk(r, 0);
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nINSERT");
  // ──────────────────────────────────────────────────────────

  const users = tbl("Users");

  await test("insert single row", async () => {
    const r = await client.query(
      `insert ${users} { name := "Alice", email := "alice@test.com", age := 30, city := "NYC" }`
    );
    assertOk(r, 1);
  });

  await test("insert multiple rows", async () => {
    const people = [
      { name: "Bob", email: "bob@test.com", age: 25, city: "SF" },
      { name: "Charlie", email: "charlie@test.com", age: 35, city: "NYC" },
      { name: "Diana", email: "diana@test.com", age: 28, city: "LA" },
      { name: "Eve", email: "eve@test.com", age: 22, city: "Austin" },
      { name: "Frank", email: "frank@test.com", age: 40, city: "NYC" },
      { name: "Grace", email: "grace@test.com", age: 33, city: "SF" },
      { name: "Hank", email: "hank@test.com", age: 45, city: "LA" },
    ];
    for (const p of people) {
      const r = await client.query(
        `insert ${users} { name := "${p.name}", email := "${p.email}", age := ${p.age}, city := "${p.city}" }`
      );
      assertOk(r, 1);
    }
  });

  await test("insert with null field (omitted)", async () => {
    const r = await client.query(
      `insert ${users} { name := "Ivy", email := "ivy@test.com" }`
    );
    assertOk(r, 1);
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nBASIC QUERIES");
  // ──────────────────────────────────────────────────────────

  await test("select all rows (table name only)", async () => {
    const r = await client.query(users);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 9);
      assert.deepStrictEqual(r.columns, ["name", "email", "age", "city"]);
    }
  });

  await test("projection — specific fields", async () => {
    const r = await client.query(`${users} { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.deepStrictEqual(r.columns, ["name", "age"]);
      assert.equal(r.rows.length, 9);
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nFILTER");
  // ──────────────────────────────────────────────────────────

  await test("filter with > operator", async () => {
    const r = await client.query(`${users} filter .age > 30 { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      // Charlie(35), Frank(40), Grace(33), Hank(45)
      assert.equal(r.rows.length, 4);
      for (const row of r.rows) {
        assert.ok(parseInt(row[1]!) > 30, `age ${row[1]} should be > 30`);
      }
    }
  });

  await test("filter with = operator (string)", async () => {
    const r = await client.query(`${users} filter .city = "NYC" { .name, .city }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 3); // Alice, Charlie, Frank
      for (const row of r.rows) {
        assert.equal(row[1], "NYC");
      }
    }
  });

  await test("filter with < operator", async () => {
    const r = await client.query(`${users} filter .age < 25 { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 1); // Eve(22)
      assert.equal(r.rows[0]![0], "Eve");
    }
  });

  await test("filter with >= operator", async () => {
    const r = await client.query(`${users} filter .age >= 40 { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 2); // Frank(40), Hank(45)
    }
  });

  await test("filter with <= operator", async () => {
    const r = await client.query(`${users} filter .age <= 25 { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 2); // Bob(25), Eve(22)
    }
  });

  await test("filter with != operator", async () => {
    const r = await client.query(`${users} filter .city != "NYC" { .name, .city }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      for (const row of r.rows) {
        assert.notEqual(row[1], "NYC");
      }
    }
  });

  await test("filter with = null and != null", async () => {
    // Ivy was inserted without an age — her row has a null age.
    const withNull = await client.query(`${users} filter .age = null { .name }`);
    assert.equal(withNull.kind, "rows");
    if (withNull.kind === "rows") {
      const names = withNull.rows.map((r) => r[0]);
      assert.ok(names.includes("Ivy"), "Ivy (null age) should match `= null`");
      assert.equal(names.length, 1);
    }

    const withoutNull = await client.query(`${users} filter .age != null { .name }`);
    assert.equal(withoutNull.kind, "rows");
    if (withoutNull.kind === "rows") {
      const names = withoutNull.rows.map((r) => r[0]);
      assert.ok(!names.includes("Ivy"), "Ivy should not match `!= null`");
      assert.ok(names.length > 0);
    }
  });

  await test("filter with AND", async () => {
    const r = await client.query(
      `${users} filter .age > 25 and .city = "NYC" { .name, .age, .city }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      // Alice(30,NYC), Charlie(35,NYC), Frank(40,NYC)
      assert.equal(r.rows.length, 3);
    }
  });

  await test("filter with OR", async () => {
    const r = await client.query(
      `${users} filter .city = "Austin" or .city = "LA" { .name, .city }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      // Eve(Austin), Diana(LA), Hank(LA)
      assert.equal(r.rows.length, 3);
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nORDER BY");
  // ──────────────────────────────────────────────────────────

  await test("order ascending", async () => {
    const r = await client.query(`${users} filter .age != null order .age asc { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(
          parseInt(r.rows[i]![1]!) >= parseInt(r.rows[i - 1]![1]!),
          `out of order at index ${i}`
        );
      }
    }
  });

  await test("order descending", async () => {
    const r = await client.query(`${users} filter .age != null order .age desc { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(
          parseInt(r.rows[i]![1]!) <= parseInt(r.rows[i - 1]![1]!),
          `out of order at index ${i}`
        );
      }
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nLIMIT");
  // ──────────────────────────────────────────────────────────

  await test("limit results", async () => {
    const r = await client.query(`${users} limit 3 { .name }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 3);
    }
  });

  await test("order + limit combined", async () => {
    const r = await client.query(`${users} order .age desc limit 3 { .name, .age }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0]![0], "Hank"); // oldest
    }
  });

  await test("filter + order + limit pipeline", async () => {
    const r = await client.query(
      `${users} filter .age > 25 order .age asc limit 2 { .name, .age }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0]![0], "Diana"); // 28
      assert.equal(r.rows[1]![0], "Alice"); // 30
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nAGGREGATES");
  // ──────────────────────────────────────────────────────────

  await test("count all", async () => {
    const r = await client.query(`count(${users})`);
    assertScalar(r, "9");
  });

  await test("count with filter", async () => {
    const r = await client.query(`count(${users} filter .age > 30)`);
    assertScalar(r, "4");
  });

  await test("sum", async () => {
    // sum() skips nulls automatically — Ivy's age is null
    const r = await client.query(`sum(${users} { .age })`);
    // 30+25+35+28+22+40+33+45 = 258
    assertScalar(r, "258");
  });

  await test("avg", async () => {
    const r = await client.query(`avg(${users} { .age })`);
    // 258/8 = 32.25 — but PowDB may return integer avg
    assert.equal(r.kind, "scalar");
    if (r.kind === "scalar") {
      const val = parseFloat(r.value);
      assert.ok(val >= 32 && val <= 33, `avg should be ~32.25, got ${val}`);
    }
  });

  await test("min", async () => {
    const r = await client.query(`min(${users} { .age })`);
    assertScalar(r, "22");
  });

  await test("max", async () => {
    const r = await client.query(`max(${users} { .age })`);
    assertScalar(r, "45");
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nUPDATE");
  // ──────────────────────────────────────────────────────────

  await test("update single row", async () => {
    const r = await client.query(
      `${users} filter .name = "Alice" update { age := 31 }`
    );
    assertOk(r, 1);

    // Verify
    const check = await client.query(`${users} filter .name = "Alice" { .name, .age }`);
    assert.equal(check.kind, "rows");
    if (check.kind === "rows") {
      assert.equal(check.rows[0]![1], "31");
    }
  });

  await test("update with expression (.age + 1)", async () => {
    const r = await client.query(
      `${users} filter .name = "Bob" update { age := .age + 1 }`
    );
    assertOk(r, 1);

    const check = await client.query(`${users} filter .name = "Bob" { .age }`);
    assert.equal(check.kind, "rows");
    if (check.kind === "rows") {
      assert.equal(check.rows[0]![0], "26"); // was 25
    }
  });

  await test("update multiple rows", async () => {
    const r = await client.query(
      `${users} filter .city = "NYC" update { city := "New York" }`
    );
    assertOk(r, 3);

    const check = await client.query(`${users} filter .city = "New York" { .name }`);
    assert.equal(check.kind, "rows");
    if (check.kind === "rows") {
      assert.equal(check.rows.length, 3);
    }
  });

  // Revert for later tests
  await client.query(`${users} filter .city = "New York" update { city := "NYC" }`);

  // ──────────────────────────────────────────────────────────
  console.log("\nINDEXES");
  // ──────────────────────────────────────────────────────────

  await test("create index", async () => {
    const r = await client.query(`alter ${users} add index .email`);
    assertOk(r);
  });

  await test("query still works with index (point lookup)", async () => {
    const r = await client.query(
      `${users} filter .email = "alice@test.com" { .name, .email }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0]![0], "Alice");
    }
  });

  await test("create index on age for range scans", async () => {
    const r = await client.query(`alter ${users} add index .age`);
    assertOk(r);
  });

  await test("range scan with index", async () => {
    const r = await client.query(
      `${users} filter .age > 30 and .age < 42 { .name, .age }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      // Alice(31), Charlie(35), Grace(33), Frank(40) — Alice was updated to 31 earlier
      assert.equal(r.rows.length, 4);
      for (const row of r.rows) {
        const age = parseInt(row[1]!);
        assert.ok(age > 30 && age < 42, `age ${age} should be between 31-41`);
      }
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nDELETE");
  // ──────────────────────────────────────────────────────────

  await test("delete single row by filter", async () => {
    const r = await client.query(`${users} filter .name = "Ivy" delete`);
    assertOk(r, 1);

    const check = await client.query(`count(${users})`);
    assertScalar(check, "8");
  });

  await test("delete multiple rows by filter", async () => {
    // Delete Eve(22) and Bob(26)
    const r = await client.query(`${users} filter .age < 27 delete`);
    assertOk(r, 2);

    const check = await client.query(`count(${users})`);
    assertScalar(check, "6");
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nDDL — add/drop column");
  // ──────────────────────────────────────────────────────────

  await test("add column", async () => {
    const r = await client.query(`alter ${users} add column score: int`);
    assertOk(r);

    // Verify column exists by selecting it
    const check = await client.query(`${users} limit 1 { .name, .score }`);
    assert.equal(check.kind, "rows");
    if (check.kind === "rows") {
      assert.deepStrictEqual(check.columns, ["name", "score"]);
    }
  });

  await test("drop column", async () => {
    const r = await client.query(`alter ${users} drop column score`);
    assertOk(r);

    // Verify column is gone
    const check = await client.query(`${users} limit 1`);
    assert.equal(check.kind, "rows");
    if (check.kind === "rows") {
      assert.ok(!check.columns.includes("score"), "score column should be gone");
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nJOINS");
  // ──────────────────────────────────────────────────────────

  const teams = tbl("Teams");

  await test("create join target table", async () => {
    const r = await client.query(
      `type ${teams} { required team_name: str, required team_city: str }`
    );
    assertOk(r, 0);

    await client.query(`insert ${teams} { team_name := "Engineering", team_city := "NYC" }`);
    await client.query(`insert ${teams} { team_name := "Design", team_city := "SF" }`);
    await client.query(`insert ${teams} { team_name := "Marketing", team_city := "LA" }`);
  });

  await test("inner join on field with right-table projection", async () => {
    const r = await client.query(
      `${users} as u inner join ${teams} as t on u.city = t.team_city { u.name, u.city, team: t.team_name }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.ok(r.rows.length > 0, "should have joined rows");
      assert.deepStrictEqual(r.columns, ["u.name", "u.city", "team"]);
      // Every row must have all three populated (right-table column included).
      for (const row of r.rows) {
        assert.ok(row[2] !== "" && row[2] != null, `team should be populated, got ${row[2]}`);
      }
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nGROUP BY + HAVING");
  // ──────────────────────────────────────────────────────────

  await test("group by with count", async () => {
    const r = await client.query(
      `${users} group .city { .city, cnt: count(.name) }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.ok(r.columns.includes("city"));
      assert.ok(r.columns.includes("cnt"));
      assert.ok(r.rows.length > 0);
    }
  });

  await test("group by with avg", async () => {
    const r = await client.query(
      `${users} group .city { .city, avg_age: avg(.age) }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.ok(r.columns.includes("avg_age"));
    }
  });

  await test("group by with having filters groups", async () => {
    const r = await client.query(
      `${users} group .city { .city, cnt: count(.name) } having cnt >= 2`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.ok(r.columns.includes("cnt"));
      assert.ok(r.rows.length > 0, "should return groups");
      // Every surviving group must satisfy the HAVING predicate.
      for (const row of r.rows) {
        const cnt = parseInt(row[r.columns.indexOf("cnt")]!);
        assert.ok(cnt >= 2, `cnt ${cnt} should be >= 2 after HAVING filter`);
      }
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nDISTINCT");
  // ──────────────────────────────────────────────────────────

  await test("distinct", async () => {
    const r = await client.query(`${users} distinct { .city }`);
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      const cities = r.rows.map((row) => row[0]);
      const unique = new Set(cities);
      assert.equal(cities.length, unique.size, "distinct should return unique cities");
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nSUBQUERIES");
  // ──────────────────────────────────────────────────────────

  const orders = tbl("Orders");

  await test("subquery with IN", async () => {
    // Create orders table
    await client.query(
      `type ${orders} { required user_name: str, required total: int }`
    );
    await client.query(`insert ${orders} { user_name := "Alice", total := 150 }`);
    await client.query(`insert ${orders} { user_name := "Charlie", total := 50 }`);
    await client.query(`insert ${orders} { user_name := "Frank", total := 200 }`);

    // Find users who have an order > 100
    const r = await client.query(
      `${users} filter .name in (${orders} filter .total > 100 { .user_name }) { .name }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      const names = r.rows.map((row) => row[0]);
      assert.ok(names.includes("Alice"));
      assert.ok(names.includes("Frank"));
      assert.ok(!names.includes("Charlie")); // total=50
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nSET OPERATIONS");
  // ──────────────────────────────────────────────────────────

  await test("union", async () => {
    // PowQL union syntax: no parentheses around each side
    const r = await client.query(
      `${users} filter .city = "NYC" { .name } union ${users} filter .city = "LA" { .name }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.ok(r.rows.length > 0, "union should return rows");
      // NYC: Alice, Charlie, Frank. LA: Diana, Hank. Union deduplicates.
      const names = r.rows.map((row) => row[0]);
      assert.ok(names.includes("Alice"), "should include NYC user");
      assert.ok(names.includes("Diana") || names.includes("Hank"), "should include LA user");
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nEXPRESSIONS");
  // ──────────────────────────────────────────────────────────

  await test("arithmetic in projection", async () => {
    const r = await client.query(
      `${users} filter .name = "Alice" { .name, double_age: .age * 2 }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      assert.equal(r.rows[0]![1], "62"); // 31 * 2
    }
  });

  await test("BETWEEN in filter", async () => {
    const r = await client.query(
      `${users} filter .age between 30 and 40 { .name, .age }`
    );
    assert.equal(r.kind, "rows");
    if (r.kind === "rows") {
      for (const row of r.rows) {
        const age = parseInt(row[1]!);
        assert.ok(age >= 30 && age <= 40, `age ${age} should be between 30 and 40`);
      }
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nERROR HANDLING");
  // ──────────────────────────────────────────────────────────

  await test("query error — invalid table", async () => {
    try {
      await client.query("NonExistentTable12345");
      assert.fail("should have thrown");
    } catch (err: any) {
      assert.ok(err.message.includes("query failed"), `unexpected error: ${err.message}`);
    }
  });

  await test("query error — syntax error", async () => {
    try {
      await client.query("SELECT * FROM foo");
      assert.fail("should have thrown");
    } catch (err: any) {
      assert.ok(err.message.includes("query failed"), `unexpected error: ${err.message}`);
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nDROP TABLE");
  // ──────────────────────────────────────────────────────────

  await test("drop table", async () => {
    const r = await client.query(`drop ${orders}`);
    assertOk(r);

    // Verify it's gone
    try {
      await client.query(orders);
      assert.fail("should have thrown — table dropped");
    } catch (err: any) {
      assert.ok(err.message.includes("query failed"));
    }
  });

  // ──────────────────────────────────────────────────────────
  console.log("\nCONNECTION LIFECYCLE");
  // ──────────────────────────────────────────────────────────

  await test("close and reconnect", async () => {
    await client.close();

    client = await Client.connect({ host: HOST, port: PORT });
    assert.ok(client.serverVersion, "should have server version after reconnect");

    // Verify data persisted
    const r = await client.query(`count(${users})`);
    assertScalar(r, "6");
  });

  await test("connect with custom timeout", async () => {
    const c2 = await Client.connect({
      host: HOST,
      port: PORT,
      connectTimeoutMs: 2000,
    });
    const r = await c2.query(`count(${users})`);
    assertScalar(r, "6");
    await c2.close();
  });

  await test("connect timeout on bad host", async () => {
    try {
      await Client.connect({
        host: "192.0.2.1", // RFC 5737 TEST-NET — will timeout
        port: 9999,
        connectTimeoutMs: 500,
      });
      assert.fail("should have thrown");
    } catch (err: any) {
      assert.ok(
        err.message.includes("timeout") || err.message.includes("ETIMEDOUT"),
        `unexpected error: ${err.message}`
      );
    }
  });

  // ──────────────────────────────────────────────────────────
  // Cleanup
  // ──────────────────────────────────────────────────────────

  await client.query(`drop ${users}`);
  await client.query(`drop ${teams}`);
  const allTypes = tbl("AllTypes");
  await client.query(`drop ${allTypes}`);
  await client.close();

  // ──────────────────────────────────────────────────────────
  console.log("\n" + "═".repeat(50));
  console.log(`Results: ${passed} passed, ${failed} failed`);
  if (failures.length > 0) {
    console.log("\nFailures:");
    for (const f of failures) {
      console.log(`  - ${f}`);
    }
  }
  console.log("═".repeat(50));

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error("Test suite crashed:", err);
  process.exit(1);
});
