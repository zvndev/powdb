/**
 * BataDB client demo.
 *
 * Connects to the BataDB server (defaults to the Fly deployment), creates a
 * table, inserts a handful of rows, and runs a few BataQL queries against it.
 *
 * Usage:
 *     BATADB_PASSWORD=... tsx demo/demo.ts
 *     BATADB_HOST=127.0.0.1 BATADB_PORT=5433 tsx demo/demo.ts
 */

import { Client, type QueryResult } from "../src/index.js";

const HOST = process.env.BATADB_HOST ?? "213.188.194.202";
const PORT = Number(process.env.BATADB_PORT ?? "5433");
const DB = process.env.BATADB_DB ?? "default";
const PASSWORD = process.env.BATADB_PASSWORD ?? null;

async function main() {
  console.log(`→ connecting to ${HOST}:${PORT} (db=${DB})`);
  const client = await Client.connect({
    host: HOST,
    port: PORT,
    dbName: DB,
    password: PASSWORD,
  });
  console.log(`✓ connected — server version: ${client.serverVersion}\n`);

  try {
    // Use a unique table name per run so reruns don't collide on a
    // persistent server. BataQL doesn't have DROP TABLE (yet).
    const table = `Demo${Date.now().toString(36)}`;
    console.log(`→ creating type ${table}`);
    await run(
      client,
      `type ${table} { required name: string, required age: int, city: string }`,
    );

    console.log(`→ inserting rows`);
    const people = [
      { name: "Alice", age: 30, city: "NYC" },
      { name: "Bob", age: 24, city: "SF" },
      { name: "Carol", age: 41, city: "LA" },
      { name: "Dave", age: 28, city: "NYC" },
      { name: "Eve", age: 35, city: "Austin" },
    ];
    for (const p of people) {
      await run(
        client,
        `insert ${table} { name := "${p.name}", age := ${p.age}, city := "${p.city}" }`,
      );
    }

    console.log(`\n→ ${table} count`);
    printResult(await client.query(`count(${table})`));

    console.log(`\n→ everyone, name + age, oldest first`);
    printResult(
      await client.query(`${table} order .age desc { .name, .age, .city }`),
    );

    console.log(`\n→ people over 27, youngest of those first, limit 3`);
    printResult(
      await client.query(
        `${table} filter .age > 27 order .age limit 3 { .name, .age, .city }`,
      ),
    );

    console.log(`\n→ only NYC residents`);
    printResult(
      await client.query(
        `${table} filter .city = "NYC" { .name, .age }`,
      ),
    );
  } finally {
    await client.close();
    console.log(`\n✓ disconnected`);
  }
}

async function run(client: Client, query: string): Promise<void> {
  const result = await client.query(query);
  if (result.kind === "ok") {
    console.log(`  ok (${result.affected} affected) — ${snippet(query)}`);
  } else {
    printResult(result);
  }
}

function printResult(result: QueryResult): void {
  switch (result.kind) {
    case "rows":
      printTable(result.columns, result.rows);
      break;
    case "scalar":
      console.log(`  → ${result.value}`);
      break;
    case "ok":
      console.log(`  → ok (${result.affected} affected)`);
      break;
  }
}

function printTable(columns: string[], rows: string[][]): void {
  if (rows.length === 0) {
    console.log(`  (empty: ${columns.join(", ")})`);
    return;
  }
  const widths = columns.map((c, i) =>
    Math.max(c.length, ...rows.map((r) => (r[i] ?? "").length)),
  );
  const pad = (s: string, w: number) => s + " ".repeat(Math.max(0, w - s.length));
  const sep = widths.map((w) => "─".repeat(w)).join("─┼─");
  console.log("  " + columns.map((c, i) => pad(c, widths[i]!)).join(" │ "));
  console.log("  " + sep);
  for (const row of rows) {
    console.log(
      "  " + row.map((v, i) => pad(v ?? "", widths[i]!)).join(" │ "),
    );
  }
}

function snippet(s: string): string {
  return s.length > 60 ? s.slice(0, 57) + "..." : s;
}

main().catch((err) => {
  console.error("demo failed:", err);
  process.exit(1);
});
