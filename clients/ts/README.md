# @zvndev/powdb-client

TypeScript client for [PowDB](https://github.com/zvndev/powdb) — speaks the native binary wire protocol over TCP.

## Install

```bash
npm install @zvndev/powdb-client
```

## Usage

```typescript
import { Client } from "@zvndev/powdb-client";

const client = await Client.connect({
  host: "localhost",
  port: 5433,
});

// Create a table
await client.query("type User { required name: str, required email: str, age: int }");

// Insert data
await client.query('insert User { name := "Alice", email := "alice@example.com", age := 30 }');

// Query
const result = await client.query("User filter .age > 25 { .name, .age }");
if (result.kind === "rows") {
  console.table(result.rows);
}

// Aggregates
const count = await client.query("count(User)");
if (count.kind === "scalar") {
  console.log(`Total users: ${count.value}`);
}

await client.close();
```

## API

### `Client.connect(options)`

Returns a `Promise<Client>`. Options:

| Option | Type | Default | Description |
|---|---|---|---|
| `host` | `string` | *(required)* | Server hostname or IP |
| `port` | `number` | *(required)* | Server port |
| `dbName` | `string` | `"default"` | Database name |
| `password` | `string \| null` | `null` | Server password (if auth is enabled) |
| `connectTimeoutMs` | `number` | `5000` | Connection timeout in milliseconds |

### `client.query(query)`

Sends a PowQL query and returns a `Promise<QueryResult>`:

- `{ kind: "rows", columns: string[], rows: string[][] }` — for SELECT-like queries
- `{ kind: "scalar", value: string }` — for aggregates (`count`, `sum`, `avg`, etc.)
- `{ kind: "ok", affected: bigint }` — for mutations (`insert`, `update`, `delete`)

Throws on server errors.

### `client.close()`

Sends a disconnect message and closes the TCP socket.

### `client.serverVersion`

The PowDB server version string (e.g., `"0.1.0"`).

## Requirements

- Node.js 18+ (uses `node:net`)
- A running PowDB server (`cargo run --release -p powdb-server`)

## License

MIT
