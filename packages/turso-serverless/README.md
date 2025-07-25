# Turso serverless JavaScript driver

A serverless database driver for Turso Cloud, using only `fetch()`. Connect to your database from serverless and edge functions, such as Cloudflare Workers and Vercel.

> [!NOTE]
> This driver is experimental and, therefore, subject to change at any time.

## Installation

```bash
npm install @tursodatabase/serverless
```

## Usage

```javascript
import { connect } from "@tursodatabase/serverless";

const conn = connect({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// Prepare a statement
const stmt = conn.prepare("SELECT * FROM users WHERE id = ?");

// Get first row
const row = await stmt.get([123]);
console.log(row);

// Get all rows
const rows = await stmt.all([123]);
console.log(rows);

// Iterate through rows (streaming)
for await (const row of stmt.iterate([123])) {
  console.log(row);
}

// Execute multiple statements in a batch
await conn.batch([
  "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
  "INSERT INTO users (email) VALUES ('user@example.com')",
  "INSERT INTO users (email) VALUES ('admin@example.com')",
]);
```

### Transactions

The driver supports interactive transactions with commit and rollback:

```javascript
// Start a transaction
const tx = await conn.transaction();

try {
  await tx.execute("INSERT INTO users (email) VALUES (?)", [
    "user1@example.com",
  ]);
  await tx.execute("INSERT INTO users (email) VALUES (?)", [
    "user2@example.com",
  ]);

  // Commit the transaction
  await tx.commit();
} catch (error) {
  // Rollback on error
  await tx.rollback();
}
```

Transaction modes are supported:

```javascript
const writeTx = await conn.transaction("write"); // Immediate write lock
const readTx = await conn.transaction("read"); // Read-only transaction
const deferredTx = await conn.transaction("deferred"); // Default mode
```

### Compatibility layer for libSQL API

This driver supports the libSQL API as a compatibility layer.

```javascript
import { createClient } from "@tursodatabase/serverless/compat";

const client = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// Execute a single SQL statement
const result = await client.execute("SELECT * FROM users WHERE id = ?", [123]);
console.log(result.rows);

// Execute multiple statements in a batch
await client.batch([
  "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
  "INSERT INTO users (email) VALUES ('user@example.com')",
  "INSERT INTO users (email) VALUES ('admin@example.com')",
]);
```

## Examples

Check out the `examples/` directory for complete usage examples.

## License

MIT
