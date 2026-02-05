<p align="center">
  <h1 align="center">Turso Serverless Driver for JavaScript</h1>
</p>

<p align="center">
  <a title="JavaScript" target="_blank" href="https://www.npmjs.com/package/@tursodatabase/serverless"><img alt="npm" src="https://img.shields.io/npm/v/@tursodatabase/serverless"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
</p>
<p align="center">
  <a title="Users Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social"></a>
</p>

---

## About

A serverless database driver for Turso Cloud, using only `fetch()`. Connect to your database from serverless and edge functions, such as Cloudflare Workers and Vercel.

> **⚠️ Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups.

## Installation

```bash
npm install @tursodatabase/serverless
```

## Getting Started

### Basic Usage

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
```

### Batch Operations

```javascript
// Execute multiple statements in a batch
await conn.batch([
  "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
  "INSERT INTO users (email) VALUES ('user@example.com')",
  "INSERT INTO users (email) VALUES ('admin@example.com')",
]);
```

### libSQL Compatibility Layer

For existing libSQL applications, use the compatibility layer:

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

## API Reference

For complete API documentation, see [JavaScript API Reference](../../docs/javascript-api-reference.md).

## Related Packages

* The [@tursodatabase/database](https://www.npmjs.com/package/@tursodatabase/database) package provides the Turso in-memory database, compatible with SQLite.
* The [@tursodatabase/sync](https://www.npmjs.com/package/@tursodatabase/sync) package provides bidirectional sync between a local Turso database and Turso Cloud. 

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)
