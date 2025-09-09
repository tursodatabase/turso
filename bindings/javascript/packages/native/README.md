<p align="center">
  <h1 align="center">Turso Database for JavaScript in Node</h1>
</p>

<p align="center">
  <a title="JavaScript" target="_blank" href="https://www.npmjs.com/package/@tursodatabase/database"><img alt="npm" src="https://img.shields.io/npm/v/@tursodatabase/database"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
</p>
<p align="center">
  <a title="Users Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social"></a>
</p>

---

## About

This package is the Turso embedded database library for JavaScript in Node.

> **⚠️ Warning:** This software is ALPHA, only use for development, testing, and experimentation. We are working to make it production ready, but do not use it for critical data right now.

## Features

- **SQLite compatible:** SQLite query language and file format support ([status](https://github.com/tursodatabase/turso/blob/main/COMPAT.md)).
- **In-process**: No network overhead, runs directly in your Node.js process
- **TypeScript support**: Full TypeScript definitions included
- **Cross-platform**: Supports Linux (x86 and arm64), macOS, Windows (browser is supported in the separate package `@tursodatabase/database-browser` package)

## Installation

```bash
npm install @tursodatabase/database
```

## Getting Started

### In-Memory Database

```javascript
import { connect } from '@tursodatabase/database';

// Create an in-memory database
const db = await connect(':memory:');

// Create a table
await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

// Insert data
const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
await insert.run('Alice', 'alice@example.com');
await insert.run('Bob', 'bob@example.com');

// Query data
const users = await db.prepare('SELECT * FROM users').all();
console.log(users);
// Output: [
//   { id: 1, name: 'Alice', email: 'alice@example.com' },
//   { id: 2, name: 'Bob', email: 'bob@example.com' }
// ]
```

### File-Based Database

```javascript
import { connect } from '@tursodatabase/database';

// Create or open a database file
const db = await connect('my-database.db');

// Create a table
await db.exec(`
  CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

// Insert a post
const insertPost = db.prepare('INSERT INTO posts (title, content) VALUES (?, ?)');
const result = await insertPost.run('Hello World', 'This is my first blog post!');

console.log(`Inserted post with ID: ${result.lastInsertRowid}`);
```

### Transactions

```javascript
import { connect } from '@tursodatabase/database';

const db = await connect('transactions.db');

// Using transactions for atomic operations
const transaction = db.transaction(async (users) => {
  const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
  for (const user of users) {
    await insert.run(user.name, user.email);
  }
});

// Execute transaction
await transaction([
  { name: 'Alice', email: 'alice@example.com' },
  { name: 'Bob', email: 'bob@example.com' }
]);
```

## API Reference

For complete API documentation, see [JavaScript API Reference](../../../../docs/javascript-api-reference.md).

## Related Packages

* The [@tursodatabase/serverless](https://www.npmjs.com/package/@tursodatabase/serverless) package provides a serverless driver with the same API.
* The [@tursodatabase/sync](https://www.npmjs.com/package/@tursodatabase/sync) package provides bidirectional sync between a local Turso database and Turso Cloud. 

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)
