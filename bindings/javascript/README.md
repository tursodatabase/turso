# @tursodatabase/turso

The next evolution of SQLite: A high-performance, SQLite-compatible database library for Node.js

## Features

- **SQLite Compatible**: Drop-in replacement for better-sqlite3 with familiar API
- **High Performance**: Built with Rust for maximum speed and efficiency  
- **In-Process**: No network overhead, runs directly in your Node.js process
- **TypeScript Support**: Full TypeScript definitions included
- **Cross-Platform**: Supports Linux, macOS, and Windows
- **Transaction Support**: Full ACID transactions with rollback support
- **Prepared Statements**: Optimized query execution with parameter binding

## Installation

```bash
npm install @tursodatabase/turso
```

## Quick Start

### In-Memory Database

```javascript
import Database from '@tursodatabase/turso';

// Create an in-memory database
const db = new Database(':memory:');

// Create a table
db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

// Insert data
const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
insert.run('Alice', 'alice@example.com');
insert.run('Bob', 'bob@example.com');

// Query data
const users = db.prepare('SELECT * FROM users').all();
console.log(users);
// Output: [
//   { id: 1, name: 'Alice', email: 'alice@example.com' },
//   { id: 2, name: 'Bob', email: 'bob@example.com' }
// ]
```

### File-Based Database

```javascript
import Database from '@tursodatabase/turso';

// Create or open a database file
const db = new Database('my-database.db');

// Create a table
db.exec(`
  CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

// Insert a post
const insertPost = db.prepare('INSERT INTO posts (title, content) VALUES (?, ?)');
const result = insertPost.run('Hello World', 'This is my first blog post!');

console.log(`Inserted post with ID: ${result.lastInsertRowid}`);
```

## API Reference
## License

MIT

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://discord.gg/turso)
