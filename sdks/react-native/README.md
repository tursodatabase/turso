# Turso Sync React Native SDK

React Native bindings for Turso embedded replicas - sync your local SQLite database with Turso cloud.

## Installation

```bash
npm install @tursodatabase/sync-react-native
```

### iOS

```bash
cd ios && pod install
```

### Android

Requires `minSdkVersion` 21+ in `android/build.gradle`.

## Quick Start

```typescript
import { Database, getDbPath } from '@tursodatabase/sync-react-native';

// Get platform-specific writable path
const dbPath = getDbPath('myapp.db');

// Create database with sync
const db = new Database({
  path: dbPath,
  url: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
});

// Connect (bootstraps from remote if empty)
await db.connect();

// Query local replica (fast)
const users = await db.all('SELECT * FROM users');

// Make local changes
await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);

// Sync with remote
await db.push();  // Push local changes
await db.pull();  // Pull remote changes

// Close when done
await db.close();
```

## Local-Only Database

```typescript
const db = new Database({ path: getDbPath('local.db') });
await db.connect();

await db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)');
await db.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
const user = await db.get('SELECT * FROM users WHERE id = ?', [1]);

await db.close();
```

## Encrypted Remote Database

```typescript
const db = new Database({
  path: getDbPath('encrypted.db'),
  url: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
  remoteEncryption: {
    cipher: 'aes256gcm',
    key: 'base64-encoded-key',
  },
});
```

## API

### Database Methods

| Method | Description |
|--------|-------------|
| `connect()` | Open/bootstrap the database |
| `exec(sql)` | Execute SQL (no results) |
| `run(sql, params?)` | Execute SQL, return `{ changes, lastInsertRowid }` |
| `get(sql, params?)` | Query single row |
| `all(sql, params?)` | Query all rows |
| `prepare(sql)` | Create prepared statement |
| `close()` | Close database |

### Sync Methods (when `url` is provided)

| Method | Description |
|--------|-------------|
| `push()` | Push local changes to remote |
| `pull()` | Pull remote changes to local |
| `sync()` | Push then pull |
| `stats()` | Get sync statistics |

### Transactions

```typescript
await db.transaction(async () => {
  await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await db.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
  // Commits on success, rolls back on error
});
```
## License

This project is licensed under the [MIT license](https://github.com/tursodatabase/turso/blob/main/LICENSE.md).

## Links

- [Turso Documentation](https://docs.turso.tech)
- [GitHub](https://github.com/tursodatabase/turso)
- [npm](https://www.npmjs.com/package/@tursodatabase/sync-react-native)
