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
import { connect, Database } from '@tursodatabase/sync-react-native';

// Local-only database (simplest usage)
const db = await connect({ path: 'local.db' });

// Create table and insert data
await db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
await db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);

// Query data
const user = await db.get('SELECT * FROM users WHERE name = ?', 'Alice');
const allUsers = await db.all('SELECT * FROM users');

// Close when done
db.close();
```

## Sync Database (Embedded Replica)

Connect to a Turso cloud database for automatic sync:

```typescript
import { connect } from '@tursodatabase/sync-react-native';

const db = await connect({
  path: 'replica.db',
  url: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
});

// Query local replica (fast, works offline)
const users = await db.all('SELECT * FROM users');

// Make local changes
await db.run('INSERT INTO users (name) VALUES (?)', 'Bob');

// Sync with remote
await db.push();  // Push local changes to remote
await db.pull();  // Pull remote changes to local

db.close();
```

## API Reference

### Connecting

#### `connect(opts: DatabaseOpts): Promise<Database>`

Creates and connects to a database. This is the recommended way to create a database.

```typescript
// Local-only
const db = await connect({ path: 'local.db' });

// Sync database
const db = await connect({
  path: 'replica.db',
  url: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
});
```

#### `new Database(opts)` + `db.connect()`

Alternative two-step approach:

```typescript
const db = new Database({ path: 'local.db' });
await db.connect();
```

### Database Options

| Option | Type | Description |
|--------|------|-------------|
| `path` | `string` | Local database file path (required) |
| `url` | `string \| (() => string \| null)` | Remote database URL (enables sync) |
| `authToken` | `string \| (() => Promise<string>)` | Authentication token for remote |
| `clientName` | `string` | Client identifier for sync |
| `remoteEncryption` | `EncryptionOpts` | Remote encryption settings |
| `longPollTimeoutMs` | `number` | Timeout for pull operations |
| `bootstrapIfEmpty` | `boolean` | Bootstrap from remote if local is empty (default: true) |
| `partialSyncExperimental` | `object` | Experimental partial sync options |

### Query Methods

All query methods are async to support both local and sync databases:

#### `db.exec(sql: string): Promise<void>`

Execute SQL without returning results. Supports multiple statements.

```typescript
await db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
  CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
`);
```

#### `db.run(sql: string, ...params): Promise<RunResult>`

Execute a single statement and return result info.

```typescript
const result = await db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
console.log(result.changes);        // Number of rows affected
console.log(result.lastInsertRowid); // Last inserted row ID
```

#### `db.get(sql: string, ...params): Promise<Row | undefined>`

Query and return the first row.

```typescript
const user = await db.get('SELECT * FROM users WHERE id = ?', 1);
if (user) {
  console.log(user.name);
}
```

#### `db.all(sql: string, ...params): Promise<Row[]>`

Query and return all rows.

```typescript
const users = await db.all('SELECT * FROM users ORDER BY name');
for (const user of users) {
  console.log(user.name, user.age);
}
```

### Parameter Binding

Parameters can be passed as separate arguments or as an array:

```typescript
// Separate arguments
await db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);

// Array
await db.run('INSERT INTO users (name, age) VALUES (?, ?)', ['Alice', 30]);

// Named parameters
await db.run('INSERT INTO users (name, age) VALUES (:name, :age)', {
  ':name': 'Alice',
  ':age': 30,
});
```

Supported types: `null`, `number`, `string`, `ArrayBuffer` (for blobs).

### Prepared Statements

For repeated queries, use prepared statements:

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE age > ?');

// Execute multiple times
const adults = await stmt.all(18);
const seniors = await stmt.all(65);

// Clean up
await stmt.finalize();
```

### Transactions

```typescript
await db.transaction(async () => {
  await db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
  await db.run('INSERT INTO users (name) VALUES (?)', 'Bob');
  // Automatically commits on success, rolls back on error
});
```
## License

This project is licensed under the [MIT license](https://github.com/tursodatabase/turso/blob/main/LICENSE.md).

### Sync Methods (when `url` is provided)

#### `db.push(): Promise<void>`

Push local changes to the remote database.

```typescript
await db.run('INSERT INTO users (name) VALUES (?)', 'New User');
await db.push(); // Send to remote
```

#### `db.pull(): Promise<boolean>`

Pull remote changes to the local database. Returns `true` if changes were applied.

```typescript
const hasChanges = await db.pull();
if (hasChanges) {
  console.log('Got new data from remote!');
}
```

#### `db.stats(): Promise<SyncStats>`

Get sync statistics.

```typescript
const stats = await db.stats();
console.log('CDC operations:', stats.cdcOperations);
console.log('Network sent:', stats.networkSentBytes);
console.log('Network received:', stats.networkReceivedBytes);
```

#### `db.checkpoint(): Promise<void>`

Checkpoint the database (advanced usage).

### Database Properties

```typescript
db.path          // Database file path
db.isSync        // true if sync database
db.open          // true if database is open
db.inTransaction // true if in transaction
db.lastInsertRowid // Last inserted row ID
```

## Encrypted Remote Database

For databases with encryption enabled on the remote:

```typescript
const db = await connect({
  path: 'encrypted.db',
  url: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
  remoteEncryption: {
    cipher: 'aes256gcm',
    key: 'your-base64-encoded-key',
  },
});
```

Supported ciphers: `aes256gcm`, `aes128gcm`, `chacha20poly1305`, `aegis128l`, `aegis128x2`, `aegis128x4`, `aegis256`, `aegis256x2`, `aegis256x4`.

## Configuration

### Logging

Enable debug logging:

```typescript
import { setup } from '@tursodatabase/sync-react-native';

setup({ logLevel: 'debug' });
```

### Version

```typescript
import { version } from '@tursodatabase/sync-react-native';

console.log('Turso version:', version());
```

### Platform Paths

Get platform-specific writable directories:

```typescript
import { paths, getDbPath } from '@tursodatabase/sync-react-native';

// Helper function (recommended)
const dbPath = getDbPath('mydb.db');

// Direct access to paths
console.log(paths.documents); // Documents directory
console.log(paths.database);  // Database directory
console.log(paths.library);   // Library directory (iOS)
```

## TypeScript Types

```typescript
import type {
  DatabaseOpts,
  EncryptionOpts,
  Row,
  RunResult,
  SyncStats,
  SQLiteValue,
  BindParams,
} from '@tursodatabase/sync-react-native';
```

## Example App

See the [examples/react-native](../../examples/react-native) directory for a complete working example that demonstrates:

- Local database operations
- Sync database with push/pull
- Encrypted database access
- Performance testing

Run the example:

```bash
cd examples/react-native
npm install

# iOS
cd ios && pod install && cd ..
npm run ios

# Android
npm run android
```

To test sync features, set environment variables:

```bash
TURSO_DATABASE_URL=libsql://your-db.turso.io \
TURSO_AUTH_TOKEN=your-token \
npm start -- --reset-cache
```

## Requirements

- React Native >= 0.76.0 (New Architecture required)
- iOS: arm64 devices and simulators
- Android: minSdkVersion 21+, NDK

## Links

- [Turso Documentation](https://docs.turso.tech)
- [GitHub](https://github.com/tursodatabase/turso)
- [npm](https://www.npmjs.com/package/@tursodatabase/sync-react-native)
