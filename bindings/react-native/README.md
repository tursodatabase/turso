# Turso React Native SDK

React Native bindings for Turso, providing SQLite and embedded replica database functionality.

## Features

- ✅ **Local SQLite databases** - Full SQLite support with synchronous API
- ✅ **Embedded replicas** - Sync local database with remote Turso database
- ✅ **TypeScript support** - Full type definitions included
- ✅ **JSI-powered** - Native performance with JavaScript Interface
- ✅ **Transactions** - Atomic operations with automatic rollback
- ✅ **Prepared statements** - Efficient query execution
- ✅ **Partial sync** - Sync only specific tables or query results

## Installation

```bash
npm install @tursodatabase/react-native
# or
yarn add @tursodatabase/react-native
```

### iOS Setup

```bash
cd ios && pod install
```

### Android Setup

Make sure your `minSdkVersion` is set to at least 21 in `android/build.gradle`.

## Quick Start

### Local Database

```typescript
import { createLocalDatabase } from '@tursodatabase/react-native';

// Create/open a local SQLite database
const db = createLocalDatabase('./mydb.sqlite');

// Create table
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)');

// Insert data
const result = db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
console.log('Inserted row:', result.lastInsertRowid);

// Query data
const user = db.get('SELECT * FROM users WHERE id = ?', 1);
console.log(user); // { id: 1, name: 'Alice' }

const allUsers = db.all('SELECT * FROM users');
console.log(allUsers); // [{ id: 1, name: 'Alice' }]

// Close database
db.close();
```

### Embedded Replica (Sync Database)

```typescript
import { createSyncDatabase } from '@tursodatabase/react-native';

// Create/open a sync database
const db = await createSyncDatabase({
  path: './replica.db',
  remoteUrl: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
  bootstrapIfEmpty: true,
});

// Query local replica (fast, synchronous)
const users = db.all('SELECT * FROM users');

// Make local changes
db.run('INSERT INTO users (name) VALUES (?)', 'Bob');

// Push local changes to remote
await db.push();

// Pull remote changes to local
const hasChanges = await db.pull();
if (hasChanges) {
  console.log('Applied remote changes');
}

// Get sync statistics
const stats = await db.stats();
console.log('Network sent:', stats.networkSentBytes, 'bytes');
console.log('Network received:', stats.networkReceivedBytes, 'bytes');

await db.close();
```

## API Reference

### Database Creation

#### `createLocalDatabase(path: string): Database`

Creates a local-only SQLite database (synchronous).

```typescript
const db = createLocalDatabase('./local.db');
```

**Parameters:**
- `path`: Path to database file. Use `:memory:` for in-memory database.

**Returns:** `Database` instance (already open)

---

#### `createSyncDatabase(config): Promise<Database>`

Creates an embedded replica database (async).

```typescript
const db = await createSyncDatabase({
  path: './replica.db',
  remoteUrl: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
  bootstrapIfEmpty: true,
  clientName: 'my-app',
  partialSync: {
    bootstrapStrategyPrefix: 2, // Sync tables with 'us' prefix
    segmentSize: 1024,
    prefetch: true,
  },
});
```

**Parameters:**
- `config.path`: Local database file path
- `config.remoteUrl`: Turso database URL
- `config.authToken`: Authentication token
- `config.bootstrapIfEmpty`: (optional) Bootstrap from remote if local DB is empty
- `config.clientName`: (optional) Client identifier
- `config.partialSync`: (optional) Partial sync configuration
  - `bootstrapStrategyPrefix`: Number of characters of table name prefix to sync
  - `bootstrapStrategyQuery`: SQL query to determine which rows to sync
  - `segmentSize`: Segment size for sync operations
  - `prefetch`: Whether to prefetch data

**Returns:** Promise resolving to `Database` instance

---

#### `openDatabase(config): Promise<Database>`

Universal database opener - detects local vs sync based on config.

```typescript
// Local database (string path)
const localDb = await openDatabase('./local.db');

// Sync database (config object with remoteUrl)
const syncDb = await openDatabase({
  path: './replica.db',
  remoteUrl: 'libsql://your-db.turso.io',
  authToken: 'your-auth-token',
});
```

**Parameters:**
- For local: `string` path
- For sync: config object (same as `createSyncDatabase`)

**Returns:** Promise resolving to `Database` instance

### Database Methods

All methods work for both local and sync databases unless noted otherwise.

#### `exec(sql: string): void`

Execute SQL without returning results. Use for DDL statements (CREATE, DROP, etc.).

```typescript
db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
db.exec('DROP TABLE IF EXISTS old_table');
```

---

#### `run(sql: string, ...params: SQLiteValue[]): RunResult`

Execute SQL and return execution metadata.

```typescript
const result = db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
console.log(result);
// { changes: 1, lastInsertRowid: 1 }
```

**Returns:** `{ changes: number, lastInsertRowid: number }`

---

#### `get(sql: string, ...params: SQLiteValue[]): Row | undefined`

Query and return first row.

```typescript
const user = db.get('SELECT * FROM users WHERE id = ?', 1);
// { id: 1, name: 'Alice' }

const missing = db.get('SELECT * FROM users WHERE id = ?', 999);
// undefined
```

**Returns:** First row as object, or `undefined` if no rows

---

#### `all(sql: string, ...params: SQLiteValue[]): Row[]`

Query and return all rows.

```typescript
const users = db.all('SELECT * FROM users WHERE age > ?', 18);
// [{ id: 1, name: 'Alice', age: 25 }, { id: 2, name: 'Bob', age: 30 }]
```

**Returns:** Array of row objects

---

#### `prepare(sql: string): Statement`

Create a prepared statement for efficient repeated execution.

```typescript
const stmt = db.prepare('INSERT INTO users (name, age) VALUES (?, ?)');

stmt.run('Alice', 25);
stmt.run('Bob', 30);
stmt.run('Charlie', 35);

stmt.finalize();
```

**Returns:** `Statement` instance

---

#### `transaction<T>(fn: () => T): T`

Execute function within a transaction. Automatically commits on success, rolls back on error.

```typescript
db.transaction(() => {
  db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
  db.run('INSERT INTO users (name) VALUES (?)', 'Bob');
  // Both inserts commit together, or both roll back on error
});
```

**Returns:** Return value of the function

---

#### `close(): void`

Close database connection. For sync databases, use `await db.close()`.

```typescript
db.close();
// or for sync databases
await db.close();
```

### Sync Database Methods

These methods only work for sync databases (created with `remoteUrl`).

#### `push(): Promise<void>`

Push local changes to remote database.

```typescript
db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
await db.push();
console.log('Local changes synced to remote');
```

---

#### `pull(): Promise<boolean>`

Pull remote changes to local database.

```typescript
const hasChanges = await db.pull();
if (hasChanges) {
  console.log('Applied remote changes');
} else {
  console.log('No remote changes');
}
```

**Returns:** `true` if changes were applied, `false` if no changes

---

#### `stats(): Promise<SyncStats>`

Get synchronization statistics.

```typescript
const stats = await db.stats();
console.log('CDC operations:', stats.cdcOperations);
console.log('Network sent:', stats.networkSentBytes, 'bytes');
console.log('Network received:', stats.networkReceivedBytes, 'bytes');
console.log('Last pull:', stats.lastPullUnixTime);
console.log('Last push:', stats.lastPushUnixTime);
console.log('Revision:', stats.revision);
```

**Returns:** `SyncStats` object

---

#### `checkpoint(): Promise<void>`

Checkpoint the database (flush WAL to main database file).

```typescript
await db.checkpoint();
```

### Statement Methods

#### `bind(...params: SQLiteValue[]): Statement`

Bind parameters to prepared statement.

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE age > ? AND city = ?');
stmt.bind(18, 'New York');
```

Supports both positional and named parameters:

```typescript
const stmt = db.prepare('INSERT INTO users (name, age) VALUES (:name, :age)');
stmt.bind({ name: 'Alice', age: 25 });
```

---

#### `run(...params: SQLiteValue[]): RunResult`

Execute statement and return metadata.

```typescript
const stmt = db.prepare('INSERT INTO users (name) VALUES (?)');
const result = stmt.run('Alice');
console.log(result.lastInsertRowid);
```

---

#### `get(...params: SQLiteValue[]): Row | undefined`

Execute statement and return first row.

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
const user = stmt.get(1);
```

---

#### `all(...params: SQLiteValue[]): Row[]`

Execute statement and return all rows.

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE age > ?');
const users = stmt.all(18);
```

---

#### `reset(): void`

Reset statement to reuse with different parameters.

```typescript
const stmt = db.prepare('INSERT INTO users (name) VALUES (?)');
stmt.run('Alice');
stmt.reset();
stmt.run('Bob');
```

---

#### `finalize(): void`

Finalize and free statement resources.

```typescript
const stmt = db.prepare('SELECT * FROM users');
// ... use statement ...
stmt.finalize();
```

### Database Properties

#### `db.isSync: boolean`

Check if database is a sync database.

```typescript
if (db.isSync) {
  await db.push();
}
```

---

#### `db.inTransaction: boolean`

Check if currently in a transaction.

```typescript
console.log(db.inTransaction); // false
db.transaction(() => {
  console.log(db.inTransaction); // true
});
```

---

#### `db.lastInsertRowid: number`

Get last inserted row ID.

```typescript
db.run('INSERT INTO users (name) VALUES (?)', 'Alice');
console.log(db.lastInsertRowid); // 1
```

---

#### `db.path: string`

Get database file path.

```typescript
console.log(db.path); // './mydb.sqlite'
```

---

#### `db.memory: boolean`

Check if database is in-memory.

```typescript
const memDb = createLocalDatabase(':memory:');
console.log(memDb.memory); // true
```

---

#### `db.open: boolean`

Check if database is open.

```typescript
console.log(db.open); // true
db.close();
console.log(db.open); // false
```

### Configuration

#### `setup(options): void`

Configure Turso settings. Call before opening databases.

```typescript
import { setup } from '@tursodatabase/react-native';

setup({ logLevel: 'debug' });
```

**Parameters:**
- `options.logLevel`: Logging level ('debug', 'info', 'warn', 'error')

---

#### `version(): string`

Get Turso library version.

```typescript
import { version } from '@tursodatabase/react-native';

console.log('Turso version:', version());
```

---

#### `setFileSystemImpl(impl): void`

Configure custom file system implementation for sync operations.

```typescript
import { setFileSystemImpl } from '@tursodatabase/react-native';
import RNFS from 'react-native-fs';

setFileSystemImpl({
  readFile: async (path) => {
    const content = await RNFS.readFile(path, 'base64');
    return Buffer.from(content, 'base64');
  },
  writeFile: async (path, data) => {
    const base64 = Buffer.from(data).toString('base64');
    await RNFS.writeFile(path, base64, 'base64');
  },
});
```

### Platform Paths

#### `paths.documents: string`

Get platform-specific documents directory.

```typescript
import { paths } from '@tursodatabase/react-native';

const dbPath = `${paths.documents}/mydb.sqlite`;
const db = createLocalDatabase(dbPath);
```

## TypeScript Support

Full TypeScript definitions are included:

```typescript
import type {
  Database,
  Statement,
  SQLiteValue,
  Row,
  RunResult,
  SyncStats,
  LocalDatabaseConfig,
  SyncDatabaseConfig,
} from '@tursodatabase/react-native';
```

## Examples

### Transaction with Error Handling

```typescript
try {
  db.transaction(() => {
    db.run('INSERT INTO accounts (user_id, balance) VALUES (?, ?)', 1, 100);
    db.run('INSERT INTO transactions (user_id, amount) VALUES (?, ?)', 1, -50);
    db.run('UPDATE accounts SET balance = balance - 50 WHERE user_id = ?', 1);
  });
  console.log('Transaction committed');
} catch (error) {
  console.error('Transaction rolled back:', error);
}
```

### Efficient Batch Inserts

```typescript
db.transaction(() => {
  const stmt = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');

  for (const user of users) {
    stmt.run(user.name, user.email);
  }

  stmt.finalize();
});
```

### Sync Database with Periodic Updates

```typescript
const db = await createSyncDatabase({
  path: './app.db',
  remoteUrl: process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
});

// Pull updates every 30 seconds
setInterval(async () => {
  try {
    const hasChanges = await db.pull();
    if (hasChanges) {
      console.log('New data available');
      // Update UI
    }
  } catch (error) {
    console.error('Sync failed:', error);
  }
}, 30000);

// Push changes when user makes updates
async function saveUser(name: string) {
  db.run('INSERT INTO users (name) VALUES (?)', name);
  try {
    await db.push();
    console.log('Changes saved to cloud');
  } catch (error) {
    console.error('Failed to push:', error);
    // Changes are still saved locally
  }
}
```

### Partial Sync - Table Prefix

```typescript
// Only sync tables starting with 'user_'
const db = await createSyncDatabase({
  path: './partial.db',
  remoteUrl: process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
  partialSync: {
    bootstrapStrategyPrefix: 5, // 'user_' = 5 chars
    prefetch: true,
  },
});

// Tables like 'user_profiles', 'user_settings' will sync
// Other tables won't be synced
```

### Partial Sync - Query-based

```typescript
// Only sync specific users
const db = await createSyncDatabase({
  path: './partial.db',
  remoteUrl: process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
  partialSync: {
    bootstrapStrategyQuery: 'SELECT * FROM users WHERE region = "US"',
    segmentSize: 2048,
  },
});
```

## Architecture

The React Native SDK uses a **thin JSI layer** architecture:

```
TypeScript (Database, Statement) + async operation logic
    ↓
JSI HostObjects (1:1 mapping of SDK-KIT C API)
    ↓
Rust sync-sdk-kit (static library)
```

**Key Design Principles:**
- JSI layer has **zero logic** - just exposes C API to JavaScript
- All business logic is in TypeScript or Rust
- IO operations (HTTP, file system) handled by JavaScript using `fetch()` and React Native APIs
- Network requests visible in React Native debugger

## License

MIT

## Links

- [Turso Documentation](https://docs.turso.tech)
- [GitHub Repository](https://github.com/tursodatabase/turso)
- [npm Package](https://www.npmjs.com/package/@tursodatabase/react-native)
