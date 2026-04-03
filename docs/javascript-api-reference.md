# JavaScript API reference

This document describes the JavaScript API for Turso. The API is implemented in two different packages:

- [@tursodatabase/database](https://www.npmjs.com/package/@tursodatabase/database) (`bindings/javascript`) - Native bindings for the Turso database.
- [@tursodatabase/serverless](https://www.npmjs.com/package/@tursodatabase/serverless) (`serverless/javascript`) - Serverless driver for Turso Cloud databases.

The API is compatible with the libSQL promise API, which is an asynchronous variant of the `better-sqlite3` API.

## Functions

#### connect(path, [options]) ⇒ Database

Opens a new database connection.

| Param       | Type                     | Description                              |
| ----------- | ------------------------ | ---------------------------------------- |
| path        | <code>string</code>      | Path to the database file                |
| options     | <code>object</code>      | Optional configuration                   |

The `path` parameter points to the SQLite database file to open. If the file pointed to by `path` does not exists, it will be created.
To open an in-memory database, please pass `:memory:` as the `path` parameter.

Supported `options` fields include:

- `timeout`: busy timeout in milliseconds
- `defaultQueryTimeout`: default maximum query execution time in milliseconds before interruption

Per-query timeout override is available via `queryOptions`, for example:

- `db.exec("SELECT 1", { queryTimeout: 100 })`
- `stmt.get(undefined, { queryTimeout: 100 })`

WASM UDF support can be enabled via the `unstableWasmRuntime` option. See [WASM UDFs](#wasm-user-defined-functions-udfs) below for details.

The function returns a `Database` object.

## class Database

The `Database` class represents a connection that can prepare and execute SQL statements.

### Methods

#### prepare(sql) ⇒ Statement

Prepares a SQL statement for execution.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to prepare. |

The function returns a `Statement` object.

#### transaction(function) ⇒ function

This function is currently not supported.

#### pragma(string, [options]) ⇒ results

This function is currently not supported.

#### backup(destination, [options]) ⇒ promise

This function is currently not supported.

#### serialize([options]) ⇒ Buffer

This function is currently not supported.

#### function(name, [options], function) ⇒ this

This function is currently not supported.

#### aggregate(name, options) ⇒ this

This function is currently not supported.

#### table(name, definition) ⇒ this

This function is currently not supported.

#### authorizer(rules) ⇒ this

This function is currently not supported.

#### loadExtension(path, [entryPoint]) ⇒ this

This function is currently not supported.

#### exec(sql) ⇒ this

Executes a SQL statement.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to execute. |

#### interrupt() ⇒ this

This function is currently not supported.

#### close() ⇒ this

Closes the database connection.

## class Statement

### Methods

#### run([...bindParameters]) ⇒ object

Executes the SQL statement and returns an info object.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

The returned info object contains two properties: `changes` that describes the number of modified rows and `info.lastInsertRowid` that represents the `rowid` of the last inserted row.

#### get([...bindParameters]) ⇒ row

Executes the SQL statement and returns the first row.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

### all([...bindParameters]) ⇒ array of rows

Executes the SQL statement and returns an array of the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

### iterate([...bindParameters]) ⇒ iterator

Executes the SQL statement and returns an iterator to the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

#### pluck([toggleState]) ⇒ this

This function is currently not supported.

#### expand([toggleState]) ⇒ this

This function is currently not supported.

#### raw([rawMode]) ⇒ this

This function is currently not supported.

#### timed([toggle]) ⇒ this

This function is currently not supported.

#### columns() ⇒ array of objects

This function is currently not supported.

#### bind([...bindParameters]) ⇒ this

This function is currently not supported.

## WASM User-Defined Functions (UDFs)

Turso supports user-defined functions written in WebAssembly. WASM UDF support is **opt-in** — by default, no WASM runtime is loaded, so users who don't need UDFs pay zero overhead.

To enable WASM UDFs, pass a WASM runtime as the third argument to `connect()` or `new Database()`.

### Choosing a runtime

Two runtimes are available:

| Runtime | Package | Fuel metering | Timeout | Overhead |
| ------- | ------- | ------------- | ------- | -------- |
| Native (`createUnstableNativeWasmRuntime`) | `@tursodatabase/database` (included) | No | No | Zero — calls host `WebAssembly` API directly |
| Wasmtime (`createWasmtimeRuntime`) | `@tursodatabase/wasm-runtime-wasmtime` (separate) | Yes | 2s epoch-based | Extra JS boundary crossing per WASM call |

**Native runtime** delegates to the host engine's built-in `WebAssembly` API (V8 in Node, JavaScriptCore in Bun). It has zero additional overhead but provides no fuel metering or timeout — a runaway UDF will block the thread indefinitely.

**Wasmtime runtime** uses the `wasmtime` engine compiled as a separate native addon. It provides fuel-based instruction counting and a 2 second wall-clock timeout via epoch interruption. UDFs exceeding 2 seconds are terminated and return an error, preventing runaway functions from blocking the process. The trade-off is an extra JS↔native boundary crossing per WASM operation, which is negligible since actual WASM execution dominates.

### Usage

#### Native runtime (included in the main SDK)

```javascript
import { connect, createUnstableNativeWasmRuntime } from "@tursodatabase/database";

const db = await connect(":memory:", { unstableWasmRuntime: createUnstableNativeWasmRuntime() });

// Register a WASM UDF from a hex-encoded binary
await db.exec("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'");

// Use it in queries
const row = await db.prepare("SELECT add2(40, 2) AS result").get();
console.log(row.result); // 42
```

#### Wasmtime runtime (separate package)

```javascript
import { connect } from "@tursodatabase/database";
import { createWasmtimeRuntime } from "@tursodatabase/wasm-runtime-wasmtime";

const db = await connect(":memory:", {}, createWasmtimeRuntime());

await db.exec("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'");
const row = await db.prepare("SELECT add2(40, 2) AS result").get();
```

#### No runtime (default)

```javascript
import { connect } from "@tursodatabase/database";

const db = await connect(":memory:");

// This will fail with "no WASM runtime registered"
await db.exec("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'");
```

### SQL syntax

```sql
-- Register from inline hex blob
CREATE FUNCTION name LANGUAGE wasm AS X'<hex>' EXPORT 'export_name';

-- Idempotent creation
CREATE FUNCTION IF NOT EXISTS name LANGUAGE wasm AS X'<hex>' EXPORT 'export_name';

-- Replace existing
CREATE OR REPLACE FUNCTION name LANGUAGE wasm AS X'<hex>' EXPORT 'export_name';

-- Remove
DROP FUNCTION name;
DROP FUNCTION IF EXISTS name;
```

### WASM module requirements

WASM modules must export:

- **`memory`** — a WebAssembly memory (at least 2 pages / 128 KB)
- **`turso_malloc(size: i32) → i32`** — a bump allocator returning a pointer
- **The UDF function** — `(argc: i32, argv: i32) → i64`, specified by the `EXPORT` clause
