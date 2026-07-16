# @tursodatabase/pg-experimental API

This package exposes Turso's Postgres frontend through an API compatible with
[PGlite](https://pglite.dev). Code written against `@electric-sql/pglite`
swaps over by changing the import. The design rationale and the exact
compatibility contract live in [postgres/PGLITE.md](../../PGLITE.md); this
document is the reference for what the package supports.

The SQL dialect the engine accepts is documented in
[postgres/COMPAT.md](../../COMPAT.md) — this page covers only the JavaScript
API surface.

# class PGlite

The `PGlite` class represents a database connection that can execute Postgres
SQL. It is the package's only entry point.

```ts
import { PGlite } from "@tursodatabase/pg-experimental";

const db = await PGlite.create("app.db");
const res = await db.query("SELECT * FROM users WHERE id = $1", [1]);
```

## Methods

### new PGlite([path], [options]) ⇒ PGlite

Creates a database connection and starts opening it. The instance is usable
once `waitReady` resolves.

| Param   | Type                | Description                            |
| ------- | ------------------- | -------------------------------------- |
| path    | <code>string</code> | Path to the database file (optional).  |
| options | <code>object</code> | Options (optional).                    |

If the file pointed to by `path` does not exist, it is created. Omitting
`path` (or passing `memory://`, PGlite's spelling) opens an in-memory
database; `:memory:` also works. A `file://` prefix is stripped.

Supported `options` fields:

- `parsers`: `{ [oid: number]: (value: string) => any }` — override how result
  values of a given Postgres type OID are converted to JS (see Type
  conversion).
- `serializers`: `{ [oid: number]: (value: any) => string }` — override how JS
  parameter values are converted for a given type OID.
- `debug`: log level, `0`–`5`.
- `readonly`: open the database in read-only mode.
- `fileMustExist`: throw instead of creating a missing file.
- `timeout`: busy timeout in milliseconds.
- `defaultQueryTimeout`: default maximum query execution time in milliseconds.
- `tracing`: `'info' | 'debug' | 'trace'`.
- `encryption`: local encryption configuration.

PGlite construction options that describe WASM or browser machinery — `fs`,
`wasmModule`, `fsBundle`, `initialMemory`, `extensions`, `loadDataDir`,
`username`, `database` — are **rejected with an error**, not silently
ignored.

Constructing two instances on the same path yields two independent
connections; concurrency between them follows the engine's WAL/MVCC rules.
(Original PGlite supports only a single connection.)

### PGlite.create([path], [options]) ⇒ Promise&lt;PGlite&gt;

Constructs a `PGlite` and awaits `waitReady`. Same parameters as the
constructor. This is the recommended way to open a database.

### query(sql, [params], [options]) ⇒ Promise&lt;Results&gt;

Executes a single SQL statement with optional positional parameters.

| Param   | Type                | Description                                  |
| ------- | ------------------- | -------------------------------------------- |
| sql     | <code>string</code> | The SQL statement. Exactly one statement.    |
| params  | <code>any[]</code>  | Values for `$1..$n` placeholders (optional). |
| options | <code>object</code> | Query options (optional).                    |

Passing more than one statement is an error. Parameter conversion is driven
by the parameter type the prepared statement reports, not by the JS type
alone.

Supported `options` fields:

- `rowMode`: `'object'` (default) returns rows as `{ column: value }`
  objects; `'array'` returns rows as positional arrays.
- `parsers`, `serializers`: per-call overrides of the constructor options.
- `paramTypes`: `number[]` — explicit Postgres type OIDs for the parameters.
- `queryTimeout`: per-query timeout in milliseconds (Turso extension).

### sql\`...\` ⇒ Promise&lt;Results&gt;

Tagged-template form of `query()`; interpolated values become parameters.

```ts
const res = await db.sql`SELECT * FROM users WHERE id = ${id}`;
```

### exec(sql, [options]) ⇒ Promise&lt;Results[]&gt;

Executes one or more SQL statements without parameters and returns one
`Results` per statement, in order. This is the method to use for DDL and
migrations.

| Param   | Type                | Description                        |
| ------- | ------------------- | ---------------------------------- |
| sql     | <code>string</code> | One or more `;`-separated statements. |
| options | <code>object</code> | Query options (optional).          |

Note: unlike original PGlite, `affectedRows` is accurate per statement.
(PGlite groups protocol messages in a way that leaks row counts of
result-set-less statements into the following result; we do not reproduce
that bug.)

### transaction(callback) ⇒ Promise&lt;T&gt;

Runs `callback` inside a transaction and returns its result.

| Param    | Type                  | Description                              |
| -------- | --------------------- | ---------------------------------------- |
| callback | <code>function</code> | `async (tx) => ...` receiving a `Transaction`. |

Semantics:

- `BEGIN` is issued before the callback, `COMMIT` after it resolves.
- If the callback throws (including a failed statement), the transaction is
  rolled back and the error is re-thrown.
- `tx.rollback()` rolls back *without* throwing; the callback's return value
  is still returned.
- The connection holds an async mutex for the duration: queries issued on the
  connection outside the callback wait until the transaction finishes.

The `Transaction` object passed to the callback supports:

- `tx.query(sql, [params], [options])` — as `PGlite.query`.
- `` tx.sql`...` `` — as `PGlite.sql`.
- `tx.exec(sql, [options])` — as `PGlite.exec`.
- `tx.rollback()` — roll back and end the transaction without throwing.
- `tx.closed` — `false` inside the callback, `true` after it settles.

### close() ⇒ Promise&lt;void&gt;

Closes the database. Afterwards `closed` is `true`, `ready` is `false`, and
any method call rejects. Also available via `await using` (the class
implements `Symbol.asyncDispose`).

## Properties

### waitReady ⇒ Promise&lt;void&gt;

Resolves when the database is open. `PGlite.create()` awaits this for you.

### ready ⇒ boolean

`true` once the database is open, `false` after `close()`.

### closed ⇒ boolean

`true` after `close()`.

### path ⇒ string

The database file path, or `:memory:`. (Turso extension.)

# Results

Every query method resolves to this shape:

```ts
type Results<T = { [key: string]: any }> = {
  rows: T[];                                       // objects, or arrays with rowMode: 'array'
  fields: { name: string; dataTypeID: number }[];  // dataTypeID is the Postgres type OID
  affectedRows?: number;
};
```

- `INSERT`/`UPDATE`/`DELETE` without `RETURNING`: `rows: []`, `fields: []`,
  `affectedRows` set to the number of affected rows.
- With `RETURNING`: `rows`/`fields` populated, `affectedRows` still set.
- Plain `SELECT` via `query()`: `affectedRows: 0`.

# Type conversion

Result values are converted from Postgres to JS by result-column type OID:

| Postgres type (OID) | JS value |
| --- | --- |
| `bool` (16) | `boolean` |
| `int2`/`int4`/`oid` (21/23/26) | `number` |
| `int8` (20) | `number`; `bigint` when outside `Number.MAX_SAFE_INTEGER` |
| `float4`/`float8` (700/701) | `number` |
| `numeric` (1700) | `string` (precision preserved) |
| `text`/`varchar`/`bpchar` (25/1043/1042) | `string` |
| `bytea` (17) | `Uint8Array` |
| `json`/`jsonb` (114/3802) | `JSON.parse`d value |
| `date`/`timestamp`/`timestamptz` (1082/1114/1184) | `Date` |
| `uuid` (2950), `interval`, any unknown OID | `string` passthrough |
| array types (`_int4`, …) | JS array, element conversion applied recursively |
| SQL `NULL` | `null` |

Parameters accept the same JS types in the other direction: `null`/`undefined`
→ NULL; `boolean`; `number`/`bigint`; `string`; `Date` → timestamp;
`Uint8Array` → bytea; plain objects and arrays bound to json parameters are
`JSON.stringify`ed; JS arrays bound to array parameters become Postgres array
values.

Custom `parsers`/`serializers` (constructor- or query-level, keyed by OID)
receive and produce the Postgres *text* representation, exactly as in PGlite.

The type OID constants and default conversion functions are exported as the
`types` module:

```ts
import { types } from "@tursodatabase/pg-experimental";
types.NUMERIC; // 1700
```

# Errors

Failed queries reject with a `DatabaseError`:

```ts
class DatabaseError extends Error {
  severity: string;      // 'ERROR'
  code: string;          // SQLSTATE, e.g. '42P01' undefined_table, '23505' unique_violation
  detail?: string;
  hint?: string;
  position?: string;
  schema?: string;
  table?: string;
  column?: string;
  constraint?: string;
  routine?: string;
  query?: string;        // the SQL that failed
  params?: any[];        // its parameters
}
```

Unlike original PGlite, the class is exported, so both `instanceof` checks and
PGlite-style shape matching on `.code`/`.severity` work.

# Unsupported PGlite APIs

Rejected or absent, by design:

| API | Why |
| --- | --- |
| `dataDir` URI schemes (`idb://`, `opfs-ahp://`), `fs`, browser filesystems | WASM/browser artifacts; we open real files |
| `wasmModule`, `fsBundle`, `initialMemory`, `username`, `database` options | no WASM to boot; single-user embedded engine |
| `dumpDataDir()`, `loadDataDir`, `clone()` | the database is a regular file; copy it |
| `extensions` (WASM bundle loading), `PGliteWorker` | Emscripten-specific |
| `execProtocol()`, `execProtocolRaw()`, `execProtocolStream()`, `runExclusive()` | no wire protocol in-process; use `pgmicro --server` for real pgwire |

Not yet supported, planned:

| API | Status |
| --- | --- |
| `describeQuery(sql)` | needs prepare-only support in `turso_pg` |
| `QueryOptions.onNotice` | needs notice plumbing from the engine |
| `listen()`, `unlisten()`, `onNotification()`, `offNotification()` | LISTEN/NOTIFY has no engine support yet |
| `refreshArrayTypes()` | matters once `CREATE TYPE` lands; no-op until then |
| `QueryOptions.blob`, `Results.blob` (`/dev/blob` COPY) | needs COPY TO and blob plumbing |
| Template helpers (`identifier`, `raw`, …) | pure JS; added when an integration needs them |
| Live queries (`live.query`, `live.incrementalQuery`, `live.changes`) | v2 candidate, backed by DBSP incremental materialized views |

# Conformance

The behavior documented here is pinned by `postgres/conformance/js`, which
runs the same test suite against real PGlite (`PROVIDER=pglite`) and this
package (`PROVIDER=turso`).
