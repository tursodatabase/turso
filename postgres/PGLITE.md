# `postgres/js` — a PGlite-compatible JavaScript API for Turso

This document specifies the API of the planned `postgres/js` package
(`@tursodatabase/pg-experimental`): a JavaScript package exposing Turso's Postgres
frontend (`turso_pg`) through an API compatible with
[PGlite](https://pglite.dev), ElectricSQL's Postgres-in-WASM. PGlite's API is
the de facto interface for an embedded Postgres in JavaScript — Drizzle,
Kysely, Prisma, and a long tail of test tooling integrate against it.

All PGlite findings below are ground-truthed against `@electric-sql/pglite`
0.5.4: its shipped TypeScript definitions, the official docs
(https://pglite.dev/docs/api), and empirical probes run in Node. The
conformance suite in `postgres/conformance/js` runs against real PGlite and
encodes the contract; the same tests must pass against our package. The
user-facing API reference for the package is
[postgres/js/docs/api.md](js/docs/api.md); this document is the design
rationale behind it.

## Design principle: compatible where it counts

The load-bearing part of PGlite's API is the **query surface**:

1. the methods `query()`, `` sql`` ``, `exec()`, `transaction()`, `close()`;
2. the `Results` shape (`rows`, `fields` with type OIDs, `affectedRows`);
3. OID-driven conversion between Postgres values and JS values;
4. errors carrying SQLSTATE codes as `.code`.

Every ORM adapter and integration duck-types a PGlite instance against exactly
this surface. Nothing downstream depends on *how the instance is constructed*
— `dataDir` URI schemes (`idb://`, `opfs-ahp://`), virtual filesystems, and
WASM module loading are all artifacts of booting a WASM Postgres in a browser.
We are a native embedded engine with real files; we don't need any of it.

So the design is **one API, one class**: the package exports a class named
`PGlite`, constructor-compatible with the original — `new PGlite(...)` and
`await PGlite.create(...)` are the only ways to open a database, exactly as in
PGlite itself. Construction takes a plain file path or `:memory:`; everything
is PGlite's contract, method-for-method and shape-for-shape, verified by the
conformance suite. What we drop is the browser/WASM machinery underneath the
constructor: no dataDir URI schemes, no virtual filesystems, no WASM options.

## The API

```ts
import { PGlite } from "@tursodatabase/pg-experimental";

const db = await PGlite.create("app.db");   // file-backed
const mem = await PGlite.create();          // in-memory

await db.exec(`
  CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
  INSERT INTO users (name) VALUES ('Alice'), ('Bob');
`);

const res = await db.query("SELECT * FROM users WHERE id = $1", [1]);
// { rows: [{ id: 1, name: 'Alice' }],
//   fields: [{ name: 'id', dataTypeID: 23 }, { name: 'name', dataTypeID: 25 }],
//   affectedRows: 0 }

await db.transaction(async (tx) => {
  await tx.query("UPDATE users SET name = $1 WHERE id = $2", ["Carol", 2]);
});

await db.close();
```

### The `PGlite` class

```ts
class PGlite {
  // Drop-in constructor: starts the async open; waitReady resolves it.
  // No path or "memory://" means :memory:; a "file://" prefix is stripped.
  constructor(path?: string, options?: PGliteOptions)
  // Constructs and awaits waitReady — the recommended way to open.
  static create(path?: string, options?: PGliteOptions): Promise<PGlite>

  // -- the PGlite query surface, byte-for-byte --
  query<T>(sql: string, params?: any[], options?: QueryOptions): Promise<Results<T>>
  sql<T>(strings: TemplateStringsArray, ...params: any[]): Promise<Results<T>>
  exec(sql: string, options?: QueryOptions): Promise<Results[]>
  transaction<T>(cb: (tx: Transaction) => Promise<T>): Promise<T>
  close(): Promise<void>

  readonly waitReady: Promise<void> // resolves when the open completes
  readonly ready: boolean           // true once open, false after close()
  readonly closed: boolean
  [Symbol.asyncDispose](): Promise<void>   // === close()

  // -- Turso extension --
  readonly path: string
}

interface Transaction {
  query<T>(sql: string, params?: any[], options?: QueryOptions): Promise<Results<T>>
  sql<T>(strings: TemplateStringsArray, ...params: any[]): Promise<Results<T>>
  exec(sql: string, options?: QueryOptions): Promise<Results[]>
  rollback(): Promise<void>
  get closed(): boolean
}

interface PGliteOptions {
  // PGlite-compatible type customization (see "Type mapping")
  parsers?: { [oid: number]: (value: string) => any }
  serializers?: { [oid: number]: (value: any) => string }
  debug?: number

  // Turso options, aligned with @tursodatabase/database DatabaseOpts
  readonly?: boolean
  fileMustExist?: boolean
  timeout?: number                  // busy timeout, ms
  defaultQueryTimeout?: number      // per-query timeout default, ms
  tracing?: 'info' | 'debug' | 'trace'
  encryption?: EncryptionOpts
}
```

Constructing two instances on the same path gives two independent
connections; concurrency between them follows the engine's WAL/MVCC rules.
This is a capability PGlite fundamentally lacks (one WASM backend, one
connection) and is deliberately *outside* the compat contract — code written
against PGlite never does it, code written for Turso can.

Method semantics (pinned by `postgres/conformance/js`):

- **`query()`** — one statement, `$1..$n` positional parameters. Multiple
  statements are an error. Parameter conversion is driven by the parameter
  type the prepared statement reports, not by the JS type alone (a JS `7`
  bound to a `TEXT` column arrives as `'7'`).
- **`` sql`` ``** — tagged template; interpolations become parameters:
  `` db.sql`SELECT * FROM users WHERE id = ${id}` ``.
- **`exec()`** — multiple statements, no parameters, one `Results` per
  statement, in order. The workhorse for DDL and migrations. Statement
  splitting uses `turso_pg::split_statements`.
- **`transaction(cb)`** — runs `BEGIN`, the callback, `COMMIT`; any throw from
  the callback (including a failed statement) triggers `ROLLBACK` and
  re-throws. `tx.rollback()` rolls back *without* throwing and the callback's
  return value still comes back. `tx.closed` is `false` inside the callback,
  `true` after it settles. The connection holds an async mutex for the
  duration, so unrelated queries on the same connection never interleave with
  an open transaction — same observable behavior as PGlite, and the natural
  behavior for a single engine connection anyway.
- **`close()`** — after it resolves, `closed === true`, `ready === false`,
  and any method call rejects.

### `Results` and `QueryOptions`

```ts
type Results<T = { [key: string]: any }> = {
  rows: T[]                                       // objects; arrays with rowMode: 'array'
  fields: { name: string; dataTypeID: number }[]  // dataTypeID = Postgres type OID
  affectedRows?: number
}

interface QueryOptions {
  rowMode?: 'object' | 'array'    // default 'object'
  parsers?: { [oid: number]: (value: string) => any }     // per-call override
  serializers?: { [oid: number]: (value: any) => string } // per-call override
  paramTypes?: number[]           // explicit parameter OIDs
  queryTimeout?: number           // Turso extension, from our SDK
}
```

Verified reference behavior: INSERT without RETURNING → `rows: []`,
`fields: []`, `affectedRows: n`; RETURNING populates `rows`/`fields` and keeps
`affectedRows`; plain SELECT via `query()` has `affectedRows: 0`.

One deliberate divergence: PGlite's `exec()` groups protocol messages by
RowDescription, so `affectedRows` of statements without result sets leaks into
the following result (a DELETE of 4 rows after an INSERT reports 5). That is a
bug, not a contract; we report accurate per-statement counts and the
conformance tests do not assert the buggy behavior.

`QueryOptions.blob` / `Results.blob` (PGlite's `/dev/blob` COPY convention) is
omitted in v1 — see "Omitted and deferred".

### Type mapping

Result values are converted to JS by result-column OID; this table is the
contract (verified against PGlite 0.5.4) and must be reproduced exactly:

| Postgres type (OID) | JS value |
|---|---|
| `bool` (16) | `boolean` |
| `int2`/`int4`/`oid` (21/23/26) | `number` |
| `int8` (20) | `number`; **`bigint` when outside `Number.MAX_SAFE_INTEGER`** |
| `float4`/`float8` (700/701) | `number` |
| `numeric` (1700) | `string` (precision preserved; no default parser) |
| `text`/`varchar`/`bpchar` (25/1043/1042) | `string` |
| `bytea` (17) | `Uint8Array` |
| `json`/`jsonb` (114/3802) | `JSON.parse`d value |
| `date`/`timestamp`/`timestamptz` (1082/1114/1184) | `Date` |
| `uuid` (2950), `interval`, any unknown OID | `string` passthrough |
| array types (`_int4`, …) | JS array, element conversion applied recursively |
| SQL `NULL` | `null` |

Parameters accept the same JS types in the other direction: `null`/`undefined`
→ NULL; `boolean`; `number`/`bigint`; `string`; `Date` → timestamp;
`Uint8Array` → bytea; plain objects/arrays bound to json parameters →
`JSON.stringify`; JS arrays bound to array parameters → Postgres array values.

PGlite funnels every value through Postgres *text format* because it speaks
the wire protocol to its WASM backend. We are in-process and may bind and
fetch natively — the text round-trip is not part of the contract, only the
resulting JS values are. The `parsers`/`serializers` hooks *are* part of the
contract and receive/produce text form, so values with a registered custom
parser are converted via their text representation, exactly as in PGlite.

`fields[].dataTypeID` must report real Postgres OIDs. The mapping from engine
types to OIDs already exists in `turso_pg_server` (`sqlite_type_to_pg_type`,
including array element OIDs and STRUCT/UNION → JSONB) and is shared so the
in-process API and the wire server agree.

Like PGlite, we export a `types` module with the OID constants and default
parsers/serializers:

```ts
import { types } from "@tursodatabase/pg-experimental";
types.NUMERIC // 1700
```

### Errors

Query failures reject with a `DatabaseError` matching PGlite's shape:

```ts
class DatabaseError extends Error {
  severity: string          // 'ERROR'
  code: string              // SQLSTATE: '42P01' undefined_table, '23505' unique_violation, ...
  detail?: string
  hint?: string
  position?: string
  schema?: string
  table?: string
  column?: string
  constraint?: string
  routine?: string
  // PGlite-added context, kept for compat:
  query?: string
  params?: any[]
}
```

PGlite does not export its error class, so all downstream code matches on
`.code`/`.severity` rather than `instanceof` — meaning only the shape is
contractual. We *do* export `DatabaseError` (strictly more useful), and the
real compat work is in `turso_pg`: engine errors must map to correct SQLSTATE
codes. This mapping is shared with the wire server, which needs it anyway.

## Drop-in construction

Because the one exported class is named `PGlite` and its constructor follows
PGlite's calling convention, existing PGlite code swaps over by changing only
the import:

```ts
import { PGlite } from "@tursodatabase/pg-experimental";

const db = new PGlite();                  // in-memory
const db2 = new PGlite("memory://");      // in-memory (PGlite spelling, accepted)
const db3 = new PGlite("./data.db");      // file (PGlite treats this as a dir; we use a file)
const db4 = await PGlite.create("./data.db");
await db.waitReady;
```

Construction options that describe WASM/browser machinery (`fs`, `wasmModule`,
`fsBundle`, `initialMemory`, `extensions`, `loadDataDir`) are rejected with a
clear error rather than silently ignored. This is also how the conformance
suite runs against us: `PROVIDER=turso` runs the exact same test code that
drives real PGlite.

## Omitted and deferred

| PGlite API | Status | Rationale |
|---|---|---|
| `dataDir` schemes, `fs`, browser filesystems | **omitted** | WASM/browser artifacts; we open real files |
| `wasmModule`, `fsBundle`, `initialMemory`, `username`, `database` | **omitted** | no WASM to boot; single-user embedded engine |
| `dumpDataDir()` / `loadDataDir` / `clone()` | **omitted** | the database *is* a regular file; copy it |
| `extensions` (WASM bundle loading), PGliteWorker | **omitted** | Emscripten-specific; our extension story differs |
| `execProtocol` / `execProtocolRaw` / `execProtocolStream`, `runExclusive` | **omitted** | wire-protocol escape hatch; in-process there is no wire. `pgmicro --server` covers clients that need real pgwire |
| `describeQuery(sql)` | **deferred** | param OIDs + result fields without executing; some tooling wants it; needs prepare-only support exposed from `turso_pg` |
| `QueryOptions.onNotice` | **deferred** | needs notice plumbing from the engine |
| `listen()` / `unlisten()` / `onNotification()` | **deferred** | LISTEN/NOTIFY has no engine support yet |
| `refreshArrayTypes()` | **deferred** | matters once CREATE TYPE lands; until then a no-op export |
| `QueryOptions.blob` + `Results.blob` (`/dev/blob` COPY) | **deferred** | needs COPY TO and blob plumbing |
| `@electric-sql/pglite/template` helpers (`identifier`, `raw`, …) | **deferred** | pure JS, no engine dependency; add when an integration needs them |
| Live queries (`live.query`, `live.incrementalQuery`, `live.changes`) | **v2 candidate** | our DBSP-based incremental materialized views are a *better* backend for this than PGlite's re-execute-on-change; the standout differentiation opportunity |

## Implementation sketch

- **Crate/package layout**: `postgres/js` mirrors `bindings/javascript` — a
  napi-rs crate binding `turso_pg` (`PgConnection`, `open_database`,
  `split_statements`), and a TypeScript layer implementing the contract above.
  The TS layer reuses `@tursodatabase/database-common` machinery where it fits
  (AsyncLock for the transaction mutex, async I/O stepping).
- **Not over TCP.** The binding embeds the frontend in-process; the wire
  protocol is `turso_pg_server`'s job. What must agree between the two are the
  OID mapping and the SQLSTATE mapping, both of which live in shared code.
- **`query()`** prepares via `turso_pg`, binds parameters (converting JS
  values per the parameter's reported type), steps, and assembles `Results`
  with OIDs from the shared type mapping.
- **`exec()`** splits with `split_statements` and runs each statement,
  collecting accurate per-statement `affectedRows`.
- **`transaction()`** issues BEGIN/COMMIT/ROLLBACK and holds the connection's
  async mutex.
- **What SQL works** is bounded by `postgres/COMPAT.md`, not by the binding.
  The conformance tests stick to SQL our stack supports or must soon support
  (SERIAL, RETURNING, ON CONFLICT, transactions, JSON/JSONB, arrays).
- **WASM build later**: `bindings/javascript` already has wasm packages; the
  same TS contract layer can sit on a wasm build of the engine, which would
  make us a true browser-side PGlite alternative. Not v1.

## Conformance tests

`postgres/conformance/js` mirrors `testing/conformance/javascript`: ava,
`__test__/*.test.js`, a provider-switching `connect()` helper.

```bash
cd postgres/js && npm install && npm run build   # build the native module first
cd ../conformance/js
npm install
npm run test:pglite         # the reference: 40/40
npm run test:turso          # our implementation: 35/40 (see below)
```

Status: all query, transaction, and lifecycle tests pass against
`@tursodatabase/pg-experimental`. The 5 remaining failures are engine-level
typing gaps for *cast/constructor expressions in SELECT* (`'…'::uuid`,
`'…'::jsonb`, `'…'::date`, `ARRAY[1,2,3]`, `numeric` scale) — the PG
translator collapses those cast targets to TEXT because a CAST to a
registered custom type would produce the *stored* representation (see the
encode-only semantics in `core/translate/expr/translator.rs`). Real table
columns of every one of these types convert correctly. Fixing the rest means
either carrying the original PG type name through `ast::Type` or giving CAST
encode-then-decode semantics in the engine — both engine work, tracked as
known gaps, not driver work.

Current coverage: `query.test.js` (results shape, params, affectedRows,
RETURNING, rowMode, error codes, upsert, lifecycle), `types.test.js` (the type
mapping table above, both directions, custom parsers), `transaction.test.js`
(commit, rollback-on-throw, `tx.rollback()`, `tx.closed`, visibility
semantics). Growth areas: `describeQuery`, template helpers, LISTEN/NOTIFY —
add the test first, then the feature.
