# JavaScript API reference

This document describes the JavaScript API for Turso. The API is implemented in two different packages:

- [@tursodatabase/database](https://www.npmjs.com/package/@tursodatabase/database) (`bindings/javascript`) - Native bindings for the Turso database.
- [@tursodatabase/serverless](https://www.npmjs.com/package/@tursodatabase/serverless) (`serverless/javascript`) - Serverless driver for Turso Cloud databases.

The API is compatible with the libSQL promise API, which is an asynchronous variant of the `better-sqlite3` API.

## Functions

#### connect(path, [options]) ⇒ Database

Opens a new database connection.

| Param   | Type                | Description               |
| ------- | ------------------- | ------------------------- |
| path    | <code>string</code> | Path to the database file |

The `path` parameter points to the SQLite database file to open. If the file pointed to by `path` does not exists, it will be created.
To open an in-memory database, please pass `:memory:` as the `path` parameter.

Supported `options` fields include:

- `timeout`: busy timeout in milliseconds
- `defaultQueryTimeout`: default maximum query execution time in milliseconds before interruption

Per-query timeout override is available via `queryOptions`, for example:

- `db.exec("SELECT 1", { queryTimeout: 100 })`
- `stmt.get(undefined, { queryTimeout: 100 })`

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

#### batch(statements, [mode]) ⇒ object

Executes an array of SQL statements over the connection. Each statement is either a SQL string or an object of the form `{ sql, args }`, where `args` is an array of positional bind parameters or an object of named bind parameters.

| Param      | Type                                                                                                                | Description                                                                                                                                                       |
| ---------- | ------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| statements | <code>Array&lt;string \| { sql: string, args?: any[] \| Record&lt;string, any&gt; }&gt;</code>                      | The SQL statements to execute as a batch.                                                                                                                         |
| mode       | <code>"deferred" \| "immediate" \| "exclusive" \| "concurrent"</code>                                               | Optional. When set, the batch is wrapped in `BEGIN <mode>` / `COMMIT` (with `ROLLBACK` on failure). Ignored when already inside a `transaction(...)` callback.    |

Without a `mode`, `batch()` is not transactional: each statement runs in its own autocommit step, so a failure mid-batch leaves earlier successful statements committed. With a `mode`, the batch becomes atomic — on the serverless driver the entire batch (including `BEGIN`, the user statements, `COMMIT`, and a conditional `ROLLBACK`) ships as a single Hrana request, so an atomic batch is still one round-trip.

When `mode` is set, `batch()` owns the surrounding `BEGIN`/`COMMIT`/`ROLLBACK`. Do not include transaction-control SQL (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`) in `statements`; the input is not validated, and a user-supplied `COMMIT` will close the wrapper transaction mid-batch and leave earlier statements committed.

For flexible all-or-nothing work that mixes `batch()` with other calls, wrap them in `transaction(...)` (or one of its `deferred`/`immediate`/`exclusive`/`concurrent` variants):

```js
const txn = db.transaction(async () => {
  await db.batch([
    { sql: "INSERT INTO users(name) VALUES (?)", args: ["Alice"] },
    { sql: "INSERT INTO users(name) VALUES (?)", args: ["Bob"] },
  ]);
  await db.exec("UPDATE counters SET n = n + 1");
});
await txn.immediate();
```

The function returns an object with two properties: `rowsAffected` (the total number of rows affected by all statements) and `lastInsertRowid` (the `rowid` of the last successful insert, or `undefined` if the batch performed no inserts).

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
