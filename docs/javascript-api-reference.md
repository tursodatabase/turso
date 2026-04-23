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
