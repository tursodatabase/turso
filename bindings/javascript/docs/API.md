# class Database

The `Database` class represents a connection that can prepare and execute SQL statements.

## Methods

### new Database(path, [options]) ⇒ Database

Creates a new database connection.

| Param   | Type                | Description               |
| ------- | ------------------- | ------------------------- |
| path    | <code>string</code> | Path to the database file |
| options | <code>object</code> | Options.                  |

The `path` parameter points to the SQLite database file to open. If the file pointed to by `path` does not exists, it will be created.
To open an in-memory database, please pass `:memory:` as the `path` parameter.

The function returns a `Database` object.

### prepare(sql) ⇒ Statement

Prepares a SQL statement for execution.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to prepare. |

The function returns a `Statement` object.

### transaction(function) ⇒ function

Returns a function that runs the given function in a transaction.

| Param    | Type                  | Description                           |
| -------- | --------------------- | ------------------------------------- |
| function | <code>function</code> | The function to run in a transaction. |

### pragma(string, [options]) ⇒ results

Executes the given PRAGMA and returns its results.

| Param    | Type                  | Description           |
| -------- | --------------------- | ----------------------|
| source   | <code>string</code>   | Pragma to be executed |
| options  | <code>object</code>   | Options.              |

Most PRAGMA return a single value, the `simple: boolean` option is provided to return the first column of the first row.

```js
db.pragma('cache_size = 32000');
console.log(db.pragma('cache_size', { simple: true })); // => 32000
```

### backup(destination, [options]) ⇒ promise

This function is currently not supported.

### serialize([options]) ⇒ Buffer

This function is currently not supported.

### function(name, [options], function) ⇒ this

This function is currently not supported.

### aggregate(name, options) ⇒ this

This function is currently not supported.

### table(name, definition) ⇒ this

This function is currently not supported.

### loadExtension(path, [entryPoint]) ⇒ this

Loads a SQLite3 extension

| Param  | Type                | Description                              |
| ------ | ------------------- | -----------------------------------------|
| path   | <code>string</code> | The path to the extention to be loaded.  |

### exec(sql) ⇒ this

Executes a SQL statement.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to execute. |

This can execute strings that contain multiple SQL statements.

### interrupt() ⇒ this

Cancel ongoing operations and make them return at earliest opportunity.

**Note:** This is an extension in libSQL and not available in `better-sqlite3`.

This function is currently not supported.

### close() ⇒ this

Closes the database connection.

# class Statement

## Methods

### run([...bindParameters]) ⇒ object

Executes the SQL statement and (currently) returns an array with results.

**Note:** It should return an info object. 

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

The returned info object contains two properties: `changes` that describes the number of modified rows and `info.lastInsertRowid` that represents the `rowid` of the last inserted row.

This function is currently not supported.

### get([...bindParameters]) ⇒ row

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

### pluck([toggleState]) ⇒ this

Makes the prepared statement only return the value of the first column of any rows that it retrieves.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| pluckMode       | <code>boolean</code>          | Enable of disable pluck mode. If you don't pass the paramenter, pluck mode is enabled.          |

```js
stmt.pluck(); // plucking ON
stmt.pluck(true); // plucking ON
stmt.pluck(false); // plucking OFF
```

> NOTE: When plucking is turned on, raw mode is turned off (they are mutually exclusive options).

### expand([toggleState]) ⇒ this

This function is currently not supported.

### raw([rawMode]) ⇒ this

Makes the prepared statement return rows as arrays instead of objects.

| Param   | Type                 | Description                                                                       |
| ------- | -------------------- | --------------------------------------------------------------------------------- |
| rawMode | <code>boolean</code> | Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled. |

This function enables or disables raw mode. Prepared statements return objects by default, but if raw mode is enabled, the functions return arrays instead.

```js
stmt.raw(); // raw mode ON
stmt.raw(true); // raw mode ON
stmt.raw(false); // raw mode OFF
```

> NOTE: When raw mode is turned on, plucking is turned off (they are mutually exclusive options).

### columns() ⇒ array of objects

Returns the columns in the result set returned by this prepared statement.

This function is currently not supported.

### bind([...bindParameters]) ⇒ this

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

Binds **permanently** the given parameters to the statement. After a statement's parameters are bound this way, you may no longer provide it with execution-specific (temporary) bound parameters.