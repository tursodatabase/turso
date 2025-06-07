"use strict";

const { Database: NativeDB } = require("./index.js");

/**
 * Database represents a connection that can prepare and execute SQL statements.
 */
class Database {
  /**
   * Creates a new database connection. If the database file pointed to by `path` does not exists, it will be created.
   *
   * @constructor
   * @param {string} path - Path to the database file.
   */
  constructor(path, opts) {
    this.db = new NativeDB(path, opts);
    this.memory = this.db.memory;
    const db = this.db;
    Object.defineProperties(this, {
      inTransaction: {
        get() {
          return db.inTransaction();
        },
      },
    });
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param {string} sql - The SQL statement string to prepare.
   */
  prepare(sql) {
    try {
      return new Statement(this.db.prepare(sql), this);
    } catch (err) {
      throw convertError(err);
    }
  }

  /**
   * Returns a function that executes the given function in a transaction.
   *
   * @param {function} fn - The function to wrap in a transaction.
   */
  transaction(fn) {
    if (typeof fn !== "function")
      throw new TypeError("Expected first argument to be a function");

    const db = this;
    const wrapTxn = (mode) => {
      return (...bindParameters) => {
        db.exec("BEGIN " + mode);
        try {
          const result = fn(...bindParameters);
          db.exec("COMMIT");
          return result;
        } catch (err) {
          db.exec("ROLLBACK");
          throw err;
        }
      };
    };
    const properties = {
      default: { value: wrapTxn("") },
      deferred: { value: wrapTxn("DEFERRED") },
      immediate: { value: wrapTxn("IMMEDIATE") },
      exclusive: { value: wrapTxn("EXCLUSIVE") },
      database: { value: this, enumerable: true },
    };
    Object.defineProperties(properties.default.value, properties);
    Object.defineProperties(properties.deferred.value, properties);
    Object.defineProperties(properties.immediate.value, properties);
    Object.defineProperties(properties.exclusive.value, properties);
    return properties.default.value;
  }

  pragma(source, options) {
    if (options == null) options = {};

    if (typeof source !== "string")
      throw new TypeError("Expected first argument to be a string");

    if (typeof options !== "object")
      throw new TypeError("Expected second argument to be an options object");

    const simple = options["simple"];
    const pragma = `PRAGMA ${source}`;

    return simple
      ? this.db.pragma(pragma, true)
      : this.db.pragma(pragma, false);
  }

  backup(filename, options) {
    throw new Error("not implemented");
  }

  serialize(options) {
    throw new Error("not implemented");
  }

  function(name, options, fn) {
    throw new Error("not implemented");
  }

  aggregate(name, options) {
    throw new Error("not implemented");
  }

  table(name, factory) {
    throw new Error("not implemented");
  }

  loadExtension(path) {
    this.db.loadExtension(path);
  }

  maxWriteReplicationIndex() {
    throw new Error("not implemented");
  }

  /**
   * Executes a SQL statement.
   *
   * @param {string} sql - The SQL statement string to execute.
   */
  exec(sql) {
    this.db.exec(sql);
  }

  /**
   * Interrupts the database connection.
   */
  interrupt() {
    this.db.interrupt();
  }

  /**
   * Closes the database connection.
   */
  close() {
    this.db.close();
  }
}

/**
 * Statement represents a prepared SQL statement that can be executed.
 */
class Statement {
  constructor(stmt, database) {
    this.stmt = stmt;
    this.db = database;
  }

  /**
   * Toggle raw mode.
   *
   * @param raw Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled.
   */
  raw(raw) {
    this.stmt.raw(raw);
    return this;
  }

  /**
   * Toggle pluck mode.
   *
   * @param pluckMode Enable or disable pluck mode. If you don't pass the parameter, pluck mode is enabled.
   */
  pluck(pluckMode) {
    this.stmt.pluck(pluckMode);
    return this;
  }

  get reader() {
    throw new Error("not implemented");
  }

  get database() {
    return this.db;
  }

  /**
   * Executes the SQL statement and returns an info object.
   */
  run(...bindParameters) {
    return this.stmt.run(bindParameters.flat());
  }

  /**
   * Executes the SQL statement and returns the first row.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  get(...bindParameters) {
    return this.stmt.get(bindParameters.flat());
  }

  /**
   * Executes the SQL statement and returns an iterator to the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  iterate(...bindParameters) {
    return this.stmt.iterate(bindParameters.flat());
  }

  /**
   * Executes the SQL statement and returns an array of the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  all(...bindParameters) {
    return this.stmt.all(bindParameters.flat());
  }

  /**
   * Interrupts the statement.
   */
  interrupt() {
    this.stmt.interrupt();
    return this;
  }

  /**
   * Returns the columns in the result set returned by this prepared statement.
   */
  columns() {
    return this.stmt.columns();
  }

  /**
   * Binds the given parameters to the statement _permanently_
   *
   * @param bindParameters - The bind parameters for binding the statement.
   * @returns this - Statement with binded parameters
   */
  bind(...bindParameters) {
    return this.stmt.bind(bindParameters.flat());
  }
}

module.exports.Database = Database;
