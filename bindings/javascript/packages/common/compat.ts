import { bindParams } from "./bind.js";
import { SqliteError } from "./sqlite-error.js";
import { NativeDatabase, NativeStatement, STEP_IO, STEP_ROW, STEP_DONE } from "./types.js";

const convertibleErrorTypes = { TypeError };
const CONVERTIBLE_ERROR_PREFIX = "[TURSO_CONVERT_TYPE]";

function convertError(err) {
  if ((err.code ?? "").startsWith(CONVERTIBLE_ERROR_PREFIX)) {
    return createErrorByName(
      err.code.substring(CONVERTIBLE_ERROR_PREFIX.length),
      err.message,
    );
  }

  return new SqliteError(err.message, err.code, err.rawCode);
}

function createErrorByName(name, message) {
  const ErrorConstructor = convertibleErrorTypes[name];
  if (!ErrorConstructor) {
    throw new Error(`unknown error type ${name} from Turso`);
  }

  return new ErrorConstructor(message);
}

/**
 * Database represents a connection that can prepare and execute SQL statements.
 */
class Database {
  name: string;
  readonly: boolean;
  open: boolean;
  memory: boolean;
  inTransaction: boolean;

  private db: NativeDatabase;
  private _inTransaction: boolean = false;

  /**
   * Creates a new database connection. If the database file pointed to by `path` does not exists, it will be created.
   *
   * @constructor
   * @param {string} path - Path to the database file.
   * @param {Object} opts - Options for database behavior.
   * @param {boolean} [opts.readonly=false] - Open the database in read-only mode.
   * @param {boolean} [opts.fileMustExist=false] - If true, throws if database file does not exist.
   * @param {number} [opts.timeout=0] - Timeout duration in milliseconds for database operations. Defaults to 0 (no timeout).
   */
  constructor(db: NativeDatabase) {
    this.db = db;
    this.db.connectSync();

    Object.defineProperties(this, {
      name: { get: () => this.db.path },
      readonly: { get: () => this.db.readonly },
      open: { get: () => this.db.open },
      memory: { get: () => this.db.memory },
      inTransaction: { get: () => this._inTransaction },
    });
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param {string} sql - The SQL statement string to prepare.
   */
  prepare(sql) {
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
    if (!sql) {
      throw new RangeError("The supplied SQL string contains no statements");
    }

    try {
      return new Statement(this.db.prepare(sql), this.db);
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
        db._inTransaction = true;
        try {
          const result = fn(...bindParameters);
          db.exec("COMMIT");
          db._inTransaction = false;
          return result;
        } catch (err) {
          db.exec("ROLLBACK");
          db._inTransaction = false;
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

    const pragma = `PRAGMA ${source}`;

    const stmt = this.prepare(pragma);
    try {
      const results = stmt.all();
      return results;
    } finally {
      stmt.close();
    }
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
    throw new Error("not implemented");
  }

  maxWriteReplicationIndex() {
    throw new Error("not implemented");
  }

  /**
   * Executes the given SQL string
   * Unlike prepared statements, this can execute strings that contain multiple SQL statements
   *
   * @param {string} sql - The string containing SQL statements to execute
   */
  exec(sql) {
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
    const exec = this.db.executor(sql);
    try {
      while (true) {
        const stepResult = exec.stepSync();
        if (stepResult === STEP_IO) {
          this.db.ioLoopSync();
          continue;
        }
        if (stepResult === STEP_DONE) {
          break;
        }
        if (stepResult === STEP_ROW) {
          // For exec(), we don't need the row data, just continue
          continue;
        }
      }
    } finally {
      exec.reset();
    }
  }

  /**
   * Interrupts the database connection.
   */
  interrupt() {
    throw new Error("not implemented");
  }

  /**
   * Sets the default safe integers mode for all statements from this database.
   *
   * @param {boolean} [toggle] - Whether to use safe integers by default.
   */
  defaultSafeIntegers(toggle) {
    this.db.defaultSafeIntegers(toggle);
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
  stmt: NativeStatement;
  db: NativeDatabase;

  constructor(stmt: NativeStatement, db: NativeDatabase) {
    this.stmt = stmt;
    this.db = db;
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

  /**
   * Sets safe integers mode for this statement.
   *
   * @param {boolean} [toggle] - Whether to use safe integers.
   */
  safeIntegers(toggle) {
    this.stmt.safeIntegers(toggle);
    return this;
  }

  /**
   * Get column information for the statement.
   *
   * @returns {Array} An array of column objects with name, column, table, database, and type properties.
   */
  columns() {
    return this.stmt.columns();
  }

  get source() {
    throw new Error("not implemented");
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
    const totalChangesBefore = this.db.totalChanges();

    this.stmt.reset();
    bindParams(this.stmt, bindParameters);
    for (; ;) {
      const stepResult = this.stmt.stepSync();
      if (stepResult === STEP_IO) {
        this.db.ioLoopSync();
        continue;
      }
      if (stepResult === STEP_DONE) {
        break;
      }
      if (stepResult === STEP_ROW) {
        // For run(), we don't need the row data, just continue
        continue;
      }
    }

    const lastInsertRowid = this.db.lastInsertRowid();
    const changes = this.db.totalChanges() === totalChangesBefore ? 0 : this.db.changes();

    return { changes, lastInsertRowid };
  }

  /**
   * Executes the SQL statement and returns the first row.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  get(...bindParameters) {
    this.stmt.reset();
    bindParams(this.stmt, bindParameters);
    let row = undefined;
    for (; ;) {
      const stepResult = this.stmt.stepSync();
      if (stepResult === STEP_IO) {
        this.db.ioLoopSync();
        continue;
      }
      if (stepResult === STEP_DONE) {
        break;
      }
      if (stepResult === STEP_ROW && row === undefined) {
        row = this.stmt.row();
      }
    }
    return row;
  }

  /**
   * Executes the SQL statement and returns an iterator to the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  *iterate(...bindParameters) {
    this.stmt.reset();
    bindParams(this.stmt, bindParameters);

    while (true) {
      const stepResult = this.stmt.stepSync();
      if (stepResult === STEP_IO) {
        this.db.ioLoopSync();
        continue;
      }
      if (stepResult === STEP_DONE) {
        break;
      }
      if (stepResult === STEP_ROW) {
        yield this.stmt.row();
      }
    }
  }

  /**
   * Executes the SQL statement and returns an array of the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  all(...bindParameters) {
    this.stmt.reset();
    bindParams(this.stmt, bindParameters);
    const rows: any[] = [];
    for (; ;) {
      const stepResult = this.stmt.stepSync();
      if (stepResult === STEP_IO) {
        this.db.ioLoopSync();
        continue;
      }
      if (stepResult === STEP_DONE) {
        break;
      }
      if (stepResult === STEP_ROW) {
        rows.push(this.stmt.row());
      }
    }
    return rows;
  }

  /**
   * Interrupts the statement.
   */
  interrupt() {
    throw new Error("not implemented");
  }


  /**
   * Binds the given parameters to the statement _permanently_
   *
   * @param bindParameters - The bind parameters for binding the statement.
   * @returns this - Statement with binded parameters
   */
  bind(...bindParameters) {
    try {
      bindParams(this.stmt, bindParameters);
      return this;
    } catch (err) {
      throw convertError(err);
    }
  }

  close() {
    this.stmt.finalize();
  }
}

export { Database, Statement }