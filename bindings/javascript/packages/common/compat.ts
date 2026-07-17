import { bindParams } from "./bind.js";
import { SqliteError } from "./sqlite-error.js";
import { NativeConnection, NativeDatabase, NativeStatement, QueryOptions, STEP_IO, STEP_ROW, STEP_DONE } from "./types.js";

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

function isQueryOptions(value) {
  return value != null
    && typeof value === "object"
    && !Array.isArray(value)
    && Object.prototype.hasOwnProperty.call(value, "queryTimeout");
}

function splitBindParameters(bindParameters) {
  if (bindParameters.length === 0) {
    return { params: undefined, queryOptions: undefined };
  }
  if (bindParameters.length > 1 && isQueryOptions(bindParameters[bindParameters.length - 1])) {
    return {
      params: bindParameters.length === 2 ? bindParameters[0] : bindParameters.slice(0, -1),
      queryOptions: bindParameters[bindParameters.length - 1],
    };
  }
  return { params: bindParameters.length === 1 ? bindParameters[0] : bindParameters, queryOptions: undefined };
}

function toBindArgs(params) {
  if (params === undefined) {
    return [];
  }
  return [params];
}

export type BatchMode = "write" | "read" | "deferred" | "immediate" | "exclusive" | "concurrent" | string;

export interface BatchOptions {
  mode?: BatchMode;
  raw?: boolean;
}

export type BatchRow = Record<string, any> | any[];

export interface ResultSet {
  columns: Array<string>;
  columnTypes: Array<string>;
  rows: Array<BatchRow>;
  rowsAffected: number;
}

function normalizeBatchMode(mode: BatchMode): string {
  switch (String(mode).toLowerCase()) {
    case "write":
      return "IMMEDIATE";
    case "read":
    case "deferred":
      return "DEFERRED";
    case "immediate":
      return "IMMEDIATE";
    case "exclusive":
      return "EXCLUSIVE";
    case "concurrent":
      return "CONCURRENT";
    default:
      return String(mode).toUpperCase();
  }
}

function normalizeBatchOptions(options?: BatchMode | BatchOptions): { mode?: BatchMode; raw: boolean } {
  if (options != null && typeof options === "object") {
    return {
      mode: options.mode,
      raw: options.raw === true,
    };
  }
  return {
    mode: options as BatchMode | undefined,
    raw: false,
  };
}

function makeResultSet(
  columns: string[],
  columnTypes: string[],
  rows: any[],
  rowsAffected: number,
): ResultSet {
  return {
    columns,
    columnTypes,
    rows,
    rowsAffected,
  };
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
  private conn: NativeConnection;

  /**
   * Creates a new database connection. If the database file pointed to by `path` does not exists, it will be created.
   *
   * @constructor
   * @param {string} path - Path to the database file.
   * @param {Object} opts - Options for database behavior.
   * @param {boolean} [opts.readonly=false] - Open the database in read-only mode.
   * @param {boolean} [opts.fileMustExist=false] - If true, throws if database file does not exist.
   * @param {number} [opts.timeout=0] - Timeout duration in milliseconds for database operations. Defaults to 0 (no timeout).
   * @param {number} [opts.defaultQueryTimeout=0] - Default maximum query execution time in milliseconds before interruption.
   */
  constructor(db: NativeDatabase) {
    this.db = db;
    this.conn = db.connectSync();

    Object.defineProperties(this, {
      name: { get: () => this.db.path },
      readonly: { get: () => this.conn.readonly },
      open: { get: () => this.conn.open },
      memory: { get: () => this.db.memory },
      inTransaction: { get: () => this.conn.inTransaction() },
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
      return new Statement(this.conn.prepare(sql), this.db, this.conn);
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
  exec(sql, queryOptions?: QueryOptions) {
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
    const exec = this.conn.executor(sql, queryOptions);
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

  batch(
    statements: Array<string | { sql: string; args?: any[] | Record<string, any> }>,
    options?: BatchMode | BatchOptions,
  ): ResultSet[] {
    if (!Array.isArray(statements)) {
      throw new TypeError("Expected first argument to be an array of statements");
    }
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }

    const { mode, raw } = normalizeBatchOptions(options);
    const wrap = mode != null && !this.conn.inTransaction();
    if (wrap) {
      this.exec(`BEGIN ${normalizeBatchMode(mode!)}`);
    }

    const results: ResultSet[] = [];
    try {
      for (const statement of statements) {
        const sql = typeof statement === "string" ? statement : statement.sql;
        const args = typeof statement === "string" ? undefined : statement.args;
        const stmt = this.conn.prepare(sql);
        try {
          if (args !== undefined) {
            bindParams(stmt, [args]);
          }
          const cols = stmt.columns();
          const columnNames = cols.map((c) => c.name);
          const columnTypes = cols.map((c) => c.type ?? "");
          if (columnNames.length > 0) {
            stmt.raw(raw);
          }

          const totalChangesBefore = this.conn.totalChanges();
          const rows: any[] = [];
          try {
            while (true) {
              const stepResult = stmt.stepSync();
              if (stepResult === STEP_IO) {
                this.db.ioLoopSync();
                continue;
              }
              if (stepResult === STEP_DONE) {
                break;
              }
              if (stepResult === STEP_ROW) {
                rows.push(stmt.row());
              }
            }
            const rowsAffected = columnNames.length > 0
              ? 0
              : this.conn.totalChanges() !== totalChangesBefore ? this.conn.changes() : 0;
            results.push(makeResultSet(columnNames, columnTypes, rows, rowsAffected));
          } finally {
            stmt.reset();
          }
        } finally {
          stmt.finalize();
        }
      }

      if (wrap) {
        this.exec("COMMIT");
      }
    } catch (err) {
      if (wrap) {
        try {
          this.exec("ROLLBACK");
        } catch {
          // Keep the original statement error.
        }
      }
      throw convertError(err);
    }
    return results;
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
    this.conn.defaultSafeIntegers(toggle);
  }

  /**
   * Closes the database connection.
   */
  close() {
    this.conn.close();
    this.db.close();
  }
}

/**
 * Statement represents a prepared SQL statement that can be executed.
 */
class Statement {
  stmt: NativeStatement;
  db: NativeDatabase;
  conn: NativeConnection;

  constructor(stmt: NativeStatement, db: NativeDatabase, conn: NativeConnection) {
    this.stmt = stmt;
    this.db = db;
    this.conn = conn;
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

  get reader(): boolean {
    return this.stmt.columns().length > 0;
  }

  get database() {
    return this.db;
  }

  /**
   * Executes the SQL statement and returns an info object.
   */
  run(...bindParameters) {
    const totalChangesBefore = this.conn.totalChanges();

    const { params, queryOptions } = splitBindParameters(bindParameters);
    this.stmt.reset();
    this.stmt.setQueryTimeout(queryOptions);
    bindParams(this.stmt, toBindArgs(params));
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

    const lastInsertRowid = this.conn.lastInsertRowid();
    const changes = this.conn.totalChanges() === totalChangesBefore ? 0 : this.conn.changes();

    return { changes, lastInsertRowid };
  }

  /**
   * Executes the SQL statement and returns the first row.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  get(...bindParameters) {
    const { params, queryOptions } = splitBindParameters(bindParameters);
    this.stmt.reset();
    this.stmt.setQueryTimeout(queryOptions);
    bindParams(this.stmt, toBindArgs(params));
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
    const { params, queryOptions } = splitBindParameters(bindParameters);
    this.stmt.reset();
    this.stmt.setQueryTimeout(queryOptions);
    bindParams(this.stmt, toBindArgs(params));

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
    const { params, queryOptions } = splitBindParameters(bindParameters);
    this.stmt.reset();
    this.stmt.setQueryTimeout(queryOptions);
    bindParams(this.stmt, toBindArgs(params));
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
