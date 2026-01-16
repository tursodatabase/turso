import { AsyncLock } from "./async-lock.js";
import { bindParams } from "./bind.js";
import { SqliteError } from "./sqlite-error.js";
import { SqlQuery, isSqlQuery } from "./sql-template.js";
import { NativeDatabase, NativeStatement, STEP_IO, STEP_ROW, STEP_DONE, DatabaseOpts } from "./types.js";

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
 * Result of a mutation operation (INSERT, UPDATE, DELETE).
 */
interface RunResult {
  changes: number;
  lastInsertRowid: number;
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
  private ioStep: () => Promise<void>;
  private execLock: AsyncLock;
  private _inTransaction: boolean = false;
  protected connected: boolean = false;

  constructor(db: NativeDatabase, ioStep?: () => Promise<void>) {
    this.db = db;
    this.execLock = new AsyncLock();
    this.ioStep = ioStep ?? (async () => { });
    Object.defineProperties(this, {
      name: { get: () => this.db.path },
      readonly: { get: () => this.db.readonly },
      open: { get: () => this.db.open },
      memory: { get: () => this.db.memory },
      inTransaction: { get: () => this._inTransaction },
    });
  }

  /**
   * connect database
   */
  async connect() {
    if (this.connected) { return; }
    await this.db.connectAsync();
    this.connected = true;
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param {string} sql - The SQL statement string to prepare.
   * @returns A typed Statement instance.
   *
   * @example
   * ```typescript
   * interface User { id: number; name: string; }
   * const stmt = db.prepare<User>('SELECT * FROM users');
   * const users = await stmt.all(); // User[]
   * ```
   */
  prepare<T = unknown>(sql: string): Statement<T> {
    if (!sql) {
      throw new RangeError("The supplied SQL string contains no statements");
    }

    try {
      if (this.connected) {
        return new Statement<T>(maybeValue(this.db.prepare(sql)), this.db, this.execLock, this.ioStep);
      } else {
        return new Statement<T>(maybePromise(() => this.connect().then(() => this.db.prepare(sql))), this.db, this.execLock, this.ioStep)
      }
    } catch (err) {
      throw convertError(err);
    }
  }

  /**
   * Execute a query and return the first result row.
   * This is a convenience method that prepares, executes, and closes the statement.
   *
   * @param {string | SqlQuery} sql - The SQL query string or a SqlQuery from the `sql` tagged template.
   * @param {any[]} params - Optional bind parameters (ignored if sql is a SqlQuery).
   * @returns The first row or undefined if no results.
   *
   * @example
   * ```typescript
   * // With string and parameters
   * const user = await db.query<User>('SELECT * FROM users WHERE id = ?', [1]);
   *
   * // With sql tagged template
   * const user = await db.query<User>(sql`SELECT * FROM users WHERE id = ${1}`);
   * ```
   */
  async query<T = unknown>(sql: string | SqlQuery, params?: unknown[]): Promise<T | undefined> {
    const [sqlStr, bindParams] = isSqlQuery(sql) ? [sql.sql, sql.params] : [sql, params];
    const stmt = this.prepare<T>(sqlStr);
    try {
      return bindParams?.length ? await stmt.get(...bindParams) : await stmt.get();
    } finally {
      stmt.close();
    }
  }

  /**
   * Execute a query and return all result rows.
   * This is a convenience method that prepares, executes, and closes the statement.
   *
   * @param {string | SqlQuery} sql - The SQL query string or a SqlQuery from the `sql` tagged template.
   * @param {any[]} params - Optional bind parameters (ignored if sql is a SqlQuery).
   * @returns An array of all result rows.
   *
   * @example
   * ```typescript
   * // With string and parameters
   * const users = await db.queryAll<User>('SELECT * FROM users WHERE age > ?', [18]);
   *
   * // With sql tagged template
   * const users = await db.queryAll<User>(sql`SELECT * FROM users WHERE age > ${18}`);
   * ```
   */
  async queryAll<T = unknown>(sql: string | SqlQuery, params?: unknown[]): Promise<T[]> {
    const [sqlStr, bindParams] = isSqlQuery(sql) ? [sql.sql, sql.params] : [sql, params];
    const stmt = this.prepare<T>(sqlStr);
    try {
      return bindParams?.length ? await stmt.all(...bindParams) : await stmt.all();
    } finally {
      stmt.close();
    }
  }

  /**
   * Returns a function that executes the given function in a transaction.
   *
   * @param {function} fn - The function to wrap in a transaction.
   */
  transaction(fn: (...any) => Promise<any>) {
    if (typeof fn !== "function")
      throw new TypeError("Expected first argument to be a function");

    const db = this;
    const wrapTxn = (mode) => {
      return async (...bindParameters) => {
        await db.exec("BEGIN " + mode);
        db._inTransaction = true;
        try {
          const result = await fn(...bindParameters);
          await db.exec("COMMIT");
          db._inTransaction = false;
          return result;
        } catch (err) {
          await db.exec("ROLLBACK");
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

  async pragma(source, options) {
    if (options == null) options = {};

    if (typeof source !== "string")
      throw new TypeError("Expected first argument to be a string");

    if (typeof options !== "object")
      throw new TypeError("Expected second argument to be an options object");

    const pragma = `PRAGMA ${source}`;

    const stmt = this.prepare(pragma);
    try {
      const results = await stmt.all();
      return results;
    } finally {
      await stmt.close();
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
  async exec(sql) {
    await this.execLock.acquire();
    const exec = this.db.executor(sql);
    try {
      while (true) {
        const stepResult = exec.stepSync();
        if (stepResult === STEP_IO) {
          await this.db.ioLoopAsync();
          await this.ioStep();
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
      this.execLock.release();
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
  async close() {
    this.db.close();
  }
}

interface MaybeLazy<T> {
  apply(fn: (value: T) => void);
  resolve(): Promise<T>,
  must(): T;
}

function maybePromise<T>(arg: () => Promise<T>): MaybeLazy<T> {
  let lazy = arg;
  let promise = null;
  let value = null;
  return {
    apply(fn) {
      let previous = lazy;
      lazy = async () => {
        const result = await previous();
        fn(result);
        return result;
      }
    },
    async resolve() {
      if (promise != null) {
        return await promise;
      }
      let valueResolve, valueReject;
      promise = new Promise((resolve, reject) => {
        valueResolve = x => { resolve(x); value = x; }
        valueReject = reject;
      });
      await lazy().then(valueResolve, valueReject);
      return await promise;
    },
    must() {
      if (value == null) {
        throw new Error(`database must be connected before execution the function`)
      }
      return value;
    },
  }
}

function maybeValue<T>(value: T): MaybeLazy<T> {
  return {
    apply(fn) { fn(value); },
    resolve() { return Promise.resolve(value); },
    must() { return value; },
  }
}

/**
 * Statement represents a prepared SQL statement that can be executed.
 *
 * @template T - The type of rows returned by this statement.
 */
class Statement<T = unknown> {
  private stmt: MaybeLazy<NativeStatement>;
  private db: NativeDatabase;
  private execLock: AsyncLock;
  private ioStep: () => Promise<void>;

  constructor(stmt: MaybeLazy<NativeStatement>, db: NativeDatabase, execLock: AsyncLock, ioStep: () => Promise<void>) {
    this.stmt = stmt;
    this.db = db;
    this.execLock = execLock;
    this.ioStep = ioStep;
  }

  /**
   * Toggle raw mode.
   *
   * @param raw Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled.
   */
  raw(raw) {
    this.stmt.apply(s => s.raw(raw));
    return this;
  }

  /**
   * Toggle pluck mode.
   *
   * @param pluckMode Enable or disable pluck mode. If you don't pass the parameter, pluck mode is enabled.
   */
  pluck(pluckMode) {
    this.stmt.apply(s => s.pluck(pluckMode));
    return this;
  }

  /**
   * Sets safe integers mode for this statement.
   *
   * @param {boolean} [toggle] - Whether to use safe integers.
   */
  safeIntegers(toggle) {
    this.stmt.apply(s => s.safeIntegers(toggle));
    return this;
  }

  /**
   * Get column information for the statement.
   *
   * @returns {Array} An array of column objects with name, column, table, database, and type properties.
   */
  columns() {
    return this.stmt.must().columns();
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
   *
   * @param bindParameters - The bind parameters for executing the statement.
   * @returns An object with `changes` and `lastInsertRowid`.
   */
  async run(...bindParameters: unknown[]): Promise<RunResult> {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);

    const totalChangesBefore = this.db.totalChanges();
    await this.execLock.acquire();
    try {
      while (true) {
        const stepResult = await stmt.stepSync();
        if (stepResult === STEP_IO) {
          await this.io();
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
    } finally {
      stmt.reset();
      this.execLock.release();
    }
  }

  /**
   * Executes the SQL statement and returns the first row.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   * @returns The first row or undefined if no results.
   */
  async get(...bindParameters: unknown[]): Promise<T | undefined> {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);

    await this.execLock.acquire();
    let row: T | undefined = undefined;
    try {
      while (true) {
        const stepResult = await stmt.stepSync();
        if (stepResult === STEP_IO) {
          await this.io();
          continue;
        }
        if (stepResult === STEP_DONE) {
          break;
        }
        if (stepResult === STEP_ROW && row === undefined) {
          row = stmt.row() as T;
          continue;
        }
      }
      return row;
    } finally {
      stmt.reset();
      this.execLock.release();
    }
  }

  /**
   * Executes the SQL statement and returns an async iterator to the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   * @returns An async iterator yielding rows.
   *
   * @example
   * ```typescript
   * for await (const user of stmt.iterate()) {
   *   console.log(user.name);
   * }
   * ```
   */
  async *iterate(...bindParameters: unknown[]): AsyncIterableIterator<T> {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);

    await this.execLock.acquire();
    try {
      while (true) {
        const stepResult = await stmt.stepSync();
        if (stepResult === STEP_IO) {
          await this.db.ioLoopAsync();
          continue;
        }
        if (stepResult === STEP_DONE) {
          break;
        }
        if (stepResult === STEP_ROW) {
          yield stmt.row() as T;
        }
      }
    } finally {
      stmt.reset();
      this.execLock.release();
    }
  }

  /**
   * Executes the SQL statement and returns an array of the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   * @returns An array of all result rows.
   */
  async all(...bindParameters: unknown[]): Promise<T[]> {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);
    const rows: T[] = [];

    await this.execLock.acquire();
    try {
      while (true) {
        const stepResult = await stmt.stepSync();
        if (stepResult === STEP_IO) {
          await this.io();
          continue;
        }
        if (stepResult === STEP_DONE) {
          break;
        }
        if (stepResult === STEP_ROW) {
          rows.push(stmt.row() as T);
        }
      }
      return rows;
    }
    finally {
      stmt.reset();
      this.execLock.release();
    }
  }

  /**
   * Executes the SQL statement and returns results as an array of arrays.
   * This is more memory-efficient than `all()` for large result sets.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   * @returns An array of arrays, where each inner array is a row's values.
   *
   * @example
   * ```typescript
   * const rows = await stmt.values();
   * // [['Alice', 1], ['Bob', 2]]
   * ```
   */
  async values(...bindParameters: unknown[]): Promise<unknown[][]> {
    let stmt = await this.stmt.resolve();

    // Temporarily enable raw mode, then restore
    stmt.raw(true);
    bindParams(stmt, bindParameters);
    const rows: unknown[][] = [];

    await this.execLock.acquire();
    try {
      while (true) {
        const stepResult = await stmt.stepSync();
        if (stepResult === STEP_IO) {
          await this.io();
          continue;
        }
        if (stepResult === STEP_DONE) {
          break;
        }
        if (stepResult === STEP_ROW) {
          rows.push(stmt.row() as unknown[]);
        }
      }
      return rows;
    }
    finally {
      stmt.raw(false);
      stmt.reset();
      this.execLock.release();
    }
  }

  async io() {
    await this.db.ioLoopAsync();
    await this.ioStep();
  }

  /**
   * Interrupts the statement.
   */
  interrupt() {
    throw new Error("not implemented");
  }


  /**
   * Binds the given parameters to the statement _permanently_.
   *
   * @param bindParameters - The bind parameters for binding the statement.
   * @returns this - Statement with bound parameters.
   */
  bind(...bindParameters: unknown[]): this {
    try {
      bindParams(this.stmt, bindParameters);
      return this;
    } catch (err) {
      throw convertError(err);
    }
  }

  close() {
    let stmt;
    try {
      stmt = this.stmt.must();
    } catch (e) {
      // ignore error - if stmt wasn't initialized it's fine
      return;
    }
    stmt.finalize();
  }
}

export { Database, Statement, maybePromise, maybeValue }
export type { RunResult }