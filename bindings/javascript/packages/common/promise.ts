import { AsyncLock } from "./async-lock.js";
import { bindParams } from "./bind.js";
import { SqliteError } from "./sqlite-error.js";
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
   */
  prepare(sql) {
    // Only throw if we connected before but now the database is closed
    // Allow implicit connection if not connected yet
    if (this.connected && !this.open) {
      throw new TypeError("The database connection is not open");
    }
    if (!sql) {
      throw new RangeError("The supplied SQL string contains no statements");
    }

    try {
      if (this.connected) {
        return new Statement(maybeValue(this.db.prepare(sql)), this.db, this.execLock, this.ioStep);
      } else {
        return new Statement(maybePromise(() => this.connect().then(() => this.db.prepare(sql))), this.db, this.execLock, this.ioStep)
      }
    } catch (err) {
      throw convertError(err);
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
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    const exec = this.db.executor(sql);
    try {
      while (true) {
        const stepResult = exec.stepSync();
        if (stepResult === STEP_IO) {
          await this.io();
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

  async io() {
    // For WASM browser builds, ioStep awaits a promise that resolves when
    // the OPFS Worker completes the I/O (via IONotifier in wasm-common).
    // For in-memory / Node.js builds, ioStep is a no-op since I/O is synchronous.
    await this.ioStep();
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
 */
class Statement {
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

  get reader(): boolean {
    return this.stmt.must().columns().length > 0;
  }

  get database() {
    return this.db;
  }

  /**
   * Executes the SQL statement and returns an info object.
   */
  async run(...bindParameters) {
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
   */
  async get(...bindParameters) {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);

    await this.execLock.acquire();
    let row = undefined;
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
          row = stmt.row();
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
   * Executes the SQL statement and returns an iterator to the resulting rows.
   *
   * @param bindParameters - The bind parameters for executing the statement.
   */
  async *iterate(...bindParameters) {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);

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
          yield stmt.row();
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
   */
  async all(...bindParameters) {
    let stmt = await this.stmt.resolve();

    bindParams(stmt, bindParameters);
    const rows: any[] = [];

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
          rows.push(stmt.row());
        }
      }
      return rows;
    }
    finally {
      stmt.reset();
      this.execLock.release();
    }
  }

  async io() {
    await this.ioStep();
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