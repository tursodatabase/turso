import { AsyncLock } from "./async-lock.js";
import { bindParams } from "./bind.js";
import { SqliteError } from "./sqlite-error.js";
import { NativeConnection, NativeDatabase, NativeStatement, QueryOptions, STEP_IO, STEP_ROW, STEP_DONE, DatabaseOpts, TransactionAsyncContext } from "./types.js";

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

/**
 * Locking mode for `Database.batch(stmts, options)`.
 */
export type BatchMode = "write" | "read" | "deferred" | "immediate" | "exclusive" | "concurrent" | string;

export interface BatchOptions {
  mode?: BatchMode;
  raw?: boolean;
}

export type BatchRow = Record<string, any> | any[];

/**
 * The result of executing a single statement within `batch()`, matching the
 * libSQL client's `ResultSet` shape.
 */
export interface ResultSet {
  /** Column names of the result, empty for statements that return no rows. */
  columns: Array<string>;
  /** Declared column types, parallel to `columns`. */
  columnTypes: Array<string>;
  /** Result rows; empty for non-SELECT statements. */
  rows: Array<BatchRow>;
  /** Number of rows changed by the statement (0 for SELECT). */
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

/**
 * Builds a libSQL-style `ResultSet` for a single statement in a `batch()`.
 */
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
 * A wrapped transaction function. Calling it runs the wrapped function inside a
 * `BEGIN`/`COMMIT` block; the mode properties (`deferred`, `concurrent`, etc.)
 * return equivalent wrappers that begin the transaction with the corresponding
 * locking mode.
 */
export interface TransactionFunction<
  F extends (...args: any[]) => Promise<any> = (...args: any[]) => Promise<any>,
> {
  (...args: Parameters<F>): ReturnType<F>;
  default: TransactionFunction<F>;
  deferred: TransactionFunction<F>;
  concurrent: TransactionFunction<F>;
  immediate: TransactionFunction<F>;
  exclusive: TransactionFunction<F>;
  database: Database;
}

/**
 * Options controlling the connection-pool behavior of the promise Database.
 */
export interface DatabasePromiseOpts {
  /** Maximum number of idle pooled transaction connections kept alive (default 1). */
  poolSize?: number;
  /**
   * Async-context tracker (Node's AsyncLocalStorage) used to route statements
   * issued inside a `transaction()` callback to the transaction's pooled
   * connection. When absent, `transaction()` runs on the main connection.
   */
  asyncContext?: TransactionAsyncContext;
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
  private conn: NativeConnection | null = null;
  private connectPromise: Promise<void> | null = null;
  private ioStep: () => Promise<void>;
  // Serializes native calls on this connection. Subclasses that also drive
  // the same underlying core database from async worker tasks (the sync
  // engine on wasm) must hold this lock around those windows too: on
  // browser wasm the main thread cannot block on a contended core lock, so
  // main-thread native calls must never overlap an in-flight worker task.
  protected execLock: AsyncLock;
  protected connected: boolean = false;
  // Extra connections dedicated to transaction() calls: element [0] of the
  // conceptual pool is this database itself (all non-transaction queries),
  // entries below are lazily created for concurrent transactions.
  private pool: { db: Database, busy: boolean }[] = [];
  private poolSize: number;
  private asyncContext: TransactionAsyncContext | null;
  // Pooled wrappers share the native database handle with their owner and
  // must not close it.
  private ownsDb: boolean = true;
  private safeIntegersDefault: boolean | undefined = undefined;

  constructor(db: NativeDatabase, ioStep?: () => Promise<void>, opts?: DatabasePromiseOpts) {
    this.db = db;
    this.execLock = new AsyncLock();
    this.ioStep = ioStep ?? (async () => { });
    this.poolSize = Math.max(0, opts?.poolSize ?? 1);
    this.asyncContext = opts?.asyncContext ?? null;
    Object.defineProperties(this, {
      name: { get: () => this.db.path },
      readonly: { get: () => this.mustConn().readonly },
      open: { get: () => this.conn != null && this.conn.open },
      memory: { get: () => this.db.memory },
      inTransaction: { get: () => this.target().mustConn().inTransaction() },
    });
  }

  private mustConn(): NativeConnection {
    if (this.conn == null) {
      throw new Error("database must be connected");
    }
    return this.conn;
  }

  /**
   * Returns the connection that must execute the current call: inside a
   * `transaction()` callback this is the transaction's pooled connection,
   * otherwise this database itself.
   */
  private target(): Database {
    const store = this.asyncContext?.getStore();
    return store instanceof Database && store !== this ? store : this;
  }

  /**
   * Takes an idle pooled connection for a transaction, lazily connecting a
   * new one when all are busy. Falls back to this database itself when the
   * platform has no async-context tracking or the native database cannot
   * open extra connections (e.g. it is managed by the sync engine).
   */
  private acquireTransactionConnection(): Database {
    if (this.asyncContext == null) {
      return this;
    }
    for (const entry of this.pool) {
      if (!entry.busy) {
        entry.busy = true;
        return entry.db;
      }
    }
    let native: NativeConnection;
    try {
      native = this.db.connectSync();
    } catch {
      return this;
    }
    if (this.safeIntegersDefault !== undefined) {
      native.defaultSafeIntegers(this.safeIntegersDefault);
    }
    const pooled = new Database(this.db, this.ioStep);
    pooled.conn = native;
    pooled.connected = true;
    pooled.ownsDb = false;
    this.pool.push({ db: pooled, busy: true });
    return pooled;
  }

  /**
   * Returns a pooled connection after a transaction ends. Keeps at most
   * `poolSize` idle connections alive and deallocates the rest.
   */
  private async releaseTransactionConnection(pooled: Database) {
    if (pooled === this) {
      return;
    }
    const entry = this.pool.find(e => e.db === pooled);
    if (entry === undefined) {
      return;
    }
    entry.busy = false;
    const idle = this.pool.filter(e => !e.busy).length;
    if (idle > this.poolSize) {
      this.pool.splice(this.pool.indexOf(entry), 1);
      await pooled.close();
    }
  }

  /**
   * connect database
   */
  async connect() {
    if (this.connected) { return; }
    if (this.connectPromise == null) {
      this.connectPromise = (async () => {
        const conn = await this.db.connectAsync();
        if (this.safeIntegersDefault !== undefined) {
          conn.defaultSafeIntegers(this.safeIntegersDefault);
        }
        this.conn = conn;
        this.connected = true;
      })().catch((err) => {
        this.connectPromise = null;
        throw err;
      });
    }
    await this.connectPromise;
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param {string} sql - The SQL statement string to prepare.
   */
  prepare(sql: string): Promise<Statement> {
    const target = this.target();
    if (target !== this) {
      return target.prepare(sql);
    }
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
        return new Statement(maybeValue(this.mustConn().prepare(sql)), () => this.mustConn(), this.execLock, this.ioStep) as unknown as Promise<Statement>;
      } else {
        return new Statement(maybePromise(() => this.connect().then(() => this.mustConn().prepare(sql))), () => this.mustConn(), this.execLock, this.ioStep) as unknown as Promise<Statement>;
      }
    } catch (err) {
      throw convertError(err);
    }
  }

  /**
   * Returns a function that executes the given function in a transaction.
   *
   * Each invocation runs on a dedicated pooled connection, so concurrent
   * transactions (and concurrent non-transaction statements on this
   * database) never interleave with the transaction's BEGIN..COMMIT window.
   * Statements issued via this database inside the callback's async context
   * are routed to that pooled connection; statements prepared *outside* the
   * callback keep targeting the main connection and do not join the
   * transaction. On platforms without async-context tracking the transaction
   * runs directly on the main connection.
   *
   * @param {function} fn - The function to wrap in a transaction.
   */
  transaction<F extends (...args: any[]) => Promise<any>>(
    fn: F,
  ): TransactionFunction<F> {
    if (typeof fn !== "function")
      throw new TypeError("Expected first argument to be a function");

    const db = this;
    const wrapTxn = (mode) => {
      return async (...bindParameters) => {
        if (!db.connected) {
          await db.connect();
        }
        const txn = db.acquireTransactionConnection();
        const body = async () => {
          await db.exec("BEGIN " + mode);
          try {
            const result = await fn(...bindParameters);
            await db.exec("COMMIT");
            return result;
          } catch (err) {
            await db.exec("ROLLBACK");
            throw err;
          }
        };
        try {
          if (txn === db) {
            return await body();
          }
          return await db.asyncContext!.run(txn, body);
        } finally {
          await db.releaseTransactionConnection(txn);
        }
      };
    };
    const properties = {
      default: { value: wrapTxn("") },
      deferred: { value: wrapTxn("DEFERRED") },
      concurrent: { value: wrapTxn("CONCURRENT") },
      immediate: { value: wrapTxn("IMMEDIATE") },
      exclusive: { value: wrapTxn("EXCLUSIVE") },
      database: { value: this, enumerable: true },
    };
    Object.defineProperties(properties.default.value, properties);
    Object.defineProperties(properties.deferred.value, properties);
    Object.defineProperties(properties.concurrent.value, properties);
    Object.defineProperties(properties.immediate.value, properties);
    Object.defineProperties(properties.exclusive.value, properties);
    return properties.default.value as TransactionFunction<F>;
  }

  /**
   * Executes a batch of SQL statements sequentially over this connection.
   *
   * By default, batch() is not transactional: each statement runs in its
   * own autocommit step, so a failure mid-batch leaves earlier successful
   * statements committed. Pass a `mode` to make the batch atomic — the
   * statements are wrapped in `BEGIN <mode>` / `COMMIT`, and `ROLLBACK`
   * runs if any statement fails. When called from inside a
   * `db.transaction(...)` callback the `mode` argument is ignored and the
   * surrounding transaction is reused.
   *
   * When `mode` is set, `batch()` owns the surrounding
   * `BEGIN`/`COMMIT`/`ROLLBACK`, so the `statements` array must not
   * contain its own transaction-control SQL (`BEGIN`, `COMMIT`,
   * `ROLLBACK`, `SAVEPOINT`, `RELEASE`). The input is not validated
   * for that — a user-supplied `COMMIT` will close the wrapper
   * transaction mid-batch and leave earlier statements committed,
   * defeating the all-or-nothing contract.
   *
   * @param statements - An array of SQL strings or `{ sql, args }` objects.
   * @param mode - When set, wraps the batch in `BEGIN <mode>` / `COMMIT`
   *   (with `ROLLBACK` on failure). Ignored when already inside a
   *   transaction.
   * @returns An array of `ResultSet`s — one per input statement, in order —
   *   matching the libsql-js batch contract. Each `ResultSet` carries that
   *   statement's `columns`, `columnTypes`, `rows`, and `rowsAffected`.
   *
   * @example
   * // Plain SQL strings (non-atomic).
   * await db.batch([
   *   "INSERT INTO users(name) VALUES ('Alice')",
   *   "INSERT INTO users(name) VALUES ('Bob')",
   * ]);
   *
   * @example
   * // Positional and named bind parameters.
   * await db.batch([
   *   { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Carol", "carol@example.net"] },
   *   { sql: "INSERT INTO users(name, email) VALUES (:name, :email)", args: { name: "Dave", email: "dave@example.net" } },
   * ]);
   *
   * @example
   * // Atomic via the mode parameter.
   * await db.batch([
   *   { sql: "INSERT INTO users(name) VALUES (?)", args: ["Eve"] },
   *   { sql: "INSERT INTO users(name) VALUES (?)", args: ["Frank"] },
   * ], "immediate");
   *
   * @example
   * // Atomic via the transaction() API for mixed workloads.
   * const txn = db.transaction(async () => {
   *   await db.batch([{ sql: "INSERT INTO users(name) VALUES (?)", args: ["Eve"] }]);
   *   await db.exec("UPDATE counters SET n = n + 1");
   * });
   * await txn.immediate();
   */
  async batch(
    statements: Array<string | { sql: string; args?: any[] | Record<string, any> }>,
    options?: BatchMode | BatchOptions,
  ): Promise<ResultSet[]> {
    const target = this.target();
    if (target !== this) {
      return target.batch(statements, options);
    }
    if (!Array.isArray(statements)) {
      throw new TypeError("Expected first argument to be an array of statements");
    }
    if (!this.connected) {
      await this.connect();
    }
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }

    const conn = this.mustConn();
    // Hold execLock across the entire batch so it is observed as a
    // single unit by other callers on this connection. The helpers
    // below run their own step loops without re-acquiring the lock.
    await this.execLock.acquire();
    try {
      const runRawSql = async (sql: string) => {
        const exec = conn.executor(sql);
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
          }
        } finally {
          exec.reset();
        }
      };

      const { mode, raw } = normalizeBatchOptions(options);
      const wrap = mode != null && !conn.inTransaction();
      if (wrap) {
        await runRawSql(`BEGIN ${normalizeBatchMode(mode!)}`);
      }

      const results: ResultSet[] = [];
      try {
        for (const statement of statements) {
          const sql = typeof statement === "string" ? statement : statement.sql;
          const args = typeof statement === "string" ? undefined : statement.args;

          let nativeStmt: NativeStatement;
          try {
            nativeStmt = conn.prepare(sql);
          } catch (err) {
            throw convertError(err);
          }
          try {
            if (args !== undefined) {
              bindParams(nativeStmt, [args]);
            }
            const cols = nativeStmt.columns();
            const columnNames = cols.map((c) => c.name);
            const columnTypes = cols.map((c) => c.type ?? "");
            if (columnNames.length > 0) {
              nativeStmt.raw(raw);
            }

            const totalChangesBefore = conn.totalChanges();
            const rows: any[] = [];
            try {
              while (true) {
                const stepResult = await nativeStmt.stepSync();
                if (stepResult === STEP_IO) {
                  await this.io();
                  continue;
                }
                if (stepResult === STEP_DONE) {
                  break;
                }
                rows.push(nativeStmt.row());
              }
              const rowsAffected = columnNames.length > 0
                ? 0
                : conn.totalChanges() !== totalChangesBefore ? conn.changes() : 0;
              results.push(
                makeResultSet(columnNames, columnTypes, rows, rowsAffected),
              );
            } finally {
              nativeStmt.reset();
            }
          } finally {
            nativeStmt.finalize();
          }
        }

        if (wrap) {
          await runRawSql("COMMIT");
        }
      } catch (err) {
        if (wrap) {
          try { await runRawSql("ROLLBACK"); } catch { /* ignore */ }
        }
        throw err;
      }
      return results;
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Prepares the SQL and executes it as `Statement.run`, returning the run info.
   *
   * @param {string} sql - The SQL statement string.
   * @param {...any} bindParameters - Bind parameters, optionally followed by a query options object.
   */
  async run(sql, ...bindParameters) {
    const stmt = await this.prepare(sql);
    try {
      return await stmt.run(...bindParameters);
    } finally {
      await stmt.close();
    }
  }

  /**
   * Prepares the SQL and executes it as `Statement.get`, returning the first row.
   *
   * @param {string} sql - The SQL statement string.
   * @param {...any} bindParameters - Bind parameters, optionally followed by a query options object.
   */
  async get(sql, ...bindParameters) {
    const stmt = await this.prepare(sql);
    try {
      return await stmt.get(...bindParameters);
    } finally {
      await stmt.close();
    }
  }

  /**
   * Prepares the SQL and executes it as `Statement.all`, returning all rows.
   *
   * @param {string} sql - The SQL statement string.
   * @param {...any} bindParameters - Bind parameters, optionally followed by a query options object.
   */
  async all(sql, ...bindParameters) {
    const stmt = await this.prepare(sql);
    try {
      return await stmt.all(...bindParameters);
    } finally {
      await stmt.close();
    }
  }

  /**
   * Prepares the SQL and executes it as `Statement.iterate`, yielding each row.
   *
   * @param {string} sql - The SQL statement string.
   * @param {...any} bindParameters - Bind parameters, optionally followed by a query options object.
   */
  async *iterate(sql, ...bindParameters) {
    const stmt = await this.prepare(sql);
    try {
      yield* stmt.iterate(...bindParameters);
    } finally {
      await stmt.close();
    }
  }

  async pragma(source, options) {
    if (options == null) options = {};

    if (typeof source !== "string")
      throw new TypeError("Expected first argument to be a string");

    if (typeof options !== "object")
      throw new TypeError("Expected second argument to be an options object");

    const pragma = `PRAGMA ${source}`;

    const stmt = await this.prepare(pragma);
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
  async exec(sql, queryOptions?: QueryOptions) {
    const target = this.target();
    if (target !== this) {
      return target.exec(sql, queryOptions);
    }
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    const exec = this.mustConn().executor(sql, queryOptions);
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
    this.safeIntegersDefault = toggle === undefined ? true : toggle;
    if (this.conn != null) {
      this.conn.defaultSafeIntegers(toggle);
    }
    for (const entry of this.pool) {
      entry.db.defaultSafeIntegers(toggle);
    }
  }

  /**
   * Closes the database connection.
   */
  async close() {
    for (const entry of this.pool.splice(0)) {
      await entry.db.close();
    }
    if (this.conn != null) {
      this.conn.close();
    }
    if (this.ownsDb) {
      this.db.close();
    }
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
  // Accessor rather than a direct reference: for lazily prepared statements
  // the connection does not exist until the owning database connects.
  private conn: () => NativeConnection;
  private execLock: AsyncLock;
  private ioStep: () => Promise<void>;

  constructor(stmt: MaybeLazy<NativeStatement>, conn: () => NativeConnection, execLock: AsyncLock, ioStep: () => Promise<void>) {
    this.stmt = stmt;
    this.conn = conn;
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
    return this.conn();
  }

  /**
   * Executes the SQL statement and returns an info object.
   */
  async run(...bindParameters) {
    let stmt = await this.stmt.resolve();
    const { params, queryOptions } = splitBindParameters(bindParameters);

    stmt.setQueryTimeout(queryOptions);
    bindParams(stmt, toBindArgs(params));

    const conn = this.conn();
    const totalChangesBefore = conn.totalChanges();
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

      const lastInsertRowid = conn.lastInsertRowid();
      const changes = conn.totalChanges() === totalChangesBefore ? 0 : conn.changes();

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
    const { params, queryOptions } = splitBindParameters(bindParameters);

    stmt.setQueryTimeout(queryOptions);
    bindParams(stmt, toBindArgs(params));

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
    const { params, queryOptions } = splitBindParameters(bindParameters);

    stmt.setQueryTimeout(queryOptions);
    bindParams(stmt, toBindArgs(params));

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
    const { params, queryOptions } = splitBindParameters(bindParameters);

    stmt.setQueryTimeout(queryOptions);
    bindParams(stmt, toBindArgs(params));
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
