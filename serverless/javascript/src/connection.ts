import { AsyncLock, type Lock } from './async-lock.js';
import { Session, type SessionConfig, type BatchMode } from './session.js';
import { Statement } from './statement.js';
import { type QueryOptions } from './protocol.js';
import { normalizeArgs, splitBindParameters } from './args.js';
import { createExpandedRow } from './row.js';

export type { BatchMode } from './session.js';

/**
 * Configuration options for connecting to a Turso database.
 */
export interface Config extends SessionConfig { }

export type BatchStatement = string | {
  sql: string;
  args?: any[] | Record<string, any>;
};

export interface BatchOptions {
  mode?: BatchMode;
  raw?: boolean;
}

function normalizeBatchOptions(options?: BatchMode | BatchOptions): { mode?: BatchMode; raw: boolean } {
  if (options != null && typeof options === 'object') {
    return {
      mode: options.mode,
      raw: options.raw === true,
    };
  }
  return {
    mode: options,
    raw: false,
  };
}

/**
 * Shapes a raw per-statement result from `session.batch()` into the
 * libsql-js batch ResultSet shape.
 */
function toResultSet(result: any): any {
  return {
    columns: result.columns ?? [],
    columnTypes: result.columnTypes ?? [],
    rows: result.rows ?? [],
    rowsAffected: result.rowsAffected ?? 0,
  };
}


/**
 * A connection to a Turso database.
 *
 * Provides methods for executing SQL statements and managing prepared statements.
 * Uses the SQL over HTTP protocol with streaming cursor support.
 *
 * ## Concurrency model
 *
 * A Connection is **single-stream**: it can only run one statement at a time.
 * This is not an implementation quirk — it follows from the SQL over HTTP protocol,
 * where each request carries a baton from the previous response to sequence operations
 * on the server. Concurrent calls on the same connection would race on that baton
 * and corrupt the stream. This is the same model as SQLite itself (one execution
 * at a time per connection).
 *
 * If you call `all()` while another statement is in flight, the call automatically
 * waits for the previous one to finish — just like the native
 * `@tursodatabase/database` binding.
 *
 * ## Parallel queries
 *
 * For parallelism, create multiple connections. `connect()` is cheap — it just
 * allocates a config object. No TCP connection is opened until the first query,
 * and the underlying `fetch()` runtime automatically pools and reuses TCP/TLS
 * connections to the same origin.
 *
 * ```typescript
 * import { connect } from "@tursodatabase/serverless";
 *
 * const config = { url: process.env.TURSO_URL, authToken: process.env.TURSO_TOKEN };
 *
 * // Option 1: one connection per parallel query
 * const [users, orders] = await Promise.all([
 *   connect(config).all("SELECT * FROM users WHERE active = 1"),
 *   connect(config).all("SELECT * FROM orders WHERE status = 'pending'"),
 * ]);
 *
 * // Option 2: reusable pool for repeated parallel work
 * const pool = Array.from({ length: 4 }, () => connect(config));
 * const results = await Promise.all(
 *   queries.map((sql, i) => pool[i % pool.length].all(sql))
 * );
 * ```
 */
export class Connection {
  private config: Config;
  private session: Session;
  private isOpen: boolean = true;
  private defaultSafeIntegerMode: boolean = false;
  private execLock: AsyncLock = new AsyncLock();

  constructor(config: Config) {
    if (!config.url) {
      throw new Error("invalid config: url is required");
    }
    this.config = config;
    this.session = new Session(config);

    // Define inTransaction property
    Object.defineProperty(this, 'inTransaction', {
      get: () => this.session.inTransaction,
      enumerable: true
    });
  }

  /**
   * Whether the database is currently in a transaction.
   *
   * Derived from the server's `get_autocommit` status (refreshed on every
   * request), so it reflects the connection's real transaction state — the
   * same as `sqlite3_get_autocommit()` on the native bindings — including
   * transactions opened with a raw `BEGIN`, not just via `transaction()`.
   */
  get inTransaction(): boolean {
    return this.session.inTransaction;
  }

  /**
   * Prepare a SQL statement for execution.
   * 
   * Prepared statements created from a Connection use the same underlying session so transaction boundaries are preserved.
   * This method fetches column metadata using the describe functionality.
   * 
   * @param sql - The SQL statement to prepare
   * @returns A Promise that resolves to a Statement object with column metadata
   * 
   * @example
   * ```typescript
   * const stmt = await client.prepare("SELECT * FROM users WHERE id = ?");
   * const columns = stmt.columns();
   * const user = await stmt.get([123]);
   * ```
   */
  async prepare(sql: string): Promise<Statement> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }

    // Describe on the existing session so it sees uncommitted DDL
    // (e.g. CREATE TABLE in the same transaction).
    await this.execLock.acquire();
    let description;
    try {
      description = await this.session.describe(sql);
    } finally {
      this.execLock.release();
    }

    const stmt = Statement.fromSession(this.session, sql, description.cols, this.execLock);
    if (this.defaultSafeIntegerMode) {
      stmt.safeIntegers(true);
    }
    return stmt;
  }


  /**
   * Like `prepare(sql).run(args)` but in a single round trip — skips `describe`
   * since run() does not need column metadata.
   */
  async run(sql: string, ...bindParameters: any[]): Promise<any> {
    if (!this.isOpen) throw new TypeError("The database connection is not open");
    const { params, queryOptions } = splitBindParameters(bindParameters);
    await this.execLock.acquire();
    try {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      return { changes: result.rowsAffected, lastInsertRowid: result.lastInsertRowid };
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Like `prepare(sql).get(args)` but in a single round trip.
   */
  async get(sql: string, ...bindParameters: any[]): Promise<any> {
    if (!this.isOpen) throw new TypeError("The database connection is not open");
    const { params, queryOptions } = splitBindParameters(bindParameters);
    await this.execLock.acquire();
    try {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      const row = result.rows[0];
      if (!row) return undefined;
      return createExpandedRow(row, result.columns);
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Like `prepare(sql).all(args)` but in a single round trip.
   */
  async all(sql: string, ...bindParameters: any[]): Promise<any[]> {
    if (!this.isOpen) throw new TypeError("The database connection is not open");
    const { params, queryOptions } = splitBindParameters(bindParameters);
    await this.execLock.acquire();
    try {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      return result.rows.map((row: any) => createExpandedRow(row, result.columns));
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Like `prepare(sql).iterate(args)` but in a single round trip. Buffers the
   * result set — the connection lock cannot be held across `yield` points
   * without risking deadlock on nested calls.
   */
  async *iterate(sql: string, ...bindParameters: any[]): AsyncGenerator<any> {
    for (const row of await this.all(sql, ...bindParameters)) yield row;
  }

  /**
   * Execute a SQL statement and return all results.
   * 
   * @param sql - The SQL statement to execute
   * @returns Promise resolving to the complete result set
   * 
   * @example
   * ```typescript
   * const result = await client.exec("SELECT * FROM users");
   * console.log(result.rows);
   * ```
   */
  async exec(sql: string, queryOptions?: QueryOptions): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      return await this.session.sequence(sql, queryOptions);
    } finally {
      this.execLock.release();
    }
  }


  /**
   * Executes a batch of SQL statements over this connection.
   *
   * By default, batch() is not transactional: each statement runs in its
   * own autocommit step, so a failure mid-batch leaves earlier successful
   * statements committed. Pass a `mode` to make the batch atomic — the
   * statements are wrapped in `BEGIN <mode>` / `COMMIT` (with `ROLLBACK`
   * on failure) and dispatched as a single Hrana request, so the whole
   * batch completes in one round-trip. When called from inside a
   * `connection.transaction(...)` callback the `mode` argument is ignored
   * and the surrounding transaction is reused.
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
   * @param mode - When set, makes the batch atomic. Accepts the same
   *   values as `connection.transaction(...)` variants: `"deferred"`,
   *   `"immediate"`, `"exclusive"`, `"concurrent"`. Ignored when already
   *   inside a transaction.
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
   * // Atomic via the transactionAsync() API for mixed workloads.
   * const txn = db.transactionAsync(async (tx) => {
   *   await tx.batch([{ sql: "INSERT INTO users(name) VALUES (?)", args: ["Eve"] }]);
   *   await tx.run("UPDATE counters SET n = n + 1");
   * });
   * await txn.immediate();
   */
  async batch(statements: BatchStatement[], options?: BatchMode | BatchOptions, queryOptions?: QueryOptions): Promise<any> {
    if (!Array.isArray(statements)) {
      throw new TypeError("Expected first argument to be an array of statements");
    }
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      const { mode, raw } = normalizeBatchOptions(options);
      // Inside an outer transaction(...) callback the surrounding BEGIN
      // already opened a transaction on this stream; emitting another
      // `BEGIN` step would fail, so ignore the user-supplied mode.
      const effectiveMode = this.session.inTransaction ? undefined : mode;
      const results = await this.session.batch(
        statements,
        effectiveMode,
        queryOptions,
        this.defaultSafeIntegerMode,
        raw,
      );
      return results.map((result: any) => toResultSet(result));
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Execute a pragma.
   * 
   * @param pragma - The pragma to execute
   * @returns Promise resolving to the result of the pragma
   */
  async pragma(pragma: string, queryOptions?: QueryOptions): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      const sql = `PRAGMA ${pragma}`;
      return await this.session.execute(sql, [], false, queryOptions);
    } finally {
      this.execLock.release();
    }
  }

  /**
   * Sets the default safe integers mode for all statements from this connection.
   * 
   * @param toggle - Whether to use safe integers by default.
   */
  defaultSafeIntegers(toggle?: boolean): void {
    this.defaultSafeIntegerMode = toggle === false ? false : true;
  }

  /**
   * Returns a function that executes the given function in a transaction.
   *
   * @deprecated Use {@link transactionAsync} instead. This wrapper only
   * emits `BEGIN`/`COMMIT` around the callback without owning the
   * connection, so concurrent statements and transactions can interleave
   * their own statements into the transaction's window (and be committed
   * or rolled back with it).
   *
   * @param fn - The function to wrap in a transaction
   * @returns A function that will execute fn within a transaction
   *
   * @example
   * ```typescript
   * const insert = await client.prepare("INSERT INTO users (name) VALUES (?)");
   * const insertMany = client.transaction((users) => {
   *   for (const user of users) {
   *     insert.run([user]);
   *   }
   * });
   *
   * await insertMany(['Alice', 'Bob', 'Charlie']);
   * ```
   */
  transaction(fn: (...args: any[]) => any): any {
    if (typeof fn !== "function") {
      throw new TypeError("Expected first argument to be a function");
    }

    const db = this;
    const wrapTxn = (mode: string) => {
      return async (...bindParameters: any[]) => {
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

    return properties.default.value;
  }

  /**
   * Returns a function that executes the given function in a transaction.
   *
   * The wrapper owns the connection for the whole BEGIN..COMMIT window: it
   * acquires the connection's execution lock before BEGIN and releases it
   * only after COMMIT/ROLLBACK, so no concurrent statement or transaction
   * can interleave its own statements into the transaction's window. The
   * callback receives a {@link Transaction} handle as its first argument,
   * followed by the arguments the wrapped function was called with — all
   * SQL inside the callback must go through that handle. Calls on the
   * `Connection` itself (or statements prepared from it) queue on the lock
   * until the transaction finishes, so awaiting them inside the callback
   * deadlocks the transaction. Callbacks that do not declare the handle
   * parameter are rejected.
   *
   * @param fn - The function to wrap in a transaction; receives the
   *   transaction handle followed by the caller's arguments
   * @returns A function that will execute fn within a transaction
   *
   * @example
   * ```typescript
   * const insertMany = client.transactionAsync(async (tx, users) => {
   *   const insert = await tx.prepare("INSERT INTO users (name) VALUES (?)");
   *   for (const user of users) {
   *     await insert.run([user]);
   *   }
   * });
   *
   * await insertMany(['Alice', 'Bob', 'Charlie']);
   * ```
   */
  transactionAsync(fn: (tx: Transaction, ...args: any[]) => any): any {
    if (typeof fn !== "function") {
      throw new TypeError("Expected first argument to be a function");
    }
    if (fn.length === 0) {
      throw new TypeError(
        "transactionAsync() callbacks receive a Transaction handle as their first argument " +
        "and must declare it: db.transactionAsync(async (tx, ...args) => { await tx.run(...) }).",
      );
    }

    const db = this;
    const wrapTxn = (mode: string) => {
      return async (...bindParameters: any[]) => {
        if (!db.isOpen) {
          throw new TypeError("The database connection is not open");
        }
        // own the session for the whole transaction: everything else
        // queues on execLock until COMMIT/ROLLBACK releases it
        await db.execLock.acquire();
        const txn = new Transaction(db.session, db.defaultSafeIntegerMode);
        try {
          await txn.exec("BEGIN " + mode);
          try {
            const result = await fn(txn, ...bindParameters);
            await txn.exec("COMMIT");
            return result;
          } catch (err) {
            await txn.exec("ROLLBACK");
            throw err;
          }
        } finally {
          txn.finish();
          db.execLock.release();
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

    return properties.default.value;
  }

  /**
   * Close the connection.
   * 
   * This sends a close request to the server to properly clean up the stream.
   */
  async close(): Promise<void> {
    this.isOpen = false;
    await this.session.close();
  }

  async reconnect(): Promise<void> {
    try {
      if (this.isOpen) {
        await this.close();
      }
    } finally {
      this.session = new Session(this.config);
      this.isOpen = true;
    }
  }

}

/**
 * A handle to an open transaction, passed as the first argument to the
 * callback of `Connection.transactionAsync()`. All SQL of the transaction
 * must go through this handle: the transaction wrapper holds the
 * connection's execution lock for the whole BEGIN..COMMIT window, so
 * `Connection` calls issued inside the callback wait for the transaction
 * to finish (and deadlock it if awaited), while the handle executes on the
 * already-owned session. Once the transaction commits or rolls back the
 * handle is closed and every method throws.
 */
export class Transaction {
  private session: Session;
  private defaultSafeIntegerMode: boolean;
  private active: boolean = true;
  // Per-transaction execution gate. The wrapper already holds the connection's
  // execLock for the whole transaction, so this never contends with other
  // transactions or connection-level calls; it serializes native calls *within*
  // this transaction — the same invariant the connection's execLock enforces on
  // the session — so statements issued concurrently in the callback cannot
  // interleave on the shared session. It also rejects use of a completed
  // transaction.
  private gate: Lock;

  constructor(session: Session, defaultSafeIntegerMode: boolean) {
    this.session = session;
    this.defaultSafeIntegerMode = defaultSafeIntegerMode;
    const lock = new AsyncLock();
    this.gate = {
      acquire: async () => { this.assertActive(); await lock.acquire(); },
      release: () => { lock.release(); },
    };
  }

  private async withGate<T>(fn: () => Promise<T>): Promise<T> {
    await this.gate.acquire();
    try {
      return await fn();
    } finally {
      this.gate.release();
    }
  }

  /** Whether the transaction is still open (COMMIT/ROLLBACK not executed yet). */
  get open(): boolean {
    return this.active;
  }

  private assertActive() {
    if (!this.active) {
      throw new TypeError("The transaction has already completed");
    }
  }

  /** @internal Marks the transaction completed; called by the wrapper. */
  finish() {
    this.active = false;
  }

  /**
   * Prepares a SQL statement scoped to the transaction. The statement runs
   * on the transaction's session without re-acquiring the connection lock
   * and becomes unusable once the transaction completes.
   */
  async prepare(sql: string): Promise<Statement> {
    const description = await this.withGate(() => this.session.describe(sql));
    const stmt = Statement.fromSession(this.session, sql, description.cols, this.gate);
    if (this.defaultSafeIntegerMode) {
      stmt.safeIntegers(true);
    }
    return stmt;
  }

  /**
   * Like `prepare(sql).run(args)` but in a single round trip.
   */
  async run(sql: string, ...bindParameters: any[]): Promise<any> {
    const { params, queryOptions } = splitBindParameters(bindParameters);
    return await this.withGate(async () => {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      return { changes: result.rowsAffected, lastInsertRowid: result.lastInsertRowid };
    });
  }

  /**
   * Like `prepare(sql).get(args)` but in a single round trip.
   */
  async get(sql: string, ...bindParameters: any[]): Promise<any> {
    const { params, queryOptions } = splitBindParameters(bindParameters);
    return await this.withGate(async () => {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      const row = result.rows[0];
      if (!row) return undefined;
      return createExpandedRow(row, result.columns);
    });
  }

  /**
   * Like `prepare(sql).all(args)` but in a single round trip.
   */
  async all(sql: string, ...bindParameters: any[]): Promise<any[]> {
    const { params, queryOptions } = splitBindParameters(bindParameters);
    return await this.withGate(async () => {
      const result = await this.session.execute(sql, normalizeArgs(params), this.defaultSafeIntegerMode, queryOptions);
      return result.rows.map((row: any) => createExpandedRow(row, result.columns));
    });
  }

  /**
   * Like `prepare(sql).iterate(args)` but buffered, matching
   * `Connection.iterate()`.
   */
  async *iterate(sql: string, ...bindParameters: any[]): AsyncGenerator<any> {
    for (const row of await this.all(sql, ...bindParameters)) yield row;
  }

  /**
   * Execute a SQL statement and return all results.
   */
  async execute(sql: string, args?: any[], queryOptions?: QueryOptions): Promise<any> {
    return await this.withGate(() => this.session.execute(sql, args || [], this.defaultSafeIntegerMode, queryOptions));
  }

  /**
   * Executes the given SQL string inside the transaction.
   * Unlike prepared statements, this can execute strings that contain multiple SQL statements.
   */
  async exec(sql: string, queryOptions?: QueryOptions): Promise<any> {
    return await this.withGate(() => this.session.sequence(sql, queryOptions));
  }

  /**
   * Executes a batch of SQL statements inside the transaction. The batch
   * joins the surrounding transaction: any `mode` option is ignored since
   * the stream is already inside `BEGIN`.
   */
  async batch(statements: BatchStatement[], options?: BatchMode | BatchOptions, queryOptions?: QueryOptions): Promise<any> {
    if (!Array.isArray(statements)) {
      throw new TypeError("Expected first argument to be an array of statements");
    }
    const { raw } = normalizeBatchOptions(options);
    return await this.withGate(async () => {
      const results = await this.session.batch(
        statements,
        undefined,
        queryOptions,
        this.defaultSafeIntegerMode,
        raw,
      );
      return results.map((result: any) => toResultSet(result));
    });
  }
}

/**
 * Create a new connection to a Turso database.
 *
 * This is a lightweight operation — it only allocates a config object. No network
 * I/O happens until the first query. The underlying `fetch()` implementation
 * automatically pools TCP/TLS connections to the same origin, so creating many
 * connections is cheap.
 *
 * Each connection is single-stream: concurrent calls on the same connection are
 * automatically serialized. For true parallelism, create multiple connections:
 *
 * ```typescript
 * import { connect } from "@tursodatabase/serverless";
 *
 * const config = { url: process.env.TURSO_URL, authToken: process.env.TURSO_TOKEN };
 *
 * // Sequential (single connection is fine)
 * const conn = connect(config);
 * const a = await conn.all("SELECT 1");
 * const b = await conn.all("SELECT 2");
 *
 * // Parallel (use separate connections)
 * const [x, y] = await Promise.all([
 *   connect(config).all("SELECT 1"),
 *   connect(config).all("SELECT 2"),
 * ]);
 * ```
 *
 * @param config - Configuration object with database URL and auth token
 * @returns A new Connection instance
 */
export function connect(config: Config): Connection {
  return new Connection(config);
}
