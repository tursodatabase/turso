import { AsyncLock } from './async-lock.js';
import { Session, type SessionConfig, type BatchMode } from './session.js';
import { Statement } from './statement.js';
import { type QueryOptions } from './protocol.js';
import { normalizeArgs, splitBindParameters } from './args.js';

export type { BatchMode } from './session.js';

/**
 * Configuration options for connecting to a Turso database.
 */
export interface Config extends SessionConfig {}

export type BatchStatement = string | {
  sql: string;
  args?: any[] | Record<string, any>;
};


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
 * If you call `execute()` while another is in flight, the call automatically
 * waits for the previous one to finish — just like the native
 * `@tursodatabase/database` binding.
 *
 * ## Parallel queries
 *
 * For parallelism, create multiple connections. `connect()` is cheap — it just
 * allocates a config object. No TCP connection is opened until the first `execute()`,
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
 *   connect(config).execute("SELECT * FROM users WHERE active = 1"),
 *   connect(config).execute("SELECT * FROM orders WHERE status = 'pending'"),
 * ]);
 *
 * // Option 2: reusable pool for repeated parallel work
 * const pool = Array.from({ length: 4 }, () => connect(config));
 * const results = await Promise.all(
 *   queries.map((sql, i) => pool[i % pool.length].execute(sql))
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
      const obj: any = {};
      result.columns.forEach((col: string, i: number) => { obj[col] = row[i]; });
      return obj;
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
      return result.rows.map((row: any) => {
        const obj: any = {};
        result.columns.forEach((col: string, i: number) => { obj[col] = row[i]; });
        return obj;
      });
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
   * @param args - Optional array of parameter values
   * @returns Promise resolving to the complete result set
   *
   * @example
   * ```typescript
   * const result = await client.execute("SELECT * FROM users WHERE id = ?", [123]);
   * console.log(result.rows);
   * ```
   */
  async execute(sql: string, args?: any[], queryOptions?: QueryOptions): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      return await this.session.execute(sql, args || [], this.defaultSafeIntegerMode, queryOptions);
    } finally {
      this.execLock.release();
    }
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
   * @returns An object with `rowsAffected` (sum of affected rows) and
   *   `lastInsertRowid` (rowid of the last successful insert).
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
   *   await db.execute("UPDATE counters SET n = n + 1");
   * });
   * await txn.immediate();
   */
  async batch(statements: BatchStatement[], mode?: BatchMode, queryOptions?: QueryOptions): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      // Inside an outer transaction(...) callback the surrounding BEGIN
      // already opened a transaction on this stream; emitting another
      // `BEGIN` step would fail, so ignore the user-supplied mode.
      const effectiveMode = this.session.inTransaction ? undefined : mode;
      return await this.session.batch(statements, effectiveMode, queryOptions);
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
 * const a = await conn.execute("SELECT 1");
 * const b = await conn.execute("SELECT 2");
 *
 * // Parallel (use separate connections)
 * const [x, y] = await Promise.all([
 *   connect(config).execute("SELECT 1"),
 *   connect(config).execute("SELECT 2"),
 * ]);
 * ```
 *
 * @param config - Configuration object with database URL and auth token
 * @returns A new Connection instance
 */
export function connect(config: Config): Connection {
  return new Connection(config);
}
