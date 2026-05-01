import { AsyncLock } from './async-lock.js';
import { Session, type SessionConfig } from './session.js';
import { Statement } from './statement.js';
import { type QueryOptions } from './protocol.js';

/**
 * Configuration options for connecting to a Turso database.
 */
export interface Config extends SessionConfig {}

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
  private _inTransaction: boolean = false;
  private execLock: AsyncLock = new AsyncLock();

  constructor(config: Config) {
    if (!config.url) {
      throw new Error("invalid config: url is required");
    }
    this.config = config;
    this.session = new Session(config);
    
    // Define inTransaction property
    Object.defineProperty(this, 'inTransaction', {
      get: () => this._inTransaction,
      enumerable: true
    });
  }

  /**
   * Whether the database is currently in a transaction.
   */
  get inTransaction(): boolean {
    return this._inTransaction;
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
   * Execute multiple SQL statements in a batch.
   * 
   * @param statements - Array of SQL statements to execute
   * @param mode - Optional transaction mode (currently unused)
   * @returns Promise resolving to batch execution results
   * 
   * @example
   * ```typescript
   * await client.batch([
   *   "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   *   "INSERT INTO users (name) VALUES ('Alice')",
   *   "INSERT INTO users (name) VALUES ('Bob')"
   * ]);
   * ```
   */
  async batch(statements: string[], mode?: string, queryOptions?: QueryOptions): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    await this.execLock.acquire();
    try {
      return await this.session.batch(statements, queryOptions);
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
