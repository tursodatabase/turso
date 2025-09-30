import { Session, type SessionConfig } from './session.js';
import { Statement } from './statement.js';

/**
 * Configuration options for connecting to a Turso database.
 */
export interface Config extends SessionConfig {}

/**
 * A connection to a Turso database.
 * 
 * Provides methods for executing SQL statements and managing prepared statements.
 * Uses the SQL over HTTP protocol with streaming cursor support for optimal performance.
 */
export class Connection {
  private config: Config;
  private session: Session;
  private isOpen: boolean = true;
  private defaultSafeIntegerMode: boolean = false;
  private _inTransaction: boolean = false;

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
   * Each prepared statement gets its own session to avoid conflicts during concurrent execution.
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
    
    // Create a session to get column metadata via describe
    const session = new Session(this.config);
    const description = await session.describe(sql);
    await session.close();
    
    const stmt = new Statement(this.config, sql, description.cols);
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
  async execute(sql: string, args?: any[]): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    return this.session.execute(sql, args || [], this.defaultSafeIntegerMode);
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
  async exec(sql: string): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    return this.session.sequence(sql);
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
  async batch(statements: string[], mode?: string): Promise<any> {
    return this.session.batch(statements);
  }

  /**
   * Execute a pragma.
   * 
   * @param pragma - The pragma to execute
   * @returns Promise resolving to the result of the pragma
   */
  async pragma(pragma: string): Promise<any> {
    if (!this.isOpen) {
      throw new TypeError("The database connection is not open");
    }
    const sql = `PRAGMA ${pragma}`;
    return this.session.execute(sql);
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
 * @param config - Configuration object with database URL and auth token
 * @returns A new Connection instance
 * 
 * @example
 * ```typescript
 * import { connect } from "@tursodatabase/serverless";
 * 
 * const client = connect({
 *   url: process.env.TURSO_DATABASE_URL,
 *   authToken: process.env.TURSO_AUTH_TOKEN
 * });
 * ```
 */
export function connect(config: Config): Connection {
  return new Connection(config);
}
