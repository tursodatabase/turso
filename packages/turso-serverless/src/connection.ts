import { Session, type SessionConfig } from './session.js';
import { Statement } from './statement.js';
import { Transaction, TransactionMode } from './transaction.js';

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

  constructor(config: Config) {
    this.config = config;
    this.session = new Session(config);
  }

  /**
   * Prepare a SQL statement for execution.
   *
   * Each prepared statement gets its own session to avoid conflicts during concurrent execution.
   *
   * @param sql - The SQL statement to prepare
   * @returns A Statement object that can be executed multiple ways
   *
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT * FROM users WHERE id = ?");
   * const user = await stmt.get([123]);
   * const allUsers = await stmt.all();
   * ```
   */
  prepare(sql: string): Statement {
    return new Statement(this.config, sql);
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
   * const result = await client.execute("SELECT * FROM users");
   * console.log(result.rows);
   * ```
   */
  async execute(sql: string, args: any[] = []): Promise<any> {
    return this.session.execute(sql, args);
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
    * Begin a new transaction.
    *
    * @param mode - Optional transaction mode (write, read, or deferred)
    * @returns Promise resolving to a new Transaction instance
    *
    * @example
    * ```typescript
    * const tx = await client.transaction();
    * try {
    *   await tx.execute("INSERT INTO users (name) VALUES (?)", ["Alice"]);
    *   await tx.execute("INSERT INTO users (name) VALUES (?)", ["Bob"]);
    *   await tx.commit();
    * } catch (error) {
    *   await tx.rollback();
    * }
    * ```
    */
  async transaction(mode: TransactionMode = "deferred"): Promise<Transaction> {
    return Transaction.create(this.config, mode);
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
