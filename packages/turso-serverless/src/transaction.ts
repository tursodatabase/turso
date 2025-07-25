import { Session, type SessionConfig } from "./session.js";
import { DatabaseError } from "./error.js";

/**
 * Transaction mode for controlling transaction behavior.
 */
export type TransactionMode = "write" | "read" | "deferred";

/**
 * Transactions use a dedicated session to maintain state across multiple operations.
 * All operations within a transaction are executed atomically - they either all
 * succeed or all fail.
 */
export class Transaction {
  private session: Session;
  private _closed = false;
  private _committed = false;
  private _rolledBack = false;

  private constructor(
    sessionConfig: SessionConfig,
    mode: TransactionMode = "deferred",
  ) {
    this.session = new Session(sessionConfig);
  }

  /**
   * Create a new transaction instance.
   *
   * @param sessionConfig - Session configuration
   * @param mode - Transaction mode
   * @returns Promise resolving to a new Transaction instance
   */
  static async create(
    sessionConfig: SessionConfig,
    mode: TransactionMode = "deferred",
  ): Promise<Transaction> {
    const transaction = new Transaction(sessionConfig, mode);
    await transaction.initializeTransaction(mode);
    return transaction;
  }

  private async initializeTransaction(mode: TransactionMode): Promise<void> {
    let beginStatement: string;

    switch (mode) {
      case "write":
        beginStatement = "BEGIN IMMEDIATE";
        break;
      case "read":
        beginStatement = "BEGIN";
        break;
      case "deferred":
      default:
        beginStatement = "BEGIN DEFERRED";
        break;
    }

    await this.session.execute(beginStatement);
  }

  /**
   * Execute a SQL statement within the transaction.
   *
   * @param sql - The SQL statement to execute
   * @param args - Optional array of parameter values
   * @returns Promise resolving to the complete result set
   *
   * @example
   * ```typescript
   * const tx = await client.transaction();
   * const result = await tx.execute("INSERT INTO users (name) VALUES (?)", ["Alice"]);
   * await tx.commit();
   * ```
   */
  async execute(sql: string, args: any[] = []): Promise<any> {
    this.checkState();
    return this.session.execute(sql, args);
  }

  /**
   * Execute multiple SQL statements as a batch within the transaction.
   *
   * @param statements - Array of SQL statements to execute
   * @returns Promise resolving to batch execution results
   *
   * @example
   * ```typescript
   * const tx = await client.transaction();
   * await tx.batch([
   *   "INSERT INTO users (name) VALUES ('Alice')",
   *   "INSERT INTO users (name) VALUES ('Bob')"
   * ]);
   * await tx.commit();
   * ```
   */
  async batch(statements: string[]): Promise<any> {
    this.checkState();
    return this.session.batch(statements);
  }

  /**
   * Execute a SQL statement and return the raw response and entries.
   *
   * @param sql - The SQL statement to execute
   * @param args - Optional array of parameter values
   * @returns Promise resolving to the raw response and cursor entries
   */
  async executeRaw(
    sql: string,
    args: any[] = [],
  ): Promise<{ response: any; entries: AsyncGenerator<any> }> {
    this.checkState();
    return this.session.executeRaw(sql, args);
  }

  /**
   * Commit the transaction, making all changes permanent.
   *
   * @returns Promise that resolves when the transaction is committed
   *
   * @example
   * ```typescript
   * const tx = await client.transaction();
   * await tx.execute("INSERT INTO users (name) VALUES (?)", ["Alice"]);
   * await tx.commit(); // Changes are now permanent
   * ```
   */
  async commit(): Promise<void> {
    this.checkState();

    try {
      await this.session.execute("COMMIT");
      this._committed = true;
      this._closed = true;
    } catch (error) {
      // If commit fails, the transaction is still open
      if (error instanceof Error) {
        throw new DatabaseError(error.message);
      }
      throw new DatabaseError('Transaction commit failed');
    }
  }

  /**
   * Rollback the transaction, undoing all changes.
   *
   * @returns Promise that resolves when the transaction is rolled back
   *
   * @example
   * ```typescript
   * const tx = await client.transaction();
   * try {
   *   await tx.execute("INSERT INTO users (name) VALUES (?)", ["Alice"]);
   *   await tx.execute("INSERT INTO invalid_table VALUES (1)"); // This will fail
   *   await tx.commit();
   * } catch (error) {
   *   await tx.rollback(); // Undo the first INSERT
   * }
   * ```
   */
  async rollback(): Promise<void> {
    if (this._closed) {
      return; // Already closed, nothing to rollback
    }

    try {
      await this.session.execute("ROLLBACK");
    } catch (error) {
      // Rollback errors are generally not critical - the transaction is abandoned anyway
    } finally {
      this._rolledBack = true;
      this._closed = true;
    }
  }

  /**
   * Close the transaction without committing or rolling back.
   *
   * This will automatically rollback any uncommitted changes.
   * It's safe to call this method multiple times.
   */
  close(): void {
    if (!this._closed) {
      // Async rollback - don't wait for it to complete
      this.rollback().catch(() => {
        // Ignore rollback errors on close
      });
    }
  }

  /**
   * Check if the transaction is closed.
   *
   * @returns True if the transaction has been committed, rolled back, or closed
   */
  get closed(): boolean {
    return this._closed;
  }

  /**
   * Check if the transaction has been committed.
   *
   * @returns True if the transaction has been successfully committed
   */
  get committed(): boolean {
    return this._committed;
  }

  /**
   * Check if the transaction has been rolled back.
   *
   * @returns True if the transaction has been rolled back
   */
  get rolledBack(): boolean {
    return this._rolledBack;
  }

  /**
   * Check transaction state and throw if it's not valid for operations.
   *
   * @throws Error if the transaction is closed
   */
  private checkState(): void {
    if (this._closed) {
      if (this._committed) {
        throw new DatabaseError("Transaction has already been committed");
      } else if (this._rolledBack) {
        throw new DatabaseError("Transaction has already been rolled back");
      } else {
        throw new DatabaseError("Transaction has been closed");
      }
    }
  }
}
