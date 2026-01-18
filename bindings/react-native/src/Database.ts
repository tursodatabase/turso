import { Statement } from './Statement';
import type {
  BindParams,
  NativeDatabase,
  OpenDatabaseOptions,
  Row,
  RunResult,
  SQLiteValue,
} from './types';

/**
 * Database represents a connection that can prepare and execute SQL statements.
 */
export class Database {
  private _db: NativeDatabase;
  private _closed = false;

  constructor(db: NativeDatabase) {
    this._db = db;
  }

  /**
   * Returns true if the database is open
   */
  get open(): boolean {
    return this._db.open && !this._closed;
  }

  /**
   * Returns true if currently in a transaction
   */
  get inTransaction(): boolean {
    return this._db.inTransaction;
  }

  /**
   * Returns the last inserted row ID
   */
  get lastInsertRowid(): number {
    return this._db.lastInsertRowid;
  }

  /**
   * Returns the database path
   */
  get path(): string {
    return this._db.path;
  }

  /**
   * Returns true if this is an in-memory database
   */
  get memory(): boolean {
    return this._db.memory;
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param sql - The SQL statement string to prepare
   * @returns A prepared Statement object
   */
  prepare(sql: string): Statement {
    this._checkOpen();
    const nativeStmt = this._db.prepare(sql);
    return new Statement(nativeStmt);
  }

  /**
   * Executes a SQL string that may contain multiple statements.
   * Does not return results - use for DDL and other non-query statements.
   *
   * @param sql - The SQL string to execute
   */
  exec(sql: string): void {
    this._checkOpen();
    this._db.exec(sql);
  }

  /**
   * Executes a SQL statement and returns run result info.
   * Use for INSERT, UPDATE, DELETE statements.
   *
   * @param sql - The SQL statement to execute
   * @param params - Optional parameters to bind
   * @returns Object with changes and lastInsertRowid
   */
  run(sql: string, ...params: SQLiteValue[]): RunResult {
    this._checkOpen();
    return this._db.run(sql, ...params);
  }

  /**
   * Executes a SQL query and returns the first row.
   *
   * @param sql - The SQL query to execute
   * @param params - Optional parameters to bind
   * @returns The first row or undefined if no results
   */
  get(sql: string, ...params: SQLiteValue[]): Row | undefined {
    this._checkOpen();
    return this._db.get(sql, ...params);
  }

  /**
   * Executes a SQL query and returns all rows.
   *
   * @param sql - The SQL query to execute
   * @param params - Optional parameters to bind
   * @returns Array of all matching rows
   */
  all(sql: string, ...params: SQLiteValue[]): Row[] {
    this._checkOpen();
    return this._db.all(sql, ...params);
  }

  /**
   * Executes a function within a transaction.
   * Automatically commits on success, rolls back on error.
   *
   * @param fn - Function to execute within the transaction
   * @returns The return value of the function
   */
  transaction<T>(fn: () => T): T {
    this._checkOpen();
    this.exec('BEGIN');
    try {
      const result = fn();
      this.exec('COMMIT');
      return result;
    } catch (error) {
      this.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Executes a function within an immediate transaction.
   *
   * @param fn - Function to execute within the transaction
   * @returns The return value of the function
   */
  immediateTransaction<T>(fn: () => T): T {
    this._checkOpen();
    this.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.exec('COMMIT');
      return result;
    } catch (error) {
      this.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Executes a function within an exclusive transaction.
   *
   * @param fn - Function to execute within the transaction
   * @returns The return value of the function
   */
  exclusiveTransaction<T>(fn: () => T): T {
    this._checkOpen();
    this.exec('BEGIN EXCLUSIVE');
    try {
      const result = fn();
      this.exec('COMMIT');
      return result;
    } catch (error) {
      this.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Closes the database connection.
   * After calling close(), the database cannot be used.
   */
  close(): void {
    if (!this._closed) {
      this._db.close();
      this._closed = true;
    }
  }

  private _checkOpen(): void {
    if (this._closed) {
      throw new Error('Database is closed');
    }
  }
}
