import type { NativeStatement, Row, RunResult, SQLiteValue } from './types';

/**
 * Statement represents a prepared SQL statement that can be executed multiple times.
 */
export class Statement {
  private _stmt: NativeStatement;
  private _finalized = false;

  constructor(stmt: NativeStatement) {
    this._stmt = stmt;
  }

  /**
   * Binds parameters to the statement.
   *
   * @param params - Parameters to bind (positional or named)
   * @returns This statement for chaining
   */
  bind(...params: SQLiteValue[]): this {
    this._checkFinalized();
    this._stmt.bind(...params);
    return this;
  }

  /**
   * Executes the statement and returns run result info.
   * Use for INSERT, UPDATE, DELETE statements.
   *
   * @param params - Optional parameters to bind before execution
   * @returns Object with changes and lastInsertRowid
   */
  run(...params: SQLiteValue[]): RunResult {
    this._checkFinalized();
    return this._stmt.run(...params);
  }

  /**
   * Executes the statement and returns the first row.
   *
   * @param params - Optional parameters to bind before execution
   * @returns The first row or undefined if no results
   */
  get(...params: SQLiteValue[]): Row | undefined {
    this._checkFinalized();
    return this._stmt.get(...params);
  }

  /**
   * Executes the statement and returns all rows.
   *
   * @param params - Optional parameters to bind before execution
   * @returns Array of all matching rows
   */
  all(...params: SQLiteValue[]): Row[] {
    this._checkFinalized();
    return this._stmt.all(...params);
  }

  /**
   * Resets the statement so it can be executed again.
   *
   * @returns This statement for chaining
   */
  reset(): this {
    this._checkFinalized();
    this._stmt.reset();
    return this;
  }

  /**
   * Finalizes the statement, releasing resources.
   * After calling finalize(), the statement cannot be used.
   */
  finalize(): void {
    if (!this._finalized) {
      this._stmt.finalize();
      this._finalized = true;
    }
  }

  private _checkFinalized(): void {
    if (this._finalized) {
      throw new Error('Statement is finalized');
    }
  }
}
