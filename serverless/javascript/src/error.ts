export class DatabaseError extends Error {
  /** Machine-readable error code (e.g., "SQLITE_CONSTRAINT") */
  code?: string;
  /** Raw numeric error code */
  rawCode?: number;
  /** Original error that caused this error */
  declare cause?: Error;
  /** For errors raised by `batch()`: the zero-based index of the failing
   * statement, when a user statement (rather than the surrounding
   * transaction control) failed. */
  batchIndex?: number;
  /** For errors raised by `batch()`: one entry per input statement, in
   * order — the completed statement's `ResultSet`, or `null` for the
   * failing statement and the statements that did not run. In a
   * non-atomic batch the completed statements' effects are committed; in
   * an atomic batch they were rolled back. Empty when the batch failed
   * client-side before anything was sent. */
  batchResults?: Array<any | null>;

  constructor(message: string, code?: string, rawCode?: number, cause?: Error) {
    super(message);
    this.name = 'DatabaseError';
    this.code = code;
    this.rawCode = rawCode;
    this.cause = cause;
    Object.setPrototypeOf(this, DatabaseError.prototype);
  }
}

/**
 * Error thrown when a query exceeds the configured timeout.
 *
 * This is a subclass of `DatabaseError` with `code` set to `"TIMEOUT"`.
 * Catch this type to distinguish timeouts from other database errors
 * and decide whether to retry or fail gracefully.
 */
export class TimeoutError extends DatabaseError {
  constructor(message: string = 'Query timed out', cause?: Error) {
    super(message, 'TIMEOUT', undefined, cause);
    this.name = 'TimeoutError';
    Object.setPrototypeOf(this, TimeoutError.prototype);
  }
}