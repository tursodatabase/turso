export class DatabaseError extends Error {
  /** Machine-readable error code (e.g., "SQLITE_CONSTRAINT") */
  code?: string;
  /** Raw numeric error code */
  rawCode?: number;
  /** Original error that caused this error */
  declare cause?: Error;

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