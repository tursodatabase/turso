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