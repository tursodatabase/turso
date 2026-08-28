export class SqliteError extends Error {
  name: string;
  code: string;
  rawCode: string;
  /** For errors raised by `batch()`: the zero-based index of the failing
   * statement. */
  batchIndex?: number;
  /** For errors raised by `batch()`: one entry per input statement, in
   * order — the completed statement's `ResultSet`, or `null` for the
   * failing statement and the statements that did not run. In a
   * non-atomic batch the completed statements' effects are committed; in
   * an atomic batch they were rolled back. Empty when the batch failed
   * client-side before anything was sent. */
  batchResults?: Array<any | null>;
  constructor(message, code, rawCode) {
    super(message);
    this.name = 'SqliteError';
    this.code = code;
    this.rawCode = rawCode;

    (Error as any).captureStackTrace(this, SqliteError);
  }
}
