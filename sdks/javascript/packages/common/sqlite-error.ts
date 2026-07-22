export class SqliteError extends Error {
  name: string;
  code: string;
  rawCode: string;
  constructor(message, code, rawCode) {
    super(message);
    this.name = 'SqliteError';
    this.code = code;
    this.rawCode = rawCode;

    (Error as any).captureStackTrace(this, SqliteError);
  }
}
