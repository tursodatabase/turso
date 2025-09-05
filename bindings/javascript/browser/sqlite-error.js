export class SqliteError extends Error {
  name;
  code;
  rawCode;
  constructor(message, code, rawCode) {
    super(message);
    this.name = 'SqliteError';
    this.code = code;
    this.rawCode = rawCode;

    Error.captureStackTrace(this, SqliteError);
  }
}
