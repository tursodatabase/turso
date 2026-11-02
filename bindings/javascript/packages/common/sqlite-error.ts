import { NativeDatabase, NativeExecutor, NativeStatement, StepResult } from "./types.js";

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
   * an atomic batch they were rolled back unless `rollbackError` is set.
   * Empty when the batch failed client-side before anything was sent. */
  batchResults?: Array<any | null>;
  /** A rollback error that occurred while handling this error. */
  rollbackError?: Error;
  constructor(message, code, rawCode) {
    super(message);
    this.name = 'SqliteError';
    this.code = code;
    this.rawCode = rawCode;

    (Error as any).captureStackTrace(this, SqliteError);
  }
}

/** Steps a native statement or executor and throws its errors as `SqliteError`. */
export function stepSync(stmt: NativeStatement | NativeExecutor): StepResult {
  try {
    return stmt.stepSync();
  } catch (err) {
    throw convertError(err);
  }
}

/** Runs the native I/O loop once and throws its errors as `SqliteError`. */
export function ioLoopSync(db: NativeDatabase) {
  try {
    db.ioLoopSync();
  } catch (err) {
    throw convertError(err);
  }
}

const convertibleErrorTypes = { TypeError };
const CONVERTIBLE_ERROR_PREFIX = "[TURSO_CONVERT_TYPE]";

export function convertError(err) {
  if ((err.code ?? "").startsWith(CONVERTIBLE_ERROR_PREFIX)) {
    return createErrorByName(
      err.code.substring(CONVERTIBLE_ERROR_PREFIX.length),
      err.message,
    );
  }

  return new SqliteError(err.message, err.code, err.rawCode);
}

function createErrorByName(name, message) {
  const ErrorConstructor = convertibleErrorTypes[name];
  if (!ErrorConstructor) {
    throw new Error(`unknown error type ${name} from Turso`);
  }

  return new ErrorConstructor(message);
}
