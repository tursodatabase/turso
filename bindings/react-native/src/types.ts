/**
 * Supported SQLite value types
 */
export type SQLiteValue = null | number | string | ArrayBuffer;

/**
 * Parameters that can be bound to SQL statements
 */
export type BindParams =
  | SQLiteValue[]
  | Record<string, SQLiteValue>
  | SQLiteValue;

/**
 * Result of a run() or exec() operation
 */
export interface RunResult {
  /** Number of rows changed by the statement */
  changes: number;
  /** Last inserted row ID */
  lastInsertRowid: number;
}

/**
 * A row returned from a query
 */
export type Row = Record<string, SQLiteValue>;

/**
 * Options for opening a database
 */
export interface OpenDatabaseOptions {
  /** Database name or path. Use ':memory:' for in-memory database */
  name: string;
  /** If true, open in read-only mode */
  readonly?: boolean;
}

/**
 * Setup options for configuring Turso behavior
 */
export interface SetupOptions {
  /**
   * Log level for Turso operations.
   * Valid values: "trace", "debug", "info", "warn", "error"
   */
  logLevel?: string;
}

/**
 * Native module interface (internal)
 */
export interface TursoNativeModule {
  install(): boolean;
}

/**
 * Native proxy interface exposed via JSI
 */
export interface TursoProxy {
  open(options: OpenDatabaseOptions | string): NativeDatabase;
  version(): string;
  setup(options: {logLevel?: string}): void;
}

/**
 * Native database interface
 */
export interface NativeDatabase {
  open: boolean;
  inTransaction: boolean;
  lastInsertRowid: number;
  path: string;
  memory: boolean;

  prepare(sql: string): NativeStatement;
  exec(sql: string): void;
  run(sql: string, ...params: SQLiteValue[]): RunResult;
  get(sql: string, ...params: SQLiteValue[]): Row | undefined;
  all(sql: string, ...params: SQLiteValue[]): Row[];
  close(): void;
}

/**
 * Native statement interface
 */
export interface NativeStatement {
  bind(...params: SQLiteValue[]): void;
  run(...params: SQLiteValue[]): RunResult;
  get(...params: SQLiteValue[]): Row | undefined;
  all(...params: SQLiteValue[]): Row[];
  finalize(): void;
  reset(): void;
}
