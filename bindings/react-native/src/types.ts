/**
 * Turso React Native SDK-KIT Types
 *
 * Clean TypeScript types matching the SDK-KIT C API patterns.
 * All logic lives in TypeScript or Rust - the C++ layer is just a thin bridge.
 */

// ============================================================================
// Core SDK-KIT Types (Local Database)
// ============================================================================

/**
 * Native database interface (local-only)
 * Thin wrapper around TursoDatabaseHostObject
 */
export interface NativeDatabase {
  open(): void;
  connect(): NativeConnection;
  close(): void;
}

/**
 * Native connection interface
 * Thin wrapper around TursoConnectionHostObject
 */
export interface NativeConnection {
  prepareSingle(sql: string): NativeStatement;
  prepareFirst(sql: string): { statement: NativeStatement | null; tailIdx: number };
  lastInsertRowid(): number;
  getAutocommit(): boolean;
  setBusyTimeout(timeoutMs: number): void;
  close(): void;
}

/**
 * Native statement interface
 * Thin wrapper around TursoStatementHostObject
 */
export interface NativeStatement {
  // Bind methods
  bindPositionalNull(position: number): number;
  bindPositionalInt(position: number, value: number): number;
  bindPositionalDouble(position: number, value: number): number;
  bindPositionalBlob(position: number, value: ArrayBuffer): number;
  bindPositionalText(position: number, value: string): number;

  // Execution methods
  execute(): { status: number; rowsChanged: number };
  step(): number;  // Returns status code
  runIo(): number;
  reset(): void;
  finalize(): void;

  // Query methods
  nChange(): number;
  columnCount(): number;
  columnName(index: number): string | null;
  rowValueKind(index: number): number;  // TursoType enum
  rowValueBytesCount(index: number): number;
  rowValueBytesPtr(index: number): ArrayBuffer | null;
  rowValueInt(index: number): number;
  rowValueDouble(index: number): number;

  // Parameter methods
  namedPosition(name: string): number;
  parametersCount(): number;
}

// ============================================================================
// Sync SDK-KIT Types (Embedded Replica)
// ============================================================================

/**
 * Native sync database interface (embedded replica)
 * Thin wrapper around TursoSyncDatabaseHostObject
 */
export interface NativeSyncDatabase {
  // Async operations - return NativeSyncOperation
  open(): NativeSyncOperation;
  create(): NativeSyncOperation;
  connect(): NativeSyncOperation;
  stats(): NativeSyncOperation;
  checkpoint(): NativeSyncOperation;
  pushChanges(): NativeSyncOperation;
  waitChanges(): NativeSyncOperation;
  applyChanges(changes: NativeSyncChanges): NativeSyncOperation;

  // IO queue management
  ioTakeItem(): NativeSyncIoItem | null;
  ioStepCallbacks(): void;

  close(): void;
}

/**
 * Native sync operation interface
 * Thin wrapper around TursoSyncOperationHostObject
 * Represents an async operation that must be driven by calling resume()
 */
export interface NativeSyncOperation {
  resume(): number;  // Returns status code (TURSO_DONE, TURSO_IO, etc.)
  resultKind(): number;  // Returns result type enum
  extractConnection(): NativeConnection;
  extractChanges(): NativeSyncChanges | null;
  extractStats(): SyncStats;
}

/**
 * Native sync IO item interface
 * Thin wrapper around TursoSyncIoItemHostObject
 * Represents an IO request that JavaScript must process using fetch() or fs
 */
export interface NativeSyncIoItem {
  getKind(): 'HTTP' | 'FULL_READ' | 'FULL_WRITE' | 'NONE';
  getHttpRequest(): HttpRequest;
  getFullReadPath(): string;
  getFullWriteRequest(): FullWriteRequest;

  // Completion methods
  poison(error: string): void;
  setStatus(statusCode: number): void;
  pushBuffer(data: ArrayBuffer): void;
  done(): void;
}

/**
 * Native sync changes interface
 * Thin wrapper around TursoSyncChangesHostObject
 * Represents changes fetched from remote (opaque, passed to applyChanges)
 */
export interface NativeSyncChanges {
  // Mostly opaque - just passed to applyChanges()
}

// ============================================================================
// Turso Status Codes
// ============================================================================

export enum TursoStatus {
  OK = 0,
  DONE = 1,
  ROW = 2,
  IO = 3,
  BUSY = 4,
  INTERRUPT = 5,
  BUSY_SNAPSHOT = 6,
  ERROR = 127,
  MISUSE = 128,
  CONSTRAINT = 129,
  READONLY = 130,
  DATABASE_FULL = 131,
  NOTADB = 132,
  CORRUPT = 133,
  IOERR = 134,
}

// ============================================================================
// Turso Value Types
// ============================================================================

export enum TursoType {
  UNKNOWN = 0,
  INTEGER = 1,
  REAL = 2,
  TEXT = 3,
  BLOB = 4,
  NULL = 5,
}

// ============================================================================
// Sync Operation Result Types
// ============================================================================

export enum SyncOperationResultType {
  NONE = 0,
  CONNECTION = 1,
  CHANGES = 2,
  STATS = 3,
}

// ============================================================================
// Public API Types (High-level TypeScript)
// ============================================================================

/**
 * Supported SQLite value types for the public API
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
 * Encryption options (matches JavaScript bindings)
 */
export interface EncryptionOpts {
  /** base64 encoded encryption key (must be either 16 or 32 bytes depending on cipher) */
  key: string;
  /** encryption cipher algorithm */
  cipher: 'aes256gcm' | 'aes128gcm' | 'chacha20poly1305' | 'aegis256';
}

/**
 * Database options (matches JavaScript bindings)
 * Single unified config for both local and sync databases
 */
export interface DatabaseOpts {
  /**
   * Local path where to store database file (e.g. local.db)
   * Sync database will write several files with that prefix
   * (e.g. local.db-info, local.db-wal, etc)
   */
  path: string;

  /**
   * Optional URL of the remote database (e.g. libsql://db-org.turso.io)
   * If omitted - local-only database will be created
   *
   * You can also provide function which will return URL or null
   * In this case local database will be created and sync will be "switched-on"
   * whenever the url returns non-empty value
   */
  url?: string | (() => string | null);

  /**
   * Auth token for the remote database
   * (can be either static string or function which will provide short-lived credentials)
   */
  authToken?: string | (() => Promise<string>);

  /**
   * Arbitrary client name which can be used to distinguish clients internally
   * The library will guarantee uniqueness of the clientId by appending unique suffix
   */
  clientName?: string;

  /**
   * Optional remote encryption parameters if cloud database was encrypted
   */
  remoteEncryption?: EncryptionOpts;

  /**
   * Optional long-polling timeout for pull operation
   * If not set - no timeout is applied
   */
  longPollTimeoutMs?: number;

  /**
   * Optional parameter to enable internal logging for the database
   */
  tracing?: 'error' | 'warn' | 'info' | 'debug' | 'trace';

  /**
   * Bootstrap database if empty; if set - client will be able to connect
   * to fresh db only when network is online
   */
  bootstrapIfEmpty?: boolean;

  /**
   * Reserved bytes which must be set for the database - necessary if
   * remote encryption is set for the db in cloud
   */
  reservedBytes?: number;

  /**
   * Optional parameter to enable partial sync for the database
   * WARNING: This feature is EXPERIMENTAL
   */
  partialSyncExperimental?: {
    /**
     * Bootstrap strategy configuration
     * - prefix strategy loads first N bytes locally at startup
     * - query strategy loads pages touched by the provided SQL statement
     */
    bootstrapStrategy:
      | { kind: 'prefix'; length: number }
      | { kind: 'query'; query: string };
    /**
     * Optional segment size which makes sync engine load pages in batches
     * (so, if loading page 1 with segment_size=128kb then 32 pages [1..32] will be loaded)
     */
    segmentSize?: number;
    /**
     * Optional parameter which makes sync engine prefetch pages which probably
     * will be accessed soon
     */
    prefetch?: boolean;
  };
}


/**
 * Sync stats returned by stats() operation
 */
export interface SyncStats {
  cdcOperations: number;
  mainWalSize: number;
  revertWalSize: number;
  lastPullUnixTime: number;
  lastPushUnixTime: number;
  networkSentBytes: number;
  networkReceivedBytes: number;
  revision: string | null;
}

/**
 * HTTP request from sync engine
 */
export interface HttpRequest {
  url: string | null;
  method: string;
  path: string;
  headers: Record<string, string>;
  body: ArrayBuffer | null;
}

/**
 * Full write request from sync engine
 */
export interface FullWriteRequest {
  path: string;
  content: ArrayBuffer | null;
}

// ============================================================================
// Global Turso Proxy Interface
// ============================================================================

/**
 * Native proxy interface exposed via JSI
 */
export interface TursoProxy {
  newDatabase(path: string, config?: any): NativeDatabase;
  newSyncDatabase(dbConfig: any, syncConfig: any): NativeSyncDatabase;
  version(): string;
  setup(options: { logLevel?: string }): void;
}

/**
 * Global __TursoProxy object injected by native code
 */
declare global {
  const __TursoProxy: TursoProxy;
}

export {};
