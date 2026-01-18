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
 * Configuration for local database
 */
export interface LocalDatabaseConfig {
  path: string;
  vfs?: string;
  experimentalFeatures?: string;
  encryptionCipher?: string;
  encryptionHexkey?: string;
}

/**
 * Configuration for sync database (embedded replica)
 */
export interface SyncDatabaseConfig {
  path: string;
  remoteUrl?: string;
  authToken?: string;  // Will be added to HTTP headers
  clientName?: string;
  longPollTimeoutMs?: number;
  bootstrapIfEmpty?: boolean;
  reservedBytes?: number;

  // Partial sync options
  partialSync?: {
    bootstrapStrategyPrefix?: number;
    bootstrapStrategyQuery?: string;
    segmentSize?: number;
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
