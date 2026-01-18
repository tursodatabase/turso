/**
 * Database
 *
 * Unified high-level API for both local and sync databases.
 * Constructor determines whether to use local-only or sync mode based on config.
 */

import { Statement } from './Statement';
import {
  NativeDatabase,
  NativeSyncDatabase,
  NativeConnection,
  Row,
  RunResult,
  SQLiteValue,
  BindParams,
  SyncDatabaseConfig,
  LocalDatabaseConfig,
  SyncStats,
} from './types';
import {
  driveVoidOperation,
  driveConnectionOperation,
  driveChangesOperation,
  driveStatsOperation,
} from './internal/asyncOperation';

/**
 * Database constructor config - either string path (local) or object (sync)
 */
export type DatabaseConfig = string | LocalDatabaseConfig | SyncDatabaseConfig;

/**
 * Check if config has sync properties
 */
function isSyncConfig(config: any): config is SyncDatabaseConfig {
  return (
    typeof config === 'object' &&
    config !== null &&
    ('remoteUrl' in config || 'authToken' in config)
  );
}

/**
 * Database class - works for both local-only and sync databases
 */
export class Database {
  private _nativeDb: NativeDatabase | null = null;
  private _nativeSyncDb: NativeSyncDatabase | null = null;
  private _connection: NativeConnection | null = null;
  private _isSync = false;
  private _path: string;
  private _closed = false;

  /**
   * Create a new database
   *
   * @param config - Path string (local) or config object (local/sync)
   */
  constructor(config: DatabaseConfig) {
    // Determine if sync or local
    const isSync = typeof config === 'object' && isSyncConfig(config);
    this._isSync = isSync;

    if (isSync) {
      // Sync database
      this._path = config.path;
      this.initSyncDatabase(config);
    } else {
      // Local database
      this._path = typeof config === 'string' ? config : config.path;
      this.initLocalDatabase(typeof config === 'string' ? config : config);
    }
  }

  /**
   * Initialize local-only database
   */
  private initLocalDatabase(config: string | LocalDatabaseConfig): void {
    const path = typeof config === 'string' ? config : config.path;

    // Create database through __TursoProxy
    if (typeof __TursoProxy === 'undefined') {
      throw new Error('Turso native module not loaded');
    }

    const dbConfig = typeof config === 'object' ? config : { path };

    // Create native database
    this._nativeDb = __TursoProxy.newDatabase(path, dbConfig);

    // Open database
    this._nativeDb.open();

    // Get connection
    this._connection = this._nativeDb.connect();
  }

  /**
   * Initialize sync database (async initialization handled by open/create)
   */
  private initSyncDatabase(config: SyncDatabaseConfig): void {
    // Create database through __TursoProxy
    if (typeof __TursoProxy === 'undefined') {
      throw new Error('Turso native module not loaded');
    }

    // Build dbConfig
    const dbConfig = {
      path: config.path,
    };

    // Build syncConfig with all options
    const syncConfig: any = {
      remoteUrl: config.remoteUrl,
      clientName: config.clientName,
      longPollTimeoutMs: config.longPollTimeoutMs,
      bootstrapIfEmpty: config.bootstrapIfEmpty,
      reservedBytes: config.reservedBytes,
    };

    // Add partial sync options if present
    if (config.partialSync) {
      syncConfig.partialBootstrapStrategyPrefix = config.partialSync.bootstrapStrategyPrefix;
      syncConfig.partialBootstrapStrategyQuery = config.partialSync.bootstrapStrategyQuery;
      syncConfig.partialBootstrapSegmentSize = config.partialSync.segmentSize;
      syncConfig.partialBootstrapPrefetch = config.partialSync.prefetch;
    }

    // Create native sync database
    this._nativeSyncDb = __TursoProxy.newSyncDatabase(dbConfig, syncConfig);

    // Note: Database is not open yet - user must call open() or create()
    // This is async and will be handled by the open() method
  }

  /**
   * Open sync database (async)
   * Only needed for sync databases - local databases auto-open in constructor
   */
  async open(): Promise<void> {
    if (!this._isSync) {
      throw new Error('open() is only for sync databases. Local databases open automatically.');
    }

    if (!this._nativeSyncDb) {
      throw new Error('Sync database not initialized');
    }

    // Drive open operation
    const operation = this._nativeSyncDb.open();
    await driveVoidOperation(operation, this._nativeSyncDb);

    // Get connection
    const connOperation = this._nativeSyncDb.connect();
    this._connection = await driveConnectionOperation(connOperation, this._nativeSyncDb);
  }

  /**
   * Create or open sync database (async)
   * Only for sync databases
   */
  async create(): Promise<void> {
    if (!this._isSync) {
      throw new Error('create() is only for sync databases.');
    }

    if (!this._nativeSyncDb) {
      throw new Error('Sync database not initialized');
    }

    // Drive create operation
    const operation = this._nativeSyncDb.create();
    await driveVoidOperation(operation, this._nativeSyncDb);

    // Get connection
    const connOperation = this._nativeSyncDb.connect();
    this._connection = await driveConnectionOperation(connOperation, this._nativeSyncDb);
  }

  /**
   * Prepare a SQL statement
   *
   * @param sql - SQL statement to prepare
   * @returns Prepared statement
   */
  prepare(sql: string): Statement {
    this.checkOpen();

    if (!this._connection) {
      throw new Error('No connection available');
    }

    const nativeStmt = this._connection.prepareSingle(sql);
    return new Statement(nativeStmt);
  }

  /**
   * Execute SQL without returning results (for DDL, multi-statement SQL)
   *
   * @param sql - SQL to execute
   */
  exec(sql: string): void {
    this.checkOpen();

    if (!this._connection) {
      throw new Error('No connection available');
    }

    // Use prepareFirst to handle multiple statements
    let remaining = sql.trim();

    while (remaining.length > 0) {
      const result = this._connection.prepareFirst(remaining);

      if (!result.statement) {
        break; // No more statements
      }

      // Execute statement
      const execResult = result.statement.execute();
      if (execResult.status !== 1 /* TURSO_DONE */) {
        throw new Error(`Execution failed with status: ${execResult.status}`);
      }

      // Move to next statement
      remaining = sql.substring(result.tailIdx).trim();
    }
  }

  /**
   * Execute statement and return result info
   *
   * @param sql - SQL statement
   * @param params - Bind parameters
   * @returns Run result with changes and lastInsertRowid
   */
  run(sql: string, ...params: BindParams[]): RunResult {
    const stmt = this.prepare(sql);
    try {
      return stmt.run(...params);
    } finally {
      stmt.finalize();
    }
  }

  /**
   * Execute query and return first row
   *
   * @param sql - SQL query
   * @param params - Bind parameters
   * @returns First row or undefined
   */
  get(sql: string, ...params: BindParams[]): Row | undefined {
    const stmt = this.prepare(sql);
    try {
      return stmt.get(...params);
    } finally {
      stmt.finalize();
    }
  }

  /**
   * Execute query and return all rows
   *
   * @param sql - SQL query
   * @param params - Bind parameters
   * @returns All rows
   */
  all(sql: string, ...params: BindParams[]): Row[] {
    const stmt = this.prepare(sql);
    try {
      return stmt.all(...params);
    } finally {
      stmt.finalize();
    }
  }

  /**
   * Execute function within a transaction
   *
   * @param fn - Function to execute
   * @returns Function result
   */
  transaction<T>(fn: () => T): T {
    this.checkOpen();
    this.exec('BEGIN');
    try {
      const result = fn();
      this.exec('COMMIT');
      return result;
    } catch (error) {
      this.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Push local changes to remote (sync databases only)
   */
  async push(): Promise<void> {
    if (!this._isSync || !this._nativeSyncDb) {
      throw new Error('push() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.pushChanges();
    await driveVoidOperation(operation, this._nativeSyncDb);
  }

  /**
   * Pull remote changes and apply locally (sync databases only)
   *
   * @returns true if changes were applied, false if no changes
   */
  async pull(): Promise<boolean> {
    if (!this._isSync || !this._nativeSyncDb) {
      throw new Error('pull() is only available for sync databases');
    }

    // Wait for changes
    const waitOperation = this._nativeSyncDb.waitChanges();
    const changes = await driveChangesOperation(waitOperation, this._nativeSyncDb);

    // If no changes, return false
    if (!changes) {
      return false;
    }

    // Apply changes
    const applyOperation = this._nativeSyncDb.applyChanges(changes);
    await driveVoidOperation(applyOperation, this._nativeSyncDb);

    return true;
  }

  /**
   * Get sync statistics (sync databases only)
   *
   * @returns Sync stats
   */
  async stats(): Promise<SyncStats> {
    if (!this._isSync || !this._nativeSyncDb) {
      throw new Error('stats() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.stats();
    return driveStatsOperation(operation, this._nativeSyncDb);
  }

  /**
   * Checkpoint database (sync databases only)
   */
  async checkpoint(): Promise<void> {
    if (!this._isSync || !this._nativeSyncDb) {
      throw new Error('checkpoint() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.checkpoint();
    await driveVoidOperation(operation, this._nativeSyncDb);
  }

  /**
   * Close the database
   */
  close(): void {
    if (this._closed) {
      return;
    }

    if (this._connection) {
      this._connection.close();
      this._connection = null;
    }

    if (this._nativeDb) {
      this._nativeDb.close();
      this._nativeDb = null;
    }

    if (this._nativeSyncDb) {
      this._nativeSyncDb.close();
      this._nativeSyncDb = null;
    }

    this._closed = true;
  }

  /**
   * Get database path
   */
  get path(): string {
    return this._path;
  }

  /**
   * Check if database is a sync database
   */
  get isSync(): boolean {
    return this._isSync;
  }

  /**
   * Check if database is open
   */
  get open(): boolean {
    return !this._closed && this._connection !== null;
  }

  /**
   * Check if in transaction
   */
  get inTransaction(): boolean {
    if (!this._connection) {
      return false;
    }
    return !this._connection.getAutocommit();
  }

  /**
   * Get last insert rowid
   */
  get lastInsertRowid(): number {
    if (!this._connection) {
      return 0;
    }
    return this._connection.lastInsertRowid();
  }

  /**
   * Check if open and throw if not
   */
  private checkOpen(): void {
    if (this._closed) {
      throw new Error('Database is closed');
    }
    if (!this._connection) {
      throw new Error('Database not connected. For sync databases, call open() or create() first.');
    }
  }
}
