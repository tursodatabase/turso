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
  DatabaseOpts,
  SyncStats,
} from './types';
import {
  driveVoidOperation,
  driveConnectionOperation,
  driveChangesOperation,
  driveStatsOperation,
} from './internal/asyncOperation';
import { drainSyncIo } from './internal/ioProcessor';

/**
 * Check if config has sync properties (url field)
 */
function isSyncConfig(opts: DatabaseOpts): boolean {
  return opts.url !== undefined && opts.url !== null;
}

/**
 * Database class - works for both local-only and sync databases
 *
 * All database operations are async to properly handle IO requirements:
 * - For local databases: async allows yielding to JS event loop
 * - For sync databases: async required for network operations
 * - For partial sync: async required to load missing pages on-demand
 */
export class Database {
  private _opts: DatabaseOpts;
  private _nativeDb: NativeDatabase | null = null;
  private _nativeSyncDb: NativeSyncDatabase | null = null;
  private _connection: NativeConnection | null = null;
  private _isSync = false;
  private _connected = false;
  private _closed = false;
  private _extraIo?: () => Promise<void>;
  private _ioContext?: { authToken?: string | (() => string | null); baseUrl?: string };

  /**
   * Create a new database (doesn't connect yet - call connect())
   *
   * @param opts - Database options
   */
  constructor(opts: DatabaseOpts) {
    this._opts = opts;
    this._isSync = isSyncConfig(opts);
  }

  /**
   * Connect to the database (matches JavaScript bindings)
   * For local databases: opens immediately
   * For sync databases: bootstraps if needed
   */
  async connect(): Promise<void> {
    if (this._connected) {
      return;
    }

    if (this._isSync) {
      await this.initSyncDatabase();
    } else {
      this.initLocalDatabase();
    }

    this._connected = true;
  }

  /**
   * Initialize local-only database
   */
  private initLocalDatabase(): void {
    if (typeof __TursoProxy === 'undefined') {
      throw new Error('Turso native module not loaded');
    }

    const dbConfig = {
      path: this._opts.path,
      async_io: true, // Always use async IO in React Native
    };

    // Create native database (path normalization happens in C++ JSI layer)
    this._nativeDb = __TursoProxy.newDatabase(this._opts.path, dbConfig);

    // Open database
    this._nativeDb.open();

    // Get connection
    this._connection = this._nativeDb.connect();
  }

  /**
   * Initialize sync database
   */
  private async initSyncDatabase(): Promise<void> {
    if (typeof __TursoProxy === 'undefined') {
      throw new Error('Turso native module not loaded');
    }

    // Get URL (can be string or function)
    let url: string | null = null;
    if (typeof this._opts.url === 'function') {
      url = this._opts.url();
    } else if (typeof this._opts.url === 'string') {
      url = this._opts.url;
    }

    if (!url) {
      throw new Error('Sync database requires a URL');
    }

    // Build dbConfig (path normalization happens in C++ JSI layer)
    const dbConfig = {
      path: this._opts.path,
      async_io: true, // Always use async IO in React Native
    };

    // Build syncConfig with all options
    const syncConfig: any = {
      remoteUrl: url,
      clientName: this._opts.clientName || 'turso-sync-react-native',
      longPollTimeoutMs: this._opts.longPollTimeoutMs,
      bootstrapIfEmpty: this._opts.bootstrapIfEmpty ?? true,
      reservedBytes: this._opts.reservedBytes,
    };

    // Add partial sync options if present
    if (this._opts.partialSyncExperimental) {
      const partial = this._opts.partialSyncExperimental;
      if (partial.bootstrapStrategy.kind === 'prefix') {
        syncConfig.partialBootstrapStrategyPrefix = partial.bootstrapStrategy.length;
      } else if (partial.bootstrapStrategy.kind === 'query') {
        syncConfig.partialBootstrapStrategyQuery = partial.bootstrapStrategy.query;
      }
      syncConfig.partialBootstrapSegmentSize = partial.segmentSize;
      syncConfig.partialBootstrapPrefetch = partial.prefetch;
    }

    // Create native sync database
    this._nativeSyncDb = __TursoProxy.newSyncDatabase(dbConfig, syncConfig);

    // Create IO context with auth token and base URL
    this._ioContext = {
      authToken: this._opts.authToken,
      baseUrl: url,
    };

    // Create extraIo callback for partial sync support
    // This callback drains the sync engine's IO queue during statement execution
    this._extraIo = async () => {
      if (this._nativeSyncDb && this._ioContext) {
        await drainSyncIo(this._nativeSyncDb, this._ioContext);
      }
    };

    // Bootstrap/open database
    const operation = this._nativeSyncDb.create();
    await driveVoidOperation(operation, this._nativeSyncDb, this._ioContext);

    // Get connection
    const connOperation = this._nativeSyncDb.connect();
    this._connection = await driveConnectionOperation(connOperation, this._nativeSyncDb, this._ioContext);
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
    // Pass extraIo callback for partial sync support
    return new Statement(nativeStmt, this._extraIo);
  }

  /**
   * Execute SQL without returning results (for DDL, multi-statement SQL)
   *
   * @param sql - SQL to execute
   */
  async exec(sql: string): Promise<void> {
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

      // Wrap in Statement to get IO handling
      const stmt = new Statement(result.statement, this._extraIo);
      try {
        // Execute - will handle IO if needed
        await stmt.run();
      } finally {
        stmt.finalize();
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
  async run(sql: string, ...params: BindParams[]): Promise<RunResult> {
    const stmt = this.prepare(sql);
    try {
      return await stmt.run(...params);
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
  async get(sql: string, ...params: BindParams[]): Promise<Row | undefined> {
    const stmt = this.prepare(sql);
    try {
      return await stmt.get(...params);
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
  async all(sql: string, ...params: BindParams[]): Promise<Row[]> {
    const stmt = this.prepare(sql);
    try {
      return await stmt.all(...params);
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
  async transaction<T>(fn: () => T | Promise<T>): Promise<T> {
    this.checkOpen();
    await this.exec('BEGIN');
    try {
      const result = await fn();
      await this.exec('COMMIT');
      return result;
    } catch (error) {
      await this.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Push local changes to remote (sync databases only)
   */
  async push(): Promise<void> {
    if (!this._isSync || !this._nativeSyncDb || !this._ioContext) {
      throw new Error('push() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.pushChanges();
    await driveVoidOperation(operation, this._nativeSyncDb, this._ioContext);
  }

  /**
   * Pull remote changes and apply locally (sync databases only)
   *
   * @returns true if changes were applied, false if no changes
   */
  async pull(): Promise<boolean> {
    if (!this._isSync || !this._nativeSyncDb || !this._ioContext) {
      throw new Error('pull() is only available for sync databases');
    }

    // Wait for changes
    const waitOperation = this._nativeSyncDb.waitChanges();
    const changes = await driveChangesOperation(waitOperation, this._nativeSyncDb, this._ioContext);

    // If no changes, return false
    if (!changes) {
      return false;
    }

    // Apply changes
    const applyOperation = this._nativeSyncDb.applyChanges(changes);
    await driveVoidOperation(applyOperation, this._nativeSyncDb, this._ioContext);

    return true;
  }

  /**
   * Get sync statistics (sync databases only)
   *
   * @returns Sync stats
   */
  async stats(): Promise<SyncStats> {
    if (!this._isSync || !this._nativeSyncDb || !this._ioContext) {
      throw new Error('stats() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.stats();
    return driveStatsOperation(operation, this._nativeSyncDb, this._ioContext);
  }

  /**
   * Checkpoint database (sync databases only)
   */
  async checkpoint(): Promise<void> {
    if (!this._isSync || !this._nativeSyncDb || !this._ioContext) {
      throw new Error('checkpoint() is only available for sync databases');
    }

    const operation = this._nativeSyncDb.checkpoint();
    await driveVoidOperation(operation, this._nativeSyncDb, this._ioContext);
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

    this._connected = false;
    this._closed = true;
  }

  /**
   * Get database path
   */
  get path(): string {
    return this._opts.path;
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
    if (!this._connected || !this._connection) {
      throw new Error('Database not connected. Call connect() first.');
    }
  }
}
