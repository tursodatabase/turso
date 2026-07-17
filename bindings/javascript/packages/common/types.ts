export type ExperimentalFeature = 'views' | 'strict' | 'encryption' | 'index_method' | 'custom_types' | 'autovacuum' | 'vacuum' | 'triggers' | 'attach' | 'generated_columns' | 'multiprocess_wal' | 'without_rowid';

/** Supported encryption ciphers for local database encryption. */
export type EncryptionCipher = 'aes128gcm' | 'aes256gcm' | 'aegis256' | 'aegis256x2' | 'aegis128l' | 'aegis128x2' | 'aegis128x4'

/** Encryption configuration for local encryption. */
export interface EncryptionOpts {
    cipher: EncryptionCipher
    /** The hex-encoded encryption key */
    hexkey: string
}

export interface DatabaseOpts {
    readonly?: boolean,
    fileMustExist?: boolean,
    timeout?: number
    /** Default maximum query execution time in milliseconds before interruption. */
    defaultQueryTimeout?: number
    tracing?: 'info' | 'debug' | 'trace'
    /** Experimental features to enable */
    experimental?: ExperimentalFeature[]
    /** Optional local encryption configuration */
    encryption?: EncryptionOpts
    /**
     * Maximum number of idle extra connections kept alive for `transaction()`
     * calls (default 1). Transactions run on dedicated pooled connections so
     * they never interleave with each other or with queries on the main
     * connection; the pool grows on demand for concurrent transactions and
     * shrinks back to this size when they finish.
     */
    poolSize?: number
}

/**
 * Minimal AsyncLocalStorage-shaped interface used to route statements issued
 * inside a `transaction()` callback to the transaction's pooled connection.
 * Platform packages provide it when the runtime supports async context
 * tracking (Node's AsyncLocalStorage); when absent, `transaction()` falls
 * back to running on the main connection.
 */
export interface TransactionAsyncContext {
    run<T>(store: unknown, fn: () => T): T;
    getStore(): unknown;
}

export interface QueryOptions {
    /** Per-query timeout in milliseconds. Overrides defaultQueryTimeout for this call. */
    queryTimeout?: number
}

/**
 * The shared per-file database state. All statement execution happens on
 * `NativeConnection`s created via the single connect path
 * (`connectSync`/`connectAsync`); the main connection and every pooled
 * transaction connection are created the same way.
 */
export interface NativeDatabase {
    memory: boolean,
    path: string,
    open: boolean;
    new(path: string): NativeDatabase;

    /** Creates a new connection, opening the database on the first call. */
    connectSync(): NativeConnection;
    /** Creates a new connection, opening the database on the first call. */
    connectAsync(): Promise<NativeConnection>;

    ioLoopSync();
    ioLoopAsync(): Promise<void>;

    classifySql?(sql: string): string;
    close();
}

/** A single connection to a database with its own transaction state. */
export interface NativeConnection {
    readonly: boolean;
    open: boolean;

    prepare(sql: string): NativeStatement;
    executor(sql: string, queryOptions?: QueryOptions): NativeExecutor;

    defaultSafeIntegers(toggle: boolean);
    inTransaction(): boolean;
    totalChanges(): number;
    changes(): number;
    lastInsertRowid(): number;
    close();
}


// Step result constants
export const STEP_ROW = 1;
export const STEP_DONE = 2;
export const STEP_IO = 3;

export interface TableColumn {
    name: string,
    type: string | null,
    column: null,
    table: null,
    database: null
}

export interface NativeExecutor {
    stepSync(): number;
    reset();
}
export interface NativeStatement {
    setQueryTimeout(queryOptions?: QueryOptions): void;
    stepAsync(): Promise<number>;
    stepSync(): number;

    pluck(pluckMode: boolean);
    safeIntegers(toggle: boolean);
    raw(toggle: boolean);
    columns(): TableColumn[];
    row(): any;
    reset();
    finalize();
}
