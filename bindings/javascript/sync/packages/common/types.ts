export declare const enum DatabaseChangeType {
    Insert = 'insert',
    Update = 'update',
    Delete = 'delete'
}

export interface DatabaseRowStatement {
    /**
     * SQL statements with positional placeholders (?)
     */
    sql: string
    /**
     * values to substitute placeholders
     */
    values: Array<any>
}


/**
 * transformation result:
 * - skip: ignore the mutation completely and do not apply it
 * - rewrite: replace mutation with the provided statement
 * - null: do not change mutation and keep it as is
 */
export type DatabaseRowTransformResult = { operation: 'skip' } | { operation: 'rewrite', stmt: DatabaseRowStatement } | null;

export interface DatabaseRowMutation {
    /**
     * unix seconds timestamp of the change
     */
    changeTime: number
    /**
     * table name of the change
     */
    tableName: string
    /**
     * rowid of the change
     */
    id: number
    /**
     * type of the change (insert/delete/update)
     */
    changeType: DatabaseChangeType
    /**
     * columns of the row before the change
     */
    before?: Record<string, any>
    /**
     * columns of the row after the change
     */
    after?: Record<string, any>
    /**
     * only updated columns of the row after the change
     */
    updates?: Record<string, any>
}

export type Transform = (arg: DatabaseRowMutation) => DatabaseRowTransformResult;
export interface DatabaseOpts {
    /**
     * local path where to store all synced database files (e.g. local.db)
     * note, that synced database will write several files with that prefix
     * (e.g. local.db-info, local.db-wal, etc)
     * */
    path: string;
    /**
     * optional url of the remote database (e.g. libsql://db-org.turso.io)
     * (if omitted - local-only database will be created)
     */
    url?: string;
    /**
     * auth token for the remote database
     * (can be either static string or function which will provide short-lived credentials for every new request)
     */
    authToken?: string | (() => string);
    /**
     * arbitrary client name which can be used to distinguish clients internally
     * the library will gurantee uniquiness of the clientId by appending unique suffix to the clientName
     */
    clientName?: string;
    /**
     * optional key if cloud database were encrypted by default
     */
    encryptionKey?: string;
    /**
     * optional callback which will be called for every mutation before sending it to the remote
     * this callback can transform the update in order to support complex conflict resolution strategy
     */
    transform?: Transform,
    /**
     * optional long-polling timeout for pull operation
     * if not set - no timeout is applied
     */
    longPollTimeoutMs?: number,
    /**
     * optional parameter to enable internal logging for the database
     */
    tracing?: 'error' | 'warn' | 'info' | 'debug' | 'trace',
}
export interface DatabaseStats {
    /**
     * amount of local changes not sent to the remote
     */
    operations: number;
    /**
     * size of the main WAL file in bytes
     */
    mainWal: number;
    /**
     * size of the revert WAL file in bytes
     */
    revertWal: number;
    /**
     * unix timestamp of last successful pull time
     */
    lastPullUnixTime: number;
    /**
     * unix timestamp of last successful push time
     */
    lastPushUnixTime: number | null;
    /**
     * opaque revision of the changes pulled locally from remote
     * (can be used as e-tag, but string must not be interpreted in any way and must be used as opaque value)
     */
    revision: string | null;
}

/* internal types used in the native/browser packages */

export interface RunOpts {
    preemptionMs: number,
    url: string,
    headers: { [K: string]: string } | (() => { [K: string]: string })
    transform?: Transform,
}

export interface ProtocolIo {
    read(path: string): Promise<Buffer | Uint8Array | null>;
    write(path: string, content: Buffer | Uint8Array): Promise<void>;
}
export type GeneratorResponse = { type: 'IO' } | { type: 'Done' } | ({ type: 'SyncEngineStats' } & DatabaseStats) | { type: 'SyncEngineChanges', changes: any }