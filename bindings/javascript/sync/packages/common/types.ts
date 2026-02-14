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

export interface EncryptionOpts {
    // base64 encoded encryption key (must be either 16 or 32 bytes depending on the cipher)
    key: string,
    // encryption cipher algorithm
    // - aes256gcm, aes128gcm, chacha20poly1305: 28 reserved bytes
    // - aegis128l, aegis128x2, aegis128x4: 32 reserved bytes
    // - aegis256, aegis256x2, aegis256x4: 48 reserved bytes
    cipher: 'aes256gcm' | 'aes128gcm' | 'chacha20poly1305' | 'aegis128l' | 'aegis128x2' | 'aegis128x4' | 'aegis256' | 'aegis256x2' | 'aegis256x4'
}
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
     * 
     * you can also provide function which will return URL or null
     * in this case local database will be created and sync will be "switched-on" whenever the url will return non-empty value
     * note, that all other parameters (like encryption) must be set in advance in order for the "deferred" sync to work properly
     */
    url?: string | (() => string | null);
    /**
     * auth token for the remote database
     * (can be either static string or function which will provide short-lived credentials for every new request)
     */
    authToken?: string | (() => Promise<string>);
    /**
     * arbitrary client name which can be used to distinguish clients internally
     * the library will gurantee uniquiness of the clientId by appending unique suffix to the clientName
     */
    clientName?: string;
    /**
     * optional remote encryption parameters if cloud database were encrypted by default
     */
    remoteEncryption?: EncryptionOpts;
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
    /**
     * optional parameter to enable partial sync for the database
     * WARNING: This feature is EXPERIMENTAL
     */
    partialSyncExperimental?: {
        /* bootstrap strategy configuration
            - prefix strategy loads first N bytes locally at the startup
            - query strategy loads pages touched by the provided SQL statement
        */
        bootstrapStrategy: { kind: 'prefix', length: number } | { kind: 'query', query: string },
        /* optional segment size which makes sync engine to load pages in batches of segment_size bytes
            (so, if loading page 1 with segment_size=128kb then 32 pages [1..32] will be loaded)
        */
        segmentSize?: number,
        /* optional parameter which makes sync engine to prefetch pages which probably will be accessed soon */
        prefetch?: boolean,
    }
}
export interface DatabaseStats {
    /**
     * amount of local changes not sent to the remote
     */
    cdcOperations: number;
    /**
     * size of the main WAL file in bytes
     */
    mainWalSize: number;
    /**
     * size of the revert WAL file in bytes
     */
    revertWalSize: number;
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
    /**
     * total amount of sent bytes over the network
     */
    networkSentBytes: number;
    /**
     * total amount of received bytes over the network
     */
    networkReceivedBytes: number;
}

/* internal types used in the native/browser packages */

export interface RunOpts {
    preemptionMs: number,
    url: string | (() => string | null),
    headers: { [K: string]: string } | (() => Promise<{ [K: string]: string }>)
    transform?: Transform,
}

export interface ProtocolIo {
    read(path: string): Promise<Buffer | Uint8Array | null>;
    write(path: string, content: Buffer | Uint8Array): Promise<void>;
}
export type GeneratorResponse = { type: 'IO' } | { type: 'Done' } | ({ type: 'SyncEngineStats' } & DatabaseStats) | { type: 'SyncEngineChanges', changes: any }
