export declare const enum DatabaseChangeType {
    Insert = 0,
    Update = 1,
    Delete = 2
}

export interface DatabaseRowMutation {
    changeTime: number
    tableName: string
    id: number
    changeType: DatabaseChangeType
    before?: Record<string, any>
    after?: Record<string, any>
    updates?: Record<string, any>
}

export type DatabaseRowTransformResult = { operation: 'skip' } | { operation: 'rewrite', stmt: DatabaseRowStatement } | null;
export type Transform = (arg: DatabaseRowMutation) => DatabaseRowTransformResult;
export interface RunOpts {
    preemptionMs: number,
    url: string,
    headers: { [K: string]: string }
    transform?: Transform,
}

export interface ProtocolIo {
    read(path: string): Promise<Buffer | Uint8Array | null>;
    write(path: string, content: Buffer | Uint8Array): Promise<void>;
}

export interface SyncOpts {
    path: string;
    clientName?: string;
    url: string;
    authToken?: string;
    encryptionKey?: string;
    tablesIgnore?: string[],
    transform?: Transform,
    tracing?: string,
}

export interface DatabaseRowStatement {
    sql: string
    values: Array<any>
}

export type GeneratorResponse =
    | { type: 'IO' }
    | { type: 'Done' }
    | { type: 'SyncEngineStats', operations: number, mainWal: number, revertWal: number, lastPullUnixTime: number, lastPushUnixTime: number | null }