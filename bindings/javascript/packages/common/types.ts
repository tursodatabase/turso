export type ExperimentalFeature = 'views' | 'strict' | 'encryption' | 'index_method' | 'autovacuum' | 'triggers' | 'attach';

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
    tracing?: 'info' | 'debug' | 'trace'
    /** Experimental features to enable */
    experimental?: ExperimentalFeature[]
    /** Optional local encryption configuration */
    encryption?: EncryptionOpts
}

export interface NativeDatabase {
    memory: boolean,
    path: string,
    readonly: boolean;
    open: boolean;
    new(path: string): NativeDatabase;

    connectSync();
    connectAsync(): Promise<void>;

    ioLoopSync();
    ioLoopAsync(): Promise<void>;

    prepare(sql: string): NativeStatement;
    executor(sql: string): NativeExecutor;

    defaultSafeIntegers(toggle: boolean);
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
    type: string
}

export interface NativeExecutor {
    stepSync(): number;
    reset();
}
export interface NativeStatement {
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