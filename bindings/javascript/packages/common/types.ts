export type ExperimentalFeature = 'views' | 'strict' | 'encryption' | 'index_method' | 'autovacuum' | 'triggers' | 'attach';

export interface DatabaseOpts {
    readonly?: boolean,
    fileMustExist?: boolean,
    timeout?: number
    tracing?: 'info' | 'debug' | 'trace'
    /** Experimental features to enable */
    experimental?: ExperimentalFeature[]
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