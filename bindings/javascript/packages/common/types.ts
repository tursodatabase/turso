export interface DatabaseOpts {
    readonly?: boolean,
    fileMustExist?: boolean,
    timeout?: number
    name?: string
    tracing?: 'info' | 'debug' | 'trace'
}

export interface NativeDatabase {
    memory: boolean,
    path: string,
    new(path: string): NativeDatabase;
    batchSync(sql: string);
    batchAsync(sql: string): Promise<void>;

    ioLoopSync();
    ioLoopAsync(): Promise<void>;

    prepare(sql: string): NativeStatement;

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