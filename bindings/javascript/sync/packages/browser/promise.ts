import { registerFileAtWorker, unregisterFileAtWorker } from "@tursodatabase/database-browser-common"
import { DatabasePromise, DatabaseOpts, NativeDatabase } from "@tursodatabase/database-common"
import { ProtocolIo, run, SyncOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, memoryIO } from "@tursodatabase/sync-common";
import { connect as nativeConnect, initThreadPool, MainWorker } from "#index";
import { Database as NativeDB, SyncEngine } from "#index";

let BrowserIo: ProtocolIo = {
    async read(path: string): Promise<Buffer | Uint8Array | null> {
        const result = localStorage.getItem(path);
        if (result == null) {
            return null;
        }
        return new TextEncoder().encode(result);
    },
    async write(path: string, data: Buffer | Uint8Array): Promise<void> {
        const array = new Uint8Array(data);
        const value = new TextDecoder('utf-8').decode(array);
        localStorage.setItem(path, value);
    }
};


class Database extends DatabasePromise {
    runOpts: RunOpts;
    engine: any;
    io: ProtocolIo;
    fsPath: string | null;
    constructor(db: NativeDatabase, io: ProtocolIo, runOpts: RunOpts, engine: any, fsPath: string | null, opts: DatabaseOpts = {}) {
        super(db, opts)
        this.runOpts = runOpts;
        this.engine = engine;
        this.fsPath = fsPath;
        this.io = io;
    }
    async sync() {
        await run(this.runOpts, this.io, this.engine, this.engine.sync());
    }
    async pull() {
        await run(this.runOpts, this.io, this.engine, this.engine.pull());
    }
    async push() {
        await run(this.runOpts, this.io, this.engine, this.engine.push());
    }
    async checkpoint() {
        await run(this.runOpts, this.io, this.engine, this.engine.checkpoint());
    }
    async stats(): Promise<{ operations: number, mainWal: number, revertWal: number, lastPullUnixTime: number, lastPushUnixTime: number | null }> {
        return (await run(this.runOpts, this.io, this.engine, this.engine.stats()));
    }
    override async close(): Promise<void> {
        this.db.close();
        this.engine.close();
        if (this.fsPath != null) {
            await Promise.all([
                unregisterFileAtWorker(MainWorker, this.fsPath),
                unregisterFileAtWorker(MainWorker, `${this.fsPath}-wal`),
                unregisterFileAtWorker(MainWorker, `${this.fsPath}-revert`),
                unregisterFileAtWorker(MainWorker, `${this.fsPath}-info`),
                unregisterFileAtWorker(MainWorker, `${this.fsPath}-changes`),
            ]);
        }
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(opts: SyncOpts): Promise<Database> {
    const engine = new SyncEngine({
        path: opts.path,
        clientName: opts.clientName,
        tablesIgnore: opts.tablesIgnore,
        useTransform: opts.transform != null,
        tracing: opts.tracing,
        protocolVersion: 1
    });
    const runOpts: RunOpts = {
        url: opts.url,
        headers: {
            ...(opts.authToken != null && { "Authorization": `Bearer ${opts.authToken}` }),
            ...(opts.encryptionKey != null && { "x-turso-encryption-key": opts.encryptionKey })
        },
        preemptionMs: 1,
        transform: opts.transform,
    };
    const isMemory = opts.path == ':memory:';
    let io = isMemory ? memoryIO() : BrowserIo;

    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not set");
    }
    if (!isMemory) {
        await Promise.all([
            registerFileAtWorker(MainWorker, opts.path),
            registerFileAtWorker(MainWorker, `${opts.path}-wal`),
            registerFileAtWorker(MainWorker, `${opts.path}-revert`),
            registerFileAtWorker(MainWorker, `${opts.path}-info`),
            registerFileAtWorker(MainWorker, `${opts.path}-changes`),
        ]);
    }
    await run(runOpts, io, engine, engine.init());

    const nativeDb = engine.open();
    return new Database(nativeDb as any, io, runOpts, engine, isMemory ? null : opts.path, {});
}

export { connect, Database, }
export type { DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
