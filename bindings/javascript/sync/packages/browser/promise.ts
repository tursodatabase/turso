import { registerFileAtWorker, unregisterFileAtWorker } from "@tursodatabase/database-browser-common"
import { DatabasePromise, DatabaseOpts, NativeDatabase } from "@tursodatabase/database-common"
import { ProtocolIo, run, SyncOpts, RunOpts, memoryIO } from "@tursodatabase/sync-common";

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
    worker: Worker | null;
    fsPath: string | null;
    constructor(db: NativeDatabase, io: ProtocolIo, worker: Worker | null, runOpts: RunOpts, engine: any, fsPath: string | null, opts: DatabaseOpts = {}) {
        super(db, opts)
        this.io = io;
        this.worker = worker;
        this.runOpts = runOpts;
        this.engine = engine;
        this.fsPath = fsPath;
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
        if (this.fsPath != null && this.worker != null) {
            await Promise.all([
                unregisterFileAtWorker(this.worker, this.fsPath),
                unregisterFileAtWorker(this.worker, `${this.fsPath}-wal`),
                unregisterFileAtWorker(this.worker, `${this.fsPath}-revert`),
                unregisterFileAtWorker(this.worker, `${this.fsPath}-info`),
                unregisterFileAtWorker(this.worker, `${this.fsPath}-changes`),
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
async function connect(opts: SyncOpts, connect: (any) => any, init: () => Promise<Worker>): Promise<Database> {
    const engine = connect({
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

    const worker = await init();
    if (!isMemory) {
        await Promise.all([
            registerFileAtWorker(worker, opts.path),
            registerFileAtWorker(worker, `${opts.path}-wal`),
            registerFileAtWorker(worker, `${opts.path}-revert`),
            registerFileAtWorker(worker, `${opts.path}-info`),
            registerFileAtWorker(worker, `${opts.path}-changes`),
        ]);
    }
    await run(runOpts, io, engine, engine.init());
    const nativeDb = engine.open();
    return new Database(nativeDb as any, io, worker, runOpts, engine, isMemory ? null : opts.path, {});
}

export { connect, Database }
