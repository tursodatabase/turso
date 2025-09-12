import { DatabasePromise, DatabaseOpts, NativeDatabase } from "@tursodatabase/database-common"
import { ProtocolIo, run, SyncOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult } from "@tursodatabase/sync-common";
import { Database as NativeDB, SyncEngine } from "#index";
import { promises } from "node:fs";

let NodeIO: ProtocolIo = {
    async read(path: string): Promise<Buffer | Uint8Array | null> {
        try {
            return await promises.readFile(path);
        } catch (error) {
            if (error.code === 'ENOENT') {
                return null;
            }
            throw error;
        }
    },
    async write(path: string, data: Buffer | Uint8Array): Promise<void> {
        const unix = Math.floor(Date.now() / 1000);
        const nonce = Math.floor(Math.random() * 1000000000);
        const tmp = `${path}.tmp.${unix}.${nonce}`;
        await promises.writeFile(tmp, new Uint8Array(data));
        try {
            await promises.rename(tmp, path);
        } catch (err) {
            await promises.unlink(tmp);
            throw err;
        }
    }
};

function memoryIO(): ProtocolIo {
    let values = new Map();
    return {
        async read(path: string): Promise<Buffer | Uint8Array | null> {
            return values.get(path);
        },
        async write(path: string, data: Buffer | Uint8Array): Promise<void> {
            values.set(path, data);
        }
    }
};
class Database extends DatabasePromise {
    runOpts: RunOpts;
    engine: any;
    io: ProtocolIo;
    constructor(db: NativeDatabase, io: ProtocolIo, runOpts: RunOpts, engine: any, opts: DatabaseOpts = {}) {
        super(db, opts)
        this.runOpts = runOpts;
        this.engine = engine;
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
        this.engine.close();
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
    let io = opts.path == ':memory:' ? memoryIO() : NodeIO;
    await run(runOpts, io, engine, engine.init());

    const nativeDb = engine.open();
    return new Database(nativeDb as any, io, runOpts, engine, {});
}

export { connect, Database, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
