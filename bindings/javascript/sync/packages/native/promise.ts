import { DatabasePromise } from "@tursodatabase/database-common"
import { ProtocolIo, run, DatabaseOpts as SyncDatabaseOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, DatabaseStats, SyncEngineGuards } from "@tursodatabase/sync-common";
import { SyncEngine, SyncEngineProtocolVersion } from "#index";
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
    #runOpts: RunOpts;
    #engine: any;
    #io: ProtocolIo;
    #guards: SyncEngineGuards
    constructor(opts: SyncDatabaseOpts) {
        const engine = new SyncEngine({
            path: opts.path,
            clientName: opts.clientName,
            useTransform: opts.transform != null,
            protocolVersion: SyncEngineProtocolVersion.V1,
            longPollTimeoutMs: opts.longPollTimeoutMs,
            tracing: opts.tracing,
        });
        super(engine.db() as unknown as any);


        let headers = typeof opts.authToken === "function" ? () => ({
            ...(opts.authToken != null && { "Authorization": `Bearer ${(opts.authToken as any)()}` }),
            ...(opts.encryptionKey != null && { "x-turso-encryption-key": opts.encryptionKey })
        }) : {
            ...(opts.authToken != null && { "Authorization": `Bearer ${opts.authToken}` }),
            ...(opts.encryptionKey != null && { "x-turso-encryption-key": opts.encryptionKey })
        };
        this.#runOpts = {
            url: opts.url,
            headers: headers,
            preemptionMs: 1,
            transform: opts.transform,
        };
        this.#engine = engine;
        this.#io = this.memory ? memoryIO() : NodeIO;
        this.#guards = new SyncEngineGuards();
    }
    /**
     * connect database and initialize it in case of clean start
     */
    override async connect() {
        await run(this.#runOpts, this.#io, this.#engine, this.#engine.connect());
    }
    /**
     * pull new changes from the remote database
     * if {@link SyncDatabaseOpts.longPollTimeoutMs} is set - then server will hold the connection open until either new changes will appear in the database or timeout occurs.
     * @returns true if new changes were pulled from the remote
     */
    async pull() {
        const changes = await this.#guards.wait(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.wait()));
        if (changes.empty()) {
            return false;
        }
        await this.#guards.apply(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.apply(changes)));
        return true;
    }
    /**
     * push new local changes to the remote database
     * if {@link SyncDatabaseOpts.transform} is set - then provided callback will be called for every mutation before sending it to the remote
     */
    async push() {
        await this.#guards.push(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.push()));
    }
    /**
     * checkpoint WAL for local database
     */
    async checkpoint() {
        await this.#guards.checkpoint(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.checkpoint()));
    }
    /**
     * @returns statistic of current local database
     */
    async stats(): Promise<DatabaseStats> {
        return (await run(this.#runOpts, this.#io, this.#engine, this.#engine.stats()));
    }
    /**
     * close the database
     */
    override async close(): Promise<void> {
        this.#engine.close();
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(opts: SyncDatabaseOpts): Promise<Database> {
    const db = new Database(opts);
    await db.connect();
    return db;
}

export { connect, Database, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
