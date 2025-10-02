import { DatabasePromise } from "@tursodatabase/database-common"
import { ProtocolIo, run, DatabaseOpts, EncryptionOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, DatabaseStats, SyncEngineGuards } from "@tursodatabase/sync-common";
import { SyncEngine, SyncEngineProtocolVersion, Database as NativeDatabase } from "#index";
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
    constructor(opts: DatabaseOpts) {
        if (opts.url == null) {
            super(new NativeDatabase(opts.path, { tracing: opts.tracing }) as any);
            this.#engine = null;
            return;
        }

        const engine = new SyncEngine({
            path: opts.path,
            clientName: opts.clientName,
            useTransform: opts.transform != null,
            protocolVersion: SyncEngineProtocolVersion.V1,
            longPollTimeoutMs: opts.longPollTimeoutMs,
            tracing: opts.tracing,
            bootstrapIfEmpty: typeof opts.url != "function" || opts.url() != null,
            remoteEncryption: opts.remoteEncryption?.cipher,
        });
        super(engine.db() as unknown as any);

        let headers: { [K: string]: string } | (() => Promise<{ [K: string]: string }>);
        if (typeof opts.authToken == "function") {
            const authToken = opts.authToken;
            headers = async () => ({
                ...(opts.authToken != null && { "Authorization": `Bearer ${await authToken()}` }),
                ...(opts.remoteEncryption != null && {
                    "x-turso-encryption-key": opts.remoteEncryption.key,
                    "x-turso-encryption-cipher": opts.remoteEncryption.cipher,
                })
            });
        } else {
            const authToken = opts.authToken;
            headers = {
                ...(opts.authToken != null && { "Authorization": `Bearer ${authToken}` }),
                ...(opts.remoteEncryption != null && {
                    "x-turso-encryption-key": opts.remoteEncryption.key,
                    "x-turso-encryption-cipher": opts.remoteEncryption.cipher,
                })
            };
        }
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
        if (this.connected) {
            return;
        } else if (this.#engine == null) {
            await super.connect();
        } else {
            await run(this.#runOpts, this.#io, this.#engine, this.#engine.connect());
        }
        this.connected = true;
    }
    /**
     * pull new changes from the remote database
     * if {@link DatabaseOpts.longPollTimeoutMs} is set - then server will hold the connection open until either new changes will appear in the database or timeout occurs.
     * @returns true if new changes were pulled from the remote
     */
    async pull() {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        const changes = await this.#guards.wait(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.wait()));
        if (changes.empty()) {
            return false;
        }
        await this.#guards.apply(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.apply(changes)));
        return true;
    }
    /**
     * push new local changes to the remote database
     * if {@link DatabaseOpts.transform} is set - then provided callback will be called for every mutation before sending it to the remote
     */
    async push() {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        await this.#guards.push(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.push()));
    }
    /**
     * checkpoint WAL for local database
     */
    async checkpoint() {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        await this.#guards.checkpoint(async () => await run(this.#runOpts, this.#io, this.#engine, this.#engine.checkpoint()));
    }
    /**
     * @returns statistic of current local database
     */
    async stats(): Promise<DatabaseStats> {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        return (await run(this.#runOpts, this.#io, this.#engine, this.#engine.stats()));
    }
    /**
     * close the database
     */
    override async close(): Promise<void> {
        await super.close();
        if (this.#engine != null) {
            this.#engine.close();
        }
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(opts: DatabaseOpts): Promise<Database> {
    const db = new Database(opts);
    await db.connect();
    return db;
}

export { connect, Database }
export type { DatabaseOpts, EncryptionOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }