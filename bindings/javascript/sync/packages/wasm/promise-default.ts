import { registerFileAtWorker, unregisterFileAtWorker, ioNotifier } from "@tursodatabase/database-wasm-common"
import { DatabasePromise } from "@tursodatabase/database-common"
import { ProtocolIo, run, DatabaseOpts, EncryptionOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, DatabaseStats, SyncEngineGuards, Runner, runner, RemoteWriter, RemoteWriteStatement } from "@tursodatabase/sync-common";
import { SyncEngine, SyncEngineProtocolVersion, initThreadPool, MainWorker, Database as NativeDatabase } from "./index-default.js";

let BrowserIO: ProtocolIo = {
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

async function init(): Promise<Worker> {
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not initialized");
    }
    return MainWorker;
}

function resolveUrl(url: string | (() => string | null)): string {
    if (typeof url === "function") {
        const resolved = url();
        if (resolved == null) {
            throw new Error("remoteWritesExperimental requires a non-null URL");
        }
        return resolved;
    }
    return url;
}

class Database extends DatabasePromise {
    #runner: Runner;
    #engine: any;
    #io: ProtocolIo;
    #guards: SyncEngineGuards;
    #worker: Worker | null;
    #remoteWriter: RemoteWriter | null = null;
    #db: any;
    constructor(opts: DatabaseOpts) {
        if (opts.url == null) {
            const db = new NativeDatabase(opts.path, { tracing: opts.tracing }) as any;
            super(
                db,
                () => ioNotifier.waitForCompletion(),
            );
            this.#db = db;
            this.#engine = null;
            return;
        }

        let partialSyncOpts = undefined;
        if (opts.partialSyncExperimental != null) {
            switch (opts.partialSyncExperimental.bootstrapStrategy.kind) {
                case "prefix":
                    partialSyncOpts = {
                        bootstrapStrategy: { type: "Prefix", length: opts.partialSyncExperimental.bootstrapStrategy.length },
                        segmentSize: opts.partialSyncExperimental.segmentSize,
                        prefetch: opts.partialSyncExperimental.prefetch,
                    };
                    break;
                case "query":
                    partialSyncOpts = {
                        bootstrapStrategy: { type: "Query", query: opts.partialSyncExperimental.bootstrapStrategy.query },
                        segmentSize: opts.partialSyncExperimental.segmentSize,
                        prefetch: opts.partialSyncExperimental.prefetch,
                    };
                    break;
            }
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
            partialSyncOpts: partialSyncOpts
        });

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
        const runOpts = {
            url: opts.url,
            headers: headers,
            preemptionMs: 1,
            transform: opts.transform,
        };
        const db = engine.db() as unknown as any;
        const memory = db.memory;
        const io = memory ? memoryIO() : BrowserIO;
        const run = runner(runOpts, io, engine);
        super(db, () => run.wait());

        this.#db = db;
        this.#runner = run;
        this.#engine = engine;
        this.#io = io;
        this.#guards = new SyncEngineGuards();

        // Initialize remote writer if remoteWrites is enabled
        if (opts.remoteWritesExperimental && opts.url) {
            const url = resolveUrl(opts.url);
            this.#remoteWriter = new RemoteWriter({
                url,
                authToken: opts.authToken,
                remoteEncryptionKey: opts.remoteEncryption?.key,
            });
        }
    }
    /**
     * connect database and initialize it in case of clean start
     */
    override async connect() {
        if (this.connected) {
            return;
        } else if (this.#engine == null) {
            if (!this.memory) {
                const worker = await init();
                await Promise.all([
                    registerFileAtWorker(worker, this.name),
                    registerFileAtWorker(worker, `${this.name}-wal`)
                ]);
                this.#worker = worker;
            }
            await super.connect();
        } else {
            if (!this.memory) {
                this.#worker = await init();
                await Promise.all([
                    registerFileAtWorker(this.#worker, this.name),
                    registerFileAtWorker(this.#worker, `${this.name}-wal`),
                    registerFileAtWorker(this.#worker, `${this.name}-wal-revert`),
                    registerFileAtWorker(this.#worker, `${this.name}-info`),
                    registerFileAtWorker(this.#worker, `${this.name}-changes`),
                ]);
            }
            await run(this.#runner, this.#engine.connect());
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
        const changes = await this.#guards.wait(async () => await run(this.#runner, this.#engine.wait()));
        if (changes.empty()) {
            return false;
        }
        await this.#guards.apply(async () => await run(this.#runner, this.#engine.apply(changes)));
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
        await this.#guards.push(async () => await run(this.#runner, this.#engine.push()));
    }
    /**
     * checkpoint WAL for local database
     */
    async checkpoint() {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        await this.#guards.checkpoint(async () => await run(this.#runner, this.#engine.checkpoint()));
    }
    /**
     * @returns statistic of current local database
     */
    async stats(): Promise<DatabaseStats> {
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        return (await run(this.#runner, this.#engine.stats()));
    }

    /**
     * Executes the given SQL string.
     * When remoteWrites is enabled, write statements are sent to the remote server.
     */
    override async exec(sql: string) {
        if (!this.#remoteWriter) return super.exec(sql);
        const category = this.#db.classifySql(sql);

        if (this.#remoteWriter.isInTransaction) {
            const { shouldPull } = await this.#remoteWriter.execRemote(sql, category);
            if (shouldPull) await this.pull();
            return;
        }

        if (category === "read") return super.exec(sql);

        const { shouldPull } = await this.#remoteWriter.execRemote(sql, category);
        if (shouldPull) await this.pull();
    }

    /**
     * Prepares a SQL statement for execution.
     * When remoteWrites is enabled, returns a wrapper that routes writes to remote.
     */
    override prepare(sql: string) {
        const localStmt = super.prepare(sql);

        if (!this.#remoteWriter) {
            return localStmt;
        }

        const category = this.#db.classifySql(sql);
        const isReadonly = category === "read";
        return new RemoteWriteStatement(
            localStmt,
            sql,
            isReadonly,
            this.#remoteWriter,
            () => this.pull(),
        ) as any;
    }

    /**
     * Returns a function that executes the given function in a transaction.
     * When remoteWrites is enabled, the entire transaction goes to remote.
     */
    override transaction(fn: (...any) => Promise<any>) {
        if (typeof fn !== "function")
            throw new TypeError("Expected first argument to be a function");

        if (!this.#remoteWriter) {
            return super.transaction(fn);
        }

        const db = this;
        const remoteWriter = this.#remoteWriter;
        const wrapTxn = (mode: string) => {
            return async (...bindParameters: any[]) => {
                await remoteWriter.beginTransaction(mode);
                try {
                    const result = await fn(...bindParameters);
                    await remoteWriter.commitTransaction();
                    await db.pull();
                    return result;
                } catch (err) {
                    await remoteWriter.rollbackTransaction();
                    throw err;
                }
            };
        };
        const properties = {
            default: { value: wrapTxn("") },
            deferred: { value: wrapTxn("DEFERRED") },
            immediate: { value: wrapTxn("IMMEDIATE") },
            exclusive: { value: wrapTxn("EXCLUSIVE") },
            database: { value: this, enumerable: true },
        };
        Object.defineProperties(properties.default.value, properties);
        Object.defineProperties(properties.deferred.value, properties);
        Object.defineProperties(properties.immediate.value, properties);
        Object.defineProperties(properties.exclusive.value, properties);
        return properties.default.value;
    }

    /**
     * close the database and relevant files
     */
    async close() {
        if (this.#remoteWriter) {
            await this.#remoteWriter.close();
        }
        if (this.#engine != null) {
            if (this.name != null && this.#worker != null) {
                await Promise.all([
                    unregisterFileAtWorker(this.#worker, this.name),
                    unregisterFileAtWorker(this.#worker, `${this.name}-wal`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-wal-revert`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-info`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-changes`),
                ]);
            }
            this.#engine.close();
        }
        await super.close();
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
