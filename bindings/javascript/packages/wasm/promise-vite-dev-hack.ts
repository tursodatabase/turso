import { DatabasePromise, DatabaseOpts, SqliteError, } from "@tursodatabase/database-common"
import { registerFileAtWorker, unregisterFileAtWorker, ioNotifier } from "@tursodatabase/database-wasm-common";
import { initThreadPool, MainWorker, Database as NativeDatabase } from "./index-vite-dev-hack.js";

async function init(): Promise<Worker> {
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not initialized");
    }
    return MainWorker;
}

class Database extends DatabasePromise {
    #worker: Worker | null;
    constructor(path: string, opts: DatabaseOpts = {}) {
        super(
            new NativeDatabase(path, opts) as unknown as any,
            () => ioNotifier.waitForCompletion(),
        )
    }
    /**
     * connect database and pre-open necessary files in the OPFS
     */
    override async connect() {
        if (!this.memory) {
            const worker = await init();
            await Promise.all([
                registerFileAtWorker(worker, this.name),
                registerFileAtWorker(worker, `${this.name}-wal`)
            ]);
            this.#worker = worker;
        }
        await super.connect();
    }
    /**
     * close the database and relevant files
     */
    async close() {
        if (this.name != null && this.#worker != null) {
            await Promise.all([
                unregisterFileAtWorker(this.#worker, this.name),
                unregisterFileAtWorker(this.#worker, `${this.name}-wal`)
            ]);
        }
        await super.close();
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(path: string, opts: DatabaseOpts = {}): Promise<Database> {
    const db = new Database(path, opts);
    await db.connect();
    return db;
}

export { connect, Database, SqliteError }
