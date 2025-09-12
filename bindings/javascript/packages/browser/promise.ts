import { registerFileAtWorker, unregisterFileAtWorker } from "@tursodatabase/database-browser-common"
import { DatabasePromise, NativeDatabase, DatabaseOpts, SqliteError, } from "@tursodatabase/database-common"

class Database extends DatabasePromise {
    path: string | null;
    worker: Worker | null;
    constructor(db: NativeDatabase, worker: Worker | null, fsPath: string | null, opts: DatabaseOpts = {}) {
        super(db, opts)
        this.path = fsPath;
        this.worker = worker;
    }
    async close() {
        if (this.path != null && this.worker != null) {
            await Promise.all([
                unregisterFileAtWorker(this.worker, this.path),
                unregisterFileAtWorker(this.worker, `${this.path}-wal`)
            ]);
        }
        this.db.close();
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(path: string, opts: DatabaseOpts, connect: any, init: () => Promise<Worker>): Promise<Database> {
    if (path == ":memory:") {
        const db = await connect(path, { tracing: opts.tracing });
        return new Database(db, null, null, opts);
    }
    const worker = await init();
    await Promise.all([
        registerFileAtWorker(worker, path),
        registerFileAtWorker(worker, `${path}-wal`)
    ]);
    const db = await connect(path, { tracing: opts.tracing });
    return new Database(db, worker, path, opts);
}

export { connect, Database, SqliteError }
