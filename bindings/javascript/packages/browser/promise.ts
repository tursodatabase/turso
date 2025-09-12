import { registerFileAtWorker, unregisterFileAtWorker } from "@tursodatabase/database-browser-common"
import { DatabasePromise, NativeDatabase, DatabaseOpts, SqliteError, } from "@tursodatabase/database-common"
import { connect as nativeConnect, initThreadPool, MainWorker } from "#index";

class Database extends DatabasePromise {
    path: string | null;
    constructor(db: NativeDatabase, fsPath: string | null, opts: DatabaseOpts = {}) {
        super(db, opts)
        this.path = fsPath;
    }
    async close() {
        if (this.path != null) {
            await Promise.all([
                unregisterFileAtWorker(MainWorker, this.path),
                unregisterFileAtWorker(MainWorker, `${this.path}-wal`)
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
async function connect(path: string, opts: DatabaseOpts = {}): Promise<Database> {
    if (path == ":memory:") {
        const db = await nativeConnect(path, { tracing: opts.tracing });
        return new Database(db, null, opts);
    }
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not set");
    }
    await Promise.all([
        registerFileAtWorker(MainWorker, path),
        registerFileAtWorker(MainWorker, `${path}-wal`)
    ]);
    const db = await nativeConnect(path, { tracing: opts.tracing });
    return new Database(db, path, opts);
}

export { connect, Database, SqliteError }
