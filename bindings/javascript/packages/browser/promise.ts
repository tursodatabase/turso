import { DatabasePromise, NativeDatabase, DatabaseOpts, SqliteError } from "@tursodatabase/database-common"
import { connect as nativeConnect, initThreadPool, MainWorker } from "#index";

let workerRequestId = 0;
class Database extends DatabasePromise {
    files: string[];
    constructor(db: NativeDatabase, files: string[], opts: DatabaseOpts = {}) {
        super(db, opts)
        this.files = files;
    }
    async close() {
        let currentId = workerRequestId;
        workerRequestId += this.files.length;

        let tasks = [];
        for (const file of this.files) {
            (MainWorker as any).postMessage({ __turso__: "unregister", path: file, id: currentId });
            tasks.push(waitFor(currentId));
            currentId += 1;
        }
        await Promise.all(tasks);
        this.db.close();
    }
}

function waitFor(id: number): Promise<any> {
    let waitResolve, waitReject;
    const callback = msg => {
        if (msg.data.id == id) {
            if (msg.data.error != null) {
                waitReject(msg.data.error)
            } else {
                waitResolve()
            }
            cleanup();
        }
    };
    const cleanup = () => (MainWorker as any).removeEventListener("message", callback);

    (MainWorker as any).addEventListener("message", callback);
    const result = new Promise((resolve, reject) => {
        waitResolve = resolve;
        waitReject = reject;
    });
    return result;
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
        return new Database(db, [], opts);
    }
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not set");
    }

    let currentId = workerRequestId;
    workerRequestId += 2;

    let dbHandlePromise = waitFor(currentId);
    let walHandlePromise = waitFor(currentId + 1);
    (MainWorker as any).postMessage({ __turso__: "register", path: `${path}`, id: currentId });
    (MainWorker as any).postMessage({ __turso__: "register", path: `${path}-wal`, id: currentId + 1 });
    await Promise.all([dbHandlePromise, walHandlePromise]);
    const db = await nativeConnect(path, { tracing: opts.tracing });
    const files = [path, `${path}-wal`];
    return new Database(db, files, opts);
}

export { connect, Database, SqliteError }
