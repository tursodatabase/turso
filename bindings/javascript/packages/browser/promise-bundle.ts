import { DatabaseOpts, SqliteError, } from "@tursodatabase/database-common"
import { connect as promiseConnect, Database } from "./promise.js";
import { connect as nativeConnect, initThreadPool, MainWorker } from "./index-bundle.js";

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(path: string, opts: DatabaseOpts = {}): Promise<Database> {
    return await promiseConnect(path, opts, nativeConnect, async () => {
        await initThreadPool();
        if (MainWorker == null) {
            throw new Error("panic: MainWorker is not initialized");
        }
        return MainWorker;
    });
}

export { connect, Database, SqliteError }
