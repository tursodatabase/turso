import { DatabaseOpts, SqliteError, } from "@tursodatabase/database-common"
import { Database, connect as promiseConnect } from "./promise.js";
import { initThreadPool, MainWorker, connectDbAsync } from "./index-bundle.js";

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(path: string, opts: DatabaseOpts = {}): Promise<Database> {
    const init = async () => {
        await initThreadPool();
        if (MainWorker == null) {
            throw new Error("panic: MainWorker is not initialized");
        }
        return MainWorker;
    };
    return await promiseConnect(
        path,
        opts,
        connectDbAsync,
        init
    );
}

export { connect, Database, SqliteError }
