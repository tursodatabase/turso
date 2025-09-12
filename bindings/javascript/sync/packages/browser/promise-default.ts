import { SyncOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult } from "@tursodatabase/sync-common";
import { connect as promiseConnect, Database } from "./promise.js";
import { SyncEngine, initThreadPool, MainWorker } from "./index-default.js";

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(opts: SyncOpts): Promise<Database> {
    return await promiseConnect(opts, x => new SyncEngine(x), async () => {
        await initThreadPool();
        if (MainWorker == null) {
            throw new Error("panic: MainWorker is not initialized");
        }
        return MainWorker;
    });
}

export { connect, Database, }
export type { DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
