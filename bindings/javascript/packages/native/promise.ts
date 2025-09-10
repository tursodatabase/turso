import { DatabasePromise, NativeDatabase, SqliteError, DatabaseOpts } from "@tursodatabase/database-common"
import { Database as NativeDB } from "#index";

class Database extends DatabasePromise {
    constructor(path: string, opts: DatabaseOpts = {}) {
        super(new NativeDB(path, { tracing: opts.tracing }) as unknown as NativeDatabase, opts)
    }
}

/**
 * Creates a new database connection asynchronously.
 * 
 * @param {string} path - Path to the database file.
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(path: string, opts: any = {}): Promise<Database> {
    return new Database(path, opts);
}

export { connect, Database, SqliteError }
