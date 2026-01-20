import { DatabasePromise, NativeDatabase, SqliteError, DatabaseOpts, sql, SqlQuery } from "@tursodatabase/database-common"
import { Database as NativeDB } from "#index";

class Database extends DatabasePromise {
    constructor(path: string, opts: DatabaseOpts = {}) {
        super(new NativeDB(path, opts) as unknown as NativeDatabase)
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

export { connect, Database, SqliteError, sql, SqlQuery }
