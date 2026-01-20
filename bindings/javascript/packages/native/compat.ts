import { DatabaseCompat, NativeDatabase, SqliteError, DatabaseOpts, sql, SqlQuery } from "@tursodatabase/database-common"
import { Database as NativeDB } from "#index";

class Database extends DatabaseCompat {
    constructor(path: string, opts: DatabaseOpts = {}) {
        super(new NativeDB(path, opts) as unknown as NativeDatabase)
    }
}

export { Database, SqliteError, sql, SqlQuery }
