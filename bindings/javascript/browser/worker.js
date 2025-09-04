import { expose } from "threads/worker"
import { bindParams } from "./bind";
import { SqliteError } from "./sqlite-error";

import {
    createOnMessage as __wasmCreateOnMessageForFsProxy,
    getDefaultContext as __emnapiGetDefaultContext,
    instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync,
    WASI as __WASI,
} from '@napi-rs/wasm-runtime'


const __wasi = new __WASI({
    version: 'preview1',
})

const __wasmUrl = new URL('./turso-browser.wasm32-wasi.wasm', import.meta.url).href
const __emnapiContext = __emnapiGetDefaultContext()

const __sharedMemory = new WebAssembly.Memory({
    initial: 4000,
    maximum: 65536,
    shared: true,
})

function getUint8ArrayFromWasm(ptr, len) {
    ptr = ptr >>> 0;
    return new Uint8Array(__sharedMemory.buffer).subarray(ptr, ptr + len);
}


var module = null;
var db = null;
var stmts = new Map();
var stmtId = 0;
var files = new Map();
let fileId = 0;

async function registerFile(path) {
    const opfsRoot = await navigator.storage.getDirectory();
    const fileHandle = await opfsRoot.getFileHandle(path, { create: true });
    const syncAccessHandle = await fileHandle.createSyncAccessHandle();
    fileId += 1;
    files.set(fileId, syncAccessHandle);
    return [path, fileId];
}

function read(handle, bufferPtr, bufferLen, offset) {
    try {
        const buffer = getUint8ArrayFromWasm(bufferPtr, bufferLen);
        const file = files.get(Number(handle));
        const result = file.read(buffer, { at: Number(offset) });
        return result;
    } catch (e) {
        console.error(e);
        return -1;
    }
}
function write(handle, bufferPtr, bufferLen, offset) {
    try {
        const buffer = getUint8ArrayFromWasm(bufferPtr, bufferLen);
        const file = files.get(Number(handle));
        const result = file.write(buffer, { at: Number(offset) });
        return result;
    } catch (e) {
        console.error(e);
        return -1;
    }
}
function sync() {
    try {
        const file = files.get(Number(handle));
        file.flush(size);
        return 0;
    } catch (e) {
        console.error(e);
        return -1;
    }
}
function truncate(handle, size) {
    try {
        const file = files.get(Number(handle));
        const result = file.truncate(size);
        return result;
    } catch (e) {
        console.error(e);
        return -1;
    }
}
function size(handle) {
    try {
        const file = files.get(Number(handle));
        const size = file.getSize()
        return size;
    } catch (e) {
        console.error(e);
        return -1;
    }
}

// Step result constants
const STEP_ROW = 1;
const STEP_DONE = 2;
const STEP_IO = 3;

const convertibleErrorTypes = { TypeError };
const CONVERTIBLE_ERROR_PREFIX = "[TURSO_CONVERT_TYPE]";

function convertError(err) {
    if ((err.code ?? "").startsWith(CONVERTIBLE_ERROR_PREFIX)) {
        return createErrorByName(
            err.code.substring(CONVERTIBLE_ERROR_PREFIX.length),
            err.message,
        );
    }

    return new SqliteError(err.message, err.code, err.rawCode);
}

function createErrorByName(name, message) {
    const ErrorConstructor = convertibleErrorTypes[name];
    if (!ErrorConstructor) {
        throw new Error(`unknown error type ${name} from Turso`);
    }

    return new ErrorConstructor(message);
}


/**
 * Database represents a connection that can prepare and execute SQL statements.
 */
class Database {
    db;
    memory;
    open;
    _inTransaction = false;
    /**
     * Creates a new database connection. If the database file pointed to by `path` does not exists, it will be created.
     *
     * @constructor
     * @param {string} path - Path to the database file.
     * @param {Object} opts - Options for database behavior.
     * @param {boolean} [opts.readonly=false] - Open the database in read-only mode.
     * @param {boolean} [opts.fileMustExist=false] - If true, throws if database file does not exist.
     * @param {number} [opts.timeout=0] - Timeout duration in milliseconds for database operations. Defaults to 0 (no timeout).
     */
    //@ts-ignore
    constructor(path, opts = {}) {
        opts.readonly = opts.readonly === undefined ? false : opts.readonly;
        opts.fileMustExist =
            opts.fileMustExist === undefined ? false : opts.fileMustExist;
        opts.timeout = opts.timeout === undefined ? 0 : opts.timeout;

        const db = new module.exports.Database(path);
        this.initialize(db, opts.path, opts.readonly);
    }
    static create() {
        return Object.create(this.prototype);
    }
    initialize(db, name, readonly) {
        this.db = db;
        this.memory = db.memory;
        Object.defineProperties(this, {
            inTransaction: {
                get: () => this._inTransaction,
            },
            name: {
                get() {
                    return name;
                },
            },
            readonly: {
                get() {
                    return readonly;
                },
            },
            open: {
                get() {
                    return this.db.open;
                },
            },
        });
    }

    /**
     * Prepares a SQL statement for execution.
     *
     * @param {string} sql - The SQL statement string to prepare.
     */
    prepare(sql) {
        if (!this.open) {
            throw new TypeError("The database connection is not open");
        }

        if (!sql) {
            throw new RangeError("The supplied SQL string contains no statements");
        }

        try {
            return new Statement(this.db.prepare(sql), this);
        } catch (err) {
            throw convertError(err);
        }
    }

    /**
     * Returns a function that executes the given function in a transaction.
     *
     * @param {function} fn - The function to wrap in a transaction.
     */
    transaction(fn) {
        if (typeof fn !== "function")
            throw new TypeError("Expected first argument to be a function");

        const db = this;
        const wrapTxn = (mode) => {
            return (...bindParameters) => {
                db.exec("BEGIN " + mode);
                db._inTransaction = true;
                try {
                    const result = fn(...bindParameters);
                    db.exec("COMMIT");
                    db._inTransaction = false;
                    return result;
                } catch (err) {
                    db.exec("ROLLBACK");
                    db._inTransaction = false;
                    throw err;
                }
            };
        };
        const properties = {
            default: { value: wrapTxn("") },
            deferred: { value: wrapTxn("DEFERRED") },
            immediate: { value: wrapTxn("IMMEDIATE") },
            exclusive: { value: wrapTxn("EXCLUSIVE") },
            database: { value: this, enumerable: true },
        };
        Object.defineProperties(properties.default.value, properties);
        Object.defineProperties(properties.deferred.value, properties);
        Object.defineProperties(properties.immediate.value, properties);
        Object.defineProperties(properties.exclusive.value, properties);
        return properties.default.value;
    }

    pragma(source, options) {
        if (options == null) options = {};

        if (typeof source !== "string")
            throw new TypeError("Expected first argument to be a string");

        if (typeof options !== "object")
            throw new TypeError("Expected second argument to be an options object");

        const pragma = `PRAGMA ${source}`;

        const stmt = this.prepare(pragma);
        const results = stmt.all();

        return results;
    }

    backup(filename, options) {
        throw new Error("not implemented");
    }

    serialize(options) {
        throw new Error("not implemented");
    }

    function(name, options, fn) {
        throw new Error("not implemented");
    }

    aggregate(name, options) {
        throw new Error("not implemented");
    }

    table(name, factory) {
        throw new Error("not implemented");
    }

    loadExtension(path) {
        throw new Error("not implemented");
    }

    maxWriteReplicationIndex() {
        throw new Error("not implemented");
    }

    /**
     * Executes a SQL statement.
     *
     * @param {string} sql - The SQL statement string to execute.
     */
    exec(sql) {
        if (!this.open) {
            throw new TypeError("The database connection is not open");
        }

        try {
            this.db.batch(sql);
        } catch (err) {
            throw convertError(err);
        }
    }

    /**
     * Interrupts the database connection.
     */
    interrupt() {
        throw new Error("not implemented");
    }

    /**
     * Sets the default safe integers mode for all statements from this database.
     *
     * @param {boolean} [toggle] - Whether to use safe integers by default.
     */
    defaultSafeIntegers(toggle) {
        this.db.defaultSafeIntegers(toggle);
    }

    /**
     * Closes the database connection.
     */
    close() {
        this.db.close();
    }
}

/**
 * Statement represents a prepared SQL statement that can be executed.
 */
class Statement {
    stmt;
    db;
    constructor(stmt, database) {
        this.stmt = stmt;
        this.db = database;
    }

    /**
     * Toggle raw mode.
     *
     * @param raw Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled.
     */
    raw(raw) {
        this.stmt.raw(raw);
        return this;
    }

    /**
     * Toggle pluck mode.
     *
     * @param pluckMode Enable or disable pluck mode. If you don't pass the parameter, pluck mode is enabled.
     */
    pluck(pluckMode) {
        this.stmt.pluck(pluckMode);
        return this;
    }

    /**
     * Sets safe integers mode for this statement.
     *
     * @param {boolean} [toggle] - Whether to use safe integers.
     */
    safeIntegers(toggle) {
        this.stmt.safeIntegers(toggle);
        return this;
    }

    /**
     * Get column information for the statement.
     *
     * @returns {Array} An array of column objects with name, column, table, database, and type properties.
     */
    columns() {
        return this.stmt.columns();
    }

    get source() {
        throw new Error("not implemented");
    }

    get reader() {
        throw new Error("not implemented");
    }

    get database() {
        return this.db;
    }

    /**
     * Executes the SQL statement and returns an info object.
     */
    async run(...bindParameters) {
        const totalChangesBefore = this.db.db.totalChanges();

        this.stmt.reset();
        bindParams(this.stmt, bindParameters);

        while (true) {
            const stepResult = this.stmt.step();
            if (stepResult === STEP_IO) {
                await this.db.db.ioLoopAsync();
                continue;
            }
            if (stepResult === STEP_DONE) {
                break;
            }
            if (stepResult === STEP_ROW) {
                // For run(), we don't need the row data, just continue
                continue;
            }
        }

        const lastInsertRowid = this.db.db.lastInsertRowid();
        const changes = this.db.db.totalChanges() === totalChangesBefore ? 0 : this.db.db.changes();

        return { changes, lastInsertRowid };
    }

    /**
     * Executes the SQL statement and returns the first row.
     *
     * @param bindParameters - The bind parameters for executing the statement.
     */
    async get(...bindParameters) {
        this.stmt.reset();
        bindParams(this.stmt, bindParameters);

        while (true) {
            const stepResult = this.stmt.step();
            if (stepResult === STEP_IO) {
                await this.db.db.ioLoopAsync();
                continue;
            }
            if (stepResult === STEP_DONE) {
                return undefined;
            }
            if (stepResult === STEP_ROW) {
                return this.stmt.row();
            }
        }
    }

    /**
     * Executes the SQL statement and returns an iterator to the resulting rows.
     *
     * @param bindParameters - The bind parameters for executing the statement.
     */
    async *iterate(...bindParameters) {
        this.stmt.reset();
        bindParams(this.stmt, bindParameters);

        while (true) {
            const stepResult = this.stmt.step();
            if (stepResult === STEP_IO) {
                await this.db.db.ioLoopAsync();
                continue;
            }
            if (stepResult === STEP_DONE) {
                break;
            }
            if (stepResult === STEP_ROW) {
                yield this.stmt.row();
            }
        }
    }

    /**
     * Executes the SQL statement and returns an array of the resulting rows.
     *
     * @param bindParameters - The bind parameters for executing the statement.
     */
    async all(...bindParameters) {
        this.stmt.reset();
        bindParams(this.stmt, bindParameters);
        const rows = [];

        while (true) {
            const stepResult = this.stmt.step();
            if (stepResult === STEP_IO) {
                await this.db.db.ioLoopAsync();
                continue;
            }
            if (stepResult === STEP_DONE) {
                break;
            }
            if (stepResult === STEP_ROW) {
                rows.push(this.stmt.row());
            }
        }
        return rows;
    }

    /**
     * Interrupts the statement.
     */
    interrupt() {
        throw new Error("not implemented");
    }


    /**
     * Binds the given parameters to the statement _permanently_
     *
     * @param bindParameters - The bind parameters for binding the statement.
     * @returns this - Statement with binded parameters
     */
    bind(...bindParameters) {
        try {
            bindParams(this.stmt, bindParameters);
            return this;
        } catch (err) {
            throw convertError(err);
        }
    }
}

async function init() {
    if (module != null) {
        return;
    }
    const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

    const {
        instance: __napiInstance,
        module: __wasiModule,
        napiModule: __napiModule,
    } = __emnapiInstantiateNapiModuleSync(__wasmFile, {
        context: __emnapiContext,
        asyncWorkPoolSize: 0,
        wasi: __wasi,
        onCreateWorker() {
            return null
        },
        overwriteImports(importObject) {
            importObject.env = {
                ...importObject.env,
                ...importObject.napi,
                ...importObject.emnapi,
                read: read,
                write: write,
                sync: sync,
                truncate: truncate,
                size: size,
                memory: __sharedMemory,
            }
            return importObject
        },
        beforeInit({ instance }) {
            for (const name of Object.keys(instance.exports)) {
                if (name.startsWith('__napi_register__')) {
                    instance.exports[name]()
                }
            }
        },
    })
    module = __napiModule;
}

expose({
    async connect(path) {
        await init();
        let [dbPath, dbId] = await registerFile(path);
        let [walPath, walId] = await registerFile(`${path}-wal`);
        db = new Database({ path: path, files: { [dbPath]: { handle: dbId }, [walPath]: { handle: walId } } });
    },
    prepare(sql) {
        stmtId += 1;
        stmts.set(stmtId, db.prepare(sql));
        return stmtId;
    },
    pragma(source, options) {
        return db.pragma(source, options);
    },
    exec(sql) {
        db.exec(sql);
    },
    defaultSafeIntegers(toggle) {
        db.defaultSafeIntegers(toggle);
    },
    close() {
        db.close();
    },
    async run(stmtId, bindParameters) {
        return stmts.get(stmtId).run(...bindParameters);
    },
    async all(stmtId, bindParameters) {
        return stmts.get(stmtId).all(...bindParameters);
    },
    pluck(stmtId, pluckMode) {
        stmts.get(stmtId).pluck(pluckMode);
    },
    safeIntegers(stmtId, toggle) {
        stmts.get(stmtId).safeIntegers(toggle);
    },
    columns(stmtId) {
        return stmts.get(stmtId).stmt.columns();
    },
    async get(stmtId, bindParameters) {
        return stmts.get(stmtId).get(...bindParameters);
    },
    bind(bindParameters) {
        stmts.get(stmtId).bind(...bindParameters);
    }
})
