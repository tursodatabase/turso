// PGlite-compatible JavaScript API for the Turso Postgres frontend.
//
// The contract this implements is specified in postgres/PGLITE.md and pinned
// by the conformance suite in postgres/conformance/js. The native layer
// (src/lib.rs) prepares and steps statements; this file implements the
// PGlite class, OID-driven value conversion, and SQLSTATE error mapping.

import { PgDatabase } from "#native";
import { parseValue, serializeValue, types } from "./types.js";

export { types };

/// PGlite construction options that describe WASM/browser machinery. They
/// have no meaning for a native embedded engine, and silently ignoring them
/// would mislead code that depends on their behavior.
const UNSUPPORTED_OPTIONS = [
  "fs",
  "wasmModule",
  "fsBundle",
  "initialMemory",
  "extensions",
  "loadDataDir",
  "username",
  "database",
];

/// SQLSTATE mapping for engine error messages. First match wins.
const SQLSTATE_PATTERNS = [
  [/no such table/i, "42P01"], // undefined_table
  [/no such view/i, "42P01"],
  [/unique constraint/i, "23505"], // unique_violation
  [/not null constraint/i, "23502"], // not_null_violation
  [/check constraint/i, "23514"], // check_violation
  [/foreign key constraint/i, "23503"], // foreign_key_violation
  [/no such column/i, "42703"], // undefined_column
  [/no such function/i, "42883"], // undefined_function
  [/parse error/i, "42601"], // syntax_error
  [/syntax error/i, "42601"],
  [/database is locked/i, "55P03"], // lock_not_available
  [/readonly|read-only/i, "25006"], // read_only_sql_transaction
];

export class DatabaseError extends Error {
  constructor(message, code, { query, params } = {}) {
    super(message);
    this.name = "DatabaseError";
    this.severity = "ERROR";
    this.code = code;
    this.query = query;
    this.params = params;
  }
}

const toDatabaseError = (err, query, params) => {
  if (err instanceof DatabaseError) return err;
  const message = err instanceof Error ? err.message : String(err);
  let code = "XX000"; // internal_error
  for (const [pattern, sqlstate] of SQLSTATE_PATTERNS) {
    if (pattern.test(message)) {
      code = sqlstate;
      break;
    }
  }
  return new DatabaseError(message, code, { query, params });
};

const STEP_ROW = 1;
const STEP_DONE = 2;
const STEP_IO = 3;

/// Statements whose command tag carries an affected-row count.
const DML_RE = /^\s*(insert|update|delete|truncate)\b/i;

const normalizePath = (dataDir) => {
  if (dataDir === undefined || dataDir === null || dataDir === "") return ":memory:";
  if (dataDir === "memory://") return ":memory:";
  if (dataDir.startsWith("memory://")) return ":memory:";
  if (dataDir.startsWith("file://")) return dataDir.slice("file://".length);
  if (dataDir.startsWith("idb://") || dataDir.startsWith("opfs-ahp://")) {
    throw new Error(
      `browser filesystem dataDir "${dataDir}" is not supported; use a file path or memory://`,
    );
  }
  return dataDir;
};

export class PGlite {
  #db;
  #waitReady;
  #closed = false;
  #chain = Promise.resolve();
  #parsers;
  #serializers;

  constructor(dataDir, options) {
    if (typeof dataDir === "object" && dataDir !== null) {
      options = dataDir;
      dataDir = options.dataDir;
    }
    options = options ?? {};
    for (const key of UNSUPPORTED_OPTIONS) {
      if (options[key] !== undefined) {
        throw new Error(`PGlite option "${key}" is not supported by @tursodatabase/pg-experimental`);
      }
    }
    const path = normalizePath(dataDir);
    this.#parsers = options.parsers;
    this.#serializers = options.serializers;
    const nativeOpts = {
      readonly: options.readonly,
      fileMustExist: options.fileMustExist,
      timeout: options.timeout,
      defaultQueryTimeout: options.defaultQueryTimeout,
    };
    this.#waitReady = Promise.resolve().then(() => {
      this.#db = new PgDatabase(path, nativeOpts);
    });
  }

  static async create(dataDir, options) {
    const db = new PGlite(dataDir, options);
    await db.waitReady;
    return db;
  }

  get waitReady() {
    return this.#waitReady;
  }

  get ready() {
    return this.#db !== undefined && !this.#closed;
  }

  get closed() {
    return this.#closed;
  }

  get path() {
    return this.#db?.path;
  }

  /// Serialize database access: PGlite runs queries and transactions under a
  /// mutex so they never interleave.
  #exclusive(fn) {
    const run = this.#chain.then(fn);
    this.#chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  #checkReady() {
    if (this.#closed) throw new Error("PGlite is closed");
    if (this.#db === undefined) throw new Error("PGlite is not ready");
  }

  /// Execute a single prepared statement and assemble a PGlite Results object.
  #runStatement(sql, params = [], options = {}) {
    const stmt = this.#db.prepare(sql);
    try {
      const paramTypes = options.paramTypes;
      const serializers = { ...this.#serializers, ...options.serializers };
      for (let i = 0; i < params.length; i++) {
        const oid = paramTypes?.[i];
        const value = serializeValue(params[i], oid, serializers);
        stmt.bindNamed(`$${i + 1}`, i + 1, value);
      }
      const hasRows = stmt.numColumns > 0;
      const fields = hasRows
        ? stmt.columns().map((c) => ({ name: c.name, dataTypeID: c.dataTypeId }))
        : [];
      const rawRows = [];
      for (;;) {
        const step = stmt.stepSync();
        if (step === STEP_IO) {
          this.#db.ioStep();
          continue;
        }
        if (step === STEP_ROW) {
          rawRows.push(stmt.row());
          continue;
        }
        if (step === STEP_DONE) break;
      }
      const parsers = { ...this.#parsers, ...options.parsers };
      const rows = rawRows.map((raw) => {
        if (options.rowMode === "array") {
          return raw.map((value, i) => parseValue(value, fields[i].dataTypeID, parsers));
        }
        const row = {};
        for (let i = 0; i < fields.length; i++) {
          row[fields[i].name] = parseValue(raw[i], fields[i].dataTypeID, parsers);
        }
        return row;
      });
      const affectedRows = DML_RE.test(sql) ? Number(stmt.nChange()) : 0;
      return hasRows ? { rows, fields, affectedRows } : { rows: [], fields: [], affectedRows };
    } finally {
      stmt.finalize();
    }
  }

  #query(sql, params, options) {
    const statements = this.#db
      .splitStatements(sql)
      .filter((s) => s.trim().length > 0);
    if (statements.length > 1) {
      throw new DatabaseError(
        "cannot insert multiple commands into a prepared statement",
        "42601",
        { query: sql, params },
      );
    }
    try {
      return this.#runStatement(sql, params, options);
    } catch (err) {
      throw toDatabaseError(err, sql, params);
    }
  }

  #exec(sql, options) {
    let statements;
    try {
      statements = this.#db.splitStatements(sql);
    } catch (err) {
      throw toDatabaseError(err, sql);
    }
    const results = [];
    for (const statement of statements) {
      if (statement.trim().length === 0) continue;
      try {
        results.push(this.#runStatement(statement, [], options));
      } catch (err) {
        throw toDatabaseError(err, statement);
      }
    }
    return results;
  }

  async query(sql, params, options) {
    await this.#waitReady;
    return this.#exclusive(() => {
      this.#checkReady();
      return this.#query(sql, params, options);
    });
  }

  async sql(strings, ...params) {
    let query = strings[0];
    for (let i = 0; i < params.length; i++) {
      query += `$${i + 1}` + strings[i + 1];
    }
    return this.query(query, params);
  }

  async exec(sql, options) {
    await this.#waitReady;
    return this.#exclusive(() => {
      this.#checkReady();
      return this.#exec(sql, options);
    });
  }

  async transaction(callback) {
    await this.#waitReady;
    return this.#exclusive(async () => {
      this.#checkReady();
      this.#query("BEGIN");
      const tx = new Transaction(this);
      try {
        const result = await callback(tx);
        if (!tx.closed) this.#query("COMMIT");
        return result;
      } catch (err) {
        if (!tx.closed) this.#query("ROLLBACK");
        throw err;
      } finally {
        tx._close();
      }
    });
  }

  async close() {
    await this.#waitReady.catch(() => {});
    return this.#exclusive(() => {
      if (this.#closed) return;
      this.#closed = true;
      this.#db?.close();
    });
  }

  async [Symbol.asyncDispose]() {
    await this.close();
  }

  // Internal accessors for Transaction.
  _txQuery(sql, params, options) {
    this.#checkReady();
    return this.#query(sql, params, options);
  }

  _txExec(sql, options) {
    this.#checkReady();
    return this.#exec(sql, options);
  }
}

class Transaction {
  #pg;
  #closed = false;

  constructor(pg) {
    this.#pg = pg;
  }

  get closed() {
    return this.#closed;
  }

  #checkOpen() {
    if (this.#closed) {
      throw new DatabaseError("transaction is closed", "25P01");
    }
  }

  async query(sql, params, options) {
    this.#checkOpen();
    return this.#pg._txQuery(sql, params, options);
  }

  async sql(strings, ...params) {
    let query = strings[0];
    for (let i = 0; i < params.length; i++) {
      query += `$${i + 1}` + strings[i + 1];
    }
    return this.query(query, params);
  }

  async exec(sql, options) {
    this.#checkOpen();
    return this.#pg._txExec(sql, options);
  }

  async rollback() {
    this.#checkOpen();
    this.#pg._txQuery("ROLLBACK");
    this.#closed = true;
  }

  _close() {
    this.#closed = true;
  }
}
