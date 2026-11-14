// Stands in for the `better-sqlite3` package: run.sh symlinks a suite's
// node_modules/better-sqlite3 to this directory for the Turso run.
const path = require("node:path");

const compat = path.resolve(__dirname, "../../../../../bindings/javascript/packages/native/dist/compat.js");
const { Database: TursoDatabase, SqliteError } = require(compat);

// better-sqlite3 exports a function that works with and without `new`.
function Database(filename, options) {
  const db = new TursoDatabase(filename || ":memory:", options);
  if (process.env.TURSO_SHIM_TRACE) traceSql(db);
  return db;
}

// Prints every SQL string the suite sends, to find the statement behind a failure.
function traceSql(db) {
  for (const method of ["prepare", "exec"]) {
    const original = db[method].bind(db);
    db[method] = (sql, ...rest) => {
      process.stderr.write(`[sql] ${method}: ${sql}\n`);
      return original(sql, ...rest);
    };
  }
}
Database.prototype = TursoDatabase.prototype;

// Same export shape as better-sqlite3, so ESM `import { SqliteError }` works.
module.exports = Database;
module.exports.SqliteError = SqliteError;
