// Hegel-style property-based parity tests for the JS turso drivers.
//
// Compares @tursodatabase/database (local NAPI native driver) against
// @tursodatabase/serverless (remote HTTP/Hrana driver) — same pattern as
// Rust, Go, and Python parity tests.
//
// Requires:
//   - NAPI native binary built (bindings/javascript/packages/native/)
//   - libsql-server running for remote:
//     docker run -d -p 8080:8080 \
//       -e SQLD_MAX_CONCURRENT_REQUESTS=1024 \
//       -e SQLD_MAX_CONCURRENT_CONNECTIONS=1024 \
//       -e SQLD_DISABLE_INTELLIGENT_THROTTLING=true \
//       ghcr.io/tursodatabase/libsql-server:latest
//   - serverless driver built:
//     cd serverless/javascript && npm run build

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import test from 'ava';
import * as fc from 'fast-check';

// ---------------------------------------------------------------------------
// Load spec from ops.json
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));
const _SPEC = JSON.parse(readFileSync(join(__dirname, '..', 'spec', 'ops.json'), 'utf8'));

// ---------------------------------------------------------------------------
// Driver setup — both drivers are required; fail hard if either is missing.
// ---------------------------------------------------------------------------

const { connect: nativeConnect } = await import('@tursodatabase/database');
const { connect: serverlessConnect } = await import('@tursodatabase/serverless');

const serverUrl = process.env.TURSO_DATABASE_URL ?? 'http://localhost:8080';
const authToken = process.env.TURSO_AUTH_TOKEN;

// Verify the remote server is actually reachable.
{
  const c = serverlessConnect({ url: serverUrl, authToken });
  await c.execute('SELECT 1');
  await c.close();
}

// ---------------------------------------------------------------------------
// Column types for dynamic DDL
// ---------------------------------------------------------------------------

const COL_TYPES = _SPEC.constants.col_types;

// ---------------------------------------------------------------------------
// Value type tag — normalize JS types for comparison
// ---------------------------------------------------------------------------

function typeTag(v) {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'bigint') return 'integer';
  if (typeof v === 'number') {
    return Number.isInteger(v) ? 'integer' : 'real';
  }
  if (typeof v === 'string') return 'text';
  if (v instanceof Buffer || v instanceof Uint8Array || v instanceof ArrayBuffer) return 'blob';
  return `unknown(${typeof v})`;
}

// ---------------------------------------------------------------------------
// Cell value comparison with float epsilon and buffer equality
// ---------------------------------------------------------------------------

function cellsEqual(a, b) {
  if (a === null || a === undefined) return b === null || b === undefined;
  if (b === null || b === undefined) return false;

  // Buffer comparison
  if ((a instanceof Buffer || a instanceof Uint8Array) &&
      (b instanceof Buffer || b instanceof Uint8Array)) {
    return Buffer.from(a).equals(Buffer.from(b));
  }

  // Float epsilon
  if (typeof a === 'number' && typeof b === 'number') {
    const max = Math.max(Math.abs(a), Math.abs(b));
    if (max === 0) return true;
    return Math.abs(a - b) / max < 1e-12;
  }

  // Int/float crossover
  if (typeof a === 'number' && typeof b === 'bigint') {
    return Math.abs(a - Number(b)) < 1e-12;
  }
  if (typeof a === 'bigint' && typeof b === 'number') {
    return Math.abs(Number(a) - b) < 1e-12;
  }

  // Bigint
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a === b;
  }

  return a === b;
}

function valuesEqual(aRows, bRows) {
  if (!aRows && !bRows) return true;
  if (!aRows || !bRows) return false;
  if (aRows.length !== bRows.length) return false;
  for (let r = 0; r < aRows.length; r++) {
    if (aRows[r].length !== bRows[r].length) return false;
    for (let c = 0; c < aRows[r].length; c++) {
      if (!cellsEqual(aRows[r][c], bRows[r][c])) return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Adapters — normalize both drivers to same result shape
// ---------------------------------------------------------------------------

async function executeLocal(db, sql, params = []) {
  try {
    const stmt = await db.prepare(sql);
    const cols = stmt.columns();
    if (cols.length > 0) {
      stmt.raw(true);
      const rows = await stmt.all(params);
      return {
        success: true,
        columnCount: cols.length,
        columnNames: cols.map(c => c.name),
        rowCount: rows.length,
        valueTypes: rows.map(row => Array.from(row).map(typeTag)),
        values: rows.map(row => Array.from(row)),
      };
    } else {
      const result = await stmt.run(params);
      return {
        success: true,
        columnCount: 0,
        rowCount: 0,
        affectedRows: result.changes,
      };
    }
  } catch {
    return { success: false };
  }
}

async function executeRemote(conn, sql, params = []) {
  try {
    const result = await conn.execute(sql, params);
    if (result.columns.length > 0) {
      return {
        success: true,
        columnCount: result.columns.length,
        columnNames: result.columns,
        rowCount: result.rows.length,
        valueTypes: result.rows.map(row =>
          Array.from(row).map(typeTag)
        ),
        values: result.rows.map(row => Array.from(row)),
      };
    } else {
      return {
        success: true,
        columnCount: 0,
        rowCount: 0,
        affectedRows: result.rowsAffected,
      };
    }
  } catch {
    return { success: false };
  }
}

// ---------------------------------------------------------------------------
// Operation generators (fast-check arbitraries)
// ---------------------------------------------------------------------------

const arbTableName = fc.integer({ min: 0, max: 5 }).map(i => `t_${i}`);

// Build value arbitraries dynamically from spec.
// Returns an array of fc arbitraries (may be more than one per spec entry).
function _buildValueArbs(v) {
  const id = v.id;
  if (id === 'null') return [fc.constant(null)];
  if (v.random) return [fc.integer({ min: v.random.min, max: v.random.max })];
  if (v.random_scaled) {
    const r = v.random_scaled;
    return [fc.integer({ min: r.min, max: r.max }).map(n => n / r.divisor)];
  }
  if (v.random_string) return [fc.string({ maxLength: v.random_string.max_len })];
  if (v.random_bytes) return [fc.uint8Array({ maxLength: v.random_bytes.max_len }).map(a => Buffer.from(a))];
  if (v.oneof) {
    // JS can't represent i64 extremes exactly; clamp to safe integer range.
    // Emit one fc.constant per value (matches original weight distribution).
    return v.oneof.map(n =>
      fc.constant(Math.max(Number.MIN_SAFE_INTEGER, Math.min(Number.MAX_SAFE_INTEGER, n)))
    );
  }
  if (v.oneof_float) {
    // JS drivers can't reliably round-trip f64::MAX/-f64::MAX through HTTP;
    // only keep 0.0 (the non-extreme entry).
    return [fc.constant(0.0)];
  }
  if (id === 'large_or_unicode') {
    return [
      fc.constant(Buffer.alloc(256, 0xAB)),
      fc.constantFrom(..._SPEC.unicode_options),
    ];
  }
  if (v.literal !== undefined) return [fc.constant(v.literal)];
  if (v.literal_float !== undefined) return [fc.constant(v.literal_float)];
  if (v.literal_bytes !== undefined) return [fc.constant(Buffer.alloc(0))];
  if (v.fill_bytes) return [fc.constant(Buffer.alloc(v.fill_bytes.len, v.fill_bytes.byte))];
  if (v.repeat_char) return [fc.constant(v.repeat_char.char.repeat(v.repeat_char.len))];
  throw new Error(`unknown value spec: ${id}`);
}

const arbValue = fc.oneof(..._SPEC.values.flatMap(_buildValueArbs));

// Generate 1-5 columns with unique names (c0..c4)
const arbDynamicCols = fc.integer({ min: 1, max: 5 }).chain(n =>
  fc.tuple(
    ...Array.from({ length: n }, (_, i) =>
      fc.constantFrom(...COL_TYPES).map(t => [`c${i}`, t])
    )
  )
);

const arbBatchSQL = fc.tuple(
  fc.integer({ min: 0, max: _SPEC.constants.num_tables - 1 }),
  fc.integer({ min: -1000, max: 1000 }),
).map(([tbl, a]) =>
  `CREATE TABLE IF NOT EXISTS t_${tbl} (a INTEGER, b TEXT); INSERT INTO t_${tbl} VALUES (${a}, 'batch')`
);

// DML ops safe to use inside a transaction (no BEGIN/COMMIT/ROLLBACK).
const _DML_OP_IDS = new Set([
  'create', 'insert', 'select', 'select_value',
  'insert_returning', 'delete_returning', 'update_returning',
  'select_limit', 'select_count', 'select_expr',
  'insert_affected', 'delete_affected', 'update_affected',
]);

// Build op arbitraries dynamically from spec
function _buildOpArb(opSpec) {
  const id = opSpec.id;
  const fields = opSpec.fields || {};

  if (['begin', 'commit', 'rollback'].includes(id)) {
    return fc.constant({ kind: id });
  }
  if (id === 'invalid') {
    return fc.constant({ kind: 'invalid', sql: opSpec.sql });
  }

  // Table-only ops
  if ('table' in fields && Object.keys(fields).length === 1 && fields.table === 'table_name') {
    return arbTableName.map(t => ({ kind: id, table: t }));
  }

  // Table + values
  if (fields.table === 'table_name' && fields.values === 'table_values') {
    return fc.tuple(arbTableName, arbValue, arbValue).map(([t, a, b]) => ({
      kind: id, table: t, values: [a, b]
    }));
  }

  // Table + one value
  if (fields.table === 'table_name' && fields.value === 'one_value') {
    return fc.tuple(arbTableName, arbValue).map(([t, v]) => ({
      kind: id, table: t, value: v
    }));
  }

  // Table + dynamic cols
  if (fields.cols === 'dynamic_cols') {
    return fc.tuple(arbTableName, arbDynamicCols).map(([t, cols]) => ({
      kind: id, table: t, cols
    }));
  }

  // Select value with expr
  if (fields.expr === 'random_int_str') {
    return fc.integer({ min: -1000, max: 1000 }).map(v => ({
      kind: id, expr: `${v}`
    }));
  }

  // Param with two values and static sql
  if (fields.params === 'two_values') {
    // Only include sql if it's a real SQL statement (not a template with placeholders like {numbered_cols})
    const sql = opSpec.sql;
    const includesSql = sql && !sql.includes('{');
    return fc.tuple(arbValue, arbValue).map(([a, b]) => ({
      kind: id, ...(includesSql ? { sql } : {}), params: [a, b]
    }));
  }

  // Named params
  if (fields.named === 'named_values') {
    const names = opSpec.named_params || [];
    return fc.tuple(...names.map(() => arbValue)).map(vals => {
      const named = {};
      names.forEach((n, i) => { named[n] = vals[i]; });
      return { kind: id, named };
    });
  }

  // Select expr with one value
  if (fields.value === 'one_value' && !fields.table) {
    return arbValue.map(v => ({ kind: id, value: v }));
  }

  // Batch SQL
  if (fields.sql === 'batch_sql') {
    return arbBatchSQL.map(sql => ({ kind: id, sql }));
  }

  // Error check
  if (fields.sql === 'error_sql') {
    return fc.constantFrom(..._SPEC.constants.error_sqls).map(sql => ({
      kind: id, sql
    }));
  }

  // Prepared reuse
  if (fields.params_sets === 'three_param_pairs') {
    return fc.tuple(arbValue, arbValue, arbValue, arbValue, arbValue, arbValue).map(
      ([a1, b1, a2, b2, a3, b3]) => ({
        kind: id, paramsSets: [[a1, b1], [a2, b2], [a3, b3]]
      })
    );
  }

  // Transaction workflow: 1-5 DML ops, then commit or rollback
  if (fields.inner_ops === 'dml_ops' && fields.commit === 'bool') {
    return fc.tuple(
      fc.array(arbDmlOp, { minLength: 1, maxLength: 5 }),
      fc.boolean()
    ).map(([innerOps, commit]) => ({ kind: id, innerOps, commit }));
  }

  // Error in transaction: good op, bad SQL, recovery op
  if (fields.good_op === 'dml_op' && fields.bad_sql === 'error_sql') {
    return fc.tuple(
      arbDmlOp,
      fc.constantFrom(..._SPEC.constants.error_sqls),
      arbDmlOp,
    ).map(([goodOp, badSql, recoveryOp]) => ({ kind: id, goodOp, badSql, recoveryOp }));
  }

  // Fallback: constant
  return fc.constant({ kind: id });
}

const arbDmlOp = fc.oneof(..._SPEC.ops.filter(o => _DML_OP_IDS.has(o.id)).map(_buildOpArb));
const arbOp = fc.oneof(..._SPEC.ops.map(_buildOpArb));

// ---------------------------------------------------------------------------
// Execute an op against a driver adapter
// ---------------------------------------------------------------------------

// Build SQL index from spec for template-driven buildSql
const _OP_SQL_MAP = Object.fromEntries(_SPEC.ops.map(o => [o.id, o.sql]));

function buildSql(op) {
  const template = _OP_SQL_MAP[op.kind];
  if (!template) return { sql: 'SELECT 1' };

  // Dynamic column definitions
  if (op.kind === 'create_dynamic') {
    const colDefs = op.cols.map(([n, t]) => `${n} ${t}`).join(', ');
    return { sql: template.replace('{col_defs}', colDefs).replace('{table}', op.table) };
  }

  // Build SQL from template
  let sql = template;
  if (op.table) sql = sql.replaceAll('{table}', op.table);
  if (op.expr) sql = sql.replace('{expr}', op.expr);
  if (op.sql && (op.kind === 'invalid' || op.kind === 'batch' || op.kind === 'error_check')) {
    return { sql: op.sql };
  }

  // Handle placeholders
  if (op.values) {
    const placeholders = op.values.map(() => '?').join(', ');
    sql = sql.replace('{placeholders}', placeholders);
    return { sql, params: op.values };
  }
  if (op.value !== undefined) {
    return { sql, params: [op.value] };
  }
  if (op.params) {
    return { sql: op.sql || sql, params: op.params };
  }

  // Named/numbered cols from template
  if (sql.includes('{named_cols}') && op.named) {
    const cols = Object.keys(op.named).map(n => `:${n}`).join(', ');
    return { sql: sql.replace('{named_cols}', cols), params: op.named };
  }
  if (sql.includes('{numbered_cols}') && op.params) {
    const cols = op.params.map((_, i) => `?${i + 1}`).join(', ');
    return { sql: sql.replace('{numbered_cols}', cols), params: op.params };
  }

  return { sql };
}

// ---------------------------------------------------------------------------
// Special op handlers for insert_rowid and batch
// ---------------------------------------------------------------------------

async function executeOpLocal(db, op) {
  if (op.kind === 'prepared_reuse') {
    try {
      const stmt = await db.prepare('SELECT ?, ?');
      stmt.raw(true);
      const allRows = [];
      for (const params of op.paramsSets) {
        const rows = await stmt.all(params);
        for (const row of rows) allRows.push(Array.from(row));
      }
      return {
        success: true,
        columnCount: 2,
        columnNames: ['?', '?'],
        rowCount: allRows.length,
        valueTypes: allRows.map(row => row.map(typeTag)),
        values: allRows,
      };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'error_check') {
    const result = await executeLocal(db, op.sql);
    return { success: result.success };
  }
  if (op.kind === 'named_param') {
    const names = Object.keys(op.named);
    const sql = `SELECT ${names.map(n => `:${n}`).join(', ')}`;
    // NAPI native driver expects an object for named params
    return executeLocal(db, sql, op.named);
  }
  if (op.kind === 'numbered_param') {
    const sql = `SELECT ${op.params.map((_, i) => `?${i + 1}`).join(', ')}`;
    return executeLocal(db, sql, op.params);
  }
  if (op.kind === 'create_trigger') {
    try {
      const audit = `${op.table}_audit`;
      await executeLocal(db, `CREATE TABLE IF NOT EXISTS ${audit} (src TEXT, val)`);
      const triggerSql = `CREATE TRIGGER IF NOT EXISTS tr_${op.table}_ins AFTER INSERT ON ${op.table} BEGIN INSERT INTO ${audit} VALUES ('${op.table}', NEW.a); INSERT INTO ${audit} VALUES ('${op.table}', NEW.a * 2); END`;
      await executeLocal(db, triggerSql);
      await executeLocal(db, `INSERT INTO ${op.table} VALUES (42, 'trigger_test')`);
      return executeLocal(db, `SELECT * FROM ${audit} ORDER BY rowid`);
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'insert_rowid') {
    const { sql, params } = buildSql(op);
    try {
      const stmt = await db.prepare(sql);
      await stmt.run(params);
      // Read last_insert_rowid via SQL
      const ridStmt = await db.prepare('SELECT last_insert_rowid()');
      ridStmt.raw(true);
      const rows = await ridStmt.all();
      const rowid = rows.length > 0 ? rows[0][0] : null;
      return { success: true, lastInsertRowid: rowid };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'batch') {
    // Split on semicolons and execute each statement
    const stmts = op.sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
    try {
      for (const s of stmts) {
        await executeLocal(db, s);
      }
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'transaction_workflow') {
    try {
      await executeLocal(db, 'BEGIN');
      for (const inner of op.innerOps) {
        const { sql, params } = buildSql(inner);
        try { await executeLocal(db, sql, params); } catch {}
      }
      await executeLocal(db, op.commit ? 'COMMIT' : 'ROLLBACK');
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'error_in_transaction') {
    try {
      await executeLocal(db, 'BEGIN');
      const { sql: goodSql, params: goodParams } = buildSql(op.goodOp);
      try { await executeLocal(db, goodSql, goodParams); } catch {}
      try { await executeLocal(db, op.badSql); } catch {}
      const { sql: recSql, params: recParams } = buildSql(op.recoveryOp);
      try { await executeLocal(db, recSql, recParams); } catch {}
      await executeLocal(db, 'ROLLBACK');
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  const { sql, params } = buildSql(op);
  return executeLocal(db, sql, params);
}

async function executeOpRemote(conn, op) {
  if (op.kind === 'prepared_reuse') {
    try {
      const allRows = [];
      for (const params of op.paramsSets) {
        const result = await conn.execute('SELECT ?, ?', params);
        for (const row of result.rows) allRows.push(Array.from(row));
      }
      return {
        success: true,
        columnCount: 2,
        columnNames: ['?', '?'],
        rowCount: allRows.length,
        valueTypes: allRows.map(row => row.map(typeTag)),
        values: allRows,
      };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'error_check') {
    const result = await executeRemote(conn, op.sql);
    return { success: result.success };
  }
  if (op.kind === 'named_param') {
    const names = Object.keys(op.named);
    const sql = `SELECT ${names.map(n => `:${n}`).join(', ')}`;
    // Serverless driver expects an object for named params
    return executeRemote(conn, sql, op.named);
  }
  if (op.kind === 'numbered_param') {
    const sql = `SELECT ${op.params.map((_, i) => `?${i + 1}`).join(', ')}`;
    return executeRemote(conn, sql, op.params);
  }
  if (op.kind === 'create_trigger') {
    try {
      const audit = `${op.table}_audit`;
      await executeRemote(conn, `CREATE TABLE IF NOT EXISTS ${audit} (src TEXT, val)`);
      const triggerSql = `CREATE TRIGGER IF NOT EXISTS tr_${op.table}_ins AFTER INSERT ON ${op.table} BEGIN INSERT INTO ${audit} VALUES ('${op.table}', NEW.a); INSERT INTO ${audit} VALUES ('${op.table}', NEW.a * 2); END`;
      await executeRemote(conn, triggerSql);
      await executeRemote(conn, `INSERT INTO ${op.table} VALUES (42, 'trigger_test')`);
      return executeRemote(conn, `SELECT * FROM ${audit} ORDER BY rowid`);
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'insert_rowid') {
    const { sql, params } = buildSql(op);
    try {
      await conn.execute(sql, params);
      const ridResult = await conn.execute('SELECT last_insert_rowid()');
      const rowid = ridResult.rows.length > 0 ? ridResult.rows[0][0] : null;
      return { success: true, lastInsertRowid: rowid };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'batch') {
    const stmts = op.sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
    try {
      for (const s of stmts) {
        await executeRemote(conn, s);
      }
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'transaction_workflow') {
    try {
      await executeRemote(conn, 'BEGIN');
      for (const inner of op.innerOps) {
        const { sql, params } = buildSql(inner);
        try { await executeRemote(conn, sql, params); } catch {}
      }
      await executeRemote(conn, op.commit ? 'COMMIT' : 'ROLLBACK');
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  if (op.kind === 'error_in_transaction') {
    try {
      await executeRemote(conn, 'BEGIN');
      const { sql: goodSql, params: goodParams } = buildSql(op.goodOp);
      try { await executeRemote(conn, goodSql, goodParams); } catch {}
      try { await executeRemote(conn, op.badSql); } catch {}
      const { sql: recSql, params: recParams } = buildSql(op.recoveryOp);
      try { await executeRemote(conn, recSql, recParams); } catch {}
      await executeRemote(conn, 'ROLLBACK');
      return { success: true, columnCount: 0, rowCount: 0 };
    } catch {
      return { success: false };
    }
  }
  const { sql, params } = buildSql(op);
  return executeRemote(conn, sql, params);
}

// ---------------------------------------------------------------------------
// Result comparison
// ---------------------------------------------------------------------------

function compareResults(localResult, remoteResult, i, op) {
  if (localResult.success !== remoteResult.success) {
    throw new Error(
      `success mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (!localResult.success) return;

  if (localResult.columnCount !== remoteResult.columnCount) {
    throw new Error(
      `columnCount mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (JSON.stringify(localResult.columnNames) !== JSON.stringify(remoteResult.columnNames)) {
    throw new Error(
      `columnNames mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (localResult.rowCount !== remoteResult.rowCount) {
    throw new Error(
      `rowCount mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (JSON.stringify(localResult.valueTypes) !== JSON.stringify(remoteResult.valueTypes)) {
    throw new Error(
      `valueTypes mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (!valuesEqual(localResult.values, remoteResult.values)) {
    throw new Error(
      `values mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (localResult.affectedRows !== undefined &&
      localResult.affectedRows !== remoteResult.affectedRows) {
    throw new Error(
      `affectedRows mismatch on op #${i} ${JSON.stringify(op)}\n` +
      `  local:  ${JSON.stringify(localResult)}\n` +
      `  remote: ${JSON.stringify(remoteResult)}`
    );
  }
  if (localResult.lastInsertRowid !== undefined) {
    if (!cellsEqual(localResult.lastInsertRowid, remoteResult.lastInsertRowid)) {
      throw new Error(
        `lastInsertRowid mismatch on op #${i} ${JSON.stringify(op)}\n` +
        `  local:  ${JSON.stringify(localResult)}\n` +
        `  remote: ${JSON.stringify(remoteResult)}`
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Prefix helper — makes table names unique per test case so parallel tests
// don't interfere. Transforms "t_N" → "t_{prefix}_N" in table fields and SQL.
// ---------------------------------------------------------------------------

function applyPrefix(op, prefix) {
  op = { ...op };
  const re = /\bt_(\d+)\b/g;
  const repl = `t_${prefix}_$1`;
  if (op.table) op.table = op.table.replace(re, repl);
  if (op.kind === 'batch' || op.kind === 'error_check') {
    op.sql = op.sql.replace(re, repl);
  }
  if (op.innerOps) op.innerOps = op.innerOps.map(o => applyPrefix(o, prefix));
  if (op.goodOp) op.goodOp = applyPrefix(op.goodOp, prefix);
  if (op.recoveryOp) op.recoveryOp = applyPrefix(op.recoveryOp, prefix);
  if (op.badSql) op.badSql = op.badSql.replace(re, repl);
  return op;
}

// ---------------------------------------------------------------------------
// The property test
// ---------------------------------------------------------------------------

test.serial('api parity: local vs remote produce identical results', async (t) => {
  try {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 65535 }),
        fc.array(arbOp, { minLength: 1, maxLength: _SPEC.constants.max_ops_per_case }),
        async (prefix, rawOps) => {
          const ops = rawOps.map(op => applyPrefix(op, prefix));
          // Fresh connections per iteration — no stale transaction state.
          const localDb = await nativeConnect(':memory:');
          const remoteConn = serverlessConnect({ url: serverUrl, authToken });
          try {
            // Drop any leftover tables for this prefix (from prior runs or fc replays).
            for (let i = 0; i < _SPEC.constants.num_tables; i++) {
              await executeRemote(remoteConn, `DROP TABLE IF EXISTS t_${prefix}_${i}`);
              await executeRemote(remoteConn, `DROP TABLE IF EXISTS t_${prefix}_${i}_audit`);
              await executeRemote(remoteConn, `DROP TRIGGER IF EXISTS tr_t_${prefix}_${i}_ins`);
            }

            // Pre-pass: track table schemas and adjust insert value counts
            const tableCols = new Map();
            for (const op of ops) {
              if (op.kind === 'create') tableCols.set(op.table, 2);
              if (op.kind === 'create_dynamic') tableCols.set(op.table, op.cols.length);
              // For insert-like ops, adjust values count
              if (['insert', 'insert_returning', 'insert_affected', 'insert_rowid'].includes(op.kind)) {
                const n = tableCols.get(op.table) ?? 2;
                while (op.values.length < n) op.values.push(null);
                if (op.values.length > n) op.values.length = n;
              }
            }

            // Accumulate an operation trace so failures show the full history.
            const trace = [];

            for (let i = 0; i < ops.length; i++) {
              const op = ops[i];

              const localResult = await executeOpLocal(localDb, op);
              const remoteResult = await executeOpRemote(remoteConn, op);

              trace.push(
                `  op[${i}]: kind=${op.kind} table=${op.table ?? ''}\n` +
                `    local:  ok=${localResult.success} cols=${localResult.columnCount} rows=${localResult.rowCount}\n` +
                `    remote: ok=${remoteResult.success} cols=${remoteResult.columnCount} rows=${remoteResult.rowCount}`
              );
              const traceDump = `\n\nFull trace (prefix=${prefix}):\n${trace.join('\n')}`;

              // ErrorCheck only compares success/failure -- error messages legitimately differ.
              if (op.kind === 'error_check') {
                if (localResult.success !== remoteResult.success) {
                  throw new Error(
                    `success mismatch on op #${i} ${JSON.stringify(op)}\n` +
                    `  local:  ${JSON.stringify(localResult)}\n` +
                    `  remote: ${JSON.stringify(remoteResult)}` + traceDump
                  );
                }
                continue;
              }

              try {
                compareResults(localResult, remoteResult, i, op);
              } catch (e) {
                throw new Error(e.message + traceDump);
              }

              // If both failed, stop — continuing with diverged implicit
              // transaction state leads to false positives.
              if (!localResult.success) break;
            }
          } finally {
            await remoteConn.close();
            await localDb.close();
          }
        }
      ),
      { numRuns: 100, verbose: 1 }
    );
    t.pass();
  } catch (e) {
    t.fail(e.message);
  }
});

// ---------------------------------------------------------------------------
// Error recovery property: errors must never prevent subsequent commands
// ---------------------------------------------------------------------------

const arbErrorSQL = fc.oneof(
  // Missing table
  fc.constant('SELECT * FROM nonexistent_table_xyz'),
  fc.constant('SELECT * FROM nonexistent_table_abc'),
  fc.constant('SELECT * FROM nonexistent_table_zzz'),
  fc.constant('INSERT INTO nonexistent_table_xyz VALUES (1)'),
  // Wrong param count (table may or may not exist — error either way)
  fc.constant('INSERT INTO t_0 VALUES (?, ?, ?)'),
  // Type mismatch in function
  fc.constant("SELECT length(1, 2, 3)"),
  // Division by zero (not an error in SQLite, but good stress)
  fc.constant('SELECT 1/0'),
);

test.serial('error recovery: errors never prevent subsequent commands', async (t) => {
  try {
    await fc.assert(
      fc.asyncProperty(arbErrorSQL, async (errorSQL) => {
        const conn = serverlessConnect({ url: serverUrl, authToken });
        try {
          // Send the error-inducing SQL (may or may not fail)
          try {
            // For multi-statement errors, split and execute each
            const stmts = errorSQL.split(';').map(s => s.trim()).filter(s => s);
            for (const s of stmts) {
              await conn.execute(s);
            }
          } catch {
            // Expected — the point is to cause an error
          }

          // The critical assertion: SELECT 1 must succeed afterward
          const result = await conn.execute('SELECT 1');
          if (result.rows.length !== 1 || result.rows[0][0] !== 1) {
            throw new Error(
              `SELECT 1 returned unexpected result after error SQL: ${errorSQL}\n` +
              `  result: ${JSON.stringify(result.rows)}`
            );
          }
        } finally {
          try { await conn.close(); } catch {}
        }
      }),
      { numRuns: 50, verbose: 1 }
    );
    t.pass();
  } catch (e) {
    t.fail(e.message);
  }
});

// ---------------------------------------------------------------------------
// DDL visibility in transactions: CREATE TABLE must be visible to subsequent
// statements within the same transaction.
// ---------------------------------------------------------------------------

test.serial('ddl in transaction: CREATE TABLE visible to INSERT in same txn', async (t) => {
  try {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 200000, max: 265535 }),
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: -1000, max: 1000 }),
        async (prefix, tableIdx, val) => {
          const localDb = await nativeConnect(':memory:');
          const remoteConn = serverlessConnect({ url: serverUrl, authToken });
          try {
            const table = `t_${prefix}_${tableIdx}`;

            // Drop any leftover from prior runs so the INSERT produces exactly 1 row.
            await executeRemote(remoteConn, `DROP TABLE IF EXISTS ${table}`);

            const createSql = `CREATE TABLE IF NOT EXISTS ${table} (a INTEGER, b TEXT)`;
            const insertSql = `INSERT INTO ${table} VALUES (?, 'txn_ddl')`;
            const selectSql = `SELECT a FROM ${table}`;

            // Local: BEGIN → CREATE → INSERT → SELECT → COMMIT
            await executeLocal(localDb, 'BEGIN');
            await executeLocal(localDb, createSql);
            await executeLocal(localDb, insertSql, [val]);
            const localInner = await executeLocal(localDb, selectSql);
            if (!localInner.success || localInner.rowCount !== 1) {
              throw new Error(
                `local: SELECT inside txn failed after CREATE+INSERT: ${JSON.stringify(localInner)}`
              );
            }
            await executeLocal(localDb, 'COMMIT');

            // Remote: BEGIN → CREATE → INSERT → SELECT → COMMIT
            const remoteBegin = await executeRemote(remoteConn, 'BEGIN');
            if (!remoteBegin.success) {
              throw new Error('remote: BEGIN failed');
            }
            const remoteCreate = await executeRemote(remoteConn, createSql);
            if (!remoteCreate.success) {
              throw new Error('remote: CREATE TABLE failed');
            }
            const remoteInsert = await executeRemote(remoteConn, insertSql, [val]);
            if (!remoteInsert.success) {
              throw new Error('remote: INSERT failed');
            }
            const remoteInner = await executeRemote(remoteConn, selectSql);
            if (!remoteInner.success || remoteInner.rowCount !== 1) {
              throw new Error(
                `remote: SELECT inside txn failed after CREATE+INSERT: ${JSON.stringify(remoteInner)}`
              );
            }
            await executeRemote(remoteConn, 'COMMIT');
          } finally {
            await remoteConn.close();
            await localDb.close();
          }
        }
      ),
      { numRuns: 50, verbose: 1 }
    );
    t.pass();
  } catch (e) {
    t.fail(e.message);
  }
});

// ---------------------------------------------------------------------------
// DDL + prepare() in transactions: prepare() calls describe which must see
// tables created earlier in the same transaction. This catches bugs where
// describe opens a new session without transaction context (issue #6562).
//
// ---------------------------------------------------------------------------

test.serial('ddl prepare in transaction: prepare() sees CREATE TABLE in same txn', async (t) => {
  try {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 300000, max: 365535 }),
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: -1000, max: 1000 }),
        async (prefix, tableIdx, val) => {
          const localDb = await nativeConnect(':memory:');
          const remoteConn = serverlessConnect({ url: serverUrl, authToken });
          try {
            const table = `t_${prefix}_${tableIdx}`;

            // Drop any leftover from prior runs so the INSERT produces exactly 1 row.
            await executeRemote(remoteConn, `DROP TABLE IF EXISTS ${table}`);

            const createSql = `CREATE TABLE IF NOT EXISTS ${table} (a INTEGER, b TEXT)`;
            const insertSql = `INSERT INTO ${table} VALUES (?, 'txn_ddl')`;
            const selectSql = `SELECT a FROM ${table}`;

            // Local: BEGIN → CREATE → prepare(INSERT) → run → SELECT → COMMIT
            await executeLocal(localDb, 'BEGIN');
            await executeLocal(localDb, createSql);
            const localStmt = await localDb.prepare(insertSql);
            await localStmt.run([val]);
            const localInner = await executeLocal(localDb, selectSql);
            if (!localInner.success || localInner.rowCount !== 1) {
              throw new Error(
                `local: SELECT inside txn failed after CREATE+prepare(INSERT): ${JSON.stringify(localInner)}`
              );
            }
            await executeLocal(localDb, 'COMMIT');

            // Remote: BEGIN → CREATE → prepare(INSERT) → all → SELECT → COMMIT
            // This exercises the describe path which opens a new session in the
            // serverless driver — known to fail (issue #6562).
            await executeRemote(remoteConn, 'BEGIN');
            await executeRemote(remoteConn, createSql);
            const remoteStmt = await remoteConn.prepare(insertSql);
            await remoteStmt.run([val]);
            const remoteInner = await executeRemote(remoteConn, selectSql);
            if (!remoteInner.success || remoteInner.rowCount !== 1) {
              throw new Error(
                `remote: SELECT inside txn failed after CREATE+prepare(INSERT): ${JSON.stringify(remoteInner)}`
              );
            }
            await executeRemote(remoteConn, 'COMMIT');
          } finally {
            await remoteConn.close();
            await localDb.close();
          }
        }
      ),
      { numRuns: 50, verbose: 1 }
    );
    t.pass();
  } catch (e) {
    t.fail(e.message);
  }
});

// ---------------------------------------------------------------------------
// API surface parity: native public methods must exist on serverless
// ---------------------------------------------------------------------------

function getPublicMethods(obj) {
  const methods = new Set();
  // Walk prototype chain (skip Object.prototype)
  let proto = obj;
  while (proto && proto !== Object.prototype) {
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (name.startsWith('_') || name === 'constructor') continue;
      methods.add(name);
    }
    proto = Object.getPrototypeOf(proto);
  }
  return methods;
}

// TODO: JS APIs don't match yet — re-enable once native and serverless
// drivers expose the same public surface.
test.serial.skip('api surface parity: native vs serverless public methods', async (t) => {
  // Members that only make sense on one driver.
  const LOCAL_ONLY_DB = new Set();
  const REMOTE_ONLY_DB = new Set();
  const LOCAL_ONLY_STMT = new Set();
  const REMOTE_ONLY_STMT = new Set();

  // Open both drivers
  const localDb = await nativeConnect(':memory:');
  const remoteConn = serverlessConnect({ url: serverUrl, authToken });

  try {
    const localDbApi = getPublicMethods(localDb);
    const remoteDbApi = getPublicMethods(remoteConn);

    // Database/Connection level
    const missingOnRemote = [];
    for (const m of localDbApi) {
      if (!remoteDbApi.has(m) && !LOCAL_ONLY_DB.has(m)) {
        missingOnRemote.push(m);
      }
    }
    const missingOnLocal = [];
    for (const m of remoteDbApi) {
      if (!localDbApi.has(m) && !REMOTE_ONLY_DB.has(m)) {
        missingOnLocal.push(m);
      }
    }

    // Statement level
    const localStmt = await localDb.prepare('SELECT 1');
    // Serverless doesn't have prepare() on the connection object — use a
    // different approach: execute returns a result, not a statement.
    // We need to check if serverless exposes a prepare() that returns a
    // statement-like object.
    let remoteStmt = null;
    try {
      remoteStmt = await remoteConn.prepare('SELECT 1');
    } catch {
      // If prepare doesn't exist or fails, that's a gap too
    }

    const stmtMissingOnRemote = [];
    const stmtMissingOnLocal = [];
    if (remoteStmt) {
      const localStmtApi = getPublicMethods(localStmt);
      const remoteStmtApi = getPublicMethods(remoteStmt);

      for (const m of localStmtApi) {
        if (!remoteStmtApi.has(m) && !LOCAL_ONLY_STMT.has(m)) {
          stmtMissingOnRemote.push(m);
        }
      }
      for (const m of remoteStmtApi) {
        if (!localStmtApi.has(m) && !REMOTE_ONLY_STMT.has(m)) {
          stmtMissingOnLocal.push(m);
        }
      }
    }

    const errors = [];
    if (missingOnRemote.length > 0) {
      errors.push(`Remote Connection missing: [${missingOnRemote.sort().join(', ')}]`);
    }
    if (missingOnLocal.length > 0) {
      errors.push(`Local Connection missing: [${missingOnLocal.sort().join(', ')}]`);
    }
    if (stmtMissingOnRemote.length > 0) {
      errors.push(`Remote Statement missing: [${stmtMissingOnRemote.sort().join(', ')}]`);
    }
    if (stmtMissingOnLocal.length > 0) {
      errors.push(`Local Statement missing: [${stmtMissingOnLocal.sort().join(', ')}]`);
    }

    if (errors.length > 0) {
      t.fail(`API surface mismatch:\n  ${errors.join('\n  ')}`);
    } else {
      t.pass();
    }
  } finally {
    await localDb.close();
    await remoteConn.close();
  }
});
