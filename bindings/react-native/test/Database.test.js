const { test } = require('node:test');
const assert = require('node:assert/strict');
const { TursoStatus } = require('../lib/commonjs/types');
const { Database } = require('../lib/commonjs/Database');

function createMockStatement() {
  return {
    execute: () => ({ status: TursoStatus.DONE, rowsChanged: 0 }),
    reset: () => {},
    finalize: () => TursoStatus.DONE,
  };
}

test('Database.exec slices multi-statement SQL using remaining instead of original sql', async () => {
  const db = new Database({ path: ':memory:' });
  const executedStatements = [];
  const remainingPassedToPrepareFirst = [];

  const mockStatement = createMockStatement();

  const mockConn = {
    prepareFirst: (sql) => {
      remainingPassedToPrepareFirst.push(sql);
      const semi = sql.indexOf(';');
      if (semi === -1) return null;
      const stmtSql = sql.substring(0, semi + 1);
      executedStatements.push(stmtSql);
      return {
        statement: mockStatement,
        tailIdx: semi + 1,
      };
    },
    lastInsertRowid: () => 0,
  };

  db._connected = true;
  db._connection = mockConn;

  // Multi-statement SQL with leading whitespace and multiple statements
  const multiSql = '\n  CREATE TABLE a(x);\n  CREATE TABLE b(y);\n  CREATE TABLE c(z);\n';
  await db.exec(multiSql);

  assert.deepEqual(executedStatements, [
    'CREATE TABLE a(x);',
    'CREATE TABLE b(y);',
    'CREATE TABLE c(z);',
  ]);

  assert.deepEqual(remainingPassedToPrepareFirst, [
    'CREATE TABLE a(x);\n  CREATE TABLE b(y);\n  CREATE TABLE c(z);',
    'CREATE TABLE b(y);\n  CREATE TABLE c(z);',
    'CREATE TABLE c(z);',
  ]);
});

test('Database.exec handles empty or whitespace-only SQL without calling prepareFirst', async () => {
  const db = new Database({ path: ':memory:' });
  let called = false;

  db._connected = true;
  db._connection = {
    prepareFirst: () => {
      called = true;
      return null;
    },
    lastInsertRowid: () => 0,
  };

  await db.exec('   \n\t  ');
  assert.equal(called, false);
});

test('Database.exec guards against non-positive tailIdx to avoid infinite loops', async () => {
  const db = new Database({ path: ':memory:' });
  let iterations = 0;

  db._connected = true;
  db._connection = {
    prepareFirst: () => {
      iterations++;
      return {
        statement: createMockStatement(),
        tailIdx: 0,
      };
    },
    lastInsertRowid: () => 0,
  };

  await db.exec('SELECT 1;');
  assert.equal(iterations, 1);
});
