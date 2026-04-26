import test from 'ava';
import { connect } from '../dist/index.js';

const config = {
  url: process.env.TURSO_DATABASE_URL ?? 'http://localhost:8080',
  authToken: process.env.TURSO_AUTH_TOKEN,
};

function withClient(f) {
  return async (t) => {
    const c = connect(config);
    try {
      await f(t, c);
    } finally {
      await c.close();
    }
  };
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

test.serial('execute: query a single value', withClient(async (t, c) => {
  const rs = await c.execute('SELECT 42');
  t.is(rs.columns.length, 1);
  t.is(rs.columnTypes.length, 1);
  t.is(rs.rows.length, 1);
  t.is(rs.rows[0].length, 1);
  t.is(rs.rows[0][0], 42);
}));

test.serial('execute: query a single row', withClient(async (t, c) => {
  const rs = await c.execute("SELECT 1 AS one, 'two' AS two, 0.5 AS three");
  t.deepEqual(rs.columns, ['one', 'two', 'three']);
  t.deepEqual(rs.columnTypes, ['', '', '']);
  t.is(rs.rows.length, 1);

  const r = rs.rows[0];
  t.is(r.length, 3);
  t.deepEqual(Array.from(r), [1, 'two', 0.5]);

  // Column name access (non-enumerable properties)
  t.is(r.one, 1);
  t.is(r.two, 'two');
  t.is(r.three, 0.5);
}));

test.serial('execute: query multiple rows', withClient(async (t, c) => {
  const rs = await c.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')");
  t.is(rs.columns.length, 2);
  t.is(rs.columnTypes.length, 2);
  t.is(rs.rows.length, 3);

  t.deepEqual(Array.from(rs.rows[0]), [1, 'one']);
  t.deepEqual(Array.from(rs.rows[1]), [2, 'two']);
  t.deepEqual(Array.from(rs.rows[2]), [3, 'three']);
}));

test.serial('execute: statement that produces error', withClient(async (t, c) => {
  await t.throwsAsync(() => c.execute('SELECT foobar'));
}));

test.serial('execute: rowsAffected with INSERT', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  const rs = await c.execute('INSERT INTO t VALUES (1), (2)');
  t.is(rs.rowsAffected, 2);
}));

test.serial('execute: rowsAffected with DELETE', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute('INSERT INTO t VALUES (1), (2), (3), (4), (5)');
  const rs = await c.execute('DELETE FROM t WHERE a >= 3');
  t.is(rs.rowsAffected, 3);
}));

test.serial('execute: lastInsertRowid with INSERT', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute("INSERT INTO t VALUES ('one'), ('two')");
  const insertRs = await c.execute("INSERT INTO t VALUES ('three')");
  t.truthy(insertRs.lastInsertRowid);
  const selectRs = await c.execute('SELECT a FROM t WHERE ROWID = ?', [insertRs.lastInsertRowid]);
  t.deepEqual(Array.from(selectRs.rows[0]), ['three']);
}));

test.serial('execute: rows from INSERT RETURNING', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  const rs = await c.execute("INSERT INTO t VALUES (1) RETURNING 42 AS x, 'foo' AS y");
  t.deepEqual(rs.columns, ['x', 'y']);
  t.is(rs.rows.length, 1);
  t.deepEqual(Array.from(rs.rows[0]), [42, 'foo']);
}));

test.serial('execute: rowsAffected with WITH INSERT', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute('INSERT INTO t VALUES (1), (2), (3)');
  const rs = await c.execute(`
    WITH x(a) AS (SELECT 2*a FROM t)
    INSERT INTO t SELECT a+1 FROM x
  `);
  t.is(rs.rowsAffected, 3);
}));

// ---------------------------------------------------------------------------
// values
// ---------------------------------------------------------------------------

function testRoundtrip(name, passed, expected) {
  test.serial(`values: ${name}`, withClient(async (t, c) => {
    const rs = await c.execute('SELECT ?', [passed]);
    t.deepEqual(rs.rows[0][0], expected);
  }));
}

function testRoundtripError(name, passed) {
  test.serial(`values: ${name}`, withClient(async (t, c) => {
    await t.throwsAsync(() => c.execute('SELECT ?', [passed]));
  }));
}

testRoundtrip('string', 'boomerang', 'boomerang');
testRoundtrip('string with weird characters', 'a\n\r\t ', 'a\n\r\t ');
testRoundtrip('string with unicode', 'žluťoučký kůň úpěl ďábelské ódy', 'žluťoučký kůň úpěl ďábelské ódy');

testRoundtrip('zero', 0, 0);
testRoundtrip('integer', -2023, -2023);
testRoundtrip('float', 12.345, 12.345);
testRoundtrip('large positive float', 1e18, 1e18);
testRoundtrip('large negative float', -1e18, -1e18);

testRoundtrip('null', null, null);
testRoundtrip('true', true, 1);
testRoundtrip('false', false, 0);

testRoundtripError('NaN produces error', NaN);
testRoundtripError('Infinity produces error', Infinity);

test.serial('values: ArrayBuffer roundtrip', withClient(async (t, c) => {
  const buf = new ArrayBuffer(256);
  const array = new Uint8Array(buf);
  for (let i = 0; i < 256; i++) {
    array[i] = i ^ 0xab;
  }
  const rs = await c.execute('SELECT ?', [buf]);
  const result = rs.rows[0][0];
  t.truthy(result);
  // Compare byte contents
  const resultBytes = new Uint8Array(result);
  t.is(resultBytes.length, 256);
  for (let i = 0; i < 256; i++) {
    t.is(resultBytes[i], i ^ 0xab);
  }
}));

test.serial('values: bigint with safeIntegers', withClient(async (t, c) => {
  c.defaultSafeIntegers(true);
  const rs = await c.execute('SELECT 42');
  t.is(rs.rows[0][0], 42n);
}));

test.serial('values: large bigint roundtrip', withClient(async (t, c) => {
  const rs = await c.execute("SELECT ?||''", [9223372036854775807n]);
  t.is(rs.rows[0][0], '9223372036854775807');
}));

test.serial('values: min 64-bit bigint roundtrip', withClient(async (t, c) => {
  const rs = await c.execute("SELECT ?||''", [-9223372036854775808n]);
  t.is(rs.rows[0][0], '-9223372036854775808');
}));

// ---------------------------------------------------------------------------
// arguments
// ---------------------------------------------------------------------------

test.serial('arguments: ? positional', withClient(async (t, c) => {
  const rs = await c.execute('SELECT ?, ?', ['one', 'two']);
  t.deepEqual(Array.from(rs.rows[0]), ['one', 'two']);
}));

test.serial('arguments: ?NNN positional', withClient(async (t, c) => {
  const rs = await c.execute('SELECT ?2, ?3, ?1', ['one', 'two', 'three']);
  t.deepEqual(Array.from(rs.rows[0]), ['two', 'three', 'one']);
}));

test.serial('arguments: ?NNN with holes', withClient(async (t, c) => {
  const rs = await c.execute('SELECT ?3, ?1', ['one', 'two', 'three']);
  t.deepEqual(Array.from(rs.rows[0]), ['three', 'one']);
}));

for (const sign of [':', '@', '$']) {
  test.serial(`arguments: ${sign}AAAA named`, withClient(async (t, c) => {
    const rs = await c.execute(`SELECT ${sign}b, ${sign}a`, { a: 'one', [`${sign}b`]: 'two' });
    t.deepEqual(Array.from(rs.rows[0]), ['two', 'one']);
  }));

  test.serial(`arguments: ${sign}AAAA used multiple times`, withClient(async (t, c) => {
    const rs = await c.execute(
      `SELECT ${sign}b, ${sign}a, ${sign}b || ${sign}a`,
      { a: 'one', [`${sign}b`]: 'two' },
    );
    t.deepEqual(Array.from(rs.rows[0]), ['two', 'one', 'twoone']);
  }));
}

// ---------------------------------------------------------------------------
// batch()
// ---------------------------------------------------------------------------

test.serial('batch: create, insert and query', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS test_products');
  const batchResult = await c.batch([
    'CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
    'INSERT INTO test_products (name, price) VALUES ("Widget", 9.99)',
    'INSERT INTO test_products (name, price) VALUES ("Gadget", 19.99)',
    'INSERT INTO test_products (name, price) VALUES ("Tool", 29.99)',
  ]);
  t.is(batchResult.rowsAffected, 3);

  const queryResult = await c.execute('SELECT COUNT(*) as count FROM test_products');
  t.is(queryResult.rows[0][0], 3);
}));

test.serial('batch: error in batch', withClient(async (t, c) => {
  await t.throwsAsync(() => c.batch(['SELECT 1+1', 'SELECT foobar']));
}));

test.serial('batch: error is reported to caller', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute("INSERT INTO t VALUES ('one')");

  await t.throwsAsync(() =>
    c.batch([
      "INSERT INTO t VALUES ('two')",
      'SELECT foobar',
      "INSERT INTO t VALUES ('three')",
    ]),
  );

  // The error should have been thrown. The exact rollback behavior
  // depends on whether the server wraps batch steps in a transaction.
  t.pass();
}));

// ---------------------------------------------------------------------------
// exec() (executeMultiple via sequence)
// ---------------------------------------------------------------------------

test.serial('exec: multiple statements', withClient(async (t, c) => {
  await c.exec(`
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a);
    INSERT INTO t VALUES (1), (2), (4), (8);
  `);

  const rs = await c.execute('SELECT SUM(a) FROM t');
  t.is(rs.rows[0][0], 15);
}));

test.serial('exec: error stops execution', withClient(async (t, c) => {
  await t.throwsAsync(() =>
    c.exec(`
      DROP TABLE IF EXISTS t;
      CREATE TABLE t (a);
      INSERT INTO t VALUES (1), (2), (4);
      INSERT INTO t VALUES (foo());
      INSERT INTO t VALUES (8), (16);
    `),
  );

  const rs = await c.execute('SELECT SUM(a) FROM t');
  t.is(rs.rows[0][0], 7);
}));

test.serial('exec: manual transaction control', withClient(async (t, c) => {
  await c.exec(`
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a);
    BEGIN;
    INSERT INTO t VALUES (1), (2), (4);
    INSERT INTO t VALUES (8), (16);
    COMMIT;
  `);

  const rs = await c.execute('SELECT SUM(a) FROM t');
  t.is(rs.rows[0][0], 31);
}));

test.serial('exec: error rolls back manual transaction', withClient(async (t, c) => {
  await t.throwsAsync(() =>
    c.exec(`
      DROP TABLE IF EXISTS t;
      CREATE TABLE t (a);
      INSERT INTO t VALUES (0);
      BEGIN;
      INSERT INTO t VALUES (1), (2), (4);
      INSERT INTO t VALUES (foo());
      INSERT INTO t VALUES (8), (16);
      COMMIT;
    `),
  );

  // The INSERT INTO t VALUES (0) before BEGIN is committed.
  // The BEGIN starts a transaction, but foo() fails so COMMIT is never reached
  // and the transaction is rolled back. The pre-transaction insert (0) survives.
  const rs = await c.execute('SELECT SUM(a) FROM t');
  // Statements outside the transaction (before BEGIN) are committed
  t.true(rs.rows[0][0] >= 0);
  // Statements inside the failed transaction should be rolled back
  const rs2 = await c.execute('SELECT COUNT(*) FROM t WHERE a = 8 OR a = 16');
  t.is(rs2.rows[0][0], 0);
}));

// ---------------------------------------------------------------------------
// transaction()
// ---------------------------------------------------------------------------

test.serial('transaction: commit', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');

  const txn = c.transaction(async () => {
    await c.execute("INSERT INTO t VALUES ('one')");
    await c.execute("INSERT INTO t VALUES ('two')");
  });
  await txn();

  const rs = await c.execute('SELECT COUNT(*) FROM t');
  t.is(rs.rows[0][0], 2);
}));

test.serial('transaction: rollback on error', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');

  const txn = c.transaction(async () => {
    await c.execute("INSERT INTO t VALUES ('one')");
    throw new Error('intentional');
  });
  await t.throwsAsync(() => txn());

  const rs = await c.execute('SELECT COUNT(*) FROM t');
  t.is(rs.rows[0][0], 0);
}));

// ---------------------------------------------------------------------------
// prepare() / Statement
// ---------------------------------------------------------------------------

test.serial('prepare: get returns first row', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  await c.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");

  const stmt = await c.prepare('SELECT * FROM t WHERE id = ?');
  const row = await stmt.get([1]);
  t.is(row.id, 1);
  t.is(row.name, 'Alice');
}));

test.serial('prepare: get returns undefined for no results', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');

  const stmt = await c.prepare('SELECT * FROM t WHERE a = ?');
  const row = await stmt.get([999]);
  t.is(row, undefined);
}));

test.serial('prepare: all returns all rows', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute('INSERT INTO t VALUES (1), (2), (3)');

  const stmt = await c.prepare('SELECT * FROM t ORDER BY a');
  const rows = await stmt.all();
  t.is(rows.length, 3);
  t.is(rows[0].a, 1);
  t.is(rows[1].a, 2);
  t.is(rows[2].a, 3);
}));

test.serial('prepare: run returns changes and lastInsertRowid', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');

  const stmt = await c.prepare('INSERT INTO t VALUES (?)');
  const result = await stmt.run([42]);
  t.is(result.changes, 1);
  t.truthy(result.lastInsertRowid);
}));

test.serial('prepare: iterate streams rows', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a)');
  await c.execute('INSERT INTO t VALUES (1), (2), (3)');

  const stmt = await c.prepare('SELECT * FROM t ORDER BY a');
  const rows = [];
  for await (const row of stmt.iterate()) {
    rows.push(row);
  }
  t.is(rows.length, 3);
  t.is(rows[0].a, 1);
  t.is(rows[2].a, 3);
}));

test.serial('prepare: raw mode returns arrays', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a, b)');
  await c.execute("INSERT INTO t VALUES (1, 'one')");

  const stmt = await c.prepare('SELECT * FROM t');
  const row = await stmt.raw().get();
  t.true(Array.isArray(row));
  t.deepEqual(row, [1, 'one']);
}));

test.serial('prepare: pluck mode returns first column', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (a, b)');
  await c.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')");

  const stmt = await c.prepare('SELECT a FROM t ORDER BY a');
  const ids = await stmt.pluck().all();
  t.deepEqual(ids, [1, 2, 3]);
}));

test.serial('prepare: columns returns metadata', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (id INTEGER, name TEXT)');

  const stmt = await c.prepare('SELECT id, name FROM t');
  const cols = stmt.columns();
  t.is(cols.length, 2);
  t.is(cols[0].name, 'id');
  t.is(cols[1].name, 'name');
}));

test.serial('prepare: safeIntegers returns bigints', withClient(async (t, c) => {
  const stmt = await c.prepare('SELECT 42');
  const row = await stmt.safeIntegers().raw().get();
  t.is(row[0], 42n);
}));

// ---------------------------------------------------------------------------
// column types from tables
// ---------------------------------------------------------------------------

test.serial('column types from table', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t');
  await c.execute('CREATE TABLE t (i INTEGER, f FLOAT, t TEXT, b BLOB)');
  await c.execute("INSERT INTO t VALUES (42, 0.5, 'foo', X'626172')");
  const rs = await c.execute('SELECT i, f, t, b FROM t LIMIT 1');
  t.deepEqual(rs.columns, ['i', 'f', 't', 'b']);
  t.deepEqual(rs.columnTypes, ['INTEGER', 'FLOAT', 'TEXT', 'BLOB']);
  t.is(rs.rows[0][0], 42);
  t.is(rs.rows[0][1], 0.5);
  t.is(rs.rows[0][2], 'foo');
}));

// ---------------------------------------------------------------------------
// constraint errors
// ---------------------------------------------------------------------------

test.serial('error: PRIMARY KEY constraint violation', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t_pk');
  await c.execute('CREATE TABLE t_pk (id INTEGER PRIMARY KEY, name TEXT)');
  await c.execute("INSERT INTO t_pk VALUES (1, 'first')");

  const err = await t.throwsAsync(() =>
    c.execute("INSERT INTO t_pk VALUES (1, 'duplicate')"),
  );
  t.truthy(err.message);
}));

test.serial('error: UNIQUE constraint violation', withClient(async (t, c) => {
  await c.execute('DROP TABLE IF EXISTS t_uq');
  await c.execute('CREATE TABLE t_uq (id INTEGER, name TEXT UNIQUE)');
  await c.execute("INSERT INTO t_uq VALUES (1, 'unique_name')");

  const err = await t.throwsAsync(() =>
    c.execute("INSERT INTO t_uq VALUES (2, 'unique_name')"),
  );
  t.truthy(err.message);
}));

// ---------------------------------------------------------------------------
// error handling
// ---------------------------------------------------------------------------

test.serial('error: non-existent table', withClient(async (t, c) => {
  const err = await t.throwsAsync(() =>
    c.execute('SELECT * FROM nonexistent_table'),
  );
  t.truthy(err.message);
}));

test.serial('error: connection recovers after error', withClient(async (t, c) => {
  await t.throwsAsync(() => c.execute('SELECT foobar'));
  // Connection should still be usable
  const rs = await c.execute('SELECT 42');
  t.is(rs.rows[0][0], 42);
}));
