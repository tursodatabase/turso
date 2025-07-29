import test from 'ava';
import { connect } from '../dist/index.js';

const client = connect({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

test.serial('execute() method creates table and inserts data', async t => {
  await client.execute('DROP TABLE IF EXISTS test_users');

  await client.execute('CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

  const insertResult = await client.execute(
    'INSERT INTO test_users (name, email) VALUES (?, ?)',
    ['John Doe', 'john@example.com']
  );

  t.is(insertResult.rowsAffected, 1);
  t.is(typeof insertResult.lastInsertRowid, 'number');
});

test.serial('execute() method queries data correctly', async t => {
  const queryResult = await client.execute('SELECT * FROM test_users WHERE name = ?', ['John Doe']);

  t.is(queryResult.columns.length, 3);
  t.true(queryResult.columns.includes('id'));
  t.true(queryResult.columns.includes('name'));
  t.true(queryResult.columns.includes('email'));

  t.is(queryResult.rows.length, 1);
  t.is(queryResult.rows[0][1], 'John Doe');
  t.is(queryResult.rows[0][2], 'john@example.com');
});

test.serial('prepare() method creates statement', async t => {
  const stmt = client.prepare('SELECT * FROM test_users WHERE name = ?');

  const row = await stmt.get(['John Doe']);
  t.is(row[1], 'John Doe');
  t.is(row[2], 'john@example.com');

  const rows = await stmt.all(['John Doe']);
  t.is(rows.length, 1);
  t.is(rows[0][1], 'John Doe');
});

test.serial('Statement.run()', async t => {
  const stmt = client.prepare('INSERT INTO test_users (name, email) VALUES (?, ?)');
  const row = await stmt.run(['Jane Doe', 'jane@example.com']);
  t.is(row.lastInsertRowid, 2);
});

test.serial('statement iterate() method works', async t => {
  // Ensure test data exists
  await client.execute('CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
  await client.execute('INSERT OR IGNORE INTO test_users (name, email) VALUES (?, ?)', ['John Doe', 'john@example.com']);

  const stmt = client.prepare('SELECT * FROM test_users');

  const rows = [];
  for await (const row of stmt.iterate()) {
    rows.push(row);
  }

  t.true(rows.length >= 1);
  t.is(rows[0][1], 'John Doe');
});

test.serial('batch() method executes multiple statements', async t => {
  await client.execute('DROP TABLE IF EXISTS test_products');

  const batchResult = await client.batch([
    'CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
    'INSERT INTO test_products (name, price) VALUES ("Widget", 9.99)',
    'INSERT INTO test_products (name, price) VALUES ("Gadget", 19.99)',
    'INSERT INTO test_products (name, price) VALUES ("Tool", 29.99)'
  ]);

  t.is(batchResult.rowsAffected, 3);

  const queryResult = await client.execute('SELECT COUNT(*) as count FROM test_products');
  t.is(queryResult.rows[0][0], 3);
});

test.serial('execute() method queries a single value', async t => {
  const rs = await client.execute('SELECT 42');

  t.is(rs.columns.length, 1);
  t.is(rs.columnTypes.length, 1);
  t.is(rs.rows.length, 1);
  t.is(rs.rows[0].length, 1);
  t.is(rs.rows[0][0], 42);
});

test.serial('execute() method queries a single row', async t => {
  const rs = await client.execute(
    "SELECT 1 AS one, 'two' AS two, 0.5 AS three"
  );

  t.deepEqual(rs.columns, ["one", "two", "three"]);
  t.deepEqual(rs.columnTypes, ["", "", ""]);
  t.is(rs.rows.length, 1);

  const r = rs.rows[0];
  t.is(r.length, 3);
  t.deepEqual(Array.from(r), [1, "two", 0.5]);
  t.deepEqual(Object.entries(r), [
    ["0", 1],
    ["1", "two"],
    ["2", 0.5],
  ]);

  // Test column name access
  t.is(r.one, 1);
  t.is(r.two, "two");
  t.is(r.three, 0.5);
});

test.serial('error handling works correctly', async t => {
  const error = await t.throwsAsync(
    () => client.execute('SELECT * FROM nonexistent_table')
  );
  t.regex(error.message, /SQLite error.*no such table|no such table|HTTP error/);
});
test.serial("transaction() method creates transaction", async (t) => {
  await client.execute("DROP TABLE IF EXISTS test_transactions");
  await client.execute(
    "CREATE TABLE test_transactions (id INTEGER PRIMARY KEY, value INTEGER)",
  );

  const tx = await client.transaction();
  t.false(tx.closed);

  await tx.execute("INSERT INTO test_transactions (value) VALUES (?)", [100]);
  await tx.execute("INSERT INTO test_transactions (value) VALUES (?)", [200]);
  await tx.commit();

  t.true(tx.closed);
  t.true(tx.committed);

  const result = await client.execute(
    "SELECT COUNT(*) as count FROM test_transactions",
  );
  t.is(result.rows[0][0], 2);
});

test.serial("transaction rollback works correctly", async (t) => {
  await client.execute("DELETE FROM test_transactions");

  const tx = await client.transaction();
  await tx.execute("INSERT INTO test_transactions (value) VALUES (?)", [300]);
  await tx.execute("INSERT INTO test_transactions (value) VALUES (?)", [400]);
  await tx.rollback();

  t.true(tx.closed);
  t.true(tx.rolledBack);

  const result = await client.execute(
    "SELECT COUNT(*) as count FROM test_transactions",
  );
  t.is(result.rows[0][0], 0);
});

test.serial("transaction batch() method works", async (t) => {
  await client.execute("DELETE FROM test_transactions");

  const tx = await client.transaction();
  await tx.batch([
    "INSERT INTO test_transactions (value) VALUES (500)",
    "INSERT INTO test_transactions (value) VALUES (600)",
  ]);
  await tx.commit();

  const result = await client.execute(
    "SELECT COUNT(*) as count FROM test_transactions",
  );
  t.is(result.rows[0][0], 2);
});

test.serial("transaction error handling works", async (t) => {
  const tx = await client.transaction();

  const error = await t.throwsAsync(() =>
    tx.execute("SELECT * FROM nonexistent_table"),
  );
  t.regex(
    error.message,
    /SQLite error.*no such table|no such table|HTTP error/,
  );

  await tx.rollback();
});

test.serial("transaction close() method works", async (t) => {
  const tx = await client.transaction();
  await tx.execute("INSERT INTO test_transactions (value) VALUES (700)");

  tx.close();
  t.true(tx.closed);

  // Changes should be rolled back
  const result = await client.execute(
    "SELECT COUNT(*) as count FROM test_transactions WHERE value = 700",
  );
  t.is(result.rows[0][0], 0);
});

test.serial("transaction modes work correctly", async (t) => {
  const writeModeTx = await client.transaction("write");
  t.false(writeModeTx.closed);
  await writeModeTx.rollback();

  const readModeTx = await client.transaction("read");
  t.false(readModeTx.closed);
  await readModeTx.rollback();

  const deferredTx = await client.transaction("deferred");
  t.false(deferredTx.closed);
  await deferredTx.rollback();
});
