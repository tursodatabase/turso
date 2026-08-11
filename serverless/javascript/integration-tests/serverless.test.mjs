import test from 'ava';
import { connect, Transaction } from '../dist/index.js';

const client = connect({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

test.serial('run() method creates table and inserts data', async t => {
  await client.exec('DROP TABLE IF EXISTS test_users');

  await client.exec('CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

  const insertResult = await client.run(
    'INSERT INTO test_users (name, email) VALUES (?, ?)',
    ['John Doe', 'john@example.com']
  );

  t.is(insertResult.changes, 1);
  t.is(typeof insertResult.lastInsertRowid, 'number');
});

test.serial('all() method queries data correctly', async t => {
  const stmt = await client.prepare('SELECT * FROM test_users WHERE name = ?');
  const columns = stmt.columns().map(col => col.name);

  t.is(columns.length, 3);
  t.true(columns.includes('id'));
  t.true(columns.includes('name'));
  t.true(columns.includes('email'));

  const rows = await client.all('SELECT * FROM test_users WHERE name = ?', ['John Doe']);
  t.is(rows.length, 1);
  t.is(rows[0].name, 'John Doe');
  t.is(rows[0].email, 'john@example.com');
});

test.serial('prepare() method creates statement', async t => {
  const stmt = await client.prepare('SELECT * FROM test_users WHERE name = ?');
  
  const row = await stmt.get(['John Doe']);
  t.is(row[1], 'John Doe');
  t.is(row[2], 'john@example.com');
  
  const rows = await stmt.all(['John Doe']);
  t.is(rows.length, 1);
  t.is(rows[0][1], 'John Doe');
});

test.serial('Statement.run()', async t => {
  const stmt = await client.prepare('INSERT INTO test_users (name, email) VALUES (?, ?)');
  const row = await stmt.run(['Jane Doe', 'jane@example.com']);
  t.is(row.lastInsertRowid, 2);
});

test.serial('statement iterate() method works', async t => {
  // Ensure test data exists
  await client.exec('CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
  await client.run('INSERT OR IGNORE INTO test_users (name, email) VALUES (?, ?)', ['John Doe', 'john@example.com']);
  
  const stmt = await client.prepare('SELECT * FROM test_users');
  
  const rows = [];
  for await (const row of stmt.iterate()) {
    rows.push(row);
  }
  
  t.true(rows.length >= 1);
  t.is(rows[0][1], 'John Doe');
});

test.serial('batch() method executes multiple statements', async t => {
  await client.exec('DROP TABLE IF EXISTS test_products');
  
  const batchResult = await client.batch([
    'CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
    'INSERT INTO test_products (name, price) VALUES ("Widget", 9.99)',
    'INSERT INTO test_products (name, price) VALUES ("Gadget", 19.99)',
    'INSERT INTO test_products (name, price) VALUES ("Tool", 29.99)'
  ]);
  
  // batch() returns one ResultSet per statement, in order.
  t.is(batchResult.length, 4);
  const insertedRows = batchResult.slice(1).reduce((sum, rs) => sum + rs.rowsAffected, 0);
  t.is(insertedRows, 3);

  const countRow = await client.get('SELECT COUNT(*) as count FROM test_products');
  t.is(countRow.count, 3);
});

test.serial('get() method queries a single value', async t => {
  const row = await client.get('SELECT 42 AS answer');

  t.is(row.answer, 42);
  t.is(row[0], 42);
});

test.serial('get() method queries a single row', async t => {
  const stmt = await client.prepare("SELECT 1 AS one, 'two' AS two, 0.5 AS three");
  t.deepEqual(stmt.columns().map(col => col.name), ["one", "two", "three"]);

  const rows = await client.all("SELECT 1 AS one, 'two' AS two, 0.5 AS three");
  t.is(rows.length, 1);

  const r = rows[0];
  t.deepEqual(Object.entries(r), [
    ["one", 1],
    ["two", "two"],
    ["three", 0.5],
  ]);

  // Positional access is also available
  t.is(r[0], 1);
  t.is(r[1], "two");
  t.is(r[2], 0.5);
});

test.serial('error handling works correctly', async t => {
  const error = await t.throwsAsync(
    () => client.all('SELECT * FROM nonexistent_table')
  );
  t.regex(error.message, /SQLite error.*no such table|no such table|HTTP error/);
});

test.serial('transaction.concurrent uses BEGIN CONCURRENT', async t => {
  const localClient = connect({ url: 'http://localhost:0' });
  const calls = [];

  localClient.exec = async sql => {
    calls.push(sql);
  };
  localClient.session.close = async () => {};

  try {
    const txn = localClient.transaction(async () => {
      calls.push('body');
    }).concurrent;
    await txn();
    t.deepEqual(calls, ['BEGIN CONCURRENT', 'body', 'COMMIT']);
  } finally {
    await localClient.close();
  }
});

test.serial('transactionAsync.concurrent uses BEGIN CONCURRENT', async t => {
  const localClient = connect({ url: 'http://localhost:0' });
  const calls = [];

  const originalExec = Transaction.prototype.exec;
  Transaction.prototype.exec = async sql => {
    calls.push(sql);
  };
  localClient.session.close = async () => {};

  try {
    const txn = localClient.transactionAsync(async (_tx) => {
      calls.push('body');
    }).concurrent;
    await txn();
    t.deepEqual(calls, ['BEGIN CONCURRENT', 'body', 'COMMIT']);
  } finally {
    Transaction.prototype.exec = originalExec;
    await localClient.close();
  }
});

const withTimeout = (promise, timeoutMs, label) => {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
};

// Each transactionAsync runs on its own dedicated stream, so all callbacks
// can be open at the same time. The barrier resolves only once every
// callback has been entered — if transactions were serialized on the
// connection (the pre-dedicated-session behavior), the first callback
// would wait on the barrier forever and the test would time out.
test.serial('transactionAsync transactions run in parallel', async t => {
  const N = 3;
  let entered = 0;
  let releaseBarrier;
  const allEntered = new Promise(resolve => { releaseBarrier = resolve; });

  const txn = client.transactionAsync(async (tx, i) => {
    if (++entered === N) releaseBarrier();
    await withTimeout(allEntered, 5000, 'all transaction callbacks open at once');
    const row = await tx.get('SELECT ? AS i', [i]);
    return row.i;
  });

  const results = await Promise.all(Array.from({ length: N }, (_, i) => txn(i)));
  t.deepEqual(results.sort(), [0, 1, 2]);
});

// Statements on the connection are not blocked by an open transactionAsync
// window; they run on the connection's own stream, outside the transaction,
// so they must not observe its uncommitted writes. Under a connection-level
// lock this await would deadlock the transaction.
test.serial('connection statements proceed while transactionAsync is open', async t => {
  await client.exec('DROP TABLE IF EXISTS txn_parallel');
  await client.exec('CREATE TABLE txn_parallel (id INTEGER PRIMARY KEY, v TEXT)');

  let uncommittedCount;
  await withTimeout(client.transactionAsync(async tx => {
    await tx.run('INSERT INTO txn_parallel (v) VALUES (?)', ['inside']);
    const rows = await client.all('SELECT COUNT(*) AS n FROM txn_parallel');
    uncommittedCount = rows[0].n;
  })(), 5000, 'connection statement inside transactionAsync callback');

  t.is(uncommittedCount, 0);
  const rows = await client.all('SELECT COUNT(*) AS n FROM txn_parallel');
  t.is(rows[0].n, 1);
});

// Two transactions with overlapping open windows commit and roll back
// independently. The write sections are kept disjoint on purpose — the
// overlap under test is the callback windows, not the server's write lock.
test.serial('rollback of a parallel transactionAsync leaves the other intact', async t => {
  await client.exec('DROP TABLE IF EXISTS txn_writers');
  await client.exec('CREATE TABLE txn_writers (v TEXT)');

  let entered = 0;
  let releaseBarrier;
  const bothOpen = new Promise(resolve => { releaseBarrier = resolve; });
  let releaseOkDone;
  const okDone = new Promise(resolve => { releaseOkDone = resolve; });

  const ok = client.transactionAsync(async tx => {
    if (++entered === 2) releaseBarrier();
    await withTimeout(bothOpen, 5000, 'both transaction callbacks open');
    await tx.run("INSERT INTO txn_writers (v) VALUES ('kept')");
  });
  const failing = client.transactionAsync(async tx => {
    if (++entered === 2) releaseBarrier();
    await withTimeout(bothOpen, 5000, 'both transaction callbacks open');
    await withTimeout(okDone, 5000, 'parallel transaction commits first');
    await tx.run("INSERT INTO txn_writers (v) VALUES ('discarded')");
    throw new Error('boom');
  });

  const [, err] = await Promise.all([
    ok().then(releaseOkDone),
    failing().then(() => null, e => e),
  ]);
  t.is(err.message, 'boom');

  const rows = await client.all('SELECT v FROM txn_writers ORDER BY v');
  t.deepEqual(rows.map(r => r.v), ['kept']);
});
