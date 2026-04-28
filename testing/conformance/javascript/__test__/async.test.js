import test from "ava";
import crypto from 'crypto';
import fs from 'fs';

const withTimeout = (promise, timeoutMs, label) => {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => {
    clearTimeout(timer);
  });
};


test.beforeEach(async (t) => {
  const [db, path,errorType] = await connect();
  await db.exec(`
      DROP TABLE IF EXISTS users;
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
  `);
  await db.exec(
    "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')"
  );
  await db.exec(
    "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')"
  );
  t.context = {
    db,
    path,
    errorType
  };
});

test.afterEach.always(async (t) => {
  // Close the database connection
  if (t.context.db != undefined) {
    t.context.db.close();
  }
  // Remove the database file if it exists
  if (t.context.path) {
    const walPath = t.context.path + "-wal";
    const shmPath = t.context.path + "-shm";
    if (fs.existsSync(t.context.path)) {
      fs.unlinkSync(t.context.path);
    }
    if (fs.existsSync(walPath)) {
      fs.unlinkSync(walPath);
    }
    if (fs.existsSync(shmPath)) {
      fs.unlinkSync(shmPath);
    }
  }
});

test.serial("Open in-memory database", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping in-memory database test for serverless");
    return;
  }
  const [db] = await connect(":memory:");
  t.is(db.memory, true);
});

// ==========================================================================
// Database.exec()
// ==========================================================================

test.skip("Database.exec() syntax error", async (t) => {
  const db = t.context.db;

  const syntaxError = await t.throwsAsync(async () => {
    await db.exec("SYNTAX ERROR");
  }, {
    instanceOf: t.context.errorType,
    message: 'near "SYNTAX": syntax error',
    code: 'SQLITE_ERROR'
  });

  t.is(syntaxError.rawCode, 1)
  const noTableError = await t.throwsAsync(async () => {
    await db.exec("SELECT * FROM missing_table");
  }, {
    instanceOf: t.context.errorType,
    message: "no such table: missing_table",
    code: 'SQLITE_ERROR'
  });
  t.is(noTableError.rawCode, 1)
});

test.serial("Database.exec() after close()", async (t) => {
  const db = t.context.db;
  await db.close();
  await t.throwsAsync(async () => {
    await db.exec("SELECT 1");
  }, {
    instanceOf: TypeError,
    message: "The database connection is not open"
  });
});

// ==========================================================================
// Database.prepare()
// ==========================================================================

test.skip("Database.prepare() syntax error", async (t) => {
  const db = t.context.db;

  await t.throwsAsync(async () => {
    return await db.prepare("SYNTAX ERROR");
  }, {
    instanceOf: t.context.errorType,
    message: 'near "SYNTAX": syntax error'
  });
});


test.serial("Database.prepare() after close()", async (t) => {
  const db = t.context.db;
  await db.close();
  await t.throwsAsync(async () => {
    await db.prepare("SELECT 1");
  }, {
    instanceOf: TypeError,
    message: "The database connection is not open"
  });
});

// ==========================================================================
// Database.pragma()
// ==========================================================================

test.serial("Database.pragma()", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping pragma test for serverless");
    return;
  }
  const db = t.context.db;
  await db.pragma("cache_size = 2000");
  t.deepEqual(await db.pragma("cache_size"), [{ "cache_size": 2000 }]);
});

test.serial("Database.pragma() after close()", async (t) => {
  const db = t.context.db;
  await db.close();
  await t.throwsAsync(async () => {
    await db.pragma("cache_size = 2000");
  }, {
    instanceOf: TypeError,
    message: "The database connection is not open"
  });
});

// ==========================================================================
// Database.transaction()
// ==========================================================================

test.serial("Database.transaction()", async (t) => {
  const db = t.context.db;

  const insert = await db.prepare(
    "INSERT INTO users(name, email) VALUES (:name, :email)"
  );

  const insertMany = db.transaction(async (users) => {
    t.is(db.inTransaction, true);
    for (const user of users) await insert.run(user);
  });

  t.is(db.inTransaction, false);
  await insertMany([
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
    { name: "Junior", email: "junior@example.org" },
  ]);
  t.is(db.inTransaction, false);

  const stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  t.is((await stmt.get(3)).name, "Joey");
  t.is((await stmt.get(4)).name, "Sally");
  t.is((await stmt.get(5)).name, "Junior");
});

test.serial("Database.transaction().immediate()", async (t) => {
  const db = t.context.db;
  const insert = await db.prepare(
    "INSERT INTO users(name, email) VALUES (:name, :email)"
  );
  const insertMany = db.transaction((users) => {
    t.is(db.inTransaction, true);
    for (const user of users) insert.run(user);
  });
  t.is(db.inTransaction, false);
  await insertMany.immediate([
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
    { name: "Junior", email: "junior@example.org" },
  ]);
  t.is(db.inTransaction, false);
});

// ==========================================================================
// Database.interrupt()
// ==========================================================================

test.skip("Database.interrupt()", async (t) => {
  const db = t.context.db;
  const stmt = await db.prepare("WITH RECURSIVE infinite_loop(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM infinite_loop) SELECT * FROM infinite_loop;");
  const fut = stmt.all();
  db.interrupt();
  await t.throwsAsync(async () => {
    await fut;
  }, {
    instanceOf: t.context.errorType,
    message: 'interrupted',
    code: 'SQLITE_INTERRUPT'
  });
});

// ==========================================================================
// Statement.run()
// ==========================================================================

test.serial("Statement.run() [positional]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("INSERT INTO users(name, email) VALUES (?, ?)");
  const info = await stmt.run(["Carol", "carol@example.net"]);
  t.is(info.changes, 1);
  t.is(info.lastInsertRowid, 3);
});

// ==========================================================================
// Statement.get()
// ==========================================================================

test.serial("Statement.get() [no parameters]", async (t) => {
  const db = t.context.db;

  var stmt = 0;

  stmt = await db.prepare("SELECT * FROM users");
  t.is((await stmt.get()).name, "Alice");
  t.deepEqual(await stmt.raw().get(), [1, 'Alice', 'alice@example.org']);
});

test.serial("Statement.get() [positional]", async (t) => {
  const db = t.context.db;

  var stmt = 0;

  stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  t.is(await stmt.get(0), undefined);
  t.is(await stmt.get([0]), undefined);
  t.is((await stmt.get(1)).name, "Alice");
  t.is((await stmt.get(2)).name, "Bob");

  stmt = await db.prepare("SELECT * FROM users WHERE id = ?1");
  t.is(await stmt.get({1: 0}), undefined);
  t.is((await stmt.get({1: 1})).name, "Alice");
  t.is((await stmt.get({1: 2})).name, "Bob");
});

test.serial("Statement.get() [named]", async (t) => {
  const db = t.context.db;

  var stmt = undefined;

  stmt = await db.prepare("SELECT * FROM users WHERE id = :id");
  t.is(await stmt.get({ id: 0 }), undefined);
  t.is((await stmt.get({ id: 1 })).name, "Alice");
  t.is((await stmt.get({ id: 2 })).name, "Bob");

  stmt = await db.prepare("SELECT * FROM users WHERE id = @id");
  t.is(await stmt.get({ id: 0 }), undefined);
  t.is((await stmt.get({ id: 1 })).name, "Alice");
  t.is((await stmt.get({ id: 2 })).name, "Bob");

  stmt = await db.prepare("SELECT * FROM users WHERE id = $id");
  t.is(await stmt.get({ id: 0 }), undefined);
  t.is((await stmt.get({ id: 1 })).name, "Alice");
  t.is((await stmt.get({ id: 2 })).name, "Bob");
});

test.serial("Statement.get() [raw]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  t.deepEqual(await stmt.raw().get(1), [1, "Alice", "alice@example.org"]);
});

test.serial("Statement.get() values", async (t) => {
  const db = t.context.db;

  const stmt = (await db.prepare("SELECT ?")).raw();
  t.deepEqual(await stmt.get(1), [1]);
  t.deepEqual(await stmt.get(Number.MIN_VALUE), [Number.MIN_VALUE]);
  t.deepEqual(await stmt.get(Number.MAX_VALUE), [Number.MAX_VALUE]);
  t.deepEqual(await stmt.get(Number.MAX_SAFE_INTEGER), [Number.MAX_SAFE_INTEGER]);
  t.deepEqual(await stmt.get(9007199254740991n), [9007199254740991]);
});

test.serial("Statement.get() [blob]", async (t) => {
  const db = t.context.db;

  // Create table with blob column
  await db.exec("CREATE TABLE IF NOT EXISTS blobs (id INTEGER PRIMARY KEY, data BLOB)");
  
  // Test inserting and retrieving blob data
  const binaryData = Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]); // "Hello World"
  const insertStmt = await db.prepare("INSERT INTO blobs (data) VALUES (?)");
  await insertStmt.run([binaryData]);
  
  // Retrieve the blob data
  const selectStmt = await db.prepare("SELECT data FROM blobs WHERE id = 1");
  const result = await selectStmt.get();
  
  t.truthy(result, "Should return a result");
  t.true(Buffer.isBuffer(result.data), "Should return Buffer for blob data");
  t.deepEqual(result.data, binaryData, "Blob data should match original");
});

// ==========================================================================
// Statement.iterate()
// ==========================================================================

test.serial("Statement.iterate() [empty]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users WHERE id = 0");
  const it = await stmt.iterate();
  t.is((await it.next()).done, true);
});

test.serial("Statement.iterate()", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [1, 2];
  var idx = 0;
  for await (const row of await stmt.iterate()) {
    t.is(row.id, expected[idx++]);
  }
});

test.serial("Statement.iterate() [expanded mode returns objects]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    { id: 1, name: "Alice", email: "alice@example.org" },
    { id: 2, name: "Bob", email: "bob@example.com" },
  ];
  var idx = 0;
  for await (const row of await stmt.iterate()) {
    t.deepEqual(row, expected[idx++]);
  }
});

test.serial("Statement.iterate() [raw]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    [1, "Alice", "alice@example.org"],
    [2, "Bob", "bob@example.com"],
  ];
  var idx = 0;
  for await (const row of await stmt.raw().iterate()) {
    t.deepEqual(row, expected[idx++]);
  }
});

// ==========================================================================
// Statement.all()
// ==========================================================================

test.serial("Statement.all()", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    { id: 1, name: "Alice", email: "alice@example.org" },
    { id: 2, name: "Bob", email: "bob@example.com" },
  ];
  t.deepEqual(await stmt.all(), expected);
});

test.serial("Statement.all() [raw]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    [1, "Alice", "alice@example.org"],
    [2, "Bob", "bob@example.com"],
  ];
  t.deepEqual(await stmt.raw().all(), expected);
});

test.serial("Statement.all() [pluck]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    1,
    2,
  ];
  t.deepEqual(await stmt.pluck().all(), expected);
});

test.serial("Statement.all() [default safe integers]", async (t) => {
  const db = t.context.db;
  db.defaultSafeIntegers();
  const stmt = await db.prepare("SELECT * FROM users");
  const expected = [
    [1n, "Alice", "alice@example.org"],
    [2n, "Bob", "bob@example.com"],
  ];
  t.deepEqual(await stmt.raw().all(), expected);
});

test.serial("Statement.all() [statement safe integers]", async (t) => {
  const db = t.context.db;
  const stmt = await db.prepare("SELECT * FROM users");
  stmt.safeIntegers();
  const expected = [
    [1n, "Alice", "alice@example.org"],
    [2n, "Bob", "bob@example.com"],
  ];
  t.deepEqual(await stmt.raw().all(), expected);
});

// ==========================================================================
// Statement.raw()
// ==========================================================================

test.skip("Statement.raw() [failure]", async (t) => {
  const db = t.context.db;
  const stmt = await db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
  await t.throws(() => {
    stmt.raw()
  }, {
    message: 'The raw() method is only for statements that return data'
  });
});

// ==========================================================================
// Statement.columns()
// ==========================================================================

test.serial("Statement.columns()", async (t) => {
  const db = t.context.db;

  var stmt = undefined;

  stmt = await db.prepare("SELECT 1");
  const columns1 = stmt.columns();
  t.is(columns1.length, 1);
  t.is(columns1[0].name, '1');
  // For "SELECT 1", type varies by provider, so just check it exists
  t.true('type' in columns1[0]);

  stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  const columns2 = stmt.columns();
  t.is(columns2.length, 3);
  
  // Check column names and types only
  t.is(columns2[0].name, "id");
  t.is(columns2[0].type, "INTEGER");
  
  t.is(columns2[1].name, "name");  
  t.is(columns2[1].type, "TEXT");
  
  t.is(columns2[2].name, "email");
  t.is(columns2[2].type, "TEXT");
});

// ==========================================================================
// Statement.reader
// ==========================================================================

test.serial("Statement.reader [SELECT is true]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  t.is(stmt.reader, true);
});

test.serial("Statement.reader [INSERT is false]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("INSERT INTO users (name, email) VALUES (?, ?)");
  t.is(stmt.reader, false);
});

test.serial("Statement.reader [UPDATE is false]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("UPDATE users SET name = ? WHERE id = ?");
  t.is(stmt.reader, false);
});

test.serial("Statement.reader [DELETE is false]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("DELETE FROM users WHERE id = ?");
  t.is(stmt.reader, false);
});

test.serial("Statement.reader [INSERT RETURNING is true]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("INSERT INTO users (name, email) VALUES (?, ?) RETURNING *");
  t.is(stmt.reader, true);
});

test.serial("Statement.reader [UPDATE RETURNING is true]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("UPDATE users SET name = ? WHERE id = ? RETURNING *");
  t.is(stmt.reader, true);
});

test.serial("Statement.reader [DELETE RETURNING is true]", async (t) => {
  const db = t.context.db;

  const stmt = await db.prepare("DELETE FROM users WHERE id = ? RETURNING *");
  t.is(stmt.reader, true);
});

// ==========================================================================
// Statement.interrupt()
// ==========================================================================

test.skip("Statement.interrupt()", async (t) => {
  const db = t.context.db;
  const stmt = await db.prepare("WITH RECURSIVE infinite_loop(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM infinite_loop) SELECT * FROM infinite_loop;");
  const fut = stmt.all();
  stmt.interrupt();
  await t.throwsAsync(async () => {
    await fut;
  }, {
    instanceOf: t.context.errorType,
    message: 'interrupted',
    code: 'SQLITE_INTERRUPT'
  });
});

test.serial("Query timeout option interrupts long-running query", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping generic timeout test for serverless");
    return;
  }
  const path = genDatabaseFilename();
  const [db] = await connect(path, { defaultQueryTimeout: 50 });
  const stmt = await db.prepare("SELECT sum(value) FROM generate_series(1, 1000000000);");

  const error = await t.throwsAsync(async () => {
    await stmt.get();
  }, { any: true });
  t.truthy(error);
  t.true(error.message.toLowerCase().includes("interrupt"));

  await db.close();
  cleanupDatabaseFiles(path);
});

test.serial("Query timeout option allows short-running query", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping generic timeout test for serverless");
    return;
  }
  const path = genDatabaseFilename();
  const [db] = await connect(path, { defaultQueryTimeout: 50 });
  const stmt = await db.prepare("SELECT 1 AS value");
  t.deepEqual(await stmt.get(), { value: 1 });

  await db.close();
  cleanupDatabaseFiles(path);
});

test.serial("Stale timeout guard from exhausted iterator does not interrupt later queries", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping in-memory test for serverless");
    return;
  }
  t.timeout(30_000);
  const [db] = await connect(":memory:", { defaultQueryTimeout: 1_000 });

  // Insert test data.
  await db.exec("CREATE TABLE t(x INTEGER)");
  const insert = await db.prepare("INSERT INTO t VALUES (?)");
  for (let i = 0; i < 2_000; i++) {
    await insert.run(i);
  }

  // Run many sequential queries via stmt.all() (which uses iterate() internally).
  // Each query finishes well under the timeout, but if the RowsIterator's
  // TimeoutGuard is not released until GC, stale guards will fire and
  // interrupt unrelated later queries.
  const stmt = await db.prepare("SELECT * FROM t ORDER BY x ASC");
  for (let i = 0; i < 150; i++) {
    const rows = await stmt.all();
    t.is(rows.length, 2_000);
  }

  db.close();
});

test.serial("Per-query timeout option interrupts long-running Statement.get()", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping generic timeout test for serverless");
    return;
  }
  const path = genDatabaseFilename();
  const [db] = await connect(path);
  const stmt = await db.prepare("SELECT sum(value) FROM generate_series(1, 1000000000);");

  const error = await t.throwsAsync(async () => {
    await stmt.get(undefined, { queryTimeout: 50 });
  }, { any: true });
  t.truthy(error);
  t.true(error.message.toLowerCase().includes("interrupt"));

  await db.close();
  cleanupDatabaseFiles(path);
});

test.serial("Per-query timeout option is accepted by Database.exec()", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping generic timeout test for serverless");
    return;
  }
  const path = genDatabaseFilename();
  const [db] = await connect(path);
  await t.notThrowsAsync(async () => {
    await db.exec("SELECT 1", { queryTimeout: 50 });
  });

  await db.close();
  cleanupDatabaseFiles(path);
});

test.skip("Timeout option", async (t) => {
  const timeout = 1000;
  const path = genDatabaseFilename();
  const [conn1] = await connect(path);
  await conn1.exec("CREATE TABLE t(x)");
  await conn1.exec("BEGIN IMMEDIATE");
  await conn1.exec("INSERT INTO t VALUES (1)")
  const options = { timeout };
  const [conn2] = await connect(path, options);
  const start = Date.now();
  try {
    await conn2.exec("INSERT INTO t VALUES (1)")
  } catch (e) {
    t.is(e.code, "SQLITE_BUSY");
    const end = Date.now();
    const elapsed = end - start;
    // Allow some tolerance for the timeout.
    t.is(elapsed > timeout/2, true);
  }
  fs.unlinkSync(path);
});

test.serial("Concurrent reads over same connection", async (t) => {
  const db = t.context.db;

  // Fire multiple reads concurrently on the same connection.
  // Each gets its own prepared statement to avoid sharing cursor state.
  // The connection should serialize them internally, not corrupt or error.
  const stmts = [];
  for (let i = 0; i < 10; i++) {
    stmts.push(await db.prepare("SELECT * FROM users ORDER BY id"));
  }
  const promises = stmts.map(stmt => stmt.all());
  const results = await Promise.all(promises);
  for (const rows of results) {
    t.is(rows.length, 2);
    t.is(rows[0].name, "Alice");
    t.is(rows[1].name, "Bob");
  }
});

test.serial("Concurrent writes over same connection", async (t) => {
  const db = t.context.db;

  // Fire multiple writes concurrently on the same connection.
  // The connection should serialize them internally, not corrupt or error.
  const promises = [];
  for (let i = 0; i < 20; i++) {
    promises.push(
      db.exec(`INSERT INTO users (name, email) VALUES ('User${i}', 'user${i}@example.org')`)
    );
  }
  await Promise.all(promises);

  const stmt = await db.prepare("SELECT count(*) as cnt FROM users");
  const rows = await stmt.raw().all();
  // 2 from beforeEach + 20 concurrent inserts
  t.is(rows[0][0], 22);
});

test.serial("Statement.iterate() with nested execute() on same connection does not deadlock", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only deadlock reproduction");
    return;
  }

  const db = t.context.db;
  await db.exec("DROP TABLE IF EXISTS iter_deadlock");
  await db.exec("CREATE TABLE iter_deadlock (id INTEGER PRIMARY KEY, value TEXT)");
  await db.exec("INSERT INTO iter_deadlock (id, value) VALUES (1, 'a')");
  await db.exec("INSERT INTO iter_deadlock (id, value) VALUES (2, 'b')");

  const stmt = await db.prepare("SELECT id FROM iter_deadlock ORDER BY id");
  const run = (async () => {
    for await (const row of stmt.iterate()) {
      const id = row.id ?? row[0];
      await db.execute("SELECT ? as echoed_id", [id]);
    }
  })();

  await t.notThrowsAsync(async () => {
    await withTimeout(run, 2000, "nested iterate/execute");
  });
});

// ==========================================================================
// Database rename
// ==========================================================================

test.serial("Open database after rename", async (t) => {
  if (process.env.PROVIDER === "serverless") {
    t.pass("Skipping rename test for serverless");
    return;
  }

  // 1. Open database A, create a table and insert data.
  const pathA = genDatabaseFilename();
  const pathB = genDatabaseFilename();
  const [dbA] = await connect(pathA);
  await dbA.exec("CREATE TABLE t(x INTEGER)");
  await dbA.exec("INSERT INTO t VALUES (42)");
  const row = await (await dbA.prepare("SELECT x FROM t")).get();
  t.is(row.x, 42);

  // 2. Close database A.
  await dbA.close();

  // 3. Rename A -> B on disk (main file + WAL + SHM).
  fs.renameSync(pathA, pathB);
  if (fs.existsSync(pathA + "-wal")) {
    fs.renameSync(pathA + "-wal", pathB + "-wal");
  }
  if (fs.existsSync(pathA + "-shm")) {
    fs.renameSync(pathA + "-shm", pathB + "-shm");
  }

  // 4. Open a new database at the original path A.
  const [dbA2] = await connect(pathA);

  // 5. The new A should be a fresh, empty database — table 't' must not exist.
  const tables = await (await dbA2.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='t'"
  )).all();
  t.is(tables.length, 0,
    "New database at A should not have table 't' — " +
    "DATABASE_MANAGER returned stale Database after rename"
  );

  // Cleanup.
  await dbA2.close();
  for (const p of [pathA, pathB]) {
    for (const suffix of ["", "-wal", "-shm"]) {
      if (fs.existsSync(p + suffix)) fs.unlinkSync(p + suffix);
    }
  }
});

// ==========================================================================
// Interactive transaction conformance
// ==========================================================================

test.serial("Interactive transaction COMMIT visibility across connections", async (t) => {
  const db = t.context.db;
  const [db2] = await connect(t.context.path);

  const countByName = async (conn, name) => {
    const stmt = await conn.prepare("SELECT COUNT(*) FROM users WHERE name = ?");
    const row = await stmt.raw().get([name]);
    return Number(row[0]);
  };

  try {
    await db.exec("BEGIN");
    const insert = await db.prepare("INSERT INTO users(name, email) VALUES (?, ?)");
    await insert.run(["TxCommit", "tx-commit@example.org"]);

    t.is(await countByName(db, "TxCommit"), 1);
    t.is(await countByName(db2, "TxCommit"), 0);

    await db.exec("COMMIT");

    t.is(await countByName(db2, "TxCommit"), 1);
  } finally {
    await db2.close();
  }
});

test.serial("Interactive transaction ROLLBACK discards writes", async (t) => {
  const db = t.context.db;

  const countByName = async (name) => {
    const stmt = await db.prepare("SELECT COUNT(*) FROM users WHERE name = ?");
    const row = await stmt.raw().get([name]);
    return Number(row[0]);
  };

  await db.exec("BEGIN IMMEDIATE");
  const insert = await db.prepare("INSERT INTO users(name, email) VALUES (?, ?)");
  await insert.run(["TxRollback", "tx-rollback@example.org"]);
  t.is(await countByName("TxRollback"), 1);

  await db.exec("ROLLBACK");
  t.is(await countByName("TxRollback"), 0);
});

test.serial("Interactive transaction error + ROLLBACK keeps connection usable", async (t) => {
  const db = t.context.db;

  const countByName = async (name) => {
    const stmt = await db.prepare("SELECT COUNT(*) FROM users WHERE name = ?");
    const row = await stmt.raw().get([name]);
    return Number(row[0]);
  };

  await db.exec("BEGIN");
  const insert = await db.prepare("INSERT INTO users(name, email) VALUES (?, ?)");
  await insert.run(["WillRollback", "will-rollback@example.org"]);

  const constraintError = await t.throwsAsync(async () => {
    const duplicateInsert = await db.prepare("INSERT INTO users(id, name, email) VALUES (?, ?, ?)");
    await duplicateInsert.run([1, "DuplicateId", "duplicate-id@example.org"]);
  }, {
    any: true,
  });
  t.truthy(constraintError);
  const constraintHint = `${constraintError.code ?? ""} ${constraintError.message ?? ""}`.toUpperCase();
  t.true(
    constraintHint.includes("CONSTRAINT")
    || constraintHint.includes("UNIQUE")
    || constraintHint.includes("PRIMARYKEY"),
  );

  await db.exec("ROLLBACK");
  t.is(await countByName("WillRollback"), 0);

  await db.exec("BEGIN");
  await insert.run(["AfterRollback", "after-rollback@example.org"]);
  await db.exec("COMMIT");

  t.is(await countByName("AfterRollback"), 1);
});
// Query timeout (serverless only — uses AbortSignal under the hood)
// ==========================================================================

test.serial("defaultQueryTimeout interrupts long-running query", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only test");
    return;
  }
  const turso = await import("@tursodatabase/serverless");
  const db = turso.connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
    defaultQueryTimeout: 100,
  });

  const error = await t.throwsAsync(async () => {
    await db.execute(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r;"
    );
  });
  t.truthy(error);
  t.true(error instanceof turso.TimeoutError);
  t.is(error.code, "TIMEOUT");

  await db.close();
});

test.serial("defaultQueryTimeout allows short-running query", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only test");
    return;
  }
  const turso = await import("@tursodatabase/serverless");
  const db = turso.connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
    defaultQueryTimeout: 5000,
  });

  const result = await db.execute("SELECT 1 AS value");
  t.is(result.rows.length, 1);
  t.is(result.rows[0].value, 1);

  await db.close();
});

test.serial("Per-query queryTimeout interrupts long-running query", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only test");
    return;
  }
  const turso = await import("@tursodatabase/serverless");
  const db = turso.connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  const error = await t.throwsAsync(async () => {
    await db.execute(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r;",
      [],
      { queryTimeout: 100 }
    );
  });
  t.truthy(error);
  t.true(error instanceof turso.TimeoutError);
  t.is(error.code, "TIMEOUT");

  await db.close();
});

test.serial("Per-query queryTimeout is accepted by exec()", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only test");
    return;
  }
  const turso = await import("@tursodatabase/serverless");
  const db = turso.connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  await t.notThrowsAsync(async () => {
    await db.exec("SELECT 1", { queryTimeout: 5000 });
  });

  await db.close();
});

test.serial("Per-query queryTimeout on Statement.get()", async (t) => {
  if (process.env.PROVIDER !== "serverless") {
    t.pass("Skipping serverless-only test");
    return;
  }
  const turso = await import("@tursodatabase/serverless");
  const db = turso.connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  const stmt = await db.prepare("SELECT 1 AS value");
  const row = await stmt.get(undefined, { queryTimeout: 5000 });
  t.is(row.value, 1);

  await db.close();
});

const connect = async (path, options = {}) => {
  if (!path) {
    path = genDatabaseFilename();
  }
  const provider = process.env.PROVIDER;
  if (provider === "turso") {
    const turso = await import("@tursodatabase/database");
    const db = await turso.connect(path, options);
    return [db, path, turso.SqliteError];
  }
  if (provider === "libsql") {
    const libsql = await import("libsql/promise");
    const db = await libsql.connect(path, options);
    return [db, path, libsql.SqliteError, path];
  }
  if (provider === "serverless") {
    const turso = await import("@tursodatabase/serverless");
    const url = process.env.TURSO_DATABASE_URL;
    if (!url) {
      throw new Error("TURSO_DATABASE_URL is not set");
    }
    const authToken = process.env.TURSO_AUTH_TOKEN;
    const db = new turso.connect({
      url,
      authToken,
    });
    return [db, null, turso.SqliteError];
  }
};

/// Generate a unique database filename
const genDatabaseFilename = () => {
  return `test-${crypto.randomBytes(8).toString('hex')}.db`;
};

const cleanupDatabaseFiles = (path) => {
  for (const suffix of ["", "-wal", "-shm"]) {
    const file = path + suffix;
    if (fs.existsSync(file)) {
      fs.unlinkSync(file);
    }
  }
};
