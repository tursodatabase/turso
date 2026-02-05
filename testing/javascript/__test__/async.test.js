import test from "ava";
import crypto from 'crypto';
import fs from 'fs';


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

test.skip("Concurrent writes over same connection", async (t) => {
  const db = t.context.db;
  await db.exec(`
    DROP TABLE IF EXISTS users;
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
  `);
  const stmt = await db.prepare("INSERT INTO users(name, email) VALUES (:name, :email)");
  const promises = [];
  for (let i = 0; i < 1000; i++) {
    promises.push(stmt.run({ name: "Alice", email: "alice@example.org" }));
  }
  await Promise.all(promises);
  const stmt2 = await db.prepare("SELECT * FROM users ORDER BY name");
  const rows = await stmt2.all();
  t.is(rows.length, 1000);
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
