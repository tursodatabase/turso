import crypto from 'crypto';
import fs from 'fs';
import DualTest from "./dual-test.mjs";

const dualTest = new DualTest();

new DualTest(":memory:").both("Open in-memory database", async (t) => {
  const db = t.context.db;
  t.is(db.memory, true);
});

dualTest.beforeEach(async (t) => {
  const db = t.context.db;

  db.exec(`
      DROP TABLE IF EXISTS users;
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
  `);
  db.exec(
    "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')"
  );
  db.exec(
    "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')"
  );
});

dualTest.onlySqlitePasses("Statement.prepare() error", async (t) => {
  const db = t.context.db;

  t.throws(() => {
    return db.prepare("SYNTAX ERROR");
  }, {
    any: true,
    instanceOf: t.context.errorType,
    message: 'near "SYNTAX": syntax error'
  });
});

dualTest.both("Statement.run() returning rows", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT 1");
  const info = stmt.run();
  t.is(info.changes, 0);
});

dualTest.both("Statement.run() [positional]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("INSERT INTO users(name, email) VALUES (?, ?)");
  const info = stmt.run(["Carol", "carol@example.net"]);
  t.is(info.changes, 1);
  t.is(info.lastInsertRowid, 3);

  // Verify that the data is inserted
  const stmt2 = db.prepare("SELECT * FROM users WHERE id = 3");
  t.is(stmt2.get().name, "Carol");
  t.is(stmt2.get().email, "carol@example.net");
});

dualTest.both("Statement.run() [named]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("INSERT INTO users(name, email) VALUES (@name, @email);");
  const info = stmt.run({ "name": "Carol", "email": "carol@example.net" });
  t.is(info.changes, 1);
  t.is(info.lastInsertRowid, 3);
});

dualTest.both("Statement.get() returns no rows", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users WHERE id = 0");
  t.is(stmt.get(), undefined);
});

dualTest.both("Statement.get() [no parameters]", async (t) => {
  const db = t.context.db;

  var stmt = 0;

  stmt = db.prepare("SELECT * FROM users");
  t.is(stmt.get().name, "Alice");
  t.deepEqual(stmt.raw().get(), [1, 'Alice', 'alice@example.org']);
});

dualTest.onlySqlitePasses("Statement.get() [positional]", async (t) => {
  const db = t.context.db;

  var stmt = 0;

  stmt = db.prepare("SELECT * FROM users WHERE id = ?");
  t.is(stmt.get(0), undefined);
  t.is(stmt.get([0]), undefined);
  t.is(stmt.get(1).name, "Alice");
  t.is(stmt.get(2).name, "Bob");

  stmt = db.prepare("SELECT * FROM users WHERE id = ?1");
  t.is(stmt.get({ 1: 0 }), undefined);
  t.is(stmt.get({ 1: 1 }).name, "Alice");
  t.is(stmt.get({ 1: 2 }).name, "Bob");
});

dualTest.onlySqlitePasses("Statement.get() [named]", async (t) => {
  const db = t.context.db;

  var stmt = undefined;

  stmt = db.prepare("SELECT :b, :a");
  t.deepEqual(stmt.raw().get({ a: 'a', b: 'b' }), ['b', 'a']);

  stmt = db.prepare("SELECT * FROM users WHERE id = :id");
  t.is(stmt.get({ id: 0 }), undefined);
  t.is(stmt.get({ id: 1 }).name, "Alice");
  t.is(stmt.get({ id: 2 }).name, "Bob");

  stmt = db.prepare("SELECT * FROM users WHERE id = @id");
  t.is(stmt.get({ id: 0 }), undefined);
  t.is(stmt.get({ id: 1 }).name, "Alice");
  t.is(stmt.get({ id: 2 }).name, "Bob");

  stmt = db.prepare("SELECT * FROM users WHERE id = $id");
  t.is(stmt.get({ id: 0 }), undefined);
  t.is(stmt.get({ id: 1 }).name, "Alice");
  t.is(stmt.get({ id: 2 }).name, "Bob");
});

dualTest.both("Statement.get() [raw]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users WHERE id = ?");
  t.deepEqual(stmt.raw().get(1), [1, "Alice", "alice@example.org"]);
});

dualTest.onlySqlitePasses("Statement.iterate() [empty]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users WHERE id = 0");
  t.is(stmt.iterate().next().done, true);
  t.is(stmt.iterate([]).next().done, true);
  t.is(stmt.iterate({}).next().done, true);
});

dualTest.both("Statement.iterate()", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [1, 2];
  var idx = 0;
  for (const row of stmt.iterate()) {
    t.is(row.id, expected[idx++]);
  }
});

dualTest.both("Statement.all()", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [
    { id: 1, name: "Alice", email: "alice@example.org" },
    { id: 2, name: "Bob", email: "bob@example.com" },
  ];
  t.deepEqual(stmt.all(), expected);
});

dualTest.both("Statement.all() [raw]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [
    [1, "Alice", "alice@example.org"],
    [2, "Bob", "bob@example.com"],
  ];
  t.deepEqual(stmt.raw().all(), expected);
});

dualTest.both("Statement.all() [pluck]", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [
    1,
    2,
  ];
  t.deepEqual(stmt.pluck().all(), expected);
});

dualTest.both(
  "Statement.raw() [passing false should disable raw mode]",
  async (t) => {
    const db = t.context.db;

    const stmt = db.prepare("SELECT * FROM users");
    const expected = [
      { id: 1, name: "Alice", email: "alice@example.org" },
      { id: 2, name: "Bob", email: "bob@example.com" },
    ];
    t.deepEqual(stmt.raw(false).all(), expected);
  },
);

dualTest.both(
  "Statement.pluck() [passing false should disable pluck mode]",
  async (t) => {
    const db = t.context.db;

    const stmt = db.prepare("SELECT * FROM users");
    const expected = [
      { id: 1, name: "Alice", email: "alice@example.org" },
      { id: 2, name: "Bob", email: "bob@example.com" },
    ];
    t.deepEqual(stmt.pluck(false).all(), expected);
  },
);

dualTest.onlySqlitePasses(
  "Statement.all() [default safe integers]",
  async (t) => {
    const db = t.context.db;
    db.defaultSafeIntegers();
    const stmt = db.prepare("SELECT * FROM users");
    const expected = [
      [1n, "Alice", "alice@example.org"],
      [2n, "Bob", "bob@example.com"],
    ];
    t.deepEqual(stmt.raw().all(), expected);
  },
);

dualTest.onlySqlitePasses(
  "Statement.all() [statement safe integers]",
  async (t) => {
    const db = t.context.db;
    const stmt = db.prepare("SELECT * FROM users");
    stmt.safeIntegers();
    const expected = [
      [1n, "Alice", "alice@example.org"],
      [2n, "Bob", "bob@example.com"],
    ];
    t.deepEqual(stmt.raw().all(), expected);
  },
);

dualTest.onlySqlitePasses("Statement.raw() [failure]", async (t) => {
  const db = t.context.db;
  const stmt = db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
  await t.throws(() => {
    stmt.raw()
  }, {
    message: 'The raw() method is only for statements that return data'
  });
});

dualTest.onlySqlitePasses("Statement.run() with array bind parameter", async (t) => {
  const db = t.context.db;

  db.exec(`
      DROP TABLE IF EXISTS t;
      CREATE TABLE t (value BLOB);
  `);

  const array = [1, 2, 3];

  const insertStmt = db.prepare("INSERT INTO t (value) VALUES (?)");
  await t.throws(() => {
    insertStmt.run([array]);
  }, {
    message: 'SQLite3 can only bind numbers, strings, bigints, buffers, and null'
  });
});

dualTest.onlySqlitePasses("Statement.run() with Float32Array bind parameter", async (t) => {
  const db = t.context.db;

  db.exec(`
      DROP TABLE IF EXISTS t;
      CREATE TABLE t (value BLOB);
  `);

  const array = new Float32Array([1, 2, 3]);

  const insertStmt = db.prepare("INSERT INTO t (value) VALUES (?)");
  insertStmt.run([array]);

  const selectStmt = db.prepare("SELECT value FROM t");
  t.deepEqual(selectStmt.raw().get()[0], Buffer.from(array.buffer));
});

/// This test is not supported by better-sqlite3, but is supported by libsql.
/// Therefore, when implementing it in Turso, only enable the test for Turso.
dualTest.skip("Statement.run() for vector feature with Float32Array bind parameter", async (t) => {
  const db = t.context.db;

  db.exec(`
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (embedding FLOAT32(8));
    CREATE INDEX t_idx ON t ( libsql_vector_idx(embedding) );
  `);

  const insertStmt = db.prepare("INSERT INTO t VALUES (?)");
  insertStmt.run([new Float32Array([1, 1, 1, 1, 1, 1, 1, 1])]);
  insertStmt.run([new Float32Array([-1, -1, -1, -1, -1, -1, -1, -1])]);

  const selectStmt = db.prepare("SELECT embedding FROM vector_top_k('t_idx', vector('[2,2,2,2,2,2,2,2]'), 1) n JOIN t ON n.rowid = t.rowid");
  t.deepEqual(selectStmt.raw().get()[0], Buffer.from(new Float32Array([1, 1, 1, 1, 1, 1, 1, 1]).buffer));

  // we need to explicitly delete this table because later when sqlite-based (not LibSQL) tests will delete table 't' they will leave 't_idx_shadow' table untouched
  db.exec(`DROP TABLE t`);
});

dualTest.onlySqlitePasses("Statement.columns()", async (t) => {
  const db = t.context.db;

  var stmt = undefined;

  stmt = db.prepare("SELECT 1");
  t.deepEqual(stmt.columns(), [
    {
      column: null,
      database: null,
      name: '1',
      table: null,
      type: null,
    },
  ]);

  stmt = db.prepare("SELECT * FROM users WHERE id = ?");
  t.deepEqual(stmt.columns(), [
    {
      column: "id",
      database: "main",
      name: "id",
      table: "users",
      type: "INTEGER",
    },
    {
      column: "name",
      database: "main",
      name: "name",
      table: "users",
      type: "TEXT",
    },
    {
      column: "email",
      database: "main",
      name: "email",
      table: "users",
      type: "TEXT",
    },
  ]);
});

dualTest.onlySqlitePasses("Database.transaction()", async (t) => {
  const db = t.context.db;

  const insert = db.prepare(
    "INSERT INTO users(name, email) VALUES (:name, :email)"
  );

  const insertMany = db.transaction((users) => {
    t.is(db.inTransaction, true);
    for (const user of users) insert.run(user);
  });

  t.is(db.inTransaction, false);
  insertMany([
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
    { name: "Junior", email: "junior@example.org" },
  ]);
  t.is(db.inTransaction, false);

  const stmt = db.prepare("SELECT * FROM users WHERE id = ?");
  t.is(stmt.get(3).name, "Joey");
  t.is(stmt.get(4).name, "Sally");
  t.is(stmt.get(5).name, "Junior");
});

dualTest.onlySqlitePasses("Database.transaction().immediate()", async (t) => {
  const db = t.context.db;
  const insert = db.prepare(
    "INSERT INTO users(name, email) VALUES (:name, :email)"
  );
  const insertMany = db.transaction((users) => {
    t.is(db.inTransaction, true);
    for (const user of users) insert.run(user);
  });
  t.is(db.inTransaction, false);
  insertMany.immediate([
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
    { name: "Junior", email: "junior@example.org" },
  ]);
  t.is(db.inTransaction, false);
});

dualTest.onlySqlitePasses("values", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT ?").raw();
  t.deepEqual(stmt.get(1), [1]);
  t.deepEqual(stmt.get(Number.MIN_VALUE), [Number.MIN_VALUE]);
  t.deepEqual(stmt.get(Number.MAX_VALUE), [Number.MAX_VALUE]);
  t.deepEqual(stmt.get(Number.MAX_SAFE_INTEGER), [Number.MAX_SAFE_INTEGER]);
  t.deepEqual(stmt.get(9007199254740991n), [9007199254740991]);
});

dualTest.both("Database.pragma()", async (t) => {
  const db = t.context.db;
  db.pragma("cache_size = 2000");
  t.deepEqual(db.pragma("cache_size"), [{ "cache_size": 2000 }]);
});

dualTest.both("errors", async (t) => {
  const db = t.context.db;

  const syntaxError = await t.throws(() => {
    db.exec("SYNTAX ERROR");
  }, {
    any: true,
    instanceOf: t.context.errorType,
    message: /near "SYNTAX": syntax error/,
    code: 'SQLITE_ERROR'
  });
  const noTableError = await t.throws(() => {
    db.exec("SELECT * FROM missing_table");
  }, {
    any: true,
    instanceOf: t.context.errorType,
    message: /(Parse error: Table missing_table not found|no such table: missing_table)/,
    code: 'SQLITE_ERROR'
  });

  if (t.context.provider === 'libsql') {
    t.is(noTableError.rawCode, 1)
    t.is(syntaxError.rawCode, 1)
  }
});

dualTest.onlySqlitePasses("Database.prepare() after close()", async (t) => {
  const db = t.context.db;
  db.close();
  t.throws(() => {
    db.prepare("SELECT 1");
  }, {
    instanceOf: TypeError,
    message: "The database connection is not open"
  });
});

dualTest.onlySqlitePasses("Database.exec() after close()", async (t) => {
  const db = t.context.db;
  db.close();
  t.throws(() => {
    db.exec("SELECT 1");
  }, {
    instanceOf: TypeError,
    message: "The database connection is not open"
  });
});

/// Generate a unique database filename
const genDatabaseFilename = () => {
  return `test-${crypto.randomBytes(8).toString('hex')}.db`;
};

new DualTest(genDatabaseFilename).onlySqlitePasses("Timeout option", async (t) => {
  t.teardown(() => fs.unlinkSync(t.context.path));

  const timeout = 1000;
  const { db: conn1 } = t.context;
  conn1.exec("CREATE TABLE t(x)");
  conn1.exec("BEGIN IMMEDIATE");
  conn1.exec("INSERT INTO t VALUES (1)")
  const options = { timeout };
  const conn2 = t.context.connect(t.context.path, options);
  const start = Date.now();
  try {
    conn2.exec("INSERT INTO t VALUES (1)")
  } catch (e) {
    t.is(e.code, "SQLITE_BUSY");
    const end = Date.now();
    const elapsed = end - start;
    // Allow some tolerance for the timeout.
    t.is(elapsed > timeout / 2, true);
  }
  conn1.close();
  conn2.close();
});

