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
// Database.batch()
//
// batch() returns an Array<ResultSet> — one entry per input statement, in
// the order the statements were supplied — matching the libSQL client
// contract. Each ResultSet has the shape:
//
//   { columns, columnTypes, rows, rowsAffected }
//
//   - columns / columnTypes are parallel arrays of the result's column
//     names and declared types. Both are empty for statements that return
//     no rows (INSERT/UPDATE/DELETE).
//   - rows is an Array<Row>. By default rows match Statement.all() object
//     rows; pass { raw: true } to receive arrays.
//   - rowsAffected is the per-statement change count (0 for SELECT).
// ==========================================================================

// Assert the ResultSet invariants shared by every batch entry, regardless
// of statement kind.
const assertResultSetShape = (t, rs) => {
  t.true(Array.isArray(rs.columns), "columns is an array");
  t.true(Array.isArray(rs.columnTypes), "columnTypes is an array");
  t.is(rs.columnTypes.length, rs.columns.length, "columnTypes parallels columns");
  t.true(Array.isArray(rs.rows), "rows is an array");
  t.is(typeof rs.rowsAffected, "number", "rowsAffected is a number");
  t.false("lastInsertRowid" in rs, "batch ResultSet does not expose lastInsertRowid");
  t.is(rs.toJSON, undefined, "batch ResultSet does not expose toJSON");
};

test.serial("Database.batch() [returns one ResultSet per statement, in order]", async (t) => {
  const db = t.context.db;

  const results = await db.batch([
    "INSERT INTO users(name, email) VALUES ('Carol', 'carol@example.net')",
    "SELECT id, name FROM users ORDER BY id",
    "DELETE FROM users WHERE name = 'Carol'",
  ]);

  t.true(Array.isArray(results), "batch() returns an array");
  t.is(results.length, 3, "one ResultSet per input statement");
  results.forEach((rs) => assertResultSetShape(t, rs));

  // INSERT: no result rows, one row affected.
  t.deepEqual(results[0].columns, []);
  t.deepEqual(results[0].rows, []);
  t.is(results[0].rowsAffected, 1);

  // SELECT: rows surfaced, nothing affected.
  t.deepEqual(results[1].columns, ["id", "name"]);
  t.is(results[1].rows.length, 3);
  t.is(results[1].rowsAffected, 0);

  // DELETE: no result rows, one row affected.
  t.deepEqual(results[2].columns, []);
  t.deepEqual(results[2].rows, []);
  t.is(results[2].rowsAffected, 1);
});

test.serial("Database.batch() [SELECT rows expose Statement.all() object shape]", async (t) => {
  const db = t.context.db;

  const [rs] = await db.batch([
    "SELECT id, name, email FROM users WHERE id = 1",
  ]);

  assertResultSetShape(t, rs);
  t.deepEqual(rs.columns, ["id", "name", "email"]);
  t.is(rs.rows.length, 1);

  const row = rs.rows[0];
  t.false(Array.isArray(row));
  t.is(row.id, 1);
  t.is(row.name, "Alice");
  t.is(row.email, "alice@example.org");
});

test.serial("Database.batch() [later statements observe earlier writes]", async (t) => {
  const db = t.context.db;

  // The statements run sequentially on the same connection, so a read can
  // see a write that an earlier statement in the same batch performed.
  const results = await db.batch([
    { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Grace", "grace@example.net"] },
    "SELECT name FROM users WHERE name = 'Grace'",
  ]);

  t.is(results[0].rowsAffected, 1);
  t.is(results[1].rows.length, 1, "the SELECT observes the row inserted earlier in the batch");
  t.is(results[1].rows[0].name, "Grace");
});

test.serial("Database.batch() [empty batch returns empty array]", async (t) => {
  const db = t.context.db;

  const results = await db.batch([]);
  t.deepEqual(results, []);
});

test.serial("Database.batch() [raw option returns array rows]", async (t) => {
  const db = t.context.db;

  const [rs] = await db.batch(["SELECT id, name FROM users ORDER BY id"], { raw: true });

  t.deepEqual(rs.columns, ["id", "name"]);
  t.deepEqual(rs.rows[0], [1, "Alice"]);
  t.deepEqual(rs.rows[1], [2, "Bob"]);
});

test.serial("Database.batch() [bound args]", async (t) => {
  const db = t.context.db;

  const results = await db.batch([
    { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Carol", "carol@example.net"] },
    { sql: "INSERT INTO users(name, email) VALUES (:name, :email)", args: { name: "Dave", email: "dave@example.net" } },
  ]);

  t.is(results.length, 2);
  t.is(results[0].rowsAffected, 1);
  t.is(results[1].rowsAffected, 1);

  const rows = await db.all("SELECT name, email FROM users WHERE id IN (3, 4) ORDER BY id");
  t.deepEqual(rows, [
    { name: "Carol", email: "carol@example.net" },
    { name: "Dave", email: "dave@example.net" },
  ]);
});

test.serial("Database.batch() [plain strings]", async (t) => {
  const db = t.context.db;

  const results = await db.batch([
    "INSERT INTO users(name, email) VALUES ('Eve', 'eve@example.net')",
    "INSERT INTO users(name, email) VALUES ('Frank', 'frank@example.net')",
  ]);

  t.is(results.length, 2);
  t.is(results[0].rowsAffected, 1);
  t.is(results[1].rowsAffected, 1);

  const rows = await db.all("SELECT name, email FROM users WHERE id IN (3, 4) ORDER BY id");
  t.deepEqual(rows, [
    { name: "Eve", email: "eve@example.net" },
    { name: "Frank", email: "frank@example.net" },
  ]);
});

test.serial("Database.batch() [mixed value types]", async (t) => {
  const db = t.context.db;

  await db.exec("DROP TABLE IF EXISTS types_batch");
  await db.exec(`
    CREATE TABLE types_batch (
      id INTEGER PRIMARY KEY,
      i INTEGER,
      r REAL,
      s TEXT,
      b BLOB,
      n INTEGER
    )
  `);

  const blob = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF]);

  const results = await db.batch([
    {
      sql: "INSERT INTO types_batch(i, r, s, b, n) VALUES (?, ?, ?, ?, ?)",
      args: [42, 3.14, "hello", blob, null],
    },
    {
      sql: "INSERT INTO types_batch(i, r, s, b, n) VALUES (:i, :r, :s, :b, :n)",
      args: { i: -7, r: -0.5, s: "", b: Buffer.alloc(0), n: null },
    },
    {
      sql: "INSERT INTO types_batch(i) VALUES (?)",
      args: [9007199254740993n],
    },
    // A trailing read exercises type fidelity through ResultSet rows.
    "SELECT i, r, s, b, n FROM types_batch ORDER BY id LIMIT 2",
  ]);

  t.is(results.length, 4);
  t.is(results[0].rowsAffected, 1);
  t.is(results[1].rowsAffected, 1);
  t.is(results[2].rowsAffected, 1);

  // Values survive the round-trip into the SELECT's ResultSet rows.
  const selectRows = results[3].rows;
  t.is(selectRows.length, 2);
  t.is(selectRows[0].i, 42);
  t.is(selectRows[0].r, 3.14);
  t.is(selectRows[0].s, "hello");
  t.deepEqual(selectRows[0].b, blob);
  t.is(selectRows[0].n, null);

  const rows = await db.all("SELECT i, r, s, b, n FROM types_batch ORDER BY id");
  t.is(rows.length, 3);

  t.is(rows[0].i, 42);
  t.is(rows[0].r, 3.14);
  t.is(rows[0].s, "hello");
  t.deepEqual(rows[0].b, blob);
  t.is(rows[0].n, null);

  t.is(rows[1].i, -7);
  t.is(rows[1].r, -0.5);
  t.is(rows[1].s, "");
  t.deepEqual(rows[1].b, Buffer.alloc(0));
  t.is(rows[1].n, null);

  // BigInt is round-tripped as Number when within safe-integer range; for
  // values above 2^53 we just confirm that the magnitude survived storage.
  const bigRow = await db.get("SELECT CAST(i AS TEXT) AS i FROM types_batch WHERE id = 3");
  t.is(bigRow.i, "9007199254740993");
});

test.serial("Database.batch() [rollback via transactionAsync()]", async (t) => {
  const db = t.context.db;

  // batch() itself is not transactional; transaction() provides
  // all-or-nothing semantics around the failing batch.
  const txn = db.transactionAsync(async (tx) => {
    return await tx.batch([
      { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Mallory", "mallory@example.net"] },
      // Duplicate primary key with the row inserted in beforeEach.
      { sql: "INSERT INTO users(id, name, email) VALUES (1, 'dup', 'dup@example.net')" },
    ]);
  });

  await t.throwsAsync(async () => { await txn(); }, { any: true });

  const rows = await db.all("SELECT name FROM users WHERE name = 'Mallory'");
  t.deepEqual(rows, []);
});

test.serial("Database.batch() [atomic mode]", async (t) => {
  const db = t.context.db;

  const results = await db.batch([
    { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Ivy", "ivy@example.net"] },
    { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Jay", "jay@example.net"] },
  ], "immediate");

  t.is(results.length, 2);
  t.is(results[0].rowsAffected, 1);
  t.is(results[1].rowsAffected, 1);

  const rows = await db.all("SELECT name, email FROM users WHERE id IN (3, 4) ORDER BY id");
  t.deepEqual(rows, [
    { name: "Ivy", email: "ivy@example.net" },
    { name: "Jay", email: "jay@example.net" },
  ]);
});

test.serial("Database.batch() [atomic mode rolls back on failure]", async (t) => {
  const db = t.context.db;

  await t.throwsAsync(async () => {
    await db.batch([
      { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Kelly", "kelly@example.net"] },
      // Duplicate primary key with the row inserted in beforeEach.
      { sql: "INSERT INTO users(id, name, email) VALUES (1, 'dup', 'dup@example.net')" },
    ], "immediate");
  }, { any: true });

  const rows = await db.all("SELECT name FROM users WHERE name = 'Kelly'");
  t.deepEqual(rows, []);
});

test.serial("Database.batch() [atomic mode joins a manually-opened outer transaction]", async (t) => {
  const db = t.context.db;

  // Manually open an outer transaction on the same stream and write a
  // row inside it.
  await db.exec("BEGIN");
  await db.run("INSERT INTO users(name, email) VALUES ('Outer', 'outer@example.net')");

  // A transaction is already open on this stream, so the batch's mode is
  // ignored and its statements join the outer transaction instead of
  // emitting a nested BEGIN — the same contract as batch() inside a
  // transaction() callback.
  await db.batch([
    { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Inner", "inner@example.net"] },
  ], "immediate");

  t.true(db.inTransaction, "outer transaction is still open after the batch");

  // Rolling back the outer transaction must undo the batch's INSERT too —
  // that proves the batch joined it rather than committing on its own.
  await db.exec("ROLLBACK");

  const names = (await db.all("SELECT name FROM users ORDER BY id")).map(r => r.name);
  t.false(names.includes("Outer"), "outer transaction's INSERT was rolled back");
  t.false(names.includes("Inner"), "batch INSERT rolled back with the outer transaction");
});

test.serial("Database.batch() [non-insert batch does not expose lastInsertRowid]", async (t) => {
  const db = t.context.db;

  // Seed an INSERT first so the connection has a non-zero lastInsertRowid.
  await db.run("INSERT INTO users(name, email) VALUES (?, ?)", "Seed", "seed@example.net");

  // Now run a batch that only mutates without inserting. SQLite keeps
  // last_insert_rowid() sticky across non-INSERT statements, so we
  // must derive the per-statement INSERT signal from the delta, not
  // from `changes > 0`.
  const results = await db.batch([
    { sql: "UPDATE users SET email = ? WHERE id = 1", args: ["alice2@example.org"] },
    { sql: "DELETE FROM users WHERE id = 2" },
  ]);

  t.is(results[0].rowsAffected, 1);
  t.false("lastInsertRowid" in results[0]);
  t.is(results[1].rowsAffected, 1);
  t.false("lastInsertRowid" in results[1]);
});


test.serial("Database.transactionAsync().deferred() [batch]", async (t) => {
  const db = t.context.db;

  const insertMany = db.transactionAsync(async (tx) => {
    t.is(db.inTransaction, true);
    return await tx.batch([
      { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Joey", "joey@example.org"] },
      { sql: "INSERT INTO users(name, email) VALUES (?, ?)", args: ["Sally", "sally@example.org"] },
      { sql: "INSERT INTO users(name, email) VALUES (:name, :email)", args: { name: "Junior", email: "junior@example.org" } },
    ]);
  });

  const results = await insertMany.deferred();
  t.is(db.inTransaction, false);
  t.is(results.length, 3);
  results.forEach((rs) => t.is(rs.rowsAffected, 1));

  const rows = await db.all("SELECT name, email FROM users WHERE id IN (3, 4, 5) ORDER BY id");
  t.deepEqual(rows, [
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
    { name: "Junior", email: "junior@example.org" },
  ]);
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

test.serial("Database.inTransaction property", async (t) => {
  const db = t.context.db;

  // 1. A fresh connection is in autocommit, not a transaction.
  t.false(db.inTransaction, "fresh connection is not in a transaction");

  // 2. A one-shot query runs in autocommit: the moment it returns, the
  //    connection is no longer in a transaction.
  const stmt = await db.prepare("SELECT 1 AS n");
  await stmt.get();
  t.false(db.inTransaction, "autocommit after a one-shot query");

  // 3. The transaction() helper reports in-transaction inside its callback and
  //    autocommit once it completes.
  let insideTxn;
  const txn = db.transactionAsync(async (tx) => { insideTxn = db.inTransaction; });
  await txn();
  t.true(insideTxn, "in a transaction inside the transactionAsync() callback");
  t.false(db.inTransaction, "autocommit after transactionAsync() completes");

  // 4. inTransaction must reflect the real transaction state, so it also tracks
  //    transactions opened with raw BEGIN/COMMIT/ROLLBACK.
  await db.exec("BEGIN");
  t.true(db.inTransaction, "in a transaction after raw BEGIN");
  await db.exec("COMMIT");
  t.false(db.inTransaction, "autocommit after raw COMMIT");

  await db.exec("BEGIN");
  t.true(db.inTransaction, "in a transaction after raw BEGIN");
  await db.exec("ROLLBACK");
  t.false(db.inTransaction, "autocommit after raw ROLLBACK");
});

test.serial("Database.inTransaction tracks raw BEGIN via run()", async (t) => {
  const db = t.context.db;

  // inTransaction must reflect the real transaction state no matter which
  // API opened or closed the transaction, not just exec().
  await db.run("BEGIN IMMEDIATE");
  t.true(db.inTransaction, "in a transaction after raw BEGIN via run()");
  await db.run("ROLLBACK");
  t.false(db.inTransaction, "autocommit after raw ROLLBACK via run()");
});

test.serial("Database.inTransaction after failed batch() with raw BEGIN", async (t) => {
  const db = t.context.db;

  // A UNIQUE constraint failure has statement-level ABORT semantics: it
  // aborts the failing INSERT but leaves the surrounding explicit
  // transaction open. inTransaction must report that open transaction —
  // cleanup code relies on it to decide whether a ROLLBACK is needed, and
  // a stale false here leaks a server-side write transaction that starves
  // every other writer.
  await t.throwsAsync(async () => {
    await db.batch([
      "BEGIN IMMEDIATE",
      "INSERT INTO users (id, name, email) VALUES (100, 'Carol', 'carol@example.org')",
      "INSERT INTO users (id, name, email) VALUES (100, 'Dave', 'dave@example.org')",
    ]);
  });

  t.true(db.inTransaction, "transaction is still open after the constraint error");

  await db.exec("ROLLBACK");
  t.false(db.inTransaction, "autocommit after ROLLBACK on the same connection");

  const rows = await db.all("SELECT COUNT(*) AS n FROM users WHERE id = 100");
  t.is(rows[0].n, 0, "ROLLBACK undid the batch's successful INSERT");
});

test.serial("Database.transactionAsync()", async (t) => {
  const db = t.context.db;

  const insertMany = db.transactionAsync(async (tx, users) => {
    t.is(db.inTransaction, true);
    // statements of the transaction must be prepared from its handle: the
    // wrapper owns the connection lock for the whole transaction, so
    // database-level statements would wait for it instead of joining it
    const insert = await tx.prepare(
      "INSERT INTO users(name, email) VALUES (:name, :email)"
    );
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

// The deprecated transaction() keeps the pre-transactionAsync contract: the
// callback receives the call's own arguments and statements issued on the
// database (or prepared from it) join the transaction.
test.serial("Database.transaction() [deprecated]", async (t) => {
  const db = t.context.db;

  const insert = await db.prepare(
    "INSERT INTO users(name, email) VALUES (:name, :email)"
  );

  const insertMany = db.transaction(async (users) => {
    t.is(db.inTransaction, true);
    for (const user of users) await insert.run(user);
  });

  await insertMany([
    { name: "Joey", email: "joey@example.org" },
    { name: "Sally", email: "sally@example.org" },
  ]);
  t.is(db.inTransaction, false);

  const stmt = await db.prepare("SELECT * FROM users WHERE id = ?");
  t.is((await stmt.get(3)).name, "Joey");
  t.is((await stmt.get(4)).name, "Sally");
});

test.serial("Database.transactionAsync().immediate()", async (t) => {
  const db = t.context.db;
  const insertMany = db.transactionAsync(async (tx, users) => {
    t.is(db.inTransaction, true);
    const insert = await tx.prepare(
      "INSERT INTO users(name, email) VALUES (:name, :email)"
    );
    for (const user of users) await insert.run(user);
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
// Database.run() / get() / all() / iterate()
//
// Convenience wrappers on Database that prepare and execute the SQL in one
// call. These are libsql extensions (not in better-sqlite3); other providers
// are expected to match the libsql shape.
// ==========================================================================

test.serial("Database.run() [positional]", async (t) => {
  const db = t.context.db;

  const info = await db.run(
    "INSERT INTO users(name, email) VALUES (?, ?)",
    "Carol",
    "carol@example.net"
  );
  t.is(info.changes, 1);
  t.is(info.lastInsertRowid, 3);

  const row = await db.get("SELECT name, email FROM users WHERE id = ?", 3);
  t.is(row.name, "Carol");
  t.is(row.email, "carol@example.net");
});

test.serial("Database.run() [named]", async (t) => {
  const db = t.context.db;

  const info = await db.run(
    "INSERT INTO users(name, email) VALUES (:name, :email)",
    { name: "Carol", email: "carol@example.net" }
  );
  t.is(info.changes, 1);
  t.is(info.lastInsertRowid, 3);
});

test.serial("Database.get() returns no rows", async (t) => {
  const db = t.context.db;
  t.is(await db.get("SELECT * FROM users WHERE id = ?", 0), undefined);
});

test.serial("Database.get() [positional]", async (t) => {
  const db = t.context.db;
  t.is((await db.get("SELECT * FROM users WHERE id = ?", 1)).name, "Alice");
  t.is((await db.get("SELECT * FROM users WHERE id = ?", 2)).name, "Bob");
});

test.serial("Database.get() [named]", async (t) => {
  const db = t.context.db;
  t.is(
    (await db.get("SELECT * FROM users WHERE id = :id", { id: 1 })).name,
    "Alice"
  );
  t.is(
    (await db.get("SELECT * FROM users WHERE id = @id", { id: 2 })).name,
    "Bob"
  );
});

test.serial("Database.all()", async (t) => {
  const db = t.context.db;
  const expected = [
    { id: 1, name: "Alice", email: "alice@example.org" },
    { id: 2, name: "Bob", email: "bob@example.com" },
  ];
  t.deepEqual(await db.all("SELECT * FROM users"), expected);
});

test.serial("Database.all() [positional]", async (t) => {
  const db = t.context.db;
  const expected = [{ id: 1, name: "Alice", email: "alice@example.org" }];
  t.deepEqual(await db.all("SELECT * FROM users WHERE id = ?", 1), expected);
});

test.serial("Database.iterate()", async (t) => {
  const db = t.context.db;
  const expected = [1, 2];
  let idx = 0;
  for await (const row of await db.iterate("SELECT * FROM users")) {
    t.is(row.id, expected[idx++]);
  }
  t.is(idx, 2);
});

test.serial("Database.iterate() [positional]", async (t) => {
  const db = t.context.db;
  let count = 0;
  for await (const row of await db.iterate(
    "SELECT * FROM users WHERE id = ?",
    2
  )) {
    t.is(row.name, "Bob");
    count++;
  }
  t.is(count, 1);
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

test.serial("Database.all() collapses duplicate column names", async (t) => {
  const db = t.context.db;

  await db.exec("DROP TABLE IF EXISTS role; DROP TABLE IF EXISTS org_unit");
  await db.exec("CREATE TABLE role(path TEXT); CREATE TABLE org_unit(path TEXT)");
  await db.exec("INSERT INTO role VALUES ('/Employee'); INSERT INTO org_unit VALUES ('/')");

  const [row] = await db.all("SELECT role.path, org_unit.path FROM role JOIN org_unit");

  t.deepEqual(Object.keys(row), ["path"]);
  t.is(row.path, "/");
  t.is(row[0], undefined);
  t.is(row[1], undefined);
  t.deepEqual(row, { path: "/" });

  const stmt = await db.prepare("SELECT role.path, org_unit.path FROM role JOIN org_unit");
  t.deepEqual(await stmt.raw().get(), ["/Employee", "/"]);
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
// Big integers
//
// Coverage for the area around tursodatabase/turso#7556. Integers outside the
// JS safe-integer range (notably i64::MAX = 9223372036854775807, the value
// Turso's internal sequence metadata stores) must round-trip losslessly when
// safe integers are enabled. The default number mode is intentionally lossy
// above 2^53, matching better-sqlite3.
// ==========================================================================

test.serial("Big integers [round-trip with safe integers]", async (t) => {
  const db = t.context.db;
  await db.exec("DROP TABLE IF EXISTS bigints");
  await db.exec("CREATE TABLE bigints (id INTEGER PRIMARY KEY, v INTEGER)");

  const values = [
    [1, 9223372036854775807n], // i64::MAX
    [2, -9223372036854775808n], // i64::MIN
    [3, 9007199254740993n], // 2^53 + 1, first integer Number cannot represent
  ];
  for (const [id, v] of values) {
    await db.run("INSERT INTO bigints (id, v) VALUES (?, ?)", [id, v]);
  }

  const stmt = await db.prepare("SELECT v FROM bigints ORDER BY id");
  stmt.safeIntegers();
  t.deepEqual(await stmt.pluck().all(), values.map(([, v]) => v));
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

test.serial("Statement.iterate() with a nested query on same connection does not deadlock", async (t) => {
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
      await db.all("SELECT ? as echoed_id", [id]);
    }
  })();

  await t.notThrowsAsync(async () => {
    await withTimeout(run, 2000, "nested iterate/query");
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
    await db.all(
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

  const rows = await db.all("SELECT 1 AS value");
  t.is(rows.length, 1);
  t.is(rows[0].value, 1);

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
    await db.all(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r;",
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
