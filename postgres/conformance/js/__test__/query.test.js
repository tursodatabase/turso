import test from "ava";

import { connect } from "./_connect.js";

test.beforeEach(async (t) => {
  const { db, provider } = await connect();
  await db.exec(`
    CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, age INT);
    INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);
  `);
  t.context = { db, provider };
});

test.afterEach.always(async (t) => {
  if (t.context.db && !t.context.db.closed) {
    await t.context.db.close();
  }
});

// === db.query() ===

test.serial("query() returns rows as objects keyed by column name", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT name, age FROM users ORDER BY id");
  t.deepEqual(res.rows, [
    { name: "Alice", age: 30 },
    { name: "Bob", age: 25 },
  ]);
});

test.serial("query() returns fields with name and dataTypeID", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT id, name FROM users LIMIT 1");
  t.deepEqual(res.fields, [
    { name: "id", dataTypeID: 23 }, // INT4
    { name: "name", dataTypeID: 25 }, // TEXT
  ]);
});

test.serial("query() binds positional $n parameters", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT name FROM users WHERE age > $1 AND name != $2", [
    20,
    "Bob",
  ]);
  t.deepEqual(res.rows, [{ name: "Alice" }]);
});

test.serial("query() reports affectedRows for INSERT", async (t) => {
  const { db } = t.context;
  const res = await db.query("INSERT INTO users (name, age) VALUES ($1, $2), ($3, $4)", [
    "Carol",
    35,
    "Dave",
    40,
  ]);
  t.is(res.affectedRows, 2);
  t.deepEqual(res.rows, []);
});

test.serial("query() reports affectedRows for UPDATE and DELETE", async (t) => {
  const { db } = t.context;
  const upd = await db.query("UPDATE users SET age = age + 1");
  t.is(upd.affectedRows, 2);
  const del = await db.query("DELETE FROM users WHERE name = $1", ["Alice"]);
  t.is(del.affectedRows, 1);
});

test.serial("query() supports RETURNING", async (t) => {
  const { db } = t.context;
  const res = await db.query(
    "INSERT INTO users (name, age) VALUES ($1, $2) RETURNING id, name",
    ["Carol", 35],
  );
  t.is(res.affectedRows, 1);
  t.deepEqual(res.rows, [{ id: 3, name: "Carol" }]);
});

test.serial("query() with rowMode array returns rows as arrays", async (t) => {
  const { db } = t.context;
  const res = await db.query("SELECT id, name FROM users ORDER BY id", [], {
    rowMode: "array",
  });
  t.deepEqual(res.rows, [
    [1, "Alice"],
    [2, "Bob"],
  ]);
});

test.serial("query() rejects multiple statements", async (t) => {
  const { db } = t.context;
  await t.throwsAsync(db.query("SELECT 1; SELECT 2"));
});

test.serial("query() error carries SQLSTATE code and severity", async (t) => {
  const { db } = t.context;
  const err = await t.throwsAsync(db.query("SELECT * FROM no_such_table"));
  t.is(err.code, "42P01"); // undefined_table
  t.is(err.severity, "ERROR");
  t.regex(err.message, /no_such_table/);
});

test.serial("query() constraint violation error", async (t) => {
  const { db } = t.context;
  const err = await t.throwsAsync(
    db.query("INSERT INTO users (id, name) VALUES (1, 'dup')"),
  );
  t.is(err.code, "23505"); // unique_violation
});

// === db.sql`` tagged template ===

test.serial("sql`` template binds interpolated values as parameters", async (t) => {
  const { db } = t.context;
  const name = "Alice";
  const res = await db.sql`SELECT age FROM users WHERE name = ${name}`;
  t.deepEqual(res.rows, [{ age: 30 }]);
});

// === db.exec() ===

test.serial("exec() runs multiple statements and returns one result each", async (t) => {
  const { db } = t.context;
  const results = await db.exec(`
    INSERT INTO users (name, age) VALUES ('Carol', 35);
    SELECT count(*)::int4 AS n FROM users;
    DELETE FROM users;
  `);
  t.is(results.length, 3);
  t.deepEqual(results[0].rows, []);
  t.deepEqual(results[1].rows, [{ n: 3 }]);
  t.deepEqual(results[2].rows, []);
  // Note: affectedRows is not asserted here. PGlite groups simple-protocol
  // messages by RowDescription, which makes affectedRows accumulate across
  // statements without result sets — a quirk, not part of the contract.
  const remaining = await db.query("SELECT count(*)::int4 AS n FROM users");
  t.deepEqual(remaining.rows, [{ n: 0 }]);
});

test.serial("exec() is used for DDL", async (t) => {
  const { db } = t.context;
  await db.exec("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)");
  await db.query("INSERT INTO items VALUES ($1, $2)", [1, "first"]);
  const res = await db.query("SELECT label FROM items");
  t.deepEqual(res.rows, [{ label: "first" }]);
});

// === ON CONFLICT ===

test.serial("INSERT ON CONFLICT DO UPDATE upserts", async (t) => {
  const { db } = t.context;
  await db.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v INT)");
  await db.query("INSERT INTO kv VALUES ($1, $2)", ["a", 1]);
  await db.query(
    "INSERT INTO kv VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
    ["a", 2],
  );
  const res = await db.query("SELECT v FROM kv WHERE k = 'a'");
  t.deepEqual(res.rows, [{ v: 2 }]);
});

// === lifecycle ===

test.serial("ready is true after waitReady, closed after close()", async (t) => {
  const { db } = t.context;
  t.true(db.ready);
  t.false(db.closed);
  await db.close();
  t.true(db.closed);
  t.false(db.ready);
});

test.serial("query() after close() rejects", async (t) => {
  const { db } = t.context;
  await db.close();
  await t.throwsAsync(db.query("SELECT 1"));
});
