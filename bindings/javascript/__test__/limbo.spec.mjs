import test from "ava";
import fs from "node:fs";
import { fileURLToPath } from "url";
import path from "node:path";

import { Database } from "../wrapper.js";

test("Open in-memory database", async (t) => {
  const [db] = await connect(":memory:");
  t.is(db.memory, true);
});

test("Statement.get() returns data", async (t) => {
  const [db] = await connect(":memory:");
  const stmt = db.prepare("SELECT 1");
  const result = stmt.get();
  t.is(result["1"], 1);
  const result2 = stmt.get();
  t.is(result2["1"], 1);
});

test("Statement.get() returns null when no data", async (t) => {
  const [db] = await connect(":memory:");
  const stmt = db.prepare("SELECT 1 WHERE 1 = 2");
  const result = stmt.get();
  t.is(result, undefined);
});

// run() isn't 100% compatible with better-sqlite3
// it should return a result object, not a row object
test("Statement.run() returns correct result object", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  let rows = db.prepare("SELECT * FROM users").all();
  t.deepEqual(rows, [{ name: "Alice", age: 42 }]);
});

test("Statment.iterate() should correctly return an iterable object", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare(
    "CREATE TABLE users (name TEXT, age INTEGER, nationality TEXT)",
  ).run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run(
    ["Alice", 42],
    "UK",
  );
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run(
    "Bob",
    24,
    "USA",
  );

  let rows = db.prepare("SELECT * FROM users").iterate();
  for (const row of rows) {
    t.truthy(row.name);
    t.true(typeof row.age === "number");
  }
});

test("Empty prepared statement should throw", async (t) => {
  const [db] = await connect(":memory:");
  t.throws(
    () => {
      db.prepare("");
    },
    { instanceOf: Error },
  );
});

test("Test pragma()", async (t) => {
  const [db] = await connect(":memory:");
  t.true(typeof db.pragma("cache_size")[0].cache_size === "number");
  t.true(typeof db.pragma("cache_size", { simple: true }) === "number");
});

test("Statement binded with bind() shouldn't be binded again", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  let stmt = db.prepare("SELECT * FROM users WHERE name = ?").bind("Alice");

  for (const row of stmt.iterate()) {
    t.truthy(row.name);
    t.true(typeof row.age === "number");
  }

  t.throws(
    () => {
      db.bind("Bob");
    },
    { instanceOf: Error },
  );
});

test("Test pluck(): Rows should only have the values of the first column", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);
  let stmt = db.prepare("SELECT * FROM users").raw().pluck();

  for (const row of stmt.iterate()) {
    t.truthy(row.name);
    t.true(typeof row.age === "undefined");
  }
});


test("Test raw()", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);
  
  // Pluck and raw should be exclusive
  let stmt = db.prepare("SELECT * FROM users").pluck().raw();

  for (const row of stmt.iterate()) {
    t.true(Array.isArray(row));
    t.true(typeof row[0] === "string");
    t.true(typeof row[1] === "number");
  }

  stmt = db.prepare("SELECT * FROM users WHERE name = ?").raw();
  const row = stmt.get("Alice");
  t.true(Array.isArray(row));
  t.is(row.length, 2);
  t.is(row[0], "Alice");
  t.is(row[1], 42);

  const noRow = stmt.get("Charlie");
  t.is(noRow, undefined);

  stmt = db.prepare("SELECT * FROM users").raw();
  const rows = stmt.all();
  t.true(Array.isArray(rows));
  t.is(rows.length, 2);
  t.deepEqual(rows[0], ["Alice", 42]);
  t.deepEqual(rows[1], ["Bob", 24]);
});


test("Test exec()", async (t) => {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);

  const [db] = await connect(":memory:");
  const file = fs.readFileSync(path.resolve(__dirname, "./artifacts/basic-test.sql"), "utf8");
  db.exec(file);
  let rows = db.prepare("SELECT * FROM users").iterate();
  for (const row of rows) {
    t.truthy(row.name);
    t.true(typeof row.age === "number");
  }
});

test("Test Statement.database gets the database object", async t => {
  const [db] = await connect(":memory:");
  let stmt = db.prepare("SELECT 1");
  t.is(stmt.database, db);
});

const connect = async (path) => {
  const db = new Database(path);
  return [db];
};
