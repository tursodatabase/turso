import test from "ava";
import fs from "node:fs";
import { fileURLToPath } from "url";
import path from "node:path"

import Database from "better-sqlite3";

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

test("Statement.get() returns undefined when no data", async (t) => {
  const [db] = await connect(":memory:");
  const stmt = db.prepare("SELECT 1 WHERE 1 = 2");
  const result = stmt.get();
  t.is(result, undefined);
});

test("Statement.run() returns correct result object", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT)").run();
  const rows = db.prepare("INSERT INTO users (name) VALUES (?)").run("Alice");
  t.deepEqual(rows, { changes: 1, lastInsertRowid: 1 });
});

test("Statment.iterate() should correctly return an iterable object", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);
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

test("Test pragma", async (t) => {
  const [db] = await connect(":memory:");
  t.deepEqual(typeof db.pragma("cache_size")[0].cache_size, "number");
  t.deepEqual(typeof db.pragma("cache_size", { simple: true }), "number");
});

test("Test bind()", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  let stmt = db.prepare("SELECT * FROM users WHERE name = ?").bind("Alice");
  console.log(db.prepare("SELECT * FROM users").raw().get());

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
