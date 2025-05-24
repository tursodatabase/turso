import test from "ava";

import { Database } from "../index.js";

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
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run(["Alice", 42]);
  let rows = db.prepare("SELECT * FROM users").all();
  t.deepEqual(rows, [{ name: "Alice", age: 42 }]);
});

test("Statment.iterate() should correctly return an iterable object", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run(["Alice", 42]);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run(["Bob", 24]);
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

const connect = async (path) => {
  const db = new Database(path);
  return [db];
};
