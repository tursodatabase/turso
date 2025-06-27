import test from "ava";
import fs from "node:fs";
import { fileURLToPath } from "url";
import path from "node:path"

import Database from "better-sqlite3";

test("Open in-memory database", async (t) => {
  const [db] = await connect(":memory:");
  t.is(db.memory, true);
});

test("Property .name of in-memory database", async (t) => {
  let name = ":memory:";
  const db = new Database(name);
  t.is(db.name,name);
});

test("Property .name of database", async (t) => {
  let name = "foobar.db";
  const db = new Database(name);
  t.is(db.name,name);
});

test("Property .readonly of database if set", async (t) => {
  const db = new Database("foobar.db", { readonly: true });
  t.is(db.readonly, true);
});

test("Property .readonly of database if not set", async (t) => {
  const db = new Database("foobar.db");
  t.is(db.readonly, false);
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
  db.prepare(
    "CREATE TABLE users (name TEXT, age INTEGER, nationality TEXT)",
  ).run();
  db.prepare("INSERT INTO users (name, age, nationality) VALUES (?, ?, ?)").run(
    ["Alice", 42],
    "UK",
  );
  db.prepare("INSERT INTO users (name, age, nationality) VALUES (?, ?, ?)").run(
    "Bob",
    24,
    "USA",
  );

  let rows = db.prepare("SELECT * FROM users").iterate();
  for (const row of rows) {
    t.truthy(row.name);
    t.truthy(row.nationality);
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
  t.deepEqual(typeof db.pragma("cache_size")[0].cache_size, "number");
  t.deepEqual(typeof db.pragma("cache_size", { simple: true }), "number");
});

test("pragma query", async (t) => {
  const [db] = await connect(":memory:");
  let page_size = db.pragma("page_size");
  let expectedValue = [{page_size: 4096}];
  t.deepEqual(page_size, expectedValue);
});

test("pragma table_list", async (t) => {
  const [db] = await connect(":memory:");
  let param = "sqlite_schema";
  let actual = db.pragma(`table_info(${param})`);
  let expectedValue = [
    {cid: 0, name: "type", type: "TEXT", notnull: 0, dflt_value: null, pk: 0},
    {cid: 1, name: "name", type: "TEXT", notnull: 0, dflt_value: null, pk: 0},
    {cid: 2, name: "tbl_name", type: "TEXT", notnull: 0, dflt_value: null, pk: 0},
    {cid: 3, name: "rootpage", type: "INT", notnull: 0, dflt_value: null, pk: 0},
    {cid: 4, name: "sql", type: "TEXT", notnull: 0, dflt_value: null, pk: 0},
  ];
  t.deepEqual(actual, expectedValue);
});

test("simple pragma table_list", async (t) => {
  const [db] = await connect(":memory:");
  let param = "sqlite_schema";
  let actual = db.pragma(`table_info(${param})`, {simple: true});
  let expectedValue = 0;
  t.deepEqual(actual, expectedValue);
});

test("Statement shouldn't bind twice with bind()", async (t) => {
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

  let stmt = db.prepare("SELECT * FROM users").pluck();

  for (const row of stmt.iterate()) {
    t.truthy(row);
    t.assert(typeof row === "string");
  }
});

test("Test raw(): Rows should be returned as arrays", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);


  let stmt = db.prepare("SELECT * FROM users").raw();

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


test("Presentation modes should be mutually exclusive", async (t) => {
  const [db] = await connect(":memory:");
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);


  // test raw()
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

  // test pluck()
  stmt = db.prepare("SELECT * FROM users").raw().pluck();

  for (const name of stmt.iterate()) {
    t.truthy(name);
    t.assert(typeof name === "string");
  }
});


test("Test exec(): Should correctly load multiple statements from file", async (t) => {
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

test("Test Statement.source", async t => {
  const [db] = await connect(":memory:");
  let sql = "CREATE TABLE t (id int)";
  let stmt = db.prepare(sql);
  t.is(stmt.source, sql);
});

const connect = async (path) => {
  const db = new Database(path);
  return [db];
};
