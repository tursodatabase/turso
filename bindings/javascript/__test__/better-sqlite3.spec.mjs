import crypto from 'crypto';
import fs from "node:fs";
import { fileURLToPath } from "url";
import path from "node:path"
import DualTest from "./dual-test.mjs";

const inMemoryTest = new DualTest(":memory:");
const foobarTest = new DualTest("foobar.db");

inMemoryTest.both("Open in-memory database", async (t) => {
  const db = t.context.db;
  t.is(db.memory, true);
});

inMemoryTest.both("Property .name of in-memory database", async (t) => {
  const db = t.context.db;
  t.is(db.name, t.context.path);
});

foobarTest.both("Property .name of database", async (t) => {
  const db = t.context.db;
  t.is(db.name, t.context.path);
});

new DualTest("foobar.db", { readonly: true })
  .both("Property .readonly of database if set", async (t) => {
    const db = t.context.db;
    t.is(db.readonly, true);
  });

const genDatabaseFilename = () => {
  return `test-${crypto.randomBytes(8).toString('hex')}.db`;
};

new DualTest().both("opening a read-only database fails if the file doesn't exist", async (t) => {
  t.throws(() => t.context.connect(genDatabaseFilename(), { readonly: true }),
    {
      any: true,
      code: 'SQLITE_CANTOPEN',
    });
})

foobarTest.both("Property .readonly of database if not set", async (t) => {
  const db = t.context.db;
  t.is(db.readonly, false);
});

foobarTest.both("Property .open of database", async (t) => {
  const db = t.context.db;
  t.is(db.open, true);
});

inMemoryTest.both("Statement.get() returns data", async (t) => {
  const db = t.context.db;
  const stmt = db.prepare("SELECT 1");
  const result = stmt.get();
  t.is(result["1"], 1);
  const result2 = stmt.get();
  t.is(result2["1"], 1);
});

inMemoryTest.both("Statement.get() returns undefined when no data", async (t) => {
  const db = t.context.db;
  const stmt = db.prepare("SELECT 1 WHERE 1 = 2");
  const result = stmt.get();
  t.is(result, undefined);
});

inMemoryTest.both("Statement.run() returns correct result object", async (t) => {
  const db = t.context.db;
  db.prepare("CREATE TABLE users (name TEXT)").run();
  const rows = db.prepare("INSERT INTO users (name) VALUES (?)").run("Alice");
  t.deepEqual(rows, { changes: 1, lastInsertRowid: 1 });
});

inMemoryTest.both("Statment.iterate() should correctly return an iterable object", async (t) => {
  const db = t.context.db;
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

inMemoryTest.both("Empty prepared statement should throw the correct error", async (t) => {
  const db = t.context.db;
  t.throws(
    () => {
      db.prepare("");
    },
    {
      instanceOf: RangeError,
      message: "The supplied SQL string contains no statements",
    },
  );
});

inMemoryTest.both("Test pragma()", async (t) => {
  const db = t.context.db;
  t.deepEqual(typeof db.pragma("cache_size")[0].cache_size, "number");
  t.deepEqual(typeof db.pragma("cache_size", { simple: true }), "number");
});

inMemoryTest.both("pragma query", async (t) => {
  const db = t.context.db;
  let page_size = db.pragma("page_size");
  let expectedValue = [{ page_size: 4096 }];
  t.deepEqual(page_size, expectedValue);
});

inMemoryTest.both("pragma table_list", async (t) => {
  const db = t.context.db;
  let param = "sqlite_schema";
  let actual = db.pragma(`table_info(${param})`);
  let expectedValue = [
    { cid: 0, name: "type", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 1, name: "name", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 2, name: "tbl_name", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 3, name: "rootpage", type: "INT", notnull: 0, dflt_value: null, pk: 0 },
    { cid: 4, name: "sql", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
  ];
  t.deepEqual(actual, expectedValue);
});

inMemoryTest.both("simple pragma table_list", async (t) => {
  const db = t.context.db;
  let param = "sqlite_schema";
  let actual = db.pragma(`table_info(${param})`, { simple: true });
  let expectedValue = 0;
  t.deepEqual(actual, expectedValue);
});

inMemoryTest.both("Statement shouldn't bind twice with bind()", async (t) => {
  const db = t.context.db;
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  let stmt = db.prepare("SELECT * FROM users WHERE name = ?").bind("Alice");

  for (const row of stmt.iterate()) {
    t.truthy(row.name);
    t.true(typeof row.age === "number");
  }

  t.throws(
    () => {
      stmt.bind("Bob");
    },
    {
      instanceOf: TypeError,
      message: 'The bind() method can only be invoked once per statement object',
    },
  );
});

inMemoryTest.both("Test pluck(): Rows should only have the values of the first column", async (t) => {
  const db = t.context.db;
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);

  let stmt = db.prepare("SELECT * FROM users").pluck();

  for (const row of stmt.iterate()) {
    t.truthy(row);
    t.assert(typeof row === "string");
  }
});

inMemoryTest.both("Test raw(): Rows should be returned as arrays", async (t) => {
  const db = t.context.db;
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

inMemoryTest.onlySqlitePasses("Test expand(): Columns should be namespaced", async (t) => {
  const expandedResults = [
    {
      users: {
        name: "Alice",
        type: "premium",
      },
      addresses: {
        userName: "Alice",
        type: "home",
        street: "Alice's street",
      },
    },
    {
      users: {
        name: "Bob",
        type: "basic",
      },
      addresses: {
        userName: "Bob",
        type: "work",
        street: "Bob's street",
      },
    },
  ];

  let regularResults = [
    {
      name: "Alice",
      street: "Alice's street",
      type: "home",
      userName: "Alice",
    },
    {
      name: "Bob",
      street: "Bob's street",
      type: "work",
      userName: "Bob",
    },
  ];

  const db = t.context.db;
  db.prepare("CREATE TABLE users (name TEXT, type TEXT)").run();
  db.prepare("CREATE TABLE addresses (userName TEXT, street TEXT, type TEXT)")
    .run();
  db.prepare("INSERT INTO users (name, type) VALUES (?, ?)")
    .run("Alice", "premium");
  db.prepare("INSERT INTO users (name, type) VALUES (?, ?)")
    .run("Bob", "basic");
  db.prepare("INSERT INTO addresses (userName, street, type) VALUES (?, ?, ?)")
    .run("Alice", "Alice's street", "home");
  db.prepare("INSERT INTO addresses (userName, street, type) VALUES (?, ?, ?)")
    .run("Bob", "Bob's street", "work");

  let allRows = db
    .prepare("SELECT * FROM users u JOIN addresses a ON (u.name = a.userName)")
    .expand(true)
    .all();

  t.deepEqual(allRows, expandedResults);

  allRows = db
    .prepare("SELECT * FROM users u JOIN addresses a ON (u.name = a.userName)")
    .expand()
    .all();

  t.deepEqual(allRows, expandedResults);

  allRows = db
    .prepare("SELECT * FROM users u JOIN addresses a ON (u.name = a.userName)")
    .expand(false)
    .all();

  t.deepEqual(allRows, regularResults);
});

inMemoryTest.both("Presentation modes should be mutually exclusive", async (t) => {
  const db = t.context.db;
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
  let rows = stmt.all();
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

inMemoryTest.onlySqlitePasses("Presentation mode 'expand' should be mutually exclusive", async (t) => {
  // this test can be appended to the previous one when 'expand' is implemented in Turso
  const db = t.context.db;
  db.prepare("CREATE TABLE users (name TEXT, age INTEGER)").run();
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Alice", 42);
  db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").run("Bob", 24);

  let stmt = db.prepare("SELECT * FROM users").pluck().raw();

  // test expand()
  stmt = db.prepare("SELECT * FROM users").raw().pluck().expand();
  const rows = stmt.all();
  t.true(Array.isArray(rows));
  t.is(rows.length, 2);
  t.deepEqual(rows[0], { users: { name: "Alice", age: 42 } });
  t.deepEqual(rows[1], { users: { name: "Bob", age: 24 } });
})

inMemoryTest.both("Test exec(): Should correctly load multiple statements from file", async (t) => {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);

  const db = t.context.db;
  const file = fs.readFileSync(path.resolve(__dirname, "./artifacts/basic-test.sql"), "utf8");
  db.exec(file);
  let rows = db.prepare("SELECT * FROM users").iterate();
  for (const row of rows) {
    t.truthy(row.name);
    t.true(typeof row.age === "number");
  }
});

inMemoryTest.both("Test Statement.database gets the database object", async t => {
  const db = t.context.db;
  let stmt = db.prepare("SELECT 1");
  t.is(stmt.database, db);
});

inMemoryTest.both("Test Statement.source", async t => {
  const db = t.context.db;
  let sql = "CREATE TABLE t (id int)";
  let stmt = db.prepare(sql);
  t.is(stmt.source, sql);
});


