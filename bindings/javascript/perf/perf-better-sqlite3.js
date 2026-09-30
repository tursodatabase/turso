import { run, bench, group, baseline } from 'mitata';

import Database from 'better-sqlite3';

const db = new Database(':memory:');

db.exec("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)");
db.exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')");

const stmtSelect = db.prepare("SELECT * FROM users WHERE id = ?");
const rawStmtSelect = db.prepare("SELECT * FROM users WHERE id = ?").raw();
const stmtInsert = db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");

bench('Statement.get() with bind parameters [expanded]', () => {
  stmtSelect.get(1);
});

bench('Statement.git() with bind parameters [raw]', () => {
  rawStmtSelect.get(1);
});

bench('Statement.run() with bind parameters', () => {
  stmtInsert.run([1, 'foobar', 'foobar@example.com']);
});

await run({
  units: false,
  silent: false,
  avg: true,
  json: false,
  colors: true,
  min_max: true,
  percentiles: true,
});
