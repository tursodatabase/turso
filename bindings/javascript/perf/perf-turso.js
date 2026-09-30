import { run, bench, group, baseline } from 'mitata';

import { Database } from '@tursodatabase/database';

const db = new Database(':memory:');

await db.exec("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)");
await db.exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')");

const stmtSelect = db.prepare("SELECT * FROM users WHERE id = ?");
const rawStmtSelect = db.prepare("SELECT * FROM users WHERE id = ?").raw();
const stmtInsert = db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");

bench('Statement.get() with bind parameters [expanded]', async () => {
  await stmtSelect.get(1);
});

bench('Statement.get() with bind parameters [raw]', async () => {
  await rawStmtSelect.get(1);
});

bench('Statement.run() with bind parameters', async () => {
  await stmtInsert.run([1, 'foobar', 'foobar@example.com']);
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
