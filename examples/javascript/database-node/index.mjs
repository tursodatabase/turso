import { connect } from "@tursodatabase/database";

const db = await connect("local.db", {
    timeout: 1000, // busy timeout for handling high-concurrency write cases
});

// execute multiple SQL statements with exec(...)
await db.exec(`
    CREATE TABLE IF NOT EXISTS guestbook (comment TEXT, created_at DEFAULT (unixepoch()));
    CREATE INDEX IF NOT EXISTS guestbook_idx ON guestbook (created_at);
`);

// use prepared statements and bind args to placeholders later
const insert = db.prepare(`INSERT INTO guestbook(comment) VALUES (?)`);

// use run(...) method if query only need to be executed till completion
await insert.run([`hello, turso at ${Math.floor(Date.now() / 1000)}`]);

const select = db.prepare(`SELECT * FROM guestbook ORDER BY created_at DESC LIMIT ?`);
// use all(...) or get(...) methods to get all or one row from the query
console.info(await select.all([5]))
