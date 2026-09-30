import { connect } from "@tursodatabase/sync";

const db = await connect({
    path: "local.db", // local path to store database
    authToken: process.env.TURSO_AUTH_TOKEN, // Turso Cloud auth token
    url: process.env.TURSO_DATABASE_URL, // Turso Cloud database url
    longPollTimeoutMs: 0, // optional long-polling interval for pull operation; useful in case when pull called in a loop
});

// execute multiple SQL statements with exec(...)
await db.exec(`
    CREATE TABLE IF NOT EXISTS guestbook (comment TEXT, created_at DEFAULT (unixepoch()));
    CREATE INDEX IF NOT EXISTS guestbook_idx ON guestbook (created_at);
`);

// use prepared statements and bind args to placeholders later
const select1 = db.prepare(`SELECT * FROM guestbook ORDER BY created_at DESC LIMIT ?`);
// use all(...) or get(...) methods to get all or one row from the query
console.info(await select1.all([5]))

try {
    // pull new changes from the remote
    await db.pull();
} catch (e) {
    console.error('pull failed', e);
}

const insert = db.prepare(`INSERT INTO guestbook(comment) VALUES (?)`);
// use run(...) method if query only need to be executed till completion
await insert.run([`hello, turso at ${Math.floor(Date.now() / 1000)}`]);

const select2 = db.prepare(`SELECT * FROM guestbook ORDER BY created_at DESC LIMIT ?`);
console.info(await select2.all([5]))

try {
    // push local changes to the remote
    await db.push();
} catch (e) {
    console.error('pull failed', e);
}
