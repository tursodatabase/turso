import { expect, test } from 'vitest'
import { connect } from './promise.js'

const TURSO_URL = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;

function localSyncedDbOpts() {
    if (!TURSO_URL) throw new Error('TURSO_DATABASE_URL env var is required');
    return {
        path: ':memory:',
        url: TURSO_URL,
        authToken: TURSO_TOKEN,
    };
}

function remoteWriteOpts() {
    return {
        ...localSyncedDbOpts(),
        remoteWritesExperimental: true,
    };
}

test('remote write: exec insert and read back', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_exec(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_exec");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("INSERT INTO rw_exec VALUES (1, 'hello')");

    const rows = await db.prepare("SELECT * FROM rw_exec").all();
    expect(rows).toEqual([{ id: 1, value: 'hello' }]);
    await db.close();
})

test('remote write: prepared statement write', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_prepared(id INTEGER PRIMARY KEY, x INTEGER)");
    await seed.exec("DELETE FROM rw_prepared");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    for (let i = 1; i <= 5; i++) {
        await db.prepare("INSERT INTO rw_prepared(x) VALUES (?)").run([i * 10]);
    }

    const rows = await db.prepare("SELECT x FROM rw_prepared ORDER BY x").all();
    expect(rows).toEqual([{ x: 10 }, { x: 20 }, { x: 30 }, { x: 40 }, { x: 50 }]);
    await db.close();
})

test('remote write: reads stay local', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_read(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_read");
    await seed.exec("INSERT INTO rw_read VALUES (1, 'local')");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    const rows = await db.prepare("SELECT * FROM rw_read").all();
    expect(rows).toEqual([{ id: 1, value: 'local' }]);
    await db.close();
})

test('remote write: transaction commit', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_txn(id INTEGER PRIMARY KEY, value INTEGER)");
    await seed.exec("DELETE FROM rw_txn");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    const txn = db.transaction(async () => {
        await db.prepare("INSERT INTO rw_txn(value) VALUES (?)").run([1]);
        await db.prepare("INSERT INTO rw_txn(value) VALUES (?)").run([2]);
        await db.prepare("INSERT INTO rw_txn(value) VALUES (?)").run([3]);
    });
    await txn();

    const rows = await db.prepare("SELECT value FROM rw_txn ORDER BY value").all();
    expect(rows).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
    await db.close();
})

test('remote write: transaction rollback on error', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_rollback(id INTEGER PRIMARY KEY, value INTEGER)");
    await seed.exec("DELETE FROM rw_rollback");
    await seed.exec("INSERT INTO rw_rollback VALUES (1, 100)");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    const txn = db.transaction(async () => {
        await db.prepare("INSERT INTO rw_rollback(value) VALUES (?)").run([200]);
        throw new Error("deliberate rollback");
    });
    await expect(txn()).rejects.toThrow("deliberate rollback");

    const rows = await db.prepare("SELECT value FROM rw_rollback ORDER BY value").all();
    expect(rows).toEqual([{ value: 100 }]);
    await db.close();
})

test('remote write: exec with BEGIN/COMMIT', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_exec_txn(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_exec_txn");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("BEGIN");
    await db.exec("INSERT INTO rw_exec_txn VALUES (1, 'a')");
    await db.exec("INSERT INTO rw_exec_txn VALUES (2, 'b')");
    await db.exec("COMMIT");

    const rows = await db.prepare("SELECT value FROM rw_exec_txn ORDER BY id").all();
    expect(rows).toEqual([{ value: 'a' }, { value: 'b' }]);
    await db.close();
})

test('remote write: second client sees writes after pull', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_visible(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_visible");
    await seed.push();
    await seed.close();

    const db1 = await connect(remoteWriteOpts());
    await db1.exec("INSERT INTO rw_visible VALUES (1, 'from-remote-write')");

    // second client pulls and should see the remote write
    const db2 = await connect(localSyncedDbOpts());

    const rows = await db2.prepare("SELECT * FROM rw_visible").all();
    expect(rows).toEqual([{ id: 1, value: 'from-remote-write' }]);

    await db1.close();
    await db2.close();
})

test('remote write: update and delete', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_update(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_update");
    await seed.exec("INSERT INTO rw_update VALUES (1, 'original')");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("UPDATE rw_update SET value = 'updated' WHERE id = 1");

    let rows = await db.prepare("SELECT value FROM rw_update WHERE id = 1").all();
    expect(rows).toEqual([{ value: 'updated' }]);

    await db.exec("DELETE FROM rw_update WHERE id = 1");
    rows = await db.prepare("SELECT COUNT(*) as cnt FROM rw_update").all();
    expect(rows).toEqual([{ cnt: 0 }]);
    await db.close();
})

test('remote write: unique conflict rejected at write time', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_conflict(id INTEGER PRIMARY KEY, key TEXT, value TEXT UNIQUE)");
    await seed.exec("DELETE FROM rw_conflict");
    await seed.push();
    await seed.close();

    const db1 = await connect(remoteWriteOpts());
    const db2 = await connect(remoteWriteOpts());

    // first client inserts successfully
    await db1.exec("INSERT INTO rw_conflict(key, value) VALUES ('a', 'taken')");

    // second client tries the same unique value — must fail at write time, not at push
    await expect(
        db2.exec("INSERT INTO rw_conflict(key, value) VALUES ('b', 'taken')")
    ).rejects.toThrow(/UNIQUE constraint failed/);

    // both clients see exactly the same single row
    const rows1 = await db1.prepare("SELECT key, value FROM rw_conflict").all();
    const rows2 = await db2.prepare("SELECT key, value FROM rw_conflict").all();
    expect(rows1).toEqual([{ key: 'a', value: 'taken' }]);
    expect(rows2).toEqual([]);
    await db2.pull();
    const rows3 = await db2.prepare("SELECT key, value FROM rw_conflict").all();
    expect(rows3).toEqual([{ key: 'a', value: 'taken' }]);

    await db1.close();
    await db2.close();
})

test('remote write: DDL create table and use it', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("DROP TABLE IF EXISTS rw_ddl");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("CREATE TABLE rw_ddl(id INTEGER PRIMARY KEY, name TEXT)");
    await db.exec("INSERT INTO rw_ddl(name) VALUES ('after-create')");

    const rows = await db.prepare("SELECT name FROM rw_ddl").all();
    expect(rows).toEqual([{ name: 'after-create' }]);

    // second client should see the new table
    const db2 = await connect(localSyncedDbOpts());
    const rows2 = await db2.prepare("SELECT name FROM rw_ddl").all();
    expect(rows2).toEqual([{ name: 'after-create' }]);

    await db.close();
    await db2.close();
})

test('remote write: DDL alter table adds column', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("DROP TABLE IF EXISTS rw_alter");
    await seed.exec("CREATE TABLE rw_alter(id INTEGER PRIMARY KEY, a TEXT)");
    await seed.exec("INSERT INTO rw_alter(a) VALUES ('before')");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("ALTER TABLE rw_alter ADD COLUMN b TEXT DEFAULT 'default_b'");
    await db.exec("INSERT INTO rw_alter(a, b) VALUES ('after', 'new_b')");

    const rows = await db.prepare("SELECT a, b FROM rw_alter ORDER BY a").all();
    expect(rows).toEqual([
        { a: 'after', b: 'new_b' },
        { a: 'before', b: 'default_b' },
    ]);
    await db.close();
})

test('remote write: DDL drop table', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_drop(id INTEGER PRIMARY KEY)");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("DROP TABLE rw_drop");

    await expect(() => db.prepare("SELECT * FROM rw_drop")).toThrow(/no such table/);
    await db.close();
})

test('remote write: DDL create index', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_idx(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_idx");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    await db.exec("CREATE INDEX IF NOT EXISTS idx_rw_value ON rw_idx(value)");
    await db.exec("INSERT INTO rw_idx(value) VALUES ('indexed')");

    const rows = await db.prepare("SELECT value FROM rw_idx").all();
    expect(rows).toEqual([{ value: 'indexed' }]);
    await db.close();
})

test('remote write: read-only transaction goes remote, not local', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("CREATE TABLE IF NOT EXISTS rw_read_txn(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.exec("DELETE FROM rw_read_txn");
    await seed.exec("INSERT INTO rw_read_txn VALUES (1, 'original')");
    await seed.push();
    await seed.close();

    // db1 connects with remote writes — local replica has 'original'
    const db1 = await connect(remoteWriteOpts());
    const localRows = await db1.prepare("SELECT value FROM rw_read_txn").all();
    expect(localRows).toEqual([{ value: 'original' }]);

    // another client updates the remote behind db1's back
    const db2 = await connect(localSyncedDbOpts());
    await db2.exec("UPDATE rw_read_txn SET value = 'updated' WHERE id = 1");
    await db2.push();
    await db2.close();

    // db1's local replica is stale — outside a txn, reads are local
    const staleRows = await db1.prepare("SELECT value FROM rw_read_txn").all();
    expect(staleRows).toEqual([{ value: 'original' }]);

    // inside a transaction, reads should go remote and see 'updated'
    await db1.exec("BEGIN");
    const txnRows = await db1.prepare("SELECT value FROM rw_read_txn").all();
    expect(txnRows[0]).toMatchObject({ value: 'updated' });
    await db1.exec("COMMIT");

    await db1.close();
})

test('remote write: insert returning', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("DROP TABLE IF EXISTS rw_returning");
    await seed.exec("CREATE TABLE rw_returning(id INTEGER PRIMARY KEY, value TEXT)");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    const rows = await db.prepare("INSERT INTO rw_returning(value) VALUES (?) RETURNING id, value").all(['hello']);
    expect(rows).toMatchObject([{ id: 1, value: 'hello' }]);

    expect(await db.prepare("SELECT * FROM rw_returning").all()).toMatchObject([{ id: 1, value: 'hello' }]);

    const rows2 = await db.prepare("INSERT INTO rw_returning(value) VALUES (?) RETURNING id, value").all(['world']);
    expect(rows2).toMatchObject([{ id: 2, value: 'world' }]);

    expect(await db.prepare("SELECT * FROM rw_returning").all()).toMatchObject([
        { id: 1, value: 'hello' },
        { id: 2, value: 'world' },
    ]);

    await db.close();
})

test('remote write: blob round-trip', async () => {
    const seed = await connect(localSyncedDbOpts());
    await seed.exec("DROP TABLE IF EXISTS rw_blob");
    await seed.exec("CREATE TABLE rw_blob(id INTEGER PRIMARY KEY, data BLOB)");
    await seed.push();
    await seed.close();

    const db = await connect(remoteWriteOpts());
    const blob = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
    await db.prepare("INSERT INTO rw_blob(data) VALUES (?)").run([blob]);

    const rows = await db.prepare("SELECT data FROM rw_blob").all();
    expect(Buffer.from(rows[0].data)).toEqual(blob);
    await db.close();
})
