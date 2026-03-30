import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { connect, Database, DatabaseRowMutation, DatabaseRowTransformResult } from './promise.js'

const localeCompare = (a, b) => a.x.localeCompare(b.x);
const intCompare = (a, b) => a.x - b.x;

function cleanup(path) {
    unlinkSync(path);
    unlinkSync(`${path}-wal`);
    unlinkSync(`${path}-info`);
    unlinkSync(`${path}-changes`);
    try { unlinkSync(`${path}-wal-revert`) } catch (e) { }
}

test('partial sync concurrency', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const dbs = [];
    for (let i = 0; i < 16; i++) {
        dbs.push(await connect({
            path: 'partial-1.db',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
                segmentSize: 128 * 1024,
            },
        }));
    }
    const qs = [];
    for (const db of dbs) {
        qs.push(db.prepare("SELECT COUNT(*) as cnt FROM partial").all());
    }
    const values = await Promise.all(qs);
    expect(values).toEqual(new Array(16).fill([{ cnt: 2000 }]))
})

test('partial sync (prefix bootstrap strategy)', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 4096,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    // select of one record shouldn't increase amount of received data
    expect(await db.prepare("SELECT length(value) as length FROM partial LIMIT 1").all()).toEqual([{ length: 1024 }]);
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    await db.prepare("INSERT INTO partial VALUES (-1)").run();

    expect(await db.prepare("SELECT COUNT(*) as cnt FROM partial").all()).toEqual([{ cnt: 2001 }]);
    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
})

test('partial sync (prefix bootstrap strategy; large segment size)', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 128 * 1024,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    const startLast = performance.now();
    // select of one record shouldn't increase amount of received data
    expect(await db.prepare("SELECT length(value) as length FROM partial LIMIT 1").all()).toEqual([{ length: 1024 }]);
    console.info('select last', 'elapsed', performance.now() - startLast);

    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(2 * 128 * (1024 + 128));

    await db.prepare("INSERT INTO partial VALUES (-1)").run();

    const startAll = performance.now();
    expect(await db.prepare("SELECT COUNT(*) as cnt FROM partial").all()).toEqual([{ cnt: 2001 }]);
    console.info('select all', 'elapsed', performance.now() - startAll);

    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
})

test('partial sync (prefix bootstrap strategy; prefetch)', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 4 * 1024,
            prefetch: true,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    const startLast = performance.now();
    // select of one record shouldn't increase amount of received data
    expect(await db.prepare("SELECT length(value) as length FROM partial LIMIT 1").all()).toEqual([{ length: 1024 }]);
    console.info('select last', 'elapsed', performance.now() - startLast);

    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * 128 * (1024 + 128));

    await db.prepare("INSERT INTO partial VALUES (-1)").run();

    const startAll = performance.now();
    expect(await db.prepare("SELECT COUNT(*) as cnt FROM partial").all()).toEqual([{ cnt: 2001 }]);
    console.info('select all', 'elapsed', performance.now() - startAll);

    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
})

test('partial sync (query bootstrap strategy)', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial_keyed(key INTEGER PRIMARY KEY, value BLOB)");
        await db.exec("DELETE FROM partial_keyed");
        await db.exec("INSERT INTO partial_keyed SELECT value, randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'query', query: 'SELECT * FROM partial_keyed WHERE key = 1000' },
            segmentSize: 4096,
        },
    });

    // we must sync only few pages
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * (4096 + 128));

    // select of one record shouldn't increase amount of received data by a lot
    expect(await db.prepare("SELECT length(value) as length FROM partial_keyed LIMIT 1").all()).toEqual([{ length: 1024 }]);
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * (4096 + 128));

    await db.prepare("INSERT INTO partial_keyed VALUES (-1, -1)").run();
    const n1 = await db.stats();

    // same as bootstrap query - we shouldn't bring any more pages
    expect(await db.prepare("SELECT length(value) as length FROM partial_keyed WHERE key = 1000").all()).toEqual([{ length: 1024 }]);
    const n2 = await db.stats();
    expect(n1.networkReceivedBytes).toEqual(n2.networkReceivedBytes);
})

test('concurrent-actions-consistency', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS rows(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM rows");
        await db.exec("INSERT INTO rows VALUES ('key', 0)");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    console.info('run_info', await db1.prepare("SELECT * FROM sqlite_master").all());
    await db1.exec("PRAGMA busy_timeout=100");
    const pull = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            console.info('pull', i);
            try { await db1.pull(); }
            catch (e) { console.error('pull', e); }
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    const push = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await new Promise(resolve => setTimeout(resolve, (Math.random() + 1)));
            console.info('push', i);
            try {
                if ((await db1.stats()).cdcOperations > 0) {
                    const start = performance.now();
                    await db1.push();
                    console.info('push', performance.now() - start);
                }
            }
            catch (e) { console.error('push', e); }
        }
    }
    const run = async function (iterations: number) {
        let rows = 0;
        for (let i = 0; i < iterations; i++) {
            // console.info('run', i, rows);
            // console.info('run_info', 'update', 'start');
            await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?").run('key');
            // console.info('run_info', 'update', 'end');
            rows += 1;
            // console.info('run_info', 'select', 'start');
            const { cnt } = await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?").get(['key']);
            // console.info('run_info', 'select', 'end', cnt, '(', rows, ')');
            expect(cnt).toBe(rows);
            await new Promise(resolve => setTimeout(resolve, 10 * (Math.random() + 1)));
        }
    }
    await Promise.all([pull(100), push(100), run(200)]);
})

test('simple-db', async () => {
    const db = new Database({ path: ':memory:' });
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }])
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    await expect(async () => await db.pull()).rejects.toThrowError(/sync is disabled as database was opened without sync support/);
})

test('implicit connect', async () => {
    const db = new Database({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    const defer = db.prepare("SELECT * FROM not_found");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: not_found/);
    expect(() => db.prepare("SELECT * FROM not_found")).toThrowError(/no such table: not_found/);
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
})

test('defered sync', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (100)");
        await db.push();
        await db.close();
    }

    let url = null;
    const db = new Database({ path: ':memory:', url: () => url });
    await db.prepare("CREATE TABLE t(x)").run();
    await db.prepare("INSERT INTO t VALUES (1), (2), (3), (42)").run();
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
    await expect(async () => await db.pull()).rejects.toThrow(/url is empty - sync is paused/);
    url = process.env.VITE_TURSO_DB_URL;
    await db.pull();
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
})

test('encryption sync', async () => {
    const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
    const URL = 'http://encrypted--a--a.localhost:10000';
    {
        const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    const db2 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
    await db2.exec("INSERT INTO t VALUES (4), (5), (6)");
    expect(await db1.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
    expect(await db2.prepare("SELECT * FROM t").all()).toEqual([{ x: 4 }, { x: 5 }, { x: 6 }]);
    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);
    const expected = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }];
    expect((await db1.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
    expect((await db2.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
});

test('defered encryption sync', async () => {
    const URL = 'http://encrypted--a--a.localhost:10000';
    const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
    let url = null;
    {
        const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (100)");
        await db.push();
        await db.close();
    }
    const db = await connect({ path: ':memory:', url: () => url, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);

    url = URL;
    await db.pull();

    const expected = [{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }];
    expect((await db.prepare("SELECT * FROM t").all())).toEqual(expected);
});

test('select-after-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
        await db.push();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        const rows = await db.prepare('SELECT * FROM t').all();
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    }
})

test('select-without-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        const rows = await db.prepare('SELECT * FROM t').all();
        expect(rows).toEqual([])
    }
})

test('merge-non-overlapping-keys', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k3', 'value3'), ('k4', 'value4'), ('k5', 'value5')");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value3' }, { x: 'k4', y: 'value4' }, { x: 'k5', y: 'value5' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value5' }, { x: 'k4', y: 'value4' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins-with-delete', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");
    await db1.exec("DELETE FROM q")

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k3', y: 'value5' }];
    expect(rows1).toEqual(expected)
    expect(rows2).toEqual(expected)
})

test('constraint-conflict', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS u(x TEXT PRIMARY KEY, y UNIQUE)");
        await db.exec("DELETE FROM u");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO u VALUES ('k1', 'value1')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO u VALUES ('k2', 'value1')");

    await db1.push();
    await expect(async () => await db2.push()).rejects.toThrow('SQLite error: UNIQUE constraint failed: u.y');
})

test('checkpoint', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    for (let i = 0; i < 1000; i++) {
        await db1.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    expect((await db1.stats()).mainWalSize).toBeGreaterThan(4096 * 1000);
    await db1.checkpoint();
    expect((await db1.stats()).mainWalSize).toBe(0);
    let revertWal = (await db1.stats()).revertWalSize;
    expect(revertWal).toBeLessThan(4096 * 1000 / 50);

    for (let i = 0; i < 1000; i++) {
        await db1.exec(`UPDATE q SET y = 'u${i}' WHERE x = 'k${i}'`);
    }
    await db1.checkpoint();
    expect((await db1.stats()).revertWalSize).toBe(revertWal);
})


test('persistence-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const db1 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
            await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
            await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
            await db1.close();
        }

        {
            const db2 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
            await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
            await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);
            const stmt = db2.prepare('SELECT * FROM q');
            const rows = await stmt.all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
            expect(rows).toEqual(expected)
            stmt.close();
            await db2.close();
        }

        {
            const db3 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
            await db3.push();
            await db3.close();
        }

        {
            const db4 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
            const rows = await db4.prepare('SELECT * FROM q').all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
            expect(rows).toEqual(expected)
            await db4.close();
        }
    }
    finally {
        cleanup(path);
    }
})

test('persistence-offline', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const db = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
            await db.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
            await db.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
            await db.push();
            await db.close();
        }
        {
            const db = await connect({ path: path, url: "https://not-valid-url.localhost" });
            const rows = await db.prepare("SELECT * FROM q").all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }];
            expect(rows.sort(localeCompare)).toEqual(expected.sort(localeCompare))
            await db.close();
        }
    } finally {
        cleanup(path);
    }
})

test('persistence-pull-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect({ path: path1, url: process.env.VITE_TURSO_DB_URL });
        await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
        await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
        const stats1 = await db1.stats();

        const db2 = await connect({ path: path2, url: process.env.VITE_TURSO_DB_URL });
        await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
        await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);
        const stats2 = await db1.stats();
        console.info(stats1, stats2);
        expect(stats1.revision).not.toBe(stats2.revision);

        const rows1 = await db1.prepare('SELECT * FROM q').all();
        const rows2 = await db2.prepare('SELECT * FROM q').all();
        const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    } finally {
        cleanup(path1);
        cleanup(path2);
    }
})

test('update', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db.exec("INSERT INTO q VALUES ('1', '2')")
    await db.push();
    await db.exec("INSERT INTO q VALUES ('1', '2') ON CONFLICT DO UPDATE SET y = '3'")
    await db.push();
})

test('concurrent-updates', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
    });
    await db1.exec("PRAGMA busy_timeout=100");
    async function pull(db) {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull error', e);
        } finally {
            console.error('pull ok');
            setTimeout(async () => await pull(db), 0);
        }
    }
    async function push(db) {
        try {
            await db.push();
        } catch (e) {
            console.error('push error', e);
        } finally {
            console.error('push ok');
            setTimeout(async () => await push(db), 0);
        }
    }

    setTimeout(async () => await pull(db1), 0)
    setTimeout(async () => await push(db1), 0)
    for (let i = 0; i < 1000; i++) {
        try {
            await Promise.all([
                db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`),
                db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`)
            ]);
            console.info('insert ok');
        } catch (e) {
            console.error('insert error', e);
        }
        await new Promise(resolve => setTimeout(resolve, 1));
    }
})

test('corruption-bug-1', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({
        path: ':memory:',
        url: process.env.VITE_TURSO_DB_URL,
    });
    for (let i = 0; i < 100; i++) {
        await db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
    }
    await db1.pull();
    await db1.push();
    for (let i = 0; i < 100; i++) {
        await db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
    }
    await db1.pull();
    await db1.push();
})

test('pull-push-concurrent', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    let pullResolve = null;
    const pullFinish = new Promise(resolve => pullResolve = resolve);
    let pushResolve = null;
    const pushFinish = new Promise(resolve => pushResolve = resolve);
    let stopPull = false;
    let stopPush = false;
    const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    let pull = async () => {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull', e);
        } finally {
            if (!stopPull) {
                setTimeout(pull, 0);
            } else {
                pullResolve()
            }
        }
    }
    let push = async () => {
        try {
            if ((await db.stats()).cdcOperations > 0) {
                await db.push();
            }
        } catch (e) {
            console.error('push', e);
        } finally {
            if (!stopPush) {
                setTimeout(push, 0);
            } else {
                pushResolve();
            }
        }
    }
    setTimeout(pull, 0);
    setTimeout(push, 0);
    for (let i = 0; i < 1000; i++) {
        await db.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
    stopPush = true;
    await pushFinish;
    stopPull = true;
    await pullFinish;
    console.info(await db.stats());
})

test('checkpoint-and-actions', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS rows(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM rows");
        await db.exec("INSERT INTO rows VALUES ('key', 0)");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("PRAGMA busy_timeout=100");
    const pull = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            try {
                await db1.pull();
            }
            catch (e) { console.error('pull', e); }
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    const push = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await new Promise(resolve => setTimeout(resolve, 5));
            try {
                if ((await db1.stats()).cdcOperations > 0) {
                    const start = performance.now();
                    await db1.push();
                    console.info('push', performance.now() - start);
                }
            }
            catch (e) { console.error('push', e); }
        }
    }
    let rows = 0;
    const run = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?").run('key');
            rows += 1;
            const { cnt } = await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?").get(['key']);
            console.info('CHECK', cnt, rows);
            expect(cnt).toBe(rows);
            await new Promise(resolve => setTimeout(resolve, 10 * (1 + Math.random())));
        }
    }
    // await run(100);
    await Promise.all([pull(40), push(40), run(100)]);
})

test('transform', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after.value - m.before.value, m.after.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });
    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });

    await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM counter').all();
    const rows2 = await db2.prepare('SELECT * FROM counter').all();
    expect(rows1).toEqual([{ key: '1', value: 2 }]);
    expect(rows2).toEqual([{ key: '1', value: 2 }]);
})

test('transform-many', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after.value - m.before.value, m.after.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });
    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });

    for (let i = 0; i < 1002; i++) {
        await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }
    for (let i = 0; i < 1001; i++) {
        await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }

    let start = performance.now();
    await Promise.all([db1.push(), db2.push()]);
    console.info('push', performance.now() - start);

    start = performance.now();
    await Promise.all([db1.pull(), db2.pull()]);
    console.info('pull', performance.now() - start);

    const rows1 = await db1.prepare('SELECT * FROM counter').all();
    const rows2 = await db2.prepare('SELECT * FROM counter').all();
    expect(rows1).toEqual([{ key: '1', value: 1001 + 1002 }]);
    expect(rows2).toEqual([{ key: '1', value: 1001 + 1002 }]);
})
