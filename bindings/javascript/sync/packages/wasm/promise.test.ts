import { expect, test, describe } from 'vitest';
import { connect, Database, DatabaseRowMutation, DatabaseRowTransformResult } from './promise-default.js';

const localeCompare = (a: any, b: any) => a.x.localeCompare(b.x);

describe('sync tests with local server (wasm)', () => {
    test('implicit connect', async () => {
        const db = new Database({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        const defer = db.prepare("SELECT * FROM not_found");
        await expect(async () => await defer.all()).rejects.toThrowError(/no such table: not_found/);
        expect(() => db.prepare("SELECT * FROM not_found")).toThrowError(/no such table: not_found/);
        expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
    });

    test('simple db without sync', async () => {
        const db = new Database({ path: ':memory:' });
        expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
        await db.exec("CREATE TABLE t(x)");
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
        expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
        await expect(async () => await db.pull()).rejects.toThrowError(/sync is disabled/);
    });

    test('select after push', async () => {
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
            expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
        }
    });

    test('select without push', async () => {
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
            expect(rows).toEqual([]);
        }
    });

    test('merge non-overlapping keys', async () => {
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
        const expected = [
            { x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' },
            { x: 'k3', y: 'value3' }, { x: 'k4', y: 'value4' }, { x: 'k5', y: 'value5' }
        ];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare));
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare));
    });

    test('last push wins', async () => {
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
        const expected = [
            { x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' },
            { x: 'k3', y: 'value5' }, { x: 'k4', y: 'value4' }
        ];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare));
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare));
    });

    test('last push wins with delete', async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
            await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
            await db.exec("DELETE FROM q");
            await db.push();
            await db.close();
        }
        const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");
        await db1.exec("DELETE FROM q");

        const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

        await db2.push();
        await db1.push();
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM q').all();
        const rows2 = await db1.prepare('SELECT * FROM q').all();
        const expected = [{ x: 'k3', y: 'value5' }];
        expect(rows1).toEqual(expected);
        expect(rows2).toEqual(expected);
    });

    test('constraint conflict', async () => {
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
        await expect(async () => await db2.push()).rejects.toThrow(/UNIQUE constraint failed/);
    });

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
        expect(revertWal).toBeLessThan(4096 * 1000 / 100);

        for (let i = 0; i < 1000; i++) {
            await db1.exec(`UPDATE q SET y = 'u${i}' WHERE x = 'k${i}'`);
        }
        await db1.checkpoint();
        expect((await db1.stats()).revertWalSize).toBe(revertWal);
    });

    test('update', async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
            await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
            await db.exec("DELETE FROM q");
            await db.push();
            await db.close();
        }
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("INSERT INTO q VALUES ('1', '2')");
        await db.push();
        await db.exec("INSERT INTO q VALUES ('1', '2') ON CONFLICT DO UPDATE SET y = '3'");
        await db.push();
    });

    test('transform', async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
            await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
            await db.exec("DELETE FROM counter");
            await db.exec("INSERT INTO counter VALUES ('1', 0)");
            await db.push();
            await db.close();
        }
        const transform = (m: DatabaseRowMutation) => ({
            operation: 'rewrite',
            stmt: {
                sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
                values: [m.after!.value - m.before!.value, m.after!.key]
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
    });

    test('transform many', async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
            await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
            await db.exec("DELETE FROM counter");
            await db.exec("INSERT INTO counter VALUES ('1', 0)");
            await db.push();
            await db.close();
        }
        const transform = (m: DatabaseRowMutation) => ({
            operation: 'rewrite',
            stmt: {
                sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
                values: [m.after!.value - m.before!.value, m.after!.key]
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

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM counter').all();
        const rows2 = await db2.prepare('SELECT * FROM counter').all();
        expect(rows1).toEqual([{ key: '1', value: 1001 + 1002 }]);
        expect(rows2).toEqual([{ key: '1', value: 1001 + 1002 }]);
    });

    test('concurrent actions consistency', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
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
                try { await db1.pull(); }
                catch (e) { /* ignore */ }
                await new Promise(resolve => setTimeout(resolve, 0));
            }
        };

        const push = async function (iterations: number) {
            for (let i = 0; i < iterations; i++) {
                await new Promise(resolve => setTimeout(resolve, (Math.random() + 1)));
                try {
                    if ((await db1.stats()).cdcOperations > 0) {
                        await db1.push();
                    }
                }
                catch (e) { /* ignore */ }
            }
        };

        const run = async function (iterations: number) {
            let rows = 0;
            for (let i = 0; i < iterations; i++) {
                await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?").run('key');
                rows += 1;
                const result = await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?").get(['key']) as { cnt: number };
                expect(result.cnt).toBe(rows);
                await new Promise(resolve => setTimeout(resolve, 10 * (Math.random() + 1)));
            }
        };

        await Promise.all([pull(100), push(100), run(200)]);
    });

    test('pull push concurrent', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
            await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
            await db.exec("DELETE FROM q");
            await db.push();
            await db.close();
        }
        let pullResolve: (() => void) | null = null;
        const pullFinish = new Promise<void>(resolve => pullResolve = resolve);
        let pushResolve: (() => void) | null = null;
        const pushFinish = new Promise<void>(resolve => pushResolve = resolve);
        let stopPull = false;
        let stopPush = false;

        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });

        const pull = async () => {
            try {
                await db.pull();
            } catch (e) {
                // ignore
            } finally {
                if (!stopPull) {
                    setTimeout(pull, 0);
                } else {
                    pullResolve!();
                }
            }
        };

        const push = async () => {
            try {
                if ((await db.stats()).cdcOperations > 0) {
                    await db.push();
                }
            } catch (e) {
                // ignore
            } finally {
                if (!stopPush) {
                    setTimeout(push, 0);
                } else {
                    pushResolve!();
                }
            }
        };

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
    });

    test('concurrent updates', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
            await db.exec("CREATE TABLE IF NOT EXISTS concurrent_upd(x TEXT PRIMARY KEY, y BLOB)");
            await db.exec("DELETE FROM concurrent_upd");
            await db.push();
            await db.close();
        }
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("PRAGMA busy_timeout=100");

        let pullResolve: (() => void) | null = null;
        const pullFinish = new Promise<void>(resolve => pullResolve = resolve);
        let pushResolve: (() => void) | null = null;
        const pushFinish = new Promise<void>(resolve => pushResolve = resolve);
        let stopPull = false;
        let stopPush = false;

        const pull = async () => {
            try { await db.pull(); }
            catch (e) { /* ignore */ }
            finally {
                if (!stopPull) setTimeout(pull, 0);
                else pullResolve!();
            }
        };

        const push = async () => {
            try { await db.push(); }
            catch (e) { /* ignore */ }
            finally {
                if (!stopPush) setTimeout(push, 0);
                else pushResolve!();
            }
        };

        setTimeout(pull, 0);
        setTimeout(push, 0);

        for (let i = 0; i < 100; i++) {
            try {
                await Promise.all([
                    db.exec(`INSERT INTO concurrent_upd VALUES ('1', randomblob(512)) ON CONFLICT DO UPDATE SET y = randomblob(512)`),
                    db.exec(`INSERT INTO concurrent_upd VALUES ('1', randomblob(512)) ON CONFLICT DO UPDATE SET y = randomblob(512)`)
                ]);
            } catch (e) { /* ignore busy errors */ }
            await new Promise(resolve => setTimeout(resolve, 1));
        }

        stopPush = true;
        await pushFinish;
        stopPull = true;
        await pullFinish;
    });

    test('corruption bug regression', { timeout: 30000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
            await db.exec("CREATE TABLE IF NOT EXISTS corruption_test(x TEXT PRIMARY KEY, y BLOB)");
            await db.exec("DELETE FROM corruption_test");
            await db.push();
            await db.close();
        }
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });

        for (let i = 0; i < 50; i++) {
            await db.exec(`INSERT INTO corruption_test VALUES ('1', randomblob(1024)) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
        }
        await db.pull();
        await db.push();

        for (let i = 0; i < 50; i++) {
            await db.exec(`INSERT INTO corruption_test VALUES ('1', randomblob(1024)) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
        }
        await db.pull();
        await db.push();

        const rows = await db.prepare("SELECT LENGTH(y) as len FROM corruption_test").all();
        expect(rows[0].len).toBe(1024);
    });

    test('checkpoint and concurrent actions', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS checkpoint_conc(key TEXT PRIMARY KEY, value INTEGER)");
            await db.exec("DELETE FROM checkpoint_conc");
            await db.exec("INSERT INTO checkpoint_conc VALUES ('key', 0)");
            await db.push();
            await db.close();
        }
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("PRAGMA busy_timeout=100");

        const pull = async (iterations: number) => {
            for (let i = 0; i < iterations; i++) {
                try { await db.pull(); }
                catch (e) { /* ignore */ }
                await new Promise(resolve => setTimeout(resolve, 0));
            }
        };

        const push = async (iterations: number) => {
            for (let i = 0; i < iterations; i++) {
                await new Promise(resolve => setTimeout(resolve, 5));
                try {
                    if ((await db.stats()).cdcOperations > 0) {
                        await db.push();
                    }
                }
                catch (e) { /* ignore */ }
            }
        };

        let rows = 0;
        const run = async (iterations: number) => {
            for (let i = 0; i < iterations; i++) {
                await db.prepare("UPDATE checkpoint_conc SET value = value + 1 WHERE key = ?").run('key');
                rows += 1;
                const result = await db.prepare("SELECT value as cnt FROM checkpoint_conc WHERE key = ?").get(['key']) as { cnt: number };
                expect(result.cnt).toBe(rows);
                await new Promise(resolve => setTimeout(resolve, 10 * (1 + Math.random())));
            }
        };

        await Promise.all([pull(20), push(20), run(50)]);
    });

    // ==================== Partial Sync Tests (in-memory only) ====================

    test('partial sync with prefix strategy - network validation', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_prefix(id INTEGER PRIMARY KEY, data BLOB)");
            await db.exec("DELETE FROM partial_prefix");
            await db.exec("INSERT INTO partial_prefix SELECT value, randomblob(1024) FROM generate_series(1, 200)");
            await db.push();
            await db.close();
        }

        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(32 * 1024);

        const row = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_prefix WHERE id = 1").get();
        expect(row).toEqual({ id: 1, len: 1024 });

        const statsAfterSingleQuery = await db.stats();
        expect(statsAfterSingleQuery.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        const total = await db.prepare("SELECT SUM(LENGTH(data)) as total FROM partial_prefix").all();
        expect(total).toEqual([{ total: 200 * 1024 }]);

        const statsAfterFullScan = await db.stats();
        expect(statsAfterFullScan.networkReceivedBytes).toBeGreaterThanOrEqual(150 * 1024);
    });

    test('partial sync with query strategy', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_query(key INTEGER PRIMARY KEY, category TEXT, data BLOB)");
            await db.exec("DELETE FROM partial_query");
            for (let i = 0; i < 100; i++) {
                const category = i < 50 ? 'hot' : 'cold';
                await db.exec(`INSERT INTO partial_query VALUES (${i}, '${category}', randomblob(1024))`);
            }
            await db.push();
            await db.close();
        }

        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'query', query: "SELECT * FROM partial_query WHERE category = 'hot'" },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const hotCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query WHERE category = 'hot'").all();
        expect(hotCount).toEqual([{ cnt: 50 }]);

        const coldCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query WHERE category = 'cold'").all();
        expect(coldCount).toEqual([{ cnt: 50 }]);

        const totalCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query").all();
        expect(totalCount).toEqual([{ cnt: 100 }]);
    });

    test('partial sync with prefetch enabled', { timeout: 30000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_prefetch(id INTEGER PRIMARY KEY, data BLOB)");
            await db.exec("DELETE FROM partial_prefetch");
            await db.exec("INSERT INTO partial_prefetch SELECT value, randomblob(512) FROM generate_series(1, 50)");
            await db.push();
            await db.close();
        }

        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: true,
            },
        });

        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        const rows = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_prefetch ORDER BY id").all();
        expect(rows.length).toBe(50);
        expect(rows[0]).toEqual({ id: 1, len: 512 });
        expect(rows[49]).toEqual({ id: 50, len: 512 });
    });

    test('partial sync two clients', async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_two(id TEXT PRIMARY KEY, client TEXT)");
            await db.exec("DELETE FROM partial_two");
            await db.push();
            await db.close();
        }

        const db1 = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const db2 = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        await db1.exec("INSERT INTO partial_two VALUES ('a', 'client1')");
        await db1.exec("INSERT INTO partial_two VALUES ('b', 'client1')");

        await db2.exec("INSERT INTO partial_two VALUES ('c', 'client2')");
        await db2.exec("INSERT INTO partial_two VALUES ('d', 'client2')");

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare("SELECT * FROM partial_two ORDER BY id").all();
        const rows2 = await db2.prepare("SELECT * FROM partial_two ORDER BY id").all();
        const expected = [
            { id: 'a', client: 'client1' },
            { id: 'b', client: 'client1' },
            { id: 'c', client: 'client2' },
            { id: 'd', client: 'client2' }
        ];
        expect(rows1).toEqual(expected);
        expect(rows2).toEqual(expected);
    });

    test('partial sync vs full sync network comparison', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_compare(id INTEGER PRIMARY KEY, data BLOB)");
            await db.exec("DELETE FROM partial_compare");
            await db.exec("INSERT INTO partial_compare SELECT value, randomblob(1024) FROM generate_series(1, 150)");
            await db.push();
            await db.close();
        }

        const dbFull = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
        });

        const fullSyncStats = await dbFull.stats();
        expect(fullSyncStats.networkReceivedBytes).toBeGreaterThanOrEqual(100 * 1024);

        await dbFull.close();

        const dbPartial = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const partialSyncStats = await dbPartial.stats();
        expect(partialSyncStats.networkReceivedBytes).toBeLessThanOrEqual(32 * 1024);
        expect(partialSyncStats.networkReceivedBytes).toBeLessThan(fullSyncStats.networkReceivedBytes / 3);
    });

    test('partial sync with large segment size', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_large_seg(id INTEGER PRIMARY KEY, data BLOB)");
            await db.exec("DELETE FROM partial_large_seg");
            await db.exec("INSERT INTO partial_large_seg SELECT value, randomblob(1024) FROM generate_series(1, 100)");
            await db.push();
            await db.close();
        }

        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 32 * 1024 },
                segmentSize: 32 * 1024,
                prefetch: false,
            },
        });

        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        const row = await db.prepare("SELECT LENGTH(data) as length FROM partial_large_seg LIMIT 1").all();
        expect(row).toEqual([{ length: 1024 }]);

        const statsAfterSelect = await db.stats();
        expect(statsAfterSelect.networkReceivedBytes).toBeLessThanOrEqual(128 * 1024);

        const count = await db.prepare("SELECT COUNT(*) as cnt FROM partial_large_seg").all();
        expect(count).toEqual([{ cnt: 100 }]);

        const statsAfterFullScan = await db.stats();
        expect(statsAfterFullScan.networkReceivedBytes).toBeGreaterThanOrEqual(75 * 1024);
    });

    test('partial sync concurrency', { timeout: 60000 }, async () => {
        {
            const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 100 });
            await db.exec("CREATE TABLE IF NOT EXISTS partial_conc(id INTEGER PRIMARY KEY, data BLOB)");
            await db.exec("DELETE FROM partial_conc");
            await db.exec("INSERT INTO partial_conc SELECT value, randomblob(1024) FROM generate_series(1, 100)");
            await db.push();
            await db.close();
        }

        const dbs: Database[] = [];
        for (let i = 0; i < 4; i++) {
            dbs.push(await connect({
                path: ':memory:',
                url: process.env.VITE_TURSO_DB_URL,
                partialSyncExperimental: {
                    bootstrapStrategy: { kind: 'prefix', length: 32 * 1024 },
                    segmentSize: 32 * 1024,
                    prefetch: false,
                },
            }));
        }

        const queries = dbs.map(db => db.prepare("SELECT COUNT(*) as cnt FROM partial_conc").all());
        const results = await Promise.all(queries);

        expect(results).toEqual(new Array(4).fill([{ cnt: 100 }]));

        await Promise.all(dbs.map(db => db.close()));
    });
});
