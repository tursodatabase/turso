import { unlinkSync, existsSync } from "node:fs";
import { spawn, ChildProcess } from "node:child_process";
import { expect, test, beforeAll, afterAll, describe } from 'vitest';
import { connect, Database, DatabaseRowMutation, DatabaseRowTransformResult } from './promise.js';

const localeCompare = (a: any, b: any) => a.x.localeCompare(b.x);
const intCompare = (a: any, b: any) => a.x - b.x;

function cleanup(dbPath: string) {
    const files = [dbPath, `${dbPath}-wal`, `${dbPath}-info`, `${dbPath}-changes`, `${dbPath}-wal-revert`];
    for (const file of files) {
        try { unlinkSync(file); } catch (e) { }
    }
}

function randomPort(): number {
    return 10000 + Math.floor(Math.random() * (65536 - 10000));
}

class TursoServer {
    private port: number;
    private server: ChildProcess | null = null;
    public dbUrl: string;
    private binaryPath: string;

    constructor() {
        this.port = randomPort();
        this.dbUrl = `http://localhost:${this.port}`;
        if (!process.env.LOCAL_SYNC_SERVER) {
            throw new Error('LOCAL_SYNC_SERVER environment variable must be set to the path of the tursodb binary');
        }
        this.binaryPath = process.env.LOCAL_SYNC_SERVER;
    }

    async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.server = spawn(this.binaryPath, ['--sync-server', `0.0.0.0:${this.port}`], {
                stdio: ['ignore', 'pipe', 'pipe'],
            });

            this.server.on('error', (err) => {
                reject(new Error(`Failed to start sync server: ${err.message}`));
            });

            this.server.stderr?.on('data', (data) => {
                const msg = data.toString();
                if (msg.includes('error') || msg.includes('Error')) {
                    console.error('Server stderr:', msg);
                }
            });

            // Wait for server to be ready by polling
            const startTime = Date.now();
            const timeout = 10000; // 10 seconds timeout

            const checkReady = async () => {
                try {
                    const response = await fetch(this.dbUrl);
                    // Server is up (even if it returns an error response)
                    resolve();
                } catch (e) {
                    if (Date.now() - startTime > timeout) {
                        reject(new Error('Timeout waiting for sync server to start'));
                    } else {
                        setTimeout(checkReady, 10);
                    }
                }
            };

            setTimeout(checkReady, 50);
        });
    }

    async stop(): Promise<void> {
        if (this.server) {
            this.server.kill('SIGTERM');
            await new Promise<void>((resolve) => {
                this.server?.on('close', () => resolve());
                setTimeout(resolve, 1000); // Force resolve after 1s
            });
            this.server = null;
        }
    }

    async dbSql(sql: string): Promise<any[][]> {
        const payload = {
            requests: [
                { type: 'execute', stmt: { sql } }
            ]
        };

        const response = await fetch(`${this.dbUrl}/v2/pipeline`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }

        const result = await response.json() as any;
        if (result.results[0].type !== 'ok') {
            throw new Error(`SQL error: ${JSON.stringify(result)}`);
        }

        const rows = result.results[0].response.result.rows;
        return rows.map((row: any[]) => row.map((cell: any) => cell.value));
    }
}

let server: TursoServer;

beforeAll(async () => {
    server = new TursoServer();
    await server.start();
}, 15000);

afterAll(async () => {
    await server.stop();
});

describe('sync tests with local server', () => {
    test('implicit connect', async () => {
        await server.dbSql("DROP TABLE IF EXISTS implicit_test");
        await server.dbSql("CREATE TABLE implicit_test(x)");
        await server.dbSql("INSERT INTO implicit_test VALUES (1)");

        // Create Database without calling connect() - it should connect implicitly
        const db = new Database({ path: ':memory:', url: server.dbUrl });

        // Deferred statement - should work after implicit connect
        const defer = db.prepare("SELECT * FROM implicit_test");
        expect(await defer.all()).toEqual([{ x: 1 }]);

        // Sync prepare should also work
        expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);

        await db.close();
    });

    test('sync bootstrap', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-js')");

        const db = await connect({
            path: ':memory:',
            clientName: 'turso-sync-js',
            url: server.dbUrl,
        });

        const rows = await db.prepare("SELECT * FROM t").all();
        expect(rows).toEqual([{ x: 'hello' }, { x: 'turso' }, { x: 'sync-js' }]);

        await db.close();
    });

    test('sync pull', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-js')");

        const db = await connect({
            path: ':memory:',
            clientName: 'turso-sync-js',
            url: server.dbUrl,
        });

        // Add data on server after initial sync
        await server.dbSql("INSERT INTO t VALUES ('pull-works')");

        // Before pull - should only see original data
        let rows = await db.prepare("SELECT * FROM t").all();
        expect(rows).toEqual([{ x: 'hello' }, { x: 'turso' }, { x: 'sync-js' }]);

        // Pull changes
        const changes = await db.pull();
        expect(changes).toBe(true);

        // Second pull should have no changes
        const noChanges = await db.pull();
        expect(noChanges).toBe(false);

        // After pull - should see new data
        rows = await db.prepare("SELECT * FROM t").all();
        expect(rows).toEqual([{ x: 'hello' }, { x: 'turso' }, { x: 'sync-js' }, { x: 'pull-works' }]);

        await db.close();
    });

    test('sync pull does not push', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-js')");

        const db = await connect({
            path: ':memory:',
            clientName: 'turso-sync-js',
            url: server.dbUrl,
        });

        // Add data on server
        await server.dbSql("INSERT INTO t VALUES ('pull-works')");

        // Before pull
        let rows = await db.prepare("SELECT * FROM t").all();
        expect(rows).toEqual([{ x: 'hello' }, { x: 'turso' }, { x: 'sync-js' }]);

        // Insert locally
        await db.exec("INSERT INTO t VALUES ('push-is-local')");

        // Pull changes
        const changes = await db.pull();
        expect(changes).toBe(true);

        // After pull - should see both local and remote data
        rows = await db.prepare("SELECT * FROM t").all();
        expect(rows).toEqual([
            { x: 'hello' }, { x: 'turso' }, { x: 'sync-js' },
            { x: 'pull-works' }, { x: 'push-is-local' }
        ]);

        // Remote should NOT have local data (no push)
        const remote = await server.dbSql("SELECT * FROM t");
        expect(remote).toEqual([['hello'], ['turso'], ['sync-js'], ['pull-works']]);

        await db.close();
    });

    test('sync push', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-js')");

        const db = await connect({
            path: ':memory:',
            clientName: 'turso-sync-js',
            url: server.dbUrl,
        });

        // Insert locally
        await db.exec("INSERT INTO t VALUES ('push-works')");

        // Before push - remote shouldn't have local data
        let remote = await server.dbSql("SELECT * FROM t");
        expect(remote).toEqual([['hello'], ['turso'], ['sync-js']]);

        // Push changes
        await db.push();

        // After push - remote should have local data
        remote = await server.dbSql("SELECT * FROM t");
        expect(remote).toEqual([['hello'], ['turso'], ['sync-js'], ['push-works']]);

        await db.close();
    });

    test('sync checkpoint', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");

        const db = await connect({
            path: ':memory:',
            clientName: 'turso-sync-js',
            url: server.dbUrl,
        });

        // Insert many rows to grow WAL
        for (let i = 0; i < 1024; i++) {
            await db.exec(`INSERT INTO t VALUES (${i})`);
        }

        const stats1 = await db.stats();
        expect(stats1.mainWalSize).toBeGreaterThan(1024 * 1024);
        expect(stats1.revertWalSize).toBe(0);

        await db.checkpoint();

        const stats2 = await db.stats();
        expect(stats2.mainWalSize).toBe(0);
        // Use 16KB threshold to account for varying overhead
        expect(stats2.revertWalSize).toBeLessThan(16 * 1024);

        // Push and verify
        await db.push();
        const remote = await server.dbSql("SELECT SUM(x) FROM t");
        expect(remote).toEqual([[`${1024 * 1023 / 2}`]]);

        await db.close();
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
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");

        {
            const db = await connect({ path: ':memory:', url: server.dbUrl });
            await db.exec("INSERT INTO t VALUES (1), (2), (3)");
            await db.push();
            await db.close();
        }

        {
            const db = await connect({ path: ':memory:', url: server.dbUrl });
            const rows = await db.prepare('SELECT * FROM t').all();
            expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
            await db.close();
        }
    });

    test('select without push', async () => {
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");

        {
            const db = await connect({ path: ':memory:', url: server.dbUrl });
            await db.exec("INSERT INTO t VALUES (1), (2), (3)");
            // No push!
            await db.close();
        }

        {
            const db = await connect({ path: ':memory:', url: server.dbUrl });
            const rows = await db.prepare('SELECT * FROM t').all();
            expect(rows).toEqual([]); // Empty because we didn't push
            await db.close();
        }
    });

    test('merge non-overlapping keys', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
        await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2')");

        const db2 = await connect({ path: ':memory:', url: server.dbUrl });
        await db2.exec("INSERT INTO q VALUES ('k3', 'value3'), ('k4', 'value4'), ('k5', 'value5')");

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM q').all();
        const rows2 = await db2.prepare('SELECT * FROM q').all();
        const expected = [
            { x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' },
            { x: 'k3', y: 'value3' }, { x: 'k4', y: 'value4' }, { x: 'k5', y: 'value5' }
        ];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare));
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare));

        await db1.close();
        await db2.close();
    });

    test('last push wins', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
        await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");

        const db2 = await connect({ path: ':memory:', url: server.dbUrl });
        await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

        await db2.push();
        await db1.push();
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM q').all();
        const rows2 = await db2.prepare('SELECT * FROM q').all();
        const expected = [
            { x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' },
            { x: 'k3', y: 'value5' }, { x: 'k4', y: 'value4' }
        ];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare));
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare));

        await db1.close();
        await db2.close();
    });

    test('last push wins with delete', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
        await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");
        await db1.exec("DELETE FROM q");

        const db2 = await connect({ path: ':memory:', url: server.dbUrl });
        await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

        await db2.push();
        await db1.push();
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM q').all();
        const rows2 = await db2.prepare('SELECT * FROM q').all();
        const expected = [{ x: 'k3', y: 'value5' }];
        expect(rows1).toEqual(expected);
        expect(rows2).toEqual(expected);

        await db1.close();
        await db2.close();
    });

    test('constraint conflict', async () => {
        await server.dbSql("DROP TABLE IF EXISTS u");
        await server.dbSql("CREATE TABLE u(x TEXT PRIMARY KEY, y UNIQUE)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
        await db1.exec("INSERT INTO u VALUES ('k1', 'value1')");

        const db2 = await connect({ path: ':memory:', url: server.dbUrl });
        await db2.exec("INSERT INTO u VALUES ('k2', 'value1')");

        await db1.push();
        await expect(async () => await db2.push()).rejects.toThrow(/UNIQUE constraint failed/);

        await db1.close();
        await db2.close();
    });

    test('checkpoint', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
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

        await db1.close();
    });

    test('persistence push', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const dbPath = `test-${(Math.random() * 10000) | 0}.db`;
        try {
            {
                const db1 = await connect({ path: dbPath, url: server.dbUrl });
                await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
                await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
                await db1.close();
            }

            {
                const db2 = await connect({ path: dbPath, url: server.dbUrl });
                await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
                await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);
                const rows = await db2.prepare('SELECT * FROM q').all();
                const expected = [
                    { x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' },
                    { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }
                ];
                expect(rows).toEqual(expected);
                await db2.close();
            }

            {
                const db3 = await connect({ path: dbPath, url: server.dbUrl });
                await db3.push();
                await db3.close();
            }

            {
                const db4 = await connect({ path: dbPath, url: server.dbUrl });
                const rows = await db4.prepare('SELECT * FROM q').all();
                const expected = [
                    { x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' },
                    { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }
                ];
                expect(rows).toEqual(expected);
                await db4.close();
            }
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence pull push', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const path1 = `test-${(Math.random() * 10000) | 0}.db`;
        const path2 = `test-${(Math.random() * 10000) | 0}.db`;
        try {
            const db1 = await connect({ path: path1, url: server.dbUrl });
            await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
            await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
            const stats1 = await db1.stats();

            const db2 = await connect({ path: path2, url: server.dbUrl });
            await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
            await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);

            await Promise.all([db1.push(), db2.push()]);
            await Promise.all([db1.pull(), db2.pull()]);
            const stats2 = await db1.stats();
            expect(stats1.revision).not.toBe(stats2.revision);

            const rows1 = await db1.prepare('SELECT * FROM q').all();
            const rows2 = await db2.prepare('SELECT * FROM q').all();
            const expected = [
                { x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' },
                { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }
            ];
            expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare));
            expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare));

            await db1.close();
            await db2.close();
        } finally {
            cleanup(path1);
            cleanup(path2);
        }
    });

    test('update', async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const db = await connect({ path: ':memory:', url: server.dbUrl, longPollTimeoutMs: 5000 });
        await db.exec("INSERT INTO q VALUES ('1', '2')");
        await db.push();
        await db.exec("INSERT INTO q VALUES ('1', '2') ON CONFLICT DO UPDATE SET y = '3'");
        await db.push();
        await db.close();
    });

    test('transform', async () => {
        await server.dbSql("DROP TABLE IF EXISTS counter");
        await server.dbSql("CREATE TABLE counter(key TEXT PRIMARY KEY, value INTEGER)");
        await server.dbSql("INSERT INTO counter VALUES ('1', 0)");

        const transform = (m: DatabaseRowMutation) => ({
            operation: 'rewrite',
            stmt: {
                sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
                values: [m.after!.value - m.before!.value, m.after!.key]
            }
        } as DatabaseRowTransformResult);

        const db1 = await connect({ path: ':memory:', url: server.dbUrl, transform: transform });
        const db2 = await connect({ path: ':memory:', url: server.dbUrl, transform: transform });

        await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
        await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);

        const rows1 = await db1.prepare('SELECT * FROM counter').all();
        const rows2 = await db2.prepare('SELECT * FROM counter').all();
        expect(rows1).toEqual([{ key: '1', value: 2 }]);
        expect(rows2).toEqual([{ key: '1', value: 2 }]);

        await db1.close();
        await db2.close();
    });

    test('transform many', async () => {
        await server.dbSql("DROP TABLE IF EXISTS counter");
        await server.dbSql("CREATE TABLE counter(key TEXT PRIMARY KEY, value INTEGER)");
        await server.dbSql("INSERT INTO counter VALUES ('1', 0)");

        const transform = (m: DatabaseRowMutation) => ({
            operation: 'rewrite',
            stmt: {
                sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
                values: [m.after!.value - m.before!.value, m.after!.key]
            }
        } as DatabaseRowTransformResult);

        const db1 = await connect({ path: ':memory:', url: server.dbUrl, transform: transform });
        const db2 = await connect({ path: ':memory:', url: server.dbUrl, transform: transform });

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

        await db1.close();
        await db2.close();
    });

    test('concurrent actions consistency', { timeout: 60000 }, async () => {
        await server.dbSql("DROP TABLE IF EXISTS rows");
        await server.dbSql("CREATE TABLE rows(key TEXT PRIMARY KEY, value INTEGER)");
        await server.dbSql("INSERT INTO rows VALUES ('key', 0)");

        const db1 = await connect({ path: ':memory:', url: server.dbUrl });
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
        await db1.close();
    });

    test('pull push concurrent', { timeout: 60000 }, async () => {
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        let pullResolve: (() => void) | null = null;
        const pullFinish = new Promise<void>(resolve => pullResolve = resolve);
        let pushResolve: (() => void) | null = null;
        const pushFinish = new Promise<void>(resolve => pushResolve = resolve);
        let stopPull = false;
        let stopPush = false;

        const db = await connect({ path: ':memory:', url: server.dbUrl });

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

        await db.close();
    });

    // ==================== Persistence Tests ====================

    test('sync config persistence', async () => {
        // Test that sync config (revision, etc.) is persisted and restored
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES (42)");

        const dbPath = `test-config-persistence-${(Math.random() * 10000) | 0}.db`;
        try {
            // First connection - bootstrap from server
            {
                const db1 = await connect({
                    path: dbPath,
                    clientName: 'turso-sync-js',
                    url: server.dbUrl,
                });
                const rows = await db1.prepare("SELECT * FROM t").all();
                expect(rows).toEqual([{ x: 42 }]);
                await db1.close();
            }

            // Add data on server
            await server.dbSql("INSERT INTO t VALUES (41)");

            // Second connection - should restore sync state and be able to pull
            {
                const db2 = await connect({
                    path: dbPath,
                    url: server.dbUrl,
                });

                // Pull new changes
                const changes = await db2.pull();
                expect(changes).toBe(true);

                const rows = await db2.prepare("SELECT * FROM t").all();
                expect(rows).toEqual([{ x: 42 }, { x: 41 }]);
                await db2.close();
            }
        } finally {
            cleanup(dbPath);
        }
    });

    test('sync bootstrap persistent', async () => {
        // Test that bootstrap works correctly with persistent database
        await server.dbSql("DROP TABLE IF EXISTS t");
        await server.dbSql("CREATE TABLE t(x)");
        await server.dbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-js')");

        const dbPath = `test-bootstrap-persistent-${(Math.random() * 10000) | 0}.db`;
        try {
            const db = await connect({
                path: dbPath,
                url: server.dbUrl,
            });
            const rows = await db.prepare("SELECT * FROM t").all();
            expect(rows).toEqual([{ x: 'hello' }, { x: 'turso' }, { x: 'sync-js' }]);
            await db.close();

            // Verify files were created
            expect(existsSync(dbPath)).toBe(true);
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence offline mode', async () => {
        // Test that local data is accessible even when server is unreachable
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const dbPath = `test-offline-${(Math.random() * 10000) | 0}.db`;
        try {
            // First, sync with server
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                await db.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
                await db.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
                await db.push();
                await db.close();
            }

            // Reopen with invalid URL - should still work locally
            {
                const db = await connect({ path: dbPath, url: "http://not-valid-url.localhost:99999" });
                const rows = await db.prepare("SELECT * FROM q").all();
                const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }];
                expect(rows.sort(localeCompare)).toEqual(expected.sort(localeCompare));
                await db.close();
            }
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence with checkpoint', async () => {
        // Test that checkpoint works correctly with persistent storage
        await server.dbSql("DROP TABLE IF EXISTS q");
        await server.dbSql("CREATE TABLE q(x TEXT PRIMARY KEY, y)");

        const dbPath = `test-checkpoint-persistent-${(Math.random() * 10000) | 0}.db`;
        try {
            // Insert data and checkpoint
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                for (let i = 0; i < 500; i++) {
                    await db.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
                }
                expect((await db.stats()).mainWalSize).toBeGreaterThan(0);
                await db.checkpoint();
                expect((await db.stats()).mainWalSize).toBe(0);
                await db.close();
            }

            // Reopen and verify data is intact
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                const rows = await db.prepare("SELECT COUNT(*) as cnt FROM q").all();
                expect(rows).toEqual([{ cnt: 500 }]);

                // Push to server
                await db.push();
                await db.close();
            }

            // Verify server has the data
            const remote = await server.dbSql("SELECT COUNT(*) FROM q");
            expect(remote).toEqual([['500']]);
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence multiple sessions', async () => {
        // Test multiple open/close cycles with persistent storage
        await server.dbSql("DROP TABLE IF EXISTS sessions");
        await server.dbSql("CREATE TABLE sessions(id INTEGER PRIMARY KEY, data TEXT)");

        const dbPath = `test-multi-session-${(Math.random() * 10000) | 0}.db`;
        try {
            // Session 1: Insert initial data
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                await db.exec("INSERT INTO sessions (data) VALUES ('session1-data1')");
                await db.exec("INSERT INTO sessions (data) VALUES ('session1-data2')");
                await db.push();
                await db.close();
            }

            // Session 2: Add more data
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                await db.exec("INSERT INTO sessions (data) VALUES ('session2-data1')");
                // Don't push yet
                await db.close();
            }

            // Session 3: Verify unpushed data is still there, then push
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                const rows = await db.prepare("SELECT data FROM sessions ORDER BY id").all();
                expect(rows).toEqual([
                    { data: 'session1-data1' },
                    { data: 'session1-data2' },
                    { data: 'session2-data1' }
                ]);
                await db.push();
                await db.close();
            }

            // Verify all data is on server
            const remote = await server.dbSql("SELECT data FROM sessions ORDER BY id");
            expect(remote).toEqual([['session1-data1'], ['session1-data2'], ['session2-data1']]);
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence sync state across restarts', async () => {
        // Test that sync state (revision) is properly persisted
        await server.dbSql("DROP TABLE IF EXISTS sync_state");
        await server.dbSql("CREATE TABLE sync_state(key TEXT PRIMARY KEY, value INTEGER)");
        await server.dbSql("INSERT INTO sync_state VALUES ('counter', 0)");

        const dbPath = `test-sync-state-${(Math.random() * 10000) | 0}.db`;
        try {
            let revision1: string | null;

            // First session: sync and record revision
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                revision1 = (await db.stats()).revision;
                await db.close();
            }

            // Add data on server
            await server.dbSql("UPDATE sync_state SET value = 100 WHERE key = 'counter'");

            // Second session: revision should be same, pull should get changes
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                const revisionBefore = (await db.stats()).revision;
                expect(revisionBefore).toBe(revision1);

                const changes = await db.pull();
                expect(changes).toBe(true);

                const revisionAfter = (await db.stats()).revision;
                expect(revisionAfter).not.toBe(revision1);

                const rows = await db.prepare("SELECT value FROM sync_state WHERE key = 'counter'").all();
                expect(rows).toEqual([{ value: 100 }]);
                await db.close();
            }
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence two clients with files', async () => {
        // Test two persistent clients syncing through the server
        await server.dbSql("DROP TABLE IF EXISTS two_clients");
        await server.dbSql("CREATE TABLE two_clients(id TEXT PRIMARY KEY, client TEXT, value INTEGER)");

        const dbPath1 = `test-client1-${(Math.random() * 10000) | 0}.db`;
        const dbPath2 = `test-client2-${(Math.random() * 10000) | 0}.db`;
        try {
            // Client 1: Insert and push
            {
                const db1 = await connect({ path: dbPath1, url: server.dbUrl });
                await db1.exec("INSERT INTO two_clients VALUES ('a', 'client1', 1)");
                await db1.exec("INSERT INTO two_clients VALUES ('b', 'client1', 2)");
                await db1.push();
                await db1.close();
            }

            // Client 2: Bootstrap, should see client1's data
            {
                const db2 = await connect({ path: dbPath2, url: server.dbUrl });
                const rows = await db2.prepare("SELECT * FROM two_clients ORDER BY id").all();
                expect(rows).toEqual([
                    { id: 'a', client: 'client1', value: 1 },
                    { id: 'b', client: 'client1', value: 2 }
                ]);

                // Insert client2's data
                await db2.exec("INSERT INTO two_clients VALUES ('c', 'client2', 3)");
                await db2.push();
                await db2.close();
            }

            // Client 1: Reopen, pull and verify
            {
                const db1 = await connect({ path: dbPath1, url: server.dbUrl });
                await db1.pull();
                const rows = await db1.prepare("SELECT * FROM two_clients ORDER BY id").all();
                expect(rows).toEqual([
                    { id: 'a', client: 'client1', value: 1 },
                    { id: 'b', client: 'client1', value: 2 },
                    { id: 'c', client: 'client2', value: 3 }
                ]);
                await db1.close();
            }
        } finally {
            cleanup(dbPath1);
            cleanup(dbPath2);
        }
    });

    test('persistence large data', async () => {
        // Test persistence with larger amounts of data
        await server.dbSql("DROP TABLE IF EXISTS large_data");
        await server.dbSql("CREATE TABLE large_data(id INTEGER PRIMARY KEY, data BLOB)");

        const dbPath = `test-large-data-${(Math.random() * 10000) | 0}.db`;
        try {
            // Insert large data
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                // Insert 100 rows with ~1KB each
                for (let i = 0; i < 100; i++) {
                    await db.exec(`INSERT INTO large_data (data) VALUES (randomblob(1024))`);
                }
                await db.push();
                await db.close();
            }

            // Verify persistence
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                const rows = await db.prepare("SELECT COUNT(*) as cnt, SUM(LENGTH(data)) as total FROM large_data").all();
                expect(rows[0].cnt).toBe(100);
                expect(rows[0].total).toBe(100 * 1024);
                await db.close();
            }

            // Verify server has the data
            const remote = await server.dbSql("SELECT COUNT(*), SUM(LENGTH(data)) FROM large_data");
            expect(remote).toEqual([['100', `${100 * 1024}`]]);
        } finally {
            cleanup(dbPath);
        }
    });

    test('persistence with updates and deletes', async () => {
        // Test that updates and deletes are properly persisted
        await server.dbSql("DROP TABLE IF EXISTS crud");
        await server.dbSql("CREATE TABLE crud(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)");

        const dbPath = `test-crud-${(Math.random() * 10000) | 0}.db`;
        try {
            // Session 1: Insert
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                await db.exec("INSERT INTO crud VALUES (1, 'alice', 1)");
                await db.exec("INSERT INTO crud VALUES (2, 'bob', 1)");
                await db.exec("INSERT INTO crud VALUES (3, 'charlie', 1)");
                await db.push();
                await db.close();
            }

            // Session 2: Update and delete
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                await db.exec("UPDATE crud SET name = 'ALICE' WHERE id = 1");
                await db.exec("DELETE FROM crud WHERE id = 2");
                await db.exec("UPDATE crud SET active = 0 WHERE id = 3");
                // Don't push - close and reopen
                await db.close();
            }

            // Session 3: Verify changes are persisted locally
            {
                const db = await connect({ path: dbPath, url: server.dbUrl });
                const rows = await db.prepare("SELECT * FROM crud ORDER BY id").all();
                expect(rows).toEqual([
                    { id: 1, name: 'ALICE', active: 1 },
                    { id: 3, name: 'charlie', active: 0 }
                ]);
                await db.push();
                await db.close();
            }

            // Verify server state
            const remote = await server.dbSql("SELECT * FROM crud ORDER BY id");
            expect(remote).toEqual([['1', 'ALICE', '1'], ['3', 'charlie', '0']]);
        } finally {
            cleanup(dbPath);
        }
    });

    // ==================== Partial Sync Tests ====================

    test('partial sync with prefix strategy - network validation', { timeout: 60000 }, async () => {
        // Create a table with large data on server (~200KB total)
        await server.dbSql("DROP TABLE IF EXISTS partial_prefix");
        await server.dbSql("CREATE TABLE partial_prefix(id INTEGER PRIMARY KEY, data BLOB)");
        // Insert 200 rows with ~1KB each = ~200KB total
        for (let i = 0; i < 200; i++) {
            await server.dbSql(`INSERT INTO partial_prefix VALUES (${i}, randomblob(1024))`);
        }

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        // After bootstrap, should only download prefix (4KB + overhead), not the full 200KB
        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(32 * 1024); // 32KB max

        // Query single row - should load only needed pages
        const row = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_prefix WHERE id = 0").get();
        expect(row).toEqual({ id: 0, len: 1024 });

        const statsAfterSingleQuery = await db.stats();
        // Should still be much less than full data
        expect(statsAfterSingleQuery.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        // Now query all data - should load everything
        const total = await db.prepare("SELECT SUM(LENGTH(data)) as total FROM partial_prefix").all();
        expect(total).toEqual([{ total: 200 * 1024 }]);

        const statsAfterFullScan = await db.stats();
        // Should have downloaded most of the data now
        expect(statsAfterFullScan.networkReceivedBytes).toBeGreaterThanOrEqual(150 * 1024);

        await db.close();
    });

    test('partial sync with query strategy', { timeout: 60000 }, async () => {
        // Create a table with keyed data
        await server.dbSql("DROP TABLE IF EXISTS partial_query");
        await server.dbSql("CREATE TABLE partial_query(key INTEGER PRIMARY KEY, category TEXT, data BLOB)");
        // Insert 100 rows with ~1KB each
        for (let i = 0; i < 100; i++) {
            const category = i < 50 ? 'hot' : 'cold';
            await server.dbSql(`INSERT INTO partial_query VALUES (${i}, '${category}', randomblob(1024))`);
        }

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'query', query: "SELECT * FROM partial_query WHERE category = 'hot'" },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        // Query 'hot' category should work
        const hotCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query WHERE category = 'hot'").all();
        expect(hotCount).toEqual([{ cnt: 50 }]);

        // Query 'cold' category should also work
        const coldCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query WHERE category = 'cold'").all();
        expect(coldCount).toEqual([{ cnt: 50 }]);

        // Total count
        const totalCount = await db.prepare("SELECT COUNT(*) as cnt FROM partial_query").all();
        expect(totalCount).toEqual([{ cnt: 100 }]);

        await db.close();
    });

    test('partial sync with prefetch enabled', { timeout: 30000 }, async () => {
        // Create a table with data on server
        await server.dbSql("DROP TABLE IF EXISTS partial_prefetch");
        await server.dbSql("CREATE TABLE partial_prefetch(id INTEGER PRIMARY KEY, data BLOB)");
        for (let i = 0; i < 50; i++) {
            await server.dbSql(`INSERT INTO partial_prefetch VALUES (${i}, randomblob(512))`);
        }

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: true,  // Enable prefetch
            },
        });

        const statsAfterBootstrap = await db.stats();
        // With prefetch, bootstrap might load more speculatively
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        // Query all rows - prefetch should help performance
        const rows = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_prefetch ORDER BY id").all();
        expect(rows.length).toBe(50);
        expect(rows[0]).toEqual({ id: 0, len: 512 });
        expect(rows[49]).toEqual({ id: 49, len: 512 });

        await db.close();
    });

    test('partial sync pull changes', async () => {
        // Create initial data on server
        await server.dbSql("DROP TABLE IF EXISTS partial_pull");
        await server.dbSql("CREATE TABLE partial_pull(id INTEGER PRIMARY KEY, data TEXT)");
        await server.dbSql("INSERT INTO partial_pull VALUES (1, 'initial')");

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        // Verify initial data
        let rows = await db.prepare("SELECT * FROM partial_pull").all();
        expect(rows).toEqual([{ id: 1, data: 'initial' }]);

        // Add data on server
        await server.dbSql("INSERT INTO partial_pull VALUES (2, 'new-data')");

        // Pull changes
        const changes = await db.pull();
        expect(changes).toBe(true);

        // Verify new data is visible
        rows = await db.prepare("SELECT * FROM partial_pull ORDER BY id").all();
        expect(rows).toEqual([
            { id: 1, data: 'initial' },
            { id: 2, data: 'new-data' }
        ]);

        await db.close();
    });

    test('partial sync push changes', async () => {
        // Create table on server
        await server.dbSql("DROP TABLE IF EXISTS partial_push");
        await server.dbSql("CREATE TABLE partial_push(id INTEGER PRIMARY KEY, data TEXT)");
        await server.dbSql("INSERT INTO partial_push VALUES (1, 'server-data')");

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        // Insert locally
        await db.exec("INSERT INTO partial_push VALUES (2, 'client-data')");

        // Verify local state
        let rows = await db.prepare("SELECT * FROM partial_push ORDER BY id").all();
        expect(rows).toEqual([
            { id: 1, data: 'server-data' },
            { id: 2, data: 'client-data' }
        ]);

        // Push to server
        await db.push();

        // Verify server has the data
        const remote = await server.dbSql("SELECT * FROM partial_push ORDER BY id");
        expect(remote).toEqual([['1', 'server-data'], ['2', 'client-data']]);

        await db.close();
    });

    test('partial sync large table - lazy loading validation', { timeout: 60000 }, async () => {
        // Create a larger table on server (~100KB)
        await server.dbSql("DROP TABLE IF EXISTS partial_large");
        await server.dbSql("CREATE TABLE partial_large(id INTEGER PRIMARY KEY, data BLOB)");
        // Insert 100 rows with ~1KB each
        for (let i = 0; i < 100; i++) {
            await server.dbSql(`INSERT INTO partial_large VALUES (${i}, randomblob(1024))`);
        }

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 8192,
                prefetch: false,
            },
        });

        // After bootstrap, should only have prefix data
        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(32 * 1024);

        // Query first row - minimal extra loading
        const row = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_large WHERE id = 0").get();
        expect(row).toEqual({ id: 0, len: 1024 });

        const statsAfterFirstRow = await db.stats();
        expect(statsAfterFirstRow.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        // Query middle row - loads more pages
        const midRow = await db.prepare("SELECT id, LENGTH(data) as len FROM partial_large WHERE id = 50").get();
        expect(midRow).toEqual({ id: 50, len: 1024 });

        // Query COUNT(*) - triggers full scan, loads all pages
        const count = await db.prepare("SELECT COUNT(*) as cnt FROM partial_large").all();
        expect(count).toEqual([{ cnt: 100 }]);

        const statsAfterFullScan = await db.stats();
        // Should have downloaded most data
        expect(statsAfterFullScan.networkReceivedBytes).toBeGreaterThanOrEqual(75 * 1024);

        await db.close();
    });

    test('partial sync two clients', async () => {
        // Create table on server
        await server.dbSql("DROP TABLE IF EXISTS partial_two");
        await server.dbSql("CREATE TABLE partial_two(id TEXT PRIMARY KEY, client TEXT)");

        const db1 = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const db2 = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        // Each client inserts data
        await db1.exec("INSERT INTO partial_two VALUES ('a', 'client1')");
        await db1.exec("INSERT INTO partial_two VALUES ('b', 'client1')");

        await db2.exec("INSERT INTO partial_two VALUES ('c', 'client2')");
        await db2.exec("INSERT INTO partial_two VALUES ('d', 'client2')");

        // Push and pull
        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);

        // Both clients should see all data
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

        await db1.close();
        await db2.close();
    });

    test('partial sync vs full sync network comparison', { timeout: 60000 }, async () => {
        // Create a table with substantial data
        await server.dbSql("DROP TABLE IF EXISTS partial_compare");
        await server.dbSql("CREATE TABLE partial_compare(id INTEGER PRIMARY KEY, data BLOB)");
        // Insert 150 rows with ~1KB each = ~150KB
        for (let i = 0; i < 150; i++) {
            await server.dbSql(`INSERT INTO partial_compare VALUES (${i}, randomblob(1024))`);
        }

        // Full sync client
        const dbFull = await connect({
            path: ':memory:',
            url: server.dbUrl,
        });

        const fullSyncStats = await dbFull.stats();
        // Full sync should download everything
        expect(fullSyncStats.networkReceivedBytes).toBeGreaterThanOrEqual(100 * 1024);

        await dbFull.close();

        // Partial sync client
        const dbPartial = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 4096 },
                segmentSize: 4096,
                prefetch: false,
            },
        });

        const partialSyncStats = await dbPartial.stats();
        // Partial sync should download much less initially
        expect(partialSyncStats.networkReceivedBytes).toBeLessThanOrEqual(32 * 1024);

        // Partial should have downloaded significantly less than full
        expect(partialSyncStats.networkReceivedBytes).toBeLessThan(fullSyncStats.networkReceivedBytes / 3);

        await dbPartial.close();
    });

    test('partial sync with large segment size', { timeout: 60000 }, async () => {
        // Create a table with data
        await server.dbSql("DROP TABLE IF EXISTS partial_large_seg");
        await server.dbSql("CREATE TABLE partial_large_seg(id INTEGER PRIMARY KEY, data BLOB)");
        for (let i = 0; i < 100; i++) {
            await server.dbSql(`INSERT INTO partial_large_seg VALUES (${i}, randomblob(1024))`);
        }

        const db = await connect({
            path: ':memory:',
            url: server.dbUrl,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 32 * 1024 },
                segmentSize: 32 * 1024,  // Large segment size
                prefetch: false,
            },
        });

        // 32KB prefix plus some overhead
        const statsAfterBootstrap = await db.stats();
        expect(statsAfterBootstrap.networkReceivedBytes).toBeLessThanOrEqual(64 * 1024);

        // Select one record - with large segment size, may load more
        const row = await db.prepare("SELECT LENGTH(data) as length FROM partial_large_seg LIMIT 1").all();
        expect(row).toEqual([{ length: 1024 }]);

        const statsAfterSelect = await db.stats();
        // With large segments, one query might load more
        expect(statsAfterSelect.networkReceivedBytes).toBeLessThanOrEqual(128 * 1024);

        // Full scan should load all data
        const count = await db.prepare("SELECT COUNT(*) as cnt FROM partial_large_seg").all();
        expect(count).toEqual([{ cnt: 100 }]);

        const statsAfterFullScan = await db.stats();
        expect(statsAfterFullScan.networkReceivedBytes).toBeGreaterThanOrEqual(75 * 1024);

        await db.close();
    });

    test('partial sync concurrency', { timeout: 60000 }, async () => {
        // Create a table with data
        await server.dbSql("DROP TABLE IF EXISTS partial_conc");
        await server.dbSql("CREATE TABLE partial_conc(id INTEGER PRIMARY KEY, data BLOB)");
        for (let i = 0; i < 100; i++) {
            await server.dbSql(`INSERT INTO partial_conc VALUES (${i}, randomblob(1024))`);
        }

        // Open multiple partial sync connections concurrently
        const dbs: Database[] = [];
        for (let i = 0; i < 4; i++) {
            dbs.push(await connect({
                path: ':memory:',
                url: server.dbUrl,
                partialSyncExperimental: {
                    bootstrapStrategy: { kind: 'prefix', length: 32 * 1024 },
                    segmentSize: 32 * 1024,
                    prefetch: false,
                },
            }));
        }

        // Query all databases concurrently
        const queries = dbs.map(db => db.prepare("SELECT COUNT(*) as cnt FROM partial_conc").all());
        const results = await Promise.all(queries);

        // All should return same count
        expect(results).toEqual(new Array(4).fill([{ cnt: 100 }]));

        // Close all
        await Promise.all(dbs.map(db => db.close()));
    });

    test('concurrent updates', { timeout: 60000 }, async () => {
        await server.dbSql("DROP TABLE IF EXISTS concurrent_upd");
        await server.dbSql("CREATE TABLE concurrent_upd(x TEXT PRIMARY KEY, y BLOB)");

        const db = await connect({ path: ':memory:', url: server.dbUrl });
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

        // Do concurrent updates
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

        await db.close();
    });

    test('corruption bug regression', { timeout: 30000 }, async () => {
        await server.dbSql("DROP TABLE IF EXISTS corruption_test");
        await server.dbSql("CREATE TABLE corruption_test(x TEXT PRIMARY KEY, y BLOB)");

        const db = await connect({ path: ':memory:', url: server.dbUrl });

        // Do many updates
        for (let i = 0; i < 50; i++) {
            await db.exec(`INSERT INTO corruption_test VALUES ('1', randomblob(1024)) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
        }
        await db.pull();
        await db.push();

        // Do more updates
        for (let i = 0; i < 50; i++) {
            await db.exec(`INSERT INTO corruption_test VALUES ('1', randomblob(1024)) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
        }
        await db.pull();
        await db.push();

        // Verify data is readable
        const rows = await db.prepare("SELECT LENGTH(y) as len FROM corruption_test").all();
        expect(rows[0].len).toBe(1024);

        await db.close();
    });

    test('checkpoint and concurrent actions', { timeout: 60000 }, async () => {
        await server.dbSql("DROP TABLE IF EXISTS checkpoint_conc");
        await server.dbSql("CREATE TABLE checkpoint_conc(key TEXT PRIMARY KEY, value INTEGER)");
        await server.dbSql("INSERT INTO checkpoint_conc VALUES ('key', 0)");

        const db = await connect({ path: ':memory:', url: server.dbUrl });
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
        await db.close();
    });
});
