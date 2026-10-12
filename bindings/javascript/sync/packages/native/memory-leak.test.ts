import assert from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { test as baseTest, expect } from 'vitest'
import { connect, Database } from './promise.js'
import { nativeAllocatedBytes } from './index.js'
import { TursoServer } from './turso-server.js'

const WARMUP_ITERATIONS = 50;
const MEASURED_ITERATIONS = Number(process.env.LEAK_CHECK_ITERATIONS ?? 300);
const MAX_NATIVE_GROWTH_BYTES = 256 * 1024;
const MAX_JS_HEAP_GROWTH_BYTES = 1024 * 1024;
const MAX_GC_ROUNDS = 100;
const ITERATIONS_BETWEEN_GC = 25;
const UNREACHABLE_URL = 'http://127.0.0.1:1';
const CLOSED_STATEMENTS = 2000;
const MAX_BYTES_PER_CLOSED_STATEMENT = 1024;

setFlagsFromString('--expose-gc');
const gc: () => void = runInNewContext('gc');

const leakCheckEnabled = nativeAllocatedBytes() != null && process.env.LOCAL_SYNC_SERVER != null;

const test = baseTest.extend<{ server: TursoServer, dir: string }>({
    server: async ({ }, use) => {
        const server = await TursoServer.create();
        try { await use(server); } finally { server.close(); }
    },
    dir: async ({ }, use) => {
        const dir = mkdtempSync(join(tmpdir(), 'turso-sync-leak-'));
        try { await use(dir); } finally { rmSync(dir, { recursive: true, force: true }); }
    },
});

test.skipIf(!leakCheckEnabled)('connect and close does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    await expectNoLeak(async () => {
        const db = await connect({ path: join(dir, 'connect.db'), url: server.dbUrl() });
        await db.close();
    });
})

test.skipIf(!leakCheckEnabled)('push does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    const db = await connectWithSingleRow(server, join(dir, 'push.db'));
    await expectNoLeak(async () => {
        await db.exec("UPDATE t SET v = randomblob(1024) WHERE x = 1");
        await db.push();
        await db.checkpoint();
    });
    await db.close();
})

test.skipIf(!leakCheckEnabled)('pull without remote changes does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    const db = await connectWithSingleRow(server, join(dir, 'pull.db'));
    await expectNoLeak(async () => {
        await db.pull();
    });
    await db.close();
})

test.skipIf(!leakCheckEnabled)('pull with remote changes does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    const writer = await connectWithSingleRow(server, join(dir, 'writer.db'));
    const reader = await connect({ path: join(dir, 'reader.db'), url: server.dbUrl() });
    await expectNoLeak(async () => {
        await writer.exec("UPDATE t SET v = randomblob(1024) WHERE x = 1");
        await writer.push();
        await writer.checkpoint();
        assert.strictEqual(await reader.pull(), true);
        await reader.checkpoint();
    });
    await writer.close();
    await reader.close();
})

test.skipIf(!leakCheckEnabled)('stats does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    const db = await connectWithSingleRow(server, join(dir, 'stats.db'));
    await expectNoLeak(async () => {
        await db.stats();
    });
    await db.close();
})

test.skipIf(!leakCheckEnabled)('local statements on a synced database do not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    const db = await connectWithSingleRow(server, join(dir, 'local.db'));
    await expectNoLeak(async () => {
        const update = await db.prepare("UPDATE t SET v = randomblob(1024) WHERE x = ?");
        await update.run(1);
        const select = await db.prepare("SELECT length(v) AS len FROM t WHERE x = ?");
        assert.deepStrictEqual(await select.get(1), { len: 1024 });
        await db.checkpoint();
    });
    await db.close();
})

test.skipIf(!leakCheckEnabled)('closing a statement frees its memory without waiting for GC', async () => {
    const db = await connect({ path: ':memory:' });
    const before = nativeAllocatedBytes()!;
    for (let i = 0; i < CLOSED_STATEMENTS; i++) {
        const stmt = await db.prepare("SELECT 1");
        stmt.close();
    }
    const bytesPerStatement = (nativeAllocatedBytes()! - before) / CLOSED_STATEMENTS;
    console.info(`memory kept per closed statement before GC: ${bytesPerStatement} bytes`);
    expect(bytesPerStatement, 'closed statements kept their memory until GC').toBeLessThan(MAX_BYTES_PER_CLOSED_STATEMENT);
    await db.close();
})

test.skipIf(!leakCheckEnabled)('push to an unreachable server does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    let url = server.dbUrl();
    const db = await connect({ path: join(dir, 'failed-push.db'), url: () => url });
    await db.exec("CREATE TABLE t(x INTEGER PRIMARY KEY, v BLOB)");
    await db.exec("INSERT INTO t VALUES (1, randomblob(1024))");
    await db.push();
    await db.exec("UPDATE t SET v = randomblob(1024) WHERE x = 1");
    url = UNREACHABLE_URL;
    await expectNoLeak(async () => {
        await assert.rejects(db.push(), /fetch error/);
    });
    url = server.dbUrl();
    await db.push();
    await db.close();
})

test.skipIf(!leakCheckEnabled)('pull from an unreachable server does not leak', { timeout: 300_000 }, async ({ server, dir }) => {
    let url = server.dbUrl();
    const db = await connect({ path: join(dir, 'failed-pull.db'), url: () => url });
    url = UNREACHABLE_URL;
    await expectNoLeak(async () => {
        await assert.rejects(db.pull(), /fetch error/);
    });
    await db.close();
})

async function connectWithSingleRow(server: TursoServer, path: string): Promise<Database> {
    const db = await connect({ path, url: server.dbUrl() });
    await db.exec("CREATE TABLE IF NOT EXISTS t(x INTEGER PRIMARY KEY, v BLOB)");
    await db.exec("INSERT OR REPLACE INTO t VALUES (1, randomblob(1024))");
    await db.push();
    await db.checkpoint();
    return db;
}

async function expectNoLeak(iteration: () => Promise<void>) {
    await runIterations(iteration, WARMUP_ITERATIONS);
    const before = await measureMemory();
    await runIterations(iteration, MEASURED_ITERATIONS);
    const after = await measureMemory();
    const nativeGrowth = after.native - before.native;
    const jsHeapGrowth = after.jsHeap - before.jsHeap;
    console.info(`memory growth after ${MEASURED_ITERATIONS} iterations: native=${nativeGrowth} bytes, js heap=${jsHeapGrowth} bytes`);
    expect(nativeGrowth, 'memory allocated by the native module kept growing').toBeLessThan(MAX_NATIVE_GROWTH_BYTES);
    expect(jsHeapGrowth, 'JS heap kept growing').toBeLessThan(MAX_JS_HEAP_GROWTH_BYTES);
}

async function runIterations(iteration: () => Promise<void>, count: number) {
    for (let i = 1; i <= count; i++) {
        await iteration();
        if (i % ITERATIONS_BETWEEN_GC === 0) {
            await collectGarbageUntilNativeMemoryIsStable();
        }
    }
}

async function measureMemory(): Promise<{ native: number, jsHeap: number }> {
    const native = await collectGarbageUntilNativeMemoryIsStable();
    return { native, jsHeap: process.memoryUsage().heapUsed };
}

async function collectGarbageUntilNativeMemoryIsStable(): Promise<number> {
    let previous = nativeAllocatedBytes()!;
    let unchangedRounds = 0;
    for (let round = 0; round < MAX_GC_ROUNDS && unchangedRounds < 3; round++) {
        gc();
        await new Promise(resolve => setImmediate(resolve));
        const current = nativeAllocatedBytes()!;
        unchangedRounds = current === previous ? unchangedRounds + 1 : 0;
        previous = current;
    }
    return previous;
}
