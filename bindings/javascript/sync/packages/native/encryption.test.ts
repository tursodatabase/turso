import { test, expect, vi } from 'vitest';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { connect } from './promise.js';
import { encryptedCiphers, encryptedServer } from './encrypted-server.js';

test.each([undefined, ['encryption'], ['sync_encryption_extra'], ['sync-encryption']] as const)(
    'local encrypted sync requires the exact experimental opt-in: %j', async (experimental) => {
        const directory = mkdtempSync(join(tmpdir(), 'encrypted-opt-in-'));
        const path = join(directory, 'replica.db');
        try {
            let requests = 0;
            await expect(connect({ path, url: 'https://fixture.test', localEncryptionKey: '16'.repeat(32),
                remoteEncryption: { cipher: 'aes256gcm', key: Buffer.alloc(32, 11).toString('base64') },
                experimental: experimental as any,
                fetch: async () => { requests++; throw new Error('unexpected HTTP'); },
            })).rejects.toThrow('sync_encryption');
            expect(requests).toBe(0);
            expect(() => readFileSync(path + '-info')).toThrow();
            expect(() => readFileSync(path)).toThrow();
        } finally { rmSync(directory, { recursive: true, force: true }); }
    });

test.each(encryptedCiphers.flatMap(cipher => [true, false].map(mvcc => ({ cipher, mvcc }))))(
    'real encrypted $cipher sync, mvcc=$mvcc, pushes and pulls multi-chunk records', async ({ cipher, mvcc }) => {
        const server = await encryptedServer(cipher, mvcc);
        const directory = mkdtempSync(join(tmpdir(), 'binding-roundtrip-'));
        const path = join(directory, 'replica.db');
        try {
            const options = { path, url: server.url, experimental: ['sync_encryption' as const], localEncryptionKey: server.localKey,
                remoteEncryption: { cipher, key: server.cloudKey } };
            const db = await connect(options);
            try {
                expect(await (await db.prepare('SELECT value FROM binding_items WHERE id=1')).get()).toEqual({ value: 'nonempty bootstrap' });
                await db.exec("INSERT INTO binding_items VALUES (2, 'local binding marker')");
                await db.push();
                expect((await server.sql('SELECT count(*) FROM binding_items')).rows[0][0].value).toBe('2');
                await server.sql('INSERT INTO binding_items VALUES (3, hex(zeroblob(40000)))');
                await db.pull();
                expect(await (await db.prepare('SELECT length(value) AS n FROM binding_items WHERE id=3')).get()).toEqual({ n: 80000 });
                await db.checkpoint();
            } finally { await db.close(); }
            const reopened = await connect({ path, experimental: ['sync_encryption' as const], localEncryptionKey: server.localKey, remoteEncryption: { cipher },
                fetch: async () => { throw new Error('offline'); } });
            try { expect(await (await reopened.prepare('SELECT count(*) AS n FROM binding_items')).get()).toEqual({ n: 3 }); }
            finally { await reopened.close(); }
            expect(readFileSync(path).includes(Buffer.from('local binding marker'))).toBe(false);
        } finally { await server.close(); rmSync(directory, { recursive: true, force: true }); }
    }, 30000);

test('encrypted remoteWritesExperimental uses the real RemoteWriter and never sends the local key', async () => {
    const server = await encryptedServer('aes256gcm');
    const directory = mkdtempSync(join(tmpdir(), 'encrypted-remote-writer-'));
    const requests: { path: string; headers: Headers }[] = [];
    const originalFetch = globalThis.fetch;
    const intercepted = vi.spyOn(globalThis, 'fetch').mockImplementation(async (url, options) => {
        requests.push({ path: String(url), headers: new Headers(options?.headers) });
        if (String(url).endsWith('/v3/pipeline')) {
            // The sample server implements v2 execute; adapt only the v3
            // sequence envelope while forwarding RemoteWriter's headers intact.
            const body = JSON.parse(String(options?.body));
            expect(body.requests[0].type).toBe('sequence');
            return originalFetch(String(url).replace('/v3/pipeline', '/v2/pipeline'), {
                ...options, body: JSON.stringify({ requests: [{ type: 'execute', stmt: { sql: body.requests[0].sql } }] }),
            });
        }
        return originalFetch(url, options);
    });
    try {
        const db = await connect({ path: join(directory, 'replica.db'), url: server.url,
            experimental: ['sync_encryption' as const], localEncryptionKey: server.localKey, remoteEncryption: { cipher: server.cipher, key: server.cloudKey },
            remoteWritesExperimental: true });
        try {
            await db.exec("INSERT INTO binding_items VALUES (2, 'remote writer value')");
            await db.pull();
            expect(await (await db.prepare('SELECT value FROM binding_items WHERE id=2')).get()).toEqual({ value: 'remote writer value' });
            expect((await server.sql('SELECT count(*) FROM binding_items')).rows[0][0].value).toBe('2');
            expect(requests.some(request => request.path.endsWith('/pull-updates'))).toBe(true);
            const pipeline = requests.find(request => request.path.endsWith('/v3/pipeline'))!;
            expect(pipeline).toBeDefined();
            expect(pipeline.headers.get('x-turso-encryption-key')).toBe(server.cloudKey);
            expect(pipeline.headers.get('x-turso-encryption-cipher')).toBeNull();
            for (const request of requests) expect(JSON.stringify([...request.headers])).not.toContain(server.localKey);
        } finally { await db.close(); }
    } finally { intercepted.mockRestore(); await server.close(); rmSync(directory, { recursive: true, force: true }); }
}, 30000);

test('encrypted bootstrap, local writes, checkpoint, offline reopen, and wrong key', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'turso-sync-encryption-'));
    const path = join(directory, 'replica.db');
    const localKey = '16'.repeat(32);
    const cloudKey = Buffer.alloc(32, 11).toString('base64');
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1]);
    const fetch: typeof globalThis.fetch = async (_url, options) => {
        const headers = new Headers(options?.headers);
        expect(headers.get('x-turso-encryption-key')).toBe(cloudKey);
        expect(headers.get('x-turso-encryption-cipher')).toBe('aes256gcm');
        expect(JSON.stringify([...headers])).not.toContain(localKey);
        return new Response(Uint8Array.from([header.length, ...header]), { status: 200 });
    };
    try {
        const db = await connect({ path, url: 'https://fixture.test', experimental: ['sync_encryption' as const], localEncryptionKey: localKey,
            remoteEncryption: { cipher: 'aes256gcm', key: cloudKey }, fetch });
        await db.exec("CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('binding private marker')");
        await db.checkpoint();
        await db.close();
        expect(readFileSync(path + '-info').subarray(0, 8).toString()).toBe('TSYNMETA');
        expect(readFileSync(path).includes(Buffer.from('binding private marker'))).toBe(false);
        const reopened = await connect({ path, experimental: ['sync_encryption' as const], localEncryptionKey: localKey,
            remoteEncryption: { cipher: 'aes256gcm' }, fetch: async () => { throw new Error('offline'); } });
        expect(await (await reopened.prepare('SELECT value FROM t')).get()).toEqual({ value: 'binding private marker' });
        await reopened.close();
        await expect(connect({ path, experimental: ['sync_encryption' as const], localEncryptionKey: '17'.repeat(32),
            remoteEncryption: { cipher: 'aes256gcm' } })).rejects.toThrow();
    } finally { rmSync(directory, { recursive: true, force: true }); }
});
