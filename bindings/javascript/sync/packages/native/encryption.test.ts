import { test, expect } from 'vitest';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { connect } from './promise.js';

test('encrypted bootstrap, local writes, checkpoint, offline reopen, and wrong key', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'turso-sync-encryption-'));
    const path = join(directory, 'replica.db');
    const localKey = '16'.repeat(32);
    const cloudKey = Buffer.alloc(32, 11).toString('base64');
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1, 72, 1]);
    const fetch: typeof globalThis.fetch = async (_url, options) => {
        const headers = new Headers(options?.headers);
        expect(headers.get('x-turso-encryption-key')).toBe(cloudKey);
        expect(headers.get('x-turso-encryption-cipher')).toBe('aes256gcm');
        expect(JSON.stringify([...headers])).not.toContain(localKey);
        return new Response(Uint8Array.from([header.length, ...header]), { status: 200 });
    };
    try {
        const db = await connect({ path, url: 'https://fixture.test', localEncryptionKey: localKey,
            remoteEncryption: { cipher: 'aes256gcm', key: cloudKey }, fetch });
        await db.exec("CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('binding private marker')");
        await db.checkpoint();
        await db.close();
        expect(readFileSync(path + '-info').subarray(0, 8).toString()).toBe('TSYNMETA');
        expect(readFileSync(path).includes(Buffer.from('binding private marker'))).toBe(false);
        const reopened = await connect({ path, localEncryptionKey: localKey,
            remoteEncryption: { cipher: 'aes256gcm' }, fetch: async () => { throw new Error('offline'); } });
        expect(await (await reopened.prepare('SELECT value FROM t')).get()).toEqual({ value: 'binding private marker' });
        await reopened.close();
        await expect(connect({ path, localEncryptionKey: '17'.repeat(32),
            remoteEncryption: { cipher: 'aes256gcm' } })).rejects.toThrow();
    } finally { rmSync(directory, { recursive: true, force: true }); }
});
