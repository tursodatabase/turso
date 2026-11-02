import { test, expect, afterAll } from 'vitest';
import { connect } from './promise-default.js';
import { MainWorker } from './index-default.js';
import { BrowserIO } from './browser-io.js';

afterAll(() => MainWorker?.terminate());

test('fresh encrypted OPFS bootstrap rejects host recovery artifacts before HTTP', async () => {
    for (const [suffix, contents] of [
        ['-replace-base-apply', new Uint8Array()],
        ['-replace-base-apply-main-db.backup', new Uint8Array([1, 2, 3])],
    ] as const) {
        const path = `orphan-${crypto.randomUUID()}.db`;
        const artifact = path + suffix;
        await BrowserIO.write(artifact, contents);
        const before = localStorage.getItem(artifact);
        let requests = 0;
        await expect(connect({ path, url: 'https://fixture.test', localEncryptionKey: '16'.repeat(32),
            remoteEncryption: { cipher: 'aes256gcm', key: btoa(String.fromCharCode(...new Uint8Array(32).fill(11))) },
            fetch: async () => { requests++; throw new Error('unexpected HTTP request'); },
        })).rejects.toThrow('existing recovery artifact');
        expect(requests).toBe(0);
        expect(localStorage.getItem(artifact)).toBe(before);
        expect(await BrowserIO.read(path + '-info')).toBeNull();
    }
});

test('encrypted OPFS bootstrap, checkpoint, offline reopen, and wrong key', async () => {
    const path = `encrypted-${crypto.randomUUID()}.db`;
    const localKey = '16'.repeat(32);
    const cloudKey = btoa(String.fromCharCode(...new Uint8Array(32).fill(11)));
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1, 72, 1]);
    const fetch: typeof globalThis.fetch = async (_url, options) => {
        const headers = new Headers(options?.headers);
        expect(headers.get('x-turso-encryption-key')).toBe(cloudKey);
        expect(headers.get('x-turso-encryption-cipher')).toBe('aes256gcm');
        return new Response(Uint8Array.from([header.length, ...header]), { status: 200 });
    };
    const db = await connect({ path, url: 'https://fixture.test', localEncryptionKey: localKey,
        remoteEncryption: { cipher: 'aes256gcm', key: cloudKey }, fetch });
    await db.exec("CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('binding private marker')");
    await db.checkpoint();
    await db.close();
    const metadata = await BrowserIO.read(path + '-info');
    expect(new TextDecoder().decode(metadata!.subarray(0, 8))).toBe('TSYNMETA');
    expect(new TextDecoder().decode(metadata!)).not.toContain('binding private marker');
    const reopened = await connect({ path, localEncryptionKey: localKey,
        remoteEncryption: { cipher: 'aes256gcm' }, fetch: async () => { throw new Error('offline'); } });
    expect(await (await reopened.prepare('SELECT value FROM t')).get()).toEqual({ value: 'binding private marker' });
    await reopened.close();
    await expect(connect({ path, localEncryptionKey: '17'.repeat(32),
        remoteEncryption: { cipher: 'aes256gcm' } })).rejects.toThrow();
});
