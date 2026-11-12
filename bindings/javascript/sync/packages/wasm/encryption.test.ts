import { test, expect, afterAll, vi } from 'vitest';
import { connect } from './promise-default.js';
import { MainWorker } from './index-default.js';
import { BrowserIO, EncryptedBrowserIO } from './browser-io.js';

afterAll(() => MainWorker?.terminate());

test('fresh encrypted OPFS bootstrap rejects host recovery artifacts before HTTP', async () => {
    for (const [suffix, contents] of [
        ['-replace-base-apply', new Uint8Array()],
        ['-replace-base-apply-main-db.backup', new Uint8Array([1, 2, 3])],
    ] as const) {
        const path = `orphan-${crypto.randomUUID()}.db`;
        const artifact = path + suffix;
        await EncryptedBrowserIO.write(artifact, contents);
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
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1]);
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
    expect(localStorage.getItem(path + '-info')).toMatch(/^turso-sync-binary-v1:/);
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

test.each(['missing', 'empty', 'partial'] as const)('encrypted OPFS bootstrap handles %s metadata after failure', async (state) => {
    const path = `interrupted-${crypto.randomUUID()}.db`;
    let requests = 0;
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1]);
    const options = {
        path, url: 'https://fixture.test', localEncryptionKey: '16'.repeat(32),
        remoteEncryption: { cipher: 'aes256gcm' as const, key: btoa(String.fromCharCode(...new Uint8Array(32).fill(11))) },
        fetch: async () => { requests++; return new Response(Uint8Array.from([header.length, ...header]), { status: 200 }); },
    };
    const write = EncryptedBrowserIO.write;
    const interrupted = vi.spyOn(EncryptedBrowserIO, 'write').mockImplementationOnce(async (name, data) => {
        expect(name).toBe(path + '-info');
        if (state !== 'missing') {
            await write(name, data.subarray(0, state === 'empty' ? 0 : Math.floor(data.length / 2)));
        }
        throw new Error('injected incomplete metadata write');
    });
    try {
        await expect(connect(options)).rejects.toThrow('injected incomplete metadata write');
    } finally {
        interrupted.mockRestore();
    }
    if (state === 'partial') {
        const stored = localStorage.getItem(path + '-info');
        requests = 0;
        await expect(connect(options)).rejects.toThrow();
        expect(requests).toBe(0);
        expect(localStorage.getItem(path + '-info')).toBe(stored);
        return;
    }
    const db = await connect(options);
    await db.exec("CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('retried')");
    await db.checkpoint();
    await db.close();
    const reopened = await connect({ ...options, url: () => null,
        fetch: async () => { throw new Error('offline'); } });
    expect(await (await reopened.prepare('SELECT value FROM t')).get()).toEqual({ value: 'retried' });
    await reopened.close();
});

test('plaintext OPFS metadata stays JSON through checkpoint and reopen', async () => {
    const path = `plain-${crypto.randomUUID()}.db`;
    const header = Uint8Array.from([10, 3, 114, 101, 118, 26, 0, 64, 1]);
    const fetch: typeof globalThis.fetch = async () =>
        new Response(Uint8Array.from([header.length, ...header]), { status: 200 });
    const db = await connect({ path, url: 'https://fixture.test', fetch });
    await db.exec("CREATE TABLE t(value TEXT); INSERT INTO t VALUES ('plaintext value')");
    await db.checkpoint();
    await db.close();
    const stored = localStorage.getItem(path + '-info')!;
    expect(() => JSON.parse(stored)).not.toThrow();
    expect(new TextDecoder().decode((await BrowserIO.read(path + '-info'))!)).toBe(stored);
    const reopened = await connect({ path, url: () => null,
        fetch: async () => { throw new Error('offline'); } });
    expect(await (await reopened.prepare('SELECT value FROM t')).get()).toEqual({ value: 'plaintext value' });
    await reopened.close();
});
