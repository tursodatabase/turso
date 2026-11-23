import { test, expect, vi } from 'vitest';
import { RemoteWriter } from '../common/remote-writer.js';

test('remote writer sends the selected cipher and cloud key together', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
        baton: null,
        results: [
            { type: 'ok', response: { type: 'sequence' } },
            { type: 'ok', response: { type: 'get_autocommit', is_autocommit: true } },
        ],
    }), { status: 200 }));
    try {
        const writer = new RemoteWriter({ url: 'https://fixture.test',
            remoteEncryption: { cipher: 'aes256gcm', key: 'cloud-key' } });
        await writer.sequence('SELECT 1');
        const headers = new Headers(fetch.mock.calls[0][1]?.headers);
        expect(headers.get('x-turso-encryption-cipher')).toBe('aes256gcm');
        expect(headers.get('x-turso-encryption-key')).toBe('cloud-key');
    } finally { fetch.mockRestore(); }
});

test.each([{ cipher: 'aes256gcm' }, { key: 'cloud-key' }])('remote writer rejects incomplete credentials before HTTP: %j', async (encryption) => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('unexpected HTTP'));
    try {
        const writer = new RemoteWriter({ url: 'https://fixture.test',
            remoteEncryption: encryption as any });
        await expect(writer.sequence('SELECT 1')).rejects.toThrow('both a cipher and a cloud key');
        expect(fetch).not.toHaveBeenCalled();
    } finally { fetch.mockRestore(); }
});
