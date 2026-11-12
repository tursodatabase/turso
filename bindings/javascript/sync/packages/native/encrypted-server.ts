import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

export const encryptedCiphers = ['aes256gcm', 'aes128gcm', 'aegis128l'] as const;

// Real encrypted sample server shared by the Node and Chromium integration tests.
export async function encryptedServer(cipher: typeof encryptedCiphers[number], mvcc = true) {
    const directory = await mkdtemp(join(tmpdir(), 'encrypted-binding-server-'));
    const size = cipher === 'aes256gcm' ? 32 : 16;
    const localKey = '16'.repeat(size);
    const cloudKey = Buffer.alloc(size, 11).toString('base64');
    const key = join(directory, 'key.hex');
    await writeFile(key, '0b'.repeat(size), { mode: 0o600 });
    const listener = createServer();
    listener.listen(0, '127.0.0.1');
    await once(listener, 'listening');
    const port = (listener.address() as { port: number }).port;
    await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
    const url = `http://127.0.0.1:${port}`;
    const executable = process.env.LOCAL_SYNC_SERVER ?? fileURLToPath(new URL('../../../../../target/debug/tursodb', import.meta.url));
    const child = spawn(executable, ['--sync-server', `127.0.0.1:${port}`,
        '--sync-encryption-cipher', cipher, '--sync-encryption-key-file', key, join(directory, 'source.db')]);
    let output = '';
    let spawnError: Error | undefined;
    child.on('error', error => { spawnError = error; });
    child.stderr.on('data', chunk => { output += chunk; });
    child.stdout.resume();
    const close = async () => {
        if (child.exitCode === null && child.pid) {
            const exited = once(child, 'exit');
            child.kill('SIGINT');
            await exited;
        }
        await rm(directory, { recursive: true, force: true });
    };
    const sql = async (sql: string) => {
        const response = await fetch(url + '/v2/pipeline', { method: 'POST', headers: {
            'x-turso-encryption-key': cloudKey, 'x-turso-encryption-cipher': cipher,
        }, body: JSON.stringify({ requests: [{ type: 'execute', stmt: { sql } }] }) });
        const body = await response.json();
        if (!response.ok || body.results?.[0]?.type !== 'ok') throw new Error(JSON.stringify(body));
        return body.results[0].response.result;
    };
    try {
        let ready = false;
        for (let attempt = 0; attempt < 200; attempt++) {
            if (spawnError) throw spawnError;
            if (child.exitCode !== null) throw new Error(`encrypted server exited: ${output}`);
            try { await fetch(url); ready = true; break; } catch { await new Promise(r => setTimeout(r, 25)); }
        }
        if (!ready) throw new Error(`encrypted server readiness timeout: ${output}`);
        if (mvcc) await sql('PRAGMA journal_mode=mvcc');
        await sql('CREATE TABLE binding_items(id INTEGER PRIMARY KEY, value TEXT)');
        await sql("INSERT INTO binding_items VALUES (1, 'nonempty bootstrap')");
        return { url, cipher, localKey, cloudKey, sql, close };
    } catch (error) { await close(); throw error; }
}
