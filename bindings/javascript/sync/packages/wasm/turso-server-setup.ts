import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function probe(url: URL): Promise<void> {
    return new Promise((resolve, reject) => {
        const req = request({
            hostname: url.hostname,
            port: url.port || 80,
            method: 'GET',
            path: '/',
        }, res => {
            res.resume();
            res.on('end', () => resolve());
            res.on('error', reject);
        });
        req.on('error', reject);
        req.end();
    });
}

const children: ChildProcess[] = [];

function run(localSyncServer: string, args: string[]): Promise<void> {
    return new Promise((resolve, reject) => {
        const proc = spawn(localSyncServer, args, { stdio: 'ignore' });
        proc.on('error', reject);
        proc.on('exit', code => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`tursodb exited with code ${code}`));
            }
        });
    });
}

async function startServer(localSyncServer: string, target: string, database?: string) {
    const url = new URL(target);
    const port = url.port || (url.protocol === 'https:' ? '443' : '80');
    const args = ['--sync-server', `0.0.0.0:${port}`];
    if (database != null) {
        args.push(database);
    }

    const proc = spawn(localSyncServer, args, {
        stdio: 'ignore',
    });
    const deadline = Date.now() + 30000;
    let ready = false;
    while (Date.now() < deadline) {
        if (proc.exitCode !== null) break;
        try {
            await probe(url);
            ready = true;
            break;
        } catch {
            await new Promise(r => setTimeout(r, 100));
        }
    }
    if (!ready) {
        proc.kill();
        throw new Error(`local sync server did not become available within 30s on port ${port}`);
    }
    children.push(proc);
}

export default async function setup() {
    const localSyncServer = process.env.LOCAL_SYNC_SERVER;
    if (!localSyncServer) {
        if (!process.env.VITE_TURSO_DB_URL || !process.env.VITE_TURSO_MVCC_DB_URL) {
            throw new Error('LOCAL_SYNC_SERVER or both remote server URLs must be set');
        }
        return;
    }

    const tempDir = await mkdtemp(join(tmpdir(), 'turso-sync-wasm-'));
    const mvccDatabase = join(tempDir, 'remote.db');
    await run(localSyncServer, [mvccDatabase, "PRAGMA journal_mode = 'mvcc'; CREATE TABLE mvcc_server_ready(x);"]);
    await Promise.all([
        startServer(localSyncServer, process.env.VITE_TURSO_DB_URL || 'http://localhost:10001'),
        startServer(localSyncServer, process.env.VITE_TURSO_MVCC_DB_URL || 'http://localhost:10002', mvccDatabase),
    ]);
    return async () => {
        for (const child of children) {
            child.kill();
        }
        children.length = 0;
        await rm(tempDir, { recursive: true, force: true });
    };
}
