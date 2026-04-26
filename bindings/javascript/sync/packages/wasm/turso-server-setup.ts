import { spawn, type ChildProcess } from 'node:child_process';
import { request } from 'node:http';

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

let child: ChildProcess | null = null;

export default async function setup() {
    const localSyncServer = process.env.LOCAL_SYNC_SERVER;
    if (!localSyncServer) {
        if (!process.env.VITE_TURSO_DB_URL) {
            throw new Error('either LOCAL_SYNC_SERVER or VITE_TURSO_DB_URL env var must be set');
        }
        return;
    }

    const target = process.env.VITE_TURSO_DB_URL || 'http://localhost:10001';
    const url = new URL(target);
    const port = url.port || (url.protocol === 'https:' ? '443' : '80');

    const proc = spawn(localSyncServer, ['--sync-server', `0.0.0.0:${port}`], {
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
    child = proc;
    return () => {
        if (child) {
            child.kill();
            child = null;
        }
    };
}
