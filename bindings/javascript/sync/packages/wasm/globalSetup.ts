import { spawn, ChildProcess, execSync } from "node:child_process";

let server: ChildProcess | null = null;
const PORT = 10001;

export async function setup() {
    const binaryPath = process.env.LOCAL_SYNC_SERVER;
    if (!binaryPath) {
        throw new Error('LOCAL_SYNC_SERVER environment variable must be set');
    }

    // Kill any existing process on this port
    try {
        execSync(`fuser -k ${PORT}/tcp 2>/dev/null || true`, { stdio: 'ignore' });
        await new Promise(resolve => setTimeout(resolve, 500));
    } catch (e) {
        // Ignore errors
    }

    return new Promise<void>((resolve, reject) => {
        server = spawn(binaryPath, ['--sync-server', `0.0.0.0:${PORT}`], {
            stdio: ['ignore', 'pipe', 'pipe'],
        });

        server.on('error', (err) => {
            reject(new Error(`Failed to start sync server: ${err.message}`));
        });

        server.stderr?.on('data', (data) => {
            const msg = data.toString();
            if (msg.includes('error') || msg.includes('Error')) {
                console.error('Server stderr:', msg);
            }
        });

        const startTime = Date.now();
        const timeout = 10000;

        const checkReady = async () => {
            try {
                await fetch(`http://localhost:${PORT}`);
                console.log(`Sync server started on http://localhost:${PORT}`);
                resolve();
            } catch (e) {
                if (Date.now() - startTime > timeout) {
                    reject(new Error('Timeout waiting for sync server to start'));
                } else {
                    setTimeout(checkReady, 50);
                }
            }
        };

        setTimeout(checkReady, 100);
    });
}

export async function teardown() {
    if (server) {
        server.kill('SIGTERM');
        await new Promise<void>((resolve) => {
            server?.on('close', () => resolve());
            setTimeout(resolve, 1000);
        });
        server = null;
    }
}
