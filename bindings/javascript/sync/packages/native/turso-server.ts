import { spawn, type ChildProcess } from 'node:child_process';
import { request } from 'node:http';
import { createServer } from 'node:net';

const ADMIN_URL = 'http://localhost:8081';
const USER_URL = 'http://localhost:8080';

function randomStr(): string {
    return Math.random().toString(36).slice(2, 10);
}

function getFreePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = createServer();
        server.unref();
        server.on('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const port = (server.address() as { port: number }).port;
            server.close(() => resolve(port));
        });
    });
}

interface HttpResponse {
    status: number;
    body: Buffer;
}

function httpPost(target: string, host: string | undefined, contentType: string, body: string): Promise<HttpResponse> {
    const url = new URL(target);
    return new Promise((resolve, reject) => {
        const req = request({
            hostname: url.hostname,
            port: url.port || (url.protocol === 'https:' ? 443 : 80),
            method: 'POST',
            path: `${url.pathname}${url.search}`,
            headers: {
                'Content-Type': contentType,
                'Content-Length': Buffer.byteLength(body).toString(),
                ...(host ? { 'Host': host } : {}),
            },
        }, res => {
            const chunks: Buffer[] = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
            res.on('error', reject);
        });
        req.on('error', reject);
        req.write(body);
        req.end();
    });
}

function httpGet(target: string): Promise<HttpResponse> {
    const url = new URL(target);
    return new Promise((resolve, reject) => {
        const req = request({
            hostname: url.hostname,
            port: url.port || (url.protocol === 'https:' ? 443 : 80),
            method: 'GET',
            path: `${url.pathname}${url.search}`,
        }, res => {
            const chunks: Buffer[] = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
            res.on('error', reject);
        });
        req.on('error', reject);
        req.end();
    });
}

async function ensureOk(resp: HttpResponse) {
    if (resp.status === 200) return;
    const text = resp.body.toString('utf-8');
    if (resp.status === 400 && text.includes('already exists')) return;
    throw new Error(`http failed: ${resp.status} ${text}`);
}

export class TursoServer {
    private constructor(
        private readonly _userUrl: string,
        private readonly _dbUrl: string,
        private readonly _host: string,
        private readonly _server: ChildProcess | null,
    ) { }

    static async create(): Promise<TursoServer> {
        const localSyncServer = process.env.LOCAL_SYNC_SERVER;
        if (localSyncServer) {
            const maxAttempts = 5;
            let lastErr: unknown = null;
            for (let attempt = 0; attempt < maxAttempts; attempt++) {
                const port = await getFreePort();
                const child = spawn(localSyncServer, ['--sync-server', `0.0.0.0:${port}`], {
                    stdio: 'ignore',
                });
                const userUrl = `http://localhost:${port}`;
                const deadline = Date.now() + 30000;
                let ready = false;
                while (Date.now() < deadline) {
                    if (child.exitCode !== null) break;
                    try {
                        await httpGet(userUrl);
                        ready = true;
                        break;
                    } catch {
                        await new Promise(r => setTimeout(r, 100));
                    }
                }
                if (ready) {
                    return new TursoServer(userUrl, userUrl, '', child);
                }
                child.kill();
                lastErr = new Error(`sync server did not become available within 30s (port ${port})`);
            }
            throw lastErr ?? new Error('failed to start local sync server');
        }
        const name = randomStr();
        const tokens = USER_URL.split('://');
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}`, undefined, 'application/json', ''));
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}/groups/${name}`, undefined, 'application/json', ''));
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}/groups/${name}/databases/${name}`, undefined, 'application/json', ''));
        return new TursoServer(
            USER_URL,
            `${tokens[0]}://${name}--${name}--${name}.${tokens[1]}`,
            `${name}--${name}--${name}.localhost`,
            null,
        );
    }

    dbUrl(): string {
        return this._dbUrl;
    }

    async dbSql(sql: string): Promise<unknown[][]> {
        const resp = await httpPost(
            `${this._userUrl}/v2/pipeline`,
            this._host || undefined,
            'application/json',
            JSON.stringify({ requests: [{ type: 'execute', stmt: { sql } }] }),
        );
        if (resp.status !== 200) {
            throw new Error(`http failed: ${resp.status} ${resp.body.toString('utf-8')}`);
        }
        const result = JSON.parse(resp.body.toString('utf-8'));
        if (result.results[0].type !== 'ok') {
            throw new Error(`remote sql execution failed: ${JSON.stringify(result)}`);
        }
        return result.results[0].response.result.rows.map((row: { value: unknown }[]) => row.map(cell => cell.value));
    }

    close(): void {
        if (this._server) this._server.kill();
    }
}
