"use strict";

import { SyncEngine, DatabaseRowMutationJs, DatabaseRowStatementJs } from '#entry-point';
import { Database } from '@tursodatabase/database';

const GENERATOR_RESUME_IO = 0;
const GENERATOR_RESUME_DONE = 1;

function trackPromise<T>(p: Promise<T>): { promise: Promise<T>, finished: boolean } {
    let status = { promise: null, finished: false };
    status.promise = p.finally(() => status.finished = true);
    return status;
}

function timeoutMs(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms,))
}

async function read(opts, path: string): Promise<Buffer | Uint8Array | null> {
    if (opts.isMemory) {
        return opts.value;
    }
    if (typeof window === 'undefined') {
        const { promises } = await import('node:fs');
        try {
            return await promises.readFile(path);
        } catch (error) {
            if (error.code === 'ENOENT') {
                return null;
            }
            throw error;
        }
    } else {
        const data = localStorage.getItem(path);
        if (data != null) {
            return new TextEncoder().encode(data);
        } else {
            return null;
        }
    }
}

async function write(opts, path: string, content: number[]): Promise<void> {
    if (opts.isMemory) {
        opts.value = content;
        return;
    }
    const data = new Uint8Array(content);
    if (typeof window === 'undefined') {
        const { promises } = await import('node:fs');
        const unix = Math.floor(Date.now() / 1000);
        const nonce = Math.floor(Math.random() * 1000000000);
        const tmp = `${path}.tmp.${unix}.${nonce}`;
        await promises.writeFile(tmp, data);
        await promises.rename(tmp, path);
    } else {
        localStorage.setItem(path, new TextDecoder().decode(data));
    }
}

async function process(opts, request) {
    const requestType = request.request();
    const completion = request.completion();
    if (requestType.type == 'Http') {
        try {
            let headers = opts.headers;
            if (requestType.headers != null && requestType.headers.length > 0) {
                headers = { ...opts.headers };
                for (let header of requestType.headers) {
                    headers[header[0]] = header[1];
                }
            }
            const response = await fetch(`${opts.url}${requestType.path}`, {
                method: requestType.method,
                headers: headers,
                body: requestType.body != null ? new Uint8Array(requestType.body) : null,
            });
            completion.status(response.status);
            const reader = response.body.getReader();
            while (true) {
                const { done, value } = await reader.read();
                if (done) {
                    completion.done();
                    break;
                }
                completion.push(value);
            }
        } catch (error) {
            completion.poison(`fetch error: ${error}`);
        }
    } else if (requestType.type == 'FullRead') {
        try {
            const metadata = await read(opts.metadata, requestType.path);
            if (metadata != null) {
                completion.push(metadata);
            }
            completion.done();
        } catch (error) {
            completion.poison(`metadata read error: ${error}`);
        }
    } else if (requestType.type == 'FullWrite') {
        try {
            await write(opts.metadata, requestType.path, requestType.content);
            completion.done();
        } catch (error) {
            completion.poison(`metadata write error: ${error}`);
        }
    }
}

async function run(opts, engine, generator): Promise<any> {
    let tasks = [];
    while (generator.resume(null) !== GENERATOR_RESUME_DONE) {
        for (let request = engine.protocolIo(); request != null; request = engine.protocolIo()) {
            tasks.push(trackPromise(process(opts, request)));
        }

        const tasksRace = tasks.length == 0 ? Promise.resolve() : Promise.race([timeoutMs(opts.preemptionMs), ...tasks.map(t => t.promise)]);
        await Promise.all([engine.ioLoopAsync(), tasksRace]);

        tasks = tasks.filter(t => !t.finished);
    }
    return generator.take();
}

interface ConnectOpts {
    path: string;
    clientName?: string;
    url: string;
    authToken?: string;
    encryptionKey?: string;
    tablesIgnore?: string[],
    transform?: (arg: DatabaseRowMutationJs) => DatabaseRowStatementJs | null,
    enableTracing?: string,
}

interface Sync {
    sync(): Promise<void>;
    push(): Promise<void>;
    pull(): Promise<void>;
    checkpoint(): Promise<void>;
    stats(): Promise<{ operations: number, wal: number }>;
}

export async function connect(opts: ConnectOpts): Database & Sync {
    const engine = new SyncEngine({
        path: opts.path,
        clientName: opts.clientName,
        tablesIgnore: opts.tablesIgnore,
        transform: opts.transform,
        enableTracing: opts.enableTracing
    });
    const httpOpts = {
        url: opts.url,
        headers: {
            ...(opts.authToken != null && { "Authorization": `Bearer ${opts.authToken}` }),
            ...(opts.encryptionKey != null && { "x-turso-encryption-key": opts.encryptionKey })
        },
        metadata: opts.path == ':memory:' ? { isMemory: true, value: null } : { isMemory: false },
        preemptionMs: 1,
    };
    await run(httpOpts, engine, engine.init());
    const nativeDb = engine.open();
    const db = Database.create();
    db.initialize(nativeDb, opts.path, false);
    db.sync = async function () { await run(httpOpts, engine, engine.sync()); }
    db.pull = async function () { await run(httpOpts, engine, engine.pull()); }
    db.push = async function () { await run(httpOpts, engine, engine.push()); }
    db.checkpoint = async function () { await run(httpOpts, engine, engine.checkpoint()); }
    db.stats = async function () { return (await run(httpOpts, engine, engine.stats())); }
    return db;
}

export { Database, Sync };
