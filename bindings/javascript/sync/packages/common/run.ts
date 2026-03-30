"use strict";

import { GeneratorResponse, ProtocolIo, RunOpts } from "./types.js";
import { AsyncLock } from "@tursodatabase/database-common";

interface TrackPromise<T> {
    promise: Promise<T>,
    finished: boolean
}

function trackPromise<T>(p: Promise<T>): TrackPromise<T> {
    let status = { promise: null, finished: false };
    status.promise = p.finally(() => status.finished = true);
    return status;
}

function timeoutMs(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

function normalizeUrl(url: string): string {
    return url.replace(/^libsql:\/\//, 'https://');
}

async function process(opts: RunOpts, io: ProtocolIo, request: any) {
    const requestType = request.request();
    const completion = request.completion();
    if (requestType.type == 'Http') {
        let url: string | null = requestType.url;
        if (typeof opts.url == "function") {
            url = opts.url();
        } else {
            url = opts.url;
        }
        if (url == null) {
            completion.poison(`url is empty - sync is paused`);
            return;
        }
        url = normalizeUrl(url);
        try {
            let headers = typeof opts.headers === "function" ? await opts.headers() : opts.headers;
            if (requestType.headers != null && requestType.headers.length > 0) {
                headers = { ...headers };
                for (let header of requestType.headers) {
                    headers[header[0]] = header[1];
                }
            }
            const response = await fetch(`${url}${requestType.path}`, {
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
                completion.pushBuffer(value);
            }
        } catch (error) {
            completion.poison(`fetch error: ${error}`);
        }
    } else if (requestType.type == 'FullRead') {
        try {
            const metadata = await io.read(requestType.path);
            if (metadata != null) {
                completion.pushBuffer(metadata);
            }
            completion.done();
        } catch (error) {
            completion.poison(`metadata read error: ${error}`);
        }
    } else if (requestType.type == 'FullWrite') {
        try {
            await io.write(requestType.path, requestType.content);
            completion.done();
        } catch (error) {
            completion.poison(`metadata write error: ${error}`);
        }
    } else if (requestType.type == 'Transform') {
        if (opts.transform == null) {
            completion.poison("transform is not set");
            return;
        }
        const results = [];
        for (const mutation of requestType.mutations) {
            const result = opts.transform(mutation);
            if (result == null) {
                results.push({ type: 'Keep' });
            } else if (result.operation == 'skip') {
                results.push({ type: 'Skip' });
            } else if (result.operation == 'rewrite') {
                results.push({ type: 'Rewrite', stmt: result.stmt });
            } else {
                completion.poison("unexpected transform operation");
                return;
            }
        }
        completion.pushTransform(results);
        completion.done();
    }
}

export function memoryIO(): ProtocolIo {
    let values = new Map();
    return {
        async read(path: string): Promise<Buffer | Uint8Array | null> {
            return values.get(path);
        },
        async write(path: string, data: Buffer | Uint8Array): Promise<void> {
            values.set(path, data);
        }
    }
};

export interface Runner {
    wait(): Promise<void>;
}

export function runner(opts: RunOpts, io: ProtocolIo, engine: any): Runner {
    let tasks = [];
    return {
        async wait() {
            for (let request = engine.protocolIo(); request != null; request = engine.protocolIo()) {
                tasks.push(trackPromise(process(opts, io, request)));
            }
            const tasksRace = tasks.length == 0 ? Promise.resolve() : Promise.race([timeoutMs(opts.preemptionMs), ...tasks.map(t => t.promise)]);
            await Promise.all([engine.ioLoopAsync(), tasksRace]);

            tasks = tasks.filter(t => !t.finished);

            engine.protocolIoStep();
        },
    }
}

export async function run(runner: Runner, generator: any): Promise<any> {
    while (true) {
        const { type, ...rest }: GeneratorResponse = await generator.resumeAsync(null);
        if (type == 'Done') {
            return null;
        }
        if (type == 'SyncEngineStats') {
            return rest;
        }
        if (type == 'SyncEngineChanges') {
            //@ts-ignore
            return rest.changes;
        }
        await runner.wait();
    }
}

export class SyncEngineGuards {
    waitLock: AsyncLock;
    pushLock: AsyncLock;
    pullLock: AsyncLock;
    checkpointLock: AsyncLock;
    constructor() {
        this.waitLock = new AsyncLock();
        this.pushLock = new AsyncLock();
        this.pullLock = new AsyncLock();
        this.checkpointLock = new AsyncLock();
    }
    async wait(f: () => Promise<any>): Promise<any> {
        try {
            await this.waitLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
        }
    }
    async push(f: () => Promise<void>) {
        try {
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
    async apply(f: () => Promise<void>) {
        try {
            await this.waitLock.acquire();
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
    async checkpoint(f: () => Promise<void>) {
        try {
            await this.waitLock.acquire();
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
}