"use strict";

import { GeneratorResponse, ProtocolIo, RunOpts } from "./types.js";

const GENERATOR_RESUME_IO = 0;
const GENERATOR_RESUME_DONE = 1;

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

async function process(opts: RunOpts, io: ProtocolIo, request: any) {
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


export async function run(opts: RunOpts, io: ProtocolIo, engine: any, generator: any): Promise<any> {
    let tasks = [];
    while (true) {
        const { type, ...rest }: GeneratorResponse = await generator.resumeAsync(null);
        if (type == 'Done') {
            return null;
        }
        if (type == 'SyncEngineStats') {
            return rest;
        }
        for (let request = engine.protocolIo(); request != null; request = engine.protocolIo()) {
            tasks.push(trackPromise(process(opts, io, request)));
        }

        const tasksRace = tasks.length == 0 ? Promise.resolve() : Promise.race([timeoutMs(opts.preemptionMs), ...tasks.map(t => t.promise)]);
        await Promise.all([engine.ioLoopAsync(), tasksRace]);

        tasks = tasks.filter(t => !t.finished);
    }
    return generator.take();
}