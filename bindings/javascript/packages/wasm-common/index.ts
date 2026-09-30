import {
    createOnMessage as __wasmCreateOnMessageForFsProxy,
    getDefaultContext as __emnapiGetDefaultContext,
    instantiateNapiModule as __emnapiInstantiateNapiModule,
    WASI as __WASI,
    instantiateNapiModuleSync,
    MessageHandler
} from '@napi-rs/wasm-runtime'

function getUint8ArrayFromMemory(memory: WebAssembly.Memory, ptr: number, len: number): Uint8Array {
    ptr = ptr >>> 0;
    return new Uint8Array(memory.buffer).subarray(ptr, ptr + len);
}

function getStringFromMemory(memory: WebAssembly.Memory, ptr: number, len: number): string {
    const shared = getUint8ArrayFromMemory(memory, ptr, len);
    const copy = new Uint8Array(shared.length);
    copy.set(shared);
    const decoder = new TextDecoder('utf-8');
    return decoder.decode(copy);
}

interface BrowserImports {
    is_web_worker(): boolean;
    lookup_file(ptr: number, len: number): number;
    read(handle: number, ptr: number, len: number, offset: number): number;
    read_async(handle: number, ptr: number, len: number, offset: number, c: number);
    write(handle: number, ptr: number, len: number, offset: number): number;
    write_async(handle: number, ptr: number, len: number, offset: number, c: number);
    sync(handle: number): number;
    sync_async(handle: number, c: number);
    truncate(handle: number, len: number): number;
    truncate_async(handle: number, len: number, c: number);
    size(handle: number): number;
}

function panicMain(name): never {
    throw new Error(`method ${name} must be invoked only from the worker thread`);
}

function panicWorker(name): never {
    throw new Error(`method ${name} must be invoked only from the main thread`);
}

function mainImports(worker: Worker, completeOpfs: (c: any, r: any) => void): BrowserImports {
    return {
        is_web_worker(): boolean {
            return false;
        },
        write_async(handle, ptr, len, offset, c) {
            writeFileAtWorker(worker, handle, ptr, len, offset)
                .then(result => {
                    completeOpfs(c, result);
                }, err => {
                    console.error('write_async', err);
                    completeOpfs(c, -1);
                });
        },
        sync_async(handle, c) {
            syncFileAtWorker(worker, handle)
                .then(result => {
                    completeOpfs(c, result);
                }, err => {
                    console.error('sync_async', err);
                    completeOpfs(c, -1);
                });
        },
        read_async(handle, ptr, len, offset, c) {
            readFileAtWorker(worker, handle, ptr, len, offset)
                .then(result => {
                    completeOpfs(c, result);
                }, err => {
                    console.error('read_async', err);
                    completeOpfs(c, -1);
                });
        },
        truncate_async(handle, len, c) {
            truncateFileAtWorker(worker, handle, len)
                .then(result => {
                    completeOpfs(c, result);
                }, err => {
                    console.error('truncate_async', err);
                    completeOpfs(c, -1);
                });
        },
        lookup_file(ptr, len): number {
            panicMain("lookup_file")
        },
        read(handle, ptr, len, offset): number {
            panicMain("read")
        },
        write(handle, ptr, len, offset): number {
            panicMain("write")
        },
        sync(handle): number {
            panicMain("sync")
        },
        truncate(handle, len): number {
            panicMain("truncate")
        },
        size(handle): number {
            panicMain("size")
        }
    };
};

function workerImports(opfs: OpfsDirectory, memory: WebAssembly.Memory): BrowserImports {
    return {
        is_web_worker(): boolean {
            return true;
        },
        lookup_file(ptr, len): number {
            try {
                const handle = opfs.lookupFileHandle(getStringFromMemory(memory, ptr, len));
                return handle == null ? -404 : handle;
            } catch (e) {
                return -1;
            }
        },
        read(handle, ptr, len, offset): number {
            try {
                return opfs.read(handle, getUint8ArrayFromMemory(memory, ptr, len), offset);
            } catch (e) {
                return -1;
            }
        },
        write(handle, ptr, len, offset): number {
            try {
                return opfs.write(handle, getUint8ArrayFromMemory(memory, ptr, len), offset)
            } catch (e) {
                return -1;
            }
        },
        sync(handle): number {
            try {
                return opfs.sync(handle);
            } catch (e) {
                return -1;
            }
        },
        truncate(handle, len): number {
            try {
                opfs.truncate(handle, len);
                return 0;
            } catch (e) {
                return -1;
            }
        },
        size(handle): number {
            try {
                return opfs.size(handle);
            } catch (e) {
                return -1;
            }
        },
        read_async(handle, ptr, len, offset, completion) {
            panicWorker("read_async")
        },
        write_async(handle, ptr, len, offset, completion) {
            panicWorker("write_async")
        },
        sync_async(handle, completion) {
            panicWorker("sync_async")
        },
        truncate_async(handle, len, c) {
            panicWorker("truncate_async")
        },
    }
}

class OpfsDirectory {
    fileByPath: Map<String, { handle: number, sync: FileSystemSyncAccessHandle }>;
    fileByHandle: Map<number, FileSystemSyncAccessHandle>;
    fileHandleNo: number;

    constructor() {
        this.fileByPath = new Map();
        this.fileByHandle = new Map();
        this.fileHandleNo = 0;
    }

    async registerFile(path: string) {
        if (this.fileByPath.has(path)) {
            return;
        }
        const opfsRoot = await navigator.storage.getDirectory();
        const opfsHandle = await opfsRoot.getFileHandle(path, { create: true });
        const opfsSync = await opfsHandle.createSyncAccessHandle();
        this.fileHandleNo += 1;
        this.fileByPath.set(path, { handle: this.fileHandleNo, sync: opfsSync });
        this.fileByHandle.set(this.fileHandleNo, opfsSync);
    }

    async unregisterFile(path: string) {
        const file = this.fileByPath.get(path);
        if (file == null) {
            return;
        }
        this.fileByPath.delete(path);
        this.fileByHandle.delete(file.handle);
        file.sync.close();
    }
    lookupFileHandle(path: string): number | null {
        try {
            const file = this.fileByPath.get(path);
            if (file == null) {
                return null;
            }
            return file.handle;
        } catch (e) {
            console.error('lookupFile', path, e);
            throw e;
        }
    }
    read(handle: number, buffer: Uint8Array, offset: number): number {
        try {
            const file = this.fileByHandle.get(handle);
            const result = file.read(buffer, { at: Number(offset) });
            return result;
        } catch (e) {
            console.error('read', handle, buffer.length, offset, e);
            throw e;
        }
    }
    write(handle: number, buffer: Uint8Array, offset: number): number {
        try {
            const file = this.fileByHandle.get(handle);
            const result = file.write(buffer, { at: Number(offset) });
            return result;
        } catch (e) {
            console.error('write', handle, buffer.length, offset, e);
            throw e;
        }
    }
    sync(handle: number): number {
        try {
            const file = this.fileByHandle.get(handle);
            file.flush();
            return 0;
        } catch (e) {
            console.error('sync', handle, e);
            throw e;
        }
    }
    truncate(handle: number, size: number) {
        try {
            const file = this.fileByHandle.get(handle);
            file.truncate(size);
            return 0;
        } catch (e) {
            console.error('truncate', handle, size, e);
            throw e;
        }
    }
    size(handle: number): number {
        try {
            const file = this.fileByHandle.get(handle);
            const size = file.getSize()
            return size;
        } catch (e) {
            console.error('size', handle, e);
            throw e;
        }
    }
}

let workerRequestId = 0;
function waitForWorkerResponse(worker: Worker, id: number, timeoutMs: number = 30_000): Promise<any> {
    let waitResolve, waitReject;
    let settled = false;
    const callback = msg => {
        if (msg.data.__turso__ && msg.data.id == id) {
            settled = true;
            clearTimeout(stallTimer);
            if (msg.data.error != null) {
                waitReject(msg.data.error)
            } else {
                waitResolve(msg.data.result)
            }
            cleanup();
        }
    };
    const cleanup = () => worker.removeEventListener("message", callback);
    const stallTimer = setTimeout(() => {
        if (!settled) {
            settled = true;
            cleanup();
            waitReject(new Error(`[turso:worker] request id=${id} timed out after ${timeoutMs}ms`));
        }
    }, timeoutMs);

    worker.addEventListener("message", callback);
    const result = new Promise((resolve, reject) => {
        waitResolve = resolve;
        waitReject = reject;
    });
    return result;
}

function readFileAtWorker(worker: Worker, handle: number, ptr: number, len: number, offset: number) {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "read_async", handle: handle, ptr: ptr, len: len, offset: offset, id: currentId });
    return promise;
}

function writeFileAtWorker(worker: Worker, handle: number, ptr: number, len: number, offset: number) {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "write_async", handle: handle, ptr: ptr, len: len, offset: offset, id: currentId });
    return promise;
}

function syncFileAtWorker(worker: Worker, handle: number) {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "sync_async", handle: handle, id: currentId });
    return promise;
}

function truncateFileAtWorker(worker: Worker, handle: number, len: number) {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "truncate_async", handle: handle, len: len, id: currentId });
    return promise;
}

function registerFileAtWorker(worker: Worker, path: string): Promise<void> {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "register", path: path, id: currentId });
    return promise;
}

function unregisterFileAtWorker(worker: Worker, path: string): Promise<void> {
    workerRequestId += 1;
    const currentId = workerRequestId;
    const promise = waitForWorkerResponse(worker, currentId);
    worker.postMessage({ __turso__: "unregister", path: path, id: currentId });
    return promise;
}

function isWebWorker(): boolean {
    return typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope;
}

function setupWebWorker() {
    let opfs = new OpfsDirectory();
    let memory = null;

    const handler = new MessageHandler({
        onLoad({ wasmModule, wasmMemory }) {
            memory = wasmMemory;
            const wasi = new __WASI({
                print: function () {
                    // eslint-disable-next-line no-console
                    console.log.apply(console, arguments)
                },
                printErr: function () {
                    // eslint-disable-next-line no-console
                    console.error.apply(console, arguments)
                },
            })
            return instantiateNapiModuleSync(wasmModule, {
                childThread: true,
                wasi,
                overwriteImports(importObject) {
                    importObject.env = {
                        ...importObject.env,
                        ...importObject.napi,
                        ...importObject.emnapi,
                        ...workerImports(opfs, memory),
                        memory: wasmMemory,
                    }
                },
            })
        },
    })

    globalThis.onmessage = async function (e) {
        if (e.data.__turso__ == 'register') {
            try {
                await opfs.registerFile(e.data.path);
                self.postMessage({ __turso__: true, id: e.data.id });
            } catch (error) {
                self.postMessage({ __turso__: true, id: e.data.id, error: error });
            }
            return;
        } else if (e.data.__turso__ == 'unregister') {
            try {
                await opfs.unregisterFile(e.data.path);
                self.postMessage({ __turso__: true, id: e.data.id });
            } catch (error) {
                self.postMessage({ __turso__: true, id: e.data.id, error: error });
            }
            return;
        } else if (e.data.__turso__ == 'read_async') {
            let result = opfs.read(e.data.handle, getUint8ArrayFromMemory(memory, e.data.ptr, e.data.len), e.data.offset);
            self.postMessage({ __turso__: true, id: e.data.id, result: result });
        } else if (e.data.__turso__ == 'write_async') {
            let result = opfs.write(e.data.handle, getUint8ArrayFromMemory(memory, e.data.ptr, e.data.len), e.data.offset);
            self.postMessage({ __turso__: true, id: e.data.id, result: result });
        } else if (e.data.__turso__ == 'sync_async') {
            let result = opfs.sync(e.data.handle);
            self.postMessage({ __turso__: true, id: e.data.id, result: result });
        } else if (e.data.__turso__ == 'truncate_async') {
            let result = opfs.truncate(e.data.handle, e.data.len);
            self.postMessage({ __turso__: true, id: e.data.id, result: result });
        }
        handler.handle(e)
    }
}

async function setupMainThread(wasmFile: ArrayBuffer, factory: () => Worker): Promise<any> {
    const worker = factory();
    let completeOpfs = null;
    const __emnapiContext = __emnapiGetDefaultContext()
    const __wasi = new __WASI({
        version: 'preview1',
    })
    const __sharedMemory = new WebAssembly.Memory({
        initial: 4000,
        maximum: 65536,
        shared: true,
    })
    const {
        instance: __napiInstance,
        module: __wasiModule,
        napiModule: __napiModule,
    } = await __emnapiInstantiateNapiModule(wasmFile, {
        context: __emnapiContext,
        asyncWorkPoolSize: 1,
        wasi: __wasi,
        onCreateWorker() { return worker; },
        overwriteImports(importObject) {
            importObject.env = {
                ...importObject.env,
                ...importObject.napi,
                ...importObject.emnapi,
                ...mainImports(worker, (c, res) => completeOpfs(c, res)),
                memory: __sharedMemory,
            }
            return importObject
        },
        beforeInit({ instance }) {
            for (const name of Object.keys(instance.exports)) {
                if (name.startsWith('__napi_register__')) {
                    instance.exports[name]()
                }
            }
        },
    });
    const nativeCompleteOpfs = __napiModule.exports.completeOpfs;
    completeOpfs = (c, res) => {
        nativeCompleteOpfs(c, res);
        ioNotifier.notify();
    };
    return __napiModule;
}

class IONotifier {
    private waiters: Array<() => void> = [];

    waitForCompletion(): Promise<void> {
        return new Promise(resolve => {
            this.waiters.push(resolve);
        });
    }

    notify() {
        const waiters = this.waiters;
        this.waiters = [];
        for (const resolve of waiters) {
            resolve();
        }
    }
}

const ioNotifier = new IONotifier();

export { OpfsDirectory, workerImports, mainImports as MainDummyImports, waitForWorkerResponse, registerFileAtWorker, unregisterFileAtWorker, isWebWorker, setupWebWorker, setupMainThread, ioNotifier }