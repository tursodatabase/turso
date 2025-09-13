import {
    createOnMessage as __wasmCreateOnMessageForFsProxy,
    getDefaultContext as __emnapiGetDefaultContext,
    instantiateNapiModule as __emnapiInstantiateNapiModule,
    WASI as __WASI,
    instantiateNapiModuleSync,
    MessageHandler
} from '@tursodatabase/wasm-runtime'

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
    write(handle: number, ptr: number, len: number, offset: number): number;
    sync(handle: number): number;
    truncate(handle: number, len: number): number;
    size(handle: number): number;
}

function panic(name): never {
    throw new Error(`method ${name} must be invoked only from the main thread`);
}

const MainDummyImports: BrowserImports = {
    is_web_worker: function (): boolean {
        return false;
    },
    lookup_file: function (ptr: number, len: number): number {
        panic("lookup_file")
    },
    read: function (handle: number, ptr: number, len: number, offset: number): number {
        panic("read")
    },
    write: function (handle: number, ptr: number, len: number, offset: number): number {
        panic("write")
    },
    sync: function (handle: number): number {
        panic("sync")
    },
    truncate: function (handle: number, len: number): number {
        panic("truncate")
    },
    size: function (handle: number): number {
        panic("size")
    }
};

function workerImports(opfs: OpfsDirectory, memory: WebAssembly.Memory): BrowserImports {
    return {
        is_web_worker: function (): boolean {
            return true;
        },
        lookup_file: function (ptr: number, len: number): number {
            try {
                const handle = opfs.lookupFileHandle(getStringFromMemory(memory, ptr, len));
                return handle == null ? -404 : handle;
            } catch (e) {
                return -1;
            }
        },
        read: function (handle: number, ptr: number, len: number, offset: number): number {
            try {
                return opfs.read(handle, getUint8ArrayFromMemory(memory, ptr, len), offset);
            } catch (e) {
                return -1;
            }
        },
        write: function (handle: number, ptr: number, len: number, offset: number): number {
            try {
                return opfs.write(handle, getUint8ArrayFromMemory(memory, ptr, len), offset)
            } catch (e) {
                return -1;
            }
        },
        sync: function (handle: number): number {
            try {
                opfs.sync(handle);
                return 0;
            } catch (e) {
                return -1;
            }
        },
        truncate: function (handle: number, len: number): number {
            try {
                opfs.truncate(handle, len);
                return 0;
            } catch (e) {
                return -1;
            }
        },
        size: function (handle: number): number {
            try {
                return opfs.size(handle);
            } catch (e) {
                return -1;
            }
        }
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
    sync(handle: number) {
        try {
            const file = this.fileByHandle.get(handle);
            file.flush();
        } catch (e) {
            console.error('sync', handle, e);
            throw e;
        }
    }
    truncate(handle: number, size: number) {
        try {
            const file = this.fileByHandle.get(handle);
            const result = file.truncate(size);
            return result;
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
function waitForWorkerResponse(worker: Worker, id: number): Promise<any> {
    let waitResolve, waitReject;
    const callback = msg => {
        if (msg.data.id == id) {
            if (msg.data.error != null) {
                waitReject(msg.data.error)
            } else {
                waitResolve()
            }
            cleanup();
        }
    };
    const cleanup = () => worker.removeEventListener("message", callback);

    worker.addEventListener("message", callback);
    const result = new Promise((resolve, reject) => {
        waitResolve = resolve;
        waitReject = reject;
    });
    return result;
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
                self.postMessage({ id: e.data.id });
            } catch (error) {
                self.postMessage({ id: e.data.id, error: error });
            }
            return;
        } else if (e.data.__turso__ == 'unregister') {
            try {
                await opfs.unregisterFile(e.data.path);
                self.postMessage({ id: e.data.id });
            } catch (error) {
                self.postMessage({ id: e.data.id, error: error });
            }
            return;
        }
        handler.handle(e)
    }
}

async function setupMainThread(wasmFile: ArrayBuffer, factory: () => Worker): Promise<any> {
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
        onCreateWorker() { return factory() },
        overwriteImports(importObject) {
            importObject.env = {
                ...importObject.env,
                ...importObject.napi,
                ...importObject.emnapi,
                ...MainDummyImports,
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
    })
    return __napiModule;
}

export { OpfsDirectory, workerImports, MainDummyImports, waitForWorkerResponse, registerFileAtWorker, unregisterFileAtWorker, isWebWorker, setupWebWorker, setupMainThread }