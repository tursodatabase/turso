/**
 * UDF (User Defined Function) manager for the browser/edge WASM runtime.
 *
 * Compiles and instantiates WASM modules using the host's native WebAssembly
 * engine, then exposes a handle-based API that Rust calls through imported
 * "env" functions.
 */

function getUint8ArrayFromMemory(memory: WebAssembly.Memory, ptr: number, len: number): Uint8Array {
    ptr = ptr >>> 0;
    return new Uint8Array(memory.buffer).subarray(ptr, ptr + len);
}

function getStringFromMemory(memory: WebAssembly.Memory, ptr: number, len: number): string {
    const shared = getUint8ArrayFromMemory(memory, ptr, len);
    const copy = new Uint8Array(shared.length);
    copy.set(shared);
    return new TextDecoder('utf-8').decode(copy);
}

function setInt32InMemory(memory: WebAssembly.Memory, ptr: number, value: number): void {
    new DataView(memory.buffer).setInt32(ptr, value, true);
}

export class UdfManager {
    private modules = new Map<number, WebAssembly.Module>();
    private instances = new Map<number, {
        instance: WebAssembly.Instance;
        func: Function;
        malloc: Function;
        memory: WebAssembly.Memory;
    }>();
    private nextHandle = 1;

    compileModule(tursoMemory: WebAssembly.Memory, ptr: number, len: number): number {
        try {
            const bytes = getUint8ArrayFromMemory(tursoMemory, ptr, len);
            // Copy because memory.buffer may be a SharedArrayBuffer
            const copy = new Uint8Array(bytes);
            const module = new WebAssembly.Module(copy);
            const handle = this.nextHandle++;
            this.modules.set(handle, module);
            return handle;
        } catch (e) {
            console.error('udf_compile_module', e);
            return -1;
        }
    }

    destroyModule(handle: number): void {
        this.modules.delete(handle);
    }

    instantiate(
        moduleHandle: number,
        tursoMemory: WebAssembly.Memory,
        exportPtr: number,
        exportLen: number,
    ): number {
        try {
            const module = this.modules.get(moduleHandle);
            if (!module) return -1;
            const exportName = getStringFromMemory(tursoMemory, exportPtr, exportLen);
            const instance = new WebAssembly.Instance(module);
            const func = instance.exports[exportName] as Function;
            const malloc = instance.exports['turso_malloc'] as Function;
            const memory = instance.exports['memory'] as WebAssembly.Memory;
            if (!func || !memory) return -2;
            const handle = this.nextHandle++;
            this.instances.set(handle, { instance, func, malloc, memory });
            return handle;
        } catch (e) {
            console.error('udf_instantiate', e);
            return -1;
        }
    }

    destroyInstance(handle: number): void {
        this.instances.delete(handle);
    }

    writeMemory(
        instanceHandle: number,
        offset: number,
        tursoMemory: WebAssembly.Memory,
        srcPtr: number,
        len: number,
    ): number {
        try {
            const entry = this.instances.get(instanceHandle);
            if (!entry) return -1;
            const src = getUint8ArrayFromMemory(tursoMemory, srcPtr, len);
            // Copy src first since tursoMemory.buffer may be shared
            const copy = new Uint8Array(src);
            new Uint8Array(entry.memory.buffer).set(copy, offset);
            return 0;
        } catch (e) {
            console.error('udf_write_memory', e);
            return -1;
        }
    }

    readMemory(
        instanceHandle: number,
        offset: number,
        tursoMemory: WebAssembly.Memory,
        dstPtr: number,
        len: number,
    ): number {
        try {
            const entry = this.instances.get(instanceHandle);
            if (!entry) return -1;
            const src = new Uint8Array(entry.memory.buffer, offset, len);
            // Copy into Turso's linear memory
            const dst = getUint8ArrayFromMemory(tursoMemory, dstPtr, len);
            dst.set(src);
            return 0;
        } catch (e) {
            console.error('udf_read_memory', e);
            return -1;
        }
    }

    memorySize(instanceHandle: number): number {
        const entry = this.instances.get(instanceHandle);
        if (!entry) return 0;
        return entry.memory.buffer.byteLength;
    }

    malloc(instanceHandle: number, size: number): number {
        try {
            const entry = this.instances.get(instanceHandle);
            if (!entry || !entry.malloc) return -1;
            return entry.malloc(size) as number;
        } catch (e) {
            console.error('udf_malloc', e);
            return -1;
        }
    }

    call(
        instanceHandle: number,
        argc: number,
        argvPtr: number,
        tursoMemory: WebAssembly.Memory,
        resultHiPtr: number,
        resultLoPtr: number,
    ): void {
        try {
            const entry = this.instances.get(instanceHandle);
            if (!entry) {
                setInt32InMemory(tursoMemory, resultHiPtr, 0);
                setInt32InMemory(tursoMemory, resultLoPtr, 0);
                return;
            }
            const result = entry.func(argc, argvPtr) as number | bigint;
            let hi: number, lo: number;
            if (typeof result === 'bigint') {
                hi = Number((result >> 32n) & 0xFFFFFFFFn);
                lo = Number(result & 0xFFFFFFFFn);
            } else {
                // i32 return — sign-extend into i64
                hi = result < 0 ? -1 : 0;
                lo = result | 0;
            }
            setInt32InMemory(tursoMemory, resultHiPtr, hi);
            setInt32InMemory(tursoMemory, resultLoPtr, lo);
        } catch (e) {
            console.error('udf_call', e);
            setInt32InMemory(tursoMemory, resultHiPtr, 0);
            setInt32InMemory(tursoMemory, resultLoPtr, 0);
        }
    }
}

export function udfImports(tursoMemory: WebAssembly.Memory) {
    const mgr = new UdfManager();
    return {
        udf_compile_module: (ptr: number, len: number) =>
            mgr.compileModule(tursoMemory, ptr, len),
        udf_destroy_module: (handle: number) =>
            mgr.destroyModule(handle),
        udf_instantiate: (moduleHandle: number, exportPtr: number, exportLen: number) =>
            mgr.instantiate(moduleHandle, tursoMemory, exportPtr, exportLen),
        udf_destroy_instance: (handle: number) =>
            mgr.destroyInstance(handle),
        udf_write_memory: (instanceHandle: number, offset: number, srcPtr: number, len: number) =>
            mgr.writeMemory(instanceHandle, offset, tursoMemory, srcPtr, len),
        udf_read_memory: (instanceHandle: number, offset: number, dstPtr: number, len: number) =>
            mgr.readMemory(instanceHandle, offset, tursoMemory, dstPtr, len),
        udf_memory_size: (instanceHandle: number) =>
            mgr.memorySize(instanceHandle),
        udf_malloc: (instanceHandle: number, size: number) =>
            mgr.malloc(instanceHandle, size),
        udf_call: (instanceHandle: number, argc: number, argvPtr: number, resultHiPtr: number, resultLoPtr: number) =>
            mgr.call(instanceHandle, argc, argvPtr, tursoMemory, resultHiPtr, resultLoPtr),
    };
}
