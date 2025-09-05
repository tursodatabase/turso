export function makeQueue(capacityBytes = 1 << 20) {
    // Header: [writeIdx, readIdx] as 32-bit ints + data area
    const hdrBytes = 8;
    const sab = new SharedArrayBuffer(hdrBytes + capacityBytes);
    const hdr = new Int32Array(sab, 0, 2);
    const buf = new Uint8Array(sab, hdrBytes);
    hdr[0] = 0; hdr[1] = 0; // write, read
    return { sab, hdr, buf, cap: capacityBytes };
}

export function enqueue(hdr, buf, cap, src) {
    const w = Atomics.load(hdr, 0);
    const r = Atomics.load(hdr, 1);
    const free = (r + cap - w - 1) % cap;
    if (src.length + 4 > free) return false; // not enough space (4 bytes length)
    // write length (LE)
    buf[w % cap] = src.length & 255;
    buf[(w + 1) % cap] = (src.length >>> 8) & 255;
    buf[(w + 2) % cap] = (src.length >>> 16) & 255;
    buf[(w + 3) % cap] = (src.length >>> 24) & 255;
    let p = (w + 4) % cap;
    const n1 = Math.min(src.length, cap - p);
    buf.set(src.subarray(0, n1), p);
    buf.set(src.subarray(n1), 0);
    Atomics.store(hdr, 0, (w + 4 + src.length) % cap);
    Atomics.notify(hdr, 0, 1); // wake consumer waiting on write index
    return true;
}

var SAB;
var ID = 0;
export class DBWorker {
    constructor() {
        const { sab, hdr, buf, cap } = makeQueue(1 << 20);
        SAB = new Int32Array(sab);
        this.worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
        // console.info("!");
        this.worker.postMessage({ sab }); // share once
        // console.info("!");

        this._reqId = 0;
        this._pending = new Map();

        this.worker.onmessage = (ev) => {
            console.info('onmessage', Date.now(), performance.now(), ev);
            const { id, ok, result } = ev.data || {};
            const entry = this._pending.get(id);
            if (!entry) return;
            this._pending.delete(id);
            ok ? entry.resolve(result) : entry.reject(result);
        };

    }

    _call(action, ...args) {
        const id = this._reqId++;
        return new Promise((resolve, reject) => {
            // console.info('before', Date.now(), performance.now(), action);
            this._pending.set(id, { resolve, reject });
            this.worker.postMessage({ id, action, args });
            // console.info('sent', Date.now(), performance.now(), action);
            // console.info('posted');
            // console.info(this.worker, { id, action, args });
        });
    }


    // === API parity with your original exposed functions ===
    fireAndForget() {
        for (let i = 0; i < 1000; i++) {
            let id = this._reqId++;
            this.worker.postMessage({ id, action: 'test' });
        }
    }
    async atomics() {
        ID += 1;
        Atomics.store(SAB, 0, ID);
        Atomics.notify(SAB, 0, 1);
        while (true) {
            const { async, value } = Atomics.waitAsync(SAB, 1, ID - 1)
            if (async) {
                await value;
            } else {
                break;
            }
        }
    }
    pingAsync() { return this._call('pingAsync'); }
    // No truly "sync" calls to a worker; this is fire-and-forget to mimic your no-return sync fn
    pingSync() { return this._call('pingSync'); }

    connect(path) { return this._call('connect', path); }
    prepare(sql) { return this._call('prepare', sql); }
    pragma(source, options) { return this._call('pragma', source, options); }
    exec(sql) { return this._call('exec', sql); }
    defaultSafeIntegers(toggle) { return this._call('defaultSafeIntegers', toggle); }
    close() { return this._call('close'); }

    run(stmtId, bindParameters = []) { return this._call('run', stmtId, bindParameters); }
    all(stmtId, bindParameters = []) { return this._call('all', stmtId, bindParameters); }
    pluck(stmtId, pluckMode) { return this._call('pluck', stmtId, pluckMode); }
    safeIntegers(stmtId, toggle) { return this._call('safeIntegers', stmtId, toggle); }
    columns(stmtId) { return this._call('columns', stmtId); }
    get(stmtId, bindParameters = []) { return this._call('get', stmtId, bindParameters); }
    bind(stmtId, bindParameters = []) { return this._call('bind', stmtId, bindParameters); }

    terminate() { this.worker.terminate(); }
}
class Statement {
    constructor(worker, id) {
        this.worker = worker;
        this.id = id;
    }
    async run(...bindParameters) {
        return this.worker.run(this.id, bindParameters);
    }
    async all(...bindParameters) {
        return this.worker.all(this.id, bindParameters);
    }
    async pluck(pluckMode) {
        await this.worker.pluck(this.id, pluckMode);
    }
    async safeIntegers(toggle) {
        await this.worker.safeIntegers(this.id, toggle);
    }
    async columns() {
        return this.worker.columns(this.id);
    }
    async get(...bindParameters) {
        return this.worker.get(this.id, bindParameters);
    }
    async bind(...bindParameters) {
        await this.worker.bind(this.id, bindParameters);
    }
}


async function connect(path) {

    // Spin up the worker and connect
    const worker = new DBWorker();
    await worker.connect(path);

    return {
        async atomics() {
            await worker.atomics()
        },
        fireAndForget() {
            worker.fireAndForget()
        },
        async pingAsync() {
            console.info('ping async');
            await worker.pingAsync();
        },
        // fire-and-forget; `await`-ing is harmless (resolves immediately)
        async pingSync() {
            console.info('ping sync');
            await worker.pingSync();
        },
        async prepare(sql) {
            const id = await worker.prepare(sql);
            return new Statement(worker, id);
        },
        async pragma(source, options) {
            return worker.pragma(source, options);
        },
        async exec(sql) {
            await worker.exec(sql);
        },
        async defaultSafeIntegers(toggle) {
            await worker.defaultSafeIntegers(toggle);
        },
        async close() {
            await worker.close();
            worker.terminate();
        },
    };
}

export { connect, Statement };
