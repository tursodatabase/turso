import { spawn, Thread, Worker } from "threads"

class Statement {
    constructor(worker, id) {
        this.worker = worker;
        this.id = id;
    }
    async run(...bindParameters) {
        return await this.worker.run(this.id, bindParameters);
    }
    async all(...bindParameters) {
        return await this.worker.all(this.id, bindParameters);
    }
    async pluck(pluckMode) {
        await this.worker.run(this.id, pluckMode);
    }
    async safeIntegers(toggle) {
        await this.worker.safeIntegers(this.id, toggle);
    }
    async columns() {
        return await this.worker.columns(this.id);
    }
    async get(...bindParameters) {
        return await this.worker.get(this.id, bindParameters);
    }
    async bind(...bindParameters) {
        await this.worker.bind(this.id, bindParameters);
    }
}

async function connect(path) {
    let w = new Worker(new URL('./worker.js', import.meta.url), { type: "module" });
    const worker = await spawn(w);
    await worker.connect(path);

    return {
        async prepare(sql) {
            return new Statement(worker, await worker.prepare(sql));
        },
        async pragma(source, options) {
            return await worker.pragma(source, options);
        },
        async exec(sql) {
            await worker.exec(sql);
        },
        async defaultSafeIntegers(toggle) {
            await worker.defaultSafeIntegers(toggle);
        },
        async close() {
            await worker.close();
        },
    };
}

export { connect, Statement };
