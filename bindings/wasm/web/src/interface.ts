interface Row {
	[key: string]: string | number | boolean | null;
}

interface Database {
	init(path: string): Promise<void>;
	exec(sql: string): Promise<void>;
	prepare(sql: string): Promise<Statement>;
	close(): void;
}

interface Statement {
	get(): Promise<Row | undefined>;
	all(): Promise<Row[]>;
}

interface WorkerMessage {
	error?: string;
	result?: unknown;
}

export class DatabaseClient implements Database {
	private worker: Worker;
	private initialized = false;

	constructor() {
		//this.worker = new Worker(
		//	new URL("./limbo-worker.js", import.meta.url),
		//	{ type: "module" },
		//);
		this.worker = new Worker(
			new URL(
				"limbo-wasm/limbo-worker.js",
				import.meta.url,
			),
			{ type: "module" },
		);
	}

	async init(path: string): Promise<void> {
		await this.call("createDb", path);
		this.initialized = true;
	}

	private ensureInitialized() {
		if (!this.initialized) {
			throw new Error(
				"Database not initialized. Call init() first",
			);
		}
	}

	async exec(sql: string): Promise<void> {
		this.ensureInitialized();
		return await this.call("exec", sql) as Promise<void>;
	}

	async prepare(sql: string): Promise<Statement> {
		this.ensureInitialized();
		await this.call("prepare", sql);
		return new StatementClient(this.worker);
	}

	close(): void {
		this.ensureInitialized();
		this.worker.terminate();
	}

	private call(fn: string, ...args: unknown[]): Promise<unknown> {
		return new Promise((resolve, reject) => {
			this.worker.onmessage = (
				e: MessageEvent<WorkerMessage>,
			) => {
				if (e.data.error) {
					reject(new Error(e.data.error));
				} else resolve(e.data.result);
			};
			this.worker.postMessage({ fn, args });
		});
	}
}

class StatementClient implements Statement {
	constructor(private worker: Worker) { }

	get(): Promise<Row | undefined> {
		return this.call("get") as Promise<Row | undefined>;
	}

	all(): Promise<Row[]> {
		return this.call("all") as Promise<Row[]>;
	}

	private call(fn: string, ...args: unknown[]): Promise<unknown> {
		return new Promise((resolve, reject) => {
			this.worker.onmessage = (
				e: MessageEvent<WorkerMessage>,
			) => {
				if (e.data.error) {
					reject(new Error(e.data.error));
				} else resolve(e.data.result as any);
			};
			this.worker.postMessage({ fn, args });
		});
	}
}
