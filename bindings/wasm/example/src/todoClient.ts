import type { Todo, WorkerRequest, WorkerResponse } from "./types";

type Pending = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
};

export class TodoClient {
  readonly #worker: Worker;
  readonly #pending = new Map<number, Pending>();
  #nextId = 0;
  #ready: Promise<void>;

  constructor() {
    this.#worker = new Worker(new URL("./worker.ts", import.meta.url), {
      type: "module",
    });

    this.#worker.onmessage = ({ data }: MessageEvent<WorkerResponse>) => {
      const pending = this.#pending.get(data.id);
      if (!pending) return;
      this.#pending.delete(data.id);

      if (data.type === "error") {
        pending.reject(new Error(data.message));
        return;
      }

      pending.resolve(data);
    };

    this.#ready = this.#request<void>("init").then(() => undefined);
  }

  async list(): Promise<Todo[]> {
    await this.#ready;
    const response = await this.#request<{ todos: Todo[] }>("list");
    return response.todos;
  }

  async add(title: string): Promise<Todo[]> {
    await this.#ready;
    const response = await this.#request<{ todos: Todo[] }>("add", { title });
    return response.todos;
  }

  async toggle(todoId: number): Promise<Todo[]> {
    await this.#ready;
    const response = await this.#request<{ todos: Todo[] }>("toggle", {
      todoId,
    });
    return response.todos;
  }

  async remove(todoId: number): Promise<Todo[]> {
    await this.#ready;
    const response = await this.#request<{ todos: Todo[] }>("remove", {
      todoId,
    });
    return response.todos;
  }

  async clearCompleted(): Promise<Todo[]> {
    await this.#ready;
    const response = await this.#request<{ todos: Todo[] }>("clearCompleted");
    return response.todos;
  }

  async close(): Promise<void> {
    await this.#ready;
    await this.#request<void>("close");
    this.#worker.terminate();
  }

  #request<T>(
    type: WorkerRequest["type"],
    fields?: Record<string, unknown>,
  ): Promise<T> {
    const id = ++this.#nextId;
    const message = { type, id, ...fields } as WorkerRequest;

    return new Promise<T>((resolve, reject) => {
      this.#pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject,
      });
      this.#worker.postMessage(message);
    });
  }
}
