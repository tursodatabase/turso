import init, { Database, preopen, closeAll } from "../../pkg/turso_wasm.js";
import type { Todo, WorkerRequest, WorkerResponse } from "./types";

const DB_PATH = "todos.db";

let db: Database | null = null;

function escapeSql(value: string): string {
  return value.replace(/'/g, "''");
}

function rowId(value: unknown): number {
  return typeof value === "bigint" ? Number(value) : Number(value);
}

function listTodos(): Todo[] {
  if (!db) throw new Error("database not open");
  const rows = db.query(
    "SELECT id, title, done FROM todos ORDER BY id ASC",
  ) as Array<{ id: unknown; title: string; done: unknown }>;

  return rows.map((row) => ({
    id: rowId(row.id),
    title: row.title,
    done: rowId(row.done) !== 0,
  }));
}

async function openDatabase(): Promise<void> {
  await init();
  await preopen(DB_PATH);
  db = new Database(DB_PATH);
  db.exec(`
    CREATE TABLE IF NOT EXISTS todos (
      id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    )
  `);
}

function reply(message: WorkerResponse): void {
  postMessage(message);
}

self.onmessage = async ({ data }: MessageEvent<WorkerRequest>) => {
  const { id } = data;

  try {
    switch (data.type) {
      case "init":
        await openDatabase();
        reply({ type: "ready", id });
        break;

      case "list":
        reply({ type: "todos", id, todos: listTodos() });
        break;

      case "add":
        if (!db) throw new Error("database not open");
        db.exec(
          `INSERT INTO todos (title) VALUES ('${escapeSql(data.title)}')`,
        );
        reply({ type: "todos", id, todos: listTodos() });
        break;

      case "toggle":
        if (!db) throw new Error("database not open");
        db.exec(
          `UPDATE todos SET done = CASE WHEN done = 0 THEN 1 ELSE 0 END WHERE id = ${data.todoId}`,
        );
        reply({ type: "todos", id, todos: listTodos() });
        break;

      case "remove":
        if (!db) throw new Error("database not open");
        db.exec(`DELETE FROM todos WHERE id = ${data.todoId}`);
        reply({ type: "todos", id, todos: listTodos() });
        break;

      case "clearCompleted":
        if (!db) throw new Error("database not open");
        db.exec("DELETE FROM todos WHERE done != 0");
        reply({ type: "todos", id, todos: listTodos() });
        break;

      case "close":
        closeAll();
        db = null;
        reply({ type: "closed", id });
        break;
    }
  } catch (error) {
    const message =
      error instanceof Error
        ? `${error.message}\n${error.stack ?? ""}`
        : String(error);
    reply({ type: "error", id, message });
  }
};
