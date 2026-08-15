export interface Todo {
  id: number;
  title: string;
  done: boolean;
}

export type WorkerRequest =
  | { type: "init"; id: number }
  | { type: "list"; id: number }
  | { type: "add"; id: number; title: string }
  | { type: "toggle"; id: number; todoId: number }
  | { type: "remove"; id: number; todoId: number }
  | { type: "clearCompleted"; id: number }
  | { type: "close"; id: number };

export type WorkerResponse =
  | { type: "ready"; id: number }
  | { type: "todos"; id: number; todos: Todo[] }
  | { type: "closed"; id: number }
  | { type: "error"; id: number; message: string };
