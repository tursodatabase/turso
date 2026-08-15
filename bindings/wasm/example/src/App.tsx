import { createSignal, onCleanup, onMount, Show } from "solid-js";
import { TodoClient } from "./todoClient";
import type { Todo } from "./types";

export function App() {
  const [todos, setTodos] = createSignal<Todo[]>([]);
  const [draft, setDraft] = createSignal("");
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  let client: TodoClient | null = null;

  onMount(async () => {
    try {
      client = new TodoClient();
      setTodos(await client.list());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  });

  onCleanup(() => {
    client?.close().catch(() => undefined);
  });

  async function run(action: () => Promise<Todo[]>) {
    if (!client) return;
    setError(null);
    try {
      setTodos(await action());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function addTodo() {
    const title = draft().trim();
    if (!title || !client) return;
    setDraft("");
    await run(() => client!.add(title));
  }

  const completedCount = () => todos().filter((todo) => todo.done).length;
  const openCount = () => todos().length - completedCount();

  return (
    <main class="mx-auto max-w-2xl px-4 py-10 sm:px-6">
      <header class="mb-8">
        <p class="text-sm font-medium uppercase tracking-wide text-indigo-600 dark:text-indigo-400">
          Turso in the browser
        </p>
        <h1 class="mt-2 text-3xl font-semibold tracking-tight">Todos</h1>
        <p class="mt-2 text-slate-600 dark:text-slate-400">
          Stored in OPFS through a Web Worker. Reload the page and your list
          stays.
        </p>
      </header>

      <Show when={error()}>
        <div
          class="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200"
          role="alert"
        >
          {error()}
        </div>
      </Show>

      <section
        class="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-900"
      >
        <form
          class="flex gap-2 border-b border-slate-200 p-4 dark:border-slate-800"
          onSubmit={(event) => {
            event.preventDefault();
            addTodo();
          }}
        >
          <input
            class="min-w-0 flex-1 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none ring-indigo-500 focus:ring-2 dark:border-slate-700 dark:bg-slate-950"
            disabled={loading()}
            placeholder="What needs doing?"
            type="text"
            value={draft()}
            onInput={(event) => setDraft(event.currentTarget.value)}
          />
          <button
            class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-indigo-500 disabled:opacity-50"
            disabled={loading() || draft().trim().length === 0}
            type="submit"
          >
            Add
          </button>
        </form>

        <Show
          when={!loading()}
          fallback={
            <p class="px-4 py-8 text-center text-sm text-slate-500">
              Opening database…
            </p>
          }
        >
          <Show
            when={todos().length > 0}
            fallback={
              <p class="px-4 py-8 text-center text-sm text-slate-500">
                No todos yet. Add one above.
              </p>
            }
          >
            <ul class="divide-y divide-slate-200 dark:divide-slate-800">
              {todos().map((todo) => (
                <li class="flex items-center gap-3 px-4 py-3">
                  <input
                    class="size-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                    checked={todo.done}
                    id={`todo-${todo.id}`}
                    type="checkbox"
                    onChange={() =>
                      run(() => client!.toggle(todo.id))
                    }
                  />
                  <label
                    class={`min-w-0 flex-1 text-sm ${
                      todo.done
                        ? "text-slate-400 line-through"
                        : "text-slate-800 dark:text-slate-100"
                    }`}
                    for={`todo-${todo.id}`}
                  >
                    {todo.title}
                  </label>
                  <button
                    class="rounded-md px-2 py-1 text-xs font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-800 dark:hover:bg-slate-800 dark:hover:text-slate-100"
                    type="button"
                    onClick={() => run(() => client!.remove(todo.id))}
                  >
                    Remove
                  </button>
                </li>
              ))}
            </ul>

            <footer
              class="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 px-4 py-3 text-sm text-slate-600 dark:border-slate-800 dark:text-slate-400"
            >
              <span>
                {openCount()} open, {completedCount()} done
              </span>
              <button
                class="rounded-md px-2 py-1 font-medium text-indigo-600 transition hover:bg-indigo-50 disabled:opacity-40 dark:text-indigo-400 dark:hover:bg-indigo-950"
                disabled={completedCount() === 0}
                type="button"
                onClick={() => run(() => client!.clearCompleted())}
              >
                Clear completed
              </button>
            </footer>
          </Show>
        </Show>
      </section>
    </main>
  );
}
