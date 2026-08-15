# Turso for the browser

Turso compiled to WebAssembly with [wasm-pack](https://rustwasm.github.io/wasm-pack/),
storing its database files in the browser through the
[Origin Private File System](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system).

## Build

```bash
wasm-pack build --target web
```

That writes `pkg/`. The crate targets `wasm32-unknown-unknown` and is
deliberately **not** a workspace member, so a plain `cargo build` at the repo
root ignores it.

**Do not ship a `--dev` build.** Without optimization the SQL parser, which is
recursive descent, uses more call frames than a browser's WebAssembly stack
allows, and the first statement dies with `RangeError: Maximum call stack size
exceeded`. This is the engine's call stack, not the shadow stack in linear
memory, so no `-z stack-size` link argument helps; the fix is to let the
optimizer inline and shrink the frames. The same build runs fine under Node,
which allows a deeper stack, so the tests do not catch this.

## Run the example

The todo app in `example/` is a SolidJS UI bundled with Vite. Turso runs in a
Web Worker and stores its database in OPFS.

```bash
wasm-pack build --target web
cd example
npm install
npm run dev
```

Then open `http://localhost:5173`. Add todos, reload the page, and they stay.

## Use it

```js
// preopen and closeAll come from the wasm package too. Do not import
// js/vfs.js directly: wasm-bindgen copies it into pkg/snippets/, so a direct
// import would give you a second handle registry that Rust never reads.
import init, { Database, preopen, closeAll } from './pkg/turso_wasm.js';

await init();
await preopen('app.db');          // must happen before opening the database

const db = new Database('app.db');
db.exec('CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)');
db.exec("INSERT INTO notes (body) VALUES ('hello')");

for (const row of db.query('SELECT id, body FROM notes')) {
  console.log(row.id, row.body);
}

closeAll();                        // release the OPFS handles when done
```

`Database.inMemory()` opens a database with no storage behind it, which is
useful for tests.

Values arrive as `null`, `number`, `string`, or `Uint8Array`. Integers too
large for a JavaScript `number` arrive as `BigInt` rather than being silently
rounded.

## Two constraints worth knowing

**It has to run in a Web Worker.** Turso's storage layer is synchronous, and the
only synchronous file API in a browser is `FileSystemSyncAccessHandle`, which
exists only inside a worker. On the main thread `preopen` throws and says so.

**`preopen` has to run first.** Creating a sync access handle is asynchronous,
while Turso opens files synchronously. `preopen(path)` resolves the handles for
`path` and `path-wal` up front and parks them in a registry; the synchronous
calls from Rust are then just lookups by integer id. Opening a database without
preopening it fails with a message pointing back here.

## How it fits together

| Piece | Role |
|---|---|
| `js/vfs.js` | OPFS handle registry plus the synchronous read/write/sync/size/truncate calls Rust makes |
| `src/vfs.rs` | Implements Turso's `IO` and `File` traits on top of those calls |
| `src/lib.rs` | The `Database` type exposed to JavaScript |

Rust holds only an integer per open file. The `FileSystemSyncAccessHandle`
objects stay in JavaScript, which keeps the Rust types `Send + Sync` without
unsafe impls, since `JsValue` is neither.

`src/vfs.rs` also supplies the clock. `std::time::Instant` panics on
`wasm32-unknown-unknown`, so monotonic time comes from `performance.now()` and
wall-clock time from `Date.now()`.

## Test

```bash
wasm-pack test --node
```

Those cover the SQL surface against the in-memory backend. They need no browser
API, so they run under Node. The OPFS path needs a real Web Worker, so
`example/` is what exercises it.
