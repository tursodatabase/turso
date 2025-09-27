import { setupMainThread } from "@tursodatabase/database-browser-common";
import { tursoWasm } from "./wasm-inline.js";

// Next (turbopack) has issues with loading wasm module: https://github.com/vercel/next.js/issues/82520
// So, we inline wasm binary in the source code in order to avoid issues with loading it from the file
const __wasmFile = await tursoWasm();

export let MainWorker = null;

const napiModule = await setupMainThread(__wasmFile, () => {
  const worker = new Worker(new URL('./worker.js', import.meta.url), {
    name: 'turso-database',
    type: 'module',
  })
  MainWorker = worker;
  return worker
});

export default napiModule.exports
export const Database = napiModule.exports.Database
export const Opfs = napiModule.exports.Opfs
export const OpfsFile = napiModule.exports.OpfsFile
export const Statement = napiModule.exports.Statement
export const initThreadPool = napiModule.exports.initThreadPool
