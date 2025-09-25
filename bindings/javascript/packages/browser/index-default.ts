import { setupMainThread } from "@tursodatabase/database-browser-common";

const __wasmUrl = new URL('./turso.wasm32-wasi.wasm', import.meta.url).href;
const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

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
