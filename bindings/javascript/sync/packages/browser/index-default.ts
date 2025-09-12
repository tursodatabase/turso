import { isWebWorker, setupWebWorker, setupMainThread } from "@tursodatabase/database-browser-common";

export let MainWorker = null;

const __wasmUrl = new URL('./sync.wasm32-wasi.wasm', import.meta.url).href;
const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

const napiModule = await setupMainThread(__wasmFile, () => {
  const worker = new Worker(new URL('./worker.js', import.meta.url), {
    name: 'turso-database-sync',
    type: 'module',
  })
  MainWorker = worker;
  return worker
});

export default napiModule.exports
export const Database = napiModule.exports.Database
export const Statement = napiModule.exports.Statement
export const Opfs = napiModule.exports.Opfs
export const OpfsFile = napiModule.exports.OpfsFile
export const connect = napiModule.exports.connect
export const initThreadPool = napiModule.exports.initThreadPool
export const GeneratorHolder = napiModule.exports.GeneratorHolder
export const JsDataCompletion = napiModule.exports.JsDataCompletion
export const JsProtocolIo = napiModule.exports.JsProtocolIo
export const JsProtocolRequestBytes = napiModule.exports.JsProtocolRequestBytes
export const SyncEngine = napiModule.exports.SyncEngine
export const DatabaseChangeTypeJs = napiModule.exports.DatabaseChangeTypeJs
export const SyncEngineProtocolVersion = napiModule.exports.SyncEngineProtocolVersion

