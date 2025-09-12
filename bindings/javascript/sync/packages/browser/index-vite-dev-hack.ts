import { isWebWorker, setupMainThread, setupWebWorker } from "@tursodatabase/database-browser-common";
import { tursoWasm } from "./wasm-inline.js";

let napiModule = {
  exports: {
    Database: {} as any,
    Opfs: {} as any,
    OpfsFile: {} as any,
    Statement: {} as any,
    connect: {} as any,
    initThreadPool: {} as any,
    GeneratorHolder: {} as any,
    JsDataCompletion: {} as any,
    JsProtocolIo: {} as any,
    JsProtocolRequestBytes: {} as any,
    SyncEngine: {} as any,
    DatabaseChangeTypeJs: {} as any,
    SyncEngineProtocolVersion: {} as any,
  }
};

export let MainWorker = null;
if (isWebWorker()) {
  setupWebWorker();
} else {
  // Vite has issues with loading wasm modules and worker in dev server: https://github.com/vitejs/vite/issues/8427
  // So, the mitigation for dev server only is:
  // 1. inline wasm binary in the source code in order to avoid issues with loading it from the file
  // 2. use same file as worker entry point
  const __wasmFile = await tursoWasm();

  napiModule = await setupMainThread(__wasmFile, () => {
    const worker = new Worker(import.meta.url, {
      name: 'turso-database-sync',
      type: 'module',
    })
    MainWorker = worker;
    return worker
  });
}

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

