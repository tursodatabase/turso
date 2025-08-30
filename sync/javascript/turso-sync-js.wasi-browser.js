import {
  createOnMessage as __wasmCreateOnMessageForFsProxy,
  getDefaultContext as __emnapiGetDefaultContext,
  instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync,
  WASI as __WASI,
} from '@napi-rs/wasm-runtime'



const __wasi = new __WASI({
  version: 'preview1',
})

const __wasmUrl = new URL('./turso-sync-js.wasm32-wasi.wasm', import.meta.url).href
const __emnapiContext = __emnapiGetDefaultContext()


const __sharedMemory = new WebAssembly.Memory({
  initial: 4000,
  maximum: 65536,
  shared: true,
})

const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

const {
  instance: __napiInstance,
  module: __wasiModule,
  napiModule: __napiModule,
} = __emnapiInstantiateNapiModuleSync(__wasmFile, {
  context: __emnapiContext,
  asyncWorkPoolSize: 4,
  wasi: __wasi,
  onCreateWorker() {
    const worker = new Worker(new URL('./wasi-worker-browser.mjs', import.meta.url), {
      type: 'module',
    })

    return worker
  },
  overwriteImports(importObject) {
    importObject.env = {
      ...importObject.env,
      ...importObject.napi,
      ...importObject.emnapi,
      memory: __sharedMemory,
    }
    return importObject
  },
  beforeInit({ instance }) {
    for (const name of Object.keys(instance.exports)) {
      if (name.startsWith('__napi_register__')) {
        instance.exports[name]()
      }
    }
  },
})
export default __napiModule.exports
export const Database = __napiModule.exports.Database
export const Statement = __napiModule.exports.Statement
export const GeneratorHolder = __napiModule.exports.GeneratorHolder
export const JsDataCompletion = __napiModule.exports.JsDataCompletion
export const JsDataPollResult = __napiModule.exports.JsDataPollResult
export const JsProtocolIo = __napiModule.exports.JsProtocolIo
export const JsProtocolRequestData = __napiModule.exports.JsProtocolRequestData
export const SyncEngine = __napiModule.exports.SyncEngine
export const DatabaseChangeTypeJs = __napiModule.exports.DatabaseChangeTypeJs
export const SyncEngineProtocolVersion = __napiModule.exports.SyncEngineProtocolVersion
