import {
  createOnMessage as __wasmCreateOnMessageForFsProxy,
  getDefaultContext as __emnapiGetDefaultContext,
  instantiateNapiModule as __emnapiInstantiateNapiModule,
  WASI as __WASI,
} from '@napi-rs/wasm-runtime'

import { MainDummyImports } from "@tursodatabase/database-browser-common";


const __wasi = new __WASI({
  version: 'preview1',
})

const __wasmUrl = new URL('./sync.wasm32-wasi.wasm', import.meta.url).href
const __emnapiContext = __emnapiGetDefaultContext()


const __sharedMemory = new WebAssembly.Memory({
  initial: 4000,
  maximum: 65536,
  shared: true,
})

const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

export let MainWorker = null;

const {
  instance: __napiInstance,
  module: __wasiModule,
  napiModule: __napiModule,
} = await __emnapiInstantiateNapiModule(__wasmFile, {
  context: __emnapiContext,
  asyncWorkPoolSize: 1,
  wasi: __wasi,
  onCreateWorker() {
    const worker = new Worker(new URL('./worker.mjs', import.meta.url), {
      type: 'module',
    })
    MainWorker = worker;
    return worker
  },
  overwriteImports(importObject) {
    importObject.env = {
      ...importObject.env,
      ...importObject.napi,
      ...importObject.emnapi,
      ...MainDummyImports,
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
export const Opfs = __napiModule.exports.Opfs
export const OpfsFile = __napiModule.exports.OpfsFile
export const connect = __napiModule.exports.connect
export const initThreadPool = __napiModule.exports.initThreadPool
export const GeneratorHolder = __napiModule.exports.GeneratorHolder
export const JsDataCompletion = __napiModule.exports.JsDataCompletion
export const JsProtocolIo = __napiModule.exports.JsProtocolIo
export const JsProtocolRequestBytes = __napiModule.exports.JsProtocolRequestBytes
export const SyncEngine = __napiModule.exports.SyncEngine
export const DatabaseChangeTypeJs = __napiModule.exports.DatabaseChangeTypeJs
export const SyncEngineProtocolVersion = __napiModule.exports.SyncEngineProtocolVersion

