import {
  createOnMessage as __wasmCreateOnMessageForFsProxy,
  getDefaultContext as __emnapiGetDefaultContext,
  instantiateNapiModule as __emnapiInstantiateNapiModule,
  WASI as __WASI,
} from '@napi-rs/wasm-runtime'



const __wasi = new __WASI({
  version: 'preview1',
})

const __wasmUrl = new URL('./turso.wasm32-wasi.wasm', import.meta.url).href
const __emnapiContext = __emnapiGetDefaultContext()


const __sharedMemory = new WebAssembly.Memory({
  initial: 4000,
  maximum: 65536,
  shared: true,
})

const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

export let MainWorker = null;

function panic(name) {
  throw new Error(`method ${name} must be invoked only from the main thread`);
}

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
      memory: __sharedMemory,
      is_web_worker: () => false,
      lookup_file: () => panic("lookup_file"),
      read: () => panic("read"),
      write: () => panic("write"),
      sync: () => panic("sync"),
      truncate: () => panic("truncate"),
      size: () => panic("size"),
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
export const Opfs = __napiModule.exports.Opfs
export const OpfsFile = __napiModule.exports.OpfsFile
export const Statement = __napiModule.exports.Statement
export const connect = __napiModule.exports.connect
export const initThreadPool = __napiModule.exports.initThreadPool
