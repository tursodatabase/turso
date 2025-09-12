import { instantiateNapiModuleSync, MessageHandler, WASI } from '@napi-rs/wasm-runtime'
import { OpfsDirectory, workerImports } from '@tursodatabase/database-browser-common';

var opfs = new OpfsDirectory();
var memory = null;

const handler = new MessageHandler({
  onLoad({ wasmModule, wasmMemory }) {
    memory = wasmMemory;
    const wasi = new WASI({
      print: function () {
        // eslint-disable-next-line no-console
        console.log.apply(console, arguments)
      },
      printErr: function () {
        // eslint-disable-next-line no-console
        console.error.apply(console, arguments)
      },
    })
    return instantiateNapiModuleSync(wasmModule, {
      childThread: true,
      wasi,
      overwriteImports(importObject) {
        importObject.env = {
          ...importObject.env,
          ...importObject.napi,
          ...importObject.emnapi,
          ...workerImports(opfs, memory),
          memory: wasmMemory,
        }
      },
    })
  },
})

globalThis.onmessage = async function (e) {
  if (e.data.__turso__ == 'register') {
    try {
      await opfs.registerFile(e.data.path);
      self.postMessage({ id: e.data.id });
    } catch (error) {
      self.postMessage({ id: e.data.id, error: error });
    }
    return;
  } else if (e.data.__turso__ == 'unregister') {
    try {
      await opfs.unregisterFile(e.data.path);
      self.postMessage({ id: e.data.id });
    } catch (error) {
      self.postMessage({ id: e.data.id, error: error });
    }
    return;
  }
  handler.handle(e)
}
