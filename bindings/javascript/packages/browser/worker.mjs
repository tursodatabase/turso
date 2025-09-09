import { instantiateNapiModuleSync, MessageHandler, WASI } from '@napi-rs/wasm-runtime'

var fileByPath = new Map();
var fileByHandle = new Map();
let fileHandles = 0;
var memory = null;

function getUint8ArrayFromWasm(ptr, len) {
  ptr = ptr >>> 0;
  return new Uint8Array(memory.buffer).subarray(ptr, ptr + len);
}


async function registerFile(path) {
  if (fileByPath.has(path)) {
    return;
  }
  const opfsRoot = await navigator.storage.getDirectory();
  const opfsHandle = await opfsRoot.getFileHandle(path, { create: true });
  const opfsSync = await opfsHandle.createSyncAccessHandle();
  fileHandles += 1;
  fileByPath.set(path, { handle: fileHandles, sync: opfsSync });
  fileByHandle.set(fileHandles, opfsSync);
}

async function unregisterFile(path) {
  const file = fileByPath.get(path);
  if (file == null) {
    return;
  }
  fileByPath.delete(path);
  fileByHandle.delete(file.handle);
  file.sync.close();
}

function lookup_file(pathPtr, pathLen) {
  try {
    const buffer = getUint8ArrayFromWasm(pathPtr, pathLen);
    const notShared = new Uint8Array(buffer.length);
    notShared.set(buffer);
    const decoder = new TextDecoder('utf-8');
    const path = decoder.decode(notShared);
    const file = fileByPath.get(path);
    if (file == null) {
      return -404;
    }
    return file.handle;
  } catch (e) {
    console.error('lookupFile', pathPtr, pathLen, e);
    return -1;
  }
}
function read(handle, bufferPtr, bufferLen, offset) {
  try {
    const buffer = getUint8ArrayFromWasm(bufferPtr, bufferLen);
    const file = fileByHandle.get(Number(handle));
    const result = file.read(buffer, { at: Number(offset) });
    return result;
  } catch (e) {
    console.error('read', handle, bufferPtr, bufferLen, offset, e);
    return -1;
  }
}
function write(handle, bufferPtr, bufferLen, offset) {
  try {
    const buffer = getUint8ArrayFromWasm(bufferPtr, bufferLen);
    const file = fileByHandle.get(Number(handle));
    const result = file.write(buffer, { at: Number(offset) });
    return result;
  } catch (e) {
    console.error('write', handle, bufferPtr, bufferLen, offset, e);
    return -1;
  }
}
function sync(handle) {
  try {
    const file = fileByHandle.get(Number(handle));
    file.flush();
    return 0;
  } catch (e) {
    console.error('sync', handle, e);
    return -1;
  }
}
function truncate(handle, size) {
  try {
    const file = fileByHandle.get(Number(handle));
    const result = file.truncate(size);
    return result;
  } catch (e) {
    console.error('truncate', handle, size, e);
    return -1;
  }
}
function size(handle) {
  try {
    const file = fileByHandle.get(Number(handle));
    const size = file.getSize()
    return size;
  } catch (e) {
    console.error('size', handle, e);
    return -1;
  }
}

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
          memory: wasmMemory,
          is_web_worker: () => true,
          lookup_file: lookup_file,
          read: read,
          write: write,
          sync: sync,
          truncate: truncate,
          size: size,
        }
      },
    })
  },
})

globalThis.onmessage = async function (e) {
  if (e.data.__turso__ == 'register') {
    try {
      await registerFile(e.data.path)
      self.postMessage({ id: e.data.id })
    } catch (error) {
      self.postMessage({ id: e.data.id, error: error });
    }
    return;
  } else if (e.data.__turso__ == 'unregister') {
    try {
      await unregisterFile(e.data.path)
      self.postMessage({ id: e.data.id })
    } catch (error) {
      self.postMessage({ id: e.data.id, error: error });
    }
    return;
  }
  handler.handle(e)
}
