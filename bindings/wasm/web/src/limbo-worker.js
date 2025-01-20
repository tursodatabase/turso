import { VFS } from "./opfs.js";
import init, { Database } from "../dist/index.js";

async function initVFS() {
  const vfs = new VFS();
  await vfs.ready;
  self.vfs = vfs;
  return vfs;
}

async function initAll() {
  await initVFS();
  await init();
}

const initialized = initAll();

// worker.ts

let db = null;
let stmt = null;

const api = {
  async createDb(path) {
    await initialized;
    db = new Database(path);
  },
  exec(sql) {
    db.exec(sql);
  },
  prepare(sql) {
    stmt = db.prepare(sql);
  },
  get() {
    return stmt.get();
  },
  all() {
    return stmt.all();
  },
};

self.onmessage = async (e) => { // Make handler async
  const { fn, args } = e.data;
  try {
    const result = await api[fn](...args); // Await the result
    self.postMessage({ result });
  } catch (err) {
    self.postMessage({ error: err.message });
  }
};

// logLevel:
//
// 0 = no logging output
// 1 = only errors
// 2 = warnings and errors
// 3 = debug, warnings, and errors
const logLevel = 1;

const loggers = {
  0: console.error.bind(console),
  1: console.warn.bind(console),
  2: console.log.bind(console),
};
const logImpl = (level, ...args) => {
  if (logLevel > level) loggers[level]("OPFS asyncer:", ...args);
};
const log = (...args) => logImpl(2, ...args);
const warn = (...args) => logImpl(1, ...args);
const error = (...args) => logImpl(0, ...args);
