import avaTest from "ava";
import turso from "../wrapper.js";
import sqlite from "better-sqlite3";

class DualTest {

  #libs = { turso, sqlite };
  #beforeEaches = [];
  #pathFn;
  #options;

  constructor(path_opt, options = {}) {
    if (typeof path_opt === 'function') {
      this.#pathFn = path_opt;
    } else {
      this.#pathFn = () => path_opt ?? "hello.db";
    }
    this.#options = options;
  }

  beforeEach(fn) {
    this.#beforeEaches.push(fn);
  }

  only(name, impl, ...rest) {
    avaTest.serial.only('[TESTING TURSO] ' + name, this.#wrap('turso', impl), ...rest);
    avaTest.serial.only('[TESTING BETTER-SQLITE3] ' + name, this.#wrap('sqlite', impl), ...rest);
  }

  onlySqlitePasses(name, impl, ...rest) {
    avaTest.serial.failing('[TESTING TURSO] ' + name, this.#wrap('turso', impl), ...rest);
    avaTest.serial('[TESTING BETTER-SQLITE3] ' + name, this.#wrap('sqlite', impl), ...rest);
  }

  both(name, impl, ...rest) {
    avaTest.serial('[TESTING TURSO] ' + name, this.#wrap('turso', impl), ...rest);
    avaTest.serial('[TESTING BETTER-SQLITE3] ' + name, this.#wrap('sqlite', impl), ...rest);
  }

  skip(name, impl, ...rest) {
    avaTest.serial.skip('[TESTING TURSO] ' + name, this.#wrap('turso', impl), ...rest);
    avaTest.serial.skip('[TESTING BETTER-SQLITE3] ' + name, this.#wrap('sqlite', impl), ...rest);
  }

  async #runBeforeEach(t) {
    for (const beforeEach of this.#beforeEaches) {
      await beforeEach(t);
    }
  }

  #wrap(provider, fn) {
    return async (t, ...rest) => {
      const path = this.#pathFn();
      const Lib = this.#libs[provider];
      const db = this.#connect(Lib, path, this.#options)
      t.context = {
        ...t,
        connect: this.#curry(this.#connect)(Lib),
        db,
        errorType: Lib.SqliteError,
        path,
        provider,
      };

      t.teardown(() => db.close());
      await this.#runBeforeEach(t);
      await fn(t, ...rest);
    };
  }

  #connect(constructor, path, options) {
    return new constructor(path, options);
  }

  #curry(fn) {
    return (first) => (...rest) => fn(first, ...rest);
  }
}

export default DualTest;


