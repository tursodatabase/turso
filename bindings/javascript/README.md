# Limbo JavaScript Driver

Limbo is a _work-in-progress_, in-process OLTP database engine library written in Rust that has:

* **Asynchronous I/O** support on Linux with `io_uring`
* **SQLite compatibility** [doc](../../COMPAT.md) for SQL dialect, file formats, and the C API
* **Language bindings** for JavaScript/WebAssembly, [Rust](../rust), [Go](../go), [Python](../python), and [Java](../java)
* **OS support** for Linux, macOS, and Windows

In the future, we will be also working on:

* **Integrated vector search** for embeddings and vector similarity.
* **`BEGIN CONCURRENT`** for improved write throughput.
* **Improved schema management** including better `ALTER` support and strict column types by default.

## Install

```sh
npm i @tursodatabase/limbo
```

## Usage

```js
import { Database } from 'limbo-wasm';

const db = new Database('sqlite.db');
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();
console.log(users);
```

## Documentation

- [API Docs](https://github.com/tursodatabase/limbo/blob/main/bindings/javascript/docs/API.md)

