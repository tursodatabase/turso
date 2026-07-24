# Code Experts

Who to ask about each area of this repository. This is about knowledge, not
review routing (that's `docs/CODEOWNERS`) or accountability. If you know an
area well, sign yourself up: edit this file on GitHub, add a `- @yourhandle`
line under the component's Experts list, and merge — no approval needed.

## storage

B-tree, pager, page cache, and the SQLite-compatible database file format.

Paths: `core/storage/`

Experts:

## wal

Write-ahead log, checkpointing, recovery.

Paths: `core/storage/wal.rs`

Experts:

## sqlite-frontend

SQL parser, AST, planner, optimizer, translation to bytecode.

Paths: `sqlite/parser/`, `core/translate/`

Experts:

## postgres-frontend

PostgreSQL wire protocol server, parser, and frontend.

Paths: `postgres/`

Experts:

## vdbe

Bytecode interpreter and SQL function implementations.

Paths: `core/vdbe/`, `core/functions/`

Experts:

## mvcc

Experimental multi-version concurrency control.

Paths: `core/mvcc/`

Experts:

## io

Async I/O backends, completions, io_uring.

Paths: `core/io/`

Experts:

## cli

tursodb REPL, MCP server, sync server.

Paths: `cli/`

Experts:

## extensions

Extension API, crypto, regexp, csv, virtual tables.

Paths: `extensions/`, `core/ext/`

Experts:

## bindings-c

C bindings.

Paths: `bindings/c/`

Experts:

- @penberg

## bindings-rust

Rust bindings.

Paths: `bindings/rust/`

Experts:

- @penberg

## bindings-js

Node and WASM bindings (NAPI).

Paths: `bindings/javascript/`, `bindings/wasm/`

Experts:

- @penberg

## bindings-python

Python bindings (PyO3).

Paths: `bindings/python/`

Experts:

## bindings-go

Go bindings (CGO).

Paths: `bindings/go/`

Experts:

## bindings-java

Java and Kotlin bindings (JNI).

Paths: `bindings/java/`

Experts:

## sync

Turso Cloud sync engine and SDK kit.

Paths: `sync/`

Experts:

## simulator

Deterministic simulation and fault injection.

Paths: `testing/simulator/`, `testing/concurrent-simulator/`

Experts:

## conformance

sqltest harness, differential testing, stress tooling.

Paths: `sqlite/conformance/`, `testing/`

Experts:

- @penberg
