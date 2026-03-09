# SQLite TCL Compatibility Tests

This directory contains TCL-based tests that verify Turso's compatibility with SQLite behavior. The tests use a native TCL extension (`libturso_tcl`) that provides an in-process `sqlite3` command backed by the Turso engine.

## Prerequisites

- **TCL** (`tclsh`) installed on your system
- **TCL dev headers** (e.g., `tcl-dev` on Debian/Ubuntu, `tcl-tk` via Homebrew on macOS)
- **Rust toolchain** (for building the native extension)

## Building the Native Extension

Before running tests, build the `libturso_tcl` shared library:

```bash
make -C bindings/tcl
```

This will:
1. Build `turso_sqlite3` via Cargo
2. Compile `turso_tcl.c` into a shared library (`libturso_tcl.dylib` on macOS, `libturso_tcl.so` on Linux)

On Linux without local TCL dev headers, you can build inside Docker:

```bash
make -C bindings/tcl docker-build
```

## Running Tests

Run all tests:

```bash
./all.test
```

Run a single test file:

```bash
tclsh select1.test
```

## Test Structure

- `tester.tcl` — Test framework (loaded by all test files). Provides `do_test`, `do_execsql_test`, `do_catchsql_test`, and other helpers.
- `all.test` — Runner that sources all individual test files.
- `*.test` — Individual test files organized by SQL feature (e.g., `select1.test`, `insert.test`, `join.test`, `func.test`, `alter.test`).
