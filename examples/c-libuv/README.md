# Turso + libuv

An example of using Turso's [C ABI](../../sdk-kit/turso.h) from a **non-Rust**
event loop. After database setup, libuv timer callbacks run small statements to
completion.

## Why this shape

Turso's C ABI has an `async_io` mode that returns `TURSO_IO` when a statement needs
the caller to drive I/O. The caller then calls `turso_statement_run_io()` and
retries. This controls who drives I/O; it does not make a synchronous backend
non-blocking.

Worth being precise about what is and is not integrated here:

- Turso has no libuv VFS. The `vfs` options in `turso.h` are `"memory"`,
  `"syscall"`, `"io_uring"` and `"experimental_win_iocp"`.
- `turso_statement_run_io()` is the only way to pump the I/O backend, and it takes
  a *statement*. There is no database- or connection-level equivalent.

File I/O still happens inside Turso. This example selects `vfs = "syscall"`, whose
Unix implementation uses synchronous file operations. Each timer callback runs
its statements to completion and does not yield back to libuv between retries.
Long queries or slow file operations can therefore delay other callbacks.

If you want libuv's *own* file I/O to be what backs the database, that needs a VFS
inside Turso, which this example does not attempt.

## Build and run

```bash
# 1. Build the Turso C library (from the repository root)
cargo build -p turso_sdk_kit --release

# 2. Build and run the example
cd examples/c-libuv
make run
```

Prerequisites: a C11 compiler, libuv (`brew install libuv` on macOS,
`apt install libuv1-dev` on Debian/Ubuntu) and the shared library built in step 1.

## What it does

A `uv_timer_t` fires every 500 ms. Each tick runs one `INSERT` and then a `SELECT`
that echoes everything written so far:

```text
libuv loop running; database I/O is driven from timer callbacks
--- after tick 1 ---
1 | tick 1
--- after tick 2 ---
1 | tick 1
2 | tick 2
...
```

Replace the timer with any other libuv source - a stream, a prepare/check/idle
handle, a process exit callback - and the same pattern applies.

## The integration pattern

Two pieces:

1. `run_statement()` pumps a statement, calling `turso_statement_run_io()` on every
   `TURSO_IO` until the statement finishes:
   ```c
   for (;;) {
       rc = turso_statement_step(stmt, &err);
       if (rc != TURSO_IO) return rc;
       if (turso_statement_run_io(stmt, &err) != TURSO_OK) return TURSO_IOERR;
   }
   ```
2. The callback returns to libuv only after the insert and query finish. A
   responsive application needs a different execution strategy, such as running
   database work on a worker thread, rather than copying this callback unchanged.

## Known limitation (please read)

If `turso_database_open()` returns `TURSO_IO`, this example cannot complete the
open: the C ABI exposes only `turso_statement_run_io()`, which needs a statement
that does not exist yet. Whether opening a particular file returns `TURSO_IO`
depends on the operation and backend; this is not a claim that every existing
database fails to open.

The example checks for this and prints a diagnostic instead of hanging:

```text
turso_database_open requested IO, but the C ABI exposes no database-level pump
(only turso_statement_run_io). Open the database before starting the event loop
with a synchronous driver, or drive the open through the Rust bindings.
```

Other open errors are reported immediately, without attempting to connect.
`make run` uses a fresh database path. Alternatives when opening returns
`TURSO_IO` include:

- create the database with `async_io = 0`, so Turso drives I/O for both opening and
  subsequent statements; the C API has no setter to switch that database to
  caller-driven I/O after opening;
- drive the open through the Rust bindings (`sdk-kit/src/rsapi.rs`), whose
  `TursoDatabase::open()` returns an `IOResult` you can pump yourself;
- or add a database-level I/O pump to the C API.
