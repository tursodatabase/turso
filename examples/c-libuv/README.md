# Turso + libuv

An example of using Turso's [C ABI](../../../sdk-kit/turso.h) from a **non-Rust**
external I/O loop. The database is driven entirely from libuv callbacks, so libuv
decides how many statements run and when.

## Why this shape

Turso's C ABI has an `async_io` mode where an operation that would normally block
returns `TURSO_IO` instead. It is then the caller's job to decide when to make
progress, by calling `turso_statement_run_io()` and retrying. That is the hook an
external event loop needs.

Worth being precise about what is and is not integrated here:

- Turso has no libuv VFS. The `vfs` options in `turso.h` are `"memory"`,
  `"syscall"`, `"io_uring"` and `"experimental_win_iocp"`.
- `turso_statement_run_io()` is the only way to pump the I/O backend, and it takes
  a *statement*. There is no database- or connection-level equivalent.

So with `async_io` the file I/O still happens inside Turso; what libuv takes over is
**scheduling**. The loop decides which callback runs when, how many statements are
in flight, and how database work is interleaved with the rest of the loop. Turso
knows nothing about libuv - the integration point is a single polling loop over
`TURSO_IO`.

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
   This matches the pattern in `sdk-kit/src/capi.rs`, which is the same loop.
2. Every libuv callback that touches the database goes through that pump, so the
   loop never blocks inside Turso and Turso never makes progress on its own.

## Known limitation (please read)

**Opening an already-populated database with `async_io = 1` is not possible from
C.** `turso_database_open()` can return `TURSO_IO` - as reading the schema of an
existing file requires I/O - and the only pump the C ABI exposes is
`turso_statement_run_io()`, which needs a statement that does not exist yet.

The example checks for this and prints a diagnostic instead of hanging:

```text
turso_database_open requested IO, but the C ABI exposes no database-level pump
(only turso_statement_run_io). Open the database before starting the event loop
with a synchronous driver, or drive the open through the Rust bindings.
```

`make run` therefore passes a fresh path each time. Workarounds while that gap is
open:

- open the database with `async_io = 0` before the loop starts, then switch to the
  polling pattern for statements (the I/O still happens inside Turso; you lose the
  caller-driven pacing for the open only);
- drive the open through the Rust bindings (`sdk-kit/src/rsapi.rs`), whose
  `TursoDatabase::open()` returns an `IOResult` you can pump yourself;
- or wait for a database-level `turso_database_run_io`.

This is the one rough edge found while writing this example; it is a limitation of
the C ABI rather than of the libuv integration pattern itself.
