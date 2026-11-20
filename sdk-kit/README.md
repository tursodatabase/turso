# Turso SDK kit

> **Building a Rust application?** Use the [`turso`](https://crates.io/crates/turso) crate instead. This crate is the low-level C ABI used to build language bindings; it is not the Rust SDK for end users.

Low-level, SQLite-compatible Turso API to make building SDKs in any language simple and predictable. The core logic is implemented in Rust and exposed through a small, portable C ABI (turso.h), so you can generate language bindings (e.g. via rust-bindgen) without re-implementing database semantics.

Key ideas:
- Async I/O: opt-in caller-driven I/O (ideal for event loops or io_uring), or library-driven I/O.
- Clear status codes: Done/Row/Io/Error/etc. No hidden blocking or exceptions.
- Minimal surface: open database, connect, prepare, bind, step/execute, read rows, finalize.
- Ownership model and lifetimes are explicit for C consumers.
- Optional page codecs: callers can provide a synchronous page transform callback table to decode/encode legacy encrypted database pages without linking that crypto into Turso core.

Note: turso.h is the single C header exported by the crate and can be translated to bindings.rs with rust-bindgen.

## External page codecs

`turso_database_config_t.page_codec` accepts a versioned `turso_page_codec_v1_t` callback table. The callbacks transform complete page images between on-disk bytes and plaintext SQLite pages. They are intended for compatibility layers such as legacy SQLite codec formats where the application supplies the crypto implementation.

Codec rules:

- callbacks are synchronous and must not perform Turso I/O or re-enter the same database;
- `probe_header` may inspect the raw first 512 bytes and report page size/reserved bytes before Turso parses page 1;
- `decode_page` and `encode_page` receive a page number and location (`DATABASE` or `WAL`) and must write exactly one page to the output buffer;
- `codec_id` must be a stable, non-secret identifier for the codec configuration and must change whenever the page transform could produce different bytes;
- the callback context and function pointers must remain valid until the codec `destroy` callback is invoked;
- callback failures are surfaced as database open/read/write errors.

Language bindings can expose this generic surface by wrapping `turso_page_codec_v1_t`
with binding-specific lifetime management for the callback context.

## Rust example

The example also depends on the `turso_core` crate for `Value` and `OpenFlags`.

```rust
use turso_core::{OpenFlags, Value};
use turso_sdk_kit::{
    rsapi::{TursoDatabase, TursoDatabaseConfig, TursoError, TursoStatusCode},
    IoBackend,
};

fn main() -> Result<(), TursoError> {
    // Create the database holder (not opened yet).
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        // When true, step/execute may return Io and you should call run_io() to progress.
        async_io: true,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });

    // Open and connect.
    while db.open()?.is_io() {
        db.io()?.step()?;
    }
    let conn = db.connect()?;

    // Prepare, bind, and step a simple query.
    let mut stmt = conn.prepare_single("SELECT :greet || ' Turso'")?;
    // The parameter name includes its prefix; positions are 1-based.
    let position = stmt.named_position(":greet")?;
    stmt.bind_positional(position, Value::from_text("Hello"))?;

    loop {
        match stmt.step(None)? {
            TursoStatusCode::Row => {
                // Read the current row value.
                match stmt.row_value(0)? {
                    Value::Text(t) => println!("{}", t.as_str()),
                    other => println!("row[0] = {other:?}"),
                }
            }
            TursoStatusCode::Io => {
                // Drive one iteration of the I/O backend.
                stmt.run_io()?;
            }
            TursoStatusCode::Done => break,
        }
    }

    // Finalize to complete the statement cleanly (may also return Io).
    match stmt.finalize(None)? {
        TursoStatusCode::Io => {
            // If needed, drive IO and finalize again.
            stmt.run_io()?;
            let _ = stmt.finalize(None)?;
        }
        _ => {}
    }

    Ok(())
}
```

## C example

```c
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "turso.h"

// Print the error message returned through an error_opt_out parameter and free it.
static void report_error(const char *what, turso_status_code_t code, const char *error) {
    fprintf(stderr, "%s failed (code %d): %s\n", what, (int)code, error ? error : "(no message)");
    turso_str_deinit(error);
}

int main(void) {
    const turso_database_t *db = NULL;
    turso_connection_t *conn = NULL;
    turso_statement_t *stmt = NULL;
    const char *error = NULL;
    turso_status_code_t rc;
    int exit_code = 1;

    // Optional: initialize logging (stdout example) and/or set log level via environment.
    // turso_setup(&(turso_config_t){ .logger = NULL, .log_level = "info" }, NULL);

    // Create database holder (not opened yet). Enable caller-driven async I/O.
    turso_database_config_t config = {
        .path = ":memory:",
        .experimental_features = NULL,
        .async_io = true,
    };
    rc = turso_database_new(&config, &db, &error);
    if (rc != TURSO_OK) { report_error("turso_database_new", rc, error); return 1; }

    rc = turso_database_open(db, &error);
    if (rc != TURSO_OK) { report_error("turso_database_open", rc, error); goto cleanup; }

    rc = turso_database_connect(db, &conn, &error);
    if (rc != TURSO_OK) { report_error("turso_database_connect", rc, error); goto cleanup; }

    rc = turso_connection_prepare_single(conn, "SELECT :greet || ' Turso'", &stmt, &error);
    if (rc != TURSO_OK) { report_error("turso_connection_prepare_single", rc, error); goto cleanup; }

    // Look up the 1-based position of a named parameter (the name includes its prefix),
    // then bind a value at that position.
    int64_t position = turso_statement_named_position(stmt, ":greet");
    if (position < 0) { fprintf(stderr, "parameter :greet not found\n"); goto cleanup; }
    const char *greet = "Hello";
    rc = turso_statement_bind_positional_text(stmt, (size_t)position, greet, strlen(greet));
    if (rc != TURSO_OK) { report_error("turso_statement_bind_positional_text", rc, NULL); goto cleanup; }

    // Drive the statement: handle ROW/IO/DONE.
    for (;;) {
        rc = turso_statement_step(stmt, &error);
        if (rc == TURSO_IO) {
            // Run one iteration of the IO backend and continue.
            rc = turso_statement_run_io(stmt, &error);
            if (rc != TURSO_OK) { report_error("turso_statement_run_io", rc, error); goto cleanup; }
            continue;
        }
        if (rc == TURSO_ROW) {
            // Row values are valid only until the next operation on the statement.
            if (turso_statement_row_value_kind(stmt, 0) == TURSO_TYPE_TEXT) {
                const char *text = turso_statement_row_value_bytes_ptr(stmt, 0);
                int64_t len = turso_statement_row_value_bytes_count(stmt, 0);
                fwrite(text, 1, (size_t)len, stdout);
                fputc('\n', stdout);
            }
            continue;
        }
        if (rc == TURSO_DONE) break;
        report_error("turso_statement_step", rc, error);
        goto cleanup;
    }

    // Finalize returns TURSO_DONE when complete; with async_io it may return TURSO_IO first.
    for (;;) {
        rc = turso_statement_finalize(stmt, &error);
        if (rc == TURSO_DONE) break;
        if (rc == TURSO_IO) {
            rc = turso_statement_run_io(stmt, &error);
            if (rc != TURSO_OK) { report_error("turso_statement_run_io", rc, error); goto cleanup; }
            continue;
        }
        report_error("turso_statement_finalize", rc, error);
        goto cleanup;
    }
    exit_code = 0;

cleanup:
    // Clean up handles.
    turso_statement_deinit(stmt);
    turso_connection_deinit(conn);
    turso_database_deinit(db);
    return exit_code;
}
```
