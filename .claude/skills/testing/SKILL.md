---
name: testing
description: How to write tests, when to use each type of test, and how to run them. Covers `.sqltest` files, Rust integration tests, and converting `.test` files to `.sqltest`
---
# Testing Guide

## Test Types

| Type | Location | Use Case |
|------|----------|----------|
| `.sqltest` | `sqlite/conformance/sqlite-sqltests/` | SQL compatibility. Preferred for new tests |
| `.sqltest` | `testing/shared-sqltests/` | SQL that both the SQLite and PostgreSQL frontends run |
| `.sqltest` | `postgres/conformance/pg-sqltests/` | PostgreSQL frontend |
| Rust integration | `tests/integration/` | Regression tests, multi-connection and storage scenarios |
| Fuzz | `tests/fuzz/` | Edge cases found by fuzzing |
| TCL `.test` | `testing/system/` | Legacy. Write new tests as `.sqltest` |

Extend an existing test file before creating a new one.

## Running Tests

```bash
make test                                   # TCL, sqlite3 compat, extensions, MVCC
make test-single TEST=select.test           # one TCL test
make -C sqlite/conformance run-rust ARGS='--snapshot-filter __never__'   # .sqltest runner
cargo test                                  # Rust unit and integration tests
cargo test -p core_tester --test leak_tests # per-database memory leak check (separate binary with a counting allocator)
```

`make` sets `RUST_LOG=off`.

## Writing a .sqltest

```
@database :default:

test example-addition {
    SELECT 1 + 1;
}
expect {
    2
}

test example-multiple-rows {
    SELECT id, name FROM users WHERE id < 3;
}
expect {
    1|alice
    2|bob
}
```

To convert a TCL test: `make -C sqlite/conformance convert INPUT=<file> OUTPUT=<dir>`.
The converter warns about parts it cannot convert, such as loops; write those by hand.
`.sqltest` databases are generated with a different seed than the TCL ones, so change
expected results, not the SQL.

## Writing a Rust integration test

Use the helpers in `tests/integration/`:

```rust
use crate::common::{limbo_exec_rows, TempDatabase};

#[test]
fn test_something() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT id, name FROM t");
    // ...
}
```

For anything beyond trivial checks, use the [`asserting`](https://github.com/innoave/asserting)
crate (`assert_that!`, `verify_that!`). `tests/integration/` also has `row!`,
`.column(n)` and query-plan assertions such as `.uses_index("idx")`.

## Rules

- Every functional change gets a test that fails without the change and passes with it.
- Prefer in-memory databases.
- Follow the existing test formats.

## Test Database

`testing/system/testing.db` has `users` and `products` tables. Schema: [docs/testing.md](../../../docs/testing.md).

## Logging

Trace logs from the engine: `RUST_LOG=turso_core=trace cargo test <name> -- --nocapture`
(very verbose). `make` overrides `RUST_LOG` to `off`.
