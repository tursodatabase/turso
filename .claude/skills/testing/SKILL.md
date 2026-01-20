---
name: testing
description: How to write tests, when to use each type of test, and how to run them
---
# Testing Guide

## Test Types & When to Use

| Type | Location | Use Case |
|------|----------|----------|
| `.sqltest` | `turso-test-runner/tests/` | SQL compatibility. **Preferred for new tests** |
| TCL `.test` | `testing/` | Legacy SQL compat (being phased out) |
| Rust integration | `tests/integration/` | Regression tests, complex scenarios |
| Fuzz | `tests/fuzz/` | Complex features, edge case discovery |

**Note:** TCL tests are being phased out in favor of turso-test-runner. The `.sqltest` format allows the same test cases to run against multiple backends (CLI, Rust bindings, etc.).

## Running Tests

```bash
# Main test suite (TCL compat, sqlite3 compat, Python wrappers)
make test

# Single TCL test
make test-single TEST=select.test

# SQL test runner
make -C turso-test-runner run-cli

# Rust unit/integration tests (full workspace)
cargo test
```

## Writing Tests

### .sqltest (Preferred)
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
Location: `turso-test-runner/tests/*.sqltest`

You can also convert TCL tests with the `convert` command from the test runner. It is not always accurate, but it will convert most of the steps. Then you need to verify the tests work by running them with `make -C turso-test-runner run-rust`, and adjust their output if something was wrong with the conversion. Also, we use harcoded databases in TCL, but with `.sqltest` we generate the database with a different seed, so you may need to adjust the output in certain tests because of this.

### TCL
```tcl
do_execsql_test_on_specific_db {:memory:} test-name {
  SELECT 1 + 1;
} {2}
```
Location: `testing/*.test`

### Rust Integration
```rust
// tests/integration/test_foo.rs
#[test]
fn test_something() {
    let conn = Connection::open_in_memory().unwrap();
    // ...
}
```

## Key Rules

- Every functional change needs a test
- Test must fail without change, pass with it
- Prefer in-memory DBs: `:memory:` (sqltest) or `{:memory:}` (TCL)
- Don't invent new test formats. Follow existing patterns
- Write tests first when possible

## Test Database Schema

`testing/system/testing.db` has `users` and `products` tables. See [docs/testing.md](../../../docs/testing.md) for schema.

## Logging During Tests

```bash
RUST_LOG=none,turso_core=trace make test
```
Output: `testing/system/test.log`. Warning: very verbose.
