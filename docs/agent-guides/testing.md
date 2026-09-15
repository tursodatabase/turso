---
name: testing
description: Test types, when to use each, how to write and run tests
---
# Testing Guide

## Test Types & When to Use

| Type | Location | Use Case |
|------|----------|----------|
| `.sqltest` | `sqlite/conformance/sqlite-sqltests/` | SQL compatibility. **Preferred for new tests** |
| TCL `.test` | `testing/` | Legacy SQL compat (being phased out) |
| Rust integration | `tests/integration/` | Regression tests, complex scenarios |
| Fuzz | `tests/fuzz/` | Complex features, edge case discovery |

**Note:** TCL tests are being phased out in favor of the `.sqltest` suites in `sqlite/conformance/`. The `.sqltest` format allows the same test cases to run against multiple backends (CLI, Rust bindings, etc.).

## Running Tests

```bash
# Main test suite (TCL compat, sqlite3 compat, Python wrappers)
make test

# Single TCL test
make test-single TEST=select.test

# SQL test runner
make -C sqlite/conformance run-cli

# Rust unit/integration tests (full workspace)
cargo test
```

## Writing Tests

### .sqltest (Preferred)
```sql
@database :memory:

@query
SELECT 1 + 1;
@expected
2
```
Location: `sqlite/conformance/sqlite-sqltests/*.sqltest`

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

### Assertions

Prefer the [`asserting`](https://github.com/innoave/asserting) crate for anything more than trivial assertions. The assertions it offers are extensive
and make tests more readable. Some simple examples:

```rust
use asserting::prelude::*;

assert_that!(conn.execute("SELECT * FROM t1;")).is_err();
assert_that_code!(|| parse(bad_input)).panics_with_message("unexpected token");

assert_that!(&rows)
    .has_length(3)
    .any_satisfies(|r| r
    .name == "bob")
    .first_element_ref()
    .is_equal_to(&Row { id: 1, name: "alice".into() });

assert_that!(&header)
    .named("page 2 header")
    .satisfies_with_message("be a leaf table page", |h| h[0] == 0x0d);

// soft assertions: mark the test as failed but don't stop
verify_that!(&plan)
    .starts_with("SEARCH")
    .contains("USING INDEX")
    .soft_panic();
```

`crate::assertions` adds `row!` for result rows, plus `column` and query-plan
assertions:

```rust
use crate::assertions::{AssertColumn, AssertQueryPlan, Cell, NULL};

assert_that!(limbo_exec_rows(&conn, "SELECT id, name FROM t ORDER BY id"))
    .is_equal_to(vec![row![1, "alice"], row![2, NULL]]);

assert_that!(limbo_exec_rows(&conn, "SELECT id FROM t WHERE id = 1"))
    .single_element()
    .is_equal_to(row![1]);

assert_that!(limbo_exec_rows(&conn, "SELECT id, name FROM t"))
    .column(1)
    .contains(Cell::from("alice"));

assert_that!(limbo_exec_rows(&conn, "EXPLAIN QUERY PLAN SELECT id FROM t WHERE name = 'a'"))
    .uses_index("idx_name")
    .searches_table("t")
    .has_table_access_order(["t"]);
```

## Key Rules

- Every functional change needs a test
- Test must fail without change, pass with it
- Prefer in-memory DBs: `:memory:` (sqltest) or `{:memory:}` (TCL)
- Don't invent new test formats. Follow existing patterns.
- Use minimal tests, i.e. the bare minimum that triggers the behaviour. No types, no PKs, etc. unless necessary.
- Use column names a, b, c... table names t1, t2, t3... view names v1, v2, v3... 
- Write tests first when possible
- If tasked with identifying a reproducer for a bug, strongly prefer using only user-facing APIs. Manipulating DB internals to artificially trigger a condition, or asserting internal state, is a bad reproducer.
- A reproducer must serve as a regression test once the bug is fixed.


## Test Database Schema

`testing/testing.db` has `users` and `products` tables. See [docs/testing.md](../testing.md) for schema.

## Logging During Tests

```bash
RUST_LOG=none,turso_core=trace make test
```
Output: `testing/test.log`. Warning: very verbose.
