# Differential Test Operations

Shared specification for the property-based differential tests that compare
an embedded Turso driver against the serverless driver of the same language.
The machine-readable source of truth is [`spec/ops.json`](spec/ops.json);
every language harness generates its operations from that file, so all
harnesses exercise the same vocabulary.

## Goal

Assert that the embedded and serverless drivers behave identically. For every
operation both drivers must agree on:

- `success`: both succeed or both fail
- `column_count` and `column_names`: result shape matches
- `row_count`: same number of rows returned
- `value_types`: per-row, per-column type tags match (for example both return `integer`)
- `values`: actual cell values match, with epsilon tolerance for floats
- `affected_rows` and `last_insert_rowid`, for operations that report them

When `success` is false on both sides, error messages are not compared; they
legitimately differ between an embedded engine and an HTTP server.

## Operations

`spec/ops.json` defines the operations. They cover, roughly grouped:

- **DDL**: `create`, `create_dynamic` (1-5 columns of random declared types),
  `create_trigger`
- **DML**: `insert`, `insert_returning`, `insert_affected`, `insert_rowid`,
  `update_returning`, `update_affected`, `delete_returning`, `delete_affected`
- **Queries**: `select`, `select_value`, `select_limit`, `select_count`,
  `select_expr`
- **Parameters**: `param` (positional), `named_param` (`:name`),
  `numbered_param` (`?1`), `prepared_reuse` (one statement, three bindings)
- **Transactions**: `begin`, `commit`, `rollback`, `transaction_workflow`
  (BEGIN, 1-5 DML operations, COMMIT or ROLLBACK), `error_in_transaction`
  (a failing statement mid-transaction followed by recovery)
- **Errors**: `invalid`, `error_check` (both drivers must agree an SQL fails),
  `batch` (multi-statement SQL)

## Values

Generated values include NULL, random integers, floats, ASCII strings, and
blobs, plus a deliberately adversarial set: 64-bit integer extremes, empty
strings and blobs, 4 KB strings and blobs, emoji, CJK and RTL text, strings
containing NUL bytes, SQL metacharacters, backslashes, whitespace-only
strings, a BOM, negative zero, and all-zero and all-0xFF blobs.

## Table names

Each generated test case gets a random numeric prefix and works on tables
`t_<prefix>_0` through `t_<prefix>_5`, so concurrent runs and replays do not
interfere. Cases drop their tables up front to be independent of leftovers.

## Result shape

```
OpResult {
    success: bool,
    column_count: Option<usize>,
    column_names: Option<Vec<String>>,
    row_count: Option<usize>,
    value_types: Option<Vec<Vec<String>>>,  // per-row, per-col type tag
    values: Option<Vec<Vec<Value>>>,
}
```

Type tags: `"null"`, `"integer"`, `"real"`, `"text"`, `"blob"`.
