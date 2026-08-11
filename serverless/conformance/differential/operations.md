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

A value entry in `spec/ops.json` may carry a `disabled` field naming a known
server bug that makes it fail differentially; every harness skips such
entries. Delete the field to re-enable the value once the server is fixed.

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

## Required properties

Beyond the differential tests, `spec/ops.json` lists properties every
harness must implement under `tests`. Most compare the two drivers against
a live Turso Cloud database; the exceptions are noted below. A harness for
a new serverless driver is not complete until it covers every entry.

### `encryption_header`

For any remote encryption key `K` drawn from the spec's `key_alphabet`
(length `key_min_len` to `key_max_len`, plus optional base64 `=` padding),
a driver configured with `K` attaches `x-turso-encryption-key: K` to every
HTTP request it sends — pipeline and cursor endpoints alike — and a driver
configured without a key never sends the header (see `PROTOCOL.md` section
3.1).

This property runs against a local stub HTTP server that records request
headers and speaks just enough of the protocol for the driver to complete a
statement. It needs no Turso Cloud database and must run unconditionally,
never skipping on missing environment configuration.
