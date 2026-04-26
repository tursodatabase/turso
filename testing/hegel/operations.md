# Hegel Parity Test — Operation Vocabulary

Shared specification for property-based driver parity tests across Rust, Go, and Python.

## Goal

Assert **structural parity** between local and remote drivers. We do **not** compare actual cell values (avoids SQLite-vs-libsql noise). We compare:

- `success` — both succeed or both fail
- `column_count` / `column_names` — result shape matches
- `row_count` — same number of rows returned
- `value_types` — per-row, per-column type tags match (e.g. both return "integer")

When `success == false`, we compare coarse `error_class` categories, not exact messages.

## Operations

| Operation | Parameters | Description |
|-----------|-----------|-------------|
| `CreateTable` | table_name, columns (name + type) | DDL — `CREATE TABLE IF NOT EXISTS` |
| `Insert` | table_name, values | DML — `INSERT INTO` |
| `Select` | table_name | Query — `SELECT * FROM` |
| `SelectValue` | literal expression | `SELECT <expr>` (no table) |
| `BeginTx` | — | `BEGIN` transaction |
| `CommitTx` | — | `COMMIT` transaction |
| `RollbackTx` | — | `ROLLBACK` transaction |
| `InvalidSQL` | garbage SQL | Should error identically |
| `Param` | SQL + bound params | Parameterized `SELECT ?, ?` |

## Value Types

Generated from: `NULL`, integers (`-10000..10000`), floats (`-100.0..100.0`), text (ASCII, 0-50 chars), blobs (0-32 bytes).

## Table Names

Generated from a fixed pool: `t_0` through `t_5`. Each test case drops all tables before starting to ensure isolation.

## Result Shape

```
OpResult {
    success: bool,
    column_count: Option<usize>,
    column_names: Option<Vec<String>>,
    row_count: Option<usize>,
    value_types: Option<Vec<Vec<String>>>,  // per-row, per-col type tag
}
```

Type tags: `"null"`, `"integer"`, `"real"`, `"text"`, `"blob"`.
