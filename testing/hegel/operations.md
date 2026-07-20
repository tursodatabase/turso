# Hegel Parity Test — Operation Vocabulary

Shared specification for the property-based driver parity tests. The same
`spec/ops.json` vocabulary is consumed by **all four** language harnesses —
`rust/`, `go/`, `python/`, and `js/` — each of which drives its own **local**
(native/embedded) and **remote** (serverless) driver and asserts they agree.

## Goal

Assert parity between the local and remote drivers for each language. For every
generated operation we compare:

- `success` — both succeed or both fail
- `column_count` / `column_names` — result shape matches
- `row_count` — same number of rows returned
- `value_types` — per-row, per-column type tag (`"null"`, `"integer"`, `"real"`,
  `"text"`, `"blob"`)
- **cell values** — actual returned values are compared, with float-epsilon
  tolerance and integer/real crossover handling (see `cell_equal` in each
  harness). (Earlier revisions compared only shape/types; values are compared now.)

When `success == false`, we compare coarse `error_class` categories, not exact
messages.

## How generation works (important when editing the spec)

Each harness builds its Hypothesis / proptest / fast-check strategies **directly
from `spec/ops.json`**. Ops are selected by their **position in the `ops` array**
(`Integers(0, len(ops)-1)` in Go/Rust, `one_of(ops)` in Python/JS), and each op's
`result` kind is switched on by a per-language dispatcher.

Consequences:

- **Every op in `ops.json` must have a dispatcher in all four harnesses.** An op
  without a handler either errors that language's run or — worse — silently
  falls through to a both-fail "parity" (a false pass). Do not add an op to the
  spec without wiring its dispatcher in `python/test_parity.py`,
  `go/parity_test.go`, `rust/src/lib.rs`+`tests/parity.rs`, and `js/parity.test.mjs`.
- **Adding a value id** requires a matching case in every harness's value-strategy
  builder, otherwise that harness raises "unknown value spec".
- Changes to the spec must be validated by actually running the four harnesses
  against a live libsql-server (see the repo's Hegel run instructions); a
  property-based cross-language harness cannot be safely extended blind.

## Operations (current)

| Operation | `result` | Description |
|-----------|----------|-------------|
| `create` / `create_dynamic` | exec | `CREATE TABLE IF NOT EXISTS` (fixed / dynamic columns) |
| `insert` / `insert_affected` / `insert_rowid` | exec_rows / affected / rowid | `INSERT INTO` |
| `insert_returning` / `delete_returning` / `update_returning` | query | `... RETURNING` |
| `delete_affected` / `update_affected` | affected | rows-affected count |
| `select` / `select_limit` / `select_count` / `select_expr` / `select_value` | query | `SELECT` variants |
| `begin` / `commit` / `rollback` | exec | raw transaction control |
| `transaction_workflow` / `error_in_transaction` | exec | multi-step BEGIN…COMMIT / error-then-rollback |
| `param` / `named_param` / `numbered_param` | query | positional `?`, named `:foo`, numbered `?N` params |
| `batch` | exec | multi-statement SQL string (**not** the driver `batch()` API — see planned) |
| `invalid` / `error_check` | query / error_check | error-parity |
| `prepared_reuse` | prepared_reuse | prepare once, execute many |
| `create_trigger` | trigger | trigger DDL + fire |

## Value Types

See the `values` array in `ops.json`: `NULL`, integers (incl. i64 extremes),
floats (incl. extremes), ASCII/unicode/BOM/null-byte/SQL-meta strings, blobs
(incl. 4 KiB and high-bit), empty string/blob, negative zero.

## Table Names

Fixed pool `t_{prefix}_0` … `t_{prefix}_5`; each case uses a per-case `prefix`
for isolation and drops its tables before starting.

## Planned additions (wire + validate with a live libsql-server)

These pin the **serverless-specific** behaviors added in the serverless drivers
across all four languages (including JS, the reference). Each needs an `ops.json`
entry **and** a dispatcher in all four harnesses, validated by running Hegel:

- **`driver_batch`** (new `result` kind `batch`): run a list of statements
  through the driver's `batch()` API on the remote driver and via sequential
  execute on the local driver; compare per-statement result count/order, rows
  affected, and selected row values. Variants: `driver_batch_atomic` (mode set,
  one failing step → whole batch rolls back), `driver_batch_concurrent`
  (mode `concurrent` → `BEGIN CONCURRENT`), and `driver_batch_inside_transaction`
  (batch with a mode inside an open transaction must **not** nest a `BEGIN`).
- **`transaction_helper`** with `mode ∈ {deferred, immediate, exclusive,
  concurrent}` — exercises each language's transaction entry point
  (JS `transaction().concurrent`, Python `conn.transaction("concurrent")`,
  Go `Transaction(ctx, conn, "concurrent", fn)`, Rust
  `transaction_with_behavior(Concurrent)`); `concurrent` must issue `BEGIN CONCURRENT`.
- **named-param prefix matrix** — extend `named_param` to also cover `@name`,
  `$name`, and `?NNN` mapping keys, not just `:name`.
- **`bool` value** — add a boolean value id (binds as integer 0/1) so bool
  round-trips are pinned for every driver.

## Known coverage carried by per-language tests (not shared)

Transport-level behaviors that Hegel's local-vs-remote SQL model can't observe
are covered per language: `requestHeaders` / `x-turso-encryption-key` /
per-request timeout, and Rust-specific API shapes (`DropBehavior`,
`set_transaction_behavior`, `IntoParams` tuple/iter forms, owned `Rows::next`).
Header/encryption assertions currently require a request-capturing endpoint the
real-server suite does not provide, and remain a coverage gap to close.
