# Generated Columns: Deferred Improvements

## Architectural

### ColumnLayout threading
`&ColumnLayout` is threaded through 15+ function signatures in insert.rs, update.rs, upsert.rs, and emitter/mod.rs. It should be part of a context object (`InsertEmitCtx`, `TranslateCtx`, or a shared DML context) instead of manually threaded.

### Duplicate virtual column computation
Three functions do the same thing (evaluate generated column expression into a target register) with different input shapes:
- `compute_virtual_columns_for_triggers` in insert.rs (operates on `ColMapping`)
- `compute_virtual_columns` in update.rs (operates on `DmlColumnContext`)
- inline loops in upsert.rs

These should be unified into a single function in a shared module (e.g., `emitter/gencol.rs`).

## Testing Gaps

### HIGH risk
- **Basic DELETE** — Only 1 DELETE test exists and it's skip-if-mvcc. Need unconditional DELETE tests: basic, WHERE on virtual column, DELETE all, DELETE RETURNING.
- **INSERT OR REPLACE / REPLACE INTO** — Upsert code was rewritten; no REPLACE test exists.
- **INSERT ... SELECT** — Different code path from INSERT VALUES, completely untested.
- **Test bug: `gencol_error_nondeterministic_unixepoch`** — References `t6` but table is `t7`. Likely passes for the wrong reason.

### MEDIUM-HIGH risk
- CHECK constraints on tables with virtual columns
- STRICT tables with virtual columns (type checking code was modified)
- WITHOUT ROWID tables with virtual columns (different internal structure)
- DELETE trigger accessing `OLD.virtual_col` (only INSERT/UPDATE triggers tested)

### MEDIUM risk
- DISTINCT with virtual columns
- UNION / INTERSECT / EXCEPT with virtual columns
- CTEs with virtual columns
- UPDATE fan-out (one base column, multiple generated dependents)
- VACUUM after creating tables with virtual columns
- INSERT OR IGNORE with virtual columns
- Multiple indexes updated simultaneously on table with virtual columns
- Table with rowid alias + only virtual columns
- Type mismatch (expression result vs declared column type)

### LOW risk
- LIMIT/OFFSET with virtual columns
- GLOB in generated column expressions
- Scalar subquery in SELECT from table with virtual columns
- Large row counts (100+ rows)
