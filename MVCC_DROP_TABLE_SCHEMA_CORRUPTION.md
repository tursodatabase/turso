# MVCC `DROP TABLE` Schema Corruption Reproducer

## Summary

There is a deterministic MVCC/yield-injection reproducer for this corruption:

```text
sqlite_schema contains index for missing table 'repro_target': rootpage=37 sql=CREATE UNIQUE INDEX IF NOT EXISTS idx_repro_target_c0 ON repro_target (c0) WHERE c1 IS NULL
```

The reproducer shows that an interrupted `DROP TABLE repro_target` can delete
the `sqlite_schema` table row for `repro_target` while leaving the index row for
`idx_repro_target_c0`. If another statement runs on the same
connection while the `DROP TABLE` statement is suspended, dropping the suspended
statement no longer rolls back the schema mutation completely. Reopening the
database then rebuilds schema from an index row whose `tbl_name` is
`repro_target`, but there is no corresponding `repro_target` table row.

This is not likely to be a parser bug for the partial index SQL. The error is
raised before `CREATE INDEX` SQL is parsed for the index object.

## Regression Test

The reproducer is now encoded as:

```text
core/mvcc/database/tests.rs::test_interrupted_drop_table_rolls_back_schema_table_and_indexes
```

Run it with:

```bash
cargo test -p turso_core test_interrupted_drop_table_rolls_back_schema_table_and_indexes -- --nocapture
```

On the buggy code, it fails at reopen with:

```text
Corrupt("sqlite_schema contains index for missing table 'repro_target': rootpage=37 sql=CREATE UNIQUE INDEX IF NOT EXISTS idx_repro_target_c0 ON repro_target (c0) WHERE c1 IS NULL")
```

## Reproducer Shape

The test uses a named `:memory:` database with shared `MemoryIO`.

It performs these steps:

1. Enable MVCC.
2. Create 17 dummy table/index pairs so the target index lands on rootpage 37.
3. Create the target table:

   ```sql
   CREATE TABLE repro_target(c0 INTEGER, c1 REAL);
   ```

4. Create the target partial index:

   ```sql
   CREATE UNIQUE INDEX IF NOT EXISTS idx_repro_target_c0
     ON repro_target (c0)
     WHERE c1 IS NULL;
   ```

5. Run `PRAGMA wal_checkpoint(TRUNCATE)` so the schema rows are checkpointed
   and have positive rootpages:

   ```text
   table repro_target rootpage=36
   index idx_repro_target_c0 rootpage=37
   ```

6. Prepare and step:

   ```sql
   DROP TABLE repro_target;
   ```

7. Inject a yield at the 35th `CursorYieldPoint::NextStart`, which lands after
   the `repro_target` table row has been deleted from `sqlite_schema`, but
   before the `idx_repro_target_c0` row is deleted.

8. Run another same-connection statement:

   ```sql
   SELECT 1;
   ```

9. Drop the suspended `DROP TABLE` statement.
10. Drop the connection/database handles, then reopen the named `:memory:` DB.

The reopen fails because schema reconstruction sees this state:

```text
index|idx_repro_target_c0|repro_target|37|CREATE UNIQUE INDEX IF NOT EXISTS idx_repro_target_c0 ON repro_target (c0) WHERE c1 IS NULL
```

with no matching:

```text
table|repro_target|repro_target|36|...
```

## Important Controls

The behavior depends on the same-connection statement that runs while
`DROP TABLE` is paused.

Observed controls:

- If the yielded `DROP TABLE` is resumed without running another statement, it
  completes cleanly.
- If the yielded `DROP TABLE` is dropped without running another statement, the
  statement reset rolls back cleanly.
- If a second connection observes while the `DROP TABLE` is paused, it sees the
  pre-drop table and index rows and stays clean.
- If the same connection runs even a harmless `SELECT 1` while the `DROP TABLE`
  is paused, dropping the suspended statement leaves the table row deleted and
  the index row present.

So the minimal bad pattern is:

```text
same connection:
  step DROP TABLE until injected IO yield
  run another statement
  drop suspended DROP TABLE statement
  reopen
```

## Code Path To The Error

`DROP TABLE` is translated in `core/translate/schema.rs`. It scans
`sqlite_schema` and deletes rows whose `tbl_name` matches the dropped table,
which should include the table row and all index rows.

The corruption is detected during schema reconstruction:

1. Schema rows are read from `sqlite_schema`.
2. Table rows are added immediately.
3. Index rows are collected as `UnparsedFromSqlIndex`.
4. `Schema::populate_indices()` looks up the table by `sqlite_schema.tbl_name`.
5. If the table row is missing, it raises:

   ```text
   sqlite_schema contains index for missing table ...
   ```

The relevant check is in `core/schema.rs` inside `Schema::populate_indices()`.
The table name used for the lookup comes from the `tbl_name` column of
`sqlite_schema`; it is not taken from parsing the `ON repro_target` clause in
the index SQL. `Index::from_sql()` is only called after the table lookup
succeeds.

## Current Hypothesis

The suspended `DROP TABLE` has already applied a schema-row tombstone for the
table row in MVCC state. Running another statement on the same connection while
the write statement is suspended appears to disturb the transaction/statement
cleanup path. When the original `DROP TABLE` statement is dropped, reset/abort no
longer restores the table-row tombstone, but the index-row delete had not yet
run. The result is an MVCC logical schema state with:

```text
table row deleted
index row still present
```

After reopen, MVCC recovery/schema rebuild merges that state and fails exactly
as reported.

The rootpage is positive in this reproducer because the schema was checkpointed
before the interrupted `DROP TABLE`. Without the checkpoint, the same shape can
be reproduced with negative MVCC table ids instead of positive rootpages.
