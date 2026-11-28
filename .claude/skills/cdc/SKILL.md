---
name: cdc
description: Change Data Capture (PRAGMA capture_data_changes_conn, the turso_cdc table) - schema versions, COMMIT records, MVCC behavior, how the sync engine reads it. Use when changing CDC or code that writes CDC rows.
---
# CDC (Change Data Capture)

CDC is per connection: `PRAGMA capture_data_changes_conn(...)` makes each write also insert a row into a CDC table (default name `turso_cdc`).

## Schema versions

- v1 columns: change_id, change_time, change_type, table_name, id, before, after, updates.
- v2 columns: change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates. v2 is the current version.
- change_type is 1 (insert), 0 (update), -1 (delete), 2 (COMMIT record, v2 only).
- The version of each CDC table is a row in the `turso_cdc_version` table (columns table_name, version).
- `turso_cdc_version` has one row for each spelling of the table name that the pragma got (`mycdc`, `MYCDC`). A new spelling takes the version of a row that matches without case. `DROP TABLE` removes the rows of all spellings.
- When the pragma is enabled and the CDC table already exists without a version row, the version is set to v1. A new CDC table gets the requested version (v2 by default).
- Writes to the CDC table and to `turso_cdc_version` are not captured.

## COMMIT records (v2)

- A statement running in autocommit mode ends by inserting one COMMIT record, and only when it captured something.
- Only the root statement writes it. Trigger and foreign key action subprograms do not.
- change_txn_id comes from the connection's CDC transaction id, which is reset to -1 after commit.

## Pragma changes apply at Halt

`PRAGMA capture_data_changes_conn` stores the new setting on the running statement. It is copied to the connection only when the statement halts after a successful commit, so a failed or rolled back statement leaves the old setting.

## MVCC

- CDC works under MVCC.
- In MVCC mode change ids come from the CDC table's AUTOINCREMENT sequence, so they are not reused after rows are pruned. In WAL mode the rowid is max rowid + 1.
- `PRAGMA journal_mode` returns an error while CDC is on for the connection. Turn CDC off first.

## Sync engine

The sync engine (`sync/engine/`) reads the CDC table. With `ignore_schema_changes` set (the default), it skips changes whose table_name is `sqlite_schema`.
