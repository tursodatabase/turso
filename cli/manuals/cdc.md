---
display_name: "Change Data Capture"
---

# CDC - Change Data Capture

## Overview

Change Data Capture (CDC) allows you to track and capture all data changes (inserts, updates, deletes) made to your database tables. This is useful for building reactive applications, syncing data between systems, replication, auditing, and more.

## Enabling CDC

CDC is enabled per connection using the PRAGMA command:

```sql
PRAGMA capture_data_changes_conn('<mode>[,<table_name>]');
```

### Parameters

- **mode**: The capture mode (see below)
- **table_name**: Optional custom table name for storing changes (defaults to `turso_cdc`)

### Capture Modes

- **`off`**: Disable CDC for this connection
- **`id`**: Capture only the primary key/rowid of changed rows
- **`before`**: Capture row state before changes (for updates/deletes)
- **`after`**: Capture row state after changes (for inserts/updates)
- **`full`**: Capture both before and after states, plus update details

## Examples

### Basic Usage

Enable CDC with ID mode (captures primary keys only):
```sql
PRAGMA capture_data_changes_conn('id');
```

### Using Different Modes

Capture the state before changes:
```sql
PRAGMA capture_data_changes_conn('before');
```

Capture the state after changes:
```sql
PRAGMA capture_data_changes_conn('after');
```

Capture complete change information:
```sql
PRAGMA capture_data_changes_conn('full');
```

### Custom CDC Table

Store changes in a custom table instead of the default `turso_cdc`:
```sql
PRAGMA capture_data_changes_conn('full,my_changes_table');
```

### Disable CDC

Turn off CDC for the current connection:
```sql
PRAGMA capture_data_changes_conn('off');
```

## CDC Table Structure

The CDC table (default name: `turso_cdc`) contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `change_id` | INTEGER | Auto-incrementing unique identifier for each change |
| `change_time` | INTEGER | Timestamp of the change (Unix epoch) |
| `change_type` | INTEGER | Type of change: 1 (INSERT), 0 (UPDATE), -1 (DELETE), 2 (COMMIT) |
| `table_name` | TEXT | Name of the table that was changed (NULL for COMMIT records) |
| `id` | varies | Primary key/rowid of the changed row (NULL for COMMIT records) |
| `before` | BLOB | Row data before the change (for modes: before, full) |
| `after` | BLOB | Row data after the change (for modes: after, full) |
| `updates` | BLOB | Details of updated columns (for mode: full) |
| `change_txn_id` | INTEGER | Identifier grouping records that belong to the same transaction |

### Commit Records

In addition to row changes, the CDC table records transaction boundaries. A row
with `change_type = 2` (COMMIT) marks the end of a transaction. Commit records
carry no row data: their `table_name`, `id`, `before`, `after`, and `updates`
columns are all NULL, so they appear as mostly empty rows when you select from
the CDC table.

Every statement executed outside an explicit transaction commits on its own, so
each such statement is followed by its own COMMIT record. Inside an explicit
transaction (`BEGIN ... COMMIT`), a single COMMIT record is written when the
transaction commits.

All records belonging to the same transaction, including its COMMIT record,
share the same `change_txn_id` value (the `change_id` of the first record in
the transaction).

## Querying Changes

Once CDC is enabled, you can query the changes table like any other table:

```sql
-- View all captured changes
SELECT * FROM turso_cdc;

-- View only inserts
SELECT * FROM turso_cdc WHERE change_type = 1;

-- View only updates
SELECT * FROM turso_cdc WHERE change_type = 0;

-- View only deletes
SELECT * FROM turso_cdc WHERE change_type = -1;

-- View row changes without transaction boundary (COMMIT) records
SELECT * FROM turso_cdc WHERE change_type != 2;

-- View all changes made by a single transaction
SELECT * FROM turso_cdc WHERE change_txn_id = 5;

-- View changes for a specific table
SELECT * FROM turso_cdc WHERE table_name = 'users';

-- View recent changes (last hour)
SELECT * FROM turso_cdc
WHERE change_time > unixepoch() - 3600;
```

## Practical Example

```sql
-- Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT
);

-- Enable full CDC
PRAGMA capture_data_changes_conn('full');

-- Make some changes
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1;
DELETE FROM users WHERE id = 2;

-- View the captured changes
SELECT change_type, table_name, id, change_txn_id
FROM turso_cdc;

-- Results will show:
-- 1 (INSERT) for Alice, followed by a 2 (COMMIT) record
-- 1 (INSERT) for Bob, followed by a 2 (COMMIT) record
-- 0 (UPDATE) for Alice's email change, followed by a 2 (COMMIT) record
-- -1 (DELETE) for Bob, followed by a 2 (COMMIT) record
--
-- Each statement above runs in autocommit mode, so each one is its own
-- transaction and produces its own COMMIT record. The table_name and id
-- columns of COMMIT records are NULL.
```

## Multiple Connections

Each connection can have its own CDC configuration:

```sql
-- Connection 1: Capture to 'audit_log' table
PRAGMA capture_data_changes_conn('full,audit_log');

-- Connection 2: Capture to 'sync_queue' table
PRAGMA capture_data_changes_conn('id,sync_queue');

-- Changes from Connection 1 go to 'audit_log'
-- Changes from Connection 2 go to 'sync_queue'
```

## Transactions

CDC records are written in the same transaction as the changes themselves, so
rolling back a transaction also discards its CDC entries. When a transaction
commits, a single COMMIT record (`change_type = 2`) marks the boundary, and
every record in the transaction shares the same `change_txn_id`:

```sql
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
UPDATE users SET name = 'Charles' WHERE id = 3;
COMMIT;

-- The CDC table now contains the INSERT, the UPDATE, and one COMMIT record,
-- all with the same change_txn_id
```

If a transaction rolls back, no CDC entries remain for those changes.

## Schema Changes

CDC also tracks schema changes when using full mode:

```sql
PRAGMA capture_data_changes_conn('full');

CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
-- Recorded in CDC as change to sqlite_schema

DROP TABLE products;
-- Also recorded as a schema change
```