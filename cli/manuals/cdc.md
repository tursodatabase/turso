---
display_name: "Change Data Capture"
---

# CDC - Change Data Capture

## Overview

Change Data Capture (CDC) allows you to track and capture all data changes (inserts, updates, deletes) made to your database tables. This is useful for building reactive applications, syncing data between systems, replication, auditing, and more.

**Note:** This feature is currently marked as unstable, meaning the API is subject to change in future versions. The functionality itself is reliable for use.

## Enabling CDC

CDC is enabled per connection using the PRAGMA command:

```sql
PRAGMA unstable_capture_data_changes_conn('<mode>[,<table_name>]');
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
PRAGMA unstable_capture_data_changes_conn('id');
```

### Using Different Modes

Capture the state before changes:
```sql
PRAGMA unstable_capture_data_changes_conn('before');
```

Capture the state after changes:
```sql
PRAGMA unstable_capture_data_changes_conn('after');
```

Capture complete change information:
```sql
PRAGMA unstable_capture_data_changes_conn('full');
```

### Custom CDC Table

Store changes in a custom table instead of the default `turso_cdc`:
```sql
PRAGMA unstable_capture_data_changes_conn('full,my_changes_table');
```

### Disable CDC

Turn off CDC for the current connection:
```sql
PRAGMA unstable_capture_data_changes_conn('off');
```

## CDC Table Structure

The CDC table (default name: `turso_cdc`) contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `change_id` | INTEGER | Auto-incrementing unique identifier for each change |
| `change_time` | INTEGER | Timestamp of the change (Unix epoch) |
| `change_type` | INTEGER | Type of change: 1 (INSERT), 0 (UPDATE), -1 (DELETE), 2 (COMMIT)* |
| `change_txn_id` | INTEGER | Transaction ID for grouping changes by transaction |
| `table_name` | TEXT | Name of the table that was changed |
| `id` | varies | Primary key/rowid of the changed row |
| `before` | BLOB | Row data before the change (for modes: before, full) |
| `after` | BLOB | Row data after the change (for modes: after, full) |
| `updates` | BLOB | Details of updated columns (for mode: full) |

\* COMMIT records (change_type=2) mark the end of a transaction. They are emitted:
  - After each statement in auto-commit mode
  - At the end of explicit transactions (when COMMIT is executed)
  - COMMIT records have NULL values for `table_name`, `id`, `before`, `after`, and `updates`
  - The `change_txn_id` of a COMMIT record matches the transaction it terminates

## Transaction Boundaries

The `change_txn_id` column allows you to group changes by transaction. All changes within the same transaction share the same transaction ID, enabling you to:

- Replay changes in transaction-consistent order
- Identify which changes were atomic
- Build transaction-aware syncing systems

### How Transaction IDs Work

- In explicit transactions (BEGIN/COMMIT), all changes get the same `change_txn_id`
- In auto-commit mode, each statement gets a unique transaction ID
- The transaction ID is typically the `change_id` of the first change in that transaction
- Rolled-back transactions do not appear in the CDC table

### Examples

Group changes by transaction:
```sql
-- Find all transactions and their change counts
SELECT change_txn_id, COUNT(*) as changes_in_txn
FROM turso_cdc
GROUP BY change_txn_id;

-- View all changes from a specific transaction
SELECT * FROM turso_cdc WHERE change_txn_id = 42;

-- Find multi-change transactions
SELECT change_txn_id, COUNT(*) as num_changes
FROM turso_cdc
GROUP BY change_txn_id
HAVING COUNT(*) > 1;
```

Replay changes in transaction order:
```sql
-- Get changes ordered by transaction, then by change order within transaction
SELECT change_txn_id, change_id, change_type, table_name
FROM turso_cdc
ORDER BY change_txn_id, change_id;
```

Detect transaction boundaries:
```sql
-- Identify where one transaction ends and another begins
SELECT
    change_id,
    change_txn_id,
    LAG(change_txn_id) OVER (ORDER BY change_id) as prev_txn_id,
    CASE
        WHEN change_txn_id != LAG(change_txn_id) OVER (ORDER BY change_id)
        THEN 'NEW_TRANSACTION'
        ELSE 'SAME_TRANSACTION'
    END as boundary
FROM turso_cdc
ORDER BY change_id;

-- Use COMMIT records to identify transaction boundaries
SELECT * FROM turso_cdc WHERE change_type = 2;  -- Show all COMMIT records

-- Get all changes with their transaction's COMMIT record
SELECT
    c1.change_id,
    c1.change_type,
    c1.table_name,
    c1.change_txn_id,
    c2.change_id as commit_change_id
FROM turso_cdc c1
LEFT JOIN turso_cdc c2
    ON c1.change_txn_id = c2.change_txn_id
    AND c2.change_type = 2
WHERE c1.table_name IS NOT NULL  -- Exclude COMMIT records from left side
ORDER BY c1.change_id;
```

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

-- View only COMMIT records
SELECT * FROM turso_cdc WHERE change_type = 2;

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
PRAGMA unstable_capture_data_changes_conn('full');

-- Make some changes
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1;
DELETE FROM users WHERE id = 2;

-- View the captured changes
SELECT change_type, table_name, id
FROM turso_cdc;

-- Results will show:
-- 1 (INSERT) for Alice
-- 1 (INSERT) for Bob
-- 0 (UPDATE) for Alice's email change
-- -1 (DELETE) for Bob
```

## Multiple Connections

Each connection can have its own CDC configuration:

```sql
-- Connection 1: Capture to 'audit_log' table
PRAGMA unstable_capture_data_changes_conn('full,audit_log');

-- Connection 2: Capture to 'sync_queue' table
PRAGMA unstable_capture_data_changes_conn('id,sync_queue');

-- Changes from Connection 1 go to 'audit_log'
-- Changes from Connection 2 go to 'sync_queue'
```

## Transactions

CDC respects transaction boundaries. Changes are only recorded when a transaction commits:

```sql
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
UPDATE users SET name = 'Charles' WHERE id = 3;
-- CDC table is not yet updated

COMMIT;
-- Now both the INSERT and UPDATE appear in the CDC table
```

If a transaction rolls back, no CDC entries are created for those changes.

## Schema Changes

CDC also tracks schema changes when using full mode:

```sql
PRAGMA unstable_capture_data_changes_conn('full');

CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
-- Recorded in CDC as change to sqlite_schema

DROP TABLE products;
-- Also recorded as a schema change
```