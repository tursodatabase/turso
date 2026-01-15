# CDC txn_id feature

This document describes specification for txn_id feature in the CDC

## Motivation

txn_id can help users to detect transaction boundaries
this will be especially useful for the sync engine - as it can split local updates in chunks by transaction boundaries (otherwise, updates of the remote will lead to inconsistent state or even can fail due to some constraints violation)

## Design

1. cdc table will be extended with `change_txn_id` integer field
2. `change_txn_id` should be an unique i64 integer (preferably small if possible - in order to save on space usage) for every transaction which represented in the CDC
   - So, `change_txn_id` sequence can "roll-over" if CDC table is truncated
3. Internally, for the first iteration, `change_txn_id` will be generated as rowid of the first change within transaction in the CDC table
   - In order to do that, we add `conn_txn_id(txn_id_if_not_set)` function which get or set current CDC transaction id for the connection and return transaction id value after that
   - We must use this function in execution plans near with the `unixtime()` function usage
4. We must extend connection object with `cdc_transaction_id` atomic field which will be set to -1 initially (unset value) and can be updated through `conn_txn_id` method or in the `COMMIT/ROLLBACK` path - so that next transaction will properly set this field to a new value
   - Note, that we have concurrent writes and they have its own `change_txn_id`. We do not rely on it in CDC
5. We also add `is_autocommit()` SQL function that returns 1 if the connection is in autocommit mode, 0 otherwise
   - This function is used in the execution plan to conditionally emit COMMIT records for write statements
   - The conditional check happens at execution time, allowing prepared statements to work correctly in both autocommit and transaction contexts
6. We also must extend `change`s options (INSERT/UPDATE/DELETE) with one more new option - `COMMIT`
   - This new record will be added when transaction is `COMMITed` (either explicitly or in auto-commit mode)
   - For explicit COMMIT statements, the COMMIT record is always emitted
   - For write statements (INSERT/UPDATE/DELETE/etc), the COMMIT record is conditionally emitted using `is_autocommit()` in the execution plan
   - Only `change_id`, `change_time`, `change_type` and `change_txn_id` fields must be set in this case
   - Properly adjust current code to skip this record for now (mostly sync engine)

## Code pointers

- CLI manual: ./cli/manuals/cdc.md
- Manual: ./docs/manual.md
- Entrypoints:
  - ./core/lib.rs (CaptureDataChangesMode)
  - ./core/translate/pragma.rs (turso_cdc_table_columns)
  - ./core/translate/emitter.rs (emit_cdc... methods)
- Sync engine:
  - Database tape: ./sync/engine/src/database_tape.rs
  - Database replay: ./sync/engine/src/database_replay_generator.rs
  - Operations: ./sync/engine/src/database_sync_operations.rs (push_logical_changes and maybe something else)
  - Sync engine: ./sync/engine/src/database_sycn_engine.rs (apply_changes_internal and maybe something else)
