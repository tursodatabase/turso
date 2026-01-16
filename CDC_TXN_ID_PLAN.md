# CDC Transaction ID Implementation Plan

## Overview

This plan implements transaction boundary tracking for CDC (Change Data Capture) by adding a `change_txn_id` field to CDC records and a new COMMIT record type. This enables the sync engine to reconstruct transaction boundaries from CDC logs.

## Spec Summary

Based on CDC_TXN_ID_SPEC.md:

- Add `change_txn_id` INTEGER column to CDC table
- Generate unique transaction ID as rowid of first CDC change in transaction
- Add `conn_txn_id(txn_id_if_not_set_optional)` SQL function to manage transaction IDs
- Add connection-level `cdc_transaction_id` atomic field (AtomicI64, init to -1)
- Add COMMIT change type (emitted when transaction commits)
- COMMIT records only populate: change_id, change_time, change_type, change_txn_id
- Sync engine should skip COMMIT records (already implemented at lines 952 and 919)

## Key Design Decisions

1. **Function Implementation**: `conn_txn_id()` as ScalarFunc (like unixepoch, changes)
2. **COMMIT change_type**: Use integer value `2` (INSERT=1, UPDATE=0, DELETE=-1)
3. **COMMIT Emission**: Only if CDC enabled, in the execution plan which will commit transaction before COMMIT opcode
4. **Function Logic**: Atomic compare-and-swap: set to parameter if -1, else return current
5. **COMMIT Fields**: Only 4 fields populated (minimal overhead)
6. **Auto-commit**: COMMIT records emitted for single-statement transactions

## Implementation Phases

### Phase 1: Connection Infrastructure

**Task 1.1: Add cdc_transaction_id field to Connection struct**

- File: `/workspace/core/connection.rs`
- Location: Line ~91 (after vtab_txn_states field)
- Add field:
  ```rust
  /// CDC transaction ID for current transaction. -1 = unset, >= 0 = valid txn ID.
  /// Set to rowid of first CDC change in transaction, used to group changes by transaction.
  pub(super) cdc_transaction_id: AtomicI64,
  ```
- Location: Line ~768 (Connection::new initialization)
- Initialize: `cdc_transaction_id: AtomicI64::new(-1),`

**Task 1.2: Reset cdc_transaction_id on transaction end**

- Do this once in the `set_tx_state` method of connection (if it set to None)

### Phase 2: CDC Table Schema Extension

**Task 2.1: Add change_txn_id column to CDC table schema**

- File: `/workspace/core/translate/pragma.rs`
- Function: `turso_cdc_table_columns()`
- Location: After line 1013 (after updates column, before closing vec!)
- Add 9th column:
  ```rust
  ast::ColumnDefinition {
      col_name: ast::Name::exact("change_txn_id".to_string()),
      col_type: Some(ast::Type {
          name: "INTEGER".to_string(),
          size: None,
      }),
      constraints: vec![],
  },
  ```

**Task 2.2: Update sync engine DatabaseChange struct**

- File: `/workspace/sync/engine/src/types.rs`
- Location: Struct definition around line 249
- Add field: `pub change_txn_id: Option<i64>,`

**Task 2.3: Add Commit variant to DatabaseChangeType enum**

- File: `/workspace/sync/engine/src/types.rs`
- Location: Enum around line 108
- Add variant: `Commit,`

**Task 2.4: Update TryFrom<&turso_core::Row> for DatabaseChange**

- File: `/workspace/sync/engine/src/types.rs`
- Location: Around line 378
- Changes:
  1. Read 9th column (add after reading updates): `let change_txn_id = get_core_value_i64_or_null(row, 8)?;`
  2. Add to match statement (around line 366): `2 => DatabaseChangeType::Commit,`
  3. Add field to struct construction: `change_txn_id,`

**Task 2.5: Handle Commit type in DatabaseChange methods**

- File: `/workspace/sync/engine/src/types.rs`
- Locations: `into_apply()` and `into_revert()` methods
- Add case:
  ```rust
  DatabaseChangeType::Commit => {
      bail_parse_error!("COMMIT records should not be applied/reverted")
  }
  ```

### Phase 3: conn_txn_id() Function Implementation

**Task 3.1: Add ConnTxnId to ScalarFunc enum**

- File: `/workspace/core/function.rs`
- Location: ScalarFunc enum around line 389
- Add variant: `ConnTxnId,`

**Task 3.2: Mark function as non-deterministic**

- File: `/workspace/core/function.rs`
- Function: `ScalarFunc::is_deterministic()`
- Location: Add case in match around line 470
- Add: `ScalarFunc::ConnTxnId => false,`

**Task 3.3: Add function name mapping**

- File: `/workspace/core/function.rs`
- Function: `ScalarFunc::as_str()`
- Add case: `ScalarFunc::ConnTxnId => "conn_txn_id",`

**Task 3.4: Register function in resolver**

- File: `/workspace/core/function.rs`
- Function: `Func::resolve_function()`
- Location: Around line 813 (after unixepoch)
- Add case:
  ```rust
  "conn_txn_id" => {
      if arg_count > 1 {
          crate::bail_parse_error!(
              "wrong number of arguments to function {}()", name
          )
      }
      Ok(Self::Scalar(ScalarFunc::ConnTxnId))
  }
  ```

**Task 3.5: Implement function execution**

- File: `/workspace/core/vdbe/execute.rs`
- Function: `op_function`
- Location: In ScalarFunc match, around line 5035+ (after UnixEpoch case)
- Add implementation:

  ```rust
  ScalarFunc::ConnTxnId => {
      if arg_count > 1 {
          return Err(LimboError::InvalidArgument(
              "conn_txn_id requires exactly 1 argument".to_string()
          ));
      }
      ...
  }
  ```

### Phase 4: CDC Record Emission Updates

**Task 4.1: Update emit_cdc_insns to emit 9-field records with change_txn_id**

- File: `/workspace/core/translate/emitter.rs`
- Function: `emit_cdc_insns`
- Location: Lines 3275-3383

Changes required:

1. Line 3287: Change register allocation from 8 to 9:

   ```rust
   let turso_cdc_registers = program.alloc_registers(9);
   ```

2. After line 3357 (after emitting updates field), resolve and call conn_txn_id:

   ```rust
   // Allocate register for NewRowid early (needed for conn_txn_id call)
   let new_rowid_reg = program.alloc_register();
   program.emit_insn(Insn::NewRowid {
       cursor: cdc_cursor_id,
       rowid_reg: new_rowid_reg,
       prev_largest_reg: 0,
   });

   // Call conn_txn_id(new_rowid_reg) to get/set transaction ID
   let Some(conn_txn_id_fn) = resolver.resolve_function("conn_txn_id", 1) else {
       bail_parse_error!("no function {}", "conn_txn_id");
   };
   let conn_txn_id_fn_ctx = crate::function::FuncCtx {
       func: conn_txn_id_fn,
       arg_count: 1,
   };
   program.emit_insn(Insn::Function {
       constant_mask: 0,
       start_reg: new_rowid_reg,
       dest: turso_cdc_registers + 8,
       func: conn_txn_id_fn_ctx,
   });
   ```

3. Remove duplicate NewRowid allocation (currently at line 3359-3364)

4. Line 3369: Update MakeRecord count from 8 to 9:

   ```rust
   count: to_u16(9),
   ```

5. Update Insert instruction to use new_rowid_reg (renamed from rowid_reg)

### Phase 5: COMMIT Record Emission

**Task 5.1: Create helper to emit COMMIT CDC records**

- File: `/workspace/core/vdbe/mod.rs` (add near commit_txn function)
- New function:

  ```rust
  /// Emit a COMMIT record to CDC table when transaction commits successfully.
  /// Only emits if CDC is enabled and transaction had CDC changes.
  fn emit_cdc_commit_record(connection: &Connection) -> Result<()> {
      // Check if CDC is enabled
      let cdc_mode = connection.capture_data_changes.read();
      let cdc_table_name = match cdc_mode.table() {
          Some(table) => table.to_string(),
          None => return Ok(()), // CDC not enabled
      };
      drop(cdc_mode);

      // Get current transaction ID
      let txn_id = connection.cdc_transaction_id.load(Ordering::SeqCst);
      if txn_id == -1 {
          // No CDC changes in this transaction, skip COMMIT record
          return Ok(());
      }

      // Insert COMMIT record: (NULL, unixepoch(), 2, NULL, NULL, NULL, NULL, NULL, txn_id)
      // Only change_id (auto), change_time, change_type (2=COMMIT), and change_txn_id are set
      let sql = format!(
          "INSERT INTO {} (change_time, change_type, change_txn_id) VALUES (unixepoch(), 2, ?)",
          cdc_table_name
      );

      // Execute as internal statement (auto-commits immediately after prior commit)
      connection.execute(&sql, vec![Value::Integer(txn_id)])?;

      Ok(())
  }
  ```

**Task 5.2: Call COMMIT emission in normal write transaction commit**

- File: `/workspace/core/vdbe/mod.rs`
- Function: `step_end_write_txn`
- Location: After line 1342 (in Done branch, before setting changes and tx state)
- Add:
  ```rust
  if !rollback {
      // Emit COMMIT CDC record before resetting transaction state
      emit_cdc_commit_record(connection)?;
  }
  ```

**Task 5.3: Call COMMIT emission in MVCC commit path**

- File: `/workspace/core/vdbe/mod.rs`
- Function: `commit_txn`
- Location: After line 1252 (after successful step_end_mvcc_txn, before setting tx state)
- Add: `emit_cdc_commit_record(&conn)?;`

**Task 5.4: Move cdc_transaction_id reset to AFTER COMMIT record**

- Important: COMMIT record needs the txn_id value
- Move all `cdc_transaction_id.store(-1)` calls from Task 1.2 to AFTER the emit_cdc_commit_record calls
- For rollback paths, reset remains immediate (no COMMIT record)

### Phase 6: Testing

**Task 6.1: Basic transaction boundary test**

- File: `/workspace/turso-test-runner/turso-tests/cdc_txn_id_basic.sqltest`
- Content:

  ```sql
  @database :memory:

  test basic-txn-id {
      PRAGMA unstable_capture_data_changes_conn=full;
      CREATE TABLE t (x INT);

      BEGIN;
      INSERT INTO t VALUES (1);
      INSERT INTO t VALUES (2);
      COMMIT;

      BEGIN;
      INSERT INTO t VALUES (3);
      COMMIT;

      -- Check transaction IDs
      SELECT change_type, table_name,
             COUNT(*) OVER (PARTITION BY change_txn_id) as changes_in_txn
      FROM turso_cdc
      ORDER BY change_id;
  }
  expect {
      1|t|3
      1|t|3
      2||3
      1|t|2
      2||2
  }
  ```

**Task 6.2: Test auto-commit mode**

- Test that single statements get COMMIT records with unique txn_ids

**Task 6.3: Test rollback (no COMMIT record)**

- Verify ROLLBACK doesn't emit COMMIT, and resets cdc_transaction_id

**Task 6.4: Test empty transaction**

- BEGIN; COMMIT; should not emit COMMIT record (no CDC changes)

**Task 6.5: Test transaction ID uniqueness**

- Verify same txn_id for all changes in one transaction
- Verify different txn_ids across transactions

**Task 6.6: Test mixed DML operations**

- INSERT, UPDATE, DELETE in same transaction all get same txn_id

**Task 6.7: Integration test with sync engine**

- Verify sync engine skips COMMIT records correctly (should already pass)

### Phase 7: Documentation

**Task 7.1: Update CDC CLI manual**

- File: `/workspace/cli/manuals/cdc.md`
- Add section documenting:
  - `change_txn_id` column (transaction boundary tracking)
  - COMMIT record type (change_type=2)
  - Transaction reconstruction using change_txn_id

**Task 7.2: Update main manual**

- File: `/workspace/docs/manual.md`
- Document `conn_txn_id()` function in functions section

## Critical Files Summary

1. **core/connection.rs** - Add cdc_transaction_id field (line ~91)
2. **core/translate/emitter.rs** - Update emit_cdc_insns for 9 fields (lines 3275-3383)
3. **core/vdbe/mod.rs** - Reset txn_id, emit COMMIT records (lines 1210-1357)
4. **core/function.rs** - Define and register conn_txn_id function (lines 326-813)
5. **core/vdbe/execute.rs** - Implement conn_txn_id execution (line ~5035)
6. **core/translate/pragma.rs** - Extend CDC schema (lines 946-1017)
7. **sync/engine/src/types.rs** - Update DatabaseChange struct (lines 104-378)

## Implementation Order

**Must follow this order:**

1. Phase 1 (Connection infrastructure) - No dependencies
2. Phase 3 (Function implementation) - Requires Phase 1
3. Phase 2 (Schema extension) - Can be parallel with Phase 3
4. Phase 4 (CDC emission) - Requires Phases 1, 2, 3
5. Phase 5 (COMMIT records) - Requires all above
6. Phase 6 (Testing) - Requires all above
7. Phase 7 (Documentation) - Can be parallel with Phase 6

## Edge Cases & Correctness

1. **Empty transactions**: No CDC changes → cdc_transaction_id stays -1 → no COMMIT record
2. **Rollback**: Resets cdc_transaction_id to -1 immediately, no COMMIT record
3. **Nested statements**: Share same cdc_transaction_id (single connection)
4. **Auto-commit mode**: Each statement gets COMMIT record with unique txn_id
5. **Concurrent connections**: Each has separate cdc_transaction_id (thread-safe)
6. **Transaction ID uniqueness**: First CDC change's rowid ensures uniqueness
7. **MVCC vs non-MVCC**: Both paths handle reset and COMMIT emission

## Verification

After implementation, verify:

- [ ] All tests pass (cargo test)
- [ ] Format clean (cargo fmt)
- [ ] No clippy warnings (cargo clippy --deny=warnings)
- [ ] CDC table has 9 columns including change_txn_id
- [ ] COMMIT records appear after transactions
- [ ] Sync engine skips COMMIT records
- [ ] Transaction boundaries reconstructible from change_txn_id
- [ ] No COMMIT records for rolled-back transactions
- [ ] Empty transactions don't emit COMMIT records
