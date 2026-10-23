// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use crate::schema::{BTreeTable, Column, Schema};
use crate::translate::semantic::context::SemanticContext;
use crate::vdbe::{
    builder::{CursorType, ProgramBuilder},
    insn::{to_u32, InsertFlags, Insn},
};
use crate::{function::Func, sync::Arc, CaptureDataChangesExt, Result};

#[derive(Debug, Clone)]
/// Update row source for UPDATE statements
/// `Normal` is the default mode, it will iterate either the table itself or an index on the table.
/// `PrebuiltEphemeralTable` is used when an ephemeral table containing the target rowids to update has
/// been built and it is being used for iteration.
pub enum UpdateRowSource {
    /// Iterate over the table itself or an index on the table
    Normal,
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE(UpdateRowSource),
    DELETE,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sqlite always considers Read transactions implicit
pub enum TransactionMode {
    None,
    Read,
    Write,
    Concurrent,
}

/// Open the CDC table's cursor when the statement captures changes.
/// `database_id` is the database of the changed table: changes to temp
/// tables are not captured.
pub fn prepare_cdc_if_necessary(
    program: &mut ProgramBuilder,
    schema: &Schema,
    database_id: usize,
    changed_table_name: Option<&str>,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    if database_id == crate::TEMP_DB_ID {
        return Ok(None);
    }
    // Self-exclusion: never capture changes to CDC's own bookkeeping tables. `None` means the
    // caller has no associated table (e.g. a transaction-boundary COMMIT record) and always
    // gets the cursor.
    if let Some(changed_table_name) = changed_table_name {
        if changed_table_name == cdc_table
            || changed_table_name == crate::cdc::TURSO_CDC_VERSION_TABLE_NAME
        {
            return Ok(None);
        }
    }
    let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let Some(cdc_btree) = turso_cdc_table.btree() else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: cdc_btree.root_page.into(),
        db: crate::MAIN_DB_ID, // CDC table always lives in the main database
    });
    Ok(Some((cursor_id, cdc_btree)))
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
) -> usize {
    let storable_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
    let columns_reg = program.alloc_registers(storable_count + 1);
    let mut slot = 0;
    for (i, column) in columns.iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + slot,
                extra_amount: 0,
            });
        } else {
            program.emit_column_or_rowid(table_cursor_id, i, columns_reg + 1 + slot);
        }
        slot += 1;
    }
    let affinity_str = columns
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(columns_reg + 1),
        count: to_u32(storable_count),
        dest_reg: to_u32(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
}

#[allow(clippy::too_many_arguments)]
/// Allocate the rowid for a CDC row into `dest_reg`. The CDC table's `change_id`
/// column is `INTEGER PRIMARY KEY`, so the rowid IS the change id.
///
/// In MVCC journal mode the id is drawn from the CDC table's implicit
/// AUTOINCREMENT sequence. This makes change ids monotonic and never reused after
/// CDC rows are pruned, and registers each in-flight allocation with the MVCC
/// store so the sync push loop can call `sequence_watermark_experimental` to
/// avoid advancing the push watermark past a change id that a concurrent
/// transaction commits out of change-id order under snapshot isolation. In WAL
/// mode we keep the cheaper `NewRowid` (max rowid + 1) assignment; the WAL push
/// loop does not depend on the sequence watermark, so its insert path is
/// unchanged and pays no per-row sequence cost.
/// Where the change ids of CDC rows come from.
pub enum CdcChangeIds<'a> {
    /// WAL: one more than the CDC table's largest rowid.
    NewRowid,
    /// MVCC: the CDC table's AUTOINCREMENT sequence.
    Sequence {
        database: usize,
        backing_table: Arc<BTreeTable>,
        sqlite_sequence: Option<Arc<BTreeTable>>,
        name: &'a str,
        sequence: &'a crate::schema::Sequence,
    },
}

/// Run `emit` with the change ids found through `catalog`.
fn with_cdc_change_ids<T>(
    program: &mut ProgramBuilder,
    catalog: &SemanticContext<'_>,
    emit: impl FnOnce(&mut ProgramBuilder, &CdcChangeIds<'_>) -> Result<T>,
) -> Result<T> {
    if !program.is_mvcc_enabled() {
        return emit(program, &CdcChangeIds::NewRowid);
    }
    let Some(cdc_table) = program
        .capture_data_changes_info()
        .as_ref()
        .map(|info| info.table.clone())
    else {
        return Err(crate::LimboError::InternalError(
            "CDC change-id allocation requested without an active CDC config".to_string(),
        ));
    };
    let seq_name = crate::schema::autoincrement_sequence_name(&cdc_table);
    let seq = catalog
        .with_schema(crate::MAIN_DB_ID, |s| s.get_sequence(&seq_name).cloned())
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing implicit AUTOINCREMENT sequence for CDC table \"{cdc_table}\""
            ))
        })?;
    let backing_table = crate::translate::sequence::resolve_sequence_backing_table(
        catalog,
        crate::MAIN_DB_ID,
        &seq_name,
    )?;
    let sqlite_sequence = crate::translate::sequence::resolve_autoincrement_sqlite_sequence(
        catalog,
        crate::MAIN_DB_ID,
        &seq_name,
    );
    emit(
        program,
        &CdcChangeIds::Sequence {
            database: crate::MAIN_DB_ID,
            backing_table,
            sqlite_sequence,
            name: &seq_name,
            sequence: &seq,
        },
    )
}

fn emit_cdc_change_id(
    program: &mut ProgramBuilder,
    change_ids: &CdcChangeIds<'_>,
    cdc_cursor_id: usize,
    dest_reg: usize,
) -> Result<()> {
    match change_ids {
        CdcChangeIds::NewRowid => {
            program.emit_insn(Insn::NewRowid {
                cursor: cdc_cursor_id,
                rowid_reg: dest_reg,
                prev_largest_reg: 0,
            });
            Ok(())
        }
        CdcChangeIds::Sequence {
            database,
            backing_table,
            sqlite_sequence,
            name,
            sequence,
        } => crate::translate::sequence::emit_disk_read_nextval(
            program,
            *database,
            backing_table.clone(),
            sqlite_sequence.clone(),
            name,
            sequence,
            dest_reg,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_insns(
    program: &mut ProgramBuilder,
    catalog: &SemanticContext<'_>,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    with_cdc_change_ids(program, catalog, |program, change_ids| {
        emit_cdc_row(
            program,
            change_ids,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        )
    })
}

/// Write one CDC row for a changed row of `table_name`.
#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_row(
    program: &mut ProgramBuilder,
    change_ids: &CdcChangeIds<'_>,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    match cdc_info.map(|info| info.cdc_version()) {
        Some(crate::CdcVersion::V2) => emit_cdc_insns_v2(
            program,
            change_ids,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        Some(crate::CdcVersion::V1) => emit_cdc_insns_v1(
            program,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        None => Err(crate::LimboError::InternalError(
            "cdc info not set".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v1(
    program: &mut ProgramBuilder,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v1: (change_id, change_time, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(8);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        extra_amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0, // todo(sivukhin): properly set value here from sqlite_sequence table when AUTOINCREMENT will be properly implemented in Turso
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(8),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v2(
    program: &mut ProgramBuilder,
    change_ids: &CdcChangeIds<'_>,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v2: (change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(9);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    // change_txn_id = conn_txn_id(change_id)
    // First allocate the change id (the CDC rowid), then pass it to conn_txn_id
    // for get-or-set. In MVCC mode this draws from the CDC AUTOINCREMENT sequence
    // (see `emit_cdc_change_id`); in WAL mode it is a plain NewRowid.
    let candidate_reg = program.alloc_register();
    emit_cdc_change_id(program, change_ids, cdc_cursor_id, candidate_reg)?;
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: candidate_reg,
        dest: turso_cdc_registers + 2,
        func: conn_txn_id_fn_ctx,
    });

    // change_type
    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    // table_name
    program.emit_string8(table_name.to_string(), turso_cdc_registers + 4);
    program.mark_last_insn_constant();

    // id
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 5,
        extra_amount: 0,
    });

    // before
    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    // after
    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    // updates
    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 8,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 8, None);
        program.mark_last_insn_constant();
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: candidate_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a COMMIT record into the CDC table (v2 only).
/// change_type=2, all other data fields NULL.
pub fn emit_cdc_commit_insns(
    program: &mut ProgramBuilder,
    catalog: &SemanticContext<'_>,
    cdc_cursor_id: usize,
) -> Result<()> {
    with_cdc_change_ids(program, catalog, |program, change_ids| {
        emit_cdc_commit_row(program, change_ids, cdc_cursor_id)
    })
}

/// Write a CDC COMMIT record (v2 only).
fn emit_cdc_commit_row(
    program: &mut ProgramBuilder,
    change_ids: &CdcChangeIds<'_>,
    cdc_cursor_id: usize,
) -> Result<()> {
    // v2 COMMIT record: (NULL, unixepoch(), conn_txn_id(-1), 2, NULL, NULL, NULL, NULL, NULL)
    let regs = program.alloc_registers(9);
    // reg+0: NULL (change_id, autoincrement)
    program.emit_insn(Insn::Null {
        dest: regs,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // reg+1: change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: regs + 1,
        func: unixepoch_fn_ctx,
    });

    // reg+2: change_txn_id = conn_txn_id(-1)
    // Pass -1 as candidate: if a txn_id exists, return it; if not, -1 is stored (and will be reset).
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: regs + 2,
        func: conn_txn_id_fn_ctx,
    });

    // reg+3: change_type = 2 (COMMIT)
    program.emit_int(2, regs + 3);
    program.mark_last_insn_constant();

    // reg+4..8: NULL (table_name, id, before, after, updates)
    program.emit_insn(Insn::Null {
        dest: regs + 4,
        dest_end: Some(regs + 8),
    });
    program.mark_last_insn_constant();

    // Allocate the COMMIT record's change id from the same source as row records
    // (the CDC AUTOINCREMENT sequence in MVCC mode) so COMMIT and row change ids
    // stay in one monotonic, never-reused stream.
    let rowid_reg = program.alloc_register();
    emit_cdc_change_id(program, change_ids, cdc_cursor_id, rowid_reg)?;

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(regs),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit the CDC COMMIT record at the end of a schema statement, once, when in autocommit
/// mode (v2 only). Only the root statement writes it; a trigger or FK action subprogram
/// never does.
pub fn emit_cdc_autocommit_commit(
    program: &mut ProgramBuilder,
    catalog: &SemanticContext<'_>,
    cdc_cursor_id: usize,
) -> Result<()> {
    if program.flags.is_subprogram() {
        return Ok(());
    }
    let cdc_info = program.capture_data_changes_info().as_ref();
    if !cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        return Ok(());
    }
    with_cdc_change_ids(program, catalog, |program, change_ids| {
        emit_cdc_statement_commit(program, change_ids, CdcCommitCursor::Open(cdc_cursor_id))
    })
}

/// The CDC table cursor a statement's COMMIT record is written through.
pub(crate) enum CdcCommitCursor {
    /// The cursor the statement captured its own rows through.
    Open(usize),
    /// The statement's own table is not captured (a TEMP table). The CDC
    /// table is opened, and the record written, only when a trigger or FK
    /// action that ran during the statement captured a row.
    OpenWhenCaptured(Arc<BTreeTable>),
}

/// At the end of a statement in autocommit mode, write a CDC COMMIT record
/// (v2 only).
pub(crate) fn emit_cdc_statement_commit(
    program: &mut ProgramBuilder,
    change_ids: &CdcChangeIds<'_>,
    cursor: CdcCommitCursor,
) -> Result<()> {
    // Check if we're in autocommit mode; if so, emit a COMMIT record.
    let is_autocommit_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::IsAutocommit),
        arg_count: 0,
    };
    let autocommit_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: autocommit_reg,
        func: is_autocommit_fn_ctx,
    });

    // IfNot jumps when reg == 0 (not autocommit). Skip the COMMIT in that case.
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: autocommit_reg,
        target_pc: skip_label,
        jump_if_null: true,
    });

    let cdc_cursor_id = match cursor {
        CdcCommitCursor::Open(cdc_cursor_id) => cdc_cursor_id,
        CdcCommitCursor::OpenWhenCaptured(cdc_table) => {
            // `conn_txn_id(-1)` is -1 until a row is captured.
            let minus_one_reg = program.alloc_register();
            program.emit_int(-1, minus_one_reg);
            let txn_id_reg = program.alloc_register();
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: minus_one_reg,
                dest: txn_id_reg,
                func: crate::function::FuncCtx {
                    func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
                    arg_count: 1,
                },
            });
            program.emit_insn(Insn::Eq {
                lhs: txn_id_reg,
                rhs: minus_one_reg,
                target_pc: skip_label,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: None,
            });
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_table.clone()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: cdc_table.root_page.into(),
                db: crate::MAIN_DB_ID,
            });
            cursor_id
        }
    };

    emit_cdc_commit_row(program, change_ids, cdc_cursor_id)?;

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}

/// Emit the CDC COMMIT record for an explicit `COMMIT` statement, gated on the transaction
/// having actually captured a change.
///
/// Data-modifying statements always establish a write transaction before reaching their CDC
/// emission, but an explicit `COMMIT` does not: for an empty or read-only transaction the
/// connection's `tx_state` is still `None`/`Read`. Emitting the record unconditionally would
/// then dirty the CDC table page without a write transaction; the commit path neither flushes
/// nor clears that page, so it leaks into the next transaction and trips the "dirty pages
/// should be empty for read txn" assertion on a later ROLLBACK
/// (https://github.com/tursodatabase/turso/issues/7677).
///
/// `conn_txn_id(-1)` returns the active CDC transaction id, or -1 when nothing was captured.
/// When it is set, the transaction already performed a write (the data-change statement
/// established the write transaction), so inserting the commit record is safe. When it is -1
/// the transaction made no changes and we skip the record entirely, leaving the transaction
/// read-only.
pub fn emit_cdc_explicit_commit_insns(
    program: &mut ProgramBuilder,
    schema: &Schema,
    catalog: &SemanticContext<'_>,
) -> Result<()> {
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let txn_id_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: txn_id_reg,
        func: crate::function::FuncCtx {
            func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
            arg_count: 1,
        },
    });

    // Skip the whole record (including the CDC OpenWrite) when no change was captured.
    // `emit_cdc_commit_insns` recomputes `conn_txn_id(-1)` for the record itself; because the
    // opcode is an idempotent get-or-set, the second call returns the same value we gated on.
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: txn_id_reg,
        rhs: minus_one_reg,
        target_pc: skip_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    // The CDC record write needs a transaction; joins the open one (keeping its mode) if any.
    program.emit_insn(Insn::Transaction {
        db: crate::MAIN_DB_ID,
        tx_mode: TransactionMode::Write,
        schema_cookie: schema.schema_version,
    });

    // A COMMIT record has no associated table, so pass `None` (no self-exclusion check).
    if let Some((cdc_cursor_id, _)) =
        prepare_cdc_if_necessary(program, schema, crate::MAIN_DB_ID, None)?
    {
        emit_cdc_commit_insns(program, catalog, cdc_cursor_id)?;
    }

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}
