//! Change data capture for HIR INSERT, UPDATE and DELETE.
//!
//! When `PRAGMA capture_data_changes_conn` is on, each changed row is
//! written to the capture table: an INSERT or DELETE
//! row with the after or before image, an UPDATE row with both images and,
//! in `full` mode, which columns changed. A statement in autocommit mode
//! then writes a COMMIT row. Semantic analysis resolves the capture table
//! and its change-id sequence (`hir::CaptureChanges`).
//!
//! A TEMP table's changes are never captured. A statement on one still
//! writes the COMMIT row when a trigger or foreign-key action it fires
//! captures rows of another table.

use crate::{
    schema::BTreeTable,
    sync::Arc,
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_row, emit_cdc_statement_commit, CdcChangeIds,
            CdcCommitCursor, OperationMode,
        },
        semantic::hir,
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral},
        CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

/// Change data capture for one statement.
pub(super) struct Capture<'a> {
    table: Arc<BTreeTable>,
    change_ids: CdcChangeIds<'a>,
    /// The capture table's cursor, open when the target's own row changes
    /// are captured.
    rows: Option<CapturedRows>,
}

struct CapturedRows {
    cursor: CursorID,
    /// The name of the table whose changes are captured.
    table_name: String,
}

impl<'a> Capture<'a> {
    /// Resolve the capture table when the statement captures changes, and
    /// open it when the changes of `target` are captured.
    pub(super) fn open(
        program: &mut ProgramBuilder,
        capture: Option<&'a hir::CaptureChanges>,
        target: &BTreeTable,
    ) -> Result<Option<Self>> {
        let Some(capture) = capture else {
            return Ok(None);
        };
        if program.capture_data_changes_info().table().is_none() {
            return Err(LimboError::InternalError(
                "HIR captures changes while change data capture is off".into(),
            ));
        }
        let table = capture.table.value().require_btree()?;
        // Under MVCC, change ids come from the capture table's sequence.
        let change_ids = if program.is_mvcc_enabled() {
            let operation = capture.change_ids.as_ref().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "missing implicit AUTOINCREMENT sequence for CDC table \"{}\"",
                    table.name
                ))
            })?;
            CdcChangeIds::Sequence {
                database: crate::MAIN_DB_ID,
                backing_table: operation.backing_table.value().require_btree()?,
                sqlite_sequence: operation
                    .sqlite_sequence
                    .as_ref()
                    .map(|table| table.value().require_btree())
                    .transpose()?,
                name: &operation.normalized_name,
                sequence: operation.sequence.value(),
            }
        } else {
            CdcChangeIds::NewRowid
        };
        let rows = capture.target_rows.then(|| {
            program.begin_write_operation()?;
            let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(table.root_page),
                db: crate::MAIN_DB_ID,
            });
            Ok::<_, LimboError>(CapturedRows {
                cursor,
                table_name: target.name.clone(),
            })
        });
        Ok(Some(Self {
            table,
            change_ids,
            rows: rows.transpose()?,
        }))
    }

    /// The capture table, open for the target's row changes, when they are
    /// captured.
    pub(super) fn rows(&self) -> Option<Rows<'_, 'a>> {
        self.rows.as_ref().map(|rows| Rows {
            change_ids: &self.change_ids,
            cursor: rows.cursor,
            table_name: &rows.table_name,
        })
    }

    /// At the end of the statement: in autocommit mode, write the COMMIT
    /// row. A trigger or foreign-key action leaves that to the statement
    /// that fired it. A statement whose own rows are not captured writes
    /// it only when a trigger or foreign-key action it fired captured a
    /// row.
    pub(super) fn emit_commit(&self, program: &mut ProgramBuilder) -> Result<()> {
        if program.flags.is_subprogram() {
            return Ok(());
        }
        let has_commit_record = program
            .capture_data_changes_info()
            .as_ref()
            .is_some_and(|info| info.cdc_version().has_commit_record());
        if !has_commit_record {
            return Ok(());
        }
        let cursor = match &self.rows {
            Some(rows) => CdcCommitCursor::Open(rows.cursor),
            None if program.has_emitted_program_call() => {
                CdcCommitCursor::OpenWhenCaptured(self.table.clone())
            }
            None => return Ok(()),
        };
        emit_cdc_statement_commit(program, &self.change_ids, cursor)
    }
}

/// The capture table, open for the target's row changes.
pub(super) struct Rows<'c, 'a> {
    change_ids: &'c CdcChangeIds<'a>,
    cursor: CursorID,
    table_name: &'c str,
}

impl Rows<'_, '_> {
    /// The before image of the row `table_cursor` is on, when the mode
    /// keeps it.
    pub(super) fn before_record(
        &self,
        program: &mut ProgramBuilder,
        table: &BTreeTable,
        table_cursor: CursorID,
        rowid: usize,
    ) -> Option<usize> {
        program
            .capture_data_changes_info()
            .has_before()
            .then(|| emit_cdc_full_record(program, table.columns(), table_cursor, rowid))
    }

    /// The after image of the row in registers `start + column`, whose
    /// rowid is in `rowid`, when the mode keeps it. The INTEGER PRIMARY KEY
    /// holds the rowid in the image.
    pub(super) fn after_record(
        &self,
        program: &mut ProgramBuilder,
        table: &BTreeTable,
        start: usize,
        rowid: usize,
    ) -> Option<usize> {
        if !program.capture_data_changes_info().has_after() {
            return None;
        }
        let columns = table.columns().len();
        let image = match table
            .columns()
            .iter()
            .position(|column| column.is_rowid_alias())
        {
            Some(alias) => {
                let image = program.alloc_registers(columns);
                program.emit_insn(Insn::Copy {
                    src_reg: start,
                    dst_reg: image,
                    extra_amount: columns - 1,
                });
                program.emit_insn(Insn::Copy {
                    src_reg: rowid,
                    dst_reg: image + alias,
                    extra_amount: 0,
                });
                image
            }
            None => start,
        };
        Some(super::dml::emit_row_record(program, table, image))
    }

    /// Whether UPDATE rows record which columns changed.
    pub(super) fn records_updates(program: &ProgramBuilder) -> bool {
        program.capture_data_changes_info().has_updates()
    }

    /// Write the capture row of one changed row.
    pub(super) fn emit_row(
        &self,
        program: &mut ProgramBuilder,
        operation: OperationMode,
        rowid: usize,
        before: Option<usize>,
        after: Option<usize>,
        updates: Option<usize>,
    ) -> Result<()> {
        emit_cdc_row(
            program,
            self.change_ids,
            operation,
            self.cursor,
            rowid,
            before,
            after,
            updates,
            self.table_name,
        )
    }
}
