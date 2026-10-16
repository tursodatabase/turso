//! Change data capture for HIR INSERT, UPDATE and DELETE.
//!
//! When `PRAGMA capture_data_changes_conn` is on, each changed row is
//! written to the capture table as legacy writes it: an INSERT or DELETE
//! row with the after or before image, an UPDATE row with both images and,
//! in `full` mode, which columns changed. A statement in autocommit mode
//! then writes a COMMIT row. Semantic analysis resolves the capture table
//! and its change-id sequence (`hir::CaptureChanges`).

use crate::{
    schema::BTreeTable,
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_row, emit_cdc_statement_commit, CdcChangeIds,
            OperationMode,
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

/// The capture table, open for one statement.
pub(super) struct Capture<'a> {
    cursor: CursorID,
    change_ids: CdcChangeIds<'a>,
    /// The name of the table whose changes are captured.
    table_name: String,
}

impl<'a> Capture<'a> {
    /// Open the capture table when the statement captures the changes of
    /// `target`.
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
        program.begin_write_operation()?;
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(table.root_page),
            db: crate::MAIN_DB_ID,
        });
        // Like legacy, change ids come from the capture table's sequence
        // under MVCC.
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
        Ok(Some(Self {
            cursor,
            change_ids,
            table_name: target.name.clone(),
        }))
    }

    /// The before image of the row `table_cursor` is on, when the mode
    /// keeps it.
    pub(super) fn before_record(
        &self,
        program: &mut ProgramBuilder,
        table: &BTreeTable,
        table_cursor: CursorID,
        rowid: usize,
    ) -> Option<usize> {
        program.capture_data_changes_info().has_before().then(|| {
            emit_cdc_full_record(
                program,
                table.columns(),
                table_cursor,
                rowid,
                table.is_strict,
            )
        })
    }

    /// The after image of the row in registers `start + column`, whose
    /// rowid is in `rowid`, when the mode keeps it. The INTEGER PRIMARY KEY
    /// holds the rowid in the image, as in legacy.
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
            &self.change_ids,
            operation,
            self.cursor,
            rowid,
            before,
            after,
            updates,
            &self.table_name,
        )
    }

    /// At the end of the statement: in autocommit mode, write the COMMIT
    /// row. A trigger or foreign-key action leaves that to the statement
    /// that fired it.
    pub(super) fn emit_commit(&self, program: &mut ProgramBuilder) -> Result<()> {
        if program.flags.is_subprogram() {
            return Ok(());
        }
        emit_cdc_statement_commit(program, &self.change_ids, self.cursor)
    }
}
