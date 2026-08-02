//! Shared per-row mutation effects for explicit and implicit HIR writes.
//!
//! Root emitters decide which rows to change and construct their OLD/NEW row
//! images. This module owns the ordered storage and foreign-key effects once a
//! row mutation has been chosen.

use std::fmt;

use crate::{
    schema::BTreeTable,
    sync::Arc,
    translate::semantic::hir::{self, Expr},
    vdbe::{
        builder::ProgramBuilder,
        insn::{CmpInsFlags, InsertFlags, Insn},
    },
};

use super::{
    emit_delete_child_repairs, emit_delete_parent_actions, emit_delete_parent_checks,
    emit_index_delete, emit_index_insert, emit_index_key, emit_replace_parent_checks,
    emit_update_child_checks, emit_update_parent_actions, emit_update_parent_checks,
    emit_update_parent_repairs, CursorId, ExpressionEmitter, IndexKey, OpenedIndex,
    PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError, PreparedTriggers,
    RegisterId, RegisterRange, RuntimeBindingError, RuntimeBindings, SourceRuntime,
};

#[derive(Debug)]
pub(crate) enum PhysicalMutationError {
    Expression(PhysicalExpressionError),
    Index(PhysicalIndexError),
    ForeignKey(PhysicalForeignKeyError),
    Runtime(RuntimeBindingError),
}

impl fmt::Display for PhysicalMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expression(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::ForeignKey(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for PhysicalMutationError {}

impl From<PhysicalExpressionError> for PhysicalMutationError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

impl From<PhysicalIndexError> for PhysicalMutationError {
    fn from(error: PhysicalIndexError) -> Self {
        Self::Index(error)
    }
}

impl From<PhysicalForeignKeyError> for PhysicalMutationError {
    fn from(error: PhysicalForeignKeyError) -> Self {
        Self::ForeignKey(error)
    }
}

impl From<RuntimeBindingError> for PhysicalMutationError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

#[derive(Clone, Copy)]
struct ReplacementRow {
    columns: RegisterRange,
    rowid: RegisterId,
}

pub(crate) fn freeze_cursor_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    width: usize,
) -> Result<RegisterRange, PhysicalMutationError> {
    let columns = RegisterRange::new(program.alloc_registers(width), width);
    for position in 0..width {
        ExpressionEmitter::new(program, bindings).emit_into(
            &Expr::column(source, position),
            RegisterRange::new(columns.first.0 + position, 1),
        )?;
    }
    Ok(columns)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_delete_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<BTreeTable>,
    indexes: &[OpenedIndex<'_>],
    rowid: RegisterId,
    old_columns: Option<RegisterRange>,
    foreign_keys: &hir::DmlForeignKeys,
) -> Result<(), PhysicalMutationError> {
    prepare_delete_row_inner(
        program,
        bindings,
        source,
        table,
        indexes,
        rowid,
        old_columns,
        foreign_keys,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_delete_row_inner(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<BTreeTable>,
    indexes: &[OpenedIndex<'_>],
    rowid: RegisterId,
    old_columns: Option<RegisterRange>,
    foreign_keys: &hir::DmlForeignKeys,
    replacement: Option<ReplacementRow>,
) -> Result<(), PhysicalMutationError> {
    let mut old_keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        old_keys.push(emit_index_key(
            program, bindings, source, rowid, index, false,
        )?);
    }
    for (index, key) in indexes.iter().zip(&old_keys) {
        emit_index_delete(program, index, key);
    }
    if !foreign_keys.outgoing.is_empty() {
        emit_delete_child_repairs(
            program,
            &foreign_keys.outgoing,
            table,
            old_columns.expect("outgoing foreign keys require the frozen OLD row"),
            rowid,
        )?;
    }
    if !foreign_keys.incoming.is_empty() {
        let old_columns = old_columns.expect("incoming foreign keys require the frozen OLD row");
        if let Some(replacement) = replacement {
            emit_replace_parent_checks(
                program,
                bindings,
                &foreign_keys.incoming,
                table,
                old_columns,
                replacement.columns,
                rowid,
                replacement.rowid,
            )?;
        } else {
            emit_delete_parent_checks(
                program,
                bindings,
                &foreign_keys.incoming,
                table,
                old_columns,
                rowid,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn finish_delete_row(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    cursor: usize,
    rowid: RegisterId,
    old_columns: Option<RegisterRange>,
    foreign_keys: &hir::DmlForeignKeys,
    prepared: &PreparedTriggers,
) -> Result<(), PhysicalMutationError> {
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
    if !foreign_keys.incoming.is_empty() {
        emit_delete_parent_actions(
            program,
            &foreign_keys.incoming,
            table,
            old_columns.expect("incoming foreign keys require the frozen OLD row"),
            rowid,
            prepared,
        )?;
    }
    Ok(())
}

/// Delete a row found by a REPLACE conflict using the same physical operation
/// as an explicit DELETE. The following insert or update repairs any deferred
/// NO ACTION debt for parent keys that it restores.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_replace_conflicting_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<BTreeTable>,
    cursor: usize,
    indexes: &[OpenedIndex<'_>],
    conflicting_rowid: RegisterId,
    return_to_rowid: Option<RegisterId>,
    not_found: crate::vdbe::BranchOffset,
    foreign_keys: &hir::DmlForeignKeys,
    replacement_columns: RegisterRange,
    replacement_rowid: RegisterId,
    prepared: &PreparedTriggers,
) -> Result<(), PhysicalMutationError> {
    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: conflicting_rowid.0,
        target_pc: not_found,
    });
    let proposed = bindings.replace_source(source, SourceRuntime::Cursor(CursorId(cursor)))?;
    let old_columns = freeze_cursor_row(program, bindings, source, table.columns().len())?;
    prepare_delete_row_inner(
        program,
        bindings,
        source,
        table,
        indexes,
        conflicting_rowid,
        Some(old_columns),
        foreign_keys,
        Some(ReplacementRow {
            columns: replacement_columns,
            rowid: replacement_rowid,
        }),
    )?;
    finish_delete_row(
        program,
        table,
        cursor,
        conflicting_rowid,
        Some(old_columns),
        foreign_keys,
        prepared,
    )?;
    bindings.replace_source(source, proposed)?;
    if let Some(rowid) = return_to_rowid {
        program.emit_insn(Insn::SeekRowid {
            cursor_id: cursor,
            src_reg: rowid.0,
            target_pc: not_found,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_replace_unique_check(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<BTreeTable>,
    table_cursor: usize,
    indexes: &[OpenedIndex<'_>],
    opened: &OpenedIndex<'_>,
    key: &IndexKey,
    current_rowid: Option<RegisterId>,
    not_found: crate::vdbe::BranchOffset,
    foreign_keys: &hir::DmlForeignKeys,
    replacement_columns: RegisterRange,
    replacement_rowid: RegisterId,
    prepared: &PreparedTriggers,
) -> Result<(), PhysicalMutationError> {
    if !opened.index.unique {
        return Ok(());
    }
    let done = program.allocate_label();
    if let Some(predicate) = key.predicate {
        program.emit_insn(Insn::IfNot {
            reg: predicate,
            target_pc: done,
            jump_if_null: true,
        });
    }
    program.emit_insn(Insn::NoConflict {
        cursor_id: opened.cursor,
        target_pc: done,
        record_reg: key.start,
        num_regs: key.columns,
    });
    let conflicting_rowid = RegisterId(program.alloc_register());
    program.emit_insn(Insn::IdxRowId {
        cursor_id: opened.cursor,
        dest: conflicting_rowid.0,
    });
    if let Some(current_rowid) = current_rowid {
        program.emit_insn(Insn::Eq {
            lhs: current_rowid.0,
            rhs: conflicting_rowid.0,
            target_pc: done,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
    }
    emit_replace_conflicting_row(
        program,
        bindings,
        source,
        table,
        table_cursor,
        indexes,
        conflicting_rowid,
        current_rowid,
        not_found,
        foreign_keys,
        replacement_columns,
        replacement_rowid,
        prepared,
    )?;
    program.preassign_label_to_next_insn(done);
    Ok(())
}

pub(crate) fn prepare_update_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    table: &BTreeTable,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
    foreign_keys: &hir::DmlForeignKeys,
) -> Result<(), PhysicalMutationError> {
    if !foreign_keys.outgoing.is_empty() {
        emit_update_child_checks(
            program,
            &foreign_keys.outgoing,
            table,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )?;
    }
    if !foreign_keys.incoming.is_empty() {
        emit_update_parent_checks(
            program,
            bindings,
            &foreign_keys.incoming,
            table,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn finish_update_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    table: &BTreeTable,
    cursor: usize,
    indexes: &[OpenedIndex<'_>],
    old_keys: &[IndexKey],
    new_keys: &[IndexKey],
    record: usize,
    old_columns: RegisterRange,
    new_columns: RegisterRange,
    old_rowid: RegisterId,
    new_rowid: RegisterId,
    foreign_keys: &hir::DmlForeignKeys,
    prepared: &PreparedTriggers,
) -> Result<(), PhysicalMutationError> {
    for (index, key) in indexes.iter().zip(old_keys) {
        emit_index_delete(program, index, key);
    }
    let table_delete_done = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: old_rowid.0,
        rhs: new_rowid.0,
        target_pc: table_delete_done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(table_delete_done);
    for (index, key) in indexes.iter().zip(new_keys) {
        emit_index_insert(program, index, key)?;
    }
    let overwrite_row = program.allocate_label();
    let table_insert_done = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: old_rowid.0,
        rhs: new_rowid.0,
        target_pc: overwrite_row,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: new_rowid.0,
        record_reg: record,
        flag: InsertFlags::new()
            .require_seek()
            .update_rowid_change()
            .skip_last_rowid(),
        table_name: table.name.clone(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: table_insert_done,
    });
    program.preassign_label_to_next_insn(overwrite_row);
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: new_rowid.0,
        record_reg: record,
        flag: InsertFlags::new().skip_last_rowid(),
        table_name: table.name.clone(),
    });
    program.preassign_label_to_next_insn(table_insert_done);
    if !foreign_keys.incoming.is_empty() {
        emit_update_parent_repairs(
            program,
            bindings,
            &foreign_keys.incoming,
            table,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
        )?;
        emit_update_parent_actions(
            program,
            &foreign_keys.incoming,
            table,
            old_columns,
            new_columns,
            old_rowid,
            new_rowid,
            prepared,
        )?;
    }
    Ok(())
}
