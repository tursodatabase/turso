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
    vdbe::{builder::ProgramBuilder, insn::Insn},
};

use super::{
    emit_delete_child_repairs, emit_delete_parent_actions, emit_delete_parent_checks,
    emit_index_delete, emit_index_key, ExpressionEmitter, OpenedIndex, PhysicalExpressionError,
    PhysicalForeignKeyError, PhysicalIndexError, PreparedTriggers, RegisterId, RegisterRange,
    RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalMutationError {
    Expression(PhysicalExpressionError),
    Index(PhysicalIndexError),
    ForeignKey(PhysicalForeignKeyError),
}

impl fmt::Display for PhysicalMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expression(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::ForeignKey(error) => error.fmt(formatter),
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
        emit_delete_parent_checks(
            program,
            &foreign_keys.incoming,
            table,
            old_columns.expect("incoming foreign keys require the frozen OLD row"),
            rowid,
        )?;
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
