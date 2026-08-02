//! Direct lowering for DELETE roots whose complete write obligations are
//! already frozen in HIR.
//!
//! The first executable slice is intentionally narrow. It deletes from a
//! rowid B-tree only when HIR proves that no index, trigger, foreign-key, or
//! returned-row work exists. Unsupported obligations are rejected before the
//! write cursor is opened, so this layer never emits a partial mutation.

use std::fmt;

use crate::{
    schema::Table,
    translate::semantic::hir::{Expr, IndexCoverage, SourceKind},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_index_delete, emit_index_key, emit_returning_result, emit_returning_values,
    open_indexes, CursorId, ExpressionEmitter, PhysicalExpressionError, PhysicalIndexError,
    PhysicalPlan, PhysicalQueryError, PhysicalRoot, PhysicalSourceKind, RegisterId,
    RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalDeleteError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Index(PhysicalIndexError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalDeleteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical DELETE: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "physical DELETE is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalDeleteError {}

impl From<RuntimeBindingError> for PhysicalDeleteError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalDeleteError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

impl From<PhysicalIndexError> for PhysicalDeleteError {
    fn from(error: PhysicalIndexError) -> Self {
        Self::Index(error)
    }
}

type DeleteResult<T> = std::result::Result<T, PhysicalDeleteError>;

#[derive(Debug)]
pub(crate) enum PhysicalRootError {
    Query(PhysicalQueryError),
    Insert(super::PhysicalInsertError),
    Update(super::PhysicalUpdateError),
    Delete(PhysicalDeleteError),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => error.fmt(formatter),
            Self::Insert(error) => error.fmt(formatter),
            Self::Update(error) => error.fmt(formatter),
            Self::Delete(error) => error.fmt(formatter),
            Self::Unsupported(message) => {
                write!(formatter, "physical root is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalRootError {}

/// Emit any root currently supported by the catalog-free physical layer.
pub(crate) fn emit_root(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> Result<(), PhysicalRootError> {
    match plan.root {
        PhysicalRoot::Query(_) => {
            super::emit_root_query(plan, program).map_err(PhysicalRootError::Query)
        }
        PhysicalRoot::Delete(_) => {
            emit_root_delete(plan, program).map_err(PhysicalRootError::Delete)
        }
        PhysicalRoot::Insert(_) => {
            super::emit_root_insert(plan, program).map_err(PhysicalRootError::Insert)
        }
        PhysicalRoot::Update(_) => {
            super::emit_root_update(plan, program).map_err(PhysicalRootError::Update)
        }
        PhysicalRoot::TriggerPredicate(_) => {
            Err(PhysicalRootError::Unsupported("trigger predicate root"))
        }
    }
}

/// Emit one simple DELETE using only the closed HIR document.
pub(crate) fn emit_root_delete(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> DeleteResult<()> {
    let delete = match &plan.root {
        PhysicalRoot::Delete(delete) => *delete,
        _ => return Err(PhysicalDeleteError::Unsupported("non-DELETE HIR root")),
    };
    let (source, table, database) = preflight_delete(plan, delete)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    bindings.bind_source(delete.target, SourceRuntime::Cursor(CursorId(cursor)))?;

    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database,
    });
    let indexes = open_indexes(program, source, database)?;
    let rowid = RegisterId(program.alloc_register());
    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);
    if let Some(predicate) = &delete.predicate {
        let condition = ExpressionEmitter::new(program, &mut bindings).emit_new(predicate)?;
        if condition.width != 1 {
            return Err(PhysicalDeleteError::Invalid("WHERE result is not scalar"));
        }
        program.emit_insn(Insn::IfNot {
            reg: condition.first.0,
            target_pc: loop_next,
            jump_if_null: true,
        });
    }
    let returning = delete
        .returning
        .as_ref()
        .map(|returning| emit_returning_values(program, &mut bindings, returning))
        .transpose()?;
    program.emit_insn(Insn::RowId {
        cursor_id: cursor,
        dest: rowid.0,
    });
    let mut keys = Vec::with_capacity(indexes.len());
    for index in &indexes {
        keys.push(emit_index_key(
            program,
            &mut bindings,
            delete.target,
            rowid,
            index,
            false,
        )?);
    }
    for (index, key) in indexes.iter().zip(&keys) {
        emit_index_delete(program, index, key);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
    if let Some(result) = returning {
        emit_returning_result(program, result);
    }
    program.preassign_label_to_next_insn(loop_next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(loop_end);
    close_indexes(program, &indexes);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

fn preflight_delete<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    delete: &crate::translate::semantic::hir::Delete,
) -> DeleteResult<(
    &'plan crate::translate::semantic::hir::Source,
    crate::sync::Arc<crate::schema::BTreeTable>,
    usize,
)> {
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(PhysicalDeleteError::Unsupported("ORDER BY or LIMIT"));
    }
    if delete.trigger.is_some() || !delete.triggers.is_empty() {
        return Err(PhysicalDeleteError::Unsupported("trigger execution"));
    }
    if !delete.foreign_keys.outgoing.is_empty() || !delete.foreign_keys.incoming.is_empty() {
        return Err(PhysicalDeleteError::Unsupported("foreign-key actions"));
    }
    if delete.predicate.as_ref().is_some_and(contains_subquery) {
        return Err(PhysicalDeleteError::Unsupported("subquery in WHERE"));
    }

    let source = plan
        .document
        .source(delete.target)
        .ok_or(PhysicalDeleteError::Invalid("target source is missing"))?;
    if !matches!(source.kind, SourceKind::Table(_)) {
        return Err(PhysicalDeleteError::Invalid(
            "target is not a catalog table",
        ));
    }
    let IndexCoverage::Complete { indexes: _ } = &source.index_coverage else {
        return Err(PhysicalDeleteError::Invalid(
            "target does not carry complete index metadata",
        ));
    };
    if !source.index_method_patterns.is_empty() {
        return Err(PhysicalDeleteError::Unsupported("custom index methods"));
    }
    let physical_source = plan
        .source(delete.target)
        .ok_or(PhysicalDeleteError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access } = &physical_source.kind else {
        return Err(PhysicalDeleteError::Invalid(
            "physical target is not a catalog table",
        ));
    };
    if !matches!(access, TableAccess::Scan) {
        return Err(PhysicalDeleteError::Unsupported("indexed target scan"));
    }
    let database = table
        .database()
        .ok_or(PhysicalDeleteError::Invalid(
            "target has no database identity",
        ))?
        .index();
    let Table::BTree(table) = table.value() else {
        return Err(PhysicalDeleteError::Unsupported("non-B-tree target"));
    };
    if !table.has_rowid {
        return Err(PhysicalDeleteError::Unsupported("WITHOUT ROWID target"));
    }
    Ok((source, table.clone(), database))
}

fn contains_subquery(expression: &Expr) -> bool {
    let mut found = false;
    expression.walk(&mut |expression| found |= matches!(expression, Expr::Subquery(_)));
    found
}
