//! Direct lowering for DELETE roots whose complete write obligations are
//! already frozen in HIR.
//!
//! The first executable slice is intentionally narrow. It deletes from a
//! rowid B-tree only when HIR proves that no index, trigger, foreign-key, or
//! returned-row work exists. Unsupported obligations are rejected before the
//! write cursor is opened, so this layer never emits a partial mutation.

use std::fmt;

use turso_parser::ast::{RefAct, TriggerTime};

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
    emit_trigger_programs, open_indexes, record_from_cursor, CdcChange, CursorId,
    ExpressionEmitter, PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError,
    PhysicalPlan, PhysicalQueryError, PhysicalRoot, PhysicalSourceKind, PhysicalTriggerError,
    PreparedCdc, PreparedTriggers, RegisterId, RegisterRange, RootRuntimeInputs,
    RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess, TriggerRow, TriggerRows,
};

#[derive(Debug)]
pub(crate) enum PhysicalDeleteError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Index(PhysicalIndexError),
    Trigger(PhysicalTriggerError),
    ForeignKey(PhysicalForeignKeyError),
    Cdc(crate::LimboError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalDeleteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::Trigger(error) => error.fmt(formatter),
            Self::ForeignKey(error) => error.fmt(formatter),
            Self::Cdc(error) => error.fmt(formatter),
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

impl From<PhysicalTriggerError> for PhysicalDeleteError {
    fn from(error: PhysicalTriggerError) -> Self {
        Self::Trigger(error)
    }
}

impl From<PhysicalForeignKeyError> for PhysicalDeleteError {
    fn from(error: PhysicalForeignKeyError) -> Self {
        Self::ForeignKey(error)
    }
}

impl From<crate::LimboError> for PhysicalDeleteError {
    fn from(error: crate::LimboError) -> Self {
        Self::Cdc(error)
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
    emit_root_with_inputs(plan, program, &RootRuntimeInputs::default())
}

pub(crate) fn emit_root_with_inputs(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> Result<(), PhysicalRootError> {
    emit_root_with_context(plan, program, inputs, &super::PreparedTriggers::default())
}

pub(crate) fn emit_root_with_context(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    triggers: &super::PreparedTriggers,
) -> Result<(), PhysicalRootError> {
    match plan.root {
        PhysicalRoot::Query(_) => super::emit_root_query_with_inputs(plan, program, inputs)
            .map_err(PhysicalRootError::Query),
        PhysicalRoot::Delete(_) => emit_root_delete_with_context(plan, program, inputs, triggers)
            .map_err(PhysicalRootError::Delete),
        PhysicalRoot::Insert(_) => {
            super::emit_root_insert_with_context(plan, program, inputs, triggers)
                .map_err(PhysicalRootError::Insert)
        }
        PhysicalRoot::Update(_) => {
            super::emit_root_update_with_context(plan, program, inputs, triggers)
                .map_err(PhysicalRootError::Update)
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
    emit_root_delete_with_inputs(plan, program, &RootRuntimeInputs::default())
}

pub(crate) fn emit_root_delete_with_inputs(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> DeleteResult<()> {
    emit_root_delete_with_context(plan, program, inputs, &super::PreparedTriggers::default())
}

pub(crate) fn emit_root_delete_with_context(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    triggers: &super::PreparedTriggers,
) -> DeleteResult<()> {
    let delete = match &plan.root {
        PhysicalRoot::Delete(delete) => *delete,
        _ => return Err(PhysicalDeleteError::Unsupported("non-DELETE HIR root")),
    };
    let (source, table, database) = preflight_delete(plan, delete, triggers)?;
    let cdc = PreparedCdc::open(program, plan.document)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.bind_source(delete.target, SourceRuntime::Cursor(CursorId(cursor)))?;

    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database,
    });
    let indexes = open_indexes(program, source, database)?;
    let rowid = RegisterId(program.alloc_register());
    let old_columns = (!delete.triggers.is_empty()
        || !delete.foreign_keys.outgoing.is_empty()
        || !delete.foreign_keys.incoming.is_empty())
    .then(|| {
        RegisterRange::new(
            program.alloc_registers(source.columns.len()),
            source.columns.len(),
        )
    });
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
    program.emit_insn(Insn::RowId {
        cursor_id: cursor,
        dest: rowid.0,
    });
    if let Some(old_columns) = old_columns {
        for position in 0..old_columns.width {
            program.emit_column_or_rowid(cursor, position, old_columns.first.0 + position);
        }
        let rows = TriggerRows {
            new: None,
            old: Some(TriggerRow {
                columns: old_columns,
                rowid,
            }),
        };
        emit_trigger_programs(
            program,
            triggers,
            delete
                .triggers
                .iter()
                .filter(|trigger| trigger.value().time == TriggerTime::Before),
            rows,
            loop_next,
        )?;
        program.emit_insn(Insn::NotExists {
            cursor,
            rowid_reg: rowid.0,
            target_pc: loop_next,
        });
    }
    let returning = if let Some(returning) = &delete.returning {
        let previous = old_columns
            .map(|columns| {
                bindings.replace_source(
                    delete.target,
                    SourceRuntime::Registers {
                        columns,
                        rowid: Some(rowid),
                    },
                )
            })
            .transpose()?;
        let result = emit_returning_values(program, &mut bindings, returning)?;
        if let Some(previous) = previous {
            bindings.replace_source(delete.target, previous)?;
        }
        Some(result)
    } else {
        None
    };
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
    if !delete.foreign_keys.outgoing.is_empty() {
        super::emit_delete_child_repairs(
            program,
            &delete.foreign_keys.outgoing,
            &table,
            old_columns.expect("foreign keys require the frozen OLD row"),
            rowid,
        )?;
    }
    if !delete.foreign_keys.incoming.is_empty() {
        super::emit_delete_parent_checks(
            program,
            &delete.foreign_keys.incoming,
            &table,
            old_columns.expect("foreign keys require the frozen OLD row"),
            rowid,
        )?;
    }
    if let Some(cdc) = cdc {
        let before = cdc
            .has_before()
            .then(|| record_from_cursor(program, &table, cursor, rowid));
        cdc.emit_change(
            program,
            CdcChange::Delete,
            rowid,
            before,
            None,
            None,
            &table.name,
        )?;
    }
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
    if !delete.foreign_keys.incoming.is_empty() {
        super::emit_delete_parent_actions(
            program,
            &delete.foreign_keys.incoming,
            &table,
            old_columns.expect("foreign keys require the frozen OLD row"),
            rowid,
            triggers,
        )?;
    }
    if let Some(old_columns) = old_columns {
        emit_trigger_programs(
            program,
            triggers,
            delete
                .triggers
                .iter()
                .filter(|trigger| trigger.value().time == TriggerTime::After),
            TriggerRows {
                new: None,
                old: Some(TriggerRow {
                    columns: old_columns,
                    rowid,
                }),
            },
            loop_next,
        )?;
    }
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
    if let Some(cdc) = cdc {
        cdc.emit_autocommit_commit(program)?;
        cdc.close(program);
    }
    Ok(())
}

fn preflight_delete<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    delete: &crate::translate::semantic::hir::Delete,
    triggers: &PreparedTriggers,
) -> DeleteResult<(
    &'plan crate::translate::semantic::hir::Source,
    crate::sync::Arc<crate::schema::BTreeTable>,
    usize,
)> {
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(PhysicalDeleteError::Unsupported("ORDER BY or LIMIT"));
    }
    if !triggers.covers(&delete.triggers) {
        return Err(PhysicalDeleteError::Invalid(
            "resolved trigger has no prepared program",
        ));
    }
    if delete.foreign_keys.incoming.iter().any(|foreign_key| {
        matches!(
            foreign_key.declaration.on_delete,
            RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
        ) && triggers
            .foreign_key_action(
                foreign_key.child_table.id(),
                &foreign_key.declaration,
                super::ForeignKeyParentChange::Delete,
            )
            .is_none()
    }) {
        return Err(PhysicalDeleteError::Invalid(
            "mutating foreign-key action has no prepared HIR program",
        ));
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
