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
    translate::semantic::hir::{IndexCoverage, SourceKind},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_returning_result, emit_returning_values, emit_trigger_programs,
    open_dml_target_scan, open_indexes, record_from_cursor, CdcChange, CursorId,
    PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError, PhysicalPlan,
    PhysicalQueryError, PhysicalRoot, PhysicalSourceKind, PhysicalTriggerError, PreparedCdc,
    PreparedTriggers, RegisterId, RootRuntimeInputs, RuntimeBindingError, RuntimeBindings,
    SourceRuntime, TriggerRow, TriggerRows,
};

#[derive(Debug)]
pub(crate) enum PhysicalDeleteError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Index(PhysicalIndexError),
    Trigger(PhysicalTriggerError),
    ForeignKey(PhysicalForeignKeyError),
    Query(PhysicalQueryError),
    Mutation(super::PhysicalMutationError),
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
            Self::Query(error) => error.fmt(formatter),
            Self::Mutation(error) => error.fmt(formatter),
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

impl From<PhysicalQueryError> for PhysicalDeleteError {
    fn from(error: PhysicalQueryError) -> Self {
        Self::Query(error)
    }
}

impl From<super::PhysicalMutationError> for PhysicalDeleteError {
    fn from(error: super::PhysicalMutationError) -> Self {
        Self::Mutation(error)
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
        PhysicalRoot::SchemaExpressions(_) => Err(PhysicalRootError::Unsupported(
            "schema-expression root requires runtime inputs",
        )),
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
    let target_scan = open_dml_target_scan(plan, program, delete.target, cursor)?;
    let rowid = RegisterId(program.alloc_register());
    let needs_old_columns = !delete.triggers.is_empty()
        || !delete.foreign_keys.outgoing.is_empty()
        || !delete.foreign_keys.incoming.is_empty();
    // Freeze every selected rowid before the first write. Besides making
    // ORDER BY/LIMIT stable, this prevents a self-referencing subquery from
    // observing rows deleted earlier by the same statement.
    let ordered_rows = super::emit_ordered_dml_rowids(
        plan,
        program,
        &mut bindings,
        target_scan,
        delete.predicate.as_ref(),
        &delete.order_by,
        delete.limit.as_ref(),
    )?;
    target_scan.close(program);
    let indexes = open_indexes(program, source, database)?;
    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: ordered_rows.cursor,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::Column {
        cursor_id: ordered_rows.cursor,
        column: 0,
        dest: rowid.0,
        default: None,
    });
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: rowid.0,
        target_pc: loop_next,
    });
    let old_columns = needs_old_columns
        .then(|| {
            super::freeze_cursor_row(program, &mut bindings, delete.target, source.columns.len())
        })
        .transpose()?;
    if let Some(old_columns) = old_columns {
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
        let result = emit_returning_values(plan, program, &mut bindings, returning)?;
        if let Some(previous) = previous {
            bindings.replace_source(delete.target, previous)?;
        }
        Some(result)
    } else {
        None
    };
    super::prepare_delete_row(
        program,
        &mut bindings,
        delete.target,
        &table,
        &indexes,
        rowid,
        old_columns,
        &delete.foreign_keys,
    )?;
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
    super::finish_delete_row(
        program,
        &table,
        cursor,
        rowid,
        old_columns,
        &delete.foreign_keys,
        triggers,
    )?;
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
        cursor_id: ordered_rows.cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(loop_end);
    close_indexes(program, &indexes);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    ordered_rows.close(program);
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
    let physical_source = plan
        .source(delete.target)
        .ok_or(PhysicalDeleteError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access: _ } = &physical_source.kind else {
        return Err(PhysicalDeleteError::Invalid(
            "physical target is not a catalog table",
        ));
    };
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
