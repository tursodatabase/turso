//! Stable-rowset UPDATE lowering from closed HIR.

use std::fmt;

use turso_parser::ast::{RefAct, ResolveType, TriggerTime};

use crate::{
    error::SQLITE_CONSTRAINT_PRIMARYKEY,
    schema::Table,
    translate::semantic::hir::{self, Expr, IndexCoverage},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_complete_logical_row, emit_expression_for_dml, emit_index_delete,
    emit_index_insert, emit_index_key, emit_new_row_constraints, emit_replace_conflicting_row,
    emit_replace_not_null_defaults, emit_replace_unique_check, emit_returning_result,
    emit_returning_values, emit_stored_record, emit_trigger_programs, emit_unique_check,
    open_indexes, record_from_registers, update_record, CdcChange, CursorId, ExpressionEmitter,
    PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError, PhysicalPlan,
    PhysicalRoot, PhysicalRowError, PhysicalSourceKind, PhysicalTriggerError, PreparedCdc,
    PreparedTriggers, RegisterId, RegisterRange, RootRuntimeInputs, RuntimeBindingError,
    RuntimeBindings, SourceRuntime, TableAccess, TriggerRow, TriggerRows,
};

#[derive(Debug)]
pub(crate) enum PhysicalUpdateError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Row(PhysicalRowError),
    Index(PhysicalIndexError),
    Query(super::PhysicalQueryError),
    Trigger(PhysicalTriggerError),
    ForeignKey(PhysicalForeignKeyError),
    Cdc(crate::LimboError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
            Self::Index(error) => error.fmt(formatter),
            Self::Query(error) => error.fmt(formatter),
            Self::Trigger(error) => error.fmt(formatter),
            Self::ForeignKey(error) => error.fmt(formatter),
            Self::Cdc(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical UPDATE: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "physical UPDATE is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalUpdateError {}

impl From<RuntimeBindingError> for PhysicalUpdateError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalUpdateError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

impl From<PhysicalRowError> for PhysicalUpdateError {
    fn from(error: PhysicalRowError) -> Self {
        Self::Row(error)
    }
}

impl From<PhysicalIndexError> for PhysicalUpdateError {
    fn from(error: PhysicalIndexError) -> Self {
        Self::Index(error)
    }
}

impl From<super::PhysicalQueryError> for PhysicalUpdateError {
    fn from(error: super::PhysicalQueryError) -> Self {
        Self::Query(error)
    }
}

impl From<PhysicalTriggerError> for PhysicalUpdateError {
    fn from(error: PhysicalTriggerError) -> Self {
        Self::Trigger(error)
    }
}

impl From<PhysicalForeignKeyError> for PhysicalUpdateError {
    fn from(error: PhysicalForeignKeyError) -> Self {
        Self::ForeignKey(error)
    }
}

impl From<crate::LimboError> for PhysicalUpdateError {
    fn from(error: crate::LimboError) -> Self {
        Self::Cdc(error)
    }
}

type UpdateResult<T> = std::result::Result<T, PhysicalUpdateError>;

pub(crate) fn emit_root_update(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> UpdateResult<()> {
    emit_root_update_with_context(
        plan,
        program,
        &RootRuntimeInputs::default(),
        &PreparedTriggers::default(),
    )
}

pub(crate) fn emit_root_update_with_inputs(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> UpdateResult<()> {
    emit_root_update_with_context(plan, program, inputs, &PreparedTriggers::default())
}

pub(crate) fn emit_root_update_with_context(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    triggers: &PreparedTriggers,
) -> UpdateResult<()> {
    let update = match &plan.root {
        PhysicalRoot::Update(update) => *update,
        _ => return Err(PhysicalUpdateError::Unsupported("non-UPDATE HIR root")),
    };
    let (source, table, database) = preflight_update(plan, update, triggers)?;
    let cdc = PreparedCdc::open(program, plan.document)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.bind_source(update.target, SourceRuntime::Cursor(CursorId(cursor)))?;

    let rowset = program.alloc_register();
    let rowid = RegisterId(program.alloc_register());
    let new_rowid = RegisterId(program.alloc_register());
    let logical = RegisterRange::new(
        program.alloc_registers(source.columns.len()),
        source.columns.len(),
    );
    let old_columns = (!update.triggers.is_empty()
        || !update.foreign_keys.outgoing.is_empty()
        || !update.foreign_keys.incoming.is_empty()
        || cdc.is_some_and(|cdc| cdc.has_before() || cdc.has_updates()))
    .then(|| {
        RegisterRange::new(
            program.alloc_registers(source.columns.len()),
            source.columns.len(),
        )
    });
    let record = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: rowset,
        dest_end: None,
    });
    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database,
    });
    let indexes = open_indexes(program, source, database)?;
    let from_rows = update
        .from
        .as_ref()
        .map(|from| {
            super::emit_update_from_rows(
                plan,
                program,
                &mut bindings,
                update.target,
                cursor,
                from,
                update.predicate.as_ref(),
                &update.assignments,
            )
        })
        .transpose()?;

    if from_rows.is_none() {
        let scan_start = program.allocate_label();
        let scan_next = program.allocate_label();
        let scan_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: scan_done,
        });
        program.preassign_label_to_next_insn(scan_start);
        if let Some(predicate) = &update.predicate {
            let condition = emit_expression_for_dml(plan, program, &mut bindings, predicate)?;
            if condition.width != 1 {
                return Err(PhysicalUpdateError::Invalid("WHERE result is not scalar"));
            }
            program.emit_insn(Insn::IfNot {
                reg: condition.first.0,
                target_pc: scan_next,
                jump_if_null: true,
            });
        }
        program.emit_insn(Insn::RowId {
            cursor_id: cursor,
            dest: rowid.0,
        });
        program.emit_insn(Insn::RowSetAdd {
            rowset_reg: rowset,
            value_reg: rowid.0,
        });
        program.preassign_label_to_next_insn(scan_next);
        program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: scan_start,
        });
        program.preassign_label_to_next_insn(scan_done);
    }

    let write_start = program.allocate_label();
    let write_next = program.allocate_label();
    let write_done = program.allocate_label();
    if let Some(from_rows) = &from_rows {
        program.emit_insn(Insn::Rewind {
            cursor_id: from_rows.cursor,
            pc_if_empty: write_done,
        });
        program.preassign_label_to_next_insn(write_start);
        program.emit_insn(Insn::RowId {
            cursor_id: from_rows.cursor,
            dest: rowid.0,
        });
    } else {
        program.preassign_label_to_next_insn(write_start);
        program.emit_insn(Insn::RowSetRead {
            rowset_reg: rowset,
            pc_if_empty: write_done,
            dest_reg: rowid.0,
        });
    }
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: rowid.0,
        target_pc: write_next,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: new_rowid.0,
        extra_amount: 0,
    });

    for (position, column) in table.columns().iter().enumerate() {
        if let Some(old_columns) = old_columns {
            ExpressionEmitter::new(program, &mut bindings).emit_into(
                &Expr::column(update.target, position),
                RegisterRange::new(old_columns.first.0 + position, 1),
            )?;
        }
        if column.generated_expr().is_some() {
            continue;
        }
        ExpressionEmitter::new(program, &mut bindings).emit_into(
            &Expr::column(update.target, position),
            RegisterRange::new(logical.first.0 + position, 1),
        )?;
    }
    let mut assignments = Vec::with_capacity(update.assignments.len());
    if let Some(from_rows) = &from_rows {
        let expected_width = update
            .assignments
            .iter()
            .map(|assignment| assignment.columns.len())
            .sum::<usize>();
        if from_rows.width != expected_width {
            return Err(PhysicalUpdateError::Invalid(
                "materialized UPDATE FROM values have the wrong width",
            ));
        }
        let mut offset = 0;
        for assignment in &update.assignments {
            let values = RegisterRange::new(
                program.alloc_registers(assignment.columns.len()),
                assignment.columns.len(),
            );
            for position in 0..assignment.columns.len() {
                program.emit_insn(Insn::Column {
                    cursor_id: from_rows.cursor,
                    column: offset + position,
                    dest: values.first.0 + position,
                    default: None,
                });
            }
            offset += assignment.columns.len();
            assignments.push((&assignment.columns, values));
        }
    } else {
        for assignment in &update.assignments {
            let values = emit_expression_for_dml(plan, program, &mut bindings, &assignment.value)?;
            if values.width != assignment.columns.len() {
                return Err(PhysicalUpdateError::Invalid(
                    "assignment width does not match its target columns",
                ));
            }
            assignments.push((&assignment.columns, values));
        }
    }
    for (columns, values) in assignments {
        for (position, column) in columns.iter().enumerate() {
            let destination = match column {
                hir::TargetColumn::Column(column)
                    if table
                        .columns()
                        .get(*column)
                        .is_some_and(|column| column.is_rowid_alias()) =>
                {
                    new_rowid.0
                }
                hir::TargetColumn::Column(column) => logical.first.0 + column,
                hir::TargetColumn::RowId => new_rowid.0,
            };
            program.emit_insn(Insn::Copy {
                src_reg: values.first.0 + position,
                dst_reg: destination,
                extra_amount: 0,
            });
        }
    }
    if let Some((position, _)) = table.get_rowid_alias_column() {
        program.emit_insn(Insn::Copy {
            src_reg: new_rowid.0,
            dst_reg: logical.first.0 + position,
            extra_amount: 0,
        });
    }

    let old_runtime = bindings.replace_source(
        update.target,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(new_rowid),
        },
    )?;
    emit_complete_logical_row(program, &mut bindings, update.target, &table, logical)?;
    if let Some(old_columns) = old_columns {
        emit_trigger_programs(
            program,
            triggers,
            update
                .triggers
                .iter()
                .filter(|trigger| trigger.value().time == TriggerTime::Before),
            TriggerRows {
                new: Some(TriggerRow {
                    columns: logical,
                    rowid: new_rowid,
                }),
                old: Some(TriggerRow {
                    columns: old_columns,
                    rowid,
                }),
            },
            write_next,
        )?;
    }
    bindings.replace_source(update.target, old_runtime)?;
    if old_columns.is_some() {
        program.emit_insn(Insn::NotExists {
            cursor,
            rowid_reg: rowid.0,
            target_pc: write_next,
        });
    }
    let mut old_keys = Vec::with_capacity(indexes.len());
    for index in &indexes {
        old_keys.push(emit_index_key(
            program,
            &mut bindings,
            update.target,
            rowid,
            index,
            false,
        )?);
    }
    let old_runtime = bindings.replace_source(
        update.target,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(new_rowid),
        },
    )?;
    emit_replace_not_null_defaults(
        program,
        &mut bindings,
        &update.defaults,
        &table,
        logical,
        update.conflict,
    )?;
    emit_new_row_constraints(
        program,
        &mut bindings,
        update.target,
        &table,
        logical,
        update.conflict,
        write_next,
    )?;
    let rowid_is_unique = program.allocate_label();
    program.emit_insn(Insn::MustBeInt {
        reg: new_rowid.0,
        target_pc: None,
    });
    program.emit_insn(Insn::Eq {
        lhs: new_rowid.0,
        rhs: rowid.0,
        target_pc: rowid_is_unique,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: new_rowid.0,
        target_pc: rowid_is_unique,
    });
    let rowid_conflict = update
        .conflict
        .or(table.rowid_alias_conflict_clause)
        .unwrap_or(ResolveType::Abort);
    match rowid_conflict {
        ResolveType::Replace => emit_replace_conflicting_row(
            program,
            &mut bindings,
            update.target,
            &table,
            cursor,
            &indexes,
            new_rowid,
            Some(rowid),
            write_next,
        )?,
        ResolveType::Ignore => program.emit_insn(Insn::Goto {
            target_pc: write_next,
        }),
        conflict => super::constraint_halt(
            program,
            SQLITE_CONSTRAINT_PRIMARYKEY,
            format!("{}.rowid", table.name),
            conflict,
        ),
    }
    program.preassign_label_to_next_insn(rowid_is_unique);
    // NotExists on the proposed key moves the table cursor. Restore the OLD
    // row before deriving its index keys and issuing the update delete.
    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: rowid.0,
        target_pc: write_next,
    });
    if !update.foreign_keys.outgoing.is_empty() {
        super::emit_update_child_checks(
            program,
            &update.foreign_keys.outgoing,
            &table,
            old_columns.expect("outgoing foreign keys require the frozen OLD row"),
            logical,
            rowid,
            new_rowid,
        )?;
    }
    if !update.foreign_keys.incoming.is_empty() {
        super::emit_update_parent_checks(
            program,
            &update.foreign_keys.incoming,
            &table,
            old_columns.expect("incoming foreign keys require the frozen OLD row"),
            logical,
            rowid,
            new_rowid,
        )?;
    }
    let mut new_keys = Vec::with_capacity(indexes.len());
    for index in &indexes {
        let key = emit_index_key(
            program,
            &mut bindings,
            update.target,
            new_rowid,
            index,
            true,
        )?;
        let conflict = update
            .conflict
            .or(index.index.on_conflict)
            .unwrap_or(ResolveType::Abort);
        if conflict == ResolveType::Replace {
            emit_replace_unique_check(
                program,
                &mut bindings,
                update.target,
                &table,
                cursor,
                &indexes,
                index,
                &key,
                Some(rowid),
                write_next,
            )?;
        } else {
            emit_unique_check(program, index, &key, Some(rowid), conflict, write_next)?;
        }
        new_keys.push(key);
    }
    emit_stored_record(
        program,
        &mut bindings,
        update.target,
        &table,
        logical,
        record,
    )?;
    bindings.replace_source(update.target, old_runtime)?;

    for (index, key) in indexes.iter().zip(&old_keys) {
        emit_index_delete(program, index, key);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: true,
    });
    for (index, key) in indexes.iter().zip(&new_keys) {
        emit_index_insert(program, index, key)?;
    }
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: new_rowid.0,
        record_reg: record,
        flag: InsertFlags::new(),
        table_name: table.name.clone(),
    });
    if !update.foreign_keys.incoming.is_empty() {
        super::emit_insert_parent_repairs(
            program,
            &update.foreign_keys.incoming,
            &table,
            logical,
            new_rowid,
        )?;
    }
    if !update.foreign_keys.incoming.is_empty() {
        super::emit_update_parent_actions(
            program,
            &update.foreign_keys.incoming,
            &table,
            old_columns.expect("incoming foreign keys require the frozen OLD row"),
            logical,
            rowid,
            new_rowid,
            triggers,
        )?;
    }
    if let Some(old_columns) = old_columns {
        let after_trigger_done = program.allocate_label();
        emit_trigger_programs(
            program,
            triggers,
            update
                .triggers
                .iter()
                .filter(|trigger| trigger.value().time == TriggerTime::After),
            TriggerRows {
                new: Some(TriggerRow {
                    columns: logical,
                    rowid: new_rowid,
                }),
                old: Some(TriggerRow {
                    columns: old_columns,
                    rowid,
                }),
            },
            after_trigger_done,
        )?;
        program.preassign_label_to_next_insn(after_trigger_done);
    }
    if let Some(cdc) = cdc {
        let before = cdc.has_before().then(|| {
            record_from_registers(
                program,
                &table,
                old_columns.expect("CDC BEFORE requires the frozen OLD row"),
                rowid,
            )
        });
        let after = cdc
            .has_after()
            .then(|| record_from_registers(program, &table, logical, new_rowid));
        let updates = cdc
            .has_updates()
            .then(|| update_record(program, source.columns.len(), &update.assignments, logical));
        cdc.emit_change(
            program,
            CdcChange::Update,
            new_rowid,
            before,
            after,
            updates,
            &table.name,
        )?;
    }
    if let Some(returning) = &update.returning {
        let old_runtime = bindings.replace_source(
            update.target,
            SourceRuntime::Registers {
                columns: logical,
                rowid: Some(new_rowid),
            },
        )?;
        let result = emit_returning_values(program, &mut bindings, returning)?;
        bindings.replace_source(update.target, old_runtime)?;
        emit_returning_result(program, result);
    }
    program.preassign_label_to_next_insn(write_next);
    if let Some(from_rows) = &from_rows {
        program.emit_insn(Insn::Next {
            cursor_id: from_rows.cursor,
            pc_if_next: write_start,
        });
    } else {
        program.emit_insn(Insn::Goto {
            target_pc: write_start,
        });
    }
    program.preassign_label_to_next_insn(write_done);
    close_indexes(program, &indexes);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    if let Some(from_rows) = from_rows {
        from_rows.close(program);
    }
    if let Some(cdc) = cdc {
        cdc.emit_autocommit_commit(program)?;
        cdc.close(program);
    }
    Ok(())
}

fn preflight_update<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    update: &hir::Update,
    triggers: &PreparedTriggers,
) -> UpdateResult<(
    &'plan hir::Source,
    crate::sync::Arc<crate::schema::BTreeTable>,
    usize,
)> {
    if !update.order_by.is_empty() || update.limit.is_some() {
        return Err(PhysicalUpdateError::Unsupported("ORDER BY or LIMIT"));
    }
    if !triggers.covers(&update.triggers) {
        return Err(PhysicalUpdateError::Invalid(
            "resolved trigger has no prepared program",
        ));
    }
    if update.foreign_keys.incoming.iter().any(|foreign_key| {
        matches!(
            foreign_key.declaration.on_update,
            RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
        ) && triggers
            .foreign_key_action(
                foreign_key.child_table.id(),
                &foreign_key.declaration,
                super::ForeignKeyParentChange::Update,
            )
            .is_none()
    }) {
        return Err(PhysicalUpdateError::Invalid(
            "mutating foreign-key action has no prepared HIR program",
        ));
    }
    let source = plan
        .document
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid("target source is missing"))?;
    let IndexCoverage::Complete { indexes: _ } = &source.index_coverage else {
        return Err(PhysicalUpdateError::Invalid(
            "target does not carry complete index metadata",
        ));
    };
    if source.check_constraints.is_none() {
        return Err(PhysicalUpdateError::Invalid(
            "target does not carry CHECK metadata",
        ));
    }
    let physical = plan
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access } = &physical.kind else {
        return Err(PhysicalUpdateError::Invalid(
            "target is not a catalog table",
        ));
    };
    if !matches!(access, TableAccess::Scan) {
        return Err(PhysicalUpdateError::Unsupported("indexed target access"));
    }
    let database = table
        .database()
        .ok_or(PhysicalUpdateError::Invalid(
            "target has no database identity",
        ))?
        .index();
    let Table::BTree(table) = table.value() else {
        return Err(PhysicalUpdateError::Unsupported("non-B-tree target"));
    };
    if !table.has_rowid {
        return Err(PhysicalUpdateError::Unsupported("WITHOUT ROWID target"));
    }
    Ok((source, table.clone(), database))
}
