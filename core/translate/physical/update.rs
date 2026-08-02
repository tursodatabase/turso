//! Stable-rowset UPDATE lowering from closed HIR.

use std::fmt;

use turso_parser::ast::{RefAct, ResolveType, TriggerTime};

use crate::{
    error::SQLITE_CONSTRAINT_PRIMARYKEY,
    schema::Table,
    translate::semantic::hir::{self, Expr, IndexCoverage},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_complete_logical_row, emit_expression_for_dml, emit_index_key,
    emit_index_key_from_expressions, emit_new_row_constraints, emit_replace_conflicting_row,
    emit_replace_not_null_defaults, emit_replace_unique_check, emit_returning_result,
    emit_returning_values, emit_stored_record, emit_trigger_programs, emit_unique_check,
    open_dml_target_scan, open_indexes, record_from_registers, update_record, CdcChange, CursorId,
    ExpressionEmitter, PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError,
    PhysicalMutationError, PhysicalPlan, PhysicalRoot, PhysicalRowError, PhysicalSourceKind,
    PhysicalTriggerError, PreparedCdc, PreparedTriggers, RegisterId, RegisterRange,
    RootRuntimeInputs, RuntimeBindingError, RuntimeBindings, SourceRuntime, TriggerRow,
    TriggerRows,
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
    Mutation(PhysicalMutationError),
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
            Self::Mutation(error) => error.fmt(formatter),
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

impl From<PhysicalMutationError> for PhysicalUpdateError {
    fn from(error: PhysicalMutationError) -> Self {
        Self::Mutation(error)
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
    emit_root_update_with_context_and_after(plan, program, inputs, triggers, |_| {})
}

pub(crate) fn emit_root_update_with_context_and_after(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    triggers: &PreparedTriggers,
    after: impl FnOnce(&mut ProgramBuilder),
) -> UpdateResult<()> {
    let update = match &plan.root {
        PhysicalRoot::Update(update) => *update,
        _ => return Err(PhysicalUpdateError::Unsupported("non-UPDATE HIR root")),
    };
    if let Some(table) = virtual_target(plan, update)? {
        let result = emit_virtual_update(plan, program, inputs, update, table);
        after(program);
        return result;
    }
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
    let old_columns = RegisterRange::new(
        program.alloc_registers(source.columns.len()),
        source.columns.len(),
    );
    bindings.bind_source(
        update.new_source,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(new_rowid),
        },
    )?;
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
    let target_scan = open_dml_target_scan(plan, program, update.target, cursor)?;
    let from_rows = update
        .from
        .as_ref()
        .map(|from| {
            super::emit_update_from_rows(
                plan,
                program,
                &mut bindings,
                update.target,
                target_scan,
                from,
                update.predicate.as_ref(),
                &update.assignments,
                &update.order_by,
                update.limit.as_ref(),
            )
        })
        .transpose()?;
    let ordered_rows = (from_rows.is_none()
        && (!update.order_by.is_empty() || update.limit.is_some()))
    .then(|| {
        super::emit_ordered_dml_rowids(
            plan,
            program,
            &mut bindings,
            target_scan,
            update.predicate.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
        )
    })
    .transpose()?;

    if from_rows.is_none() && ordered_rows.is_none() {
        let scan_start = program.allocate_label();
        let scan_next = program.allocate_label();
        let scan_done = program.allocate_label();
        target_scan.rewind(program, scan_done);
        program.preassign_label_to_next_insn(scan_start);
        target_scan.prepare_row(program);
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
        target_scan.rowid(program, rowid.0);
        program.emit_insn(Insn::RowSetAdd {
            rowset_reg: rowset,
            value_reg: rowid.0,
        });
        program.preassign_label_to_next_insn(scan_next);
        target_scan.next(program, scan_start);
        program.preassign_label_to_next_insn(scan_done);
    }
    target_scan.close(program);
    let indexes = open_indexes(program, source, database)?;

    let write_start = program.allocate_label();
    let write_next = program.allocate_label();
    let write_done = program.allocate_label();
    if let Some(from_rows) = &from_rows {
        program.emit_insn(Insn::Rewind {
            cursor_id: from_rows.cursor,
            pc_if_empty: write_done,
        });
        program.preassign_label_to_next_insn(write_start);
        if let Some(column) = from_rows.rowid_column {
            program.emit_insn(Insn::Column {
                cursor_id: from_rows.cursor,
                column,
                dest: rowid.0,
                default: None,
            });
        } else {
            program.emit_insn(Insn::RowId {
                cursor_id: from_rows.cursor,
                dest: rowid.0,
            });
        }
    } else if let Some(ordered_rows) = &ordered_rows {
        program.emit_insn(Insn::Rewind {
            cursor_id: ordered_rows.cursor,
            pc_if_empty: write_done,
        });
        program.preassign_label_to_next_insn(write_start);
        program.emit_insn(Insn::Column {
            cursor_id: ordered_rows.cursor,
            column: 0,
            dest: rowid.0,
            default: None,
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
        ExpressionEmitter::new(program, &mut bindings).emit_into(
            &Expr::column(update.target, position),
            RegisterRange::new(old_columns.first.0 + position, 1),
        )?;
        if column.generated_expr().is_some() {
            continue;
        }
        program.emit_insn(Insn::Copy {
            src_reg: old_columns.first.0 + position,
            dst_reg: logical.first.0 + position,
            extra_amount: 0,
        });
    }
    bindings.replace_source(
        update.target,
        SourceRuntime::Registers {
            columns: old_columns,
            rowid: Some(rowid),
        },
    )?;
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
                    column: from_rows.assignment_offset + offset + position,
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

    emit_complete_logical_row(program, &mut bindings, update.new_source, &table, logical)?;
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
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: rowid.0,
        target_pc: write_next,
    });
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
        update.new_source,
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
            &update.foreign_keys,
            logical,
            new_rowid,
            triggers,
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
    let mut new_keys = Vec::with_capacity(indexes.len());
    for index in &indexes {
        let expressions = plan
            .document
            .source(update.new_source)
            .and_then(|source| source.index_expressions.get(new_keys.len()))
            .ok_or(PhysicalUpdateError::Invalid(
                "NEW row is missing an index program",
            ))?;
        let key = emit_index_key_from_expressions(
            program,
            &mut bindings,
            update.new_source,
            new_rowid,
            index,
            expressions,
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
                &update.foreign_keys,
                logical,
                new_rowid,
                triggers,
            )?;
        } else {
            emit_unique_check(program, index, &key, Some(rowid), conflict, write_next)?;
        }
        new_keys.push(key);
    }
    if !update.foreign_keys.outgoing.is_empty() || !update.foreign_keys.incoming.is_empty() {
        super::prepare_update_row(
            program,
            &mut bindings,
            &table,
            old_columns,
            logical,
            rowid,
            new_rowid,
            &update.foreign_keys,
        )?;
    }
    emit_stored_record(
        program,
        &mut bindings,
        update.new_source,
        &table,
        logical,
        record,
    )?;
    super::finish_update_row(
        program,
        &mut bindings,
        &table,
        cursor,
        &indexes,
        &old_keys,
        &new_keys,
        record,
        old_columns,
        logical,
        rowid,
        new_rowid,
        &update.foreign_keys,
        triggers,
    )?;
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
    if let Some(cdc) = cdc {
        let before = cdc
            .has_before()
            .then(|| record_from_registers(program, &table, old_columns, rowid));
        let after = cdc
            .has_after()
            .then(|| record_from_registers(program, &table, logical, new_rowid));
        let updates = cdc.has_updates().then(|| {
            update_record(
                program,
                source.columns.len(),
                &update.assignments,
                logical,
                update
                    .cdc_updates_override
                    .as_ref()
                    .map(|(position, value)| (*position, value.as_str())),
            )
        });
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
        let result = emit_returning_values(plan, program, &mut bindings, returning)?;
        emit_returning_result(program, result);
    }
    program.preassign_label_to_next_insn(write_next);
    if let Some(from_rows) = &from_rows {
        program.emit_insn(Insn::Next {
            cursor_id: from_rows.cursor,
            pc_if_next: write_start,
        });
    } else if let Some(ordered_rows) = &ordered_rows {
        program.emit_insn(Insn::Next {
            cursor_id: ordered_rows.cursor,
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
    if let Some(ordered_rows) = ordered_rows {
        ordered_rows.close(program);
    }
    if let Some(cdc) = cdc {
        cdc.emit_autocommit_commit(program)?;
        cdc.close(program);
    }
    after(program);
    Ok(())
}

fn virtual_target(
    plan: &PhysicalPlan<'_>,
    update: &hir::Update,
) -> UpdateResult<Option<crate::sync::Arc<crate::VirtualTable>>> {
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
    if !matches!(access, super::TableAccess::Scan) {
        return Err(PhysicalUpdateError::Unsupported("indexed target access"));
    }
    Ok(match table.value() {
        Table::Virtual(table) => Some(table.clone()),
        Table::BTree(_) => None,
    })
}

fn emit_virtual_update<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    update: &hir::Update,
    table: crate::sync::Arc<crate::VirtualTable>,
) -> UpdateResult<()> {
    if update.from.is_some() {
        return Err(PhysicalUpdateError::Unsupported(
            "UPDATE FROM on a virtual table",
        ));
    }
    if !update.order_by.is_empty() || update.limit.is_some() {
        return Err(PhysicalUpdateError::Unsupported(
            "ordered or limited virtual table UPDATE",
        ));
    }
    let source = plan
        .document
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid("target source is missing"))?;
    let new_source = plan
        .document
        .source(update.new_source)
        .ok_or(PhysicalUpdateError::Invalid("NEW source is missing"))?;
    if source.columns.len() != new_source.columns.len() {
        return Err(PhysicalUpdateError::Invalid(
            "OLD and NEW row widths differ",
        ));
    }

    #[cfg(feature = "cli_only")]
    let is_dbpage = table.name == crate::dbpage::DBPAGE_TABLE_NAME;
    #[cfg(not(feature = "cli_only"))]
    let is_dbpage = false;
    if table.readonly() && !is_dbpage {
        return Err(
            crate::LimboError::Constraint(format!("Table is read-only: {}", table.name)).into(),
        );
    }

    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    if !is_dbpage {
        program.emit_insn(Insn::VBegin { cursor_id: cursor });
    }
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.bind_source(update.target, SourceRuntime::Cursor(CursorId(cursor)))?;

    let arguments = program.alloc_registers(source.columns.len() + 2);
    let old_rowid = RegisterId(arguments);
    let new_rowid = RegisterId(arguments + 1);
    let logical = RegisterRange::new(arguments + 2, source.columns.len());
    bindings.bind_source(
        update.new_source,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(new_rowid),
        },
    )?;

    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_done = program.allocate_label();
    program.emit_insn(Insn::VFilter {
        cursor_id: cursor,
        pc_if_empty: loop_done,
        arg_count: 0,
        args_reg: 0,
        idx_str: None,
        idx_num: 0,
    });
    program.preassign_label_to_next_insn(loop_start);
    if let Some(predicate) = &update.predicate {
        let condition = emit_expression_for_dml(plan, program, &mut bindings, predicate)?;
        if condition.width != 1 {
            return Err(PhysicalUpdateError::Invalid("WHERE result is not scalar"));
        }
        program.emit_insn(Insn::IfNot {
            reg: condition.first.0,
            target_pc: loop_next,
            jump_if_null: true,
        });
    }

    program.emit_insn(Insn::RowId {
        cursor_id: cursor,
        dest: old_rowid.0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: old_rowid.0,
        dst_reg: new_rowid.0,
        extra_amount: 0,
    });
    for position in 0..source.columns.len() {
        program.emit_insn(Insn::VColumn {
            cursor_id: cursor,
            column: position,
            dest: logical.first.0 + position,
        });
    }

    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let values = emit_expression_for_dml(plan, program, &mut bindings, &assignment.value)?;
        if values.width != assignment.columns.len() {
            return Err(PhysicalUpdateError::Invalid(
                "assignment width does not match its target columns",
            ));
        }
        assignments.push((&assignment.columns, values));
    }
    for (columns, values) in assignments {
        for (position, column) in columns.iter().enumerate() {
            let destination = match column {
                hir::TargetColumn::Column(column) => {
                    logical
                        .register(*column)
                        .ok_or(PhysicalUpdateError::Invalid(
                            "assignment target is outside the row",
                        ))?
                        .0
                }
                hir::TargetColumn::RowId => new_rowid.0,
            };
            program.emit_insn(Insn::Copy {
                src_reg: values.first.0 + position,
                dst_reg: destination,
                extra_amount: 0,
            });
        }
    }
    program.emit_insn(Insn::VUpdate {
        cursor_id: cursor,
        arg_count: source.columns.len() + 2,
        start_reg: arguments,
        conflict_action: 0,
    });
    if let Some(returning) = &update.returning {
        let result = emit_returning_values(plan, program, &mut bindings, returning)?;
        emit_returning_result(program, result);
    }
    program.preassign_label_to_next_insn(loop_next);
    program.emit_insn(Insn::VNext {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(loop_done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
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
    if update.foreign_keys.incoming.iter().any(|foreign_key| {
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
        return Err(PhysicalUpdateError::Invalid(
            "REPLACE foreign-key action has no prepared HIR program",
        ));
    }
    let source = plan
        .document
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid("target source is missing"))?;
    let new_source = plan
        .document
        .source(update.new_source)
        .ok_or(PhysicalUpdateError::Invalid("NEW source is missing"))?;
    if source.columns.len() != new_source.columns.len() {
        return Err(PhysicalUpdateError::Invalid(
            "OLD and NEW row widths differ",
        ));
    }
    let IndexCoverage::Complete { indexes: _ } = &source.index_coverage else {
        return Err(PhysicalUpdateError::Invalid(
            "target does not carry complete index metadata",
        ));
    };
    if new_source.check_constraints.is_none() {
        return Err(PhysicalUpdateError::Invalid(
            "NEW row does not carry CHECK metadata",
        ));
    }
    let physical = plan
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access: _ } = &physical.kind else {
        return Err(PhysicalUpdateError::Invalid(
            "target is not a catalog table",
        ));
    };
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
