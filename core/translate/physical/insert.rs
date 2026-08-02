//! Direct INSERT lowering from a closed HIR row image.

use std::fmt;

use turso_parser::ast::{RefAct, ResolveType, TriggerTime};

use crate::{
    error::{SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_FULL},
    schema::{Table, SQLITE_SEQUENCE_TABLE_NAME},
    translate::semantic::hir::{self, IndexCoverage, InsertSource, UpsertAction},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_complete_logical_row, emit_expression_for_dml, emit_index_insert,
    emit_index_key, emit_new_row_constraints, emit_query_for_dml, emit_replace_conflicting_row,
    emit_replace_not_null_defaults, emit_replace_unique_check, emit_returning_result,
    emit_returning_values, emit_stored_record, emit_trigger_programs, emit_unique_check,
    open_indexes, record_from_registers, CdcChange, CursorId, ExpressionEmitter, OpenedIndex,
    PhysicalExpressionError, PhysicalForeignKeyError, PhysicalIndexError, PhysicalMutationError,
    PhysicalPlan, PhysicalQueryError, PhysicalRoot, PhysicalRowError, PhysicalSourceKind,
    PhysicalTriggerError, PreparedCdc, PreparedTriggers, RegisterId, RegisterRange,
    RootRuntimeInputs, RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess,
    TriggerRow, TriggerRows,
};

#[derive(Debug)]
pub(crate) enum PhysicalInsertError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Row(PhysicalRowError),
    Index(PhysicalIndexError),
    Query(PhysicalQueryError),
    Trigger(PhysicalTriggerError),
    ForeignKey(PhysicalForeignKeyError),
    Mutation(PhysicalMutationError),
    Sequence(crate::LimboError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalInsertError {
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
            Self::Sequence(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical INSERT: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "physical INSERT is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalInsertError {}

impl From<RuntimeBindingError> for PhysicalInsertError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalInsertError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

impl From<PhysicalRowError> for PhysicalInsertError {
    fn from(error: PhysicalRowError) -> Self {
        Self::Row(error)
    }
}

impl From<PhysicalIndexError> for PhysicalInsertError {
    fn from(error: PhysicalIndexError) -> Self {
        Self::Index(error)
    }
}

impl From<PhysicalQueryError> for PhysicalInsertError {
    fn from(error: PhysicalQueryError) -> Self {
        Self::Query(error)
    }
}

impl From<PhysicalTriggerError> for PhysicalInsertError {
    fn from(error: PhysicalTriggerError) -> Self {
        Self::Trigger(error)
    }
}

impl From<PhysicalForeignKeyError> for PhysicalInsertError {
    fn from(error: PhysicalForeignKeyError) -> Self {
        Self::ForeignKey(error)
    }
}

impl From<PhysicalMutationError> for PhysicalInsertError {
    fn from(error: PhysicalMutationError) -> Self {
        Self::Mutation(error)
    }
}

impl From<crate::LimboError> for PhysicalInsertError {
    fn from(error: crate::LimboError) -> Self {
        Self::Sequence(error)
    }
}

type InsertResult<T> = std::result::Result<T, PhysicalInsertError>;

#[derive(Clone, Copy)]
struct AutoincrementRuntime {
    cursor: usize,
    maximum: usize,
    sequence_rowid: usize,
    table_name: usize,
}

pub(crate) fn emit_root_insert(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> InsertResult<()> {
    emit_root_insert_with_context(
        plan,
        program,
        &RootRuntimeInputs::default(),
        &PreparedTriggers::default(),
    )
}

pub(crate) fn emit_root_insert_with_inputs(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> InsertResult<()> {
    emit_root_insert_with_context(plan, program, inputs, &PreparedTriggers::default())
}

pub(crate) fn emit_root_insert_with_context(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    triggers: &PreparedTriggers,
) -> InsertResult<()> {
    let insert = match &plan.root {
        PhysicalRoot::Insert(insert) => *insert,
        _ => return Err(PhysicalInsertError::Unsupported("non-INSERT HIR root")),
    };
    if let Table::Virtual(table) = target_table(plan, insert)?.value() {
        return emit_virtual_insert(plan, program, inputs, insert, table.clone());
    }
    let (source, table, database) = preflight_insert(plan, insert, triggers)?;
    let cdc = PreparedCdc::open(program, plan.document)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let logical = RegisterRange::new(
        program.alloc_registers(source.columns.len()),
        source.columns.len(),
    );
    let rowid = RegisterId(program.alloc_register());
    let record = program.alloc_register();
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.bind_source(
        insert.target,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(rowid),
        },
    )?;
    if let Some(excluded) = insert.excluded_source {
        bindings.bind_source(
            excluded,
            SourceRuntime::Registers {
                columns: logical,
                rowid: Some(rowid),
            },
        )?;
    }
    let query_rows = match &insert.source {
        InsertSource::Query(query) => {
            Some(emit_query_for_dml(plan, program, &mut bindings, *query)?)
        }
        InsertSource::DefaultValues | InsertSource::Values(_) => None,
    };

    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database,
    });
    let mut autoincrement = if program.is_mvcc_enabled() && insert.autoincrement_sequence.is_some()
    {
        None
    } else {
        open_autoincrement(program, insert.autoincrement.as_ref(), &table, database)?
    };
    let indexes = open_indexes(program, source, database)?;

    match &insert.source {
        InsertSource::DefaultValues => {
            let skip_row = program.allocate_label();
            emit_insert_row(
                plan,
                program,
                &mut bindings,
                insert,
                &table,
                cursor,
                logical,
                rowid,
                record,
                &indexes,
                &[],
                skip_row,
                triggers,
                autoincrement.as_mut(),
                cdc,
            )?;
            program.preassign_label_to_next_insn(skip_row);
        }
        InsertSource::Values(rows) => {
            for row in rows {
                let skip_row = program.allocate_label();
                emit_insert_row(
                    plan,
                    program,
                    &mut bindings,
                    insert,
                    &table,
                    cursor,
                    logical,
                    rowid,
                    record,
                    &indexes,
                    row,
                    skip_row,
                    triggers,
                    autoincrement.as_mut(),
                    cdc,
                )?;
                program.preassign_label_to_next_insn(skip_row);
            }
        }
        InsertSource::Query(_) => {
            let query_rows = query_rows.as_ref().ok_or(PhysicalInsertError::Invalid(
                "INSERT query was not materialized",
            ))?;
            let done = program.allocate_label();
            let row_start = program.allocate_label();
            let row_done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: query_rows.cursor,
                pc_if_empty: done,
            });
            program.preassign_label_to_next_insn(row_start);
            emit_insert_query_row(
                plan,
                program,
                &mut bindings,
                insert,
                &table,
                cursor,
                logical,
                rowid,
                record,
                &indexes,
                query_rows.cursor,
                row_done,
                triggers,
                autoincrement.as_mut(),
                cdc,
            )?;
            program.preassign_label_to_next_insn(row_done);
            program.emit_insn(Insn::Next {
                cursor_id: query_rows.cursor,
                pc_if_next: row_start,
            });
            program.preassign_label_to_next_insn(done);
        }
    }
    close_indexes(program, &indexes);
    if let Some(autoincrement) = autoincrement {
        let already_exists = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: autoincrement.sequence_rowid,
            target_pc: already_exists,
        });
        emit_update_sqlite_sequence(program, autoincrement, autoincrement.maximum)?;
        program.preassign_label_to_next_insn(already_exists);
        program.emit_insn(Insn::Close {
            cursor_id: autoincrement.cursor,
        });
    }
    program.emit_insn(Insn::Close { cursor_id: cursor });
    if let Some(query_rows) = query_rows {
        query_rows.close(program);
    }
    if let Some(cdc) = cdc {
        cdc.emit_autocommit_commit(program)?;
        cdc.close(program);
    }
    Ok(())
}

fn target_table<'document>(
    plan: &'document PhysicalPlan<'document>,
    insert: &hir::Insert,
) -> InsertResult<&'document hir::ResolvedTable> {
    let physical = plan
        .source(insert.target)
        .ok_or(PhysicalInsertError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access } = &physical.kind else {
        return Err(PhysicalInsertError::Invalid(
            "target is not a catalog table",
        ));
    };
    if !matches!(access, TableAccess::Scan) {
        return Err(PhysicalInsertError::Unsupported("indexed target access"));
    }
    Ok(table)
}

fn emit_virtual_insert<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    insert: &hir::Insert,
    table: crate::sync::Arc<crate::VirtualTable>,
) -> InsertResult<()> {
    let source = plan
        .document
        .source(insert.target)
        .ok_or(PhysicalInsertError::Invalid("target source is missing"))?;
    let values = match &insert.source {
        InsertSource::DefaultValues => &[][..],
        // Match the old virtual-table path: VALUES is a single VUpdate call.
        InsertSource::Values(rows) => rows
            .last()
            .map(Vec::as_slice)
            .ok_or(PhysicalInsertError::Invalid("VALUES has no rows"))?,
        InsertSource::Query(_) => {
            return Err(PhysicalInsertError::Unsupported(
                "virtual tables only support VALUES in INSERT",
            ));
        }
    };
    if !matches!(insert.source, InsertSource::DefaultValues) && values.len() != insert.columns.len()
    {
        return Err(PhysicalInsertError::Invalid(
            "VALUES width does not match the target column list",
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
    let arguments = program.alloc_registers(source.columns.len() + 2);
    let rowid = RegisterId(arguments + 1);
    let logical = RegisterRange::new(arguments + 2, source.columns.len());
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    if !is_dbpage {
        program.emit_insn(Insn::VBegin { cursor_id: cursor });
    }
    program.emit_insn(Insn::Null {
        dest: arguments,
        dest_end: Some(arguments + source.columns.len() + 1),
    });

    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.bind_source(
        insert.target,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(rowid),
        },
    )?;
    for (target, value) in insert.columns.iter().zip(values) {
        let result = emit_expression_for_dml(plan, program, &mut bindings, value)?;
        if result.width != 1 {
            return Err(PhysicalInsertError::Invalid("INSERT value is not scalar"));
        }
        if target.uses_value {
            let destination = target_register(target.column, logical, rowid)?;
            program.emit_insn(Insn::Copy {
                src_reg: result.first.0,
                dst_reg: destination.0,
                extra_amount: 0,
            });
        }
    }
    emit_insert_defaults(plan, program, &mut bindings, insert, logical, |column| {
        source
            .columns
            .get(column)
            .is_some_and(|column| !column.hidden)
    })?;

    program.emit_insn(Insn::VUpdate {
        cursor_id: cursor,
        arg_count: source.columns.len() + 2,
        start_reg: arguments,
        conflict_action: insert.conflict.map_or(0, |conflict| conflict.bit_value()) as u16,
    });
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_insert_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    values: &[hir::Expr],
    skip_row: crate::vdbe::BranchOffset,
    triggers: &PreparedTriggers,
    autoincrement: Option<&mut AutoincrementRuntime>,
    cdc: Option<PreparedCdc<'_>>,
) -> InsertResult<()> {
    if !matches!(insert.source, InsertSource::DefaultValues) && values.len() != insert.columns.len()
    {
        return Err(PhysicalInsertError::Invalid(
            "VALUES width does not match the target column list",
        ));
    }
    initialize_insert_row(program, logical, rowid);

    for (target, value) in insert.columns.iter().zip(values) {
        let result = emit_expression_for_dml(plan, program, bindings, value)?;
        if result.width != 1 {
            return Err(PhysicalInsertError::Invalid("INSERT value is not scalar"));
        }
        if target.uses_value {
            let destination = target_register(target.column, logical, rowid)?;
            program.emit_insn(Insn::Copy {
                src_reg: result.first.0,
                dst_reg: destination.0,
                extra_amount: 0,
            });
        }
    }
    emit_insert_defaults(plan, program, bindings, insert, logical, |_| true)?;
    finish_insert_row(
        plan,
        program,
        bindings,
        insert,
        table,
        cursor,
        logical,
        rowid,
        record,
        indexes,
        skip_row,
        triggers,
        autoincrement,
        cdc,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_insert_query_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    query_cursor: usize,
    skip_row: crate::vdbe::BranchOffset,
    triggers: &PreparedTriggers,
    autoincrement: Option<&mut AutoincrementRuntime>,
    cdc: Option<PreparedCdc<'_>>,
) -> InsertResult<()> {
    initialize_insert_row(program, logical, rowid);
    for (position, target) in insert.columns.iter().enumerate() {
        if target.uses_value {
            let destination = target_register(target.column, logical, rowid)?;
            program.emit_column_or_rowid(query_cursor, position, destination.0);
        }
    }
    emit_insert_defaults(plan, program, bindings, insert, logical, |_| true)?;
    finish_insert_row(
        plan,
        program,
        bindings,
        insert,
        table,
        cursor,
        logical,
        rowid,
        record,
        indexes,
        skip_row,
        triggers,
        autoincrement,
        cdc,
    )
}

fn initialize_insert_row(program: &mut ProgramBuilder, logical: RegisterRange, rowid: RegisterId) {
    program.emit_insn(Insn::Null {
        dest: logical.first.0,
        dest_end: Some(logical.first.0 + logical.width - 1),
    });
    program.emit_insn(Insn::Null {
        dest: rowid.0,
        dest_end: None,
    });
}

fn target_register(
    target: hir::TargetColumn,
    logical: RegisterRange,
    rowid: RegisterId,
) -> InsertResult<RegisterId> {
    match target {
        hir::TargetColumn::Column(column) => logical.register(column).ok_or(
            PhysicalInsertError::Invalid("target column is outside the row"),
        ),
        hir::TargetColumn::RowId => Ok(rowid),
    }
}

fn emit_insert_defaults<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    logical: RegisterRange,
    mut include_column: impl FnMut(usize) -> bool,
) -> InsertResult<()> {
    for default in &insert.defaults {
        if !include_column(default.column)
            || !insert_column_needs_default(&insert.source, &insert.columns, default.column)
        {
            continue;
        }
        let destination = logical
            .register(default.column)
            .ok_or(PhysicalInsertError::Invalid(
                "default column is outside the row",
            ))?;
        let result = emit_expression_for_dml(plan, program, bindings, &default.value)?;
        if result.width != 1 {
            return Err(PhysicalInsertError::Invalid("INSERT default is not scalar"));
        }
        program.emit_insn(Insn::Copy {
            src_reg: result.first.0,
            dst_reg: destination.0,
            extra_amount: 0,
        });
    }
    Ok(())
}

pub(super) fn insert_column_needs_default(
    source: &InsertSource,
    columns: &[hir::InsertTarget],
    column: usize,
) -> bool {
    matches!(source, InsertSource::DefaultValues) || column_needs_default(columns, column)
}

pub(super) fn column_needs_default(columns: &[hir::InsertTarget], column: usize) -> bool {
    !columns
        .iter()
        .any(|target| target.column == hir::TargetColumn::Column(column))
}

#[allow(clippy::too_many_arguments)]
fn finish_insert_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    skip_row: crate::vdbe::BranchOffset,
    triggers: &PreparedTriggers,
    mut autoincrement: Option<&mut AutoincrementRuntime>,
    cdc: Option<PreparedCdc<'_>>,
) -> InsertResult<()> {
    if let Some((position, _)) = table.get_rowid_alias_column().filter(|(position, _)| {
        insert
            .columns
            .iter()
            .any(|target| target.column == hir::TargetColumn::Column(*position))
    }) {
        program.emit_insn(Insn::Copy {
            src_reg: logical.first.0 + position,
            dst_reg: rowid.0,
            extra_amount: 0,
        });
    }
    let explicit_rowid = program.allocate_label();
    let rowid_ready = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: rowid.0,
        target_pc: explicit_rowid,
    });
    if let Some(sequence) = insert
        .autoincrement_sequence
        .as_ref()
        .filter(|_| program.is_mvcc_enabled())
    {
        emit_generated_sequence_rowid(program, sequence, rowid)?;
    } else if let Some(autoincrement) = autoincrement.as_deref_mut() {
        emit_generated_autoincrement_rowid(program, cursor, rowid, *autoincrement)?;
    } else {
        program.emit_insn(Insn::NewRowid {
            cursor,
            rowid_reg: rowid.0,
            prev_largest_reg: 0,
        });
    }
    program.emit_insn(Insn::Goto {
        target_pc: rowid_ready,
    });
    program.preassign_label_to_next_insn(explicit_rowid);
    program.emit_insn(Insn::MustBeInt {
        reg: rowid.0,
        target_pc: None,
    });
    if let Some(sequence) = insert
        .autoincrement_sequence
        .as_ref()
        .filter(|_| program.is_mvcc_enabled())
    {
        emit_explicit_sequence_rowid(program, sequence, rowid)?;
    } else if let Some(autoincrement) = autoincrement {
        emit_explicit_autoincrement_rowid(program, rowid, *autoincrement)?;
    }
    program.preassign_label_to_next_insn(rowid_ready);
    if let Some((position, _)) = table.get_rowid_alias_column() {
        program.emit_insn(Insn::Copy {
            src_reg: rowid.0,
            dst_reg: logical.first.0 + position,
            extra_amount: 0,
        });
    }
    emit_complete_logical_row(program, bindings, insert.target, table, logical)?;
    let trigger_rows = TriggerRows {
        new: Some(TriggerRow {
            columns: logical,
            rowid,
        }),
        old: None,
    };
    emit_trigger_programs(
        program,
        triggers,
        insert
            .triggers
            .iter()
            .filter(|trigger| trigger.value().time == TriggerTime::Before),
        trigger_rows,
        skip_row,
    )?;
    emit_replace_not_null_defaults(
        program,
        bindings,
        &insert.defaults,
        table,
        logical,
        insert.conflict,
    )?;
    emit_new_row_constraints(
        program,
        bindings,
        insert.target,
        table,
        logical,
        insert.conflict,
        skip_row,
    )?;
    let rowid_is_unique = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: rowid.0,
        target_pc: rowid_is_unique,
    });
    let rowid_name = table
        .get_rowid_alias_column()
        .and_then(|(_, column)| column.name.as_deref())
        .unwrap_or("rowid");
    if let Some(upsert) = upsert_for_rowid(insert) {
        emit_upsert_action(
            plan, program, bindings, insert, upsert, table, cursor, logical, record, indexes,
            rowid, skip_row, triggers,
        )?;
    } else if insert
        .conflict
        .or(table.rowid_alias_conflict_clause)
        .unwrap_or(ResolveType::Abort)
        == ResolveType::Replace
    {
        emit_replace_conflicting_row(
            program,
            bindings,
            insert.target,
            table,
            cursor,
            indexes,
            rowid,
            None,
            skip_row,
            &insert.foreign_keys,
            logical,
            rowid,
            triggers,
        )?;
    } else if insert
        .conflict
        .or(table.rowid_alias_conflict_clause)
        .unwrap_or(ResolveType::Abort)
        == ResolveType::Ignore
    {
        program.emit_insn(Insn::Goto {
            target_pc: skip_row,
        });
    } else {
        super::constraint_halt(
            program,
            SQLITE_CONSTRAINT_PRIMARYKEY,
            format!("{}.{}", table.name, rowid_name),
            insert
                .conflict
                .or(table.rowid_alias_conflict_clause)
                .unwrap_or(ResolveType::Abort),
        );
    }
    program.preassign_label_to_next_insn(rowid_is_unique);
    let mut keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        let key = emit_index_key(program, bindings, insert.target, rowid, index, true)?;
        if let Some(upsert) = upsert_for_index(insert, index) {
            emit_upsert_unique_check(
                plan, program, bindings, insert, upsert, table, cursor, logical, record, indexes,
                index, &key, skip_row, triggers,
            )?;
        } else {
            let conflict = insert
                .conflict
                .or(index.index.on_conflict)
                .unwrap_or(ResolveType::Abort);
            if conflict == ResolveType::Replace {
                emit_replace_unique_check(
                    program,
                    bindings,
                    insert.target,
                    table,
                    cursor,
                    indexes,
                    index,
                    &key,
                    None,
                    skip_row,
                    &insert.foreign_keys,
                    logical,
                    rowid,
                    triggers,
                )?;
            } else {
                emit_unique_check(program, index, &key, None, conflict, skip_row)?;
            }
        }
        keys.push(key);
    }
    super::emit_insert_child_checks(
        program,
        &insert.foreign_keys.outgoing,
        table,
        logical,
        rowid,
    )?;
    super::emit_insert_parent_repairs(
        program,
        bindings,
        &insert.foreign_keys.incoming,
        table,
        logical,
        rowid,
    )?;
    emit_stored_record(program, bindings, insert.target, table, logical, record)?;
    for (index, key) in indexes.iter().zip(&keys) {
        emit_index_insert(program, index, key)?;
    }
    let may_have_replaced_row = match insert.conflict {
        Some(conflict) => conflict == ResolveType::Replace,
        None => {
            table.rowid_alias_conflict_clause == Some(ResolveType::Replace)
                || indexes
                    .iter()
                    .any(|index| index.index.on_conflict == Some(ResolveType::Replace))
        }
    };
    let insert_flags = if may_have_replaced_row {
        InsertFlags::new().require_seek()
    } else {
        InsertFlags::new()
    };
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: rowid.0,
        record_reg: record,
        flag: insert_flags,
        table_name: table.name.clone(),
    });
    let after_trigger_done = program.allocate_label();
    emit_trigger_programs(
        program,
        triggers,
        insert
            .triggers
            .iter()
            .filter(|trigger| trigger.value().time == TriggerTime::After),
        trigger_rows,
        after_trigger_done,
    )?;
    program.preassign_label_to_next_insn(after_trigger_done);
    if let Some(cdc) = cdc {
        let after = cdc
            .has_after()
            .then(|| record_from_registers(program, table, logical, rowid));
        cdc.emit_change(
            program,
            CdcChange::Insert,
            rowid,
            None,
            after,
            None,
            &table.name,
        )?;
    }
    if let Some(returning) = &insert.returning {
        let result = emit_returning_values(plan, program, bindings, returning)?;
        emit_returning_result(program, result);
    }
    Ok(())
}

fn open_autoincrement(
    program: &mut ProgramBuilder,
    resolved: Option<&hir::ResolvedTable>,
    target: &crate::schema::BTreeTable,
    database: usize,
) -> InsertResult<Option<AutoincrementRuntime>> {
    let Some(resolved) = resolved else {
        if target.has_autoincrement {
            return Err(PhysicalInsertError::Invalid(
                "AUTOINCREMENT target has no resolved sqlite_sequence table",
            ));
        }
        return Ok(None);
    };
    if !target.has_autoincrement {
        return Err(PhysicalInsertError::Invalid(
            "non-AUTOINCREMENT target carries sqlite_sequence metadata",
        ));
    }
    if resolved.database().map(hir::DatabaseId::index) != Some(database) {
        return Err(PhysicalInsertError::Invalid(
            "sqlite_sequence belongs to a different database",
        ));
    }
    let Table::BTree(sequence) = resolved.value() else {
        return Err(PhysicalInsertError::Invalid(
            "resolved sqlite_sequence is not a B-tree table",
        ));
    };
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(sequence.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(sequence.root_page),
        db: database,
    });
    let runtime = AutoincrementRuntime {
        cursor,
        maximum: program.alloc_register(),
        sequence_rowid: program.alloc_register(),
        table_name: program.emit_string8_new_reg(target.name.clone()),
    };
    program.emit_insn(Insn::Integer {
        dest: runtime.maximum,
        value: 0,
    });
    program.emit_insn(Insn::Null {
        dest: runtime.sequence_rowid,
        dest_end: None,
    });
    let scan = program.allocate_label();
    let next = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(scan);
    let name = program.alloc_register();
    program.emit_column_or_rowid(cursor, 0, name);
    program.emit_insn(Insn::Ne {
        lhs: runtime.table_name,
        rhs: name,
        target_pc: next,
        flags: Default::default(),
        collation: None,
    });
    program.emit_column_or_rowid(cursor, 1, runtime.maximum);
    program.emit_insn(Insn::AddImm {
        register: runtime.maximum,
        value: 0,
    });
    program.emit_insn(Insn::RowId {
        cursor_id: cursor,
        dest: runtime.sequence_rowid,
    });
    program.emit_insn(Insn::Goto { target_pc: done });
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: scan,
    });
    program.preassign_label_to_next_insn(done);
    Ok(Some(runtime))
}

fn emit_generated_sequence_rowid(
    program: &mut ProgramBuilder,
    operation: &hir::SequenceOperation,
    rowid: RegisterId,
) -> InsertResult<()> {
    if operation.kind != hir::SequenceOperationKind::NextValue {
        return Err(PhysicalInsertError::Invalid(
            "AUTOINCREMENT sequence is not a next-value operation",
        ));
    }
    let Table::BTree(backing_table) = operation.backing_table.value() else {
        return Err(PhysicalInsertError::Invalid(
            "AUTOINCREMENT backing object is not a B-tree table",
        ));
    };
    let sqlite_sequence = operation
        .sqlite_sequence
        .as_ref()
        .map(|resolved| match resolved.value() {
            Table::BTree(table) => Ok(table.clone()),
            _ => Err(PhysicalInsertError::Invalid(
                "AUTOINCREMENT sqlite_sequence object is not a B-tree table",
            )),
        })
        .transpose()?;
    crate::translate::sequence::emit_disk_read_nextval_from_resolved(
        program,
        operation.database.index(),
        &operation.normalized_name,
        &operation.sequence,
        backing_table.clone(),
        sqlite_sequence,
        rowid.0,
        None,
    )?;
    Ok(())
}

fn emit_explicit_sequence_rowid(
    program: &mut ProgramBuilder,
    operation: &hir::SequenceOperation,
    rowid: RegisterId,
) -> InsertResult<()> {
    let Table::BTree(backing_table) = operation.backing_table.value() else {
        return Err(PhysicalInsertError::Invalid(
            "AUTOINCREMENT backing object is not a B-tree table",
        ));
    };
    let sqlite_sequence = operation
        .sqlite_sequence
        .as_ref()
        .map(|resolved| match resolved.value() {
            Table::BTree(table) => Ok(table.clone()),
            _ => Err(PhysicalInsertError::Invalid(
                "AUTOINCREMENT sqlite_sequence object is not a B-tree table",
            )),
        })
        .transpose()?;
    crate::translate::sequence::emit_disk_advance_past_from_resolved(
        program,
        operation.database.index(),
        &operation.normalized_name,
        &operation.sequence,
        backing_table.clone(),
        sqlite_sequence,
        rowid.0,
    )?;
    Ok(())
}

fn emit_generated_autoincrement_rowid(
    program: &mut ProgramBuilder,
    table_cursor: usize,
    rowid: RegisterId,
    runtime: AutoincrementRuntime,
) -> InsertResult<()> {
    let table_maximum = program.alloc_register();
    let ignored = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: table_cursor,
        rowid_reg: ignored,
        prev_largest_reg: table_maximum,
    });
    program.emit_insn(Insn::Copy {
        src_reg: runtime.maximum,
        dst_reg: rowid.0,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MemMax {
        dest_reg: rowid.0,
        src_reg: table_maximum,
    });
    let not_full = program.allocate_label();
    let maximum_i64 = program.alloc_register();
    program.emit_insn(Insn::Integer {
        dest: maximum_i64,
        value: i64::MAX,
    });
    program.emit_insn(Insn::Ne {
        lhs: rowid.0,
        rhs: maximum_i64,
        target_pc: not_full,
        flags: Default::default(),
        collation: None,
    });
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_FULL,
        description: "database or disk is full".to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(not_full);
    program.emit_insn(Insn::AddImm {
        register: rowid.0,
        value: 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: runtime.maximum,
        extra_amount: 0,
    });
    emit_update_sqlite_sequence(program, runtime, rowid.0)
}

fn emit_explicit_autoincrement_rowid(
    program: &mut ProgramBuilder,
    rowid: RegisterId,
    runtime: AutoincrementRuntime,
) -> InsertResult<()> {
    let existing_row = program.allocate_label();
    let write = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: runtime.sequence_rowid,
        target_pc: existing_row,
    });
    program.emit_insn(Insn::MemMax {
        dest_reg: runtime.maximum,
        src_reg: rowid.0,
    });
    program.emit_insn(Insn::Goto { target_pc: write });
    program.preassign_label_to_next_insn(existing_row);
    program.emit_insn(Insn::Le {
        lhs: rowid.0,
        rhs: runtime.maximum,
        target_pc: done,
        flags: Default::default(),
        collation: None,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: runtime.maximum,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(write);
    emit_update_sqlite_sequence(program, runtime, runtime.maximum)?;
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn emit_update_sqlite_sequence(
    program: &mut ProgramBuilder,
    runtime: AutoincrementRuntime,
    value: usize,
) -> InsertResult<()> {
    let fields = program.alloc_registers(2);
    let record = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: runtime.table_name,
        dst_reg: fields,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: value,
        dst_reg: fields + 1,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: fields as u32,
        count: 2,
        dest_reg: record as u32,
        index_name: None,
        // When the engine advances sqlite_sequence, SQLite stores the new value
        // as an integer even if a user previously wrote text into seq.
        affinity_str: Some("BI".to_string()),
    });
    let replace = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: runtime.sequence_rowid,
        target_pc: replace,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: runtime.cursor,
        rowid_reg: runtime.sequence_rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: runtime.cursor,
        key_reg: runtime.sequence_rowid,
        record_reg: record,
        flag: InsertFlags::new(),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::Goto { target_pc: done });
    program.preassign_label_to_next_insn(replace);
    program.emit_insn(Insn::Insert {
        cursor: runtime.cursor,
        key_reg: runtime.sequence_rowid,
        record_reg: record,
        flag: InsertFlags(ResolveType::Replace.bit_value() as u8),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn preflight_insert<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    insert: &hir::Insert,
    triggers: &PreparedTriggers,
) -> InsertResult<(
    &'plan hir::Source,
    crate::sync::Arc<crate::schema::BTreeTable>,
    usize,
)> {
    if let InsertSource::Query(query) = &insert.source {
        let query = plan
            .query(*query)
            .ok_or(PhysicalInsertError::Invalid("INSERT query is missing"))?;
        if query.hir.output.len() != insert.columns.len() {
            return Err(PhysicalInsertError::Invalid(
                "INSERT query width does not match the target column list",
            ));
        }
    }
    if insert.upserts.is_empty() != insert.excluded_source.is_none() {
        return Err(PhysicalInsertError::Invalid(
            "UPSERT and excluded source must exist together",
        ));
    }
    if !triggers.covers(&insert.triggers) {
        return Err(PhysicalInsertError::Invalid(
            "resolved trigger has no prepared program",
        ));
    }
    if !triggers.covers(&insert.upsert_triggers) {
        return Err(PhysicalInsertError::Invalid(
            "resolved UPSERT trigger has no prepared program",
        ));
    }
    if insert.foreign_keys.incoming.iter().any(|foreign_key| {
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
        return Err(PhysicalInsertError::Invalid(
            "UPSERT foreign-key action has no prepared HIR program",
        ));
    }
    if insert.foreign_keys.incoming.iter().any(|foreign_key| {
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
        return Err(PhysicalInsertError::Invalid(
            "REPLACE foreign-key action has no prepared HIR program",
        ));
    }
    let source = plan
        .document
        .source(insert.target)
        .ok_or(PhysicalInsertError::Invalid("target source is missing"))?;
    let IndexCoverage::Complete { indexes: _ } = &source.index_coverage else {
        return Err(PhysicalInsertError::Invalid(
            "target does not carry complete index metadata",
        ));
    };
    if source.check_constraints.is_none() {
        return Err(PhysicalInsertError::Invalid(
            "target does not carry CHECK metadata",
        ));
    }
    let physical = plan
        .source(insert.target)
        .ok_or(PhysicalInsertError::Invalid(
            "physical target source is missing",
        ))?;
    let PhysicalSourceKind::CatalogTable { table, access } = &physical.kind else {
        return Err(PhysicalInsertError::Invalid(
            "target is not a catalog table",
        ));
    };
    if !matches!(access, TableAccess::Scan) {
        return Err(PhysicalInsertError::Unsupported("indexed target access"));
    }
    let database = table
        .database()
        .ok_or(PhysicalInsertError::Invalid(
            "target has no database identity",
        ))?
        .index();
    let Table::BTree(table) = table.value() else {
        return Err(PhysicalInsertError::Unsupported("non-B-tree target"));
    };
    if !table.has_rowid {
        return Err(PhysicalInsertError::Unsupported("WITHOUT ROWID target"));
    }
    Ok((source, table.clone(), database))
}

fn upsert_for_rowid(insert: &hir::Insert) -> Option<&hir::Upsert> {
    insert.upserts.iter().find(|upsert| match &upsert.target {
        Some(target) => target.matched_index.is_none(),
        None => true,
    })
}

fn upsert_for_index<'insert>(
    insert: &'insert hir::Insert,
    index: &OpenedIndex<'_>,
) -> Option<&'insert hir::Upsert> {
    insert.upserts.iter().find(|upsert| match &upsert.target {
        Some(target) => target
            .matched_index
            .as_ref()
            .is_some_and(|matched| matched.id() == index.expressions.index.id()),
        None => true,
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_upsert_unique_check<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    upsert: &hir::Upsert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    excluded: RegisterRange,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    index: &OpenedIndex<'_>,
    key: &super::IndexKey,
    skip_row: crate::vdbe::BranchOffset,
    triggers: &PreparedTriggers,
) -> InsertResult<()> {
    if !index.index.unique {
        return Ok(());
    }
    let no_conflict = program.allocate_label();
    if let Some(predicate) = key.predicate {
        program.emit_insn(Insn::IfNot {
            reg: predicate,
            target_pc: no_conflict,
            jump_if_null: true,
        });
    }
    program.emit_insn(Insn::NoConflict {
        cursor_id: index.cursor,
        target_pc: no_conflict,
        record_reg: key.start,
        num_regs: key.columns,
    });
    let conflicting_rowid = RegisterId(program.alloc_register());
    program.emit_insn(Insn::IdxRowId {
        cursor_id: index.cursor,
        dest: conflicting_rowid.0,
    });
    emit_upsert_action(
        plan,
        program,
        bindings,
        insert,
        upsert,
        table,
        cursor,
        excluded,
        record,
        indexes,
        conflicting_rowid,
        skip_row,
        triggers,
    )?;
    program.preassign_label_to_next_insn(no_conflict);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_upsert_action<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    insert: &hir::Insert,
    upsert: &hir::Upsert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    excluded: RegisterRange,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    conflicting_rowid: RegisterId,
    skip_row: crate::vdbe::BranchOffset,
    triggers: &PreparedTriggers,
) -> InsertResult<()> {
    let UpsertAction::Update {
        assignments,
        predicate,
    } = &upsert.action
    else {
        program.emit_insn(Insn::Goto {
            target_pc: skip_row,
        });
        return Ok(());
    };

    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: conflicting_rowid.0,
        target_pc: skip_row,
    });
    let insert_target =
        bindings.replace_source(insert.target, SourceRuntime::Cursor(CursorId(cursor)))?;
    if let Some(predicate) = predicate {
        let condition = emit_expression_for_dml(plan, program, bindings, predicate)?;
        if condition.width != 1 {
            return Err(PhysicalInsertError::Invalid(
                "UPSERT WHERE result is not scalar",
            ));
        }
        program.emit_insn(Insn::IfNot {
            reg: condition.first.0,
            target_pc: skip_row,
            jump_if_null: true,
        });
    }

    let mut assignment_values = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let value = emit_expression_for_dml(plan, program, bindings, &assignment.value)?;
        if value.width != assignment.columns.len() {
            return Err(PhysicalInsertError::Invalid(
                "UPSERT assignment width does not match its target columns",
            ));
        }
        assignment_values.push((&assignment.columns, value));
    }

    let old_columns = RegisterRange::new(
        program.alloc_registers(table.columns().len()),
        table.columns().len(),
    );
    for position in 0..table.columns().len() {
        ExpressionEmitter::new(program, bindings).emit_into(
            &hir::Expr::column(insert.target, position),
            RegisterRange::new(old_columns.first.0 + position, 1),
        )?;
    }

    let updated = RegisterRange::new(program.alloc_registers(excluded.width), excluded.width);
    let updated_rowid = RegisterId(program.alloc_register());
    program.emit_insn(Insn::Copy {
        src_reg: conflicting_rowid.0,
        dst_reg: updated_rowid.0,
        extra_amount: 0,
    });
    for (position, column) in table.columns().iter().enumerate() {
        if column.generated_expr().is_some() {
            continue;
        }
        ExpressionEmitter::new(program, bindings).emit_into(
            &hir::Expr::column(insert.target, position),
            RegisterRange::new(updated.first.0 + position, 1),
        )?;
    }
    for (columns, value) in assignment_values {
        for (position, column) in columns.iter().enumerate() {
            let destination = match column {
                hir::TargetColumn::Column(column)
                    if table
                        .columns()
                        .get(*column)
                        .is_some_and(|column| column.is_rowid_alias()) =>
                {
                    updated_rowid.0
                }
                hir::TargetColumn::Column(column) => updated.first.0 + column,
                hir::TargetColumn::RowId => updated_rowid.0,
            };
            program.emit_insn(Insn::Copy {
                src_reg: value.first.0 + position,
                dst_reg: destination,
                extra_amount: 0,
            });
        }
    }
    if let Some((position, _)) = table.get_rowid_alias_column() {
        program.emit_insn(Insn::Copy {
            src_reg: updated_rowid.0,
            dst_reg: updated.first.0 + position,
            extra_amount: 0,
        });
    }
    let cursor_target = bindings.replace_source(
        insert.target,
        SourceRuntime::Registers {
            columns: updated,
            rowid: Some(updated_rowid),
        },
    )?;
    emit_complete_logical_row(program, bindings, insert.target, table, updated)?;
    emit_trigger_programs(
        program,
        triggers,
        insert
            .upsert_triggers
            .iter()
            .filter(|trigger| trigger.value().time == TriggerTime::Before),
        TriggerRows {
            new: Some(TriggerRow {
                columns: updated,
                rowid: updated_rowid,
            }),
            old: Some(TriggerRow {
                columns: old_columns,
                rowid: conflicting_rowid,
            }),
        },
        skip_row,
    )?;
    bindings.replace_source(insert.target, cursor_target)?;
    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: conflicting_rowid.0,
        target_pc: skip_row,
    });
    let mut old_keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        old_keys.push(emit_index_key(
            program,
            bindings,
            insert.target,
            conflicting_rowid,
            index,
            false,
        )?);
    }
    let cursor_target = bindings.replace_source(
        insert.target,
        SourceRuntime::Registers {
            columns: updated,
            rowid: Some(updated_rowid),
        },
    )?;
    emit_new_row_constraints(
        program,
        bindings,
        insert.target,
        table,
        updated,
        Some(ResolveType::Abort),
        skip_row,
    )?;
    let rowid_is_unique = program.allocate_label();
    program.emit_insn(Insn::MustBeInt {
        reg: updated_rowid.0,
        target_pc: None,
    });
    program.emit_insn(Insn::Eq {
        lhs: updated_rowid.0,
        rhs: conflicting_rowid.0,
        target_pc: rowid_is_unique,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: updated_rowid.0,
        target_pc: rowid_is_unique,
    });
    super::constraint_halt(
        program,
        SQLITE_CONSTRAINT_PRIMARYKEY,
        format!("{}.rowid", table.name),
        ResolveType::Abort,
    );
    program.preassign_label_to_next_insn(rowid_is_unique);
    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: conflicting_rowid.0,
        target_pc: skip_row,
    });
    let mut new_keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        let key = emit_index_key(program, bindings, insert.target, updated_rowid, index, true)?;
        emit_unique_check(
            program,
            index,
            &key,
            Some(conflicting_rowid),
            ResolveType::Abort,
            skip_row,
        )?;
        new_keys.push(key);
    }
    super::prepare_update_row(
        program,
        bindings,
        table,
        old_columns,
        updated,
        conflicting_rowid,
        updated_rowid,
        &insert.foreign_keys,
    )?;
    emit_stored_record(program, bindings, insert.target, table, updated, record)?;
    bindings.replace_source(insert.target, cursor_target)?;
    super::finish_update_row(
        program,
        bindings,
        table,
        cursor,
        indexes,
        &old_keys,
        &new_keys,
        record,
        old_columns,
        updated,
        conflicting_rowid,
        updated_rowid,
        &insert.foreign_keys,
        triggers,
    )?;
    let after_trigger_done = program.allocate_label();
    emit_trigger_programs(
        program,
        triggers,
        insert
            .upsert_triggers
            .iter()
            .filter(|trigger| trigger.value().time == TriggerTime::After),
        TriggerRows {
            new: Some(TriggerRow {
                columns: updated,
                rowid: updated_rowid,
            }),
            old: Some(TriggerRow {
                columns: old_columns,
                rowid: conflicting_rowid,
            }),
        },
        after_trigger_done,
    )?;
    program.preassign_label_to_next_insn(after_trigger_done);
    if let Some(returning) = &insert.returning {
        let cursor_target = bindings.replace_source(
            insert.target,
            SourceRuntime::Registers {
                columns: updated,
                rowid: Some(updated_rowid),
            },
        )?;
        let result = emit_returning_values(plan, program, bindings, returning)?;
        bindings.replace_source(insert.target, cursor_target)?;
        emit_returning_result(program, result);
    }
    bindings.replace_source(insert.target, insert_target)?;
    program.emit_insn(Insn::Goto {
        target_pc: skip_row,
    });
    Ok(())
}
