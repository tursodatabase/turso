//! Direct INSERT lowering from a closed HIR row image.

use std::fmt;

use turso_parser::ast::ResolveType;

use crate::{
    error::SQLITE_CONSTRAINT_PRIMARYKEY,
    schema::Table,
    translate::semantic::hir::{self, IndexCoverage, InsertSource, UpsertAction},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_complete_logical_row, emit_index_insert, emit_index_key,
    emit_new_row_constraints, emit_query_for_dml, emit_returning_result, emit_returning_values,
    emit_stored_record, emit_unique_check, open_indexes, CursorId, ExpressionEmitter, OpenedIndex,
    PhysicalExpressionError, PhysicalIndexError, PhysicalPlan, PhysicalQueryError, PhysicalRoot,
    PhysicalRowError, PhysicalSourceKind, RegisterId, RegisterRange, RuntimeBindingError,
    RuntimeBindings, SourceRuntime, TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalInsertError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Row(PhysicalRowError),
    Index(PhysicalIndexError),
    Query(PhysicalQueryError),
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

type InsertResult<T> = std::result::Result<T, PhysicalInsertError>;

pub(crate) fn emit_root_insert(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> InsertResult<()> {
    let insert = match &plan.root {
        PhysicalRoot::Insert(insert) => *insert,
        _ => return Err(PhysicalInsertError::Unsupported("non-INSERT HIR root")),
    };
    let (source, table, database) = preflight_insert(plan, insert)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let logical = RegisterRange::new(
        program.alloc_registers(source.columns.len()),
        source.columns.len(),
    );
    let rowid = RegisterId(program.alloc_register());
    let record = program.alloc_register();
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
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
    let indexes = open_indexes(program, source, database)?;

    match &insert.source {
        InsertSource::DefaultValues => {
            let skip_row = program.allocate_label();
            emit_insert_row(
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
            )?;
            program.preassign_label_to_next_insn(skip_row);
        }
        InsertSource::Values(rows) => {
            for row in rows {
                let skip_row = program.allocate_label();
                emit_insert_row(
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
                )?;
                program.preassign_label_to_next_insn(skip_row);
            }
        }
        InsertSource::Query(_) => {
            let query_rows = query_rows.as_ref().ok_or(PhysicalInsertError::Invalid(
                "INSERT query was not materialized",
            ))?;
            let done = program.allocate_label();
            let next = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: query_rows.cursor,
                pc_if_empty: done,
            });
            program.preassign_label_to_next_insn(next);
            emit_insert_query_row(
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
                next,
            )?;
            program.emit_insn(Insn::Next {
                cursor_id: query_rows.cursor,
                pc_if_next: next,
            });
            program.preassign_label_to_next_insn(done);
        }
    }
    close_indexes(program, &indexes);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    if let Some(query_rows) = query_rows {
        query_rows.close(program);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_insert_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    values: &[hir::Expr],
    skip_row: crate::vdbe::BranchOffset,
) -> InsertResult<()> {
    if values.len() != insert.columns.len() {
        return Err(PhysicalInsertError::Invalid(
            "VALUES width does not match the target column list",
        ));
    }
    initialize_insert_row(program, logical, rowid);

    for (target, value) in insert.columns.iter().zip(values) {
        let destination = target_register(*target, logical, rowid)?;
        ExpressionEmitter::new(program, bindings)
            .emit_into(value, RegisterRange::new(destination.0, 1))?;
    }
    emit_insert_defaults(program, bindings, insert, logical)?;
    finish_insert_row(
        program, bindings, insert, table, cursor, logical, rowid, record, indexes, skip_row,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_insert_query_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    query_cursor: usize,
    skip_row: crate::vdbe::BranchOffset,
) -> InsertResult<()> {
    initialize_insert_row(program, logical, rowid);
    for (position, target) in insert.columns.iter().enumerate() {
        let destination = target_register(*target, logical, rowid)?;
        program.emit_column_or_rowid(query_cursor, position, destination.0);
    }
    emit_insert_defaults(program, bindings, insert, logical)?;
    finish_insert_row(
        program, bindings, insert, table, cursor, logical, rowid, record, indexes, skip_row,
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

fn emit_insert_defaults(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    insert: &hir::Insert,
    logical: RegisterRange,
) -> InsertResult<()> {
    for default in &insert.defaults {
        let destination = logical
            .register(default.column)
            .ok_or(PhysicalInsertError::Invalid(
                "default column is outside the row",
            ))?;
        ExpressionEmitter::new(program, bindings)
            .emit_into(&default.value, RegisterRange::new(destination.0, 1))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_insert_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    insert: &hir::Insert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    logical: RegisterRange,
    rowid: RegisterId,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    skip_row: crate::vdbe::BranchOffset,
) -> InsertResult<()> {
    let statement_conflict = insert.conflict.unwrap_or(ResolveType::Abort);
    if let Some((position, _)) = table.get_rowid_alias_column().filter(|(position, _)| {
        insert
            .columns
            .contains(&hir::TargetColumn::Column(*position))
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
    program.emit_insn(Insn::NewRowid {
        cursor,
        rowid_reg: rowid.0,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Goto {
        target_pc: rowid_ready,
    });
    program.preassign_label_to_next_insn(explicit_rowid);
    program.emit_insn(Insn::MustBeInt {
        reg: rowid.0,
        target_pc: None,
    });
    program.preassign_label_to_next_insn(rowid_ready);
    if let Some((position, _)) = table.get_rowid_alias_column() {
        program.emit_insn(Insn::Copy {
            src_reg: rowid.0,
            dst_reg: logical.first.0 + position,
            extra_amount: 0,
        });
    }
    emit_complete_logical_row(program, bindings, insert.target, table, logical)?;
    emit_new_row_constraints(
        program,
        bindings,
        insert.target,
        table,
        logical,
        statement_conflict,
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
            program, bindings, insert, upsert, table, cursor, logical, record, indexes, rowid,
            skip_row,
        )?;
    } else if statement_conflict == ResolveType::Ignore {
        program.emit_insn(Insn::Goto {
            target_pc: skip_row,
        });
    } else {
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!("{}.{}", table.name, rowid_name),
            on_error: Some(statement_conflict),
            description_reg: None,
        });
    }
    program.preassign_label_to_next_insn(rowid_is_unique);
    let mut keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        let key = emit_index_key(program, bindings, insert.target, rowid, index, true)?;
        if let Some(upsert) = upsert_for_index(insert, index) {
            emit_upsert_unique_check(
                program, bindings, insert, upsert, table, cursor, logical, record, indexes, index,
                &key, skip_row,
            )?;
        } else {
            let conflict = insert
                .conflict
                .or(index.index.on_conflict)
                .unwrap_or(ResolveType::Abort);
            emit_unique_check(program, index, &key, None, conflict, skip_row)?;
        }
        keys.push(key);
    }
    emit_stored_record(program, bindings, insert.target, table, logical, record)?;
    for (index, key) in indexes.iter().zip(&keys) {
        emit_index_insert(program, index, key)?;
    }
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: rowid.0,
        record_reg: record,
        flag: InsertFlags::new(),
        table_name: table.name.clone(),
    });
    if let Some(returning) = &insert.returning {
        let result = emit_returning_values(program, bindings, returning)?;
        emit_returning_result(program, result);
    }
    Ok(())
}

fn preflight_insert<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    insert: &hir::Insert,
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
    if insert.conflict == Some(ResolveType::Replace) {
        return Err(PhysicalInsertError::Unsupported("REPLACE conflict policy"));
    }
    if insert.upserts.is_empty() != insert.excluded_source.is_none() {
        return Err(PhysicalInsertError::Invalid(
            "UPSERT and excluded source must exist together",
        ));
    }
    if insert.upserts.iter().any(|upsert| {
        matches!(
            &upsert.action,
            UpsertAction::Update { assignments, .. }
                if assignments
                    .iter()
                    .flat_map(|assignment| &assignment.columns)
                    .any(|column| matches!(column, hir::TargetColumn::RowId))
        )
    }) {
        return Err(PhysicalInsertError::Unsupported(
            "rowid assignment in UPSERT DO UPDATE",
        ));
    }
    if insert.trigger.is_some() || !insert.triggers.is_empty() {
        return Err(PhysicalInsertError::Unsupported("trigger execution"));
    }
    if !insert.foreign_keys.outgoing.is_empty() || !insert.foreign_keys.incoming.is_empty() {
        return Err(PhysicalInsertError::Unsupported("foreign-key checks"));
    }
    let source = plan
        .document
        .source(insert.target)
        .ok_or(PhysicalInsertError::Invalid("target source is missing"))?;
    if source.index_expressions.iter().any(|index| {
        insert.conflict.or(index.index.value().on_conflict) == Some(ResolveType::Replace)
    }) {
        return Err(PhysicalInsertError::Unsupported(
            "REPLACE index conflict policy",
        ));
    }
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
    if table
        .columns()
        .iter()
        .any(|column| column.notnull_conflict_clause.is_some())
    {
        return Err(PhysicalInsertError::Unsupported(
            "column NOT NULL conflict policy",
        ));
    }
    if table.rowid_alias_conflict_clause.is_some() {
        return Err(PhysicalInsertError::Unsupported(
            "INTEGER PRIMARY KEY conflict policy",
        ));
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
fn emit_upsert_unique_check(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
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
    )?;
    program.preassign_label_to_next_insn(no_conflict);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_upsert_action(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    insert: &hir::Insert,
    upsert: &hir::Upsert,
    table: &crate::sync::Arc<crate::schema::BTreeTable>,
    cursor: usize,
    excluded: RegisterRange,
    record: usize,
    indexes: &[OpenedIndex<'_>],
    conflicting_rowid: RegisterId,
    skip_row: crate::vdbe::BranchOffset,
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
    let proposed_target =
        bindings.replace_source(insert.target, SourceRuntime::Cursor(CursorId(cursor)))?;
    if let Some(predicate) = predicate {
        let condition = ExpressionEmitter::new(program, bindings).emit_new(predicate)?;
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
        let value = ExpressionEmitter::new(program, bindings).emit_new(&assignment.value)?;
        if value.width != assignment.columns.len() {
            return Err(PhysicalInsertError::Invalid(
                "UPSERT assignment width does not match its target columns",
            ));
        }
        assignment_values.push((&assignment.columns, value));
    }

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

    let updated = RegisterRange::new(program.alloc_registers(excluded.width), excluded.width);
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
            let hir::TargetColumn::Column(column) = column else {
                return Err(PhysicalInsertError::Unsupported(
                    "rowid assignment in UPSERT DO UPDATE",
                ));
            };
            program.emit_insn(Insn::Copy {
                src_reg: value.first.0 + position,
                dst_reg: updated.first.0 + column,
                extra_amount: 0,
            });
        }
    }
    let old_target = bindings.replace_source(
        insert.target,
        SourceRuntime::Registers {
            columns: updated,
            rowid: Some(conflicting_rowid),
        },
    )?;
    emit_complete_logical_row(program, bindings, insert.target, table, updated)?;
    emit_new_row_constraints(
        program,
        bindings,
        insert.target,
        table,
        updated,
        ResolveType::Abort,
        skip_row,
    )?;
    let mut new_keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        let key = emit_index_key(
            program,
            bindings,
            insert.target,
            conflicting_rowid,
            index,
            true,
        )?;
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
    emit_stored_record(program, bindings, insert.target, table, updated, record)?;
    for (index, key) in indexes.iter().zip(&old_keys) {
        super::emit_index_delete(program, index, key);
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
        key_reg: conflicting_rowid.0,
        record_reg: record,
        flag: InsertFlags::new(),
        table_name: table.name.clone(),
    });
    if let Some(returning) = &insert.returning {
        let result = emit_returning_values(program, bindings, returning)?;
        emit_returning_result(program, result);
    }
    bindings.replace_source(insert.target, old_target)?;
    bindings.replace_source(insert.target, proposed_target)?;
    program.emit_insn(Insn::Goto {
        target_pc: skip_row,
    });
    Ok(())
}
