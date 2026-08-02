//! Direct INSERT lowering from a closed HIR row image.

use std::fmt;

use crate::{
    error::SQLITE_CONSTRAINT_PRIMARYKEY,
    schema::Table,
    translate::semantic::hir::{self, IndexCoverage, InsertSource},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    close_indexes, emit_complete_logical_row, emit_index_insert, emit_index_key,
    emit_new_row_constraints, emit_stored_record, emit_unique_check, open_indexes,
    ExpressionEmitter, OpenedIndex, PhysicalExpressionError, PhysicalIndexError, PhysicalPlan,
    PhysicalRoot, PhysicalRowError, PhysicalSourceKind, RegisterId, RegisterRange,
    RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalInsertError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Row(PhysicalRowError),
    Index(PhysicalIndexError),
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

    program.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database,
    });
    let indexes = open_indexes(program, source, database)?;

    match &insert.source {
        InsertSource::DefaultValues => {
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
            )?;
        }
        InsertSource::Values(rows) => {
            for row in rows {
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
                )?;
            }
        }
        InsertSource::Query(_) => {
            return Err(PhysicalInsertError::Unsupported("INSERT SELECT"));
        }
    }
    close_indexes(program, &indexes);
    program.emit_insn(Insn::Close { cursor_id: cursor });
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
) -> InsertResult<()> {
    if values.len() != insert.columns.len() {
        return Err(PhysicalInsertError::Invalid(
            "VALUES width does not match the target column list",
        ));
    }
    program.emit_insn(Insn::Null {
        dest: logical.first.0,
        dest_end: Some(logical.first.0 + logical.width - 1),
    });
    program.emit_insn(Insn::Null {
        dest: rowid.0,
        dest_end: None,
    });

    for (target, value) in insert.columns.iter().zip(values) {
        let destination = match target {
            hir::TargetColumn::Column(column) => {
                logical
                    .register(*column)
                    .ok_or(PhysicalInsertError::Invalid(
                        "target column is outside the row",
                    ))?
            }
            hir::TargetColumn::RowId => rowid,
        };
        ExpressionEmitter::new(program, bindings)
            .emit_into(value, RegisterRange::new(destination.0, 1))?;
    }
    for default in &insert.defaults {
        let destination = logical
            .register(default.column)
            .ok_or(PhysicalInsertError::Invalid(
                "default column is outside the row",
            ))?;
        ExpressionEmitter::new(program, bindings)
            .emit_into(&default.value, RegisterRange::new(destination.0, 1))?;
    }

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
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
        description: format!("{}.{}", table.name, rowid_name),
        on_error: Some(turso_parser::ast::ResolveType::Abort),
        description_reg: None,
    });
    program.preassign_label_to_next_insn(rowid_is_unique);
    emit_complete_logical_row(program, bindings, insert.target, table, logical)?;
    emit_new_row_constraints(program, bindings, insert.target, table, logical)?;
    let mut keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        let key = emit_index_key(program, bindings, insert.target, rowid, index, true)?;
        emit_unique_check(program, index, &key, None)?;
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
    if insert.returning.is_some() {
        return Err(PhysicalInsertError::Unsupported("RETURNING"));
    }
    if insert.conflict.is_some() {
        return Err(PhysicalInsertError::Unsupported("conflict policy"));
    }
    if !insert.upserts.is_empty() || insert.excluded_source.is_some() {
        return Err(PhysicalInsertError::Unsupported("UPSERT"));
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
