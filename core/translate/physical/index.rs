//! Shared secondary-index maintenance for HIR DML row images.

use std::fmt;

use turso_parser::ast::ResolveType;

use crate::{
    error::SQLITE_CONSTRAINT_UNIQUE,
    schema::Index,
    sync::Arc,
    translate::semantic::hir::{self, Expr},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    CursorId, ExpressionEmitter, PhysicalExpressionError, RegisterId, RuntimeBindingError,
    RuntimeBindings, SourceRuntime,
};

#[derive(Debug)]
pub(crate) enum PhysicalIndexError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical index: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "physical index is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalIndexError {}

impl From<PhysicalExpressionError> for PhysicalIndexError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

impl From<RuntimeBindingError> for PhysicalIndexError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

type IndexResult<T> = std::result::Result<T, PhysicalIndexError>;

pub(crate) struct OpenedIndex<'hir> {
    pub(crate) expressions: &'hir hir::IndexExpressions,
    pub(crate) index: Arc<Index>,
    pub(crate) cursor: usize,
}

pub(crate) struct IndexKey {
    pub(crate) predicate: Option<usize>,
    pub(crate) start: usize,
    pub(crate) columns: usize,
    pub(crate) record: Option<usize>,
}

pub(crate) fn open_indexes<'hir>(
    program: &mut ProgramBuilder,
    source: &'hir hir::Source,
    database: usize,
) -> IndexResult<Vec<OpenedIndex<'hir>>> {
    if !source.index_method_patterns.is_empty() {
        return Err(PhysicalIndexError::Unsupported("custom index methods"));
    }
    let hir::IndexCoverage::Complete { indexes } = &source.index_coverage else {
        return Err(PhysicalIndexError::Invalid(
            "DML target does not carry complete index metadata",
        ));
    };
    if indexes.len() != source.index_expressions.len() {
        return Err(PhysicalIndexError::Invalid(
            "index identities and expressions have different widths",
        ));
    }

    let mut opened = Vec::with_capacity(source.index_expressions.len());
    for expressions in &source.index_expressions {
        let index = expressions.index.handle();
        if index.index_method.is_some() {
            return Err(PhysicalIndexError::Unsupported("custom index methods"));
        }
        if expressions.columns.len() != index.columns.len() {
            return Err(PhysicalIndexError::Invalid(
                "index expression and catalog column widths differ",
            ));
        }
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: database,
        });
        opened.push(OpenedIndex {
            expressions,
            index,
            cursor,
        });
    }
    Ok(opened)
}

/// Build `[index columns..., rowid]` from the source's current runtime image.
/// Partial-index membership is kept as a register so callers can prepare all
/// keys before performing mutations in SQLite's required order.
pub(crate) fn emit_index_key(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    rowid: RegisterId,
    opened: &OpenedIndex<'_>,
    make_record: bool,
) -> IndexResult<IndexKey> {
    let predicate = opened
        .expressions
        .predicate
        .as_ref()
        .map(|predicate| ExpressionEmitter::new(program, bindings).emit_new(predicate))
        .transpose()?
        .map(|result| {
            if result.width != 1 {
                return Err(PhysicalIndexError::Invalid(
                    "partial-index predicate is not scalar",
                ));
            }
            Ok(result.first.0)
        })
        .transpose()?;
    let columns = opened.index.columns.len();
    let start = program.alloc_registers(columns + 1);
    let source_definition = bindings
        .document()
        .source(source)
        .ok_or(PhysicalIndexError::Invalid("index source is missing"))?;

    for (position, (catalog_column, expression)) in opened
        .index
        .columns
        .iter()
        .zip(&opened.expressions.columns)
        .enumerate()
    {
        let expression = expression
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Expr::column(source, catalog_column.pos_in_table));
        ExpressionEmitter::new(program, bindings)
            .emit_into(&expression, super::RegisterRange::new(start + position, 1))?;
        if opened.expressions.columns[position].is_none() {
            let affinity = source_definition
                .columns
                .get(catalog_column.pos_in_table)
                .ok_or(PhysicalIndexError::Invalid(
                    "index column is outside its table",
                ))?
                .affinity;
            program.emit_column_affinity(start + position, affinity);
        }
    }
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: start + columns,
        extra_amount: 0,
    });
    let record = make_record.then(|| {
        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(columns + 1),
            dest_reg: to_u32(record),
            index_name: Some(opened.index.name.clone()),
            affinity_str: None,
        });
        record
    });
    Ok(IndexKey {
        predicate,
        start,
        columns,
        record,
    })
}

pub(crate) fn emit_unique_check(
    program: &mut ProgramBuilder,
    opened: &OpenedIndex<'_>,
    key: &IndexKey,
    current_rowid: Option<RegisterId>,
    conflict: ResolveType,
    skip_row: crate::vdbe::BranchOffset,
) -> IndexResult<()> {
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
    if let Some(current_rowid) = current_rowid {
        let conflicting_rowid = program.alloc_register();
        program.emit_insn(Insn::IdxRowId {
            cursor_id: opened.cursor,
            dest: conflicting_rowid,
        });
        program.emit_insn(Insn::Eq {
            lhs: current_rowid.0,
            rhs: conflicting_rowid,
            target_pc: done,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
    }
    if conflict == ResolveType::Ignore {
        program.emit_insn(Insn::Goto {
            target_pc: skip_row,
        });
    } else {
        let description = opened
            .index
            .columns
            .iter()
            .map(|column| format!("{}.{}", opened.index.table_name, column.name))
            .collect::<Vec<_>>()
            .join(", ");
        super::constraint_halt(program, SQLITE_CONSTRAINT_UNIQUE, description, conflict);
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_replace_unique_check(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<crate::schema::BTreeTable>,
    table_cursor: usize,
    indexes: &[OpenedIndex<'_>],
    opened: &OpenedIndex<'_>,
    key: &IndexKey,
    current_rowid: Option<RegisterId>,
    not_found: crate::vdbe::BranchOffset,
) -> IndexResult<()> {
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
    )?;
    program.preassign_label_to_next_insn(done);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_replace_conflicting_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &Arc<crate::schema::BTreeTable>,
    cursor: usize,
    indexes: &[OpenedIndex<'_>],
    conflicting_rowid: RegisterId,
    return_to_rowid: Option<RegisterId>,
    not_found: crate::vdbe::BranchOffset,
) -> IndexResult<()> {
    program.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: conflicting_rowid.0,
        target_pc: not_found,
    });
    let proposed = bindings.replace_source(source, SourceRuntime::Cursor(CursorId(cursor)))?;
    let mut old_keys = Vec::with_capacity(indexes.len());
    for index in indexes {
        old_keys.push(emit_index_key(
            program,
            bindings,
            source,
            conflicting_rowid,
            index,
            false,
        )?);
    }
    for (index, key) in indexes.iter().zip(&old_keys) {
        emit_index_delete(program, index, key);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
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

pub(crate) fn emit_index_delete(
    program: &mut ProgramBuilder,
    opened: &OpenedIndex<'_>,
    key: &IndexKey,
) {
    let done = key.predicate.map(|predicate| {
        let done = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: predicate,
            target_pc: done,
            jump_if_null: true,
        });
        done
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: key.start,
        num_regs: key.columns + 1,
        cursor_id: opened.cursor,
        raise_error_if_no_matching_entry: opened.expressions.predicate.is_none(),
    });
    if let Some(done) = done {
        program.preassign_label_to_next_insn(done);
    }
}

pub(crate) fn emit_index_insert(
    program: &mut ProgramBuilder,
    opened: &OpenedIndex<'_>,
    key: &IndexKey,
) -> IndexResult<()> {
    let record = key.record.ok_or(PhysicalIndexError::Invalid(
        "index insertion key has no packed record",
    ))?;
    let done = key.predicate.map(|predicate| {
        let done = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: predicate,
            target_pc: done,
            jump_if_null: true,
        });
        done
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: opened.cursor,
        record_reg: record,
        unpacked_start: Some(key.start),
        unpacked_count: Some((key.columns + 1) as u32),
        flags: IdxInsertFlags::new().nchange(true),
    });
    if let Some(done) = done {
        program.preassign_label_to_next_insn(done);
    }
    Ok(())
}

pub(crate) fn close_indexes(program: &mut ProgramBuilder, opened: &[OpenedIndex<'_>]) {
    for index in opened {
        program.emit_insn(Insn::Close {
            cursor_id: index.cursor,
        });
    }
}
