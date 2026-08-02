//! Shared secondary-index maintenance for HIR DML row images.

use std::fmt;

use turso_parser::ast::ResolveType;

use crate::{
    error::SQLITE_CONSTRAINT_UNIQUE,
    schema::Index,
    sync::Arc,
    translate::semantic::hir::{self, Expr},
    vdbe::{
        builder::ProgramBuilder,
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, Insn, RegisterOrLiteral},
    },
    LimboError,
};

use super::{
    ExpressionEmitter, PhysicalExpressionError, RegisterId, RuntimeBindingError, RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalIndexError {
    Engine(LimboError),
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
}

impl fmt::Display for PhysicalIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical index: {message}"),
        }
    }
}

impl std::error::Error for PhysicalIndexError {}

impl From<LimboError> for PhysicalIndexError {
    fn from(error: LimboError) -> Self {
        Self::Engine(error)
    }
}

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
        if expressions.columns.len() != index.columns.len() {
            return Err(PhysicalIndexError::Invalid(
                "index expression and catalog column widths differ",
            ));
        }
        let cursor = program.alloc_cursor_index(None, &index)?;
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
    emit_index_key_from_expressions(
        program,
        bindings,
        source,
        rowid,
        opened,
        opened.expressions,
        make_record,
    )
}

/// Build a key for a different row identity of the same opened index. UPDATE
/// uses this to keep OLD and NEW schema programs distinct without opening the
/// physical index twice.
pub(crate) fn emit_index_key_from_expressions(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    rowid: RegisterId,
    opened: &OpenedIndex<'_>,
    expressions: &hir::IndexExpressions,
    make_record: bool,
) -> IndexResult<IndexKey> {
    if expressions.index != opened.expressions.index {
        return Err(PhysicalIndexError::Invalid(
            "OLD and NEW index programs refer to different indexes",
        ));
    }
    let predicate = expressions
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
        .zip(&expressions.columns)
        .enumerate()
    {
        let expression = expression
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Expr::column(source, catalog_column.pos_in_table));
        ExpressionEmitter::new(program, bindings)
            .emit_into(&expression, super::RegisterRange::new(start + position, 1))?;
        // Direct generated columns carry their stored expression in the
        // catalog, but they are still table columns and must receive that
        // column's declared affinity. Only a true expression-index term uses
        // the sentinel position and has no table-column affinity.
        if catalog_column.pos_in_table != crate::schema::EXPR_INDEX_SENTINEL {
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
        // Index-method cursors must stay alive until the statement commits so
        // pre_commit can flush their writes and their Drop can cache the writer.
        if index.index.index_method.is_some() && !index.index.is_backing_btree_index() {
            continue;
        }
        program.emit_insn(Insn::Close {
            cursor_id: index.cursor,
        });
    }
}
