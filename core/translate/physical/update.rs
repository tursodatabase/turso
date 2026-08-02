//! Stable-rowset UPDATE lowering from closed HIR.

use std::fmt;

use crate::{
    schema::Table,
    translate::semantic::hir::{self, Expr, IndexCoverage},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    emit_complete_logical_row, emit_stored_record, CursorId, ExpressionEmitter,
    PhysicalExpressionError, PhysicalPlan, PhysicalRoot, PhysicalRowError, PhysicalSourceKind,
    RegisterId, RegisterRange, RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalUpdateError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Row(PhysicalRowError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
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

type UpdateResult<T> = std::result::Result<T, PhysicalUpdateError>;

pub(crate) fn emit_root_update(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> UpdateResult<()> {
    let update = match &plan.root {
        PhysicalRoot::Update(update) => *update,
        _ => return Err(PhysicalUpdateError::Unsupported("non-UPDATE HIR root")),
    };
    let (source, table, database) = preflight_update(plan, update)?;
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    bindings.bind_source(update.target, SourceRuntime::Cursor(CursorId(cursor)))?;

    let rowset = program.alloc_register();
    let rowid = RegisterId(program.alloc_register());
    let logical = RegisterRange::new(
        program.alloc_registers(source.columns.len()),
        source.columns.len(),
    );
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

    let scan_start = program.allocate_label();
    let scan_next = program.allocate_label();
    let scan_done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: scan_done,
    });
    program.preassign_label_to_next_insn(scan_start);
    if let Some(predicate) = &update.predicate {
        let condition = ExpressionEmitter::new(program, &mut bindings).emit_new(predicate)?;
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

    let write_start = program.allocate_label();
    let write_next = program.allocate_label();
    let write_done = program.allocate_label();
    program.preassign_label_to_next_insn(write_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: rowset,
        pc_if_empty: write_done,
        dest_reg: rowid.0,
    });
    program.emit_insn(Insn::NotExists {
        cursor,
        rowid_reg: rowid.0,
        target_pc: write_next,
    });

    for (position, column) in table.columns().iter().enumerate() {
        if column.generated_expr().is_some() {
            continue;
        }
        ExpressionEmitter::new(program, &mut bindings).emit_into(
            &Expr::column(update.target, position),
            RegisterRange::new(logical.first.0 + position, 1),
        )?;
    }
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let values = ExpressionEmitter::new(program, &mut bindings).emit_new(&assignment.value)?;
        if values.width != assignment.columns.len() {
            return Err(PhysicalUpdateError::Invalid(
                "assignment width does not match its target columns",
            ));
        }
        assignments.push((&assignment.columns, values));
    }
    for (columns, values) in assignments {
        for (position, column) in columns.iter().enumerate() {
            let hir::TargetColumn::Column(column) = column else {
                return Err(PhysicalUpdateError::Unsupported("rowid assignment"));
            };
            program.emit_insn(Insn::Copy {
                src_reg: values.first.0 + position,
                dst_reg: logical.first.0 + column,
                extra_amount: 0,
            });
        }
    }

    let old_runtime = bindings.replace_source(
        update.target,
        SourceRuntime::Registers {
            columns: logical,
            rowid: Some(rowid),
        },
    )?;
    emit_complete_logical_row(program, &mut bindings, update.target, &table, logical)?;
    emit_stored_record(
        program,
        &mut bindings,
        update.target,
        &table,
        logical,
        record,
    )?;
    bindings.replace_source(update.target, old_runtime)?;

    program.emit_insn(Insn::Delete {
        cursor_id: cursor,
        table_name: table.name.clone(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Insert {
        cursor,
        key_reg: rowid.0,
        record_reg: record,
        flag: InsertFlags::new(),
        table_name: table.name.clone(),
    });
    program.preassign_label_to_next_insn(write_next);
    program.emit_insn(Insn::Goto {
        target_pc: write_start,
    });
    program.preassign_label_to_next_insn(write_done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

fn preflight_update<'plan>(
    plan: &'plan PhysicalPlan<'plan>,
    update: &hir::Update,
) -> UpdateResult<(
    &'plan hir::Source,
    crate::sync::Arc<crate::schema::BTreeTable>,
    usize,
)> {
    if update.from.is_some() {
        return Err(PhysicalUpdateError::Unsupported("UPDATE FROM"));
    }
    if !update.order_by.is_empty() || update.limit.is_some() {
        return Err(PhysicalUpdateError::Unsupported("ORDER BY or LIMIT"));
    }
    if update.returning.is_some() {
        return Err(PhysicalUpdateError::Unsupported("RETURNING"));
    }
    if update.conflict.is_some() {
        return Err(PhysicalUpdateError::Unsupported("conflict policy"));
    }
    if update.trigger.is_some() || !update.triggers.is_empty() {
        return Err(PhysicalUpdateError::Unsupported("trigger execution"));
    }
    if !update.foreign_keys.outgoing.is_empty() || !update.foreign_keys.incoming.is_empty() {
        return Err(PhysicalUpdateError::Unsupported("foreign-key checks"));
    }
    if update
        .assignments
        .iter()
        .flat_map(|assignment| &assignment.columns)
        .any(|column| matches!(column, hir::TargetColumn::RowId))
    {
        return Err(PhysicalUpdateError::Unsupported("rowid assignment"));
    }
    let source = plan
        .document
        .source(update.target)
        .ok_or(PhysicalUpdateError::Invalid("target source is missing"))?;
    let IndexCoverage::Complete { indexes } = &source.index_coverage else {
        return Err(PhysicalUpdateError::Invalid(
            "target does not carry complete index metadata",
        ));
    };
    if !indexes.is_empty() || !source.index_method_patterns.is_empty() {
        return Err(PhysicalUpdateError::Unsupported("secondary indexes"));
    }
    if source
        .check_constraints
        .as_ref()
        .is_some_and(|checks| !checks.is_empty())
    {
        return Err(PhysicalUpdateError::Unsupported("CHECK constraints"));
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
    if table.is_strict {
        return Err(PhysicalUpdateError::Unsupported("STRICT table checks"));
    }
    if table.columns().iter().any(|column| column.notnull()) {
        return Err(PhysicalUpdateError::Unsupported("NOT NULL constraints"));
    }
    if table.get_rowid_alias_column().is_some() {
        return Err(PhysicalUpdateError::Unsupported("INTEGER PRIMARY KEY"));
    }
    Ok((source, table.clone(), database))
}
