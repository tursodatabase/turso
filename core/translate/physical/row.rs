//! Shared logical-row construction and record storage for DML roots.

use std::fmt;

use turso_parser::ast::ResolveType;

use crate::{
    error::{
        SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY,
        SQLITE_CONSTRAINT_UNIQUE,
    },
    schema::BTreeTable,
    translate::semantic::hir::{self, ColumnReadExpression},
    vdbe::{
        builder::ProgramBuilder,
        insn::{to_u32, Insn},
    },
};

pub(super) fn constraint_halt(
    program: &mut ProgramBuilder,
    err_code: usize,
    description: String,
    conflict: ResolveType,
) {
    let (description, on_error) = if program.flags.has_statement_conflict() {
        (description, None)
    } else {
        match conflict {
            ResolveType::Fail | ResolveType::Rollback => {
                let kind = match err_code {
                    SQLITE_CONSTRAINT_CHECK => "CHECK",
                    SQLITE_CONSTRAINT_NOTNULL => "NOT NULL",
                    SQLITE_CONSTRAINT_PRIMARYKEY | SQLITE_CONSTRAINT_UNIQUE => "UNIQUE",
                    _ => "constraint",
                };
                (
                    format!("{kind} constraint failed: {description} (19)"),
                    Some(conflict),
                )
            }
            ResolveType::Abort | ResolveType::Ignore | ResolveType::Replace => (description, None),
        }
    };
    program.emit_insn(Insn::Halt {
        err_code,
        description,
        on_error,
        description_reg: None,
    });
}

use super::{
    ExpressionEmitter, PhysicalExpressionError, RegisterRange, RuntimeBindingError, RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalRowError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
    Emission(String),
}

impl fmt::Display for PhysicalRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical row: {message}"),
            Self::Emission(message) => write!(formatter, "could not emit physical row: {message}"),
        }
    }
}

impl std::error::Error for PhysicalRowError {}

impl From<RuntimeBindingError> for PhysicalRowError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalRowError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

type RowResult<T> = std::result::Result<T, PhysicalRowError>;

/// Apply base affinities, then evaluate generated columns in dependency order.
/// The source must already be bound to `logical` registers.
pub(crate) fn emit_complete_logical_row(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &BTreeTable,
    logical: RegisterRange,
) -> RowResult<()> {
    for (position, column) in table.columns().iter().enumerate() {
        if column.generated_expr().is_none() {
            program.emit_column_affinity(
                logical.first.0 + position,
                source_affinity(bindings, source, position)?,
            );
        }
    }
    for (position, column) in table
        .columns_topo_sort()
        .map_err(|error| PhysicalRowError::Emission(error.to_string()))?
        .iter()
    {
        if column.generated_expr().is_none() {
            continue;
        }
        let ColumnReadExpression::Planned(expression) = &bindings
            .document()
            .source(source)
            .expect("validated DML target exists")
            .generated_expressions[position]
        else {
            return Err(PhysicalRowError::Invalid(
                "generated column has no closed HIR expression",
            ));
        };
        ExpressionEmitter::new(program, bindings).emit_into(
            expression,
            RegisterRange::new(logical.first.0 + position, 1),
        )?;
        program.emit_column_affinity(
            logical.first.0 + position,
            source_affinity(bindings, source, position)?,
        );
    }
    Ok(())
}

/// Enforce every CHECK program selected and frozen for this exact DML source.
/// SQLite CHECK succeeds for true or NULL and fails only for false.
pub(crate) fn emit_check_constraints(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    conflict: ResolveType,
    skip_row: crate::vdbe::BranchOffset,
) -> RowResult<()> {
    let checks = bindings
        .document()
        .source(source)
        .and_then(|source| source.check_constraints.as_ref())
        .ok_or(PhysicalRowError::Invalid(
            "DML target does not carry CHECK metadata",
        ))?;
    for check in checks {
        let result = ExpressionEmitter::new(program, bindings).emit_new(&check.expression)?;
        if result.width != 1 {
            return Err(PhysicalRowError::Invalid("CHECK result is not scalar"));
        }
        let passed = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: result.first.0,
            target_pc: passed,
        });
        program.emit_insn(Insn::If {
            reg: result.first.0,
            target_pc: passed,
            jump_if_null: false,
        });
        if conflict == ResolveType::Ignore {
            program.emit_insn(Insn::Goto {
                target_pc: skip_row,
            });
        } else {
            constraint_halt(
                program,
                SQLITE_CONSTRAINT_CHECK,
                check.description.clone(),
                conflict,
            );
        }
        program.preassign_label_to_next_insn(passed);
    }
    Ok(())
}

/// Enforce the row constraints that depend only on the completed logical NEW
/// image. Stored-value encoders run later while the record is assembled.
pub(crate) fn emit_new_row_constraints(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &crate::sync::Arc<BTreeTable>,
    logical: RegisterRange,
    statement_conflict: Option<ResolveType>,
    skip_row: crate::vdbe::BranchOffset,
) -> RowResult<()> {
    for (position, column) in table.columns().iter().enumerate() {
        if column.notnull() && !column.is_rowid_alias() {
            let conflict = statement_conflict
                .or(column.notnull_conflict_clause)
                .unwrap_or(ResolveType::Abort);
            // REPLACE has already tried the frozen default. A NULL default
            // falls back to ABORT, matching SQLite's constraint rule.
            let conflict = if conflict == ResolveType::Replace {
                ResolveType::Abort
            } else {
                conflict
            };
            if conflict == ResolveType::Ignore {
                program.emit_insn(Insn::IsNull {
                    reg: logical.first.0 + position,
                    target_pc: skip_row,
                });
            } else {
                let present = program.allocate_label();
                program.emit_insn(Insn::NotNull {
                    reg: logical.first.0 + position,
                    target_pc: present,
                });
                constraint_halt(
                    program,
                    SQLITE_CONSTRAINT_NOTNULL,
                    format!("{}.{}", table.name, column.name.as_deref().unwrap_or("")),
                    conflict,
                );
                program.preassign_label_to_next_insn(present);
            }
        }
    }
    if table.is_strict {
        program.emit_insn(Insn::TypeCheck {
            start_reg: logical.first.0,
            count: logical.width,
            check_generated: true,
            table_reference: table.clone(),
        });
    }
    emit_check_constraints(
        program,
        bindings,
        source,
        statement_conflict.unwrap_or(ResolveType::Abort),
        skip_row,
    )
}

/// Apply SQLite's REPLACE rule for NOT NULL columns before the ordinary row
/// constraints run. The default expressions were bound for this exact target
/// source during semantic analysis.
pub(crate) fn emit_replace_not_null_defaults(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    defaults: &[hir::ResolvedDefault],
    table: &BTreeTable,
    logical: RegisterRange,
    statement_conflict: Option<ResolveType>,
) -> RowResult<()> {
    for (position, column) in table.columns().iter().enumerate() {
        let conflict = statement_conflict
            .or(column.notnull_conflict_clause)
            .unwrap_or(ResolveType::Abort);
        if !column.notnull() || column.is_rowid_alias() || conflict != ResolveType::Replace {
            continue;
        }
        let present = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: logical.first.0 + position,
            target_pc: present,
        });
        if let Some(default) = defaults.iter().find(|default| default.column == position) {
            ExpressionEmitter::new(program, bindings).emit_into(
                &default.value,
                RegisterRange::new(logical.first.0 + position, 1),
            )?;
            program.emit_insn(Insn::NotNull {
                reg: logical.first.0 + position,
                target_pc: present,
            });
        }
        constraint_halt(
            program,
            SQLITE_CONSTRAINT_NOTNULL,
            format!("{}.{}", table.name, column.name.as_deref().unwrap_or("")),
            ResolveType::Abort,
        );
        program.preassign_label_to_next_insn(present);
    }
    Ok(())
}

/// Encode one complete logical row and build its on-disk table record.
pub(crate) fn emit_stored_record(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    source: hir::SourceId,
    table: &BTreeTable,
    logical: RegisterRange,
    record: usize,
) -> RowResult<()> {
    let stored_count = table
        .columns()
        .iter()
        .filter(|column| !column.is_virtual_generated())
        .count();
    let stored = program.alloc_registers(stored_count);
    let mut physical = 0;
    for (position, column) in table.columns().iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        if column.is_rowid_alias() {
            program.emit_insn(Insn::SoftNull {
                reg: stored + physical,
            });
        } else {
            ExpressionEmitter::new(program, bindings).emit_column_storage_value(
                source,
                position,
                logical.first.0 + position,
            )?;
            program.emit_insn(Insn::Copy {
                src_reg: logical.first.0 + position,
                dst_reg: stored + physical,
                extra_amount: 0,
            });
        }
        physical += 1;
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(stored),
        count: to_u32(stored_count),
        dest_reg: to_u32(record),
        index_name: Some(table.name.clone()),
        affinity_str: None,
    });
    Ok(())
}

fn source_affinity(
    bindings: &RuntimeBindings<'_>,
    source: hir::SourceId,
    column: usize,
) -> RowResult<crate::vdbe::affinity::Affinity> {
    bindings
        .document()
        .source(source)
        .and_then(|source| source.columns.get(column))
        .map(|column| column.affinity)
        .ok_or(PhysicalRowError::Invalid(
            "target column is outside the row",
        ))
}
