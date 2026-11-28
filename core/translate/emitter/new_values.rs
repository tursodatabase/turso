use std::num::NonZeroUsize;

use crate::ast;
use crate::dialect::{CastContext, CastDecision, CastSource};
use crate::schema::{BTreeTable, Column, ColumnLayout};
use crate::translate::expr::{translate_expr_no_constant_opt, NoConstantOptReason};
use crate::translate::plan::ColumnMask;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::insn::Insn;
use crate::{Arc, Result};

use super::{ProgramBuilder, Resolver};

/// Which columns of a row hold a value that the statement gave. The other
/// columns hold stored values.
pub(crate) enum StatementValues<'a> {
    /// Every column, as in an INSERT.
    All,
    /// The columns of the mask, as the SET columns of an UPDATE.
    Only(&'a ColumnMask),
    /// No column: the statement copies stored rows.
    Stored,
}

impl StatementValues<'_> {
    /// The values of a row that an INSERT writes.
    pub(crate) fn of_insert(program: &ProgramBuilder) -> Self {
        if program.flags.writes_stored_values() {
            Self::Stored
        } else {
            Self::All
        }
    }
}

// FIXME: Each statement still emits its own NOT NULL checks, ENCODE and last type check. INSERT, UPDATE and ON CONFLICT DO UPDATE order them differently, also on a table with no custom type, so one order for all would change which error SQLite reports first.
/// Converts the values that a statement gave for a row of `table`, in the
/// registers that start at `start_reg`, to the input types of their columns:
/// affinity, or for a STRICT table the dialect's cast on assignment and then
/// the type check, which needs the converted values. `sources` has the source
/// of the value of each column of the table, and a column past its end has a
/// value with no known source type.
pub(crate) fn emit_input_conversion(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
    start_reg: usize,
    layout: &ColumnLayout,
    values: StatementValues,
    sources: &[CastSource],
) -> Result<()> {
    if table.is_strict {
        let only_columns = match values {
            StatementValues::Stored => return Ok(()),
            StatementValues::All => None,
            StatementValues::Only(columns) => Some(columns),
        };
        for (i, column) in table.columns().iter().enumerate() {
            let statement_gave_value = match only_columns {
                Some(columns) => columns.get(i),
                None => !column.is_generated(),
            };
            if statement_gave_value {
                let source = || sources.get(i).cloned().unwrap_or(CastSource::Unnamed);
                let reg = layout.to_register(start_reg, i);
                emit_assignment_cast(program, resolver, column, reg, &source)?;
            }
        }
        program.emit_insn(Insn::TypeCheck {
            start_reg,
            count: layout.num_non_virtual_cols(),
            check_generated: false,
            table_reference: BTreeTable::input_type_check_table_ref(
                table,
                resolver.schema(),
                only_columns,
            )?,
        });
        return Ok(());
    }
    if let StatementValues::Only(_) = values {
        // UPDATE gives the new values their affinity itself, earlier.
        return Ok(());
    }
    let affinities = table
        .columns()
        .iter()
        .filter(|column| !column.is_virtual_generated())
        .map(|column| column.affinity());
    if affinities
        .clone()
        .all(|affinity| affinity == Affinity::Blob)
    {
        return Ok(());
    }
    if let Ok(count) = NonZeroUsize::try_from(layout.num_non_virtual_cols()) {
        program.emit_insn(Insn::Affinity {
            start_reg,
            count,
            affinities: affinities.map(|affinity| affinity.aff_mask()).collect(),
        });
    }
    Ok(())
}

/// Converts the value in `reg` to the type of `column` as the dialect does a
/// cast on assignment. `source` gives the source of the value.
pub(crate) fn emit_assignment_cast(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    column: &Column,
    reg: usize,
    source: &dyn Fn() -> CastSource,
) -> Result<()> {
    let Some(converted) = assignment_cast(&ast::Expr::register(reg), source, column, resolver)?
    else {
        return Ok(());
    };
    translate_expr_no_constant_opt(
        program,
        None,
        &converted,
        reg,
        resolver,
        NoConstantOptReason::RegisterReuse,
    )?;
    Ok(())
}

/// `value` converted to the type of `column` as the dialect does a cast on
/// assignment, or None when the dialect leaves the value to the rules of the
/// engine: the type check and the ENCODE of the column.
pub(crate) fn assignment_cast(
    value: &ast::Expr,
    source: &dyn Fn() -> CastSource,
    column: &Column,
    resolver: &Resolver,
) -> Result<Option<ast::Expr>> {
    if column.is_array() || column.ty_str.is_empty() {
        return Ok(None);
    }
    let target =
        crate::translate::expr::expr_type::type_ref_of_name(&column.ty_str, 0, resolver.schema());
    let applied =
        resolver
            .catalog
            .dialect
            .apply_cast(value, source, &target, CastContext::Assignment)?;
    Ok(match applied {
        CastDecision::Applied(converted) => Some(converted),
        CastDecision::EngineRules => None,
    })
}
