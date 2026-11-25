use std::num::NonZeroUsize;

use crate::schema::{BTreeTable, ColumnLayout};
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
/// affinity, or the type check of a STRICT table.
pub(crate) fn emit_input_conversion(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
    start_reg: usize,
    layout: &ColumnLayout,
    values: StatementValues,
) -> Result<()> {
    if table.is_strict {
        let only_columns = match values {
            StatementValues::Stored => return Ok(()),
            StatementValues::All => None,
            StatementValues::Only(columns) => Some(columns),
        };
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
