use crate::{
    schema::Schema,
    types::IndexInfo,
    vdbe::{
        builder::ProgramBuilder,
        insn::{Insn, IntegrityCheckIndex, IntegrityCheckTable},
    },
};
use std::sync::Arc;

use super::emitter::Resolver;
use super::expr::translate_expr;

/// Maximum number of errors to report with integrity check. If we exceed this number we will short
/// circuit the procedure and return early to not waste time. SQLite uses 100 as the default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

/// Translate PRAGMA integrity_check.
/// This performs a full integrity check including index consistency validation.
pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, resolver, max_errors, false)
}

/// Translate PRAGMA quick_check.
/// This performs a quick integrity check that skips expensive index consistency validation.
pub fn translate_quick_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, resolver, max_errors, true)
}

// FIXME: This entire integrity check implementation is architecturally wrong.
//
// SQLite generates proper VDBE bytecode for integrity_check: it opens cursors,
// iterates with Rewind/Next, reads columns with OP_Column (which has a P4 parameter
// for default values), and uses Found to check index entries. Each of these is a
// separate instruction that the VM executes.
//
// Our implementation instead uses a single monolithic IntegrityCk instruction that
// does everything internally in execute.rs. This means we can't leverage the existing
// Column instruction's default value handling, and instead have to hack around it by
// pre-evaluating default expressions to registers and passing them to the instruction.
// Also who knows what other hacks we might need to add in the future.
//
// The proper fix would be to refactor integrity_check to emit bytecode like SQLite does:
// - OpenRead cursors for tables and indexes
// - Rewind/Next loops to iterate
// - Column instructions with P4 defaults for reading values
// - Found/NotFound for index lookups
//
// This would eliminate the need for the IntegrityCk mega-instruction and make the
// implementation consistent with the rest of the VDBE.

/// Internal implementation for both integrity_check and quick_check.
fn translate_integrity_check_impl(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    max_errors: usize,
    quick: bool,
) -> crate::Result<()> {
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    let mut tables = Vec::with_capacity(schema.tables.len());

    // Collect root pages and table/index metadata for integrity check
    for table in schema.tables.values() {
        if let crate::schema::Table::BTree(btree_table) = table.as_ref() {
            if btree_table.root_page < 0 {
                continue;
            }
            root_pages.push(btree_table.root_page);

            let mut table_indexes = Vec::new();
            if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
                for index in indexes.iter() {
                    if index.root_page > 0 {
                        root_pages.push(index.root_page);

                        let column_positions: Vec<usize> =
                            index.columns.iter().map(|c| c.pos_in_table).collect();

                        // Allocate contiguous registers for default values of indexed columns.
                        // For columns added via ALTER TABLE ADD COLUMN with DEFAULT, old rows
                        // don't physically have these columns, so we need the defaults.
                        //
                        // FIXME: This is a hack. SQLite passes defaults via P4 to OP_Column.
                        // We pre-evaluate them here because our monolithic IntegrityCk doesn't
                        // use OP_Column internally. See the FIXME comment above.
                        let default_values_start_reg =
                            program.alloc_registers(column_positions.len());
                        for (i, &pos) in column_positions.iter().enumerate() {
                            let target_reg = default_values_start_reg + i;
                            let col = &btree_table.columns[pos];
                            if let Some(default_expr) = col.default.as_ref() {
                                // Evaluate the default expression to the register.
                                // Pass None for table references since defaults must be
                                // constant expressions (no column references allowed).
                                translate_expr(program, None, default_expr, target_reg, resolver)?;
                            } else {
                                // No default specified, use NULL
                                program.emit_insn(Insn::Null {
                                    dest: target_reg,
                                    dest_end: None,
                                });
                            }
                        }

                        table_indexes.push(IntegrityCheckIndex {
                            name: index.name.clone(),
                            root_page: index.root_page,
                            unique: index.unique,
                            column_positions,
                            default_values_start_reg,
                            index_info: Arc::new(IndexInfo::new_from_index(index)),
                        });
                    }
                }
            }

            // Get INTEGER PRIMARY KEY column position if it exists (rowid alias).
            // This column's value is stored as NULL in the record because the actual value is the rowid.
            let rowid_alias_column_pos = btree_table.get_rowid_alias_column().map(|(pos, _)| pos);

            // Collect NOT NULL columns for constraint validation.
            // We must exclude the rowid alias column because it's stored as NULL in the record
            // (the actual value is the rowid, not in the record payload).
            let not_null_columns: Vec<(usize, String)> = btree_table
                .columns
                .iter()
                .enumerate()
                .filter(|(idx, col)| col.notnull() && Some(*idx) != rowid_alias_column_pos)
                .map(|(idx, col)| {
                    let name = col.name.clone().unwrap_or_else(|| format!("column{idx}"));
                    (idx, name)
                })
                .collect();

            tables.push(IntegrityCheckTable {
                name: btree_table.name.clone(),
                root_page: btree_table.root_page,
                indexes: table_indexes,
                not_null_columns,
                rowid_alias_column_pos,
            });
        };
    }

    // Include root pages from tables/indexes that have been dropped but not yet checkpointed.
    // In MVCC mode, dropped tables' btree pages are not freed until checkpoint, so we need
    // to check them to avoid "page never used" false positives.
    for &dropped_root in &schema.dropped_root_pages {
        root_pages.push(dropped_root);
    }

    let message_register = program.alloc_register();
    program.emit_insn(Insn::IntegrityCk {
        max_errors,
        roots: root_pages,
        message_register,
        quick,
        tables,
    });
    program.emit_insn(Insn::ResultRow {
        start_reg: message_register,
        count: 1,
    });
    let column_name = if quick {
        "quick_check"
    } else {
        "integrity_check"
    };
    program.add_pragma_result_column(column_name.into());
    Ok(())
}
