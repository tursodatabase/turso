use crate::{
    schema::Schema,
    types::IndexInfo,
    vdbe::{
        builder::ProgramBuilder,
        insn::{Insn, IntegrityCheckIndex, IntegrityCheckTable},
    },
};
use std::sync::Arc;

/// Maximum number of errors to report with integrity check. If we exceed this number we will short
/// circuit the procedure and return early to not waste time. SQLite uses 100 as the default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

/// Translate PRAGMA integrity_check.
/// This performs a full integrity check including index consistency validation.
pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, max_errors, false)
}

/// Translate PRAGMA quick_check.
/// This performs a quick integrity check that skips expensive index consistency validation.
pub fn translate_quick_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, max_errors, true)
}

/// Internal implementation for both integrity_check and quick_check.
fn translate_integrity_check_impl(
    schema: &Schema,
    program: &mut ProgramBuilder,
    max_errors: usize,
    quick: bool,
) -> crate::Result<()> {
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    let mut tables = Vec::with_capacity(schema.tables.len());

    // Collect root pages and table/index metadata for integrity check
    for table in schema.tables.values() {
        if let crate::schema::Table::BTree(btree_table) = table.as_ref() {
            root_pages.push(btree_table.root_page);

            let mut table_indexes = Vec::new();
            if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
                for index in indexes.iter() {
                    if index.root_page > 0 {
                        root_pages.push(index.root_page);
                        table_indexes.push(IntegrityCheckIndex {
                            name: index.name.clone(),
                            root_page: index.root_page,
                            unique: index.unique,
                            column_positions: index
                                .columns
                                .iter()
                                .map(|c| c.pos_in_table)
                                .collect(),
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
