use crate::{
    schema::{BTreeTable, Index, Schema, Table, EXPR_INDEX_SENTINEL},
    translate::semantic_lowering::{PlannedSelect, ScanValue},
    vdbe::{
        builder::ProgramBuilder,
        insn::{CmpInsFlags, Insn, IntegrityCkData},
    },
    HashSet,
};
use turso_parser::ast;

/// Maximum number of errors to report with integrity check. If we exceed this number we will
/// short circuit the procedure and return early to not waste time. SQLite uses 100 as default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

/// An index the integrity check verifies, with its open cursor.
struct IntegrityIndex {
    index: crate::sync::Arc<Index>,
    cursor_id: usize,
    expected_count_reg: usize,
    unique_nullable: Vec<bool>,
}

use crate::translate::HirContext;

/// Translate PRAGMA integrity_check.
pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    database_id: usize,
    max_errors: usize,
    connection: &std::sync::Arc<crate::Connection>,
    hir: HirContext<'_>,
) -> crate::Result<()> {
    translate_integrity_check_impl(
        schema,
        program,
        database_id,
        max_errors,
        false,
        connection,
        hir,
    )
}

/// Translate PRAGMA quick_check.
pub fn translate_quick_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    database_id: usize,
    max_errors: usize,
    connection: &std::sync::Arc<crate::Connection>,
    hir: HirContext<'_>,
) -> crate::Result<()> {
    translate_integrity_check_impl(
        schema,
        program,
        database_id,
        max_errors,
        true,
        connection,
        hir,
    )
}

fn emit_integrity_result_row(
    program: &mut ProgramBuilder,
    remaining_errors_reg: usize,
    message_reg: usize,
    had_error_reg: usize,
) {
    program.emit_int(1, had_error_reg);
    program.emit_result_row(message_reg, 1);

    let continue_label = program.allocate_label();
    program.emit_insn(Insn::IfPos {
        reg: remaining_errors_reg,
        target_pc: continue_label,
        decrement_by: 1,
    });
    program.emit_insn(Insn::Halt {
        err_code: 0,
        on_error: None,
        description_reg: None,
        description: String::new(),
    });
    program.preassign_label_to_next_insn(continue_label);
}

fn emit_row_missing_from_index_error(
    program: &mut ProgramBuilder,
    row_number_reg: usize,
    scratch_reg: usize,
    message_reg: usize,
    index_name: &str,
    remaining_errors_reg: usize,
    had_error_reg: usize,
) {
    program.emit_string8("row ".to_string(), message_reg);
    program.emit_insn(Insn::Concat {
        lhs: message_reg,
        rhs: row_number_reg,
        dest: message_reg,
    });
    program.emit_string8(" missing from index ".to_string(), scratch_reg);
    program.emit_insn(Insn::Concat {
        lhs: message_reg,
        rhs: scratch_reg,
        dest: message_reg,
    });
    program.emit_string8(index_name.to_string(), scratch_reg);
    program.emit_insn(Insn::Concat {
        lhs: message_reg,
        rhs: scratch_reg,
        dest: message_reg,
    });
    emit_integrity_result_row(program, remaining_errors_reg, message_reg, had_error_reg);
}

#[allow(clippy::too_many_arguments)]
fn translate_integrity_check_impl(
    schema: &Schema,
    program: &mut ProgramBuilder,
    database_id: usize,
    max_errors: usize,
    quick: bool,
    connection: &std::sync::Arc<crate::Connection>,
    hir: HirContext<'_>,
) -> crate::Result<()> {
    // 1) Run low-level btree/freelist/overflow verification first. This mirrors
    // SQLite's OP_IntegrityCk front-pass and can already emit corruption errors
    // before any row-by-row semantic checks run.
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    let mut live_root_pages = HashSet::default();

    // integrity_check verifies the physical file, so a placeholder (negative) root for an
    // object a passive checkpoint has since materialized must be resolved to its real page.
    let mv_store_guard = connection.db.get_mv_store();
    let resolve_root = |root_page: i64| -> i64 {
        match mv_store_guard.as_ref() {
            Some(mv) => mv.resolve_root_page(root_page),
            None => root_page,
        }
    };

    for table in schema.tables.values() {
        if let Table::BTree(btree_table) = table.as_ref() {
            let table_root = resolve_root(btree_table.root_page);
            if table_root < 0 {
                continue;
            }
            root_pages.push(table_root);
            live_root_pages.insert(table_root);
            if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
                for index in indexes {
                    let index_root = resolve_root(index.root_page);
                    if index_root > 0 {
                        root_pages.push(index_root);
                        live_root_pages.insert(index_root);
                    }
                }
            }
        }
    }

    let passive =
        mv_store_guard.is_some() && connection.experimental_mvcc_passive_checkpoint_enabled();
    let mut dropped_roots = Vec::new();
    for &dropped_root in &schema.dropped_root_pages {
        if live_root_pages.contains(&dropped_root) {
            continue;
        }
        if passive {
            dropped_roots.push(dropped_root);
        } else {
            root_pages.push(dropped_root);
        }
    }

    let remaining_errors_reg = program.alloc_register();
    program.emit_int((max_errors.saturating_sub(1)) as i64, remaining_errors_reg);

    let had_error_reg = program.alloc_register();
    program.emit_int(0, had_error_reg);

    let message_reg = program.alloc_register();
    let scratch_reg = program.alloc_register();

    program.emit_insn(Insn::IntegrityCk {
        data: Box::new(IntegrityCkData {
            db: database_id,
            max_errors,
            roots: root_pages,
            dropped_roots,
            message_register: message_reg,
        }),
    });

    let no_structural_error_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg: message_reg,
        target_pc: no_structural_error_label,
    });

    program.emit_string8("*** in database main ***\n".to_string(), scratch_reg);
    program.emit_insn(Insn::Concat {
        lhs: scratch_reg,
        rhs: message_reg,
        dest: message_reg,
    });
    emit_integrity_result_row(program, remaining_errors_reg, message_reg, had_error_reg);
    program.preassign_label_to_next_insn(no_structural_error_label);

    let reporter = Reporter {
        remaining_errors_reg,
        had_error_reg,
        message_reg,
        scratch_reg,
    };

    // 2) For each ordinary btree table, scan every row and validate:
    //    - NOT NULL constraints
    //    - CHECK constraints
    //    - index membership/uniqueness (integrity_check only)
    //    - index cardinality cross-checks
    for table in schema.tables.values() {
        let Table::BTree(btree_table) = table.as_ref() else {
            continue;
        };

        if btree_table.root_page <= 0 {
            continue;
        }

        let table_indexes: Vec<_> = schema
            .indexes
            .get(btree_table.name.as_str())
            .into_iter()
            .flatten()
            .filter(|index| index.root_page > 0)
            .cloned()
            .collect();
        let rows = hir_table_rows(connection, hir, database_id, btree_table, &table_indexes)?;

        let mut indexes = Vec::with_capacity(table_indexes.len());
        for index in table_indexes {
            let cursor_id = program.alloc_cursor_index(None, &index)?;
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: index.root_page,
                db: database_id,
            });

            let expected_count_reg = program.alloc_register();
            program.emit_int(0, expected_count_reg);

            let unique_nullable = index
                .columns
                .iter()
                .map(|col| col.expr.is_some() || !btree_table.columns()[col.pos_in_table].notnull())
                .collect();
            indexes.push(IntegrityIndex {
                index,
                cursor_id,
                expected_count_reg,
                unique_nullable,
            });
        }

        let row_number_reg = program.alloc_register();
        program.emit_int(0, row_number_reg);

        match rows {
            TableRows::NothingToCheck => {}
            TableRows::Hir(select, layout) => {
                select.emit_each_row(program, |program, values| {
                    program.emit_insn(Insn::AddImm {
                        register: row_number_reg,
                        value: 1,
                    });
                    for (i, col_name) in layout.not_null.iter().enumerate() {
                        reporter.not_null(program, values + i, &btree_table.name, col_name);
                    }
                    let checks = values + layout.not_null.len();
                    reporter.checks(
                        program,
                        checks,
                        btree_table.check_constraints.len(),
                        &btree_table.name,
                    );
                    for (index, offsets) in indexes.iter().zip(&layout.indexes) {
                        let skip_current_index = program.allocate_label();
                        if let Some(predicate) = offsets.predicate {
                            // Rows whose predicate is FALSE or NULL are not
                            // part of a partial index.
                            program.emit_insn(Insn::IfNot {
                                reg: values + predicate,
                                target_pc: skip_current_index,
                                jump_if_null: true,
                            });
                        }
                        program.emit_insn(Insn::AddImm {
                            register: index.expected_count_reg,
                            value: 1,
                        });
                        if !quick {
                            reporter.index_entry(
                                program,
                                index,
                                values + offsets.keys,
                                row_number_reg,
                            );
                        }
                        program.preassign_label_to_next_insn(skip_current_index);
                    }
                    Ok(())
                })?;
            }
        }

        for index in &indexes {
            // An index method's backing B-tree stores its data in the index
            // alone; the owning table has zero rows by construction, so the
            // entry-count comparison below would report every healthy FTS
            // database as corrupt. Its pages are still visited above.
            if index.index.is_backing_btree_index() {
                program.emit_insn(Insn::Close {
                    cursor_id: index.cursor_id,
                });
                continue;
            }
            if index.index.where_clause.is_none() {
                let actual_count_reg = program.alloc_register();
                program.emit_insn(Insn::Count {
                    cursor_id: index.cursor_id,
                    target_reg: actual_count_reg,
                    exact: true,
                });

                let counts_match = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: actual_count_reg,
                    rhs: index.expected_count_reg,
                    target_pc: counts_match,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.emit_string8(
                    format!("wrong # of entries in index {}", index.index.name),
                    message_reg,
                );
                reporter.report(program);
                program.preassign_label_to_next_insn(counts_match);
            }

            program.emit_insn(Insn::Close {
                cursor_id: index.cursor_id,
            });
        }
    }

    let has_errors_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: had_error_reg,
        target_pc: has_errors_label,
        jump_if_null: false,
    });
    program.emit_string8("ok".to_string(), message_reg);
    program.emit_result_row(message_reg, 1);
    program.preassign_label_to_next_insn(has_errors_label);

    let column_name = if quick {
        "quick_check"
    } else {
        "integrity_check"
    };
    program.add_pragma_result_column(column_name.into());

    Ok(())
}

/// The registers an integrity check reports errors through.
struct Reporter {
    remaining_errors_reg: usize,
    had_error_reg: usize,
    message_reg: usize,
    scratch_reg: usize,
}

impl Reporter {
    /// Return the message in `message_reg` as a result row.
    fn report(&self, program: &mut ProgramBuilder) {
        emit_integrity_result_row(
            program,
            self.remaining_errors_reg,
            self.message_reg,
            self.had_error_reg,
        );
    }

    /// Report a NULL in `value_reg`, the value of a NOT NULL column.
    fn not_null(&self, program: &mut ProgramBuilder, value_reg: usize, table: &str, column: &str) {
        let not_null_ok = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: value_reg,
            target_pc: not_null_ok,
        });
        program.emit_string8(format!("NULL value in {table}.{column}"), self.message_reg);
        self.report(program);
        program.preassign_label_to_next_insn(not_null_ok);
    }

    /// Report the row's CHECK constraints, whose values are in `count`
    /// registers from `first_reg`. A constraint passes when its value is
    /// TRUE or NULL; only FALSE is a violation. Like SQLite, a row is
    /// reported once however many of its constraints fail.
    fn checks(&self, program: &mut ProgramBuilder, first_reg: usize, count: usize, table: &str) {
        if count == 0 {
            return;
        }
        let failed = program.allocate_label();
        let all_ok = program.allocate_label();
        for i in 0..count {
            let check_ok = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: first_reg + i,
                target_pc: check_ok,
                jump_if_null: true,
            });
            program.emit_insn(Insn::Goto { target_pc: failed });
            program.preassign_label_to_next_insn(check_ok);
        }
        program.emit_insn(Insn::Goto { target_pc: all_ok });
        program.preassign_label_to_next_insn(failed);
        program.emit_string8(
            format!("CHECK constraint failed in {table}"),
            self.message_reg,
        );
        self.report(program);
        program.preassign_label_to_next_insn(all_ok);
    }

    /// Check that the index has an entry for the row whose key columns and
    /// rowid start at `key_start_reg`, and, for a UNIQUE index, that the
    /// next entry has another key.
    fn index_entry(
        &self,
        program: &mut ProgramBuilder,
        index: &IntegrityIndex,
        key_start_reg: usize,
        row_number_reg: usize,
    ) {
        let key_count = index.index.columns.len();
        let found_label = program.allocate_label();
        // Verify the table row has a matching index entry (key columns + rowid).
        program.emit_insn(Insn::Found {
            cursor_id: index.cursor_id,
            target_pc: found_label,
            record_reg: key_start_reg,
            num_regs: key_count + 1,
        });
        emit_row_missing_from_index_error(
            program,
            row_number_reg,
            self.scratch_reg,
            self.message_reg,
            &index.index.name,
            self.remaining_errors_reg,
            self.had_error_reg,
        );
        program.preassign_label_to_next_insn(found_label);

        if index.index.unique {
            // This intentionally runs even after a "missing from index"
            // report above. SQLite does the same: a single corrupt row
            // can violate multiple invariants and each should be
            // independently reportable.
            //
            // Uniqueness rule matches SQLite:
            //   unique key is valid if any key column is NULL, OR
            //   the next index entry is strictly greater on key columns.
            let unique_ok = program.allocate_label();
            for (i, is_nullable) in index.unique_nullable.iter().enumerate() {
                if *is_nullable {
                    program.emit_insn(Insn::IsNull {
                        reg: key_start_reg + i,
                        target_pc: unique_ok,
                    });
                }
            }

            let next_exists = program.allocate_label();
            program.emit_insn(Insn::Next {
                cursor_id: index.cursor_id,
                pc_if_next: next_exists,
                fullscan: false,
            });
            program.emit_insn(Insn::Goto {
                target_pc: unique_ok,
            });
            program.preassign_label_to_next_insn(next_exists);

            program.emit_insn(Insn::IdxGT {
                cursor_id: index.cursor_id,
                start_reg: key_start_reg,
                num_regs: key_count,
                target_pc: unique_ok,
            });
            program.emit_string8(
                format!("non-unique entry in index {}", index.index.name),
                self.message_reg,
            );
            self.report(program);
            program.preassign_label_to_next_insn(unique_ok);
        }
    }
}

/// How the integrity check reads a table's rows.
enum TableRows {
    /// Run a SELECT planned through HIR.
    Hir(PlannedSelect, HirRowLayout),
    /// The table has no NOT NULL column, CHECK constraint or index, so no
    /// row needs reading.
    NothingToCheck,
}

/// Where each value of a row sits among the columns of the HIR SELECT that
/// reads the table for an integrity check.
struct HirRowLayout {
    /// The name of each NOT NULL column, whose values come first.
    not_null: Vec<String>,
    /// For each index, in order.
    indexes: Vec<HirIndexOffsets>,
}

struct HirIndexOffsets {
    /// The partial index's condition.
    predicate: Option<usize>,
    /// The key columns, then the rowid.
    keys: usize,
}

/// Plan the SELECT that reads each row of `table` for an integrity check:
/// the NOT NULL columns, the CHECK constraints, then for each index its
/// partial condition, its key columns and the rowid. Values are read the
/// way DML writes them: a plain column as stored, an expression or a
/// generated column computed from decoded values, and a CHECK constraint
/// over stored values. A partial index's computed keys are evaluated only
/// when its condition is true, so an expression that fails on rows outside
/// the index is not run on them.
fn hir_table_rows(
    connection: &std::sync::Arc<crate::Connection>,
    hir: HirContext<'_>,
    database_id: usize,
    table: &BTreeTable,
    indexes: &[crate::sync::Arc<Index>],
) -> crate::Result<TableRows> {
    let (schema, settings) = hir;
    let database = connection
        .get_database_name_by_index(database_id)
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!("no database with id {database_id}"))
        })?;
    let column = |position: usize| -> crate::Result<ast::Expr> {
        let name = table.columns()[position].name.as_deref().ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "column {position} of {} has no name",
                table.name
            ))
        })?;
        Ok(ast::Expr::Qualified(
            ast::Name::exact(table.name.clone()),
            ast::Name::exact(name.to_string()),
        ))
    };

    let mut values = Vec::new();
    let mut not_null = Vec::new();
    for (i, col) in table.columns().iter().enumerate() {
        if !col.notnull() || col.is_rowid_alias() {
            continue;
        }
        values.push(if col.is_virtual_generated() {
            ScanValue::Expr(column(i)?)
        } else {
            ScanValue::Stored(column(i)?)
        });
        not_null.push(col.name.clone().unwrap_or_else(|| format!("column{i}")));
    }
    for check in &table.check_constraints {
        values.push(ScanValue::Stored(check.expr.clone()));
    }
    let mut offsets = Vec::with_capacity(indexes.len());
    for index in indexes {
        let predicate = index.where_clause.as_deref().map(|predicate| {
            values.push(ScanValue::Expr(predicate.clone()));
            values.len() - 1
        });
        let guarded = |key: ast::Expr| match index.where_clause.as_deref() {
            Some(predicate) => ast::Expr::Case {
                base: None,
                when_then_pairs: vec![(Box::new(predicate.clone()), Box::new(key))],
                else_expr: None,
            },
            None => key,
        };
        let keys = values.len();
        for col in &index.columns {
            // A virtual generated column's index entry holds its bound
            // expression; name the column instead, and the SELECT computes it.
            let value = if col.pos_in_table == EXPR_INDEX_SENTINEL {
                let Some(expr) = &col.expr else {
                    return Err(crate::LimboError::InternalError(format!(
                        "expression index column of {} has no expression",
                        index.name
                    )));
                };
                ScanValue::Expr(guarded(*expr.clone()))
            } else if table.columns()[col.pos_in_table].is_virtual_generated() {
                ScanValue::Expr(guarded(column(col.pos_in_table)?))
            } else {
                ScanValue::Stored(column(col.pos_in_table)?)
            };
            values.push(value);
        }
        values.push(ScanValue::RowId);
        offsets.push(HirIndexOffsets { predicate, keys });
    }

    if values.is_empty() {
        return Ok(TableRows::NothingToCheck);
    }
    let select = PlannedSelect::scan(
        values,
        database,
        table.name.clone(),
        None,
        schema,
        settings,
        connection,
    )?;
    Ok(TableRows::Hir(
        select,
        HirRowLayout {
            not_null,
            indexes: offsets,
        },
    ))
}
