use std::num::NonZeroUsize;

use crate::{
    schema::{Index, Schema, Table},
    translate::{
        collate::CollationSeq,
        emitter::Resolver,
        expr::{
            bind_and_rewrite_expr, translate_condition_expr, translate_expr_no_constant_opt,
            BindingBehavior, ConditionMetadata, NoConstantOptReason,
        },
        fkeys::build_index_affinity_string,
        plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences},
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
    },
};
use turso_parser::ast;

/// Maximum number of errors to report with integrity check. If we exceed this number we will
/// short circuit the procedure and return early to not waste time. SQLite uses 100 as default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

enum BoundIndexColumn {
    Column(usize),
    Expr(Box<ast::Expr>),
}

struct BoundIntegrityIndex {
    index: crate::sync::Arc<Index>,
    cursor_id: usize,
    expected_count_reg: usize,
    where_expr: Option<ast::Expr>,
    columns: Vec<BoundIndexColumn>,
    unique_nullable: Vec<bool>,
    affinity: String,
}

/// Translate PRAGMA integrity_check.
pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, resolver, database_id, max_errors, false)
}

/// Translate PRAGMA quick_check.
pub fn translate_quick_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
) -> crate::Result<()> {
    translate_integrity_check_impl(schema, program, resolver, database_id, max_errors, true)
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

fn emit_rowid_not_at_eor_error(
    program: &mut ProgramBuilder,
    row_number_reg: usize,
    scratch_reg: usize,
    message_reg: usize,
    index_name: &str,
    remaining_errors_reg: usize,
    had_error_reg: usize,
) {
    program.emit_string8(
        "rowid not at end-of-record for row ".to_string(),
        message_reg,
    );
    program.emit_insn(Insn::Concat {
        lhs: message_reg,
        rhs: row_number_reg,
        dest: message_reg,
    });
    program.emit_string8(" of index ".to_string(), scratch_reg);
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

fn emit_values_differ_error(
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
    program.emit_string8(" values differ from index ".to_string(), scratch_reg);
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

fn bind_expr_for_table(
    expr: &ast::Expr,
    table_references: &mut TableReferences,
    resolver: &Resolver,
) -> crate::Result<ast::Expr> {
    let mut out = expr.clone();
    bind_and_rewrite_expr(
        &mut out,
        Some(table_references),
        None,
        resolver,
        BindingBehavior::ResultColumnsNotAllowed,
    )?;
    Ok(out)
}

fn translate_integrity_check_impl(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    quick: bool,
) -> crate::Result<()> {
    // 1) Run low-level btree/freelist/overflow verification first. This mirrors
    // SQLite's OP_IntegrityCk front-pass and can already emit corruption errors
    // before any row-by-row semantic checks run.
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());

    for table in schema.tables.values() {
        if let Table::BTree(btree_table) = table.as_ref() {
            if btree_table.root_page < 0 {
                continue;
            }
            root_pages.push(btree_table.root_page);
            if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
                for index in indexes {
                    if index.root_page > 0 {
                        root_pages.push(index.root_page);
                    }
                }
            }
        }
    }

    for &dropped_root in &schema.dropped_root_pages {
        root_pages.push(dropped_root);
    }

    let remaining_errors_reg = program.alloc_register();
    program.emit_int((max_errors.saturating_sub(1)) as i64, remaining_errors_reg);

    let had_error_reg = program.alloc_register();
    program.emit_int(0, had_error_reg);

    let message_reg = program.alloc_register();
    let scratch_reg = program.alloc_register();

    program.emit_insn(Insn::IntegrityCk {
        max_errors,
        roots: root_pages,
        message_register: message_reg,
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

        let table_ref_id = program.table_reference_counter.next();
        let table_cursor_id = program.alloc_cursor_id_keyed(
            CursorKey::table(table_ref_id),
            CursorType::BTreeTable(btree_table.clone()),
        );
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: btree_table.root_page,
            db: database_id,
        });

        let mut table_references = TableReferences::new(
            vec![JoinedTable {
                op: Operation::Scan(Scan::BTreeTable {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                }),
                table: Table::BTree(btree_table.clone()),
                identifier: btree_table.name.clone(),
                internal_id: table_ref_id,
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
                database_id,
            }],
            vec![],
        );

        let mut bound_indexes = Vec::new();
        if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
            for index in indexes {
                if index.root_page <= 0 {
                    continue;
                }

                let cursor_id = program.alloc_cursor_index(None, index)?;
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: index.root_page,
                    db: database_id,
                });

                let expected_count_reg = program.alloc_register();
                program.emit_int(0, expected_count_reg);

                let mut where_expr = None;
                if let Some(pred) = index.where_clause.as_deref() {
                    where_expr = Some(bind_expr_for_table(pred, &mut table_references, resolver)?);
                }

                let mut columns = Vec::with_capacity(index.columns.len());
                let mut unique_nullable = Vec::with_capacity(index.columns.len());
                for col in &index.columns {
                    if let Some(expr) = col.expr.as_deref() {
                        columns.push(BoundIndexColumn::Expr(Box::new(bind_expr_for_table(
                            expr,
                            &mut table_references,
                            resolver,
                        )?)));
                        unique_nullable.push(true);
                    } else {
                        columns.push(BoundIndexColumn::Column(col.pos_in_table));
                        unique_nullable.push(!btree_table.columns[col.pos_in_table].notnull());
                    }
                }

                let affinity = if index.columns.iter().any(|c| c.expr.is_some()) {
                    index
                        .columns
                        .iter()
                        .map(|c| {
                            if c.expr.is_some() {
                                Affinity::Blob.aff_mask()
                            } else {
                                btree_table.columns[c.pos_in_table].affinity().aff_mask()
                            }
                        })
                        .collect::<String>()
                } else {
                    build_index_affinity_string(index, btree_table)
                };

                bound_indexes.push(BoundIntegrityIndex {
                    index: index.clone(),
                    cursor_id,
                    expected_count_reg,
                    where_expr,
                    columns,
                    unique_nullable,
                    affinity,
                });
            }
        }

        let mut bound_checks = Vec::with_capacity(btree_table.check_constraints.len());
        for check in &btree_table.check_constraints {
            bound_checks.push(bind_expr_for_table(
                &check.expr,
                &mut table_references,
                resolver,
            )?);
        }

        let not_null_columns: Vec<(usize, String)> = btree_table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if col.notnull() && !col.is_rowid_alias() {
                    Some((
                        idx,
                        col.name.clone().unwrap_or_else(|| format!("column{idx}")),
                    ))
                } else {
                    None
                }
            })
            .collect();

        let row_number_reg = program.alloc_register();
        program.emit_int(0, row_number_reg);

        let table_empty_label = program.allocate_label();
        let loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: table_cursor_id,
            pc_if_empty: table_empty_label,
        });
        program.preassign_label_to_next_insn(loop_start_label);

        program.emit_insn(Insn::AddImm {
            register: row_number_reg,
            value: 1,
        });

        for (col_idx, col_name) in &not_null_columns {
            let col_value_reg = program.alloc_register();
            program.emit_column_or_rowid(table_cursor_id, *col_idx, col_value_reg);

            let not_null_ok = program.allocate_label();
            program.emit_insn(Insn::NotNull {
                reg: col_value_reg,
                target_pc: not_null_ok,
            });
            program.emit_string8(
                format!("NULL value in {}.{}", btree_table.name, col_name),
                message_reg,
            );
            emit_integrity_result_row(program, remaining_errors_reg, message_reg, had_error_reg);
            program.preassign_label_to_next_insn(not_null_ok);
        }

        for check_expr in &bound_checks {
            let check_ok = program.allocate_label();
            let check_fail = program.allocate_label();
            translate_condition_expr(
                program,
                &table_references,
                check_expr,
                ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_true: check_ok,
                    jump_target_when_false: check_fail,
                    jump_target_when_null: check_ok,
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(check_fail);
            program.emit_string8(
                format!("CHECK constraint failed in {}", btree_table.name),
                message_reg,
            );
            emit_integrity_result_row(program, remaining_errors_reg, message_reg, had_error_reg);
            program.preassign_label_to_next_insn(check_ok);
        }

        for bound_index in &bound_indexes {
            let skip_current_index = program.allocate_label();

            if let Some(where_expr) = bound_index.where_expr.as_ref() {
                let where_failed = skip_current_index;
                let where_true_fallthrough = program.allocate_label();
                translate_condition_expr(
                    program,
                    &table_references,
                    where_expr,
                    ConditionMetadata {
                        // For partial indexes, rows that evaluate predicate to FALSE/NULL
                        // are not part of the index and must be skipped.
                        jump_if_condition_is_true: false,
                        jump_target_when_true: where_true_fallthrough,
                        jump_target_when_false: where_failed,
                        jump_target_when_null: where_failed,
                    },
                    resolver,
                )?;
                program.preassign_label_to_next_insn(where_true_fallthrough);
            }

            // Count rows that are expected to appear in this index. For partial
            // indexes this is only rows where the predicate is true.
            program.emit_insn(Insn::AddImm {
                register: bound_index.expected_count_reg,
                value: 1,
            });

            let key_start_reg = program.alloc_registers(bound_index.columns.len() + 1);
            for (i, col) in bound_index.columns.iter().enumerate() {
                let target = key_start_reg + i;
                match col {
                    BoundIndexColumn::Column(pos) => {
                        program.emit_column_or_rowid(table_cursor_id, *pos, target);
                    }
                    BoundIndexColumn::Expr(expr) => {
                        translate_expr_no_constant_opt(
                            program,
                            Some(&table_references),
                            expr,
                            target,
                            resolver,
                            NoConstantOptReason::RegisterReuse,
                        )?;
                    }
                }
            }

            let rowid_reg = key_start_reg + bound_index.columns.len();
            program.emit_insn(Insn::RowId {
                cursor_id: table_cursor_id,
                dest: rowid_reg,
            });

            if let Some(count) = NonZeroUsize::new(bound_index.columns.len()) {
                program.emit_insn(Insn::Affinity {
                    start_reg: key_start_reg,
                    count,
                    affinities: bound_index.affinity.clone(),
                });
            }

            if !quick {
                let found_label = program.allocate_label();
                // Verify the table row has a matching index entry (key columns + rowid).
                program.emit_insn(Insn::Found {
                    cursor_id: bound_index.cursor_id,
                    target_pc: found_label,
                    record_reg: key_start_reg,
                    num_regs: bound_index.columns.len() + 1,
                });
                emit_row_missing_from_index_error(
                    program,
                    row_number_reg,
                    scratch_reg,
                    message_reg,
                    &bound_index.index.name,
                    remaining_errors_reg,
                    had_error_reg,
                );
                // When Found fails, skip the remaining checks for this index
                // (IdxRowid, values-differ, uniqueness). The cursor position is
                // indeterminate after a failed Found.
                program.emit_insn(Insn::Goto {
                    target_pc: skip_current_index,
                });
                program.preassign_label_to_next_insn(found_label);

                // Verify the index entry's rowid matches the table row's rowid.
                {
                    let idx_rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::IdxRowId {
                        cursor_id: bound_index.cursor_id,
                        dest: idx_rowid_reg,
                    });
                    let rowid_ok_label = program.allocate_label();
                    program.emit_insn(Insn::Eq {
                        lhs: idx_rowid_reg,
                        rhs: rowid_reg,
                        target_pc: rowid_ok_label,
                        flags: CmpInsFlags::default(),
                        collation: None,
                    });
                    emit_rowid_not_at_eor_error(
                        program,
                        row_number_reg,
                        scratch_reg,
                        message_reg,
                        &bound_index.index.name,
                        remaining_errors_reg,
                        had_error_reg,
                    );
                    program.preassign_label_to_next_insn(rowid_ok_label);
                }

                // For non-binary collation columns, verify the stored index values
                // match the table values exactly (binary comparison). Found uses the
                // index's collation, so collation-equivalent but non-identical values
                // (e.g. "Hello" vs "hello" with NOCASE) wouldn't be caught otherwise.
                {
                    let non_binary_cols: Vec<usize> = bound_index
                        .index
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| {
                            let coll = c.collation.unwrap_or_default();
                            !matches!(coll, CollationSeq::Binary)
                        })
                        .map(|(i, _)| i)
                        .collect();

                    if !non_binary_cols.is_empty() {
                        let values_ok_label = program.allocate_label();
                        let values_differ_label = program.allocate_label();
                        let idx_col_reg = program.alloc_register();

                        for &col_idx in &non_binary_cols {
                            program.emit_insn(Insn::Column {
                                cursor_id: bound_index.cursor_id,
                                column: col_idx,
                                dest: idx_col_reg,
                                default: None,
                            });
                            program.emit_insn(Insn::Ne {
                                lhs: idx_col_reg,
                                rhs: key_start_reg + col_idx,
                                target_pc: values_differ_label,
                                flags: CmpInsFlags::default(),
                                collation: None,
                            });
                        }
                        program.emit_insn(Insn::Goto {
                            target_pc: values_ok_label,
                        });

                        program.preassign_label_to_next_insn(values_differ_label);
                        emit_values_differ_error(
                            program,
                            row_number_reg,
                            scratch_reg,
                            message_reg,
                            &bound_index.index.name,
                            remaining_errors_reg,
                            had_error_reg,
                        );

                        program.preassign_label_to_next_insn(values_ok_label);
                    }
                }

                if bound_index.index.unique {
                    // Uniqueness rule matches SQLite:
                    //   unique key is valid if any key column is NULL, OR
                    //   the next index entry is strictly greater on key columns.
                    let unique_ok = program.allocate_label();
                    for (i, is_nullable) in bound_index.unique_nullable.iter().enumerate() {
                        if *is_nullable {
                            program.emit_insn(Insn::IsNull {
                                reg: key_start_reg + i,
                                target_pc: unique_ok,
                            });
                        }
                    }

                    let next_exists = program.allocate_label();
                    program.emit_insn(Insn::Next {
                        cursor_id: bound_index.cursor_id,
                        pc_if_next: next_exists,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: unique_ok,
                    });
                    program.preassign_label_to_next_insn(next_exists);

                    program.emit_insn(Insn::IdxGT {
                        cursor_id: bound_index.cursor_id,
                        start_reg: key_start_reg,
                        num_regs: bound_index.columns.len(),
                        target_pc: unique_ok,
                    });
                    program.emit_string8(
                        format!("non-unique entry in index {}", bound_index.index.name),
                        message_reg,
                    );
                    emit_integrity_result_row(
                        program,
                        remaining_errors_reg,
                        message_reg,
                        had_error_reg,
                    );
                    program.preassign_label_to_next_insn(unique_ok);
                }
            }
            program.preassign_label_to_next_insn(skip_current_index);
        }

        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: loop_start_label,
        });
        program.preassign_label_to_next_insn(table_empty_label);

        for bound_index in &bound_indexes {
            if bound_index.where_expr.is_none() {
                let actual_count_reg = program.alloc_register();
                program.emit_insn(Insn::Count {
                    cursor_id: bound_index.cursor_id,
                    target_reg: actual_count_reg,
                    exact: true,
                });

                let counts_match = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: actual_count_reg,
                    rhs: bound_index.expected_count_reg,
                    target_pc: counts_match,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.emit_string8(
                    format!("wrong # of entries in index {}", bound_index.index.name),
                    message_reg,
                );
                emit_integrity_result_row(
                    program,
                    remaining_errors_reg,
                    message_reg,
                    had_error_reg,
                );
                program.preassign_label_to_next_insn(counts_match);
            }

            program.emit_insn(Insn::Close {
                cursor_id: bound_index.cursor_id,
            });
        }

        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id,
        });
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
