use crate::translate::expr::emit_table_column;
use crate::vdbe::affinity::Affinity;
use crate::{
    schema::{Index, Schema, Table, EXPR_INDEX_SENTINEL},
    translate::{
        emitter::Resolver,
        expr::{
            translate_plan_condition_expr, translate_plan_expr_no_constant_opt, ConditionMetadata,
            NoConstantOptReason,
        },
        plan::{
            ColumnUsedMask, IterationDirection, JoinedTable, Operation, PlanIndexHint, Scan,
            SourceReadPrograms, TableReferences,
        },
        plan_expr::{lower_hir_expr, PlanExpr, PlanIdentityMap},
        semantic::schema_expr::analyze_schema_exprs,
    },
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
    },
    HashSet, LimboError,
};

/// Maximum number of errors to report with integrity check. If we exceed this number we will
/// short circuit the procedure and return early to not waste time. SQLite uses 100 as default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

enum PlannedIndexColumn {
    Column(usize),
    /// The affiniy is `Some` when the index column refers to a virtual generated column
    /// (the affinity is the column's declared type).
    Expr(PlanExpr, Option<Affinity>),
}

struct PlannedIntegrityIndex {
    index: crate::sync::Arc<Index>,
    cursor_id: usize,
    expected_count_reg: usize,
    where_expr: Option<PlanExpr>,
    columns: Vec<PlannedIndexColumn>,
    unique_nullable: Vec<bool>,
}

enum PendingIndexColumn {
    Column(usize),
    Expr(usize, Option<Affinity>),
}

struct PendingIntegrityIndex {
    index: crate::sync::Arc<Index>,
    cursor_id: usize,
    expected_count_reg: usize,
    where_slot: Option<usize>,
    columns: Vec<PendingIndexColumn>,
    unique_nullable: Vec<bool>,
}

/// Translate PRAGMA integrity_check.
pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    connection: &std::sync::Arc<crate::Connection>,
) -> crate::Result<()> {
    translate_integrity_check_impl(
        schema,
        program,
        resolver,
        database_id,
        max_errors,
        false,
        connection,
    )
}

/// Translate PRAGMA quick_check.
pub fn translate_quick_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    connection: &std::sync::Arc<crate::Connection>,
) -> crate::Result<()> {
    translate_integrity_check_impl(
        schema,
        program,
        resolver,
        database_id,
        max_errors,
        true,
        connection,
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

fn translate_integrity_check_impl(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    quick: bool,
    connection: &std::sync::Arc<crate::Connection>,
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
        db: database_id,
        max_errors,
        roots: root_pages,
        dropped_roots,
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

        let table_ref_id = program.next_plan_source_id();
        let table_cursor_id = program.alloc_cursor_id_keyed(
            CursorKey::table(table_ref_id),
            CursorType::BTreeTable(btree_table.clone()),
        );
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: btree_table.root_page,
            db: database_id,
        });

        let mut stored_expressions = Vec::new();
        let mut generated_slots = Vec::with_capacity(btree_table.columns().len());
        for column in btree_table.columns() {
            let slot = column
                .generated_expr()
                .map(|expression| -> crate::Result<usize> {
                    let slot = stored_expressions.len();
                    stored_expressions.push(expression.as_valid()?);
                    Ok(slot)
                })
                .transpose()?;
            generated_slots.push(slot);
        }
        let mut default_slots = Vec::with_capacity(btree_table.columns().len());
        for column in btree_table.columns() {
            let slot = column
                .default
                .as_deref()
                .map(|expression| -> crate::Result<usize> {
                    let slot = stored_expressions.len();
                    stored_expressions.push(expression.as_valid()?);
                    Ok(slot)
                })
                .transpose()?;
            default_slots.push(slot);
        }

        let mut pending_indexes = Vec::new();
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

                let where_slot = index
                    .where_clause
                    .as_deref()
                    .map(|predicate| -> crate::Result<usize> {
                        let slot = stored_expressions.len();
                        stored_expressions.push(predicate.as_valid()?);
                        Ok(slot)
                    })
                    .transpose()?;

                let mut columns = Vec::with_capacity(index.columns.len());
                let mut unique_nullable = Vec::with_capacity(index.columns.len());
                for col in &index.columns {
                    if let Some(expr) = col.expr.as_deref() {
                        let affinity = if col.pos_in_table != EXPR_INDEX_SENTINEL {
                            Some(btree_table.columns()[col.pos_in_table].affinity())
                        } else {
                            // Expression indexes do not apply affinity from the base table.
                            None
                        };
                        let slot = stored_expressions.len();
                        stored_expressions.push(expr.as_valid()?);
                        columns.push(PendingIndexColumn::Expr(slot, affinity));
                        unique_nullable.push(true);
                    } else {
                        columns.push(PendingIndexColumn::Column(col.pos_in_table));
                        unique_nullable.push(!btree_table.columns()[col.pos_in_table].notnull());
                    }
                }

                pending_indexes.push(PendingIntegrityIndex {
                    index: index.clone(),
                    cursor_id,
                    expected_count_reg,
                    where_slot,
                    columns,
                    unique_nullable,
                });
            }
        }

        let mut check_slots = Vec::with_capacity(btree_table.check_constraints.len());
        for check in &btree_table.check_constraints {
            let slot = stored_expressions.len();
            stored_expressions.push(check.expr.as_valid()?);
            check_slots.push(slot);
        }

        let context = resolver.semantic_context();
        let analyzed =
            analyze_schema_exprs(&context, database_id, table.clone(), &stored_expressions)?;
        let mut identities = PlanIdentityMap::new();
        identities.bind_source_definition(&analyzed.source, table_ref_id);
        let lowered = analyzed
            .expressions
            .iter()
            .map(|expression| {
                lower_hir_expr(expression, &identities).map_err(|error| {
                    LimboError::InternalError(format!(
                        "failed to lower integrity-check expression: {error}"
                    ))
                })
            })
            .collect::<crate::Result<Vec<PlanExpr>>>()?;
        let generated_expressions: Vec<Option<PlanExpr>> = generated_slots
            .into_iter()
            .map(|slot| slot.map(|slot| lowered[slot].clone()))
            .collect();
        let default_expressions: Vec<Option<PlanExpr>> = default_slots
            .into_iter()
            .map(|slot| slot.map(|slot| lowered[slot].clone()))
            .collect();
        let read_programs = crate::sync::Arc::new(SourceReadPrograms {
            column_type_programs: vec![None; generated_expressions.len()],
            generated_expressions,
            default_expressions,
        });
        let table_references = TableReferences::new(
            vec![JoinedTable {
                op: Operation::Scan(Scan::BTreeTable {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                }),
                table: Table::BTree(btree_table.clone()),
                resolved_table: None,
                identifier: btree_table.name.clone(),
                internal_id: table_ref_id,
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
                database_id,
                index_hint: PlanIndexHint::None,
                index_method_patterns: Vec::new(),
                index_expressions: Vec::new(),
                read_programs,
                check_constraints: Vec::new(),
            }],
            vec![],
        );
        let planned_indexes = pending_indexes
            .into_iter()
            .map(|pending| PlannedIntegrityIndex {
                index: pending.index,
                cursor_id: pending.cursor_id,
                expected_count_reg: pending.expected_count_reg,
                where_expr: pending.where_slot.map(|slot| lowered[slot].clone()),
                columns: pending
                    .columns
                    .into_iter()
                    .map(|column| match column {
                        PendingIndexColumn::Column(position) => {
                            PlannedIndexColumn::Column(position)
                        }
                        PendingIndexColumn::Expr(slot, affinity) => {
                            PlannedIndexColumn::Expr(lowered[slot].clone(), affinity)
                        }
                    })
                    .collect(),
                unique_nullable: pending.unique_nullable,
            })
            .collect::<Vec<_>>();
        let planned_checks = check_slots
            .into_iter()
            .map(|slot| lowered[slot].clone())
            .collect::<Vec<_>>();

        let not_null_columns: Vec<(usize, String)> = btree_table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.notnull() && !col.is_rowid_alias())
            .map(|(idx, col)| {
                let name = col.name.clone().unwrap_or_else(|| format!("column{idx}"));
                (idx, name)
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

        for (column_index, col_name) in &not_null_columns {
            let col_value_reg = program.alloc_register();
            emit_table_column(
                program,
                table_cursor_id,
                table_ref_id,
                &table_references,
                &btree_table.columns()[*column_index],
                *column_index,
                col_value_reg,
                resolver,
            )?;

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

        for check_expr in &planned_checks {
            let check_ok = program.allocate_label();
            // Evaluate the CHECK expression into a register, then branch.
            // A CHECK constraint passes when the result is TRUE *or* NULL
            // (only explicit FALSE/0 is a violation), so we use
            // jump_if_null: true to treat NULL as passing.
            let check_reg = program.alloc_register();
            translate_plan_expr_no_constant_opt(
                program,
                Some(&table_references),
                check_expr,
                check_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            program.emit_insn(Insn::If {
                reg: check_reg,
                target_pc: check_ok,
                jump_if_null: true,
            });
            program.emit_string8(
                format!("CHECK constraint failed in {}", btree_table.name),
                message_reg,
            );
            emit_integrity_result_row(program, remaining_errors_reg, message_reg, had_error_reg);
            program.preassign_label_to_next_insn(check_ok);
        }

        for planned_index in &planned_indexes {
            let skip_current_index = program.allocate_label();

            if let Some(where_expr) = planned_index.where_expr.as_ref() {
                let where_failed = skip_current_index;
                let where_true_fallthrough = program.allocate_label();
                translate_plan_condition_expr(
                    program,
                    Some(&table_references),
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
                register: planned_index.expected_count_reg,
                value: 1,
            });

            let key_start_reg = program.alloc_registers(planned_index.columns.len() + 1);
            for (i, col) in planned_index.columns.iter().enumerate() {
                let target = key_start_reg + i;
                match col {
                    PlannedIndexColumn::Column(pos) => {
                        emit_table_column(
                            program,
                            table_cursor_id,
                            table_ref_id,
                            &table_references,
                            &btree_table.columns()[*pos],
                            *pos,
                            target,
                            resolver,
                        )?;
                    }
                    PlannedIndexColumn::Expr(expr, affinity) => {
                        translate_plan_expr_no_constant_opt(
                            program,
                            Some(&table_references),
                            expr,
                            target,
                            resolver,
                            NoConstantOptReason::RegisterReuse,
                        )?;
                        if let Some(aff) = affinity {
                            program.emit_column_affinity(target, *aff);
                        }
                    }
                }
            }

            let rowid_reg = key_start_reg + planned_index.columns.len();
            program.emit_insn(Insn::RowId {
                cursor_id: table_cursor_id,
                dest: rowid_reg,
            });

            if !quick {
                let found_label = program.allocate_label();
                // Verify the table row has a matching index entry (key columns + rowid).
                program.emit_insn(Insn::Found {
                    cursor_id: planned_index.cursor_id,
                    target_pc: found_label,
                    record_reg: key_start_reg,
                    num_regs: planned_index.columns.len() + 1,
                });
                emit_row_missing_from_index_error(
                    program,
                    row_number_reg,
                    scratch_reg,
                    message_reg,
                    &planned_index.index.name,
                    remaining_errors_reg,
                    had_error_reg,
                );
                program.preassign_label_to_next_insn(found_label);

                if planned_index.index.unique {
                    // This intentionally runs even after a "missing from index"
                    // report above. SQLite does the same: a single corrupt row
                    // can violate multiple invariants and each should be
                    // independently reportable.
                    //
                    // Uniqueness rule matches SQLite:
                    //   unique key is valid if any key column is NULL, OR
                    //   the next index entry is strictly greater on key columns.
                    let unique_ok = program.allocate_label();
                    for (i, is_nullable) in planned_index.unique_nullable.iter().enumerate() {
                        if *is_nullable {
                            program.emit_insn(Insn::IsNull {
                                reg: key_start_reg + i,
                                target_pc: unique_ok,
                            });
                        }
                    }

                    let next_exists = program.allocate_label();
                    program.emit_insn(Insn::Next {
                        cursor_id: planned_index.cursor_id,
                        pc_if_next: next_exists,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: unique_ok,
                    });
                    program.preassign_label_to_next_insn(next_exists);

                    program.emit_insn(Insn::IdxGT {
                        cursor_id: planned_index.cursor_id,
                        start_reg: key_start_reg,
                        num_regs: planned_index.columns.len(),
                        target_pc: unique_ok,
                    });
                    program.emit_string8(
                        format!("non-unique entry in index {}", planned_index.index.name),
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

        for planned_index in &planned_indexes {
            if planned_index.where_expr.is_none() {
                let actual_count_reg = program.alloc_register();
                program.emit_insn(Insn::Count {
                    cursor_id: planned_index.cursor_id,
                    target_reg: actual_count_reg,
                    exact: true,
                });

                let counts_match = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: actual_count_reg,
                    rhs: planned_index.expected_count_reg,
                    target_pc: counts_match,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.emit_string8(
                    format!("wrong # of entries in index {}", planned_index.index.name),
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
                cursor_id: planned_index.cursor_id,
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
