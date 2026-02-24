use crate::schema::{BTreeTable, Index, IndexColumn};
use crate::sync::Arc;
use crate::translate::collate::get_collseq_from_expr;
use crate::translate::emitter::{emit_query, LimitCtx, Resolver, TranslateCtx};
use crate::translate::expr::translate_expr;
use crate::translate::plan::{
    CompoundSelectOrderBy, EphemeralRowidMode, Plan, QueryDestination, SelectPlan,
};
use crate::translate::result_row::{emit_columns_to_destination, emit_offset};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u16, Insn};
use crate::{emit_explain, LimboError, QueryMode};
use tracing::instrument;
use turso_parser::ast::{CompoundOperator, Expr, Literal, SortOrder};

use crate::schema::PseudoCursorType;
use tracing::Level;

/// Emits bytecode for a compound SELECT statement (UNION, INTERSECT, EXCEPT, UNION ALL).
/// Returns the result column start register when in coroutine mode (for CTE subqueries),
/// or None for top-level queries.
#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_compound_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: Plan,
) -> crate::Result<Option<usize>> {
    // Destructure by value so we can modify fields for ORDER BY
    let (left, mut right_most, limit, offset, order_by) = match plan {
        Plan::CompoundSelect {
            left,
            right_most,
            limit,
            offset,
            order_by,
        } => (left, right_most, limit, offset, order_by),
        _ => crate::bail_parse_error!("expected compound select plan"),
    };

    let has_order_by = order_by.is_some();
    let num_result_cols = right_most.result_columns.len();
    let original_destination = right_most.query_destination.clone();

    let right_most_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        right_most.table_references.joined_tables().len(),
    );

    // Each subselect shares the same limit_ctx and offset, because the LIMIT, OFFSET applies to
    // the entire compound select, not just a single subselect.
    let limit_ctx = limit
        .as_ref()
        .map(|limit| {
            let reg = program.alloc_register();
            match limit.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => {
                    if let Ok(value) = n.parse::<i64>() {
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::Integer { value, dest: reg });
                    } else {
                        let value = n
                            .parse::<f64>()
                            .map_err(|_| LimboError::ParseError("invalid limit".to_string()))?;
                        program.emit_insn(Insn::Real { value, dest: reg });
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt { reg });
                    }
                }
                _ => {
                    _ = translate_expr(program, None, limit, reg, &right_most_ctx.resolver);
                    program.add_comment(program.offset(), "LIMIT counter");
                    program.emit_insn(Insn::MustBeInt { reg });
                }
            }
            Ok::<_, LimboError>(LimitCtx::new_shared(reg))
        })
        .transpose()?;
    let offset_reg = offset
        .as_ref()
        .map(|offset_expr| {
            let reg = program.alloc_register();
            match offset_expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => {
                    // Compile-time constant offset
                    if let Ok(value) = n.parse::<i64>() {
                        program.emit_insn(Insn::Integer { value, dest: reg });
                    } else {
                        let value = n
                            .parse::<f64>()
                            .map_err(|_| LimboError::ParseError("invalid offset".to_string()))?;
                        program.emit_insn(Insn::Real { value, dest: reg });
                    }
                }
                _ => {
                    _ = translate_expr(program, None, offset_expr, reg, &right_most_ctx.resolver);
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt { reg });
            let combined_reg = program.alloc_register();
            program.add_comment(program.offset(), "OFFSET + LIMIT");
            program.emit_insn(Insn::OffsetLimit {
                offset_reg: reg,
                combined_reg,
                limit_reg: limit_ctx.as_ref().unwrap().reg_limit,
            });

            Ok::<_, LimboError>(reg)
        })
        .transpose()?;

    // When ORDER BY is present, redirect compound output to an ephemeral table,
    // then sort and emit to the original destination.
    let (eph_cursor_id, sort_cursor_id) = if let Some(ref order_by_cols) = order_by {
        let eph_table = Arc::new(BTreeTable {
            root_page: 0,
            name: String::new(),
            columns: vec![],
            primary_key_columns: vec![],
            has_rowid: true,
            is_strict: false,
            has_autoincrement: false,
            unique_sets: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        });
        let eph_cursor = program.alloc_cursor_id(CursorType::BTreeTable(eph_table.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: eph_cursor,
            is_table: true,
        });

        right_most.query_destination = QueryDestination::EphemeralTable {
            cursor_id: eph_cursor,
            table: eph_table,
            rowid_mode: EphemeralRowidMode::Auto,
        };

        // Set up Sorter for ORDER BY
        let order_and_collations: Vec<(SortOrder, Option<_>)> = order_by_cols
            .iter()
            .map(|ob| (ob.order, ob.collation))
            .collect();
        let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
        program.emit_insn(Insn::SorterOpen {
            cursor_id: sort_cursor,
            columns: order_by_cols.len(),
            order_and_collations,
        });

        (Some(eph_cursor), Some(sort_cursor))
    } else {
        (None, None)
    };

    // When a compound SELECT is part of a query that yields results to a coroutine (e.g. within an INSERT clause),
    // we must allocate registers for the result columns to be yielded. Each subselect will then yield to
    // the coroutine using the same set of registers.
    // When the destination is an EphemeralTable or EphemeralIndex (for CTE materialization), we need to
    // insert into that table/index.
    // For top-level queries (ResultRows), we emit ResultRow instructions directly.
    // Allocate registers for result columns when we need to hold values before emitting.
    // For ResultRows we allocate fresh registers per-row in read_deduplicated_*.
    let reg_result_cols_start = match &right_most.query_destination {
        QueryDestination::CoroutineYield { .. }
        | QueryDestination::EphemeralTable { .. }
        | QueryDestination::EphemeralIndex { .. } => {
            Some(program.alloc_registers(right_most.result_columns.len()))
        }
        QueryDestination::ResultRows => None,
        other => {
            return Err(LimboError::InternalError(format!(
                "Unexpected query destination: {other:?} for compound select"
            )));
        }
    };
    // Clone the destination for passing to emit_compound_select
    let query_destination = right_most.query_destination.clone();

    emit_explain!(program, true, "COMPOUND QUERY".to_owned());

    // This is inefficient, but emit_compound_select() takes ownership of 'plan' and we
    // must set the result columns to the leftmost subselect's result columns to be compatible
    // with SQLite.
    program.result_columns.clone_from(&left[0].0.result_columns);

    // These must also be set because we make the decision to start a transaction based on whether
    // any tables are actually touched by the query. Previously this only used the rightmost subselect's
    // table references, but that breaks down with e.g. "SELECT * FROM t UNION VALUES(1)" where VALUES(1)
    // does not have any table references and we would erroneously not start a transaction.
    let right_plan_table_refs = right_most.table_references.clone();
    for (p, _) in &left {
        program.table_references.extend(p.table_references.clone());
    }
    program.table_references.extend(right_plan_table_refs);

    // When ORDER BY is present, don't pass LIMIT/OFFSET to the compound select —
    // they will be applied after sorting. But we still need to check LIMIT 0 to
    // skip the entire compound select.
    let label_order_by_end = if has_order_by {
        let label = program.allocate_label();
        if let Some(limit_ctx) = &limit_ctx {
            program.emit_insn(Insn::IfNot {
                reg: limit_ctx.reg_limit,
                target_pc: label,
                jump_if_null: false,
            });
        }
        Some(label)
    } else {
        None
    };

    let (compound_limit_ctx, compound_offset_reg) = if has_order_by {
        (None, None)
    } else {
        (limit_ctx, offset_reg)
    };

    // Reconstruct plan without ORDER BY (it's handled at this level)
    let compound_plan = Plan::CompoundSelect {
        left,
        right_most,
        limit: if has_order_by { None } else { limit },
        offset: if has_order_by { None } else { offset },
        order_by: None,
    };

    emit_compound_select(
        program,
        compound_plan,
        &right_most_ctx.resolver,
        compound_limit_ctx,
        compound_offset_reg,
        reg_result_cols_start,
        &query_destination,
    )?;
    program.pop_current_parent_explain();

    // If ORDER BY is present, sort the collected rows and emit to the original destination
    let final_reg_result_cols_start = if let Some(order_by_cols) = order_by {
        let eph_cursor = eph_cursor_id.unwrap();
        let sort_cursor = sort_cursor_id.unwrap();
        let num_sort_keys = order_by_cols.len();

        emit_sort_from_ephemeral(
            program,
            eph_cursor,
            sort_cursor,
            &order_by_cols,
            num_result_cols,
            num_sort_keys,
            &original_destination,
            limit_ctx,
            offset_reg,
        )?;

        // For ResultRows, we return None (same as without ORDER BY)
        // For CoroutineYield/EphemeralTable, the sorted output uses different registers
        if matches!(original_destination, QueryDestination::ResultRows) {
            None
        } else {
            reg_result_cols_start
        }
    } else {
        reg_result_cols_start
    };

    // Resolve the LIMIT 0 early-exit label if ORDER BY was present
    if let Some(label) = label_order_by_end {
        program.preassign_label_to_next_insn(label);
    }

    // Restore reg_result_cols_start after emit_compound_select, because nested
    // subqueries (e.g. CTE coroutines) can overwrite program.reg_result_cols_start
    // with their own result column registers. This mirrors the same fix in
    // emit_program_for_select_with_inputs.
    program.reg_result_cols_start = final_reg_result_cols_start;

    Ok(final_reg_result_cols_start)
}

// Emits bytecode for a compound SELECT statement. This function processes the rightmost part of
// the compound SELECT and handles the left parts recursively based on the compound operator type.
#[allow(clippy::too_many_arguments)]
fn emit_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    resolver: &Resolver,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let Plan::CompoundSelect {
        mut left,
        mut right_most,
        limit,
        offset,
        order_by,
    } = plan
    else {
        unreachable!()
    };

    let compound_select_end = program.allocate_label();
    if let Some(limit_ctx) = &limit_ctx {
        program.emit_insn(Insn::IfNot {
            reg: limit_ctx.reg_limit,
            target_pc: compound_select_end,
            jump_if_null: false,
        });
    }
    let mut right_most_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        right_most.table_references.joined_tables().len(),
    );
    right_most_ctx.reg_result_cols_start = reg_result_cols_start;
    match left.pop() {
        Some((mut plan, operator)) => match operator {
            CompoundOperator::UnionAll => {
                if matches!(
                    right_most.query_destination,
                    QueryDestination::EphemeralIndex { .. }
                        | QueryDestination::CoroutineYield { .. }
                        | QueryDestination::EphemeralTable { .. }
                ) {
                    plan.query_destination = right_most.query_destination.clone();
                }
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit: limit.clone(),
                    offset: offset.clone(),
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    limit_ctx,
                    offset_reg,
                    reg_result_cols_start,
                    query_destination,
                )?;

                let label_next_select = program.allocate_label();
                if let Some(limit_ctx) = limit_ctx {
                    program.emit_insn(Insn::IfNot {
                        reg: limit_ctx.reg_limit,
                        target_pc: label_next_select,
                        jump_if_null: true,
                    });
                    right_most.limit = limit;
                    right_most_ctx.limit_ctx = Some(limit_ctx);
                }
                if offset_reg.is_some() {
                    right_most.offset = offset;
                    right_most_ctx.reg_offset = offset_reg;
                }

                emit_explain!(program, true, "UNION ALL".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                program.preassign_label_to_next_insn(label_next_select);
            }
            CompoundOperator::Union => {
                let mut new_dedupe_index = false;
                let dedupe_index = match right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } => (cursor_id, index),
                    _ => {
                        new_dedupe_index = true;
                        create_dedupe_index(program, &plan, &right_most)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;

                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                    affinity_str: None,
                    is_delete: false,
                };

                emit_explain!(program, true, "UNION USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();

                if new_dedupe_index {
                    read_deduplicated_union_or_except_rows(
                        program,
                        dedupe_index.0,
                        dedupe_index.1.as_ref(),
                        limit_ctx,
                        offset_reg,
                        reg_result_cols_start,
                        query_destination,
                    )?;
                }
            }
            CompoundOperator::Intersect => {
                // For nested compound selects (e.g., A INTERSECT B UNION C), the outer UNION
                // sets right_most.query_destination to its dedupe_index. We need to capture
                // this BEFORE we overwrite it with our own indexes for the intersection.
                let intersect_destination = right_most.query_destination.clone();

                let (left_cursor_id, left_index) =
                    create_dedupe_index(program, &plan, &right_most)?;
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: left_cursor_id,
                    index: left_index.clone(),
                    affinity_str: None,
                    is_delete: false,
                };

                let (right_cursor_id, right_index) =
                    create_dedupe_index(program, &plan, &right_most)?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: right_cursor_id,
                    index: right_index,
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;

                emit_explain!(program, true, "INTERSECT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                read_intersect_rows(
                    program,
                    left_cursor_id,
                    &left_index,
                    right_cursor_id,
                    limit_ctx,
                    offset_reg,
                    reg_result_cols_start,
                    &intersect_destination,
                )?;
            }
            CompoundOperator::Except => {
                let mut new_index = false;
                let (cursor_id, index) = match right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } => (cursor_id, index),
                    _ => {
                        new_index = true;
                        create_dedupe_index(program, &plan, &right_most)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    affinity_str: None,
                    is_delete: true,
                };
                emit_explain!(program, true, "EXCEPT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                if new_index {
                    read_deduplicated_union_or_except_rows(
                        program,
                        cursor_id,
                        &index,
                        limit_ctx,
                        offset_reg,
                        reg_result_cols_start,
                        query_destination,
                    )?;
                }
            }
        },
        None => {
            if let Some(limit_ctx) = limit_ctx {
                right_most_ctx.limit_ctx = Some(limit_ctx);
                right_most.limit = limit;
            }
            if offset_reg.is_some() {
                right_most.offset = offset;
                right_most_ctx.reg_offset = offset_reg;
            }
            emit_explain!(program, true, "LEFT-MOST SUBQUERY".to_owned());
            emit_query(program, &mut right_most, &mut right_most_ctx)?;
            program.pop_current_parent_explain();
        }
    }

    program.preassign_label_to_next_insn(compound_select_end);

    Ok(())
}

// Creates an ephemeral index that will be used to deduplicate the results of any sub-selects
fn create_dedupe_index(
    program: &mut ProgramBuilder,
    left_select: &SelectPlan,
    right_select: &SelectPlan,
) -> crate::Result<(usize, Arc<Index>)> {
    let mut dedupe_columns = right_select
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c
                .name(&right_select.table_references)
                .map(|n| n.to_string())
                .unwrap_or_default(),
            order: SortOrder::Asc,
            pos_in_table: i,
            default: None,
            collation: None,
            expr: None,
        })
        .collect::<Vec<_>>();
    for (i, column) in dedupe_columns.iter_mut().enumerate() {
        let left_collation = get_collseq_from_expr(
            &left_select.result_columns[i].expr,
            &left_select.table_references,
        )?;
        let right_collation = get_collseq_from_expr(
            &right_select.result_columns[i].expr,
            &right_select.table_references,
        )?;
        // Left precedence
        let collation = match (left_collation, right_collation) {
            (None, None) => None,
            (Some(coll), None) | (None, Some(coll)) => Some(coll),
            (Some(coll), Some(_)) => Some(coll),
        };
        column.collation = collation;
    }

    let dedupe_index = Arc::new(Index {
        columns: dedupe_columns,
        name: "compound_dedupe".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: false,
        has_rowid: false,
        where_clause: None,
        index_method: None,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(dedupe_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    Ok((cursor_id, dedupe_index))
}

/// Emits the bytecode for reading deduplicated rows from the ephemeral index created for
/// UNION or EXCEPT operators.
#[allow(clippy::too_many_arguments)]
fn read_deduplicated_union_or_except_rows(
    program: &mut ProgramBuilder,
    dedupe_cursor_id: usize,
    dedupe_index: &Index,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let label_close = program.allocate_label();
    let label_dedupe_next = program.allocate_label();
    let label_dedupe_loop_start = program.allocate_label();
    // When in coroutine mode or emitting to index/table, use the pre-allocated result column registers.
    // Otherwise, allocate new registers for reading from the dedupe index.
    let dedupe_cols_start_reg = reg_result_cols_start
        .unwrap_or_else(|| program.alloc_registers(dedupe_index.columns.len()));
    program.emit_insn(Insn::Rewind {
        cursor_id: dedupe_cursor_id,
        pc_if_empty: label_dedupe_next,
    });
    program.preassign_label_to_next_insn(label_dedupe_loop_start);
    if let Some(reg) = offset_reg {
        program.emit_insn(Insn::IfPos {
            reg,
            target_pc: label_dedupe_next,
            decrement_by: 1,
        });
    }
    for col_idx in 0..dedupe_index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: dedupe_cursor_id,
            column: col_idx,
            dest: dedupe_cols_start_reg + col_idx,
            default: None,
        });
    }
    emit_columns_to_destination(
        program,
        query_destination,
        dedupe_cols_start_reg,
        dedupe_index.columns.len(),
    )?;

    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_close,
        })
    }
    program.preassign_label_to_next_insn(label_dedupe_next);
    program.emit_insn(Insn::Next {
        cursor_id: dedupe_cursor_id,
        pc_if_next: label_dedupe_loop_start,
    });
    program.preassign_label_to_next_insn(label_close);
    program.emit_insn(Insn::Close {
        cursor_id: dedupe_cursor_id,
    });
    Ok(())
}

/// Emits the bytecode for reading rows from the intersection of two cursors.
#[allow(clippy::too_many_arguments)]
fn read_intersect_rows(
    program: &mut ProgramBuilder,
    left_cursor_id: usize,
    index: &Index,
    right_cursor_id: usize,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let label_close = program.allocate_label();
    let label_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: left_cursor_id,
        pc_if_empty: label_close,
    });

    program.preassign_label_to_next_insn(label_loop_start);
    let row_content_reg = program.alloc_register();
    program.emit_insn(Insn::RowData {
        cursor_id: left_cursor_id,
        dest: row_content_reg,
    });
    let label_next = program.allocate_label();
    program.emit_insn(Insn::NotFound {
        cursor_id: right_cursor_id,
        target_pc: label_next,
        record_reg: row_content_reg,
        num_regs: 0,
    });
    if let Some(reg) = offset_reg {
        program.emit_insn(Insn::IfPos {
            reg,
            target_pc: label_next,
            decrement_by: 1,
        });
    }
    let column_count = index.columns.len();
    // When in coroutine mode, use the pre-allocated result column registers.
    // Otherwise, allocate new registers for reading from the index.
    let cols_start_reg =
        reg_result_cols_start.unwrap_or_else(|| program.alloc_registers(column_count));
    for i in 0..column_count {
        program.emit_insn(Insn::Column {
            cursor_id: left_cursor_id,
            column: i,
            dest: cols_start_reg + i,
            default: None,
        });
    }

    emit_columns_to_destination(program, query_destination, cols_start_reg, column_count)?;

    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_close,
        });
    }
    program.preassign_label_to_next_insn(label_next);
    program.emit_insn(Insn::Next {
        cursor_id: left_cursor_id,
        pc_if_next: label_loop_start,
    });

    program.preassign_label_to_next_insn(label_close);
    program.emit_insn(Insn::Close {
        cursor_id: right_cursor_id,
    });
    program.emit_insn(Insn::Close {
        cursor_id: left_cursor_id,
    });
    Ok(())
}

/// Reads all rows from an ephemeral table, inserts them into a Sorter (with ORDER BY keys),
/// then reads sorted rows and emits them to the given destination with LIMIT/OFFSET.
#[allow(clippy::too_many_arguments)]
fn emit_sort_from_ephemeral(
    program: &mut ProgramBuilder,
    eph_cursor: usize,
    sort_cursor: usize,
    order_by_cols: &[CompoundSelectOrderBy],
    num_result_cols: usize,
    num_sort_keys: usize,
    destination: &QueryDestination,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
) -> crate::Result<()> {
    emit_explain!(program, false, "USE SORTER FOR ORDER BY".to_owned());

    // Phase 1: Iterate ephemeral table and insert each row into the Sorter.
    // Sorter layout: [sort_key_1, ..., sort_key_n, result_col_1, ..., result_col_m]
    let total_sorter_cols = num_sort_keys + num_result_cols;
    let sorter_regs = program.alloc_registers(total_sorter_cols);

    let label_eph_end = program.allocate_label();
    let label_eph_loop = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: eph_cursor,
        pc_if_empty: label_eph_end,
    });
    program.preassign_label_to_next_insn(label_eph_loop);

    // Copy sort key columns from the ephemeral table
    for (i, ob) in order_by_cols.iter().enumerate() {
        program.emit_insn(Insn::Column {
            cursor_id: eph_cursor,
            column: ob.column_idx,
            dest: sorter_regs + i,
            default: None,
        });
    }
    // Copy all result columns from the ephemeral table
    for i in 0..num_result_cols {
        program.emit_insn(Insn::Column {
            cursor_id: eph_cursor,
            column: i,
            dest: sorter_regs + num_sort_keys + i,
            default: None,
        });
    }

    // Insert into Sorter
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(sorter_regs),
        count: to_u16(total_sorter_cols),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sort_cursor,
        record_reg,
    });

    program.emit_insn(Insn::Next {
        cursor_id: eph_cursor,
        pc_if_next: label_eph_loop,
    });
    program.preassign_label_to_next_insn(label_eph_end);
    program.emit_insn(Insn::Close {
        cursor_id: eph_cursor,
    });

    // Phase 2: Read sorted rows from the Sorter and emit to destination.
    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: total_sorter_cols,
    }));
    let sorter_data_reg = program.alloc_register();

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sorter_data_reg,
        num_fields: total_sorter_cols,
    });

    let label_sort_end = program.allocate_label();
    let label_sort_next = program.allocate_label();
    let label_sort_loop = program.allocate_label();

    program.emit_insn(Insn::SorterSort {
        cursor_id: sort_cursor,
        pc_if_empty: label_sort_end,
    });
    program.preassign_label_to_next_insn(label_sort_loop);

    // Apply OFFSET
    emit_offset(program, label_sort_next, offset_reg);

    program.emit_insn(Insn::SorterData {
        cursor_id: sort_cursor,
        dest_reg: sorter_data_reg,
        pseudo_cursor,
    });

    // Read result columns from the Sorter (they start after the sort keys)
    let result_regs = program.alloc_registers(num_result_cols);
    for i in 0..num_result_cols {
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: num_sort_keys + i,
            dest: result_regs + i,
            default: None,
        });
    }

    // Emit to destination
    emit_columns_to_destination(program, destination, result_regs, num_result_cols)?;

    // Apply LIMIT
    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_sort_end,
        });
    }

    program.preassign_label_to_next_insn(label_sort_next);
    program.emit_insn(Insn::SorterNext {
        cursor_id: sort_cursor,
        pc_if_next: label_sort_loop,
    });
    program.preassign_label_to_next_insn(label_sort_end);

    Ok(())
}
