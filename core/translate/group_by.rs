use turso_parser::ast::{self, SortOrder};

use super::{
    emitter::TranslateCtx,
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    order_by::order_by_sorter_insert,
    plan::{Distinctness, GroupBy, SelectPlan},
    result_row::emit_select_result,
};
use crate::translate::plan::ResultSetColumn;
use crate::translate::{
    aggregation::{translate_aggregation_step, AggArgumentSource},
    plan::Aggregate,
};
use crate::translate::{
    emitter::Resolver,
    expr::{walk_expr, WalkControl},
    optimizer::Optimizable,
};
use crate::{
    schema::PseudoCursorType,
    translate::collate::CollationSeq,
    util::exprs_are_equivalent,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
        BranchOffset,
    },
    Result,
};

/// Labels needed for various jumps in GROUP BY handling.
#[derive(Debug)]
pub struct GroupByLabels {
    /// Label for the subroutine that clears the accumulator registers (temporary storage for per-group aggregate calculations)
    pub label_subrtn_acc_clear: BranchOffset,
    /// Label for the subroutine that outputs the current group's data
    pub label_subrtn_acc_output: BranchOffset,
    /// Label for the instruction that sets the accumulator indicator to true (indicating data exists in the accumulator for the current group)
    pub label_acc_indicator_set_flag_true: BranchOffset,
    /// Label for the instruction that jumps to the end of the grouping process without emitting a row
    pub label_group_by_end_without_emitting_row: BranchOffset,
    /// Label for the instruction that jumps to the end of the grouping process
    pub label_agg_final: BranchOffset,
    /// Label for the instruction that jumps to the end of the grouping process
    pub label_group_by_end: BranchOffset,
    /// Label for the instruction that jumps to the start of the loop that processed sorted data for GROUP BY.
    /// Not relevant for cases where the data is already sorted.
    pub label_sort_loop_start: BranchOffset,
    /// Label for the instruction that jumps to the end of the loop that processed sorted data for GROUP BY.
    /// Not relevant for cases where the data is already sorted.
    pub label_sort_loop_end: BranchOffset,
    /// Label for the instruction that jumps to the start of the aggregation step
    pub label_grouping_agg_step: BranchOffset,
}

/// Registers allocated for GROUP BY operations.
#[derive(Debug)]
pub struct GroupByRegisters {
    pub reg_group_by_source_cols_start: usize,
    /// Register holding the return offset for the accumulator clear subroutine
    pub reg_subrtn_acc_clear_return_offset: usize,
    /// Register holding a flag to abort the grouping process if necessary
    pub reg_abort_flag: usize,
    /// Register holding the start of the non aggregate query members (all columns except aggregate arguments)
    pub reg_non_aggregate_exprs_acc: usize,
    /// Register holding the return offset for the accumulator output subroutine
    pub reg_subrtn_acc_output_return_offset: usize,
    /// Register holding a flag to indicate if data exists in the accumulator for the current group
    pub reg_data_in_acc_flag: usize,
    /// Starting index of the register(s) that hold the comparison result between the current row and the previous row
    /// The comparison result is used to determine if the current row belongs to the same group as the previous row
    /// Each group by expression has a corresponding register
    pub reg_group_exprs_cmp: usize,
}

// Metadata for handling GROUP BY operations
#[derive(Debug)]
pub struct GroupByMetadata {
    // Source of rows for the GROUP BY operation - either a sorter or the main loop itself, incase the rows are already sorted in GROUP BY required order
    pub row_source: GroupByRowSource,
    pub labels: GroupByLabels,
    pub registers: GroupByRegisters,
}

/// Initialize resources needed for GROUP BY processing
pub fn init_group_by<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    group_by: &'a GroupBy,
    plan: &SelectPlan,
    result_columns: &'a [ResultSetColumn],
    order_by: &'a [(Box<ast::Expr>, ast::SortOrder)],
) -> Result<()> {
    collect_non_aggregate_expressions(
        &mut t_ctx.non_aggregate_expressions,
        group_by,
        plan,
        result_columns,
        order_by,
    )?;

    let label_subrtn_acc_output = program.allocate_label();
    let label_group_by_end_without_emitting_row = program.allocate_label();
    let label_acc_indicator_set_flag_true = program.allocate_label();
    let label_agg_final = program.allocate_label();
    let label_group_by_end = program.allocate_label();
    let label_subrtn_acc_clear = program.allocate_label();
    let label_sort_loop_start = program.allocate_label();
    let label_sort_loop_end = program.allocate_label();
    let label_grouping_agg_step = program.allocate_label();

    let reg_subrtn_acc_output_return_offset = program.alloc_register();
    let reg_data_in_acc_flag = program.alloc_register();
    let reg_abort_flag = program.alloc_register();
    let reg_group_exprs_cmp = program.alloc_registers(group_by.exprs.len());

    // The following two blocks of registers should always be allocated contiguously,
    // because they are cleared in a contiguous block in the GROUP BYs clear accumulator subroutine.
    // START BLOCK
    let reg_non_aggregate_exprs_acc =
        program.alloc_registers(t_ctx.non_aggregate_expressions.len());
    if !plan.aggregates.is_empty() {
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    }
    // END BLOCK

    let reg_sorter_key = program.alloc_register();
    let column_count = plan.agg_args_count() + t_ctx.non_aggregate_expressions.len();
    let reg_group_by_source_cols_start = program.alloc_registers(column_count);

    let row_source = if let Some(sort_order) = group_by.sort_order.as_ref() {
        let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
        // Should work the same way as Order By
        /*
         * Terms of the ORDER BY clause that is part of a SELECT statement may be assigned a collating sequence using the COLLATE operator,
         * in which case the specified collating function is used for sorting.
         * Otherwise, if the expression sorted by an ORDER BY clause is a column,
         * then the collating sequence of the column is used to determine sort order.
         * If the expression is not a column and has no COLLATE clause, then the BINARY collating sequence is used.
         */
        let collations = group_by
            .exprs
            .iter()
            .map(|expr| match expr {
                ast::Expr::Collate(_, collation_name) => {
                    CollationSeq::new(collation_name.as_str()).map(Some)
                }
                ast::Expr::Column { table, column, .. } => {
                    let table_reference = plan
                        .table_references
                        .find_joined_table_by_internal_id(*table)
                        .unwrap();

                    let Some(table_column) = table_reference.table.get_column_at(*column) else {
                        crate::bail_parse_error!("column index out of bounds");
                    };

                    Ok(table_column.collation)
                }
                _ => Ok(Some(CollationSeq::default())),
            })
            .collect::<Result<Vec<_>>>()?;

        program.emit_insn(Insn::SorterOpen {
            cursor_id: sort_cursor,
            columns: column_count,
            order: sort_order.clone(),
            collations,
        });
        let pseudo_cursor = group_by_create_pseudo_table(program, column_count);
        GroupByRowSource::Sorter {
            pseudo_cursor,
            sort_cursor,
            reg_sorter_key,
            sorter_column_count: column_count,
            start_reg_dest: reg_non_aggregate_exprs_acc,
        }
    } else {
        GroupByRowSource::MainLoop {
            start_reg_src: reg_group_by_source_cols_start,
            start_reg_dest: reg_non_aggregate_exprs_acc,
        }
    };

    program.add_comment(program.offset(), "clear group by abort flag");
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_abort_flag,
    });

    program.add_comment(
        program.offset(),
        "initialize group by comparison registers to NULL",
    );
    program.emit_insn(Insn::Null {
        dest: reg_group_exprs_cmp,
        dest_end: if group_by.exprs.len() > 1 {
            Some(reg_group_exprs_cmp + group_by.exprs.len() - 1)
        } else {
            None
        },
    });

    program.add_comment(program.offset(), "go to clear accumulator subroutine");

    let reg_subrtn_acc_clear_return_offset = program.alloc_register();
    program.emit_insn(Insn::Gosub {
        target_pc: label_subrtn_acc_clear,
        return_reg: reg_subrtn_acc_clear_return_offset,
    });

    t_ctx.meta_group_by = Some(GroupByMetadata {
        row_source,
        labels: GroupByLabels {
            label_subrtn_acc_output,
            label_group_by_end_without_emitting_row,
            label_acc_indicator_set_flag_true,
            label_agg_final,
            label_group_by_end,
            label_subrtn_acc_clear,
            label_sort_loop_start,
            label_sort_loop_end,
            label_grouping_agg_step,
        },
        registers: GroupByRegisters {
            reg_subrtn_acc_output_return_offset,
            reg_data_in_acc_flag,
            reg_abort_flag,
            reg_non_aggregate_exprs_acc,
            reg_group_exprs_cmp,
            reg_subrtn_acc_clear_return_offset,
            reg_group_by_source_cols_start,
        },
    });
    Ok(())
}

/// Returns whether an ORDER BY expression should be treated as an
/// aggregate-position term for the purposes of tie-ordering.
///
/// We classify an ORDER BY term as "aggregate or constant" when:
/// it is syntactically equivalent to one of the finalized aggregate
/// expressions for this SELECT (`COUNT(*)`, `SUM(col)`, `MAX(price)`), or
/// it is a constant literal
///
/// Why this matters:
/// When ORDER BY consists only of aggregates and/or constants, SQLite relies
/// on the stability of the ORDER BY sorter to preserve the traversal order
/// of groups established by GROUP BY iteration, and no extra tiebreak
/// `Sequence` column is appended
pub fn is_orderby_agg_or_const(resolver: &Resolver, e: &ast::Expr, aggs: &[Aggregate]) -> bool {
    if aggs
        .iter()
        .any(|agg| exprs_are_equivalent(&agg.original_expr, e))
    {
        return true;
    }
    e.is_constant(resolver)
}

/// Computes the traversal order of GROUP BY keys so that the final
/// ORDER BY matches SQLite's tie-breaking semantics.
///
/// If there are no GROUP BY keys or no ORDER BY terms, all keys default to ascending.
///
/// If *every* ORDER BY term is an aggregate or a constant then we mirror the
/// direction of the first ORDER BY term across all GROUP BY keys.
///
/// Otherwise (mixed ORDER BY: at least one non-aggregate, non-constant term),
/// we try to mirror explicit directions for any GROUP BY expression that
/// appears in ORDER BY, and the remaining keys default to `ASC`.
pub fn compute_group_by_sort_order(
    group_by_exprs: &[ast::Expr],
    order_by: &[(Box<ast::Expr>, SortOrder)],
    aggs: &[Aggregate],
    resolver: &Resolver,
) -> Vec<SortOrder> {
    let groupby_len = group_by_exprs.len();
    if groupby_len == 0 || order_by.is_empty() {
        return vec![SortOrder::Asc; groupby_len];
    }
    let only_agg_or_const = order_by
        .iter()
        .all(|(e, _)| is_orderby_agg_or_const(resolver, e, aggs));

    if only_agg_or_const {
        let first_direction = order_by[0].1;
        return vec![first_direction; groupby_len];
    }

    let mut result = vec![SortOrder::Asc; groupby_len];
    for (idx, groupby_expr) in group_by_exprs.iter().enumerate() {
        if let Some((_, direction)) = order_by
            .iter()
            .find(|(expr, _)| exprs_are_equivalent(expr, groupby_expr))
        {
            result[idx] = *direction;
        }
    }
    result
}

fn collect_non_aggregate_expressions<'a>(
    non_aggregate_expressions: &mut Vec<(&'a ast::Expr, bool)>,
    group_by: &'a GroupBy,
    plan: &SelectPlan,
    root_result_columns: &'a [ResultSetColumn],
    order_by: &'a [(Box<ast::Expr>, ast::SortOrder)],
) -> Result<()> {
    let mut result_columns = Vec::new();
    for expr in root_result_columns
        .iter()
        .map(|col| &col.expr)
        .chain(order_by.iter().map(|(e, _)| e.as_ref()))
        .chain(group_by.having.iter().flatten())
    {
        collect_result_columns(expr, plan, &mut result_columns)?;
    }

    for group_expr in &group_by.exprs {
        let in_result = result_columns
            .iter()
            .any(|expr| exprs_are_equivalent(expr, group_expr));
        non_aggregate_expressions.push((group_expr, in_result));
    }
    for expr in result_columns {
        let in_group_by = group_by
            .exprs
            .iter()
            .any(|group_expr| exprs_are_equivalent(expr, group_expr));
        if !in_group_by {
            non_aggregate_expressions.push((expr, true));
        }
    }
    Ok(())
}

fn collect_result_columns<'a>(
    root_expr: &'a ast::Expr,
    plan: &SelectPlan,
    result_columns: &mut Vec<&'a ast::Expr>,
) -> Result<()> {
    walk_expr(root_expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => {
                if plan
                    .table_references
                    .find_joined_table_by_internal_id(*table)
                    .is_some()
                {
                    result_columns.push(expr);
                }
            }
            _ => {
                if plan.aggregates.iter().any(|a| a.original_expr == *expr) {
                    return Ok(WalkControl::SkipChildren);
                }
            }
        };
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// In case sorting is needed for GROUP BY, creates a pseudo table that matches
/// the number of columns in the GROUP BY sorter. Rows are individually read
/// from the sorter into this pseudo table and processed.
pub fn group_by_create_pseudo_table(
    program: &mut ProgramBuilder,
    sorter_column_count: usize,
) -> usize {
    // Create a pseudo-table to read one row at a time from the sorter
    // This allows us to use standard table access operations on the sorted data
    program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter_column_count,
    }))
}

/// In case sorting is needed for GROUP BY, sorts the rows in the GROUP BY sorter
/// and opens a pseudo table from which the sorted rows are read.
pub fn emit_group_by_sort_loop_start(
    program: &mut ProgramBuilder,
    row_source: &GroupByRowSource,
    label_sort_loop_end: BranchOffset,
) -> Result<()> {
    let GroupByRowSource::Sorter {
        sort_cursor,
        pseudo_cursor,
        reg_sorter_key,
        sorter_column_count,
        ..
    } = row_source
    else {
        crate::bail_parse_error!("sort cursor must be opened for GROUP BY if we got here");
    };
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: *pseudo_cursor,
        content_reg: *reg_sorter_key,
        num_fields: *sorter_column_count,
    });

    // Sort the sorter based on the group by columns
    program.emit_insn(Insn::SorterSort {
        cursor_id: *sort_cursor,
        pc_if_empty: label_sort_loop_end,
    });

    Ok(())
}

/// In case sorting is needed for GROUP BY, advances to the next row
/// in the GROUP BY sorter.
pub fn emit_group_by_sort_loop_end(
    program: &mut ProgramBuilder,
    sort_cursor: usize,
    label_sort_loop_start: BranchOffset,
    label_sort_loop_end: BranchOffset,
) {
    // Continue to the next row in the sorter
    program.emit_insn(Insn::SorterNext {
        cursor_id: sort_cursor,
        pc_if_next: label_sort_loop_start,
    });
    program.preassign_label_to_next_insn(label_sort_loop_end);
}

/// Enum representing the source for the rows processed during a GROUP BY.
/// In case sorting is needed (which is most of the time), the variant
/// [GroupByRowSource::Sorter] encodes the necessary information about that
/// sorter.
///
/// In case where the rows are already ordered, for example:
/// "SELECT indexed_col, count(1) FROM t GROUP BY indexed_col"
/// the rows are processed directly in the order they arrive from
/// the main query loop.
#[derive(Debug)]
pub enum GroupByRowSource {
    Sorter {
        /// Cursor opened for the pseudo table that GROUP BY reads rows from.
        pseudo_cursor: usize,
        /// The sorter opened for ensuring the rows are in GROUP BY order.
        sort_cursor: usize,
        /// Register holding the key used for sorting in the Sorter
        reg_sorter_key: usize,
        /// Number of columns in the GROUP BY sorter
        sorter_column_count: usize,
        start_reg_dest: usize,
    },
    MainLoop {
        /// If GROUP BY rows are read directly in the main loop, start_reg is the first register
        /// holding the value of a relevant column.
        start_reg_src: usize,
        /// The grouping columns for a group that is not yet finalized must be placed in new registers,
        /// so that they don't get overwritten by the next group's data.
        /// This is because the emission of a group that is "done" is made after a comparison between the "current" and "next" grouping
        /// columns returns nonequal. If we don't store the "current" group in a separate set of registers, the "next" group's data will
        /// overwrite the "current" group's columns and the wrong grouping column values will be emitted.
        /// Aggregation results do not require new registers as they are not at risk of being overwritten before a given group
        /// is processed.
        start_reg_dest: usize,
    },
}

/// Emits bytecode for processing a single GROUP BY group.
pub fn group_by_process_single_group(
    program: &mut ProgramBuilder,
    group_by: &GroupBy,
    plan: &SelectPlan,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let GroupByMetadata {
        registers,
        labels,
        row_source,
        ..
    } = t_ctx
        .meta_group_by
        .as_ref()
        .expect("group by metadata not found");
    program.preassign_label_to_next_insn(labels.label_sort_loop_start);
    let groups_start_reg = match &row_source {
        GroupByRowSource::Sorter {
            sort_cursor,
            pseudo_cursor,
            reg_sorter_key,
            ..
        } => {
            // Read a row from the sorted data in the sorter into the pseudo cursor
            program.emit_insn(Insn::SorterData {
                cursor_id: *sort_cursor,
                dest_reg: *reg_sorter_key,
                pseudo_cursor: *pseudo_cursor,
            });
            // Read the group by columns from the pseudo cursor
            let groups_start_reg = program.alloc_registers(group_by.exprs.len());
            for i in 0..group_by.exprs.len() {
                let sorter_column_index = i;
                let group_reg = groups_start_reg + i;
                program.emit_column_or_rowid(*pseudo_cursor, sorter_column_index, group_reg);
            }
            groups_start_reg
        }

        GroupByRowSource::MainLoop { start_reg_src, .. } => *start_reg_src,
    };

    // Compare the group by columns to the previous group by columns to see if we are at a new group or not
    program.emit_insn(Insn::Compare {
        start_reg_a: registers.reg_group_exprs_cmp,
        start_reg_b: groups_start_reg,
        count: group_by.exprs.len(),
        collation: program.curr_collation(),
    });

    program.add_comment(
        program.offset(),
        "start new group if comparison is not equal",
    );
    // If we are at a new group, continue. If we are at the same group, jump to the aggregation step (i.e. accumulate more values into the aggregations)
    let label_jump_after_comparison = program.allocate_label();
    program.emit_insn(Insn::Jump {
        target_pc_lt: label_jump_after_comparison,
        target_pc_eq: labels.label_grouping_agg_step,
        target_pc_gt: label_jump_after_comparison,
    });

    program.add_comment(
        program.offset(),
        "check if ended group had data, and output if so",
    );
    program.resolve_label(label_jump_after_comparison, program.offset());
    program.emit_insn(Insn::Gosub {
        target_pc: labels.label_subrtn_acc_output,
        return_reg: registers.reg_subrtn_acc_output_return_offset,
    });

    // New group, move current group by columns into the comparison register
    program.emit_insn(Insn::Move {
        source_reg: groups_start_reg,
        dest_reg: registers.reg_group_exprs_cmp,
        count: group_by.exprs.len(),
    });

    program.add_comment(program.offset(), "check abort flag");
    program.emit_insn(Insn::IfPos {
        reg: registers.reg_abort_flag,
        target_pc: labels.label_group_by_end,
        decrement_by: 0,
    });

    program.add_comment(program.offset(), "goto clear accumulator subroutine");
    program.emit_insn(Insn::Gosub {
        target_pc: labels.label_subrtn_acc_clear,
        return_reg: registers.reg_subrtn_acc_clear_return_offset,
    });

    // Process each aggregate function for the current row
    program.preassign_label_to_next_insn(labels.label_grouping_agg_step);
    let cursor_index = t_ctx.non_aggregate_expressions.len(); // Skipping all columns in sorter that not an aggregation arguments
    let mut offset = 0;
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let start_reg = t_ctx
            .reg_agg_start
            .expect("aggregate registers must be initialized");
        let agg_result_reg = start_reg + i;
        let agg_arg_source = match &row_source {
            GroupByRowSource::Sorter { pseudo_cursor, .. } => AggArgumentSource::new_from_cursor(
                program,
                *pseudo_cursor,
                cursor_index + offset,
                agg,
            ),
            GroupByRowSource::MainLoop { start_reg_src, .. } => {
                // Aggregation arguments are always placed in the registers that follow any scalars.
                let start_reg_aggs = start_reg_src + t_ctx.non_aggregate_expressions.len();
                AggArgumentSource::new_from_registers(start_reg_aggs + offset, agg)
            }
        };
        translate_aggregation_step(
            program,
            &plan.table_references,
            agg_arg_source,
            agg_result_reg,
            &t_ctx.resolver,
        )?;
        if let Distinctness::Distinct { ctx } = &agg.distinctness {
            let ctx = ctx
                .as_ref()
                .expect("distinct aggregate context not populated");
            program.preassign_label_to_next_insn(ctx.label_on_conflict);
        }
        offset += agg.args.len();
    }

    // We only need to store non-aggregate columns once per group
    // Skip if we've already stored them for this group
    program.add_comment(
        program.offset(),
        "don't emit group columns if continuing existing group",
    );
    program.emit_insn(Insn::If {
        target_pc: labels.label_acc_indicator_set_flag_true,
        reg: registers.reg_data_in_acc_flag,
        jump_if_null: false,
    });

    // Read non-aggregate columns from the current row
    match row_source {
        GroupByRowSource::Sorter {
            pseudo_cursor,
            start_reg_dest,
            ..
        } => {
            let mut next_reg = *start_reg_dest;

            for (sorter_column_index, (expr, in_result)) in
                t_ctx.non_aggregate_expressions.iter().enumerate()
            {
                if *in_result {
                    program.emit_column_or_rowid(*pseudo_cursor, sorter_column_index, next_reg);
                    t_ctx.resolver.expr_to_reg_cache.push((expr, next_reg));
                    next_reg += 1;
                }
            }
        }
        GroupByRowSource::MainLoop { start_reg_dest, .. } => {
            // Re-translate all the non-aggregate expressions into destination registers. We cannot use the same registers as emitted
            // in the earlier part of the main loop, because they would be overwritten by the next group before the group results
            // are processed.
            for (i, expr) in t_ctx
                .non_aggregate_expressions
                .iter()
                .filter_map(|(expr, in_result)| if *in_result { Some(expr) } else { None })
                .enumerate()
            {
                let dest_reg = start_reg_dest + i;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    dest_reg,
                    &t_ctx.resolver,
                )?;
                t_ctx.resolver.expr_to_reg_cache.push((expr, dest_reg));
            }
        }
    }

    // Mark that we've stored data for this group
    program.resolve_label(labels.label_acc_indicator_set_flag_true, program.offset());
    program.add_comment(program.offset(), "indicate data in accumulator");
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: registers.reg_data_in_acc_flag,
    });

    Ok(())
}

/// Emits the bytecode for processing the aggregation phase of a GROUP BY clause.
/// This is called either when:
/// 1. the main query execution loop has finished processing,
///    and we now have data in the GROUP BY sorter.
/// 2. the rows are already sorted in the order that the GROUP BY keys are defined,
///    and we can start aggregating inside the main loop.
pub fn group_by_agg_phase(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let GroupByMetadata {
        labels, row_source, ..
    } = t_ctx.meta_group_by.as_mut().unwrap();
    let group_by = plan.group_by.as_ref().unwrap();

    let label_sort_loop_start = labels.label_sort_loop_start;
    let label_sort_loop_end = labels.label_sort_loop_end;

    if matches!(row_source, GroupByRowSource::Sorter { .. }) {
        emit_group_by_sort_loop_start(program, row_source, label_sort_loop_end)?;
    }

    group_by_process_single_group(program, group_by, plan, t_ctx)?;

    let row_source = &t_ctx.meta_group_by.as_ref().unwrap().row_source;

    // Continue to the next row in the sorter
    if let GroupByRowSource::Sorter { sort_cursor, .. } = row_source {
        emit_group_by_sort_loop_end(
            program,
            *sort_cursor,
            label_sort_loop_start,
            label_sort_loop_end,
        );
    }
    Ok(())
}

pub fn group_by_emit_row_phase<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    let group_by = plan.group_by.as_ref().expect("group by not found");
    let GroupByMetadata {
        labels, registers, ..
    } = t_ctx
        .meta_group_by
        .as_ref()
        .expect("group by metadata not found");
    program.add_comment(program.offset(), "emit row for final group");
    program.emit_insn(Insn::Gosub {
        target_pc: labels.label_subrtn_acc_output,
        return_reg: registers.reg_subrtn_acc_output_return_offset,
    });

    program.add_comment(program.offset(), "group by finished");
    program.emit_insn(Insn::Goto {
        target_pc: labels.label_group_by_end,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: registers.reg_abort_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: registers.reg_subrtn_acc_output_return_offset,
        can_fallthrough: false,
    });

    program.resolve_label(labels.label_subrtn_acc_output, program.offset());

    // Only output a row if there's data in the accumulator
    program.add_comment(program.offset(), "output group by row subroutine start");
    program.emit_insn(Insn::IfPos {
        reg: registers.reg_data_in_acc_flag,
        target_pc: labels.label_agg_final,
        decrement_by: 0,
    });

    // If no data, return without outputting a row
    program.resolve_label(
        labels.label_group_by_end_without_emitting_row,
        program.offset(),
    );
    // SELECT DISTINCT also jumps here if there is a duplicate.
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.resolve_label(distinct_ctx.label_on_conflict, program.offset());
    }
    program.emit_insn(Insn::Return {
        return_reg: registers.reg_subrtn_acc_output_return_offset,
        can_fallthrough: false,
    });

    // Resolve the label for the start of the group by output row subroutine
    program.resolve_label(labels.label_agg_final, program.offset());
    // Finalize aggregate values for output
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_start_reg = t_ctx
            .reg_agg_start
            .expect("aggregate registers must be initialized");
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((&agg.original_expr, agg_result_reg));
    }

    t_ctx.resolver.enable_expr_to_reg_cache();

    if let Some(having) = &group_by.having {
        for expr in having.iter() {
            let if_true_target = program.allocate_label();
            translate_condition_expr(
                program,
                &plan.table_references,
                expr,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_false: labels.label_group_by_end_without_emitting_row,
                    jump_target_when_true: if_true_target,
                },
                &t_ctx.resolver,
            )?;
            program.preassign_label_to_next_insn(if_true_target);
        }
    }

    match plan.order_by.is_empty() {
        true => {
            emit_select_result(
                program,
                &t_ctx.resolver,
                plan,
                Some(labels.label_group_by_end),
                Some(labels.label_group_by_end_without_emitting_row),
                t_ctx.reg_nonagg_emit_once_flag,
                t_ctx.reg_offset,
                t_ctx.reg_result_cols_start.unwrap(),
                t_ctx.limit_ctx,
            )?;
        }
        false => {
            order_by_sorter_insert(program, t_ctx, plan)?;
        }
    }

    program.emit_insn(Insn::Return {
        return_reg: registers.reg_subrtn_acc_output_return_offset,
        can_fallthrough: false,
    });

    // Subroutine to clear accumulators for a new group
    program.add_comment(program.offset(), "clear accumulator subroutine start");
    program.resolve_label(labels.label_subrtn_acc_clear, program.offset());
    let start_reg = registers.reg_non_aggregate_exprs_acc;

    // Reset all accumulator registers to NULL
    program.emit_insn(Insn::Null {
        dest: start_reg,
        dest_end: Some(
            start_reg + t_ctx.non_aggregate_expressions.len() + plan.aggregates.len() - 1,
        ),
    });

    // Reopen ephemeral indexes for distinct aggregates (effectively clearing them).
    plan.aggregates
        .iter()
        .filter_map(|agg| {
            if let Distinctness::Distinct { ctx } = &agg.distinctness {
                Some(ctx)
            } else {
                None
            }
        })
        .for_each(|ctx| {
            let ctx = ctx
                .as_ref()
                .expect("distinct aggregate context not populated");
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: ctx.cursor_id,
                is_table: false,
            });
        });

    program.emit_insn(Insn::Integer {
        value: 0,
        dest: registers.reg_data_in_acc_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: registers.reg_subrtn_acc_clear_return_offset,
        can_fallthrough: false,
    });
    program.preassign_label_to_next_insn(labels.label_group_by_end);
    Ok(())
}
