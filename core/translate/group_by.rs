use std::rc::Rc;

use limbo_sqlite3_parser::ast;

use crate::{
    function::AggFunc,
    schema::{Column, PseudoTable},
    types::{OwnedValue, Record},
    util::exprs_are_equivalent,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
        BranchOffset,
    },
    Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    order_by::order_by_sorter_insert,
    plan::{Aggregate, GroupBy, SelectPlan, TableReference},
    result_row::emit_select_result,
};

// Metadata for handling GROUP BY operations
#[derive(Debug)]
pub struct GroupByMetadata {
    // Cursor ID for the Sorter table where the grouped rows are stored
    pub sort_cursor: usize,
    // Label for the subroutine that clears the accumulator registers (temporary storage for per-group aggregate calculations)
    pub label_subrtn_acc_clear: BranchOffset,
    // Label for the instruction that sets the accumulator indicator to true (indicating data exists in the accumulator for the current group)
    pub label_acc_indicator_set_flag_true: BranchOffset,
    // Register holding the return offset for the accumulator clear subroutine
    pub reg_subrtn_acc_clear_return_offset: usize,
    // Register holding the key used for sorting in the Sorter
    pub reg_sorter_key: usize,
    // Register holding a flag to abort the grouping process if necessary
    pub reg_abort_flag: usize,
    // Register holding the start of the non aggregate query members (all columns except aggregate arguments)
    pub reg_non_aggregate_exprs_acc: usize,
    // Starting index of the register(s) that hold the comparison result between the current row and the previous row
    // The comparison result is used to determine if the current row belongs to the same group as the previous row
    // Each group by expression has a corresponding register
    pub reg_group_exprs_cmp: usize,
    // Columns that not part of GROUP BY clause and not arguments of Aggregation function.
    // Heavy calculation and needed in different functions, so it is reasonable to do it once and save.
    pub non_group_by_non_agg_column_count: Option<usize>,
}

/// Initialize resources needed for GROUP BY processing
pub fn init_group_by(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    group_by: &GroupBy,
    plan: &SelectPlan,
) -> Result<()> {
    let num_aggs = plan.aggregates.len();

    let non_aggregate_count = plan
        .result_columns
        .iter()
        .filter(|rc| !rc.contains_aggregates)
        .count();

    let sort_cursor = program.alloc_cursor_id(None, CursorType::Sorter);

    let reg_abort_flag = program.alloc_register();
    let reg_group_exprs_cmp = program.alloc_registers(group_by.exprs.len());
    let reg_non_aggregate_exprs_acc = program.alloc_registers(non_aggregate_count);
    let reg_agg_exprs_start = program.alloc_registers(num_aggs);
    let reg_sorter_key = program.alloc_register();

    let label_subrtn_acc_clear = program.allocate_label();

    let mut order = Vec::new();
    const ASCENDING: i64 = 0;
    for _ in group_by.exprs.iter() {
        order.push(OwnedValue::Integer(ASCENDING));
    }
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: non_aggregate_count + plan.aggregates.len(),
        order: Record::new(order),
    });

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

    t_ctx.reg_agg_start = Some(reg_agg_exprs_start);

    t_ctx.meta_group_by = Some(GroupByMetadata {
        sort_cursor,
        label_subrtn_acc_clear,
        label_acc_indicator_set_flag_true: program.allocate_label(),
        reg_subrtn_acc_clear_return_offset,
        reg_abort_flag,
        reg_non_aggregate_exprs_acc,
        reg_group_exprs_cmp,
        reg_sorter_key,
        non_group_by_non_agg_column_count: None,
    });
    Ok(())
}

/// Emits the bytecode for processing a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we now have data in the GROUP BY sorter.
pub fn emit_group_by<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    // Label for the first instruction of the grouping loop.
    // This is the start of the loop that reads the sorted data and groups&aggregates it.
    let label_grouping_loop_start = program.allocate_label();
    // Label for the instruction immediately after the grouping loop.
    let label_grouping_loop_end = program.allocate_label();
    // Label for the instruction where a row for a finished group is output.
    // Distinct from subroutine_accumulator_output_label, which is the start of the subroutine, but may still skip emitting a row.
    let label_agg_final = program.allocate_label();
    // Label for the instruction immediately after the entire group by phase.
    let label_group_by_end = program.allocate_label();
    // Label for the beginning of the subroutine that potentially outputs a row for a finished group.
    let label_subrtn_acc_output = program.allocate_label();
    // Register holding the return offset of the subroutine that potentially outputs a row for a finished group.
    let reg_subrtn_acc_output_return_offset = program.alloc_register();
    // Register holding a boolean indicating whether there's data in the accumulator (used for aggregation)
    let reg_data_in_acc_flag = program.alloc_register();

    let GroupByMetadata {
        sort_cursor,
        reg_group_exprs_cmp,
        reg_subrtn_acc_clear_return_offset,
        reg_non_aggregate_exprs_acc,
        reg_abort_flag,
        reg_sorter_key,
        label_subrtn_acc_clear,
        label_acc_indicator_set_flag_true,
        non_group_by_non_agg_column_count,
        ..
    } = *t_ctx.meta_group_by.as_mut().unwrap();
    let group_by = plan.group_by.as_ref().unwrap();

    let agg_args_count = plan
        .aggregates
        .iter()
        .map(|agg| agg.args.len())
        .sum::<usize>();
    let group_by_count = group_by.exprs.len();
    let non_group_by_non_agg_column_count = non_group_by_non_agg_column_count.unwrap();

    // We have to know which group by expr present in resulting set
    let group_by_expr_in_res_cols = group_by.exprs.iter().map(|expr| {
        plan.result_columns
            .iter()
            .any(|e| exprs_are_equivalent(&e.expr, expr))
    });

    // Create a map from sorter column index to result register
    // This helps track where each column from the sorter should be stored
    let mut column_register_mapping =
        vec![None; group_by_count + non_group_by_non_agg_column_count];
    let mut next_reg = reg_non_aggregate_exprs_acc;

    // Map GROUP BY columns that are in the result set to registers
    for (i, is_in_result) in group_by_expr_in_res_cols.clone().enumerate() {
        if is_in_result {
            column_register_mapping[i] = Some(next_reg);
            next_reg += 1;
        }
    }

    // Handle other non-aggregate columns that aren't part of GROUP BY and not part of Aggregation function
    for i in group_by_count..group_by_count + non_group_by_non_agg_column_count {
        column_register_mapping[i] = Some(next_reg);
        next_reg += 1;
    }

    // Calculate total number of columns in the sorter
    // The sorter contains all GROUP BY columns, aggregate arguments, and other columns
    let sorter_column_count = agg_args_count + group_by_count + non_group_by_non_agg_column_count;

    // Create pseudo-columns for the pseudo-table
    // (these are placeholders as we only care about structure, not semantics)
    let ty = crate::schema::Type::Null;
    let pseudo_columns = (0..sorter_column_count)
        .map(|_| Column {
            name: None,
            primary_key: false,
            ty,
            ty_str: ty.to_string().to_uppercase(),
            is_rowid_alias: false,
            notnull: false,
            default: None,
        })
        .collect::<Vec<_>>();

    // Create a pseudo-table to read one row at a time from the sorter
    // This allows us to use standard table access operations on the sorted data
    let pseudo_table = Rc::new(PseudoTable {
        columns: pseudo_columns,
    });

    let pseudo_cursor = program.alloc_cursor_id(None, CursorType::Pseudo(pseudo_table.clone()));

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: reg_sorter_key,
        num_fields: sorter_column_count,
    });

    // Sort the sorter based on the group by columns
    program.emit_insn(Insn::SorterSort {
        cursor_id: sort_cursor,
        pc_if_empty: label_grouping_loop_end,
    });

    program.resolve_label(label_grouping_loop_start, program.offset());
    // Read a row from the sorted data in the sorter into the pseudo cursor
    program.emit_insn(Insn::SorterData {
        cursor_id: sort_cursor,
        dest_reg: reg_sorter_key,
        pseudo_cursor,
    });

    // Read the group by columns from the pseudo cursor
    let groups_start_reg = program.alloc_registers(group_by.exprs.len());
    for i in 0..group_by.exprs.len() {
        let sorter_column_index = i;
        let group_reg = groups_start_reg + i;
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: sorter_column_index,
            dest: group_reg,
        });
    }

    // Compare the group by columns to the previous group by columns to see if we are at a new group or not
    program.emit_insn(Insn::Compare {
        start_reg_a: reg_group_exprs_cmp,
        start_reg_b: groups_start_reg,
        count: group_by.exprs.len(),
    });

    let agg_step_label = program.allocate_label();

    program.add_comment(
        program.offset(),
        "start new group if comparison is not equal",
    );
    // If we are at a new group, continue. If we are at the same group, jump to the aggregation step (i.e. accumulate more values into the aggregations)
    program.emit_insn(Insn::Jump {
        target_pc_lt: program.offset().add(1u32),
        target_pc_eq: agg_step_label,
        target_pc_gt: program.offset().add(1u32),
    });

    program.add_comment(
        program.offset(),
        "check if ended group had data, and output if so",
    );
    program.emit_insn(Insn::Gosub {
        target_pc: label_subrtn_acc_output,
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    // New group, move current group by columns into the comparison register
    program.emit_insn(Insn::Move {
        source_reg: groups_start_reg,
        dest_reg: reg_group_exprs_cmp,
        count: group_by.exprs.len(),
    });

    program.add_comment(program.offset(), "check abort flag");
    program.emit_insn(Insn::IfPos {
        reg: reg_abort_flag,
        target_pc: label_group_by_end,
        decrement_by: 0,
    });

    program.add_comment(program.offset(), "goto clear accumulator subroutine");
    program.emit_insn(Insn::Gosub {
        target_pc: label_subrtn_acc_clear,
        return_reg: reg_subrtn_acc_clear_return_offset,
    });

    // Process each aggregate function for the current row
    program.resolve_label(agg_step_label, program.offset());
    let start_reg = t_ctx.reg_agg_start.unwrap();
    let mut cursor_index = group_by_count + non_group_by_non_agg_column_count; // Skipping all columns in sorter that not an aggregation arguments
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_result_reg = start_reg + i;
        translate_aggregation_step_groupby(
            program,
            &plan.table_references,
            pseudo_cursor,
            cursor_index,
            agg,
            agg_result_reg,
            &t_ctx.resolver,
        )?;
        cursor_index += agg.args.len();
    }

    // We only need to store non-aggregate columns once per group
    // Skip if we've already stored them for this group
    program.add_comment(
        program.offset(),
        "don't emit group columns if continuing existing group",
    );
    program.emit_insn(Insn::If {
        target_pc: label_acc_indicator_set_flag_true,
        reg: reg_data_in_acc_flag,
        jump_if_null: false,
    });

    // Read non-aggregate columns from the current row
    for (sorter_column_index, dest_reg) in column_register_mapping.iter().enumerate() {
        if let Some(dest_reg) = dest_reg {
            program.emit_insn(Insn::Column {
                cursor_id: pseudo_cursor,
                column: sorter_column_index,
                dest: *dest_reg,
            });
        }
    }

    // Mark that we've stored data for this group
    program.resolve_label(label_acc_indicator_set_flag_true, program.offset());
    program.add_comment(program.offset(), "indicate data in accumulator");
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_data_in_acc_flag,
    });

    // Continue to the next row in the sorter
    program.emit_insn(Insn::SorterNext {
        cursor_id: sort_cursor,
        pc_if_next: label_grouping_loop_start,
    });

    program.resolve_label(label_grouping_loop_end, program.offset());

    program.add_comment(program.offset(), "emit row for final group");
    program.emit_insn(Insn::Gosub {
        target_pc: label_subrtn_acc_output,
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    program.add_comment(program.offset(), "group by finished");
    program.emit_insn(Insn::Goto {
        target_pc: label_group_by_end,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_abort_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    program.resolve_label(label_subrtn_acc_output, program.offset());

    // Only output a row if there's data in the accumulator
    program.add_comment(program.offset(), "output group by row subroutine start");
    program.emit_insn(Insn::IfPos {
        reg: reg_data_in_acc_flag,
        target_pc: label_agg_final,
        decrement_by: 0,
    });

    // If no data, return without outputting a row
    let group_by_end_without_emitting_row_label = program.allocate_label();
    program.resolve_label(group_by_end_without_emitting_row_label, program.offset());
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    // Finalize aggregate values for output
    let agg_start_reg = t_ctx.reg_agg_start.unwrap();
    // Resolve the label for the start of the group by output row subroutine
    program.resolve_label(label_agg_final, program.offset());
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }

    // Map GROUP BY expressions to their registers in the result set
    for (i, (expr, is_in_result)) in group_by
        .exprs
        .iter()
        .zip(group_by_expr_in_res_cols)
        .enumerate()
    {
        if is_in_result {
            if let Some(reg) = &column_register_mapping.get(i).and_then(|opt| *opt) {
                t_ctx.resolver.expr_to_reg_cache.push((expr, *reg));
            }
        }
    }

    // Map non-aggregate, non-GROUP BY columns to their registers
    let non_agg_cols = plan
        .result_columns
        .iter()
        .filter(|rc| !rc.contains_aggregates && !is_column_in_group_by(&rc.expr, &group_by.exprs));

    for (idx, rc) in non_agg_cols.enumerate() {
        let sorter_idx = group_by_count + idx;
        if let Some(reg) = column_register_mapping.get(sorter_idx).and_then(|opt| *opt) {
            t_ctx.resolver.expr_to_reg_cache.push((&rc.expr, reg));
        }
    }

    // Map aggregate expressions to their result registers
    for (i, agg) in plan.aggregates.iter().enumerate() {
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((&agg.original_expr, agg_start_reg + i));
    }

    if let Some(having) = &group_by.having {
        for expr in having.iter() {
            translate_condition_expr(
                program,
                &plan.table_references,
                expr,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_false: group_by_end_without_emitting_row_label,
                    jump_target_when_true: BranchOffset::Placeholder, // not used. FIXME: this is a bug. HAVING can have e.g. HAVING a OR b.
                },
                &t_ctx.resolver,
            )?;
        }
    }

    match &plan.order_by {
        None => {
            emit_select_result(
                program,
                t_ctx,
                plan,
                Some(label_group_by_end),
                Some(group_by_end_without_emitting_row_label),
            )?;
        }
        Some(_) => {
            order_by_sorter_insert(program, t_ctx, plan)?;
        }
    }

    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    // Subroutine to clear accumulators for a new group
    program.add_comment(program.offset(), "clear accumulator subroutine start");
    program.resolve_label(label_subrtn_acc_clear, program.offset());
    let start_reg = reg_non_aggregate_exprs_acc;

    // Reset all accumulator registers to NULL
    program.emit_insn(Insn::Null {
        dest: start_reg,
        dest_end: Some(
            start_reg + non_group_by_non_agg_column_count + group_by_count + plan.aggregates.len()
                - 1,
        ),
    });

    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_data_in_acc_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_clear_return_offset,
    });

    program.resolve_label(label_group_by_end, program.offset());

    Ok(())
}

/// Emits the bytecode for processing an aggregate step within a GROUP BY clause.
/// Eg. in `SELECT product_category, SUM(price) FROM t GROUP BY line_item`, 'price' is evaluated for every row
/// where the 'product_category' is the same, and the result is added to the accumulator for that category.
///
/// This is distinct from the final step, which is called after a single group has been entirely accumulated,
/// and the actual result value of the aggregation is materialized.
pub fn translate_aggregation_step_groupby(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    group_by_sorter_cursor_id: usize,
    cursor_index: usize,
    agg: &Aggregate,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let emit_column = |program: &mut ProgramBuilder, expr_reg: usize| {
        program.emit_insn(Insn::Column {
            cursor_id: group_by_sorter_cursor_id,
            column: cursor_index,
            dest: expr_reg,
        });
    };
    let dest = match agg.func {
        AggFunc::Avg => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count | AggFunc::Count0 => {
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: if matches!(agg.func, AggFunc::Count0) {
                    AggFunc::Count0
                } else {
                    AggFunc::Count
                },
            });
            target_register
        }
        AggFunc::GroupConcat => {
            if agg.args.len() != 1 && agg.args.len() != 2 {
                crate::bail_parse_error!("group_concat bad number of arguments");
            }

            let expr_reg = program.alloc_register();
            let delimiter_reg = program.alloc_register();

            let delimiter_expr: ast::Expr;

            if agg.args.len() == 2 {
                match &agg.args[1] {
                    ast::Expr::Column { .. } => {
                        delimiter_expr = agg.args[1].clone();
                    }
                    ast::Expr::Literal(ast::Literal::String(s)) => {
                        delimiter_expr = ast::Expr::Literal(ast::Literal::String(s.to_string()));
                    }
                    _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
                };
            } else {
                delimiter_expr = ast::Expr::Literal(ast::Literal::String(String::from("\",\"")));
            }

            emit_column(program, expr_reg);
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                resolver,
            )?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::GroupConcat,
            });

            target_register
        }
        AggFunc::Max => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Max,
            });
            target_register
        }
        AggFunc::Min => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Min,
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::JsonGroupArray,
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            if agg.args.len() != 2 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let value_expr = &agg.args[1];
            let value_reg = program.alloc_register();

            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                value_expr,
                value_reg,
                resolver,
            )?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: value_reg,
                func: AggFunc::JsonGroupObject,
            });
            target_register
        }
        AggFunc::StringAgg => {
            if agg.args.len() != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = program.alloc_register();
            let delimiter_reg = program.alloc_register();

            let delimiter_expr = match &agg.args[1] {
                ast::Expr::Column { .. } => agg.args[1].clone(),
                ast::Expr::Literal(ast::Literal::String(s)) => {
                    ast::Expr::Literal(ast::Literal::String(s.to_string()))
                }
                _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
            };

            emit_column(program, expr_reg);
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                resolver,
            )?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::StringAgg,
            });

            target_register
        }
        AggFunc::Sum => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("sum bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Sum,
            });
            target_register
        }
        AggFunc::Total => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("total bad number of arguments");
            }
            let expr_reg = program.alloc_register();
            emit_column(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Total,
            });
            target_register
        }
        AggFunc::External(_) => {
            todo!("External aggregate functions are not yet supported in GROUP BY");
        }
    };
    Ok(dest)
}

pub fn is_column_in_group_by(expr: &ast::Expr, group_by_exprs: &[ast::Expr]) -> bool {
    group_by_exprs
        .iter()
        .any(|expr2| exprs_are_equivalent(expr, expr2))
}
