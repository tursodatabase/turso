use turso_parser::ast;

use crate::{
    function::AggFunc,
    schema::Table,
    translate::collate::CollationSeq,
    vdbe::{
        builder::ProgramBuilder,
        insn::{HashDistinctData, Insn},
    },
    LimboError, Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::{
        translate_condition_expr, translate_expr, translate_expr_no_constant_opt,
        ConditionMetadata, NoConstantOptReason,
    },
    plan::{Aggregate, Distinctness, SelectPlan, TableReferences},
    result_row::emit_select_result,
};

/// Emits the bytecode for processing an aggregate without a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we can now materialize the aggregate results.
pub fn emit_ungrouped_aggregation<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    let agg_start_reg = t_ctx.reg_agg_start.unwrap();

    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }
    // we now have the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    for (i, agg) in plan.aggregates.iter().enumerate() {
        t_ctx.resolver.expr_to_reg_cache.push((
            std::borrow::Cow::Borrowed(&agg.original_expr),
            agg_start_reg + i,
        ));
    }
    t_ctx.resolver.enable_expr_to_reg_cache();

    // Allocate a label for the end (used by both HAVING and OFFSET to skip row emission)
    let end_label = program.allocate_label();

    // Handle HAVING clause without GROUP BY for ungrouped aggregation
    if let Some(group_by) = &plan.group_by {
        if group_by.exprs.is_empty() {
            if let Some(having) = &group_by.having {
                for expr in having.iter() {
                    let if_true_target = program.allocate_label();
                    translate_condition_expr(
                        program,
                        &plan.table_references,
                        expr,
                        ConditionMetadata {
                            jump_if_condition_is_true: false,
                            jump_target_when_false: end_label,
                            jump_target_when_true: if_true_target,
                            // treat null result as false
                            jump_target_when_null: end_label,
                        },
                        &t_ctx.resolver,
                    )?;
                    program.preassign_label_to_next_insn(if_true_target);
                }
            }
        }
    }

    // Handle OFFSET for ungrouped aggregates
    // Since we only have one result row, either skip it (offset > 0) or emit it
    if let Some(offset_reg) = t_ctx.reg_offset {
        // If offset > 0, jump to end (skip the single row)
        program.emit_insn(Insn::IfPos {
            reg: offset_reg,
            target_pc: end_label,
            decrement_by: 0,
        });
    }

    // If the loop never ran (once-flag is still 0), we need to evaluate non-aggregate columns now.
    // This ensures literals return their values and column references return NULL (since cursor
    // is not on a valid row). The once-flag mechanism normally evaluates non-agg columns on first
    // iteration, but if there were no iterations, we must do it here.
    //
    // For direct table access, Column on an invalid cursor returns NULL naturally. But for
    // coroutine-based sources (CTEs, FROM-clause subqueries), the output registers still hold
    // the last yielded row's values. We null those registers first so that expressions
    // referencing coroutine columns evaluate to NULL instead of leaking stale values.
    if let Some(once_flag) = t_ctx.reg_nonagg_emit_once_flag {
        let skip_nonagg_eval = program.allocate_label();
        // If once-flag is non-zero (loop ran at least once), skip evaluation
        program.emit_insn(Insn::If {
            reg: once_flag,
            target_pc: skip_nonagg_eval,
            jump_if_null: false,
        });
        // Null out coroutine output registers so column references from CTEs/subqueries
        // don't leak stale values from the last yielded row.
        for table_ref in plan.table_references.joined_tables() {
            if let Table::FromClauseSubquery(subquery) = &table_ref.table {
                if let Some(start_reg) = subquery.result_columns_start_reg {
                    let num_cols = subquery.columns.len();
                    if num_cols > 0 {
                        program.emit_insn(Insn::Null {
                            dest: start_reg,
                            dest_end: if num_cols > 1 {
                                Some(start_reg + num_cols - 1)
                            } else {
                                None
                            },
                        });
                    }
                }
            }
        }
        // Evaluate non-aggregate columns now (with cursor in invalid state, columns return NULL)
        // Must use no_constant_opt to prevent constant hoisting which would place the label
        // after the hoisted constants, causing infinite loops in compound selects.
        let col_start = t_ctx.reg_result_cols_start.unwrap();
        for (i, rc) in plan.result_columns.iter().enumerate() {
            if !rc.contains_aggregates {
                translate_expr_no_constant_opt(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    col_start + i,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
            }
        }
        program.preassign_label_to_next_insn(skip_nonagg_eval);
    }

    // Emit the result row (if we didn't skip it due to HAVING or OFFSET)
    emit_select_result(
        program,
        &t_ctx.resolver,
        plan,
        None,
        None,
        t_ctx.reg_nonagg_emit_once_flag,
        None, // we've already handled offset
        t_ctx.reg_result_cols_start.unwrap(),
        t_ctx.limit_ctx,
    )?;

    // Resolve the SELECT DISTINCT label if present
    // When a duplicate is found by the Found instruction, jump here to skip emitting the row
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    program.resolve_label(end_label, program.offset());

    Ok(())
}

fn emit_collseq_if_needed(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
) {
    // Check if this is a column expression with explicit COLLATE clause
    if let ast::Expr::Collate(_, collation_name) = expr {
        if let Ok(collation) = CollationSeq::new(collation_name.as_str()) {
            program.emit_insn(Insn::CollSeq {
                reg: None,
                collation,
            });
        }
        return;
    }

    // If no explicit collation, check if this is a column with table-defined collation
    if let ast::Expr::Column { table, column, .. } = expr {
        if let Some((_, table_ref)) = referenced_tables.find_table_by_internal_id(*table) {
            if let Some(table_column) = table_ref.get_column_at(*column) {
                if let Some(c) = table_column.collation_opt() {
                    program.emit_insn(Insn::CollSeq {
                        reg: None,
                        collation: c,
                    });
                    return;
                }
            }
        }
    }

    // Always emit a CollSeq to reset to BINARY default, preventing collation
    // from a previous aggregate leaking into this one.
    program.emit_insn(Insn::CollSeq {
        reg: None,
        collation: CollationSeq::Binary,
    });
}

/// Emits the bytecode for handling duplicates in a distinct aggregate.
/// This is used in both GROUP BY and non-GROUP BY aggregations to jump over
/// the AggStep that would otherwise accumulate the same value multiple times.
pub fn handle_distinct(
    program: &mut ProgramBuilder,
    distinctness: &Distinctness,
    agg_arg_reg: usize,
) {
    let Distinctness::Distinct { ctx } = distinctness else {
        return;
    };
    let distinct_ctx = ctx
        .as_ref()
        .expect("distinct aggregate context not populated");
    let num_regs = 1;
    program.emit_insn(Insn::HashDistinct {
        data: Box::new(HashDistinctData {
            hash_table_id: distinct_ctx.hash_table_id,
            key_start_reg: agg_arg_reg,
            num_keys: num_regs,
            collations: distinct_ctx.collations.clone(),
            target_pc: distinct_ctx.label_on_conflict,
        }),
    });
}

/// Enum representing the source of the aggregate function arguments
///
/// Aggregate arguments can come from different sources, depending on how the aggregation
/// is evaluated:
/// * In the common grouped case, the aggregate function arguments are  first inserted
///   into a sorter in the main loop, and in the group by aggregation phase we read
///   the data from the sorter.
/// * In grouped cases where no sorting is required, arguments are retrieved  directly
///   from registers allocated in the main loop.
/// * In ungrouped cases, arguments are computed directly from the `args` expressions.
pub enum AggArgumentSource<'a> {
    /// The aggregate function arguments are retrieved from a pseudo cursor
    /// which reads from the GROUP BY sorter.
    PseudoCursor {
        cursor_id: usize,
        col_start: usize,
        dest_reg_start: usize,
        aggregate: &'a Aggregate,
    },
    /// The aggregate function arguments are retrieved from a contiguous block of registers
    /// allocated in the main loop for that given aggregate function.
    Register {
        src_reg_start: usize,
        aggregate: &'a Aggregate,
    },
    /// The aggregate function arguments are retrieved by evaluating expressions.
    Expression {
        func: &'a AggFunc,
        args: &'a Vec<ast::Expr>,
        distinctness: &'a Distinctness,
    },
}

impl<'a> AggArgumentSource<'a> {
    /// Create a new [AggArgumentSource] that retrieves the values from a GROUP BY sorter.
    pub fn new_from_cursor(
        program: &mut ProgramBuilder,
        cursor_id: usize,
        col_start: usize,
        aggregate: &'a Aggregate,
    ) -> Self {
        let dest_reg_start = program.alloc_registers(aggregate.args.len());
        Self::PseudoCursor {
            cursor_id,
            col_start,
            dest_reg_start,
            aggregate,
        }
    }
    /// Create a new [AggArgumentSource] that retrieves the values directly from an already
    /// populated register or registers.
    pub fn new_from_registers(src_reg_start: usize, aggregate: &'a Aggregate) -> Self {
        Self::Register {
            src_reg_start,
            aggregate,
        }
    }

    /// Create a new [AggArgumentSource] that retrieves the values by evaluating `args` expressions.
    pub fn new_from_expression(
        func: &'a AggFunc,
        args: &'a Vec<ast::Expr>,
        distinctness: &'a Distinctness,
    ) -> Self {
        Self::Expression {
            func,
            args,
            distinctness,
        }
    }

    pub fn distinctness(&self) -> &Distinctness {
        match self {
            AggArgumentSource::PseudoCursor { aggregate, .. } => &aggregate.distinctness,
            AggArgumentSource::Register { aggregate, .. } => &aggregate.distinctness,
            AggArgumentSource::Expression { distinctness, .. } => distinctness,
        }
    }

    pub fn agg_func(&self) -> &AggFunc {
        match self {
            AggArgumentSource::PseudoCursor { aggregate, .. } => &aggregate.func,
            AggArgumentSource::Register { aggregate, .. } => &aggregate.func,
            AggArgumentSource::Expression { func, .. } => func,
        }
    }
    pub fn arg_at(&self, idx: usize) -> &ast::Expr {
        match self {
            AggArgumentSource::PseudoCursor { aggregate, .. } => &aggregate.args[idx],
            AggArgumentSource::Register { aggregate, .. } => &aggregate.args[idx],
            AggArgumentSource::Expression { args, .. } => &args[idx],
        }
    }
    pub fn num_args(&self) -> usize {
        match self {
            AggArgumentSource::PseudoCursor { aggregate, .. } => aggregate.args.len(),
            AggArgumentSource::Register { aggregate, .. } => aggregate.args.len(),
            AggArgumentSource::Expression { args, .. } => args.len(),
        }
    }
    /// Read the value of an aggregate function argument
    pub fn translate(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: &TableReferences,
        resolver: &Resolver,
        arg_idx: usize,
    ) -> Result<usize> {
        match self {
            AggArgumentSource::PseudoCursor {
                cursor_id,
                col_start,
                dest_reg_start,
                ..
            } => {
                program.emit_column_or_rowid(
                    *cursor_id,
                    *col_start + arg_idx,
                    dest_reg_start + arg_idx,
                );
                Ok(dest_reg_start + arg_idx)
            }
            AggArgumentSource::Register {
                src_reg_start: start_reg,
                ..
            } => Ok(*start_reg + arg_idx),
            AggArgumentSource::Expression { args, .. } => {
                let dest_reg = program.alloc_register();
                translate_expr(
                    program,
                    Some(referenced_tables),
                    &args[arg_idx],
                    dest_reg,
                    resolver,
                )
            }
        }
    }
}

/// Emits the bytecode for processing an aggregate step.
///
/// This is distinct from the final step, which is called after a single group has been entirely accumulated,
/// and the actual result value of the aggregation is materialized.
///
/// Ungrouped aggregation is a special case of grouped aggregation that involves a single group.
///
/// Examples:
/// * In `SELECT SUM(price) FROM t`, `price` is evaluated for each row and added to the accumulator.
/// * In `SELECT product_category, SUM(price) FROM t GROUP BY product_category`, `price` is evaluated for
///   each row in the group and added to that group’s accumulator.
pub fn translate_aggregation_step(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    agg_arg_source: AggArgumentSource,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let num_args = agg_arg_source.num_args();
    let func = agg_arg_source.agg_func();
    let dest = match func {
        AggFunc::Avg => {
            if num_args != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count0 => {
            let expr = ast::Expr::Literal(ast::Literal::Numeric("1".to_string()));
            let expr_reg = translate_const_arg(program, referenced_tables, resolver, &expr)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Count0,
            });
            target_register
        }
        AggFunc::Count => {
            if num_args != 1 {
                crate::bail_parse_error!("count bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Count,
            });
            target_register
        }
        AggFunc::GroupConcat => {
            if num_args != 1 && num_args != 2 {
                crate::bail_parse_error!("group_concat bad number of arguments");
            }

            let delimiter_reg = if num_args == 2 {
                agg_arg_source.translate(program, referenced_tables, resolver, 1)?
            } else {
                let delimiter_expr =
                    ast::Expr::Literal(ast::Literal::String(String::from("\",\"")));
                translate_const_arg(program, referenced_tables, resolver, &delimiter_expr)?
            };

            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::GroupConcat,
            });

            target_register
        }
        AggFunc::Max => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            let expr = &agg_arg_source.arg_at(0);
            emit_collseq_if_needed(program, referenced_tables, expr);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Max,
            });
            target_register
        }
        AggFunc::Min => {
            if num_args != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            let expr = &agg_arg_source.arg_at(0);
            emit_collseq_if_needed(program, referenced_tables, expr);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Min,
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            if num_args != 2 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            let value_reg = agg_arg_source.translate(program, referenced_tables, resolver, 1)?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: value_reg,
                func: AggFunc::JsonGroupObject,
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::JsonGroupArray,
            });
            target_register
        }
        AggFunc::StringAgg => {
            if num_args != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            let delimiter_reg =
                agg_arg_source.translate(program, referenced_tables, resolver, 1)?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::StringAgg,
            });

            target_register
        }
        AggFunc::Sum => {
            if num_args != 1 {
                crate::bail_parse_error!("sum bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Sum,
            });
            target_register
        }
        AggFunc::Total => {
            if num_args != 1 {
                crate::bail_parse_error!("total bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            handle_distinct(program, agg_arg_source.distinctness(), expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Total,
            });
            target_register
        }
        AggFunc::External(ref func) => {
            let argc = func.agg_args().map_err(|_| {
                LimboError::ExtensionError(
                    "External aggregate function called with wrong number of arguments".to_string(),
                )
            })?;
            if argc != num_args {
                crate::bail_parse_error!(
                    "External aggregate function called with wrong number of arguments"
                );
            }
            let expr_reg = agg_arg_source.translate(program, referenced_tables, resolver, 0)?;
            for i in 0..argc {
                if i != 0 {
                    let _ = agg_arg_source.translate(program, referenced_tables, resolver, i)?;
                }
                // invariant: distinct aggregates are only supported for single-argument functions
                if argc == 1 {
                    handle_distinct(program, agg_arg_source.distinctness(), expr_reg + i);
                }
            }
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::External(func.clone()),
            });
            target_register
        }
    };
    Ok(dest)
}

fn translate_const_arg(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    resolver: &Resolver,
    expr: &ast::Expr,
) -> Result<usize> {
    let target_register = program.alloc_register();
    translate_expr(
        program,
        Some(referenced_tables),
        expr,
        target_register,
        resolver,
    )
}
