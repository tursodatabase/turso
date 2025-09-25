use turso_parser::ast;

use crate::{
    function::AggFunc,
    translate::collate::CollationSeq,
    vdbe::{
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, Insn},
    },
    LimboError, Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::translate_expr,
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
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((&agg.original_expr, agg_start_reg + i));
    }
    t_ctx.resolver.enable_expr_to_reg_cache();

    // Handle OFFSET for ungrouped aggregates
    // Since we only have one result row, either skip it (offset > 0) or emit it
    if let Some(offset_reg) = t_ctx.reg_offset {
        let done_label = program.allocate_label();

        // If offset > 0, jump to end (skip the single row)
        program.emit_insn(Insn::IfPos {
            reg: offset_reg,
            target_pc: done_label,
            decrement_by: 0,
        });

        // Offset is 0, fall through to emit the row
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

        program.resolve_label(done_label, program.offset());
    } else {
        // No offset specified, just emit the row
        emit_select_result(
            program,
            &t_ctx.resolver,
            plan,
            None,
            None,
            t_ctx.reg_nonagg_emit_once_flag,
            t_ctx.reg_offset,
            t_ctx.reg_result_cols_start.unwrap(),
            t_ctx.limit_ctx,
        )?;
    }

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
        if let Some(table_ref) = referenced_tables.find_table_by_internal_id(*table) {
            if let Some(table_column) = table_ref.get_column_at(*column) {
                if let Some(collation) = &table_column.collation {
                    program.emit_insn(Insn::CollSeq {
                        reg: None,
                        collation: *collation,
                    });
                }
            }
        }
    }
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
    program.emit_insn(Insn::Found {
        cursor_id: distinct_ctx.cursor_id,
        target_pc: distinct_ctx.label_on_conflict,
        record_reg: agg_arg_reg,
        num_regs,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: agg_arg_reg,
        count: num_regs,
        dest_reg: record_reg,
        index_name: Some(distinct_ctx.ephemeral_index_name.to_string()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: distinct_ctx.cursor_id,
        record_reg,
        unpacked_start: None,
        unpacked_count: None,
        flags: IdxInsertFlags::new(),
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
