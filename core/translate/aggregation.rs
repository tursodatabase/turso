use limbo_sqlite3_parser::ast;

use crate::{
    function::AggFunc,
    vdbe::{
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, Insn},
    },
    LimboError, Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::translate_expr,
    plan::{Aggregate, Distinctness, SelectPlan, TableReference},
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

    // This always emits a ResultRow because currently it can only be used for a single row result
    // Limit is None because we early exit on limit 0 and the max rows here is 1
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
        t_ctx.reg_limit_offset_sum,
    )?;

    Ok(())
}

/// Emits the bytecode for handling duplicates in a distinct aggregate.
/// This is used in both GROUP BY and non-GROUP BY aggregations to jump over
/// the AggStep that would otherwise accumulate the same value multiple times.
pub fn handle_distinct(program: &mut ProgramBuilder, agg: &Aggregate, agg_arg_reg: usize) {
    let Distinctness::Distinct { ctx } = &agg.distinctness else {
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
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: distinct_ctx.cursor_id,
        record_reg: record_reg,
        unpacked_start: None,
        unpacked_count: None,
        flags: IdxInsertFlags::new(),
    });
}

/// Emits the bytecode for processing an aggregate step.
/// E.g. in `SELECT SUM(price) FROM t`, 'price' is evaluated for every row, and the result is added to the accumulator.
///
/// This is distinct from the final step, which is called after the main loop has finished processing
/// and the actual result value of the aggregation is materialized.
pub fn translate_aggregation_step(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    agg: &Aggregate,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let dest = match agg.func {
        AggFunc::Avg => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count | AggFunc::Count0 => {
            let expr_reg = if agg.args.is_empty() {
                program.alloc_register()
            } else {
                let expr = &agg.args[0];
                let expr_reg = program.alloc_register();
                let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
                expr_reg
            };
            handle_distinct(program, agg, expr_reg);
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

            let expr = &agg.args[0];
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

            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
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
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
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
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
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
            if agg.args.len() != 2 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let value_expr = &agg.args[1];
            let value_reg = program.alloc_register();

            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
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
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::JsonGroupArray,
            });
            target_register
        }
        AggFunc::StringAgg => {
            if agg.args.len() != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = program.alloc_register();
            let delimiter_reg = program.alloc_register();

            let expr = &agg.args[0];
            let delimiter_expr = match &agg.args[1] {
                ast::Expr::Column { .. } => agg.args[1].clone(),
                ast::Expr::Literal(ast::Literal::String(s)) => {
                    ast::Expr::Literal(ast::Literal::String(s.to_string()))
                }
                _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
            };

            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
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
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
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
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            handle_distinct(program, agg, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Total,
            });
            target_register
        }
        AggFunc::External(ref func) => {
            let expr_reg = program.alloc_register();
            let argc = func.agg_args().map_err(|_| {
                LimboError::ExtensionError(
                    "External aggregate function called with wrong number of arguments".to_string(),
                )
            })?;
            if argc != agg.args.len() {
                crate::bail_parse_error!(
                    "External aggregate function called with wrong number of arguments"
                );
            }
            for i in 0..argc {
                if i != 0 {
                    let _ = program.alloc_register();
                }
                let _ = translate_expr(
                    program,
                    Some(referenced_tables),
                    &agg.args[i],
                    expr_reg + i,
                    resolver,
                )?;
                // invariant: distinct aggregates are only supported for single-argument functions
                if argc == 1 {
                    handle_distinct(program, agg, expr_reg + i);
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
