use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset},
    Result,
};

use super::{
    emitter::{LimitCtx, Resolver},
    expr::translate_expr,
    plan::{Distinctness, SelectPlan, SelectQueryType},
};

/// Emits the bytecode for:
/// - all result columns
/// - result row (or if a subquery, yields to the parent query)
/// - limit
pub fn emit_select_result(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &SelectPlan,
    label_on_limit_reached: Option<BranchOffset>,
    offset_jump_to: Option<BranchOffset>,
    reg_nonagg_emit_once_flag: Option<usize>,
    reg_offset: Option<usize>,
    reg_result_cols_start: usize,
    limit_ctx: Option<LimitCtx>,
    reg_limit_offset_sum: Option<usize>,
) -> Result<()> {
    if let (Some(jump_to), Some(_)) = (offset_jump_to, label_on_limit_reached) {
        emit_offset(program, plan, jump_to, reg_offset)?;
    }

    let start_reg = reg_result_cols_start;
    for (i, rc) in plan.result_columns.iter().enumerate().filter(|(_, rc)| {
        // For aggregate queries, we handle columns differently; example: select id, first_name, sum(age) from users limit 1;
        // 1. Columns with aggregates (e.g., sum(age)) are computed in each iteration of aggregation
        // 2. Non-aggregate columns (e.g., id, first_name) are only computed once in the first iteration
        // This filter ensures we only emit expressions for non aggregate columns once,
        // preserving previously calculated values while updating aggregate results
        // For all other queries where reg_nonagg_emit_once_flag is none we do nothing.
        reg_nonagg_emit_once_flag.is_some() && rc.contains_aggregates
            || reg_nonagg_emit_once_flag.is_none()
    }) {
        let reg = start_reg + i;
        translate_expr(
            program,
            Some(&plan.table_references),
            &rc.expr,
            reg,
            resolver,
        )?;
    }

    // Handle SELECT DISTINCT deduplication
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        let num_regs = plan.result_columns.len();
        distinct_ctx.emit_deduplication_insns(program, num_regs, start_reg);
    }

    emit_result_row_and_limit(
        program,
        plan,
        start_reg,
        limit_ctx,
        reg_offset,
        reg_limit_offset_sum,
        label_on_limit_reached,
    )?;
    Ok(())
}

/// Emits the bytecode for:
/// - result row (or if a subquery, yields to the parent query)
/// - limit
pub fn emit_result_row_and_limit(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    result_columns_start_reg: usize,
    limit_ctx: Option<LimitCtx>,
    reg_offset: Option<usize>,
    reg_limit_offset_sum: Option<usize>,
    label_on_limit_reached: Option<BranchOffset>,
) -> Result<()> {
    match &plan.query_type {
        SelectQueryType::TopLevel => {
            program.emit_insn(Insn::ResultRow {
                start_reg: result_columns_start_reg,
                count: plan.result_columns.len(),
            });
        }
        SelectQueryType::Subquery { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: BranchOffset::Offset(0),
            });
        }
    }

    if let Some(limit) = plan.limit {
        if label_on_limit_reached.is_none() {
            // There are cases where LIMIT is ignored, e.g. aggregation without a GROUP BY clause.
            // We already early return on LIMIT 0, so we can just return here since the n of rows
            // is always 1 here.
            return Ok(());
        }
        let limit_ctx = limit_ctx.expect("limit_ctx must be Some if plan.limit is Some");
        if limit_ctx.initialize_counter {
            program.emit_insn(Insn::Integer {
                value: limit as i64,
                dest: limit_ctx.reg_limit,
            });
            program.mark_last_insn_constant();
        }

        if let Some(offset) = plan.offset {
            program.emit_insn(Insn::Integer {
                value: offset as i64,
                dest: reg_offset.expect("reg_offset must be Some"),
            });
            program.mark_last_insn_constant();

            program.emit_insn(Insn::OffsetLimit {
                limit_reg: limit_ctx.reg_limit,
                combined_reg: reg_limit_offset_sum.expect("reg_limit_offset_sum must be Some"),
                offset_reg: reg_offset.expect("reg_offset must be Some"),
            });
            program.mark_last_insn_constant();
        }

        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_on_limit_reached.unwrap(),
        });
    }
    Ok(())
}

pub fn emit_offset(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    jump_to: BranchOffset,
    reg_offset: Option<usize>,
) -> Result<()> {
    match plan.offset {
        Some(offset) if offset > 0 => {
            program.add_comment(program.offset(), "OFFSET");
            program.emit_insn(Insn::IfPos {
                reg: reg_offset.expect("reg_offset must be Some"),
                target_pc: jump_to,
                decrement_by: 1,
            });
        }
        _ => {}
    }
    Ok(())
}
