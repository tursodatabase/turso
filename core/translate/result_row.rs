use turso_parser::ast::{Expr, Literal, Name, Operator, UnaryOperator};

use crate::{
    vdbe::{
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, InsertFlags, Insn},
        BranchOffset,
    },
    Result,
};

use super::{
    emitter::{LimitCtx, Resolver},
    expr::translate_expr,
    plan::{Distinctness, QueryDestination, SelectPlan},
};

/// Emits the bytecode for:
/// - all result columns
/// - result row (or if a subquery, yields to the parent query)
/// - limit
#[allow(clippy::too_many_arguments)]
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
) -> Result<()> {
    if let (Some(jump_to), Some(_)) = (offset_jump_to, label_on_limit_reached) {
        emit_offset(program, plan, jump_to, reg_offset, resolver);
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

    emit_result_row_and_limit(program, plan, start_reg, limit_ctx, label_on_limit_reached)?;
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
    label_on_limit_reached: Option<BranchOffset>,
) -> Result<()> {
    match &plan.query_destination {
        QueryDestination::ResultRows => {
            program.emit_insn(Insn::ResultRow {
                start_reg: result_columns_start_reg,
                count: plan.result_columns.len(),
            });
        }
        QueryDestination::EphemeralIndex {
            cursor_id: index_cursor_id,
            index: dedupe_index,
            is_delete,
        } => {
            if *is_delete {
                program.emit_insn(Insn::IdxDelete {
                    start_reg: result_columns_start_reg,
                    num_regs: plan.result_columns.len(),
                    cursor_id: *index_cursor_id,
                    raise_error_if_no_matching_entry: false,
                });
            } else {
                let record_reg = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: result_columns_start_reg,
                    count: plan.result_columns.len(),
                    dest_reg: record_reg,
                    index_name: Some(dedupe_index.name.clone()),
                    affinity_str: None,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: *index_cursor_id,
                    record_reg,
                    unpacked_start: None,
                    unpacked_count: None,
                    flags: IdxInsertFlags::new().no_op_duplicate(),
                });
            }
        }
        QueryDestination::EphemeralTable {
            cursor_id: table_cursor_id,
            table,
        } => {
            let record_reg = program.alloc_register();
            if plan.result_columns.len() > 1 {
                program.emit_insn(Insn::MakeRecord {
                    start_reg: result_columns_start_reg,
                    count: plan.result_columns.len() - 1,
                    dest_reg: record_reg,
                    index_name: Some(table.name.clone()),
                    affinity_str: None,
                });
            }
            program.emit_insn(Insn::Insert {
                cursor: *table_cursor_id,
                key_reg: result_columns_start_reg + (plan.result_columns.len() - 1), // Rowid reg is the last register
                record_reg,
                // since we are not doing an Insn::NewRowid or an Insn::NotExists here, we need to seek to ensure the insertion happens in the correct place.
                flag: InsertFlags::new().require_seek(),
                table_name: table.name.clone(),
            });
        }
        QueryDestination::CoroutineYield { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: BranchOffset::Offset(0),
            });
        }
    }

    if plan.limit.is_some() {
        if label_on_limit_reached.is_none() {
            // There are cases where LIMIT is ignored, e.g. aggregation without a GROUP BY clause.
            // We already early return on LIMIT 0, so we can just return here since the n of rows
            // is always 1 here.
            return Ok(());
        }
        let limit_ctx = limit_ctx.expect("limit_ctx must be Some if plan.limit is Some");

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
    resolver: &Resolver,
) {
    let Some(offset_expr) = &plan.offset else {
        return;
    };

    if let Some(val) = try_fold_expr_to_i64(offset_expr) {
        if val > 0 {
            program.add_comment(program.offset(), "OFFSET const");
            program.emit_insn(Insn::IfPos {
                reg: reg_offset.expect("reg_offset must be Some"),
                target_pc: jump_to,
                decrement_by: 1,
            });
        }
        return;
    }

    let r = reg_offset.expect("reg_offset must be Some");

    program.add_comment(program.offset(), "OFFSET expr");

    _ = translate_expr(program, None, offset_expr, r, resolver);

    program.emit_insn(Insn::MustBeInt { reg: r });

    program.emit_insn(Insn::IfPos {
        reg: r,
        target_pc: jump_to,
        decrement_by: 1,
    });
}

#[allow(clippy::borrowed_box)]
pub fn try_fold_expr_to_i64(expr: &Box<Expr>) -> Option<i64> {
    match expr.as_ref() {
        Expr::Literal(Literal::Numeric(n)) => n.parse::<i64>().ok(),
        Expr::Literal(Literal::Null) => Some(0),
        Expr::Id(Name::Ident(s)) => {
            let lowered = s.to_ascii_lowercase();
            if lowered == "true" {
                Some(1)
            } else if lowered == "false" {
                Some(0)
            } else {
                None
            }
        }
        Expr::Unary(UnaryOperator::Negative, inner) => try_fold_expr_to_i64(inner).map(|v| -v),
        Expr::Unary(UnaryOperator::Positive, inner) => try_fold_expr_to_i64(inner),
        Expr::Binary(left, op, right) => {
            let l = try_fold_expr_to_i64(left)?;
            let r = try_fold_expr_to_i64(right)?;
            match op {
                Operator::Add => Some(l.saturating_add(r)),
                Operator::Subtract => Some(l.saturating_sub(r)),
                Operator::Multiply => Some(l.saturating_mul(r)),
                Operator::Divide if r != 0 => Some(l.saturating_div(r)),
                _ => None,
            }
        }

        _ => None,
    }
}
