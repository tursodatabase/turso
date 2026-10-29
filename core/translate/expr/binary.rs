use super::*;
use std::borrow::Cow;

#[allow(clippy::too_many_arguments)]
pub(super) fn binary_expr_shared(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    target_register: usize,
    resolver: &Resolver,
    emit_mode: BinaryEmitMode,
) -> Result<usize> {
    let lhs_arity = expr_vector_size(e1)?;
    let rhs_arity = expr_vector_size(e2)?;
    if lhs_arity != rhs_arity {
        crate::bail_parse_error!("row value misused");
    }

    if lhs_arity == 1 {
        emit_binary_expr_scalar(
            program,
            referenced_tables,
            e1,
            e2,
            op,
            target_register,
            resolver,
            emit_mode,
        )?;
        return Ok(target_register);
    }

    emit_row_valued_binary_expr(
        program,
        referenced_tables,
        e1,
        e2,
        op,
        lhs_arity,
        target_register,
        resolver,
        emit_mode,
    )
}

/// Row-valued comparisons are rare, so they get their own function that is
/// never inlined, to keep their stack slots out of the frame of
/// [binary_expr_shared()], which is on the stack at every level of a chain
/// like `a + b + c`.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn emit_row_valued_binary_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    lhs_arity: usize,
    target_register: usize,
    resolver: &Resolver,
    emit_mode: BinaryEmitMode,
) -> Result<usize> {
    if !supports_row_value_binary_comparison(op) {
        crate::bail_parse_error!("row value misused");
    }

    let lhs_reg = program.alloc_registers(lhs_arity);
    let rhs_reg = program.alloc_registers(lhs_arity);
    translate_expr(program, referenced_tables, e1, lhs_reg, resolver)?;
    translate_expr(program, referenced_tables, e2, rhs_reg, resolver)?;

    emit_binary_expr_row_valued(
        program,
        op,
        lhs_reg,
        rhs_reg,
        lhs_arity,
        target_register,
        e1,
        e2,
        referenced_tables,
        resolver,
    )?;

    if let BinaryEmitMode::Condition(metadata) = emit_mode {
        emit_cond_jump(program, metadata, target_register);
    }
    Ok(target_register)
}

/// Function instructions read contiguous registers start_reg..start_reg+arg_count.
/// When both operands are equivalent the compiler reuses a single shared register,
/// so copy it into a contiguous pair.
fn contiguous_operands(program: &mut ProgramBuilder, lhs: usize, rhs: usize) -> usize {
    if lhs != rhs {
        return lhs;
    }
    let regs = program.alloc_registers(2);
    program.emit_insn(Insn::Copy {
        src_reg: lhs,
        dst_reg: regs,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: lhs,
        dst_reg: regs + 1,
        extra_amount: 0,
    });
    regs
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_binary_expr_scalar(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    target_register: usize,
    resolver: &Resolver,
    emit_mode: BinaryEmitMode,
) -> Result<usize> {
    let compared_as = if op.is_comparison() {
        stored_comparison_type([e1, e2], referenced_tables, resolver)?
    } else {
        None
    };
    let compared_as = compared_as.as_ref();
    // Equivalent expressions are reusable only when each occurrence is deterministic.
    if exprs_are_equivalent(e1, e2)
        && !expr_contains_nondeterministic_scalar_function(e1, resolver)?
    {
        let shared_reg = program.alloc_register();
        translate_compared_expr(
            program,
            referenced_tables,
            e1,
            shared_reg,
            resolver,
            compared_as,
        )?;

        match emit_mode {
            BinaryEmitMode::Value => emit_binary_insn(
                program,
                op,
                shared_reg,
                shared_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                resolver,
            )?,
            BinaryEmitMode::Condition(metadata) => emit_binary_condition_insn(
                program,
                op,
                shared_reg,
                shared_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                metadata,
                resolver,
            )?,
        }
        Ok(target_register)
    } else {
        let e1_reg = program.alloc_registers(2);
        let e2_reg = e1_reg + 1;

        translate_compared_expr(
            program,
            referenced_tables,
            e1,
            e1_reg,
            resolver,
            compared_as,
        )?;
        translate_compared_expr(
            program,
            referenced_tables,
            e2,
            e2_reg,
            resolver,
            compared_as,
        )?;

        match emit_mode {
            BinaryEmitMode::Value => emit_binary_insn(
                program,
                op,
                e1_reg,
                e2_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                resolver,
            )?,
            BinaryEmitMode::Condition(metadata) => emit_binary_condition_insn(
                program,
                op,
                e1_reg,
                e2_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                metadata,
                resolver,
            )?,
        }
        Ok(target_register)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_binary_expr_row_valued(
    program: &mut ProgramBuilder,
    op: &ast::Operator,
    lhs_start: usize,
    rhs_start: usize,
    arity: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<()> {
    enum RowOrderingOp {
        Less,
        Greater,
    }

    let metadata = ExprContext::new(referenced_tables, resolver);
    let mut emit_eq = |result_reg: usize, null_eq: bool| -> Result<()> {
        let null_seen_reg = if null_eq {
            None
        } else {
            let reg = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: reg,
            });
            Some(reg)
        };

        let done_label = program.allocate_label();
        for i in 0..arity {
            let next_label = program.allocate_label();
            let (affinity, collation) =
                row_component_affinity_collation(lhs_expr, rhs_expr, i, &metadata)?;
            program.emit_insn(Insn::Eq {
                lhs: lhs_start + i,
                rhs: rhs_start + i,
                target_pc: next_label,
                flags: if null_eq {
                    CmpInsFlags::default().null_eq().with_affinity(affinity)
                } else {
                    CmpInsFlags::default().with_affinity(affinity)
                },
                collation,
            });
            if null_eq {
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: result_reg,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: done_label,
                });
            } else {
                let mark_null_label = program.allocate_label();
                program.emit_insn(Insn::IsNull {
                    reg: lhs_start + i,
                    target_pc: mark_null_label,
                });
                program.emit_insn(Insn::IsNull {
                    reg: rhs_start + i,
                    target_pc: mark_null_label,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: result_reg,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: done_label,
                });
                program.preassign_label_to_next_insn(mark_null_label);
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: null_seen_reg.expect("null tracking register must exist"),
                });
            }
            program.preassign_label_to_next_insn(next_label);
        }
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: result_reg,
        });
        if !null_eq {
            let finish_label = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: null_seen_reg.expect("null tracking register must exist"),
                target_pc: finish_label,
                jump_if_null: true,
            });
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
            program.preassign_label_to_next_insn(finish_label);
        }
        program.preassign_label_to_next_insn(done_label);
        Ok(())
    };

    let emit_order =
        |program: &mut ProgramBuilder, order_op: RowOrderingOp, include_eq: bool| -> Result<()> {
            let done_label = program.allocate_label();
            let null_result_label = program.allocate_label();
            for i in 0..arity {
                let next_cmp_label = program.allocate_label();
                let (aff, collation) =
                    row_component_affinity_collation(lhs_expr, rhs_expr, i, &metadata)?;
                let lhs = lhs_start + i;
                let rhs = rhs_start + i;
                program.emit_insn(Insn::IsNull {
                    reg: lhs,
                    target_pc: null_result_label,
                });
                program.emit_insn(Insn::IsNull {
                    reg: rhs,
                    target_pc: null_result_label,
                });
                program.emit_insn(Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: next_cmp_label,
                    flags: CmpInsFlags::default().with_affinity(aff),
                    collation,
                });
                let true_label = program.allocate_label();
                match order_op {
                    RowOrderingOp::Less => {
                        program.emit_insn(Insn::Lt {
                            lhs,
                            rhs,
                            target_pc: true_label,
                            flags: CmpInsFlags::default().with_affinity(aff),
                            collation,
                        });
                    }
                    RowOrderingOp::Greater => {
                        program.emit_insn(Insn::Gt {
                            lhs,
                            rhs,
                            target_pc: true_label,
                            flags: CmpInsFlags::default().with_affinity(aff),
                            collation,
                        });
                    }
                }
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: target_register,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: done_label,
                });
                program.preassign_label_to_next_insn(true_label);
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: target_register,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: done_label,
                });
                program.preassign_label_to_next_insn(next_cmp_label);
            }
            program.emit_insn(Insn::Integer {
                value: if include_eq { 1 } else { 0 },
                dest: target_register,
            });
            program.emit_insn(Insn::Goto {
                target_pc: done_label,
            });
            program.preassign_label_to_next_insn(null_result_label);
            program.emit_insn(Insn::Null {
                dest: target_register,
                dest_end: None,
            });
            program.preassign_label_to_next_insn(done_label);
            Ok(())
        };

    match op {
        ast::Operator::Equals => emit_eq(target_register, false)?,
        ast::Operator::NotEquals => {
            emit_eq(target_register, false)?;
            invert_boolean_register(program, target_register);
        }
        ast::Operator::Is => emit_eq(target_register, true)?,
        ast::Operator::IsNot => {
            emit_eq(target_register, true)?;
            invert_boolean_register(program, target_register);
        }
        ast::Operator::Less => emit_order(program, RowOrderingOp::Less, false)?,
        ast::Operator::LessEquals => emit_order(program, RowOrderingOp::Less, true)?,
        ast::Operator::Greater => emit_order(program, RowOrderingOp::Greater, false)?,
        ast::Operator::GreaterEquals => emit_order(program, RowOrderingOp::Greater, true)?,
        _ => crate::bail_parse_error!("row value misused"),
    }
    Ok(())
}

pub(super) fn invert_boolean_register(program: &mut ProgramBuilder, target_register: usize) {
    program.emit_insn(Insn::Not {
        reg: target_register,
        dest: target_register,
    });
}

pub(super) fn row_component_affinity_collation(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    idx: usize,
    metadata: &ExprContext<'_, '_>,
) -> Result<(Affinity, CollationSeq)> {
    let lhs_for_cmp = row_value_component_expr(lhs_expr, idx)?;
    let rhs_for_cmp = row_value_component_expr(rhs_expr, idx)?;
    Ok((
        comparison_affinity(&*lhs_for_cmp, &*rhs_for_cmp, metadata),
        comparison_collation(&lhs_for_cmp, &rhs_for_cmp, metadata)?,
    ))
}

/// Component `idx` of a row value: an element of `(a, b)`, or one column of a
/// row subquery's result, so each component compares with its own type. A
/// subquery column does not bring its text collation. Any other expression
/// stands for itself.
fn row_value_component_expr(expr: &Expr, idx: usize) -> Result<Cow<'_, Expr>> {
    match unwrap_parens(expr)? {
        Expr::Parenthesized(exprs) if exprs.len() > 1 => {
            Ok(exprs.get(idx).map_or(Cow::Borrowed(expr), |component| {
                Cow::Borrowed(component.as_ref())
            }))
        }
        Expr::SubqueryResult {
            subquery_id,
            lhs: None,
            not_in,
            query_type:
                ast::SubqueryType::RowValue {
                    result_reg_start,
                    num_regs,
                },
        } if *num_regs > 1 && idx < *num_regs => Ok(Cow::Owned(Expr::SubqueryResult {
            subquery_id: *subquery_id,
            lhs: None,
            not_in: *not_in,
            query_type: ast::SubqueryType::RowValue {
                result_reg_start: result_reg_start + idx,
                num_regs: 1,
            },
        })),
        _ => Ok(Cow::Borrowed(expr)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_binary_insn(
    program: &mut ProgramBuilder,
    op: &ast::Operator,
    lhs: usize,
    rhs: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<()> {
    let metadata = ExprContext::new(referenced_tables, resolver);
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, &metadata);
    }
    let collation = || comparison_collation(lhs_expr, rhs_expr, &metadata);
    let cmp_flags = || CmpInsFlags::default().with_affinity(affinity);

    match op {
        ast::Operator::NotEquals => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::Equals => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::Less => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::LessEquals => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::Greater => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::GreaterEquals => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: cmp_flags(),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
                lhs,
                rhs,
            );
        }
        ast::Operator::Add => {
            program.emit_insn(Insn::Add {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Subtract => {
            program.emit_insn(Insn::Subtract {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Multiply => {
            program.emit_insn(Insn::Multiply {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Divide => {
            program.emit_insn(Insn::Divide {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Modulus => {
            program.emit_insn(Insn::Remainder {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::And => {
            program.emit_insn(Insn::And {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Or => {
            program.emit_insn(Insn::Or {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::BitwiseAnd => {
            program.emit_insn(Insn::BitAnd {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::BitwiseOr => {
            program.emit_insn(Insn::BitOr {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::RightShift => {
            program.emit_insn(Insn::ShiftRight {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::LeftShift => {
            program.emit_insn(Insn::ShiftLeft {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        ast::Operator::Is => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr(
                program,
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: CmpInsFlags::default().null_eq().with_affinity(affinity),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
            );
        }
        ast::Operator::IsNot => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr(
                program,
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: CmpInsFlags::default().null_eq().with_affinity(affinity),
                    collation: collation()?,
                },
                target_register,
                if_true_label,
            );
        }
        #[cfg(feature = "json")]
        op @ (ast::Operator::ArrowRight | ast::Operator::ArrowRightShift) => {
            let json_func = match op {
                ast::Operator::ArrowRight => JsonFunc::JsonArrowExtract,
                ast::Operator::ArrowRightShift => JsonFunc::JsonArrowShiftExtract,
                _ => unreachable!(),
            };
            let start = contiguous_operands(program, lhs, rhs);

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: start,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Json(json_func),
                    arg_count: 2,
                    collation: CollationSeq::BINARY,
                },
            })
        }
        ast::Operator::Concat => {
            if is_array(lhs_expr, referenced_tables, resolver)
                || is_array(rhs_expr, referenced_tables, resolver)
            {
                program.emit_insn(Insn::ArrayConcat {
                    lhs,
                    rhs,
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Concat {
                    lhs,
                    rhs,
                    dest: target_register,
                });
            }
        }
        ast::Operator::ArrayContains | ast::Operator::ArrayOverlap => {
            resolver.require_custom_types("Array features")?;
            let start = contiguous_operands(program, lhs, rhs);
            let func = match op {
                ast::Operator::ArrayContains => ScalarFunc::ArrayContainsAll,
                ast::Operator::ArrayOverlap => ScalarFunc::ArrayOverlap,
                _ => unreachable!(),
            };
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: start,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(func),
                    arg_count: 2,
                    collation: CollationSeq::BINARY,
                },
            });
        }
        other_unimplemented => todo!("{:?}", other_unimplemented),
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_binary_condition_insn(
    program: &mut ProgramBuilder,
    op: &ast::Operator,
    lhs: usize,
    rhs: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
) -> Result<()> {
    let metadata = ExprContext::new(referenced_tables, resolver);
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, &metadata);
    }
    let collation = || comparison_collation(lhs_expr, rhs_expr, &metadata);

    let opposite_op = match op {
        ast::Operator::NotEquals => ast::Operator::Equals,
        ast::Operator::Equals => ast::Operator::NotEquals,
        ast::Operator::Less => ast::Operator::GreaterEquals,
        ast::Operator::LessEquals => ast::Operator::Greater,
        ast::Operator::Greater => ast::Operator::LessEquals,
        ast::Operator::GreaterEquals => ast::Operator::Less,
        ast::Operator::Is => ast::Operator::IsNot,
        ast::Operator::IsNot => ast::Operator::Is,
        other => *other,
    };

    // For conditional jumps we need to use the opposite comparison operator
    // when we intend to jump if the condition is false. Jumping when the condition is false
    // is the common case, e.g.:
    // WHERE x=1 turns into "jump if x != 1".
    // However, in e.g. "WHERE x=1 OR y=2" we want to jump if the condition is true
    // when evaluating "x=1", because we are jumping over the "y=2" condition, and if the condition
    // is false we move on to the "y=2" condition without jumping.
    let op_to_use = if condition_metadata.jump_if_condition_is_true {
        *op
    } else {
        opposite_op
    };

    // Set the "jump if NULL" flag when the NULL target matches the jump target.
    // When jump_if_condition_is_true: we jump on true, so set jump_if_null when NULL should also jump (e.g. CHECK constraints in integrity_check).
    // When !jump_if_condition_is_true: we jump on false, so set jump_if_null when NULL should also jump (standard SQL 3-valued logic).
    let mut flags = CmpInsFlags::default().with_affinity(affinity);
    if condition_metadata.jump_if_condition_is_true {
        if condition_metadata.jump_target_when_null == condition_metadata.jump_target_when_true {
            flags = flags.jump_if_null()
        }
    } else if condition_metadata.jump_target_when_null == condition_metadata.jump_target_when_false
    {
        flags = flags.jump_if_null()
    };

    let target_pc = if condition_metadata.jump_if_condition_is_true {
        condition_metadata.jump_target_when_true
    } else {
        condition_metadata.jump_target_when_false
    };

    // For conditional jumps that don't have a clear "opposite op" (e.g. x+y), we check whether the result is nonzero/nonnull
    // (or zero/null) depending on the condition metadata.
    let eval_result = |program: &mut ProgramBuilder, result_reg: usize| {
        if condition_metadata.jump_if_condition_is_true {
            program.emit_insn(Insn::If {
                reg: result_reg,
                target_pc,
                jump_if_null: false,
            });
        } else {
            program.emit_insn(Insn::IfNot {
                reg: result_reg,
                target_pc,
                jump_if_null: true,
            });
        }
    };

    match op_to_use {
        ast::Operator::NotEquals => {
            program.emit_insn(Insn::Ne {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::Equals => {
            program.emit_insn(Insn::Eq {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::Less => {
            program.emit_insn(Insn::Lt {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::LessEquals => {
            program.emit_insn(Insn::Le {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::Greater => {
            program.emit_insn(Insn::Gt {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::GreaterEquals => {
            program.emit_insn(Insn::Ge {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: collation()?,
            });
        }
        ast::Operator::Is => {
            program.emit_insn(Insn::Eq {
                lhs,
                rhs,
                target_pc,
                flags: flags.null_eq(),
                collation: collation()?,
            });
        }
        ast::Operator::IsNot => {
            program.emit_insn(Insn::Ne {
                lhs,
                rhs,
                target_pc,
                flags: flags.null_eq(),
                collation: collation()?,
            });
        }
        ast::Operator::Add => {
            program.emit_insn(Insn::Add {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::Subtract => {
            program.emit_insn(Insn::Subtract {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::Multiply => {
            program.emit_insn(Insn::Multiply {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::Divide => {
            program.emit_insn(Insn::Divide {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::Modulus => {
            program.emit_insn(Insn::Remainder {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::And => {
            program.emit_insn(Insn::And {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::Or => {
            program.emit_insn(Insn::Or {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::BitwiseAnd => {
            program.emit_insn(Insn::BitAnd {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::BitwiseOr => {
            program.emit_insn(Insn::BitOr {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::RightShift => {
            program.emit_insn(Insn::ShiftRight {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        ast::Operator::LeftShift => {
            program.emit_insn(Insn::ShiftLeft {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
        }
        #[cfg(feature = "json")]
        op @ (ast::Operator::ArrowRight | ast::Operator::ArrowRightShift) => {
            let json_func = match op {
                ast::Operator::ArrowRight => JsonFunc::JsonArrowExtract,
                ast::Operator::ArrowRightShift => JsonFunc::JsonArrowShiftExtract,
                _ => unreachable!(),
            };
            let start = contiguous_operands(program, lhs, rhs);

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: start,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Json(json_func),
                    arg_count: 2,
                    collation: CollationSeq::BINARY,
                },
            });
            eval_result(program, target_register);
        }
        ast::Operator::Concat => {
            if is_array(lhs_expr, referenced_tables, resolver)
                || is_array(rhs_expr, referenced_tables, resolver)
            {
                program.emit_insn(Insn::ArrayConcat {
                    lhs,
                    rhs,
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Concat {
                    lhs,
                    rhs,
                    dest: target_register,
                });
            }
            eval_result(program, target_register);
        }
        ast::Operator::ArrayContains | ast::Operator::ArrayOverlap => {
            resolver.require_custom_types("Array features")?;
            let start = contiguous_operands(program, lhs, rhs);
            let func = match op {
                ast::Operator::ArrayContains => ScalarFunc::ArrayContainsAll,
                ast::Operator::ArrayOverlap => ScalarFunc::ArrayOverlap,
                _ => unreachable!(),
            };
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: start,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(func),
                    arg_count: 2,
                    collation: CollationSeq::BINARY,
                },
            });
            eval_result(program, target_register);
        }
        other_unimplemented => todo!("{:?}", other_unimplemented),
    }

    Ok(())
}

fn is_array(expr: &Expr, referenced_tables: Option<&TableReferences>, resolver: &Resolver) -> bool {
    type_of(expr, &TypeEnv::new(referenced_tables, resolver)).is_array()
}
