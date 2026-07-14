use super::*;

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
        crate::bail_parse_error!(
            "all arguments to binary operator {op} must return the same number of values. Got: ({lhs_arity}) {op} ({rhs_arity})"
        );
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
        Some(resolver),
    )?;

    if let BinaryEmitMode::Condition(metadata) = emit_mode {
        emit_cond_jump(program, metadata, target_register);
    }
    Ok(target_register)
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
    // A left spine of binary operators (`a OR b OR c ...`) can be up to
    // MAX_EXPR_DEPTH links long, while recursion in the translator must stay
    // within MAX_EXPR_NESTING frames. Translate the spine iteratively:
    // descend it running each node's prologue (constant-span bookkeeping,
    // operand-equivalence check, register allocation) in exactly the order
    // the recursive translate_expr -> binary path ran them, then emit from
    // the innermost node outwards. Only the operands hanging off the spine
    // are translated via (nesting-bounded) recursion.

    /// Registers of one spine node: either one register shared by both
    /// operands (when they are equivalent expressions), or a contiguous
    /// (lhs, rhs) pair starting at the given register.
    enum SpineRegs {
        Shared(usize),
        Pair(usize),
    }

    struct SpineNode<'a> {
        lhs: &'a ast::Expr,
        op: &'a ast::Operator,
        rhs: &'a ast::Expr,
        /// Register this node's value is emitted into.
        target: usize,
        regs: SpineRegs,
        /// Constant span opened by this node's prologue. Always `None` for
        /// the outermost node: its prologue ran in its own translate_expr
        /// frame, which also closes the span.
        constant_span: Option<usize>,
        emit_mode: BinaryEmitMode,
    }

    // Check if both sides of the expression are equivalent and reuse the same register if so
    fn alloc_operand_regs(
        program: &mut ProgramBuilder,
        e1: &ast::Expr,
        e2: &ast::Expr,
    ) -> (SpineRegs, usize) {
        if exprs_are_equivalent(e1, e2) {
            let shared_reg = program.alloc_register();
            (SpineRegs::Shared(shared_reg), shared_reg)
        } else {
            let e1_reg = program.alloc_registers(2);
            (SpineRegs::Pair(e1_reg), e1_reg)
        }
    }

    let (regs, mut cur_target) = alloc_operand_regs(program, e1, e2);
    let mut nodes = vec![SpineNode {
        lhs: e1,
        op,
        rhs: e2,
        target: target_register,
        regs,
        constant_span: None,
        emit_mode,
    }];
    let mut cur = e1;
    loop {
        let ast::Expr::Binary(cur_lhs, cur_op, cur_rhs) = cur else {
            break;
        };
        // Nodes that translate_expr's Binary arm routes to dedicated paths
        // terminate the spine and are translated as opaque operands below:
        // IS TRUE/FALSE forms (bounded because the parser counts them as
        // nesting), custom-type operator nodes (bounded because they require
        // a Column lhs, so they can never be consecutive on a spine), and
        // cached subexpressions (terminal: they emit a register copy).
        if matches!(
            (cur_op, cur_rhs.as_ref()),
            (
                ast::Operator::Is | ast::Operator::IsNot,
                ast::Expr::Literal(ast::Literal::True | ast::Literal::False)
            )
        ) {
            break;
        }
        if resolver.resolve_cached_expr_reg(cur).is_some() {
            break;
        }
        if find_custom_type_operator(cur_lhs, cur_rhs, cur_op, referenced_tables, resolver)
            .is_some()
        {
            break;
        }
        // Row-valued comparisons (e.g. `(a,b,c) < (1,2,3)`) produce a scalar,
        // so they can sit on a spine, but their own emission needs the
        // row-valued path; only nodes with scalar operands are foldable.
        // These arities were already validated by the caller's
        // expr_vector_size checks, so this cannot fail here.
        if expr_vector_size(cur_lhs)? != 1 || expr_vector_size(cur_rhs)? != 1 {
            break;
        }
        // Replica of translate_expr's entry prologue for this node.
        let constant_span = if cur.is_constant(resolver) {
            if !program.constant_span_is_open() {
                Some(program.constant_span_start())
            } else {
                None
            }
        } else {
            program.constant_span_end_all();
            None
        };
        let (regs, next_target) = alloc_operand_regs(program, cur_lhs, cur_rhs);
        nodes.push(SpineNode {
            lhs: cur_lhs,
            op: cur_op,
            rhs: cur_rhs,
            target: cur_target,
            regs,
            constant_span,
            emit_mode: BinaryEmitMode::Value,
        });
        cur_target = next_target;
        cur = cur_lhs;
    }

    // The leftmost operand of the spine.
    translate_expr(program, referenced_tables, cur, cur_target, resolver)?;

    for node in nodes.into_iter().rev() {
        let (lhs_reg, rhs_reg) = match node.regs {
            SpineRegs::Shared(shared_reg) => (shared_reg, shared_reg),
            SpineRegs::Pair(e1_reg) => {
                let e2_reg = e1_reg + 1;
                let left_collation_ctx = program.curr_collation_ctx();
                program.reset_collation();

                translate_expr(program, referenced_tables, node.rhs, e2_reg, resolver)?;
                let right_collation_ctx = program.curr_collation_ctx();
                program.reset_collation();

                /*
                 * The rules for determining which collating function to use for a binary comparison
                 * operator (=, <, >, <=, >=, !=, IS, and IS NOT) are as follows:
                 *
                 * 1. If either operand has an explicit collating function assignment using the postfix COLLATE operator,
                 * then the explicit collating function is used for comparison,
                 * with precedence to the collating function of the left operand.
                 *
                 * 2. If either operand is a column, then the collating function of that column is used
                 * with precedence to the left operand. For the purposes of the previous sentence,
                 * a column name preceded by one or more unary "+" operators and/or CAST operators is still considered a column name.
                 *
                 * 3. Otherwise, the BINARY collating function is used for comparison.
                 */
                let collation_ctx = {
                    match (left_collation_ctx, right_collation_ctx) {
                        (Some((c_left, true)), _) => Some((c_left, true)),
                        (_, Some((c_right, true))) => Some((c_right, true)),
                        (Some((c_left, from_collate_left)), None) => {
                            Some((c_left, from_collate_left))
                        }
                        (None, Some((c_right, from_collate_right))) => {
                            Some((c_right, from_collate_right))
                        }
                        (Some((c_left, from_collate_left)), Some((_, false))) => {
                            Some((c_left, from_collate_left))
                        }
                        _ => None,
                    }
                };
                program.set_collation(collation_ctx);
                (e1_reg, e2_reg)
            }
        };

        match node.emit_mode {
            BinaryEmitMode::Value => emit_binary_insn(
                program,
                node.op,
                lhs_reg,
                rhs_reg,
                node.target,
                node.lhs,
                node.rhs,
                referenced_tables,
                Some(resolver),
            )?,
            BinaryEmitMode::Condition(metadata) => emit_binary_condition_insn(
                program,
                node.op,
                lhs_reg,
                rhs_reg,
                node.target,
                node.lhs,
                node.rhs,
                referenced_tables,
                metadata,
                Some(resolver),
            )?,
        }
        // Only reset collation for comparison operators, which consume it.
        // Non-comparison operators (Concat, Add, etc.) must propagate the
        // collation to the parent expression so that e.g.
        //   (name COLLATE NOCASE || '') <> 'admin'
        // correctly applies NOCASE to the Ne comparison.
        if node.op.is_comparison() {
            program.reset_collation();
        }
        if let Some(span) = node.constant_span {
            program.constant_span_end(span);
        }
    }
    Ok(target_register)
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
    resolver: Option<&Resolver>,
) -> Result<()> {
    enum RowOrderingOp {
        Less,
        Greater,
    }

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
            let (affinity, collation) = row_component_affinity_collation(
                lhs_expr,
                rhs_expr,
                i,
                referenced_tables,
                resolver,
            )?;
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
                let (aff, collation) = row_component_affinity_collation(
                    lhs_expr,
                    rhs_expr,
                    i,
                    referenced_tables,
                    resolver,
                )?;
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

pub(super) fn row_value_component_expr(expr: &Expr, idx: usize) -> Result<Option<&Expr>> {
    match unwrap_parens(expr)? {
        Expr::Parenthesized(exprs) if exprs.len() > 1 => Ok(exprs.get(idx).map(Box::as_ref)),
        _ => Ok(None),
    }
}

pub(super) fn row_component_affinity_collation(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    idx: usize,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Result<(Affinity, Option<CollationSeq>)> {
    // If one side is a decomposable row literal and the other is not, still prefer
    // the component that is available instead of falling back both sides.
    // TODO: when both sides are non-decomposable row sources (e.g. subquery row-values),
    // this falls back to whole-expression affinity/collation and cannot distinguish
    // per-component metadata.
    let lhs_for_cmp = row_value_component_expr(lhs_expr, idx)?.unwrap_or(lhs_expr);
    let rhs_for_cmp = row_value_component_expr(rhs_expr, idx)?.unwrap_or(rhs_expr);
    Ok((
        comparison_affinity(lhs_for_cmp, rhs_for_cmp, referenced_tables, resolver),
        comparison_collation(lhs_for_cmp, rhs_for_cmp, referenced_tables, resolver)?,
    ))
}

pub(super) fn explicit_collation(
    expr: &Expr,
    resolver: Option<&Resolver>,
) -> Result<Option<CollationSeq>> {
    let mut found = None;
    walk_expr(expr, &mut |e| -> Result<WalkControl> {
        if let Expr::Collate(_, seq) = e {
            if found.is_none() {
                let collation = match resolver {
                    Some(resolver) => resolver.resolve_collation(seq.as_str()),
                    None => CollationSeq::new(seq.as_str()),
                }
                .unwrap_or_default();
                found = Some(collation);
            }
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

pub(super) fn comparison_collation(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Result<Option<CollationSeq>> {
    if let Some(tables) = referenced_tables {
        let symbol_table = resolver.map(|resolver| resolver.symbol_table);
        let lhs_collation = get_collseq_from_expr_with_symbols(lhs_expr, tables, symbol_table)?;
        if lhs_collation.is_some() {
            return Ok(lhs_collation);
        }
        return get_collseq_from_expr_with_symbols(rhs_expr, tables, symbol_table);
    }

    let lhs_collation = explicit_collation(lhs_expr, resolver)?;
    if lhs_collation.is_some() {
        return Ok(lhs_collation);
    }
    explicit_collation(rhs_expr, resolver)
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
    resolver: Option<&Resolver>,
) -> Result<()> {
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, referenced_tables, resolver);
    }
    let is_array_cmp =
        expr_is_array(lhs_expr, referenced_tables) && expr_is_array(rhs_expr, referenced_tables);
    let cmp_flags = || {
        let f = CmpInsFlags::default().with_affinity(affinity);
        if is_array_cmp {
            f.array_cmp()
        } else {
            f
        }
    };

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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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
                    collation: program.curr_collation(),
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

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: lhs,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Json(json_func),
                    arg_count: 2,
                },
            })
        }
        ast::Operator::Concat => {
            if expr_is_array(lhs_expr, referenced_tables)
                || expr_is_array(rhs_expr, referenced_tables)
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
            if let Some(r) = resolver {
                r.require_custom_types("Array features")?;
            }
            // Function instructions read contiguous registers start_reg..start_reg+arg_count.
            // When both operands are equivalent the compiler reuses a single shared register,
            // so we must copy it into a contiguous pair.
            let start = if lhs == rhs {
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
            } else {
                lhs
            };
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
                },
            });
        }
        other_unimplemented => todo!("{:?}", other_unimplemented),
    }

    Ok(())
}

/// Check if an expression is known to produce an array value.
pub(crate) fn expr_is_array(expr: &Expr, referenced_tables: Option<&TableReferences>) -> bool {
    // Iterative (explicit work stack) because Concat chains can be up to
    // MAX_EXPR_DEPTH links long, which recursion could not traverse within
    // MAX_EXPR_NESTING-sized stack budgets.
    let mut stack: smallvec::SmallVec<[&Expr; 8]> = smallvec::smallvec![expr];
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::Column { table, column, .. } => {
                if let Some(tables) = referenced_tables {
                    if tables
                        .find_table_by_internal_id(*table)
                        .map(|(_, t)| t)
                        .and_then(|t| t.get_column_at(*column))
                        .is_some_and(|col| col.is_array())
                    {
                        return true;
                    }
                }
            }
            Expr::FunctionCall { name, args, .. } => {
                if let Ok(Some(f)) = Func::resolve_function(name.as_str(), args.len()) {
                    match &f {
                        Func::Scalar(sf) if sf.returns_array_blob() => return true,
                        Func::Agg(AggFunc::ArrayAgg) => return true,
                        _ => {}
                    }
                }
                // Wrapper functions that pass through an array value
                match name.as_str().to_lowercase().as_str() {
                    "coalesce" | "ifnull" | "min" | "max" => {
                        for a in args {
                            stack.push(a);
                        }
                    }
                    "iif" => {
                        // args: condition, then_val, else_val
                        if let Some(a) = args.get(1) {
                            stack.push(a);
                        }
                        if let Some(a) = args.get(2) {
                            stack.push(a);
                        }
                    }
                    "nullif" => {
                        if let Some(a) = args.first() {
                            stack.push(a);
                        }
                    }
                    "array_element" => {
                        // Subscripting a multi-dim array yields a lower-dim array
                        if let Some(tables) = referenced_tables {
                            if args
                                .first()
                                .is_some_and(|a| expr_array_dimensions(a, tables) > 1)
                            {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Expr::Array { .. } | Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
            Expr::Binary(lhs, ast::Operator::Concat, rhs) => {
                stack.push(lhs);
                stack.push(rhs);
            }
            Expr::Case {
                when_then_pairs,
                else_expr,
                ..
            } => {
                for (_, then_expr) in when_then_pairs {
                    stack.push(then_expr);
                }
                if let Some(e) = else_expr {
                    stack.push(e);
                }
            }
            _ => {}
        }
    }
    false
}

/// Return the number of array dimensions for an expression, or 0 for non-array.
pub(super) fn expr_array_dimensions(expr: &Expr, tables: &TableReferences) -> u32 {
    match expr {
        Expr::Column { table, column, .. } => tables
            .find_table_by_internal_id(*table)
            .map(|(_, t)| t)
            .and_then(|t| t.get_column_at(*column))
            .map(|col| col.array_dimensions())
            .unwrap_or(0),
        Expr::FunctionCall { name, args, .. }
            if name.as_str().eq_ignore_ascii_case("array_element") =>
        {
            let d = args
                .first()
                .map(|a| expr_array_dimensions(a, tables))
                .unwrap_or(0);
            d.saturating_sub(1)
        }
        Expr::FunctionCall { name, .. } if name.as_str().eq_ignore_ascii_case("array") => 1,
        Expr::Subscript { .. } | Expr::Array { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        _ => 0,
    }
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
    resolver: Option<&Resolver>,
) -> Result<()> {
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, referenced_tables, resolver);
    }

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
    if expr_is_array(lhs_expr, referenced_tables) && expr_is_array(rhs_expr, referenced_tables) {
        flags = flags.array_cmp();
    }
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
                collation: program.curr_collation(),
            });
        }
        ast::Operator::Equals => {
            program.emit_insn(Insn::Eq {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: program.curr_collation(),
            });
        }
        ast::Operator::Less => {
            program.emit_insn(Insn::Lt {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: program.curr_collation(),
            });
        }
        ast::Operator::LessEquals => {
            program.emit_insn(Insn::Le {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: program.curr_collation(),
            });
        }
        ast::Operator::Greater => {
            program.emit_insn(Insn::Gt {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: program.curr_collation(),
            });
        }
        ast::Operator::GreaterEquals => {
            program.emit_insn(Insn::Ge {
                lhs,
                rhs,
                target_pc,
                flags,
                collation: program.curr_collation(),
            });
        }
        ast::Operator::Is => {
            program.emit_insn(Insn::Eq {
                lhs,
                rhs,
                target_pc,
                flags: flags.null_eq(),
                collation: program.curr_collation(),
            });
        }
        ast::Operator::IsNot => {
            program.emit_insn(Insn::Ne {
                lhs,
                rhs,
                target_pc,
                flags: flags.null_eq(),
                collation: program.curr_collation(),
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

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: lhs,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Json(json_func),
                    arg_count: 2,
                },
            });
            eval_result(program, target_register);
        }
        ast::Operator::Concat => {
            if expr_is_array(lhs_expr, referenced_tables)
                || expr_is_array(rhs_expr, referenced_tables)
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
            if let Some(r) = resolver {
                r.require_custom_types("Array features")?;
            }
            let start = if lhs == rhs {
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
            } else {
                lhs
            };
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
                },
            });
            eval_result(program, target_register);
        }
        other_unimplemented => todo!("{:?}", other_unimplemented),
    }

    Ok(())
}
