use super::*;

/// Core implementation of IN expression logic that can be used in both conditional and expression contexts.
/// This follows SQLite's approach where a single core function handles all InList cases.
///
/// This is extracted from the original conditional implementation to be reusable.
/// The logic exactly matches the original conditional InList implementation.
///
/// An IN expression has one of the following formats:
///  ```sql
///      x IN (y1, y2,...,yN)
///      x IN (subquery) (Not yet implemented)
///  ```
/// The result of an IN operator is one of TRUE, FALSE, or NULL.  A NULL result
/// means that it cannot be determined if the LHS is contained in the RHS due
/// to the presence of NULL values.
///
/// Currently, we do a simple full-scan, yet it's not ideal when there are many rows
/// on RHS. (Check sqlite's in-operator.md)
///
/// Algorithm:
/// 1. Set the null-flag to false
/// 2. For each row in the RHS:
///     - Compare LHS and RHS
///     - If LHS matches RHS, returns TRUE
///     - If the comparison results in NULL, set the null-flag to true
/// 3. If the null-flag is true, return NULL
/// 4. Return FALSE
///
/// A "NOT IN" operator is computed by first computing the equivalent IN
/// operator, then interchanging the TRUE and FALSE results.
/// Compute the affinity for an IN expression.
/// For `x IN (y1, y2, ..., yN)`, the affinity is determined by the LHS expression `x`.
/// This follows SQLite's `exprINAffinity()` function.
pub(super) fn in_expr_affinity(
    lhs: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    // For parenthesized expressions (vectors), we take the first element's affinity
    // since scalar IN comparisons only use the first element
    match lhs {
        Expr::Parenthesized(exprs) if !exprs.is_empty() => {
            get_expr_affinity(&exprs[0], referenced_tables, resolver)
        }
        _ => get_expr_affinity(lhs, referenced_tables, resolver),
    }
}

#[instrument(skip(program, referenced_tables, resolver), level = Level::DEBUG)]
pub(super) fn translate_in_list(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    rhs: &[Box<ast::Expr>],
    condition_metadata: ConditionMetadata,
    // dest if null should be in ConditionMetadata
    resolver: &Resolver,
) -> Result<()> {
    let lhs_arity = expr_vector_size(lhs)?;
    let lhs_reg = program.alloc_registers(lhs_arity);
    let _ = translate_expr(program, referenced_tables, lhs, lhs_reg, resolver)?;
    let mut check_null_reg = 0;
    let label_ok = program.allocate_label();

    // Compute the affinity for the IN comparison based on the LHS expression
    // This follows SQLite's exprINAffinity() approach
    let affinity = in_expr_affinity(lhs, referenced_tables, Some(resolver));
    let cmp_flags = CmpInsFlags::default().with_affinity(affinity);

    if condition_metadata.jump_target_when_false != condition_metadata.jump_target_when_null {
        check_null_reg = program.alloc_register();
        program.emit_insn(Insn::BitAnd {
            lhs: lhs_reg,
            rhs: lhs_reg,
            dest: check_null_reg,
        });
    }

    for (i, expr) in rhs.iter().enumerate() {
        let last_condition = i == rhs.len() - 1;
        let rhs_reg = program.alloc_registers(lhs_arity);
        let _ = translate_expr(program, referenced_tables, expr, rhs_reg, resolver)?;

        if check_null_reg != 0 && expr.can_be_null() {
            program.emit_insn(Insn::BitAnd {
                lhs: check_null_reg,
                rhs: rhs_reg,
                dest: check_null_reg,
            });
        }

        if lhs_arity == 1 {
            // Scalar comparison path
            if !last_condition
                || condition_metadata.jump_target_when_false
                    != condition_metadata.jump_target_when_null
            {
                if lhs_reg != rhs_reg {
                    program.emit_insn(Insn::Eq {
                        lhs: lhs_reg,
                        rhs: rhs_reg,
                        target_pc: label_ok,
                        flags: cmp_flags,
                        collation: program.curr_collation(),
                    });
                } else {
                    program.emit_insn(Insn::NotNull {
                        reg: lhs_reg,
                        target_pc: label_ok,
                    });
                }
            } else if lhs_reg != rhs_reg {
                program.emit_insn(Insn::Ne {
                    lhs: lhs_reg,
                    rhs: rhs_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                    flags: cmp_flags.jump_if_null(),
                    collation: program.curr_collation(),
                });
            } else {
                program.emit_insn(Insn::IsNull {
                    reg: lhs_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        } else {
            // Row-valued comparison path: compare each component
            if !last_condition
                || condition_metadata.jump_target_when_false
                    != condition_metadata.jump_target_when_null
            {
                // If all components match, jump to label_ok; otherwise skip to next RHS item
                let skip_label = program.allocate_label();
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff);
                    if j < lhs_arity - 1 {
                        program.emit_insn(Insn::Ne {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: skip_label,
                            flags,
                            collation,
                        });
                    } else {
                        program.emit_insn(Insn::Eq {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: label_ok,
                            flags,
                            collation,
                        });
                    }
                }
                program.preassign_label_to_next_insn(skip_label);
            } else {
                // Last condition, simple case: jump to false if any component doesn't match
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff).jump_if_null();
                    program.emit_insn(Insn::Ne {
                        lhs: lhs_reg + j,
                        rhs: rhs_reg + j,
                        target_pc: condition_metadata.jump_target_when_false,
                        flags,
                        collation,
                    });
                }
            }
        }
    }

    if check_null_reg != 0 {
        program.emit_insn(Insn::IsNull {
            reg: check_null_reg,
            target_pc: condition_metadata.jump_target_when_null,
        });
        program.emit_insn(Insn::Goto {
            target_pc: condition_metadata.jump_target_when_false,
        });
    }

    // We do not know which instruction comes next, so attach the label to the
    // execution flow rather than to a specific instruction. The optimizer can
    // move a register assignment into the constant section, in which case the
    // label must follow the current translation unit instead.
    program.preassign_label_to_next_insn(label_ok);

    // by default if IN expression is true we just continue to the next instruction
    if condition_metadata.jump_if_condition_is_true {
        program.emit_insn(Insn::Goto {
            target_pc: condition_metadata.jump_target_when_true,
        });
    }
    // todo: deallocate check_null_reg

    Ok(())
}
