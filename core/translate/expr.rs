use tracing::{instrument, Level};
use turso_sqlite3_parser::ast::{self, Expr, UnaryOperator};

use super::emitter::Resolver;
use super::optimizer::Optimizable;
use super::plan::TableReferences;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{Affinity, Table, Type};
use crate::util::{exprs_are_equivalent, parse_numeric_literal};
use crate::vdbe::builder::CursorKey;
use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{CmpInsFlags, Insn},
    BranchOffset,
};
use crate::{Result, Value};

use super::collate::CollationSeq;

#[derive(Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
}

#[instrument(skip_all, level = Level::INFO)]
fn emit_cond_jump(program: &mut ProgramBuilder, cond_meta: ConditionMetadata, reg: usize) {
    if cond_meta.jump_if_condition_is_true {
        program.emit_insn(Insn::If {
            reg,
            target_pc: cond_meta.jump_target_when_true,
            jump_if_null: false,
        });
    } else {
        program.emit_insn(Insn::IfNot {
            reg,
            target_pc: cond_meta.jump_target_when_false,
            jump_if_null: true,
        });
    }
}

macro_rules! expect_arguments_exact {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() != $expected_arguments {
                crate::bail_parse_error!(
                    "{} function called with not exactly {} arguments",
                    $func.to_string(),
                    $expected_arguments,
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };

        args
    }};
}

macro_rules! expect_arguments_max {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() > $expected_arguments {
                crate::bail_parse_error!(
                    "{} function called with more than {} arguments",
                    $func.to_string(),
                    $expected_arguments,
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };

        args
    }};
}

macro_rules! expect_arguments_min {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() < $expected_arguments {
                crate::bail_parse_error!(
                    "{} function with less than {} arguments",
                    $func.to_string(),
                    $expected_arguments
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };
        args
    }};
}

#[allow(unused_macros)]
macro_rules! expect_arguments_even {
    (
        $args:expr,
        $func:ident
    ) => {{
        let args = $args.as_deref().unwrap_or_default();
        if args.len() % 2 != 0 {
            crate::bail_parse_error!(
                "{} function requires an even number of arguments",
                $func.to_string()
            );
        };
        // The only function right now that requires an even number is `json_object` and it allows
        // to have no arguments, so thats why in this macro we do not bail with the `function with no arguments` error
        args
    }};
}

#[instrument(skip(program, referenced_tables, expr, resolver), level = Level::INFO)]
pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
) -> Result<()> {
    match expr {
        ast::Expr::Between { .. } => {
            unreachable!("expression should have been rewritten in optmizer")
        }
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            // In a binary AND, never jump to the parent 'jump_target_when_true' label on the first condition, because
            // the second condition MUST also be true. Instead we instruct the child expression to jump to a local
            // true label.
            let jump_target_when_true = program.allocate_label();
            translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true,
                    ..condition_metadata
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(jump_target_when_true);
            translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                condition_metadata,
                resolver,
            )?;
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            // In a binary OR, never jump to the parent 'jump_target_when_false' label on the first condition, because
            // the second condition CAN also be true. Instead we instruct the child expression to jump to a local
            // false label.
            let jump_target_when_false = program.allocate_label();
            translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_false,
                    ..condition_metadata
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(jump_target_when_false);
            translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                condition_metadata,
                resolver,
            )?;
        }
        ast::Expr::Binary(_, _, _) => {
            let result_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, result_reg, resolver)?;
            emit_cond_jump(program, condition_metadata, result_reg);
        }
        ast::Expr::Literal(_)
        | ast::Expr::Cast { .. }
        | ast::Expr::FunctionCall { .. }
        | ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Case { .. } => {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
        }
        ast::Expr::InList { lhs, not, rhs } => {
            // lhs is e.g. a column reference
            // rhs is an Option<Vec<Expr>>
            // If rhs is None, it means the IN expression is always false, i.e. tbl.id IN ().
            // If rhs is Some, it means the IN expression has a list of values to compare against, e.g. tbl.id IN (1, 2, 3).
            //
            // The IN expression is equivalent to a series of OR expressions.
            // For example, `a IN (1, 2, 3)` is equivalent to `a = 1 OR a = 2 OR a = 3`.
            // The NOT IN expression is equivalent to a series of AND expressions.
            // For example, `a NOT IN (1, 2, 3)` is equivalent to `a != 1 AND a != 2 AND a != 3`.
            //
            // SQLite typically optimizes IN expressions to use a binary search on an ephemeral index if there are many values.
            // For now we don't have the plumbing to do that, so we'll just emit a series of comparisons,
            // which is what SQLite also does for small lists of values.
            // TODO: Let's refactor this later to use a more efficient implementation conditionally based on the number of values.

            if rhs.is_none() {
                // If rhs is None, IN expressions are always false and NOT IN expressions are always true.
                if *not {
                    // On a trivially true NOT IN () expression we can only jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'; otherwise me must fall through.
                    // This is because in a more complex condition we might need to evaluate the rest of the condition.
                    // Note that we are already breaking up our WHERE clauses into a series of terms at "AND" boundaries, so right now we won't be running into cases where jumping on true would be incorrect,
                    // but once we have e.g. parenthesization and more complex conditions, not having this 'if' here would introduce a bug.
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn(Insn::Goto {
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    }
                } else {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_false,
                    });
                }
                return Ok(());
            }

            // The left hand side only needs to be evaluated once we have a list of values to compare against.
            let lhs_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), lhs, lhs_reg, resolver)?;

            let rhs = rhs.as_ref().unwrap();

            // The difference between a local jump and an "upper level" jump is that for example in this case:
            // WHERE foo IN (1,2,3) OR bar = 5,
            // we can immediately jump to the 'jump_target_when_true' label of the ENTIRE CONDITION if foo = 1, foo = 2, or foo = 3 without evaluating the bar = 5 condition.
            // This is why in Binary-OR expressions we set jump_if_condition_is_true to true for the first condition.
            // However, in this example:
            // WHERE foo IN (1,2,3) AND bar = 5,
            // we can't jump to the 'jump_target_when_true' label of the entire condition foo = 1, foo = 2, or foo = 3, because we still need to evaluate the bar = 5 condition later.
            // This is why in that case we just jump over the rest of the IN conditions in this "local" branch which evaluates the IN condition.
            let jump_target_when_true = if condition_metadata.jump_if_condition_is_true {
                condition_metadata.jump_target_when_true
            } else {
                program.allocate_label()
            };

            if !*not {
                // If it's an IN expression, we need to jump to the 'jump_target_when_true' label if any of the conditions are true.
                for (i, expr) in rhs.iter().enumerate() {
                    let rhs_reg = program.alloc_register();
                    let last_condition = i == rhs.len() - 1;
                    let _ =
                        translate_expr(program, Some(referenced_tables), expr, rhs_reg, resolver)?;
                    // If this is not the last condition, we need to jump to the 'jump_target_when_true' label if the condition is true.
                    if !last_condition {
                        program.emit_insn(Insn::Eq {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: jump_target_when_true,
                            flags: CmpInsFlags::default(),
                            collation: program.curr_collation(),
                        });
                    } else {
                        // If this is the last condition, we need to jump to the 'jump_target_when_false' label if there is no match.
                        program.emit_insn(Insn::Ne {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                            flags: CmpInsFlags::default().jump_if_null(),
                            collation: program.curr_collation(),
                        });
                    }
                }
                // If we got here, then the last condition was a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_true,
                    });
                }
            } else {
                // If it's a NOT IN expression, we need to jump to the 'jump_target_when_false' label if any of the conditions are true.
                for expr in rhs.iter() {
                    let rhs_reg = program.alloc_register();
                    let _ =
                        translate_expr(program, Some(referenced_tables), expr, rhs_reg, resolver)?;
                    program.emit_insn(Insn::Eq {
                        lhs: lhs_reg,
                        rhs: rhs_reg,
                        target_pc: condition_metadata.jump_target_when_false,
                        flags: CmpInsFlags::default().jump_if_null(),
                        collation: program.curr_collation(),
                    });
                }
                // If we got here, then none of the conditions were a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_true,
                    });
                }
            }

            if !condition_metadata.jump_if_condition_is_true {
                program.preassign_label_to_next_insn(jump_target_when_true);
            }
        }
        ast::Expr::Like { not, .. } => {
            let cur_reg = program.alloc_register();
            translate_like_base(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if !*not {
                emit_cond_jump(program, condition_metadata, cur_reg);
            } else if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IfNot {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                    jump_if_null: false,
                });
            } else {
                program.emit_insn(Insn::If {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                    jump_if_null: true,
                });
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                let _ = translate_condition_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    condition_metadata,
                    resolver,
                );
            } else {
                crate::bail_parse_error!(
                    "parenthesized conditional should have exactly one expression"
                );
            }
        }
        ast::Expr::NotNull(expr) => {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::IsNull(expr) => {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::Unary(_, _) => {
            // This is an inefficient implementation for op::NOT, because translate_expr() will emit an Insn::Not,
            // and then we immediately emit an Insn::If/Insn::IfNot for the conditional jump. In reality we would not
            // like to emit the negation instruction Insn::Not at all, since we could just emit the "opposite" jump instruction
            // directly. However, using translate_expr() directly simplifies our conditional jump code for unary expressions,
            // and we'd rather be correct than maximally efficient, for now.
            let expr_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            emit_cond_jump(program, condition_metadata, expr_reg);
        }
        other => todo!("expression {:?} not implemented", other),
    }
    Ok(())
}

/// Reason why [translate_expr_no_constant_opt()] was called.
#[derive(Debug)]
pub enum NoConstantOptReason {
    /// The expression translation involves reusing register(s),
    /// so hoisting those register assignments is not safe.
    /// e.g. SELECT COALESCE(1, t.x, NULL) would overwrite 1 with NULL, which is invalid.
    RegisterReuse,
}

/// Translate an expression into bytecode via [translate_expr()], and forbid any constant values from being hoisted
/// into the beginning of the program. This is a good idea in most cases where
/// a register will end up being reused e.g. in a coroutine.
pub fn translate_expr_no_constant_opt(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    deopt_reason: NoConstantOptReason,
) -> Result<usize> {
    tracing::debug!(
        "translate_expr_no_constant_opt: expr={:?}, deopt_reason={:?}",
        expr,
        deopt_reason
    );
    let next_span_idx = program.constant_spans_next_idx();
    let translated = translate_expr(program, referenced_tables, expr, target_register, resolver)?;
    program.constant_spans_invalidate_after(next_span_idx);
    Ok(translated)
}

/// Translate an expression into bytecode.
pub fn translate_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let constant_span = if expr.is_constant(resolver) {
        if !program.constant_span_is_open() {
            Some(program.constant_span_start())
        } else {
            None
        }
    } else {
        program.constant_span_end_all();
        None
    };

    if let Some(reg) = resolver.resolve_cached_expr_reg(expr) {
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: target_register,
            amount: 0,
        });
        if let Some(span) = constant_span {
            program.constant_span_end(span);
        }
        return Ok(target_register);
    }

    match expr {
        ast::Expr::Between { .. } => {
            unreachable!("expression should have been rewritten in optmizer")
        }
        ast::Expr::Binary(e1, op, e2) => {
            // Check if both sides of the expression are equivalent and reuse the same register if so
            if exprs_are_equivalent(e1, e2) {
                let shared_reg = program.alloc_register();
                translate_expr(program, referenced_tables, e1, shared_reg, resolver)?;

                emit_binary_insn(
                    program,
                    op,
                    shared_reg,
                    shared_reg,
                    target_register,
                    e1,
                    e2,
                    referenced_tables,
                )?;
                program.reset_collation();
                Ok(target_register)
            } else {
                let e1_reg = program.alloc_registers(2);
                let e2_reg = e1_reg + 1;

                translate_expr(program, referenced_tables, e1, e1_reg, resolver)?;
                let left_collation_ctx = program.curr_collation_ctx();
                program.reset_collation();

                translate_expr(program, referenced_tables, e2, e2_reg, resolver)?;
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

                emit_binary_insn(
                    program,
                    op,
                    e1_reg,
                    e2_reg,
                    target_register,
                    e1,
                    e2,
                    referenced_tables,
                )?;
                program.reset_collation();
                Ok(target_register)
            }
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            // There's two forms of CASE, one which checks a base expression for equality
            // against the WHEN values, and returns the corresponding THEN value if it matches:
            //   CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END
            // And one which evaluates a series of boolean predicates:
            //   CASE WHEN is_good THEN 'good' WHEN is_bad THEN 'bad' ELSE 'okay' END
            // This just changes which sort of branching instruction to issue, after we
            // generate the expression if needed.
            let return_label = program.allocate_label();
            let mut next_case_label = program.allocate_label();
            // Only allocate a reg to hold the base expression if one was provided.
            // And base_reg then becomes the flag we check to see which sort of
            // case statement we're processing.
            let base_reg = base.as_ref().map(|_| program.alloc_register());
            let expr_reg = program.alloc_register();
            if let Some(base_expr) = base {
                translate_expr(
                    program,
                    referenced_tables,
                    base_expr,
                    base_reg.unwrap(),
                    resolver,
                )?;
            };
            for (when_expr, then_expr) in when_then_pairs {
                translate_expr_no_constant_opt(
                    program,
                    referenced_tables,
                    when_expr,
                    expr_reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                match base_reg {
                    // CASE 1 WHEN 0 THEN 0 ELSE 1 becomes 1==0, Ne branch to next clause
                    Some(base_reg) => program.emit_insn(Insn::Ne {
                        lhs: base_reg,
                        rhs: expr_reg,
                        target_pc: next_case_label,
                        // A NULL result is considered untrue when evaluating WHEN terms.
                        flags: CmpInsFlags::default().jump_if_null(),
                        collation: program.curr_collation(),
                    }),
                    // CASE WHEN 0 THEN 0 ELSE 1 becomes ifnot 0 branch to next clause
                    None => program.emit_insn(Insn::IfNot {
                        reg: expr_reg,
                        target_pc: next_case_label,
                        jump_if_null: true,
                    }),
                };
                // THEN...
                translate_expr_no_constant_opt(
                    program,
                    referenced_tables,
                    then_expr,
                    target_register,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.emit_insn(Insn::Goto {
                    target_pc: return_label,
                });
                // This becomes either the next WHEN, or in the last WHEN/THEN, we're
                // assured to have at least one instruction corresponding to the ELSE immediately follow.
                program.preassign_label_to_next_insn(next_case_label);
                next_case_label = program.allocate_label();
            }
            match else_expr {
                Some(expr) => {
                    translate_expr_no_constant_opt(
                        program,
                        referenced_tables,
                        expr,
                        target_register,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                // If ELSE isn't specified, it means ELSE null.
                None => {
                    program.emit_insn(Insn::Null {
                        dest: target_register,
                        dest_end: None,
                    });
                }
            };
            program.preassign_label_to_next_insn(return_label);
            Ok(target_register)
        }
        ast::Expr::Cast { expr, type_name } => {
            let type_name = type_name.as_ref().unwrap(); // TODO: why is this optional?
            let reg_expr = program.alloc_registers(2);
            translate_expr(program, referenced_tables, expr, reg_expr, resolver)?;
            program.emit_insn(Insn::String8 {
                // we make a comparison against uppercase static strs in the affinity() function,
                // so we need to make sure we're comparing against the uppercase version,
                // and it's better to do this once instead of every time we check affinity
                value: type_name.name.to_uppercase(),
                dest: reg_expr + 1,
            });
            program.mark_last_insn_constant();
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: reg_expr,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(ScalarFunc::Cast),
                    arg_count: 2,
                },
            });
            Ok(target_register)
        }
        ast::Expr::Collate(expr, collation) => {
            // First translate inner expr, then set the curr collation. If we set curr collation before,
            // it may be overwritten later by inner translate.
            translate_expr(program, referenced_tables, expr, target_register, resolver)?;
            let collation = CollationSeq::new(collation)?;
            program.set_collation(Some((collation, true)));
            Ok(target_register)
        }
        ast::Expr::DoublyQualified(_, _, _) => todo!(),
        ast::Expr::Exists(_) => todo!(),
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            filter_over: _,
            order_by: _,
        } => {
            let args_count = if let Some(args) = args { args.len() } else { 0 };
            let func_type = resolver.resolve_function(&name.0, args_count);

            if func_type.is_none() {
                crate::bail_parse_error!("unknown function {}", name.0);
            }

            let func_ctx = FuncCtx {
                func: func_type.unwrap(),
                arg_count: args_count,
            };

            match &func_ctx.func {
                Func::Agg(_) => {
                    crate::bail_parse_error!("misuse of aggregate function {}()", name.0)
                }
                Func::External(_) => {
                    let regs = program.alloc_registers(args_count);
                    if let Some(args) = args {
                        for (i, arg_expr) in args.iter().enumerate() {
                            translate_expr(
                                program,
                                referenced_tables,
                                arg_expr,
                                regs + i,
                                resolver,
                            )?;
                        }
                    }
                    program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: regs,
                        dest: target_register,
                        func: func_ctx,
                    });

                    Ok(target_register)
                }
                #[cfg(feature = "json")]
                Func::Json(j) => match j {
                    JsonFunc::Json | JsonFunc::Jsonb => {
                        let args = expect_arguments_exact!(args, 1, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonArray
                    | JsonFunc::JsonbArray
                    | JsonFunc::JsonExtract
                    | JsonFunc::JsonSet
                    | JsonFunc::JsonbSet
                    | JsonFunc::JsonbExtract
                    | JsonFunc::JsonReplace
                    | JsonFunc::JsonbReplace
                    | JsonFunc::JsonbRemove
                    | JsonFunc::JsonInsert
                    | JsonFunc::JsonbInsert => translate_function(
                        program,
                        args.as_deref().unwrap_or_default(),
                        referenced_tables,
                        resolver,
                        target_register,
                        func_ctx,
                    ),
                    JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                        unreachable!(
                            "These two functions are only reachable via the -> and ->> operators"
                        )
                    }
                    JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                        let args = expect_arguments_max!(args, 2, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonErrorPosition => {
                        let args = if let Some(args) = args {
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "{} function with not exactly 1 argument",
                                    j.to_string()
                                );
                            }
                            args
                        } else {
                            crate::bail_parse_error!(
                                "{} function with no arguments",
                                j.to_string()
                            );
                        };
                        let json_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], json_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: json_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    JsonFunc::JsonObject | JsonFunc::JsonbObject => {
                        let args = expect_arguments_even!(args, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonValid => translate_function(
                        program,
                        args.as_deref().unwrap_or_default(),
                        referenced_tables,
                        resolver,
                        target_register,
                        func_ctx,
                    ),
                    JsonFunc::JsonPatch | JsonFunc::JsonbPatch => {
                        let args = expect_arguments_exact!(args, 2, j);
                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonRemove => {
                        let start_reg =
                            program.alloc_registers(args.as_ref().map(|x| x.len()).unwrap_or(1));
                        if let Some(args) = args {
                            for (i, arg) in args.iter().enumerate() {
                                // register containing result of each argument expression
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    start_reg + i,
                                    resolver,
                                )?;
                            }
                        }
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    JsonFunc::JsonQuote => {
                        let args = expect_arguments_exact!(args, 1, j);
                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonPretty => {
                        let args = expect_arguments_max!(args, 2, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                },
                Func::Vector(vector_func) => match vector_func {
                    VectorFunc::Vector | VectorFunc::Vector32 => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    VectorFunc::Vector64 => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    VectorFunc::VectorExtract => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    VectorFunc::VectorDistanceCos => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: regs,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                    VectorFunc::VectorDistanceEuclidean => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: regs,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                },
                Func::Scalar(srf) => {
                    match srf {
                        ScalarFunc::Cast => {
                            unreachable!("this is always ast::Expr::Cast")
                        }
                        ScalarFunc::Changes => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with more than 0 arguments",
                                    srf
                                );
                            }
                            let start_reg = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Char => translate_function(
                            program,
                            args.as_deref().unwrap_or_default(),
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::Coalesce => {
                            let args = expect_arguments_min!(args, 2, srf);

                            // coalesce function is implemented as a series of not null checks
                            // whenever a not null check succeeds, we jump to the end of the series
                            let label_coalesce_end = program.allocate_label();
                            for (index, arg) in args.iter().enumerate() {
                                let reg = translate_expr_no_constant_opt(
                                    program,
                                    referenced_tables,
                                    arg,
                                    target_register,
                                    resolver,
                                    NoConstantOptReason::RegisterReuse,
                                )?;
                                if index < args.len() - 1 {
                                    program.emit_insn(Insn::NotNull {
                                        reg,
                                        target_pc: label_coalesce_end,
                                    });
                                }
                            }
                            program.preassign_label_to_next_insn(label_coalesce_end);

                            Ok(target_register)
                        }
                        ScalarFunc::LastInsertRowid => {
                            let regs = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Concat => {
                            let args = if let Some(args) = args {
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };
                            let mut start_reg = None;
                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                start_reg = Some(start_reg.unwrap_or(reg));
                                translate_expr(program, referenced_tables, arg, reg, resolver)?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: start_reg.unwrap(),
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::ConcatWs => {
                            let args = expect_arguments_min!(args, 2, srf);

                            let temp_register = program.alloc_register();
                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                translate_expr(program, referenced_tables, arg, reg, resolver)?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: temp_register + 1,
                                dest: temp_register,
                                func: func_ctx,
                            });

                            program.emit_insn(Insn::Copy {
                                src_reg: temp_register,
                                dst_reg: target_register,
                                amount: 1,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::IfNull => {
                            let args = match args {
                                Some(args) if args.len() == 2 => args,
                                Some(_) => crate::bail_parse_error!(
                                    "{} function requires exactly 2 arguments",
                                    srf.to_string()
                                ),
                                None => crate::bail_parse_error!(
                                    "{} function requires arguments",
                                    srf.to_string()
                                ),
                            };

                            let temp_reg = program.alloc_register();
                            translate_expr_no_constant_opt(
                                program,
                                referenced_tables,
                                &args[0],
                                temp_reg,
                                resolver,
                                NoConstantOptReason::RegisterReuse,
                            )?;
                            let before_copy_label = program.allocate_label();
                            program.emit_insn(Insn::NotNull {
                                reg: temp_reg,
                                target_pc: before_copy_label,
                            });

                            translate_expr_no_constant_opt(
                                program,
                                referenced_tables,
                                &args[1],
                                temp_reg,
                                resolver,
                                NoConstantOptReason::RegisterReuse,
                            )?;
                            program.resolve_label(before_copy_label, program.offset());
                            program.emit_insn(Insn::Copy {
                                src_reg: temp_reg,
                                dst_reg: target_register,
                                amount: 0,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::Iif => {
                            let args = match args {
                                Some(args) if args.len() == 3 => args,
                                _ => crate::bail_parse_error!(
                                    "{} requires exactly 3 arguments",
                                    srf.to_string()
                                ),
                            };
                            let temp_reg = program.alloc_register();
                            translate_expr_no_constant_opt(
                                program,
                                referenced_tables,
                                &args[0],
                                temp_reg,
                                resolver,
                                NoConstantOptReason::RegisterReuse,
                            )?;
                            let jump_target_when_false = program.allocate_label();
                            program.emit_insn(Insn::IfNot {
                                reg: temp_reg,
                                target_pc: jump_target_when_false,
                                jump_if_null: true,
                            });
                            translate_expr_no_constant_opt(
                                program,
                                referenced_tables,
                                &args[1],
                                target_register,
                                resolver,
                                NoConstantOptReason::RegisterReuse,
                            )?;
                            let jump_target_result = program.allocate_label();
                            program.emit_insn(Insn::Goto {
                                target_pc: jump_target_result,
                            });
                            program.preassign_label_to_next_insn(jump_target_when_false);
                            translate_expr_no_constant_opt(
                                program,
                                referenced_tables,
                                &args[2],
                                target_register,
                                resolver,
                                NoConstantOptReason::RegisterReuse,
                            )?;
                            program.preassign_label_to_next_insn(jump_target_result);
                            Ok(target_register)
                        }
                        ScalarFunc::Glob | ScalarFunc::Like => {
                            let args = if let Some(args) = args {
                                if args.len() < 2 {
                                    crate::bail_parse_error!(
                                        "{} function with less than 2 arguments",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };
                            let func_registers = program.alloc_registers(args.len());
                            for (i, arg) in args.iter().enumerate() {
                                let _ = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    func_registers + i,
                                    resolver,
                                )?;
                            }
                            program.emit_insn(Insn::Function {
                                // Only constant patterns for LIKE are supported currently, so this
                                // is always 1
                                constant_mask: 1,
                                start_reg: func_registers,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Abs
                        | ScalarFunc::Lower
                        | ScalarFunc::Upper
                        | ScalarFunc::Length
                        | ScalarFunc::OctetLength
                        | ScalarFunc::Typeof
                        | ScalarFunc::Unicode
                        | ScalarFunc::Quote
                        | ScalarFunc::RandomBlob
                        | ScalarFunc::Sign
                        | ScalarFunc::Soundex
                        | ScalarFunc::ZeroBlob => {
                            let args = expect_arguments_exact!(args, 1, srf);
                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        #[cfg(feature = "fs")]
                        ScalarFunc::LoadExtension => {
                            let args = expect_arguments_exact!(args, 1, srf);
                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Random => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with arguments",
                                    srf.to_string()
                                );
                            }
                            let regs = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Date | ScalarFunc::DateTime | ScalarFunc::JulianDay => {
                            let start_reg = program
                                .alloc_registers(args.as_ref().map(|x| x.len()).unwrap_or(1));
                            if let Some(args) = args {
                                for (i, arg) in args.iter().enumerate() {
                                    // register containing result of each argument expression
                                    translate_expr(
                                        program,
                                        referenced_tables,
                                        arg,
                                        start_reg + i,
                                        resolver,
                                    )?;
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Substr | ScalarFunc::Substring => {
                            let args = if let Some(args) = args {
                                if !(args.len() == 2 || args.len() == 3) {
                                    crate::bail_parse_error!(
                                        "{} function with wrong number of arguments",
                                        srf.to_string()
                                    )
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            let str_reg = program.alloc_register();
                            let start_reg = program.alloc_register();
                            let length_reg = program.alloc_register();
                            let str_reg = translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                str_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                start_reg,
                                resolver,
                            )?;
                            if args.len() == 3 {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    &args[2],
                                    length_reg,
                                    resolver,
                                )?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: str_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Hex => {
                            let args = if let Some(args) = args {
                                if args.len() != 1 {
                                    crate::bail_parse_error!(
                                        "hex function must have exactly 1 argument",
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("hex function with no arguments",);
                            };
                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::UnixEpoch => {
                            let mut start_reg = 0;
                            match args {
                                Some(args) if args.len() > 1 => {
                                    crate::bail_parse_error!("epoch function with > 1 arguments. Modifiers are not yet supported.");
                                }
                                Some(args) if args.len() == 1 => {
                                    let arg_reg = program.alloc_register();
                                    let _ = translate_expr(
                                        program,
                                        referenced_tables,
                                        &args[0],
                                        arg_reg,
                                        resolver,
                                    )?;
                                    start_reg = arg_reg;
                                }
                                _ => {}
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Time => {
                            let start_reg = program
                                .alloc_registers(args.as_ref().map(|x| x.len()).unwrap_or(1));
                            if let Some(args) = args {
                                for (i, arg) in args.iter().enumerate() {
                                    // register containing result of each argument expression
                                    translate_expr(
                                        program,
                                        referenced_tables,
                                        arg,
                                        start_reg + i,
                                        resolver,
                                    )?;
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::TimeDiff => {
                            let args = expect_arguments_exact!(args, 2, srf);

                            let start_reg = program.alloc_registers(2);
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                start_reg + 1,
                                resolver,
                            )?;

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::TotalChanges => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with more than 0 arguments",
                                    srf.to_string()
                                );
                            }
                            let start_reg = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Trim
                        | ScalarFunc::LTrim
                        | ScalarFunc::RTrim
                        | ScalarFunc::Round
                        | ScalarFunc::Unhex => {
                            let args = expect_arguments_max!(args, 2, srf);

                            let start_reg = program.alloc_registers(args.len());
                            for (i, arg) in args.iter().enumerate() {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    start_reg + i,
                                    resolver,
                                )?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Min => {
                            let args = if let Some(args) = args {
                                if args.is_empty() {
                                    crate::bail_parse_error!(
                                        "min function with less than one argument"
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("min function with no arguments");
                            };
                            let start_reg = program.alloc_registers(args.len());
                            for (i, arg) in args.iter().enumerate() {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    start_reg + i,
                                    resolver,
                                )?;
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Max => {
                            let args = if let Some(args) = args {
                                if args.is_empty() {
                                    crate::bail_parse_error!(
                                        "max function with less than one argument"
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("max function with no arguments");
                            };
                            let start_reg = program.alloc_registers(args.len());
                            for (i, arg) in args.iter().enumerate() {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    start_reg + i,
                                    resolver,
                                )?;
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Nullif | ScalarFunc::Instr => {
                            let args = if let Some(args) = args {
                                if args.len() != 2 {
                                    crate::bail_parse_error!(
                                        "{} function must have two argument",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            let first_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                first_reg,
                                resolver,
                            )?;
                            let second_reg = program.alloc_register();
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                second_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: first_reg,
                                dest: target_register,
                                func: func_ctx,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::SqliteVersion => {
                            if args.is_some() {
                                crate::bail_parse_error!("sqlite_version function with arguments");
                            }

                            let output_register = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: output_register,
                                dest: output_register,
                                func: func_ctx,
                            });

                            program.emit_insn(Insn::Copy {
                                src_reg: output_register,
                                dst_reg: target_register,
                                amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::SqliteSourceId => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "sqlite_source_id function with arguments"
                                );
                            }

                            let output_register = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: output_register,
                                dest: output_register,
                                func: func_ctx,
                            });

                            program.emit_insn(Insn::Copy {
                                src_reg: output_register,
                                dst_reg: target_register,
                                amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Replace => {
                            let args = if let Some(args) = args {
                                if !args.len() == 3 {
                                    crate::bail_parse_error!(
                                        "function {}() requires exactly 3 arguments",
                                        srf.to_string()
                                    )
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "function {}() requires exactly 3 arguments",
                                    srf.to_string()
                                );
                            };
                            let str_reg = program.alloc_register();
                            let pattern_reg = program.alloc_register();
                            let replacement_reg = program.alloc_register();
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                str_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                pattern_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[2],
                                replacement_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: str_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::StrfTime => {
                            let start_reg = program
                                .alloc_registers(args.as_ref().map(|x| x.len()).unwrap_or(1));
                            if let Some(args) = args {
                                for (i, arg) in args.iter().enumerate() {
                                    // register containing result of each argument expression
                                    translate_expr(
                                        program,
                                        referenced_tables,
                                        arg,
                                        start_reg + i,
                                        resolver,
                                    )?;
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Printf => translate_function(
                            program,
                            args.as_deref().unwrap_or(&[]),
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::Likely => {
                            let args = if let Some(args) = args {
                                if args.len() != 1 {
                                    crate::bail_parse_error!(
                                        "likely function must have exactly 1 argument",
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("likely function with no arguments",);
                            };
                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Likelihood => {
                            let args = if let Some(args) = args {
                                if args.len() != 2 {
                                    crate::bail_parse_error!(
                                        "likelihood() function must have exactly 2 arguments",
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("likelihood() function with no arguments",);
                            };

                            if let ast::Expr::Literal(ast::Literal::Numeric(ref value)) = args[1] {
                                if let Ok(probability) = value.parse::<f64>() {
                                    if !(0.0..=1.0).contains(&probability) {
                                        crate::bail_parse_error!(
                                            "second argument of likelihood() must be between 0.0 and 1.0",
                                        );
                                    }
                                    if !value.contains('.') {
                                        crate::bail_parse_error!(
                                            "second argument of likelihood() must be a floating point number with decimal point",
                                        );
                                    }
                                } else {
                                    crate::bail_parse_error!(
                                        "second argument of likelihood() must be a floating point constant",
                                    );
                                }
                            } else {
                                crate::bail_parse_error!(
                                    "second argument of likelihood() must be a numeric literal",
                                );
                            }

                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Copy {
                                src_reg: start_reg,
                                dst_reg: target_register,
                                amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::TableColumnsJsonArray => {
                            if args.is_none() || args.as_ref().unwrap().len() != 1 {
                                crate::bail_parse_error!(
                                    "table_columns_json_array() function must have exactly 1 argument",
                                );
                            }
                            let args = args.as_ref().unwrap();
                            let start_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::BinRecordJsonObject => {
                            if args.is_none() || args.as_ref().unwrap().len() != 2 {
                                crate::bail_parse_error!(
                                    "bin_record_json_object() function must have exactly 2 arguments",
                                );
                            }
                            let args = args.as_ref().unwrap();
                            let start_reg = program.alloc_registers(2);
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                start_reg,
                                resolver,
                            )?;
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                start_reg + 1,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                    }
                }
                Func::Math(math_func) => match math_func.arity() {
                    MathFuncArity::Nullary => {
                        if args.is_some() {
                            crate::bail_parse_error!("{} function with arguments", math_func);
                        }

                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: 0,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::Unary => {
                        let args = expect_arguments_exact!(args, 1, math_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::Binary => {
                        let args = expect_arguments_exact!(args, 2, math_func);
                        let start_reg = program.alloc_registers(2);
                        let _ = translate_expr(
                            program,
                            referenced_tables,
                            &args[0],
                            start_reg,
                            resolver,
                        )?;
                        let _ = translate_expr(
                            program,
                            referenced_tables,
                            &args[1],
                            start_reg + 1,
                            resolver,
                        )?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::UnaryOrBinary => {
                        let args = expect_arguments_max!(args, 2, math_func);

                        let regs = program.alloc_registers(args.len());
                        for (i, arg) in args.iter().enumerate() {
                            translate_expr(program, referenced_tables, arg, regs + i, resolver)?;
                        }

                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: regs,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                },
                Func::AlterTable(_) => unreachable!(),
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        ast::Expr::Id(id) => crate::bail_parse_error!(
            "no such column: {} - should this be a string literal in single-quotes?",
            id.0
        ),
        ast::Expr::Column {
            database: _,
            table: table_ref_id,
            column,
            is_rowid_alias,
        } => {
            let (index, use_covering_index) = {
                if let Some(table_reference) = referenced_tables
                    .unwrap()
                    .find_joined_table_by_internal_id(*table_ref_id)
                {
                    (
                        table_reference.op.index(),
                        table_reference.utilizes_covering_index(),
                    )
                } else {
                    (None, false)
                }
            };

            let table = referenced_tables
                .unwrap()
                .find_table_by_internal_id(*table_ref_id)
                .expect("table reference should be found");

            let Some(table_column) = table.get_column_at(*column) else {
                crate::bail_parse_error!("column index out of bounds");
            };
            // Counter intuitive but a column always needs to have a collation
            program.set_collation(Some((table_column.collation.unwrap_or_default(), false)));

            // If we are reading a column from a table, we find the cursor that corresponds to
            // the table and read the column from the cursor.
            // If we have a covering index, we don't have an open table cursor so we read from the index cursor.
            match &table {
                Table::BTree(_) => {
                    let table_cursor_id = if use_covering_index {
                        None
                    } else {
                        Some(program.resolve_cursor_id(&CursorKey::table(*table_ref_id)))
                    };
                    let index_cursor_id = index.map(|index| {
                        program.resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()))
                    });
                    if *is_rowid_alias {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::IdxRowId {
                                cursor_id: index_cursor_id,
                                dest: target_register,
                            });
                        } else if let Some(table_cursor_id) = table_cursor_id {
                            program.emit_insn(Insn::RowId {
                                cursor_id: table_cursor_id,
                                dest: target_register,
                            });
                        } else {
                            unreachable!("Either index or table cursor must be opened");
                        }
                    } else {
                        let read_cursor = if use_covering_index {
                            index_cursor_id.expect(
                                "index cursor should be opened when use_covering_index=true",
                            )
                        } else {
                            table_cursor_id.expect(
                                "table cursor should be opened when use_covering_index=false",
                            )
                        };
                        let column = if use_covering_index {
                            let index = index.expect(
                                "index cursor should be opened when use_covering_index=true",
                            );
                            index.column_table_pos_to_index_pos(*column).unwrap_or_else(|| {
                                        panic!("covering index {} does not contain column number {} of table {}", index.name, column, table_ref_id)
                                    })
                        } else {
                            *column
                        };

                        program.emit_column(read_cursor, column, target_register);
                    }
                    let Some(column) = table.get_column_at(*column) else {
                        crate::bail_parse_error!("column index out of bounds");
                    };
                    maybe_apply_affinity(column.ty, target_register, program);
                    Ok(target_register)
                }
                Table::FromClauseSubquery(from_clause_subquery) => {
                    // If we are reading a column from a subquery, we instead copy the column from the
                    // subquery's result registers.
                    program.emit_insn(Insn::Copy {
                        src_reg: from_clause_subquery
                            .result_columns_start_reg
                            .expect("Subquery result_columns_start_reg must be set")
                            + *column,
                        dst_reg: target_register,
                        amount: 0,
                    });
                    Ok(target_register)
                }
                Table::Virtual(_) => {
                    let cursor_id = program.resolve_cursor_id(&CursorKey::table(*table_ref_id));
                    program.emit_insn(Insn::VColumn {
                        cursor_id,
                        column: *column,
                        dest: target_register,
                    });
                    Ok(target_register)
                }
            }
        }
        ast::Expr::RowId {
            database: _,
            table: table_ref_id,
        } => {
            let (index, use_covering_index) = {
                if let Some(table_reference) = referenced_tables
                    .unwrap()
                    .find_joined_table_by_internal_id(*table_ref_id)
                {
                    (
                        table_reference.op.index(),
                        table_reference.utilizes_covering_index(),
                    )
                } else {
                    (None, false)
                }
            };

            if use_covering_index {
                let index =
                    index.expect("index cursor should be opened when use_covering_index=true");
                let cursor_id =
                    program.resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()));
                program.emit_insn(Insn::IdxRowId {
                    cursor_id,
                    dest: target_register,
                });
            } else {
                let cursor_id = program.resolve_cursor_id(&CursorKey::table(*table_ref_id));
                program.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: target_register,
                });
            }
            Ok(target_register)
        }
        ast::Expr::InList { .. } => todo!(),
        ast::Expr::InSelect { .. } => todo!(),
        ast::Expr::InTable { .. } => todo!(),
        ast::Expr::IsNull(expr) => {
            let reg = program.alloc_register();
            translate_expr(program, referenced_tables, expr, reg, resolver)?;
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            let label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: label,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            program.preassign_label_to_next_insn(label);
            Ok(target_register)
        }
        ast::Expr::Like { not, .. } => {
            let like_reg = if *not {
                program.alloc_register()
            } else {
                target_register
            };
            translate_like_base(program, referenced_tables, expr, like_reg, resolver)?;
            if *not {
                program.emit_insn(Insn::Not {
                    reg: like_reg,
                    dest: target_register,
                });
            }
            Ok(target_register)
        }
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                match parse_numeric_literal(val)? {
                    Value::Integer(int_value) => {
                        program.emit_insn(Insn::Integer {
                            value: int_value,
                            dest: target_register,
                        });
                    }
                    Value::Float(real_value) => {
                        program.emit_insn(Insn::Real {
                            value: real_value,
                            dest: target_register,
                        });
                    }
                    _ => unreachable!(),
                }
                Ok(target_register)
            }
            ast::Literal::String(s) => {
                program.emit_insn(Insn::String8 {
                    value: sanitize_string(s),
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::Blob(s) => {
                let bytes = s
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        // We assume that sqlite3-parser has already validated that
                        // the input is valid hex string, thus unwrap is safe.
                        let hex_byte = std::str::from_utf8(pair).unwrap();
                        u8::from_str_radix(hex_byte, 16).unwrap()
                    })
                    .collect();
                program.emit_insn(Insn::Blob {
                    value: bytes,
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::Keyword(_) => todo!(),
            ast::Literal::Null => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
                Ok(target_register)
            }
            ast::Literal::CurrentDate => {
                program.emit_insn(Insn::String8 {
                    value: datetime::exec_date(&[]).to_string(),
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::CurrentTime => {
                program.emit_insn(Insn::String8 {
                    value: datetime::exec_time(&[]).to_string(),
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::CurrentTimestamp => {
                program.emit_insn(Insn::String8 {
                    value: datetime::exec_datetime_full(&[]).to_string(),
                    dest: target_register,
                });
                Ok(target_register)
            }
        },
        ast::Expr::Name(_) => todo!(),
        ast::Expr::NotNull(expr) => {
            let reg = program.alloc_register();
            translate_expr(program, referenced_tables, expr, reg, resolver)?;
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            let label = program.allocate_label();
            program.emit_insn(Insn::NotNull {
                reg,
                target_pc: label,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            program.preassign_label_to_next_insn(label);
            Ok(target_register)
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.is_empty() {
                crate::bail_parse_error!("parenthesized expression with no arguments");
            }
            if exprs.len() == 1 {
                translate_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    target_register,
                    resolver,
                )?;
            } else {
                // Parenthesized expressions with multiple arguments are reserved for special cases
                // like `(a, b) IN ((1, 2), (3, 4))`.
                todo!("TODO: parenthesized expression with multiple arguments not yet supported");
            }
            Ok(target_register)
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before translation")
        }
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(op, expr) => match (op, expr.as_ref()) {
            (UnaryOperator::Positive, expr) => {
                translate_expr(program, referenced_tables, expr, target_register, resolver)
            }
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(numeric_value))) => {
                let numeric_value = "-".to_owned() + numeric_value;
                match parse_numeric_literal(&numeric_value)? {
                    Value::Integer(int_value) => {
                        program.emit_insn(Insn::Integer {
                            value: int_value,
                            dest: target_register,
                        });
                    }
                    Value::Float(real_value) => {
                        program.emit_insn(Insn::Real {
                            value: real_value,
                            dest: target_register,
                        });
                    }
                    _ => unreachable!(),
                }
                Ok(target_register)
            }
            (UnaryOperator::Negative, _) => {
                let value = 0;

                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, expr, reg, resolver)?;
                let zero_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value,
                    dest: zero_reg,
                });
                program.mark_last_insn_constant();
                program.emit_insn(Insn::Subtract {
                    lhs: zero_reg,
                    rhs: reg,
                    dest: target_register,
                });
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(num_val))) => {
                match parse_numeric_literal(num_val)? {
                    Value::Integer(int_value) => {
                        program.emit_insn(Insn::Integer {
                            value: !int_value,
                            dest: target_register,
                        });
                    }
                    Value::Float(real_value) => {
                        program.emit_insn(Insn::Integer {
                            value: !(real_value as i64),
                            dest: target_register,
                        });
                    }
                    _ => unreachable!(),
                }
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, _) => {
                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, expr, reg, resolver)?;
                program.emit_insn(Insn::BitNot {
                    reg,
                    dest: target_register,
                });
                Ok(target_register)
            }
            (UnaryOperator::Not, _) => {
                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, expr, reg, resolver)?;
                program.emit_insn(Insn::Not {
                    reg,
                    dest: target_register,
                });
                Ok(target_register)
            }
        },
        ast::Expr::Variable(name) => {
            let index = program.parameters.push(name);
            program.emit_insn(Insn::Variable {
                index,
                dest: target_register,
            });
            Ok(target_register)
        }
    }?;

    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }

    Ok(target_register)
}

#[allow(clippy::too_many_arguments)]
fn emit_binary_insn(
    program: &mut ProgramBuilder,
    op: &ast::Operator,
    lhs: usize,
    rhs: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
) -> Result<()> {
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, referenced_tables);
    }

    match op {
        ast::Operator::NotEquals => {
            let if_true_label = program.allocate_label();
            wrap_eval_jump_expr_zero_or_null(
                program,
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
                    flags: CmpInsFlags::default().with_affinity(affinity),
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
            program.emit_insn(Insn::Concat {
                lhs,
                rhs,
                dest: target_register,
            });
        }
        other_unimplemented => todo!("{:?}", other_unimplemented),
    }

    Ok(())
}

/// The base logic for translating LIKE and GLOB expressions.
/// The logic for handling "NOT LIKE" is different depending on whether the expression
/// is a conditional jump or not. This is why the caller handles the "NOT LIKE" behavior;
/// see [translate_condition_expr] and [translate_expr] for implementations.
fn translate_like_base(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Like {
        lhs,
        op,
        rhs,
        escape,
        ..
    } = expr
    else {
        crate::bail_parse_error!("expected Like expression");
    };
    match op {
        ast::LikeOperator::Like | ast::LikeOperator::Glob => {
            let arg_count = if escape.is_some() { 3 } else { 2 };
            let start_reg = program.alloc_registers(arg_count);
            let mut constant_mask = 0;
            translate_expr(program, referenced_tables, lhs, start_reg + 1, resolver)?;
            let _ = translate_expr(program, referenced_tables, rhs, start_reg, resolver)?;
            if arg_count == 3 {
                if let Some(escape) = escape {
                    translate_expr(program, referenced_tables, escape, start_reg + 2, resolver)?;
                }
            }
            if matches!(rhs.as_ref(), ast::Expr::Literal(_)) {
                program.mark_last_insn_constant();
                constant_mask = 1;
            }
            let func = match op {
                ast::LikeOperator::Like => ScalarFunc::Like,
                ast::LikeOperator::Glob => ScalarFunc::Glob,
                _ => unreachable!(),
            };
            program.emit_insn(Insn::Function {
                constant_mask,
                start_reg,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(func),
                    arg_count,
                },
            });
        }
        ast::LikeOperator::Match => todo!(),
        ast::LikeOperator::Regexp => todo!(),
    }

    Ok(target_register)
}

/// Emits a whole insn for a function call.
/// Assumes the number of parameters is valid for the given function.
/// Returns the target register for the function.
fn translate_function(
    program: &mut ProgramBuilder,
    args: &[ast::Expr],
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    target_register: usize,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len());
    let mut current_reg = start_reg;

    for arg in args.iter() {
        translate_expr(program, referenced_tables, arg, current_reg, resolver)?;
        current_reg += 1;
    }

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(target_register)
}

fn wrap_eval_jump_expr(
    program: &mut ProgramBuilder,
    insn: Insn,
    target_register: usize,
    if_true_label: BranchOffset,
) {
    program.emit_insn(Insn::Integer {
        value: 1, // emit True by default
        dest: target_register,
    });
    program.emit_insn(insn);
    program.emit_insn(Insn::Integer {
        value: 0, // emit False if we reach this point (no jump)
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

fn wrap_eval_jump_expr_zero_or_null(
    program: &mut ProgramBuilder,
    insn: Insn,
    target_register: usize,
    if_true_label: BranchOffset,
    e1_reg: usize,
    e2_reg: usize,
) {
    program.emit_insn(Insn::Integer {
        value: 1, // emit True by default
        dest: target_register,
    });
    program.emit_insn(insn);
    program.emit_insn(Insn::ZeroOrNull {
        rg1: e1_reg,
        rg2: e2_reg,
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

pub fn maybe_apply_affinity(col_type: Type, target_register: usize, program: &mut ProgramBuilder) {
    if col_type == Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}

/// Sanitaizes a string literal by removing single quote at front and back
/// and escaping double single quotes
pub fn sanitize_string(input: &str) -> String {
    input[1..input.len() - 1].replace("''", "'").to_string()
}

/// Returns the components of a binary expression
/// e.g. t.x = 5 -> Some((t.x, =, 5))
pub fn as_binary_components(
    expr: &ast::Expr,
) -> Result<Option<(&ast::Expr, ast::Operator, &ast::Expr)>> {
    match unwrap_parens(expr)? {
        ast::Expr::Binary(lhs, operator, rhs)
            if matches!(
                operator,
                ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::Less
                    | ast::Operator::GreaterEquals
                    | ast::Operator::LessEquals
            ) =>
        {
            Ok(Some((lhs.as_ref(), *operator, rhs.as_ref())))
        }
        _ => Ok(None),
    }
}

/// Recursively unwrap parentheses from an expression
/// e.g. (((t.x > 5))) -> t.x > 5
fn unwrap_parens(expr: &ast::Expr) -> Result<&ast::Expr> {
    match expr {
        ast::Expr::Column { .. } => Ok(expr),
        ast::Expr::Parenthesized(exprs) => match exprs.len() {
            1 => unwrap_parens(exprs.first().unwrap()),
            _ => crate::bail_parse_error!("expected single expression in parentheses"),
        },
        _ => Ok(expr),
    }
}

/// Recursively unwrap parentheses from an owned Expr.
/// Returns how many pairs of parentheses were removed.
pub fn unwrap_parens_owned(expr: ast::Expr) -> Result<(ast::Expr, usize)> {
    let mut paren_count = 0;
    match expr {
        ast::Expr::Parenthesized(mut exprs) => match exprs.len() {
            1 => {
                paren_count += 1;
                let (expr, count) = unwrap_parens_owned(exprs.pop().unwrap())?;
                paren_count += count;
                Ok((expr, paren_count))
            }
            _ => crate::bail_parse_error!("expected single expression in parentheses"),
        },
        _ => Ok((expr, paren_count)),
    }
}

pub enum WalkControl {
    Continue,     // Visit children
    SkipChildren, // Skip children but continue walking siblings
}

/// Recursively walks an immutable expression, applying a function to each sub-expression.
pub fn walk_expr<'a, F>(expr: &'a ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&'a ast::Expr) -> Result<WalkControl>,
{
    match func(expr)? {
        WalkControl::Continue => {
            match expr {
                ast::Expr::Between {
                    lhs, start, end, ..
                } => {
                    walk_expr(lhs, func)?;
                    walk_expr(start, func)?;
                    walk_expr(end, func)?;
                }
                ast::Expr::Binary(lhs, _, rhs) => {
                    walk_expr(lhs, func)?;
                    walk_expr(rhs, func)?;
                }
                ast::Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => {
                    if let Some(base_expr) = base {
                        walk_expr(base_expr, func)?;
                    }
                    for (when_expr, then_expr) in when_then_pairs {
                        walk_expr(when_expr, func)?;
                        walk_expr(then_expr, func)?;
                    }
                    if let Some(else_expr) = else_expr {
                        walk_expr(else_expr, func)?;
                    }
                }
                ast::Expr::Cast { expr, .. } => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Collate(expr, _) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Exists(_select) | ast::Expr::Subquery(_select) => {
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::FunctionCall {
                    args,
                    order_by,
                    filter_over,
                    ..
                } => {
                    if let Some(args) = args {
                        for arg in args {
                            walk_expr(arg, func)?;
                        }
                    }
                    if let Some(order_by) = order_by {
                        for sort_col in order_by {
                            walk_expr(&sort_col.expr, func)?;
                        }
                    }
                    if let Some(filter_over) = filter_over {
                        if let Some(filter_clause) = &filter_over.filter_clause {
                            walk_expr(filter_clause, func)?;
                        }
                        if let Some(over_clause) = &filter_over.over_clause {
                            match over_clause.as_ref() {
                                ast::Over::Window(window) => {
                                    if let Some(partition_by) = &window.partition_by {
                                        for part_expr in partition_by {
                                            walk_expr(part_expr, func)?;
                                        }
                                    }
                                    if let Some(order_by_clause) = &window.order_by {
                                        for sort_col in order_by_clause {
                                            walk_expr(&sort_col.expr, func)?;
                                        }
                                    }
                                    if let Some(frame_clause) = &window.frame_clause {
                                        walk_expr_frame_bound(&frame_clause.start, func)?;
                                        if let Some(end_bound) = &frame_clause.end {
                                            walk_expr_frame_bound(end_bound, func)?;
                                        }
                                    }
                                }
                                ast::Over::Name(_) => {}
                            }
                        }
                    }
                }
                ast::Expr::FunctionCallStar { filter_over, .. } => {
                    if let Some(filter_over) = filter_over {
                        if let Some(filter_clause) = &filter_over.filter_clause {
                            walk_expr(filter_clause, func)?;
                        }
                        if let Some(over_clause) = &filter_over.over_clause {
                            match over_clause.as_ref() {
                                ast::Over::Window(window) => {
                                    if let Some(partition_by) = &window.partition_by {
                                        for part_expr in partition_by {
                                            walk_expr(part_expr, func)?;
                                        }
                                    }
                                    if let Some(order_by_clause) = &window.order_by {
                                        for sort_col in order_by_clause {
                                            walk_expr(&sort_col.expr, func)?;
                                        }
                                    }
                                    if let Some(frame_clause) = &window.frame_clause {
                                        walk_expr_frame_bound(&frame_clause.start, func)?;
                                        if let Some(end_bound) = &frame_clause.end {
                                            walk_expr_frame_bound(end_bound, func)?;
                                        }
                                    }
                                }
                                ast::Over::Name(_) => {}
                            }
                        }
                    }
                }
                ast::Expr::InList { lhs, rhs, .. } => {
                    walk_expr(lhs, func)?;
                    if let Some(rhs_exprs) = rhs {
                        for expr in rhs_exprs {
                            walk_expr(expr, func)?;
                        }
                    }
                }
                ast::Expr::InSelect { lhs, rhs: _, .. } => {
                    walk_expr(lhs, func)?;
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::InTable { lhs, args, .. } => {
                    walk_expr(lhs, func)?;
                    if let Some(arg_exprs) = args {
                        for expr in arg_exprs {
                            walk_expr(expr, func)?;
                        }
                    }
                }
                ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Like {
                    lhs, rhs, escape, ..
                } => {
                    walk_expr(lhs, func)?;
                    walk_expr(rhs, func)?;
                    if let Some(esc_expr) = escape {
                        walk_expr(esc_expr, func)?;
                    }
                }
                ast::Expr::Parenthesized(exprs) => {
                    for expr in exprs {
                        walk_expr(expr, func)?;
                    }
                }
                ast::Expr::Raise(_, expr) => {
                    if let Some(raise_expr) = expr {
                        walk_expr(raise_expr, func)?;
                    }
                }
                ast::Expr::Unary(_, expr) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Id(_)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::Literal(_)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::Variable(_) => {
                    // No nested expressions
                }
            }
        }
        WalkControl::SkipChildren => return Ok(WalkControl::Continue),
    };
    Ok(WalkControl::Continue)
}

fn walk_expr_frame_bound<'a, F>(bound: &'a ast::FrameBound, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&'a ast::Expr) -> Result<WalkControl>,
{
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            walk_expr(expr, func)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(WalkControl::Continue)
}

/// Recursively walks a mutable expression, applying a function to each sub-expression.
pub fn walk_expr_mut<F>(expr: &mut ast::Expr, func: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    func(expr)?;
    match expr {
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            walk_expr_mut(lhs, func)?;
            walk_expr_mut(start, func)?;
            walk_expr_mut(end, func)?;
        }
        ast::Expr::Binary(lhs, _, rhs) => {
            walk_expr_mut(lhs, func)?;
            walk_expr_mut(rhs, func)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base_expr) = base {
                walk_expr_mut(base_expr, func)?;
            }
            for (when_expr, then_expr) in when_then_pairs {
                walk_expr_mut(when_expr, func)?;
                walk_expr_mut(then_expr, func)?;
            }
            if let Some(else_expr) = else_expr {
                walk_expr_mut(else_expr, func)?;
            }
        }
        ast::Expr::Cast { expr, .. } => {
            walk_expr_mut(expr, func)?;
        }
        ast::Expr::Collate(expr, _) => {
            walk_expr_mut(expr, func)?;
        }
        ast::Expr::Exists(_) | ast::Expr::Subquery(_) => {
            // TODO: Walk through select statements if needed
        }
        ast::Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            if let Some(args) = args {
                for arg in args {
                    walk_expr_mut(arg, func)?;
                }
            }
            if let Some(order_by) = order_by {
                for sort_col in order_by {
                    walk_expr_mut(&mut sort_col.expr, func)?;
                }
            }
            if let Some(filter_over) = filter_over {
                if let Some(filter_clause) = &mut filter_over.filter_clause {
                    walk_expr_mut(filter_clause, func)?;
                }
                if let Some(over_clause) = &mut filter_over.over_clause {
                    match over_clause.as_mut() {
                        ast::Over::Window(window) => {
                            if let Some(partition_by) = &mut window.partition_by {
                                for part_expr in partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                            }
                            if let Some(order_by_clause) = &mut window.order_by {
                                for sort_col in order_by_clause {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
                                }
                            }
                            if let Some(frame_clause) = &mut window.frame_clause {
                                walk_expr_mut_frame_bound(&mut frame_clause.start, func)?;
                                if let Some(end_bound) = &mut frame_clause.end {
                                    walk_expr_mut_frame_bound(end_bound, func)?;
                                }
                            }
                        }
                        ast::Over::Name(_) => {}
                    }
                }
            }
        }
        ast::Expr::FunctionCallStar { filter_over, .. } => {
            if let Some(filter_over) = filter_over {
                if let Some(filter_clause) = &mut filter_over.filter_clause {
                    walk_expr_mut(filter_clause, func)?;
                }
                if let Some(over_clause) = &mut filter_over.over_clause {
                    match over_clause.as_mut() {
                        ast::Over::Window(window) => {
                            if let Some(partition_by) = &mut window.partition_by {
                                for part_expr in partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                            }
                            if let Some(order_by_clause) = &mut window.order_by {
                                for sort_col in order_by_clause {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
                                }
                            }
                            if let Some(frame_clause) = &mut window.frame_clause {
                                walk_expr_mut_frame_bound(&mut frame_clause.start, func)?;
                                if let Some(end_bound) = &mut frame_clause.end {
                                    walk_expr_mut_frame_bound(end_bound, func)?;
                                }
                            }
                        }
                        ast::Over::Name(_) => {}
                    }
                }
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            walk_expr_mut(lhs, func)?;
            if let Some(rhs_exprs) = rhs {
                for expr in rhs_exprs {
                    walk_expr_mut(expr, func)?;
                }
            }
        }
        ast::Expr::InSelect { lhs, rhs: _, .. } => {
            walk_expr_mut(lhs, func)?;
            // TODO: Walk through select statements if needed
        }
        ast::Expr::InTable { lhs, args, .. } => {
            walk_expr_mut(lhs, func)?;
            if let Some(arg_exprs) = args {
                for expr in arg_exprs {
                    walk_expr_mut(expr, func)?;
                }
            }
        }
        ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
            walk_expr_mut(expr, func)?;
        }
        ast::Expr::Like {
            lhs, rhs, escape, ..
        } => {
            walk_expr_mut(lhs, func)?;
            walk_expr_mut(rhs, func)?;
            if let Some(esc_expr) = escape {
                walk_expr_mut(esc_expr, func)?;
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for expr in exprs {
                walk_expr_mut(expr, func)?;
            }
        }
        ast::Expr::Raise(_, expr) => {
            if let Some(raise_expr) = expr {
                walk_expr_mut(raise_expr, func)?;
            }
        }
        ast::Expr::Unary(_, expr) => {
            walk_expr_mut(expr, func)?;
        }
        ast::Expr::Id(_)
        | ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Literal(_)
        | ast::Expr::DoublyQualified(..)
        | ast::Expr::Name(_)
        | ast::Expr::Qualified(..)
        | ast::Expr::Variable(_) => {
            // No nested expressions
        }
    }

    Ok(())
}

fn walk_expr_mut_frame_bound<F>(bound: &mut ast::FrameBound, func: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<()>,
{
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            walk_expr_mut(expr, func)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(())
}

pub fn get_expr_affinity(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
) -> Affinity {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            if let Some(tables) = referenced_tables {
                if let Some(table_ref) = tables.find_table_by_internal_id(*table) {
                    if let Some(col) = table_ref.get_column_at(*column) {
                        return col.affinity();
                    }
                }
            }
            Affinity::Blob
        }
        ast::Expr::Cast { type_name, .. } => {
            if let Some(type_name) = type_name {
                crate::schema::affinity(&type_name.name)
            } else {
                Affinity::Blob
            }
        }
        ast::Expr::Collate(expr, _) => get_expr_affinity(expr, referenced_tables),
        // Literals have NO affinity in SQLite!
        ast::Expr::Literal(_) => Affinity::Blob, // No affinity!
        _ => Affinity::Blob,                     // This may need to change. For now this works.
    }
}

pub fn comparison_affinity(
    lhs_expr: &ast::Expr,
    rhs_expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
) -> Affinity {
    let mut aff = get_expr_affinity(lhs_expr, referenced_tables);

    aff = compare_affinity(rhs_expr, aff, referenced_tables);

    // If no affinity determined (both operands are literals), default to BLOB
    if !aff.has_affinity() {
        Affinity::Blob
    } else {
        aff
    }
}

pub fn compare_affinity(
    expr: &ast::Expr,
    other_affinity: Affinity,
    referenced_tables: Option<&TableReferences>,
) -> Affinity {
    let expr_affinity = get_expr_affinity(expr, referenced_tables);

    if expr_affinity.has_affinity() && other_affinity.has_affinity() {
        // Both sides have affinity - use numeric if either is numeric
        if expr_affinity.is_numeric() || other_affinity.is_numeric() {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    } else {
        // One or both sides have no affinity - use the one that does, or Blob if neither
        if expr_affinity.has_affinity() {
            expr_affinity
        } else if other_affinity.has_affinity() {
            other_affinity
        } else {
            Affinity::Blob
        }
    }
}
