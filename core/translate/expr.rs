use std::sync::Arc;

use tracing::{instrument, Level};
use turso_parser::ast::{self, As, Expr, SubqueryType, UnaryOperator};

use super::emitter::Resolver;
use super::optimizer::Optimizable;
use super::plan::TableReferences;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{affinity, Affinity, Table, Type};
use crate::translate::optimizer::TakeOwnership;
use crate::translate::plan::{Operation, ResultSetColumn};
use crate::translate::planner::parse_row_id;
use crate::util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal};
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
    pub jump_target_when_null: BranchOffset,
}

/// Container for register locations of values that can be referenced in RETURNING expressions
pub struct ReturningValueRegisters {
    /// Register containing the rowid/primary key
    pub rowid_register: usize,
    /// Starting register for column values (in column order)
    pub columns_start_register: usize,
    /// Number of columns available
    pub num_columns: usize,
}

#[instrument(skip_all, level = Level::DEBUG)]
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
        let args = $args;
        let args = if !args.is_empty() {
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
        let args = $args;
        let args = if !args.is_empty() {
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
        let args = $args;
        let args = if !args.is_empty() {
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
        let args = $args;
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
// todo: Check right affinities
#[instrument(skip(program, referenced_tables, resolver), level = Level::DEBUG)]
fn translate_in_list(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    rhs: &[Box<ast::Expr>],
    condition_metadata: ConditionMetadata,
    // dest if null should be in ConditionMetadata
    resolver: &Resolver,
) -> Result<()> {
    let lhs_reg = if let Expr::Parenthesized(v) = lhs {
        program.alloc_registers(v.len())
    } else {
        program.alloc_register()
    };
    let _ = translate_expr(program, referenced_tables, lhs, lhs_reg, resolver)?;
    let mut check_null_reg = 0;
    let label_ok = program.allocate_label();

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
        let rhs_reg = program.alloc_register();
        let _ = translate_expr(program, referenced_tables, expr, rhs_reg, resolver)?;

        if check_null_reg != 0 && expr.can_be_null() {
            program.emit_insn(Insn::BitAnd {
                lhs: check_null_reg,
                rhs: rhs_reg,
                dest: check_null_reg,
            });
        }

        if !last_condition
            || condition_metadata.jump_target_when_false != condition_metadata.jump_target_when_null
        {
            if lhs_reg != rhs_reg {
                program.emit_insn(Insn::Eq {
                    lhs: lhs_reg,
                    rhs: rhs_reg,
                    target_pc: label_ok,
                    // Use affinity instead
                    flags: CmpInsFlags::default(),
                    collation: program.curr_collation(),
                });
            } else {
                program.emit_insn(Insn::NotNull {
                    reg: lhs_reg,
                    target_pc: label_ok,
                });
            }
            // sqlite3VdbeChangeP5(v, zAff[0]);
        } else if lhs_reg != rhs_reg {
            program.emit_insn(Insn::Ne {
                lhs: lhs_reg,
                rhs: rhs_reg,
                target_pc: condition_metadata.jump_target_when_false,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });
        } else {
            program.emit_insn(Insn::IsNull {
                reg: lhs_reg,
                target_pc: condition_metadata.jump_target_when_false,
            });
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

    program.resolve_label(label_ok, program.offset());

    // by default if IN expression is true we just continue to the next instruction
    if condition_metadata.jump_if_condition_is_true {
        program.emit_insn(Insn::Goto {
            target_pc: condition_metadata.jump_target_when_true,
        });
    }
    // todo: deallocate check_null_reg

    Ok(())
}

#[instrument(skip(program, referenced_tables, expr, resolver), level = Level::DEBUG)]
pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
) -> Result<()> {
    match expr {
        ast::Expr::SubqueryResult { query_type, .. } => match query_type {
            SubqueryType::Exists { result_reg } => {
                emit_cond_jump(program, condition_metadata, *result_reg);
            }
            SubqueryType::In { .. } => {
                let result_reg = program.alloc_register();
                translate_expr(program, Some(referenced_tables), expr, result_reg, resolver)?;
                emit_cond_jump(program, condition_metadata, result_reg);
            }
            SubqueryType::RowValue { num_regs, .. } => {
                if *num_regs != 1 {
                    // A query like SELECT * FROM t WHERE (SELECT ...) must return a single column.
                    crate::bail_parse_error!("sub-select returns {num_regs} columns - expected 1");
                }
                let result_reg = program.alloc_register();
                translate_expr(program, Some(referenced_tables), expr, result_reg, resolver)?;
                emit_cond_jump(program, condition_metadata, result_reg);
            }
        },
        ast::Expr::Register(_) => {
            crate::bail_parse_error!("Register in WHERE clause is currently unused. Consider removing Resolver::expr_to_reg_cache and using Expr::Register instead");
        }
        ast::Expr::Collate(_, _) => {
            crate::bail_parse_error!("Collate in WHERE clause is not supported");
        }
        ast::Expr::DoublyQualified(_, _, _) | ast::Expr::Id(_) | ast::Expr::Qualified(_, _) => {
            crate::bail_parse_error!(
                "DoublyQualified/Id/Qualified should have been rewritten in optimizer"
            );
        }
        ast::Expr::Exists(_) => {
            crate::bail_parse_error!("EXISTS in WHERE clause is not supported");
        }
        ast::Expr::Subquery(_) => {
            crate::bail_parse_error!("Subquery in WHERE clause is not supported");
        }
        ast::Expr::InSelect { .. } => {
            crate::bail_parse_error!("IN (...subquery) in WHERE clause is not supported");
        }
        ast::Expr::InTable { .. } => {
            crate::bail_parse_error!("Table expression in WHERE clause is not supported");
        }
        ast::Expr::FunctionCallStar { .. } => {
            crate::bail_parse_error!("FunctionCallStar in WHERE clause is not supported");
        }
        ast::Expr::Raise(_, _) => {
            crate::bail_parse_error!("RAISE in WHERE clause is not supported");
        }
        ast::Expr::Between { .. } => {
            crate::bail_parse_error!("BETWEEN expression should have been rewritten in optmizer")
        }
        ast::Expr::Variable(_) => {
            crate::bail_parse_error!(
                "Variable as a direct predicate in WHERE clause is not supported"
            );
        }
        ast::Expr::Name(_) => {
            crate::bail_parse_error!("Name as a direct predicate in WHERE clause is not supported");
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
        ast::Expr::Binary(e1, op, e2) => {
            let result_reg = program.alloc_register();
            binary_expr_shared(
                program,
                Some(referenced_tables),
                e1,
                e2,
                op,
                result_reg,
                resolver,
                Some(condition_metadata),
                emit_binary_condition_insn,
            )?;
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
            let ConditionMetadata {
                jump_if_condition_is_true,
                jump_target_when_true,
                jump_target_when_false,
                jump_target_when_null,
            } = condition_metadata;

            // Adjust targets if `NOT IN`
            let (adjusted_metadata, not_true_label, not_false_label) = if *not {
                let not_true_label = program.allocate_label();
                let not_false_label = program.allocate_label();
                (
                    ConditionMetadata {
                        jump_if_condition_is_true,
                        jump_target_when_true: not_true_label,
                        jump_target_when_false: not_false_label,
                        jump_target_when_null,
                    },
                    Some(not_true_label),
                    Some(not_false_label),
                )
            } else {
                (condition_metadata, None, None)
            };

            translate_in_list(
                program,
                Some(referenced_tables),
                lhs,
                rhs,
                adjusted_metadata,
                resolver,
            )?;

            if *not {
                // When IN is TRUE (match found), NOT IN should be FALSE
                program.resolve_label(not_true_label.unwrap(), program.offset());
                program.emit_insn(Insn::Goto {
                    target_pc: jump_target_when_false,
                });

                // When IN is FALSE (no match), NOT IN should be TRUE
                program.resolve_label(not_false_label.unwrap(), program.offset());
                program.emit_insn(Insn::Goto {
                    target_pc: jump_target_when_true,
                });
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
            extra_amount: 0,
        });
        if let Some(span) = constant_span {
            program.constant_span_end(span);
        }
        return Ok(target_register);
    }

    match expr {
        ast::Expr::SubqueryResult {
            lhs,
            not_in,
            query_type,
            ..
        } => {
            match query_type {
                SubqueryType::Exists { result_reg } => {
                    program.emit_insn(Insn::Copy {
                        src_reg: *result_reg,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                    Ok(target_register)
                }
                SubqueryType::In { cursor_id } => {
                    // jump here when we can definitely skip the row
                    let label_skip_row = program.allocate_label();
                    // jump here when we can definitely include the row
                    let label_include_row = program.allocate_label();
                    // jump here when we need to make extra null-related checks, because sql null is the greatest thing ever
                    let label_null_rewind = program.allocate_label();
                    let label_null_checks_loop_start = program.allocate_label();
                    let label_null_checks_next = program.allocate_label();
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target_register,
                    });
                    let lhs_columns = match unwrap_parens(lhs.as_ref().unwrap())? {
                        ast::Expr::Parenthesized(exprs) => {
                            exprs.iter().map(|e| e.as_ref()).collect()
                        }
                        expr => vec![expr],
                    };
                    let lhs_column_count = lhs_columns.len();
                    let lhs_column_regs_start = program.alloc_registers(lhs_column_count);
                    for (i, lhs_column) in lhs_columns.iter().enumerate() {
                        translate_expr(
                            program,
                            referenced_tables,
                            lhs_column,
                            lhs_column_regs_start + i,
                            resolver,
                        )?;
                        if !lhs_column.is_nonnull(referenced_tables.as_ref().unwrap()) {
                            program.emit_insn(Insn::IsNull {
                                reg: lhs_column_regs_start + i,
                                target_pc: if *not_in {
                                    label_null_rewind
                                } else {
                                    label_skip_row
                                },
                            });
                        }
                    }
                    if *not_in {
                        // WHERE ... NOT IN (SELECT ...)
                        // We must skip the row if we find a match.
                        program.emit_insn(Insn::Found {
                            cursor_id: *cursor_id,
                            target_pc: label_skip_row,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                        // Ok, so Found didn't return a match.
                        // Because SQL NULL, we need do extra checks to see if we can include the row.
                        // Consider:
                        // 1. SELECT * FROM T WHERE 1 NOT IN (SELECT NULL),
                        // 2. SELECT * FROM T WHERE 1 IN (SELECT NULL) -- or anything else where the subquery evaluates to NULL.
                        // _Both_ of these queries should return nothing, because... SQL NULL.
                        // The same goes for e.g. SELECT * FROM T WHERE (1,1) NOT IN (SELECT NULL, NULL).
                        // However, it does _NOT_ apply for SELECT * FROM T WHERE (1,1) NOT IN (SELECT NULL, 1).
                        // BUT: it DOES apply for SELECT * FROM T WHERE (2,2) NOT IN ((1,1), (NULL, NULL))!!!
                        // Ergo: if the subquery result has _ANY_ tuples with all NULLs, we need to NOT include the row.
                        //
                        // So, if we didn't found a match (and hence, so far, our 'NOT IN' condition still applies),
                        // we must still rewind the subquery's ephemeral index cursor and go through ALL rows and compare each LHS column (with !=) to the corresponding column in the ephemeral index.
                        // Comparison instructions have the default behavior that if either operand is NULL, the comparison is completely skipped.
                        // That means: if we, for ANY row in the ephemeral index, get through all the != comparisons without jumping,
                        // it means our subquery result has a tuple that is exactly NULL (or (NULL, NULL) etc.),
                        // in which case we need to NOT include the row.
                        // If ALL the rows jump at one of the != comparisons, it means our subquery result has no tuples with all NULLs -> we can include the row.
                        program.preassign_label_to_next_insn(label_null_rewind);
                        program.emit_insn(Insn::Rewind {
                            cursor_id: *cursor_id,
                            pc_if_empty: label_include_row,
                        });
                        program.preassign_label_to_next_insn(label_null_checks_loop_start);
                        let column_check_reg = program.alloc_register();
                        for i in 0..lhs_column_count {
                            program.emit_insn(Insn::Column {
                                cursor_id: *cursor_id,
                                column: i,
                                dest: column_check_reg,
                                default: None,
                            });
                            program.emit_insn(Insn::Ne {
                                lhs: lhs_column_regs_start + i,
                                rhs: column_check_reg,
                                target_pc: label_null_checks_next,
                                flags: CmpInsFlags::default(),
                                collation: program.curr_collation(),
                            });
                        }
                        program.emit_insn(Insn::Goto {
                            target_pc: label_skip_row,
                        });
                        program.preassign_label_to_next_insn(label_null_checks_next);
                        program.emit_insn(Insn::Next {
                            cursor_id: *cursor_id,
                            pc_if_next: label_null_checks_loop_start,
                        })
                    } else {
                        // WHERE ... IN (SELECT ...)
                        // We can skip the row if we don't find a match
                        program.emit_insn(Insn::NotFound {
                            cursor_id: *cursor_id,
                            target_pc: label_skip_row,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                    }
                    program.preassign_label_to_next_insn(label_include_row);
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target_register,
                    });
                    program.preassign_label_to_next_insn(label_skip_row);
                    Ok(target_register)
                }
                SubqueryType::RowValue {
                    result_reg_start,
                    num_regs,
                } => {
                    program.emit_insn(Insn::Copy {
                        src_reg: *result_reg_start,
                        dst_reg: target_register,
                        extra_amount: num_regs - 1,
                    });
                    Ok(target_register)
                }
            }
        }
        ast::Expr::Between { .. } => {
            unreachable!("expression should have been rewritten in optmizer")
        }
        ast::Expr::Binary(e1, op, e2) => {
            binary_expr_shared(
                program,
                referenced_tables,
                e1,
                e2,
                op,
                target_register,
                resolver,
                None,
                emit_binary_insn,
            )?;
            Ok(target_register)
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
            translate_expr(program, referenced_tables, expr, target_register, resolver)?;
            let type_affinity = affinity(&type_name.name);
            program.emit_insn(Insn::Cast {
                reg: target_register,
                affinity: type_affinity,
            });
            Ok(target_register)
        }
        ast::Expr::Collate(expr, collation) => {
            // First translate inner expr, then set the curr collation. If we set curr collation before,
            // it may be overwritten later by inner translate.
            translate_expr(program, referenced_tables, expr, target_register, resolver)?;
            let collation = CollationSeq::new(collation.as_str())?;
            program.set_collation(Some((collation, true)));
            Ok(target_register)
        }
        ast::Expr::DoublyQualified(_, _, _) => {
            crate::bail_parse_error!("DoublyQualified should have been rewritten in optimizer")
        }
        ast::Expr::Exists(_) => {
            crate::bail_parse_error!("EXISTS is not supported in this position")
        }
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            filter_over,
            order_by: _,
        } => {
            let args_count = args.len();
            let func_type = resolver.resolve_function(name.as_str(), args_count);

            if func_type.is_none() {
                crate::bail_parse_error!("unknown function {}", name.as_str());
            }

            let func_ctx = FuncCtx {
                func: func_type.unwrap(),
                arg_count: args_count,
            };

            match &func_ctx.func {
                Func::Agg(_) => {
                    crate::bail_parse_error!(
                        "misuse of {} function {}()",
                        if filter_over.over_clause.is_some() {
                            "window"
                        } else {
                            "aggregate"
                        },
                        name.as_str()
                    )
                }
                Func::External(_) => {
                    let regs = program.alloc_registers(args_count);
                    for (i, arg_expr) in args.iter().enumerate() {
                        translate_expr(program, referenced_tables, arg_expr, regs + i, resolver)?;
                    }

                    // Use shared function call helper
                    let arg_registers: Vec<usize> = (regs..regs + args_count).collect();
                    emit_function_call(program, func_ctx, &arg_registers, target_register)?;

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
                        args,
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
                        if args.len() != 1 {
                            crate::bail_parse_error!(
                                "{} function with not exactly 1 argument",
                                j.to_string()
                            );
                        }
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
                        args,
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
                        let start_reg = program.alloc_registers(args.len().max(1));
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

                        emit_function_call(program, func_ctx, &[start_reg], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::Vector32Sparse => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;

                        emit_function_call(program, func_ctx, &[start_reg], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::Vector64 => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;

                        emit_function_call(program, func_ctx, &[start_reg], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorExtract => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;

                        emit_function_call(program, func_ctx, &[start_reg], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorDistanceCos => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;

                        emit_function_call(program, func_ctx, &[regs, regs + 1], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorDistanceL2 => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;

                        emit_function_call(program, func_ctx, &[regs, regs + 1], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorDistanceJaccard => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;

                        emit_function_call(program, func_ctx, &[regs, regs + 1], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorConcat => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let regs = program.alloc_registers(2);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;

                        emit_function_call(program, func_ctx, &[regs, regs + 1], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::VectorSlice => {
                        let args = expect_arguments_exact!(args, 3, vector_func);
                        let regs = program.alloc_registers(3);
                        translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                        translate_expr(program, referenced_tables, &args[1], regs + 1, resolver)?;
                        translate_expr(program, referenced_tables, &args[2], regs + 2, resolver)?;

                        emit_function_call(program, func_ctx, &[regs, regs + 2], target_register)?;
                        Ok(target_register)
                    }
                },
                Func::Scalar(srf) => {
                    match srf {
                        ScalarFunc::Cast => {
                            unreachable!("this is always ast::Expr::Cast")
                        }
                        ScalarFunc::Changes => {
                            if !args.is_empty() {
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
                            args,
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
                            if args.is_empty() {
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

                            let temp_register = program.alloc_registers(args.len() + 1);
                            for (i, arg) in args.iter().enumerate() {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    temp_register + i + 1,
                                    resolver,
                                )?;
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
                                extra_amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::IfNull => {
                            if args.len() != 2 {
                                crate::bail_parse_error!(
                                    "{} function requires exactly 2 arguments",
                                    srf.to_string()
                                );
                            }

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
                                extra_amount: 0,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::Iif => {
                            let args = expect_arguments_min!(args, 2, srf);

                            let iif_end_label = program.allocate_label();
                            let condition_reg = program.alloc_register();

                            for pair in args.chunks_exact(2) {
                                let condition_expr = &pair[0];
                                let value_expr = &pair[1];
                                let next_check_label = program.allocate_label();

                                translate_expr_no_constant_opt(
                                    program,
                                    referenced_tables,
                                    condition_expr,
                                    condition_reg,
                                    resolver,
                                    NoConstantOptReason::RegisterReuse,
                                )?;

                                program.emit_insn(Insn::IfNot {
                                    reg: condition_reg,
                                    target_pc: next_check_label,
                                    jump_if_null: true,
                                });

                                translate_expr_no_constant_opt(
                                    program,
                                    referenced_tables,
                                    value_expr,
                                    target_register,
                                    resolver,
                                    NoConstantOptReason::RegisterReuse,
                                )?;
                                program.emit_insn(Insn::Goto {
                                    target_pc: iif_end_label,
                                });

                                program.preassign_label_to_next_insn(next_check_label);
                            }

                            if args.len() % 2 != 0 {
                                translate_expr_no_constant_opt(
                                    program,
                                    referenced_tables,
                                    args.last().unwrap(),
                                    target_register,
                                    resolver,
                                    NoConstantOptReason::RegisterReuse,
                                )?;
                            } else {
                                program.emit_insn(Insn::Null {
                                    dest: target_register,
                                    dest_end: None,
                                });
                            }

                            program.preassign_label_to_next_insn(iif_end_label);
                            Ok(target_register)
                        }

                        ScalarFunc::Glob | ScalarFunc::Like => {
                            if args.len() < 2 {
                                crate::bail_parse_error!(
                                    "{} function with less than 2 arguments",
                                    srf.to_string()
                                );
                            }
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
                        #[cfg(not(target_family = "wasm"))]
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
                            if !args.is_empty() {
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
                            let start_reg = program.alloc_registers(args.len().max(1));
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
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Substr | ScalarFunc::Substring => {
                            if !(args.len() == 2 || args.len() == 3) {
                                crate::bail_parse_error!(
                                    "{} function with wrong number of arguments",
                                    srf.to_string()
                                )
                            }

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
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "hex function must have exactly 1 argument",
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
                            if args.len() > 1 {
                                crate::bail_parse_error!("epoch function with > 1 arguments. Modifiers are not yet supported.");
                            }
                            if args.len() == 1 {
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
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Time => {
                            let start_reg = program.alloc_registers(args.len().max(1));
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
                            if !args.is_empty() {
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
                            if args.is_empty() {
                                crate::bail_parse_error!("min function with no arguments");
                            }
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
                            if args.is_empty() {
                                crate::bail_parse_error!("min function with no arguments");
                            }
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
                            if args.len() != 2 {
                                crate::bail_parse_error!(
                                    "{} function must have two argument",
                                    srf.to_string()
                                );
                            }

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
                        ScalarFunc::SqliteVersion
                        | ScalarFunc::TursoVersion
                        | ScalarFunc::SqliteSourceId => {
                            if !args.is_empty() {
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
                                extra_amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Replace => {
                            if !args.len() == 3 {
                                crate::bail_parse_error!(
                                    "function {}() requires exactly 3 arguments",
                                    srf.to_string()
                                )
                            }

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
                            let start_reg = program.alloc_registers(args.len().max(1));
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
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::Likely => {
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "likely function must have exactly 1 argument",
                                );
                            }
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                target_register,
                                resolver,
                            )?;
                            Ok(target_register)
                        }
                        ScalarFunc::Likelihood => {
                            if args.len() != 2 {
                                crate::bail_parse_error!(
                                    "likelihood() function must have exactly 2 arguments",
                                );
                            }

                            if let ast::Expr::Literal(ast::Literal::Numeric(ref value)) =
                                args[1].as_ref()
                            {
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
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                target_register,
                                resolver,
                            )?;
                            Ok(target_register)
                        }
                        ScalarFunc::TableColumnsJsonArray => {
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "table_columns_json_array() function must have exactly 1 argument",
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
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::BinRecordJsonObject => {
                            if args.len() != 2 {
                                crate::bail_parse_error!(
                                    "bin_record_json_object() function must have exactly 2 arguments",
                                );
                            }
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
                        ScalarFunc::Attach => {
                            // ATTACH is handled by the attach.rs module, not here
                            crate::bail_parse_error!(
                                "ATTACH should be handled at statement level, not as expression"
                            );
                        }
                        ScalarFunc::Detach => {
                            // DETACH is handled by the attach.rs module, not here
                            crate::bail_parse_error!(
                                "DETACH should be handled at statement level, not as expression"
                            );
                        }
                        ScalarFunc::Unlikely => {
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "Unlikely function must have exactly 1 argument",
                                );
                            }
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                target_register,
                                resolver,
                            )?;

                            Ok(target_register)
                        }
                    }
                }
                Func::Math(math_func) => match math_func.arity() {
                    MathFuncArity::Nullary => {
                        if !args.is_empty() {
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
        ast::Expr::FunctionCallStar { name, filter_over } => {
            // Handle func(*) syntax as a function call with 0 arguments
            // This is equivalent to func() for functions that accept 0 arguments
            let args_count = 0;
            let func_type = resolver.resolve_function(name.as_str(), args_count);

            if func_type.is_none() {
                crate::bail_parse_error!("unknown function {}", name.as_str());
            }

            let func_ctx = FuncCtx {
                func: func_type.unwrap(),
                arg_count: args_count,
            };

            // Check if this function supports the (*) syntax by verifying it can be called with 0 args
            match &func_ctx.func {
                Func::Agg(_) => {
                    crate::bail_parse_error!(
                        "misuse of {} function {}(*)",
                        if filter_over.over_clause.is_some() {
                            "window"
                        } else {
                            "aggregate"
                        },
                        name.as_str()
                    )
                }
                // For supported functions, delegate to the existing FunctionCall logic
                // by creating a synthetic FunctionCall with empty args
                _ => {
                    let synthetic_call = ast::Expr::FunctionCall {
                        name: name.clone(),
                        distinctness: None,
                        args: vec![], // Empty args for func(*)
                        filter_over: filter_over.clone(),
                        order_by: vec![], // Empty order_by for func(*)
                    };

                    // Recursively call translate_expr with the synthetic function call
                    translate_expr(
                        program,
                        referenced_tables,
                        &synthetic_call,
                        target_register,
                        resolver,
                    )
                }
            }
        }
        ast::Expr::Id(id) => {
            // Treat double-quoted identifiers as string literals (SQLite compatibility)
            program.emit_insn(Insn::String8 {
                value: id.as_str().to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Expr::Column {
            database: _,
            table: table_ref_id,
            column,
            is_rowid_alias,
        } => {
            let (index, index_method, use_covering_index) = {
                if let Some(table_reference) = referenced_tables
                    .unwrap()
                    .find_joined_table_by_internal_id(*table_ref_id)
                {
                    (
                        table_reference.op.index(),
                        if let Operation::IndexMethodQuery(index_method) = &table_reference.op {
                            Some(index_method)
                        } else {
                            None
                        },
                        table_reference.utilizes_covering_index(),
                    )
                } else {
                    (None, None, false)
                }
            };
            let use_index_method = index_method.and_then(|m| m.covered_columns.get(column));

            let (is_from_outer_query_scope, table) = referenced_tables
                .unwrap()
                .find_table_by_internal_id(*table_ref_id)
                .unwrap_or_else(|| {
                    unreachable!(
                        "table reference should be found: {} (referenced_tables: {:?})",
                        table_ref_id, referenced_tables
                    )
                });

            if use_index_method.is_none() {
                let Some(table_column) = table.get_column_at(*column) else {
                    crate::bail_parse_error!("column index out of bounds");
                };
                // Counter intuitive but a column always needs to have a collation
                program.set_collation(Some((table_column.collation(), false)));
            }

            // If we are reading a column from a table, we find the cursor that corresponds to
            // the table and read the column from the cursor.
            // If we have a covering index, we don't have an open table cursor so we read from the index cursor.
            match &table {
                Table::BTree(_) => {
                    let (table_cursor_id, index_cursor_id) = if is_from_outer_query_scope {
                        // Due to a limitation of our translation system, a subquery that references an outer query table
                        // cannot know whether a table cursor, index cursor, or both were opened for that table reference.
                        // Hence: currently we first try to resolve a table cursor, and if that fails,
                        // we resolve an index cursor.
                        if let Some(table_cursor_id) =
                            program.resolve_cursor_id_safe(&CursorKey::table(*table_ref_id))
                        {
                            (Some(table_cursor_id), None)
                        } else {
                            (
                                None,
                                Some(program.resolve_any_index_cursor_id_for_table(*table_ref_id)),
                            )
                        }
                    } else {
                        let table_cursor_id = if use_covering_index || use_index_method.is_some() {
                            None
                        } else {
                            Some(program.resolve_cursor_id(&CursorKey::table(*table_ref_id)))
                        };
                        let index_cursor_id = index.map(|index| {
                            program
                                .resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()))
                        });
                        (table_cursor_id, index_cursor_id)
                    };

                    if let Some(custom_module_column) = use_index_method {
                        program.emit_column_or_rowid(
                            index_cursor_id.unwrap(),
                            *custom_module_column,
                            target_register,
                        );
                    } else {
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
                            let read_from_index = if is_from_outer_query_scope {
                                index_cursor_id.is_some()
                            } else {
                                use_covering_index
                            };
                            let read_cursor = if read_from_index {
                                index_cursor_id.expect("index cursor should be opened")
                            } else {
                                table_cursor_id.expect("table cursor should be opened")
                            };
                            let column = if read_from_index {
                                let index = program.resolve_index_for_cursor_id(
                                    index_cursor_id.expect("index cursor should be opened"),
                                );
                                index
                                    .column_table_pos_to_index_pos(*column)
                                    .unwrap_or_else(|| {
                                        panic!(
                                        "index {} does not contain column number {} of table {}",
                                        index.name, column, table_ref_id
                                    )
                                    })
                            } else {
                                *column
                            };

                            program.emit_column_or_rowid(read_cursor, column, target_register);
                        }
                        let Some(column) = table.get_column_at(*column) else {
                            crate::bail_parse_error!("column index out of bounds");
                        };
                        maybe_apply_affinity(column.ty(), target_register, program);
                    }
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
                        extra_amount: 0,
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
        ast::Expr::InList { lhs, rhs, not } => {
            // Following SQLite's approach: use the same core logic as conditional InList,
            // but wrap it with appropriate expression context handling
            let result_reg = target_register;

            let dest_if_false = program.allocate_label();
            let dest_if_null = program.allocate_label();
            let dest_if_true = program.allocate_label();

            // Ideally we wouldn't need a tmp register, but currently if an IN expression
            // is used inside an aggregator the target_register is cleared on every iteration,
            // losing the state of the aggregator.
            let tmp = program.alloc_register();
            program.emit_no_constant_insn(Insn::Null {
                dest: tmp,
                dest_end: None,
            });

            translate_in_list(
                program,
                referenced_tables,
                lhs,
                rhs,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: dest_if_true,
                    jump_target_when_false: dest_if_false,
                    jump_target_when_null: dest_if_null,
                },
                resolver,
            )?;

            // condition true: set result to 1
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: tmp,
            });

            // False path: set result to 0
            program.resolve_label(dest_if_false, program.offset());

            // Force integer conversion with AddImm 0
            program.emit_insn(Insn::AddImm {
                register: tmp,
                value: 0,
            });

            if *not {
                program.emit_insn(Insn::Not {
                    reg: tmp,
                    dest: tmp,
                });
            }
            program.resolve_label(dest_if_null, program.offset());
            program.emit_insn(Insn::Copy {
                src_reg: tmp,
                dst_reg: result_reg,
                extra_amount: 0,
            });
            Ok(result_reg)
        }
        ast::Expr::InSelect { .. } => {
            crate::bail_parse_error!("IN (...subquery) is not supported in this position")
        }
        ast::Expr::InTable { .. } => {
            crate::bail_parse_error!("Table expression is not supported in this position")
        }
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
        ast::Expr::Literal(lit) => emit_literal(program, lit, target_register),
        ast::Expr::Name(_) => {
            crate::bail_parse_error!("ast::Expr::Name is not supported in this position")
        }
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
                crate::bail_parse_error!(
                    "TODO: parenthesized expression with multiple arguments not yet supported"
                );
            }
            Ok(target_register)
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before translation")
        }
        ast::Expr::Raise(_, _) => crate::bail_parse_error!("RAISE is not supported"),
        ast::Expr::Subquery(_) => {
            crate::bail_parse_error!("Subquery is not supported in this position")
        }
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
        ast::Expr::Register(src_reg) => {
            // For DBSP expression compilation: copy from source register to target
            program.emit_insn(Insn::Copy {
                src_reg: *src_reg,
                dst_reg: target_register,
                extra_amount: 0,
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
fn binary_expr_shared(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    target_register: usize,
    resolver: &Resolver,
    condition_metadata: Option<ConditionMetadata>,
    emit_fn: impl Fn(
        &mut ProgramBuilder,
        &ast::Operator,
        usize,      // left reg
        usize,      // right reg
        usize,      // target reg
        &ast::Expr, // left expr
        &ast::Expr, // right expr
        Option<&TableReferences>,
        Option<ConditionMetadata>,
    ) -> Result<()>,
) -> Result<usize> {
    // Check if both sides of the expression are equivalent and reuse the same register if so
    if exprs_are_equivalent(e1, e2) {
        let shared_reg = program.alloc_register();
        translate_expr(program, referenced_tables, e1, shared_reg, resolver)?;

        emit_fn(
            program,
            op,
            shared_reg,
            shared_reg,
            target_register,
            e1,
            e2,
            referenced_tables,
            condition_metadata,
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
                (Some((c_left, from_collate_left)), None) => Some((c_left, from_collate_left)),
                (None, Some((c_right, from_collate_right))) => Some((c_right, from_collate_right)),
                (Some((c_left, from_collate_left)), Some((_, false))) => {
                    Some((c_left, from_collate_left))
                }
                _ => None,
            }
        };
        program.set_collation(collation_ctx);

        emit_fn(
            program,
            op,
            e1_reg,
            e2_reg,
            target_register,
            e1,
            e2,
            referenced_tables,
            condition_metadata,
        )?;
        program.reset_collation();
        Ok(target_register)
    }
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
    _: Option<ConditionMetadata>,
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

#[allow(clippy::too_many_arguments)]
fn emit_binary_condition_insn(
    program: &mut ProgramBuilder,
    op: &ast::Operator,
    lhs: usize,
    rhs: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    condition_metadata: Option<ConditionMetadata>,
) -> Result<()> {
    let condition_metadata = condition_metadata
        .expect("condition metadata must be provided for emit_binary_insn_conditional");
    let mut affinity = Affinity::Blob;
    if op.is_comparison() {
        affinity = comparison_affinity(lhs_expr, rhs_expr, referenced_tables);
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

    // Similarly, we "jump if NULL" only when we intend to jump if the condition is false.
    let flags = if condition_metadata.jump_if_condition_is_true {
        CmpInsFlags::default().with_affinity(affinity)
    } else {
        CmpInsFlags::default()
            .with_affinity(affinity)
            .jump_if_null()
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
            program.emit_insn(Insn::Concat {
                lhs,
                rhs,
                dest: target_register,
            });
            eval_result(program, target_register);
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
        ast::LikeOperator::Match => crate::bail_parse_error!("MATCH in LIKE is not supported"),
        ast::LikeOperator::Regexp => crate::bail_parse_error!("REGEXP in LIKE is not supported"),
    }

    Ok(target_register)
}

/// Emits a whole insn for a function call.
/// Assumes the number of parameters is valid for the given function.
/// Returns the target register for the function.
fn translate_function(
    program: &mut ProgramBuilder,
    args: &[Box<ast::Expr>],
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

/// Sanitizes a string literal by removing single quote at front and back
/// and escaping double single quotes
pub fn sanitize_string(input: &str) -> String {
    let inner = &input[1..input.len() - 1];

    // Fast path, avoid replacing.
    if !inner.contains("''") {
        return inner.to_string();
    }

    inner.replace("''", "'")
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
pub fn unwrap_parens(expr: &ast::Expr) -> Result<&ast::Expr> {
    match expr {
        ast::Expr::Column { .. } => Ok(expr),
        ast::Expr::Parenthesized(exprs) => match exprs.len() {
            1 => unwrap_parens(exprs.first().unwrap()),
            _ => Ok(expr), // If the expression is e.g. (x, y), as used in e.g. (x, y) IN (SELECT ...), return as is.
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
                let (expr, count) = unwrap_parens_owned(*exprs.pop().unwrap().clone())?;
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
                ast::Expr::SubqueryResult { lhs, .. } => {
                    if let Some(lhs) = lhs {
                        walk_expr(lhs, func)?;
                    }
                }
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
                    for arg in args {
                        walk_expr(arg, func)?;
                    }
                    for sort_col in order_by {
                        walk_expr(&sort_col.expr, func)?;
                    }
                    if let Some(filter_clause) = &filter_over.filter_clause {
                        walk_expr(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &window.partition_by {
                                    walk_expr(part_expr, func)?;
                                }
                                for sort_col in &window.order_by {
                                    walk_expr(&sort_col.expr, func)?;
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
                ast::Expr::FunctionCallStar { filter_over, .. } => {
                    if let Some(filter_clause) = &filter_over.filter_clause {
                        walk_expr(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &window.partition_by {
                                    walk_expr(part_expr, func)?;
                                }
                                for sort_col in &window.order_by {
                                    walk_expr(&sort_col.expr, func)?;
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
                ast::Expr::InList { lhs, rhs, .. } => {
                    walk_expr(lhs, func)?;
                    for expr in rhs {
                        walk_expr(expr, func)?;
                    }
                }
                ast::Expr::InSelect { lhs, rhs: _, .. } => {
                    walk_expr(lhs, func)?;
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::InTable { lhs, args, .. } => {
                    walk_expr(lhs, func)?;
                    for expr in args {
                        walk_expr(expr, func)?;
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
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_) => {
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

pub struct ParamState {
    // flag which allow or forbid usage of parameters during translation of AST to the program
    //
    // for example, parameters are not allowed in the partial index definition
    // so tursodb set allowed to false when it parsed WHERE clause of partial index definition
    pub allowed: bool,
}

impl Default for ParamState {
    fn default() -> Self {
        Self { allowed: true }
    }
}
impl ParamState {
    pub fn is_valid(&self) -> bool {
        self.allowed
    }
    pub fn disallow() -> Self {
        Self { allowed: false }
    }
}

/// The precedence of binding identifiers to columns.
///
/// TryResultColumnsFirst means that result columns (e.g. SELECT x AS y, ...) take precedence over canonical columns (e.g. SELECT x, y AS z, ...). This is the default behavior.
///
/// TryCanonicalColumnsFirst means that canonical columns take precedence over result columns. This is used for e.g. WHERE clauses.
///
/// ResultColumnsNotAllowed means that referring to result columns is not allowed. This is used e.g. for DML statements.
///
/// AllowUnboundIdentifiers means that unbound identifiers are allowed. This is used for INSERT ... ON CONFLICT DO UPDATE SET ... where binding is handled later than this phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingBehavior {
    TryResultColumnsFirst,
    TryCanonicalColumnsFirst,
    ResultColumnsNotAllowed,
    AllowUnboundIdentifiers,
}

/// Rewrite ast::Expr in place, binding Column references/rewriting Expr::Id -> Expr::Column
/// using the provided TableReferences, and replacing anonymous parameters with internal named
/// ones
pub fn bind_and_rewrite_expr<'a>(
    top_level_expr: &mut ast::Expr,
    mut referenced_tables: Option<&'a mut TableReferences>,
    result_columns: Option<&'a [ResultSetColumn]>,
    connection: &'a Arc<crate::Connection>,
    param_state: &mut ParamState,
    binding_behavior: BindingBehavior,
) -> Result<()> {
    walk_expr_mut(
        top_level_expr,
        &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                ast::Expr::Variable(_) => {
                    if !param_state.is_valid() {
                        crate::bail_parse_error!("Parameters are not allowed in this context");
                    }
                }
                ast::Expr::Between {
                    lhs,
                    not,
                    start,
                    end,
                } => {
                    let (lower_op, upper_op) = if *not {
                        (ast::Operator::Greater, ast::Operator::Greater)
                    } else {
                        (ast::Operator::LessEquals, ast::Operator::LessEquals)
                    };
                    let start = start.take_ownership();
                    let lhs_v = lhs.take_ownership();
                    let end = end.take_ownership();

                    let lower =
                        ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs_v.clone()));
                    let upper = ast::Expr::Binary(Box::new(lhs_v), upper_op, Box::new(end));

                    *expr = if *not {
                        ast::Expr::Binary(Box::new(lower), ast::Operator::Or, Box::new(upper))
                    } else {
                        ast::Expr::Binary(Box::new(lower), ast::Operator::And, Box::new(upper))
                    };
                }
                _ => {}
            }
            match expr {
                Expr::Id(id) => {
                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!("no such column: {}", id.as_str());
                    };
                    let normalized_id = normalize_ident(id.as_str());

                    if binding_behavior == BindingBehavior::TryResultColumnsFirst {
                        if let Some(result_columns) = result_columns {
                            for result_column in result_columns.iter() {
                                if let Some(alias) = &result_column.alias {
                                    if alias.eq_ignore_ascii_case(&normalized_id) {
                                        *expr = result_column.expr.clone();
                                        return Ok(WalkControl::Continue);
                                    }
                                }
                            }
                        }
                    }
                    let mut match_result = None;

                    // First check joined tables
                    for joined_table in referenced_tables.joined_tables().iter() {
                        let col_idx = joined_table.table.columns().iter().position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                        });
                        if col_idx.is_some() {
                            if match_result.is_some() {
                                let mut ok = false;
                                // Column name ambiguity is ok if it is in the USING clause because then it is deduplicated
                                // and the left table is used.
                                if let Some(join_info) = &joined_table.join_info {
                                    if join_info.using.iter().any(|using_col| {
                                        using_col.as_str().eq_ignore_ascii_case(&normalized_id)
                                    }) {
                                        ok = true;
                                    }
                                }
                                if !ok {
                                    crate::bail_parse_error!("Column {} is ambiguous", id.as_str());
                                }
                            } else {
                                let col =
                                    joined_table.table.columns().get(col_idx.unwrap()).unwrap();
                                match_result = Some((
                                    joined_table.internal_id,
                                    col_idx.unwrap(),
                                    col.is_rowid_alias(),
                                ));
                            }
                        // only if we haven't found a match, check for explicit rowid reference
                        } else {
                            let is_btree_table = matches!(joined_table.table, Table::BTree(_));
                            if is_btree_table {
                                if let Some(row_id_expr) = parse_row_id(
                                    &normalized_id,
                                    referenced_tables.joined_tables()[0].internal_id,
                                    || referenced_tables.joined_tables().len() != 1,
                                )? {
                                    *expr = row_id_expr;
                                    return Ok(WalkControl::Continue);
                                }
                            }
                        }
                    }

                    // Then check outer query references, if we still didn't find something.
                    // Normally finding multiple matches for a non-qualified column is an error (column x is ambiguous)
                    // but in the case of subqueries, the inner query takes precedence.
                    // For example:
                    // SELECT * FROM t WHERE x = (SELECT x FROM t2)
                    // In this case, there is no ambiguity:
                    // - x in the outer query refers to t.x,
                    // - x in the inner query refers to t2.x.
                    if match_result.is_none() {
                        for outer_ref in referenced_tables.outer_query_refs().iter() {
                            let col_idx = outer_ref.table.columns().iter().position(|c| {
                                c.name
                                    .as_ref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                            });
                            if col_idx.is_some() {
                                if match_result.is_some() {
                                    crate::bail_parse_error!("Column {} is ambiguous", id.as_str());
                                }
                                let col = outer_ref.table.columns().get(col_idx.unwrap()).unwrap();
                                match_result = Some((
                                    outer_ref.internal_id,
                                    col_idx.unwrap(),
                                    col.is_rowid_alias(),
                                ));
                            }
                        }
                    }

                    if let Some((table_id, col_idx, is_rowid_alias)) = match_result {
                        *expr = Expr::Column {
                            database: None, // TODO: support different databases
                            table: table_id,
                            column: col_idx,
                            is_rowid_alias,
                        };
                        referenced_tables.mark_column_used(table_id, col_idx);
                        return Ok(WalkControl::Continue);
                    }

                    if binding_behavior == BindingBehavior::TryCanonicalColumnsFirst {
                        if let Some(result_columns) = result_columns {
                            for result_column in result_columns.iter() {
                                if let Some(alias) = &result_column.alias {
                                    if alias.eq_ignore_ascii_case(&normalized_id) {
                                        *expr = result_column.expr.clone();
                                        return Ok(WalkControl::Continue);
                                    }
                                }
                            }
                        }
                    }

                    // SQLite behavior: Only double-quoted identifiers get fallback to string literals
                    // Single quotes are handled as literals earlier, unquoted identifiers must resolve to columns
                    if id.quoted_with('"') {
                        // Convert failed double-quoted identifier to string literal
                        *expr = Expr::Literal(ast::Literal::String(id.as_literal()));
                        return Ok(WalkControl::Continue);
                    } else {
                        // Unquoted identifiers must resolve to columns - no fallback
                        crate::bail_parse_error!("no such column: {}", id.as_str())
                    }
                }
                Expr::Qualified(tbl, id) => {
                    tracing::debug!("bind_and_rewrite_expr({:?}, {:?})", tbl, id);
                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!(
                            "no such column: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        );
                    };
                    let normalized_table_name = normalize_ident(tbl.as_str());
                    let matching_tbl = referenced_tables
                        .find_table_and_internal_id_by_identifier(&normalized_table_name);
                    if matching_tbl.is_none() {
                        crate::bail_parse_error!("no such table: {}", normalized_table_name);
                    }
                    let (tbl_id, tbl) = matching_tbl.unwrap();
                    let normalized_id = normalize_ident(id.as_str());
                    let col_idx = tbl.columns().iter().position(|c| {
                        c.name
                            .as_ref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                    });
                    if let Some(row_id_expr) = parse_row_id(&normalized_id, tbl_id, || false)? {
                        *expr = row_id_expr;

                        return Ok(WalkControl::Continue);
                    }
                    let Some(col_idx) = col_idx else {
                        crate::bail_parse_error!("no such column: {}", normalized_id);
                    };
                    let col = tbl.columns().get(col_idx).unwrap();
                    *expr = Expr::Column {
                        database: None, // TODO: support different databases
                        table: tbl_id,
                        column: col_idx,
                        is_rowid_alias: col.is_rowid_alias(),
                    };
                    tracing::debug!("rewritten to column");
                    referenced_tables.mark_column_used(tbl_id, col_idx);
                    return Ok(WalkControl::Continue);
                }
                Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!(
                            "no such column: {}.{}.{}",
                            db_name.as_str(),
                            tbl_name.as_str(),
                            col_name.as_str()
                        );
                    };
                    let normalized_col_name = normalize_ident(col_name.as_str());

                    // Create a QualifiedName and use existing resolve_database_id method
                    let qualified_name = ast::QualifiedName {
                        db_name: Some(db_name.clone()),
                        name: tbl_name.clone(),
                        alias: None,
                    };
                    let database_id = connection.resolve_database_id(&qualified_name)?;

                    // Get the table from the specified database
                    let table = connection
                        .with_schema(database_id, |schema| schema.get_table(tbl_name.as_str()))
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "no such table: {}.{}",
                                db_name.as_str(),
                                tbl_name.as_str()
                            ))
                        })?;

                    // Find the column in the table
                    let col_idx = table
                        .columns()
                        .iter()
                        .position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_col_name))
                        })
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "Column: {}.{}.{} not found",
                                db_name.as_str(),
                                tbl_name.as_str(),
                                col_name.as_str()
                            ))
                        })?;

                    let col = table.columns().get(col_idx).unwrap();

                    // Check if this is a rowid alias
                    let is_rowid_alias = col.is_rowid_alias();

                    // Convert to Column expression - since this is a cross-database reference,
                    // we need to create a synthetic table reference for it
                    // For now, we'll error if the table isn't already in the referenced tables
                    let normalized_tbl_name = normalize_ident(tbl_name.as_str());
                    let matching_tbl = referenced_tables
                        .find_table_and_internal_id_by_identifier(&normalized_tbl_name);

                    if let Some((tbl_id, _)) = matching_tbl {
                        // Table is already in referenced tables, use existing internal ID
                        *expr = Expr::Column {
                            database: Some(database_id),
                            table: tbl_id,
                            column: col_idx,
                            is_rowid_alias,
                        };
                        referenced_tables.mark_column_used(tbl_id, col_idx);
                    } else {
                        return Err(crate::LimboError::ParseError(format!(
                            "table {normalized_tbl_name} is not in FROM clause - cross-database column references require the table to be explicitly joined"
                        )));
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )?;
    Ok(())
}

/// Recursively walks a mutable expression, applying a function to each sub-expression.
pub fn walk_expr_mut<F>(expr: &mut ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match func(expr)? {
        WalkControl::Continue => {
            match expr {
                ast::Expr::SubqueryResult { lhs, .. } => {
                    if let Some(lhs) = lhs {
                        walk_expr_mut(lhs, func)?;
                    }
                }
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
                    for arg in args {
                        walk_expr_mut(arg, func)?;
                    }
                    for sort_col in order_by {
                        walk_expr_mut(&mut sort_col.expr, func)?;
                    }
                    if let Some(filter_clause) = &mut filter_over.filter_clause {
                        walk_expr_mut(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &mut filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &mut window.partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                                for sort_col in &mut window.order_by {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
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
                ast::Expr::FunctionCallStar { filter_over, .. } => {
                    if let Some(ref mut filter_clause) = filter_over.filter_clause {
                        walk_expr_mut(filter_clause, func)?;
                    }
                    if let Some(ref mut over_clause) = filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &mut window.partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                                for sort_col in &mut window.order_by {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
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
                ast::Expr::InList { lhs, rhs, .. } => {
                    walk_expr_mut(lhs, func)?;
                    for expr in rhs {
                        walk_expr_mut(expr, func)?;
                    }
                }
                ast::Expr::InSelect { lhs, rhs: _, .. } => {
                    walk_expr_mut(lhs, func)?;
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::InTable { lhs, args, .. } => {
                    walk_expr_mut(lhs, func)?;
                    for expr in args {
                        walk_expr_mut(expr, func)?;
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
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_) => {
                    // No nested expressions
                }
            }
        }
        WalkControl::SkipChildren => return Ok(WalkControl::Continue),
    };
    Ok(WalkControl::Continue)
}

fn walk_expr_mut_frame_bound<F>(bound: &mut ast::FrameBound, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            walk_expr_mut(expr, func)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(WalkControl::Continue)
}

pub fn get_expr_affinity(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
) -> Affinity {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            if let Some(tables) = referenced_tables {
                if let Some((_, table_ref)) = tables.find_table_by_internal_id(*table) {
                    if let Some(col) = table_ref.get_column_at(*column) {
                        return col.affinity();
                    }
                }
            }
            Affinity::Blob
        }
        ast::Expr::RowId { .. } => Affinity::Integer,
        ast::Expr::Cast { type_name, .. } => {
            if let Some(type_name) = type_name {
                crate::schema::affinity(&type_name.name)
            } else {
                Affinity::Blob
            }
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            get_expr_affinity(exprs.first().unwrap(), referenced_tables)
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

/// Evaluate a RETURNING expression using register-based evaluation instead of cursor-based.
/// This is used for RETURNING clauses where we have register values instead of cursor data.
pub fn translate_expr_for_returning(
    program: &mut ProgramBuilder,
    expr: &Expr,
    value_registers: &ReturningValueRegisters,
    target_register: usize,
) -> Result<usize> {
    match expr {
        Expr::Column {
            column,
            is_rowid_alias,
            ..
        } => {
            if *is_rowid_alias {
                // For rowid references, copy from the rowid register
                program.emit_insn(Insn::Copy {
                    src_reg: value_registers.rowid_register,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
            } else {
                // For regular column references, copy from the appropriate column register
                let column_idx = *column;
                if column_idx < value_registers.num_columns {
                    let column_reg = value_registers.columns_start_register + column_idx;
                    program.emit_insn(Insn::Copy {
                        src_reg: column_reg,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                } else {
                    crate::bail_parse_error!("Column index out of bounds in RETURNING clause");
                }
            }
            Ok(target_register)
        }
        Expr::RowId { .. } => {
            // For ROWID expressions, copy from the rowid register
            program.emit_insn(Insn::Copy {
                src_reg: value_registers.rowid_register,
                dst_reg: target_register,
                extra_amount: 0,
            });
            Ok(target_register)
        }
        Expr::Literal(literal) => emit_literal(program, literal, target_register),
        Expr::Binary(lhs, op, rhs) => {
            let lhs_reg = program.alloc_register();
            let rhs_reg = program.alloc_register();

            // Recursively evaluate left-hand side
            translate_expr_for_returning(program, lhs, value_registers, lhs_reg)?;

            // Recursively evaluate right-hand side
            translate_expr_for_returning(program, rhs, value_registers, rhs_reg)?;

            // Use the shared emit_binary_insn function
            emit_binary_insn(
                program,
                op,
                lhs_reg,
                rhs_reg,
                target_register,
                lhs,
                rhs,
                None, // No table references needed for RETURNING
                None, // No condition metadata needed for RETURNING
            )?;

            Ok(target_register)
        }
        Expr::FunctionCall { name, args, .. } => {
            // Evaluate arguments into registers
            let mut arg_regs = Vec::new();
            for arg in args.iter() {
                let arg_reg = program.alloc_register();
                translate_expr_for_returning(program, arg, value_registers, arg_reg)?;
                arg_regs.push(arg_reg);
            }

            // Resolve and call the function using shared helper
            let func = Func::resolve_function(name.as_str(), arg_regs.len())?;
            let func_ctx = FuncCtx {
                func,
                arg_count: arg_regs.len(),
            };

            emit_function_call(program, func_ctx, &arg_regs, target_register)?;
            Ok(target_register)
        }
        _ => {
            crate::bail_parse_error!(
                "Unsupported expression type in RETURNING clause: {:?}",
                expr
            );
        }
    }
}

/// Emit literal values - shared between regular and RETURNING expression evaluation
pub fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &ast::Literal,
    target_register: usize,
) -> Result<usize> {
    match literal {
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
        ast::Literal::Keyword(_) => {
            crate::bail_parse_error!("Keyword in WHERE clause is not supported")
        }
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
    }
}

/// Emit a function call instruction with pre-allocated argument registers
/// This is shared between different function call contexts
pub fn emit_function_call(
    program: &mut ProgramBuilder,
    func_ctx: FuncCtx,
    arg_registers: &[usize],
    target_register: usize,
) -> Result<()> {
    let start_reg = if arg_registers.is_empty() {
        target_register // If no arguments, use target register as start
    } else {
        arg_registers[0] // Use first argument register as start
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(())
}

/// Process a RETURNING clause, converting ResultColumn expressions into ResultSetColumn structures
/// with proper column binding and alias handling.
pub fn process_returning_clause(
    returning: &mut [ast::ResultColumn],
    table: &Table,
    table_name: &str,
    program: &mut ProgramBuilder,
    connection: &std::sync::Arc<crate::Connection>,
) -> Result<(
    Vec<super::plan::ResultSetColumn>,
    super::plan::TableReferences,
)> {
    use super::plan::{ColumnUsedMask, JoinedTable, Operation, ResultSetColumn, TableReferences};

    let mut result_columns = vec![];

    let internal_id = program.table_reference_counter.next();
    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            table: match table {
                Table::Virtual(vtab) => Table::Virtual(vtab.clone()),
                Table::BTree(btree_table) => Table::BTree(btree_table.clone()),
                _ => unreachable!(),
            },
            identifier: table_name.to_string(),
            internal_id,
            op: Operation::default_scan_for(table),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
        }],
        vec![],
    );

    for rc in returning.iter_mut() {
        match rc {
            ast::ResultColumn::Expr(expr, alias) => {
                bind_and_rewrite_expr(
                    expr,
                    Some(&mut table_references),
                    None,
                    connection,
                    &mut program.param_ctx,
                    BindingBehavior::TryResultColumnsFirst,
                )?;

                let column_alias = determine_column_alias(expr, alias, table);

                result_columns.push(ResultSetColumn {
                    expr: *expr.clone(),
                    alias: column_alias,
                    contains_aggregates: false,
                });
            }
            ast::ResultColumn::Star => {
                // Handle RETURNING * by expanding to all table columns
                // Use the shared internal_id for all columns
                for (column_index, column) in table.columns().iter().enumerate() {
                    let column_expr = Expr::Column {
                        database: None,
                        table: internal_id,
                        column: column_index,
                        is_rowid_alias: false,
                    };

                    result_columns.push(ResultSetColumn {
                        expr: column_expr,
                        alias: column.name.clone(),
                        contains_aggregates: false,
                    });
                }
            }
            ast::ResultColumn::TableStar(_table_name) => {
                // Handle RETURNING table.* by expanding to all table columns
                // For single table RETURNING, this is equivalent to *
                for (column_index, column) in table.columns().iter().enumerate() {
                    let column_expr = Expr::Column {
                        database: None,
                        table: internal_id,
                        column: column_index,
                        is_rowid_alias: false,
                    };

                    result_columns.push(ResultSetColumn {
                        expr: column_expr,
                        alias: column.name.clone(),
                        contains_aggregates: false,
                    });
                }
            }
        }
    }

    Ok((result_columns, table_references))
}

/// Determine the appropriate alias for a RETURNING column expression
fn determine_column_alias(
    expr: &Expr,
    explicit_alias: &Option<ast::As>,
    table: &Table,
) -> Option<String> {
    // First check for explicit alias
    if let Some(As::As(name)) = explicit_alias {
        return Some(name.as_str().to_string());
    }

    // For ROWID expressions, use "rowid" as the alias
    if let Expr::RowId { .. } = expr {
        return Some("rowid".to_string());
    }

    // For column references, always use the column name from the table
    if let Expr::Column {
        column,
        is_rowid_alias,
        ..
    } = expr
    {
        if let Some(name) = table
            .columns()
            .get(*column)
            .and_then(|col| col.name.clone())
        {
            return Some(name);
        } else if *is_rowid_alias {
            // If it's a rowid alias, return "rowid"
            return Some("rowid".to_string());
        } else {
            return None;
        }
    }

    // For other expressions, use the expression string representation
    Some(expr.to_string())
}

/// Emit bytecode to evaluate RETURNING expressions and produce result rows.
/// This function handles the actual evaluation of expressions using the values
/// from the DML operation.
pub(crate) fn emit_returning_results(
    program: &mut ProgramBuilder,
    result_columns: &[super::plan::ResultSetColumn],
    value_registers: &ReturningValueRegisters,
) -> Result<()> {
    if result_columns.is_empty() {
        return Ok(());
    }

    let result_start_reg = program.alloc_registers(result_columns.len());

    for (i, result_column) in result_columns.iter().enumerate() {
        let reg = result_start_reg + i;

        translate_expr_for_returning(program, &result_column.expr, value_registers, reg)?;
    }

    program.emit_insn(Insn::ResultRow {
        start_reg: result_start_reg,
        count: result_columns.len(),
    });

    Ok(())
}

/// Get the number of values returned by an expression
pub fn expr_vector_size(expr: &Expr) -> Result<usize> {
    Ok(match unwrap_parens(expr)? {
        Expr::Between {
            lhs, start, end, ..
        } => {
            let evs_left = expr_vector_size(lhs)?;
            let evs_start = expr_vector_size(start)?;
            let evs_end = expr_vector_size(end)?;
            if evs_left != evs_start || evs_left != evs_end {
                crate::bail_parse_error!("all arguments to BETWEEN must return the same number of values. Got: ({evs_left}) BETWEEN ({evs_start}) AND ({evs_end})");
            }
            1
        }
        Expr::Binary(expr, operator, expr1) => {
            let evs_left = expr_vector_size(expr)?;
            let evs_right = expr_vector_size(expr1)?;
            if evs_left != evs_right {
                crate::bail_parse_error!("all arguments to binary operator {operator} must return the same number of values. Got: ({evs_left}) {operator} ({evs_right})");
            }
            1
        }
        Expr::Register(_) => 1,
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                let evs_base = expr_vector_size(base)?;
                if evs_base != 1 {
                    crate::bail_parse_error!(
                        "base expression in CASE must return 1 value. Got: ({evs_base})"
                    );
                }
            }
            for (when, then) in when_then_pairs {
                let evs_when = expr_vector_size(when)?;
                if evs_when != 1 {
                    crate::bail_parse_error!(
                        "when expression in CASE must return 1 value. Got: ({evs_when})"
                    );
                }
                let evs_then = expr_vector_size(then)?;
                if evs_then != 1 {
                    crate::bail_parse_error!(
                        "then expression in CASE must return 1 value. Got: ({evs_then})"
                    );
                }
            }
            if let Some(else_expr) = else_expr {
                let evs_else_expr = expr_vector_size(else_expr)?;
                if evs_else_expr != 1 {
                    crate::bail_parse_error!(
                        "else expression in CASE must return 1 value. Got: ({evs_else_expr})"
                    );
                }
            }
            1
        }
        Expr::Cast { expr, .. } => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("argument to CAST must return 1 value. Got: ({evs_expr})");
            }
            1
        }
        Expr::Collate(expr, _) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to COLLATE must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::DoublyQualified(..) => 1,
        Expr::Exists(_) => todo!(),
        Expr::FunctionCall { name, args, .. } => {
            for (pos, arg) in args.iter().enumerate() {
                let evs_arg = expr_vector_size(arg)?;
                if evs_arg != 1 {
                    crate::bail_parse_error!(
                        "argument {} to function call {name} must return 1 value. Got: ({evs_arg})",
                        pos + 1
                    );
                }
            }
            1
        }
        Expr::FunctionCallStar { .. } => 1,
        Expr::Id(_) => 1,
        Expr::Column { .. } => 1,
        Expr::RowId { .. } => 1,
        Expr::InList { lhs, rhs, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            for rhs in rhs.iter() {
                let evs_rhs = expr_vector_size(rhs)?;
                if evs_lhs != evs_rhs {
                    crate::bail_parse_error!("all arguments to IN list must return the same number of values, got: ({evs_lhs}) IN ({evs_rhs})");
                }
            }
            1
        }
        Expr::InSelect { .. } => {
            crate::bail_parse_error!("InSelect is not supported in this position")
        }
        Expr::InTable { .. } => {
            crate::bail_parse_error!("InTable is not supported in this position")
        }
        Expr::IsNull(expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to IS NULL must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::Like { lhs, rhs, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            if evs_lhs != 1 {
                crate::bail_parse_error!(
                    "left operand of LIKE must return 1 value. Got: ({evs_lhs})"
                );
            }
            let evs_rhs = expr_vector_size(rhs)?;
            if evs_rhs != 1 {
                crate::bail_parse_error!(
                    "right operand of LIKE must return 1 value. Got: ({evs_rhs})"
                );
            }
            1
        }
        Expr::Literal(_) => 1,
        Expr::Name(_) => 1,
        Expr::NotNull(expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to NOT NULL must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::Parenthesized(exprs) => exprs.len(),
        Expr::Qualified(..) => 1,
        Expr::Raise(..) => crate::bail_parse_error!("RAISE is not supported"),
        Expr::Subquery(_) => todo!(),
        Expr::Unary(unary_operator, expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("argument to unary operator {unary_operator} must return 1 value. Got: ({evs_expr})");
            }
            1
        }
        Expr::Variable(_) => 1,
        Expr::SubqueryResult { query_type, .. } => match query_type {
            SubqueryType::Exists { .. } => 1,
            SubqueryType::In { .. } => 1,
            SubqueryType::RowValue { num_regs, .. } => *num_regs,
        },
    })
}
