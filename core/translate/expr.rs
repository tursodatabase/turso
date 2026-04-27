use crate::error::{SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR};
use crate::translate::optimizer::constraints::ConstraintOperator;
use crate::turso_assert;
use tracing::{instrument, Level};
use turso_parser::ast::{self, Expr, ResolveType, SubqueryType, TableInternalId, UnaryOperator};

use super::collate::{get_collseq_from_expr, CollationSeq};
use super::emitter::Resolver;
use super::optimizer::Optimizable;
use super::plan::TableReferences;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{AggFunc, Func, FuncCtx, MathFuncArity, ScalarFunc, VectorFunc};
use crate::functions::datetime;
use crate::schema::{
    BTreeTable, ColDef, Column, ColumnLayout, GeneratedType, Table, Type, TypeDef,
};
use crate::sync::Arc;
use crate::translate::expression_index::{
    normalize_expr_for_index_matching, single_table_column_usage,
};
use crate::translate::plan::{ColumnMask, Operation, ResultSetColumn, Search};
use crate::translate::planner::parse_row_id;
use crate::util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, DmlColumnContext, SelfTableContext};
use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{CmpInsFlags, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{LimboError, Numeric, Result, Value};

#[derive(Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
    pub jump_target_when_null: BranchOffset,
}

fn translate_between_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    mut between_expr: ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Between {
        ref mut lhs,
        not,
        ref mut start,
        ref mut end,
    } = between_expr
    else {
        unreachable!("translate_between_expr expects Expr::Between");
    };

    let lhs_reg = program.alloc_register();
    translate_expr(program, referenced_tables, &*lhs, lhs_reg, resolver)?;

    let mut between_resolver = resolver.fork_with_expr_cache();
    between_resolver.enable_expr_to_reg_cache();
    #[allow(clippy::or_fun_call)]
    between_resolver.cache_scalar_expr_reg(
        std::borrow::Cow::Owned(*lhs.to_owned()),
        lhs_reg,
        false,
        referenced_tables.unwrap_or(&TableReferences::default()),
    )?;

    let (lower_expr, upper_expr, combine_op) = build_between_terms(
        std::mem::take(lhs),
        not,
        std::mem::take(start),
        std::mem::take(end),
    );
    let lower_reg = program.alloc_register();
    translate_expr(
        program,
        referenced_tables,
        &lower_expr,
        lower_reg,
        &between_resolver,
    )?;
    let upper_reg = program.alloc_register();
    translate_expr(
        program,
        referenced_tables,
        &upper_expr,
        upper_reg,
        &between_resolver,
    )?;

    program.emit_insn(match combine_op {
        ast::Operator::And => Insn::And {
            lhs: lower_reg,
            rhs: upper_reg,
            dest: target_register,
        },
        ast::Operator::Or => Insn::Or {
            lhs: lower_reg,
            rhs: upper_reg,
            dest: target_register,
        },
        _ => unreachable!("BETWEEN combine operator must be AND/OR"),
    });

    Ok(target_register)
}

fn build_between_terms(
    lhs: ast::Expr,
    not: bool,
    start: ast::Expr,
    end: ast::Expr,
) -> (ast::Expr, ast::Expr, ast::Operator) {
    let (lower_op, upper_op, combine_op) = if not {
        (
            ast::Operator::Less,
            ast::Operator::Greater,
            ast::Operator::Or,
        )
    } else {
        (
            ast::Operator::GreaterEquals,
            ast::Operator::LessEquals,
            ast::Operator::And,
        )
    };
    let lower_expr = ast::Expr::Binary(Box::new(lhs.clone()), lower_op, Box::new(start));
    let upper_expr = ast::Expr::Binary(Box::new(lhs), upper_op, Box::new(end));
    (lower_expr, upper_expr, combine_op)
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

fn assert_register_range_allocated(
    program: &mut ProgramBuilder,
    start_register: usize,
    count: usize,
) -> Result<()> {
    // Invariant: callers must have pre-allocated [start_register, start_register + count)
    // before asking expression translation to write a vector into that range.
    let required_next = start_register + count;
    let next_free = program.peek_next_register();
    if required_next <= next_free {
        Ok(())
    } else {
        crate::bail_parse_error!(
            "insufficient registers allocated for expression vector write (start={start_register}, count={count}, next_free={next_free})"
        )
    }
}

fn supports_row_value_binary_comparison(operator: &ast::Operator) -> bool {
    matches!(
        operator,
        ast::Operator::Equals
            | ast::Operator::NotEquals
            | ast::Operator::Less
            | ast::Operator::LessEquals
            | ast::Operator::Greater
            | ast::Operator::GreaterEquals
            | ast::Operator::Is
            | ast::Operator::IsNot
    )
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

/// Translate a function that is desugared into a dedicated variadic-argument instruction.
/// Evaluates all args into consecutive registers, then emits the instruction.
/// The instruction must have fields `start_reg`, `count`, and `dest`.
///
/// Used by: Array (MakeArray), struct_pack (also MakeArray).
macro_rules! translate_variadic_insn {
    ($program:expr, $referenced_tables:expr, $resolver:expr, $args:expr, $dest:expr,
     $Insn:ident) => {{
        let start_reg = $program.alloc_registers($args.len());
        for (i, arg) in $args.iter().enumerate() {
            translate_expr($program, $referenced_tables, arg, start_reg + i, $resolver)?;
        }
        $program.emit_insn(Insn::$Insn {
            start_reg,
            count: $args.len(),
            dest: $dest,
        });
        Ok($dest)
    }};
}

/// Translate a function that is desugared into a dedicated fixed-argument instruction.
/// Evaluates each arg into its own register, then emits the instruction using a
/// caller-provided expression that can reference the allocated registers by index.
///
/// `$reg_names` is a comma-separated list of identifiers bound to allocated
/// registers for the corresponding arg indices.
///
/// Used by: ArrayElement, ArraySetElement, UnionTag, UnionExtract, UnionValue.
macro_rules! translate_fixed_insn {
    ($program:expr, $referenced_tables:expr, $resolver:expr, $args:expr, $dest:expr,
     [$($reg_name:ident <- $idx:expr),+], $insn_expr:expr) => {{
        $(
            let $reg_name = $program.alloc_register();
            translate_expr($program, $referenced_tables, &$args[$idx], $reg_name, $resolver)?;
        )+
        $program.emit_insn($insn_expr);
        Ok($dest)
    }};
}

#[inline]
/// For expression indexes, try to emit code that directly reads the value from the index
/// under the following conditions:
/// - The expression only references columns from a single table
/// - The referenced table has an index whose expression matches the given expression
///
/// If an expression index exactly matches the requested expression, we can
/// fetch the precomputed value from the index key instead of re-evaluating
/// the expression. That matters for:
/// - SELECT a/b FROM t with INDEX ON t(a/b) (avoid computing a/b for every row)
/// - ORDER BY a+b when the index already stores a+b (preserves ordering)
///
/// We mut do this check early in translate_expr so downstream translation does
/// not build redundant bytecode.
fn try_emit_expression_index_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
) -> Result<bool> {
    let Some(referenced_tables) = referenced_tables else {
        return Ok(false);
    };
    let Some((table_id, _)) = single_table_column_usage(expr) else {
        return Ok(false);
    };
    let Some(table_reference) = referenced_tables.find_joined_table_by_internal_id(table_id) else {
        return Ok(false);
    };
    let Some(index) = table_reference.op.index() else {
        return Ok(false);
    };
    let normalized = normalize_expr_for_index_matching(expr, table_reference, referenced_tables);
    let Some(expr_pos) = index.expression_to_index_pos(&normalized) else {
        return Ok(false);
    };
    let Some(cursor_id) =
        program.resolve_cursor_id_safe(&CursorKey::index(table_id, index.clone()))
    else {
        return Ok(false);
    };
    program.emit_column_or_rowid(cursor_id, expr_pos, target_register);
    Ok(true)
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
/// Compute the affinity for an IN expression.
/// For `x IN (y1, y2, ..., yN)`, the affinity is determined by the LHS expression `x`.
/// This follows SQLite's `exprINAffinity()` function.
fn in_expr_affinity(
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
fn translate_in_list(
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

    // we don't know exactly what instruction will came next and it's important to chain label to the execution flow rather then exact next instruction
    // for example, next instruction can be register assignment, which can be moved by optimized to the constant section
    // in this case, label_ok must be changed accordingly and be re-binded to another instruction followed the current translation unit after constants reording
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
            crate::bail_parse_error!(
                "Register in WHERE clause is currently unused. Consider removing Resolver::expr_to_reg_cache and using Expr::Register instead"
            );
        }
        ast::Expr::Collate(_, _) => {
            crate::bail_parse_error!("Collate in WHERE clause is not supported");
        }
        ast::Expr::DoublyQualified(_, _, _) | ast::Expr::Id(_) | ast::Expr::Qualified(_, _) => {
            crate::bail_parse_error!(
                "DoublyQualified/Id/Qualified should have been rewritten to Column during binding"
            );
        }
        ast::Expr::FieldAccess { .. } => {
            crate::bail_parse_error!(
                "struct/union field access cannot be used as a bare boolean condition in WHERE"
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
            let between_result_reg = program.alloc_register();
            translate_between_expr(
                program,
                Some(referenced_tables),
                expr.clone(),
                between_result_reg,
                resolver,
            )?;
            emit_cond_jump(program, condition_metadata, between_result_reg);
        }
        ast::Expr::Variable(_) => {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
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
            // In a binary OR, never jump to the parent 'jump_target_when_false' or
            // 'jump_target_when_null' label on the first condition, because the second
            // condition CAN also be true. Instead we instruct the child expression to
            // jump to a local false label so the right side of OR gets evaluated.
            // This is critical for cases like `x IN (NULL, 3) OR b` where the left side
            // evaluates to NULL — we must still evaluate the right side.
            let jump_target_when_false = program.allocate_label();
            translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_false,
                    jump_target_when_null: jump_target_when_false,
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
        // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE in conditions
        // Delegate to translate_expr which handles these correctly with IsTrue instruction
        ast::Expr::Binary(_, ast::Operator::Is | ast::Operator::IsNot, e2)
            if matches!(
                e2.as_ref(),
                ast::Expr::Literal(ast::Literal::True) | ast::Expr::Literal(ast::Literal::False)
            ) =>
        {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
        }
        // Handle IS NULL/IS NOT NULL in conditions using IsNull/NotNull opcodes.
        // "a IS NULL" is parsed as Binary(a, Is, Null), but we need to use the IsNull opcode
        // (not Eq/Ne with null_eq flag) for correct NULL handling in WHERE clauses.
        ast::Expr::Binary(e1, ast::Operator::Is, e2)
            if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
        {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), e1, cur_reg, resolver)?;
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
        ast::Expr::Binary(e1, ast::Operator::IsNot, e2)
            if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
        {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), e1, cur_reg, resolver)?;
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
        ast::Expr::Binary(e1, op, e2) => {
            // Check if either operand has a custom type with a matching operator
            if let Some(resolved) =
                find_custom_type_operator(e1, e2, op, Some(referenced_tables), resolver)
            {
                let result_reg = emit_custom_type_operator(
                    program,
                    Some(referenced_tables),
                    e1,
                    e2,
                    &resolved,
                    resolver,
                )?;
                emit_cond_jump(program, condition_metadata, result_reg);
            } else {
                let result_reg = program.alloc_register();
                binary_expr_shared(
                    program,
                    Some(referenced_tables),
                    e1,
                    e2,
                    op,
                    result_reg,
                    resolver,
                    BinaryEmitMode::Condition(condition_metadata),
                )?;
            }
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
                program.preassign_label_to_next_insn(not_true_label.unwrap());
                program.emit_insn(Insn::Goto {
                    target_pc: jump_target_when_false,
                });

                // When IN is FALSE (no match), NOT IN should be TRUE
                program.preassign_label_to_next_insn(not_false_label.unwrap());
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
                translate_condition_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    condition_metadata,
                    resolver,
                )?;
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
        ast::Expr::Default => {
            crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
        }
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
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
    /// The column has a custom type encode function that will be applied
    /// in-place after this expression is evaluated. We must not hoist the
    /// expression because:
    ///
    /// 1. The encode function may be non-deterministic (e.g. it could use
    ///    datetime('now')), so hoisting would produce incorrect results.
    ///
    /// 2. Even if the encode function were deterministic, the encode is
    ///    applied in-place to the target register inside the update loop.
    ///    If the original value were hoisted (evaluated once before the
    ///    loop), the second iteration would read the already-encoded value
    ///    from the register and encode it again, causing progressive
    ///    double-encoding (e.g. 99 → 9900 → 990000 → ...).
    ///
    /// The correct fix for deterministic encode functions would be to hoist
    /// the *encoded* result (i.e. `encode_fn(99)` not `99`), but that
    /// requires tracking the encode through the hoisting machinery. For now
    /// we simply disable hoisting for these columns.
    CustomTypeEncode,
    /// IN-list values are inserted into an ephemeral table in a loop.
    /// Each value reuses the same register, so hoisting would collapse
    /// all values into the last one.
    InListEphemeral,
}

/// Controls how binary expressions are emitted.
///
/// This makes scalar and row-valued paths explicit:
/// - scalar binary expressions use mode to pick either value emission or conditional jump emission
/// - row-valued binary expressions always emit a value register first, then optionally a conditional jump
#[derive(Clone, Copy)]
enum BinaryEmitMode {
    Value,
    Condition(ConditionMetadata),
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

/// Resolve an expression to a register, reusing an existing register when possible.
///
/// Unlike `translate_expr`, this does not require a pre-allocated target register.
/// If the expression is found in the `expr_to_reg_cache`, the cached register is
/// returned directly without emitting a Copy instruction. Otherwise, a new register
/// is allocated and the expression is translated into it.
///
/// Callers MUST use the returned register — they cannot assume a specific destination.
#[must_use = "the returned register must be used, because that is where the expression value is stored"]
pub fn resolve_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    resolver: &Resolver,
) -> Result<usize> {
    if let Some((reg, needs_decode, _collation)) = resolver.resolve_cached_expr_reg(expr) {
        if !needs_decode {
            return Ok(reg);
        }
    }
    let dest_reg = program.alloc_register();
    translate_expr(program, referenced_tables, expr, dest_reg, resolver)
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

    if let Some((reg, needs_decode, collation_ctx)) = resolver.resolve_cached_expr_reg(expr) {
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: target_register,
            extra_amount: 0,
        });
        // Hash join payloads store raw encoded values; apply DECODE for custom
        // type columns so the result set contains human-readable text.
        if needs_decode && !program.flags.suppress_custom_type_decode() {
            if let ast::Expr::Column {
                table: table_ref_id,
                column,
                ..
            } = expr
            {
                if let Some(referenced_tables) = referenced_tables {
                    if let Some((_, table)) =
                        referenced_tables.find_table_by_internal_id(*table_ref_id)
                    {
                        if let Some(col) = table.get_column_at(*column) {
                            if let Some(type_def) = resolver
                                .schema()
                                .get_type_def(&col.ty_str, table.is_strict())
                            {
                                if let Some(decode_expr) = type_def.decode() {
                                    let skip_label = program.allocate_label();
                                    program.emit_insn(Insn::IsNull {
                                        reg: target_register,
                                        target_pc: skip_label,
                                    });
                                    emit_type_expr(
                                        program,
                                        decode_expr,
                                        target_register,
                                        target_register,
                                        col,
                                        type_def,
                                        resolver,
                                    )?;
                                    program.preassign_label_to_next_insn(skip_label);
                                }
                            }
                        }
                    }
                }
            }
        }
        program.set_collation(collation_ctx);
        if let Some(span) = constant_span {
            program.constant_span_end(span);
        }
        return Ok(target_register);
    }

    // At the very start we try to satisfy the expression from an expression index
    let has_expression_indexes = referenced_tables.is_some_and(|tables| {
        tables
            .joined_tables()
            .iter()
            .any(|t| !t.expression_index_usages.is_empty())
    });
    if has_expression_indexes
        && try_emit_expression_index_value(program, referenced_tables, expr, target_register)?
    {
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
                SubqueryType::In {
                    cursor_id,
                    affinity_str,
                } => {
                    // jump here when we can definitely skip the row (result = 0/false)
                    let label_skip_row = program.allocate_label();
                    // jump here when we can definitely include the row (result = 1/true)
                    let label_include_row = program.allocate_label();
                    // jump here when the result should be NULL (unknown)
                    let label_null_result = program.allocate_label();
                    // jump here when we need to make extra null-related checks
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
                        // If LHS is NULL, we need to check if ephemeral is empty first.
                        // - If empty: IN returns FALSE, NOT IN returns TRUE
                        // - If not empty: result is NULL (unknown)
                        // Jump to label_null_rewind which does Rewind and handles empty case.
                        //
                        // Always emit this check even for NOT NULL columns because NullRow
                        // (used in ungrouped aggregates when no rows match) overrides all
                        // column values to NULL regardless of the NOT NULL constraint.
                        program.emit_insn(Insn::IsNull {
                            reg: lhs_column_regs_start + i,
                            target_pc: label_null_rewind,
                        });
                    }

                    // Only emit Affinity instruction if there's meaningful affinity to apply
                    // (i.e., not all BLOB/NONE affinity)
                    if affinity_str
                        .chars()
                        .map(Affinity::from_char)
                        .any(|a| a != Affinity::Blob)
                    {
                        if let Ok(count) = std::num::NonZeroUsize::try_from(lhs_column_count) {
                            program.emit_insn(Insn::Affinity {
                                start_reg: lhs_column_regs_start,
                                count,
                                affinities: affinity_str.as_ref().clone(),
                            });
                        }
                    }

                    // For NOT IN: empty ephemeral or no all-NULL row means TRUE (include)
                    // For IN: empty ephemeral or no all-NULL row means FALSE (skip)
                    let label_on_no_null = if *not_in {
                        label_include_row
                    } else {
                        label_skip_row
                    };

                    if *not_in {
                        // NOT IN: skip row if value is found
                        program.emit_insn(Insn::Found {
                            cursor_id: *cursor_id,
                            target_pc: label_skip_row,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                    } else {
                        // IN: if value found, include row; otherwise check for NULLs
                        program.emit_insn(Insn::NotFound {
                            cursor_id: *cursor_id,
                            target_pc: label_null_rewind,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                        program.emit_insn(Insn::Goto {
                            target_pc: label_include_row,
                        });
                    }

                    // Null checking loop: scan ephemeral for any all-NULL tuples.
                    // If found, result is NULL (unknown). If not found, result depends on IN vs NOT IN.
                    program.preassign_label_to_next_insn(label_null_rewind);
                    program.emit_insn(Insn::Rewind {
                        cursor_id: *cursor_id,
                        pc_if_empty: label_on_no_null,
                    });
                    program.preassign_label_to_next_insn(label_null_checks_loop_start);
                    let column_check_reg = program.alloc_register();
                    for (i, affinity) in affinity_str.chars().map(Affinity::from_char).enumerate() {
                        program.emit_insn(Insn::Column {
                            cursor_id: *cursor_id,
                            column: i,
                            dest: column_check_reg,
                            default: None,
                        });
                        // Ne with NULL operand does NOT jump (comparison is NULL/unknown)
                        program.emit_insn(Insn::Ne {
                            lhs: lhs_column_regs_start + i,
                            rhs: column_check_reg,
                            target_pc: label_null_checks_next,
                            flags: CmpInsFlags::default().with_affinity(affinity),
                            collation: program.curr_collation(),
                        });
                    }
                    // All Ne comparisons fell through -> this row has all NULLs -> result is NULL
                    program.emit_insn(Insn::Goto {
                        target_pc: label_null_result,
                    });
                    program.preassign_label_to_next_insn(label_null_checks_next);
                    program.emit_insn(Insn::Next {
                        cursor_id: *cursor_id,
                        pc_if_next: label_null_checks_loop_start,
                    });
                    // Loop exhausted without finding all-NULL row
                    program.emit_insn(Insn::Goto {
                        target_pc: label_on_no_null,
                    });
                    // Final result handling:
                    // label_include_row: result = 1 (TRUE)
                    // label_skip_row: result = 0 (FALSE)
                    // label_null_result: result = NULL (unknown)
                    let label_done = program.allocate_label();
                    program.preassign_label_to_next_insn(label_include_row);
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target_register,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: label_done,
                    });
                    program.preassign_label_to_next_insn(label_skip_row);
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target_register,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: label_done,
                    });
                    program.preassign_label_to_next_insn(label_null_result);
                    program.emit_insn(Insn::Null {
                        dest: target_register,
                        dest_end: None,
                    });
                    program.preassign_label_to_next_insn(label_done);
                    Ok(target_register)
                }
                SubqueryType::RowValue {
                    result_reg_start,
                    num_regs,
                } => {
                    assert_register_range_allocated(program, target_register, *num_regs)?;
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
            translate_between_expr(
                program,
                referenced_tables,
                expr.clone(),
                target_register,
                resolver,
            )?;
            Ok(target_register)
        }
        ast::Expr::Binary(e1, op, e2) => {
            // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE specially.
            // These use truth semantics (only non-zero numbers are truthy) rather than equality.
            if let Some((is_not, is_true_literal)) = match (op, e2.as_ref()) {
                (ast::Operator::Is, ast::Expr::Literal(ast::Literal::True)) => Some((false, true)),
                (ast::Operator::Is, ast::Expr::Literal(ast::Literal::False)) => {
                    Some((false, false))
                }
                (ast::Operator::IsNot, ast::Expr::Literal(ast::Literal::True)) => {
                    Some((true, true))
                }
                (ast::Operator::IsNot, ast::Expr::Literal(ast::Literal::False)) => {
                    Some((true, false))
                }
                _ => None,
            } {
                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, e1, reg, resolver)?;
                // For NULL: IS variants return 0, IS NOT variants return 1
                // For non-NULL: IS TRUE/IS NOT FALSE return truthy, IS FALSE/IS NOT TRUE return !truthy
                let null_value = is_not;
                let invert = is_not == is_true_literal;
                program.emit_insn(Insn::IsTrue {
                    reg,
                    dest: target_register,
                    null_value,
                    invert,
                });
                if let Some(span) = constant_span {
                    program.constant_span_end(span);
                }
                return Ok(target_register);
            }

            // Check if either operand has a custom type with a matching operator
            if let Some(resolved) =
                find_custom_type_operator(e1, e2, op, referenced_tables, resolver)
            {
                let result_reg = emit_custom_type_operator(
                    program,
                    referenced_tables,
                    e1,
                    e2,
                    &resolved,
                    resolver,
                )?;
                if result_reg != target_register {
                    program.emit_insn(Insn::Copy {
                        src_reg: result_reg,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                }
                return Ok(target_register);
            }

            binary_expr_shared(
                program,
                referenced_tables,
                e1,
                e2,
                op,
                target_register,
                resolver,
                BinaryEmitMode::Value,
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
            translate_expr(program, referenced_tables, expr, target_register, resolver)?;

            // Check if casting to a custom type
            if let Some(ref tn) = type_name {
                if let Some(resolved) = resolver.schema().resolve_type_unchecked(&tn.name)? {
                    // Build ty_params from AST TypeSize so parametric types
                    // (e.g. numeric(10,2)) get their parameters passed through.
                    let ty_params: Vec<Box<ast::Expr>> = match &tn.size {
                        Some(ast::TypeSize::MaxSize(e)) => vec![e.clone()],
                        Some(ast::TypeSize::TypeSize(e1, e2)) => {
                            vec![e1.clone(), e2.clone()]
                        }
                        None => Vec::new(),
                    };

                    // Domains: apply parent encode chain, then validate constraints
                    // on the encoded value (domain CHECK sees the stored representation).
                    if resolved.is_domain() {
                        // Apply encode from parent custom types (domain itself has encode: None)
                        let cast_col = Column::new(
                            None,
                            tn.name.clone(),
                            None,
                            None,
                            Type::Null,
                            None,
                            ColDef::default(),
                        );
                        for td in &resolved.chain {
                            if let Some(encode_expr) = td.encode() {
                                emit_type_expr(
                                    program,
                                    encode_expr,
                                    target_register,
                                    target_register,
                                    &cast_col,
                                    td,
                                    resolver,
                                )?;
                            }
                        }

                        // Validate domain constraints on the encoded value
                        emit_domain_cast_constraints(
                            program,
                            &resolved.chain,
                            target_register,
                            resolver,
                        )?;
                        return Ok(target_register);
                    }

                    let type_def = resolved.leaf();
                    // If the custom type requires parameters but the CAST
                    // doesn't provide them (e.g. CAST(x AS NUMERIC) vs
                    // CAST(x AS numeric(10,2))), fall through to regular CAST.
                    let user_param_count = type_def.user_params().count();
                    if user_param_count == 0 || ty_params.len() == user_param_count {
                        let mut cast_col = Column::new(
                            None,
                            tn.name.clone(),
                            None,
                            None,
                            Type::Null,
                            None,
                            ColDef::default(),
                        );
                        cast_col.ty_params = ty_params;

                        // CAST to custom type applies only the encode function,
                        // producing the stored representation.
                        // e.g. CAST(42 AS cents) → 4200
                        if let Some(encode_expr) = type_def.encode() {
                            emit_type_expr(
                                program,
                                encode_expr,
                                target_register,
                                target_register,
                                &cast_col,
                                type_def,
                                resolver,
                            )?;
                        }
                        return Ok(target_register);
                    }
                }
            }

            // SQLite allows CAST(x AS) without a type name, treating it as NUMERIC affinity
            let type_affinity = type_name
                .as_ref()
                .map(|t| Affinity::affinity(&t.name))
                .unwrap_or(Affinity::Numeric);
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
            let func_type = resolver.resolve_function(name.as_str(), args_count)?;

            if func_type.is_none() {
                crate::bail_parse_error!("no such function: {}", name.as_str());
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
                Func::Window(_) => {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str())
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
                    JsonFunc::JsonValid => {
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
                    VectorFunc::Vector8 => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;

                        emit_function_call(program, func_ctx, &[start_reg], target_register)?;
                        Ok(target_register)
                    }
                    VectorFunc::Vector1Bit => {
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
                    VectorFunc::VectorDistanceDot => {
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
                        // Arity and custom-types checks are done at bind time
                        // in validate_custom_type_function_call.
                        ScalarFunc::Array => {
                            translate_variadic_insn!(
                                program,
                                referenced_tables,
                                resolver,
                                args,
                                target_register,
                                MakeArray
                            )
                        }
                        ScalarFunc::ArrayElement => {
                            translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1],
                                Insn::ArrayElement { array_reg, index_reg, dest: target_register })
                        }
                        ScalarFunc::ArraySetElement => {
                            translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1, value_reg <- 2],
                                Insn::ArraySetElement { array_reg, index_reg, value_reg, dest: target_register })
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
                                    "wrong number of arguments to function {}()",
                                    srf.to_string()
                                );
                            };
                            // Allocate all registers upfront to ensure they're consecutive,
                            // since translate_expr may allocate internal registers.
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
                        ScalarFunc::ConcatWs => {
                            if args.len() < 2 {
                                crate::bail_parse_error!(
                                    "wrong number of arguments to function {}()",
                                    srf.to_string()
                                );
                            }

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
                            program.preassign_label_to_next_insn(before_copy_label);
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
                                constant_mask: 0,
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
                        | ScalarFunc::Unistr
                        | ScalarFunc::UnistrQuote
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

                            // Allocate both registers first to ensure they're consecutive,
                            // since translate_expr may allocate internal registers.
                            let first_reg = program.alloc_register();
                            let second_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                first_reg,
                                resolver,
                            )?;
                            translate_expr(
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
                            if args.len() != 3 {
                                crate::bail_parse_error!(
                                    "wrong number of arguments to function {}()",
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
                                            "second argument to likelihood() must be a constant between 0.0 and 1.0",
                                        );
                                    }
                                    if !value.contains('.') {
                                        crate::bail_parse_error!(
                                            "second argument to likelihood() must be a floating point number with decimal point",
                                        );
                                    }
                                } else {
                                    crate::bail_parse_error!(
                                        "second argument to likelihood() must be a floating point constant",
                                    );
                                }
                            } else {
                                crate::bail_parse_error!(
                                    "second argument to likelihood() must be a constant between 0.0 and 1.0",
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
                        ScalarFunc::StatInit | ScalarFunc::StatPush | ScalarFunc::StatGet => {
                            crate::bail_parse_error!(
                                "{} is an internal function used by ANALYZE",
                                srf
                            );
                        }
                        ScalarFunc::ConnTxnId | ScalarFunc::IsAutocommit => {
                            crate::bail_parse_error!("{} is an internal function used by CDC", srf);
                        }
                        ScalarFunc::TestUintEncode
                        | ScalarFunc::TestUintDecode
                        | ScalarFunc::TestUintAdd
                        | ScalarFunc::TestUintSub
                        | ScalarFunc::TestUintMul
                        | ScalarFunc::TestUintDiv
                        | ScalarFunc::TestUintLt
                        | ScalarFunc::TestUintEq
                        | ScalarFunc::StringReverse
                        | ScalarFunc::BooleanToInt
                        | ScalarFunc::IntToBoolean
                        | ScalarFunc::ValidateIpAddr
                        | ScalarFunc::NumericEncode
                        | ScalarFunc::NumericDecode
                        | ScalarFunc::NumericAdd
                        | ScalarFunc::NumericSub
                        | ScalarFunc::NumericMul
                        | ScalarFunc::NumericDiv
                        | ScalarFunc::NumericLt
                        | ScalarFunc::NumericEq => translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::ArrayLength
                        | ScalarFunc::ArrayAppend
                        | ScalarFunc::ArrayPrepend
                        | ScalarFunc::ArrayCat
                        | ScalarFunc::ArrayRemove
                        | ScalarFunc::ArrayContains
                        | ScalarFunc::ArrayPosition
                        | ScalarFunc::ArraySlice
                        | ScalarFunc::StringToArray
                        | ScalarFunc::ArrayToString
                        | ScalarFunc::ArrayOverlap
                        | ScalarFunc::ArrayContainsAll => translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::StructPack => {
                            translate_variadic_insn!(
                                program,
                                referenced_tables,
                                resolver,
                                args,
                                target_register,
                                MakeArray
                            )
                        }
                        ScalarFunc::UnionValueFunc => {
                            let args = expect_arguments_exact!(args, 2, srf);
                            let tag_name = extract_string_literal(&args[0])?;
                            // union_value('tag', val): resolve the tag against the
                            // target column's union type. The target is set by
                            // INSERT/UPDATE/UPSERT before translating the value.
                            let Some(ref union_td) = program.target_union_type else {
                                return Err(crate::LimboError::ParseError(
                                    "union_value() can only be used in INSERT/UPDATE targeting a union-typed column".to_string()
                                ));
                            };
                            let Some((tag_index, variant)) = union_td.find_union_variant(&tag_name)
                            else {
                                return Err(crate::LimboError::ParseError(format!(
                                    "unknown variant '{}' in union type '{}'",
                                    tag_name, union_td.name
                                )));
                            };
                            // If the variant's type is itself a union, set
                            // target_union_type so nested union_value() resolves
                            // against the inner union type, not the outer one.
                            let inner_union_td = resolver
                                .schema()
                                .get_type_def_unchecked(&variant.type_name)
                                .filter(|td| td.is_union())
                                .cloned();
                            let prev = program.target_union_type.take();
                            program.target_union_type = inner_union_td;
                            let value_reg = program.alloc_register();
                            let result = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                value_reg,
                                resolver,
                            );
                            program.target_union_type = prev;
                            result?;
                            program.emit_insn(Insn::UnionPack {
                                tag_index,
                                value_reg,
                                dest: target_register,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::UnionTagFunc => {
                            // union_tag(col): resolve col's union type for index→name lookup.
                            let args = expect_arguments_exact!(args, 1, srf);
                            let td = resolve_union_from_column(
                                &*args[0],
                                referenced_tables,
                                resolver,
                                program,
                            );
                            let tag_names = td
                                .as_ref()
                                .and_then(|td| td.union_def())
                                .map(|ud| Arc::clone(&ud.tag_names))
                                .unwrap_or_else(|| Arc::from(Vec::<String>::new()));
                            translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::UnionTag { src_reg, dest: target_register, tag_names })
                        }
                        ScalarFunc::UnionExtractFunc => {
                            let args = expect_arguments_exact!(args, 2, srf);
                            let tag_name = extract_string_literal(&*args[1])?;
                            // union_extract(col, 'tag'): resolve col's union type for name→index.
                            let td = resolve_union_from_column(
                                &*args[0],
                                referenced_tables,
                                resolver,
                                program,
                            );
                            let tag_index = td
                                .as_ref()
                                .and_then(|td| td.resolve_union_tag_index(&tag_name))
                                .ok_or_else(|| {
                                    crate::LimboError::ParseError(format!(
                                        "cannot resolve union variant '{tag_name}' for union_extract"
                                    ))
                                })?;
                            translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::UnionExtract { src_reg, expected_tag: tag_index, dest: target_register })
                        }
                        ScalarFunc::StructExtractFunc => {
                            let args = expect_arguments_exact!(args, 2, srf);
                            let field_name = extract_string_literal(&*args[1])?;
                            let td = resolve_struct_from_expr(
                                &*args[0],
                                referenced_tables,
                                resolver,
                                program,
                            );
                            let (field_index, _) = td
                                .as_ref()
                                .and_then(|td| td.find_struct_field(&field_name))
                                .ok_or_else(|| {
                                    crate::LimboError::ParseError(format!(
                                        "cannot resolve struct field '{field_name}' for struct_extract"
                                    ))
                                })?;
                            translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::StructField { src_reg, field_index, dest: target_register })
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
                #[cfg(all(feature = "fts", not(target_family = "wasm")))]
                Func::Fts(_) => {
                    // FTS functions are handled via index method pattern matching.
                    // If we reach here, no index matched, so translate as a regular function call.
                    translate_function(
                        program,
                        args,
                        referenced_tables,
                        resolver,
                        target_register,
                        func_ctx,
                    )
                }
                Func::AlterTable(_) => unreachable!(),
            }
        }
        ast::Expr::FunctionCallStar { name, filter_over } => {
            // Handle func(*) syntax as a function call with 0 arguments
            // This is equivalent to func() for functions that accept 0 arguments
            let args_count = 0;
            let func_type = resolver.resolve_function(name.as_str(), args_count)?;

            if func_type.is_none() {
                crate::bail_parse_error!("no such function: {}", name.as_str());
            }

            let func = func_type.unwrap();

            // Check if this function supports the (*) syntax by verifying it can be called with 0 args
            match &func {
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
                Func::Window(_) => {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str())
                }
                // For functions that need star expansion (json_object, jsonb_object),
                // expand the * to all columns from the referenced tables as key-value pairs
                _ if func.needs_star_expansion() => {
                    let tables = referenced_tables.ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "{}(*) requires a FROM clause",
                            name.as_str()
                        ))
                    })?;

                    // Verify there's at least one table to expand
                    if tables.joined_tables().is_empty() {
                        return Err(LimboError::ParseError(format!(
                            "{}(*) requires a FROM clause",
                            name.as_str()
                        )));
                    }

                    // Build arguments: alternating column_name (as string literal), column_value (as column reference)
                    let mut args: Vec<Box<ast::Expr>> = Vec::new();

                    for table in tables.joined_tables().iter() {
                        for (col_idx, col) in table.columns().iter().enumerate() {
                            // Skip hidden columns (like rowid in some cases)
                            if col.hidden() {
                                continue;
                            }

                            // Add column name as a string literal
                            // Note: ast::Literal::String values must be wrapped in single quotes
                            // because sanitize_string() strips the first and last character
                            let col_name = col
                                .name
                                .clone()
                                .unwrap_or_else(|| format!("column{}", col_idx + 1));
                            let quoted_col_name = format!("'{col_name}'");
                            args.push(Box::new(ast::Expr::Literal(ast::Literal::String(
                                quoted_col_name,
                            ))));

                            // Add column reference using Expr::Column
                            args.push(Box::new(ast::Expr::Column {
                                database: None,
                                table: table.internal_id,
                                column: col_idx,
                                is_rowid_alias: col.is_rowid_alias(),
                            }));
                        }
                    }

                    // Create a synthetic FunctionCall with the expanded arguments
                    let synthetic_call = ast::Expr::FunctionCall {
                        name: name.clone(),
                        distinctness: None,
                        args,
                        filter_over: filter_over.clone(),
                        order_by: vec![],
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
            // Check for custom type expression overrides (e.g. `value` placeholder)
            if let Some(&reg) = program.id_register_overrides.get(id.as_str()) {
                program.emit_insn(Insn::Copy {
                    src_reg: reg,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
                return Ok(target_register);
            }
            if !resolver.dqs_dml.is_enabled() {
                crate::bail_parse_error!("no such column: {}", id.as_str());
            }
            // DQS enabled: treat double-quoted identifiers as string literals (SQLite compatibility)
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
        } if table_ref_id.is_self_table() => {
            // the table is a SELF_TABLE placeholder (used for generated columns), so we now have
            // to resolve it to the actual reference id using the SelfTableContext.
            return program.with_existing_self_table_context(|program, self_table_context| {
                match self_table_context {
                    Some(SelfTableContext::ForSelect {
                        table_ref_id: real_id,
                        ref referenced_tables,
                    }) => {
                        let real_col = Expr::Column {
                            database: None,
                            table: *real_id,
                            column: *column,
                            is_rowid_alias: *is_rowid_alias,
                        };
                        translate_expr(
                            program,
                            Some(referenced_tables),
                            &real_col,
                            target_register,
                            resolver,
                        )
                    }
                    Some(SelfTableContext::ForDML { dml_ctx, .. }) => {
                        let src_reg = dml_ctx.to_column_reg(*column);
                        program.emit_insn(Insn::Copy {
                            src_reg,
                            dst_reg: target_register,
                            extra_amount: 0,
                        });
                        Ok(target_register)
                    }
                    None => {
                        // This error means that a program.with_self_table_context() was missing
                        // somewhere in the call stack.
                        crate::bail_parse_error!(
                            "SELF_TABLE column reference outside of generated column context"
                        );
                    }
                }
            });
        }
        ast::Expr::Column {
            database: _,
            table: table_ref_id,
            column,
            is_rowid_alias,
        } => {
            // When a cursor override is active for this table, we bypass all index logic
            // and read directly from the override cursor. This is used during hash join
            // build phases where we iterate using a separate cursor and don't want to use any index.
            let has_cursor_override = program.has_cursor_override(*table_ref_id);

            let (index, index_method, use_covering_index) = {
                if has_cursor_override {
                    (None, None, false)
                } else if let Some(table_reference) = referenced_tables
                    .expect("table_references needed translating Expr::Column")
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
                    } else if *is_rowid_alias {
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
                        let is_btree_index = index_cursor_id.is_some_and(|cid| {
                            program.get_cursor_type(cid).is_some_and(|ct| ct.is_index())
                        });
                        // FIXME(https://github.com/tursodatabase/turso/issues/4801):
                        // This is a defensive workaround for cursor desynchronization.
                        //
                        // When `use_covering_index` is false, both table AND index cursors
                        // are open and positioned at the same row. If we read some columns
                        // from the index cursor and others from the table cursor, we rely
                        // on both cursors staying synchronized.
                        //
                        // The problem: AFTER triggers can INSERT into the same table,
                        // which modifies the index btree. This repositions or invalidates
                        // the parent program's index cursor, while the table cursor remains
                        // at the correct position. Result: we read a mix of data from
                        // different rows - corruption.
                        //
                        // Why does the table cursor not have this problem? Because it's
                        // explicitly re-sought by rowid (via NotExists instruction) before
                        // each use. The rowid is stored in a register and used as a stable
                        // key. The index cursor, by contrast, just trusts its internal
                        // position (page + cell index) without re-seeking.
                        //
                        // Why not check if the table has triggers and allow the optimization
                        // when there are none? Several reasons:
                        // 1. ProgramBuilder.trigger indicates if THIS program is a trigger
                        //    subprogram, not whether the table has triggers.
                        // 2. In translate_expr(), we lack context about which table is being
                        //    modified or whether we're even in an UPDATE/INSERT/DELETE.
                        // 3. Triggers can be recursive (trigger on T inserts into U, whose
                        //    trigger inserts back into T).
                        //
                        // The proper fix is to implement SQLite's `saveAllCursors()` approach:
                        // before ANY btree write, find all cursors pointing to that btree
                        // (by root_page) and save their positions. When those cursors are
                        // next accessed, they re-seek to their saved position. This could
                        // be done lazily with a generation number per btree - cursors check
                        // if the generation changed and re-seek if needed. This would
                        // require a global cursor registry and significant refactoring.
                        //
                        // For now, we only read from the index cursor when `use_covering_index`
                        // is true, meaning only the index cursor exists (no table cursor to
                        // get out of sync with). This foregoes the optimization of reading
                        // individual columns from a non-covering index.
                        let read_from_index = if is_from_outer_query_scope {
                            is_btree_index
                        } else if is_btree_index && use_covering_index {
                            index.as_ref().is_some_and(|idx| {
                                idx.column_table_pos_to_index_pos(*column).is_some()
                            })
                        } else {
                            false
                        };

                        let Some(table_column) = table.get_column_at(*column) else {
                            crate::bail_parse_error!("column index out of bounds");
                        };
                        match table_column.generated_type() {
                            // if we're reading from an index that contains this virtual column,
                            // the index already has the computed value, so read it from the index
                            GeneratedType::Virtual { expr, .. } if !read_from_index => {
                                program.with_self_table_context(
                                    Some(&SelfTableContext::ForSelect {
                                        table_ref_id: *table_ref_id,
                                        referenced_tables: referenced_tables.unwrap().clone(),
                                    }),
                                    |program, _| {
                                        translate_expr(
                                            program,
                                            referenced_tables,
                                            expr,
                                            target_register,
                                            resolver,
                                        )?;
                                        Ok(())
                                    },
                                )?;

                                program
                                    .emit_column_affinity(target_register, table_column.affinity());
                                // The virtual column's declared collation must override
                                // whatever collation the inner expression resolved to.
                                program.set_collation(Some((table_column.collation(), false)));
                            }
                            _ => {
                                let read_cursor = if read_from_index {
                                    index_cursor_id.expect("index cursor should be opened")
                                } else {
                                    table_cursor_id
                                        .or(index_cursor_id)
                                        .expect("cursor should be opened")
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

                                // For custom type columns with ENCODE/DECODE and a
                                // default, suppress the Column instruction's default.
                                // We handle short records (ALTER TABLE ADD COLUMN) via
                                // ColumnHasField after the Column instruction.
                                let col_ref = table.get_column_at(column);
                                if let Some(col) = col_ref {
                                    if col.default.is_some() {
                                        if let Ok(Some(resolved)) = resolver
                                            .schema()
                                            .resolve_type(&col.ty_str, table.is_strict())
                                        {
                                            if resolved.chain.iter().any(|td| td.encode().is_some())
                                            {
                                                program.flags.set_suppress_column_default(true);
                                            }
                                        }
                                    }
                                }
                                program.emit_column_or_rowid(read_cursor, column, target_register);
                            }
                        }
                        let table_col_idx = *column;
                        let Some(column) = table.get_column_at(table_col_idx) else {
                            crate::bail_parse_error!("column index out of bounds");
                        };
                        // Skip affinity for custom types — the stored value is
                        // already in BASE type format; the custom type name may
                        // produce wrong affinity (e.g. "doubled" → REAL due to "DOUB").
                        //
                        // Also skip for virtual columns without a stored index value,
                        // we already applied affinity for these.
                        let virtual_already_applied =
                            table_column.is_virtual_generated() && !read_from_index;
                        if !(virtual_already_applied
                            || resolver
                                .schema()
                                .get_type_def(&column.ty_str, table.is_strict())
                                .is_some())
                        {
                            maybe_apply_affinity(column.ty(), target_register, program);
                        }

                        // Decode custom type columns (skipped when building ORDER BY sort keys
                        // for types without a `<` operator, so the sorter sorts on encoded values)
                        if !program.flags.suppress_custom_type_decode() {
                            // For custom type columns with ENCODE and a DEFAULT,
                            // we suppressed the Column default so short records
                            // (ALTER TABLE ADD COLUMN) return NULL.  Use
                            // ColumnHasField to detect short records and compute
                            // ENCODE(DEFAULT) at runtime via bytecode.
                            if let Some(type_def) = resolver
                                .schema()
                                .get_type_def(&column.ty_str, table.is_strict())
                            {
                                if type_def.encode().is_some() {
                                    if let Some(ref default_expr) = column.default {
                                        // Reconstruct the cursor id used for reading
                                        let read_cursor = if read_from_index {
                                            index_cursor_id.expect("index cursor should be opened")
                                        } else {
                                            table_cursor_id
                                                .or(index_cursor_id)
                                                .expect("cursor should be opened")
                                        };
                                        let done_label = program.allocate_label();
                                        // Jump past the default block if the record
                                        // actually has this column (not a short record).
                                        program.emit_column_has_field(
                                            read_cursor,
                                            table_col_idx,
                                            done_label,
                                        );
                                        // Short record: compute DEFAULT then ENCODE it
                                        translate_expr_no_constant_opt(
                                            program,
                                            referenced_tables,
                                            default_expr,
                                            target_register,
                                            resolver,
                                            NoConstantOptReason::RegisterReuse,
                                        )?;
                                        if let Some(encode_expr) = type_def.encode() {
                                            emit_type_expr(
                                                program,
                                                encode_expr,
                                                target_register,
                                                target_register,
                                                column,
                                                type_def,
                                                resolver,
                                            )?;
                                        }
                                        program.preassign_label_to_next_insn(done_label);
                                    }
                                }
                            }
                            emit_user_facing_column_value(
                                program,
                                target_register,
                                target_register,
                                column,
                                table.is_strict(),
                                resolver,
                            )?;
                        }
                    }
                    Ok(target_register)
                }
                Table::FromClauseSubquery(from_clause_subquery) => {
                    // For outer-scope references during table-backed materialized-subquery
                    // seeks, read from the auxiliary index cursor: coroutine result
                    // registers are not refreshed while the seek path is iterating.
                    if is_from_outer_query_scope {
                        if let Some(cursor_id) =
                            program.resolve_any_index_cursor_id_for_table_safe(*table_ref_id)
                        {
                            let index = program.resolve_index_for_cursor_id(cursor_id);
                            let idx_col = index
                                .columns
                                .iter()
                                .position(|c| c.pos_in_table == *column)
                                .expect("index column not found for subquery column");
                            program.emit_insn(Insn::Column {
                                cursor_id,
                                column: idx_col,
                                dest: target_register,
                                default: None,
                            });
                            if let Some(col) = from_clause_subquery.columns.get(*column) {
                                maybe_apply_affinity(col.ty(), target_register, program);
                            }
                            return Ok(target_register);
                        }
                    }

                    // Check if this subquery was materialized with an ephemeral index.
                    // If so, read from the index cursor; otherwise copy from result registers.
                    if let Some(refs) = referenced_tables {
                        if let Some(table_reference) = refs
                            .joined_tables()
                            .iter()
                            .find(|t| t.internal_id == *table_ref_id)
                        {
                            // Check if the operation is Search::Seek with an ephemeral index
                            if let Operation::Search(Search::Seek {
                                index: Some(index), ..
                            }) = &table_reference.op
                            {
                                if index.ephemeral {
                                    // Read from the index cursor. Index columns may be reordered
                                    // (key columns first), so find the index column position that
                                    // corresponds to the original subquery column position.
                                    let idx_col = index
                                        .columns
                                        .iter()
                                        .position(|c| c.pos_in_table == *column)
                                        .expect("index column not found for subquery column");
                                    let cursor_id = program.resolve_cursor_id(&CursorKey::index(
                                        *table_ref_id,
                                        index.clone(),
                                    ));
                                    program.emit_insn(Insn::Column {
                                        cursor_id,
                                        column: idx_col,
                                        dest: target_register,
                                        default: None,
                                    });
                                    if let Some(col) = from_clause_subquery.columns.get(*column) {
                                        maybe_apply_affinity(col.ty(), target_register, program);
                                    }
                                    return Ok(target_register);
                                }
                            }
                        }
                    }

                    // Fallback: copy from result registers (coroutine-based subquery)
                    let result_columns_start = if is_from_outer_query_scope {
                        // For outer query subqueries, look up the register from the program builder
                        // since the cloned subquery doesn't have the register set yet.
                        program.get_subquery_result_reg(*table_ref_id).expect(
                            "Outer query subquery result_columns_start_reg must be set in program",
                        )
                    } else {
                        from_clause_subquery
                            .result_columns_start_reg
                            .expect("Subquery result_columns_start_reg must be set")
                    };
                    program.emit_insn(Insn::Copy {
                        src_reg: result_columns_start + *column,
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
            let referenced_tables =
                referenced_tables.expect("table_references needed translating Expr::RowId");
            let (_, table) = referenced_tables
                .find_table_by_internal_id(*table_ref_id)
                .expect("table reference should be found");
            let Table::BTree(btree) = table else {
                crate::bail_parse_error!("no such column: rowid");
            };
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: rowid");
            }

            // When a cursor override is active, always read rowid from the override cursor.
            let has_cursor_override = program.has_cursor_override(*table_ref_id);
            let (index, use_covering_index) = if has_cursor_override {
                (None, false)
            } else if let Some(table_reference) =
                referenced_tables.find_joined_table_by_internal_id(*table_ref_id)
            {
                (
                    table_reference.op.index(),
                    table_reference.utilizes_covering_index(),
                )
            } else {
                (None, false)
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
            program.preassign_label_to_next_insn(dest_if_false);

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
            program.preassign_label_to_next_insn(dest_if_null);
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
            assert_register_range_allocated(program, target_register, exprs.len())?;
            for (i, expr) in exprs.iter().enumerate() {
                translate_expr(
                    program,
                    referenced_tables,
                    expr,
                    target_register + i,
                    resolver,
                )?;
            }
            Ok(target_register)
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before translation")
        }
        ast::Expr::FieldAccess {
            base,
            field,
            resolved,
        } => {
            let base_reg = program.alloc_register();
            translate_expr(program, referenced_tables, base, base_reg, resolver)?;

            // Fast path: if the field was resolved during binding, emit directly.
            if let Some(resolution) = resolved {
                match resolution {
                    ast::FieldAccessResolution::StructField { field_index } => {
                        program.emit_insn(Insn::StructField {
                            src_reg: base_reg,
                            field_index: *field_index,
                            dest: target_register,
                        });
                        return Ok(target_register);
                    }
                    ast::FieldAccessResolution::UnionVariant { tag_index } => {
                        program.emit_insn(Insn::UnionExtract {
                            src_reg: base_reg,
                            expected_tag: *tag_index,
                            dest: target_register,
                        });
                        return Ok(target_register);
                    }
                }
            }

            // Slow path: recursively resolve the base expression's output type,
            // then look up the field/variant in that type.
            let td = resolve_expr_output_type(base, referenced_tables, resolver)?;
            let field_name = normalize_ident(field.as_str());

            if let Some((idx, _)) = td.find_struct_field(&field_name) {
                program.emit_insn(Insn::StructField {
                    src_reg: base_reg,
                    field_index: idx,
                    dest: target_register,
                });
                return Ok(target_register);
            } else if let Some(tag_index) = td.resolve_union_tag_index(&field_name) {
                program.emit_insn(Insn::UnionExtract {
                    src_reg: base_reg,
                    expected_tag: tag_index,
                    dest: target_register,
                });
                return Ok(target_register);
            } else if td.is_struct() {
                crate::bail_parse_error!(
                    "no such field '{}' in struct type '{}'",
                    field_name,
                    td.name
                );
            } else if td.is_union() {
                crate::bail_parse_error!(
                    "no such variant '{}' in union type '{}'",
                    field_name,
                    td.name
                );
            } else {
                crate::bail_parse_error!("type '{}' is not a struct or union type", td.name);
            }
        }
        ast::Expr::Raise(resolve_type, msg_expr) => {
            let in_trigger = program.trigger.is_some();
            match resolve_type {
                ResolveType::Ignore => {
                    if !in_trigger {
                        crate::bail_parse_error!(
                            "RAISE() may only be used within a trigger-program"
                        );
                    }
                    // RAISE(IGNORE): halt the trigger subprogram and skip the triggering row
                    program.emit_insn(Insn::Halt {
                        err_code: 0,
                        description: String::new(),
                        on_error: Some(ResolveType::Ignore),
                        description_reg: None,
                    });
                }
                ResolveType::Fail | ResolveType::Abort | ResolveType::Rollback => {
                    if !in_trigger && *resolve_type != ResolveType::Abort {
                        crate::bail_parse_error!(
                            "RAISE() may only be used within a trigger-program"
                        );
                    }
                    let err_code = if in_trigger {
                        SQLITE_CONSTRAINT_TRIGGER
                    } else {
                        SQLITE_ERROR
                    };
                    match msg_expr {
                        Some(e) => match e.as_ref() {
                            ast::Expr::Literal(ast::Literal::String(s)) => {
                                program.emit_insn(Insn::Halt {
                                    err_code,
                                    description: sanitize_string(s),
                                    on_error: Some(*resolve_type),
                                    description_reg: None,
                                });
                            }
                            _ => {
                                // Expression-based error message: evaluate at runtime
                                let reg = program.alloc_register();
                                translate_expr(program, referenced_tables, e, reg, resolver)?;
                                program.emit_insn(Insn::Halt {
                                    err_code,
                                    description: String::new(),
                                    on_error: Some(*resolve_type),
                                    description_reg: Some(reg),
                                });
                            }
                        },
                        None => {
                            crate::bail_parse_error!("RAISE requires an error message");
                        }
                    };
                }
                ResolveType::Replace => {
                    crate::bail_parse_error!("REPLACE is not valid for RAISE");
                }
            }
            Ok(target_register)
        }
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
                    Value::Numeric(Numeric::Integer(int_value)) => {
                        program.emit_insn(Insn::Integer {
                            value: int_value,
                            dest: target_register,
                        });
                    }
                    Value::Numeric(Numeric::Float(real_value)) => {
                        program.emit_insn(Insn::Real {
                            value: real_value.into(),
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
                    Value::Numeric(Numeric::Integer(int_value)) => {
                        program.emit_insn(Insn::Integer {
                            value: !int_value,
                            dest: target_register,
                        });
                    }
                    Value::Numeric(Numeric::Float(real_value)) => {
                        program.emit_insn(Insn::Integer {
                            value: !(f64::from(real_value) as i64),
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
        ast::Expr::Variable(variable) => {
            let index = program.register_variable(variable);
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
        ast::Expr::Default => {
            crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
        }
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
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
fn emit_binary_expr_scalar(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    target_register: usize,
    resolver: &Resolver,
    emit_mode: BinaryEmitMode,
) -> Result<usize> {
    let (emit_fn, condition_metadata) = match emit_mode {
        BinaryEmitMode::Value => (
            emit_binary_insn
                as fn(
                    &mut ProgramBuilder,
                    &ast::Operator,
                    usize,
                    usize,
                    usize,
                    &ast::Expr,
                    &ast::Expr,
                    Option<&TableReferences>,
                    Option<ConditionMetadata>,
                    Option<&Resolver>,
                ) -> Result<()>,
            None,
        ),
        BinaryEmitMode::Condition(metadata) => (
            emit_binary_condition_insn
                as fn(
                    &mut ProgramBuilder,
                    &ast::Operator,
                    usize,
                    usize,
                    usize,
                    &ast::Expr,
                    &ast::Expr,
                    Option<&TableReferences>,
                    Option<ConditionMetadata>,
                    Option<&Resolver>,
                ) -> Result<()>,
            Some(metadata),
        ),
    };

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
            Some(resolver),
        )?;
        if op.is_comparison() {
            program.reset_collation();
        }
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
            Some(resolver),
        )?;
        // Only reset collation for comparison operators, which consume it.
        // Non-comparison operators (Concat, Add, etc.) must propagate the
        // collation to the parent expression so that e.g.
        //   (name COLLATE NOCASE || '') <> 'admin'
        // correctly applies NOCASE to the Ne comparison.
        if op.is_comparison() {
            program.reset_collation();
        }
        Ok(target_register)
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_binary_expr_row_valued(
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

fn invert_boolean_register(program: &mut ProgramBuilder, target_register: usize) {
    program.emit_insn(Insn::Not {
        reg: target_register,
        dest: target_register,
    });
}

fn row_value_component_expr(expr: &Expr, idx: usize) -> Result<Option<&Expr>> {
    match unwrap_parens(expr)? {
        Expr::Parenthesized(exprs) if exprs.len() > 1 => Ok(exprs.get(idx).map(Box::as_ref)),
        _ => Ok(None),
    }
}

fn row_component_affinity_collation(
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
        comparison_collation(lhs_for_cmp, rhs_for_cmp, referenced_tables)?,
    ))
}

fn explicit_collation(expr: &Expr) -> Result<Option<CollationSeq>> {
    let mut found = None;
    walk_expr(expr, &mut |e| -> Result<WalkControl> {
        if let Expr::Collate(_, seq) = e {
            if found.is_none() {
                found = Some(CollationSeq::new(seq.as_str()).unwrap_or_default());
            }
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

fn comparison_collation(
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
) -> Result<Option<CollationSeq>> {
    if let Some(tables) = referenced_tables {
        let lhs_collation = get_collseq_from_expr(lhs_expr, tables)?;
        if lhs_collation.is_some() {
            return Ok(lhs_collation);
        }
        return get_collseq_from_expr(rhs_expr, tables);
    }

    let lhs_collation = explicit_collation(lhs_expr)?;
    if lhs_collation.is_some() {
        return Ok(lhs_collation);
    }
    explicit_collation(rhs_expr)
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
    match expr {
        Expr::Column { table, column, .. } => {
            if let Some(tables) = referenced_tables {
                tables
                    .find_table_by_internal_id(*table)
                    .map(|(_, t)| t)
                    .and_then(|t| t.get_column_at(*column))
                    .is_some_and(|col| col.is_array())
            } else {
                false
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
                    args.iter().any(|a| expr_is_array(a, referenced_tables))
                }
                "iif" => {
                    // args: condition, then_val, else_val
                    args.get(1)
                        .is_some_and(|a| expr_is_array(a, referenced_tables))
                        || args
                            .get(2)
                            .is_some_and(|a| expr_is_array(a, referenced_tables))
                }
                "nullif" => args
                    .first()
                    .is_some_and(|a| expr_is_array(a, referenced_tables)),
                "array_element" => {
                    // Subscripting a multi-dim array yields a lower-dim array
                    if let Some(tables) = referenced_tables {
                        args.first()
                            .is_some_and(|a| expr_array_dimensions(a, tables) > 1)
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
        Expr::Array { .. } | Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        Expr::Binary(lhs, ast::Operator::Concat, rhs) => {
            expr_is_array(lhs, referenced_tables) || expr_is_array(rhs, referenced_tables)
        }
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            when_then_pairs
                .iter()
                .any(|(_, then_expr)| expr_is_array(then_expr, referenced_tables))
                || else_expr
                    .as_ref()
                    .is_some_and(|e| expr_is_array(e, referenced_tables))
        }
        _ => false,
    }
}

/// Return the number of array dimensions for an expression, or 0 for non-array.
fn expr_array_dimensions(expr: &Expr, tables: &TableReferences) -> u32 {
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
    resolver: Option<&Resolver>,
) -> Result<()> {
    let condition_metadata = condition_metadata
        .expect("condition metadata must be provided for emit_binary_insn_conditional");
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
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        ast::LikeOperator::Match => {
            // Transform MATCH to fts_match():
            // - `col MATCH 'query'` -> `fts_match(col, 'query')`
            // - `(col1, col2) MATCH 'query'` -> `fts_match(col1, col2, 'query')`
            let columns: Vec<&ast::Expr> = match lhs.as_ref() {
                ast::Expr::Parenthesized(cols) => cols.iter().map(|c| c.as_ref()).collect(),
                other => vec![other],
            };
            let arg_count = columns.len() + 1; // columns + query
            let start_reg = program.alloc_registers(arg_count);

            for (i, col) in columns.iter().enumerate() {
                translate_expr(program, referenced_tables, col, start_reg + i, resolver)?;
            }
            translate_expr(
                program,
                referenced_tables,
                rhs,
                start_reg + columns.len(),
                resolver,
            )?;

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Fts(FtsFunc::Match),
                    arg_count,
                },
            });
        }
        #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
        ast::LikeOperator::Match => {
            crate::bail_parse_error!("MATCH requires the 'fts' feature to be enabled")
        }
        ast::LikeOperator::Regexp => {
            if escape.is_some() {
                crate::bail_parse_error!("wrong number of arguments to function regexp()");
            }
            let func = resolver.resolve_function("regexp", 2)?;
            let Some(func) = func else {
                crate::bail_parse_error!("no such function: regexp");
            };
            let arg_count = 2;
            let start_reg = program.alloc_registers(arg_count);
            // regexp(pattern, haystack) — pattern is rhs, haystack is lhs
            translate_expr(program, referenced_tables, rhs, start_reg, resolver)?;
            translate_expr(program, referenced_tables, lhs, start_reg + 1, resolver)?;
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg,
                dest: target_register,
                func: FuncCtx { func, arg_count },
            });
        }
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

/// Read a single column from a BTreeTable cursor, transparently computing
/// virtual generated columns inline instead of hitting `emit_column`.
/// All bulk column-reading call sites should use this instead of
/// `emit_column_or_rowid` directly.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    table_ref_id: TableInternalId,
    referenced_tables: &TableReferences,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    do_emit_table_column(
        program,
        cursor_id,
        &SelfTableContext::ForSelect {
            table_ref_id,
            referenced_tables: referenced_tables.clone(),
        },
        Some(referenced_tables),
        column,
        column_index,
        target_register,
        resolver,
    )
}

/// Equivalent of [emit_table_column] for when registers are laid out for DML.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column_for_dml(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    dml_column_context: DmlColumnContext,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    do_emit_table_column(
        program,
        cursor_id,
        &SelfTableContext::ForDML {
            dml_ctx: dml_column_context,
            table: Arc::clone(table),
        },
        None,
        column,
        column_index,
        target_register,
        resolver,
    )
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn do_emit_table_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    self_table_context: &SelfTableContext,
    referenced_tables: Option<&TableReferences>,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    match column.generated_type() {
        GeneratedType::Virtual { expr, .. } => {
            program.with_self_table_context(Some(self_table_context), |program, _| {
                translate_expr(program, referenced_tables, expr, target_register, resolver)?;
                Ok(())
            })?;
            program.emit_column_affinity(target_register, column.affinity());
        }
        _ => {
            program.emit_column_or_rowid(cursor_id, column_index, target_register);
        }
    }
    Ok(())
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
) -> Result<Option<(&ast::Expr, ConstraintOperator, &ast::Expr)>> {
    match unwrap_parens(expr)? {
        ast::Expr::Binary(lhs, operator, rhs)
            if matches!(
                operator,
                ast::Operator::Equals
                    | ast::Operator::NotEquals
                    | ast::Operator::Greater
                    | ast::Operator::Less
                    | ast::Operator::GreaterEquals
                    | ast::Operator::LessEquals
                    | ast::Operator::Is
                    | ast::Operator::IsNot
            ) =>
        {
            // Row-valued binary comparisons are translated directly in expression codegen.
            // They are not safe to expose as scalar binary constraints in optimizer paths.
            if expr_vector_size(lhs)? > 1 || expr_vector_size(rhs)? > 1 {
                return Ok(None);
            }
            Ok(Some((lhs.as_ref(), (*operator).into(), rhs.as_ref())))
        }
        ast::Expr::Like { lhs, not, rhs, .. } => Ok(Some((
            lhs.as_ref(),
            ConstraintOperator::Like { not: *not },
            rhs.as_ref(),
        ))),
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
                let (expr, count) = unwrap_parens_owned(*exprs.pop().unwrap())?;
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
                ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                    unreachable!(
                        "Array and Subscript are desugared into function calls by the parser"
                    )
                }
                ast::Expr::Id(_)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::Literal(_)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_)
                | ast::Expr::Default => {
                    // No nested expressions
                }
                ast::Expr::FieldAccess { base, .. } => {
                    walk_expr(base, func)?;
                }
            }
        }
        WalkControl::SkipChildren => return Ok(WalkControl::Continue),
    };
    Ok(WalkControl::Continue)
}

pub fn expr_references_subquery_id(expr: &ast::Expr, subquery_id: TableInternalId) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::SubqueryResult {
            subquery_id: sid, ..
        } = e
        {
            if *sid == subquery_id {
                found = true;
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    });
    found
}

pub fn expr_references_any_subquery(expr: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if matches!(e, ast::Expr::SubqueryResult { .. }) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
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

/// The precedence of binding identifiers to columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingBehavior {
    /// `TryResultColumnsFirst` means that result columns (e.g. SELECT x AS y, ...) take precedence over canonical columns (e.g. SELECT x, y AS z, ...). This is the default behavior.
    TryResultColumnsFirst,
    /// `TryCanonicalColumnsFirst` means that canonical columns take precedence over result columns. This is used for e.g. WHERE clauses.
    TryCanonicalColumnsFirst,
    /// `ResultColumnsNotAllowed` means that referring to result columns is not allowed. This is used e.g. for DML statements.
    ResultColumnsNotAllowed,
    /// `AllowUnboundIdentifiers` means that unbound identifiers are allowed. This is used for INSERT ... ON CONFLICT DO UPDATE SET ... where binding is handled later than this phase.
    AllowUnboundIdentifiers,
}

/// The result of resolving the `<id>` half of a qualified `<tbl>.<id>`
/// reference against a single candidate table whose identifier already
/// matches `<tbl>`.
#[derive(Debug, Clone, Copy)]
enum QualifiedMatch {
    /// `<id>` named a real column on the candidate table.
    Column {
        col_idx: usize,
        is_rowid_alias: bool,
    },
    /// `<id>` named the rowid (`rowid`/`oid`/`_rowid_`) of a btree.
    /// There is no column index — the result must become an
    /// `Expr::RowId`, not an `Expr::Column`.
    RowId,
}

/// Resolve `<id>` against a single table reference.
///
/// The caller is responsible for:
///   * filtering candidate refs down to those whose identifier matches `<tbl>`,
///   * detecting ambiguity across multiple candidate refs,
///   * applying any scope-specific USING/NATURAL dedup rules.
///
/// Returns:
///   * `Ok(Some(Column { .. }))` — `<id>` is a real column on `table`.
///   * `Ok(Some(RowId))` — `<id>` is a rowid alias on a rowid btree.
///   * `Ok(None)` — `<id>` is not present on this ref.
///   * `Err(_)` — `<id>` is a rowid alias but the btree has no rowid
///     (definitively invalid; reported as "no such column: <id>" per SQLite).
fn resolve_qualified_on_ref(
    table: &Table,
    internal_id: TableInternalId,
    normalized_id: &str,
) -> Result<Option<QualifiedMatch>> {
    if let Some(col_idx) = table.columns().iter().position(|c| {
        c.name
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case(normalized_id))
    }) {
        let col = table.columns().get(col_idx).unwrap();
        return Ok(Some(QualifiedMatch::Column {
            col_idx,
            is_rowid_alias: col.is_rowid_alias(),
        }));
    }

    if let Table::BTree(btree) = table {
        if parse_row_id(normalized_id, internal_id, || false)?.is_some() {
            if !btree.has_rowid {
                crate::bail_parse_error!("no such column: {}", normalized_id);
            }
            return Ok(Some(QualifiedMatch::RowId));
        }
    }

    Ok(None)
}

/// Rewrite ast::Expr in place, binding Column references/rewriting Expr::Id -> Expr::Column
/// using the provided TableReferences, and replacing anonymous parameters with internal named
/// ones
pub fn bind_and_rewrite_expr<'a>(
    top_level_expr: &mut ast::Expr,
    mut referenced_tables: Option<&'a mut TableReferences>,
    result_columns: Option<&'a [ResultSetColumn]>,
    resolver: &Resolver<'_>,
    binding_behavior: BindingBehavior,
) -> Result<()> {
    let _stack = crate::stack::trace_scope("expr:bind_and_rewrite");
    walk_expr_mut(
        top_level_expr,
        &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                Expr::Id(id) => {
                    let _stack = crate::stack::trace_scope("expr:bind_id");
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
                    let joined_tables = referenced_tables.joined_tables();

                    // First check joined tables
                    for joined_table in joined_tables.iter() {
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
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}",
                                        id.as_str()
                                    );
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
                        } else if let Table::BTree(btree) = &joined_table.table {
                            if let Some(row_id_expr) =
                                parse_row_id(&normalized_id, joined_tables[0].internal_id, || {
                                    joined_tables.len() != 1
                                })?
                            {
                                if !btree.has_rowid {
                                    crate::bail_parse_error!("no such column: {}", id.as_str());
                                }
                                *expr = row_id_expr;
                                return Ok(WalkControl::Continue);
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
                    //
                    // Ambiguity is only checked within the same scope depth. Once a match
                    // is found at depth N, deeper scopes (N+1, N+2, ...) are not checked.
                    if match_result.is_none() {
                        let mut matched_scope_depth = None;
                        for outer_ref in referenced_tables.outer_query_refs().iter() {
                            // CTEs (FromClauseSubquery) in outer_query_refs are only for table
                            // lookup (e.g., FROM cte1), not for column resolution. Columns from
                            // CTEs should only be accessible when the CTE is explicitly in the
                            // FROM clause, not as implicit outer references.
                            if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
                                continue;
                            }
                            // Skip refs from deeper scopes once we found a match
                            if let Some(depth) = matched_scope_depth {
                                if outer_ref.scope_depth > depth {
                                    continue;
                                }
                            }
                            let col_idx = outer_ref.table.columns().iter().position(|c| {
                                c.name
                                    .as_ref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
                            });
                            if col_idx.is_some() {
                                let col_idx = col_idx.unwrap();
                                if outer_ref.using_dedup_hidden_cols.get(col_idx) {
                                    continue;
                                }
                                if match_result.is_some() {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}",
                                        id.as_str()
                                    );
                                }
                                let col = outer_ref.table.columns().get(col_idx).unwrap();
                                match_result =
                                    Some((outer_ref.internal_id, col_idx, col.is_rowid_alias()));
                                matched_scope_depth = Some(outer_ref.scope_depth);
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

                    // SQLite DQS misfeature: double-quoted identifiers fall back to string literals
                    // only when DQS is enabled for DML statements
                    if id.quoted_with('"') && resolver.dqs_dml.is_enabled() {
                        *expr = Expr::Literal(ast::Literal::String(id.as_literal()));
                        return Ok(WalkControl::Continue);
                    } else {
                        crate::bail_parse_error!("no such column: {}", id.as_str())
                    }
                }
                Expr::Qualified(tbl, id) => {
                    let _stack = crate::stack::trace_scope("expr:bind_qualified");
                    // Resolve a `<tbl>.<id>` reference.
                    //
                    // Two-stage lookup with shadowing:
                    //   1. Search the current scope's FROM tables (`joined_tables`).
                    //   2. Fall back to enclosing scopes (`outer_query_refs`), restricted to
                    //      the *nearest* scope whose identifier matches — so an inner alias
                    //      shadows a same-named alias in an outer scope instead of conflicting.
                    //
                    // Produces either `Expr::Column` (real column) or `Expr::RowId`
                    // (bare rowid alias like `t.rowid` on a btree with rowids).
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
                    let normalized_table_name = {
                        let _stack = crate::stack::trace_scope("expr:qualified:normalize_table");
                        normalize_ident(tbl.as_str())
                    };
                    let normalized_id = {
                        let _stack = crate::stack::trace_scope("expr:qualified:normalize_column");
                        normalize_ident(id.as_str())
                    };

                    // `resolved` holds the accepted binding (at most one).
                    // `identifier_matched` is true once *any* scope produced a table whose
                    // identifier equals `tbl`; it distinguishes "no such table" from
                    // "no such column" in error reporting below.
                    let mut resolved: Option<(TableInternalId, QualifiedMatch)> = None;
                    let mut identifier_matched = false;

                    let ambiguous = || -> LimboError {
                        LimboError::ParseError(format!(
                            "ambiguous column name: {}.{}",
                            tbl.as_str(),
                            id.as_str()
                        ))
                    };

                    // --- Stage 1: search the current scope's FROM tables. ---
                    {
                        let _stack = crate::stack::trace_scope("expr:qualified:find_table");
                        for joined_table in referenced_tables
                            .joined_tables()
                            .iter()
                            .filter(|t| t.identifier == normalized_table_name)
                        {
                            identifier_matched = true;
                            let Some(candidate) = resolve_qualified_on_ref(
                                &joined_table.table,
                                joined_table.internal_id,
                                &normalized_id,
                            )?
                            else {
                                continue;
                            };

                            // Multiple FROM tables share this identifier and both contain `id`.
                            // For column matches, a USING/NATURAL join on `id` lets the first
                            // match stand (the duplicate side is implicitly merged). Rowid
                            // matches never get this exception (rowid isn't a USING column).
                            if resolved.is_some() {
                                let allowed_by_using =
                                    matches!(candidate, QualifiedMatch::Column { .. })
                                        && joined_table.join_info.as_ref().is_some_and(|ji| {
                                            ji.using.iter().any(|u| {
                                                u.as_str().eq_ignore_ascii_case(&normalized_id)
                                            })
                                        });
                                if !allowed_by_using {
                                    return Err(ambiguous());
                                }
                                continue;
                            }
                            resolved = Some((joined_table.internal_id, candidate));
                        }
                    }
                    // --- Stage 2: fall back to enclosing scopes ---
                    // Only attempted if no inner-scope table matched the identifier — an
                    // inner alias of the same name shadows everything outside.
                    //
                    // We pick the *nearest* outer scope that contains a matching identifier
                    // (smallest `scope_depth`) and search only refs at that depth. This lets
                    // the same alias be reused at different nesting levels without triggering
                    // spurious "ambiguous column" errors across unrelated scopes.
                    //
                    // `cte_definition_only` refs are excluded: those entries exist purely so
                    // that a subquery's FROM clause can *look up* the CTE by name; once the
                    // CTE is consumed into a FROM, column resolution must go through the
                    // corresponding `joined_table`, not the definition-only ref.
                    if !identifier_matched {
                        let _stack = crate::stack::trace_scope("expr:qualified:find_outer_ref");
                        let nearest_outer_scope = referenced_tables
                            .outer_query_refs()
                            .iter()
                            .filter(|t| {
                                !t.cte_definition_only && t.identifier == normalized_table_name
                            })
                            .map(|t| t.scope_depth)
                            .min();

                        if let Some(scope_depth) = nearest_outer_scope {
                            identifier_matched = true;
                            for outer_ref in
                                referenced_tables.outer_query_refs().iter().filter(|t| {
                                    !t.cte_definition_only
                                        && t.scope_depth == scope_depth
                                        && t.identifier == normalized_table_name
                                })
                            {
                                let Some(candidate) = resolve_qualified_on_ref(
                                    &outer_ref.table,
                                    outer_ref.internal_id,
                                    &normalized_id,
                                )?
                                else {
                                    continue;
                                };

                                // When multiple outer refs share this identifier
                                // (e.g. self-join `t1 JOIN t1 USING(a)`), a USING-hidden
                                // column on the duplicate side lets the first match stand,
                                // mirroring the Stage 1 logic for local-scope tables.
                                if resolved.is_some() {
                                    let allowed_by_using = matches!(
                                        candidate,
                                        QualifiedMatch::Column { col_idx, .. }
                                            if outer_ref.using_dedup_hidden_cols.get(col_idx)
                                    );
                                    if !allowed_by_using {
                                        return Err(ambiguous());
                                    }
                                    continue;
                                }
                                resolved = Some((outer_ref.internal_id, candidate));
                            }
                        }
                    }

                    // --- Error reporting. ---
                    if resolved.is_none() && !identifier_matched {
                        // No scope contains a table with this identifier. Normally we
                        // report "no such table", but there is one case where SQLite
                        // reports "no such column" instead: when the identifier names a
                        // CTE that was preplanned for subquery FROM visibility and kept
                        // as a definition-only outer ref. The CTE *name* is valid in
                        // principle; it's the column access through it that isn't,
                        // because the CTE hasn't been brought into this scope's FROM.
                        // The `cte_id`/`cte_select` check restricts this to real CTE
                        // definition refs so any other future use of `cte_definition_only`
                        // still falls through to "no such table".
                        let is_definition_only_cte = {
                            let _stack =
                                crate::stack::trace_scope("expr:qualified:outer_ref_check");
                            referenced_tables
                                .find_outer_query_ref_by_identifier(&normalized_table_name)
                                .is_some_and(|outer_ref| {
                                    outer_ref.cte_definition_only
                                        && (outer_ref.cte_id.is_some()
                                            || outer_ref.cte_select.is_some())
                                })
                        };
                        if is_definition_only_cte {
                            crate::bail_parse_error!(
                                "no such column: {}.{}",
                                tbl.as_str(),
                                id.as_str()
                            );
                        }
                        // Dot-notation fallback for struct/union field access (DuckDB-style precedence).
                        //
                        // For `a.b`, resolution order is:
                        //   1. a=table, b=column       (handled above — if we're here, this failed)
                        //   2. a=column, b=struct field (handled below)
                        //
                        // This means table references always win over struct field access.
                        // If a table `t` has a struct column also named `t` with field `x`,
                        // `t.x` resolves as table.column, not column.field. The user can
                        // disambiguate with an alias: `SELECT s.t.x FROM t AS s`.
                        //
                        // We do NOT reject ambiguous schemas at CREATE TABLE time because
                        // the combinatorial explosion (CREATE TYPE, CREATE TABLE, ALTER TABLE)
                        // makes that impractical. Deterministic precedence is sufficient.
                        let field_name = {
                            let _stack =
                                crate::stack::trace_scope("expr:qualified:normalize_field");
                            normalize_ident(id.as_str())
                        };
                        if let Some(m) = {
                            let _stack =
                                crate::stack::trace_scope("expr:qualified:find_custom_type_column");
                            find_custom_type_column(
                                referenced_tables,
                                &normalized_table_name,
                                resolver,
                            )?
                        } {
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:make_field_access");
                                *expr = make_field_access_expr(
                                    m.table_id,
                                    m.col_idx,
                                    m.is_rowid_alias,
                                    &field_name,
                                    m.type_def,
                                );
                            }
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:mark_column_used");
                                referenced_tables.mark_column_used(m.table_id, m.col_idx);
                            }
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!("no such table: {}", normalized_table_name);
                    }
                    // Identifier matched somewhere but no column/rowid binding was
                    // produced — the table exists, the column doesn't.
                    let Some((tbl_id, binding)) = resolved else {
                        crate::bail_parse_error!("no such column: {}", normalized_id);
                    };

                    match binding {
                        QualifiedMatch::Column {
                            col_idx,
                            is_rowid_alias,
                        } => {
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:rewrite_column");
                                *expr = Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: tbl_id,
                                    column: col_idx,
                                    is_rowid_alias,
                                };
                            }
                            tracing::debug!("rewritten to column");
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:mark_column_used");
                                referenced_tables.mark_column_used(tbl_id, col_idx);
                            }
                        }
                        QualifiedMatch::RowId => {
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:rewrite_rowid");
                                *expr = Expr::RowId {
                                    database: None, // TODO: support different databases
                                    table: tbl_id,
                                };
                            }
                            tracing::debug!("rewritten to rowid");
                            {
                                let _stack =
                                    crate::stack::trace_scope("expr:qualified:mark_rowid_used");
                                referenced_tables.mark_rowid_referenced(tbl_id);
                            }
                        }
                    }
                    return Ok(WalkControl::Continue);
                }
                Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                    let _stack = crate::stack::trace_scope("expr:bind_doubly_qualified");
                    // Clone the names upfront so we can reassign *expr later
                    // without lifetime conflicts.
                    let db_name_str = db_name.as_str().to_string();
                    let tbl_name_str = tbl_name.as_str().to_string();
                    let col_name_str = col_name.as_str().to_string();
                    let tbl_name_clone = tbl_name.clone();
                    let db_name_clone = db_name.clone();

                    let Some(referenced_tables) = &mut referenced_tables else {
                        if binding_behavior == BindingBehavior::AllowUnboundIdentifiers {
                            return Ok(WalkControl::Continue);
                        }
                        crate::bail_parse_error!(
                            "no such column: {}.{}.{}",
                            db_name_str,
                            tbl_name_str,
                            col_name_str
                        );
                    };
                    let normalized_col_name = normalize_ident(&col_name_str);

                    // DoublyQualified: `a.b.c` — DuckDB-style precedence:
                    //   1. a=database, b=table, c=column     (tried first)
                    //   2. a=table,    b=column, c=struct field  (fallback)
                    //
                    // Same principle as Qualified: schema-level references always win.
                    let qualified_name = ast::QualifiedName {
                        db_name: Some(db_name_clone),
                        name: tbl_name_clone,
                        alias: None,
                    };
                    let db_resolution = resolver.resolve_database_id(&qualified_name);

                    // Try db.table.column interpretation first. If database resolves AND
                    // the table+column exist, use that. Otherwise fall through to
                    // table.column.field for struct/union field access.
                    let mut resolved_as_db_table_col = false;
                    if let Ok(database_id) = db_resolution {
                        let table = resolver
                            .with_schema(database_id, |schema| schema.get_table(&tbl_name_str));

                        if let Some(table) = table {
                            let col_idx = table.columns().iter().position(|c| {
                                c.name.as_ref().is_some_and(|name| {
                                    name.eq_ignore_ascii_case(&normalized_col_name)
                                })
                            });

                            if let Some(col_idx) = col_idx {
                                let col = table.columns().get(col_idx).unwrap();
                                let is_rowid_alias = col.is_rowid_alias();
                                let normalized_tbl_name = normalize_ident(&tbl_name_str);
                                let matching_tbl = referenced_tables
                                    .find_table_and_internal_id_by_identifier(&normalized_tbl_name);

                                if let Some((tbl_id, _)) = matching_tbl {
                                    *expr = Expr::Column {
                                        database: Some(database_id),
                                        table: tbl_id,
                                        column: col_idx,
                                        is_rowid_alias,
                                    };
                                    referenced_tables.mark_column_used(tbl_id, col_idx);
                                    resolved_as_db_table_col = true;
                                } else {
                                    // Table exists in database but not in FROM clause
                                    return Err(LimboError::ParseError(format!(
                                        "table {normalized_tbl_name} is not in FROM clause — \
                                         cross-database column references require the table to be explicitly joined"
                                    )));
                                }
                            }
                        }
                    }
                    if !resolved_as_db_table_col {
                        // db.table.column failed — try table.column.field for struct/union
                        let normalized_tbl_name = normalize_ident(&db_name_str);
                        let normalized_col = normalize_ident(&tbl_name_str);
                        let field_name = normalize_ident(&col_name_str);
                        let matching_tbl = referenced_tables
                            .find_table_and_internal_id_by_identifier(&normalized_tbl_name);
                        if let Some((tbl_id, tbl)) = matching_tbl {
                            let col_idx = tbl.columns().iter().position(|c| {
                                c.name
                                    .as_ref()
                                    .is_some_and(|n| n.eq_ignore_ascii_case(&normalized_col))
                            });
                            if let Some(col_idx) = col_idx {
                                let col = &tbl.columns()[col_idx];
                                let type_def =
                                    resolver.schema().get_type_def_unchecked(&col.ty_str);
                                let is_struct_or_union = type_def
                                    .map(|td| td.is_struct() || td.is_union())
                                    .unwrap_or(false);
                                if is_struct_or_union {
                                    *expr = make_field_access_expr(
                                        tbl_id,
                                        col_idx,
                                        col.is_rowid_alias(),
                                        &field_name,
                                        type_def.unwrap(),
                                    );
                                    referenced_tables.mark_column_used(tbl_id, col_idx);
                                    return Ok(WalkControl::Continue);
                                } else {
                                    // Column exists but is not a struct/union type
                                    return Err(LimboError::ParseError(format!(
                                        "column '{normalized_col}' is not a STRUCT or UNION type; \
                                         cannot access field '{field_name}'"
                                    )));
                                }
                            }
                        }
                        // Fallback (3): column.field.subfield for nested struct/union access
                        // Handles:
                        //   data.telegram.chat_id — UNION column, variant with struct type
                        //   data.sub.a            — STRUCT column, struct-typed field, sub-field
                        let col_name_norm = normalize_ident(&db_name_str);
                        let mid_name = normalize_ident(&tbl_name_str);
                        let leaf_field = normalize_ident(&col_name_str);
                        if let Some((nested_expr, tbl_id, col_idx)) =
                            try_resolve_nested_field_access(
                                referenced_tables,
                                &col_name_norm,
                                &mid_name,
                                &leaf_field,
                                resolver,
                            )?
                        {
                            *expr = nested_expr;
                            referenced_tables.mark_column_used(tbl_id, col_idx);
                        } else {
                            return Err(LimboError::ParseError(format!(
                                "no such column or database: {db_name_str}.{tbl_name_str}.{col_name_str}"
                            )));
                        }
                    }
                }
                Expr::FunctionCallStar { name, filter_over } => {
                    // For functions that need star expansion (json_object, jsonb_object),
                    // expand the * to all columns from the referenced tables as key-value pairs
                    // This needs to happen during bind/rewrite so WHERE clauses can use these functions
                    if let Some(referenced_tables) = &mut referenced_tables {
                        if let Ok(Some(func)) = Func::resolve_function(name.as_str(), 0) {
                            if func.needs_star_expansion() {
                                // Only expand if there are actual tables - otherwise leave as
                                // FunctionCallStar so translate_expr can generate the error
                                let joined_tables = referenced_tables.joined_tables();
                                if !joined_tables.is_empty() {
                                    // Mark all columns as used so the optimizer doesn't
                                    // create partial covering indexes that would miss columns
                                    let joined_tables = referenced_tables.joined_tables_mut();
                                    for table in joined_tables.iter_mut() {
                                        for col_idx in 0..table.columns().len() {
                                            table.mark_column_used(col_idx);
                                        }
                                    }

                                    // Build arguments: alternating column_name (as string literal), column_value (as column reference)
                                    let mut args: Vec<Box<ast::Expr>> = Vec::new();

                                    let joined_tables = referenced_tables.joined_tables();
                                    for table in joined_tables.iter() {
                                        for (col_idx, col) in table.columns().iter().enumerate() {
                                            // Skip hidden columns (like rowid in some cases)
                                            if col.hidden() {
                                                continue;
                                            }

                                            // Add column name as a string literal
                                            let col_name = col.name.clone().unwrap_or_else(|| {
                                                format!("column{}", col_idx + 1)
                                            });
                                            let quoted_col_name = format!("'{col_name}'");
                                            args.push(Box::new(ast::Expr::Literal(
                                                ast::Literal::String(quoted_col_name),
                                            )));

                                            // Add column reference using Expr::Column
                                            args.push(Box::new(ast::Expr::Column {
                                                database: None,
                                                table: table.internal_id,
                                                column: col_idx,
                                                is_rowid_alias: col.is_rowid_alias(),
                                            }));
                                        }
                                    }

                                    // Replace FunctionCallStar with expanded FunctionCall
                                    *expr = ast::Expr::FunctionCall {
                                        name: name.clone(),
                                        distinctness: None,
                                        args,
                                        filter_over: filter_over.clone(),
                                        order_by: vec![],
                                    };
                                }
                            }
                        }
                    }
                }
                // Validate struct/union function calls at bind time.
                // Principle: compile-time checks belong in the earliest phase that
                // has enough context. Binding has the resolver (for custom-types
                // gate) and the raw AST args (for arity and literal checks).
                // Catching errors here avoids wasting optimizer and translation
                // cycles on invalid queries, and keeps the translate_expr match
                // arms focused on code generation.
                Expr::FunctionCall { name, args, .. } => {
                    validate_custom_type_function_call(name.as_str(), args, resolver)?;
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )?;
    Ok(())
}

/// Extract a string literal value from an expression that has already been
/// validated as `Expr::Literal(Literal::String(_))` during bind-time checks.
fn extract_string_literal(expr: &ast::Expr) -> crate::Result<String> {
    match expr {
        ast::Expr::Literal(ast::Literal::String(s)) => Ok(s.trim_matches('\'').to_string()),
        _ => crate::bail_parse_error!("expected a string literal argument"),
    }
}

/// Resolve the UnionDef for a column expression. Returns the variant names list
/// and optionally resolves a tag name to its numeric index.
/// Used by union_value, union_tag, union_extract function translation.
///
/// In the DML index-maintenance path (INSERT with expression indexes),
/// `referenced_tables` is `None` and columns use `SELF_TABLE`. We fall back
/// to the ProgramBuilder's `SelfTableContext::ForDML` to obtain column metadata.
/// Resolve the TypeDef for a column expression (Column or DML self-table column).
fn resolve_typedef_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    program: &ProgramBuilder,
) -> Option<Arc<TypeDef>> {
    let ty_str = match expr {
        ast::Expr::Column { table, column, .. } => {
            resolve_column_type_str(*table, *column, referenced_tables, resolver, program)?
        }
        ast::Expr::Variable(var) => var.col_type.as_ref()?.to_string(),
        _ => return None,
    };
    let td = resolver.schema().get_type_def_unchecked(&ty_str)?;
    Some(Arc::clone(td))
}

fn resolve_union_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    program: &ProgramBuilder,
) -> Option<Arc<TypeDef>> {
    resolve_typedef_from_column(expr, referenced_tables, resolver, program)
        .filter(|td| td.is_union())
}

/// Resolve the struct TypeDef that an expression evaluates to.
///
/// Handles column references (direct struct column),
/// `union_extract(...)` (variant's struct type), and
/// `struct_extract(...)` (field's struct type for nested extraction).
fn resolve_struct_from_expr(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    program: &ProgramBuilder,
) -> Option<Arc<TypeDef>> {
    match expr {
        ast::Expr::Column { .. } => {
            resolve_typedef_from_column(expr, referenced_tables, resolver, program)
                .filter(|td| td.is_struct())
        }
        ast::Expr::FunctionCall { name, args, .. } => {
            let normalized = crate::util::normalize_ident(name.as_str());
            match normalized.as_str() {
                // union_extract(col, 'tag') → variant's type
                "union_extract" if args.len() == 2 => {
                    let tag_name = extract_string_literal(&args[1]).ok()?;
                    let union_td =
                        resolve_union_from_column(&args[0], referenced_tables, resolver, program)?;
                    let (_, variant) = union_td.find_union_variant(&tag_name)?;
                    let struct_td = resolver
                        .schema()
                        .get_type_def_unchecked(&variant.type_name)?;
                    if struct_td.is_struct() {
                        Some(Arc::clone(struct_td))
                    } else {
                        None
                    }
                }
                // struct_extract(expr, 'field') → field's type (if it's a struct)
                "struct_extract" if args.len() == 2 => {
                    let field_name = extract_string_literal(&args[1]).ok()?;
                    let parent_td =
                        resolve_struct_from_expr(&args[0], referenced_tables, resolver, program)?;
                    let (_, field_def) = parent_td.find_struct_field(&field_name)?;
                    let field_td = resolver
                        .schema()
                        .get_type_def_unchecked(&field_def.type_name)?;
                    if field_td.is_struct() {
                        Some(Arc::clone(field_td))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the type string for a column, handling both referenced_tables and DML context.
fn resolve_column_type_str(
    table: ast::TableInternalId,
    column: usize,
    referenced_tables: Option<&TableReferences>,
    _resolver: &Resolver,
    program: &ProgramBuilder,
) -> Option<String> {
    if let Some(rt) = referenced_tables {
        if let Some((_, tbl)) = rt.find_table_by_internal_id(table) {
            return Some(tbl.columns().get(column)?.ty_str.clone());
        }
    }
    if table.is_self_table() {
        if let Some(SelfTableContext::ForDML { table, .. }) = program.current_self_table_context() {
            return Some(table.columns().get(column)?.ty_str.clone());
        }
    }
    None
}

/// Result of finding a column with a custom (struct/union) type across joined tables.
struct CustomTypeColumnMatch<'a> {
    table_id: TableInternalId,
    col_idx: usize,
    is_rowid_alias: bool,
    type_def: &'a crate::schema::TypeDef,
}

/// Search all joined tables for a column named `col_name` with a struct/union type.
/// Errors on ambiguity (>1 match). Returns `None` if no match.
fn find_custom_type_column<'a>(
    referenced_tables: &TableReferences,
    col_name: &str,
    resolver: &'a Resolver<'a>,
) -> crate::Result<Option<CustomTypeColumnMatch<'a>>> {
    let mut result: Option<CustomTypeColumnMatch<'a>> = None;
    let mut match_count = 0usize;
    for joined_table in referenced_tables.joined_tables().iter() {
        let cols = joined_table.table.columns();
        if let Some(col_idx) = cols.iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
        }) {
            let col = &cols[col_idx];
            let type_def = resolver.schema().get_type_def_unchecked(&col.ty_str);
            let is_struct_or_union = type_def
                .map(|td| td.is_struct() || td.is_union())
                .unwrap_or(false);
            if is_struct_or_union {
                match_count += 1;
                result = Some(CustomTypeColumnMatch {
                    table_id: joined_table.internal_id,
                    col_idx,
                    is_rowid_alias: col.is_rowid_alias(),
                    type_def: type_def.unwrap(),
                });
            }
        }
    }
    if match_count > 1 {
        crate::bail_parse_error!(
            "ambiguous column reference: '{}' — multiple tables have a struct/union column with this name",
            col_name
        );
    }
    Ok(result)
}

/// Build an `Expr::FieldAccess { base: Expr::Column { ... }, field, resolved }` node,
/// pre-resolving the field index via `resolve_field_access`.
fn make_field_access_expr(
    table_id: TableInternalId,
    col_idx: usize,
    is_rowid_alias: bool,
    field_name: &str,
    td: &crate::schema::TypeDef,
) -> Expr {
    let resolved = resolve_field_access(td, field_name);
    Expr::FieldAccess {
        base: Box::new(Expr::Column {
            database: None,
            table: table_id,
            column: col_idx,
            is_rowid_alias,
        }),
        field: ast::Name::from_bytes(field_name.as_bytes()),
        resolved,
    }
}

/// Try to resolve `col_name.mid_name.leaf_name` as 2-level deep field access
/// (e.g. `data.telegram.chat_id` where `data` is a UNION column, `telegram` is
/// a variant with struct type, and `chat_id` is a struct field).
///
/// Returns the nested FieldAccess expr plus table_id/col_idx for `mark_column_used`.
fn try_resolve_nested_field_access<'a>(
    referenced_tables: &TableReferences,
    col_name: &str,
    mid_name: &str,
    leaf_name: &str,
    resolver: &'a Resolver<'a>,
) -> crate::Result<Option<(Expr, TableInternalId, usize)>> {
    let m = find_custom_type_column(referenced_tables, col_name, resolver)?;
    let Some(m) = m else {
        return Ok(None);
    };
    let td = m.type_def;

    // Resolve the inner type name reached via mid_name:
    // Case A: UNION column — mid_name is a variant tag
    // Case B: STRUCT column — mid_name is a struct field
    let inner_type_name = td
        .find_union_variant(mid_name)
        .map(|(_, v)| v.type_name.as_str())
        .or_else(|| {
            td.find_struct_field(mid_name)
                .map(|(_, f)| f.type_name.as_str())
        });

    let has_leaf = inner_type_name
        .and_then(|tn| resolver.schema().get_type_def_unchecked(tn))
        .is_some_and(|itd| itd.find_struct_field(leaf_name).is_some());

    if !has_leaf {
        return Ok(None);
    }

    let nested_expr = Expr::FieldAccess {
        base: Box::new(Expr::FieldAccess {
            base: Box::new(Expr::Column {
                database: None,
                table: m.table_id,
                column: m.col_idx,
                is_rowid_alias: m.is_rowid_alias,
            }),
            field: ast::Name::from_bytes(mid_name.as_bytes()),
            resolved: None,
        }),
        field: ast::Name::from_bytes(leaf_name.as_bytes()),
        resolved: None,
    };

    Ok(Some((nested_expr, m.table_id, m.col_idx)))
}

/// Resolve a field/variant name against a TypeDef to produce a FieldAccessResolution.
fn resolve_field_access(
    td: &crate::schema::TypeDef,
    field_name: &str,
) -> Option<ast::FieldAccessResolution> {
    if let Some((idx, _)) = td.find_struct_field(field_name) {
        Some(ast::FieldAccessResolution::StructField { field_index: idx })
    } else if let Some((tag_idx, _)) = td.find_union_variant(field_name) {
        Some(ast::FieldAccessResolution::UnionVariant { tag_index: tag_idx })
    } else {
        None
    }
}

/// Recursively resolve the output TypeDef of an expression.
///
/// For `Expr::Column`, returns the column's declared custom type.
/// For `Expr::FieldAccess`, recurses into the base to find the parent type,
/// then looks up what type the accessed field/variant produces.
/// Returns `None` for expressions that don't produce a known custom type.
fn resolve_expr_output_type<'a>(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &'a Resolver<'a>,
) -> crate::Result<&'a crate::schema::TypeDef> {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            let Some(referenced_tables) = referenced_tables else {
                crate::bail_parse_error!("cannot resolve type: no table context");
            };
            let Some((_is_outer, tbl)) = referenced_tables.find_table_by_internal_id(*table) else {
                crate::bail_parse_error!("cannot resolve type: table not found");
            };
            let col = &tbl.columns()[*column];
            let Some(td) = resolver.schema().get_type_def_unchecked(&col.ty_str) else {
                crate::bail_parse_error!(
                    "column '{}' has type '{}' which is not a known struct or union type",
                    col.name.as_deref().unwrap_or("?"),
                    col.ty_str
                );
            };
            Ok(td)
        }
        ast::Expr::FieldAccess { base, field, .. } => {
            let parent_td = resolve_expr_output_type(base, referenced_tables, resolver)?;
            let field_name = normalize_ident(field.as_str());
            // Find what type this field/variant produces
            let inner_type_name =
                if let Some((_, variant)) = parent_td.find_union_variant(&field_name) {
                    &variant.type_name
                } else if let Some((_, f)) = parent_td.find_struct_field(&field_name) {
                    &f.type_name
                } else {
                    let kind = if parent_td.is_union() {
                        "variant"
                    } else {
                        "field"
                    };
                    crate::bail_parse_error!("no such {} '{}' in type", kind, field_name);
                };
            let Some(td) = resolver.schema().get_type_def_unchecked(inner_type_name) else {
                crate::bail_parse_error!(
                    "'{}' resolves to type '{}' which is not a known type",
                    field_name,
                    inner_type_name
                );
            };
            Ok(td)
        }
        _ => {
            crate::bail_parse_error!("expression does not produce a known custom type");
        }
    }
}

/// Validates custom-type function calls (arrays, structs, unions) at bind time.
///
/// Compile-time checks belong in the earliest phase that has enough context.
/// Binding has the resolver (for the custom-types gate) and the raw AST args
/// (for arity and literal checks). Catching errors here avoids wasting
/// optimizer and translation cycles on invalid queries, and keeps the
/// translate_expr match arms focused purely on register allocation and codegen.
fn validate_custom_type_function_call(
    name: &str,
    args: &[Box<ast::Expr>],
    resolver: &Resolver<'_>,
) -> Result<()> {
    let normalized = crate::util::normalize_ident(name);
    match normalized.as_str() {
        // Arrays
        "array" | "array_element" | "array_set_element" | "array_length" | "array_append"
        | "array_prepend" | "array_cat" | "array_remove" | "array_contains" | "array_position"
        | "array_slice" | "string_to_array" | "array_to_string" | "array_overlap"
        | "array_contains_all" => {
            resolver.require_custom_types("Array features")?;
        }
        // Structs
        "struct_pack" => {
            resolver.require_custom_types("Struct features")?;
        }
        "struct_extract" => {
            resolver.require_custom_types("Struct features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("struct_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "struct_extract() second argument must be a string literal"
                );
            }
        }
        // Unions
        "union_value" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_value() requires exactly 2 arguments");
            }
            if !matches!(&*args[0], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!("union_value() first argument must be a string literal");
            }
        }
        "union_tag" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 1 {
                crate::bail_parse_error!("union_tag() requires exactly 1 argument");
            }
        }
        "union_extract" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "union_extract() second argument must be a string literal"
                );
            }
        }
        _ => {}
    }
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
                ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                    unreachable!(
                        "Array and Subscript are desugared into function calls by the parser"
                    )
                }
                ast::Expr::Id(_)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::Literal(_)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_)
                | ast::Expr::Default => {
                    // No nested expressions
                }
                ast::Expr::FieldAccess { base, .. } => {
                    walk_expr_mut(base, func)?;
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExprAffinityInfo {
    affinity: Affinity,
    has_affinity: bool,
}

impl ExprAffinityInfo {
    const fn with_affinity(affinity: Affinity) -> Self {
        Self {
            affinity,
            has_affinity: true,
        }
    }

    const fn no_affinity() -> Self {
        Self {
            affinity: Affinity::Blob,
            has_affinity: false,
        }
    }
}

pub(crate) fn get_expr_affinity_info(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> ExprAffinityInfo {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            if let Some(tables) = referenced_tables {
                if let Some((_, table_ref)) = tables.find_table_by_internal_id(*table) {
                    if let Some(col) = table_ref.get_column_at(*column) {
                        if let Some(btree) = table_ref.btree() {
                            return ExprAffinityInfo::with_affinity(
                                col.affinity_with_strict(btree.is_strict),
                            );
                        }
                        return ExprAffinityInfo::with_affinity(col.affinity());
                    }
                }
            }
            // SELF_TABLE fallback: during DML expression index evaluation,
            // referenced_tables is None but column affinities are available
            // via the resolver's self_table_column_affinities.
            if table.is_self_table() {
                if let Some(resolver) = resolver {
                    if let Some(aff) = resolver.self_table_column_affinities.get(*column) {
                        return ExprAffinityInfo::with_affinity(*aff);
                    }
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        ast::Expr::RowId { .. } => ExprAffinityInfo::with_affinity(Affinity::Integer),
        ast::Expr::Cast { type_name, .. } => {
            if let Some(type_name) = type_name {
                ExprAffinityInfo::with_affinity(Affinity::affinity(&type_name.name))
            } else {
                ExprAffinityInfo::no_affinity()
            }
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            get_expr_affinity_info(exprs.first().unwrap(), referenced_tables, resolver)
        }
        ast::Expr::Collate(expr, _) => get_expr_affinity_info(expr, referenced_tables, resolver),
        // Literals have NO affinity in SQLite.
        ast::Expr::Literal(_) => ExprAffinityInfo::no_affinity(),
        ast::Expr::Register(reg) => {
            // During UPDATE expression index evaluation, column references are
            // rewritten to Expr::Register. Look up the original column affinity
            // from the resolver's register_affinities map.
            if let Some(resolver) = resolver {
                if let Some(aff) = resolver.register_affinities.get(reg) {
                    return ExprAffinityInfo::with_affinity(*aff);
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        ast::Expr::SubqueryResult {
            subquery_id,
            query_type: ast::SubqueryType::RowValue { num_regs, .. },
            ..
        } if *num_regs == 1 => {
            if let Some(resolver) = resolver {
                if let Some(aff) = resolver.subquery_affinities.borrow().get(subquery_id) {
                    return *aff;
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        _ => ExprAffinityInfo::no_affinity(),
    }
}

pub fn get_expr_affinity(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    get_expr_affinity_info(expr, referenced_tables, resolver).affinity
}

pub fn comparison_affinity(
    lhs_expr: &ast::Expr,
    rhs_expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    compare_affinity(
        rhs_expr,
        get_expr_affinity_info(lhs_expr, referenced_tables, resolver),
        referenced_tables,
        resolver,
    )
}

fn comparison_affinity_from_info(lhs: ExprAffinityInfo, rhs: ExprAffinityInfo) -> Affinity {
    if lhs.has_affinity && rhs.has_affinity {
        // Both sides have affinity - use numeric if either is numeric
        if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    } else if lhs.has_affinity {
        lhs.affinity
    } else if rhs.has_affinity {
        rhs.affinity
    } else {
        Affinity::Blob
    }
}

pub(crate) fn compare_affinity(
    expr: &ast::Expr,
    other: ExprAffinityInfo,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    comparison_affinity_from_info(
        other,
        get_expr_affinity_info(expr, referenced_tables, resolver),
    )
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
                Value::Numeric(Numeric::Integer(int_value)) => {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                }
                Value::Numeric(Numeric::Float(real_value)) => {
                    program.emit_insn(Insn::Real {
                        value: real_value.into(),
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
        ast::Literal::True => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::False => {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentDate => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_date::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTime => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_time::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTimestamp => {
            program.emit_insn(
                Insn::String8 {
                    value: datetime::exec_datetime_full::<
                        &[_; 0],
                        std::slice::Iter<'_, Value>,
                        &Value,
                    >(&[])
                    .to_string(),
                    dest: target_register,
                },
            );
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
    table_references: &mut TableReferences,
    resolver: &Resolver<'_>,
) -> Result<Vec<ResultSetColumn>> {
    let mut result_columns = Vec::with_capacity(returning.len());

    let alias_to_string = |alias: &ast::As| alias.name().as_str().to_string();

    for rc in returning.iter_mut() {
        match rc {
            ast::ResultColumn::Expr(expr, alias) => {
                bind_and_rewrite_expr(
                    expr,
                    Some(table_references),
                    None,
                    resolver,
                    BindingBehavior::TryResultColumnsFirst,
                )?;

                let vec_size = expr_vector_size(expr)?;
                if vec_size != 1 {
                    crate::bail_parse_error!(
                        "sub-select returns {} columns - expected 1",
                        vec_size
                    );
                }

                result_columns.push(ResultSetColumn {
                    expr: expr.as_ref().clone(),
                    alias: alias.as_ref().map(alias_to_string),
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }
            ast::ResultColumn::Star => {
                let table = table_references
                    .joined_tables()
                    .first()
                    .expect("RETURNING clause must reference at least one table");
                let internal_id = table.internal_id;

                // Handle RETURNING * by expanding to all table columns
                // Use the shared internal_id for all columns
                for (column_index, column) in table.columns().iter().enumerate() {
                    let column_expr = Expr::Column {
                        database: None,
                        table: internal_id,
                        column: column_index,
                        is_rowid_alias: column.is_rowid_alias(),
                    };

                    result_columns.push(ResultSetColumn {
                        expr: column_expr,
                        alias: column.name.clone(),
                        implicit_column_name: None,
                        contains_aggregates: false,
                    });
                }
            }
            ast::ResultColumn::TableStar(_) => {
                crate::bail_parse_error!("RETURNING may not use \"TABLE.*\" wildcards");
            }
        }
    }

    Ok(result_columns)
}

/// Context for buffering RETURNING results into an ephemeral table
/// instead of yielding them immediately via ResultRow.
/// When used, the DML loop buffers each result row into the ephemeral table,
/// and a scan-back loop after the DML loop yields them to the caller.
pub struct ReturningBufferCtx {
    /// Cursor ID of the ephemeral table to buffer results into
    pub cursor_id: usize,
    /// Number of RETURNING columns (used for scan-back)
    pub num_columns: usize,
}

/// Emit the scan-back loop that reads all buffered RETURNING rows from the
/// ephemeral table and yields them via ResultRow. Called after all DML is complete.
pub(crate) fn emit_returning_scan_back(program: &mut ProgramBuilder, buf: &ReturningBufferCtx) {
    let end_label = program.allocate_label();
    let scan_start = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: buf.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(scan_start);

    let result_start_reg = program.alloc_registers(buf.num_columns);
    for i in 0..buf.num_columns {
        program.emit_insn(Insn::Column {
            cursor_id: buf.cursor_id,
            column: i,
            dest: result_start_reg + i,
            default: None,
        });
    }
    program.emit_insn(Insn::ResultRow {
        start_reg: result_start_reg,
        count: buf.num_columns,
    });
    program.emit_insn(Insn::Next {
        cursor_id: buf.cursor_id,
        pc_if_next: scan_start,
    });
    program.preassign_label_to_next_insn(end_label);
}

/// Emit bytecode to evaluate RETURNING expressions and produce result rows.
/// RETURNING result expressions are otherwise evaluated as normal, but the columns of the target table
/// are added to [Resolver::expr_to_reg_cache], meaning a reference to e.g tbl.col will effectively
/// refer to a register where the OLD/NEW value of tbl.col is stored after an INSERT/UPDATE/DELETE.
///
/// When `returning_buffer` is `Some`, the results are buffered into an ephemeral table
/// instead of being yielded immediately. A subsequent call to `emit_returning_scan_back`
/// will drain the buffer and yield the rows to the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_returning_results<'a>(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    returning_buffer: Option<&ReturningBufferCtx>,
    layout: &ColumnLayout,
) -> Result<()> {
    if result_columns.is_empty() {
        return Ok(());
    }

    let cache_state = seed_returning_row_image_in_cache(
        program,
        table_references,
        reg_columns_start,
        rowid_reg,
        resolver,
        layout,
    )?;

    let result = (|| {
        let result_start_reg = program.alloc_registers(result_columns.len());

        for (i, result_column) in result_columns.iter().enumerate() {
            let reg = result_start_reg + i;
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
                &result_column.expr,
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }

        // Decode array columns in RETURNING results (record blob -> JSON text).
        super::result_row::emit_array_decode_for_results(
            program,
            result_columns,
            table_references,
            result_start_reg,
            resolver,
        )?;

        if let Some(buf) = returning_buffer {
            // Buffer into ephemeral table instead of yielding directly.
            // All DML completes before any RETURNING rows are yielded to the caller.
            let record_reg = program.alloc_register();
            let eph_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: crate::vdbe::insn::to_u16(result_start_reg),
                count: crate::vdbe::insn::to_u16(result_columns.len()),
                dest_reg: crate::vdbe::insn::to_u16(record_reg),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::NewRowid {
                cursor: buf.cursor_id,
                rowid_reg: eph_rowid_reg,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: buf.cursor_id,
                key_reg: eph_rowid_reg,
                record_reg,
                flag: InsertFlags::new().is_ephemeral_table_insert(),
                table_name: String::new(),
            });
        } else {
            program.emit_insn(Insn::ResultRow {
                start_reg: result_start_reg,
                count: result_columns.len(),
            });
        }

        Ok(())
    })();

    restore_returning_row_image_in_cache(resolver, cache_state);
    result
}

pub(crate) struct ReturningRowImageCacheState {
    cache_len: usize,
    cache_enabled: bool,
}

pub(crate) fn seed_returning_row_image_in_cache<'a>(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    layout: &ColumnLayout,
) -> Result<ReturningRowImageCacheState> {
    turso_assert!(
        table_references.joined_tables().len() == 1,
        "RETURNING is only used with INSERT, UPDATE, or DELETE statements, which target a single table"
    );
    let table = table_references.joined_tables().first().unwrap();

    let cache_len = resolver.expr_to_reg_cache.len();
    let cache_enabled = resolver.expr_to_reg_cache_enabled;
    resolver.enable_expr_to_reg_cache();
    resolver.cache_expr_reg(
        std::borrow::Cow::Owned(Expr::RowId {
            database: None,
            table: table.internal_id,
        }),
        rowid_reg,
        false,
        None,
    );
    for (i, column) in table.columns().iter().enumerate() {
        let raw_reg = if column.is_rowid_alias() {
            rowid_reg
        } else {
            reg_columns_start + layout.to_reg_offset(i)
        };
        // The write registers hold stored (encoded) values. Produce the
        // user-facing value in a fresh register so RETURNING shows decoded
        // results — this is a no-op for regular columns.
        let decoded_reg = program.alloc_register();
        emit_user_facing_column_value(
            program,
            raw_reg,
            decoded_reg,
            column,
            table.table.is_strict(),
            resolver,
        )?;
        let expr = Expr::Column {
            database: None,
            table: table.internal_id,
            column: i,
            is_rowid_alias: column.is_rowid_alias(),
        };
        resolver.cache_scalar_expr_reg(
            std::borrow::Cow::Owned(expr),
            decoded_reg,
            false,
            table_references,
        )?;
    }

    Ok(ReturningRowImageCacheState {
        cache_len,
        cache_enabled,
    })
}

pub(crate) fn restore_returning_row_image_in_cache(
    resolver: &mut Resolver<'_>,
    state: ReturningRowImageCacheState,
) {
    resolver.expr_to_reg_cache.truncate(state.cache_len);
    resolver.expr_to_reg_cache_enabled = state.cache_enabled;
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
                crate::bail_parse_error!(
                    "all arguments to BETWEEN must return the same number of values. Got: ({evs_left}) BETWEEN ({evs_start}) AND ({evs_end})"
                );
            }
            1
        }
        Expr::Binary(expr, operator, expr1) => {
            let evs_left = expr_vector_size(expr)?;
            let evs_right = expr_vector_size(expr1)?;
            if evs_left != evs_right {
                crate::bail_parse_error!(
                    "all arguments to binary operator {operator} must return the same number of values. Got: ({evs_left}) {operator} ({evs_right})"
                );
            }
            if evs_left > 1 && !supports_row_value_binary_comparison(operator) {
                crate::bail_parse_error!("row value misused");
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
        Expr::Exists(_) => 1, // EXISTS returns a single boolean value (0 or 1)
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
                    crate::bail_parse_error!(
                        "all arguments to IN list must return the same number of values, got: ({evs_lhs}) IN ({evs_rhs})"
                    );
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
        Expr::Like { lhs, rhs, op, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            // MATCH allows multi-column LHS: (col1, col2) MATCH 'query'
            if evs_lhs != 1 && *op != ast::LikeOperator::Match {
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
        Expr::FieldAccess { .. } => 1,
        Expr::Raise(..) => 1,
        Expr::Subquery(_) => {
            crate::bail_parse_error!("Scalar subquery is not supported in this context")
        }
        Expr::Unary(unary_operator, expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to unary operator {unary_operator} must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::Variable(_) => 1,
        Expr::SubqueryResult { query_type, .. } => match query_type {
            SubqueryType::Exists { .. } => 1,
            SubqueryType::In { .. } => 1,
            SubqueryType::RowValue { num_regs, .. } => *num_regs,
        },
        Expr::Default => 1,
        Expr::Array { .. } | Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    })
}

/// Map an AST operator to the string representation used in custom type operator definitions.
fn operator_to_str(op: &ast::Operator) -> Option<&'static str> {
    match op {
        ast::Operator::Add => Some("+"),
        ast::Operator::Subtract => Some("-"),
        ast::Operator::Multiply => Some("*"),
        ast::Operator::Divide => Some("/"),
        ast::Operator::Modulus => Some("%"),
        ast::Operator::Less => Some("<"),
        ast::Operator::LessEquals => Some("<="),
        ast::Operator::Greater => Some(">"),
        ast::Operator::GreaterEquals => Some(">="),
        ast::Operator::Equals => Some("="),
        ast::Operator::NotEquals => Some("!="),
        _ => None,
    }
}

/// Emit bytecode for a resolved custom type operator call.
/// Handles argument swapping, literal encoding, and result negation.
fn emit_custom_type_operator(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    e1: &ast::Expr,
    e2: &ast::Expr,
    resolved: &ResolvedOperator,
    resolver: &Resolver,
) -> Result<usize> {
    let func = resolver
        .resolve_function(&resolved.func_name, 2)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("function not found: {}", resolved.func_name))
        })?;
    let (first, second) = if resolved.swap_args {
        (e2, e1)
    } else {
        (e1, e2)
    };

    // When encoding a literal operand, we must use separate registers for the
    // function call arguments. translate_expr may place literals in preamble
    // registers (constant optimization), and encoding in-place would clobber
    // that register — breaking subsequent loop iterations.
    let func_start = if let Some(ref encode_info) = resolved.encode_info {
        if let Some(encode_expr) = encode_info.type_def.encode() {
            // Translate operands into temporary registers first.
            let tmp1 = program.alloc_register();
            let tmp2 = program.alloc_register();
            translate_expr(program, referenced_tables, first, tmp1, resolver)?;
            translate_expr(program, referenced_tables, second, tmp2, resolver)?;

            // Determine which tmp holds the literal and which holds the column.
            let (lit_tmp, col_tmp) = match encode_info.which {
                EncodeArg::First if resolved.swap_args => (tmp2, tmp1),
                EncodeArg::First => (tmp1, tmp2),
                EncodeArg::Second if resolved.swap_args => (tmp1, tmp2),
                EncodeArg::Second => (tmp2, tmp1),
            };

            // Allocate fresh contiguous registers for the function call.
            let func_args = program.alloc_registers(2);
            // The literal goes in the same position it occupied in arg layout.
            let (lit_dst, col_dst) = match encode_info.which {
                EncodeArg::First if resolved.swap_args => (func_args + 1, func_args),
                EncodeArg::First => (func_args, func_args + 1),
                EncodeArg::Second if resolved.swap_args => (func_args, func_args + 1),
                EncodeArg::Second => (func_args + 1, func_args),
            };

            // Copy column value as-is.
            program.emit_insn(Insn::Copy {
                src_reg: col_tmp,
                dst_reg: col_dst,
                extra_amount: 0,
            });
            // Encode the literal into the fresh function arg slot.
            emit_type_expr(
                program,
                encode_expr,
                lit_tmp,
                lit_dst,
                &encode_info.column,
                &encode_info.type_def,
                resolver,
            )?;
            func_args
        } else {
            // Type has no encode expression; translate directly into arg slots.
            let arg_reg = program.alloc_registers(2);
            translate_expr(program, referenced_tables, first, arg_reg, resolver)?;
            translate_expr(program, referenced_tables, second, arg_reg + 1, resolver)?;
            arg_reg
        }
    } else {
        // No encoding needed; translate directly into arg slots.
        let arg_reg = program.alloc_registers(2);
        translate_expr(program, referenced_tables, first, arg_reg, resolver)?;
        translate_expr(program, referenced_tables, second, arg_reg + 1, resolver)?;
        arg_reg
    };

    let result_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: func_start,
        dest: result_reg,
        func: FuncCtx { func, arg_count: 2 },
    });
    if resolved.negate {
        program.emit_insn(Insn::Not {
            reg: result_reg,
            dest: result_reg,
        });
    }
    Ok(result_reg)
}

/// Info about a column with a custom type, extracted from an expression.
struct ExprCustomTypeInfo {
    type_name: String,
    column: Column,
    type_def: Arc<TypeDef>,
}

/// If the expression is a column reference to a custom type, return the type info.
fn expr_custom_type_info(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<ExprCustomTypeInfo> {
    if let ast::Expr::Column {
        table: table_ref_id,
        column,
        ..
    } = expr
    {
        let tables = referenced_tables?;
        let (_, table) = tables.find_table_by_internal_id(*table_ref_id)?;
        let col = table.get_column_at(*column)?;
        let type_name = &col.ty_str;
        let type_def = resolver
            .schema()
            .get_type_def(type_name, table.is_strict())?;
        return Some(ExprCustomTypeInfo {
            type_name: type_name.to_lowercase(),
            column: col.clone(),
            type_def: Arc::clone(type_def),
        });
    }
    None
}

/// Get the effective type name of a literal expression.
fn literal_type_name(expr: &ast::Expr) -> Option<&'static str> {
    match expr {
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(s) => {
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    Some("real")
                } else {
                    Some("integer")
                }
            }
            ast::Literal::String(_) => Some("text"),
            ast::Literal::Blob(_) => Some("blob"),
            ast::Literal::True | ast::Literal::False => Some("integer"),
            _ => None,
        },
        _ => None,
    }
}

/// Check if a literal type is compatible with a custom type's value input type.
/// "any" matches everything; otherwise exact match (case-insensitive).
fn literal_compatible_with_value_type(literal_type: &str, value_input_type: &str) -> bool {
    value_input_type.eq_ignore_ascii_case("any")
        || literal_type.eq_ignore_ascii_case(value_input_type)
}

/// Which operand of a binary expression needs encoding before the operator call.
enum EncodeArg {
    /// Encode the first argument (e1 is a literal, e2 is the custom type column)
    First,
    /// Encode the second argument (e1 is the custom type column, e2 is a literal)
    Second,
}

/// Info needed to encode a literal argument for an operator call.
struct OperatorEncodeInfo {
    column: Column,
    type_def: Arc<TypeDef>,
    which: EncodeArg,
}

/// Result of resolving a custom type operator. May be a direct match or derived
/// from `<` and `=` operators (e.g. `>` is derived as swap_args + `<`).
struct ResolvedOperator {
    func_name: String,
    swap_args: bool,
    negate: bool,
    /// If a literal operand needs encoding before the operator call.
    encode_info: Option<OperatorEncodeInfo>,
}

/// Find a custom type operator function for a binary expression.
///
/// Operators fire when:
/// 1. Both operands are columns of the same custom type, OR
/// 2. One operand is a custom type column and the other is a literal whose type
///    is compatible with the custom type's `value` input type.
///
/// When case 2 applies, the literal is encoded before being passed to the operator
/// function so both arguments are in the same (encoded) representation.
fn find_custom_type_operator(
    e1: &ast::Expr,
    e2: &ast::Expr,
    op: &ast::Operator,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<ResolvedOperator> {
    let op_str = operator_to_str(op)?;
    let lhs_info = expr_custom_type_info(e1, referenced_tables, resolver);
    let rhs_info = expr_custom_type_info(e2, referenced_tables, resolver);

    // Try to find a direct or derived operator match on a type definition.
    let find_in_type_def = |type_def: &TypeDef| -> Option<(String, bool, bool)> {
        // Direct match: just check op symbol (no right_type constraint)
        for op_def in type_def.operators() {
            if op_def.op == op_str {
                // Naked operator (func_name = None): fall through to standard comparison
                let func_name = op_def.func_name.as_ref()?;
                return Some((func_name.clone(), false, false));
            }
        }

        // Derive missing operators from < and =
        let find_op = |sym: &str| -> Option<String> {
            type_def
                .operators()
                .iter()
                .find(|o| o.op == sym)
                .and_then(|o| o.func_name.clone())
        };

        match *op {
            // a > b  →  lt(b, a)
            ast::Operator::Greater => find_op("<").map(|f| (f, true, false)),
            // a >= b  →  NOT lt(a, b)
            ast::Operator::GreaterEquals => find_op("<").map(|f| (f, false, true)),
            // a <= b  →  NOT lt(b, a)
            ast::Operator::LessEquals => find_op("<").map(|f| (f, true, true)),
            // a != b  →  NOT eq(a, b)
            ast::Operator::NotEquals => find_op("=").map(|f| (f, false, true)),
            _ => None,
        }
    };

    // Case 1: Both operands are custom type columns of the SAME type.
    if let (Some(ref lhs), Some(ref rhs)) = (&lhs_info, &rhs_info) {
        if lhs.type_name == rhs.type_name {
            if let Some((func_name, swap_args, negate)) = find_in_type_def(&lhs.type_def) {
                return Some(ResolvedOperator {
                    func_name,
                    swap_args,
                    negate,
                    encode_info: None,
                });
            }
        }
        // Different custom types: fall through to standard operator.
        return None;
    }

    // Case 2: LHS is custom type, RHS is a compatible literal.
    if let Some(ref lhs) = lhs_info {
        if let Some(lit_type) = literal_type_name(e2) {
            if literal_compatible_with_value_type(lit_type, lhs.type_def.value_input_type()) {
                if let Some((func_name, swap_args, negate)) = find_in_type_def(&lhs.type_def) {
                    return Some(ResolvedOperator {
                        func_name,
                        swap_args,
                        negate,
                        encode_info: Some(OperatorEncodeInfo {
                            column: lhs.column.clone(),
                            type_def: lhs.type_def.clone(),
                            which: EncodeArg::Second,
                        }),
                    });
                }
            }
        }
    }

    // Case 3: RHS is custom type, LHS is a compatible literal (reversed).
    if let Some(ref rhs) = rhs_info {
        if let Some(lit_type) = literal_type_name(e1) {
            if literal_compatible_with_value_type(lit_type, rhs.type_def.value_input_type()) {
                if let Some((func_name, swap_args, negate)) = find_in_type_def(&rhs.type_def) {
                    return Some(ResolvedOperator {
                        func_name,
                        swap_args,
                        negate,
                        encode_info: Some(OperatorEncodeInfo {
                            column: rhs.column.clone(),
                            type_def: rhs.type_def.clone(),
                            which: EncodeArg::First,
                        }),
                    });
                }
            }
        }
    }

    None
}

/// Evaluate an expression-index expression in a DML context (INSERT/UPDATE/UPSERT).
///
/// Shared logic: decode custom-type column registers into temps (so the
/// expression sees user-facing values), build a `SelfTableContext::ForDML`,
/// and translate the expression.
///
/// The caller must:
/// 1. Clone the expression from `idx_col.expr`
/// 2. Build the initial `column_regs` mapping (before decode)
///
/// The expression is resolved via `resolve_gencol_expr_columns` and custom-type
/// columns are decoded in-place in `column_regs`.
pub(crate) fn emit_dml_expr_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut expr: ast::Expr,
    columns: &[Column],
    column_regs: &mut [usize],
    table: &Arc<BTreeTable>,
    dest_reg: usize,
) -> Result<()> {
    crate::schema::resolve_gencol_expr_columns(&mut expr, columns)?;

    let is_strict = table.is_strict;
    for (i, col) in columns.iter().enumerate() {
        if col.is_rowid_alias() {
            continue;
        }
        if let Some(type_def) = resolver.schema().get_type_def(&col.ty_str, is_strict) {
            if type_def.decode().is_some() {
                let src_reg = column_regs[i];
                let tmp = program.alloc_register();
                emit_user_facing_column_value(program, src_reg, tmp, col, is_strict, resolver)?;
                column_regs[i] = tmp;
            }
        }
    }

    let pairs = columns.iter().zip(column_regs.iter().copied());
    let ctx = SelfTableContext::ForDML {
        dml_ctx: DmlColumnContext::from_column_reg_mapping(pairs),
        table: Arc::clone(table),
    };
    program.with_self_table_context(Some(&ctx), |program, _| {
        translate_expr(program, None, &expr, dest_reg, resolver)?;
        Ok(())
    })?;
    Ok(())
}

/// Emit bytecode that transforms a stored column value into its user-facing
/// representation.
///
/// For regular columns this is a simple copy (or no-op when source == dest).
/// For custom type columns with a DECODE function the decode expression is
/// applied, converting the internal storage form back to the value the user
/// expects to see.
///
/// Every code path that surfaces a stored column value to the user — SELECT,
/// RETURNING, trigger OLD/NEW — should go through this function so decode
/// logic lives in one place.
pub(crate) fn emit_user_facing_column_value(
    program: &mut ProgramBuilder,
    source_reg: usize,
    dest_reg: usize,
    column: &Column,
    is_strict: bool,
    resolver: &Resolver,
) -> Result<()> {
    if source_reg != dest_reg {
        program.emit_insn(Insn::Copy {
            src_reg: source_reg,
            dst_reg: dest_reg,
            extra_amount: 0,
        });
    }
    // Array columns: pass through raw record blob. ArrayDecode is emitted
    // at display time (ResultRow) so that functions/subscripts see raw blobs.
    if column.is_array() {
        return Ok(());
    }
    if let Ok(Some(resolved)) = resolver.schema().resolve_type(&column.ty_str, is_strict) {
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: dest_reg,
            target_pc: skip_label,
        });

        // Apply decode in reverse order (parent/ancestor first, then child)
        for td in resolved.chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(
                    program,
                    decode_expr,
                    dest_reg,
                    dest_reg,
                    column,
                    td,
                    resolver,
                )?;
            }
        }

        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}

/// Emit domain constraint checks for CAST(expr AS domain).
/// Validates NOT NULL and CHECK constraints from the domain type chain.
fn emit_domain_cast_constraints(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    use crate::error::{SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL};

    let any_not_null = chain.iter().any(|td| td.not_null);

    if any_not_null {
        program.emit_insn(Insn::HaltIfNull {
            target_reg: reg,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: format!(
                "domain {} does not allow null values",
                chain.first().map(|td| td.name.as_str()).unwrap_or("?")
            ),
        });
    }

    for td in chain {
        for (i, dc) in td.domain_checks.iter().enumerate() {
            let constraint_name = dc
                .name
                .clone()
                .unwrap_or_else(|| format!("{}_{}", td.name, i));

            // Bind `value` → reg, translate check expr, verify truthy
            program
                .id_register_overrides
                .insert("value".to_string(), reg);

            let expr_result_reg = program.alloc_register();
            translate_expr(program, None, &dc.check, expr_result_reg, resolver)?;

            program.id_register_overrides.remove("value");

            let passed_label = program.allocate_label();

            // NULL result passes CHECK constraints (SQLite semantics)
            program.emit_insn(Insn::IsNull {
                reg: expr_result_reg,
                target_pc: passed_label,
            });

            program.emit_insn(Insn::If {
                reg: expr_result_reg,
                target_pc: passed_label,
                jump_if_null: false,
            });

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_CHECK,
                description: format!(
                    "value for domain {} violates check constraint \"{}\"",
                    td.name, constraint_name
                ),
                on_error: None,
                description_reg: None,
            });

            program.preassign_label_to_next_insn(passed_label);
        }
    }
    Ok(())
}

/// Emit bytecode for a custom type encode/decode expression.
/// Sets up `value` to reference `value_reg`, and type parameter overrides
/// from `column.ty_params` matched against `type_def.params`.
/// The expression result is written to `dest_reg`.
pub(crate) fn emit_type_expr(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    value_reg: usize,
    dest_reg: usize,
    column: &Column,
    type_def: &TypeDef,
    resolver: &Resolver,
) -> Result<usize> {
    // Set up value override
    program
        .id_register_overrides
        .insert("value".to_string(), value_reg);

    // Set up type parameter overrides. Capture the result so we can
    // clean up overrides even if param translation fails.
    let param_result: Result<()> = (|| {
        // Skip `value` param (already handled above); match remaining params
        // against the user-provided ty_params by position.
        let user_params: Vec<_> = type_def.user_params().collect();
        for (i, param) in user_params.iter().enumerate() {
            if let Some(param_expr) = column.ty_params.get(i) {
                let reg = program.alloc_register();
                translate_expr(program, None, param_expr, reg, resolver)?;
                program
                    .id_register_overrides
                    .insert(param.name.clone(), reg);
            }
        }
        Ok(())
    })();

    // Translate body expression only if param setup succeeded
    let result = param_result.and_then(|()| {
        // Translate the expression, disabling constant optimization since
        // the `value` placeholder refers to a register that changes per row.
        translate_expr_no_constant_opt(
            program,
            None,
            expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )
    });

    // Always clean up overrides, even on error
    program.id_register_overrides.clear();

    result
}

/// Decode custom type columns for AFTER trigger NEW registers.
///
/// For each column with a custom type decode expression, copies the encoded register
/// to a new register and emits the decode expression. NULL values are skipped.
/// Returns a Vec of registers: one per column (decoded or original) plus the rowid at the end.
pub(crate) fn emit_trigger_decode_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    source_regs: &dyn Fn(usize) -> usize,
    rowid_reg: usize,
    is_strict: bool,
) -> Result<Vec<usize>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| -> Result<usize> {
            let type_def = resolver.schema().get_type_def(&col.ty_str, is_strict);
            if let Some(type_def) = type_def {
                if let Some(decode_expr) = type_def.decode() {
                    let src = source_regs(i);
                    let decoded_reg = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: src,
                        dst_reg: decoded_reg,
                        extra_amount: 0,
                    });
                    let skip_label = program.allocate_label();
                    program.emit_insn(Insn::IsNull {
                        reg: decoded_reg,
                        target_pc: skip_label,
                    });
                    emit_type_expr(
                        program,
                        decode_expr,
                        decoded_reg,
                        decoded_reg,
                        col,
                        type_def,
                        resolver,
                    )?;
                    program.preassign_label_to_next_insn(skip_label);
                    return Ok(decoded_reg);
                }
            }
            Ok(source_regs(i))
        })
        .chain(std::iter::once(Ok(rowid_reg)))
        .collect::<Result<Vec<usize>>>()
}

/// Maximum number of array elements supported in the per-element transform loop.
/// Limited by the fixed register block allocated at compile time.
const MAX_ARRAY_LOOP_ELEMENTS: usize = 1024;

/// Emit a per-element transform loop on an array blob in `reg`.
/// Extracts each element, applies `transform_expr` via emit_type_expr,
/// stores results into contiguous registers, then rebuilds the blob with
/// MakeArrayDynamic. O(N) instead of O(N²) ArraySetElement per iteration.
fn emit_array_element_loop(
    program: &mut ProgramBuilder,
    reg: usize,
    transform_expr: &ast::Expr,
    col: &Column,
    type_def: &TypeDef,
    resolver: &Resolver,
) -> Result<()> {
    let reg_len = program.alloc_register();
    let reg_idx = program.alloc_register();
    let reg_elem = program.alloc_register();
    // Reserve a contiguous block for transformed elements.
    // At runtime, we only use registers[elem_base..elem_base+len].
    let elem_base = program.alloc_registers(MAX_ARRAY_LOOP_ELEMENTS);

    program.emit_insn(Insn::ArrayLength { reg, dest: reg_len });

    // Guard: halt if the array exceeds the register block size.
    let max_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: MAX_ARRAY_LOOP_ELEMENTS as i64,
        dest: max_reg,
    });
    let ok_label = program.allocate_label();
    program.emit_insn(Insn::Le {
        lhs: reg_len,
        rhs: max_reg,
        target_pc: ok_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT,
        description: format!(
            "array exceeds maximum element count for custom type transform ({MAX_ARRAY_LOOP_ELEMENTS})"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(ok_label);
    // reg_idx is the 1-based array index for ArrayElement (PG convention)
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_idx,
    });
    // reg_offset is the 0-based offset for RegCopyOffset into the register block
    let reg_offset = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_offset,
    });

    let loop_start = program.offset();
    let loop_end_label = program.allocate_label();

    program.emit_insn(Insn::Gt {
        lhs: reg_idx,
        rhs: reg_len,
        target_pc: loop_end_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    // Extract element from record blob (1-based index)
    program.emit_insn(Insn::ArrayElement {
        array_reg: reg,
        index_reg: reg_idx,
        dest: reg_elem,
    });

    // Apply per-element transform expression
    emit_type_expr(
        program,
        transform_expr,
        reg_elem,
        reg_elem,
        col,
        type_def,
        resolver,
    )?;

    // Store transformed element into contiguous register block at 0-based offset
    program.emit_insn(Insn::RegCopyOffset {
        src: reg_elem,
        base: elem_base,
        offset_reg: reg_offset,
    });

    program.emit_insn(Insn::AddImm {
        register: reg_idx,
        value: 1,
    });
    program.emit_insn(Insn::AddImm {
        register: reg_offset,
        value: 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end_label);

    // Rebuild the array blob from the contiguous register block in one pass
    program.emit_insn(Insn::MakeArrayDynamic {
        start_reg: elem_base,
        count_reg: reg_len,
        dest: reg,
    });

    Ok(())
}

/// Emit bytecode to encode an array value: parse JSON text input, validate/coerce
/// elements, and serialize to a native record-format BLOB.
/// For custom element types with encode expressions, a per-element bytecode loop
/// normalizes input to blob, applies encode per element, then rebuilds the blob.
fn emit_array_encode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
    table_name: &str,
) -> Result<()> {
    if let Some(type_def) = resolver.schema().get_type_def_unchecked(&col.ty_str) {
        if let Some(encode_expr) = type_def.encode() {
            // Normalize input (text or blob) to blob with ANY affinity first
            program.emit_insn(Insn::ArrayEncode {
                reg,
                element_affinity: Affinity::Blob,
                element_type: "ANY".into(),
                table_name: table_name.into(),
                col_name: col.name.as_deref().unwrap_or("").into(),
            });

            emit_array_element_loop(program, reg, encode_expr, col, type_def, resolver)?;
        }
    }

    // ArrayEncode: parse JSON text → validate/coerce → serialize to record blob.
    // For multi-dimensional arrays (e.g. INTEGER[][]), the outer array's elements
    // are themselves arrays (blobs), so we use ANY/Blob for validation.
    // Only 1-dimensional arrays validate elements against the declared base type.
    let is_any = col.ty_str.eq_ignore_ascii_case("ANY");
    let is_multidim = col.array_dimensions() > 1;
    let col_name = col.name.as_deref().unwrap_or("");
    let element_affinity = if is_any || is_multidim {
        Affinity::Blob
    } else {
        Affinity::affinity(&col.ty_str)
    };
    let element_type = if is_any || is_multidim {
        "ANY".into()
    } else {
        col.ty_str.to_uppercase().into()
    };
    program.emit_insn(Insn::ArrayEncode {
        reg,
        element_affinity,
        element_type,
        table_name: table_name.into(),
        col_name: col_name.into(),
    });
    Ok(())
}

/// Emit bytecode to decode an array value: convert record-format BLOB to JSON text.
/// For base element types, this is a single ArrayDecode instruction.
/// For custom element types with decode expressions, a per-element loop
/// extracts elements via ArrayElement, applies decode, then rebuilds the blob.
pub(crate) fn emit_array_decode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(type_def) = resolver.schema().get_type_def_unchecked(&col.ty_str) {
        if let Some(decode_expr) = type_def.decode() {
            emit_array_element_loop(program, reg, decode_expr, col, type_def, resolver)?;
        }
    }

    // Convert record blob to JSON text for display
    program.emit_insn(Insn::ArrayDecode { reg });
    Ok(())
}

/// Emit encode expressions for columns with custom types in a contiguous register range.
/// Used by INSERT, UPDATE, and UPSERT paths to encode values before TypeCheck.
///
/// If `only_columns` is `Some`, only encode columns whose index is in the set.
/// This is needed for UPDATE/UPSERT where non-SET columns are already encoded
/// (read from disk), and re-encoding them would corrupt data.
pub(crate) fn emit_custom_type_encode_columns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    table_name: &str,
    layout: &ColumnLayout,
) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if let Some(filter) = only_columns {
            if !filter.get(i) {
                continue;
            }
        }

        let reg = layout.to_register(start_reg, i);

        // Handle array columns: encode input (text or blob) -> record blob for storage
        if col.is_array() {
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: skip_label,
            });
            emit_array_encode(program, reg, col, resolver, table_name)?;
            program.preassign_label_to_next_insn(skip_label);
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) else {
            continue;
        };

        // Check if any type in the chain has not_null — if so, don't skip NULLs
        let any_not_null = resolved.chain.iter().any(|td| td.not_null);

        let skip_label = if !any_not_null {
            // Skip NULL values: jump over encode if NULL
            let label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: label,
            });
            Some(label)
        } else {
            None
        };

        // Apply encode for each type in the chain (child first, then parent)
        for td in &resolved.chain {
            if let Some(encode_expr) = td.encode() {
                emit_type_expr(program, encode_expr, reg, reg, col, td, resolver)?;
            }
        }

        if let Some(label) = skip_label {
            program.preassign_label_to_next_insn(label);
        }
    }
    Ok(())
}

/// Emit decode expressions for columns with custom types in a contiguous register range.
/// Used by the UPSERT path to decode values that were read from disk (encoded) so that
/// WHERE/SET expressions in DO UPDATE see user-facing values.
///
/// If `only_columns` is `Some`, only decode columns whose index is in the set.
pub(crate) fn emit_custom_type_decode_columns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if let Some(filter) = only_columns {
            if !filter.get(i) {
                continue;
            }
        }

        let reg = layout.to_register(start_reg, i);

        // Handle array columns: decode record blob -> JSON text for display
        if col.is_array() {
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: skip_label,
            });
            emit_array_decode(program, reg, col, resolver)?;
            program.preassign_label_to_next_insn(skip_label);
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) else {
            continue;
        };

        // Skip NULL values: jump over decode if NULL
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg,
            target_pc: skip_label,
        });

        // Apply decode in reverse order (parent/ancestor first, then child)
        for td in resolved.chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(program, decode_expr, reg, reg, col, td, resolver)?;
            }
        }

        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}
