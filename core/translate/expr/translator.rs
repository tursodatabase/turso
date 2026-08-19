use super::*;

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
pub(super) enum BinaryEmitMode {
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

/// Maximum recursion depth of [translate_expr()].
///
/// The parser already bounds the height of any parsed expression tree by
/// [`turso_parser::MAX_EXPR_DEPTH`] (100); the translator can add a bounded
/// number of extra levels on top of that (generated column expressions,
/// custom type ENCODE/DECODE chains, synthetic rewrites), so this limit is
/// deliberately higher. It turns runaway recursion on programmatically
/// constructed expression trees into an error instead of a stack overflow.
///
/// Stack budget: every level of the translator recursion path must keep its
/// stack frame small, which is why each expression variant is translated in
/// its own `#[inline(never)]` helper (unoptimized builds give every
/// temporary in a function body a distinct stack slot, so one function
/// containing all the arms had a ~110 KiB frame that overflowed the stack at
/// depth ~16, see issue #6655). The worst measured per-level cost in a debug
/// build is ~12 KiB, so 150 levels stay under ~1.8 MiB and fit in a
/// default-sized (2 MiB) thread stack. If you add a helper to the recursion
/// path, keep its frame small.
const MAX_EXPR_TRANSLATION_DEPTH: usize = 150;

/// Translate an expression into bytecode.
#[turso_macros::trace_stack]
pub fn translate_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    if program.expr_translation_depth >= MAX_EXPR_TRANSLATION_DEPTH {
        crate::bail_parse_error!(
            "Expression tree is too large (maximum depth {MAX_EXPR_TRANSLATION_DEPTH})"
        );
    }
    program.expr_translation_depth += 1;
    let result =
        translate_expr_dispatch(program, referenced_tables, expr, target_register, resolver);
    program.expr_translation_depth -= 1;
    result
}

/// Dispatch expression translation to a per-variant helper.
///
/// IMPORTANT: expression translation recurses once per expression tree level
/// and unoptimized builds give every temporary in a function body a distinct
/// stack slot, so a single function containing every match arm ends up with a
/// huge (~100 KiB) stack frame that overflows the stack on moderately nested
/// expressions (issue #6655). Keeping each variant in its own
/// `#[inline(never)]` helper keeps the per-recursion-level stack footprint
/// small. Do not inline the helpers back into this match, and put new arms
/// that emit more than a couple of instructions in their own helper.
fn translate_expr_dispatch(
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

    if emit_cached_expr_copy(program, referenced_tables, expr, target_register, resolver)? {
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
        ast::Expr::SubqueryResult { .. } => translate_expr_subquery_result(
            program,
            referenced_tables,
            expr,
            target_register,
            resolver,
        ),
        ast::Expr::Between { .. } => {
            translate_expr_between(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Binary(_, _, _) => {
            return translate_expr_binary(
                program,
                referenced_tables,
                expr,
                target_register,
                resolver,
                constant_span,
            )
        }
        ast::Expr::Case { .. } => {
            translate_expr_case(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Cast { .. } => {
            return translate_expr_cast(
                program,
                referenced_tables,
                expr,
                target_register,
                resolver,
                constant_span,
            )
        }
        ast::Expr::Collate(_, _) => {
            translate_expr_collate(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::DoublyQualified(_, _, _) | ast::Expr::Exists(_) => {
            translate_expr_unsupported(expr)
        }
        ast::Expr::FunctionCall { .. } => translate_expr_function_call(
            program,
            referenced_tables,
            expr,
            target_register,
            resolver,
        ),
        ast::Expr::FunctionCallStar { .. } => translate_expr_function_call_star(
            program,
            referenced_tables,
            expr,
            target_register,
            resolver,
        ),
        ast::Expr::Id(_) => {
            return translate_expr_id(
                program,
                referenced_tables,
                expr,
                target_register,
                resolver,
                constant_span,
            )
        }
        ast::Expr::Column { table, .. } if table.is_self_table() => {
            translate_expr_self_table_column(
                program,
                referenced_tables,
                expr,
                target_register,
                resolver,
            )
        }
        ast::Expr::Column { .. } => {
            translate_expr_column(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::RowId { .. } => {
            translate_expr_rowid(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::InList { .. } => {
            translate_expr_in_list(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::InSelect { .. } | ast::Expr::InTable { .. } => translate_expr_unsupported(expr),
        ast::Expr::IsNull(_) => {
            translate_expr_is_null(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Like { .. } => {
            translate_expr_like(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Literal(lit) => emit_literal(program, lit, target_register),
        ast::Expr::Name(_) => translate_expr_unsupported(expr),
        ast::Expr::NotNull(_) => {
            translate_expr_not_null(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Parenthesized(_) => translate_expr_parenthesized(
            program,
            referenced_tables,
            expr,
            target_register,
            resolver,
        ),
        ast::Expr::Qualified(_, _) => translate_expr_unsupported(expr),
        ast::Expr::FieldAccess { .. } => {
            translate_expr_field_access(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Raise(_, _) => {
            translate_expr_raise(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Subquery(_) => translate_expr_unsupported(expr),
        ast::Expr::Unary(_, _) => {
            translate_expr_unary(program, referenced_tables, expr, target_register, resolver)
        }
        ast::Expr::Variable(_) => translate_expr_variable(program, expr, target_register),
        ast::Expr::Register(_) => translate_expr_register(program, expr, target_register, resolver),
        ast::Expr::Default | ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            translate_expr_unsupported(expr)
        }
    }?;

    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }

    Ok(target_register)
}

/// If `expr` has already been evaluated into a cached register, copy it into
/// `target_register` (applying custom type DECODE when needed) and return
/// true. Kept out of [translate_expr_dispatch] so its locals don't inflate
/// the per-recursion-level stack frame (#6655).
#[inline(never)]
fn emit_cached_expr_copy(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<bool> {
    let Some((reg, needs_decode, collation_ctx)) = resolver.resolve_cached_expr_reg(expr) else {
        return Ok(false);
    };

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
                if let Some((_, table)) = referenced_tables.find_table_by_internal_id(*table_ref_id)
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
    Ok(true)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_subquery_result(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::SubqueryResult {
        lhs,
        not_in,
        query_type,
        ..
    } = expr
    else {
        unreachable!("translate_expr_subquery_result called with wrong expression variant");
    };
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
                ast::Expr::Parenthesized(exprs) => exprs.iter().map(|e| e.as_ref()).collect(),
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
                fullscan: false,
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_binary(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    constant_span: Option<usize>,
) -> Result<usize> {
    let ast::Expr::Binary(e1, op, e2) = expr else {
        unreachable!("translate_expr_binary called with wrong expression variant");
    };
    // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE specially.
    // These use truth semantics (only non-zero numbers are truthy) rather than equality.
    if let Some((is_not, is_true_literal)) = match (op, truth_test_rhs(e2)) {
        (ast::Operator::Is, Some(is_true_literal)) => Some((false, is_true_literal)),
        (ast::Operator::IsNot, Some(is_true_literal)) => Some((true, is_true_literal)),
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
    if let Some(resolved) = find_custom_type_operator(e1, e2, op, referenced_tables, resolver) {
        let result_reg =
            emit_custom_type_operator(program, referenced_tables, e1, e2, &resolved, resolver)?;
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
    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_case(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Case {
        base,
        when_then_pairs,
        else_expr,
    } = expr
    else {
        unreachable!("translate_expr_case called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_cast(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    constant_span: Option<usize>,
) -> Result<usize> {
    let ast::Expr::Cast { expr, type_name } = expr else {
        unreachable!("translate_expr_cast called with wrong expression variant");
    };
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
                emit_domain_cast_constraints(program, &resolved.chain, target_register, resolver)?;
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
    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_function_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::FunctionCall {
        name,
        args,
        filter_over,
        ..
    } = expr
    else {
        unreachable!("translate_expr_function_call called with wrong expression variant");
    };
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
        Func::External(_) | Func::Dialect(_) => {
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
        Func::Json(_) => translate_expr_json_function(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        Func::Vector(_) => translate_expr_vector_function(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        Func::Scalar(_) => translate_expr_scalar_function(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        Func::Math(_) => translate_expr_math_function(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_function_call_star(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::FunctionCallStar { name, filter_over } = expr else {
        unreachable!("translate_expr_function_call_star called with wrong expression variant");
    };
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
                LimboError::ParseError(format!("{}(*) requires a FROM clause", name.as_str()))
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
                within_group: vec![],
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
                within_group: vec![],
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_id(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    constant_span: Option<usize>,
) -> Result<usize> {
    let ast::Expr::Id(id) = expr else {
        unreachable!("translate_expr_id called with wrong expression variant");
    };
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
    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_self_table_column(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Column {
        database: _,
        table: _,
        column,
        is_rowid_alias,
    } = expr
    else {
        unreachable!("translate_expr_self_table_column called with wrong expression variant");
    };
    // the table is a SELF_TABLE placeholder (used for generated columns), so we now have
    // to resolve it to the actual reference id using the SelfTableContext.
    resolver.with_existing_self_table_context(|self_table_context| {
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
                // This error means that a resolver.with_self_table_context() scope was missing
                // somewhere in the call stack.
                crate::bail_parse_error!(
                    "SELF_TABLE column reference outside of generated column context"
                );
            }
        }
    })
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_column(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Column {
        database: _,
        table: table_ref_id,
        column,
        is_rowid_alias,
    } = expr
    else {
        unreachable!("translate_expr_column called with wrong expression variant");
    };
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
                let table_cursor_id = if use_index_method.is_some() {
                    None
                } else if use_covering_index {
                    // If we have a covering index, we don't have an open table cursor so we
                    // read from the index cursor, but the requested column might
                    // legitimately not be in the index if it's a dependency of a generated
                    // column.
                    // Example: CREATE TABLE t(c0, c1 AS (c2 + 1), c2);
                    // CREATE INDEX i ON t(c1, c0); DELETE FROM t WHERE c0 < 'X';
                    program.resolve_cursor_id_safe(&CursorKey::table(*table_ref_id))
                } else {
                    Some(program.resolve_cursor_id(&CursorKey::table(*table_ref_id)))
                };
                let index_cursor_id = index.map(|index| {
                    program.resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()))
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
                // For non-outer-scope reads: an index can supply a
                // column's value only when the index actually
                // stores it. Presence in the index's column list
                // (`column_table_pos_to_index_pos`) is necessary
                // but not sufficient for VIRTUAL generated
                // columns — the index has a slot but the value
                // is only materialized when the index is
                // covering. For stored columns the slot implies
                // the value, so any in-index hit is fine.
                let read_from_index = if is_from_outer_query_scope {
                    is_btree_index
                } else if is_btree_index {
                    let column_is_in_index = index
                        .as_ref()
                        .is_some_and(|idx| idx.column_table_pos_to_index_pos(*column).is_some());
                    let column_is_virtual = matches!(
                        table.get_column_at(*column).map(|c| c.generated_type()),
                        Some(GeneratedType::Virtual { .. })
                    );
                    column_is_in_index && (!column_is_virtual || use_covering_index)
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
                        resolver.with_self_table_context(
                            program,
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

                        program.emit_column_affinity(target_register, table_column.affinity());
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
                                    if resolved.chain.iter().any(|td| td.encode().is_some()) {
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
                            let cursor_id = program
                                .resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()));
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
                program
                    .get_subquery_result_reg(*table_ref_id)
                    .expect("Outer query subquery result_columns_start_reg must be set in program")
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
        Table::RecursiveCteInput(_) => {
            let cursor_id = program.resolve_cursor_id(&CursorKey::table(*table_ref_id));
            program.emit_insn(Insn::Column {
                cursor_id,
                column: *column,
                dest: target_register,
                default: None,
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_rowid(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    _resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::RowId {
        database: _,
        table: table_ref_id,
    } = expr
    else {
        unreachable!("translate_expr_rowid called with wrong expression variant");
    };
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
        let index = index.expect("index cursor should be opened when use_covering_index=true");
        let cursor_id = program.resolve_cursor_id(&CursorKey::index(*table_ref_id, index.clone()));
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_in_list(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::InList { lhs, rhs, not } = expr else {
        unreachable!("translate_expr_in_list called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_is_null(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::IsNull(expr) = expr else {
        unreachable!("translate_expr_is_null called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_like(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Like { not, .. } = expr else {
        unreachable!("translate_expr_like called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_not_null(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::NotNull(expr) = expr else {
        unreachable!("translate_expr_not_null called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_parenthesized(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Parenthesized(exprs) = expr else {
        unreachable!("translate_expr_parenthesized called with wrong expression variant");
    };
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_field_access(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::FieldAccess {
        base,
        field,
        resolved,
    } = expr
    else {
        unreachable!("translate_expr_field_access called with wrong expression variant");
    };
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
        Ok(target_register)
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

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_raise(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Raise(resolve_type, msg_expr) = expr else {
        unreachable!("translate_expr_raise called with wrong expression variant");
    };
    let in_trigger = program.trigger.is_some();
    match resolve_type {
        ResolveType::Ignore => {
            if !in_trigger {
                crate::bail_parse_error!("RAISE() may only be used within a trigger-program");
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
                crate::bail_parse_error!("RAISE() may only be used within a trigger-program");
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
        } // If the custom type requires parameters but the CAST
          // doesn't provide them (e.g. CAST(x AS NUMERIC) vs
          // CAST(x AS numeric(10,2))), fall through to regular CAST.
    }
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_unary(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Unary(op, expr) = expr else {
        unreachable!("translate_expr_unary called with wrong expression variant");
    };
    match (op, expr.as_ref()) {
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
    }
}

/// Kept out of [translate_expr_function_call] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[cfg(feature = "json")]
#[inline(never)]
fn translate_expr_json_function(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Json(j) = &func_ctx.func else {
        unreachable!("translate_expr_json_function called with wrong function type");
    };
    match j {
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
            unreachable!("These two functions are only reachable via the -> and ->> operators")
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
                crate::bail_parse_error!("{} function with not exactly 1 argument", j.to_string());
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
                translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
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
    }
}

/// Kept out of [translate_expr_function_call] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_vector_function(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Vector(vector_func) = &func_ctx.func else {
        unreachable!("translate_expr_vector_function called with wrong function type");
    };
    // All unary/binary vector functions share identical translations; the
    // bodies are deduplicated so this helper's stack frame stays small (#6655).
    match vector_func {
        VectorFunc::Vector
        | VectorFunc::Vector32
        | VectorFunc::Vector32Sparse
        | VectorFunc::Vector64
        | VectorFunc::Vector8
        | VectorFunc::Vector1Bit
        | VectorFunc::VectorExtract => {
            let args = expect_arguments_exact!(args, 1, vector_func);
            let start_reg = program.alloc_register();
            translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;

            emit_function_call(program, func_ctx, &[start_reg], target_register)?;
            Ok(target_register)
        }
        VectorFunc::VectorDistanceCos
        | VectorFunc::VectorDistanceL2
        | VectorFunc::VectorDistanceJaccard
        | VectorFunc::VectorDistanceDot
        | VectorFunc::VectorConcat => {
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
    }
}

/// Kept out of [translate_expr_function_call] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_scalar_function(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("translate_expr_scalar_function called with wrong function type");
    };
    match srf {
        ScalarFunc::Cast => {
            unreachable!("this is always ast::Expr::Cast")
        }
        // Arity and custom-types checks are done at bind time
        // in validate_custom_type_function_call.
        ScalarFunc::Array => translate_scalar_array(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::ArrayElement => translate_scalar_array_element(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::ArraySetElement => translate_scalar_array_set_element(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Changes => translate_scalar_changes(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Char => translate_function(
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx,
        ),
        ScalarFunc::Coalesce => translate_scalar_coalesce(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::LastInsertRowid => translate_scalar_last_insert_rowid(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Concat => translate_scalar_concat(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::ConcatWs => translate_scalar_concat_ws(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::IfNull => translate_scalar_ifnull(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Iif => translate_scalar_iif(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),

        ScalarFunc::Glob | ScalarFunc::Like => translate_scalar_glob_like(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
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
        | ScalarFunc::ZeroBlob
        | ScalarFunc::SequenceWatermark => translate_scalar_single_arg(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        #[cfg(feature = "fs")]
        #[cfg(not(target_family = "wasm"))]
        ScalarFunc::LoadExtension => translate_scalar_load_extension(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Random => translate_scalar_random(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        #[cfg(feature = "test_helper")]
        ScalarFunc::TestNondetCounter => translate_scalar_test_nondet_counter(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Date | ScalarFunc::DateTime | ScalarFunc::JulianDay => {
            translate_scalar_datetime(
                program,
                referenced_tables,
                args,
                target_register,
                resolver,
                func_ctx,
            )
        }
        ScalarFunc::Substr | ScalarFunc::Substring => translate_scalar_substr(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Hex => translate_scalar_hex(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::UnixEpoch => translate_scalar_unixepoch(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Time => translate_scalar_time(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::TimeDiff => translate_scalar_timediff(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::TotalChanges => translate_scalar_total_changes(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim
        | ScalarFunc::Round
        | ScalarFunc::Unhex => translate_scalar_trim_round_unhex(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Min => translate_scalar_min(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Max => translate_scalar_max(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Nullif | ScalarFunc::Instr => translate_scalar_nullif_instr(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::SqliteVersion | ScalarFunc::TursoVersion | ScalarFunc::SqliteSourceId => {
            translate_scalar_version(
                program,
                referenced_tables,
                args,
                target_register,
                resolver,
                func_ctx,
            )
        }
        ScalarFunc::Replace => translate_scalar_replace(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::StrfTime => translate_scalar_strftime(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Printf => translate_function(
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx,
        ),
        ScalarFunc::GetByte | ScalarFunc::SetByte => translate_function(
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx,
        ),
        ScalarFunc::Likely => translate_scalar_likely(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::Likelihood => translate_scalar_likelihood(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::TableColumnsJsonArray => translate_scalar_table_columns_json_array(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::BinRecordJsonObject => translate_scalar_bin_record_json_object(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
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
        ScalarFunc::Unlikely => translate_scalar_unlikely(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::StatInit | ScalarFunc::StatPush | ScalarFunc::StatGet => {
            crate::bail_parse_error!("{} is an internal function used by ANALYZE", srf);
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
        | ScalarFunc::Gcd
        | ScalarFunc::Lcm
        | ScalarFunc::Repeat
        | ScalarFunc::Lpad
        | ScalarFunc::Rpad
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
        ScalarFunc::StructPack => translate_scalar_struct_pack(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::UnionValueFunc => translate_scalar_union_value(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::UnionTagFunc => translate_scalar_union_tag(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::UnionExtractFunc => translate_scalar_union_extract(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::StructExtractFunc => translate_scalar_struct_extract(
            program,
            referenced_tables,
            args,
            target_register,
            resolver,
            func_ctx,
        ),
        ScalarFunc::NextVal | ScalarFunc::SetVal => translate_sequence_function(
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx,
        ),
        ScalarFunc::CurrVal => translate_function(
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx,
        ),
    }
}

/// Kept out of [translate_expr_function_call] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_math_function(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Math(math_func) = &func_ctx.func else {
        unreachable!("translate_expr_math_function called with wrong function type");
    };
    match math_func.arity() {
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
            let _ = translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
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
    }
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_between(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    translate_between_expr(
        program,
        referenced_tables,
        expr.clone(),
        target_register,
        resolver,
    )?;
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_collate(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Collate(expr, collation) = expr else {
        unreachable!("translate_expr_collate called with wrong expression variant");
    };
    // First translate inner expr, then set the curr collation. If we set curr collation before,
    // it may be overwritten later by inner translate.
    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
    let collation = resolver.resolve_collation(collation.as_str())?;
    program.set_collation(Some((collation, true)));
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_variable(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    target_register: usize,
) -> Result<usize> {
    let ast::Expr::Variable(variable) = expr else {
        unreachable!("translate_expr_variable called with wrong expression variant");
    };
    let index = program.register_variable(variable);
    program.emit_insn(Insn::Variable {
        index,
        dest: target_register,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_dispatch] so its locals don't inflate the
/// per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_register(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Register(src_reg) = expr else {
        unreachable!("translate_expr_register called with wrong expression variant");
    };
    // When a column reference has been rewritten to a register
    // (UPSERT DO UPDATE WHERE/SET), the register still carries the
    // column's implicit collation for comparison purposes, same as
    // the Expr::Column arm above.
    if let Some(collation) = resolver.register_collations.get(src_reg) {
        program.set_collation(Some((*collation, false)));
    }
    // For DBSP expression compilation: copy from source register to target
    program.emit_insn(Insn::Copy {
        src_reg: *src_reg,
        dst_reg: target_register,
        extra_amount: 0,
    });
    Ok(target_register)
}

/// Error/unreachable expression variants, kept out of [translate_expr_dispatch]
/// so the error formatting locals don't inflate the per-recursion-level stack
/// frame of expression translation (#6655).
#[inline(never)]
fn translate_expr_unsupported(expr: &ast::Expr) -> Result<usize> {
    match expr {
        ast::Expr::DoublyQualified(_, _, _) => {
            crate::bail_parse_error!("DoublyQualified should have been rewritten in optimizer")
        }
        ast::Expr::Exists(_) => {
            crate::bail_parse_error!("EXISTS is not supported in this position")
        }
        ast::Expr::InSelect { .. } => {
            crate::bail_parse_error!("IN (...subquery) is not supported in this position")
        }
        ast::Expr::InTable { .. } => {
            crate::bail_parse_error!("Table expression is not supported in this position")
        }
        ast::Expr::Name(_) => {
            crate::bail_parse_error!("ast::Expr::Name is not supported in this position")
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before translation")
        }
        ast::Expr::Subquery(_) => {
            crate::bail_parse_error!("Subquery is not supported in this position")
        }
        ast::Expr::Default => {
            crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
        }
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        _ => unreachable!("translate_expr_unsupported called with a supported expression variant"),
    }
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_array(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    translate_variadic_insn!(
        program,
        referenced_tables,
        resolver,
        args,
        target_register,
        MakeArray
    )
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_array_element(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1],
                                Insn::ArrayElement { array_reg, index_reg, dest: target_register })
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_array_set_element(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1, value_reg <- 2],
                                Insn::ArraySetElement { array_reg, index_reg, value_reg, dest: target_register })
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_changes(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if !args.is_empty() {
        crate::bail_parse_error!("{} function with more than 0 arguments", srf);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_coalesce(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_last_insert_rowid(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    _args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let regs = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: regs,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_concat(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
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
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_concat_ws(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_ifnull(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if args.len() != 2 {
        crate::bail_parse_error!("{} function requires exactly 2 arguments", srf.to_string());
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_iif(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_glob_like(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if args.len() < 2 {
        crate::bail_parse_error!("{} function with less than 2 arguments", srf.to_string());
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_single_arg(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 1, srf);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[cfg(feature = "fs")]
#[cfg(not(target_family = "wasm"))]
#[inline(never)]
fn translate_scalar_load_extension(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 1, srf);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_random(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if !args.is_empty() {
        crate::bail_parse_error!("{} function with arguments", srf.to_string());
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[cfg(feature = "test_helper")]
#[inline(never)]
fn translate_scalar_test_nondet_counter(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if !args.is_empty() {
        crate::bail_parse_error!("{} function with arguments", srf.to_string());
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_datetime(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len().max(1));
    for (i, arg) in args.iter().enumerate() {
        // register containing result of each argument expression
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_substr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if !(args.len() == 2 || args.len() == 3) {
        crate::bail_parse_error!(
            "{} function with wrong number of arguments",
            srf.to_string()
        )
    }

    let str_reg = program.alloc_register();
    let start_reg = program.alloc_register();
    let length_reg = program.alloc_register();
    let str_reg = translate_expr(program, referenced_tables, &args[0], str_reg, resolver)?;
    let _ = translate_expr(program, referenced_tables, &args[1], start_reg, resolver)?;
    if args.len() == 3 {
        translate_expr(program, referenced_tables, &args[2], length_reg, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: str_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_hex(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 1 {
        crate::bail_parse_error!("hex function must have exactly 1 argument",);
    }
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_unixepoch(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len().max(1));
    for (i, arg) in args.iter().enumerate() {
        // register containing result of each argument expression
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_time(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len().max(1));
    for (i, arg) in args.iter().enumerate() {
        // register containing result of each argument expression
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_timediff(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 2, srf);

    let start_reg = program.alloc_registers(2);
    translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_total_changes(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if !args.is_empty() {
        crate::bail_parse_error!("{} function with more than 0 arguments", srf.to_string());
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_trim_round_unhex(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_max!(args, 2, srf);

    let start_reg = program.alloc_registers(args.len());
    for (i, arg) in args.iter().enumerate() {
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_min(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    if args.is_empty() {
        crate::bail_parse_error!("min function with no arguments");
    }
    let start_reg = program.alloc_registers(args.len());
    for (i, arg) in args.iter().enumerate() {
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_max(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    if args.is_empty() {
        crate::bail_parse_error!("min function with no arguments");
    }
    let start_reg = program.alloc_registers(args.len());
    for (i, arg) in args.iter().enumerate() {
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_nullif_instr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if args.len() != 2 {
        crate::bail_parse_error!("{} function must have two argument", srf.to_string());
    }

    // Allocate both registers first to ensure they're consecutive,
    // since translate_expr may allocate internal registers.
    let first_reg = program.alloc_register();
    let second_reg = program.alloc_register();
    translate_expr(program, referenced_tables, &args[0], first_reg, resolver)?;
    translate_expr(program, referenced_tables, &args[1], second_reg, resolver)?;
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: first_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_version(
    program: &mut ProgramBuilder,
    _referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    _resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_replace(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    if args.len() != 3 {
        crate::bail_parse_error!(
            "wrong number of arguments to function {}()",
            srf.to_string()
        )
    }

    let str_reg = program.alloc_register();
    let pattern_reg = program.alloc_register();
    let replacement_reg = program.alloc_register();
    let _ = translate_expr(program, referenced_tables, &args[0], str_reg, resolver)?;
    let _ = translate_expr(program, referenced_tables, &args[1], pattern_reg, resolver)?;
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_strftime(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len().max(1));
    for (i, arg) in args.iter().enumerate() {
        // register containing result of each argument expression
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_likely(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 1 {
        crate::bail_parse_error!("likely function must have exactly 1 argument",);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_likelihood(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 2 {
        crate::bail_parse_error!("likelihood() function must have exactly 2 arguments",);
    }

    if let ast::Expr::Literal(ast::Literal::Numeric(ref value)) = args[1].as_ref() {
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_table_columns_json_array(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 1 {
        crate::bail_parse_error!(
            "table_columns_json_array() function must have exactly 1 argument",
        );
    }
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_bin_record_json_object(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 2 {
        crate::bail_parse_error!("bin_record_json_object() function must have exactly 2 arguments",);
    }
    let start_reg = program.alloc_registers(2);
    translate_expr(program, referenced_tables, &args[0], start_reg, resolver)?;
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_unlikely(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    if args.len() != 1 {
        crate::bail_parse_error!("Unlikely function must have exactly 1 argument",);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_struct_pack(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    _func_ctx: FuncCtx,
) -> Result<usize> {
    translate_variadic_insn!(
        program,
        referenced_tables,
        resolver,
        args,
        target_register,
        MakeArray
    )
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_union_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 2, srf);
    let tag_name = extract_string_literal(&args[0])?;
    // union_value('tag', val): resolve the tag against the
    // target column's union type. The target is set by
    // INSERT/UPDATE/UPSERT before translating the value.
    let Some(ref union_td) = program.target_union_type else {
        return Err(crate::LimboError::ParseError(
            "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                .to_string(),
        ));
    };
    let Some((tag_index, variant)) = union_td.find_union_variant(&tag_name) else {
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
    let result = translate_expr(program, referenced_tables, &args[1], value_reg, resolver);
    program.target_union_type = prev;
    result?;
    program.emit_insn(Insn::UnionPack {
        tag_index,
        value_reg,
        dest: target_register,
    });
    Ok(target_register)
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_union_tag(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    // union_tag(col): resolve col's union type for index→name lookup.
    let args = expect_arguments_exact!(args, 1, srf);
    let td = resolve_union_from_column(&args[0], referenced_tables, resolver);
    let tag_names = td
        .as_ref()
        .and_then(|td| td.union_def())
        .map(|ud| Arc::clone(&ud.tag_names))
        .unwrap_or_else(|| Arc::from(Vec::<String>::new()));
    translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::UnionTag { src_reg, dest: target_register, tag_names })
}

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_union_extract(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 2, srf);
    let tag_name = extract_string_literal(&args[1])?;
    // union_extract(col, 'tag'): resolve col's union type for name→index.
    let td = resolve_union_from_column(&args[0], referenced_tables, resolver);
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

/// Kept out of [translate_expr_scalar_function] so its locals don't inflate
/// the per-recursion-level stack frame of expression translation (#6655).
#[inline(never)]
fn translate_scalar_struct_extract(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    target_register: usize,
    resolver: &Resolver,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let Func::Scalar(srf) = &func_ctx.func else {
        unreachable!("scalar function helper called with wrong function type");
    };
    let args = expect_arguments_exact!(args, 2, srf);
    let field_name = extract_string_literal(&args[1])?;
    let td = resolve_struct_from_expr(&args[0], referenced_tables, resolver);
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
