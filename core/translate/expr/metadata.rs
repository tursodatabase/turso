use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
    pub jump_target_when_null: BranchOffset,
}

pub(super) fn translate_between_expr(
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

pub(super) fn build_between_terms(
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
pub(super) fn emit_cond_jump(
    program: &mut ProgramBuilder,
    cond_meta: ConditionMetadata,
    reg: usize,
) {
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

pub(super) fn assert_register_range_allocated(
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

pub(super) fn supports_row_value_binary_comparison(operator: &ast::Operator) -> bool {
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
/// Try to read a complete expression from the selected index.
///
/// If an outer join nulls the index row, SQLite computes the original
/// expression. A NULL input does not always produce a NULL result.
pub(super) fn try_emit_expression_index_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<bool> {
    let Some(referenced_tables) = referenced_tables else {
        return Ok(false);
    };
    let Some((table, index, expression_position)) =
        selected_expression_index(expr, referenced_tables)
    else {
        return Ok(false);
    };
    let Some(index_cursor) =
        program.resolve_cursor_id_safe(&CursorKey::index(table.internal_id, index.clone()))
    else {
        return Ok(false);
    };
    // SQLite cannot use the stored index value after an outer join replaces
    // the source row with NULL. It computes the original expression instead.
    let needs_null_row_fallback = referenced_tables.outer_join_may_null_extend(table.internal_id);
    if !needs_null_row_fallback {
        program.emit_column_or_rowid(index_cursor, expression_position, target_register);
        return Ok(true);
    }

    let compute_expression = program.allocate_label();
    let expression_done = program.allocate_label();
    program.emit_insn(Insn::IfNullRow {
        cursor_id: index_cursor,
        target_pc: compute_expression,
        dest: target_register,
    });
    program.emit_column_or_rowid(index_cursor, expression_position, target_register);
    program.emit_insn(Insn::Goto {
        target_pc: expression_done,
    });

    program.preassign_label_to_next_insn(compute_expression);
    // Disable index substitution while the fallback computes the original
    // expression. SQLite clears pIdxEpr for the same interval.
    let skipped_index_values = program.flags.skip_expression_index_values();
    program.flags.set_skip_expression_index_values(true);
    let result = translate_expr(
        program,
        Some(referenced_tables),
        expr,
        target_register,
        resolver,
    );
    program
        .flags
        .set_skip_expression_index_values(skipped_index_values);
    result?;
    program.preassign_label_to_next_insn(expression_done);
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
