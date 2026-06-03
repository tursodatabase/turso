use super::*;

/// The base logic for translating LIKE and GLOB expressions.
/// The logic for handling "NOT LIKE" is different depending on whether the expression
/// is a conditional jump or not. This is why the caller handles the "NOT LIKE" behavior;
/// see [translate_condition_expr] and [translate_expr] for implementations.
pub(super) fn translate_like_base(
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
pub(super) fn translate_function(
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

pub(super) fn wrap_eval_jump_expr(
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

pub(super) fn wrap_eval_jump_expr_zero_or_null(
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
