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

/// Translate an expression into bytecode.
#[turso_macros::trace_stack]
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

    if let Some((reg, _needs_decode, collation_ctx)) = resolver.resolve_cached_expr_reg(expr) {
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: target_register,
            extra_amount: 0,
        });
        program.set_collation(collation_ctx);
        if let Some(span) = constant_span {
            program.constant_span_end(span);
        }
        return Ok(target_register);
    }

    match expr {
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
                    let resolve_parameter =
                        |syntax: &ast::Expr| -> Result<Box<crate::schema_expr::SchemaExpr>> {
                            Ok(Box::new(crate::schema_expr::SchemaExpr::resolve(
                                syntax,
                                crate::schema_expr::SchemaExprProfile::Default,
                                crate::schema_expr::SchemaExprContext::without_table(),
                                resolver,
                                crate::schema_expr::ResolutionMode::Strict,
                            )?))
                        };
                    let ty_params = match &tn.size {
                        Some(ast::TypeSize::MaxSize(expr)) => {
                            vec![resolve_parameter(expr)?]
                        }
                        Some(ast::TypeSize::TypeSize(precision, scale)) => {
                            vec![resolve_parameter(precision)?, resolve_parameter(scale)?]
                        }
                        None => Vec::new(),
                    };
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

                    // Domains: apply parent encode chain, then validate constraints
                    // on the encoded value (domain CHECK sees the stored representation).
                    if resolved.is_domain() {
                        // Apply encode from parent custom types (domain itself has encode: None)
                        for td in &resolved.chain {
                            if let Some(encode_expr) = td.encode() {
                                emit_schema_type_transform(
                                    program,
                                    Some(encode_expr),
                                    target_register,
                                    target_register,
                                    &cast_col,
                                    td,
                                    resolver,
                                )?;
                            }
                        }

                        // Validate domain constraints on the encoded value
                        emit_schema_domain_constraints(
                            program,
                            target_register,
                            &resolved.chain,
                            resolver,
                        )?;
                        return Ok(target_register);
                    }

                    let type_def = resolved.leaf();
                    // If the custom type requires parameters but the CAST
                    // doesn't provide them (e.g. CAST(x AS NUMERIC) vs
                    // CAST(x AS numeric(10,2))), fall through to regular CAST.
                    let user_param_count = type_def.user_params().count();
                    if user_param_count == 0 || cast_col.ty_params.len() == user_param_count {
                        // CAST to custom type applies only the encode function,
                        // producing the stored representation.
                        // e.g. CAST(42 AS cents) → 4200
                        if let Some(encode_expr) = type_def.encode() {
                            emit_schema_type_transform(
                                program,
                                Some(encode_expr),
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
            let collation = resolver.resolve_collation(collation.as_str())?;
            program.set_collation(Some((collation, true)));
            Ok(target_register)
        }
        ast::Expr::DoublyQualified(_, _, _) => {
            crate::bail_parse_error!(
                "qualified column must be resolved during semantic planning before emission"
            )
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
            within_group: _,
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
                        // Semantic statement analysis validates arity and the
                        // custom-types feature gate before this direct emitter runs.
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
                        | ScalarFunc::ZeroBlob
                        | ScalarFunc::SequenceWatermark => {
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
                        #[cfg(feature = "test_helper")]
                        ScalarFunc::TestNondetCounter => {
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
                        ScalarFunc::GetByte | ScalarFunc::SetByte => translate_function(
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
                        ScalarFunc::UnionValueFunc => Err(crate::LimboError::InternalError(
                            "union_value must be resolved during semantic planning before emission"
                                .to_string(),
                        )),
                        ScalarFunc::UnionTagFunc => Err(crate::LimboError::InternalError(
                            "union_tag must be resolved during semantic planning before emission"
                                .to_string(),
                        )),
                        ScalarFunc::UnionExtractFunc => Err(crate::LimboError::InternalError(
                            "union_extract must be resolved during semantic planning before emission"
                                .to_string(),
                        )),
                        ScalarFunc::StructExtractFunc => Err(crate::LimboError::InternalError(
                            "struct_extract must be resolved during semantic planning before emission"
                                .to_string(),
                        )),
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
                _ if func.needs_star_expansion() => {
                    crate::bail_parse_error!(
                        "{}(*) must be expanded during semantic planning",
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
        ast::Expr::Id(id) => {
            crate::bail_parse_error!(
                "identifier must be resolved during semantic planning before emission: {}",
                id
            )
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
            crate::bail_parse_error!(
                "qualified column must be resolved during semantic planning before emission"
            )
        }
        ast::Expr::FieldAccess { .. } => {
            crate::bail_parse_error!(
                "field access must be resolved during semantic planning before emission"
            )
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
                } // If the custom type requires parameters but the CAST
                  // doesn't provide them (e.g. CAST(x AS NUMERIC) vs
                  // CAST(x AS numeric(10,2))), fall through to regular CAST.
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
