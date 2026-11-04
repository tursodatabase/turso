use super::*;

/// Get the effective type name of a literal expression.
pub(super) fn literal_type_name(expr: &ast::Expr) -> Option<&'static str> {
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

/// Whether a custom type takes values of the SQL type `type_name` as input,
/// such as `integer` for a type based on integer. A type whose input type is
/// `any` takes every value. A comparison of the type's values with such a
/// value is well typed, as it is for an operator of the type.
pub(crate) fn takes_input_of_type(type_def: &TypeDef, type_name: &str) -> bool {
    let value_input_type = type_def.value_input_type();
    value_input_type.eq_ignore_ascii_case("any") || type_name.eq_ignore_ascii_case(value_input_type)
}

/// Whether a custom type's function runs the operator expression `expr`, such
/// as `a < b` or `-a`. An operator that does not exist, such as `date * 2`,
/// counts too: emitting it fails.
pub(crate) fn runs_custom_type_function(expr: &ast::Expr, env: &TypeEnv) -> bool {
    !matches!(custom_operator_call(expr, env), Ok(None))
}

/// Emits the operator expression `expr` with a custom type's function, if one
/// runs it, and returns the register holding the result. `None` means SQLite's
/// operator runs.
pub(super) fn emit_custom_type_operator(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    resolver: &Resolver,
) -> Result<Option<usize>> {
    let env = TypeEnv::new(referenced_tables, resolver);
    let Some(call) = custom_operator_call(expr, &env)? else {
        return Ok(None);
    };
    let result_reg = emit_operator_call(program, referenced_tables, expr, &call, resolver)?;
    Ok(Some(result_reg))
}

/// Evaluate an expression-index expression in a DML context (INSERT/UPDATE/UPSERT)
/// from the row's stored values in `column_regs`. Reading a custom-type column
/// in the expression decodes it, so the expression sees the user's values.
pub(crate) fn emit_dml_expr_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut expr: ast::Expr,
    columns: &[Column],
    column_regs: &[usize],
    table: &Arc<BTreeTable>,
    dest_reg: usize,
) -> Result<()> {
    crate::schema::resolve_gencol_expr_columns(&mut expr, columns)?;

    let pairs = columns.iter().zip(column_regs.iter().copied());
    let ctx = SelfTableContext::ForDML {
        dml_ctx: DmlColumnContext::from_column_reg_mapping(pairs, DmlRowForm::Stored),
        table: Arc::clone(table),
    };
    resolver.with_self_table_context(program, Some(&ctx), |program, _| {
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
        emit_decode(program, &resolved.chain, dest_reg, resolver)?;
    }
    Ok(())
}

/// Emits the DECODE of each of `chain`'s types on the stored value in `reg`,
/// which turns it into the value a column of the type reads back. NULL is
/// left as it is.
pub(crate) fn emit_decode(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    if chain.iter().all(|td| td.decode().is_none()) {
        return Ok(());
    }
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_label,
    });

    // Apply decode in reverse order (parent/ancestor first, then child)
    for td in chain.iter().rev() {
        if let Some(decode_expr) = td.decode() {
            emit_type_expr(program, decode_expr, reg, reg, resolver)?;
        }
    }

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}

/// Emit domain constraint checks for CAST(expr AS domain).
/// Validates NOT NULL and CHECK constraints from the domain type chain.
pub(super) fn emit_domain_cast_constraints(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    cast_type: &ast::Type,
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    use crate::error::{SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL};

    let any_not_null = chain.iter().any(|td| td.not_null);

    if any_not_null {
        program.emit_insn(Insn::HaltIfNull {
            constraint_info: None,
            target_reg: reg,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: format!(
                "domain {} does not allow null values",
                chain.first().map(|td| td.name.as_str()).unwrap_or("?")
            ),
            on_error: None,
        });
    }

    for td in chain {
        for (i, dc) in td.domain_checks.iter().enumerate() {
            let constraint_name = dc
                .name
                .clone()
                .unwrap_or_else(|| format!("{}_{}", td.name, i));

            // `value` reads the CAST's result with the CAST's type, as a
            // column of the domain does, so `value < 100` on a domain over
            // numeric compares numbers, not numeric's decoded text.
            let mut check = dc.check.as_ref().clone();
            walk_expr_mut(&mut check, &mut |e| {
                if matches!(e, ast::Expr::Id(name) if name.as_str().eq_ignore_ascii_case("value")) {
                    *e = ast::Expr::Register {
                        register: reg,
                        custom_type: Some(Box::new(cast_type.clone())),
                    };
                }
                Ok(WalkControl::Continue)
            })?;

            let expr_result_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                None,
                &check,
                expr_result_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

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
                constraint_info: None,
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

/// Emits what storing the value in `reg` as a value of `chain`'s types runs:
/// each type's ENCODE, then its COERCE with `type_params`, the arguments
/// written after the type name. A CAST with type arguments runs the same.
pub(crate) fn emit_store_value(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    reg: usize,
    type_params: &[Box<ast::Expr>],
    resolver: &Resolver,
) -> Result<()> {
    for type_def in chain {
        if let Some(encode) = type_def.encode() {
            emit_type_expr(program, encode, reg, reg, resolver)?;
        }
        emit_coerce(program, type_def, reg, type_params, resolver)?;
    }
    Ok(())
}

/// Emit bytecode for a custom type ENCODE or DECODE expression, with `value`
/// referencing `value_reg`. The result is written to `dest_reg`.
pub(crate) fn emit_type_expr(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    value_reg: usize,
    dest_reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    emit_expr_with_bindings(program, expr, value_reg, dest_reg, &[], resolver)
}

/// Emits the type's COERCE on the value in `reg`, with the type's parameters
/// bound to `type_params` by position. A type with parameters used without
/// arguments, as a column declared `numeric`, has no limits and nothing is
/// emitted. NULL is left as it is.
fn emit_coerce(
    program: &mut ProgramBuilder,
    type_def: &TypeDef,
    reg: usize,
    type_params: &[Box<ast::Expr>],
    resolver: &Resolver,
) -> Result<()> {
    let Some(coerce) = type_def.coerce() else {
        return Ok(());
    };
    if type_params.is_empty() && type_def.user_params().next().is_some() {
        return Ok(());
    }
    // A parameter without an argument, as scale in numeric(10), is NULL.
    let null = ast::Expr::Literal(ast::Literal::Null);
    let bindings: Vec<(&str, &ast::Expr)> = type_def
        .user_params()
        .enumerate()
        .map(|(i, param)| {
            let arg = type_params.get(i).map_or(&null, |arg| arg.as_ref());
            (param.name.as_str(), arg)
        })
        .collect();
    let skip_null = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_null,
    });
    emit_expr_with_bindings(program, coerce, reg, reg, &bindings, resolver)?;
    program.preassign_label_to_next_insn(skip_null);
    Ok(())
}

/// Translates `expr` with `value` referencing `value_reg` and each name in
/// `bindings` referencing its expression's value.
fn emit_expr_with_bindings(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    value_reg: usize,
    dest_reg: usize,
    bindings: &[(&str, &ast::Expr)],
    resolver: &Resolver,
) -> Result<usize> {
    program
        .id_register_overrides
        .insert("value".to_string(), value_reg);

    // Capture the result so the overrides are cleared even on error.
    let bind_result: Result<()> = (|| {
        for (name, bound_expr) in bindings {
            let reg = program.alloc_register();
            translate_expr(program, None, bound_expr, reg, resolver)?;
            program.id_register_overrides.insert(name.to_string(), reg);
        }
        Ok(())
    })();

    let result = bind_result.and_then(|()| {
        // The `value` placeholder refers to a register that changes per row,
        // so the expression must not be hoisted as a constant.
        translate_expr_no_constant_opt(
            program,
            None,
            expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )
    });

    program.id_register_overrides.clear();

    result
}

/// Decode the custom type columns of a trigger's NEW or OLD row.
///
/// For each column that `holds_stored_value` and has a custom type decode
/// expression, copies the encoded register to a new register and emits the
/// decode expression. NULL values are skipped. Returns a Vec of registers: one
/// per column (decoded or original) plus the rowid at the end.
pub(crate) fn emit_trigger_decode_registers(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    source_regs: &dyn Fn(usize) -> usize,
    holds_stored_value: &dyn Fn(usize) -> bool,
    rowid_reg: usize,
    is_strict: bool,
) -> Result<Vec<usize>> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| -> Result<usize> {
            if !holds_stored_value(i) || !resolver.schema().column_has_decode(col, is_strict) {
                return Ok(source_regs(i));
            }
            let decoded_reg = program.alloc_register();
            emit_user_facing_column_value(
                program,
                source_regs(i),
                decoded_reg,
                col,
                is_strict,
                resolver,
            )?;
            Ok(decoded_reg)
        })
        .chain(std::iter::once(Ok(rowid_reg)))
        .collect::<Result<Vec<usize>>>()
}

/// Decode the custom type and array values a constraint error reports, so the
/// error shows the values the user wrote instead of their stored encoding.
///
/// Emit this right before the instruction that raises the error, so only rows
/// that fail the constraint run the decode.
pub(crate) fn emit_constraint_values_decode(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &BTreeTable,
    info: &mut ConstraintInfo,
) -> Result<()> {
    if !resolver.catalog.dialect.constraint_errors_show_values() {
        return Ok(());
    }
    let names = if info.row_col_names.is_empty() {
        &info.column_names
    } else {
        &info.row_col_names
    };
    for (i, name) in names.iter().enumerate() {
        let Some((_, column)) = table.get_column(name) else {
            continue;
        };
        if !column.is_array() && !resolver.schema().column_has_decode(column, table.is_strict) {
            continue;
        }
        let decoded_reg = program.alloc_register();
        emit_user_facing_column_value(
            program,
            info.violation_regs[i],
            decoded_reg,
            column,
            table.is_strict,
            resolver,
        )?;
        // emit_user_facing_column_value leaves arrays encoded, because SELECT
        // decodes them when it outputs the row.
        if column.is_array() {
            emit_array_decode(program, decoded_reg, column, resolver)?;
        }
        info.violation_regs[i] = decoded_reg;
    }
    Ok(())
}
