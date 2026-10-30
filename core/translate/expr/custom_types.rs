use super::*;
use crate::schema::{IndexColumn, ResolvedType, Schema, EXPR_INDEX_SENTINEL};
use crate::translate::expr_context::ExprContext;
use crate::translate::plan::{Plan, SelectPlan};
use std::borrow::Borrow;

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

/// Check if a literal type is compatible with a custom type's value input type.
/// "any" matches everything; otherwise exact match (case-insensitive).
pub(super) fn literal_compatible_with_value_type(
    literal_type: &str,
    value_input_type: &str,
) -> bool {
    value_input_type.eq_ignore_ascii_case("any")
        || literal_type.eq_ignore_ascii_case(value_input_type)
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

/// Turns the value of index expression `idx_col` in `reg` into the index key.
/// The key of an expression of a custom type is the expression's stored form,
/// as the key of a column of the type is the column's stored value. So an
/// index on `coalesce(c, c)` sorts its rows as `ORDER BY coalesce(c, c)` does.
/// An index column on a generated column keeps the column's value.
pub(crate) fn emit_expression_index_key(
    program: &mut ProgramBuilder,
    reg: usize,
    idx_col: &IndexColumn,
    table: &BTreeTable,
    resolver: &Resolver,
) -> Result<()> {
    if idx_col.pos_in_table != EXPR_INDEX_SENTINEL {
        return Ok(());
    }
    if let Some(stored) = expression_index_stored_form(idx_col, table, resolver)? {
        emit_stored_form(program, reg, &stored, resolver)?;
    }
    Ok(())
}

/// Turns the key of index expression `idx_col`, read from the index into
/// `reg`, into the expression's value (see [emit_expression_index_key]).
pub(crate) fn emit_expression_index_value(
    program: &mut ProgramBuilder,
    reg: usize,
    idx_col: &IndexColumn,
    table: &BTreeTable,
    resolver: &Resolver,
) -> Result<()> {
    if idx_col.pos_in_table != EXPR_INDEX_SENTINEL {
        return Ok(());
    }
    if let Some(stored) = expression_index_stored_form(idx_col, table, resolver)? {
        emit_decode(program, &stored.chain, reg, resolver)?;
    }
    Ok(())
}

/// The type whose stored form the index keeps for index expression `idx_col`
/// over the columns of `table`.
fn expression_index_stored_form(
    idx_col: &IndexColumn,
    table: &BTreeTable,
    resolver: &Resolver,
) -> Result<Option<ResolvedType>> {
    let mut expr = idx_col
        .expr
        .as_deref()
        .expect("expression index column must have an expression")
        .clone();
    crate::schema::resolve_gencol_expr_columns(&mut expr, table.columns())?;
    let schema = resolver.schema();
    let env = TypeEnv::over_table(table, schema, resolver.catalog.symbol_table);
    stored_form_type(&type_of(&expr, &env), schema)
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
        if resolved.chain.iter().all(|td| td.decode().is_none()) {
            return Ok(());
        }
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: dest_reg,
            target_pc: skip_label,
        });

        // Apply decode in reverse order (parent/ancestor first, then child)
        for td in resolved.chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(program, decode_expr, dest_reg, dest_reg, resolver)?;
            }
        }

        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}

/// Emit domain constraint checks for CAST(expr AS domain).
/// Validates NOT NULL and CHECK constraints from the domain type chain.
pub(super) fn emit_domain_cast_constraints(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
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
            if let Ok(Some(resolved)) = resolver.schema().resolve_type(&col.ty_str, is_strict) {
                if resolved.chain.iter().any(|td| td.decode().is_some()) {
                    let decoded_reg = program.alloc_register();
                    emit_user_facing_column_value(
                        program,
                        source_regs(i),
                        decoded_reg,
                        col,
                        is_strict,
                        resolver,
                    )?;
                    return Ok(decoded_reg);
                }
            }
            Ok(source_regs(i))
        })
        .chain(std::iter::once(Ok(rowid_reg)))
        .collect::<Result<Vec<usize>>>()
}

/// [stored_comparison_type] for each component of a row-value comparison, as
/// in `(c, d) > (1, 0)` or `(c, d) IN ((1, 0), (2, 0))`. A comparison of
/// single values has one component.
pub(crate) fn row_value_stored_types(
    lhs: &ast::Expr,
    rhs_operands: &[&ast::Expr],
    arity: usize,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Vec<Option<ResolvedType>>> {
    (0..arity)
        .map(|idx| {
            let components = std::iter::once(lhs)
                .chain(rhs_operands.iter().copied())
                .map(|operand| row_value_component_expr(operand, idx))
                .collect::<Result<Vec<_>>>()?;
            stored_comparison_type(
                components.iter().map(|component| component.as_ref()),
                referenced_tables,
                resolver,
            )
        })
        .collect()
}

/// Translates `expr`, one side of a comparison with `compared_as.len()`
/// components, into consecutive registers from `target_register`, each
/// component in the form [translate_compared_expr] gives it.
pub(crate) fn translate_compared_row_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    compared_as: &[Option<ResolvedType>],
) -> Result<usize> {
    if let [compared_as] = compared_as {
        return translate_compared_expr(
            program,
            referenced_tables,
            expr,
            target_register,
            resolver,
            compared_as.as_ref(),
        );
    }
    if compared_as.iter().all(Option::is_none) {
        return translate_expr(program, referenced_tables, expr, target_register, resolver);
    }
    if let ast::Expr::Parenthesized(components) = unwrap_parens(expr)? {
        for (idx, (component, compared_as)) in components.iter().zip(compared_as).enumerate() {
            translate_compared_expr(
                program,
                referenced_tables,
                component,
                target_register + idx,
                resolver,
                compared_as.as_ref(),
            )?;
        }
        return Ok(target_register);
    }
    // A row subquery outputs its columns decoded, so each component of a
    // custom type is encoded again.
    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
    for (idx, compared_as) in compared_as.iter().enumerate() {
        if let Some(compared_as) = compared_as {
            emit_stored_form(program, target_register + idx, compared_as, resolver)?;
        }
    }
    Ok(target_register)
}

/// The custom type whose stored form `operands` compare in, or None when they
/// compare as SQLite values.
///
/// A value of a custom type compares in the form its column stores: the form
/// an index keeps and ORDER BY sorts. So a table scan, an index search, a sort
/// and min/max put values in the same order. With `ENCODE -value`, 5 is stored
/// as -5, so `c > 4` holds for 1 and not for 5 or 10, and ORDER BY c gives
/// 10, 5, 1.
///
/// Types with their own order (numeric, arrays) compare in that order instead
/// (see `TypeOrdering`). Values of two different custom types can't be
/// compared: neither type's stored form fits both.
pub(crate) fn stored_comparison_type<'a>(
    operands: impl IntoIterator<Item = &'a ast::Expr>,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<ResolvedType>> {
    let context = ExprContext::new(referenced_tables, resolver);
    stored_comparison_type_across_queries(operands.into_iter().map(|operand| (operand, context)))
}

/// [stored_comparison_type] for operands that belong to different queries, as
/// in `x IN (SELECT y ...)`, so each operand's columns are looked up in its own
/// tables.
pub(crate) fn stored_comparison_type_across_queries<'a, 'c, 's: 'c>(
    operands: impl IntoIterator<Item = (&'a ast::Expr, impl Borrow<ExprContext<'c, 's>>)>,
) -> Result<Option<ResolvedType>> {
    let mut found: Option<ResolvedType> = None;
    // Operands of no custom type whose value changes per row.
    let mut varying_others = Vec::new();
    for (operand, context) in operands {
        let context = context.borrow();
        let operand_type = type_of(operand, &context.type_env());
        let Some(ty) = stored_form_type(&operand_type, context.schema())? else {
            if !operand.is_constant(context.resolver()) {
                varying_others.push(operand_type);
            }
            continue;
        };
        match &found {
            Some(other) if !same_type_def(other.leaf(), ty.leaf()) => {
                crate::bail_parse_error!(
                    "cannot compare type {} with type {}",
                    other.leaf().name,
                    ty.leaf().name
                );
            }
            Some(_) => {}
            None => found = Some(ty),
        }
    }
    // As in PostgreSQL, only a constant becomes a value of the type it is
    // compared with. A value of another type that changes per row, as in
    // `date_col = text_col`, would have to go through ENCODE on every row,
    // and a hash join or an IN subquery compares it as it is, so the answer
    // would depend on the plan.
    if let Some(compared_as) = &found {
        let other_type = varying_others
            .iter()
            .find(|operand_type| !is_truth_value_for(operand_type, compared_as.leaf()));
        if let Some(operand_type) = other_type {
            crate::bail_parse_error!(
                "cannot compare type {} with {}: cast it to {} first",
                compared_as.leaf().name,
                operand_type.name(),
                compared_as.leaf().name
            );
        }
    }
    Ok(found)
}

/// Whether `operand_type` is a truth value, such as `u > 0`, compared with the
/// built-in boolean type. A truth value already is a boolean.
fn is_truth_value_for(operand_type: &ExprType, compared_as: &TypeDef) -> bool {
    matches!(operand_type, ExprType::Builtin(BuiltinType::Boolean))
        && compared_as.is_builtin
        && compared_as.name.eq_ignore_ascii_case("boolean")
}

/// Rewrites the comparison `expr` between values of different built-in types
/// that the dialect compares as one of them (see `Dialect::comparison_type`):
/// in PostgreSQL, `ts > d` becomes `ts > CAST(d AS timestamp)`. The cast is in
/// the expression, so every plan compares the same values: a hash join or an
/// IN list compares the cast value as a table scan does. Other expressions are
/// left as they are.
pub(crate) fn cast_compared_builtin_types(
    expr: &mut ast::Expr,
    env: &TypeEnv,
    resolver: &Resolver,
) -> Result<()> {
    let operands: Vec<&mut ast::Expr> = match expr {
        ast::Expr::Binary(lhs, op, rhs) if op.is_comparison() => vec![lhs, rhs],
        ast::Expr::Between {
            lhs, start, end, ..
        } => vec![lhs, start, end],
        ast::Expr::InList { lhs, rhs, .. } => std::iter::once(lhs)
            .chain(rhs.iter_mut())
            .map(|operand| operand.as_mut())
            .collect(),
        // `CASE x WHEN y ...` compares x with each y.
        ast::Expr::Case {
            base: Some(base),
            when_then_pairs,
            ..
        } => std::iter::once(base.as_mut())
            .chain(when_then_pairs.iter_mut().map(|(when, _)| when.as_mut()))
            .collect(),
        _ => return Ok(()),
    };
    let operands = operands.into_iter().map(|operand| (operand, env));
    cast_operands_to_builtin_comparison_type(operands, resolver)
}

/// [cast_compared_builtin_types] for `x IN (SELECT y ...)`: when x and y have
/// different built-in types that the dialect compares as one of them, both are
/// cast to it. The cast on y is in every SELECT of the subquery, so the values
/// the subquery writes to its ephemeral index are the cast ones. Row values,
/// `(x1, x2) IN (SELECT y1, y2 ...)`, are cast column by column.
pub(crate) fn cast_in_subquery_builtin_types(
    lhs: &mut ast::Expr,
    outer_tables: Option<&TableReferences>,
    query: &mut Plan,
    resolver: &Resolver,
) -> Result<()> {
    let mut lhs_columns: Vec<&mut ast::Expr> = match lhs {
        ast::Expr::Parenthesized(exprs) if exprs.len() > 1 => {
            exprs.iter_mut().map(|expr| expr.as_mut()).collect()
        }
        lhs => vec![lhs],
    };
    let outer_env = TypeEnv::new(outer_tables, resolver);
    for (column, lhs_column) in lhs_columns.iter_mut().enumerate() {
        let column_types = [
            stored_form_type(&type_of(lhs_column, &outer_env), resolver.schema())?,
            stored_form_type(
                &query_column_type(query, column, resolver),
                resolver.schema(),
            )?,
        ];
        let Some(target) = builtin_comparison_type(&column_types, resolver) else {
            continue;
        };
        if is_other_custom_type(&column_types[1], &target) {
            // A recursive CTE feeds its own rows back into itself, so a cast
            // on its result would change what it computes. The comparison
            // then fails as one of two different types.
            let Some(selects) = query_selects_mut(query) else {
                continue;
            };
            for select in selects {
                cast_to(&mut select.result_columns[column].expr, &target);
            }
        }
        if is_other_custom_type(&column_types[0], &target) {
            cast_to(lhs_column, &target);
        }
    }
    Ok(())
}

/// Casts each of `operands` whose built-in type is not the one the dialect
/// compares them all as. Each operand comes with the tables its columns
/// belong to.
fn cast_operands_to_builtin_comparison_type<'e>(
    operands: impl IntoIterator<Item = (&'e mut ast::Expr, &'e TypeEnv<'e>)>,
    resolver: &Resolver,
) -> Result<()> {
    let (operands, types): (Vec<_>, Vec<_>) = operands
        .into_iter()
        .map(|(operand, env)| {
            let ty = stored_form_type(&type_of(operand, env), env.schema());
            (operand, ty)
        })
        .unzip();
    let types = types.into_iter().collect::<Result<Vec<_>>>()?;
    let Some(target) = builtin_comparison_type(&types, resolver) else {
        return Ok(());
    };
    for (operand, ty) in operands.into_iter().zip(&types) {
        if is_other_custom_type(ty, &target) {
            cast_to(operand, &target);
        }
    }
    Ok(())
}

/// Whether an operand of type `operand_type` needs a cast to `target`. An
/// operand of no custom type is a constant, which the comparison converts
/// anyway.
fn is_other_custom_type(operand_type: &Option<ResolvedType>, target: &TypeDef) -> bool {
    operand_type
        .as_ref()
        .is_some_and(|ty| !same_type_def(ty.leaf(), target))
}

/// Every SELECT of `query` whose result is the query's result: one, or each
/// SELECT of a compound SELECT. None for a recursive CTE.
fn query_selects_mut(query: &mut Plan) -> Option<Vec<&mut SelectPlan>> {
    match query {
        Plan::Select(select) => Some(vec![select]),
        Plan::CompoundSelect {
            left, right_most, ..
        } => Some(
            left.iter_mut()
                .map(|(select, _)| select)
                .chain(core::iter::once(right_most.as_mut()))
                .collect(),
        ),
        Plan::RecursiveCte(_) => None,
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE and UPDATE plans have no result columns")
        }
    }
}

/// Replaces `operand` with `CAST(operand AS target)`.
fn cast_to(operand: &mut ast::Expr, target: &TypeDef) {
    let value = std::mem::replace(operand, ast::Expr::Literal(ast::Literal::Null));
    *operand = ast::Expr::Cast {
        expr: Box::new(value),
        type_name: Some(ast::Type {
            name: target.name.clone(),
            size: None,
            array_dimensions: 0,
        }),
    };
}

/// The type that the dialect compares all the custom types of compared
/// operands, `types`, as, when they are not all the same. None when they are,
/// or when some pair of them has no common type. User types never have one.
fn builtin_comparison_type(
    types: &[Option<ResolvedType>],
    resolver: &Resolver,
) -> Option<crate::sync::Arc<TypeDef>> {
    let type_defs: Vec<&crate::sync::Arc<TypeDef>> =
        types.iter().flatten().map(|ty| &ty.chain[0]).collect();
    if type_defs
        .windows(2)
        .all(|pair| same_type_def(pair[0], pair[1]))
    {
        return None;
    }
    if !type_defs.iter().all(|ty| ty.is_builtin) {
        return None;
    }
    type_defs
        .iter()
        .find(|target| {
            type_defs.iter().all(|ty| {
                same_type_def(ty, target)
                    || resolver
                        .catalog
                        .dialect
                        .comparison_type(&ty.name.to_lowercase(), &target.name.to_lowercase())
                        .is_some_and(|common| target.name.eq_ignore_ascii_case(common))
            })
        })
        .map(|target| crate::sync::Arc::clone(target))
}

/// Translates `expr`, an operand of a comparison, into `target_register`. When
/// the comparison is between values of the custom type `compared_as` (see
/// [stored_comparison_type]), the operand is put in that type's stored form:
/// a column of the type is read without its DECODE, and any other value goes
/// through the type's ENCODE, as an index search key does. `c = 42` compares
/// the stored `c` with `42` encoded.
pub(crate) fn translate_compared_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    compared_as: Option<&ResolvedType>,
) -> Result<usize> {
    let Some(compared_as) = compared_as else {
        return translate_expr(program, referenced_tables, expr, target_register, resolver);
    };
    match compared_operand_form(expr, referenced_tables, resolver, compared_as)? {
        ComparedOperandForm::StoredColumn => {
            let suppressed = program.flags.suppress_custom_type_decode();
            program.flags.set_suppress_custom_type_decode(true);
            let result =
                translate_expr(program, referenced_tables, expr, target_register, resolver);
            program.flags.set_suppress_custom_type_decode(suppressed);
            return result;
        }
        ComparedOperandForm::Encoded => {}
    }
    if !expr.is_constant(resolver) {
        translate_expr(program, referenced_tables, expr, target_register, resolver)?;
        emit_stored_form(program, target_register, compared_as, resolver)?;
        return Ok(target_register);
    }
    // A constant may be computed once before the loop, but ENCODE rewrites its
    // register and runs on every row, so it works on a copy.
    let value_reg = program.alloc_register();
    translate_expr(program, referenced_tables, expr, value_reg, resolver)?;
    program.emit_insn(Insn::Copy {
        src_reg: value_reg,
        dst_reg: target_register,
        extra_amount: 0,
    });
    emit_stored_form(program, target_register, compared_as, resolver)?;
    Ok(target_register)
}

/// Whether comparing `operand` with `others` runs `operand` through a custom
/// type's ENCODE (see [translate_compared_expr]). `CAST(text_col AS date) =
/// date_col` encodes the cast text as a date. An index or rowid holds the
/// column's own values, not encoded ones, so a seek on it can't find the rows
/// such a comparison matches.
pub(crate) fn comparison_encodes_operand<'a>(
    operand: &'a ast::Expr,
    others: impl IntoIterator<Item = &'a ast::Expr>,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<bool> {
    let Some(compared_as) = stored_comparison_type(
        std::iter::once(operand).chain(others),
        referenced_tables,
        resolver,
    )?
    else {
        return Ok(false);
    };
    Ok(
        compared_operand_form(operand, referenced_tables, resolver, &compared_as)?
            == ComparedOperandForm::Encoded,
    )
}

/// The form [translate_compared_expr] gives an operand of a comparison done in
/// the stored form of a custom type.
#[derive(Debug, PartialEq, Eq)]
enum ComparedOperandForm {
    /// A table column of the type, read without its DECODE.
    StoredColumn,
    /// Any other value, put through the type's ENCODE.
    Encoded,
}

fn compared_operand_form(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    compared_as: &ResolvedType,
) -> Result<ComparedOperandForm> {
    let env = TypeEnv::new(referenced_tables, resolver);
    let has_compared_type = stored_form_type(&type_of(expr, &env), resolver.schema())?
        .is_some_and(|ty| same_type_def(ty.leaf(), compared_as.leaf()));
    if !has_compared_type {
        return Ok(ComparedOperandForm::Encoded);
    }
    if reads_stored_column(expr, referenced_tables, resolver) {
        return Ok(ComparedOperandForm::StoredColumn);
    }
    Ok(ComparedOperandForm::Encoded)
}

/// Turns the value in `reg` into the stored form of `ty` with the type's
/// ENCODE. COERCE does not run: a column's limits apply to what it stores,
/// not to what it is compared with, so `n = 3.504` on a numeric(10,2) column
/// holding 3.50 is false. NULL is left as it is.
pub(crate) fn emit_stored_form(
    program: &mut ProgramBuilder,
    reg: usize,
    ty: &ResolvedType,
    resolver: &Resolver,
) -> Result<()> {
    if ty.chain.iter().all(|td| td.encode().is_none()) {
        return Ok(());
    }
    let skip_null = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_null,
    });
    for td in &ty.chain {
        if let Some(encode) = td.encode() {
            emit_type_expr(program, encode, reg, reg, resolver)?;
        }
    }
    program.preassign_label_to_next_insn(skip_null);
    Ok(())
}

/// Runs the DECODE of each type in `chain` on the value in `reg`, the last
/// type's first. NULL is left as it is.
pub(crate) fn emit_decode(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    if chain.iter().all(|td| td.decode().is_none()) {
        return Ok(());
    }
    let skip_null = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_null,
    });
    for td in chain.iter().rev() {
        if let Some(decode) = td.decode() {
            emit_type_expr(program, decode, reg, reg, resolver)?;
        }
    }
    program.preassign_label_to_next_insn(skip_null);
    Ok(())
}

/// The type whose stored form a value of type `ty` compares in. A domain
/// stores its values as its base type does, so it compares as that type. None
/// for a type with its own order and for a type without ENCODE, such as
/// `varchar`: it stores the value as it is, so a `varchar` compares with a
/// `text` like two texts do.
pub(crate) fn stored_form_type(ty: &ExprType, schema: &Schema) -> Result<Option<ResolvedType>> {
    let ExprType::Custom { type_def, .. } = ty else {
        return Ok(None);
    };
    if schema.type_ordering(type_def).is_some() {
        return Ok(None);
    }
    let Some(resolved) = schema.resolve_type_unchecked(&type_def.name)? else {
        return Ok(None);
    };
    resolved.stored_form()
}

/// Whether `expr` reads a table column that [translate_expr] DECODEs only
/// while `suppress_custom_type_decode` is off, so that reading it with the
/// flag on gives the stored value. A column of a subquery holds the value its
/// SELECT returned, which is already decoded.
fn reads_stored_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> bool {
    let Ok(expr) = unwrap_parens(expr) else {
        return false;
    };
    let ast::Expr::Column { table, .. } = expr else {
        return false;
    };
    let is_table_column = referenced_tables
        .and_then(|tables| tables.find_table_by_internal_id(*table))
        .is_some_and(|(_, table)| matches!(table, Table::BTree(_)));
    // A register the query already computed for the column holds the decoded
    // value, unless it is a hash join's copy of the stored one.
    let cached_decoded = matches!(resolver.resolve_cached_expr_reg(expr), Some((_, false)));
    is_table_column && !cached_decoded
}
