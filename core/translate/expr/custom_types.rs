use super::*;
use crate::schema::ResolvedType;

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

/// Translates `operand`, which is compared with `other`, into `reg`. A string
/// literal or parameter compared with a value of a custom type is read as a
/// value of the type, as PostgreSQL reads it: it is stored as a column of the
/// type stores it, then read back. So `ts = '2024-01-05T09:31'` compares ts
/// with '2024-01-05 09:31:00', and a text the type does not accept is an
/// error. In a row value such as `(tm, id) = ('10:00', 2)`, each element is
/// compared with the element of `other` at its position.
pub(crate) fn translate_compared_operand(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    operand: &ast::Expr,
    other: &ast::Expr,
    reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let len = row_value_len(operand)?;
    if len <= 1 {
        return translate_compared_value(program, referenced_tables, operand, other, reg, resolver);
    }
    // Each component is translated as one value, so a row value nested
    // inside another is "row value misused", as in SQLite.
    for i in 0..len {
        let component = row_value_component_expr(operand, i)?;
        let other_component = row_value_component_expr(other, i)?;
        translate_compared_value(
            program,
            referenced_tables,
            &component,
            &other_component,
            reg + i,
            resolver,
        )?;
    }
    Ok(reg)
}

/// Translates `operand`, one value compared with `other`, into `reg`. See
/// [translate_compared_operand].
fn translate_compared_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    operand: &ast::Expr,
    other: &ast::Expr,
    reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    match untyped_operand_type(operand, other, referenced_tables, resolver)? {
        Some(resolved) => translate_untyped_operand_as(
            program,
            referenced_tables,
            operand,
            &resolved,
            reg,
            resolver,
        ),
        None => translate_expr(program, referenced_tables, operand, reg, resolver),
    }
}

/// Translates the two operands of a comparison into `lhs_reg` and `rhs_reg`.
/// See [translate_compared_operand].
pub(crate) fn translate_compared_pair(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    rhs: &ast::Expr,
    lhs_reg: usize,
    rhs_reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    translate_compared_operand(program, referenced_tables, lhs, rhs, lhs_reg, resolver)?;
    translate_compared_operand(program, referenced_tables, rhs, lhs, rhs_reg, resolver)?;
    Ok(())
}

/// The registers holding `lhs` as it is compared with `element`, an element of
/// the list on the right of IN. PostgreSQL reads `x IN (a, b)` as
/// `x = a OR x = b`, so a string literal or parameter in `lhs` is read as the
/// type of each element it is compared with. `lhs_reg` holds `lhs` as written
/// in `lhs_arity` registers; it is returned when nothing in `lhs` is read as a
/// custom type, and otherwise a copy with those components converted.
pub(crate) fn translate_in_lhs_for_element(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    lhs_reg: usize,
    lhs_arity: usize,
    element: &ast::Expr,
    resolver: &Resolver,
) -> Result<usize> {
    if resolver.schema().type_registry.is_empty() {
        return Ok(lhs_reg);
    }
    let mut converted = Vec::new();
    match unwrap_parens(lhs)? {
        ast::Expr::Parenthesized(components) if components.len() > 1 => {
            for (i, component) in components.iter().enumerate() {
                let other = row_value_component_expr(element, i)?;
                if let Some(resolved) =
                    untyped_operand_type(component, &other, referenced_tables, resolver)?
                {
                    converted.push((i, component.as_ref(), resolved));
                }
            }
        }
        _ => {
            if let Some(resolved) = untyped_operand_type(lhs, element, referenced_tables, resolver)?
            {
                converted.push((0, lhs, resolved));
            }
        }
    }
    if converted.is_empty() {
        return Ok(lhs_reg);
    }
    let reg = program.alloc_registers(lhs_arity);
    // A converted component is translated from its expression instead of being
    // copied from `lhs_reg` first: its conversion is a constant, which runs
    // before the copy would.
    for i in 0..lhs_arity {
        match converted.iter().find(|(index, _, _)| *index == i) {
            Some((_, component, resolved)) => {
                translate_untyped_operand_as(
                    program,
                    referenced_tables,
                    component,
                    resolved,
                    reg + i,
                    resolver,
                )?;
            }
            None => {
                program.emit_insn(Insn::Copy {
                    src_reg: lhs_reg + i,
                    dst_reg: reg + i,
                    extra_amount: 0,
                });
            }
        }
    }
    Ok(reg)
}

/// Translates the arguments of a scalar min or max into `start_reg..`. Their
/// string literals and parameters are read as described in
/// [untyped_values_type].
pub(crate) fn translate_compared_args(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    args: &[Box<ast::Expr>],
    start_reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    let values_type = untyped_values_type(
        args.iter().map(|arg| arg.as_ref()),
        referenced_tables,
        resolver,
    )?;
    for (i, arg) in args.iter().enumerate() {
        let reg = start_reg + i;
        if !translate_untyped_value(
            program,
            referenced_tables,
            arg,
            values_type.as_ref(),
            reg,
            resolver,
        )? {
            translate_expr(program, referenced_tables, arg, reg, resolver)?;
        }
    }
    Ok(())
}

/// The type the string literals and parameters among `values` are read as,
/// where `values` are what an expression returns one of or compares with each
/// other: the arguments of coalesce, ifnull, min and max, the values of iif,
/// and the results of CASE. As in PostgreSQL, a string literal or parameter
/// is read as a value of the values' common type when that is a custom type:
/// in `coalesce(tm, '00:00')` on a time column, '00:00' is the time 00:00:00.
/// Values of two custom types leave no single type to read a string literal
/// as, which is an error. None when nothing is read as a custom type.
pub(crate) fn untyped_values_type<'e>(
    values: impl Iterator<Item = &'e ast::Expr> + Clone,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<ResolvedType>> {
    let schema = resolver.schema();
    if schema.type_registry.is_empty() {
        return Ok(None);
    }
    let mut has_untyped_value = false;
    for value in values.clone() {
        has_untyped_value |= is_untyped_operand(value)?;
    }
    if !has_untyped_value {
        return Ok(None);
    }
    let env = TypeEnv::new(referenced_tables, resolver);
    let common = common_type(values.clone(), &env);
    match common {
        ExprType::Custom { .. } => untyped_operands_read_as(schema, common),
        ExprType::Array { .. }
        | ExprType::Builtin(_)
        | ExprType::Untyped
        | ExprType::Null
        | ExprType::Unknown
        | ExprType::RowValue
        | ExprType::NotKnownYet => {
            refuse_two_custom_types(values, &env)?;
            Ok(None)
        }
    }
}

/// Fails when `values` have two different custom types. Called when string
/// literals or parameters among them would be read as their common type.
fn refuse_two_custom_types<'e>(
    values: impl Iterator<Item = &'e ast::Expr>,
    env: &TypeEnv,
) -> Result<()> {
    let mut seen: Option<Arc<TypeDef>> = None;
    for value in values {
        let type_def = match type_of(value, env) {
            ExprType::Custom { type_def, .. } => type_def,
            ExprType::Array { .. }
            | ExprType::Builtin(_)
            | ExprType::Untyped
            | ExprType::Null
            | ExprType::Unknown
            | ExprType::RowValue
            | ExprType::NotKnownYet => continue,
        };
        match &seen {
            None => seen = Some(type_def),
            Some(seen) if seen.name == type_def.name => {}
            Some(seen) => crate::bail_parse_error!(
                "cannot tell which type to read a string literal or parameter as: \
                 it is compared with type {} and type {}",
                seen.name,
                type_def.name
            ),
        }
    }
    Ok(())
}

/// Translates `value`, one of the values an expression returns one of (see
/// [untyped_values_type]), into `reg`, which the other values write too.
pub(crate) fn translate_returned_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    value: &ast::Expr,
    values_type: Option<&ResolvedType>,
    reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    if translate_untyped_value(
        program,
        referenced_tables,
        value,
        values_type,
        reg,
        resolver,
    )? {
        return Ok(reg);
    }
    translate_expr_no_constant_opt(
        program,
        referenced_tables,
        value,
        reg,
        resolver,
        NoConstantOptReason::RegisterReuse,
    )
}

/// Translates `value` into `reg` if it is a string literal or parameter read
/// as `values_type` (see [untyped_values_type]), and returns whether it did.
/// The conversion is computed once in a register of its own and copied into
/// `reg`, so the other values may write `reg` too.
pub(crate) fn translate_untyped_value(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    value: &ast::Expr,
    values_type: Option<&ResolvedType>,
    reg: usize,
    resolver: &Resolver,
) -> Result<bool> {
    let Some(resolved) = values_type else {
        return Ok(false);
    };
    if !is_untyped_operand(value)? {
        return Ok(false);
    }
    let value_reg = program.alloc_register();
    translate_untyped_operand_as(
        program,
        referenced_tables,
        value,
        resolved,
        value_reg,
        resolver,
    )?;
    program.emit_insn(Insn::Copy {
        src_reg: value_reg,
        dst_reg: reg,
        extra_amount: 0,
    });
    Ok(true)
}

/// Translates the string literal or parameter `operand` into `reg` as a value
/// of `resolved`: stored as a column of the type stores it, then read back.
pub(crate) fn translate_untyped_operand_as(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    operand: &ast::Expr,
    resolved: &ResolvedType,
    reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    // The operand is a constant, so its conversion is computed once with it.
    let constant_span = (!program.constant_span_is_open())
        .then(|| program.constant_span_start(ConstantSpanPlacement::Hoisted));
    translate_expr(program, referenced_tables, operand, reg, resolver)?;
    emit_untyped_operand_store(program, resolved, reg, resolver)?;
    emit_decode(program, resolved, reg, resolver)?;
    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }
    Ok(reg)
}

/// The type a string literal or parameter compared with `other` is read as,
/// or None when it is compared as written.
pub(crate) fn untyped_operand_type(
    operand: &ast::Expr,
    other: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<ResolvedType>> {
    read_untyped_operand_as(operand, resolver.schema(), || {
        Ok(type_of(other, &TypeEnv::new(referenced_tables, resolver)))
    })
}

/// The type a string literal or parameter compared with a value of
/// `other_type` is read as, or None when it is compared as written. Scans and
/// index seeks both decide here, so they read the same rows.
pub(crate) fn read_untyped_operand_as(
    operand: &ast::Expr,
    schema: &crate::schema::Schema,
    other_type: impl FnOnce() -> Result<ExprType>,
) -> Result<Option<ResolvedType>> {
    if schema.type_registry.is_empty() || !is_untyped_operand(operand)? {
        return Ok(None);
    }
    untyped_operands_read_as(schema, other_type()?)
}

/// The type a string literal or parameter compared with a value of
/// `other_type` is read as, if it is a string literal or parameter.
fn untyped_operands_read_as(
    schema: &crate::schema::Schema,
    other_type: ExprType,
) -> Result<Option<ResolvedType>> {
    let type_def = match other_type {
        ExprType::Custom { type_def, .. } => type_def,
        // An array compares in its own order, which converts the other operand.
        ExprType::Array { .. }
        | ExprType::Builtin(_)
        | ExprType::Untyped
        | ExprType::Null
        | ExprType::Unknown
        | ExprType::RowValue
        // `value` in a type's ENCODE, DECODE or CHECK, which is compiled
        // without its type.
        | ExprType::NotKnownYet => return Ok(None),
    };
    let resolved = schema
        .resolve_type_unchecked(&type_def.name)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("type {} does not resolve", type_def.name))
        })?;
    Ok(converts_untyped_operands(&resolved).then_some(resolved))
}

/// Whether `expr` takes its type from the value it is compared with: a string
/// literal, or a parameter. A parameter that stands for a column, such as a
/// foreign key's parent key, carries the column's type or collation and holds
/// the column's value. Parentheses don't change this: `('10:00')` is a string
/// literal too.
fn is_untyped_operand(expr: &ast::Expr) -> Result<bool> {
    Ok(match unwrap_parens(expr)? {
        ast::Expr::Literal(ast::Literal::String(_)) => true,
        ast::Expr::Variable(variable) => {
            variable.custom_type.is_none() && variable.col_collation.is_none()
        }
        _ => false,
    })
}

/// Whether a string literal or parameter compared with a value of `resolved`
/// is converted to the type. A type with its own order, such as numeric,
/// converts the other operand when it compares instead, and a type without
/// ENCODE or DECODE reads a value as it is written.
fn converts_untyped_operands(resolved: &ResolvedType) -> bool {
    resolved.ordering().is_none()
        && (resolved
            .chain
            .iter()
            .any(|type_def| type_def.encode().is_some())
            || !resolved.decode_returns_value_unchanged())
}

/// Stores the string literal or parameter in `reg` as a value of `resolved`,
/// as INSERT stores it in a column of the type, but without COERCE: as in
/// PostgreSQL, a column's limits, such as the scale of `numeric(10,1)`, do not
/// apply to the value it is compared with. NULL is left as it is.
pub(crate) fn emit_untyped_operand_store(
    program: &mut ProgramBuilder,
    resolved: &ResolvedType,
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    emit_store_value(program, resolved, reg, Coerce::Skip, resolver)
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
    // An array stays a record blob, holding the values its elements read as.
    // It is turned into text only for output.
    if column.is_array() {
        return emit_array_decode(program, dest_reg, column, resolver);
    }
    if let Ok(Some(resolved)) = resolver.schema().resolve_type(&column.ty_str, is_strict) {
        emit_decode(program, &resolved, dest_reg, resolver)?;
    }
    Ok(())
}

/// Emits the DECODE of each of `resolved`'s types on the stored value in
/// `reg`, which turns it into the value a column of the type reads back. NULL
/// is left as it is.
pub(crate) fn emit_decode(
    program: &mut ProgramBuilder,
    resolved: &ResolvedType,
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    if !resolved.has_decode() {
        return Ok(());
    }
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_label,
    });

    // Apply decode in reverse order (parent/ancestor first, then child)
    for td in resolved.chain.iter().rev() {
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
    resolved: &ResolvedType,
    cast_type: &ast::Type,
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    use crate::error::{SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL};

    if resolved.not_null() {
        program.emit_insn(Insn::HaltIfNull {
            constraint_info: None,
            target_reg: reg,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: format!("domain {} does not allow null values", resolved.leaf().name),
            on_error: None,
        });
    }

    for td in &resolved.chain {
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

/// Whether storing a value runs the type's COERCE.
#[derive(Clone, Copy)]
pub(crate) enum Coerce<'a> {
    /// Run COERCE with these arguments, the ones written after the type name,
    /// such as `10, 2` in `numeric(10, 2)`.
    WithArgs(&'a [Box<ast::Expr>]),
    /// Don't run COERCE: the value is only compared with values of the type.
    Skip,
}

/// Emits what storing the value in `reg` as a value of `resolved` runs: each
/// type's ENCODE, then its COERCE unless `coerce` is [Coerce::Skip]. The
/// result is converted to the kind of the type's BASE as a STRICT column of
/// the BASE converts it, so a type BASE integer whose ENCODE gives '5' stores
/// 5, and one whose ENCODE gives 'abc' is an error. COERCE sees the converted
/// value too. A CAST runs the same. NULL is left as it is, without running
/// ENCODE; a domain's NOT NULL is checked afterwards, by the column's NOT NULL
/// constraint or by the CAST.
pub(crate) fn emit_store_value(
    program: &mut ProgramBuilder,
    resolved: &ResolvedType,
    reg: usize,
    coerce: Coerce,
    resolver: &Resolver,
) -> Result<()> {
    let convert = Insn::ConvertToType {
        reg,
        storage_class: resolved.storage_class()?,
        description: format!(
            "as type {} (BASE {})",
            resolved.leaf().name,
            resolved.primitive.to_uppercase()
        ),
    };
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_label,
    });
    for type_def in &resolved.chain {
        if let Some(encode) = type_def.encode() {
            emit_type_expr(program, encode, reg, reg, resolver)?;
        }
        let type_params = match coerce {
            Coerce::WithArgs(type_params) => type_params,
            Coerce::Skip => continue,
        };
        if let Some(coerce_expr) = coerce_to_run(type_def, type_params) {
            program.emit_insn(convert.clone());
            emit_coerce(program, type_def, coerce_expr, reg, type_params, resolver)?;
        }
    }
    program.emit_insn(convert);
    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}

/// Emits what comparing the value in `reg` with stored values of `chain`'s
/// types runs: each type's ENCODE, then the affinity of `storage_class`, so
/// the value has the kind the stored values have. COERCE does not run, and a
/// value that doesn't convert is kept as it is.
pub(crate) fn emit_encode_for_comparison(
    program: &mut ProgramBuilder,
    chain: &[crate::sync::Arc<TypeDef>],
    storage_class: crate::types::StorageClass,
    reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    for type_def in chain {
        if let Some(encode) = type_def.encode() {
            emit_type_expr(program, encode, reg, reg, resolver)?;
        }
    }
    program.emit_insn(Insn::Affinity {
        start_reg: reg,
        count: std::num::NonZeroUsize::MIN,
        affinities: storage_class.affinity().aff_mask().to_string(),
    });
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

/// The type's COERCE, unless the type has parameters and is used without
/// arguments, as a column declared `numeric`: it then has no limits.
fn coerce_to_run<'a>(
    type_def: &'a TypeDef,
    type_params: &[Box<ast::Expr>],
) -> Option<&'a ast::Expr> {
    let coerce = type_def.coerce()?;
    if type_params.is_empty() && type_def.user_params().next().is_some() {
        return None;
    }
    Some(coerce)
}

/// Emits `coerce`, the type's COERCE, on the value in `reg`, with the type's
/// parameters bound to `type_params` by position. NULL is left as it is.
fn emit_coerce(
    program: &mut ProgramBuilder,
    type_def: &TypeDef,
    coerce: &ast::Expr,
    reg: usize,
    type_params: &[Box<ast::Expr>],
    resolver: &Resolver,
) -> Result<()> {
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
        if column.is_array() {
            program.emit_insn(Insn::ArrayDecode { reg: decoded_reg });
        }
        info.violation_regs[i] = decoded_reg;
    }
    Ok(())
}
