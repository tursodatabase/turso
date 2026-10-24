use super::*;

/// Map an AST operator to the string representation used in custom type operator definitions.
pub(super) fn operator_to_str(op: &ast::Operator) -> Option<&'static str> {
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
pub(super) fn emit_custom_type_operator(
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
pub(super) struct ExprCustomTypeInfo {
    type_name: String,
    column: Column,
    is_strict: bool,
    type_def: Arc<TypeDef>,
}

/// If the expression is a column reference to a custom type, return the type info.
pub(super) fn expr_custom_type_info(
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
        let (col, is_strict) = match referenced_tables
            .and_then(|tables| tables.find_table_by_internal_id(*table_ref_id))
        {
            Some((_, table)) => (table.get_column_at(*column)?.clone(), table.is_strict()),
            None if table_ref_id.is_self_table() => {
                let table = resolver.self_table_with_custom_type_operators()?;
                (table.columns().get(*column)?.clone(), table.is_strict)
            }
            None => return None,
        };
        let type_def = resolver.schema().get_type_def(&col.ty_str, is_strict)?;
        return Some(ExprCustomTypeInfo {
            type_name: col.ty_str.to_lowercase(),
            type_def: Arc::clone(type_def),
            column: col,
            is_strict,
        });
    }
    None
}

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

/// Which operand of a binary expression needs encoding before the operator call.
pub(super) enum EncodeArg {
    /// Encode the first argument (e1 is a literal, e2 is the custom type column)
    First,
    /// Encode the second argument (e1 is the custom type column, e2 is a literal)
    Second,
}

/// Info needed to encode a literal argument for an operator call.
pub(super) struct OperatorEncodeInfo {
    column: Column,
    type_def: Arc<TypeDef>,
    which: EncodeArg,
}

/// Result of resolving a custom type operator. May be a direct match or derived
/// from `<` and `=` operators (e.g. `>` is derived as swap_args + `<`).
pub(super) struct ResolvedOperator {
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
///    is compatible with the custom type's `value` input type, OR
/// 3. The operator is arithmetic, the type reads stored values back unchanged,
///    one operand is a column of that type and the other is anything except a
///    column of a different custom type. This lets `date + 1`, `date + int_col`
///    and `date + ?` add days, while a comparison such as `int_col = '10'` still
///    uses the standard operator.
/// 4. The operator is arithmetic and the operands are columns of two different
///    types that both read stored values back unchanged and both declare a
///    function for the operator. The left type's function is used. This lets
///    `timestamptz_col - date_col` subtract.
///
/// When case 2 applies, the literal is encoded before being passed to the operator
/// function so both arguments are in the same (encoded) representation. In case 3
/// the other operand is passed as is, which is only safe when encoded and decoded
/// values look the same. The operator function checks its type.
pub(super) fn find_custom_type_operator(
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
            return None;
        }
        // Case 4: Different custom types with an arithmetic operator.
        let both_decode_unchanged = lhs.type_def.decode_returns_value_unchanged()
            && rhs.type_def.decode_returns_value_unchanged();
        if op.is_comparison() || !both_decode_unchanged {
            return None;
        }
        arithmetic_function(&rhs.type_def, op_str)?;
        let func_name = arithmetic_function(&lhs.type_def, op_str)?;
        return Some(ResolvedOperator {
            func_name,
            swap_args: false,
            negate: false,
            encode_info: None,
        });
    }

    let resolve_one_custom_operand =
        |info: &ExprCustomTypeInfo, other: &ast::Expr, which: EncodeArg| {
            let encode_other = literal_type_name(other).is_some_and(|lit_type| {
                literal_compatible_with_value_type(lit_type, info.type_def.value_input_type())
            });
            let pass_other_unencoded =
                !op.is_comparison() && info.type_def.decode_returns_value_unchanged();
            if !encode_other && !pass_other_unencoded {
                return None;
            }
            let (func_name, swap_args, negate) = find_in_type_def(&info.type_def)?;
            Some(ResolvedOperator {
                func_name,
                swap_args,
                negate,
                encode_info: encode_other.then(|| OperatorEncodeInfo {
                    column: info.column.clone(),
                    type_def: info.type_def.clone(),
                    which,
                }),
            })
        };

    match (&lhs_info, &rhs_info) {
        (Some(lhs), None) => resolve_one_custom_operand(lhs, e2, EncodeArg::Second),
        (None, Some(rhs)) => resolve_one_custom_operand(rhs, e1, EncodeArg::First),
        _ => None,
    }
}

fn arithmetic_function(type_def: &TypeDef, op_str: &str) -> Option<String> {
    type_def
        .operators()
        .iter()
        .find(|o| o.op == op_str)
        .and_then(|o| o.func_name.clone())
}

/// Converts a quoted literal or a bound parameter that is compared with an
/// expression of a custom type (a column, or a CAST to the type) to that type,
/// the same way storing it would. A column is decoded when it is read, so the
/// value is encoded and then decoded; a CAST gives the stored form, so the
/// value is only encoded. Returns the register that holds the value to compare.
pub(super) fn emit_comparison_value_as_column_type(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    value_expr: &ast::Expr,
    value_reg: usize,
    typed_expr: &ast::Expr,
    resolver: &Resolver,
) -> Result<usize> {
    let Some(typed) = comparison_type_of(typed_expr, referenced_tables, resolver)? else {
        return Ok(value_reg);
    };
    let converted = emit_untyped_value_as_type(
        program,
        value_expr,
        &typed.column,
        &typed.chain,
        typed.form,
        resolver,
    )?;
    Ok(converted.unwrap_or(value_reg))
}

struct ComparisonType {
    column: Column,
    chain: crate::alloc::Vec<Arc<TypeDef>>,
    form: ValueForm,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueForm {
    Stored,
    Decoded,
}

fn comparison_type_of(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<Option<ComparisonType>> {
    match expr {
        ast::Expr::Column { .. } => {
            let Some(info) = expr_custom_type_info(expr, referenced_tables, resolver) else {
                return Ok(None);
            };
            if info.column.is_array() {
                return Ok(None);
            }
            let Some(resolved) = resolver
                .schema()
                .resolve_type(&info.column.ty_str, info.is_strict)?
            else {
                return Ok(None);
            };
            Ok(Some(ComparisonType {
                column: info.column,
                chain: resolved.chain,
                form: ValueForm::Decoded,
            }))
        }
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } if type_name.array_dimensions == 0 => {
            let cast_type_name = crate::util::dequote(&type_name.name);
            let Some(resolved) = resolver.schema().resolve_type_unchecked(&cast_type_name)? else {
                return Ok(None);
            };
            let ty_params: Vec<Box<ast::Expr>> = match &type_name.size {
                Some(ast::TypeSize::MaxSize(e)) => vec![e.clone()],
                Some(ast::TypeSize::TypeSize(e1, e2)) => vec![e1.clone(), e2.clone()],
                None => Vec::new(),
            };
            let user_param_count = resolved.leaf().user_params().count();
            if !resolved.is_domain() && user_param_count != 0 && ty_params.len() != user_param_count
            {
                return Ok(None);
            }
            let mut column = Column::new(
                None,
                cast_type_name,
                None,
                None,
                Type::Null,
                None,
                ColDef::default(),
            );
            if !resolved.is_domain() {
                column.ty_params = ty_params;
            }
            let form = if resolver.dialect.cast_to_custom_type_decodes() {
                ValueForm::Decoded
            } else {
                ValueForm::Stored
            };
            Ok(Some(ComparisonType {
                column,
                chain: resolved.chain,
                form,
            }))
        }
        _ => Ok(None),
    }
}

/// Converts `value_expr` to the custom type described by `chain` when it is a
/// quoted string or a bound parameter and the type compares its values with
/// the standard comparison. Returns None when the value is left as it is.
///
/// The conversion only depends on the value, so it is computed once per
/// execution, before the first row is read, whenever the type's ENCODE and
/// DECODE expressions allow it. An invalid literal then fails even when no row
/// is read.
pub(crate) fn emit_untyped_value_as_type(
    program: &mut ProgramBuilder,
    value_expr: &ast::Expr,
    column: &Column,
    chain: &[Arc<TypeDef>],
    form: ValueForm,
    resolver: &Resolver,
) -> Result<Option<usize>> {
    let Some(leaf_type) = chain.first() else {
        return Ok(None);
    };
    let comparing_type = chain
        .iter()
        .find(|td| !td.operators().is_empty())
        .unwrap_or(leaf_type);
    let compares_natively =
        comparing_type.operators().is_empty() || comparing_type.compares_with_standard_operator();
    if !compares_natively || !value_has_no_type_of_its_own(value_expr, comparing_type) {
        return Ok(None);
    }
    let encode_is_skipped = chain
        .iter()
        .any(|td| td.encode().is_some() && td.encode_for_comparison_value().is_none());
    if encode_is_skipped || chain.iter().all(|td| td.encode().is_none()) {
        return Ok(None);
    }
    let decodes: Vec<&ast::Expr> = match form {
        ValueForm::Stored => Vec::new(),
        ValueForm::Decoded => chain.iter().rev().filter_map(|td| td.decode()).collect(),
    };
    let same_for_every_row = chain
        .iter()
        .filter_map(|td| td.encode())
        .chain(decodes.iter().copied())
        .all(|expr| expr.is_constant(resolver));
    let constant_span = (same_for_every_row && !program.constant_span_is_open())
        .then(|| program.constant_span_start());

    let converted_reg = program.alloc_register();
    translate_expr(program, None, value_expr, converted_reg, resolver)?;
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg: converted_reg,
        target_pc: skip_label,
    });
    for td in chain {
        if let Some(encode_expr) = td.encode() {
            emit_type_expr(
                program,
                encode_expr,
                converted_reg,
                converted_reg,
                column,
                td,
                resolver,
            )?;
        }
    }
    if form == ValueForm::Decoded {
        for td in chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(
                    program,
                    decode_expr,
                    converted_reg,
                    converted_reg,
                    column,
                    td,
                    resolver,
                )?;
            }
        }
    }
    program.preassign_label_to_next_insn(skip_label);

    if let Some(span) = constant_span {
        program.constant_span_end(span);
    }
    Ok(Some(converted_reg))
}

/// A quoted string or a bound parameter, as opposed to a number or an
/// expression with a type. PostgreSQL converts such a value to the type of
/// the column it is compared with.
fn value_has_no_type_of_its_own(value_expr: &ast::Expr, type_def: &TypeDef) -> bool {
    let is_untyped = matches!(
        value_expr,
        ast::Expr::Literal(ast::Literal::String(_)) | ast::Expr::Variable(_)
    );
    is_untyped && literal_compatible_with_value_type("text", type_def.value_input_type())
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
///
/// `uses_custom_type_operators` must be false when `expr` is the expression of a
/// virtual generated column, see `SelfTableContext::ForDML`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_dml_expr_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut expr: ast::Expr,
    columns: &[Column],
    column_regs: &mut [usize],
    table: &Arc<BTreeTable>,
    dest_reg: usize,
    uses_custom_type_operators: bool,
) -> Result<()> {
    crate::schema::resolve_gencol_expr_columns(&mut expr, columns)?;

    let is_strict = table.is_strict;
    for (i, col) in columns.iter().enumerate() {
        if col.is_rowid_alias() {
            continue;
        }
        if let Ok(Some(resolved)) = resolver.schema().resolve_type(&col.ty_str, is_strict) {
            if resolved.chain.iter().any(|td| td.decode().is_some()) {
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
        uses_custom_type_operators,
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
