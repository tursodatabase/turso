//! Arithmetic operators on custom types: which function runs them.
//!
//! The decision is made once, before code generation, by
//! [resolve_custom_type_operators]. It rewrites `amount + 1` into
//! `TypedCall { func: numeric_add, args: [amount, 1] }`, wrapping an operand that
//! must be converted to the type's stored form in `Convert`. Code generation then
//! emits the call it finds in the tree.
//!
//! Comparisons are not rewritten: index seeks look for a plain comparison, so
//! they still pick the type's function while emitting (see `custom_types.rs`).

use super::*;
use crate::schema::{ArithmeticArg, ArithmeticSignature};
use crate::translate::expr::expr_type::{
    same_type_def, type_of, BuiltinType, ColumnCustomType, ExprType, TypeEnv,
};
use crate::Schema;

/// Rewrites every arithmetic operator on a custom-type value in `expr` into the
/// call that runs it. Running it again on its own output changes nothing.
pub(crate) fn resolve_custom_type_operators(
    expr: &mut ast::Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<()> {
    if resolver.schema().type_registry.is_empty() {
        return Ok(());
    }
    let env = TypeEnv::new(tables, resolver);
    resolve_operators(expr, &env, resolver)
}

/// Operands are resolved before their operator, so the operator sees their
/// types: in `(d + 7) - d` the left operand is a date.
fn resolve_operators(expr: &mut ast::Expr, env: &TypeEnv, resolver: &Resolver) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        let ast::Expr::Binary(lhs, op, rhs) = expr else {
            return Ok(WalkControl::Continue);
        };
        resolve_operators(lhs, env, resolver)?;
        resolve_operators(rhs, env, resolver)?;
        if let Some(call) = find_arithmetic_call(lhs, op, rhs, env, resolver)? {
            let operator = *op;
            let ast::Expr::Binary(lhs, _, rhs) = std::mem::take(expr) else {
                unreachable!("matched a binary expression above");
            };
            *expr = call.into_expr(operator, *lhs, *rhs);
        }
        Ok(WalkControl::SkipChildren)
    })?;
    Ok(())
}

/// The function that runs `lhs op rhs` when an operand is a custom-type value.
/// `None` means SQLite's operator runs: no operand has a custom type, an
/// operand is NULL, or the signature says so (`smallint`, `bigint`). When no
/// signature fits, as in `date + date` or `date * 2`, it is an error: running
/// SQLite's operator on the value would give an answer nobody asked for.
fn find_arithmetic_call(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    env: &TypeEnv,
    resolver: &Resolver,
) -> Result<Option<ArithmeticCall>> {
    if !is_arithmetic(op) {
        return Ok(None);
    }
    let Some(op_str) = operator_to_str(op) else {
        return Ok(None);
    };
    if is_null(lhs) || is_null(rhs) {
        return Ok(None);
    }
    let operands = [
        Operand::new(lhs, env, resolver)?,
        Operand::new(rhs, env, resolver)?,
    ];
    let mut custom_types = operands.iter().filter_map(|o| o.value_type.as_ref());
    let Some(first) = custom_types.next() else {
        return Ok(None);
    };
    let second = custom_types.next();
    let candidates = [Some(first), second]
        .into_iter()
        .flatten()
        .flat_map(|ty| ty.type_def.arithmetic(op_str).map(move |s| (s, ty)));
    let Some((signature, ty)) = candidates
        .into_iter()
        .find(|(s, ty)| accepts(s, ty, &operands))
    else {
        crate::bail_parse_error!(
            "operator does not exist: {} {op_str} {}",
            operands[0].type_name,
            operands[1].type_name
        );
    };
    call_for(signature, ty, resolver)
}

/// Arithmetic with NULL is NULL whichever function runs it.
fn is_null(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Literal(ast::Literal::Null))
}

/// Arithmetic on a custom type is rewritten into a `TypedCall` before emission,
/// unless the type's arithmetic is SQLite's operator. Any other operand
/// reaching emission under a plain operator means a path skipped
/// [resolve_custom_type_operators], and SQLite's operator would silently run
/// instead of the type's function.
pub(super) fn assert_custom_type_arithmetic_resolved(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<()> {
    let Some(op_str) = operator_to_str(op).filter(|_| is_arithmetic(op)) else {
        return Ok(());
    };
    if is_null(lhs) || is_null(rhs) {
        return Ok(());
    }
    let env = TypeEnv::new(referenced_tables, resolver);
    for expr in [lhs, rhs] {
        let operand = Operand::new(expr, &env, resolver)?;
        if operand
            .value_type
            .is_some_and(|ty| !has_sqlite_arithmetic(&ty.type_def, op_str))
        {
            return Err(LimboError::InternalError(format!(
                "custom type operator was not resolved before emission: {lhs} {op} {rhs}"
            )));
        }
    }
    Ok(())
}

fn is_arithmetic(op: &ast::Operator) -> bool {
    matches!(
        op,
        ast::Operator::Add
            | ast::Operator::Subtract
            | ast::Operator::Multiply
            | ast::Operator::Divide
            | ast::Operator::Modulus
    )
}

/// What a signature needs to know about an operand. A domain is replaced by the
/// custom type it is based on, if any.
struct Operand<'a> {
    /// The custom type of the operand's value: a column, a CAST, the result of
    /// another operator.
    value_type: Option<ColumnCustomType>,
    /// The custom type of the operand when it is a column. A user type's function
    /// is only given columns: a CAST gives the stored form, a column the value
    /// its DECODE returns, and the function can't tell them apart.
    column_type: Option<ColumnCustomType>,
    /// The kind of literal the operand is, for literals: "integer", "text", ...
    literal_kind: Option<&'a str>,
    /// The operand's type for an error message: `date`, `integer`, ...
    type_name: String,
}

impl Operand<'_> {
    fn new(expr: &ast::Expr, env: &TypeEnv, resolver: &Resolver) -> Result<Self> {
        let column_type = match operand_custom_type(expr, env, resolver) {
            Some(ty) => without_domains(ty, resolver.schema())?,
            None => None,
        };
        let expr_type = type_of(expr, env);
        let value_type = match (expr, &expr_type) {
            (ast::Expr::Column { .. }, _) => column_type.clone(),
            (_, ExprType::Custom { type_def, params }) => without_domains(
                ColumnCustomType {
                    type_def: type_def.clone(),
                    params: params.clone(),
                },
                resolver.schema(),
            )?,
            _ => None,
        };
        let type_name = match (&value_type, expr_type) {
            (Some(ty), _) => ty.type_def.name.clone(),
            (None, ExprType::Builtin(BuiltinType::Any)) => "value".to_string(),
            (None, ExprType::Builtin(builtin)) => format!("{builtin:?}").to_lowercase(),
            (None, ExprType::Custom { type_def, .. }) => type_def.name.clone(),
            (None, ExprType::Untyped) => "text".to_string(),
            (None, ExprType::Null) => "null".to_string(),
            (None, _) => "value".to_string(),
        };
        Ok(Self {
            value_type,
            column_type,
            literal_kind: literal_type_name(expr),
            type_name,
        })
    }
}

/// A domain has the operators of the type it is based on. A domain over a plain
/// SQLite type, such as `CREATE DOMAIN pos AS integer`, has SQLite's operators.
fn without_domains(ty: ColumnCustomType, schema: &Schema) -> Result<Option<ColumnCustomType>> {
    if !ty.type_def.is_domain {
        return Ok(Some(ty));
    }
    let resolved = schema
        .resolve_type_unchecked(&ty.type_def.name)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("domain {} is not registered", ty.type_def.name))
        })?;
    Ok(resolved
        .chain
        .into_iter()
        .find(|type_def| !type_def.is_domain)
        .map(|type_def| ColumnCustomType {
            type_def,
            params: ty.params,
        }))
}

/// Whether `signature`, declared by `ty`, accepts both operands.
fn accepts(
    signature: &ArithmeticSignature,
    ty: &ColumnCustomType,
    operands: &[Operand; 2],
) -> bool {
    signature
        .args
        .iter()
        .zip(operands)
        .all(|(arg, operand)| accepts_operand(*arg, signature.op, ty, operand))
}

fn accepts_operand(arg: ArithmeticArg, op: &str, ty: &ColumnCustomType, operand: &Operand) -> bool {
    let is_this_type =
        |t: Option<&ColumnCustomType>| t.is_some_and(|t| same_type_def(&t.type_def, &ty.type_def));
    match arg {
        ArithmeticArg::Column => is_this_type(operand.column_type.as_ref()),
        ArithmeticArg::Value => is_this_type(operand.value_type.as_ref()),
        ArithmeticArg::EncodableLiteral => {
            operand.value_type.is_none()
                && operand.literal_kind.is_some_and(|kind| {
                    literal_compatible_with_value_type(kind, ty.type_def.value_input_type())
                })
        }
        ArithmeticArg::PlainValue => operand
            .value_type
            .as_ref()
            .is_none_or(|t| has_sqlite_arithmetic(&t.type_def, op)),
    }
}

/// A type whose own arithmetic is SQLite's operator, such as `smallint`.
fn has_sqlite_arithmetic(type_def: &TypeDef, op: &str) -> bool {
    type_def.arithmetic(op).any(|s| s.func.is_none())
}

/// The call that `signature` makes, or `None` for SQLite's operator. The
/// function must exist: a type may name one that was never defined.
fn call_for(
    signature: &ArithmeticSignature,
    ty: &ColumnCustomType,
    resolver: &Resolver,
) -> Result<Option<ArithmeticCall>> {
    let Some(func) = &signature.func else {
        return Ok(None);
    };
    if resolver.resolve_function(func, 2)?.is_none() {
        crate::bail_parse_error!(
            "function {func} for operator {} on type {} does not exist",
            signature.op,
            ty.type_def.name
        );
    }
    let encode = signature.args.map(|arg| match arg {
        ArithmeticArg::EncodableLiteral if ty.type_def.encode().is_some() => Some(ty.type_name()),
        _ => None,
    });
    let result = signature.result.map(|name| ast::Type {
        name: name.to_string(),
        size: None,
        array_dimensions: 0,
    });
    Ok(Some(ArithmeticCall {
        func: func.clone(),
        encode,
        result,
    }))
}

pub(super) struct ArithmeticCall {
    func: String,
    /// For each operand, the type to encode it to before the call.
    encode: [Option<ast::Type>; 2],
    result: Option<ast::Type>,
}

impl ArithmeticCall {
    fn into_expr(self, operator: ast::Operator, lhs: ast::Expr, rhs: ast::Expr) -> ast::Expr {
        let [encode_lhs, encode_rhs] = self.encode;
        let arg = |expr: ast::Expr, encode: Option<ast::Type>| {
            Box::new(match encode {
                Some(to) => ast::Expr::Convert {
                    expr: Box::new(expr),
                    to: Box::new(to),
                },
                None => expr,
            })
        };
        ast::Expr::TypedCall {
            operator,
            func: ast::Name::from_string(&self.func),
            args: vec![arg(lhs, encode_lhs), arg(rhs, encode_rhs)],
            result: self.result.map(Box::new),
        }
    }
}

/// Emits a call chosen by [resolve_custom_type_operators] and returns the
/// register holding its result.
pub(super) fn emit_typed_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    func_name: &ast::Name,
    args: &[Box<ast::Expr>],
    resolver: &Resolver,
) -> Result<usize> {
    let func = resolver
        .resolve_function(func_name.as_str(), args.len())?
        .ok_or_else(|| {
            LimboError::InternalError(format!("function not found: {}", func_name.as_str()))
        })?;
    let start_reg = if args
        .iter()
        .any(|arg| matches!(**arg, ast::Expr::Convert { .. }))
    {
        // Translate into temporary registers and encode into separate argument
        // registers: a literal may be translated into a preamble register that
        // is reused on every row, so it must not be encoded in place.
        let temps: Vec<usize> = args.iter().map(|_| program.alloc_register()).collect();
        for (arg, &temp) in args.iter().zip(&temps) {
            let value = match &**arg {
                ast::Expr::Convert { expr, .. } => expr,
                other => other,
            };
            translate_expr(program, referenced_tables, value, temp, resolver)?;
        }
        let start_reg = program.alloc_registers(args.len());
        for (i, (arg, &temp)) in args.iter().zip(&temps).enumerate() {
            if !matches!(**arg, ast::Expr::Convert { .. }) {
                program.emit_insn(Insn::Copy {
                    src_reg: temp,
                    dst_reg: start_reg + i,
                    extra_amount: 0,
                });
            }
        }
        for (i, (arg, &temp)) in args.iter().zip(&temps).enumerate() {
            if let ast::Expr::Convert { to, .. } = &**arg {
                emit_encode(program, to, temp, start_reg + i, resolver)?;
            }
        }
        start_reg
    } else {
        let start_reg = program.alloc_registers(args.len());
        for (i, arg) in args.iter().enumerate() {
            translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
        }
        start_reg
    };
    let result_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: result_reg,
        func: FuncCtx {
            func,
            arg_count: args.len(),
        },
    });
    Ok(result_reg)
}

/// Emits a `Convert` found outside a call: the value, then its encoding.
pub(super) fn emit_convert(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    to: &ast::Type,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let value_reg = program.alloc_register();
    translate_expr(program, referenced_tables, expr, value_reg, resolver)?;
    emit_encode(program, to, value_reg, target_register, resolver)
}

fn emit_encode(
    program: &mut ProgramBuilder,
    to: &ast::Type,
    value_reg: usize,
    dest_reg: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let type_def = resolver
        .schema()
        .get_type_def_unchecked(&to.name)
        .ok_or_else(|| LimboError::InternalError(format!("type not found: {}", to.name)))?;
    let encode = type_def.encode().ok_or_else(|| {
        LimboError::InternalError(format!("type {} has no ENCODE", type_def.name))
    })?;
    let params: Vec<Box<ast::Expr>> = match &to.size {
        Some(ast::TypeSize::MaxSize(max)) => vec![max.clone()],
        Some(ast::TypeSize::TypeSize(precision, scale)) => vec![precision.clone(), scale.clone()],
        None => Vec::new(),
    };
    emit_type_expr(
        program, encode, value_reg, dest_reg, &params, type_def, resolver,
    )
}
