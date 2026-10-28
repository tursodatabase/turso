//! Operators on custom types, such as `+`, `&` and `-x`: which function runs
//! them.
//!
//! [find_operator_call] decides. Code generation calls it to emit the call and
//! `type_of` calls it to know the type of the result, so the two always agree.
//! Nothing is written into the expression tree.

use super::*;
use crate::function::Deterministic;
use crate::schema::{OperatorArg, OperatorSignature, BINARY_OPERATORS, PREFIX_OPERATORS};
use crate::translate::expr::expr_type::{
    same_type_def, sqlite_operand_type, type_of, without_wrappers, BuiltinType, ColumnCustomType,
    ExprType, TypeEnv,
};
use crate::Schema;

/// The function that runs the operator expression `expr`, such as `a + b` or
/// `-a`, for code that has not computed the operands' types yet. See
/// [find_operator_call].
pub(crate) fn custom_operator_call(
    expr: &ast::Expr,
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some((_, operands)) = decided_operator(expr, env) else {
        return Ok(None);
    };
    let types: Vec<ExprType> = operands
        .iter()
        .map(|operand| type_of(operand, env))
        .collect();
    find_operator_call(expr, &types, env)
}

/// The function that runs the operator expression `expr` when an operand is a
/// custom-type value. `None` means SQLite's operator runs: `expr` is not an
/// operator a custom type decides, no operand has a custom type, an operand is
/// NULL, or the signature says so (`smallint`, `bigint`). When no signature
/// fits, as in `date + date`, `date * 2` or `-date`, it is an error: running
/// SQLite's operator on the value would give an answer nobody asked for.
///
/// `types` are the types of the operands, in order, which `type_of` passes in so
/// that it computes each operand's type once.
pub(super) fn find_operator_call(
    expr: &ast::Expr,
    types: &[ExprType],
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some((op, operands)) = decided_operator(expr, env) else {
        return Ok(None);
    };
    assert_eq!(operands.len(), types.len(), "one type per operand");
    // An operator on NULL gives NULL whichever function runs it.
    if types.iter().any(|ty| matches!(ty, ExprType::Null)) {
        return Ok(None);
    }
    let operands = operands
        .iter()
        .zip(types)
        .map(|(operand, ty)| Operand::new(operand, ty, env))
        .collect::<Result<Vec<_>>>()?;
    let custom_types: Vec<&ColumnCustomType> = operands
        .iter()
        .filter_map(|operand| operand.value_type.as_ref())
        .collect();
    if custom_types.is_empty() {
        return Ok(None);
    }
    let found = custom_types
        .iter()
        .flat_map(|ty| {
            ty.type_def
                .operator_signatures(op, operands.len())
                .map(move |signature| (signature, *ty))
        })
        .find(|(signature, ty)| accepts(signature, ty, &operands));
    let Some((signature, ty)) = found else {
        match operands.as_slice() {
            [operand] => {
                crate::bail_parse_error!("operator does not exist: {op} {}", operand.type_name())
            }
            [lhs, rhs] => crate::bail_parse_error!(
                "operator does not exist: {} {op} {}",
                lhs.type_name(),
                rhs.type_name()
            ),
            _ => unreachable!("an operator has one or two operands"),
        }
    };
    call_for(signature, ty, env.resolver())
}

/// The symbol and operands of `expr` when it is an operator that custom types
/// decide: `-x` is `-` with one operand, `a & b` is `&` with two. Comparisons
/// use the order of the operands' type (see `comparison_collation`), and unary
/// `+` gives its operand unchanged.
///
/// Without custom types no operand can have one, so plain SQLite does no more
/// work per operator than before custom types existed.
fn decided_operator<'a>(
    expr: &'a ast::Expr,
    env: &TypeEnv,
) -> Option<(&'static str, Vec<&'a ast::Expr>)> {
    if env.schema().type_registry.is_empty() {
        return None;
    }
    let (op, operands, known) = match expr {
        ast::Expr::Binary(lhs, op, rhs) => (
            binary_symbol(op)?,
            vec![lhs.as_ref(), rhs.as_ref()],
            BINARY_OPERATORS.as_slice(),
        ),
        ast::Expr::Unary(op, operand) => (
            prefix_symbol(op)?,
            vec![operand.as_ref()],
            PREFIX_OPERATORS.as_slice(),
        ),
        _ => return None,
    };
    assert!(known.contains(&op), "custom types do not decide {op}");
    Some((op, operands))
}

fn binary_symbol(op: &ast::Operator) -> Option<&'static str> {
    use ast::Operator;
    Some(match op {
        Operator::Add => "+",
        Operator::Subtract => "-",
        Operator::Multiply => "*",
        Operator::Divide => "/",
        Operator::Modulus => "%",
        Operator::BitwiseAnd => "&",
        Operator::BitwiseOr => "|",
        Operator::LeftShift => "<<",
        Operator::RightShift => ">>",
        _ => return None,
    })
}

fn prefix_symbol(op: &ast::UnaryOperator) -> Option<&'static str> {
    match op {
        ast::UnaryOperator::Negative => Some("-"),
        ast::UnaryOperator::BitwiseNot => Some("~"),
        ast::UnaryOperator::Positive | ast::UnaryOperator::Not => None,
    }
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
    /// Whether SQLite's arithmetic would see an integer, or a value whose type
    /// is not known before the statement runs.
    may_be_integer: bool,
    /// Whether the operand is a REAL value. A decimal written in the statement
    /// is not one: as in PostgreSQL, it is an exact number.
    is_real: bool,
    /// Whether the operand is a blob, which is not a number.
    is_blob: bool,
    /// The operand's type, as `type_of` gives it.
    expr_type: &'a ExprType,
}

impl<'a> Operand<'a> {
    fn new(expr: &'a ast::Expr, expr_type: &'a ExprType, env: &TypeEnv) -> Result<Self> {
        let expr = without_wrappers(expr);
        let resolver = env.resolver();
        let column_type = match column_custom_type(expr, env) {
            Some(ty) => without_domains(ty, resolver.schema())?,
            None => None,
        };
        let value_type = match expr_type {
            ExprType::Custom { type_def, params } => without_domains(
                ColumnCustomType {
                    type_def: type_def.clone(),
                    params: params.clone(),
                },
                resolver.schema(),
            )?,
            _ => None,
        };
        let sqlite_type = sqlite_operand_type(expr_type.clone(), resolver.schema());
        let may_be_integer = matches!(
            sqlite_type,
            ExprType::Builtin(BuiltinType::Integer | BuiltinType::Any)
        );
        let is_real = matches!(sqlite_type, ExprType::Builtin(BuiltinType::Real))
            && !matches!(expr_type, ExprType::Builtin(BuiltinType::Decimal));
        let is_blob = matches!(sqlite_type, ExprType::Builtin(BuiltinType::Blob));
        Ok(Self {
            value_type,
            column_type,
            literal_kind: literal_type_name(expr),
            may_be_integer,
            is_real,
            is_blob,
            expr_type,
        })
    }

    /// The operand's type for an error message: `date`, `integer`, ...
    fn type_name(&self) -> String {
        match (&self.value_type, self.expr_type) {
            (Some(ty), _) => ty.type_def.name.clone(),
            (None, ExprType::Builtin(BuiltinType::Any)) => "value".to_string(),
            (None, ExprType::Builtin(BuiltinType::Decimal)) => "numeric".to_string(),
            (None, ExprType::Builtin(builtin)) => format!("{builtin:?}").to_lowercase(),
            (None, ExprType::Custom { type_def, .. }) => type_def.name.clone(),
            (None, ExprType::Untyped) => "text".to_string(),
            (None, ExprType::Null) => "null".to_string(),
            (None, _) => "value".to_string(),
        }
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

/// Whether `signature`, declared by `ty`, accepts every operand.
fn accepts(signature: &OperatorSignature, ty: &ColumnCustomType, operands: &[Operand]) -> bool {
    signature
        .args
        .iter()
        .zip(operands)
        .all(|(arg, operand)| accepts_operand(*arg, signature, ty, operand))
}

fn accepts_operand(
    arg: OperatorArg,
    signature: &OperatorSignature,
    ty: &ColumnCustomType,
    operand: &Operand,
) -> bool {
    let is_this_type =
        |t: Option<&ColumnCustomType>| t.is_some_and(|t| same_type_def(&t.type_def, &ty.type_def));
    match arg {
        OperatorArg::Column => is_this_type(operand.column_type.as_ref()),
        OperatorArg::Value => is_this_type(operand.value_type.as_ref()),
        OperatorArg::EncodableLiteral => {
            operand.value_type.is_none()
                && operand.literal_kind.is_some_and(|kind| {
                    literal_compatible_with_value_type(kind, ty.type_def.value_input_type())
                })
        }
        OperatorArg::PlainValue => is_plain_value(signature, operand),
        OperatorArg::Integer => is_plain_value(signature, operand) && operand.may_be_integer,
        OperatorArg::Exact => {
            is_plain_value(signature, operand) && !operand.is_real && !operand.is_blob
        }
        OperatorArg::Real => is_plain_value(signature, operand) && operand.is_real,
    }
}

fn is_plain_value(signature: &OperatorSignature, operand: &Operand) -> bool {
    operand
        .value_type
        .as_ref()
        .is_none_or(|t| has_sqlite_operator(&t.type_def, signature))
}

/// A type whose only way to run the operator of `signature` is SQLite's, such
/// as `smallint`. numeric runs SQLite's `+` with a REAL, but its own function
/// otherwise, so `smallint + numeric` must not run SQLite's `+`.
fn has_sqlite_operator(type_def: &TypeDef, signature: &OperatorSignature) -> bool {
    let mut signatures = type_def
        .operator_signatures(signature.op, signature.args.len())
        .peekable();
    signatures.peek().is_some() && signatures.all(|s| s.func.is_none())
}

/// The call that `signature` makes, or `None` for SQLite's operator. The
/// function must exist: a type may name one that was never defined.
fn call_for(
    signature: &OperatorSignature,
    ty: &ColumnCustomType,
    resolver: &Resolver,
) -> Result<Option<OperatorCall>> {
    let Some(func) = &signature.func else {
        return Ok(None);
    };
    let Some(function) = resolver.resolve_function(func, signature.args.len())? else {
        crate::bail_parse_error!(
            "function {func} for operator {} on type {} does not exist",
            signature.op,
            ty.type_def.name
        );
    };
    let encode = signature
        .args
        .iter()
        .map(|arg| match arg {
            OperatorArg::EncodableLiteral if ty.type_def.encode().is_some() => Some(ty.type_name()),
            _ => None,
        })
        .collect();
    // Checks such as "is this expression constant" treat `a + b` as SQLite's
    // operator, which gives the same result for the same operands. A type's
    // operator must too. CREATE TYPE already checks its ENCODE.
    if !function.is_deterministic() {
        crate::bail_parse_error!(
            "operator {} on type {} must be deterministic",
            signature.op,
            ty.type_def.name
        );
    }
    let result = signature.result.map(|name| ast::Type {
        name: name.to_string(),
        size: None,
        array_dimensions: 0,
    });
    Ok(Some(OperatorCall {
        func: function,
        encode,
        result,
    }))
}

pub(crate) struct OperatorCall {
    func: Func,
    /// For each operand, the type to encode it to before the call.
    encode: Vec<Option<ast::Type>>,
    /// The type the function returns. `None` when it is not known, as for a
    /// user type's function.
    pub(super) result: Option<ast::Type>,
}

/// Emits `call` on the operands of `expr` and returns the register holding its
/// result.
pub(super) fn emit_operator_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    call: &OperatorCall,
    resolver: &Resolver,
) -> Result<usize> {
    let operands: Vec<&ast::Expr> = match expr {
        ast::Expr::Binary(lhs, _, rhs) => vec![lhs, rhs],
        ast::Expr::Unary(_, operand) => vec![operand],
        _ => unreachable!("a custom type's function only runs operators"),
    };
    assert_eq!(
        operands.len(),
        call.encode.len(),
        "one argument per operand"
    );
    // An operand is translated into its own register and encoded into the
    // argument register: a literal may be translated into a register that is
    // set once and read on every row, so it must not be encoded in place.
    let start_reg = program.alloc_registers(operands.len());
    for (i, (operand, encode)) in operands.into_iter().zip(&call.encode).enumerate() {
        match encode {
            Some(to) => {
                let value_reg = program.alloc_register();
                translate_expr(program, referenced_tables, operand, value_reg, resolver)?;
                emit_encode(program, to, value_reg, start_reg + i, resolver)?;
            }
            None => {
                translate_expr(program, referenced_tables, operand, start_reg + i, resolver)?;
            }
        }
    }
    let result_reg = program.alloc_register();
    emit_function(program, call, start_reg, result_reg);
    Ok(result_reg)
}

/// Emits `call` on operands that are already in registers, one register per
/// operand, and writes the result to `dest`.
pub(crate) fn emit_operator_call_on_registers(
    program: &mut ProgramBuilder,
    call: &OperatorCall,
    operand_regs: &[usize],
    dest: usize,
    resolver: &Resolver,
) -> Result<()> {
    assert_eq!(
        operand_regs.len(),
        call.encode.len(),
        "one argument per operand"
    );
    let start_reg = program.alloc_registers(operand_regs.len());
    for (i, (&operand_reg, encode)) in operand_regs.iter().zip(&call.encode).enumerate() {
        match encode {
            Some(to) => {
                emit_encode(program, to, operand_reg, start_reg + i, resolver)?;
            }
            None => program.emit_insn(Insn::Copy {
                src_reg: operand_reg,
                dst_reg: start_reg + i,
                extra_amount: 0,
            }),
        }
    }
    emit_function(program, call, start_reg, dest);
    Ok(())
}

fn emit_function(program: &mut ProgramBuilder, call: &OperatorCall, start_reg: usize, dest: usize) {
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest,
        func: FuncCtx {
            func: call.func.clone(),
            arg_count: call.encode.len(),
            collation: CollationSeq::BINARY,
        },
    });
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
