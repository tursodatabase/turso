//! Arithmetic and comparison operators on custom types: which function runs
//! them.
//!
//! [custom_arithmetic_call] and [custom_comparison_call] decide, from the
//! operands' types and the signatures their types declare. Code generation,
//! `type_of`, ORDER BY and the optimizer all ask them, so they always agree.
//! Nothing is written into the expression tree.

use super::*;
use crate::function::Deterministic;
use crate::schema::{OperatorArg, OperatorSignature, COMPARISON_OPERATORS};
use crate::translate::expr::expr_type::{
    same_type_def, sqlite_operand_type, type_of, without_wrappers, BuiltinType, ColumnCustomType,
    ExprType, TypeEnv,
};
use crate::Schema;

/// The function that runs `lhs op rhs`, for code that has not computed the
/// operands' types yet. See [find_arithmetic_call].
pub(crate) fn custom_arithmetic_call(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    if !may_run_custom_function(op, env) {
        return Ok(None);
    }
    let lhs_type = type_of(lhs, env);
    let rhs_type = type_of(rhs, env);
    find_arithmetic_call(lhs, op, rhs, [&lhs_type, &rhs_type], env)
}

/// The function that runs the comparison `lhs op rhs`. `None` means SQLite's
/// comparison runs: no operand has a custom type, an operand is NULL, or no
/// signature fits, as in `n = date_col`.
///
/// A comparison the type does not declare is made from `<` and `=`: `a > b` is
/// `b < a`, `a >= b` is `NOT a < b`, `a <= b` is `NOT b < a`, and `a != b` is
/// `NOT a = b`.
///
/// Comparisons are not written into the tree, because index seeks look for a
/// plain comparison there; the optimizer asks [runs_custom_type_function] to
/// skip the ones a function runs.
pub(crate) fn custom_comparison_call(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    if !may_run_custom_function(op, env) {
        return Ok(None);
    }
    let Some(op_str) = operator_to_str(op).filter(|op| COMPARISON_OPERATORS.contains(op)) else {
        return Ok(None);
    };
    let lhs_type = type_of(lhs, env);
    let rhs_type = type_of(rhs, env);
    if matches!(lhs_type, ExprType::Null) || matches!(rhs_type, ExprType::Null) {
        return Ok(None);
    }
    let operands = [
        Operand::new(lhs, &lhs_type, env)?,
        Operand::new(rhs, &rhs_type, env)?,
    ];
    for &(signature_op, swap_args, negate) in comparison_forms(op_str) {
        let args = if swap_args {
            [&operands[1], &operands[0]]
        } else {
            [&operands[0], &operands[1]]
        };
        if let Some((signature, ty)) = find_signature(signature_op, args) {
            let Some(call) = call_for(signature, ty, env.resolver())? else {
                return Ok(None);
            };
            return Ok(Some(OperatorCall {
                swap_args,
                negate,
                ..call
            }));
        }
    }
    Ok(None)
}

/// The ways to run comparison `op`: the operator itself, then made from `<` or
/// `=` as (operator, swap the arguments, negate the result).
fn comparison_forms(op: &str) -> &'static [(&'static str, bool, bool)] {
    match op {
        "<" => &[("<", false, false)],
        "=" => &[("=", false, false)],
        ">" => &[(">", false, false), ("<", true, false)],
        ">=" => &[(">=", false, false), ("<", false, true)],
        "<=" => &[("<=", false, false), ("<", true, true)],
        "!=" => &[("!=", false, false), ("=", false, true)],
        _ => unreachable!("{op} is not a comparison"),
    }
}

/// The function that runs `lhs op rhs` when an operand is a custom-type value.
/// `None` means SQLite's operator runs: no operand has a custom type, an
/// operand is NULL, or the signature says so (`smallint`, `bigint`). When no
/// signature fits, as in `date + date` or `date * 2`, it is an error: running
/// SQLite's operator on the value would give an answer nobody asked for.
///
/// `types` are the types of `lhs` and `rhs`, which `type_of` passes in so that it
/// computes each operand's type once.
pub(super) fn find_arithmetic_call(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    types: [&ExprType; 2],
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    if !may_run_custom_function(op, env) || !is_arithmetic(op) {
        return Ok(None);
    }
    let Some(op_str) = operator_to_str(op) else {
        return Ok(None);
    };
    let [lhs_type, rhs_type] = types;
    // Arithmetic with NULL is NULL whichever function runs it.
    if matches!(lhs_type, ExprType::Null) || matches!(rhs_type, ExprType::Null) {
        return Ok(None);
    }
    let operands = [
        Operand::new(lhs, lhs_type, env)?,
        Operand::new(rhs, rhs_type, env)?,
    ];
    if operands.iter().all(|o| o.value_type.is_none()) {
        return Ok(None);
    }
    let Some((signature, ty)) = find_signature(op_str, [&operands[0], &operands[1]]) else {
        crate::bail_parse_error!(
            "operator does not exist: {} {op_str} {}",
            operands[0].type_name(),
            operands[1].type_name()
        );
    };
    call_for(signature, ty, env.resolver())
}

/// The first signature for `op`, declared by either operand's custom type, that
/// accepts both operands.
fn find_signature<'a>(
    op: &'a str,
    operands: [&'a Operand; 2],
) -> Option<(&'a OperatorSignature, &'a ColumnCustomType)> {
    operands
        .into_iter()
        .filter_map(|o| o.value_type.as_ref())
        .flat_map(|ty| ty.type_def.signatures(op).map(move |s| (s, ty)))
        .find(|(s, ty)| accepts(s, ty, operands))
}

/// Without custom types no operand can have one, so plain SQLite does no more
/// work per operator than before custom types existed.
fn may_run_custom_function(op: &ast::Operator, env: &TypeEnv) -> bool {
    (is_arithmetic(op) || op.is_comparison()) && !env.schema().type_registry.is_empty()
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
    /// Whether SQLite's arithmetic would see an integer, or a value whose type
    /// is not known before the statement runs.
    may_be_integer: bool,
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
        let may_be_integer = matches!(
            sqlite_operand_type(expr_type.clone(), resolver.schema()),
            ExprType::Builtin(BuiltinType::Integer | BuiltinType::Any)
        );
        Ok(Self {
            value_type,
            column_type,
            literal_kind: literal_type_name(expr),
            may_be_integer,
            expr_type,
        })
    }

    /// The operand's type for an error message: `date`, `integer`, ...
    fn type_name(&self) -> String {
        match (&self.value_type, self.expr_type) {
            (Some(ty), _) => ty.type_def.name.clone(),
            (None, ExprType::Builtin(BuiltinType::Any)) => "value".to_string(),
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

/// Whether `signature`, declared by `ty`, accepts both operands.
fn accepts(signature: &OperatorSignature, ty: &ColumnCustomType, operands: [&Operand; 2]) -> bool {
    signature
        .args
        .iter()
        .zip(operands)
        .all(|(arg, operand)| accepts_operand(*arg, signature.op, ty, operand))
}

fn accepts_operand(arg: OperatorArg, op: &str, ty: &ColumnCustomType, operand: &Operand) -> bool {
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
        OperatorArg::PlainValue => is_plain_value(op, operand),
        OperatorArg::Integer => is_plain_value(op, operand) && operand.may_be_integer,
    }
}

fn is_plain_value(op: &str, operand: &Operand) -> bool {
    operand
        .value_type
        .as_ref()
        .is_none_or(|t| has_sqlite_operator(&t.type_def, op))
}

/// A type whose own `op` is SQLite's operator, such as `smallint`.
fn has_sqlite_operator(type_def: &TypeDef, op: &str) -> bool {
    type_def.signatures(op).any(|s| s.func.is_none())
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
    let Some(function) = resolver.resolve_function(func, 2)? else {
        crate::bail_parse_error!(
            "function {func} for operator {} on type {} does not exist",
            signature.op,
            ty.type_def.name
        );
    };
    let encode = signature.args.map(|arg| match arg {
        OperatorArg::EncodableLiteral if ty.type_def.encode().is_some() => Some(ty.type_name()),
        _ => None,
    });
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
        func_name: func.clone(),
        encode,
        result,
        swap_args: false,
        negate: false,
    }))
}

pub(crate) struct OperatorCall {
    func: Func,
    /// The name the type's signature gives the function.
    func_name: String,
    /// For each argument, the type to encode it to before the call.
    encode: [Option<ast::Type>; 2],
    /// The type the function returns. `None` when it is not known, as for a
    /// user type's function.
    pub(super) result: Option<ast::Type>,
    /// The function is called as `func(rhs, lhs)`.
    swap_args: bool,
    /// The result is negated: `a >= b` runs `NOT lt(a, b)`.
    negate: bool,
}

impl OperatorCall {
    /// The function behind a comparison, for the sorter: `a < b` that runs
    /// `lt(a, b)` as it is, neither swapped nor negated.
    pub(crate) fn less_than_function(&self) -> Option<&str> {
        (!self.swap_args && !self.negate).then_some(self.func_name.as_str())
    }
}

/// Emits `call` on `lhs` and `rhs` and returns the register holding its result.
pub(super) fn emit_operator_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    rhs: &ast::Expr,
    call: &OperatorCall,
    resolver: &Resolver,
) -> Result<usize> {
    let args = if call.swap_args {
        [rhs, lhs]
    } else {
        [lhs, rhs]
    };
    // An argument is translated into its own register and encoded into the
    // argument register: a literal may be translated into a register that is
    // set once and read on every row, so it must not be encoded in place.
    let start_reg = program.alloc_registers(2);
    for (i, (operand, encode)) in args.into_iter().zip(&call.encode).enumerate() {
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
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: result_reg,
        func: FuncCtx {
            func: call.func.clone(),
            arg_count: 2,
        },
    });
    if call.negate {
        program.emit_insn(Insn::Not {
            reg: result_reg,
            dest: result_reg,
        });
    }
    Ok(result_reg)
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
