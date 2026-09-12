//! The functions that rules call.
//!
//! A rule names a function where a pattern is not enough, as in
//! `(IsConst $x)` or `(FoldBinary (OpName) $left $right)`. `lookup` maps the
//! name to the Rust function. A function that gives several results, for a
//! `Let`, returns `Value::Tuple`.

use std::borrow::Cow;

use smallvec::SmallVec;

use turso_parser::ast::{
    blob_literal_hex, Expr, Literal, Operator, TableInternalId, Type, UnaryOperator,
};

use crate::function::{Deterministic, Func};
use crate::numeric::Numeric;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, expression_can_fail_on_input, sanitize_string,
    walk_expr, WalkControl,
};
use crate::translate::logical::walk::leaf_ids;
use crate::translate::logical::LogicalPlan;
use crate::translate::plan::{JoinType, WhereTerm};
use crate::types::{TextSubtype, Value as SqlValue};
use crate::util::{exprs_are_equivalent, parse_numeric_literal};
use crate::{LimboError, Result};

use super::engine::{ArgRef, CustomFn, EngineContext};
use super::nodes::{self, expr_value, strip_parens, Context, NodeRef, Op, PrivateRef, Value};

pub(crate) fn lookup(name: &str) -> Option<CustomFn> {
    Some(match name {
        "IsConst" => is_const,
        "IsConstOrAbsent" => is_const_or_absent,
        "IsTruthy" => is_truthy,
        "IsFalsy" => is_falsy,
        "IsFalsyOrNull" => is_falsy_or_null,
        "IsFalsyOrNullOrAbsent" => is_falsy_or_null_or_absent,
        "IsNeverNull" => is_never_null,
        "IsDeterministic" => is_deterministic,
        "CanFail" => can_fail,
        "VarsAreSame" => vars_are_same,
        "FoldBinary" => fold_binary,
        "FoldUnary" => fold_unary,
        "FoldComparison" => fold_comparison,
        "FoldCast" => fold_cast,
        "CanNegateComparison" => can_negate_comparison,
        "NegateComparison" => negate_comparison,
        "CommuteInequality" => commute_inequality,
        "ConcatLeftDeepAnds" => concat_left_deep_ands,
        "FindRedundantConjunct" => find_redundant_conjunct,
        "ExtractRedundantConjunct" => extract_redundant_conjunct,
        "SimplifyCoalesce" => simplify_coalesce,
        "CollapseRepeatedLikePatternWildcards" => collapse_repeated_like_pattern_wildcards,
        "NormalizeTupleEquality" => normalize_tuple_equality,
        "NeedSortedUniqueList" => need_sorted_unique_list,
        "ConstructSortedUniqueList" => construct_sorted_unique_list,
        "SimplifyWhens" => simplify_whens,
        "LenGT" => len_gt,
        "ConstStringEquals" => const_string_equals,
        "DropLast" => drop_last,
        "ElseOrNull" => else_or_null,
        "CanSimplifyTerm" => can_simplify_term,
        "SimplifyTerms" => simplify_terms,
        "IsFilterFalse" => is_filter_false,
        "HasDuplicateTerms" => has_duplicate_terms,
        "DeduplicateTerms" => deduplicate_terms,
        "ConcatTerms" => concat_terms,
        "IsNotNullColumn" => is_not_null_column,
        "IsPlainTerm" => is_plain_term,
        "RemoveTerm" => remove_term,
        _ => return None,
    })
}

fn error(text: String) -> LimboError {
    LimboError::InternalError(format!("logical plan rules: {text}"))
}

fn expr_arg<'a>(args: &'a [ArgRef<'_, '_>], index: usize, function: &str) -> Result<&'a Expr> {
    args.get(index)
        .and_then(ArgRef::expr)
        .ok_or_else(|| error(format!("{function}: argument {index} is not an expression")))
}

fn op_arg(args: &[ArgRef<'_, '_>], index: usize, function: &str) -> Result<Op> {
    args.get(index)
        .and_then(ArgRef::op)
        .ok_or_else(|| error(format!("{function}: argument {index} is not an operator")))
}

fn exprs_arg<'a>(
    args: &'a [ArgRef<'_, '_>],
    index: usize,
    function: &str,
) -> Result<Vec<&'a Expr>> {
    args.get(index)
        .and_then(ArgRef::exprs)
        .ok_or_else(|| error(format!("{function}: argument {index} is not a list")))
}

fn terms_arg<'a>(
    args: &'a [ArgRef<'_, '_>],
    index: usize,
    function: &str,
) -> Result<Vec<&'a WhereTerm>> {
    args.get(index).and_then(ArgRef::terms).ok_or_else(|| {
        error(format!(
            "{function}: argument {index} is not a list of terms"
        ))
    })
}

fn whens_arg<'a>(
    args: &'a [ArgRef<'_, '_>],
    index: usize,
    function: &str,
) -> Result<Vec<(&'a Expr, &'a Expr)>> {
    args.get(index).and_then(ArgRef::whens).ok_or_else(|| {
        error(format!(
            "{function}: argument {index} is not a list of branches"
        ))
    })
}

fn found(value: Expr) -> Value {
    Value::Tuple(vec![expr_value(value), Value::Bool(true)])
}

fn not_found() -> Value {
    Value::Tuple(vec![Value::Absent, Value::Bool(false)])
}

/// The value of a constant expression: a literal, or a number with a sign.
pub(crate) fn const_value(expr: &Expr) -> Option<SqlValue> {
    match strip_parens(expr) {
        Expr::Literal(literal) => literal_value(literal),
        Expr::Unary(UnaryOperator::Negative, inner) => match strip_parens(inner) {
            Expr::Literal(Literal::Numeric(text)) => {
                parse_numeric_literal(&format!("-{text}")).ok()
            }
            _ => None,
        },
        _ => None,
    }
}

fn literal_value(literal: &Literal) -> Option<SqlValue> {
    match literal {
        Literal::Numeric(text) => parse_numeric_literal(text).ok(),
        Literal::String(text) => Some(SqlValue::build_text(sanitize_string(text))),
        Literal::Blob(text) => {
            let bytes: Option<Vec<u8>> = blob_literal_hex(text)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    std::str::from_utf8(pair)
                        .ok()
                        .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                })
                .collect();
            SqlValue::from_slice(&bytes?).ok()
        }
        Literal::True => Some(SqlValue::from_i64(1)),
        Literal::False => Some(SqlValue::from_i64(0)),
        Literal::Null => Some(SqlValue::Null),
        Literal::Keyword(_)
        | Literal::CurrentDate
        | Literal::CurrentTime
        | Literal::CurrentTimestamp => None,
    }
}

/// A literal expression with this value, when one can be written.
pub(crate) fn value_expr(value: &SqlValue) -> Option<Expr> {
    Some(match value {
        SqlValue::Null => Expr::Literal(Literal::Null),
        SqlValue::Numeric(Numeric::Integer(value)) => integer_literal(*value),
        SqlValue::Numeric(Numeric::Float(value)) => {
            let value: f64 = (*value).into();
            if !value.is_finite() {
                return None;
            }
            let text = format!("{:?}", value.abs());
            if value.is_sign_negative() {
                Expr::Unary(
                    UnaryOperator::Negative,
                    Box::new(Expr::Literal(Literal::Numeric(text))),
                )
            } else {
                Expr::Literal(Literal::Numeric(text))
            }
        }
        SqlValue::Text(text) => {
            if !matches!(text.subtype, TextSubtype::Text) {
                return None;
            }
            Expr::Literal(Literal::String(format!(
                "'{}'",
                text.as_str().replace('\'', "''")
            )))
        }
        SqlValue::Blob(blob) => {
            let hex: String = blob.iter().map(|byte| format!("{byte:02x}")).collect();
            Expr::Literal(Literal::Blob(format!("x'{hex}'")))
        }
    })
}

fn integer_literal(value: i64) -> Expr {
    if value < 0 && value != i64::MIN {
        return Expr::Unary(
            UnaryOperator::Negative,
            Box::new(Expr::Literal(Literal::Numeric((-value).to_string()))),
        );
    }
    Expr::Literal(Literal::Numeric(value.to_string()))
}

/// The truth value of a constant. `NULL` counts as false. `None` when the
/// expression is not a constant.
pub(crate) fn constant_truth(expr: &Expr) -> Option<bool> {
    match strip_parens(expr) {
        Expr::Literal(Literal::Numeric(text)) => numeric_truth(text),
        Expr::Literal(Literal::String(text)) => {
            Some(Numeric::from(unquoted(text).as_ref()).to_bool())
        }
        Expr::Unary(UnaryOperator::Negative, inner) => match strip_parens(inner) {
            Expr::Literal(Literal::Numeric(text)) => numeric_truth(text),
            _ => None,
        },
        other => {
            let value = const_value(other)?;
            Some(Numeric::from_value(&value).is_some_and(|number| number.to_bool()))
        }
    }
}

fn numeric_truth(text: &str) -> Option<bool> {
    let value = parse_numeric_literal(text).ok()?;
    Some(Numeric::from_value(&value).is_some_and(|number| number.to_bool()))
}

/// The text of a string literal without its quotes. Borrowed unless the
/// text has a quote in it.
fn unquoted(text: &str) -> Cow<'_, str> {
    let inner = &text[1..text.len() - 1];
    if inner.contains("''") {
        Cow::Owned(inner.replace("''", "'"))
    } else {
        Cow::Borrowed(inner)
    }
}

fn is_null(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::Literal(Literal::Null))
}

pub(crate) fn node_is_string(node: NodeRef<'_>, text: &str) -> bool {
    match node {
        NodeRef::Private(PrivateRef::Function(function)) => {
            function.name.as_str().eq_ignore_ascii_case(text)
        }
        NodeRef::Private(PrivateRef::Name(name)) => name.as_str().eq_ignore_ascii_case(text),
        NodeRef::Expr(expr) | NodeRef::Private(PrivateRef::Expr(expr)) => matches!(
            strip_parens(expr),
            Expr::Literal(Literal::String(literal)) if unquoted(literal) == text
        ),
        _ => false,
    }
}

pub(crate) fn node_is_integer(node: NodeRef<'_>, value: i64) -> bool {
    match node {
        NodeRef::Expr(expr) | NodeRef::Private(PrivateRef::Expr(expr)) => {
            match strip_parens(expr) {
                Expr::Literal(Literal::Numeric(_) | Literal::True | Literal::False)
                | Expr::Unary(UnaryOperator::Negative, _) => matches!(
                    const_value(expr),
                    Some(SqlValue::Numeric(Numeric::Integer(found))) if found == value
                ),
                _ => false,
            }
        }
        _ => false,
    }
}

fn is_const(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsConst")?;
    Ok(Value::Bool(is_constant(expr)))
}

/// Whether `const_value` gives a value, without building it.
pub(crate) fn is_constant(expr: &Expr) -> bool {
    match strip_parens(expr) {
        Expr::Literal(literal) => match literal {
            Literal::Numeric(text) => parse_numeric_literal(text).is_ok(),
            Literal::String(_) | Literal::True | Literal::False | Literal::Null => true,
            Literal::Blob(text) => {
                let hex = blob_literal_hex(text);
                hex.len() % 2 == 0 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            }
            Literal::Keyword(_)
            | Literal::CurrentDate
            | Literal::CurrentTime
            | Literal::CurrentTimestamp => false,
        },
        Expr::Unary(UnaryOperator::Negative, inner) => matches!(
            strip_parens(inner),
            Expr::Literal(Literal::Numeric(text)) if parse_numeric_literal(text).is_ok()
        ),
        _ => false,
    }
}

fn is_const_or_absent(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    if args.first().is_some_and(ArgRef::is_absent) {
        return Ok(Value::Bool(true));
    }
    is_const(ctx, args, context)
}

fn is_truthy(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsTruthy")?;
    Ok(Value::Bool(
        !is_null(expr) && constant_truth(expr) == Some(true),
    ))
}

/// A constant other than `NULL` whose truth value is false.
fn is_falsy(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsFalsy")?;
    Ok(Value::Bool(
        !is_null(expr) && constant_truth(expr) == Some(false),
    ))
}

fn is_falsy_or_null(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsFalsyOrNull")?;
    Ok(Value::Bool(constant_truth(expr) == Some(false)))
}

fn is_falsy_or_null_or_absent(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    if args.first().is_some_and(ArgRef::is_absent) {
        return Ok(Value::Bool(true));
    }
    is_falsy_or_null(ctx, args, context)
}

fn is_never_null(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsNeverNull")?;
    Ok(Value::Bool(!is_null(expr) && const_value(expr).is_some()))
}

/// Whether the expression gives the same value each time it runs, so a rule
/// can copy it.
fn is_deterministic(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let expr = expr_arg(args, 0, "IsDeterministic")?;
    let Some(resolver) = ctx.resolver else {
        return Ok(Value::Bool(builtin_functions_are_deterministic(expr)?));
    };
    Ok(Value::Bool(
        !expr_contains_nondeterministic_scalar_function(expr, resolver)?,
    ))
}

/// Without a resolver only the built-in functions are known, so a call of
/// another function counts as non-deterministic.
fn builtin_functions_are_deterministic(expr: &Expr) -> Result<bool> {
    let mut deterministic = true;
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        let func = match expr {
            Expr::FunctionCall { name, args, .. } => {
                Func::resolve_function(name.as_str(), args.len())?
            }
            Expr::FunctionCallStar { name, .. } => Func::resolve_function(name.as_str(), 0)?,
            _ => return Ok(WalkControl::Continue),
        };
        match func {
            Some(Func::Agg(_) | Func::Window(_)) => Ok(WalkControl::Continue),
            Some(func) if func.is_deterministic() => Ok(WalkControl::Continue),
            _ => {
                deterministic = false;
                Ok(WalkControl::SkipChildren)
            }
        }
    })?;
    Ok(deterministic)
}

fn can_fail(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let expr = expr_arg(args, 0, "CanFail")?;
    Ok(Value::Bool(expression_can_fail_on_input(expr)))
}

fn vars_are_same(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let left = expr_arg(args, 0, "VarsAreSame")?;
    let right = expr_arg(args, 1, "VarsAreSame")?;
    Ok(Value::Bool(strip_parens(left) == strip_parens(right)))
}

fn fold_binary(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let op = op_arg(args, 0, "FoldBinary")?;
    let left = expr_arg(args, 1, "FoldBinary")?;
    let right = expr_arg(args, 2, "FoldBinary")?;
    let (Some(left), Some(right)) = (const_value(left), const_value(right)) else {
        return Ok(not_found());
    };
    let value = match op {
        Op::Plus => left.exec_add(&right),
        Op::Minus => left.exec_subtract(&right),
        Op::Mult => left.exec_multiply(&right),
        Op::Div => left.exec_divide(&right),
        Op::Mod => left.exec_remainder(&right),
        Op::BitAnd => left.exec_bit_and(&right),
        Op::BitOr => left.exec_bit_or(&right),
        Op::LShift => left.exec_shift_left(&right),
        Op::RShift => left.exec_shift_right(&right),
        Op::Concat => match left.exec_concat(&right) {
            Ok(value) => value,
            Err(_) => return Ok(not_found()),
        },
        _ => return Ok(not_found()),
    };
    Ok(value_expr(&value).map(found).unwrap_or_else(not_found))
}

fn fold_unary(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let op = op_arg(args, 0, "FoldUnary")?;
    let input = expr_arg(args, 1, "FoldUnary")?;
    let Some(input) = const_value(input) else {
        return Ok(not_found());
    };
    let value = match op {
        Op::UnaryMinus => SqlValue::from_i64(0).exec_subtract(&input),
        Op::UnaryPlus => input,
        Op::BitNot => input.exec_bit_not(),
        Op::Not => input.exec_boolean_not(),
        _ => return Ok(not_found()),
    };
    Ok(value_expr(&value).map(found).unwrap_or_else(not_found))
}

fn fold_comparison(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let op = op_arg(args, 0, "FoldComparison")?;
    let left = expr_arg(args, 1, "FoldComparison")?;
    let right = expr_arg(args, 2, "FoldComparison")?;
    let (Some(left), Some(right)) = (const_value(left), const_value(right)) else {
        return Ok(not_found());
    };
    let order = left.cmp(&right);
    let value = match op {
        Op::Is => SqlValue::from_i64(order.is_eq() as i64),
        Op::IsNot => SqlValue::from_i64(!order.is_eq() as i64),
        Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge => {
            if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                SqlValue::Null
            } else {
                let holds = match op {
                    Op::Eq => order.is_eq(),
                    Op::Ne => order.is_ne(),
                    Op::Lt => order.is_lt(),
                    Op::Le => order.is_le(),
                    Op::Gt => order.is_gt(),
                    _ => order.is_ge(),
                };
                SqlValue::from_i64(holds as i64)
            }
        }
        _ => return Ok(not_found()),
    };
    Ok(value_expr(&value).map(found).unwrap_or_else(not_found))
}

const FOLDABLE_CAST_TYPES: &[&str] = &["integer", "int", "real", "text", "blob", "numeric"];

fn fold_cast(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let input = expr_arg(args, 0, "FoldCast")?;
    let Some(type_name) = args.get(1).and_then(ArgRef::type_name) else {
        return Ok(not_found());
    };
    if !cast_can_fold(type_name) {
        return Ok(not_found());
    }
    let Some(input) = const_value(input) else {
        return Ok(not_found());
    };
    let Ok(value) = input.exec_cast(&type_name.name) else {
        return Ok(not_found());
    };
    Ok(value_expr(&value).map(found).unwrap_or_else(not_found))
}

fn cast_can_fold(type_name: &Type) -> bool {
    type_name.array_dimensions == 0
        && type_name.size.is_none()
        && FOLDABLE_CAST_TYPES
            .iter()
            .any(|known| type_name.name.eq_ignore_ascii_case(known))
}

fn negated_comparison(op: Op) -> Option<Op> {
    Some(match op {
        Op::Eq => Op::Ne,
        Op::Ne => Op::Eq,
        Op::Lt => Op::Ge,
        Op::Ge => Op::Lt,
        Op::Le => Op::Gt,
        Op::Gt => Op::Le,
        Op::Is => Op::IsNot,
        Op::IsNot => Op::Is,
        _ => return None,
    })
}

fn can_negate_comparison(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let op = op_arg(args, 0, "CanNegateComparison")?;
    Ok(Value::Bool(negated_comparison(op).is_some()))
}

fn negate_comparison(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let op = op_arg(args, 0, "NegateComparison")?;
    let left = expr_arg(args, 1, "NegateComparison")?.clone();
    let right = expr_arg(args, 2, "NegateComparison")?.clone();
    let negated = negated_comparison(op)
        .ok_or_else(|| error(format!("NegateComparison: {} cannot be negated", op.name())))?;
    settled(
        ctx,
        nodes::construct(negated, vec![expr_value(left), expr_value(right)])?,
        context,
    )
}

/// Apply the rules to an expression that a function built from normalized
/// parts.
fn settled(ctx: &EngineContext<'_, '_>, value: Value, context: Context) -> Result<Value> {
    match value {
        Value::Expr(mut expr) => {
            ctx.settle(&mut expr, context)?;
            Ok(Value::Expr(expr))
        }
        other => Ok(other),
    }
}

fn commute_inequality(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let op = op_arg(args, 0, "CommuteInequality")?;
    let left = expr_arg(args, 1, "CommuteInequality")?.clone();
    let right = expr_arg(args, 2, "CommuteInequality")?.clone();
    let commuted = match op {
        Op::Lt => Op::Gt,
        Op::Gt => Op::Lt,
        Op::Le => Op::Ge,
        Op::Ge => Op::Le,
        other => {
            return Err(error(format!(
                "CommuteInequality: {} is not an inequality",
                other.name()
            )))
        }
    };
    settled(
        ctx,
        nodes::construct(commuted, vec![expr_value(right), expr_value(left)])?,
        context,
    )
}

fn and_parts(expr: &Expr) -> Option<(&Expr, &Expr)> {
    match strip_parens(expr) {
        Expr::Binary(left, Operator::And, right) => Some((left, right)),
        _ => None,
    }
}

fn concat_left_deep_ands(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let left = expr_arg(args, 0, "ConcatLeftDeepAnds")?.clone();
    let right = expr_arg(args, 1, "ConcatLeftDeepAnds")?;
    let mut items = vec![left];
    items.extend(conjuncts(right).into_iter().cloned());
    Ok(expr_value(and_chain(ctx, items, context)?))
}

/// The conjuncts of a left-deep `AND` tree, in order, without parentheses.
fn conjuncts(expr: &Expr) -> SmallVec<[&Expr; 8]> {
    fn collect<'e>(expr: &'e Expr, out: &mut SmallVec<[&'e Expr; 8]>) {
        match and_parts(expr) {
            Some((left, right)) => {
                collect(left, out);
                collect(right, out);
            }
            None => out.push(strip_parens(expr)),
        }
    }
    let mut out = SmallVec::new();
    collect(expr, &mut out);
    out
}

/// A left-deep `AND` tree of the items, with the rules applied to each new
/// node. The top node stands in `context`; the nodes below it are AND
/// operands.
fn and_chain(ctx: &EngineContext<'_, '_>, items: Vec<Expr>, context: Context) -> Result<Expr> {
    let count = items.len();
    let mut items = items.into_iter();
    let mut chain = items
        .next()
        .ok_or_else(|| error("an AND chain needs at least one item".to_string()))?;
    for (index, next) in items.enumerate() {
        let node_context = if index + 2 == count {
            context
        } else {
            context.under_and_or()
        };
        chain = ctx.and(chain, next, node_context)?;
    }
    Ok(chain)
}

/// Whether `candidate` is one of the conjuncts.
fn is_conjunct(candidate: &Expr, conjuncts: &[&Expr]) -> bool {
    conjuncts
        .iter()
        .any(|conjunct| exprs_are_equivalent(conjunct, candidate))
}

/// The conjuncts that both sides of an OR have, as one left-deep AND tree
/// in the order of the left side.
fn find_redundant_conjunct(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let left = expr_arg(args, 0, "FindRedundantConjunct")?;
    let right = expr_arg(args, 1, "FindRedundantConjunct")?;
    let right_conjuncts = conjuncts(right);
    let shared: Vec<Expr> = conjuncts(left)
        .into_iter()
        .filter(|candidate| is_conjunct(candidate, &right_conjuncts))
        .cloned()
        .collect();
    if shared.is_empty() {
        return Ok(not_found());
    }
    Ok(found(and_chain(ctx, shared, context.under_and_or())?))
}

/// `shared AND (left' OR right')`, where `left'` and `right'` are the sides
/// without the shared conjuncts. A side that has nothing else makes the OR
/// true when the shared conjuncts hold, so the result is then the shared
/// conjuncts alone.
fn extract_redundant_conjunct(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let shared = expr_arg(args, 0, "ExtractRedundantConjunct")?;
    let left = expr_arg(args, 1, "ExtractRedundantConjunct")?;
    let right = expr_arg(args, 2, "ExtractRedundantConjunct")?;
    let shared_conjuncts = conjuncts(shared);
    let operand_context = context.under_and_or();
    let rest_left = remove_conjuncts(ctx, &shared_conjuncts, left, operand_context)?;
    let rest_right = remove_conjuncts(ctx, &shared_conjuncts, right, operand_context)?;
    match (rest_left, rest_right) {
        (Some(rest_left), Some(rest_right)) => {
            let or = ctx.or(rest_left, rest_right, operand_context)?;
            Ok(expr_value(ctx.and(shared.clone(), or, context)?))
        }
        _ => Ok(expr_value(shared.clone())),
    }
}

/// The conjunction without the shared conjuncts, or nothing when every
/// conjunct is shared.
fn remove_conjuncts(
    ctx: &EngineContext<'_, '_>,
    shared: &[&Expr],
    conjunction: &Expr,
    context: Context,
) -> Result<Option<Expr>> {
    let rest: Vec<Expr> = conjuncts(conjunction)
        .into_iter()
        .filter(|conjunct| !is_conjunct(conjunct, shared))
        .cloned()
        .collect();
    if rest.is_empty() {
        return Ok(None);
    }
    Ok(Some(and_chain(ctx, rest, context)?))
}

fn simplify_coalesce(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let exprs = exprs_arg(args, 0, "SimplifyCoalesce")?;
    for (index, expr) in exprs.iter().enumerate().take(exprs.len().saturating_sub(1)) {
        if const_value(expr).is_none() {
            let rest: Vec<Value> = exprs[index..]
                .iter()
                .map(|expr| expr_value((*expr).clone()))
                .collect();
            return nodes::construct(Op::Coalesce, vec![Value::List(rest)]);
        }
        if !is_null(expr) {
            return Ok(expr_value((*expr).clone()));
        }
    }
    let last = exprs
        .last()
        .ok_or_else(|| error("SimplifyCoalesce: no arguments".to_string()))?;
    Ok(expr_value((*last).clone()))
}

fn collapse_repeated_like_pattern_wildcards(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let pattern = expr_arg(args, 0, "CollapseRepeatedLikePatternWildcards")?;
    let Some(SqlValue::Text(text)) = const_value(pattern) else {
        return Ok(not_found());
    };
    let mut collapsed = String::with_capacity(text.as_str().len());
    let mut previous_was_wildcard = false;
    for ch in text.as_str().chars() {
        if ch == '%' && previous_was_wildcard {
            continue;
        }
        previous_was_wildcard = ch == '%';
        collapsed.push(ch);
    }
    if collapsed.len() == text.as_str().len() {
        return Ok(not_found());
    }
    match value_expr(&SqlValue::build_text(collapsed)) {
        Some(expr) => Ok(found(expr)),
        None => Ok(not_found()),
    }
}

fn normalize_tuple_equality(
    ctx: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    context: Context,
) -> Result<Value> {
    let left = exprs_arg(args, 0, "NormalizeTupleEquality")?;
    let right = exprs_arg(args, 1, "NormalizeTupleEquality")?;
    if left.is_empty() || left.len() != right.len() {
        return Ok(not_found());
    }
    let mut pairs = Vec::with_capacity(left.len());
    for (left, right) in left.iter().zip(right) {
        let mut pair = Expr::Binary(
            Box::new((*left).clone()),
            Operator::Equals,
            Box::new(right.clone()),
        );
        ctx.settle(&mut pair, context.under_and_or())?;
        pairs.push(pair);
    }
    Ok(found(and_chain(ctx, pairs, context)?))
}

/// Whether an IN list of constants has duplicates or is not sorted.
fn need_sorted_unique_list(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let exprs = exprs_arg(args, 0, "NeedSortedUniqueList")?;
    for pair in exprs.windows(2) {
        match pair_in_order(pair[0], pair[1]) {
            Some(true) => {}
            Some(false) => return Ok(Value::Bool(true)),
            None => return Ok(Value::Bool(false)),
        }
    }
    Ok(Value::Bool(false))
}

/// Whether `left` sorts before `right` and is not the same constant, or
/// nothing when one of them is not a constant. Two string literals are
/// compared without building their values.
fn pair_in_order(left: &Expr, right: &Expr) -> Option<bool> {
    if let (Expr::Literal(Literal::String(left)), Expr::Literal(Literal::String(right))) =
        (strip_parens(left), strip_parens(right))
    {
        return Some(unquoted(left) < unquoted(right));
    }
    let left = const_value(left)?;
    let right = const_value(right)?;
    Some(left <= right && !same_constant(&left, &right))
}

fn construct_sorted_unique_list(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let exprs = exprs_arg(args, 0, "ConstructSortedUniqueList")?;
    let mut items: Vec<(SqlValue, Expr)> = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let value = const_value(expr)
            .ok_or_else(|| error("ConstructSortedUniqueList: not a constant".to_string()))?;
        items.push((value, expr.clone()));
    }
    items.sort_by(|left, right| left.0.cmp(&right.0));
    items.dedup_by(|next, previous| same_constant(&next.0, &previous.0));
    Ok(Value::Exprs(
        items.into_iter().map(|(_, expr)| expr).collect(),
    ))
}

/// Equal constants of one type. `1` and `1.0` are not the same: a TEXT
/// column converts them to different strings.
fn same_constant(left: &SqlValue, right: &SqlValue) -> bool {
    let same_type = matches!(
        (left, right),
        (SqlValue::Null, SqlValue::Null)
            | (
                SqlValue::Numeric(Numeric::Integer(_)),
                SqlValue::Numeric(Numeric::Integer(_))
            )
            | (
                SqlValue::Numeric(Numeric::Float(_)),
                SqlValue::Numeric(Numeric::Float(_))
            )
            | (SqlValue::Text(_), SqlValue::Text(_))
            | (SqlValue::Blob(_), SqlValue::Blob(_))
    );
    same_type && left.cmp(right).is_eq()
}

fn simplify_whens(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let base = if args.first().is_some_and(ArgRef::is_absent) {
        None
    } else {
        Some(expr_arg(args, 0, "SimplifyWhens")?)
    };
    let whens = whens_arg(args, 1, "SimplifyWhens")?;
    let else_expr = if args.get(2).is_some_and(ArgRef::is_absent) {
        None
    } else {
        Some(expr_arg(args, 2, "SimplifyWhens")?.clone())
    };
    let base_value = match base {
        Some(base) => Some(
            const_value(base)
                .ok_or_else(|| error("SimplifyWhens: the base is not a constant".to_string()))?,
        ),
        None => None,
    };
    let mut kept: Vec<(Expr, Expr)> = Vec::new();
    for (condition, result) in whens {
        let Some(value) = const_value(condition) else {
            kept.push((condition.clone(), result.clone()));
            continue;
        };
        let taken = match &base_value {
            None => Numeric::from_value(&value).is_some_and(|number| number.to_bool()),
            Some(base) => {
                !matches!(base, SqlValue::Null)
                    && !matches!(value, SqlValue::Null)
                    && base.cmp(&value).is_eq()
            }
        };
        if taken {
            return Ok(expr_value(build_case(
                base.cloned(),
                kept,
                Some(result.clone()),
            )));
        }
    }
    Ok(expr_value(build_case(base.cloned(), kept, else_expr)))
}

fn build_case(base: Option<Expr>, whens: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    if whens.is_empty() {
        return else_expr.unwrap_or(Expr::Literal(Literal::Null));
    }
    Expr::Case {
        base: base.map(Box::new),
        when_then_pairs: whens
            .into_iter()
            .map(|(condition, result)| (Box::new(condition), Box::new(result)))
            .collect(),
        else_expr: else_expr.map(Box::new),
    }
}

fn len_gt(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let whens = whens_arg(args, 0, "LenGT")?;
    let limit = args
        .get(1)
        .and_then(ArgRef::int)
        .ok_or_else(|| error("LenGT: argument 1 is not a number".to_string()))?;
    Ok(Value::Bool(whens.len() as i64 > limit))
}

fn const_string_equals(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let expr = expr_arg(args, 0, "ConstStringEquals")?;
    let text = args
        .get(1)
        .and_then(ArgRef::str)
        .ok_or_else(|| error("ConstStringEquals: argument 1 is not a string".to_string()))?;
    Ok(Value::Bool(matches!(
        const_value(expr),
        Some(SqlValue::Text(value)) if value.as_str() == text
    )))
}

fn drop_last(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let mut whens = whens_arg(args, 0, "DropLast")?;
    whens.pop();
    Ok(Value::Whens(
        whens
            .into_iter()
            .map(|(condition, result)| (condition.clone(), result.clone()))
            .collect(),
    ))
}

fn else_or_null(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    if args.first().is_some_and(ArgRef::is_absent) {
        return Ok(expr_value(Expr::Literal(Literal::Null)));
    }
    Ok(expr_value(expr_arg(args, 0, "ElseOrNull")?.clone()))
}

fn term_arg<'a>(args: &'a [ArgRef<'_, '_>], index: usize, function: &str) -> Result<&'a WhereTerm> {
    args.get(index)
        .and_then(ArgRef::term)
        .ok_or_else(|| error(format!("{function}: argument {index} is not a term")))
}

/// Whether `SimplifyTerms` changes this term: an `AND` splits, a true
/// constant goes, and a false or `NULL` constant makes the whole filter
/// false unless the term belongs to an outer join.
fn can_simplify_term(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let term = term_arg(args, 0, "CanSimplifyTerm")?;
    if term.consumed {
        return Ok(Value::Bool(false));
    }
    if and_parts(&term.expr).is_some() {
        return Ok(Value::Bool(true));
    }
    Ok(Value::Bool(match constant_truth(&term.expr) {
        Some(true) => true,
        Some(false) => term.from_outer_join.is_none(),
        None => false,
    }))
}

/// Whether the filter is the one term 0, the form that SimplifyTerms gives
/// for a filter that can never hold.
fn is_filter_false(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let terms = terms_arg(args, 0, "IsFilterFalse")?;
    Ok(Value::Bool(matches!(terms.as_slice(), [term]
        if term.from_outer_join.is_none()
            && !term.consumed
            && nodes::expr_op(&term.expr) == Op::False)))
}

fn simplify_terms(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let terms = terms_arg(args, 0, "SimplifyTerms")?;
    let mut out = Vec::with_capacity(terms.len());
    for term in terms {
        if term.consumed {
            out.push(term.clone());
            continue;
        }
        if !add_conjuncts(&term.expr, term.from_outer_join, &mut out) {
            return Ok(Value::Terms(vec![WhereTerm {
                expr: Expr::Literal(Literal::Numeric("0".to_string())),
                from_outer_join: None,
                consumed: false,
            }]));
        }
    }
    Ok(Value::Terms(out))
}

/// Add the conjuncts of `expr` to `out`. Return false when a conjunct is
/// always false, so the whole filter is.
fn add_conjuncts(
    expr: &Expr,
    from_outer_join: Option<TableInternalId>,
    out: &mut Vec<WhereTerm>,
) -> bool {
    if let Some((left, right)) = and_parts(expr) {
        return add_conjuncts(left, from_outer_join, out)
            && add_conjuncts(right, from_outer_join, out);
    }
    let expr = strip_parens(expr);
    match constant_truth(expr) {
        Some(true) => true,
        Some(false) if from_outer_join.is_none() => false,
        _ => {
            out.push(WhereTerm {
                expr: expr.clone(),
                from_outer_join,
                consumed: false,
            });
            true
        }
    }
}

fn same_term(left: &WhereTerm, right: &WhereTerm) -> bool {
    !left.consumed
        && !right.consumed
        && left.from_outer_join == right.from_outer_join
        && exprs_are_equivalent(strip_parens(&left.expr), strip_parens(&right.expr))
}

fn has_duplicate_terms(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let terms = terms_arg(args, 0, "HasDuplicateTerms")?;
    let duplicate = terms.iter().enumerate().any(|(index, term)| {
        terms[..index]
            .iter()
            .any(|earlier| same_term(earlier, term))
    });
    Ok(Value::Bool(duplicate))
}

fn deduplicate_terms(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let terms = terms_arg(args, 0, "DeduplicateTerms")?;
    let mut out: Vec<WhereTerm> = Vec::with_capacity(terms.len());
    for term in terms {
        if !out.iter().any(|earlier| same_term(earlier, term)) {
            out.push(term.clone());
        }
    }
    Ok(Value::Terms(out))
}

fn concat_terms(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let mut terms: Vec<WhereTerm> = terms_arg(args, 0, "ConcatTerms")?
        .into_iter()
        .cloned()
        .collect();
    terms.extend(terms_arg(args, 1, "ConcatTerms")?.into_iter().cloned());
    Ok(Value::Terms(terms))
}

/// The list without one term. The term is the one at the same address when
/// it comes from the list, or the first equal term otherwise.
fn remove_term(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let terms = terms_arg(args, 0, "RemoveTerm")?;
    let target = term_arg(args, 1, "RemoveTerm")?;
    let position = terms
        .iter()
        .position(|term| std::ptr::eq(*term, target))
        .or_else(|| terms.iter().position(|term| same_term(term, target)))
        .ok_or_else(|| error("RemoveTerm: the term is not in the list".to_string()))?;
    Ok(Value::Terms(
        terms
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != position)
            .map(|(_, term)| (*term).clone())
            .collect(),
    ))
}

/// A term of the WHERE clause itself: not an outer join term, not consumed.
fn is_plain_term(_: &EngineContext<'_, '_>, args: &[ArgRef<'_, '_>], _: Context) -> Result<Value> {
    let term = term_arg(args, 0, "IsPlainTerm")?;
    Ok(Value::Bool(
        term.from_outer_join.is_none() && !term.consumed,
    ))
}

/// Whether a column can never be `NULL` in the rows of a join tree: it is
/// declared `NOT NULL` or is the rowid, and its table is not on the side of
/// an outer join that gets `NULL` rows.
fn is_not_null_column(
    _: &EngineContext<'_, '_>,
    args: &[ArgRef<'_, '_>],
    _: Context,
) -> Result<Value> {
    let column = expr_arg(args, 0, "IsNotNullColumn")?;
    let Some(input) = args.get(1).and_then(ArgRef::plan) else {
        return Err(error(
            "IsNotNullColumn: argument 1 is not a plan node".to_string(),
        ));
    };
    let (table_id, column_index) = match strip_parens(column) {
        Expr::Column { table, column, .. } => (*table, Some(*column)),
        Expr::RowId { table, .. } => (*table, None),
        _ => return Ok(Value::Bool(false)),
    };
    if nullable_tables(input).contains(&table_id) {
        return Ok(Value::Bool(false));
    }
    let mut not_null = false;
    input.for_each_node(&mut |node| {
        if let LogicalPlan::Scan(scan) = node {
            if scan.table.internal_id != table_id {
                return;
            }
            not_null = match column_index {
                None => true,
                Some(index) => scan
                    .table
                    .columns()
                    .get(index)
                    .is_some_and(|column| column.is_rowid_alias() || column.notnull()),
            };
        }
    });
    Ok(Value::Bool(not_null))
}

/// The tables whose rows an outer join can fill with `NULL`.
fn nullable_tables(plan: &LogicalPlan) -> Vec<TableInternalId> {
    let mut tables = Vec::new();
    plan.for_each_node(&mut |node| {
        if let LogicalPlan::Join(join) = node {
            match join.info.join_type {
                JoinType::LeftOuter => tables.extend(leaf_ids(&join.right)),
                JoinType::FullOuter => {
                    tables.extend(leaf_ids(&join.left));
                    tables.extend(leaf_ids(&join.right));
                }
                JoinType::Inner | JoinType::Semi | JoinType::Anti => {}
            }
        }
    });
    tables
}

impl ArgRef<'_, '_> {
    pub fn type_name(&self) -> Option<&Type> {
        match self {
            ArgRef::Node(NodeRef::Private(PrivateRef::Type(type_name))) => type_name.as_ref(),
            ArgRef::Node(_) => None,
            _ => match self.value()? {
                Value::Private(private) => match private.as_ref() {
                    nodes::Private::Type(type_name) => type_name.as_ref(),
                    _ => None,
                },
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric(text: &str) -> Expr {
        Expr::Literal(Literal::Numeric(text.to_string()))
    }

    #[test]
    fn literal_values_round_trip_through_expressions() {
        for text in ["0", "1", "42", "1.5", "0.1", "1e300", "2.5e-7", "0x10"] {
            let value = const_value(&numeric(text)).expect("a number");
            let back = value_expr(&value).expect("a literal");
            assert_eq!(const_value(&back), Some(value), "{text}");
        }
        let negative = Expr::Unary(UnaryOperator::Negative, Box::new(numeric("5")));
        assert_eq!(const_value(&negative), Some(SqlValue::from_i64(-5)));
        assert_eq!(value_expr(&SqlValue::from_i64(-5)), Some(negative));
        let text = Expr::Literal(Literal::String("'it''s'".to_string()));
        assert_eq!(
            const_value(&text),
            Some(SqlValue::build_text("it's".to_string()))
        );
        assert_eq!(
            value_expr(&SqlValue::build_text("it's".to_string())),
            Some(text)
        );
        let blob = Expr::Literal(Literal::Blob("x'0aff'".to_string()));
        let value = const_value(&blob).expect("a blob");
        assert_eq!(value.to_blob(), Some(&[0x0a, 0xff][..]));
        assert_eq!(value_expr(&value), Some(blob));
        assert_eq!(value_expr(&SqlValue::from_f64(f64::INFINITY)), None);
        assert_eq!(
            value_expr(&SqlValue::from_i64(i64::MIN)),
            Some(numeric("-9223372036854775808"))
        );
    }

    #[test]
    fn truth_of_constants_follows_sqlite() {
        assert_eq!(constant_truth(&numeric("2")), Some(true));
        assert_eq!(constant_truth(&numeric("0.0")), Some(false));
        assert_eq!(
            constant_truth(&Expr::Literal(Literal::String("'abc'".to_string()))),
            Some(false)
        );
        assert_eq!(
            constant_truth(&Expr::Literal(Literal::String("'9S'".to_string()))),
            Some(true)
        );
        assert_eq!(constant_truth(&Expr::Literal(Literal::Null)), Some(false));
        assert_eq!(constant_truth(&Expr::Literal(Literal::True)), Some(true));
        assert_eq!(constant_truth(&Expr::Literal(Literal::CurrentDate)), None);
    }
}
