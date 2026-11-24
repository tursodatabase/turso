//! What the integer types of PostgreSQL share: the functions of their
//! operators and the signatures of the operators.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::schema::{OperatorArg, OperatorSignature};
use turso_core::{LimboError, Numeric, Register, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE;

const BINARY_OPERATORS: [(&str, &str); 7] = [
    ("+", "pl"),
    ("-", "mi"),
    ("*", "mul"),
    ("/", "div"),
    ("%", "mod"),
    ("&", "and"),
    ("|", "or"),
];

/// The operators on an integer type. With a value of its own type or a
/// narrower integer type, `+ - * / %` and `-x` give the type and fail with
/// 22003 when the result does not fit. A plain integer, such as the result of
/// a function, has no known width and computes as an int8 with a bigint, and
/// as SQLite does with a smallint. With any other plain value, such as a
/// decimal, SQLite's operators run. The shifts are SQLite's.
///
/// The signatures own their strings and differ by type in the narrower types
/// and in the function for a plain integer, so one helper builds them from the
/// table of operators.
pub(super) fn operator_signatures(
    name: &'static str,
    prefix: &str,
    narrower: &[&'static str],
    computes_plain_integers: bool,
) -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Type, Untyped, UntypedInteger, Value};
    let signature = |op, args: Vec<OperatorArg>, function: Option<String>| OperatorSignature {
        op,
        args,
        result: function.as_ref().map(|_| name),
        func: function,
    };
    let mut signatures = Vec::new();
    for (op, suffix) in BINARY_OPERATORS {
        let own = || Some(format!("{prefix}{suffix}"));
        signatures.push(signature(op, vec![Value, Value], own()));
        for narrower in narrower {
            signatures.push(signature(op, vec![Value, Type(narrower)], own()));
            signatures.push(signature(op, vec![Type(narrower), Value], own()));
        }
        let plain = computes_plain_integers.then(|| format!("int8{suffix}"));
        signatures.push(signature(op, vec![Value, UntypedInteger], plain.clone()));
        signatures.push(signature(op, vec![UntypedInteger, Value], plain));
        signatures.push(signature(op, vec![Value, Untyped], None));
        signatures.push(signature(op, vec![Untyped, Value], None));
    }
    for op in ["<<", ">>"] {
        signatures.push(signature(op, vec![Value, PlainValue], None));
        signatures.push(signature(op, vec![PlainValue, Value], None));
    }
    signatures.push(signature("-", vec![Value], Some(format!("{prefix}um"))));
    signatures.push(signature("~", vec![Value], Some(format!("{prefix}not"))));
    signatures
}

pub(super) fn value_param(ty: &str) -> ast::TypeParam {
    ast::TypeParam {
        name: "value".to_string(),
        ty: Some(ty.to_string()),
    }
}

macro_rules! integer_functions {
    (
        $int:ty, $display:literal,
        $pl:ident, $mi:ident, $mul:ident, $div:ident, $rem:ident,
        $and:ident, $or:ident, $not:ident, $um:ident
    ) => {
        pub(crate) fn $pl(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| Ok(a.checked_add(b)))
        }

        pub(crate) fn $mi(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| Ok(a.checked_sub(b)))
        }

        pub(crate) fn $mul(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| Ok(a.checked_mul(b)))
        }

        pub(crate) fn $div(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| match b {
                0 => Err(turso_core::LimboError::DivisionByZero),
                _ => Ok(a.checked_div(b)),
            })
        }

        pub(crate) fn $rem(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| match b {
                0 => Err(turso_core::LimboError::DivisionByZero),
                _ => Ok(Some(a.checked_rem(b).unwrap_or(0))),
            })
        }

        pub(crate) fn $and(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| Ok(Some(a & b)))
        }

        pub(crate) fn $or(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::binary::<$int>(args, $display, |a, b| Ok(Some(a | b)))
        }

        pub(crate) fn $not(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::unary::<$int>(args, $display, |a| Some(!a))
        }

        pub(crate) fn $um(args: &[Register]) -> Result<Value> {
            $crate::pg_type::integer::unary::<$int>(args, $display, |a| a.checked_neg())
        }
    };
}

pub(super) use integer_functions;

/// The result of `operation` on two integers of type `T`, or NULL when an
/// argument is NULL. `operation` gives None when the result does not fit.
pub(crate) fn binary<T>(
    args: &[Register],
    display_name: &str,
    operation: impl Fn(T, T) -> Result<Option<T>>,
) -> Result<Value>
where
    T: TryFrom<i64> + Into<i64>,
{
    let (left, right) = (args[0].get_value(), args[1].get_value());
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let (left, right) = (
        integer::<T>(left, display_name)?,
        integer::<T>(right, display_name)?,
    );
    result(operation(left, right)?, display_name)
}

pub(crate) fn unary<T>(
    args: &[Register],
    display_name: &str,
    operation: impl Fn(T) -> Option<T>,
) -> Result<Value>
where
    T: TryFrom<i64> + Into<i64>,
{
    let value = args[0].get_value();
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    result(operation(integer::<T>(value, display_name)?), display_name)
}

fn integer<T: TryFrom<i64>>(value: &Value, display_name: &str) -> Result<T> {
    match value {
        Value::Numeric(Numeric::Integer(n)) => {
            T::try_from(*n).map_err(|_| out_of_range(display_name))
        }
        other => Err(LimboError::InternalError(format!(
            "expected an integer, got {other:?}"
        ))),
    }
}

fn result<T: Into<i64>>(value: Option<T>, display_name: &str) -> Result<Value> {
    match value {
        Some(n) => Ok(Value::from_i64(n.into())),
        None => Err(out_of_range(display_name)),
    }
}

pub(super) fn out_of_range(display_name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{display_name} out of range"),
        None,
    )
}
