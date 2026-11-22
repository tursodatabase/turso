//! The operators of PostgreSQL's `pg_operator` for the types the frontend
//! knows, and PostgreSQL's choice of an operator for the operands of a call.

use crate::casts::{has_implicit_cast, source_type, CastType};
use turso_core::dialect::{OperandType, OperatorResolution};

pub(crate) struct Operator {
    pub name: &'static str,
    /// None for a prefix operator.
    pub left: Option<Arg>,
    pub right: Arg,
    pub result: Arg,
}

/// The type of an operand of an operator. `AnyNonArray` is PostgreSQL's
/// pseudo-type of the same name, as in `text || anynonarray`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arg {
    Type(CastType),
    AnyNonArray,
}

use CastType::*;

const INTEGERS: [CastType; 3] = [Int2, Int4, Int8];
const FLOATS: [CastType; 2] = [Float4, Float8];
const COMPARISONS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// The `oid` of the first row of `pg_operator`. Each row after it has the
/// next one, so a row keeps its `oid` while operators are only added at the
/// end.
pub(crate) const FIRST_OPERATOR_OID: i64 = 9_000;

pub(crate) fn arg_oid(arg: Arg) -> i64 {
    match arg {
        Arg::AnyNonArray => 2776,
        Arg::Type(ty) => ty.oid(),
    }
}

/// The rows of `pg_operator` that the frontend has. Where PostgreSQL has an
/// operator for each pair of integer types, so do we. Other pairs of number
/// types have no row: they run through an implicit cast, as in PostgreSQL.
pub(crate) fn operators() -> &'static [Operator] {
    static OPERATORS: std::sync::OnceLock<Vec<Operator>> = std::sync::OnceLock::new();
    OPERATORS.get_or_init(build_operators)
}

fn build_operators() -> Vec<Operator> {
    let mut rows = Vec::new();
    let mut binary = |name, left: CastType, right: CastType, result: Arg| {
        rows.push(Operator {
            name,
            left: Some(Arg::Type(left)),
            right: Arg::Type(right),
            result,
        });
    };
    let same_or_cross = |left: CastType, right: CastType| {
        left == right
            || (INTEGERS.contains(&left) && INTEGERS.contains(&right))
            || (FLOATS.contains(&left) && FLOATS.contains(&right))
    };
    for left in CastType::ALL {
        for right in CastType::ALL {
            if !same_or_cross(left, right) {
                continue;
            }
            for name in COMPARISONS {
                binary(name, left, right, Arg::Type(Bool));
            }
        }
    }
    for left in numbers() {
        for right in numbers() {
            if !same_or_cross(left, right) {
                continue;
            }
            let wider = wider_number(left, right);
            for name in ["+", "-", "*", "/"] {
                binary(name, left, right, Arg::Type(wider));
            }
            if left == right && !FLOATS.contains(&left) {
                binary("%", left, right, Arg::Type(wider));
            }
        }
    }
    for left in INTEGERS {
        for right in INTEGERS {
            if left == right {
                for name in ["&", "|"] {
                    binary(name, left, right, Arg::Type(left));
                }
            }
        }
        for name in ["<<", ">>"] {
            binary(name, left, Int4, Arg::Type(left));
        }
    }
    for name in ["~~", "!~~"] {
        binary(name, Text, Text, Arg::Type(Bool));
        binary(name, Bytea, Bytea, Arg::Type(Bool));
    }
    for name in ["~", "!~", "~*", "!~*"] {
        binary(name, Text, Text, Arg::Type(Bool));
    }
    binary("^", Float8, Float8, Arg::Type(Float8));
    binary("^", Numeric, Numeric, Arg::Type(Numeric));
    binary("||", Text, Text, Arg::Type(Text));
    binary("||", Bytea, Bytea, Arg::Type(Bytea));
    rows.push(Operator {
        name: "||",
        left: Some(Arg::Type(Text)),
        right: Arg::AnyNonArray,
        result: Arg::Type(Text),
    });
    rows.push(Operator {
        name: "||",
        left: Some(Arg::AnyNonArray),
        right: Arg::Type(Text),
        result: Arg::Type(Text),
    });
    for ty in numbers() {
        for name in ["-", "+"] {
            rows.push(prefix(name, ty));
        }
    }
    for ty in INTEGERS {
        rows.push(prefix("~", ty));
    }
    rows
}

fn prefix(name: &'static str, ty: CastType) -> Operator {
    Operator {
        name,
        left: None,
        right: Arg::Type(ty),
        result: Arg::Type(ty),
    }
}

fn numbers() -> impl Iterator<Item = CastType> {
    CastType::ALL.into_iter().filter(|ty| ty.is_number())
}

/// The type that the other can be cast to implicitly.
fn wider_number(left: CastType, right: CastType) -> CastType {
    if has_implicit_cast(left, right) {
        right
    } else {
        left
    }
}

/// PostgreSQL's choice of an operator (`oper_select` in parse_oper.c) for
/// operands of the given types:
///
/// 1. A string literal takes the type of the other operand, and an operator
///    for two operands of that type must exist.
/// 2. Otherwise the operator for the types of the operands must exist, or one
///    that both operands can be cast to implicitly.
///
/// An operator the frontend has no row for, such as one of dates, is left to
/// the engine, as is an operand whose type is not known.
pub(crate) fn resolve_operator(op: &str, operands: &[OperandType]) -> OperatorResolution {
    if !operators().iter().any(|row| row.name == op) {
        return OperatorResolution::Keep;
    }
    match operands {
        [operand] => resolve_prefix(op, operand),
        [left, right] => resolve_binary(op, left, right),
        _ => OperatorResolution::Keep,
    }
}

fn resolve_prefix(op: &str, operand: &OperandType) -> OperatorResolution {
    let Some(ty) = known_type(operand) else {
        return OperatorResolution::Keep;
    };
    let found = operators()
        .iter()
        .any(|row| row.name == op && row.left.is_none() && accepts(row.right, ty));
    if found {
        OperatorResolution::Keep
    } else {
        OperatorResolution::Reject
    }
}

fn resolve_binary(op: &str, left: &OperandType, right: &OperandType) -> OperatorResolution {
    let untyped = |operand: &OperandType| matches!(operand, OperandType::Untyped);
    let (left_type, right_type) = match (untyped(left), untyped(right)) {
        (true, true) => return OperatorResolution::Keep,
        (true, false) => {
            let Some(ty) = known_type(right) else {
                return OperatorResolution::Keep;
            };
            return resolve_literal(op, ty);
        }
        (false, true) => {
            let Some(ty) = known_type(left) else {
                return OperatorResolution::Keep;
            };
            return resolve_literal(op, ty);
        }
        (false, false) => match (known_type(left), known_type(right)) {
            (Some(left), Some(right)) => (left, right),
            _ => return OperatorResolution::Keep,
        },
    };
    if !exists(op, left_type, right_type, true) {
        return OperatorResolution::Reject;
    }
    compare_as_doubles(op, left_type, right_type)
}

/// A double precision or a real compared with a numeric: the numeric is cast
/// to double precision, which is the only direction PostgreSQL casts them.
fn compare_as_doubles(op: &str, left: CastType, right: CastType) -> OperatorResolution {
    let is_float = |ty: CastType| matches!(ty, Float4 | Float8);
    let as_double = || Some(Float8.name().to_string());
    match (COMPARISONS.contains(&op), left, right) {
        (true, left, Numeric) if is_float(left) => {
            OperatorResolution::CastOperands(None, as_double())
        }
        (true, Numeric, right) if is_float(right) => {
            OperatorResolution::CastOperands(as_double(), None)
        }
        _ => OperatorResolution::Keep,
    }
}

/// A string literal next to an operand of type `ty`: the literal is read as
/// `ty`, except for text, which needs no cast.
fn resolve_literal(op: &str, ty: CastType) -> OperatorResolution {
    if matches!(op, "||" | "<<" | ">>") {
        return OperatorResolution::Keep;
    }
    if !exists(op, ty, ty, false) {
        return OperatorResolution::Reject;
    }
    if ty == Text {
        OperatorResolution::Keep
    } else {
        OperatorResolution::CastUntyped(ty.name().to_string())
    }
}

/// Whether an operator `op` takes operands of these types. With `implicit`,
/// an operand may be cast implicitly to the type the operator takes.
fn exists(op: &str, left: CastType, right: CastType, implicit: bool) -> bool {
    operators().iter().any(|row| {
        row.name == op
            && row.left.is_some_and(|arg| {
                accepts(arg, left) || (implicit && accepts_after_cast(arg, left))
            })
            && (accepts(row.right, right) || (implicit && accepts_after_cast(row.right, right)))
    })
}

fn accepts(arg: Arg, ty: CastType) -> bool {
    match arg {
        Arg::Type(expected) => expected == ty,
        Arg::AnyNonArray => true,
    }
}

fn accepts_after_cast(arg: Arg, ty: CastType) -> bool {
    match arg {
        Arg::Type(expected) => has_implicit_cast(ty, expected),
        Arg::AnyNonArray => true,
    }
}

fn known_type(operand: &OperandType) -> Option<CastType> {
    match operand {
        OperandType::Untyped => None,
        other => source_type(other),
    }
}
