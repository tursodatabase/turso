//! The operators of PostgreSQL's `pg_operator` for the types the frontend
//! knows, and PostgreSQL's choice of an operator for the operands of a call.

use crate::casts::{has_implicit_cast, source_type, CastType};
use turso_core::dialect::{OperandType, OperatorResolution};

pub(crate) struct Operator {
    pub name: &'static str,
    pub left: CastType,
    pub right: CastType,
    pub result: CastType,
}

use CastType::*;

const INTEGERS: [CastType; 3] = [Int2, Int4, Int8];
const FLOATS: [CastType; 2] = [Float4, Float8];
const COMPARISONS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// The `oid` of the first row of `pg_operator`. Each row after it has the
/// next one, so a row keeps its `oid` while operators are only added at the
/// end.
pub(crate) const FIRST_OPERATOR_OID: i64 = 9_000;

/// The rows of `pg_operator` that the frontend has. Where PostgreSQL has an
/// operator for each pair of integer types, so do we. Other pairs of number
/// types have no row: they run through an implicit cast, as in PostgreSQL.
pub(crate) fn operators() -> &'static [Operator] {
    static OPERATORS: std::sync::OnceLock<Vec<Operator>> = std::sync::OnceLock::new();
    OPERATORS.get_or_init(build_operators)
}

fn build_operators() -> Vec<Operator> {
    let mut rows = Vec::new();
    let mut binary = |name, left: CastType, right: CastType, result: CastType| {
        rows.push(Operator {
            name,
            left,
            right,
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
                binary(name, left, right, Bool);
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
                binary(name, left, right, wider);
            }
            if left == right && !FLOATS.contains(&left) {
                binary("%", left, right, wider);
            }
        }
    }
    rows
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
        [left, right] => resolve_binary(op, left, right),
        _ => OperatorResolution::Keep,
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
    if exists(op, left_type, right_type, true) {
        OperatorResolution::Keep
    } else {
        OperatorResolution::Reject
    }
}

/// A string literal next to an operand of type `ty`: the literal is read as
/// `ty`, which needs an operator for two operands of that type.
fn resolve_literal(op: &str, ty: CastType) -> OperatorResolution {
    if exists(op, ty, ty, false) {
        OperatorResolution::Keep
    } else {
        OperatorResolution::Reject
    }
}

/// Whether an operator `op` takes operands of these types. With `implicit`,
/// an operand may be cast implicitly to the type the operator takes.
fn exists(op: &str, left: CastType, right: CastType, implicit: bool) -> bool {
    let accepts = |expected: CastType, ty: CastType| {
        expected == ty || (implicit && has_implicit_cast(ty, expected))
    };
    operators()
        .iter()
        .any(|row| row.name == op && accepts(row.left, left) && accepts(row.right, right))
}

fn known_type(operand: &OperandType) -> Option<CastType> {
    match operand {
        OperandType::Untyped => None,
        other => source_type(other),
    }
}
