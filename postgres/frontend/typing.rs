//! The types of the results of PostgreSQL functions and of values that meet
//! in a CASE, coalesce or UNION, for the types that the engine cannot work out
//! by itself.

use crate::casts::{source_type, CastType};
use turso_core::dialect::OperandType;
use turso_core::schema::Schema;

/// count is a bigint, sum of a smallint or an integer is a bigint, abs, min
/// and max of an integer type are that type, abs, sum, min, max and avg of a
/// float8 are a float8, abs, sum, min and max of a float4 are a float4, and avg
/// of one is a float8.
pub(crate) fn function_result_type(
    name: &str,
    args: &dyn Fn() -> Vec<OperandType>,
    schema: &Schema,
) -> Option<OperandType> {
    let name = name.to_ascii_lowercase();
    if name == "count" {
        return bigint(schema);
    }
    if matches!(
        name.as_str(),
        "regexp" | "array_contains" | "array_contains_all" | "array_overlap"
    ) {
        return Some(OperandType::Boolean);
    }
    if !matches!(name.as_str(), "abs" | "sum" | "min" | "max" | "avg") {
        return None;
    }
    let args = args();
    let [arg] = args.as_slice() else {
        return None;
    };
    if name == "sum" && is_custom(arg, &["smallint", "int4"]) {
        return bigint(schema);
    }
    if matches!(name.as_str(), "abs" | "min" | "max")
        && is_custom(arg, &["smallint", "int4", "bigint"])
    {
        return Some(arg.clone());
    }
    if matches!(arg, OperandType::Real) {
        return Some(OperandType::Real);
    }
    if !is_float4(arg) {
        return None;
    }
    Some(if name == "avg" {
        OperandType::Real
    } else {
        arg.clone()
    })
}

fn bigint(schema: &Schema) -> Option<OperandType> {
    schema
        .get_type_def_unchecked("bigint")
        .cloned()
        .map(OperandType::Custom)
}

fn is_custom(operand: &OperandType, names: &[&str]) -> bool {
    matches!(operand, OperandType::Custom(type_def) if names.contains(&type_def.name.as_str()))
}

/// PostgreSQL adds a double precision (or a real) and a numeric as doubles,
/// because a numeric casts to double precision implicitly and not the other
/// way.
pub(crate) fn uses_sqlite_arithmetic(op: &str, operands: &dyn Fn() -> Vec<OperandType>) -> bool {
    if !matches!(op, "+" | "-" | "*" | "/" | "%") {
        return false;
    }
    let operands = operands();
    let [left, right] = operands.as_slice() else {
        return false;
    };
    let is_float = |operand: &OperandType| {
        matches!(operand, OperandType::Real) || is_custom(operand, &["float4"])
    };
    let is_numeric = |operand: &OperandType| {
        matches!(operand, OperandType::Decimal) || is_custom(operand, &["numeric"])
    };
    (is_float(left) && is_numeric(right)) || (is_numeric(left) && is_float(right))
}

/// `bytea || bytea` joins two byte strings; every other `||` joins text.
pub(crate) fn concatenates_as_text(operands: &dyn Fn() -> Vec<OperandType>) -> bool {
    !operands()
        .iter()
        .any(|operand| matches!(operand, OperandType::Blob))
}

/// The type PostgreSQL gives values that meet in a CASE, coalesce or UNION,
/// when one of them is a float4 or when they are integers of different widths:
/// the first type that every other type can be implicitly cast to, where a
/// type that the first can be cast to takes its place unless the first is the
/// preferred type of its category (float8).
pub(crate) fn common_type(types: &dyn Fn() -> Vec<OperandType>) -> Option<OperandType> {
    let types = types();
    if !types.iter().any(is_float4) && !mixes_integer_widths(&types) {
        return None;
    }
    let mut candidate: Option<(CastType, &OperandType)> = None;
    for operand in &types {
        if matches!(operand, OperandType::Untyped | OperandType::Null) {
            continue;
        }
        let ty = source_type(operand)?;
        if !ty.is_number() {
            return None;
        }
        candidate = match candidate {
            Some((current, _))
                if current != ty
                    && !current.is_preferred()
                    && casts_implicitly(current, ty)
                    && !casts_implicitly(ty, current) =>
            {
                Some((ty, operand))
            }
            None => Some((ty, operand)),
            other => other,
        };
    }
    candidate.map(|(_, operand)| operand.clone())
}

fn mixes_integer_widths(types: &[OperandType]) -> bool {
    let mut widths = Vec::new();
    for operand in types {
        match (operand, source_type(operand)) {
            (OperandType::Untyped | OperandType::Null, _) => {}
            (
                OperandType::Custom(_),
                Some(ty @ (CastType::Int2 | CastType::Int4 | CastType::Int8)),
            ) => {
                if !widths.contains(&ty) {
                    widths.push(ty);
                }
            }
            _ => return false,
        }
    }
    widths.len() > 1
}

fn is_float4(operand: &OperandType) -> bool {
    matches!(source_type(operand), Some(CastType::Float4))
        && matches!(operand, OperandType::Custom(_))
}

fn casts_implicitly(from: CastType, to: CastType) -> bool {
    crate::casts::has_implicit_cast(from, to)
}
