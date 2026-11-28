//! Which casts PostgreSQL has between its types, and how they convert a value.
//!
//! `CASTS` lists the casts of `pg_cast`. A cast to text goes through the
//! output function of the source type, and a cast from text through the input
//! function of the target type, as in PostgreSQL. Any other pair of types is
//! refused with SQLSTATE 42846.
//!
//! Nothing here reads a statement: `resolve` and `convert` take the types as
//! `CastType`. cast_planning.rs finds the `CastType` of an expression.

use crate::catalog::regclass_text;
use crate::error::sqlstate_error;
use crate::functions::{built_in_input_error, float8_text};
use crate::pg_type::regtype_text;
use crate::pg_type::{check_oid_range, type_input};
use crate::text::{value_text, TextType};
use std::sync::Arc;
use turso_core::dialect::CastContext;
use turso_core::{Connection, LimboError, Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_CANNOT_COERCE, SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastType {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Bool,
    Oid,
    RegClass,
    Uuid,
    RegType,
    // FIXME(pg-catalog): bytea is not a registered type yet, so the engine casts it as before. It is listed here and in CASTS once bytea has a pg_type row.
    Timestamp,
    TimestampTz,
}

impl CastType {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Numeric => "numeric",
            Self::Text => "text",
            Self::Bool => "bool",
            Self::Oid => "oid",
            Self::RegClass => "regclass",
            Self::Uuid => "uuid",
            Self::RegType => "regtype",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamptz",
        }
    }

    pub(crate) fn oid(self) -> i64 {
        crate::catalog::pg_type_oid(self.name()).expect("every cast type has a pg_type oid")
    }

    /// The type with the name that `name()` gives.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "int2" => Self::Int2,
            "int4" => Self::Int4,
            "int8" => Self::Int8,
            "float4" => Self::Float4,
            "float8" => Self::Float8,
            "numeric" => Self::Numeric,
            "text" => Self::Text,
            "bool" => Self::Bool,
            "oid" => Self::Oid,
            "regclass" => Self::RegClass,
            "uuid" => Self::Uuid,
            "regtype" => Self::RegType,
            "timestamp" => Self::Timestamp,
            "timestamptz" => Self::TimestampTz,
            _ => return None,
        })
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Int2 | Self::Int4 | Self::Int8)
    }

    /// The smallest number that a type of integers holds.
    fn smallest_integer(self) -> Option<i64> {
        match self {
            Self::Int2 => Some(i64::from(i16::MIN)),
            Self::Int4 => Some(i64::from(i32::MIN)),
            Self::Int8 => Some(i64::MIN),
            Self::Oid | Self::RegClass | Self::RegType => Some(0),
            _ => None,
        }
    }

    fn is_float(self) -> bool {
        matches!(self, Self::Float4 | Self::Float8)
    }
}

struct Cast {
    source: CastType,
    target: CastType,
    context: CastContext,
}

const fn cast(source: CastType, target: CastType, context: CastContext) -> Cast {
    Cast {
        source,
        target,
        context,
    }
}

use CastContext::{Assignment as A, Explicit as E, Implicit as I};
use CastType::*;

const CASTS: &[Cast] = &[
    cast(Int2, Int4, I),
    cast(Int2, Int8, I),
    cast(Int4, Int8, I),
    cast(Int4, Int2, A),
    cast(Int8, Int2, A),
    cast(Int8, Int4, A),
    cast(Int2, Float4, I),
    cast(Int4, Float4, I),
    cast(Int8, Float4, I),
    cast(Int2, Float8, I),
    cast(Int4, Float8, I),
    cast(Int8, Float8, I),
    cast(Int2, Numeric, I),
    cast(Int4, Numeric, I),
    cast(Int8, Numeric, I),
    cast(Int2, Oid, I),
    cast(Int4, Oid, I),
    cast(Int8, Oid, I),
    cast(Oid, Int4, A),
    cast(Oid, Int8, A),
    cast(Int2, RegClass, I),
    cast(Int4, RegClass, I),
    cast(Int8, RegClass, I),
    cast(Oid, RegClass, I),
    cast(RegClass, Oid, I),
    cast(RegClass, Int4, A),
    cast(RegClass, Int8, A),
    cast(Text, RegClass, I),
    cast(Int2, RegType, I),
    cast(Int4, RegType, I),
    cast(Int8, RegType, I),
    cast(Oid, RegType, I),
    cast(RegType, Oid, I),
    cast(RegType, Int4, A),
    cast(RegType, Int8, A),
    cast(Int4, Bool, E),
    cast(Bool, Int4, E),
    cast(Bool, Text, A),
    cast(Float4, Float8, I),
    cast(Float8, Float4, A),
    cast(Float4, Int2, A),
    cast(Float4, Int4, A),
    cast(Float4, Int8, A),
    cast(Float8, Int2, A),
    cast(Float8, Int4, A),
    cast(Float8, Int8, A),
    cast(Float4, Numeric, A),
    cast(Float8, Numeric, A),
    cast(Numeric, Int2, A),
    cast(Numeric, Int4, A),
    cast(Numeric, Int8, A),
    cast(Numeric, Float4, I),
    cast(Numeric, Float8, I),
    cast(Timestamp, TimestampTz, I),
    cast(TimestampTz, Timestamp, A),
];

/// The `oid` of the first row of `pg_cast`.
pub(crate) const FIRST_CAST_OID: i64 = 10_000;

/// The rows of `pg_cast`: source, target, the letter of the context and the
/// letter of the method. Every cast in `CASTS` goes through a conversion
/// function.
pub(crate) fn cast_rows() -> impl Iterator<Item = (CastType, CastType, &'static str, &'static str)>
{
    CASTS.iter().map(|cast| {
        let context = match cast.context {
            CastContext::Implicit => "i",
            CastContext::Assignment => "a",
            CastContext::Explicit => "e",
        };
        (cast.source, cast.target, context, "f")
    })
}

/// Whether the cast of a value of `source` to `target` changes the number that
/// the engine stores, so that comparing the numbers as they are stored gives
/// another answer than comparing the cast values. `source` is `None` for a
/// value of unknown type, such as a parameter. A negative integer cast to a
/// type that holds no negative number wraps around to a large one.
pub(crate) fn wraps_negative_integers(source: Option<CastType>, target: CastType) -> bool {
    source != Some(target)
        && target.smallest_integer().is_some_and(|min| min >= 0)
        && source.is_none_or(|source| source.smallest_integer().is_some_and(|min| min < 0))
}

pub(crate) enum Resolution {
    Unchanged,
    Convert,
}

pub(crate) fn resolve(
    source: CastType,
    target: CastType,
    context: CastContext,
) -> Result<Resolution> {
    if source == target {
        return Ok(Resolution::Unchanged);
    }
    let listed = CASTS
        .iter()
        .any(|cast| cast.source == source && cast.target == target && cast.context <= context);
    let through_text = (target == Text && context >= CastContext::Assignment)
        || (source == Text && context == CastContext::Explicit);
    if listed || through_text {
        return Ok(Resolution::Convert);
    }
    let (sqlstate, message) = match context {
        CastContext::Assignment => (
            SQLSTATE_DATATYPE_MISMATCH,
            format!(
                "a value of type {} cannot be assigned to type {}",
                source.name(),
                target.name()
            ),
        ),
        _ => (
            SQLSTATE_CANNOT_COERCE,
            format!("cannot cast type {} to {}", source.name(), target.name()),
        ),
    };
    Err(sqlstate_error(sqlstate, message, None))
}

pub(crate) fn convert(
    connection: &Arc<Connection>,
    value: &Value,
    source: CastType,
    target: CastType,
) -> Result<Value> {
    if let (Value::Text(text), CastType::Text, Some(read)) =
        (value, source, type_input(target.oid()))
    {
        return read(Some(connection), text.as_str());
    }
    if target.is_integer() {
        return to_integer(value, source, target);
    }
    if target.is_float() {
        return to_float(value, source, target);
    }
    if matches!(target, Oid | RegClass | RegType) {
        return to_oid(value, source);
    }
    match target {
        Text => to_text(connection, value, source),
        Bool => to_bool(value),
        _ => Ok(value.clone()),
    }
}

fn to_oid(value: &Value, source: CastType) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(n @ -0x8000_0000..=-1)) if source != Int8 => {
            Ok(Value::from_i64(n + (1 << 32)))
        }
        _ => check_oid_range(value),
    }
}

fn to_integer(value: &Value, source: CastType, target: CastType) -> Result<Value> {
    let number = match (value, source) {
        (Value::Text(text), CastType::Numeric) => {
            float_to_i64(parse_numeric(text.as_str())?.round(), target)?
        }
        (Value::Numeric(Numeric::Integer(n)), Oid | RegClass | RegType) if target == Int4 => {
            i64::from(*n as u32 as i32)
        }
        (Value::Numeric(Numeric::Integer(n)), _) => *n,
        // The server reads the text of a parameter with no type as a float
        // when it looks like one. As text it is not an integer.
        (Value::Numeric(Numeric::Float(f)), CastType::Text) => {
            let read = type_input(target.oid()).expect("an integer type has an INPUT");
            return read(None, &format!("{:?}", f64::from(*f)));
        }
        (Value::Numeric(Numeric::Float(f)), CastType::Numeric) => {
            float_to_i64(f64::from(*f).round(), target)?
        }
        (Value::Numeric(Numeric::Float(f)), _) => {
            float_to_i64(f64::from(*f).round_ties_even(), target)?
        }
        _ => return Ok(value.clone()),
    };
    check_integer_range(number, target)?;
    Ok(Value::from_i64(number))
}

fn to_bool(value: &Value) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(n)) => Ok(Value::from_i64((*n != 0) as i64)),
        _ => Ok(value.clone()),
    }
}

fn float_to_i64(rounded: f64, target: CastType) -> Result<i64> {
    // 2^63 is not an i64, and every float at or past it is out of range.
    if !(-9223372036854775808.0..9223372036854775808.0).contains(&rounded) {
        return Err(out_of_range(target));
    }
    Ok(rounded as i64)
}

fn check_integer_range(number: i64, target: CastType) -> Result<()> {
    let fits = match target {
        Int2 => i16::try_from(number).is_ok(),
        Int4 => i32::try_from(number).is_ok(),
        _ => true,
    };
    if fits {
        Ok(())
    } else {
        Err(out_of_range(target))
    }
}

fn out_of_range(target: CastType) -> LimboError {
    let message = match target {
        Int2 => "smallint out of range",
        Int4 => "integer out of range",
        _ => "bigint out of range",
    };
    sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, message, None)
}

fn sqlstate_of(code: &str) -> &'static str {
    if code == SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE {
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE
    } else {
        SQLSTATE_INVALID_TEXT_REPRESENTATION
    }
}

fn input_error(text: &str, type_name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {type_name}: \"{text}\""),
        None,
    )
}

fn to_float(value: &Value, source: CastType, target: CastType) -> Result<Value> {
    let number = match value {
        Value::Numeric(Numeric::Integer(n)) => *n as f64,
        Value::Numeric(Numeric::Float(f)) => f64::from(*f),
        Value::Text(text) if source == CastType::Numeric => parse_numeric(text.as_str())?,
        Value::Text(text) => parse_float(text.as_str(), target)?,
        _ => return Ok(value.clone()),
    };
    // FIXME(pg-catalog): This is a stop gap. The engine's float cannot hold NaN and stores it as NULL, so NaN is refused here, where PostgreSQL stores it. Registered float4, float8 and numeric types, with their own stored form, will store NaN.
    if number.is_nan() {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "the value NaN is not supported",
            None,
        ));
    }
    if target == Float4 {
        let narrowed = number as f32;
        if narrowed.is_infinite() && number.is_finite() {
            return Err(sqlstate_error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow",
                None,
            ));
        }
        if narrowed == 0.0 && number != 0.0 {
            return Err(sqlstate_error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: underflow",
                None,
            ));
        }
        return Ok(Value::from_f64(shortest_float8(narrowed)));
    }
    Ok(Value::from_f64(number))
}

/// The float8 that prints with the digits of the float4, so 0.1 as a float4
/// stays 0.1 and is not 0.10000000149011612.
fn shortest_float8(narrowed: f32) -> f64 {
    narrowed
        .to_string()
        .parse()
        .unwrap_or_else(|_| f64::from(narrowed))
}

/// A numeric with a type modifier is stored as its text.
fn parse_numeric(text: &str) -> Result<f64> {
    text.trim()
        .parse()
        .map_err(|_| input_error(text, "numeric"))
}

// FIXME(pg-catalog): A value too small for the type, such as `1e-400`, reads as 0 here, where PostgreSQL fails with 22003. The check belongs in the input function of float8 and float4 once they are registered types.
fn parse_float(text: &str, target: CastType) -> Result<f64> {
    let type_name = if target == Float4 {
        "real"
    } else {
        "double precision"
    };
    if let Some((message, code)) = built_in_input_error(target.name(), text) {
        return Err(sqlstate_error(sqlstate_of(&code), message, None));
    }
    text.trim()
        .parse()
        .map_err(|_| input_error(text, type_name))
}

fn to_text(connection: &Arc<Connection>, value: &Value, source: CastType) -> Result<Value> {
    let text = match (value, source) {
        (Value::Numeric(Numeric::Integer(n)), CastType::Bool) => {
            if *n == 0 { "false" } else { "true" }.to_string()
        }
        (Value::Numeric(Numeric::Integer(n)), CastType::RegClass) => regclass_text(connection, *n),
        (Value::Numeric(Numeric::Integer(n)), CastType::RegType) => regtype_text(connection, *n),
        (Value::Numeric(Numeric::Integer(n)), _) => n.to_string(),
        // FIXME(pg-catalog): A numeric without a type modifier is REAL, which is read as float8, so `1.0::numeric::text` is `1` where PostgreSQL gives `1.0`. A registered numeric type that needs no modifier gives the value its own output function.
        // FIXME(pg-catalog): Core types a float4 expression REAL, which is read as float8, so no float4 value gets here and `1e6::float4::text` is `1000000` where PostgreSQL gives `1e+06`. A registered float4 type gives the value its own output function.
        (Value::Numeric(Numeric::Float(f)), CastType::Float4 | CastType::Float8) => {
            float8_text(f64::from(*f))
        }
        (Value::Text(_), CastType::TimestampTz) => {
            match value_text(value, TextType::TimestampTz, connection) {
                Some(text) => text.into_owned(),
                None => return Ok(value.clone()),
            }
        }
        _ => return Ok(value.clone()),
    };
    Ok(Value::build_text(text))
}
