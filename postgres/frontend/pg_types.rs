//! The scalar types that PostgreSQL has and the engine does not.

use crate::error::sqlstate_error;
use std::sync::Arc;
use turso_core::schema::{
    OperatorArg, OperatorSignature, RangeOffsets, SchemaBuilder, TypeDef, TypeDefKind, TypeOrigin,
    BINARY_OPERATORS,
};
use turso_core::{LimboError, Numeric, Register, Result, Value};
use turso_parser::ast::{Cmd, Stmt};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

const FLOAT4: &str = "CREATE TYPE float4(value real) BASE real COERCE pg_float4_round(value)";
const INT2: &str = "CREATE TYPE smallint(value integer) BASE integer COERCE CASE WHEN value BETWEEN -32768 AND 32767 THEN value ELSE RAISE(ABORT, 'integer out of range for smallint') END OPERATOR '<'";
const INT8: &str = "CREATE TYPE bigint(value integer) BASE integer";
const INT4: &str = "CREATE TYPE int4(value integer) BASE integer COERCE CASE WHEN value BETWEEN -2147483648 AND 2147483647 THEN value ELSE RAISE(ABORT, 'integer out of range') END";

pub(crate) fn register_scalar_types(
    dialect: &crate::catalog::PostgresDialect,
    builder: SchemaBuilder,
) -> Result<SchemaBuilder> {
    let builder = register_type(
        dialect,
        builder,
        FLOAT4,
        RangeOffsets::Numeric,
        false,
        float4_operators(),
        &[],
    )?;
    let builder = register_type(
        dialect,
        builder,
        INT4,
        RangeOffsets::Integer,
        true,
        integer_operators(INT4_WIDTH),
        &[],
    )?;
    let builder = register_type(
        dialect,
        builder,
        INT2,
        RangeOffsets::Integer,
        false,
        integer_operators(INT2_WIDTH),
        &["int2"],
    )?;
    register_type(
        dialect,
        builder,
        INT8,
        RangeOffsets::Integer,
        false,
        integer_operators(INT8_WIDTH),
        &["int8"],
    )
}

fn register_type(
    dialect: &crate::catalog::PostgresDialect,
    builder: SchemaBuilder,
    sql: &str,
    range_offsets: RangeOffsets,
    can_be_rowid_alias: bool,
    signatures: Vec<OperatorSignature>,
    aliases: &[&str],
) -> Result<SchemaBuilder> {
    let Ok(Some(Cmd::Stmt(Stmt::CreateType {
        type_name, body, ..
    }))) = turso_core::dialect::parse_statement(dialect, sql)
    else {
        return Err(LimboError::InternalError(format!(
            "failed to parse built-in type SQL: {sql}"
        )));
    };
    let origin = TypeOrigin::Builtin {
        accepts_collate: false,
        range_offsets,
        stored_form_sorts_in_value_order: true,
        can_be_rowid_alias,
    };
    let mut type_def = TypeDef::from_create_type(&type_name, &body, origin, sql.to_string())?;
    if let TypeDefKind::Custom {
        operator_signatures,
        ..
    } = &mut type_def.kind
    {
        *operator_signatures = signatures;
    }
    let type_def = Arc::new(type_def);
    let builder = aliases.iter().fold(builder, |builder, alias| {
        builder.type_def(alias, Arc::clone(&type_def))
    });
    Ok(builder.type_def(&type_name, type_def))
}

/// An integer type of PostgreSQL: its name in the registry, the prefix of its
/// functions, the narrower integer types, and the type of the result with an
/// integer written in the statement, which PostgreSQL types as an int4.
#[derive(Clone, Copy)]
struct IntegerWidth {
    type_name: &'static str,
    prefix: &'static str,
    narrower: &'static [&'static str],
    literal_type_name: &'static str,
    literal_prefix: &'static str,
}

const INT2_WIDTH: IntegerWidth = IntegerWidth {
    type_name: "smallint",
    prefix: "int2",
    narrower: &[],
    literal_type_name: "int4",
    literal_prefix: "int4",
};

const INT8_WIDTH: IntegerWidth = IntegerWidth {
    type_name: "bigint",
    prefix: "int8",
    narrower: &["int4", "smallint"],
    literal_type_name: "bigint",
    literal_prefix: "int8",
};

const INT4_WIDTH: IntegerWidth = IntegerWidth {
    type_name: "int4",
    prefix: "int4",
    narrower: &["smallint"],
    literal_type_name: "int4",
    literal_prefix: "int4",
};

/// The operators on an integer type. With a value of its own type, an
/// integer written in the statement or a narrower integer type, `+ - * / %`
/// and `-x` give the type and fail with 22003 when the result does not fit.
/// With any other plain value, such as a decimal, SQLite's operators run. The
/// bit operators are SQLite's.
fn integer_operators(width: IntegerWidth) -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Type, Untyped, UntypedInteger, Value};
    let signature = |op, args: Vec<OperatorArg>, function: Option<String>, result: &'static str| {
        OperatorSignature {
            op,
            args,
            result: function.as_ref().map(|_| result),
            func: function,
        }
    };
    let mut signatures = Vec::new();
    for (op, suffix) in [
        ("+", "pl"),
        ("-", "mi"),
        ("*", "mul"),
        ("/", "div"),
        ("%", "mod"),
        ("&", "and"),
        ("|", "or"),
    ] {
        let own = || Some(format!("{}{suffix}", width.prefix));
        let with_literal = || Some(format!("{}{suffix}", width.literal_prefix));
        let own_type = width.type_name;
        signatures.push(signature(op, vec![Value, Value], own(), own_type));
        for narrower in width.narrower {
            signatures.push(signature(op, vec![Value, Type(narrower)], own(), own_type));
            signatures.push(signature(op, vec![Type(narrower), Value], own(), own_type));
        }
        let literal_type = width.literal_type_name;
        signatures.push(signature(
            op,
            vec![Value, UntypedInteger],
            with_literal(),
            literal_type,
        ));
        signatures.push(signature(
            op,
            vec![UntypedInteger, Value],
            with_literal(),
            literal_type,
        ));
        signatures.push(signature(op, vec![Value, Untyped], None, own_type));
        signatures.push(signature(op, vec![Untyped, Value], None, own_type));
    }
    for op in ["<<", ">>"] {
        signatures.push(signature(
            op,
            vec![Value, PlainValue],
            None,
            width.type_name,
        ));
        signatures.push(signature(
            op,
            vec![PlainValue, Value],
            None,
            width.type_name,
        ));
    }
    signatures.push(signature(
        "-",
        vec![Value],
        Some(format!("{}um", width.prefix)),
        width.type_name,
    ));
    signatures.push(signature(
        "~",
        vec![Value],
        Some(format!("{}not", width.prefix)),
        width.type_name,
    ));
    signatures
}

#[derive(Clone, Copy)]
pub(crate) enum IntegerOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    And,
    Or,
}

/// `int2pl`, `int4mi`, `int8mul` and the like: the result of the operation
/// as an integer of `bits` bits.
pub(crate) fn exec_integer_arithmetic(
    args: &[Register],
    operation: IntegerOperation,
    bits: u32,
) -> Result<Value> {
    let (left, right) = (args[0].get_value(), args[1].get_value());
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let (Some(left), Some(right)) = (integer(left)?, integer(right)?) else {
        return float_arithmetic(left, right, operation);
    };
    let result = match operation {
        IntegerOperation::Add => left.checked_add(right),
        IntegerOperation::Subtract => left.checked_sub(right),
        IntegerOperation::Multiply => left.checked_mul(right),
        IntegerOperation::Divide | IntegerOperation::Remainder if right == 0 => {
            return Err(LimboError::DivisionByZero);
        }
        IntegerOperation::Divide => left.checked_div(right),
        IntegerOperation::Remainder => Some(left.checked_rem(right).unwrap_or(0)),
        IntegerOperation::And => Some(left & right),
        IntegerOperation::Or => Some(left | right),
    };
    fit(result, bits)
}

/// `int2not`, `int4not` and `int8not`: the complement of an integer.
pub(crate) fn exec_integer_not(value: &Value) -> Result<Value> {
    match integer(value)? {
        Some(number) => Ok(Value::from_i64(!number)),
        None => match value {
            Value::Null => Ok(Value::Null),
            other => Ok(number(other).map_or(Value::Null, |n| Value::from_i64(!(n as i64)))),
        },
    }
}

/// `int2um`, `int4um` and `int8um`: the negation as an integer of `bits` bits.
pub(crate) fn exec_integer_negate(value: &Value, bits: u32) -> Result<Value> {
    match integer(value)? {
        Some(number) => fit(number.checked_neg(), bits),
        None => match value {
            Value::Null => Ok(Value::Null),
            other => Ok(number(other).map_or(Value::Null, |n| Value::from_f64(-n))),
        },
    }
}

fn integer(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Numeric(Numeric::Integer(n)) => Ok(Some(*n)),
        Value::Text(text) => text.as_str().trim().parse().map(Some).map_err(|_| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type integer: \"{}\"",
                    text.as_str()
                ),
                None,
            )
        }),
        _ => Ok(None),
    }
}

/// SQLite's arithmetic, for a value that is not an integer.
fn float_arithmetic(left: &Value, right: &Value, operation: IntegerOperation) -> Result<Value> {
    let (Some(left), Some(right)) = (number(left), number(right)) else {
        return Ok(Value::Null);
    };
    Ok(match operation {
        IntegerOperation::Add => Value::from_f64(left + right),
        IntegerOperation::Subtract => Value::from_f64(left - right),
        IntegerOperation::Multiply => Value::from_f64(left * right),
        IntegerOperation::Divide if right == 0.0 => Value::Null,
        IntegerOperation::Divide => Value::from_f64(left / right),
        IntegerOperation::Remainder if right == 0.0 => Value::Null,
        IntegerOperation::Remainder => Value::from_f64(left % right),
        IntegerOperation::And => Value::from_i64(left as i64 & right as i64),
        IntegerOperation::Or => Value::from_i64(left as i64 | right as i64),
    })
}

fn fit(result: Option<i64>, bits: u32) -> Result<Value> {
    let fits = |n: i64| match bits {
        16 => i16::try_from(n).is_ok(),
        32 => i32::try_from(n).is_ok(),
        _ => true,
    };
    match result {
        Some(n) if fits(n) => Ok(Value::from_i64(n)),
        _ => Err(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("{} out of range", integer_type_name(bits)),
            None,
        )),
    }
}

fn integer_type_name(bits: u32) -> &'static str {
    match bits {
        16 => "smallint",
        32 => "integer",
        _ => "bigint",
    }
}

/// float4 with float4 is float4. With any other number it is float8, which is
/// SQLite's arithmetic on the stored value.
fn float4_operators() -> Vec<OperatorSignature> {
    let float4_function = |op| match op {
        "+" => Some("float4pl"),
        "-" => Some("float4mi"),
        "*" => Some("float4mul"),
        "/" => Some("float4div"),
        _ => None,
    };
    let mut signatures = Vec::new();
    for op in BINARY_OPERATORS {
        let Some(function) = float4_function(op) else {
            continue;
        };
        signatures.push(OperatorSignature {
            op,
            args: vec![OperatorArg::Value, OperatorArg::Value],
            func: Some(function.to_string()),
            result: Some("float4"),
        });
        for args in [
            [OperatorArg::Value, OperatorArg::PlainValue],
            [OperatorArg::PlainValue, OperatorArg::Value],
        ] {
            signatures.push(OperatorSignature {
                op,
                args: args.to_vec(),
                func: None,
                result: None,
            });
        }
    }
    signatures.push(OperatorSignature {
        op: "-",
        args: vec![OperatorArg::Value],
        func: Some("float4um".to_string()),
        result: Some("float4"),
    });
    signatures
}

/// `float4pl`, `float4mi`, `float4mul` and `float4div`: the result of the
/// operation as a float4.
pub(crate) fn exec_float4_arithmetic(
    args: &[Register],
    operation: fn(f64, f64) -> f64,
    divides: bool,
) -> Result<Value> {
    let (Some(left), Some(right)) = (number(args[0].get_value()), number(args[1].get_value()))
    else {
        return Ok(Value::Null);
    };
    if divides && right == 0.0 {
        return Err(LimboError::DivisionByZero);
    }
    narrow(
        operation(left, right),
        left.is_finite() && right.is_finite(),
    )
}

pub(crate) fn exec_float4_negate(value: &Value) -> Result<Value> {
    match number(value) {
        Some(number) => Ok(Value::from_f64(-number)),
        None => Ok(Value::Null),
    }
}

fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Numeric(Numeric::Integer(n)) => Some(*n as f64),
        Value::Numeric(Numeric::Float(f)) => Some(f64::from(*f)),
        _ => None,
    }
}

/// The value as a float4. A result that is out of range for a float4 while
/// its operands were not is an error.
fn narrow(result: f64, operands_are_finite: bool) -> Result<Value> {
    let narrowed = result as f32;
    if narrowed.is_infinite() && operands_are_finite {
        return Err(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            "value out of range: overflow",
            None,
        ));
    }
    Ok(Value::from_f64(f64::from(narrowed)))
}
