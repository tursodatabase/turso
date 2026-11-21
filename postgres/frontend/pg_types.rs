//! The scalar types that PostgreSQL has and the engine does not.

use crate::error::sqlstate_error;
use std::sync::Arc;
use turso_core::schema::{
    OperatorArg, OperatorSignature, RangeOffsets, SchemaBuilder, TypeDef, TypeDefKind, TypeOrigin,
    BINARY_OPERATORS,
};
use turso_core::{LimboError, Numeric, Register, Result, Value};
use turso_parser::ast::{Cmd, Stmt};
use turso_pg_parser::sqlstate::SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE;

const FLOAT4: &str = "CREATE TYPE float4(value real) BASE real COERCE pg_float4_round(value)";

pub(crate) fn register_scalar_types(
    dialect: &crate::catalog::PostgresDialect,
    builder: SchemaBuilder,
) -> Result<SchemaBuilder> {
    let Ok(Some(Cmd::Stmt(Stmt::CreateType {
        type_name, body, ..
    }))) = turso_core::dialect::parse_statement(dialect, FLOAT4)
    else {
        return Err(LimboError::InternalError(format!(
            "failed to parse built-in type SQL: {FLOAT4}"
        )));
    };
    let origin = TypeOrigin::Builtin {
        accepts_collate: false,
        range_offsets: RangeOffsets::Numeric,
        stored_form_sorts_in_value_order: true,
        can_be_rowid_alias: false,
    };
    let mut type_def = TypeDef::from_create_type(&type_name, &body, origin, FLOAT4.to_string())?;
    if let TypeDefKind::Custom {
        operator_signatures,
        ..
    } = &mut type_def.kind
    {
        *operator_signatures = float4_operators();
    }
    Ok(builder.type_def(&type_name, Arc::new(type_def)))
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
