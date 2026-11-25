//! PostgreSQL's numeric type.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::schema::{sqlite_operator, OperatorArg, OperatorSignature, RangeOffsets};
use turso_core::type_ordering::TypeOrdering;
use turso_core::types::{Conversion, StorageClass, Type};
use turso_core::{
    exec_numeric_decode, exec_numeric_encode, exec_numeric_fit, LimboError, Result, Value,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// PostgreSQL's numeric. A value is stored as a blob that holds the exact
/// decimal, as the numeric of core stores it, and is read as text. It differs
/// from that type in the errors that it raises: PostgreSQL's SQLSTATEs in
/// place of a constraint error.
#[derive(Debug)]
pub(super) struct Numeric;

impl Type for Numeric {
    fn name(&self) -> &str {
        "numeric"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![
            param("value", "any"),
            param("precision", "integer"),
            param("scale", "integer"),
        ]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_encode))
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_coerce))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(numeric_decode))
    }

    fn params_may_be_omitted(&self) -> bool {
        true
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Numeric
    }

    fn ordering(&self) -> Option<TypeOrdering> {
        Some(TypeOrdering::Numeric)
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }

    /// `numeric op x` and `x op numeric` for `+ - * / %`, and `-numeric`,
    /// compute exactly and give a numeric. With a real value `+ - * /` give a
    /// real, as PostgreSQL gives double precision, and `%` does not exist.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        use OperatorArg::{Exact, Real, Value};
        let mut signatures = Vec::new();
        for (op, function) in [
            ("+", "numeric_add"),
            ("-", "numeric_sub"),
            ("*", "numeric_mul"),
            ("/", "numeric_div"),
            ("%", "numeric_mod"),
        ] {
            for args in [[Value, Value], [Value, Exact], [Exact, Value]] {
                signatures.push(numeric_call(op, &args, function));
            }
            if op != "%" {
                signatures.extend(sqlite_operator(op, Real));
            }
        }
        signatures.push(numeric_call("-", &[Value], "numeric_neg"));
        signatures
    }
}

fn param(name: &str, ty: &str) -> ast::TypeParam {
    ast::TypeParam {
        name: name.to_string(),
        ty: Some(ty.to_string()),
    }
}

fn numeric_call(op: &'static str, args: &[OperatorArg], function: &str) -> OperatorSignature {
    OperatorSignature {
        op,
        args: args.to_vec(),
        func: Some(function.to_string()),
        result: Some("numeric"),
    }
}

/// Fails as a numeric column does when `text` is not a number that numeric
/// takes.
pub fn check_pg_numeric(text: &str) -> Result<()> {
    numeric_encode(&Value::build_text(text.to_string()), &[]).map(|_| ())
}

fn numeric_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_numeric_encode(value).map_err(numeric_error)
}

fn numeric_coerce(value: &Value, params: &[Value]) -> Result<Value> {
    exec_numeric_fit(value, &params[0], &params[1]).map_err(numeric_error)
}

fn numeric_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    exec_numeric_decode(value).map_err(numeric_error)
}

/// Gives the errors of core's numeric the SQLSTATE that PostgreSQL gives.
fn numeric_error(error: LimboError) -> LimboError {
    let LimboError::Constraint(message) = &error else {
        return error;
    };
    let sqlstate = if message.starts_with("invalid input syntax for type numeric") {
        SQLSTATE_INVALID_TEXT_REPRESENTATION
    } else if message.starts_with("value overflows numeric format")
        || message.starts_with("numeric value out of range")
    {
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE
    } else {
        return error;
    };
    sqlstate_error(sqlstate, message.clone(), None)
}
