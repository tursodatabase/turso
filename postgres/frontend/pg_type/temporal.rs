//! PostgreSQL's date, timestamp and timestamptz types.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::schema::{OperatorArg, OperatorSignature};
use turso_core::types::{Conversion, TextInput, Type};
use turso_core::{exec_temporal_encode, LimboError, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DATETIME_FIELD_OVERFLOW, SQLSTATE_INVALID_DATETIME_FORMAT,
};

/// A date, a timestamp or a timestamptz. A value is stored as the text that
/// the date and time types of core store, and read back as that text. The
/// types differ from the ones in core in that they have an INPUT.
#[derive(Debug)]
pub(super) struct Temporal(Kind);

#[derive(Debug, Clone, Copy)]
enum Kind {
    Date,
    Timestamp,
    Timestamptz,
}

pub(super) const DATE: Temporal = Temporal(Kind::Date);
pub(super) const TIMESTAMP: Temporal = Temporal(Kind::Timestamp);
pub(super) const TIMESTAMPTZ: Temporal = Temporal(Kind::Timestamptz);

impl Type for Temporal {
    fn name(&self) -> &str {
        match self.0 {
            Kind::Date => "date",
            Kind::Timestamp => "timestamp",
            Kind::Timestamptz => "timestamptz",
        }
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("text".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn input(&self) -> Option<TextInput> {
        Some(match self.0 {
            Kind::Date => date_input,
            Kind::Timestamp => timestamp_input,
            Kind::Timestamptz => timestamptz_input,
        })
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(match self.0 {
            Kind::Date => date_encode,
            Kind::Timestamp => timestamp_encode,
            Kind::Timestamptz => timestamptz_encode,
        }))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    /// `OPERATOR '<'`: the stored text sorts in the order of the values.
    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }

    /// `date + integer`, `integer + date`, `date - integer` and `date -
    /// date`, which add or subtract days or count them.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        use OperatorArg::{EncodableLiteral, Integer, Value};
        match self.0 {
            Kind::Date => vec![
                call("+", &[Value, Integer], "date_plus", "date"),
                call("+", &[Integer, Value], "date_plus", "date"),
                call("-", &[Value, Value], "date_diff", "integer"),
                call("-", &[Value, EncodableLiteral], "date_diff", "integer"),
                call("-", &[EncodableLiteral, Value], "date_diff", "integer"),
                call("-", &[Value, Integer], "date_minus", "date"),
            ],
            Kind::Timestamp | Kind::Timestamptz => Vec::new(),
        }
    }
}

fn call(
    op: &'static str,
    args: &[OperatorArg],
    func: &str,
    result: &'static str,
) -> OperatorSignature {
    OperatorSignature {
        op,
        args: args.to_vec(),
        func: Some(func.to_string()),
        result: Some(result),
    }
}

fn read(value: &Value, type_name: &'static str) -> Result<Value> {
    exec_temporal_encode(value, &Value::build_text(type_name))
}

/// The INPUT reports a bad value with the SQLSTATE of PostgreSQL's message.
fn read_text(text: &str, type_name: &'static str) -> Result<Value> {
    read(&Value::build_text(text.to_string()), type_name).map_err(|error| match error {
        LimboError::InvalidDate(message) => {
            sqlstate_error(SQLSTATE_INVALID_DATETIME_FORMAT, message, None)
        }
        LimboError::DatetimeFieldOverflow(message) => {
            sqlstate_error(SQLSTATE_DATETIME_FIELD_OVERFLOW, message, None)
        }
        other => other,
    })
}

fn date_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    read(value, "date")
}

fn timestamp_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    read(value, "timestamp")
}

fn timestamptz_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    read(value, "timestamptz")
}

fn date_input(text: &str) -> Result<Value> {
    read_text(text, "date")
}

fn timestamp_input(text: &str) -> Result<Value> {
    read_text(text, "timestamp")
}

fn timestamptz_input(text: &str) -> Result<Value> {
    read_text(text, "timestamptz")
}
