use magnus::{value::ReprValue, RBignum, RFloat, RString, TryConvert, Value as RValue};

use std::str::FromStr;

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// The possible types a column can be in libsql.
#[derive(Debug, Copy, Clone)]
pub enum ValueType {
    Integer = 1,
    Real,
    Text,
    Blob,
    Null,
}

impl FromStr for ValueType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<ValueType, Self::Err> {
        match s {
            "TEXT" => Ok(ValueType::Text),
            "INTEGER" => Ok(ValueType::Integer),
            "BLOB" => Ok(ValueType::Blob),
            "NULL" => Ok(ValueType::Null),
            "REAL" => Ok(ValueType::Real),
            _ => Err(()),
        }
    }
}

impl Into<limbo_core::OwnedValue> for Value {
    fn into(self) -> limbo_core::OwnedValue {
        match self {
            Value::Null => limbo_core::OwnedValue::Null,
            Value::Integer(n) => limbo_core::OwnedValue::Integer(n),
            Value::Real(n) => limbo_core::OwnedValue::Float(n),
            Value::Text(t) => limbo_core::OwnedValue::from_text(&t),
            Value::Blob(items) => limbo_core::OwnedValue::from_blob(items),
        }
    }
}

impl TryFrom<RValue> for Value {
    type Error = Error;
    fn try_from(value: RValue) -> Result<Value> {
        if value.is_nil() {
            return Ok(Value::Null);
        }
        if let Ok(int) = RBignum::try_convert(value) {
            return Ok(Value::Integer(int.to_i64().unwrap()));
        }
        if let Ok(float) = RFloat::try_convert(value) {
            return Ok(Value::Real(float.to_f64()));
        }
        if let Ok(text) = RString::try_convert(value) {
            return Ok(Value::Text(text.to_string().unwrap()));
        }
        Err(Error::ConversionFailure(value))
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Value {
        Value::Integer(value)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Value {
        Value::Integer(value as i64)
    }
}

impl TryFrom<u64> for Value {
    type Error = Error;

    fn try_from(value: u64) -> Result<Value> {
        if value > i64::MAX as u64 {
            Err(Error::ToSqlConversionFailure(
                "u64 is too large to fit in an i64".into(),
            ))
        } else {
            Ok(Value::Integer(value as i64))
        }
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Value {
        Value::Real(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Value {
        Value::Real(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::Text(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Value {
        Value::Blob(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Value {
        Value::Blob(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Integer(value as i64)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(inner) => inner.into(),
            None => Value::Null,
        }
    }
}
