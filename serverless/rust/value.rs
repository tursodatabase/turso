use std::str::FromStr;

use crate::{Error, Result};

/// A SQL value.
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

impl Value {
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&String> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Value {
        Value::Integer(value as i64)
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

impl<const N: usize> From<[u8; N]> for Value {
    fn from(value: [u8; N]) -> Value {
        Value::Blob(value.to_vec())
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

/// A borrowed version of `Value`.
#[derive(Debug)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

impl ValueRef<'_> {
    pub fn data_type(&self) -> ValueType {
        match *self {
            ValueRef::Null => ValueType::Null,
            ValueRef::Integer(_) => ValueType::Integer,
            ValueRef::Real(_) => ValueType::Real,
            ValueRef::Text(_) => ValueType::Text,
            ValueRef::Blob(_) => ValueType::Blob,
        }
    }

    /// Returns `true` if the value ref is [`Null`].
    ///
    /// [`Null`]: ValueRef::Null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns `true` if the value ref is [`Integer`].
    ///
    /// [`Integer`]: ValueRef::Integer
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Real`].
    ///
    /// [`Real`]: ValueRef::Real
    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Text`].
    ///
    /// [`Text`]: ValueRef::Text
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&[u8]> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Blob`].
    ///
    /// [`Blob`]: ValueRef::Blob
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(vr: ValueRef<'_>) -> Value {
        match vr {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(r) => Value::Real(r),
            ValueRef::Text(s) => Value::Text(String::from_utf8_lossy(s).to_string()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &str) -> ValueRef<'_> {
        ValueRef::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(s: &[u8]) -> ValueRef<'_> {
        ValueRef::Blob(s)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(v: &'a Value) -> ValueRef<'a> {
        match *v {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(i),
            Value::Real(r) => ValueRef::Real(r),
            Value::Text(ref s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(ref b) => ValueRef::Blob(b),
        }
    }
}

impl<'a, T> From<Option<T>> for ValueRef<'a>
where
    T: Into<ValueRef<'a>>,
{
    #[inline]
    fn from(s: Option<T>) -> ValueRef<'a> {
        match s {
            Some(x) => x.into(),
            None => ValueRef::Null,
        }
    }
}

/// Converts a SQL [`Value`] into a Rust type. Used by [`crate::Row::get`].
pub trait FromValue: Sized {
    fn from_value(value: Value) -> Result<Self>;
}

impl FromValue for Value {
    fn from_value(value: Value) -> Result<Self> {
        Ok(value)
    }
}

impl FromValue for i32 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => i32::try_from(n)
                .map_err(|_| Error::ConversionFailure(format!("integer {n} out of i32 range"))),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromValue for u32 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => u32::try_from(n)
                .map_err(|_| Error::ConversionFailure(format!("integer {n} out of u32 range"))),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromValue for u64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => u64::try_from(n)
                .map_err(|_| Error::ConversionFailure(format!("integer {n} out of u64 range"))),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Real(n) => Ok(n),
            Value::Integer(n) => Ok(n as f64),
            _ => Err(Error::ConversionFailure(format!(
                "expected float, got {value:?}"
            ))),
        }
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(Error::ConversionFailure(format!(
                "expected text, got {value:?}"
            ))),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Blob(b) => Ok(b),
            _ => Err(Error::ConversionFailure(format!(
                "expected blob, got {value:?}"
            ))),
        }
    }
}

impl<const N: usize> FromValue for [u8; N] {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Blob(b) => {
                let len = b.len();
                b.try_into().map_err(|_| {
                    Error::ConversionFailure(format!("expected blob of {N} bytes, got {len}"))
                })
            }
            _ => Err(Error::ConversionFailure(format!(
                "expected blob, got {value:?}"
            ))),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            Value::Integer(n) => Err(Error::ConversionFailure(format!(
                "expected 0 or 1 for bool, got {n}"
            ))),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer for bool, got {value:?}"
            ))),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            other => Ok(Some(T::from_value(other)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_conversions_are_checked() {
        assert_eq!(i32::from_value(Value::Integer(-5)).unwrap(), -5);
        i32::from_value(Value::Integer(i64::from(i32::MAX) + 1)).unwrap_err();
        i32::from_value(Value::Integer(i64::from(i32::MIN) - 1)).unwrap_err();

        assert_eq!(u32::from_value(Value::Integer(7)).unwrap(), 7);
        u32::from_value(Value::Integer(-1)).unwrap_err();
        u32::from_value(Value::Integer(i64::from(u32::MAX) + 1)).unwrap_err();

        assert_eq!(
            u64::from_value(Value::Integer(i64::MAX)).unwrap(),
            i64::MAX as u64
        );
        u64::from_value(Value::Integer(-1)).unwrap_err();
    }

    #[test]
    fn f64_accepts_integers() {
        assert_eq!(f64::from_value(Value::Integer(3)).unwrap(), 3.0);
        assert_eq!(f64::from_value(Value::Real(1.5)).unwrap(), 1.5);
        f64::from_value(Value::Text("x".to_string())).unwrap_err();
    }

    #[test]
    fn bool_accepts_only_zero_and_one() {
        assert!(!bool::from_value(Value::Integer(0)).unwrap());
        assert!(bool::from_value(Value::Integer(1)).unwrap());
        bool::from_value(Value::Integer(2)).unwrap_err();
        bool::from_value(Value::Integer(-1)).unwrap_err();
        bool::from_value(Value::Text("true".to_string())).unwrap_err();
    }

    #[test]
    fn fixed_size_byte_arrays_require_exact_length() {
        assert_eq!(
            <[u8; 3]>::from_value(Value::Blob(vec![1, 2, 3])).unwrap(),
            [1, 2, 3]
        );
        <[u8; 3]>::from_value(Value::Blob(vec![1, 2])).unwrap_err();
        <[u8; 3]>::from_value(Value::Integer(1)).unwrap_err();
    }

    #[test]
    fn option_maps_null() {
        assert_eq!(Option::<i64>::from_value(Value::Null).unwrap(), None);
        assert_eq!(
            Option::<i64>::from_value(Value::Integer(4)).unwrap(),
            Some(4)
        );
    }
}
