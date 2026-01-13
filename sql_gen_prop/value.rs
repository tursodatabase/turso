//! SQL value types and generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::schema::DataType;

/// A SQL literal value.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl PartialEq for SqlValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SqlValue::Null, SqlValue::Null) => true,
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a == b,
            (SqlValue::Real(a), SqlValue::Real(b)) => {
                // Handle NaN and compare with tolerance for floating point
                if a.is_nan() && b.is_nan() {
                    true
                } else if a.is_nan() || b.is_nan() {
                    false
                } else {
                    (a - b).abs() < 1e-10 || a == b
                }
            }
            (SqlValue::Text(a), SqlValue::Text(b)) => a == b,
            (SqlValue::Blob(a), SqlValue::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Integer(i) => write!(f, "{i}"),
            SqlValue::Real(r) => write!(f, "{r}"),
            SqlValue::Text(s) => write!(f, "'{}'", s.replace('\'', "''")),
            SqlValue::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}

/// Generate an integer value.
pub fn integer_value() -> impl Strategy<Value = SqlValue> {
    any::<i64>().prop_map(SqlValue::Integer)
}

/// Generate a real (floating point) value.
pub fn real_value() -> impl Strategy<Value = SqlValue> {
    any::<f64>().prop_map(SqlValue::Real)
}

/// Generate a text value with safe characters.
pub fn text_value() -> impl Strategy<Value = SqlValue> {
    "[a-zA-Z0-9_ ]{0,100}".prop_map(SqlValue::Text)
}

/// Generate a blob value.
pub fn blob_value() -> impl Strategy<Value = SqlValue> {
    proptest::collection::vec(any::<u8>(), 0..100).prop_map(SqlValue::Blob)
}

/// Generate a NULL value.
pub fn null_value() -> impl Strategy<Value = SqlValue> {
    Just(SqlValue::Null)
}

/// Generate a value appropriate for the given data type.
pub fn value_for_type(data_type: &DataType, nullable: bool) -> BoxedStrategy<SqlValue> {
    let base: BoxedStrategy<SqlValue> = match data_type {
        DataType::Integer => integer_value().boxed(),
        DataType::Real => real_value().boxed(),
        DataType::Text => text_value().boxed(),
        DataType::Blob => blob_value().boxed(),
        DataType::Null => null_value().boxed(),
    };

    if nullable {
        prop_oneof![base, null_value()].boxed()
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_value_display() {
        assert_eq!(SqlValue::Integer(42).to_string(), "42");
        assert_eq!(SqlValue::Real(3.5).to_string(), "3.5");
        assert_eq!(SqlValue::Text("hello".to_string()).to_string(), "'hello'");
        assert_eq!(SqlValue::Text("it's".to_string()).to_string(), "'it''s'");
        assert_eq!(SqlValue::Blob(vec![0xDE, 0xAD]).to_string(), "X'DEAD'");
        assert_eq!(SqlValue::Null.to_string(), "NULL");
    }
}
