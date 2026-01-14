//! SQL value types and generation strategies.

use proptest::prelude::*;
use proptest::string::string_regex;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::profile::StatementProfile;
use crate::schema::DataType;

// =============================================================================
// VALUE GENERATION PROFILE
// =============================================================================

/// Profile for controlling value generation parameters.
#[derive(Debug, Clone)]
pub struct ValueProfile {
    /// Maximum length for generated text values.
    pub text_max_length: usize,
    /// Maximum size for generated blob values.
    pub blob_max_size: usize,
    /// Pattern for text generation (regex pattern).
    pub text_pattern: String,
}

impl Default for ValueProfile {
    fn default() -> Self {
        Self {
            text_max_length: 100,
            blob_max_size: 100,
            text_pattern: "[a-zA-Z0-9_ ]{0,100}".to_string(),
        }
    }
}

impl ValueProfile {
    /// Create a profile with minimal value sizes (for faster tests).
    pub fn minimal() -> Self {
        Self {
            text_max_length: 10,
            blob_max_size: 10,
            text_pattern: "[a-z]{0,10}".to_string(),
        }
    }

    /// Create a profile with large value sizes.
    pub fn large() -> Self {
        Self {
            text_max_length: 1000,
            blob_max_size: 1000,
            text_pattern: "[a-zA-Z0-9_ ]{0,1000}".to_string(),
        }
    }

    /// Builder method to set the text max length.
    pub fn with_text_max_length(mut self, length: usize) -> Self {
        self.text_max_length = length;
        self
    }

    /// Builder method to set the blob max size.
    pub fn with_blob_max_size(mut self, size: usize) -> Self {
        self.blob_max_size = size;
        self
    }

    /// Builder method to set the text pattern.
    pub fn with_text_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.text_pattern = pattern.into();
        self
    }
}

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

impl Eq for SqlValue {}

impl Hash for SqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            SqlValue::Integer(i) => i.hash(state),
            SqlValue::Real(f) => {
                // Hash the bit representation for consistency
                f.to_bits().hash(state);
            }
            SqlValue::Text(s) => s.hash(state),
            SqlValue::Blob(b) => b.hash(state),
            SqlValue::Null => {}
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

/// Generate a text value with profile-controlled parameters.
pub fn text_value(profile: &StatementProfile) -> impl Strategy<Value = SqlValue> + 'static {
    let value_profile = &profile.generation.value;
    string_regex(&value_profile.text_pattern)
        .unwrap()
        .prop_map(SqlValue::Text)
}

/// Generate a blob value with profile-controlled parameters.
pub fn blob_value(profile: &StatementProfile) -> impl Strategy<Value = SqlValue> + 'static {
    let max_size = profile.generation.value.blob_max_size;
    proptest::collection::vec(any::<u8>(), 0..=max_size).prop_map(SqlValue::Blob)
}

/// Generate a NULL value.
pub fn null_value() -> impl Strategy<Value = SqlValue> {
    Just(SqlValue::Null)
}

/// Generate a value appropriate for the given data type with profile.
pub fn value_for_type(
    data_type: &DataType,
    nullable: bool,
    profile: &StatementProfile,
) -> BoxedStrategy<SqlValue> {
    let base: BoxedStrategy<SqlValue> = match data_type {
        DataType::Integer => integer_value().boxed(),
        DataType::Real => real_value().boxed(),
        DataType::Text => text_value(profile).boxed(),
        DataType::Blob => blob_value(profile).boxed(),
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
