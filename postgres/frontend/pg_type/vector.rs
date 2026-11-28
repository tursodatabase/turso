//! PostgreSQL's int2vector and oidvector.

use super::{FormatName, RegisteredType};
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::type_ordering::TypeOrdering;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{ArrayValue, Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_CANNOT_COERCE, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

const LOWER_BOUND: i32 = 0;

/// An array of numbers that starts at subscript 0, read as numbers separated by spaces.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Vector {
    Int2,
    Oid,
}

impl Vector {
    fn name(self) -> &'static str {
        match self {
            Vector::Int2 => "int2vector",
            Vector::Oid => "oidvector",
        }
    }

    fn element_name(self) -> &'static str {
        match self {
            Vector::Int2 => "smallint",
            Vector::Oid => "oid",
        }
    }

    /// The vector that `text` is, as `int2vectorin` and `oidvectorin` read it.
    fn read(self, text: &str) -> Result<Value> {
        let mut elements = Vec::new();
        let mut rest = text.trim_start_matches(' ');
        while !rest.is_empty() {
            let token_len = rest.find(' ').unwrap_or(rest.len());
            elements.push(Value::from_i64(
                self.read_element(&rest[..token_len], rest)?,
            ));
            rest = rest[token_len..].trim_start_matches(' ');
        }
        encode(elements)
    }

    /// The element that `token` is, with `rest` quoted in an error as PostgreSQL does.
    fn read_element(self, token: &str, rest: &str) -> Result<i64> {
        let digits = token.strip_prefix(['+', '-']).unwrap_or(token);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type {}: \"{rest}\"",
                    self.element_name()
                ),
                None,
            ));
        }
        let out_of_range = || {
            sqlstate_error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                format!(
                    "value \"{rest}\" is out of range for type {}",
                    self.element_name()
                ),
                None,
            )
        };
        let number: i128 = token.parse().map_err(|_| out_of_range())?;
        self.element(number).ok_or_else(out_of_range)
    }

    /// The element for `number`, where an oid wraps a negative number as `oidin` does.
    fn element(self, number: i128) -> Option<i64> {
        match self {
            Vector::Int2 => i16::try_from(number).ok().map(i64::from),
            Vector::Oid => match number {
                0..=0xFFFF_FFFF => i64::try_from(number).ok(),
                n if (i128::from(i32::MIN)..0).contains(&n) => i64::try_from(n + (1 << 32)).ok(),
                _ => None,
            },
        }
    }

    /// The vector that a value written to a column or cast to the type is.
    fn encode(self, value: &Value) -> Result<Value> {
        match value {
            Value::Text(text) => self.read(text.as_str()),
            Value::Blob(blob) if self.is_vector(blob) => Ok(value.clone()),
            _ => Err(sqlstate_error(
                SQLSTATE_CANNOT_COERCE,
                format!("cannot cast this value to {}", self.name()),
                None,
            )),
        }
    }

    fn is_vector(self, blob: &[u8]) -> bool {
        let Ok(array) = ArrayValue::decode(blob) else {
            return false;
        };
        (array.lower_bound == LOWER_BOUND || array.elements.is_empty())
            && array.elements.iter().all(|element| match element {
                Value::Numeric(Numeric::Integer(number)) => {
                    self.element(i128::from(*number)) == Some(*number)
                }
                _ => false,
            })
    }
}

fn encode(elements: Vec<Value>) -> Result<Value> {
    ArrayValue {
        lower_bound: LOWER_BOUND,
        elements,
    }
    .encode()
}

/// The text of a stored vector: its elements separated by spaces.
pub(crate) fn vector_text(blob: &[u8]) -> Option<String> {
    let array = ArrayValue::decode(blob).ok()?;
    let elements: Vec<String> = array.elements.iter().map(Value::to_string).collect();
    Some(elements.join(" "))
}

fn int2vector_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    Vector::Int2.encode(value)
}

fn oidvector_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    Vector::Oid.encode(value)
}

impl Type for Vector {
    fn name(&self) -> &str {
        Vector::name(*self)
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("any".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn input(&self) -> Option<TextInput> {
        Some(match self {
            Vector::Int2 => |_, text| Vector::Int2.read(text),
            Vector::Oid => |_, text| Vector::Oid.read(text),
        })
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(match self {
            Vector::Int2 => int2vector_encode,
            Vector::Oid => oidvector_encode,
        }))
    }

    fn array_element(&self) -> Option<&str> {
        Some(Vector::element_name(*self))
    }

    fn ordering(&self) -> Option<TypeOrdering> {
        Some(TypeOrdering::StoredArray)
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Blob)
    }
}

impl RegisteredType for Vector {
    fn format_name(&self) -> FormatName {
        FormatName::Plain(Vector::name(*self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(vector: Vector, text: &str) -> Result<Vec<i64>> {
        let Value::Blob(blob) = vector.read(text)? else {
            panic!("a vector is a blob");
        };
        let array = ArrayValue::decode(&blob).unwrap();
        assert!(array.lower_bound == 0 || array.elements.is_empty());
        Ok(array
            .elements
            .iter()
            .map(|element| element.as_int().unwrap())
            .collect())
    }

    #[test]
    fn int2vector_reads_numbers_separated_by_spaces() {
        assert_eq!(read(Vector::Int2, " 1  -2 +3 ").unwrap(), vec![1, -2, 3]);
        assert!(read(Vector::Int2, "").unwrap().is_empty());
    }

    #[test]
    fn int2vector_refuses_other_text() {
        for text in ["1,2", "1 null", "0x10", "1_0", "1\t2", "--2", "-", "1 x 2"] {
            let error = read(Vector::Int2, text).unwrap_err().to_string();
            assert!(error.contains("invalid input syntax"), "{text}: {error}");
        }
    }

    #[test]
    fn int2vector_quotes_the_rest_of_the_text_in_an_error() {
        let error = read(Vector::Int2, "1 x 2").unwrap_err().to_string();
        assert!(error.contains("\"x 2\""), "{error}");
        let error = read(Vector::Int2, "1 70000 2").unwrap_err().to_string();
        assert!(error.contains("\"70000 2\""), "{error}");
    }

    #[test]
    fn int2vector_refuses_a_number_out_of_range() {
        assert!(read(Vector::Int2, "32767 -32768").is_ok());
        assert!(read(Vector::Int2, "32768").is_err());
        assert!(read(Vector::Int2, "-32769").is_err());
    }

    #[test]
    fn oidvector_reads_an_oid_and_wraps_a_negative_number() {
        assert_eq!(
            read(Vector::Oid, "4294967295 0").unwrap(),
            vec![4294967295, 0]
        );
        assert_eq!(read(Vector::Oid, "-1").unwrap(), vec![4294967295]);
        assert!(read(Vector::Oid, "4294967296").is_err());
        assert!(read(Vector::Oid, "-2147483649").is_err());
    }

    #[test]
    fn a_vector_prints_its_elements_separated_by_spaces() {
        let value = Vector::Int2.read("1 2 3").unwrap();
        let Value::Blob(blob) = value else {
            panic!("a vector is a blob");
        };
        assert_eq!(vector_text(&blob).unwrap(), "1 2 3");
    }

    #[test]
    fn a_blob_is_a_vector_only_if_it_starts_at_subscript_zero() {
        let one_based = ArrayValue::new(vec![Value::from_i64(1)]).encode().unwrap();
        assert!(Vector::Int2.encode(&one_based).is_err());
        let zero_based = Vector::Int2.read("1").unwrap();
        assert!(Vector::Int2.encode(&zero_based).is_ok());
    }
}
