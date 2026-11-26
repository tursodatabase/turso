//! PostgreSQL's integer types: smallint (int2), integer (int4) and bigint
//! (int8).

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::schema::{
    OperatorArg, OperatorSignature, RangeOffsets, BINARY_OPERATORS, PREFIX_OPERATORS,
};
use turso_core::types::{Conversion, TextInput, Type};
use turso_core::{Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// One of PostgreSQL's integer types. A value is stored as an integer.
/// The variants are in the order of their ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IntegerType {
    Int2,
    Int4,
    Int8,
}

impl IntegerType {
    /// The name PostgreSQL gives the type in its messages.
    pub fn sql_name(self) -> &'static str {
        match self {
            IntegerType::Int2 => "smallint",
            IntegerType::Int4 => "integer",
            IntegerType::Int8 => "bigint",
        }
    }

    /// The number in the type's name in pg_type: 2, 4 or 8.
    pub(crate) fn digits(self) -> u8 {
        match self {
            IntegerType::Int2 => 2,
            IntegerType::Int4 => 4,
            IntegerType::Int8 => 8,
        }
    }

    pub(crate) fn contains(self, integer: i64) -> bool {
        (self.min()..=self.max()).contains(&integer)
    }

    fn min(self) -> i64 {
        match self {
            IntegerType::Int2 => i64::from(i16::MIN),
            IntegerType::Int4 => i64::from(i32::MIN),
            IntegerType::Int8 => i64::MIN,
        }
    }

    fn max(self) -> i64 {
        match self {
            IntegerType::Int2 => i64::from(i16::MAX),
            IntegerType::Int4 => i64::from(i32::MAX),
            IntegerType::Int8 => i64::MAX,
        }
    }

    fn out_of_range(self) -> turso_core::LimboError {
        sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("{} out of range", self.sql_name()),
            None,
        )
    }

    fn check_range(self, value: &Value) -> Result<Value> {
        match value {
            Value::Numeric(Numeric::Integer(integer)) if self.contains(*integer) => {
                Ok(value.clone())
            }
            _ => Err(self.out_of_range()),
        }
    }

    /// The integer that `text` is as input of the type, with the rules of
    /// PostgreSQL's integer input: white space around the number, a sign,
    /// decimal digits or `0x`, `0o` or `0b` digits, and `_` between digits.
    pub fn parse(self, text: &str) -> Result<i64> {
        let invalid = || {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type {}: \"{text}\"",
                    self.sql_name()
                ),
                None,
            )
        };
        let out_of_range = || {
            sqlstate_error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                format!(
                    "value \"{text}\" is out of range for type {}",
                    self.sql_name()
                ),
                None,
            )
        };
        let (negative, unsigned) = split_sign(text);
        let (radix, digits) = match unsigned.as_bytes() {
            [b'0', b'x' | b'X', ..] => (16, &unsigned[2..]),
            [b'0', b'o' | b'O', ..] => (8, &unsigned[2..]),
            [b'0', b'b' | b'B', ..] => (2, &unsigned[2..]),
            _ => (10, unsigned),
        };
        let has_prefix = radix != 10;
        let largest_magnitude = self.min().unsigned_abs();
        let mut magnitude: u64 = 0;
        let mut previous_was_digit = false;
        let mut bytes = digits.bytes().peekable();
        if bytes.peek().is_none() {
            return Err(invalid());
        }
        while let Some(byte) = bytes.next() {
            if byte == b'_' {
                let digit_follows = bytes
                    .peek()
                    .is_some_and(|next| char::from(*next).is_digit(radix));
                if !(digit_follows && (previous_was_digit || has_prefix)) {
                    return Err(invalid());
                }
                previous_was_digit = false;
                continue;
            }
            let Some(digit) = char::from(byte).to_digit(radix) else {
                return Err(invalid());
            };
            magnitude = magnitude
                .checked_mul(u64::from(radix))
                .and_then(|shifted| shifted.checked_add(u64::from(digit)))
                .filter(|magnitude| *magnitude <= largest_magnitude)
                .ok_or_else(out_of_range)?;
            previous_was_digit = true;
        }
        if negative {
            Ok(0i64.wrapping_sub_unsigned(magnitude))
        } else {
            i64::try_from(magnitude)
                .ok()
                .filter(|integer| *integer <= self.max())
                .ok_or_else(out_of_range)
        }
    }

    /// FIXME(pg-catalog): The cast table of casts.rs decides a cast to an integer type when it can name both types, and then this function is not asked. It still converts for the casts that the engine decides: a cast to an array of the type or to a domain over it, and a cast from a type that has no pg_cast rows yet, such as uuid, which PostgreSQL refuses. It sees the value but not the type it is cast from, so a decimal rounds half away from zero, as numeric does, where PostgreSQL rounds a float8 half to even. This function goes away when the cast table decides those casts too.
    fn cast(self, value: &Value) -> Result<Value> {
        match value {
            Value::Text(text) => Ok(Value::from_i64(self.parse(text.as_str())?)),
            Value::Numeric(Numeric::Float(float)) => {
                Ok(Value::from_i64(f64::from(*float).round() as i64))
            }
            other => Ok(other.clone()),
        }
    }
}

/// Whether `text` is negative, and the digits that follow the sign, as
/// PostgreSQL's integer and oid input read them: white space around the
/// number does not count, and one `+` or `-` may come first.
pub(crate) fn split_sign(text: &str) -> (bool, &str) {
    let number = text.trim_matches([' ', '\t', '\n', '\r', '\u{b}', '\u{c}']);
    match number.as_bytes().first() {
        Some(b'-') => (true, &number[1..]),
        Some(b'+') => (false, &number[1..]),
        _ => (false, number),
    }
}

impl Type for IntegerType {
    /// FIXME(pg-catalog): integer is registered as `int4`, because `integer` is the name of the storage type of the engine, so an error that names the type says `int4` where PostgreSQL says `integer`. format_type on the pg_type row of the type replaces the registered name in messages.
    fn name(&self) -> &str {
        match self {
            IntegerType::Int2 => "smallint",
            IntegerType::Int4 => "int4",
            IntegerType::Int8 => "bigint",
        }
    }

    fn aliases(&self) -> &[&str] {
        match self {
            IntegerType::Int2 => &["int2"],
            IntegerType::Int4 => &[],
            IntegerType::Int8 => &["int8"],
        }
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("integer".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn coerce(&self) -> Option<Conversion> {
        match self {
            IntegerType::Int2 => Some(Conversion::Function(int2_coerce)),
            IntegerType::Int4 => Some(Conversion::Function(int4_coerce)),
            IntegerType::Int8 => None,
        }
    }

    fn input(&self) -> Option<TextInput> {
        Some(match self {
            IntegerType::Int2 => |text| IntegerType::Int2.parse(text).map(Value::from_i64),
            IntegerType::Int4 => |text| IntegerType::Int4.parse(text).map(Value::from_i64),
            IntegerType::Int8 => |text| IntegerType::Int8.parse(text).map(Value::from_i64),
        })
    }

    fn cast(&self) -> Option<Conversion> {
        Some(Conversion::Function(match self {
            IntegerType::Int2 => int2_cast,
            IntegerType::Int4 => int4_cast,
            IntegerType::Int8 => int8_cast,
        }))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::Integer
    }

    fn can_be_rowid_alias(&self) -> bool {
        true
    }

    /// FIXME(pg-catalog): The operators that a pg_operator row takes with exactly the types of the operands run the function of the row (see `IntegerOperator`). These signatures run SQLite's operator for every other pair, such as `smallint % integer` and `integer + numeric`, where PostgreSQL casts an operand first. The implicit casts of pg_cast replace this.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        let binary = BINARY_OPERATORS.into_iter().flat_map(|op| {
            [
                [OperatorArg::Value, OperatorArg::PlainValue],
                [OperatorArg::PlainValue, OperatorArg::Value],
            ]
            .map(|args| OperatorSignature {
                op,
                args: args.to_vec(),
                func: None,
                result: None,
            })
        });
        let prefix = PREFIX_OPERATORS.into_iter().map(|op| OperatorSignature {
            op,
            args: vec![OperatorArg::Value],
            func: None,
            result: None,
        });
        binary.chain(prefix).collect()
    }
}

fn int2_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    IntegerType::Int2.check_range(value)
}

fn int4_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    IntegerType::Int4.check_range(value)
}

fn int2_cast(value: &Value, _params: &[Value]) -> Result<Value> {
    IntegerType::Int2.cast(value)
}

fn int4_cast(value: &Value, _params: &[Value]) -> Result<Value> {
    IntegerType::Int4.cast(value)
}

fn int8_cast(value: &Value, _params: &[Value]) -> Result<Value> {
    IntegerType::Int8.cast(value)
}

#[cfg(test)]
mod tests {
    use super::IntegerType;

    fn message(result: turso_core::Result<i64>) -> String {
        result.unwrap_err().to_string()
    }

    #[test]
    fn reads_what_postgres_reads_as_an_integer() {
        for (text, expected) in [
            ("42", 42),
            (" \t42\n ", 42),
            ("+7", 7),
            ("-7", -7),
            ("007", 7),
            ("1_000", 1000),
            ("0x1F", 31),
            ("0X1f", 31),
            ("0o17", 15),
            ("0b101", 5),
            ("0x_1F", 31),
            ("-0x80000000", -2147483648),
            ("2147483647", 2147483647),
            ("-2147483648", -2147483648),
        ] {
            assert_eq!(IntegerType::Int4.parse(text).unwrap(), expected, "{text}");
        }
        assert_eq!(IntegerType::Int2.parse("-32768").unwrap(), -32768);
        assert_eq!(
            IntegerType::Int8.parse("-9223372036854775808").unwrap(),
            i64::MIN
        );
        assert_eq!(
            IntegerType::Int8.parse("9223372036854775807").unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn refuses_text_that_is_not_an_integer() {
        for text in [
            "", " ", "abc", "1.5", "1e2", "1 2", "--1", "+", "0x", "1_", "_1", "1__0", "12abc",
            "0b102",
        ] {
            assert_eq!(
                message(IntegerType::Int4.parse(text)),
                format!("invalid input syntax for type integer: \"{text}\""),
            );
        }
    }

    #[test]
    fn refuses_an_integer_outside_the_range_of_the_type() {
        for (ty, text) in [
            (IntegerType::Int2, "32768"),
            (IntegerType::Int2, "-32769"),
            (IntegerType::Int4, "2147483648"),
            (IntegerType::Int4, "-2147483649"),
            (IntegerType::Int4, "0xFFFFFFFF"),
            (IntegerType::Int4, "99999999999abc"),
            (IntegerType::Int8, "9223372036854775808"),
            (IntegerType::Int8, "-9223372036854775809"),
            (IntegerType::Int8, "99999999999999999999999999"),
        ] {
            assert_eq!(
                message(ty.parse(text)),
                format!(
                    "value \"{text}\" is out of range for type {}",
                    ty.sql_name()
                ),
            );
        }
    }
}
