//! PostgreSQL's float4 and float8.

use crate::error::sqlstate_error;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use turso_core::ast;
use turso_core::native_ext::{ReturnType, ScalarReturn};
use turso_core::schema::{OperatorArg, OperatorSignature};
use turso_core::type_ordering::{OrderingDef, TypeOrdering};
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{Double, LimboError, Numeric, Register, Result, Value, ValueRef};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_DIVISION_BY_ZERO, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// PostgreSQL's float8, a 64-bit float. A value is stored as the 8 bytes of
/// the float (big endian), which can be a NaN and keeps the sign of a zero.
/// A value is read as a number, which can be NaN.
#[derive(Debug)]
pub(super) struct Float8;

/// PostgreSQL's float4, a 32-bit float. It is stored as a float8 that holds
/// a float4, so it has the same 8 bytes.
#[derive(Debug)]
pub(super) struct Float4;

impl Type for Float8 {
    fn name(&self) -> &str {
        "float8"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        float_params()
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn input(&self) -> Option<TextInput> {
        Some(|text| parse_float_text(text, Precision::Double).map(Value::from_f64))
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| {
            encode(value, Precision::Double)
        }))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(decode))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Real)
    }

    fn ordering(&self) -> Option<TypeOrdering> {
        Some(TypeOrdering::new(&PG_FLOAT))
    }

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        float_operators("float8")
    }
}

impl Type for Float4 {
    fn name(&self) -> &str {
        "float4"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        float_params()
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn input(&self) -> Option<TextInput> {
        Some(|text| parse_float_text(text, Precision::Single).map(Value::from_f64))
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| {
            encode(value, Precision::Single)
        }))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(decode))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Real)
    }

    fn ordering(&self) -> Option<TypeOrdering> {
        Some(TypeOrdering::new(&PG_FLOAT))
    }

    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        float_operators("float4")
    }
}

fn float_params() -> Vec<ast::TypeParam> {
    vec![ast::TypeParam {
        name: "value".to_string(),
        ty: Some("any".to_string()),
    }]
}

/// The operators of a float type. As in PostgreSQL, an operator on two
/// values of the type gives that type, and an operator on a float and
/// anything else gives a float8: an integer, a decimal number, a numeric, or
/// a float of the other type. `own` is the name of the type. PostgreSQL has
/// no other operator for floats.
///
/// FIXME(perf): Each of these runs as a call of a scalar function, with a copy
/// of every operand into argument registers and the generic function dispatch,
/// where SQLite runs a native instruction. A VDBE instruction that calls a
/// whitelisted function on two registers directly removes that cost.
fn float_operators(own: &'static str) -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Registered, Value};
    let mut others = vec![PlainValue, Registered("numeric")];
    if own == "float8" {
        others.push(Registered("float4"));
    }
    let signature = |op, args: Vec<OperatorArg>, func: String, result| OperatorSignature {
        op,
        args,
        func: Some(func),
        result: Some(result),
    };
    let mut signatures = Vec::new();
    for (op, name) in [("+", "pl"), ("-", "mi"), ("*", "mul"), ("/", "div")] {
        signatures.push(signature(
            op,
            vec![Value, Value],
            format!("{own}{name}"),
            own,
        ));
        for other in &others {
            for args in [vec![Value, *other], vec![*other, Value]] {
                signatures.push(signature(op, args, format!("float8{name}"), "float8"));
            }
        }
    }
    signatures.push(signature("-", vec![Value], format!("{own}um"), own));
    signatures
}

#[derive(Clone, Copy, PartialEq)]
enum Precision {
    Single,
    Double,
}

impl Precision {
    fn type_name(self) -> &'static str {
        match self {
            Precision::Single => "real",
            Precision::Double => "double precision",
        }
    }

    /// The nearest number that the type holds. PostgreSQL refuses a finite
    /// number that is too large for a float4 or becomes zero.
    fn round(self, number: f64) -> Result<f64> {
        if self == Precision::Double {
            return Ok(number);
        }
        let rounded = number as f32;
        if rounded.is_infinite() && number.is_finite() {
            return Err(out_of_range("value out of range: overflow"));
        }
        if rounded == 0.0 && number != 0.0 {
            return Err(out_of_range("value out of range: underflow"));
        }
        Ok(f64::from(rounded))
    }
}

/// The bytes of a NaN that is stored, so that every NaN is stored alike.
const NAN_BITS: u64 = 0x7FF8_0000_0000_0000;

/// What a column of the type stores for a value that is written: the 8 bytes
/// of the float. Text is read as PostgreSQL's float input does.
fn encode(value: &Value, precision: Precision) -> Result<Value> {
    let number = match value {
        Value::Text(text) => parse_float_text(text.as_str(), precision)?,
        Value::Numeric(Numeric::Integer(integer)) => {
            round_numeric(Numeric::Integer(*integer), precision)?
        }
        Value::Numeric(Numeric::Float(float)) => round_numeric(Numeric::Float(*float), precision)?,
        other => {
            return Err(sqlstate_error(
                SQLSTATE_DATATYPE_MISMATCH,
                format!("the value {other} is not of type {}", precision.type_name()),
                None,
            ))
        }
    };
    let number = if number.is_nan() {
        f64::from_bits(NAN_BITS)
    } else {
        number
    };
    Ok(Value::from_blob(number.to_be_bytes().to_vec()))
}

/// The number that the stored bytes are.
fn decode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(bytes) => match <[u8; 8]>::try_from(bytes.as_slice()) {
            Ok(bytes) => Ok(Value::from_f64(f64::from_be_bytes(bytes))),
            Err(_) => Err(LimboError::Corrupt(
                "a stored float is not 8 bytes".to_string(),
            )),
        },
        other => Ok(other.clone()),
    }
}

/// Reads text as PostgreSQL's float input does: spaces around the number are
/// allowed, and `NaN`, `Infinity` and `-Infinity` (also `inf`) are numbers in
/// any case. A number that is too large or too small for the type is an
/// error, and so is anything else that is not a number.
fn parse_float_text(input: &str, precision: Precision) -> Result<f64> {
    let trimmed = input.trim_matches(|c: char| c.is_ascii_whitespace() || c == '\u{b}');
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        let number = parse_hex_float(hex, precision)
            .map_err(|error| float_input_error(input, precision, error))?;
        return Ok(if trimmed.starts_with('-') {
            -number
        } else {
            number
        });
    }
    let parsed = match precision {
        Precision::Single => trimmed.parse::<f32>().map(f64::from),
        Precision::Double => trimmed.parse::<f64>(),
    };
    let number =
        parsed.map_err(|_| float_input_error(input, precision, FloatInputError::Syntax))?;
    let is_infinity_word =
        unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity");
    let has_nonzero_digit = trimmed
        .split(['e', 'E'])
        .next()
        .is_some_and(|mantissa| mantissa.chars().any(|c| ('1'..='9').contains(&c)));
    if (number.is_infinite() && !is_infinity_word) || (number == 0.0 && has_nonzero_digit) {
        return Err(float_input_error(input, precision, FloatInputError::Range));
    }
    Ok(number)
}

#[derive(Debug)]
enum FloatInputError {
    Syntax,
    Range,
}

fn float_input_error(input: &str, precision: Precision, error: FloatInputError) -> LimboError {
    let (code, message) = match error {
        FloatInputError::Syntax => (
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!(
                "invalid input syntax for type {}: \"{input}\"",
                precision.type_name()
            ),
        ),
        FloatInputError::Range => (
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!(
                "\"{input}\" is out of range for type {}",
                precision.type_name()
            ),
        ),
    };
    sqlstate_error(code, message, None)
}

/// Reads an unsigned hexadecimal float, after its 0x prefix. Fifteen hex
/// digits retain more than 53 bits of precision. A sticky bit records any
/// nonzero discarded digit so rounding happens once, at the target width,
/// including at the subnormal boundary.
fn parse_hex_float(text: &str, precision: Precision) -> std::result::Result<f64, FloatInputError> {
    let (significand, exponent) = match text.split_once(['p', 'P']) {
        Some((significand, exponent)) => {
            let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(FloatInputError::Syntax);
            }
            let magnitude = digits.bytes().fold(0i64, |n, b| {
                n.saturating_mul(10).saturating_add(i64::from(b - b'0'))
            });
            (
                significand,
                if exponent.starts_with('-') {
                    -magnitude
                } else {
                    magnitude
                },
            )
        }
        None => (text, 0),
    };
    let mut mantissa = 0u64;
    let mut kept = 0;
    let mut discarded = 0i64;
    let mut fraction = 0i64;
    let mut point = false;
    let mut saw_digit = false;
    let mut sticky = false;
    for byte in significand.bytes() {
        if byte == b'.' && !point {
            point = true;
            continue;
        }
        let digit = char::from(byte)
            .to_digit(16)
            .ok_or(FloatInputError::Syntax)?;
        saw_digit = true;
        fraction += i64::from(point);
        if kept == 0 && digit == 0 {
            continue;
        }
        if kept < 15 {
            mantissa = (mantissa << 4) | u64::from(digit);
            kept += 1;
        } else {
            discarded += 1;
            sticky |= digit != 0;
        }
    }
    if !saw_digit {
        return Err(FloatInputError::Syntax);
    }
    if mantissa == 0 {
        return Ok(0.0);
    }
    let exponent = exponent.saturating_add(4 * (discarded - fraction));
    let (bits, min_exponent, max_exponent, bias) = match precision {
        Precision::Single => (24, -126, 127, 127),
        Precision::Double => (53, -1022, 1023, 1023),
    };
    let top_exponent = exponent.saturating_add(i64::from(63 - mantissa.leading_zeros()));
    if top_exponent > max_exponent {
        return Err(FloatInputError::Range);
    }
    let unit_exponent = top_exponent
        .saturating_sub(bits - 1)
        .max(min_exponent - (bits - 1));
    let shift = unit_exponent.saturating_sub(exponent);
    let rounded = if shift >= 64 {
        0
    } else if shift > 0 {
        let whole = mantissa >> shift;
        let remainder = mantissa & ((1u64 << shift) - 1);
        let halfway = 1u64 << (shift - 1);
        whole
            + u64::from(remainder > halfway || (remainder == halfway && (sticky || whole & 1 != 0)))
    } else {
        mantissa << -shift
    };
    if rounded == 0 {
        return Err(FloatInputError::Range);
    }
    let highest_bit = i64::from(63 - rounded.leading_zeros());
    let rounded_exponent = highest_bit + unit_exponent;
    if rounded_exponent > max_exponent {
        return Err(FloatInputError::Range);
    }
    let raw = if rounded_exponent < min_exponent {
        rounded
    } else {
        let significand = rounded >> (highest_bit - (bits - 1));
        (((rounded_exponent + bias) as u64) << (bits - 1))
            | (significand & ((1u64 << (bits - 1)) - 1))
    };
    Ok(match precision {
        Precision::Single => f64::from(f32::from_bits(raw as u32)),
        Precision::Double => f64::from_bits(raw),
    })
}

fn round_numeric(number: Numeric, precision: Precision) -> Result<f64> {
    match number {
        Numeric::Integer(integer) => Ok(match precision {
            Precision::Single => f64::from(integer as f32),
            Precision::Double => integer as f64,
        }),
        Numeric::Float(float) => precision.round(f64::from(float)),
    }
}

fn out_of_range(message: &str) -> LimboError {
    sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, message, None)
}

/// A float4 that a function returns, in the form that a column of the type is
/// read as.
pub(crate) struct Float4Result(Value);

/// A float8 that a function returns. See [Float4Result].
pub(crate) struct Float8Result(Value);

impl ScalarReturn for Float4Result {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("float4"));

    fn into_value(self) -> Value {
        self.0
    }
}

impl ScalarReturn for Float8Result {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("float8"));

    fn into_value(self) -> Value {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum FloatOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

/// `float4pl`, `float4mi`, `float4mul` and `float4div` of PostgreSQL, and the
/// same for float8. NULL gives NULL.
pub(crate) fn float4_arithmetic(args: &[Register], op: FloatOp) -> Result<Option<Float4Result>> {
    Ok(arithmetic(args, op, Precision::Single)?.map(Float4Result))
}

pub(crate) fn float8_arithmetic(args: &[Register], op: FloatOp) -> Result<Option<Float8Result>> {
    Ok(arithmetic(args, op, Precision::Double)?.map(Float8Result))
}

/// `float4um` and `float8um`.
pub(crate) fn float4_negate(args: &[Register]) -> Option<Float4Result> {
    operand(&args[0]).map(|number| Float4Result(Value::from_f64(-number)))
}

pub(crate) fn float8_negate(args: &[Register]) -> Option<Float8Result> {
    operand(&args[0]).map(|number| Float8Result(Value::from_f64(-number)))
}

fn arithmetic(args: &[Register], op: FloatOp, precision: Precision) -> Result<Option<Value>> {
    let (Some(left), Some(right)) = (operand(&args[0]), operand(&args[1])) else {
        return Ok(None);
    };
    let result = match precision {
        Precision::Single => f64::from(single_arithmetic(op, left as f32, right as f32)?),
        Precision::Double => double_arithmetic(op, left, right)?,
    };
    Ok(Some(Value::from_f64(result)))
}

/// Reads an argument after the planner converted it to the declared float
/// type. Text parsing and numeric casts belong to that conversion.
fn operand(register: &Register) -> Option<f64> {
    match register.get_value() {
        Value::Null => None,
        Value::Numeric(Numeric::Float(float)) => Some(f64::from(*float)),
        other => unreachable!("a float function receives converted arguments, got {other:?}"),
    }
}

/// The checks of `float4pl`, `float4mi`, `float4mul` and `float4div` in
/// PostgreSQL, written once for each width.
macro_rules! float_arithmetic {
    ($name:ident, $float:ty) => {
        fn $name(op: FloatOp, left: $float, right: $float) -> Result<$float> {
            if op == FloatOp::Divide && right == 0.0 && !left.is_nan() {
                return Err(sqlstate_error(
                    SQLSTATE_DIVISION_BY_ZERO,
                    "division by zero",
                    None,
                ));
            }
            let result = match op {
                FloatOp::Add => left + right,
                FloatOp::Subtract => left - right,
                FloatOp::Multiply => left * right,
                FloatOp::Divide => left / right,
            };
            if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
                return Err(out_of_range("value out of range: overflow"));
            }
            let underflows = match op {
                FloatOp::Multiply => result == 0.0 && left != 0.0 && right != 0.0,
                FloatOp::Divide => result == 0.0 && left != 0.0 && !right.is_infinite(),
                FloatOp::Add | FloatOp::Subtract => false,
            };
            if underflows {
                return Err(out_of_range("value out of range: underflow"));
            }
            Ok(result)
        }
    };
}

float_arithmetic!(single_arithmetic, f32);
float_arithmetic!(double_arithmetic, f64);

/// How floats compare, as PostgreSQL's float8 and float4 do: NaN equals NaN
/// and is greater than every number, and -0 equals 0. It reads the stored
/// bytes, a number and text that is a number, so a comparison with a literal
/// such as 'Infinity' works.
///
/// FIXME(perf): Every comparison, sort, index key and hash calls these
/// functions through a pointer and builds a key for each value. An inline case
/// for two stored floats, in the comparison of core, removes that cost.
static PG_FLOAT: OrderingDef = OrderingDef {
    name: "pg_float",
    compare: float_compare,
    compare_operands: None,
    hash: float_hash,
};

fn float_compare(left: ValueRef, right: ValueRef) -> Ordering {
    FloatKey::of(left).cmp(&FloatKey::of(right))
}

fn float_hash(value: ValueRef, mut state: &mut dyn Hasher) {
    match FloatKey::of(value) {
        FloatKey::Null => 0u8.hash(&mut state),
        FloatKey::Number(number) => {
            1u8.hash(&mut state);
            let number = f64::from(number);
            let bits = if number.is_nan() {
                NAN_BITS
            } else if number == 0.0 {
                0
            } else {
                number.to_bits()
            };
            bits.hash(&mut state);
        }
        FloatKey::Text(text) => {
            2u8.hash(&mut state);
            text.hash(&mut state);
        }
        FloatKey::Blob(bytes) => {
            3u8.hash(&mut state);
            bytes.hash(&mut state);
        }
    }
}

/// NULL first, then numbers, then text that is not a number, then other
/// blobs. A value that is not a float still has a place.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum FloatKey<'a> {
    Null,
    Number(Double),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> FloatKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Numeric(Numeric::Integer(integer)) => {
                Self::Number(Double::new(integer as f64))
            }
            ValueRef::Numeric(Numeric::Float(float)) => Self::Number(float),
            ValueRef::Blob(bytes) => match <[u8; 8]>::try_from(bytes) {
                Ok(bytes) => Self::Number(Double::new(f64::from_be_bytes(bytes))),
                Err(_) => Self::Blob(bytes),
            },
            ValueRef::Text(text) => match parse_float_text(text.as_str(), Precision::Double) {
                Ok(number) => Self::Number(Double::new(number)),
                Err(_) => Self::Text(text.as_str()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::types::AsValueRef;

    #[test]
    fn hexadecimal_input_rounds_at_normal_and_subnormal_boundaries() {
        for (text, bits) in [
            ("0x1p-149", 1),
            ("0x1.0000000000000000000001p-150", 1),
            ("0x1.fffffcp-127", 0x007f_ffff),
            ("0x1.fffffep-127", 0x0080_0000),
            ("0x1.000001p0", 0x3f80_0000),
            ("0x1.0000010000000000000001p0", 0x3f80_0001),
            ("0x1.000003p0", 0x3f80_0002),
            ("0x1.fffffep127", 0x7f7f_ffff),
            ("-0x0p9999999999999999999999999", 0x8000_0000),
        ] {
            assert_eq!(
                (parse_float_text(text, Precision::Single).unwrap() as f32).to_bits(),
                bits,
                "{text}"
            );
        }
        for (text, bits) in [
            ("0x1p-1074", 1),
            ("0x1.0000000000000000000001p-1075", 1),
            ("0x1.ffffffffffffep-1023", 0x000f_ffff_ffff_ffff),
            ("0x1.fffffffffffffp-1023", 0x0010_0000_0000_0000),
            ("0x1.00000000000008p0", 0x3ff0_0000_0000_0000),
            ("0x1.00000000000008000001p0", 0x3ff0_0000_0000_0001),
            ("0x1.00000000000018p0", 0x3ff0_0000_0000_0002),
            ("0x1.fffffffffffff7p1023", 0x7fef_ffff_ffff_ffff),
        ] {
            assert_eq!(
                parse_float_text(text, Precision::Double).unwrap().to_bits(),
                bits,
                "{text}"
            );
        }
        for (text, precision) in [
            ("0x1.ffffffp127", Precision::Single),
            ("0x1p-150", Precision::Single),
            ("0x1.fffffffffffff8p1023", Precision::Double),
            ("0x1p-1075", Precision::Double),
            ("0x1p-9999999999999999999999999", Precision::Double),
        ] {
            assert!(parse_float_text(text, precision).is_err(), "{text}");
        }
    }

    #[test]
    fn encoding_an_integer_as_float4_rounds_once() {
        for (integer, expected) in [
            (9_007_199_791_611_905, 9_007_200_328_482_816.0),
            (-9_007_199_791_611_905, -9_007_200_328_482_816.0),
        ] {
            let encoded = encode(&Value::from_i64(integer), Precision::Single).unwrap();
            assert_eq!(decode(&encoded, &[]).unwrap(), Value::from_f64(expected));
        }
    }

    #[test]
    fn registered_float_input_preserves_values_before_encoding() {
        for (oid, precision, tenth) in [
            (700, Precision::Single, f64::from(0.1_f32)),
            (701, Precision::Double, 0.1_f64),
        ] {
            let input = crate::pg_type::type_input(oid).expect("a float type has an INPUT");
            for (text, expected) in [
                (" 0.1 ", tenth),
                ("-0", -0.0),
                ("Infinity", f64::INFINITY),
                ("-inf", f64::NEG_INFINITY),
                ("NaN", f64::NAN),
            ] {
                let value = input(text).unwrap();
                let Value::Numeric(Numeric::Float(number)) = &value else {
                    panic!("INPUT must return a number before ENCODE: {value:?}");
                };
                let number = f64::from(*number);
                if expected.is_nan() {
                    assert!(number.is_nan());
                } else {
                    assert_eq!(number.to_bits(), expected.to_bits(), "{text} as {oid}");
                }
                let encoded = encode(&value, precision).unwrap();
                let Value::Numeric(Numeric::Float(decoded)) = decode(&encoded, &[]).unwrap() else {
                    panic!("a stored float decodes to a number");
                };
                if expected.is_nan() {
                    assert!(f64::from(decoded).is_nan());
                } else {
                    assert_eq!(f64::from(decoded).to_bits(), expected.to_bits());
                }
            }
        }
    }

    #[test]
    fn registered_float_input_reports_syntax_and_range_errors() {
        for (oid, name, overflow, underflow) in [
            (700, "real", "1e39", "1e-46"),
            (701, "double precision", "1e400", "1e-400"),
        ] {
            let input = crate::pg_type::type_input(oid).expect("a float type has an INPUT");
            for text in ["", "abc", "1 2"] {
                let error = crate::Error::from(input(text).unwrap_err());
                assert_eq!(error.sqlstate, SQLSTATE_INVALID_TEXT_REPRESENTATION);
                assert_eq!(
                    error.message,
                    format!("invalid input syntax for type {name}: \"{text}\"")
                );
            }
            for text in [overflow, underflow] {
                let error = crate::Error::from(input(text).unwrap_err());
                assert_eq!(error.sqlstate, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE);
                assert_eq!(
                    error.message,
                    format!("\"{text}\" is out of range for type {name}")
                );
            }
        }
    }

    fn stored(text: &str, precision: Precision) -> f64 {
        let Value::Blob(bytes) = encode(&Value::from_text(text.to_string()), precision).unwrap()
        else {
            panic!("a float is stored as a blob");
        };
        f64::from_be_bytes(bytes.as_slice().try_into().unwrap())
    }

    fn key(value: &Value) -> ValueRef<'_> {
        value.as_value_ref()
    }

    fn error_of(text: &str, precision: Precision) -> String {
        encode(&Value::from_text(text.to_string()), precision)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn text_is_read_as_postgresql_reads_it() {
        assert!(stored(" NaN ", Precision::Double).is_nan());
        assert!(stored("nan", Precision::Single).is_nan());
        assert_eq!(stored("inf", Precision::Double), f64::INFINITY);
        assert_eq!(stored("-Infinity", Precision::Single), f64::NEG_INFINITY);
        assert_eq!(stored("  1.5  ", Precision::Double), 1.5);
        assert_eq!(stored("1e3", Precision::Double), 1000.0);
        assert!(stored("-0", Precision::Double).is_sign_negative());
    }

    #[test]
    fn a_float4_holds_what_an_f32_holds() {
        assert_eq!(stored("0.1", Precision::Single), f64::from(0.1_f32));
        assert_ne!(stored("0.1", Precision::Single), 0.1);
        assert_eq!(stored("1e-45", Precision::Single), f64::from(1e-45_f32));
    }

    #[test]
    fn a_number_out_of_range_is_refused() {
        assert!(
            error_of("1e39", Precision::Single).contains("\"1e39\" is out of range for type real")
        );
        assert!(error_of("1e400", Precision::Double)
            .contains("is out of range for type double precision"));
        assert!(error_of("1e-400", Precision::Double).contains("is out of range"));
        assert!(error_of("1e-46", Precision::Single).contains("is out of range"));
        assert!(error_of("abc", Precision::Double)
            .contains("invalid input syntax for type double precision: \"abc\""));
        assert!(error_of("", Precision::Single).contains("invalid input syntax for type real"));
    }

    #[test]
    fn nan_is_read_back_as_a_number_like_the_rest() {
        let nan = encode(&Value::from_text("NaN"), Precision::Double).unwrap();
        assert!(decode(&nan, &[]).unwrap().is_nan());
        let zero = encode(&Value::from_text("-0"), Precision::Double).unwrap();
        let Value::Numeric(Numeric::Float(read)) = decode(&zero, &[]).unwrap() else {
            panic!("a zero is read as a number");
        };
        assert!(f64::from(read).is_sign_negative());
    }

    #[test]
    fn floats_order_as_postgresql_orders_them() {
        let bytes =
            |text: &str| encode(&Value::from_text(text.to_string()), Precision::Double).unwrap();
        let (nan, other_nan, infinity, minus_zero, zero) = (
            bytes("NaN"),
            bytes(" nan"),
            bytes("Infinity"),
            bytes("-0"),
            bytes("0"),
        );
        assert_eq!(float_compare(key(&nan), key(&other_nan)), Ordering::Equal);
        assert_eq!(float_compare(key(&nan), key(&infinity)), Ordering::Greater);
        assert_eq!(float_compare(key(&minus_zero), key(&zero)), Ordering::Equal);
        assert_eq!(
            float_compare(key(&nan), Value::from_text("Infinity").as_value_ref()),
            Ordering::Greater
        );
        let hash = |value: &Value| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            float_hash(value.as_value_ref(), &mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&nan), hash(&other_nan));
        assert_eq!(hash(&minus_zero), hash(&zero));
        assert_eq!(hash(&zero), hash(&Value::from_i64(0)));
    }
}
