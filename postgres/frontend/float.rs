use crate::statement::Statement;
use turso_core::{schema::Schema, Result};
use turso_pg_parser::translator::{FLOAT4_TYPE, FLOAT8_TYPE};

/// A PostgreSQL floating-point type. Values of both are stored as REAL, but
/// each prints with its own precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatType {
    Float4,
    Float8,
}

impl FloatType {
    /// The float type a Turso type name stands for, if any.
    pub fn from_type_name(type_name: &str) -> Option<Self> {
        if type_name.eq_ignore_ascii_case(FLOAT8_TYPE) {
            Some(Self::Float8)
        } else if type_name.eq_ignore_ascii_case(FLOAT4_TYPE) {
            Some(Self::Float4)
        } else {
            None
        }
    }

    /// Formats `value` the way PostgreSQL prints this type.
    pub fn format(self, value: f64) -> String {
        match self {
            Self::Float4 => format_float4(value),
            Self::Float8 => format_float8(value),
        }
    }
}

/// The float type of result column `idx`: a float8 or float4 table column, or
/// an expression of one of those types such as a CAST. Other float values,
/// such as plain NUMERIC columns and decimal literals, have none and keep
/// SQLite's format.
pub fn column_float_type(stmt: &Statement, idx: usize) -> Option<FloatType> {
    let info = stmt.get_column_type_info(idx).ok().flatten()?;
    if info.array_dimensions > 0 {
        return None;
    }
    FloatType::from_type_name(
        info.custom_type_name
            .as_deref()
            .unwrap_or(&info.declared_name),
    )
}

/// Registers the float8 and float4 types. They are stored as REAL like any
/// float, but their own names let result columns and casts tell them apart
/// from plain NUMERIC, which PostgreSQL prints differently.
pub(crate) fn register_float_types(schema: &mut Schema) -> Result<()> {
    for type_name in [FLOAT8_TYPE, FLOAT4_TYPE] {
        schema.add_builtin_type_from_sql(&format!(
            "CREATE TYPE {type_name}(value real) BASE real OPERATOR '<'"
        ))?;
    }
    Ok(())
}

/// Formats a float8 the way PostgreSQL's `float8out` does with the default
/// `extra_float_digits = 1`: the shortest digits that read back as the same
/// value, written as a plain decimal when the decimal exponent is between -4
/// and 14 and in scientific notation otherwise.
pub fn format_float8(value: f64) -> String {
    // Rust's `{:e}` prints the shortest digits that read back as the same
    // value, e.g. "-1.2345e-7".
    format_shortest(value, &format!("{value:e}"), 15)
}

/// Formats a float4 the way PostgreSQL's `float4out` does: the value is
/// rounded to single precision, then written with the shortest digits that
/// read back as the same single-precision value, as a plain decimal when the
/// decimal exponent is between -4 and 5.
pub fn format_float4(value: f64) -> String {
    let value = value as f32;
    format_shortest(f64::from(value), &format!("{value:e}"), 6)
}

/// Writes `value`, whose shortest digits are `scientific` (Rust's `{:e}`
/// output), in plain notation when its decimal exponent is at least -4 and
/// below `plain_exponent_limit`, and in scientific notation otherwise.
fn format_shortest(value: f64, scientific: &str, plain_exponent_limit: i32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }

    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` output always has an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("`{:e}` exponent is always an integer");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(unsigned) => ("-", unsigned),
        None => ("", mantissa),
    };

    if !(-4..plain_exponent_limit).contains(&exponent) {
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        return format!("{sign}{mantissa}e{exponent_sign}{:02}", exponent.abs());
    }

    let digits = mantissa.replace('.', "");
    let integer_digits = exponent + 1;
    if integer_digits <= 0 {
        let leading_zeros = "0".repeat(integer_digits.unsigned_abs() as usize);
        return format!("{sign}0.{leading_zeros}{digits}");
    }
    let integer_digits = integer_digits as usize;
    if integer_digits >= digits.len() {
        let trailing_zeros = "0".repeat(integer_digits - digits.len());
        return format!("{sign}{digits}{trailing_zeros}");
    }
    let (integer_part, fraction_part) = digits.split_at(integer_digits);
    format!("{sign}{integer_part}.{fraction_part}")
}

#[cfg(test)]
mod tests {
    use super::{format_float4, format_float8};

    #[test]
    fn whole_numbers_have_no_fraction() {
        assert_eq!(format_float8(1.0), "1");
        assert_eq!(format_float8(-42.0), "-42");
        assert_eq!(format_float8(1e14), "100000000000000");
    }

    #[test]
    fn fractions_use_shortest_digits() {
        assert_eq!(format_float8(2.75), "2.75");
        assert_eq!(format_float8(0.1), "0.1");
        assert_eq!(format_float8(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(format_float8(-123.456), "-123.456");
        assert_eq!(format_float8(0.0001), "0.0001");
        assert_eq!(format_float8(0.000123), "0.000123");
    }

    #[test]
    fn large_and_small_values_use_signed_two_digit_exponent() {
        assert_eq!(format_float8(1e15), "1e+15");
        assert_eq!(format_float8(1e20), "1e+20");
        assert_eq!(format_float8(-1.5e20), "-1.5e+20");
        assert_eq!(format_float8(1e-5), "1e-05");
        assert_eq!(format_float8(1.234e-7), "1.234e-07");
        assert_eq!(format_float8(f64::MAX), "1.7976931348623157e+308");
        assert_eq!(format_float8(f64::MIN_POSITIVE), "2.2250738585072014e-308");
        assert_eq!(format_float8(5e-324), "5e-324");
    }

    #[test]
    fn zero_keeps_its_sign() {
        assert_eq!(format_float8(0.0), "0");
        assert_eq!(format_float8(-0.0), "-0");
    }

    #[test]
    fn special_values_use_postgres_names() {
        assert_eq!(format_float8(f64::NAN), "NaN");
        assert_eq!(format_float8(f64::INFINITY), "Infinity");
        assert_eq!(format_float8(f64::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn float4_rounds_to_single_precision() {
        assert_eq!(format_float4(1.0), "1");
        assert_eq!(format_float4(2.5), "2.5");
        assert_eq!(format_float4(0.1), "0.1");
        assert_eq!(format_float4(1.23456789), "1.2345679");
        assert_eq!(format_float4(100000.0), "100000");
    }

    #[test]
    fn float4_uses_exponent_from_one_million() {
        assert_eq!(format_float4(1234567.0), "1.234567e+06");
        assert_eq!(format_float4(1e6), "1e+06");
        assert_eq!(format_float4(1e14), "1e+14");
        assert_eq!(format_float4(1e-5), "1e-05");
        assert_eq!(format_float4(-0.0), "-0");
        assert_eq!(format_float4(f64::INFINITY), "Infinity");
    }
}
