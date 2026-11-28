//! The text that PostgreSQL prints for a vector, an array and a `float4`,
//! in its text output format.
//!
//! `turso_pg_catalog_gen` includes this file too, so that the values from
//! which it computes the version of the built-in rows are the values that the rows
//! writes.

use std::fmt::{self, Display, Write};

/// Appends the text of an `oidvector` or `int2vector` to `out`: the
/// elements separated by one space, as `oidvectorout` and `int2vectorout`
/// print them. An empty vector is an empty text.
pub(crate) fn push_vector<T: Display>(elements: &[T], out: &mut String) {
    for (position, element) in elements.iter().enumerate() {
        if position > 0 {
            out.push(' ');
        }
        write!(out, "{element}").expect("a String accepts any text");
    }
}

/// Appends the text of a one-dimensional array to `out`, as `array_out`
/// prints it: the elements in braces, separated by commas. An element is
/// in double quotes when it is empty, when it is `NULL` in any case, or
/// when it has a double quote, a backslash, a brace, a comma or white
/// space in it. In quotes, a backslash comes before each double quote and
/// backslash.
pub(crate) fn push_array<T: Display>(elements: &[T], out: &mut String) {
    out.push('{');
    let mut text = String::new();
    for (position, element) in elements.iter().enumerate() {
        if position > 0 {
            out.push(',');
        }
        text.clear();
        write!(text, "{element}").expect("a String accepts any text");
        if needs_quotes(&text) {
            out.push('"');
            for c in text.chars() {
                if matches!(c, '"' | '\\') {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
        } else {
            out.push_str(&text);
        }
    }
    out.push('}');
}

/// Whether `array_out` puts the element `text` in double quotes.
fn needs_quotes(text: &str) -> bool {
    text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|c| matches!(c, '"' | '\\' | '{' | '}' | ',') || is_array_space(c))
}

/// Whether `array_in` and `array_out` of PostgreSQL take `c` as white
/// space.
pub(crate) fn is_array_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0B' | '\x0C')
}

/// The `f64` of the text that PostgreSQL prints for the `float4` `value`:
/// `0.01` and not `0.009999999776482582`, the exact value of the `f32`.
/// Both print the shortest text that gives the same `f32`.
pub(crate) fn float4_as_f64(value: f32) -> f64 {
    value
        .to_string()
        .parse()
        .expect("the text of an f32 is a valid f64")
}

/// A `float4` that displays as PostgreSQL 18's `float4out` prints it with
/// the default `extra_float_digits`: the shortest text that gives the same
/// value. The text is in fixed notation when the decimal exponent is from
/// -4 to 5, and otherwise in exponent notation with a sign and at least
/// two digits in the exponent, such as `1e+10` and `1.5e-05`.
pub(crate) struct Float4(pub(crate) f32);

impl Display for Float4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0;
        if value.is_nan() {
            return f.write_str("NaN");
        }
        if value.is_infinite() {
            return f.write_str(if value > 0.0 { "Infinity" } else { "-Infinity" });
        }
        if value == 0.0 {
            return f.write_str(if value.is_sign_negative() { "-0" } else { "0" });
        }
        // Rust's exponent notation has the shortest digits, as `float4out`
        // has: "-1.5e-5".
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("exponent notation has an e");
        let exponent: i32 = exponent.parse().expect("the exponent is an integer");
        let (sign, mantissa) = match mantissa.strip_prefix('-') {
            Some(mantissa) => ("-", mantissa),
            None => ("", mantissa),
        };
        let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
        f.write_str(sign)?;
        if (-4..6).contains(&exponent) {
            write_fixed(f, &digits, exponent)
        } else {
            let (first, rest) = digits.split_at(1);
            f.write_str(first)?;
            if !rest.is_empty() {
                write!(f, ".{rest}")?;
            }
            let exponent_sign = if exponent < 0 { '-' } else { '+' };
            write!(f, "e{exponent_sign}{:02}", exponent.abs())
        }
    }
}

/// Writes `digits`, whose first digit has the decimal exponent `exponent`,
/// in fixed notation without trailing zeros after the decimal point.
fn write_fixed(f: &mut fmt::Formatter<'_>, digits: &str, exponent: i32) -> fmt::Result {
    if exponent < 0 {
        f.write_str("0.")?;
        for _ in 0..(-exponent - 1) {
            f.write_str("0")?;
        }
        return f.write_str(digits);
    }
    let integer_length = usize::try_from(exponent).expect("the exponent is positive") + 1;
    if digits.len() <= integer_length {
        f.write_str(digits)?;
        for _ in digits.len()..integer_length {
            f.write_str("0")?;
        }
        Ok(())
    } else {
        let (integer, fraction) = digits.split_at(integer_length);
        write!(f, "{integer}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector<T: Display>(elements: &[T]) -> String {
        let mut out = String::new();
        push_vector(elements, &mut out);
        out
    }

    fn array<T: Display>(elements: &[T]) -> String {
        let mut out = String::new();
        push_array(elements, &mut out);
        out
    }

    #[test]
    fn a_vector_separates_its_elements_with_spaces() {
        assert_eq!(vector::<u32>(&[]), "");
        assert_eq!(vector(&[23u32]), "23");
        assert_eq!(vector(&[23u32, 25, 1184]), "23 25 1184");
        assert_eq!(vector(&[1i16, -2]), "1 -2");
    }

    #[test]
    fn an_array_quotes_an_element_as_postgresql_does() {
        assert_eq!(array::<u32>(&[]), "{}");
        assert_eq!(array(&[25u32, 20, 1184]), "{25,20,1184}");
        assert_eq!(array(&[-1i16, 2]), "{-1,2}");
        assert_eq!(array(&['i', 'o', 'v']), "{i,o,v}");
        assert_eq!(array(&["string", "pattern"]), "{string,pattern}");
        assert_eq!(
            array(&["", "null", "NULL", "Null", "nulls"]),
            r#"{"","null","NULL","Null",nulls}"#
        );
        assert_eq!(
            array(&[
                "a b",
                "a,b",
                "{a}",
                "a\"b",
                "a\\b",
                "tab\there",
                "line\nend"
            ]),
            "{\"a b\",\"a,b\",\"{a}\",\"a\\\"b\",\"a\\\\b\",\"tab\there\",\"line\nend\"}"
        );
        assert_eq!(array(&[',', ' ', '"']), r#"{","," ","\""}"#);
        // An equals sign or a slash needs no quotes, as in an aclitem.
        assert_eq!(
            array(&["=Tc/postgres", "postgres=CTc/postgres"]),
            "{=Tc/postgres,postgres=CTc/postgres}"
        );
    }

    #[test]
    fn a_float4_has_the_text_of_float4out() {
        let text = |value: f32| Float4(value).to_string();
        assert_eq!(text(1.0), "1");
        assert_eq!(text(0.01), "0.01");
        assert_eq!(text(-2.5), "-2.5");
        assert_eq!(text(100.0), "100");
        assert_eq!(text(123456.0), "123456");
        assert_eq!(text(1_000_000.0), "1e+06");
        assert_eq!(text(1234567.0), "1.234567e+06");
        assert_eq!(text(0.0001), "0.0001");
        assert_eq!(text(0.00001), "1e-05");
        assert_eq!(text(-0.000015), "-1.5e-05");
        assert_eq!(text(1e10), "1e+10");
        assert_eq!(text(3.4028235e38), "3.4028235e+38");
        assert_eq!(text(0.0), "0");
        assert_eq!(text(-0.0), "-0");
        assert_eq!(text(f32::INFINITY), "Infinity");
        assert_eq!(text(f32::NEG_INFINITY), "-Infinity");
        assert_eq!(text(f32::NAN), "NaN");
        assert_eq!(array(&[Float4(0.5), Float4(1e10)]), "{0.5,1e+10}");
    }

    #[test]
    fn a_float4_becomes_the_f64_of_its_text() {
        assert_eq!(float4_as_f64(0.01), 0.01);
        assert_eq!(float4_as_f64(1.0), 1.0);
        assert_eq!(float4_as_f64(100.0), 100.0);
        assert_eq!(float4_as_f64(1e-5), 1e-5);
        assert_ne!(f64::from(0.01f32), 0.01);
    }
}
