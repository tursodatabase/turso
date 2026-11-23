//! The text of float4 and float8 values as PostgreSQL writes them.

/// PostgreSQL writes a float in scientific notation from this exponent up.
const FLOAT4_PLAIN_BELOW: i32 = 6;
const FLOAT8_PLAIN_BELOW: i32 = 15;

/// The text of a float8 value.
pub(crate) fn float8_text(number: f64) -> String {
    if let Some(special) = special_text(number) {
        return special;
    }
    let scientific = format!("{:e}", number.abs());
    text_of_digits(number.is_sign_negative(), &scientific, FLOAT8_PLAIN_BELOW)
}

/// The text of a float4 value: the shortest digits that read back as the same f32.
pub(crate) fn float4_text(number: f32) -> String {
    if let Some(special) = special_text(f64::from(number)) {
        return special;
    }
    let scientific = format!("{:e}", number.abs());
    text_of_digits(number.is_sign_negative(), &scientific, FLOAT4_PLAIN_BELOW)
}

fn special_text(number: f64) -> Option<String> {
    if number.is_nan() {
        return Some("NaN".to_string());
    }
    if number.is_infinite() {
        return Some(
            if number > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            }
            .to_string(),
        );
    }
    if number == 0.0 {
        return Some(if number.is_sign_negative() { "-0" } else { "0" }.to_string());
    }
    None
}

/// `scientific` is Rust's `{:e}` of the absolute value, such as `1.5e-7`.
/// PostgreSQL writes scientific notation when the exponent is below -4 or
/// `plain_below` or more, and plain digits otherwise.
fn text_of_digits(negative: bool, scientific: &str, plain_below: i32) -> String {
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` of a float has an exponent");
    let exponent: i32 = exponent.parse().expect("the exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let sign = if negative { "-" } else { "" };
    if !(-4..plain_below).contains(&exponent) {
        let fraction = if digits.len() > 1 {
            format!(".{}", &digits[1..])
        } else {
            String::new()
        };
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        return format!(
            "{sign}{}{fraction}e{exponent_sign}{:02}",
            &digits[..1],
            exponent.abs()
        );
    }
    if exponent < 0 {
        return format!("{sign}0.{}{digits}", "0".repeat((-exponent - 1) as usize));
    }
    let integer_digits = exponent as usize + 1;
    if digits.len() <= integer_digits {
        return format!(
            "{sign}{digits}{}",
            "0".repeat(integer_digits - digits.len())
        );
    }
    format!(
        "{sign}{}.{}",
        &digits[..integer_digits],
        &digits[integer_digits..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float8_text_follows_float8out() {
        let cases = [
            (1.5, "1.5"),
            (0.1, "0.1"),
            (10000000000.0, "10000000000"),
            (1e14, "100000000000000"),
            (1e15, "1e+15"),
            (1e20, "1e+20"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (-2.5, "-2.5"),
            (123456.789, "123456.789"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (number, text) in cases {
            assert_eq!(float8_text(number), text, "{number}");
        }
    }

    #[test]
    fn float4_text_uses_the_digits_of_an_f32() {
        assert_eq!(float4_text(0.1), "0.1");
        assert_eq!(float4_text(2147483648.0), "2.1474836e+09");
        assert_eq!(float4_text(12.345), "12.345");
        assert_eq!(float4_text(999999.0), "999999");
        assert_eq!(float4_text(1e6), "1e+06");
        assert_eq!(float4_text(1234567.0), "1.234567e+06");
        assert_eq!(float4_text(0.00001), "1e-05");
    }
}
