/// Formats a float8 the way PostgreSQL's `float8out` does with the default
/// `extra_float_digits = 1`: the shortest digits that read back as the same
/// value, written as a plain decimal when the decimal exponent is between -4
/// and 14 and in scientific notation otherwise.
pub fn format_float8(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }

    // Rust's `{:e}` prints the shortest digits that read back as the same
    // value, e.g. "-1.2345e-7".
    let scientific = format!("{value:e}");
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

    if !(-4..15).contains(&exponent) {
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
    use super::format_float8;

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
}
