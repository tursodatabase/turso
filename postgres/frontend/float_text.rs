//! The text of float values as PostgreSQL writes them.

/// The text of a double as PostgreSQL writes it: the shortest digits that read
/// back the same, in scientific notation when the exponent is below -4 or
/// above 14.
pub(crate) fn float8_text(number: f64) -> String {
    if number.is_nan() {
        return "NaN".to_string();
    }
    if number.is_infinite() {
        return if number > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        }
        .to_string();
    }
    if number == 0.0 {
        return if number.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let scientific = format!("{:e}", number.abs());
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` of a float has an exponent");
    let exponent: i32 = exponent.parse().expect("the exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let sign = if number < 0.0 { "-" } else { "" };
    if !(-4..15).contains(&exponent) {
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
