/// Format a floating-point number to match SQLite's output
/// SQLite uses %!.15g format (15 significant digits, no trailing zeros)
/// Also used by the matrix oracle so both engines' reals are rendered
/// through the identical formatter, making text comparison value-exact.
pub fn format_real(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }

    // Use exponential notation for very large or very small numbers (like SQLite's %g)
    let abs_f = f.abs();
    if abs_f != 0.0 && !(1e-4..1e15).contains(&abs_f) {
        // Use exponential notation with up to 15 significant digits
        let formatted = format!("{f:.14e}");
        // Clean up: remove trailing zeros in mantissa and unnecessary + in exponent
        let formatted = clean_exponential(&formatted);
        return formatted;
    }

    // Check if it's a whole number that can be represented exactly
    if f.fract() == 0.0 {
        format!("{}.0", f as i64)
    } else {
        // Format with 15 significant digits to match SQLite's %!.15g format
        // Then remove trailing zeros after the decimal point
        format_with_significant_digits(f, 15)
    }
}

/// Format a float with a specific number of significant digits, removing trailing zeros
fn format_with_significant_digits(f: f64, sig_digits: usize) -> String {
    if f == 0.0 {
        return "0.0".to_string();
    }

    let abs_f = f.abs();
    // Count digits before decimal point
    let digits_before_decimal = if abs_f >= 1.0 {
        (abs_f.log10().floor() as usize) + 1
    } else {
        0
    };

    // Calculate decimal places needed for the desired significant digits
    let decimal_places = sig_digits.saturating_sub(digits_before_decimal);

    // Format with calculated decimal places
    let formatted = format!("{f:.decimal_places$}");

    // Remove trailing zeros after decimal point, but keep at least one digit after decimal
    if formatted.contains('.') {
        let trimmed = formatted.trim_end_matches('0');
        if trimmed.ends_with('.') {
            format!("{trimmed}0")
        } else {
            trimmed.to_string()
        }
    } else {
        formatted
    }
}

/// Clean up exponential notation to match SQLite's format
fn clean_exponential(s: &str) -> String {
    // Input format: "-1.23456789012340e+18" or "1.23456789012340e-05"
    if let Some(e_pos) = s.find('e') {
        let mantissa = &s[..e_pos];
        let exponent = &s[e_pos + 1..];

        // Remove trailing zeros from mantissa (but keep at least one digit after decimal)
        let mantissa = mantissa.trim_end_matches('0');
        let mantissa = if mantissa.ends_with('.') {
            format!("{mantissa}0")
        } else {
            mantissa.to_string()
        };

        // Parse and format exponent (remove leading zeros and +)
        let exp_num: i32 = exponent.parse().unwrap_or(0);
        format!("{mantissa}e{exp_num:+}")
    } else {
        s.to_string()
    }
}
