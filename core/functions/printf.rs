use std::fmt::Display;
use std::iter::Map;
use std::slice::Iter;
use std::str::from_utf8_unchecked;

use crate::types::{OwnedValue, MAX_REAL_SIZE};
use crate::vdbe::Register;
use crate::LimboError;
use std::fmt::Write;
use std::io::Write as IOWrite;

const PRINTF_PRECISION_LIMIT: usize = 100000000;

///    You might ask why?
///
/// * **Consistency:** By using its own built-in implementation, Limbo (SQLite) guarantees
///     that the output will be the same on all platforms and in all LOCALEs.
///     This is important for consistency and for testing. It would be problematic
///     if one machine gave an answer of "5.25e+08" and another gave an answer of
///     "5.250e+008". Both answers are correct, but it is better when Limbo (SQLite)
///     always gives the same answer.
///
/// * **SQL `format()` Function:** There is no known way to use the standard library
///     printf() C interface to implement the `format()` SQL function feature of SQLite.
///     The built-in printf() implementation can be easily adapted to that task.
///
/// * **Extensibility & Custom Specifiers:** The printf() in Limbo (SQLite) supports new
///     non-standard substitution types (`%q`, `%Q`, `%w`, and `%z`), and enhanced
///     substitution behavior (`%s` and `%z`) that are useful both internally to SQLite
///     and to applications using SQLite. Standard library printf()s cannot normally
///     be extended in this way.
///
/// * **Alternate Form 2 Flag (`!`):** The SQLite-specific printf() supports a new flag (`!`)
///     called the "alternate-form-2" flag. This flag changes the processing of
///     floating-point conversions subtly so that the output is always an SQL-compatible
///     text representation. For string substitutions, this flag causes width and precision
///     to be measured in characters instead of bytes, simplifying UTF-8 processing.
///
/// * **Security:** The built-in SQLite implementation has compile-time options such as
///     `SQLITE_PRINTF_PRECISION_LIMIT` that provide defense against denial-of-service
///     attacks for applications that expose the printf() functionality to untrusted users.
///
/// * **Portability & Reduced Dependencies:** Using a built-in printf() implementation
///     means that SQLite has one fewer dependency on the host environment, making it
///     more portable.
#[inline(always)]
pub fn exec_printf(values: &[Register]) -> crate::Result<OwnedValue> {
    if values.is_empty() {
        return Ok(OwnedValue::Null);
    }
    let format_str = match &values[0].get_owned_value() {
        OwnedValue::Text(t) => t.as_str(),
        _ => return Ok(OwnedValue::Null),
    };
    let args: Map<Iter<'_, Register>, fn(&Register) -> PrintfArg> =
        values[1..].iter().map(|reg| reg.get_owned_value().into());
    let result = printf(format_str, args);
    let text = unsafe { from_utf8_unchecked(result.as_slice()) };
    Ok(OwnedValue::from_text(text))
}

pub enum PrintfArg<'a> {
    Int(i64),
    Float(f64),
    Str(&'a str),
    Blob(&'a [u8]),
    Null,
}

impl<'a> From<&'a OwnedValue> for PrintfArg<'a> {
    fn from(value: &'a OwnedValue) -> Self {
        match value {
            OwnedValue::Integer(int) => PrintfArg::Int(*int),
            OwnedValue::Float(f) => PrintfArg::Float(*f),
            OwnedValue::Text(t) => PrintfArg::Str(t.as_str()),
            OwnedValue::Blob(b) => PrintfArg::Blob(b.as_slice()),
            OwnedValue::Null => PrintfArg::Null,
        }
    }
}

impl Display for PrintfArg<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, ""),
            Self::Int(i) => {
                write!(f, "{}", i)
            }
            Self::Float(fl) => {
                let fl = *fl;
                if fl == f64::INFINITY {
                    return write!(f, "Inf");
                }
                if fl == f64::NEG_INFINITY {
                    return write!(f, "-Inf");
                }
                if fl.is_nan() {
                    return write!(f, "");
                }
                // handle negative 0
                if fl == -0.0 {
                    return write!(f, "{:.1}", fl.abs());
                }

                // handle scientific notation without trailing zeros
                if (fl.abs() < 1e-4 || fl.abs() >= 1e15) && fl != 0.0 {
                    let sci_notation = format!("{:.14e}", fl);
                    let parts: Vec<&str> = sci_notation.split('e').collect();

                    if parts.len() == 2 {
                        let mantissa = parts[0];
                        let exponent = parts[1];

                        let decimal_parts: Vec<&str> = mantissa.split('.').collect();
                        if decimal_parts.len() == 2 {
                            let whole = decimal_parts[0];
                            // 1.{this part}
                            let mut fraction = String::from(decimal_parts[1]);

                            //removing trailing 0 from fraction
                            while fraction.ends_with('0') {
                                fraction.pop();
                            }

                            let trimmed_mantissa = if fraction.is_empty() {
                                whole.to_string()
                            } else {
                                format!("{}.{}", whole, fraction)
                            };
                            let (prefix, exponent) = if exponent.starts_with('-') {
                                ("-0", &exponent[1..])
                            } else {
                                ("+", exponent)
                            };
                            return write!(f, "{}e{}{}", trimmed_mantissa, prefix, exponent);
                        }
                    }

                    // fallback
                    return write!(f, "{}", sci_notation);
                }

                // handle floating point max size is 15.
                // If left > right && right + left > 15 go to sci notation
                // If right > left && right + left > 15 truncate left so right + left == 15
                let rounded = fl.round();
                if (fl - rounded).abs() < 1e-14 {
                    // if we very close to integer trim decimal part to 1 digit
                    if rounded == rounded as i64 as f64 {
                        return write!(f, "{:.1}", fl);
                    }
                }
                let fl_str = format!("{}", fl);
                let splitted = fl_str.split('.').collect::<Vec<&str>>();
                // fallback
                if splitted.len() != 2 {
                    return write!(f, "{:.14e}", fl);
                }

                let first_part = if fl < 0.0 {
                    // remove -
                    &splitted[0][1..]
                } else {
                    splitted[0]
                };

                let second = splitted[1];

                // We want more precision for smaller numbers. in SQLite case we want 15 non zero digits in 0 < number < 1
                // leading zeroes added to max real size. But if float < 1e-4 we go to scientific notation
                let leading_zeros = second.chars().take_while(|c| c == &'0').count();
                let reminder = if first_part != "0" {
                    MAX_REAL_SIZE as isize - first_part.len() as isize
                } else {
                    MAX_REAL_SIZE as isize + leading_zeros as isize
                };
                // float that have integer part > 15 converted to sci notation
                if reminder < 0 {
                    return write!(f, "{:.14e}", fl);
                }
                // trim decimal part to reminder or self len so total digits is 15;
                let mut fl = format!("{:.*}", second.len().min(reminder as usize), fl);
                // if decimal part ends with 0 we trim it
                while fl.ends_with('0') {
                    fl.pop();
                }
                write!(f, "{}", fl)
            }
            Self::Str(s) => {
                write!(f, "{}", s)
            }
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatSpecifierType {
    /// Signed decimal integer (`%d`, `%i`). Argument treated as `i64`.
    SignedDecimal,

    /// Unsigned decimal integer (`%u`). Argument treated as `u64`.
    UnsignedDecimal,

    /// Unsigned octal integer (`%o`). Argument treated as `u64`.
    Octal,

    /// Unsigned hexadecimal integer, lowercase (`%x`). Argument treated as `u64`.
    HexLower,

    /// Unsigned hexadecimal integer, uppercase (`%X`). Argument treated as `u64`.
    HexUpper,

    /// Decimal floating point, lowercase (`%f`). Argument treated as `f64`. Formats as [-]ddd.ddd.
    FloatF,

    /// Scientific notation, lowercase 'e' (`%e`). Argument treated as `f64`. Formats as [-]d.ddde±dd.
    FloatELower,

    /// Scientific notation, uppercase 'E' (`%E`). Argument treated as `f64`. Formats as [-]d.dddE±dd.
    FloatEUpper,

    /// Use `%f` or `%e` depending on exponent, lowercase (`%g`). Argument treated as `f64`.
    FloatGLower,

    /// Use `%f` or `%E` depending on exponent, uppercase (`%G`). Argument treated as `f64`.
    FloatGUpper,

    /// Single character (`%c`). Argument treated as integer, converted to `char`.
    Character,

    /// String (`%s`). Argument treated as string (`&str`). Precision specifies maximum characters.
    String,

    /// Pointer address (`%p`). Argument treated as pointer, usually formatted as hexadecimal.
    Pointer,

    /// SQL Literal String (`%q`). (SQLite specific) Argument is string. Single quotes are doubled.
    SqlEscapedString,

    /// SQL Literal String or NULL (`%Q`). (SQLite specific) Like `%q`, but surrounds with single quotes, or outputs `NULL` if argument is NULL.
    SqlEscapedStringOrNull,

    /// SQL Identifier (`%w`). (SQLite specific) Argument is string. Double quotes are doubled.
    SqlEscapedIdentifier,

    /// Default variant
    None,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FormatSpec {
    flags: u8,
    width: Option<usize>,
    precision: Option<usize>,
    specifier: FormatSpecifierType,
}

impl FormatSpec {
    pub const FLAG_LEFT_JUSTIFY: u8 = 1 << 0;
    pub const FLAG_SIGN_ALWAYS: u8 = 1 << 1;
    pub const FLAG_SPACE_POSITIVE: u8 = 1 << 2;
    pub const FLAG_PAD_ZERO: u8 = 1 << 3;
    pub const FLAG_ALTERNATE_FORM_1: u8 = 1 << 4;
    pub const FLAG_ALTERNATE_FORM_2: u8 = 1 << 5;
    pub const FLAG_COMMA_OPTION: u8 = 1 << 6;

    #[inline]
    pub fn set_flag(&mut self, flag: u8) -> Option<()> {
        match flag {
            b'-' => self.flags |= Self::FLAG_LEFT_JUSTIFY,
            b'+' => self.flags |= Self::FLAG_SIGN_ALWAYS,
            b' ' => self.flags |= Self::FLAG_SPACE_POSITIVE,
            b'#' => self.flags |= Self::FLAG_ALTERNATE_FORM_1,
            b'!' => self.flags |= Self::FLAG_ALTERNATE_FORM_2,
            b',' => self.flags |= Self::FLAG_COMMA_OPTION,
            b'0' => self.flags |= Self::FLAG_PAD_ZERO,
            _ => return None,
        };
        Some(())
    }

    #[inline]
    pub fn set_width(&mut self, width: usize) {
        self.width = Some(width)
    }

    #[inline]
    pub fn set_precision(&mut self, precision: usize) {
        self.precision = Some(precision)
    }

    #[inline]
    pub fn set_specifier(&mut self, spec_char: u8) -> Result<(), LimboError> {
        self.specifier = match spec_char {
            b'd' | b'i' => FormatSpecifierType::SignedDecimal,
            b'u' => FormatSpecifierType::UnsignedDecimal,
            b'o' => FormatSpecifierType::Octal,
            b'x' => FormatSpecifierType::HexLower,
            b'X' => FormatSpecifierType::HexUpper,
            b'f' => FormatSpecifierType::FloatF,
            b'e' => FormatSpecifierType::FloatELower,
            b'E' => FormatSpecifierType::FloatEUpper,
            b'g' => FormatSpecifierType::FloatGLower,
            b'G' => FormatSpecifierType::FloatGUpper,
            b'c' => FormatSpecifierType::Character,
            b's' => FormatSpecifierType::String,
            b'p' => FormatSpecifierType::Pointer,
            b'q' => FormatSpecifierType::SqlEscapedString,
            b'Q' => FormatSpecifierType::SqlEscapedStringOrNull,
            b'w' => FormatSpecifierType::SqlEscapedIdentifier,
            _ => return Err(LimboError::InternalError("Unknown specifier".to_string())),
        };
        Ok(())
    }

    /// Checks if the '-' flag is set, indicating left-justification.
    ///
    /// If set, the output should be padded on the right with spaces to fill the field width.
    /// If not set (default), the output is right-justified, padded on the left.
    #[inline]
    pub fn is_left_justified(&self) -> bool {
        (self.flags & Self::FLAG_LEFT_JUSTIFY) != 0
    }

    /// Checks if the '0' flag is set, indicating zero-padding.
    ///
    /// If set, the output should be padded on the left with zeros instead of spaces
    /// to fill the field width.
    /// This flag is ignored if the '-' (left-justify) flag is also present.
    /// For numerical conversions, the padding is inserted after the sign or base indicator (like 0x).
    #[inline]
    pub fn should_pad_zero(&self) -> bool {
        // Zero padding is usually ignored if left-justified
        (self.flags & Self::FLAG_PAD_ZERO) != 0 && !self.is_left_justified()
    }

    /// Checks if the '+' flag is set, indicating the sign should always be shown.
    ///
    /// If set, the result of a signed conversion will always begin with a '+' or '-'.
    /// By default, only negative numbers are prefixed with '-'.
    #[inline]
    pub fn should_always_sign(&self) -> bool {
        (self.flags & Self::FLAG_SIGN_ALWAYS) != 0
    }

    /// Checks if the ' ' (space) flag is set.
    ///
    /// If set *and* the '+' flag is not set, a blank space is inserted before a positive
    /// signed number or an empty string. Ignored if '+' flag is present.
    #[inline]
    pub fn should_space_if_positive(&self) -> bool {
        // Space flag ignored if + flag is present
        (self.flags & Self::FLAG_SPACE_POSITIVE) != 0 && !self.should_always_sign()
    }

    /// Checks if the '#' flag is set, indicating alternate form.
    ///
    /// The behavior depends on the conversion specifier:
    /// - For 'o': Increases precision (if necessary) to force the first digit to be '0'.
    /// - For 'x', 'X': Prepends "0x" or "0X" to non-zero results.
    /// - For 'e', 'E', 'f': Always includes a decimal point, even if no digits follow.
    /// - For 'g', 'G': Trailing zeros are not removed after the decimal point.
    #[inline]
    pub fn use_alternate_form_1(&self) -> bool {
        (self.flags & Self::FLAG_ALTERNATE_FORM_1) != 0
    }
    /// This is the alternate-form-2 flag. For string substitutions,
    /// this flag causes the width and precision to be understand in terms of characters rather than bytes.
    /// For floating point substitutions, the alternate-form-2 flag increases the maximum number of significant digits displayed from 16 to 26,
    /// forces the display of the decimal point and causes at least one digit to appear after the decimal point.
    #[inline]
    pub fn use_alternate_form_2(&self) -> bool {
        (self.flags & Self::FLAG_ALTERNATE_FORM_2) != 0
    }
    /// The comma option causes comma separators to be added to the output of numeric substitutions (%d, %f, and similar) before every 3rd digits to the left of the decimal point.
    /// No commas are added for digits to the right of the decimal point.
    /// This can help humans to more easily discern the magnitude of large integer values.
    /// For example, the value 2147483647 would be rendered as "2147483647" using "%d" but would appear as "2,147,483,647" with "%,d". This flag is a non-standard extension.
    #[inline]
    pub fn use_comma_option(&self) -> bool {
        (self.flags & Self::FLAG_COMMA_OPTION) != 0
    }

    pub fn parse(&mut self, input: &[u8], mut pos: usize) -> Result<usize, LimboError> {
        // parse flags
        while pos < input.len() {
            if input[pos] == b'0' {
                break;
            }
            if let Some(()) = self.set_flag(input[pos]) {
                pos += 1;
            } else {
                break;
            }
        }
        // parse width
        let mut width: usize = 0;
        let start_pos = pos;
        while pos < input.len() && input[pos].is_ascii_digit() {
            if pos == start_pos
                && input[pos] == b'0'
                && pos + 1 < input.len()
                && input[pos + 1].is_ascii_digit()
            {
                self.set_flag(b'0');
                pos += 1;
                continue;
            }
            width = width
                .saturating_mul(10)
                .saturating_add((input[pos] - b'0') as usize);
            pos += 1;
        }
        if pos > start_pos {
            self.set_width(width);
        }
        // parse precision
        if pos < input.len() && input[pos] == b'.' {
            pos += 1;
            let mut precision: usize = 0;
            while pos < input.len() && (b'0'..=b'9').contains(&input[pos]) {
                precision = precision
                    .saturating_mul(10)
                    .saturating_add((input[pos] - b'0') as usize);
                pos += 1;
            }
            self.set_precision(precision);
        }
        // parse specifier
        if pos < input.len() {
            self.set_specifier(input[pos])?;
        } else {
            return Err(LimboError::InternalError(
                "Unexpected end of input".to_string(),
            ));
        }

        Ok(pos + 1)
    }
}

impl Default for FormatSpec {
    // Using Default derive
    fn default() -> Self {
        Self {
            flags: 0,
            width: None,
            precision: None,
            specifier: FormatSpecifierType::None, // Default state
        }
    }
}

/// SQLite implements its own printf function to be platform agnostic
/// %[flags][width][.precision]specifier <-- Common format specifier string
fn printf(input: &str, mut args: Map<Iter<'_, Register>, fn(&Register) -> PrintfArg>) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::with_capacity(input.len());
    let mut helper_buffer = String::new();
    let mut spec = FormatSpec::default();
    let input_bytes = input.as_bytes();
    let len = input_bytes.len();
    let mut start_pos = 0;

    let mut pos = 0;
    while pos < len {
        if input_bytes[pos] != b'%' {
            pos += 1;
            continue;
        }
        result.extend_from_slice(&input_bytes[start_pos..pos]);
        pos += 1;
        if pos < len && input_bytes[pos] != b'%' {
            //process specifier
            pos = if let Ok(new_pos) = spec.parse(input_bytes, pos) {
                new_pos
            } else {
                result.extend_from_slice(&[b'%', input_bytes[pos]]);
                pos + 1
            };
            let arg = args.next();
            format_value(&mut result, &mut helper_buffer, &mut spec, arg);
        } else {
            result.push(input_bytes[pos]);
            pos += 1;
        }
        start_pos = pos;
    }
    result.extend_from_slice(&input_bytes[start_pos..len]);
    result
}

pub fn format_value(
    output: &mut Vec<u8>,
    buffer: &mut String,
    spec: &mut FormatSpec,
    arg: Option<PrintfArg>,
) -> () {
    let Some(arg) = arg else { return };
    if spec.precision > Some(PRINTF_PRECISION_LIMIT) {
        return;
    }
    buffer.clear();
    match spec.specifier {
        FormatSpecifierType::SignedDecimal
        | FormatSpecifierType::UnsignedDecimal
        | FormatSpecifierType::Octal
        | FormatSpecifierType::HexLower
        | FormatSpecifierType::HexUpper
        | FormatSpecifierType::Pointer => {
            let _ = format_numeric(arg, output, spec, buffer);
        }
        FormatSpecifierType::FloatF
        | FormatSpecifierType::FloatELower
        | FormatSpecifierType::FloatEUpper
        | FormatSpecifierType::FloatGLower
        | FormatSpecifierType::FloatGUpper => {
            let _ = format_float(arg, output, spec, buffer);
        }
        FormatSpecifierType::Character => {
            let _ = write!(buffer, "{}", arg);
            let _ = write!(output, "{:.1}", buffer);
        }
        FormatSpecifierType::String
        | FormatSpecifierType::SqlEscapedStringOrNull
        | FormatSpecifierType::SqlEscapedString
        | FormatSpecifierType::SqlEscapedIdentifier => {
            let text = match arg {
                PrintfArg::Str(text) => text,
                other => {
                    let _ = write!(buffer, "{}", other);
                    buffer.as_str()
                }
            };

            let start_idx = output.len();
            if matches!(spec.specifier, FormatSpecifierType::SqlEscapedStringOrNull) {
                output.push(b'\'');
            }
            for ch in text.chars().take(spec.precision.unwrap_or(text.len())) {
                if ch == '"' && matches!(spec.specifier, FormatSpecifierType::SqlEscapedIdentifier)
                {
                    output.extend_from_slice(&[b'"', b'"']);
                } else if ch == '\''
                    && matches!(
                        spec.specifier,
                        FormatSpecifierType::SqlEscapedString
                            | FormatSpecifierType::SqlEscapedStringOrNull
                    )
                {
                    output.extend_from_slice(&[b'\'', b'\'']);
                } else {
                    output.push(ch as u8);
                }
            }
            if matches!(spec.specifier, FormatSpecifierType::SqlEscapedStringOrNull) {
                output.push(b'\'');
            }
            let diff = output.len() - start_idx;
            match spec.width {
                Some(width) if width > diff => {
                    let padding = std::iter::repeat_n(b' ', width - diff);
                    if spec.is_left_justified() {
                        output.extend(padding);
                        return;
                    }
                    output.splice(start_idx..start_idx, padding);
                }
                _ => {}
            }
        }
        _ => unreachable!("Placeholder should not be present here"),
    }
}

fn format_float(
    arg: PrintfArg,
    output: &mut Vec<u8>,
    spec: &FormatSpec,
    core_buffer: &mut String,
) -> Result<(), LimboError> {
    let num = match arg {
        PrintfArg::Float(f) => f,
        PrintfArg::Int(i) => i as f64,
        _ => 0.0,
    };
    if !num.is_finite() {
        format_special_float(num, output, spec)?;
        return Ok(());
    }
    let prelim_precision = spec.precision.unwrap_or(6);

    let rounded_num = round_float(num, prelim_precision, spec.specifier);

    let (is_negative, exponent) = extract_decimal_components(rounded_num, core_buffer)?;

    let (effective_spec, effective_precision) = choose_format_and_precision(exponent, spec);

    build_core_string(
        exponent,
        effective_spec,
        effective_precision,
        spec,
        core_buffer,
    )?;

    let prefix = determine_prefix_float(num, spec);
    assemble_output(output, prefix, core_buffer.as_str(), spec, is_negative)?;

    Ok(())
}

/// Formats special float values (NaN, Inf) and applies padding.
fn format_special_float(
    num: f64,
    output: &mut Vec<u8>,
    spec: &FormatSpec,
) -> Result<(), LimboError> {
    let prefix = determine_prefix_float(num, spec);
    let num_str = if num.is_nan() {
        if spec.should_pad_zero() {
            "null"
        } else {
            "NaN"
        }
    } else {
        if spec.should_pad_zero() {
            "9.0e+999"
        } else {
            "Inf"
        }
    };
    assemble_output(output, prefix, num_str, spec, false)?;
    Ok(())
}

/// Approximates sqlite3FpDecode: extracts sign, decimal digits, and exponent.
/// Takes a finite f64. Returns (is_negative, digits_vec, exponent_base10).
fn extract_decimal_components(num: f64, buffer: &mut String) -> Result<(bool, i32), LimboError> {
    let mut e_pos = 0;
    let exponent = {
        if num == 0.0 {
            0
        } else {
            let _ = write!(buffer, "{:.e}", num);
            e_pos = buffer.find('e').expect("Exponent should be present!");

            buffer[e_pos + 1..]
                .parse::<i32>()
                .expect("Exponent should be in i32 bounds!")
        }
    };
    buffer.truncate(e_pos);
    buffer.retain(|c| c != '.');

    Ok((num.is_sign_negative(), exponent))
}

/// Implements %g/%G selection logic based on exponent and original spec.
/// Returns the effective specifier (f/e/E) and effective precision to use for formatting.
fn choose_format_and_precision(
    exponent: i32,
    spec: &FormatSpec, // Pass original spec with potentially g/G
) -> (FormatSpecifierType, usize) {
    let og_precision = spec.precision.unwrap_or(6);

    let result = match spec.specifier {
        FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower
            if exponent < -4 || exponent >= (og_precision as i32) =>
        {
            let effective_precision = og_precision.saturating_sub(1);
            let specifier = if spec.specifier == FormatSpecifierType::FloatGUpper {
                FormatSpecifierType::FloatEUpper
            } else {
                FormatSpecifierType::FloatELower
            };
            (specifier, effective_precision)
        }
        FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower => {
            let digits_before_or_at_decimal = (exponent + 1).max(0) as usize;
            let effective_precision = og_precision.saturating_sub(digits_before_or_at_decimal);
            (FormatSpecifierType::FloatF, effective_precision)
        }
        spec @ (FormatSpecifierType::FloatF
        | FormatSpecifierType::FloatELower
        | FormatSpecifierType::FloatEUpper) => (spec, og_precision),
        _ => unreachable!("Should parse only floating point"),
    };
    return result;
}

/// Manually builds the core formatted string (no prefix/padding) from components.
/// Applies rounding, precision, places decimal/exponent, handles '#' / '!' effects.
fn build_core_string(
    exponent: i32,                       // From extraction
    effective_spec: FormatSpecifierType, // From choose_format_and_precision
    effective_precision: usize,          // From choose_format_and_precision
    spec: &FormatSpec,
    buffer: &mut String,
) -> Result<(), LimboError> {
    let mut current_pos = buffer.len();
    // I assume we are working with ascii digit only
    // TODO: replace buffer.as_mut_vec().extend_from_within with same string method when 1.87.0 arrives.
    unsafe {
        assert!(buffer.as_mut_vec().len() == buffer.len());
    }

    let og_spec = spec.specifier;
    match effective_spec {
        FormatSpecifierType::FloatF => {
            if exponent < 0 {
                buffer.push_str("0.");
                let dot_position = buffer.len() - 1;
                let abs_exp = (exponent.abs() - 1) as usize;
                buffer.extend(std::iter::repeat_n('0', abs_exp.min(effective_precision)));
                // This is unsafe because there is no guarantee that I will write valid utf8 into vec, but I extend from within so it is safe.
                if abs_exp < effective_precision {
                    unsafe {
                        buffer.as_mut_vec().extend_from_within(0..current_pos);
                        buffer.as_mut_vec().drain(0..current_pos);
                        let current_reminder = buffer[dot_position..].len();
                        if current_reminder < effective_precision {
                            let diff = effective_precision - current_reminder;
                            buffer.extend(std::iter::repeat_n('0', diff));
                        }
                    };
                };
            } else {
                let exponent_diff = if buffer.len() < (exponent as usize) + 1 {
                    let buf_len = buffer.len();
                    buffer.extend(std::iter::repeat_n('0', (exponent as usize) + 1 - buf_len));
                    (exponent as usize) + 1 - buf_len
                } else {
                    0
                };
                current_pos = current_pos + exponent_diff;
                let dot_position = unsafe {
                    buffer
                        .as_mut_vec()
                        .extend_from_within(0..(exponent + 1) as usize);
                    let dot_position = buffer.len();

                    buffer.push('.');
                    if (exponent as usize) + 1 < current_pos {
                        buffer.push('.');
                        buffer
                            .as_mut_vec()
                            .extend_from_within((exponent + 1) as usize..current_pos);
                    }
                    dot_position
                };
                let current_reminder = buffer[dot_position..].len();
                if current_reminder < effective_precision {
                    buffer.extend(std::iter::repeat_n('0', effective_precision));
                } else {
                    buffer.drain(dot_position + effective_precision..);
                }
                buffer.drain(0..current_pos);
                if spec.use_comma_option() {
                    apply_commas(buffer, dot_position - current_pos);
                }
            }
        }
        FormatSpecifierType::FloatEUpper | FormatSpecifierType::FloatELower => {
            let e = if matches!(effective_spec, FormatSpecifierType::FloatEUpper) {
                'E'
            } else {
                'e'
            };
            if matches!(
                og_spec,
                FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower
            ) {
                buffer.truncate(effective_precision);
            } else {
            }
            if buffer.len() != 1 || spec.use_alternate_form_1() {
                if buffer.len() == 1 {
                    buffer.push_str(".0");
                } else {
                    buffer.insert(1, '.');
                }
            }
            buffer.push(e);
            let _ = write!(buffer, "{:+02}", exponent);
        }
        _ => unreachable!("Filtered out"),
    }

    Ok(())
}

/// Determines the prefix ("+" or " ") for positive numbers based on flags.
#[inline(always)]
fn determine_prefix_float(num: f64, spec: &FormatSpec) -> &'static str {
    if num.is_sign_negative() {
        ""
    } else if spec.should_always_sign() || num.is_infinite() {
        "+"
    } else if spec.should_space_if_positive() {
        " "
    } else {
        ""
    }
}

fn format_numeric(
    arg: PrintfArg,
    output: &mut Vec<u8>,
    spec: &FormatSpec,
    core_buffer: &mut String,
) -> Result<(), LimboError> {
    let (_, is_negative, prefix, abs_value_u64, radix, uppercase) =
        determine_numeric_properties(arg, spec)?;

    format_core_number(core_buffer, abs_value_u64, spec.precision, radix, uppercase)?;

    if spec.use_comma_option() && radix == 10 {
        apply_commas(core_buffer, core_buffer.len());
    }

    // 4. Assemble final output with padding
    assemble_output(output, prefix, core_buffer, spec, is_negative)?;

    Ok(())
}

fn determine_numeric_properties<'a>(
    arg: PrintfArg<'a>,
    spec: &FormatSpec,
) -> Result<(bool, bool, &'static str, u64, u32, bool), LimboError> {
    let (is_signed, num_i64, num_u64) = match arg {
        PrintfArg::Int(i) => {
            if spec.specifier == FormatSpecifierType::UnsignedDecimal {
                (i != 0, Some(i.wrapping_abs() as i64), Some(i as u64))
            } else {
                (i != 0, Some(i), Some(i.unsigned_abs()))
            }
        }
        PrintfArg::Float(f) => {
            if spec.specifier == FormatSpecifierType::UnsignedDecimal {
                (f != 0., Some((f as u64) as i64), Some(f as u64))
            } else {
                (f != 0., Some(f as i64), Some((f as i64).unsigned_abs()))
            }
        }
        _ => (false, None, Some(0u64)),
    };

    let is_negative = num_i64.map_or(false, |n| n < 0);
    let abs_value_u64 = num_u64.unwrap_or(0);

    let prefix = if is_negative {
        "-"
    } else if is_signed && spec.should_always_sign() {
        "+"
    } else if is_signed && spec.should_space_if_positive() {
        " "
    } else if spec.use_alternate_form_1() && abs_value_u64 != 0 {
        match spec.specifier {
            FormatSpecifierType::Octal => "0",
            FormatSpecifierType::HexLower | FormatSpecifierType::Pointer => "0x",
            FormatSpecifierType::HexUpper => "0X",
            _ => "",
        }
    } else {
        ""
    };

    let (radix, uppercase) = match spec.specifier {
        FormatSpecifierType::SignedDecimal | FormatSpecifierType::UnsignedDecimal => (10, false),
        FormatSpecifierType::Octal => (8, false),
        FormatSpecifierType::HexLower | FormatSpecifierType::Pointer => (16, false),
        FormatSpecifierType::HexUpper => (16, true),
        _ => {
            return Err(LimboError::InternalError(
                "Invalid numeric specifier".into(),
            ))
        }
    };

    Ok((
        is_signed,
        is_negative,
        prefix,
        abs_value_u64,
        radix,
        uppercase,
    ))
}

fn format_core_number(
    buffer: &mut String,
    abs_value: u64,
    precision: Option<usize>,
    radix: u32,
    uppercase: bool,
) -> Result<(), LimboError> {
    buffer.clear();

    let min_digits = precision.unwrap_or(1);
    if min_digits == 0 && abs_value == 0 {
        return Ok(()); // Print nothing for %.0d with value 0
    }

    match radix {
        8 => write!(buffer, "{:0min_digits$o}", abs_value),
        10 => write!(buffer, "{:0min_digits$}", abs_value),
        16 if uppercase => write!(buffer, "{:0min_digits$X}", abs_value),
        16 => write!(buffer, "{:0min_digits$x}", abs_value),
        _ => unreachable!(),
    }
    .map_err(|_| LimboError::InternalError("Failed to format core number".into()))?;

    Ok(())
}

fn apply_commas(buffer: &mut String, len: usize) {
    if len <= 3 {
        return;
    }
    let mut num_commas = (len - 1) / 3;
    let mut insert_pos = len % 3;
    if insert_pos == 0 {
        insert_pos = 3;
    }

    while num_commas > 0 {
        buffer.insert(insert_pos, ',');
        insert_pos += 3 + 1; // Move past the 3 digits and the inserted comma
        num_commas -= 1;
    }
}

fn assemble_output(
    output: &mut Vec<u8>,
    prefix: &str,
    core_num_str: &str,
    spec: &FormatSpec,
    _is_negative: bool,
) -> Result<(), LimboError> {
    let content_len = prefix.len() + core_num_str.len();
    let width = spec.width.unwrap_or(0);
    let pad_len = width.saturating_sub(content_len);

    let pad_char = if spec.should_pad_zero() { b'0' } else { b' ' };

    if !spec.is_left_justified() {
        // Right-justified
        if spec.should_pad_zero() {
            // Pad with '0' after the sign/prefix
            output.extend_from_slice(prefix.as_bytes());
            for _ in 0..pad_len {
                output.push(pad_char);
            }
            output.extend_from_slice(core_num_str.as_bytes());
        } else {
            // Pad with ' ' before the sign/prefix
            for _ in 0..pad_len {
                output.push(pad_char);
            }
            output.extend_from_slice(prefix.as_bytes());
            output.extend_from_slice(core_num_str.as_bytes());
        }
    } else {
        // Left-justified (padding char is always space)
        output.extend_from_slice(prefix.as_bytes());
        output.extend_from_slice(core_num_str.as_bytes());
        for _ in 0..pad_len {
            output.push(b' ');
        }
    }

    Ok(())
}

fn round_float(num: f64, effective_precision: usize, effective_spec: FormatSpecifierType) -> f64 {
    if num == 0.0 {
        // Handle zero separately
        0.0
    } else {
        match effective_spec {
            FormatSpecifierType::FloatF => {
                let scale = 10.0_f64.powi(effective_precision as i32);
                let scaled = num * scale;
                if !scaled.is_finite() {
                    if scaled.is_infinite() && scaled.is_sign_positive() {
                        f64::INFINITY
                    } else if scaled.is_infinite() && scaled.is_sign_negative() {
                        f64::NEG_INFINITY
                    } else {
                        num
                    }
                } else {
                    scaled.round() / scale
                }
            }
            _ => num,
        }
    }
}

#[cfg(test)]
mod printf_tests {

    #[cfg(test)]
    mod format_spec_tests {

        use super::super::*;
        use crate::LimboError;

        fn default_spec() -> FormatSpec {
            FormatSpec::default()
        }

        #[test]
        fn test_format_spec_default() {
            let spec = FormatSpec::default();
            assert_eq!(spec.flags, 0);
            assert_eq!(spec.width, None);
            assert_eq!(spec.precision, None);
            assert_eq!(spec.specifier, FormatSpecifierType::None);
            assert!(!spec.is_left_justified());
            assert!(!spec.should_pad_zero());
            assert!(!spec.should_always_sign());
            assert!(!spec.should_space_if_positive());
            assert!(!spec.use_alternate_form_1());
        }

        #[test]
        fn test_set_flag() {
            let mut spec = default_spec();

            assert!(spec.set_flag(b'-').is_some());
            assert_eq!(spec.flags, FormatSpec::FLAG_LEFT_JUSTIFY);
            assert!(spec.is_left_justified());

            assert!(spec.set_flag(b'+').is_some());
            assert_eq!(
                spec.flags,
                FormatSpec::FLAG_LEFT_JUSTIFY | FormatSpec::FLAG_SIGN_ALWAYS
            );
            assert!(spec.should_always_sign());

            assert!(spec.set_flag(b' ').is_some());
            assert_eq!(
                spec.flags,
                FormatSpec::FLAG_LEFT_JUSTIFY
                    | FormatSpec::FLAG_SIGN_ALWAYS
                    | FormatSpec::FLAG_SPACE_POSITIVE
            );
            // Note: space flag is ignored if '+' is present, but the bit is still set
            assert!(!spec.should_space_if_positive());

            assert!(spec.set_flag(b'0').is_some());
            assert_eq!(
                spec.flags,
                FormatSpec::FLAG_LEFT_JUSTIFY
                    | FormatSpec::FLAG_SIGN_ALWAYS
                    | FormatSpec::FLAG_SPACE_POSITIVE
                    | FormatSpec::FLAG_PAD_ZERO
            );
            // Note: zero padding ignored if '-' is present, but the bit is still set
            assert!(!spec.should_pad_zero());

            assert!(spec.set_flag(b'#').is_some());
            assert_eq!(
                spec.flags,
                FormatSpec::FLAG_LEFT_JUSTIFY
                    | FormatSpec::FLAG_SIGN_ALWAYS
                    | FormatSpec::FLAG_SPACE_POSITIVE
                    | FormatSpec::FLAG_PAD_ZERO
                    | FormatSpec::FLAG_ALTERNATE_FORM_1
            );
            assert!(spec.use_alternate_form_1());

            // Test invalid flag
            assert!(spec.set_flag(b'a').is_none());
            // Flags should remain unchanged
            assert_eq!(
                spec.flags,
                FormatSpec::FLAG_LEFT_JUSTIFY
                    | FormatSpec::FLAG_SIGN_ALWAYS
                    | FormatSpec::FLAG_SPACE_POSITIVE
                    | FormatSpec::FLAG_PAD_ZERO
                    | FormatSpec::FLAG_ALTERNATE_FORM_1
            );
        }

        #[test]
        fn test_flag_logic_interactions() {
            let mut spec = default_spec();

            // '+' overrides ' ' for should_space_if_positive
            spec.set_flag(b' ').unwrap();
            assert!(spec.should_space_if_positive());
            spec.set_flag(b'+').unwrap();
            assert!(!spec.should_space_if_positive()); // '+' is set, so space is ignored
            assert!(spec.should_always_sign());

            // '-' overrides '0' for should_pad_zero
            spec.set_flag(b'0').unwrap();
            assert!(spec.should_pad_zero());
            spec.set_flag(b'-').unwrap();
            assert!(!spec.should_pad_zero()); // '-' is set, so zero padding is ignored
            assert!(spec.is_left_justified());
        }

        #[test]
        fn test_set_width() {
            let mut spec = default_spec();
            spec.set_width(123);
            assert_eq!(spec.width, Some(123));
        }

        #[test]
        fn test_set_precision() {
            let mut spec = default_spec();
            spec.set_precision(45);
            assert_eq!(spec.precision, Some(45));
        }

        #[test]
        fn test_set_specifier_valid() {
            let mut spec = default_spec();
            let valid_specifiers = [
                (b'd', FormatSpecifierType::SignedDecimal),
                (b'i', FormatSpecifierType::SignedDecimal),
                (b'u', FormatSpecifierType::UnsignedDecimal),
                (b'o', FormatSpecifierType::Octal),
                (b'x', FormatSpecifierType::HexLower),
                (b'X', FormatSpecifierType::HexUpper),
                (b'f', FormatSpecifierType::FloatF),
                (b'e', FormatSpecifierType::FloatELower),
                (b'E', FormatSpecifierType::FloatEUpper),
                (b'g', FormatSpecifierType::FloatGLower),
                (b'G', FormatSpecifierType::FloatGUpper),
                (b'c', FormatSpecifierType::Character),
                (b's', FormatSpecifierType::String),
                (b'p', FormatSpecifierType::Pointer),
                (b'q', FormatSpecifierType::SqlEscapedString),
                (b'Q', FormatSpecifierType::SqlEscapedStringOrNull),
                (b'w', FormatSpecifierType::SqlEscapedIdentifier),
            ];

            for (byte, expected_type) in valid_specifiers {
                assert!(spec.set_specifier(byte).is_ok());
                assert_eq!(spec.specifier, expected_type);
            }
        }

        #[test]
        fn test_set_specifier_invalid() {
            let mut spec = default_spec();
            let invalid_specifiers = [b'a', b'Z', b'1', b'?'];

            for byte in invalid_specifiers {
                let result = spec.set_specifier(byte);
                assert!(result.is_err());
                // Optional: Check the specific error type if LimboError has variants
                match result {
                    Err(LimboError::InternalError(msg)) => assert!(
                        msg.contains("Unknown specifier"),
                        "Unexpected error message: {}",
                        msg
                    ),
                    Err(e) => panic!("Expected InternalError, got {:?}", e),
                    Ok(_) => panic!("Expected error for invalid specifier '{}'", byte as char),
                }
                assert_eq!(spec.specifier, FormatSpecifierType::None); // Should remain default
            }
        }

        // --- Tests for parse() ---

        // Helper for parse tests
        // Parses the format specifier part *after* the initial '%'
        fn parse_spec(input_after_percent: &str) -> Result<(FormatSpec, usize), LimboError> {
            let mut spec = FormatSpec::default();
            let bytes = input_after_percent.as_bytes();
            // Start parsing from the beginning of the provided slice (which is after '%')
            match spec.parse(bytes, 0) {
                // The returned position is relative to the start of input_after_percent
                Ok(pos) => Ok((spec, pos)),
                Err(e) => Err(e),
            }
        }

        #[test]
        fn test_parse_simple() {
            // Input is "d", representing "%d"
            let (spec, pos) = parse_spec("d").unwrap();
            assert_eq!(pos, 1); // Consumed 'd'
            assert_eq!(spec.flags, 0);
            assert_eq!(spec.width, None); // No width digits encountered before 'd'
            assert_eq!(spec.precision, None);
            assert_eq!(spec.specifier, FormatSpecifierType::SignedDecimal);
        }

        #[test]
        fn test_parse_flags() {
            // Input represents "%-+# 0d"
            let (spec, pos) = parse_spec("-+# 00d").unwrap();
            assert_eq!(pos, 7); // flags + d
            assert!(spec.is_left_justified());
            assert!(spec.should_always_sign());
            // space flag bit is set, but should_space_if_positive is false due to '+'
            assert!((spec.flags & FormatSpec::FLAG_SPACE_POSITIVE) != 0);
            assert!(!spec.should_space_if_positive());
            assert!(spec.use_alternate_form_1());
            // zero flag bit is set, but should_pad_zero is false due to '-'
            assert!((spec.flags & FormatSpec::FLAG_PAD_ZERO) != 0);
            assert!(!spec.should_pad_zero());
            assert_eq!(spec.width, Some(0));
            assert_eq!(spec.precision, None);
            assert_eq!(spec.specifier, FormatSpecifierType::SignedDecimal);
        }

        #[test]
        fn test_parse_width() {
            // Input represents "%123d"
            let (spec, pos) = parse_spec("123d").unwrap();
            assert_eq!(pos, 4); // 1, 2, 3, d
            assert_eq!(spec.flags, 0);
            assert_eq!(spec.width, Some(123)); // Should be correct now
            assert_eq!(spec.precision, None);
            assert_eq!(spec.specifier, FormatSpecifierType::SignedDecimal);
        }

        #[test]
        fn test_parse_precision() {
            // Input represents "%.45s"
            let (spec, pos) = parse_spec(".45s").unwrap();
            assert_eq!(pos, 4); // ., 4, 5, s
            assert_eq!(spec.flags, 0);
            // Width is None because no digits preceded the '.'
            assert_eq!(spec.width, None);
            assert_eq!(spec.precision, Some(45)); // Should be correct now
            assert_eq!(spec.specifier, FormatSpecifierType::String);
        }

        #[test]
        fn test_parse_width_and_precision() {
            // Input represents "%10.5f"
            let (spec, pos) = parse_spec("10.5f").unwrap();
            assert_eq!(pos, 5); // 1, 0, ., 5, f
            assert_eq!(spec.flags, 0);
            assert_eq!(spec.width, Some(10)); // Should be correct now
            assert_eq!(spec.precision, Some(5)); // Should be correct now
            assert_eq!(spec.specifier, FormatSpecifierType::FloatF);
        }

        #[test]
        fn test_parse_all_components() {
            // Input represents "%-0#15.7X"
            let (spec, pos) = parse_spec("-#015.7X").unwrap();
            assert_eq!(pos, 8); // flags, width, ., precision, X
            assert!(spec.is_left_justified());
            assert!(!spec.should_pad_zero()); // Overridden by '-'
            assert!(spec.use_alternate_form_1());
            assert_eq!(spec.width, Some(15)); // Should be correct now
            assert_eq!(spec.precision, Some(7)); // Should be correct now
            assert_eq!(spec.specifier, FormatSpecifierType::HexUpper);
        }

        #[test]
        fn test_parse_just_dot() {
            // Input represents "%.f"
            let (spec, pos) = parse_spec(".f").unwrap();
            assert_eq!(pos, 2); // ., f
            assert_eq!(spec.flags, 0);
            // Width is None because no digits preceded the '.'
            assert_eq!(spec.width, None);
            // Precision is 0 because '.' was present but followed by no digits
            assert_eq!(spec.precision, Some(0));
            assert_eq!(spec.specifier, FormatSpecifierType::FloatF);
        }

        #[test]
        fn test_parse_zero_width_zero_precision() {
            // Input represents "%0.0f"
            let (spec, pos) = parse_spec("0.0f").unwrap();
            assert_eq!(pos, 4); // 0, ., 0, f
            assert_eq!(spec.flags, 0); // The '0' here is width, not the flag
            assert_eq!(spec.width, Some(0));
            assert_eq!(spec.precision, Some(0));
            assert_eq!(spec.specifier, FormatSpecifierType::FloatF);
        }

        #[test]
        fn test_parse_flag_zero_width_zero_precision() {
            // Input represents "%00.0f" - First '0' is flag, second is width
            let (spec, pos) = parse_spec("00.0f").unwrap();
            assert_eq!(pos, 5); // flag 0, width 0, ., precision 0, f
            assert!(spec.should_pad_zero());
            assert_eq!(spec.width, Some(0));
            assert_eq!(spec.precision, Some(0));
            assert_eq!(spec.specifier, FormatSpecifierType::FloatF);
        }

        #[test]
        fn test_parse_invalid_specifier() {
            // Input represents "%10.5Z"
            let result = parse_spec("10.5Z"); // Z is invalid
            assert!(result.is_err());
            match result {
                Err(LimboError::InternalError(msg)) => assert!(
                    msg.contains("Unknown specifier"),
                    "Unexpected error message: {}",
                    msg
                ),
                Err(e) => panic!("Expected InternalError, got {:?}", e),
                Ok(_) => panic!("Expected error for invalid specifier 'Z'"),
            }
        }

        #[test]
        fn test_parse_missing_specifier_after_flags() {
            // Input represents "%-+" (ends abruptly)
            let result = parse_spec("-+");
            // Expect an error because a specifier character is required
            assert!(result.is_err());
            // You might want to check for a specific error type or message
            // indicating unexpected end of input or missing specifier.
            match result {
                Err(LimboError::InternalError(msg)) => {
                    assert!(msg.contains("Unknown specifier") || msg.contains("Unexpected end"))
                } // Adjust expected message
                Err(e) => println!(
                    "test_parse_missing_specifier_after_flags got error: {:?}",
                    e
                ), // Or panic if specific error needed
                Ok(_) => panic!("Expected error for missing specifier after flags"),
            }
        }

        #[test]
        fn test_parse_missing_specifier_after_width() {
            // Input represents "%10" (ends abruptly)
            let result = parse_spec("10");
            assert!(result.is_err());
            match result {
                Err(LimboError::InternalError(msg)) => {
                    assert!(msg.contains("Unknown specifier") || msg.contains("Unexpected end"))
                } // Adjust expected message
                Err(e) => println!(
                    "test_parse_missing_specifier_after_width got error: {:?}",
                    e
                ),
                Ok(_) => panic!("Expected error for missing specifier after width"),
            }
        }

        #[test]
        fn test_parse_missing_specifier_after_dot() {
            // Input represents "%10." (ends abruptly)
            let result = parse_spec("10.");
            assert!(result.is_err());
            match result {
                Err(LimboError::InternalError(msg)) => {
                    assert!(msg.contains("Unknown specifier") || msg.contains("Unexpected end"))
                } // Adjust expected message
                Err(e) => println!("test_parse_missing_specifier_after_dot got error: {:?}", e),
                Ok(_) => panic!("Expected error for missing specifier after dot"),
            }
        }

        #[test]
        fn test_parse_missing_specifier_after_precision() {
            // Input represents "%10.5" (ends abruptly)
            let result = parse_spec("10.5");
            assert!(result.is_err());
            match result {
                Err(LimboError::InternalError(msg)) => {
                    assert!(msg.contains("Unknown specifier") || msg.contains("Unexpected end"))
                } // Adjust expected message
                Err(e) => println!(
                    "test_parse_missing_specifier_after_precision got error: {:?}",
                    e
                ),
                Ok(_) => panic!("Expected error for missing specifier after precision"),
            }
        }

        #[test]
        fn test_parse_missing_precision_digits() {
            // Input represents "%.f" -> same as test_parse_just_dot
            let (spec, pos) = parse_spec(".f").unwrap();
            assert_eq!(pos, 2);
            assert_eq!(spec.precision, Some(0)); // Precision defaults to 0 if digits are missing
            assert_eq!(spec.specifier, FormatSpecifierType::FloatF);
            assert_eq!(spec.width, None);
        }
    }
}
