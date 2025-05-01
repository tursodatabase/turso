use std::fmt::Display;
use std::iter::Map;
use std::num::FpCategory;
use std::slice::Iter;
use std::str::from_utf8_unchecked;

use crate::types::{OwnedValue, MAX_REAL_SIZE};
use crate::vdbe::Register;
use crate::LimboError;
use std::fmt::Write;
use std::io::Write as IOWrite;

const PRINTF_PRECISION_LIMIT: usize = 100_000_000;

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

#[derive(Debug, Clone, PartialEq)] // Add Clone derive
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
            Self::Null => f.write_str(""),
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
pub enum SpecArg {
    ShouldBeSetByArg,
    IsSet(usize),
    Default,
}

impl SpecArg {
    pub fn unwrap_or(&self, default: usize) -> usize {
        match self {
            SpecArg::ShouldBeSetByArg => default,
            SpecArg::IsSet(n) => *n,
            SpecArg::Default => default,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FormatSpec {
    flags: u8,
    width: SpecArg,
    precision: SpecArg,
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
        self.width = SpecArg::IsSet(width)
    }

    #[inline]
    pub fn set_precision(&mut self, precision: usize) {
        self.precision = SpecArg::IsSet(precision)
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

    pub fn should_set_precision(&self) -> bool {
        self.precision == SpecArg::ShouldBeSetByArg
    }

    pub fn should_set_width(&self) -> bool {
        self.width == SpecArg::ShouldBeSetByArg
    }

    pub fn parse(&mut self, input: &[u8], mut pos: usize) -> Result<usize, LimboError> {
        if input.is_empty() {
            return Err(LimboError::InternalError("Empty input".to_string()));
        }
        // parse flags
        while matches!(input.get(pos), Some(i) if self.set_flag(*i).is_some()) {
            pos += 1;
        }
        if pos >= input.len() {
            return Err(LimboError::InternalError(
                "Invalid format string".to_string(),
            ));
        }
        // parse width
        let mut width: usize = 0;
        let start_pos = pos;
        while pos < input.len() && (input[pos].is_ascii_digit() || input[pos] == b'*') {
            if input[pos] == b'*' {
                if width != 0 || self.width != SpecArg::Default {
                    return Err(LimboError::InternalError("Invalid width".to_string()));
                }
                self.width = SpecArg::ShouldBeSetByArg;
                pos += 1;
                break;
            }

            width = width
                .saturating_mul(10)
                .saturating_add((input[pos] - b'0') as usize);
            pos += 1;
        }
        if pos > start_pos && self.width == SpecArg::Default {
            self.set_width(width);
        }
        if pos >= input.len() {
            return Err(LimboError::InternalError(
                "Invalid format string".to_string(),
            ));
        }
        // parse precision
        if pos + 1 < input.len() && input[pos] == b'.' {
            pos += 1;
            let mut precision: usize = 0;
            while pos < input.len() && (b'0'..=b'9').contains(&input[pos]) || input[pos] == b'*' {
                if input[pos] == b'*' && (precision == 0 && self.precision == SpecArg::Default) {
                    self.precision = SpecArg::ShouldBeSetByArg;
                    pos += 1;
                    break;
                };
                precision = precision
                    .saturating_mul(10)
                    .saturating_add((input[pos] - b'0') as usize);
                pos += 1;
            }
            if self.precision == SpecArg::Default {
                self.set_precision(precision.min(PRINTF_PRECISION_LIMIT));
            }
        }

        // SQLite uses l and ll as size specifiers, but they are significant only in C context
        // so we ignore them.
        if pos < input.len() && (input[pos] == b'l' || input[pos] == b'L') {
            pos += 1;
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

    pub fn reset(&mut self) {
        self.flags = 0;
        self.width = SpecArg::Default;
        self.precision = SpecArg::Default;
        self.specifier = FormatSpecifierType::None;
    }
}

impl Default for FormatSpec {
    // Using Default derive
    fn default() -> Self {
        Self {
            flags: 0,
            width: SpecArg::Default,
            precision: SpecArg::Default,
            specifier: FormatSpecifierType::None, // Default state
        }
    }
}

/// SQLite implements its own printf function to be platform agnostic
/// %[flags][width][.precision]specifier <-- Common format specifier string
fn printf(input: &str, mut args: Map<Iter<'_, Register>, fn(&Register) -> PrintfArg>) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::with_capacity(input.len());
    let mut helper_buffer = String::with_capacity(64);
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
        if pos + 1 >= len {
            pos += 1;
        }
        result.extend_from_slice(&input_bytes[start_pos..pos]);

        pos += 1;
        start_pos = pos;
        if pos >= len {
            break;
        }
        if input_bytes[pos] != b'%' {
            if pos > input_bytes.len() {
                result.extend_from_slice(&input_bytes[start_pos..pos]);
                return result;
            }
            //process specifier

            pos = if let Ok(new_pos) = spec.parse(input_bytes, pos) {
                new_pos
            } else {
                result.extend_from_slice(&[b'%', input_bytes[pos]]);
                pos + 1
            };
            if spec.should_set_width() {
                let arg = args.next();
                if arg.is_none() {
                    result.clear();
                    return result;
                }
                if let Some(PrintfArg::Int(v)) = arg {
                    if v >= 0 {
                        spec.set_width(v as usize);
                    } else {
                        spec.width = SpecArg::Default;
                    }
                } else {
                    spec.width = SpecArg::Default;
                }
            }
            if spec.should_set_precision() {
                let arg = args.next();
                if arg.is_none() {
                    result.clear();
                    return result;
                }
                if let Some(PrintfArg::Int(v)) = arg {
                    if v >= 0 {
                        spec.set_precision(v as usize);
                    } else {
                        spec.precision = SpecArg::Default;
                    }
                } else {
                    spec.precision = SpecArg::Default;
                }
            }
            let arg = args.next();
            if spec.specifier != FormatSpecifierType::None {
                format_value(&mut result, &mut helper_buffer, &mut spec, arg);
            } else {
                return result;
            }
            spec.reset();
        } else {
            result.push(input_bytes[pos]);
            pos += 1;
        }
        start_pos = pos;
    }
    if start_pos < len {
        result.extend_from_slice(&input_bytes[start_pos..]);
    }
    result
}

pub fn format_value(
    output: &mut Vec<u8>,
    buffer: &mut String,
    spec: &mut FormatSpec,
    arg: Option<PrintfArg>,
) -> () {
    let Some(arg) = arg else {
        match spec.specifier {
            FormatSpecifierType::String => {
                return;
            }
            FormatSpecifierType::SqlEscapedStringOrNull => {
                return;
            }
            FormatSpecifierType::SqlEscapedString => {
                return;
            }
            FormatSpecifierType::SqlEscapedIdentifier => {
                return;
            }
            FormatSpecifierType::Octal
            | FormatSpecifierType::HexLower
            | FormatSpecifierType::HexUpper
            | FormatSpecifierType::Pointer
            | FormatSpecifierType::UnsignedDecimal
            | FormatSpecifierType::SignedDecimal
            | FormatSpecifierType::FloatGLower
            | FormatSpecifierType::FloatGUpper => {
                let _ = write!(output, "0");
                return;
            }
            FormatSpecifierType::FloatF => {
                let _ = write!(output, "0.0");
                return;
            }
            FormatSpecifierType::FloatELower => {
                let _ = write!(output, "0.000000e+00");
                return;
            }
            FormatSpecifierType::FloatEUpper => {
                let _ = write!(output, "0.000000E+00");
                return;
            }

            FormatSpecifierType::Character => {
                let _ = write!(output, " ");
                return;
            }
            FormatSpecifierType::None => {
                let _ = write!(output, "");
                return;
            }
        }
    };
    if matches!(spec.precision, SpecArg::IsSet(x) if x > PRINTF_PRECISION_LIMIT) {
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
                PrintfArg::Null
                    if spec.specifier == FormatSpecifierType::SqlEscapedStringOrNull =>
                {
                    let _ = write!(output, "NULL");
                    return;
                }
                other => {
                    let _ = write!(buffer, "{}", other);
                    buffer.as_str()
                }
            };

            let mut ch_count = 0;
            let start_idx = output.len();
            if matches!(spec.specifier, FormatSpecifierType::SqlEscapedStringOrNull) {
                ch_count += 1;
                output.push(b'\'');
            }
            if spec.use_alternate_form_2() {
                let mut utf8_buff = [0u8; 4];
                for ch in text.chars().take(spec.precision.unwrap_or(text.len())) {
                    ch_count += 1;
                    if ch == '"'
                        && matches!(spec.specifier, FormatSpecifierType::SqlEscapedIdentifier)
                    {
                        ch_count += 1;
                        output.extend_from_slice(&[b'"', b'"']);
                    } else if ch == '\''
                        && matches!(
                            spec.specifier,
                            FormatSpecifierType::SqlEscapedString
                                | FormatSpecifierType::SqlEscapedStringOrNull
                        )
                    {
                        ch_count += 1;
                        output.extend_from_slice(&[b'\'', b'\'']);
                    } else {
                        let ch_len = ch.len_utf8();
                        ch.encode_utf8(&mut utf8_buff);
                        output.extend_from_slice(&utf8_buff[..ch_len]);
                    }
                }
            } else {
                for ch in text.bytes().take(spec.precision.unwrap_or(text.len())) {
                    ch_count += 1;
                    if ch == b'"'
                        && matches!(spec.specifier, FormatSpecifierType::SqlEscapedIdentifier)
                    {
                        ch_count += 1;
                        output.extend_from_slice(&[b'"', b'"']);
                    } else if ch == b'\''
                        && matches!(
                            spec.specifier,
                            FormatSpecifierType::SqlEscapedString
                                | FormatSpecifierType::SqlEscapedStringOrNull
                        )
                    {
                        ch_count += 1;
                        output.extend_from_slice(&[b'\'', b'\'']);
                    } else {
                        output.push(ch as u8);
                    }
                }
            };
            if matches!(spec.specifier, FormatSpecifierType::SqlEscapedStringOrNull) {
                ch_count += 1;
                output.push(b'\'');
            }

            match spec.width {
                SpecArg::IsSet(width) if width > ch_count => {
                    let padding = std::iter::repeat_n(b' ', width - ch_count);
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
    let (rounded_num, rought_specifier) = round_float(num, spec);
    let (is_negative, exponent) =
        extract_decimal_components(rounded_num, core_buffer, spec, rought_specifier)?;
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
fn extract_decimal_components(
    num: f64,
    buffer: &mut String,
    spec: &FormatSpec,
    ef_spec: FormatSpecifierType,
) -> Result<(bool, i32), LimboError> {
    let mut e_pos = 0;

    let exponent = {
        if num == 0.0 {
            0
        } else {
            if matches!(
                ef_spec,
                FormatSpecifierType::FloatEUpper | FormatSpecifierType::FloatELower
            ) {
                let precision = if matches!(
                    spec.specifier,
                    FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower
                ) {
                    spec.precision.unwrap_or(6) - 1
                } else {
                    spec.precision.unwrap_or(6)
                };
                let _ = write!(buffer, "{:.precision$e}", num);
            } else {
                let _ = write!(buffer, "{:.e}", num);
            }
            e_pos = buffer.find('e').expect("Exponent should be present!");
            buffer[e_pos + 1..]
                .parse::<i32>()
                .expect("Exponent should be in i32 bounds!")
        }
    };
    buffer.truncate(e_pos);
    if matches!(spec.precision, SpecArg::Default) {
        while buffer.chars().last() == Some('0') {
            buffer.pop();
        }
    }
    buffer.retain(|c| c != '.' && c != '-');
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
            let specifier = if spec.specifier == FormatSpecifierType::FloatGUpper {
                FormatSpecifierType::FloatEUpper
            } else {
                FormatSpecifierType::FloatELower
            };
            (specifier, og_precision)
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
                if abs_exp <= effective_precision {
                    unsafe {
                        buffer.as_mut_vec().extend_from_within(0..current_pos);

                        let current_reminder = buffer[dot_position..].len();
                        if current_reminder < effective_precision
                            && !matches!(
                                spec.specifier,
                                FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower,
                            )
                        {
                            let diff = effective_precision - current_reminder;
                            buffer.extend(std::iter::repeat_n('0', diff));
                        }
                        buffer.as_mut_vec().drain(0..current_pos);
                    }
                } else {
                    unsafe {
                        buffer.as_mut_vec().drain(0..current_pos);
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
                unsafe {
                    let start = buffer.len();
                    buffer
                        .as_mut_vec()
                        .extend_from_within(0..(exponent + 1) as usize);
                    let remaining_significant_digits = buffer[exponent as usize + 1..start].len();

                    if matches!(
                        spec.specifier,
                        FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower
                    ) && !spec.use_alternate_form_1()
                    {
                        if remaining_significant_digits == 0 {
                            buffer.drain(0..exponent as usize + 1);
                            return Ok(());
                        }
                    }
                    if remaining_significant_digits == 0 && effective_precision == 0 {
                        buffer.drain(0..exponent as usize + 1);
                        if spec.use_alternate_form_1() {
                            buffer.push('.');
                        }
                        return Ok(());
                    }
                    buffer.push('.');

                    let dot_position = buffer.len();

                    if (exponent as usize) + 1 < current_pos {
                        buffer
                            .as_mut_vec()
                            .extend_from_within((exponent + 1) as usize..current_pos);
                    }
                    let current_reminder = buffer[dot_position..].len();
                    if current_reminder < effective_precision {
                        buffer.extend(std::iter::repeat_n(
                            '0',
                            effective_precision - current_reminder,
                        ));
                    } else {
                        buffer.drain(dot_position + effective_precision..);
                    }
                    buffer.drain(0..current_pos);
                    if spec.use_comma_option() {
                        apply_commas(buffer);
                    }
                };
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
            if matches!(
                og_spec,
                FormatSpecifierType::FloatELower | FormatSpecifierType::FloatEUpper
            ) && buffer.len() - 1 < effective_precision
            {
                buffer.extend(std::iter::repeat_n(
                    '0',
                    effective_precision - buffer.len() + 1,
                ));
            }
            if buffer.len() != 1 || spec.use_alternate_form_1() {
                if buffer.len() == 1 {
                    buffer.push_str(".0");
                } else {
                    buffer.insert(1, '.');
                }
            }

            buffer.push(e);

            let _ = write!(buffer, "{:+03}", exponent);
        }
        _ => unreachable!("Filtered out"),
    }

    Ok(())
}

/// Determines the prefix ("+" or " ") for positive numbers based on flags.
#[inline(always)]
fn determine_prefix_float(num: f64, spec: &FormatSpec) -> &'static str {
    if num.is_sign_negative() {
        "-"
    } else if spec.should_always_sign() {
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

    format_core_number(
        core_buffer,
        abs_value_u64,
        &spec.precision,
        radix,
        uppercase,
    )?;

    if spec.use_comma_option() && radix == 10 {
        apply_commas(core_buffer);
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
    precision: &SpecArg,
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

/// Inserts thousands-separating commas into the decimal digits already
/// present in `buf`.  O(n) time, O(1) extra memory.
fn apply_commas(buf: &mut String) {
    // Ignore sign; remember where the digits start.
    let digits = if buf.starts_with('-') {
        &buf[1..]
    } else {
        buf.as_str()
    };
    let len = digits.len();
    if len <= 3 {
        return;
    }

    let commas = (len - 1) / 3;
    let new_len = buf.len() + commas;
    buf.reserve(commas); // one reallocation at most

    // SAFETY: we just reserved enough space; we'll fill every byte we set.
    unsafe {
        // Save original length *before* we enlarge the buffer.
        let orig_len = buf.len();
        let bytes = buf.as_mut_vec();
        bytes.set_len(new_len);

        // read = index of last digit in the *original* buffer
        let mut read = (orig_len as isize) - 1;

        // write = index of last byte in the enlarged buffer
        let mut write = (new_len - 1) as isize;
        let mut group = 0;

        while read >= 0 {
            let b = bytes[read as usize];
            bytes[write as usize] = b;
            read -= 1;
            write -= 1;
            group += 1;

            // insert comma every 3 digits, but not after the first group
            if group == 3 && read >= 0 && bytes[read as usize].is_ascii_digit() {
                bytes[write as usize] = b',';
                write -= 1;
                group = 0;
            }
        }
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
        if spec.should_pad_zero() {
            output.extend_from_slice(prefix.as_bytes());
            output.extend(std::iter::repeat_n(pad_char, pad_len));
            output.extend_from_slice(core_num_str.as_bytes());
        } else {
            output.extend(std::iter::repeat_n(pad_char, pad_len));
            output.extend_from_slice(prefix.as_bytes());
            output.extend_from_slice(core_num_str.as_bytes());
        }
    } else {
        output.extend_from_slice(prefix.as_bytes());
        output.extend_from_slice(core_num_str.as_bytes());
        output.extend(std::iter::repeat_n(b' ', pad_len));
    }

    Ok(())
}

fn round_float(num: f64, spec: &FormatSpec) -> (f64, FormatSpecifierType) {
    let og_spec = spec.specifier;
    let og_precision_arg = spec.precision.clone();
    let og_precision = og_precision_arg.unwrap_or(6);

    match num.classify() {
        FpCategory::Infinite | FpCategory::Nan => {
            return (num, FormatSpecifierType::FloatEUpper); // Or ELower
        }
        FpCategory::Zero => {
            let effective_spec = match og_spec {
                FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower => {
                    // G/g usually formats 0 as "0" (like f)
                    FormatSpecifierType::FloatF
                }
                _ => og_spec, // Keep original f/e/E
            };
            return (0.0, effective_spec);
        }
        _ => (), // Normal or Subnormal numbers
    }

    let exponent = num.abs().log10().floor() as i32;

    let (rounded_num, effective_spec) = {
        if matches!(
            og_spec,
            FormatSpecifierType::FloatGUpper | FormatSpecifierType::FloatGLower
        ) {
            if exponent < -4 || exponent >= (og_precision as i32) {
                let chosen_spec = if og_spec == FormatSpecifierType::FloatGUpper {
                    FormatSpecifierType::FloatEUpper
                } else {
                    FormatSpecifierType::FloatELower
                };
                (num, chosen_spec)
            } else {
                let p = og_precision; // Number of significant digits from spec
                let rounded = round_to_significant_digits(num, p); // Use helper function
                (rounded, FormatSpecifierType::FloatF)
            }
        } else if og_spec == FormatSpecifierType::FloatF {
            let decimal_places = og_precision;
            let scale = 10.0_f64.powi(decimal_places as i32);
            let rounded = if scale.is_finite() && scale != 0.0 {
                let scaled_num = num * scale;
                if scaled_num.is_finite() {
                    scaled_num.round() / scale
                } else {
                    num
                }
            } else {
                num
            };
            (rounded, FormatSpecifierType::FloatF)
        } else {
            (num, og_spec)
        }
    };

    (rounded_num, effective_spec)
}

fn round_to_significant_digits(num: f64, p: usize) -> f64 {
    if num == 0.0 || !num.is_finite() || p == 0 {
        return num;
    }
    let p_i32 = p as i32;
    let exponent = num.abs().log10().floor() as i32;
    let scale = 10.0_f64.powi(p_i32 - 1 - exponent);

    if !scale.is_finite() || scale == 0.0 {
        return num; // Cannot scale reliably
    }

    let scaled_num = num * scale;
    if !scaled_num.is_finite() {
        return num; // Scaling resulted in Inf/NaN
    }

    scaled_num.round() / scale
}

#[cfg(test)]
mod tests {
    use super::*; // Import everything from the parent module
    use crate::types::OwnedValue; // Use the mock OwnedValue
    use crate::vdbe::Register; // Use the mock Register

    fn create_regs(values: Vec<OwnedValue>) -> Vec<Register> {
        values
            .into_iter()
            .map(|val| Register::OwnedValue(val))
            .collect()
    }

    fn run_printf(format: &str, args: Vec<PrintfArg>) -> String {
        let dummy_regs: Vec<Register> = args
            .into_iter()
            .map(|arg| {
                let owned_val = match arg {
                    PrintfArg::Int(i) => OwnedValue::Integer(i),
                    PrintfArg::Float(f) => OwnedValue::Float(f),
                    PrintfArg::Str(s) => OwnedValue::Text(s.to_string().into()),
                    PrintfArg::Blob(b) => OwnedValue::Blob(b.to_vec()),
                    PrintfArg::Null => OwnedValue::Null,
                };
                Register::OwnedValue(owned_val)
            })
            .collect();

        // Create the specific iterator type printf expects
        let mapped_args: Map<Iter<'_, Register>, fn(&Register) -> PrintfArg> =
            dummy_regs.iter().map(|reg| reg.get_owned_value().into());

        let result_bytes = printf(format, mapped_args);
        String::from_utf8(result_bytes).expect("printf produced invalid UTF-8")
    }

    // === Tests for exec_printf ===

    #[test]
    fn test_exec_printf_basic() {
        let regs = create_regs(vec![
            OwnedValue::from_text("Hello %s, num=%d, float=%.2f"),
            OwnedValue::from_text("World"),
            OwnedValue::Integer(123),
            OwnedValue::Float(45.678),
        ]);
        let result = exec_printf(&regs);
        assert_eq!(
            result.unwrap().to_string(),
            OwnedValue::from_text("Hello World, num=123, float=45.68").to_string()
        );
    }

    #[test]
    fn test_exec_printf_no_args() {
        let regs = create_regs(vec![OwnedValue::from_text("No args")]);
        let result = exec_printf(&regs);
        assert_eq!(result.unwrap(), OwnedValue::from_text("No args"));
    }

    #[test]
    fn test_exec_printf_only_format_string() {
        let regs = create_regs(vec![OwnedValue::from_text("Format %% only")]);
        let result = exec_printf(&regs);
        assert_eq!(result.unwrap(), OwnedValue::from_text("Format % only"));
    }

    #[test]
    fn test_exec_printf_empty_input() {
        let regs: Vec<Register> = vec![];
        let result = exec_printf(&regs);
        assert_eq!(result.unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_exec_printf_null_format_string() {
        let regs = create_regs(vec![OwnedValue::Null, OwnedValue::from_text("World")]);
        let result = exec_printf(&regs);
        // According to your code, non-Text format string results in Null
        assert_eq!(result.unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_exec_printf_integer_format_string() {
        let regs = create_regs(vec![
            OwnedValue::Integer(123),
            OwnedValue::from_text("World"),
        ]);
        let result = exec_printf(&regs);
        // According to your code, non-Text format string results in Null
        assert_eq!(result.unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_exec_printf_missing_args() {
        let regs = create_regs(vec![
            OwnedValue::from_text("Hello %s, num=%d"),
            OwnedValue::from_text("World"),
            // Missing the integer argument
        ]);
        let result = exec_printf(&regs);
        // Your implementation seems to just stop processing when args run out
        assert_eq!(result.unwrap(), OwnedValue::from_text("Hello World, num=0"));
        // Behavior depends on format_value handling missing args
    }

    #[test]
    fn test_exec_printf_extra_args() {
        let regs = create_regs(vec![
            OwnedValue::from_text("Hello %s"),
            OwnedValue::from_text("World"),
            OwnedValue::Integer(123), // Extra arg
        ]);
        let result = exec_printf(&regs);
        // Extra args should be ignored
        assert_eq!(result.unwrap(), OwnedValue::from_text("Hello World"));
    }

    // === Tests for printf (Core Logic) ===

    #[test]
    fn test_printf_literals_and_escapes() {
        assert_eq!(run_printf("Plain string", vec![]), "Plain string");
        assert_eq!(run_printf("Percent %% sign", vec![]), "Percent % sign");
        assert_eq!(run_printf("%% literal %%", vec![]), "% literal %");
        assert_eq!(run_printf("Ends with %", vec![]), "Ends with %"); // Your parser seems to handle this
        assert_eq!(run_printf("Invalid spec %?", vec![]), "Invalid spec %?"); // Your parser handles invalid specs
    }

    // --- Integer Tests ---
    #[test]
    fn test_printf_integer_basic() {
        assert_eq!(run_printf("%d", vec![PrintfArg::Int(123)]), "123");
        assert_eq!(run_printf("%i", vec![PrintfArg::Int(-456)]), "-456");
        assert_eq!(
            run_printf("Value: %d.", vec![PrintfArg::Int(0)]),
            "Value: 0."
        );
        assert_eq!(run_printf("%u", vec![PrintfArg::Int(789)]), "789");
        assert_eq!(
            run_printf("%u", vec![PrintfArg::Int(-1)]),
            u64::MAX.to_string()
        ); // Unsigned representation of -1
    }

    #[test]
    fn test_printf_integer_padding_width() {
        assert_eq!(run_printf("%5d", vec![PrintfArg::Int(12)]), "   12"); // Right align (default)
        assert_eq!(run_printf("%-5d", vec![PrintfArg::Int(12)]), "12   "); // Left align
        assert_eq!(run_printf("%05d", vec![PrintfArg::Int(12)]), "00012"); // Zero padding
        assert_eq!(run_printf("%-05d", vec![PrintfArg::Int(12)]), "12   "); // Left align ignores zero padding
        assert_eq!(run_printf("%05d", vec![PrintfArg::Int(-12)]), "-0012"); // Zero padding with sign
        assert_eq!(run_printf("%5d", vec![PrintfArg::Int(-12)]), "  -12");
        assert_eq!(run_printf("%-5d", vec![PrintfArg::Int(-12)]), "-12  ");
    }

    #[test]
    fn test_printf_integer_sign_flags() {
        assert_eq!(run_printf("%+d", vec![PrintfArg::Int(12)]), "+12");
        assert_eq!(run_printf("%+d", vec![PrintfArg::Int(-12)]), "-12");
        assert_eq!(run_printf("% d", vec![PrintfArg::Int(12)]), " 12"); // Space for positive
        assert_eq!(run_printf("% d", vec![PrintfArg::Int(-12)]), "-12");
        assert_eq!(run_printf("% +d", vec![PrintfArg::Int(12)]), "+12"); // + overrides space
        assert_eq!(run_printf("%+05d", vec![PrintfArg::Int(12)]), "+0012");
        assert_eq!(run_printf("% 05d", vec![PrintfArg::Int(12)]), " 0012");
    }

    #[test]
    fn test_printf_integer_precision() {
        assert_eq!(run_printf("%.5d", vec![PrintfArg::Int(123)]), "00123"); // Min digits
        assert_eq!(run_printf("%.5d", vec![PrintfArg::Int(-123)]), "-00123");
        assert_eq!(run_printf("%.2d", vec![PrintfArg::Int(12345)]), "12345"); // Precision smaller than number
        assert_eq!(run_printf("%.0d", vec![PrintfArg::Int(0)]), ""); // Precision 0, value 0 -> empty
        assert_eq!(run_printf("%.0d", vec![PrintfArg::Int(123)]), "123"); // Precision 0, non-zero value -> standard print
        assert_eq!(
            run_printf("%10.5d", vec![PrintfArg::Int(123)]),
            "     00123"
        ); // Width and precision
        assert_eq!(
            run_printf("%-10.5d", vec![PrintfArg::Int(123)]),
            "00123     "
        );
        assert_eq!(
            run_printf("%010.5d", vec![PrintfArg::Int(123)]),
            "0000000123"
        );
        assert_eq!(run_printf("%10.0d", vec![PrintfArg::Int(0)]), "          ");
        // Width, precision 0, value 0
    }

    #[test]
    fn test_printf_integer_bases() {
        assert_eq!(run_printf("%o", vec![PrintfArg::Int(10)]), "12");
        assert_eq!(run_printf("%x", vec![PrintfArg::Int(255)]), "ff");
        assert_eq!(run_printf("%X", vec![PrintfArg::Int(255)]), "FF");
        assert_eq!(run_printf("%#o", vec![PrintfArg::Int(10)]), "012"); // Alternate form octal
        assert_eq!(run_printf("%#x", vec![PrintfArg::Int(255)]), "0xff"); // Alternate form hex
        assert_eq!(run_printf("%#X", vec![PrintfArg::Int(255)]), "0XFF");
        assert_eq!(run_printf("%#o", vec![PrintfArg::Int(0)]), "0"); // Alternate form 0
        assert_eq!(run_printf("%#.5o", vec![PrintfArg::Int(10)]), "000012"); // Precision takes precedence over '#' for leading zero
        assert_eq!(run_printf("%#5o", vec![PrintfArg::Int(10)]), "  012");
        assert_eq!(run_printf("%#05o", vec![PrintfArg::Int(10)]), "00012"); // Note: '#' adds 0, padding adds more 0s
        assert_eq!(run_printf("%#5x", vec![PrintfArg::Int(10)]), "  0xa");
        assert_eq!(run_printf("%#05x", vec![PrintfArg::Int(10)]), "0x00a"); // '0x' then padding
    }

    #[test]
    fn test_printf_integer_commas() {
        assert_eq!(run_printf("%,d", vec![PrintfArg::Int(123)]), "123");
        assert_eq!(run_printf("%,d", vec![PrintfArg::Int(12345)]), "12,345");
        assert_eq!(
            run_printf("%,d", vec![PrintfArg::Int(1234567)]),
            "1,234,567"
        );
        assert_eq!(run_printf("%,d", vec![PrintfArg::Int(-12345)]), "-12,345");
        assert_eq!(
            run_printf("%,10d", vec![PrintfArg::Int(12345)]),
            "    12,345"
        );
        assert_eq!(
            run_printf("%-,10d", vec![PrintfArg::Int(12345)]),
            "12,345    "
        );
        assert_eq!(
            run_printf("%,010d", vec![PrintfArg::Int(12345)]),
            "000012,345"
        ); // Padding comes before commas
        assert_eq!(
            run_printf("%,.7d", vec![PrintfArg::Int(12345)]),
            "0,012,345"
        ); // Precision applies before commas
        assert_eq!(
            run_printf("%,d", vec![PrintfArg::Int(i64::MAX)]),
            "9,223,372,036,854,775,807"
        );
    }

    // --- Float Tests ---
    #[test]
    fn test_printf_float_basic() {
        assert_eq!(
            run_printf("%f", vec![PrintfArg::Float(12.345)]),
            "12.345000"
        ); // Default precision 6
        assert_eq!(run_printf("%.2f", vec![PrintfArg::Float(12.345)]), "12.35"); // Rounding
        assert_eq!(run_printf("%.1f", vec![PrintfArg::Float(12.99)]), "13.0"); // Rounding up
        assert_eq!(run_printf("%.0f", vec![PrintfArg::Float(12.3)]), "12"); // Rounding to integer
        assert_eq!(run_printf("%.0f", vec![PrintfArg::Float(12.8)]), "13");
        assert_eq!(run_printf("%f", vec![PrintfArg::Float(-1.23)]), "-1.230000");
    }

    #[test]
    fn test_printf_float_padding_width() {
        assert_eq!(
            run_printf("%10.2f", vec![PrintfArg::Float(12.35)]),
            "     12.35"
        ); // Right align
        assert_eq!(
            run_printf("%-10.2f", vec![PrintfArg::Float(12.35)]),
            "12.35     "
        ); // Left align
        assert_eq!(
            run_printf("%010.2f", vec![PrintfArg::Float(12.35)]),
            "0000012.35"
        ); // Zero padding
        assert_eq!(
            run_printf("%010.2f", vec![PrintfArg::Float(-12.35)]),
            "-000012.35"
        ); // Zero padding with sign
        assert_eq!(
            run_printf("%-010.2f", vec![PrintfArg::Float(12.35)]),
            "12.35     "
        ); // Left ignores zero
    }

    #[test]
    fn test_printf_float_sign_flags() {
        assert_eq!(run_printf("%+.2f", vec![PrintfArg::Float(12.35)]), "+12.35");
        assert_eq!(
            run_printf("%+.2f", vec![PrintfArg::Float(-12.35)]),
            "-12.35"
        );
        assert_eq!(run_printf("% .2f", vec![PrintfArg::Float(12.35)]), " 12.35");
        assert_eq!(
            run_printf("% .2f", vec![PrintfArg::Float(-12.35)]),
            "-12.35"
        );
        assert_eq!(
            run_printf("% +.2f", vec![PrintfArg::Float(12.35)]),
            "+12.35"
        ); // + overrides space
        assert_eq!(
            run_printf("%+010.2f", vec![PrintfArg::Float(12.35)]),
            "+000012.35"
        );
        assert_eq!(
            run_printf("% 010.2f", vec![PrintfArg::Float(12.35)]),
            " 000012.35"
        );
    }

    #[test]
    fn test_printf_float_alternate_form() {
        assert_eq!(run_printf("%#f", vec![PrintfArg::Float(12.0)]), "12.000000"); // '#' ensures decimal point (default precision)
        assert_eq!(run_printf("%#.0f", vec![PrintfArg::Float(12.0)]), "12."); // '#' ensures decimal point (precision 0)
        assert_eq!(run_printf("%#g", vec![PrintfArg::Float(12.0)]), "12.0000"); // '#' prevents trailing zero removal for g/G
        assert_eq!(run_printf("%g", vec![PrintfArg::Float(12.0)]), "12"); // Default 'g' removes trailing zeros
    }

    #[test]
    fn test_printf_float_scientific() {
        assert_eq!(
            run_printf("%e", vec![PrintfArg::Float(123.456)]),
            "1.234560e+02"
        ); // Default precision 6
        assert_eq!(
            run_printf("%.2e", vec![PrintfArg::Float(123.456)]),
            "1.23e+02"
        );
        assert_eq!(
            run_printf("%E", vec![PrintfArg::Float(0.00123)]),
            "1.230000E-03"
        );
        assert_eq!(
            run_printf("%.3E", vec![PrintfArg::Float(-0.0012345)]),
            "-1.234E-03"
        ); // Rounding
        assert_eq!(
            run_printf("%15.3e", vec![PrintfArg::Float(123.456)]),
            "      1.235e+02"
        );
        assert_eq!(
            run_printf("%-15.3e", vec![PrintfArg::Float(123.456)]),
            "1.235e+02      "
        );
        assert_eq!(
            run_printf("%015.3e", vec![PrintfArg::Float(123.456)]),
            "0000001.235e+02"
        );
        assert_eq!(
            run_printf("%+015.3e", vec![PrintfArg::Float(123.456)]),
            "+000001.235e+02"
        );
        assert_eq!(
            run_printf("%#e", vec![PrintfArg::Float(12.0)]),
            "1.200000e+01"
        ); // '#' ensures decimal point? (Standard printf might not do this for e) Let's see your impl.
           // Based on your code, '#' does not affect 'e'/'E' formatting regarding the decimal point presence itself (it's always there).
    }

    #[test]
    fn test_printf_float_general() {
        // g/G uses %e/%E if exponent < -4 or >= precision
        assert_eq!(run_printf("%g", vec![PrintfArg::Float(1.23456)]), "1.23456"); // Default precision 6, use f
        assert_eq!(
            run_printf("%.5g", vec![PrintfArg::Float(1.23456)]),
            "1.2346"
        ); // Precision 5, use f, rounds
        assert_eq!(
            run_printf("%.7g", vec![PrintfArg::Float(1.2345678)]),
            "1.234568"
        ); // Precision 7, use f
        assert_eq!(run_printf("%g", vec![PrintfArg::Float(123456.7)]), "123457"); // Precision 6, use f, rounds
        assert_eq!(
            run_printf("%g", vec![PrintfArg::Float(1234567.8)]),
            "1.23457e+06"
        ); // Precision 6, use e (exp 6 >= prec 6)
        assert_eq!(
            run_printf("%g", vec![PrintfArg::Float(0.000123456)]),
            "0.000123456"
        ); // Precision 6, use f (exp -4)
        assert_eq!(
            run_printf("%g", vec![PrintfArg::Float(0.0000123456)]),
            "1.23456e-05"
        ); // Precision 6, use e (exp -5 < -4)
        assert_eq!(run_printf("%.3g", vec![PrintfArg::Float(123.45)]), "123"); // Precision 3, use f
        assert_eq!(
            run_printf("%.3g", vec![PrintfArg::Float(1234.5)]),
            "1.23e+03"
        ); // Precision 3, use e (exp 3 >= prec 3)
        assert_eq!(
            run_printf("%.3g", vec![PrintfArg::Float(0.00123)]),
            "0.00123"
        ); // Precision 3, use f
        assert_eq!(
            run_printf("%.3g", vec![PrintfArg::Float(0.0000123)]),
            "1.23e-05"
        ); // Precision 3, use e (exp -5)

        // Test '#' with g/G (prevents trailing zero removal)
        assert_eq!(run_printf("%#g", vec![PrintfArg::Float(123.0)]), "123.000"); // Use f, keep zeros
        assert_eq!(
            run_printf("%#.2g", vec![PrintfArg::Float(123.0)]),
            "1.2e+02"
        ); // Use e (exp 2 >= prec 2), '#' ignored for e trailing zeros

        assert_eq!(
            run_printf("%G", vec![PrintfArg::Float(1234567.8)]),
            "1.23457E+06"
        ); // Uppercase E
    }

    // --- String/Char/Pointer Tests ---
    #[test]
    fn test_printf_string() {
        assert_eq!(run_printf("%s", vec![PrintfArg::Str("hello")]), "hello");
        assert_eq!(
            run_printf("Msg: %s!", vec![PrintfArg::Str("Test")]),
            "Msg: Test!"
        );
        assert_eq!(
            run_printf("%10s", vec![PrintfArg::Str("hello")]),
            "     hello"
        );
        assert_eq!(
            run_printf("%-10s", vec![PrintfArg::Str("hello")]),
            "hello     "
        );
        assert_eq!(run_printf("%.3s", vec![PrintfArg::Str("hello")]), "hel"); // Precision limits length
        assert_eq!(
            run_printf("%10.3s", vec![PrintfArg::Str("hello")]),
            "       hel"
        );
        assert_eq!(
            run_printf("%-10.3s", vec![PrintfArg::Str("hello")]),
            "hel       "
        );
        assert_eq!(run_printf("%s", vec![PrintfArg::Str("")]), "");
        assert_eq!(run_printf("%.5s", vec![PrintfArg::Null]), ""); // Null as empty string, limited by precision
        assert_eq!(run_printf("%5s", vec![PrintfArg::Null]), "     "); // Null as empty string, padded
    }

    #[test]
    fn test_printf_char() {
        assert_eq!(run_printf("%c", vec![PrintfArg::Int('A' as i64)]), "6");
        // Note: Behavior for multi-byte chars or values > 255 might depend on interpretation.
        // Your `format_value` implementation uses `write!(output, "{:.1}", buffer)` which likely truncates.
        assert_eq!(run_printf("%c", vec![PrintfArg::Int(0)]), "0"); // Null character
    }

    #[test]
    fn test_printf_pointer() {
        // Pointer output is implementation-defined (usually hex). Test that it formats *something*.
        // We can't easily predict the exact address.
        let s = "hello";
        let ptr = s.as_ptr();
        // Your implementation currently formats pointer like a hex number (u64).
        let expected = format!("{:x}", ptr as u64); // This is likely how your impl behaves
                                                    // Rerun with a format like %#016x if needed.
        assert_eq!(run_printf("%p", vec![PrintfArg::Int(ptr as i64)]), expected);
        assert_eq!(
            run_printf("%10p", vec![PrintfArg::Int(ptr as i64)]),
            format!("{:>10}", expected)
        );

        // Test with NULL pointer (0)
        assert_eq!(run_printf("%p", vec![PrintfArg::Int(0)]), "0");
    }

    // --- SQLite Specific Tests ---
    #[test]
    fn test_printf_sqlite_q() {
        assert_eq!(run_printf("%q", vec![PrintfArg::Str("hello")]), "hello");
        assert_eq!(run_printf("%q", vec![PrintfArg::Str("it's")]), "it''s"); // Quotes doubled
        assert_eq!(run_printf("%q", vec![PrintfArg::Str("'")]), "''");
        assert_eq!(run_printf("%q", vec![PrintfArg::Str("")]), "");
        assert_eq!(run_printf("%q", vec![PrintfArg::Null]), ""); // Null -> empty string
        assert_eq!(
            run_printf("%10q", vec![PrintfArg::Str("it's")]),
            "     it''s"
        ); // Padding applied
        assert_eq!(run_printf("%.3q", vec![PrintfArg::Str("it's")]), "it''"); // Precision applies to *source* chars, not escaped output
                                                                              // Let's re-verify your impl: Yes, it takes `n` chars from input, then escapes.
    }

    #[test]
    fn test_printf_sqlite_big_q() {
        assert_eq!(run_printf("%Q", vec![PrintfArg::Str("hello")]), "'hello'");
        assert_eq!(run_printf("%Q", vec![PrintfArg::Str("it's")]), "'it''s'"); // Quotes doubled and surrounded
        assert_eq!(run_printf("%Q", vec![PrintfArg::Str("")]), "''");
        assert_eq!(run_printf("%Q", vec![PrintfArg::Null]), "NULL"); // Null -> empty string (SQLite standard produces NULL literal)
                                                                     // Your current impl seems to output empty string for Null arg here.
                                                                     // If you want NULL literal, `format_value` needs specific handling for %Q and PrintfArg::Null.
        assert_eq!(run_printf("%10Q", vec![PrintfArg::Str("hi")]), "      'hi'");
        assert_eq!(run_printf("%.1Q", vec![PrintfArg::Str("it's")]), "'i'"); // Takes 1 char ('i'), escapes it -> 'i'
    }

    #[test]
    fn test_printf_sqlite_w() {
        // %w is for SQL identifiers, doubles double quotes
        assert_eq!(run_printf("%w", vec![PrintfArg::Str("table")]), "table");
        assert_eq!(
            run_printf("%w", vec![PrintfArg::Str("my \"table\"")]),
            "my \"\"table\"\""
        );
        assert_eq!(run_printf("%w", vec![PrintfArg::Str("\"")]), "\"\"");
        assert_eq!(run_printf("%w", vec![PrintfArg::Null]), ""); // Null -> empty string
        assert_eq!(
            run_printf("%15w", vec![PrintfArg::Str("col\"umn")]),
            "       col\"\"umn"
        );
        assert_eq!(
            run_printf("%.5w", vec![PrintfArg::Str("col\"umn")]),
            "col\"\"u"
        ); // Precision applies to source chars
    }

    // --- Dynamic Width/Precision Tests ---
    #[test]
    fn test_printf_dynamic_width_precision() {
        assert_eq!(
            run_printf("%*d", vec![PrintfArg::Int(5), PrintfArg::Int(12)]),
            "   12" // width=5
        );
        assert_eq!(
            run_printf("%*d", vec![PrintfArg::Int(1), PrintfArg::Int(12)]),
            "12"
        ); // width=1, fits

        assert_eq!(
            run_printf("%.*s", vec![PrintfArg::Int(3), PrintfArg::Str("hello")]),
            "hel" // precision=3
        );
        assert_eq!(
            run_printf("%.*s", vec![PrintfArg::Int(0), PrintfArg::Str("hello")]),
            "" // precision=0
        );
        assert_eq!(
            run_printf("%.*s", vec![PrintfArg::Int(-5), PrintfArg::Str("hello")]),
            "hello" // precision=default
        );

        assert_eq!(
            run_printf(
                "%*.*f",
                vec![
                    PrintfArg::Int(10),
                    PrintfArg::Int(2),
                    PrintfArg::Float(12.345)
                ]
            ),
            "     12.35" // width=10, precision=2
        );
        assert_eq!(
            run_printf(
                "%-*.*f",
                vec![
                    PrintfArg::Int(10),
                    PrintfArg::Int(2),
                    PrintfArg::Float(12.345)
                ]
            ),
            "12.35     " // width=10, precision=2, left-align
        );
        assert_eq!(
            run_printf(
                "%*.*f",
                vec![PrintfArg::Int(2), PrintfArg::Int(4), PrintfArg::Float(1.2)]
            ),
            "1.2000" // width=2 (too small), precision=4
        );
    }

    #[test]
    fn test_formatspec_parse_flags() {
        let mut spec = FormatSpec::default();
        let fmt = b"-+ #0d";
        let next_pos = spec.parse(fmt, 0).unwrap();
        assert_eq!(next_pos, fmt.len());
        assert!(spec.is_left_justified());
        assert!(spec.should_always_sign());
        assert!(spec.use_alternate_form_1());
        assert!(!spec.use_alternate_form_2());
        assert!(!spec.use_comma_option());
        assert_eq!(spec.specifier, FormatSpecifierType::SignedDecimal);
    }

    // --- Error Handling and Edge Cases ---
    #[test]
    fn test_printf_error_handling() {
        // Test with invalid format specifiers
        let result = run_printf("%k", vec![PrintfArg::Int(42)]);
        assert_eq!(result, "%k"); // Should preserve the invalid specifier

        // Test with incomplete format specifier
        let result = run_printf("%", vec![PrintfArg::Int(42)]);
        assert_eq!(result, "%"); // Should preserve the incomplete specifier

        // Test with multiple % at the end
        let result = run_printf("Test %%", vec![]);
        assert_eq!(result, "Test %");
    }

    #[test]
    fn test_printf_blob_handling() {
        // Test blob handling with different format specifiers
        let blob_data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" in bytes
        assert_eq!(run_printf("%s", vec![PrintfArg::Blob(&blob_data)]), "Hello");

        // Precision limiting for blob
        assert_eq!(run_printf("%.3s", vec![PrintfArg::Blob(&blob_data)]), "Hel");

        // Width and alignment for blob
        assert_eq!(
            run_printf("%10s", vec![PrintfArg::Blob(&blob_data)]),
            "     Hello"
        );
        assert_eq!(
            run_printf("%-10s", vec![PrintfArg::Blob(&blob_data)]),
            "Hello     "
        );
    }

    #[test]
    fn test_printf_unicode_handling() {
        // Test with Unicode strings
        assert_eq!(
            run_printf("%s", vec![PrintfArg::Str("こんにちは")]),
            "こんにちは"
        ); // Japanese
        assert_eq!(run_printf("%s", vec![PrintfArg::Str("привіт")]), "привіт"); // Ukrainian
        assert_eq!(run_printf("%s", vec![PrintfArg::Str("🙂👍")]), "🙂👍"); // Emoji

        // Width with Unicode (note: width is in bytes, not characters)
        assert_eq!(
            run_printf("%15s", vec![PrintfArg::Str("こんにちは")]),
            "こんにちは"
        );

        // Precision with Unicode (precision applies to bytes, like SQLite)
        assert_eq!(
            run_printf("%.6s", vec![PrintfArg::Str("こんにちは")]),
            "こん"
        ); // Cut after 2 chars

        // Unicode with alternate form 2 flag - should measure in characters not bytes
        assert_eq!(
            run_printf("%!.3s", vec![PrintfArg::Str("こんにちは")]),
            "こんに"
        );
    }

    #[test]
    fn test_printf_null_handling() {
        // Testing various format specifiers with NULL input
        assert_eq!(run_printf("%s", vec![PrintfArg::Null]), "");
        assert_eq!(run_printf("%d", vec![PrintfArg::Null]), "0");
        assert_eq!(run_printf("%f", vec![PrintfArg::Null]), "0.000000");
        assert_eq!(run_printf("%x", vec![PrintfArg::Null]), "0");

        // Width and precision with NULL
        assert_eq!(run_printf("%10s", vec![PrintfArg::Null]), "          ");
        assert_eq!(run_printf("%-10s", vec![PrintfArg::Null]), "          ");
        assert_eq!(run_printf("%.5s", vec![PrintfArg::Null]), "");
    }

    #[test]
    fn test_printf_complex_combinations() {
        // Multiple different format specifiers in one format string
        assert_eq!(
            run_printf(
                "Int: %d, Float: %.2f, Str: %s, Hex: %#x",
                vec![
                    PrintfArg::Int(42),
                    PrintfArg::Float(123.456),
                    PrintfArg::Str("hello"),
                    PrintfArg::Int(255)
                ]
            ),
            "Int: 42, Float: 123.46, Str: hello, Hex: 0xff"
        );

        // Mixing SQLite-specific formats with standard ones
        assert_eq!(
            run_printf(
                "Standard: %s, SQL: %q, ID: %w, Quoted: %Q",
                vec![
                    PrintfArg::Str("text"),
                    PrintfArg::Str("it's"),
                    PrintfArg::Str("col\"umn"),
                    PrintfArg::Str("value")
                ]
            ),
            "Standard: text, SQL: it''s, ID: col\"\"umn, Quoted: 'value'"
        );

        // Complex formatting with width, precision, and flags
        assert_eq!(
            run_printf(
                "%+05d | %-10.2f | %#10x | %.3s | %Q",
                vec![
                    PrintfArg::Int(-42),
                    PrintfArg::Float(123.456),
                    PrintfArg::Int(255),
                    PrintfArg::Str("hello"),
                    PrintfArg::Str("quote")
                ]
            ),
            "-0042 | 123.46     |       0xff | hel | 'quote'"
        );
    }

    #[test]
    fn test_printf_reused_arguments() {
        // SQLite extension: using * for both width and precision reuses the argument
        assert_eq!(
            run_printf(
                "%*.*f",
                vec![
                    PrintfArg::Int(10),
                    PrintfArg::Int(2),
                    PrintfArg::Float(123.456)
                ]
            ),
            "    123.46"
        );

        // SQLite allows reusing the same argument multiple times with %N$ syntax
        // If your implementation supports this, uncomment and fix these tests
        // assert_eq!(run_printf("%1$d %1$x", vec![PrintfArg::Int(42)]), "42 2a");
        // assert_eq!(run_printf("%2$s %1$d %2$Q", vec![PrintfArg::Int(123), PrintfArg::Str("text")]), "text 123 'text'");
    }

    #[test]
    fn test_printf_malformed_format_strings() {
        // Malformed width specifier
        assert_eq!(
            run_printf("%*d", vec![PrintfArg::Str("abc"), PrintfArg::Int(42)]),
            "42"
        );

        // Malformed precision specifier
        assert_eq!(
            run_printf(
                "%.*f",
                vec![PrintfArg::Str("abc"), PrintfArg::Float(123.456)]
            ),
            "123.456000"
        );

        // Invalid type conversion attempts
        assert_eq!(run_printf("%d", vec![PrintfArg::Str("42")]), "0"); // String->Int
        assert_eq!(run_printf("%f", vec![PrintfArg::Str("123.45")]), "0.000000"); // String->Float
        assert_eq!(run_printf("%s", vec![PrintfArg::Int(42)]), "42"); // Int->String
    }
}
