use core::f64;
use std::fmt::Write;

use crate::numeric::Numeric;
use crate::types::Value;
use crate::vdbe::Register;
use crate::LimboError;

fn get_exponential_formatted_str(number: &f64, uppercase: bool) -> crate::Result<String> {
    // TODO: Implement ryu or zmij style algorithm somewhere in the library and use it here instead
    let mut result = String::with_capacity(24);
    write!(&mut result, "{number:.6e}",).expect("write! to a String cannot fail; it panics on OOM");

    let parts = result.split_once('e');

    match parts {
        Some((mantissa, exponent)) => {
            match exponent.parse::<i32>() {
                Ok(exponent_number) => {
                    // we have mantissa in result[..pos]
                    if uppercase {
                        result.truncate(mantissa.len());
                        result.push('E');
                    } else {
                        // we already have 'e' after mantissa
                        result.truncate(mantissa.len() + 1);
                    }
                    write!(&mut result, "{exponent_number:+03}").unwrap();
                    Ok(result)
                }
                Err(_) => Err(LimboError::InternalError(
                    "unable to parse exponential expression's exponent".into(),
                )),
            }
        }
        None => Err(LimboError::InternalError(
            "unable to parse exponential expression".into(),
        )),
    }
}

// TODO: Support %!.3s. flags: - + 0 ! ,
#[inline(always)]
pub fn exec_printf(values: &[Register]) -> crate::Result<Value> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    // SQLite converts the format argument to text if not already text.
    // Non-text formats get converted via their string representation,
    // e.g. PRINTF(1, 'a') returns "1" not NULL.
    let format_value = values[0].get_value();
    let format_string: String;
    let format_str = match &format_value {
        Value::Text(t) => t.as_str(),
        Value::Null => return Ok(Value::Null),
        Value::Numeric(Numeric::Integer(i)) => {
            format_string = i.to_string();
            format_string.as_str()
        }
        Value::Numeric(Numeric::Float(f)) => {
            format_string = f64::from(*f).to_string();
            format_string.as_str()
        }
        Value::Blob(b) => {
            // Blob to string - use lossy UTF-8 conversion like SQLite
            format_string = String::from_utf8_lossy(b).to_string();
            format_string.as_str()
        }
    };

    let mut result = String::new();
    let mut args_index = 1;
    let mut chars = format_str.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            result.push(c);
            continue;
        }

        // Check for %% escape
        if let Some(&'%') = chars.peek() {
            chars.next(); // consume the second %
            result.push('%');
            continue;
        }

        let mut zero_pad = false;
        let mut width = None;
        let mut precision = None;

        if let Some(&'0') = chars.peek() {
            chars.next();
            zero_pad = true;
        }

        // Parse width
        let mut width_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                width_str.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if !width_str.is_empty() {
            match width_str.parse::<i32>() {
                Ok(w) => width = Some(w as usize),
                Err(_) => return Ok(Value::Null),
            }
        }

        if let Some(&'.') = chars.peek() {
            chars.next();
            let mut precision_str = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    precision_str.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            precision = if precision_str.is_empty() {
                Some(0)
            } else {
                match precision_str.parse::<i32>() {
                    Ok(p) => Some(p as usize),
                    Err(_) => return Ok(Value::Null),
                }
            };
        }

        match chars.next() {
            Some('d') | Some('i') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let int_value = match value {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    Value::Numeric(Numeric::Float(f)) => f64::from(*f) as i64,
                    _ => 0,
                };

                let mut formatted = if let Some(p) = precision {
                    if int_value < 0 {
                        let abs_value = int_value.unsigned_abs();
                        format!("-{abs_value:0p$}")
                    } else {
                        format!("{int_value:0p$}")
                    }
                } else {
                    int_value.to_string()
                };

                if let Some(w) = width {
                    if w > formatted.len() {
                        if zero_pad && precision.is_none() {
                            if int_value < 0 {
                                let abs_value = int_value.unsigned_abs();
                                let pad_width = w - 1;
                                formatted = format!("-{abs_value:0pad_width$}");
                            } else {
                                formatted = format!("{int_value:0w$}");
                            }
                        } else {
                            formatted = format!("{formatted:>w$}");
                        }
                    }
                }

                result.push_str(&formatted);
                args_index += 1;
            }
            Some('u') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Numeric(Numeric::Integer(_)) => {
                        let converted_value = value.as_uint();
                        write!(result, "{converted_value}")
                            .expect("write! to a String cannot fail; it panics on OOM");
                    }
                    _ => result.push('0'),
                }
                args_index += 1;
            }
            Some('s') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let mut formatted = match &values[args_index].get_value() {
                    Value::Text(t) => t.as_str().to_string(),
                    Value::Null => String::new(),
                    v => format!("{v}"),
                };
                if let Some(p) = precision {
                    formatted.truncate(p);
                }
                if let Some(w) = width {
                    if w > formatted.len() {
                        formatted = format!("{formatted:>w$}");
                    }
                }
                result.push_str(&formatted);
                args_index += 1;
            }
            Some('f') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let f_val = match value {
                    Value::Numeric(Numeric::Float(f)) => f64::from(*f),
                    Value::Numeric(Numeric::Integer(i)) => *i as f64,
                    _ => 0.0,
                };
                let p = precision.unwrap_or(6);
                let mut formatted = format!("{f_val:.p$}");
                if let Some(w) = width {
                    if w > formatted.len() {
                        if zero_pad {
                            if f_val.is_sign_negative() && f_val != 0.0 {
                                let abs_formatted = format!("{:.p$}", f_val.abs());
                                let pad_width = w - 1;
                                formatted = format!("-{abs_formatted:0>pad_width$}");
                            } else {
                                formatted = format!("{formatted:0>w$}");
                            }
                        } else {
                            formatted = format!("{formatted:>w$}");
                        }
                    }
                }
                result.push_str(&formatted);
                args_index += 1;
            }
            Some('e') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Numeric(Numeric::Float(f)) => {
                        let f = f64::from(*f);
                        match get_exponential_formatted_str(&f, false) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        }
                    }
                    Value::Numeric(Numeric::Integer(i)) => {
                        let f = *i as f64;
                        match get_exponential_formatted_str(&f, false) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        }
                    }
                    Value::Text(s) => {
                        let number: f64 = s
                            .as_str()
                            .trim_start()
                            .trim_end_matches(|c: char| !c.is_numeric())
                            .parse()
                            .unwrap_or(0.0);
                        match get_exponential_formatted_str(&number, false) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        };
                    }
                    _ => result.push_str("0.000000e+00"),
                }
                args_index += 1;
            }
            Some('E') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Numeric(Numeric::Float(f)) => {
                        let f = f64::from(*f);
                        match get_exponential_formatted_str(&f, true) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        }
                    }
                    Value::Numeric(Numeric::Integer(i)) => {
                        let f = *i as f64;
                        match get_exponential_formatted_str(&f, true) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        }
                    }
                    Value::Text(s) => {
                        let number: f64 = s
                            .as_str()
                            .trim_start()
                            .trim_end_matches(|c: char| !c.is_numeric())
                            .parse()
                            .unwrap_or(0.0);
                        match get_exponential_formatted_str(&number, true) {
                            Ok(str) => result.push_str(&str),
                            Err(e) => return Err(e),
                        };
                    }
                    _ => result.push_str("0.000000e+00"),
                }
                args_index += 1;
            }
            Some('c') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let char: char = match value {
                    Value::Text(s) => s.value.chars().next().unwrap_or('\0'),
                    _ => {
                        let as_str = format!("{value}");
                        as_str.chars().next().unwrap_or('\0')
                    }
                };
                if char != '\0' {
                    result.push(char);
                }
                args_index += 1;
            }
            Some('x') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let mut formatted = match value {
                    Value::Numeric(Numeric::Float(f)) => format!("{:x}", f64::from(*f) as i64),
                    Value::Numeric(Numeric::Integer(i)) => format!("{i:x}"),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        format!("{i:x}")
                    }
                    _ => "0".to_string(),
                };
                if let Some(w) = width {
                    if w > formatted.len() {
                        if zero_pad {
                            formatted = format!("{formatted:0>w$}");
                        } else {
                            formatted = format!("{formatted:>w$}");
                        }
                    }
                }
                result.push_str(&formatted);
                args_index += 1;
            }
            Some('X') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let mut formatted = match value {
                    Value::Numeric(Numeric::Float(f)) => format!("{:X}", f64::from(*f) as i64),
                    Value::Numeric(Numeric::Integer(i)) => format!("{i:X}"),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        format!("{i:X}")
                    }
                    _ => "0".to_string(),
                };
                if let Some(w) = width {
                    if w > formatted.len() {
                        if zero_pad {
                            formatted = format!("{formatted:0>w$}");
                        } else {
                            formatted = format!("{formatted:>w$}");
                        }
                    }
                }
                result.push_str(&formatted);
                args_index += 1;
            }
            Some('o') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let mut formatted = match value {
                    Value::Numeric(Numeric::Float(f)) => format!("{:o}", f64::from(*f) as i64),
                    Value::Numeric(Numeric::Integer(i)) => format!("{i:o}"),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        format!("{i:o}")
                    }
                    _ => "0".to_string(),
                };
                if let Some(w) = width {
                    if w > formatted.len() {
                        if zero_pad {
                            formatted = format!("{formatted:0>w$}");
                        } else {
                            formatted = format!("{formatted:>w$}");
                        }
                    }
                }
                result.push_str(&formatted);
                args_index += 1;
            }
            None => {
                return Err(LimboError::InvalidArgument(
                    "incomplete format specifier".into(),
                ))
            }
            _ => {
                return Err(LimboError::InvalidFormatter(
                    "this formatter is not supported".into(),
                ));
            }
        }
    }
    Ok(Value::build_text(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Register {
        Register::Value(Value::build_text(value.to_string()))
    }

    fn integer(value: i64) -> Register {
        Register::Value(Value::from_i64(value))
    }

    fn float(value: f64) -> Register {
        Register::Value(Value::from_f64(value))
    }

    #[test]
    fn test_printf_no_args() {
        assert_eq!(exec_printf(&[]).unwrap(), Value::Null);
    }

    #[test]
    fn test_printf_basic_string() {
        assert_eq!(
            exec_printf(&[text("Hello World")]).unwrap(),
            *text("Hello World").get_value()
        );
    }

    #[test]
    fn test_printf_string_formatting() {
        let test_cases = vec![
            // Simple string substitution
            (
                vec![text("Hello, %s!"), text("World")],
                text("Hello, World!"),
            ),
            // Multiple string substitutions
            (
                vec![text("%s %s!"), text("Hello"), text("World")],
                text("Hello World!"),
            ),
            // String with null value
            (
                vec![text("Hello, %s!"), Register::Value(Value::Null)],
                text("Hello, !"),
            ),
            // String with number conversion
            (vec![text("Value: %s"), integer(42)], text("Value: 42")),
            // Escaping percent sign
            (vec![text("100%% complete")], text("100% complete")),
        ];
        for (input, output) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *output.get_value());
        }
    }

    #[test]
    fn test_printf_integer_formatting() {
        let test_cases = vec![
            // Basic integer formatting
            (vec![text("Number: %d"), integer(42)], text("Number: 42")),
            // Negative integer
            (vec![text("Number: %d"), integer(-42)], text("Number: -42")),
            // Multiple integers
            (
                vec![text("%d + %d = %d"), integer(2), integer(3), integer(5)],
                text("2 + 3 = 5"),
            ),
            // Non-numeric value defaults to 0
            (
                vec![text("Number: %d"), text("not a number")],
                text("Number: 0"),
            ),
            (
                vec![text("Truncated float: %d"), float(3.9)],
                text("Truncated float: 3"),
            ),
            (vec![text("Number: %i"), integer(42)], text("Number: 42")),
        ];
        for (input, output) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *output.get_value())
        }
    }

    #[test]
    fn test_printf_unsigned_integer_formatting() {
        let test_cases = vec![
            // Basic
            (vec![text("Number: %u"), integer(42)], text("Number: 42")),
            // Multiple numbers
            (
                vec![text("%u + %u = %u"), integer(2), integer(3), integer(5)],
                text("2 + 3 = 5"),
            ),
            // Negative number should be represented as its uint representation
            (
                vec![text("Negative: %u"), integer(-1)],
                text("Negative: 18446744073709551615"),
            ),
            // Non-numeric value defaults to 0
            (vec![text("NaN: %u"), text("not a number")], text("NaN: 0")),
        ];
        for (input, output) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *output.get_value())
        }
    }

    #[test]
    fn test_printf_float_formatting() {
        let test_cases = vec![
            // Basic float formatting
            (
                vec![text("Number: %f"), float(42.5)],
                text("Number: 42.500000"),
            ),
            // Negative float
            (
                vec![text("Number: %f"), float(-42.5)],
                text("Number: -42.500000"),
            ),
            // Integer as float
            (
                vec![text("Number: %f"), integer(42)],
                text("Number: 42.000000"),
            ),
            // Multiple floats
            (
                vec![text("%f + %f = %f"), float(2.5), float(3.5), float(6.0)],
                text("2.500000 + 3.500000 = 6.000000"),
            ),
            // Non-numeric value defaults to 0.0
            (
                vec![text("Number: %f"), text("not a number")],
                text("Number: 0.000000"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_character_formatting() {
        let test_cases = vec![
            // Simple character
            (vec![text("character: %c"), text("a")], text("character: a")),
            // Character with string
            (
                vec![text("character: %c"), text("this is a test")],
                text("character: t"),
            ),
            // Character with empty
            (vec![text("character: %c"), text("")], text("character: ")),
            // Character with integer
            (
                vec![text("character: %c"), integer(123)],
                text("character: 1"),
            ),
            // Character with float
            (
                vec![text("character: %c"), float(42.5)],
                text("character: 4"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_exponential_formatting() {
        let test_cases = vec![
            // Simple number
            (
                vec![text("Exp: %e"), float(23000000.0)],
                text("Exp: 2.300000e+07"),
            ),
            // Negative number
            (
                vec![text("Exp: %e"), float(-23000000.0)],
                text("Exp: -2.300000e+07"),
            ),
            // Non integer float
            (
                vec![text("Exp: %e"), float(250.375)],
                text("Exp: 2.503750e+02"),
            ),
            // Positive, but smaller than zero
            (
                vec![text("Exp: %e"), float(0.0003235)],
                text("Exp: 3.235000e-04"),
            ),
            // Zero
            (vec![text("Exp: %e"), float(0.0)], text("Exp: 0.000000e+00")),
            // Uppercase "e"
            (
                vec![text("Exp: %e"), float(0.0003235)],
                text("Exp: 3.235000e-04"),
            ),
            // String with integer number
            (
                vec![text("Exp: %e"), text("123")],
                text("Exp: 1.230000e+02"),
            ),
            // String with floating point number
            (
                vec![text("Exp: %e"), text("123.45")],
                text("Exp: 1.234500e+02"),
            ),
            // String with number with leftmost zeroes
            (
                vec![text("Exp: %e"), text("00123")],
                text("Exp: 1.230000e+02"),
            ),
            // String with text
            (
                vec![text("Exp: %e"), text("test")],
                text("Exp: 0.000000e+00"),
            ),
            // String starting with number, but with text on the end
            (
                vec![text("Exp: %e"), text("123ab")],
                text("Exp: 1.230000e+02"),
            ),
            // String starting with text, but with number on the end
            (
                vec![text("Exp: %e"), text("ab123")],
                text("Exp: 0.000000e+00"),
            ),
            // String with exponential representation
            (
                vec![text("Exp: %e"), text("1.230000e+02")],
                text("Exp: 1.230000e+02"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_hexadecimal_formatting() {
        let test_cases = vec![
            // Simple number
            (vec![text("hex: %x"), integer(4)], text("hex: 4")),
            // Bigger Number
            (
                vec![text("hex: %x"), integer(15565303546)],
                text("hex: 39fc3aefa"),
            ),
            // Uppercase letters
            (
                vec![text("hex: %X"), integer(15565303546)],
                text("hex: 39FC3AEFA"),
            ),
            // Negative
            (
                vec![text("hex: %x"), integer(-15565303546)],
                text("hex: fffffffc603c5106"),
            ),
            // Float
            (vec![text("hex: %x"), float(42.5)], text("hex: 2a")),
            // Negative Float
            (
                vec![text("hex: %x"), float(-42.5)],
                text("hex: ffffffffffffffd6"),
            ),
            // Text
            (vec![text("hex: %x"), text("42")], text("hex: 2a")),
            // Empty Text
            (vec![text("hex: %x"), text("")], text("hex: 0")),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_octal_formatting() {
        let test_cases = vec![
            // Simple number
            (vec![text("octal: %o"), integer(4)], text("octal: 4")),
            // Bigger Number
            (
                vec![text("octal: %o"), integer(15565303546)],
                text("octal: 163760727372"),
            ),
            // Negative
            (
                vec![text("octal: %o"), integer(-15565303546)],
                text("octal: 1777777777614017050406"),
            ),
            // Float
            (vec![text("octal: %o"), float(42.5)], text("octal: 52")),
            // Negative Float
            (
                vec![text("octal: %o"), float(-42.5)],
                text("octal: 1777777777777777777726"),
            ),
            // Text
            (vec![text("octal: %o"), text("42")], text("octal: 52")),
            // Empty Text
            (vec![text("octal: %o"), text("")], text("octal: 0")),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_mixed_formatting() {
        let test_cases = vec![
            // Mix of string and integer
            (
                vec![text("%s: %d"), text("Count"), integer(42)],
                text("Count: 42"),
            ),
            // Mix of all types
            (
                vec![
                    text("%s: %d (%f%%)"),
                    text("Progress"),
                    integer(75),
                    float(75.5),
                ],
                text("Progress: 75 (75.500000%)"),
            ),
            // Complex format
            (
                vec![
                    text("Name: %s, ID: %d, Score: %f"),
                    text("John"),
                    integer(123),
                    float(95.5),
                ],
                text("Name: John, ID: 123, Score: 95.500000"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }

    #[test]
    fn test_printf_error_cases() {
        let error_cases = vec![
            // Not enough arguments
            vec![text("%d %d"), integer(42)],
            // Invalid format string
            vec![text("%z"), integer(42)],
            // Incomplete format specifier
            vec![text("incomplete %")],
        ];

        for case in error_cases {
            assert!(exec_printf(&case).is_err());
        }
    }

    #[test]
    fn test_get_exponential_formatted_str() {
        let test_cases = vec![
            (23000000.0, false, "2.300000e+07"),
            (-23000000.0, false, "-2.300000e+07"),
            (250.375, false, "2.503750e+02"),
            (0.0003235, false, "3.235000e-04"),
            (0.0, false, "0.000000e+00"),
            (23000000.0, true, "2.300000E+07"),
            (-23000000.0, true, "-2.300000E+07"),
            (250.375, true, "2.503750E+02"),
            (0.0003235, true, "3.235000E-04"),
            (0.0, true, "0.000000E+00"),
        ];

        for (number, uppercase, expected) in test_cases {
            let result = get_exponential_formatted_str(&number, uppercase).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_printf_edge_cases() {
        let test_cases = vec![
            // Empty format string
            (vec![text("")], text("")),
            // Only percent signs
            (vec![text("%%%%")], text("%%")),
            // String with no format specifiers
            (vec![text("No substitutions")], text("No substitutions")),
            // Multiple consecutive format specifiers
            (
                vec![text("%d%d%d"), integer(1), integer(2), integer(3)],
                text("123"),
            ),
            // Format string with special characters
            (
                vec![text("Special chars: %s"), text("\n\t\r")],
                text("Special chars: \n\t\r"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
        }
    }
}
