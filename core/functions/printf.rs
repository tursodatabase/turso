use core::f64;

use crate::types::Value;
use crate::vdbe::Register;
use crate::LimboError;

fn get_exponential_formatted_str(number: &f64, uppercase: bool) -> crate::Result<String> {
    let pre_formatted = format!("{number:.6e}");
    let mut parts = pre_formatted.split("e");

    let maybe_base = parts.next();
    let maybe_exponent = parts.next();

    let mut result = String::new();
    match (maybe_base, maybe_exponent) {
        (Some(base), Some(exponent)) => {
            result.push_str(base);
            result.push_str(if uppercase { "E" } else { "e" });

            match exponent.parse::<i32>() {
                Ok(exponent_number) => {
                    let exponent_fmt = format!("{exponent_number:+03}");
                    result.push_str(&exponent_fmt);
                    Ok(result)
                }
                Err(_) => Err(LimboError::InternalError(
                    "unable to parse exponential expression's exponent".into(),
                )),
            }
        }
        (_, _) => Err(LimboError::InternalError(
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
    let format_str = match &values[0].get_value() {
        Value::Text(t) => t.as_str(),
        _ => return Ok(Value::Null),
    };

    let mut result = String::new();
    let mut args_index = 1;
    let mut chars = format_str.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            result.push(c);
            continue;
        }

        match chars.next() {
            Some('%') => {
                result.push('%');
                continue;
            }
            Some('d') | Some('i') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Integer(_) => result.push_str(&format!("{value}")),
                    Value::Float(_) => result.push_str(&format!("{value}")),
                    _ => result.push('0'),
                }
                args_index += 1;
            }
            Some('u') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Integer(_) => {
                        let converted_value = value.as_uint();
                        result.push_str(&format!("{converted_value}"))
                    }
                    _ => result.push('0'),
                }
                args_index += 1;
            }
            Some('s') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                match &values[args_index].get_value() {
                    Value::Text(t) => result.push_str(t.as_str()),
                    Value::Null => (),
                    v => result.push_str(&format!("{v}")),
                }
                args_index += 1;
            }
            Some('f') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Float(f) => result.push_str(&format!("{f:.6}")),
                    Value::Integer(i) => result.push_str(&format!("{:.6}", *i as f64)),
                    _ => result.push_str("0.000000"),
                }
                args_index += 1;
            }
            Some('e') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Float(f) => match get_exponential_formatted_str(f, false) {
                        Ok(str) => result.push_str(&str),
                        Err(e) => return Err(e),
                    },
                    Value::Integer(i) => {
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
                    Value::Float(f) => match get_exponential_formatted_str(f, false) {
                        Ok(str) => result.push_str(&str),
                        Err(e) => return Err(e),
                    },
                    Value::Integer(i) => {
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
            Some('c') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                let value_str: String = format!("{value}");
                if !value_str.is_empty() {
                    result.push_str(&value_str[0..1]);
                }
                args_index += 1;
            }
            Some('x') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Float(f) => result.push_str(&format!("{:x}", *f as i64)),
                    Value::Integer(i) => result.push_str(&format!("{i:x}")),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        result.push_str(&format!("{i:x}"))
                    }
                    _ => result.push('0'),
                }
                args_index += 1;
            }
            Some('X') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Float(f) => result.push_str(&format!("{:X}", *f as i64)),
                    Value::Integer(i) => result.push_str(&format!("{i:X}")),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        result.push_str(&format!("{i:X}"))
                    }
                    _ => result.push('0'),
                }
                args_index += 1;
            }
            Some('o') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                let value = &values[args_index].get_value();
                match value {
                    Value::Float(f) => result.push_str(&format!("{:o}", *f as i64)),
                    Value::Integer(i) => result.push_str(&format!("{i:o}")),
                    Value::Text(s) => {
                        let i: i64 = s.as_str().parse::<i64>().unwrap_or(0);
                        result.push_str(&format!("{i:o}"))
                    }
                    _ => result.push('0'),
                }
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
        Register::Value(Value::build_text(value))
    }

    fn integer(value: i64) -> Register {
        Register::Value(Value::Integer(value))
    }

    fn float(value: f64) -> Register {
        Register::Value(Value::Float(value))
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
