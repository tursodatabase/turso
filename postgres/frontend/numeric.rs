use std::num::ParseIntError;

const VERTICAL_TAB: char = '\u{b}';

pub fn str_to_i64_checked(input: impl AsRef<str>) -> Result<i64, ParseIntError> {
    let mut input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB);

    let sign = match input.chars().next() {
        Some(s @ ('+' | '-')) => {
            input = &input[1..];
            Some(s)
        }
        _ => None,
    };

    let (radix, clean) = match input.get(..2) {
        Some("0x" | "0X") => (16, &input[2..]),
        Some("0o" | "0O") => (8, &input[2..]),
        Some("0b" | "0B") => (2, &input[2..]),
        _ => (10, input),
    };

    let clean = clean.replace('_', "");

    match sign {
        Some(s) => {
            let signed = format!("{s}{clean}");
            i64::from_str_radix(&signed, radix)
        }
        None => i64::from_str_radix(&clean, radix),
    }
}
