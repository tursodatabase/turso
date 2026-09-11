//! Common SQL string functions not in SQLite's built-in set.
//!
//! `repeat`, `lpad`, `rpad` are present in PostgreSQL, MySQL and Oracle. All
//! three operate on the character count of the input string (not byte length),
//! matching the PG contract.

use crate::types::Value;

/// `repeat(string, n)` — return `string` concatenated `n` times. Returns NULL
/// for NULL input or NULL count. `n <= 0` produces an empty string (matches
/// PG; SQLite's analogue isn't built-in).
pub fn exec_repeat(input: &Value, count: &Value) -> Value {
    let s = match input {
        Value::Null => return Value::Null,
        Value::Text(t) => t.as_str().to_owned(),
        v => v.to_string(),
    };
    let n = match count {
        Value::Null => return Value::Null,
        Value::Numeric(crate::Numeric::Integer(i)) => *i,
        Value::Numeric(crate::Numeric::Float(f)) => {
            let f: f64 = (*f).into();
            f as i64
        }
        Value::Text(t) => t.as_str().parse::<i64>().unwrap_or(0),
        _ => return Value::Null,
    };
    if n <= 0 {
        return Value::build_text(String::new());
    }
    Value::build_text(s.repeat(n as usize))
}

/// `lpad(string, length [, fill])` — left-pad `string` with `fill` (default
/// space) so the result is exactly `length` characters. If `string` already
/// has `length` or more characters, it is truncated from the right.
pub fn exec_lpad(input: &Value, length: &Value, fill: Option<&Value>) -> Value {
    pad(input, length, fill, /* on_left */ true)
}

/// `rpad(string, length [, fill])` — right-pad `string` with `fill` (default
/// space) to exactly `length` characters. Same truncation behaviour as
/// [`exec_lpad`].
pub fn exec_rpad(input: &Value, length: &Value, fill: Option<&Value>) -> Value {
    pad(input, length, fill, /* on_left */ false)
}

/// Shared body for [`exec_lpad`] / [`exec_rpad`]. The only difference between
/// the two is which side the padding goes on, so we route by a boolean to
/// avoid duplicating the truncation, count, and fill-cycling logic.
fn pad(input: &Value, length: &Value, fill: Option<&Value>, on_left: bool) -> Value {
    let s = match input {
        Value::Null => return Value::Null,
        Value::Text(t) => t.as_str().to_owned(),
        v => v.to_string(),
    };
    let target_len = match length {
        Value::Null => return Value::Null,
        Value::Numeric(crate::Numeric::Integer(i)) => *i,
        Value::Numeric(crate::Numeric::Float(f)) => {
            let f: f64 = (*f).into();
            f as i64
        }
        Value::Text(t) => t.as_str().parse::<i64>().unwrap_or(0),
        _ => return Value::Null,
    };
    if target_len <= 0 {
        return Value::build_text(String::new());
    }
    let target_len = target_len as usize;
    let char_count = s.chars().count();
    if char_count >= target_len {
        // Truncate. lpad/rpad both trim from the right when over-length, per PG.
        return Value::build_text(s.chars().take(target_len).collect::<String>());
    }
    // Pull the fill string (default space) and bail if it's empty: there's
    // nothing to repeat, so just return the input as-is.
    let fill_str = match fill {
        None => " ".to_string(),
        Some(Value::Null) => return Value::Null,
        Some(Value::Text(t)) => t.as_str().to_owned(),
        Some(v) => v.to_string(),
    };
    let fill_chars: Vec<char> = fill_str.chars().collect();
    if fill_chars.is_empty() {
        return Value::build_text(s);
    }
    let pad: String = fill_chars
        .iter()
        .cycle()
        .take(target_len - char_count)
        .collect();
    Value::build_text(if on_left {
        format!("{pad}{s}")
    } else {
        format!("{s}{pad}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Value {
        Value::build_text(s.to_string())
    }
    fn i(n: i64) -> Value {
        Value::from_i64(n)
    }

    #[test]
    fn repeat_basic() {
        assert_eq!(exec_repeat(&t("ab"), &i(3)), t("ababab"));
        assert_eq!(exec_repeat(&t("x"), &i(0)), t(""));
        assert_eq!(exec_repeat(&t("x"), &i(-1)), t(""));
        assert_eq!(exec_repeat(&t(""), &i(5)), t(""));
    }

    #[test]
    fn repeat_null() {
        assert!(matches!(exec_repeat(&Value::Null, &i(3)), Value::Null));
        assert!(matches!(exec_repeat(&t("x"), &Value::Null), Value::Null));
    }

    #[test]
    fn lpad_basic() {
        assert_eq!(exec_lpad(&t("abc"), &i(6), None), t("   abc"));
        assert_eq!(exec_lpad(&t("abc"), &i(6), Some(&t("xy"))), t("xyxabc"));
        // Already long enough — truncate from the right.
        assert_eq!(exec_lpad(&t("abcdef"), &i(3), None), t("abc"));
        // Equal length — pass through.
        assert_eq!(exec_lpad(&t("abc"), &i(3), Some(&t("x"))), t("abc"));
    }

    #[test]
    fn rpad_basic() {
        assert_eq!(exec_rpad(&t("abc"), &i(6), None), t("abc   "));
        assert_eq!(exec_rpad(&t("abc"), &i(6), Some(&t("xy"))), t("abcxyx"));
        assert_eq!(exec_rpad(&t("abcdef"), &i(3), None), t("abc"));
    }

    #[test]
    fn pad_null() {
        assert!(matches!(exec_lpad(&Value::Null, &i(3), None), Value::Null));
        assert!(matches!(
            exec_lpad(&t("x"), &Value::Null, None),
            Value::Null
        ));
        assert!(matches!(
            exec_lpad(&t("x"), &i(3), Some(&Value::Null)),
            Value::Null
        ));
    }

    #[test]
    fn pad_empty_fill_returns_input() {
        // PG: if fill is empty the result is the input unchanged.
        assert_eq!(exec_lpad(&t("abc"), &i(10), Some(&t(""))), t("abc"));
        assert_eq!(exec_rpad(&t("abc"), &i(10), Some(&t(""))), t("abc"));
    }

    #[test]
    fn pad_unicode_counts_characters_not_bytes() {
        // 'é' is 2 bytes in UTF-8 but one char. lpad/rpad pad to character
        // count, so a 3-char input needs 1 more char to reach length 4.
        assert_eq!(exec_lpad(&t("aéc"), &i(4), None), t(" aéc"));
        assert_eq!(exec_rpad(&t("aéc"), &i(4), None), t("aéc "));
    }
}
