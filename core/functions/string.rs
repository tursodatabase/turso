//! Common SQL string functions not in SQLite's built-in set.
//!
//! `repeat`, `lpad`, `rpad` are present in PostgreSQL, MySQL and Oracle. All
//! three operate on the character count of the input string (not byte length),
//! matching the PG contract.

use crate::types::Value;
use crate::{LimboError, Result};

/// `repeat(string, n)` — return `string` concatenated `n` times. Returns NULL
/// for NULL input or NULL count. `n <= 0` produces an empty string (matches
/// PG; SQLite's analogue isn't built-in).
pub fn exec_repeat(input: &Value, count: &Value) -> Result<Value> {
    let s = match input {
        Value::Null => return Ok(Value::Null),
        Value::Text(t) => t.as_str().to_owned(),
        v => v.to_string(),
    };
    let n = match count {
        Value::Null => return Ok(Value::Null),
        Value::Numeric(crate::Numeric::Integer(i)) => *i,
        Value::Numeric(crate::Numeric::Float(f)) => {
            let f: f64 = (*f).into();
            f as i64
        }
        Value::Text(t) => t.as_str().parse::<i64>().unwrap_or(0),
        _ => return Ok(Value::Null),
    };
    if n <= 0 {
        return Ok(Value::build_text(String::new()));
    }
    // Check before allocating: the allocator aborts the process on failure.
    let total = (s.len() as i64).checked_mul(n);
    if total.is_none_or(|bytes| bytes > Value::MAX_BLOB_LENGTH) {
        return Err(LimboError::TooBig);
    }
    Ok(Value::build_text(s.repeat(n as usize)))
}

/// `lpad(string, length [, fill])` — left-pad `string` with `fill` (default
/// space) so the result is exactly `length` characters. If `string` already
/// has `length` or more characters, it is truncated from the right.
pub fn exec_lpad(input: &Value, length: &Value, fill: Option<&Value>) -> Result<Value> {
    pad(input, length, fill, /* on_left */ true)
}

/// `rpad(string, length [, fill])` — right-pad `string` with `fill` (default
/// space) to exactly `length` characters. Same truncation behaviour as
/// [`exec_lpad`].
pub fn exec_rpad(input: &Value, length: &Value, fill: Option<&Value>) -> Result<Value> {
    pad(input, length, fill, /* on_left */ false)
}

/// Shared body for [`exec_lpad`] / [`exec_rpad`]. The only difference between
/// the two is which side the padding goes on, so we route by a boolean to
/// avoid duplicating the truncation, count, and fill-cycling logic.
fn pad(input: &Value, length: &Value, fill: Option<&Value>, on_left: bool) -> Result<Value> {
    let s = match input {
        Value::Null => return Ok(Value::Null),
        Value::Text(t) => t.as_str().to_owned(),
        v => v.to_string(),
    };
    let target_len = match length {
        Value::Null => return Ok(Value::Null),
        Value::Numeric(crate::Numeric::Integer(i)) => *i,
        Value::Numeric(crate::Numeric::Float(f)) => {
            let f: f64 = (*f).into();
            f as i64
        }
        Value::Text(t) => t.as_str().parse::<i64>().unwrap_or(0),
        _ => return Ok(Value::Null),
    };
    if target_len <= 0 {
        return Ok(Value::build_text(String::new()));
    }
    // Check before allocating: the allocator aborts the process on failure.
    if target_len > Value::MAX_BLOB_LENGTH {
        return Err(LimboError::TooBig);
    }
    let target_len = target_len as usize;
    let char_count = s.chars().count();
    if char_count >= target_len {
        // Truncate. lpad/rpad both trim from the right when over-length, per PG.
        return Ok(Value::build_text(
            s.chars().take(target_len).collect::<String>(),
        ));
    }
    // Pull the fill string (default space) and bail if it's empty: there's
    // nothing to repeat, so just return the input as-is.
    let fill_str = match fill {
        None => " ".to_string(),
        Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Text(t)) => t.as_str().to_owned(),
        Some(v) => v.to_string(),
    };
    let fill_chars: Vec<char> = fill_str.chars().collect();
    if fill_chars.is_empty() {
        return Ok(Value::build_text(s));
    }
    let pad: String = fill_chars
        .iter()
        .cycle()
        .take(target_len - char_count)
        .collect();
    Ok(Value::build_text(if on_left {
        format!("{pad}{s}")
    } else {
        format!("{s}{pad}")
    }))
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
        assert_eq!(exec_repeat(&t("ab"), &i(3)).unwrap(), t("ababab"));
        assert_eq!(exec_repeat(&t("x"), &i(0)).unwrap(), t(""));
        assert_eq!(exec_repeat(&t("x"), &i(-1)).unwrap(), t(""));
        assert_eq!(exec_repeat(&t(""), &i(5)).unwrap(), t(""));
    }

    #[test]
    fn repeat_null() {
        assert!(matches!(
            exec_repeat(&Value::Null, &i(3)).unwrap(),
            Value::Null
        ));
        assert!(matches!(
            exec_repeat(&t("x"), &Value::Null).unwrap(),
            Value::Null
        ));
    }

    #[test]
    fn lpad_basic() {
        assert_eq!(exec_lpad(&t("abc"), &i(6), None).unwrap(), t("   abc"));
        assert_eq!(
            exec_lpad(&t("abc"), &i(6), Some(&t("xy"))).unwrap(),
            t("xyxabc")
        );
        // Already long enough — truncate from the right.
        assert_eq!(exec_lpad(&t("abcdef"), &i(3), None).unwrap(), t("abc"));
        // Equal length — pass through.
        assert_eq!(
            exec_lpad(&t("abc"), &i(3), Some(&t("x"))).unwrap(),
            t("abc")
        );
    }

    #[test]
    fn rpad_basic() {
        assert_eq!(exec_rpad(&t("abc"), &i(6), None).unwrap(), t("abc   "));
        assert_eq!(
            exec_rpad(&t("abc"), &i(6), Some(&t("xy"))).unwrap(),
            t("abcxyx")
        );
        assert_eq!(exec_rpad(&t("abcdef"), &i(3), None).unwrap(), t("abc"));
    }

    #[test]
    fn pad_null() {
        assert!(matches!(
            exec_lpad(&Value::Null, &i(3), None).unwrap(),
            Value::Null
        ));
        assert!(matches!(
            exec_lpad(&t("x"), &Value::Null, None).unwrap(),
            Value::Null
        ));
        assert!(matches!(
            exec_lpad(&t("x"), &i(3), Some(&Value::Null)).unwrap(),
            Value::Null
        ));
    }

    #[test]
    fn pad_empty_fill_returns_input() {
        // PG: if fill is empty the result is the input unchanged.
        assert_eq!(
            exec_lpad(&t("abc"), &i(10), Some(&t(""))).unwrap(),
            t("abc")
        );
        assert_eq!(
            exec_rpad(&t("abc"), &i(10), Some(&t(""))).unwrap(),
            t("abc")
        );
    }

    #[test]
    fn pad_unicode_counts_characters_not_bytes() {
        // 'é' is 2 bytes in UTF-8 but one char. lpad/rpad pad to character
        // count, so a 3-char input needs 1 more char to reach length 4.
        assert_eq!(exec_lpad(&t("aéc"), &i(4), None).unwrap(), t(" aéc"));
        assert_eq!(exec_rpad(&t("aéc"), &i(4), None).unwrap(), t("aéc "));
    }

    /// Regression tests for the allocator abort reported in #11219.
    ///
    /// Before the length check, `lpad`/`rpad`/`repeat` sized the result from
    /// the caller's argument and allocated it directly, so a length near
    /// `i64::MAX` aborted the whole process with "memory allocation of
    /// 9223372036854775806 bytes failed" instead of raising an error. Without
    /// the fix these tests do not fail — they abort the test runner.
    #[test]
    fn pad_rejects_a_length_over_the_limit() {
        for n in [i64::MAX, Value::MAX_BLOB_LENGTH + 1, 4_294_967_296] {
            assert!(
                matches!(
                    exec_lpad(&t("a"), &i(n), Some(&t("b"))),
                    Err(LimboError::TooBig)
                ),
                "lpad should refuse length {n}"
            );
            assert!(
                matches!(
                    exec_rpad(&t("a"), &i(n), Some(&t("b"))),
                    Err(LimboError::TooBig)
                ),
                "rpad should refuse length {n}"
            );
        }
    }

    #[test]
    fn repeat_rejects_a_count_that_overflows_the_limit() {
        assert!(matches!(
            exec_repeat(&t("a"), &i(i64::MAX)),
            Err(LimboError::TooBig)
        ));
        assert!(matches!(
            exec_repeat(&t("ab"), &i(Value::MAX_BLOB_LENGTH)),
            Err(LimboError::TooBig)
        ));
    }

    #[test]
    fn pad_and_repeat_still_accept_lengths_the_limit_allows() {
        // The guard must not be a blanket refusal of large-but-legal arguments.
        assert_eq!(
            exec_lpad(&t("a"), &i(5), Some(&t("b"))).unwrap(),
            t("bbbba")
        );
        assert_eq!(
            exec_rpad(&t("a"), &i(5), Some(&t("b"))).unwrap(),
            t("abbbb")
        );
        assert_eq!(exec_repeat(&t("ab"), &i(3)).unwrap(), t("ababab"));
        // The check is `>`, so MAX_BLOB_LENGTH itself must be admitted. An
        // empty fill returns the input before any padding is built, so this
        // pins that off-by-one without the ~1 GB allocation and ~26 s that
        // asserting it through the padding path would cost a parallel CI job.
        assert_eq!(
            exec_lpad(&t("a"), &i(Value::MAX_BLOB_LENGTH), Some(&t(""))).unwrap(),
            t("a")
        );
    }
}
