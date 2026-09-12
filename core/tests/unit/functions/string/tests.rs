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
