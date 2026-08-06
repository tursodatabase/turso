pub mod datetime;
pub mod math;
pub mod printf;
pub mod string;

/// Reads PostgreSQL's boolean input syntax: `true`/`false`, `t`/`f`,
/// `yes`/`no`, `y`/`n`, `on`/`off` and `1`/`0`, case-insensitively, ignoring
/// surrounding whitespace, and accepting any unambiguous prefix of the
/// spelled-out words (`tr`, `fal`, `ye`). None for anything else.
///
/// `o` alone is rejected because it could start either `on` or `off`, which
/// is why the `on`/`off` arm needs two characters where the others take one.
pub fn parse_boolean_text(text: &str) -> Option<bool> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let lower = text.to_ascii_lowercase();
    // A prefix of `word`, requiring at least `min_len` characters of it.
    let prefix_of = |word: &str, min_len: usize| {
        lower.len() >= min_len && lower.len() <= word.len() && word.starts_with(&lower)
    };
    match lower.as_bytes()[0] {
        b't' if prefix_of("true", 1) => Some(true),
        b'f' if prefix_of("false", 1) => Some(false),
        b'y' if prefix_of("yes", 1) => Some(true),
        b'n' if prefix_of("no", 1) => Some(false),
        b'o' if prefix_of("on", 2) => Some(true),
        b'o' if prefix_of("off", 2) => Some(false),
        b'1' if lower.len() == 1 => Some(true),
        b'0' if lower.len() == 1 => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod boolean_input_tests {
    use super::parse_boolean_text;

    #[test]
    fn every_spelling_postgresql_accepts() {
        for s in ["t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"] {
            assert_eq!(parse_boolean_text(s), Some(true), "{s} should be true");
        }
        for s in [
            "f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0",
        ] {
            assert_eq!(parse_boolean_text(s), Some(false), "{s} should be false");
        }
    }

    #[test]
    fn case_and_surrounding_space_do_not_matter() {
        for s in ["TRUE", "True", "  true  ", "\tT\n", " YES "] {
            assert_eq!(parse_boolean_text(s), Some(true), "{s} should be true");
        }
        for s in ["FALSE", "  f  ", "Off", " N "] {
            assert_eq!(parse_boolean_text(s), Some(false), "{s} should be false");
        }
    }

    /// Numbers do not go through this function — PostgreSQL treats any
    /// nonzero number as true, which the caller handles.
    #[test]
    fn digits_are_only_accepted_as_the_single_characters_1_and_0() {
        assert_eq!(parse_boolean_text("1"), Some(true));
        assert_eq!(parse_boolean_text("0"), Some(false));
        assert_eq!(parse_boolean_text("2"), None);
        assert_eq!(parse_boolean_text("10"), None);
    }

    #[test]
    fn anything_else_is_rejected() {
        // `o` could start either `on` or `off`.
        for s in [
            "o", "", "   ", "bogus", "test", "yeah", "nay", "truer", "offx", "2", "-1", "10", "t f",
        ] {
            assert_eq!(parse_boolean_text(s), None, "{s} should be rejected");
        }
    }
}
