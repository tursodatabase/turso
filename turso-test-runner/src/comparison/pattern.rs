use super::{ComparisonResult, format_rows};
use regex::RegexBuilder;

/// Compare error string against a pattern (case-insensitive)
/// The pattern is escaped to treat it as a literal substring match
pub fn compare_error(actual: &str, pattern: &str) -> ComparisonResult {
    // Escape the pattern to treat it as a literal string for substring matching
    let escaped_pattern = regex::escape(pattern);
    match RegexBuilder::new(&escaped_pattern)
        .case_insensitive(true)
        .build()
    {
        Ok(re) => {
            if re.is_match(actual) {
                ComparisonResult::Match
            } else {
                ComparisonResult::mismatch(format!(
                    "error message '{}' does not contain expected pattern '{}'",
                    actual, pattern
                ))
            }
        }
        Err(e) => ComparisonResult::mismatch(format!("invalid regex pattern: {}", e)),
    }
}

/// Compare rows against a regex pattern
pub fn compare(actual: &[Vec<String>], pattern: &str) -> ComparisonResult {
    let actual_str = format_rows(actual);

    match RegexBuilder::new(pattern).build() {
        Ok(re) => {
            if re.is_match(&actual_str) {
                ComparisonResult::Match
            } else {
                ComparisonResult::mismatch(format!(
                    "output does not match pattern\nPattern: {}\nActual:\n{}",
                    pattern, actual_str
                ))
            }
        }
        Err(e) => ComparisonResult::mismatch(format!("invalid regex pattern: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_match() {
        let actual = vec![vec!["42".to_string()]];
        let pattern = r"^\d+$";
        assert!(compare(&actual, pattern).is_match());
    }

    #[test]
    fn test_pattern_match_multiline() {
        let actual = vec![vec!["1".to_string()], vec!["2".to_string()]];
        let pattern = r"(?m)^\d+$";
        assert!(compare(&actual, pattern).is_match());
    }

    #[test]
    fn test_pattern_mismatch() {
        let actual = vec![vec!["abc".to_string()]];
        let pattern = r"^\d+$";

        let result = compare(&actual, pattern);
        assert!(!result.is_match());
    }

    #[test]
    fn test_pattern_invalid_regex() {
        let actual = vec![vec!["test".to_string()]];
        let pattern = r"[invalid";

        let result = compare(&actual, pattern);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("invalid regex"));
        }
    }

    #[test]
    fn test_pattern_match_negative_number() {
        let actual = vec![vec!["-12345".to_string()]];
        let pattern = r"^-?\d+$";
        assert!(compare(&actual, pattern).is_match());
    }
}
