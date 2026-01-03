use super::{format_rows, parse_expected_rows, ComparisonResult};
use similar::{ChangeTag, TextDiff};

/// Compare rows exactly (same order, same values)
/// Uses the `similar` crate to produce a unified diff on mismatch
pub fn compare(actual: &[Vec<String>], expected_lines: &[String]) -> ComparisonResult {
    let expected_rows = parse_expected_rows(expected_lines);

    let actual_str = format_rows(actual);
    let expected_str = format_rows(&expected_rows);

    if actual_str == expected_str {
        return ComparisonResult::Match;
    }

    // Generate unified diff
    let diff = TextDiff::from_lines(&expected_str, &actual_str);

    let mut diff_output = String::new();
    diff_output.push_str("--- expected\n");
    diff_output.push_str("+++ actual\n");

    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        diff_output.push_str(&format!("{}{}", sign, change));
    }

    ComparisonResult::mismatch(diff_output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_empty() {
        let actual: Vec<Vec<String>> = vec![];
        let expected: Vec<String> = vec![];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_exact_single_row() {
        let actual = vec![vec!["1".to_string(), "Alice".to_string()]];
        let expected = vec!["1|Alice".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_exact_multiple_rows() {
        let actual = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];
        let expected = vec!["1|Alice".to_string(), "2|Bob".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_exact_single_column() {
        let actual = vec![vec!["1".to_string()], vec!["2".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_exact_mismatch_shows_diff() {
        let actual = vec![
            vec!["1".to_string(), "Bob".to_string()],
            vec!["2".to_string(), "Charlie".to_string()],
        ];
        let expected = vec!["1|Alice".to_string(), "2|Bob".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            // Should contain diff markers
            assert!(reason.contains("--- expected"));
            assert!(reason.contains("+++ actual"));
            assert!(reason.contains("-1|Alice"));
            assert!(reason.contains("+1|Bob"));
        }
    }

    #[test]
    fn test_exact_extra_row() {
        let actual = vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        let expected = vec!["1".to_string(), "2".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("+3"));
        }
    }

    #[test]
    fn test_exact_missing_row() {
        let actual = vec![vec!["1".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("-2"));
        }
    }
}
