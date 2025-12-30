use super::{ComparisonResult, parse_expected_rows};
use std::collections::HashSet;

/// Compare rows as sets (order-independent)
pub fn compare(actual: &[Vec<String>], expected_lines: &[String]) -> ComparisonResult {
    let expected_rows = parse_expected_rows(expected_lines);

    // Convert to sets of formatted rows
    let actual_set: HashSet<String> = actual.iter().map(|row| row.join("|")).collect();
    let expected_set: HashSet<String> = expected_rows.iter().map(|row| row.join("|")).collect();

    if actual_set == expected_set {
        return ComparisonResult::Match;
    }

    // Find differences
    let missing: Vec<_> = expected_set.difference(&actual_set).collect();
    let extra: Vec<_> = actual_set.difference(&expected_set).collect();

    let mut reason = String::new();

    if !missing.is_empty() {
        reason.push_str("Missing rows:\n");
        for row in &missing {
            reason.push_str(&format!("  - {}\n", row));
        }
    }

    if !extra.is_empty() {
        reason.push_str("Extra rows:\n");
        for row in &extra {
            reason.push_str(&format!("  + {}\n", row));
        }
    }

    ComparisonResult::mismatch(reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unordered_empty() {
        let actual: Vec<Vec<String>> = vec![];
        let expected: Vec<String> = vec![];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_unordered_same_order() {
        let actual = vec![vec!["1".to_string()], vec!["2".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_unordered_different_order() {
        let actual = vec![vec!["2".to_string()], vec!["1".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_unordered_multi_column() {
        let actual = vec![
            vec!["2".to_string(), "Bob".to_string()],
            vec!["1".to_string(), "Alice".to_string()],
        ];
        let expected = vec!["1|Alice".to_string(), "2|Bob".to_string()];
        assert!(compare(&actual, &expected).is_match());
    }

    #[test]
    fn test_unordered_missing_row() {
        let actual = vec![vec!["1".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("Missing rows"));
            assert!(reason.contains("2"));
        }
    }

    #[test]
    fn test_unordered_extra_row() {
        let actual = vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        let expected = vec!["1".to_string(), "2".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("Extra rows"));
            assert!(reason.contains("3"));
        }
    }

    #[test]
    fn test_unordered_wrong_value() {
        let actual = vec![vec!["1".to_string()], vec!["3".to_string()]];
        let expected = vec!["1".to_string(), "2".to_string()];

        let result = compare(&actual, &expected);
        assert!(!result.is_match());

        if let ComparisonResult::Mismatch { reason } = result {
            assert!(reason.contains("Missing rows"));
            assert!(reason.contains("2"));
            assert!(reason.contains("Extra rows"));
            assert!(reason.contains("3"));
        }
    }
}
