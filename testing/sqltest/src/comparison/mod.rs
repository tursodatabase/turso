pub mod exact;
pub mod pattern;
pub mod unordered;

use crate::backends::QueryResult;
use crate::parser::ast::Expectation;

/// Result of comparing actual vs expected output
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonResult {
    /// Results match
    Match,
    /// Results don't match
    Mismatch { reason: String },
}

impl ComparisonResult {
    /// Check if comparison passed
    pub fn is_match(&self) -> bool {
        matches!(self, ComparisonResult::Match)
    }

    /// Create a mismatch result
    pub fn mismatch(reason: impl Into<String>) -> Self {
        ComparisonResult::Mismatch {
            reason: reason.into(),
        }
    }
}

/// Compare actual query result against expectation
pub fn compare(actual: &QueryResult, expectation: &Expectation) -> ComparisonResult {
    match expectation {
        Expectation::Exact(expected_rows) => {
            if actual.is_error() {
                return ComparisonResult::mismatch(format!(
                    "expected success but got error: {}",
                    actual.error.as_deref().unwrap_or("unknown")
                ));
            }
            exact::compare(&actual.rows, expected_rows)
        }
        Expectation::Pattern(pattern) => {
            if actual.is_error() {
                return ComparisonResult::mismatch(format!(
                    "expected success but got error: {}",
                    actual.error.as_deref().unwrap_or("unknown")
                ));
            }
            pattern::compare(&actual.rows, pattern)
        }
        Expectation::Unordered(expected_rows) => {
            if actual.is_error() {
                return ComparisonResult::mismatch(format!(
                    "expected success but got error: {}",
                    actual.error.as_deref().unwrap_or("unknown")
                ));
            }
            unordered::compare(&actual.rows, expected_rows)
        }
        Expectation::Error(expected_pattern) => compare_error(actual, expected_pattern.as_deref()),
    }
}

/// Normalize whitespace: collapse all whitespace sequences (spaces, tabs, newlines) into single spaces
fn normalize_whitespace(s: &str) -> String {
    s.split_whitespace()
        .filter(|s| *s != "│")
        .collect::<Vec<_>>()
        .join(" ")
}

/// Compare when expecting an error
fn compare_error(actual: &QueryResult, expected_pattern: Option<&str>) -> ComparisonResult {
    match (&actual.error, expected_pattern) {
        (None, _) => ComparisonResult::mismatch("expected error but query succeeded"),
        (Some(_err), None) => {
            // Any error is fine
            ComparisonResult::Match
        }
        (Some(err), Some(pattern)) => {
            // Normalize whitespace in both error and pattern for flexible matching
            let normalized_err = normalize_whitespace(err);
            let normalized_pattern = normalize_whitespace(pattern);

            pattern::compare_error(&normalized_err, &normalized_pattern)
        }
    }
}

/// Format rows for display (pipe-separated)
pub fn format_rows(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse expected rows from string lines
/// Empty lines are preserved as rows with a single empty cell (representing NULL)
pub fn parse_expected_rows(lines: &[String]) -> Vec<Vec<String>> {
    lines
        .iter()
        .map(|line| {
            if line.is_empty() {
                // Empty line represents a row with a single empty cell (NULL)
                vec!["".to_string()]
            } else {
                line.split('|').map(|s| s.to_string()).collect()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_exact_success() {
        let actual = QueryResult::success(vec![vec!["1".to_string(), "Alice".to_string()]]);
        let expectation = Expectation::Exact(vec!["1|Alice".to_string()]);

        assert!(compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_exact_failure() {
        let actual = QueryResult::success(vec![vec!["1".to_string(), "Bob".to_string()]]);
        let expectation = Expectation::Exact(vec!["1|Alice".to_string()]);

        assert!(!compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_error_any() {
        let actual = QueryResult::error("no such table: users");
        let expectation = Expectation::Error(None);

        assert!(compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_error_pattern() {
        let actual = QueryResult::error("no such table: users");
        let expectation = Expectation::Error(Some("no such table".to_string()));

        assert!(compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_error_pattern_mismatch() {
        let actual = QueryResult::error("syntax error");
        let expectation = Expectation::Error(Some("no such table".to_string()));

        assert!(!compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_expected_error_got_success() {
        let actual = QueryResult::success(vec![vec!["1".to_string()]]);
        let expectation = Expectation::Error(None);

        assert!(!compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_expected_success_got_error() {
        let actual = QueryResult::error("some error");
        let expectation = Expectation::Exact(vec!["1".to_string()]);

        assert!(!compare(&actual, &expectation).is_match());
    }
}
