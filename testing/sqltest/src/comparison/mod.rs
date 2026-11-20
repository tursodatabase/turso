pub mod exact;
pub mod pattern;
pub mod unordered;

use crate::backends::QueryResult;
use crate::parser::ast::Expectation;
use regex::RegexBuilder;
use std::collections::HashSet;

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
        Expectation::Error { sqlstate, pattern } => {
            compare_error(actual, sqlstate.as_deref(), pattern.as_deref())
        }
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
fn compare_error(
    actual: &QueryResult,
    expected_sqlstate: Option<&str>,
    expected_pattern: Option<&str>,
) -> ComparisonResult {
    if let (Some(err), Some(expected)) = (&actual.error, expected_sqlstate) {
        match actual.sqlstate.as_deref() {
            Some(sqlstate) if sqlstate == expected => {}
            Some(sqlstate) => {
                return ComparisonResult::mismatch(format!(
                    "expected SQLSTATE {expected} but got {sqlstate}: {err}"
                ));
            }
            None => {
                return ComparisonResult::mismatch(format!(
                    "expected SQLSTATE {expected} but the error has no SQLSTATE: {err}"
                ));
            }
        }
    }
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

/// Whether one query result can match both expectations under the rules of
/// [`compare`]. It assumes that no output value holds a line break. Where
/// it cannot decide, it answers true: for an `expect pattern` block against
/// an `expect unordered` or another `expect pattern` block, and for two error
/// messages, because two regexes can match the same message.
pub fn may_match_same_output(a: &Expectation, b: &Expectation) -> bool {
    match (a, b) {
        (Expectation::Error { sqlstate: a, .. }, Expectation::Error { sqlstate: b, .. }) => {
            // compare_error checks a SQLSTATE only when the block names one.
            match (a, b) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            }
        }
        (Expectation::Error { .. }, _) | (_, Expectation::Error { .. }) => false,
        (Expectation::Exact(a), Expectation::Exact(b)) => exact_text(a) == exact_text(b),
        (Expectation::Unordered(a), Expectation::Unordered(b)) => {
            unordered_set(a) == unordered_set(b)
        }
        (Expectation::Exact(exact), Expectation::Unordered(unordered))
        | (Expectation::Unordered(unordered), Expectation::Exact(exact)) => {
            exact_matches_unordered(exact, unordered)
        }
        (Expectation::Pattern(pattern), Expectation::Exact(exact))
        | (Expectation::Exact(exact), Expectation::Pattern(pattern)) => {
            // exact::compare accepts only output whose text is exact_text, and
            // pattern::compare matches the regex against that same text.
            RegexBuilder::new(pattern)
                .build()
                .is_ok_and(|re| re.is_match(&exact_text(exact)))
        }
        (Expectation::Pattern(_), _) | (_, Expectation::Pattern(_)) => true,
    }
}

/// The only output text that exact::compare accepts for these lines.
fn exact_text(lines: &[String]) -> String {
    format_rows(&parse_expected_rows(lines))
}

/// The set of rows that unordered::compare accepts for these lines.
fn unordered_set(lines: &[String]) -> HashSet<String> {
    parse_expected_rows(lines)
        .iter()
        .map(|row| row.join("|"))
        .collect()
}

/// Whether some output has the text of the exact block and the row set of
/// the unordered block. Without line breaks in values, the rows of an output
/// are the lines of its text. The empty text is also the text of an output
/// with no rows.
fn exact_matches_unordered(exact: &[String], unordered: &[String]) -> bool {
    let text = exact_text(exact);
    let expected = unordered_set(unordered);
    if text.is_empty() && expected.is_empty() {
        return true;
    }
    let rows: HashSet<String> = text.split('\n').map(str::to_string).collect();
    rows == expected
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
        let expectation = Expectation::Error {
            sqlstate: None,
            pattern: None,
        };

        assert!(compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_error_pattern() {
        let actual = QueryResult::error("no such table: users");
        let expectation = Expectation::Error {
            sqlstate: None,
            pattern: Some("no such table".to_string()),
        };

        assert!(compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_error_pattern_mismatch() {
        let actual = QueryResult::error("syntax error");
        let expectation = Expectation::Error {
            sqlstate: None,
            pattern: Some("no such table".to_string()),
        };

        assert!(!compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_expected_error_got_success() {
        let actual = QueryResult::success(vec![vec!["1".to_string()]]);
        let expectation = Expectation::Error {
            sqlstate: None,
            pattern: None,
        };

        assert!(!compare(&actual, &expectation).is_match());
    }

    #[test]
    fn test_compare_expected_success_got_error() {
        let actual = QueryResult::error("some error");
        let expectation = Expectation::Exact(vec!["1".to_string()]);

        assert!(!compare(&actual, &expectation).is_match());
    }

    fn missing_table_error(sqlstate: Option<&str>) -> QueryResult {
        QueryResult::error_with_sqlstate(
            "relation \"t\" does not exist",
            sqlstate.map(str::to_string),
        )
    }

    fn sqlstate_expectation(sqlstate: &str, pattern: Option<&str>) -> Expectation {
        Expectation::Error {
            sqlstate: Some(sqlstate.to_string()),
            pattern: pattern.map(str::to_string),
        }
    }

    #[test]
    fn test_compare_error_same_sqlstate() {
        let actual = missing_table_error(Some("42P01"));

        assert!(compare(&actual, &sqlstate_expectation("42P01", None)).is_match());
        assert!(
            compare(
                &actual,
                &sqlstate_expectation("42P01", Some("does not exist"))
            )
            .is_match()
        );
    }

    #[test]
    fn test_compare_error_different_sqlstate() {
        let actual = missing_table_error(Some("42601"));

        assert_eq!(
            compare(&actual, &sqlstate_expectation("42P01", None)),
            ComparisonResult::mismatch(
                "expected SQLSTATE 42P01 but got 42601: relation \"t\" does not exist"
            )
        );
    }

    #[test]
    fn test_compare_error_without_sqlstate() {
        let actual = missing_table_error(None);

        assert_eq!(
            compare(&actual, &sqlstate_expectation("42P01", None)),
            ComparisonResult::mismatch(
                "expected SQLSTATE 42P01 but the error has no SQLSTATE: relation \"t\" does not exist"
            )
        );
    }

    #[test]
    fn test_compare_error_same_sqlstate_but_other_message() {
        let actual = missing_table_error(Some("42P01"));

        assert!(
            !compare(
                &actual,
                &sqlstate_expectation("42P01", Some("no such table"))
            )
            .is_match()
        );
    }

    #[test]
    fn test_compare_sqlstate_expected_but_query_succeeded() {
        let actual = QueryResult::success(vec![]);

        assert_eq!(
            compare(&actual, &sqlstate_expectation("42P01", None)),
            ComparisonResult::mismatch("expected error but query succeeded")
        );
    }

    fn lines(rows: &[&str]) -> Vec<String> {
        rows.iter().map(|r| r.to_string()).collect()
    }

    #[test]
    fn unordered_blocks_with_repeated_rows_match_the_same_output() {
        // unordered::compare compares row sets, so the output `1` matches both.
        let a = Expectation::Unordered(lines(&["1", "1"]));
        let b = Expectation::Unordered(lines(&["1"]));
        let output = QueryResult::success(vec![vec!["1".to_string()]]);
        assert!(compare(&output, &a).is_match() && compare(&output, &b).is_match());
        assert!(may_match_same_output(&a, &b));
    }

    #[test]
    fn a_null_row_makes_exact_blocks_differ() {
        // An empty line is a row with one NULL value.
        let a = Expectation::Exact(lines(&["1", ""]));
        let b = Expectation::Exact(lines(&["1"]));
        assert!(!may_match_same_output(&a, &b));
        let with_null = QueryResult::success(vec![vec!["1".to_string()], vec!["".to_string()]]);
        assert!(compare(&with_null, &a).is_match() && !compare(&with_null, &b).is_match());
    }

    #[test]
    fn exact_text_is_not_trimmed_inside_rows() {
        let a = Expectation::Exact(lines(&["a "]));
        let b = Expectation::Exact(lines(&["a"]));
        assert!(!may_match_same_output(&a, &b));
    }

    #[test]
    fn exact_and_unordered_blocks() {
        let exact = Expectation::Exact(lines(&["2", "1", "2"]));
        assert!(may_match_same_output(
            &exact,
            &Expectation::Unordered(lines(&["1", "2"]))
        ));
        assert!(!may_match_same_output(
            &exact,
            &Expectation::Unordered(lines(&["1"]))
        ));
        assert!(may_match_same_output(
            &Expectation::Exact(vec![]),
            &Expectation::Unordered(vec![])
        ));
    }

    #[test]
    fn patterns_and_errors() {
        let exact = Expectation::Exact(lines(&["12"]));
        assert!(may_match_same_output(
            &Expectation::Pattern("^1".to_string()),
            &exact
        ));
        assert!(!may_match_same_output(
            &Expectation::Pattern("^2".to_string()),
            &exact
        ));
        assert!(may_match_same_output(
            &Expectation::Pattern("x".to_string()),
            &Expectation::Unordered(lines(&["1"]))
        ));
        let error = |s: Option<&str>| Expectation::Error {
            sqlstate: s.map(str::to_string),
            pattern: None,
        };
        assert!(may_match_same_output(
            &error(Some("22P02")),
            &error(Some("22P02"))
        ));
        assert!(!may_match_same_output(
            &error(Some("22P02")),
            &error(Some("42883"))
        ));
        assert!(may_match_same_output(&error(None), &error(Some("42883"))));
        assert!(!may_match_same_output(&error(None), &exact));
    }
}
