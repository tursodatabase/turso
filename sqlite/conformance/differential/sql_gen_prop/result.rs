//! Result set comparison utilities for testing.
//!
//! When comparing query results between SQLite and Turso, rows may be returned
//! in different orders (especially with LIMIT without ORDER BY). This module
//! provides utilities to compare result sets as multisets (bags), where the
//! count of each row matters but order does not.

use std::collections::HashMap;
use std::hash::Hash;

/// Compare two result sets as multisets (bags).
///
/// Returns true if both result sets contain the same rows with the same
/// multiplicities, regardless of order.
///
/// # Example
/// ```
/// use sql_gen_prop::result::results_equal;
///
/// let a = vec![vec!["1", "alice"], vec!["2", "bob"], vec!["1", "alice"]];
/// let b = vec![vec!["2", "bob"], vec!["1", "alice"], vec!["1", "alice"]];
/// assert!(results_equal(&a, &b));
///
/// let c = vec![vec!["1", "alice"], vec!["2", "bob"]];
/// assert!(!results_equal(&a, &c)); // Different multiplicities
/// ```
pub fn results_equal<T>(a: &[T], b: &[T]) -> bool
where
    T: Eq + Hash,
{
    if a.len() != b.len() {
        return false;
    }

    let mut counts: HashMap<&T, i64> = HashMap::new();

    for item in a {
        *counts.entry(item).or_insert(0) += 1;
    }

    for item in b {
        *counts.entry(item).or_insert(0) -= 1;
    }

    counts.values().all(|&count| count == 0)
}

/// Detailed result of comparing two result sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultDiff<T> {
    /// Rows present in the first set but not the second (or with higher multiplicity).
    pub only_in_first: Vec<T>,
    /// Rows present in the second set but not the first (or with higher multiplicity).
    pub only_in_second: Vec<T>,
}

impl<T> ResultDiff<T> {
    /// Returns true if there are no differences.
    pub fn is_empty(&self) -> bool {
        self.only_in_first.is_empty() && self.only_in_second.is_empty()
    }
}

/// Compare two result sets and return the differences.
///
/// Returns a `ResultDiff` containing rows that differ between the two sets,
/// accounting for multiplicities.
pub fn diff_results<T>(a: &[T], b: &[T]) -> ResultDiff<T>
where
    T: Eq + Hash + Clone,
{
    let mut counts: HashMap<&T, i64> = HashMap::new();

    for item in a {
        *counts.entry(item).or_insert(0) += 1;
    }

    for item in b {
        *counts.entry(item).or_insert(0) -= 1;
    }

    let mut only_in_first = Vec::new();
    let mut only_in_second = Vec::new();

    for (item, count) in counts {
        if count > 0 {
            for _ in 0..count {
                only_in_first.push(item.clone());
            }
        } else if count < 0 {
            for _ in 0..(-count) {
                only_in_second.push(item.clone());
            }
        }
    }

    ResultDiff {
        only_in_first,
        only_in_second,
    }
}

/// Assert that two result sets are equal as multisets.
///
/// Panics with a detailed diff if the sets are not equal.
#[track_caller]
pub fn assert_results_equal<T>(expected: &[T], actual: &[T])
where
    T: Eq + Hash + Clone + std::fmt::Debug,
{
    let diff = diff_results(expected, actual);
    if !diff.is_empty() {
        panic!(
            "Result sets differ:\n  Only in expected: {:?}\n  Only in actual: {:?}",
            diff.only_in_first, diff.only_in_second
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_results_equal_same_order() {
        let a = vec![vec!["1", "alice"], vec!["2", "bob"]];
        let b = vec![vec!["1", "alice"], vec!["2", "bob"]];
        assert!(results_equal(&a, &b));
    }

    #[test]
    fn test_results_equal_different_order() {
        let a = vec![vec!["1", "alice"], vec!["2", "bob"]];
        let b = vec![vec!["2", "bob"], vec!["1", "alice"]];
        assert!(results_equal(&a, &b));
    }

    #[test]
    fn test_results_equal_with_duplicates() {
        let a = vec![vec!["1", "alice"], vec!["2", "bob"], vec!["1", "alice"]];
        let b = vec![vec!["2", "bob"], vec!["1", "alice"], vec!["1", "alice"]];
        assert!(results_equal(&a, &b));
    }

    #[test]
    fn test_results_not_equal_different_multiplicities() {
        let a = vec![vec!["1", "alice"], vec!["1", "alice"]];
        let b = vec![vec!["1", "alice"]];
        assert!(!results_equal(&a, &b));
    }

    #[test]
    fn test_results_not_equal_different_rows() {
        let a = vec![vec!["1", "alice"]];
        let b = vec![vec!["2", "bob"]];
        assert!(!results_equal(&a, &b));
    }

    #[test]
    fn test_diff_results() {
        let a = vec!["a", "b", "b", "c"];
        let b = vec!["b", "c", "c", "d"];

        let diff = diff_results(&a, &b);
        assert_eq!(diff.only_in_first.len(), 2); // "a" and one "b"
        assert_eq!(diff.only_in_second.len(), 2); // one "c" and "d"
    }

    #[test]
    fn test_diff_results_equal() {
        let a = vec!["a", "b", "c"];
        let b = vec!["c", "a", "b"];

        let diff = diff_results(&a, &b);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_empty_results() {
        let a: Vec<i32> = vec![];
        let b: Vec<i32> = vec![];
        assert!(results_equal(&a, &b));
    }
}
