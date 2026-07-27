//! Typed query-result comparison for correctness tests.
//!
//! SQL text rendering is not a sufficient correctness oracle: it collapses
//! NULL and empty text, can collapse integer and real values, and loses the
//! result arity when a query returns no rows. This crate keeps those properties
//! explicit and compares result rows as a multiset.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// A SQL value as observed at a query-result boundary.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) => 1,
            Self::Real(_) => 2,
            Self::Text(_) => 3,
            Self::Blob(_) => 4,
        }
    }
}

fn canonical_real_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else if value.is_nan() {
        CANONICAL_NAN_BITS
    } else {
        value.to_bits()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Real(left), Self::Real(right)) => {
                canonical_real_bits(*left) == canonical_real_bits(*right)
            }
            (Self::Text(left), Self::Text(right)) => left == right,
            (Self::Blob(left), Self::Blob(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rank().hash(state);
        match self {
            Self::Null => {}
            Self::Integer(value) => value.hash(state),
            Self::Real(value) => canonical_real_bits(*value).hash(state),
            Self::Text(value) => value.hash(state),
            Self::Blob(value) => value.hash(state),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank()
            .cmp(&other.rank())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Integer(left), Self::Integer(right)) => left.cmp(right),
                (Self::Real(left), Self::Real(right)) => f64::from_bits(canonical_real_bits(*left))
                    .total_cmp(&f64::from_bits(canonical_real_bits(*right))),
                (Self::Text(left), Self::Text(right)) => left.cmp(right),
                (Self::Blob(left), Self::Blob(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

impl fmt::Display for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("null"),
            Self::Integer(value) => write!(formatter, "integer({value})"),
            Self::Real(value) => write!(formatter, "real({value:?})"),
            Self::Text(value) => write!(formatter, "text({value:?})"),
            Self::Blob(value) => {
                formatter.write_str("blob(x'")?;
                for byte in value {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("')")
            }
        }
    }
}

/// One typed query-result row.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Row(pub Vec<Value>);

impl fmt::Display for Row {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_char('[')?;
        for (index, value) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{value}")?;
        }
        formatter.write_char(']')
    }
}

/// A typed query result, including arity when it has no rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSet {
    column_count: usize,
    rows: Vec<Row>,
}

impl ResultSet {
    pub fn new(column_count: usize, rows: Vec<Row>) -> Result<Self, InvalidResultSet> {
        if let Some((row_index, row)) = rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.0.len() != column_count)
        {
            return Err(InvalidResultSet {
                column_count,
                row_index,
                row_column_count: row.0.len(),
            });
        }
        Ok(Self { column_count, rows })
    }

    pub fn column_count(&self) -> usize {
        self.column_count
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// An inconsistent result supplied by a test backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidResultSet {
    pub column_count: usize,
    pub row_index: usize,
    pub row_column_count: usize,
}

impl fmt::Display for InvalidResultSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "result declares {} columns but row {} has {}",
            self.column_count, self.row_index, self.row_column_count
        )
    }
}

impl std::error::Error for InvalidResultSet {}

/// One distinct row and the amount by which its multiplicity differs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountedRow {
    pub row: Row,
    pub count: u64,
}

/// Deterministic, multiplicity-aware difference between two result sets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSetDiff {
    pub left_column_count: usize,
    pub right_column_count: usize,
    pub only_in_left: Vec<CountedRow>,
    pub only_in_right: Vec<CountedRow>,
    pub total_only_in_left: u64,
    pub total_only_in_right: u64,
}

impl ResultSetDiff {
    pub fn is_empty(&self) -> bool {
        self.left_column_count == self.right_column_count
            && self.only_in_left.is_empty()
            && self.only_in_right.is_empty()
    }

    /// Format a stable, bounded diagnostic for a failed comparison.
    pub fn describe(&self, left_label: &str, right_label: &str, row_limit: usize) -> String {
        let mut output = String::new();
        if self.left_column_count != self.right_column_count {
            let _ = writeln!(
                output,
                "column count differs: {left_label} has {}, {right_label} has {}",
                self.left_column_count, self.right_column_count
            );
        }
        describe_side(
            &mut output,
            left_label,
            &self.only_in_left,
            self.total_only_in_left,
            row_limit,
        );
        describe_side(
            &mut output,
            right_label,
            &self.only_in_right,
            self.total_only_in_right,
            row_limit,
        );
        output.trim_end().to_string()
    }
}

fn describe_side(
    output: &mut String,
    label: &str,
    rows: &[CountedRow],
    total: u64,
    row_limit: usize,
) {
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(
        output,
        "only in {label}: {total} row occurrence(s) across {} distinct row(s)",
        rows.len()
    );
    for counted in rows.iter().take(row_limit) {
        let _ = writeln!(output, "  {} × {}", counted.count, counted.row);
    }
    if rows.len() > row_limit {
        let _ = writeln!(
            output,
            "  ... {} additional distinct row(s) omitted",
            rows.len() - row_limit
        );
    }
}

/// Compare two typed result sets as multisets.
pub fn diff_result_sets(left: &ResultSet, right: &ResultSet) -> ResultSetDiff {
    let mut counts = BTreeMap::<Row, i64>::new();
    for row in left.rows() {
        *counts.entry(row.clone()).or_default() += 1;
    }
    for row in right.rows() {
        *counts.entry(row.clone()).or_default() -= 1;
    }

    let mut only_in_left = Vec::new();
    let mut only_in_right = Vec::new();
    let mut total_only_in_left = 0;
    let mut total_only_in_right = 0;
    for (row, count) in counts {
        if count > 0 {
            let count = count as u64;
            total_only_in_left += count;
            only_in_left.push(CountedRow { row, count });
        } else if count < 0 {
            let count = count.unsigned_abs();
            total_only_in_right += count;
            only_in_right.push(CountedRow { row, count });
        }
    }

    ResultSetDiff {
        left_column_count: left.column_count(),
        right_column_count: right.column_count(),
        only_in_left,
        only_in_right,
        total_only_in_left,
        total_only_in_right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn result(column_count: usize, rows: Vec<Vec<Value>>) -> ResultSet {
        ResultSet::new(column_count, rows.into_iter().map(Row).collect()).unwrap()
    }

    fn hash(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn comparison_is_order_independent_and_multiplicity_sensitive() {
        let one = Row(vec![Value::Integer(1)]);
        let two = Row(vec![Value::Integer(2)]);
        let left = ResultSet::new(1, vec![one.clone(), two.clone(), one.clone()]).unwrap();
        let reordered = ResultSet::new(1, vec![two.clone(), one.clone(), one.clone()]).unwrap();
        assert!(diff_result_sets(&left, &reordered).is_empty());

        let missing_duplicate = ResultSet::new(1, vec![two, one.clone()]).unwrap();
        let diff = diff_result_sets(&left, &missing_duplicate);
        assert_eq!(diff.only_in_left, vec![CountedRow { row: one, count: 1 }]);
        assert_eq!(diff.total_only_in_left, 1);
    }

    #[test]
    fn comparison_is_type_strict() {
        let null = result(1, vec![vec![Value::Null]]);
        let empty_text = result(1, vec![vec![Value::Text(String::new())]]);
        assert!(!diff_result_sets(&null, &empty_text).is_empty());

        let integer = result(1, vec![vec![Value::Integer(1)]]);
        let real = result(1, vec![vec![Value::Real(1.0)]]);
        assert!(!diff_result_sets(&integer, &real).is_empty());
    }

    #[test]
    fn empty_results_still_compare_arity() {
        let zero = result(0, vec![]);
        let one = result(1, vec![]);
        let diff = diff_result_sets(&zero, &one);
        assert!(!diff.is_empty());
        assert_eq!(diff.left_column_count, 0);
        assert_eq!(diff.right_column_count, 1);
    }

    #[test]
    fn real_equality_and_hashing_have_the_same_canonicalization() {
        let positive_zero = Value::Real(0.0);
        let negative_zero = Value::Real(-0.0);
        assert_eq!(positive_zero, negative_zero);
        assert_eq!(hash(&positive_zero), hash(&negative_zero));

        let nan_a = Value::Real(f64::NAN);
        let nan_b = Value::Real(f64::from_bits(0x7ff0_0000_0000_0001));
        assert_eq!(nan_a, nan_b);
        assert_eq!(hash(&nan_a), hash(&nan_b));

        assert_ne!(Value::Real(1.0), Value::Real(1.0 + 5e-11));
    }

    #[test]
    fn invalid_row_arity_is_rejected() {
        assert_eq!(
            ResultSet::new(2, vec![Row(vec![Value::Integer(1)])]).unwrap_err(),
            InvalidResultSet {
                column_count: 2,
                row_index: 0,
                row_column_count: 1,
            }
        );
    }

    #[test]
    fn diagnostics_are_deterministic_counted_and_bounded() {
        let left = result(
            1,
            vec![
                vec![Value::Integer(3)],
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(1)],
            ],
        );
        let right = result(1, vec![]);
        let description = diff_result_sets(&left, &right).describe("left", "right", 2);
        assert_eq!(
            description,
            "only in left: 4 row occurrence(s) across 3 distinct row(s)\n  \
             2 × [integer(1)]\n  1 × [integer(2)]\n  \
             ... 1 additional distinct row(s) omitted"
        );
    }
}
