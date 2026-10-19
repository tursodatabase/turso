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

/// A built-in SQL collation, for a column whose value a query chose among
/// several values that compare equal under it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChosenValueCollation {
    Binary,
    NoCase,
    RTrim,
}

impl ChosenValueCollation {
    /// Parse a built-in collation name, ignoring case.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BINARY" => Some(Self::Binary),
            "NOCASE" => Some(Self::NoCase),
            "RTRIM" => Some(Self::RTrim),
            _ => None,
        }
    }

    /// One fixed member of `value`'s class: values equal under this
    /// collation map to the same member. As in SQLite comparisons, an integer
    /// and a real are equal when they are the same number, and the collation
    /// only applies to text: NOCASE folds ASCII letters, RTRIM ignores
    /// trailing spaces.
    fn class_member(self, value: &Value) -> Value {
        match value {
            Value::Real(real) => match real_as_exact_integer(*real) {
                Some(integer) => Value::Integer(integer),
                None => value.clone(),
            },
            Value::Text(text) => Value::Text(match self {
                Self::Binary => text.clone(),
                Self::NoCase => text.to_ascii_lowercase(),
                Self::RTrim => text.trim_end_matches(' ').to_string(),
            }),
            Value::Null | Value::Integer(_) | Value::Blob(_) => value.clone(),
        }
    }
}

/// The integer equal to `real`, when one exists.
fn real_as_exact_integer(real: f64) -> Option<i64> {
    // i64::MAX is not a float; 2^63 is the first float above the range.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    (real.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&real)).then_some(real as i64)
}

/// Compare two typed result sets as multisets.
pub fn diff_result_sets(left: &ResultSet, right: &ResultSet) -> ResultSetDiff {
    diff_result_sets_with_chosen_values(left, right, &[])
}

/// Compare two typed result sets as multisets, where in each column `i` with
/// `chosen[i] == Some(collation)` any two values equal under `collation`
/// match. Such a column shows one value chosen from several that compare
/// equal, and SQL leaves open which one (a GROUP BY key under NOCASE may show
/// 'a' or 'A'). Every other column, and every column past `chosen.len()`,
/// compares exactly.
pub fn diff_result_sets_with_chosen_values(
    left: &ResultSet,
    right: &ResultSet,
    chosen: &[Option<ChosenValueCollation>],
) -> ResultSetDiff {
    let class_row = |row: &Row| {
        Row(row
            .0
            .iter()
            .enumerate()
            .map(
                |(column, value)| match chosen.get(column).copied().flatten() {
                    Some(collation) => collation.class_member(value),
                    None => value.clone(),
                },
            )
            .collect())
    };
    // Rows are counted per class row. For diagnostics, each class also keeps
    // the first actual row each side had in it.
    let mut counts = BTreeMap::<Row, (i64, [Option<&Row>; 2])>::new();
    for (side, (rows, weight)) in [(left.rows(), 1), (right.rows(), -1)]
        .into_iter()
        .enumerate()
    {
        for row in rows {
            let (count, examples) = counts.entry(class_row(row)).or_default();
            *count += weight;
            examples[side].get_or_insert(row);
        }
    }

    let mut only_in_left = Vec::new();
    let mut only_in_right = Vec::new();
    let mut total_only_in_left = 0;
    let mut total_only_in_right = 0;
    for (count, [left_example, right_example]) in counts.into_values() {
        if count > 0 {
            let count = count as u64;
            total_only_in_left += count;
            only_in_left.push(CountedRow {
                row: left_example
                    .expect("a class with more left rows has a left row")
                    .clone(),
                count,
            });
        } else if count < 0 {
            let count = count.unsigned_abs();
            total_only_in_right += count;
            only_in_right.push(CountedRow {
                row: right_example
                    .expect("a class with more right rows has a right row")
                    .clone(),
                count,
            });
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
    fn chosen_value_columns_match_only_values_equal_under_their_collation() {
        let text = |value: &str| Value::Text(value.to_string());
        let row = |chosen: Value, exact: Value| vec![chosen, exact];
        let view = result(2, vec![row(text("a"), text("x"))]);
        let nocase = [Some(ChosenValueCollation::NoCase)];
        let rtrim = [Some(ChosenValueCollation::RTrim)];
        let binary = [Some(ChosenValueCollation::Binary)];

        let other_case = result(2, vec![row(text("A"), text("x"))]);
        assert!(diff_result_sets_with_chosen_values(&view, &other_case, &nocase).is_empty());
        assert!(!diff_result_sets_with_chosen_values(&view, &other_case, &rtrim).is_empty());
        assert!(!diff_result_sets(&view, &other_case).is_empty());

        let other_exact_column = result(2, vec![row(text("a"), text("X"))]);
        assert!(
            !diff_result_sets_with_chosen_values(&view, &other_exact_column, &nocase).is_empty()
        );

        let trailing_space = result(2, vec![row(text("a  "), text("x"))]);
        let leading_space = result(2, vec![row(text(" a"), text("x"))]);
        assert!(diff_result_sets_with_chosen_values(&view, &trailing_space, &rtrim).is_empty());
        assert!(!diff_result_sets_with_chosen_values(&view, &leading_space, &rtrim).is_empty());
        assert!(!diff_result_sets_with_chosen_values(&view, &trailing_space, &nocase).is_empty());

        let integer = result(2, vec![row(Value::Integer(2), text("x"))]);
        let real = result(2, vec![row(Value::Real(2.0), text("x"))]);
        let other_real = result(2, vec![row(Value::Real(2.5), text("x"))]);
        assert!(diff_result_sets_with_chosen_values(&integer, &real, &binary).is_empty());
        assert!(!diff_result_sets_with_chosen_values(&integer, &other_real, &binary).is_empty());
        assert!(!diff_result_sets(&integer, &real).is_empty());
    }

    #[test]
    fn chosen_value_columns_still_compare_multiplicity() {
        let text = |value: &str| vec![Value::Text(value.to_string())];
        let view = result(1, vec![text("a")]);
        let fresh = result(1, vec![text("A"), text("a")]);
        let diff = diff_result_sets_with_chosen_values(
            &view,
            &fresh,
            &[Some(ChosenValueCollation::NoCase)],
        );
        assert_eq!(
            diff.only_in_right,
            vec![CountedRow {
                row: Row(text("A")),
                count: 1
            }]
        );
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
