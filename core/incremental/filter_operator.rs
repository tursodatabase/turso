#![allow(dead_code)]
// Filter operator for DBSP-style incremental computation
// This operator filters rows based on predicates

use crate::incremental::dbsp::{Delta, DeltaPair};
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::types::IOResult;
use crate::{Result, Value};
use std::sync::{Arc, Mutex};

/// Filter predicate for filtering rows
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Column = value (using column index)
    Equals { column_idx: usize, value: Value },
    /// Column != value (using column index)
    NotEquals { column_idx: usize, value: Value },
    /// Column > value (using column index)
    GreaterThan { column_idx: usize, value: Value },
    /// Column >= value (using column index)
    GreaterThanOrEqual { column_idx: usize, value: Value },
    /// Column < value (using column index)
    LessThan { column_idx: usize, value: Value },
    /// Column <= value (using column index)
    LessThanOrEqual { column_idx: usize, value: Value },

    /// Column = Column comparisons
    ColumnEquals { left_idx: usize, right_idx: usize },
    /// Column != Column comparisons
    ColumnNotEquals { left_idx: usize, right_idx: usize },
    /// Column > Column comparisons
    ColumnGreaterThan { left_idx: usize, right_idx: usize },
    /// Column >= Column comparisons
    ColumnGreaterThanOrEqual { left_idx: usize, right_idx: usize },
    /// Column < Column comparisons
    ColumnLessThan { left_idx: usize, right_idx: usize },
    /// Column <= Column comparisons
    ColumnLessThanOrEqual { left_idx: usize, right_idx: usize },

    /// Column IS NULL check
    IsNull { column_idx: usize },
    /// Column IS NOT NULL check
    IsNotNull { column_idx: usize },

    /// Logical AND of two predicates
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    /// Logical OR of two predicates
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    /// No predicate (accept all rows)
    None,
}

/// Filter operator - filters rows based on predicate
#[derive(Debug)]
pub struct FilterOperator {
    predicate: FilterPredicate,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
}

impl FilterOperator {
    pub fn new(predicate: FilterPredicate) -> Self {
        Self {
            predicate,
            tracker: None,
        }
    }

    /// Get the predicate for this filter
    pub fn predicate(&self) -> &FilterPredicate {
        &self.predicate
    }

    pub fn evaluate_predicate(&self, values: &[Value]) -> bool {
        match &self.predicate {
            FilterPredicate::None => true,
            FilterPredicate::Equals { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    return v == value;
                }
                false
            }
            FilterPredicate::NotEquals { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    return v != value;
                }
                false
            }
            FilterPredicate::GreaterThan { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    // Compare based on value types
                    match (v, value) {
                        (Value::Integer(a), Value::Integer(b)) => return a > b,
                        (Value::Float(a), Value::Float(b)) => return a > b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() > b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::GreaterThanOrEqual { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    match (v, value) {
                        (Value::Integer(a), Value::Integer(b)) => return a >= b,
                        (Value::Float(a), Value::Float(b)) => return a >= b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() >= b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::LessThan { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    match (v, value) {
                        (Value::Integer(a), Value::Integer(b)) => return a < b,
                        (Value::Float(a), Value::Float(b)) => return a < b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() < b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::LessThanOrEqual { column_idx, value } => {
                if let Some(v) = values.get(*column_idx) {
                    match (v, value) {
                        (Value::Integer(a), Value::Integer(b)) => return a <= b,
                        (Value::Float(a), Value::Float(b)) => return a <= b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() <= b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::And(left, right) => {
                // Temporarily create sub-filters to evaluate
                let left_filter = FilterOperator::new((**left).clone());
                let right_filter = FilterOperator::new((**right).clone());
                left_filter.evaluate_predicate(values) && right_filter.evaluate_predicate(values)
            }
            FilterPredicate::Or(left, right) => {
                let left_filter = FilterOperator::new((**left).clone());
                let right_filter = FilterOperator::new((**right).clone());
                left_filter.evaluate_predicate(values) || right_filter.evaluate_predicate(values)
            }

            // Column-to-column comparisons
            FilterPredicate::ColumnEquals {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    return left == right;
                }
                false
            }
            FilterPredicate::ColumnNotEquals {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    return left != right;
                }
                false
            }
            FilterPredicate::ColumnGreaterThan {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    match (left, right) {
                        (Value::Integer(a), Value::Integer(b)) => return a > b,
                        (Value::Float(a), Value::Float(b)) => return a > b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() > b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::ColumnGreaterThanOrEqual {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    match (left, right) {
                        (Value::Integer(a), Value::Integer(b)) => return a >= b,
                        (Value::Float(a), Value::Float(b)) => return a >= b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() >= b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::ColumnLessThan {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    match (left, right) {
                        (Value::Integer(a), Value::Integer(b)) => return a < b,
                        (Value::Float(a), Value::Float(b)) => return a < b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() < b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::ColumnLessThanOrEqual {
                left_idx,
                right_idx,
            } => {
                if let (Some(left), Some(right)) = (values.get(*left_idx), values.get(*right_idx)) {
                    match (left, right) {
                        (Value::Integer(a), Value::Integer(b)) => return a <= b,
                        (Value::Float(a), Value::Float(b)) => return a <= b,
                        (Value::Text(a), Value::Text(b)) => return a.as_str() <= b.as_str(),
                        _ => {}
                    }
                }
                false
            }
            FilterPredicate::IsNull { column_idx } => {
                if let Some(v) = values.get(*column_idx) {
                    return matches!(v, Value::Null);
                }
                false
            }
            FilterPredicate::IsNotNull { column_idx } => {
                if let Some(v) = values.get(*column_idx) {
                    return !matches!(v, Value::Null);
                }
                false
            }
        }
    }
}

impl IncrementalOperator for FilterOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { deltas } => {
                // Filter operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "FilterOperator expects right_delta to be empty"
                );
                std::mem::take(&mut deltas.left)
            }
            _ => unreachable!(
                "FilterOperator doesn't execute the state machine. Should be in Init state"
            ),
        };

        let mut output_delta = Delta::new();

        // Process the delta through the filter
        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            // Only pass through rows that satisfy the filter predicate
            // For deletes (weight < 0), we only pass them if the row values
            // would have passed the filter (meaning it was in the view)
            if self.evaluate_predicate(&row.values) {
                output_delta.changes.push((row, weight));
            }
        }

        *state = EvalState::Done;
        Ok(IOResult::Done(output_delta))
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Filter operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "FilterOperator expects right delta to be empty in commit"
        );

        let mut output_delta = Delta::new();

        // Commit the delta to our internal state
        // Only pass through and track rows that satisfy the filter predicate
        for (row, weight) in deltas.left.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            // Only track and output rows that pass the filter
            // For deletes, this means the row was in the view (its values pass the filter)
            // For inserts, this means the row should be in the view
            if self.evaluate_predicate(&row.values) {
                output_delta.changes.push((row, weight));
            }
        }

        Ok(IOResult::Done(output_delta))
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Text;

    #[test]
    fn test_is_null_predicate() {
        let predicate = FilterPredicate::IsNull { column_idx: 1 };
        let filter = FilterOperator::new(predicate);

        // Test with NULL value
        let values_with_null = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_with_null));

        // Test with non-NULL value
        let values_without_null = vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_without_null));

        // Test with different non-NULL types
        let values_with_text = vec![
            Value::Integer(1),
            Value::Text(Text::from("not null")),
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_with_text));

        let values_with_blob = vec![
            Value::Integer(1),
            Value::Blob(vec![1, 2, 3]),
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_with_blob));
    }

    #[test]
    fn test_is_not_null_predicate() {
        let predicate = FilterPredicate::IsNotNull { column_idx: 1 };
        let filter = FilterOperator::new(predicate);

        // Test with NULL value
        let values_with_null = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_with_null));

        // Test with non-NULL value (Integer)
        let values_with_integer = vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_with_integer));

        // Test with non-NULL value (Text)
        let values_with_text = vec![
            Value::Integer(1),
            Value::Text(Text::from("not null")),
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_with_text));

        // Test with non-NULL value (Blob)
        let values_with_blob = vec![
            Value::Integer(1),
            Value::Blob(vec![1, 2, 3]),
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_with_blob));
    }

    #[test]
    fn test_is_null_with_and() {
        // Test: column_0 = 1 AND column_1 IS NULL
        let predicate = FilterPredicate::And(
            Box::new(FilterPredicate::Equals {
                column_idx: 0,
                value: Value::Integer(1),
            }),
            Box::new(FilterPredicate::IsNull { column_idx: 1 }),
        );
        let filter = FilterOperator::new(predicate);

        // Should match: column_0 = 1 AND column_1 IS NULL
        let values_match = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_match));

        // Should not match: column_0 = 2 AND column_1 IS NULL
        let values_wrong_first = vec![
            Value::Integer(2),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_wrong_first));

        // Should not match: column_0 = 1 AND column_1 IS NOT NULL
        let values_not_null = vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_not_null));
    }

    #[test]
    fn test_is_not_null_with_or() {
        // Test: column_0 = 1 OR column_1 IS NOT NULL
        let predicate = FilterPredicate::Or(
            Box::new(FilterPredicate::Equals {
                column_idx: 0,
                value: Value::Integer(1),
            }),
            Box::new(FilterPredicate::IsNotNull { column_idx: 1 }),
        );
        let filter = FilterOperator::new(predicate);

        // Should match: column_0 = 1 (regardless of column_1)
        let values_first_matches = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_first_matches));

        // Should match: column_1 IS NOT NULL (regardless of column_0)
        let values_second_matches = vec![
            Value::Integer(2),
            Value::Integer(42),
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values_second_matches));

        // Should not match: column_0 != 1 AND column_1 IS NULL
        let values_no_match = vec![
            Value::Integer(2),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values_no_match));
    }

    #[test]
    fn test_complex_null_predicates() {
        // Test: (column_0 IS NULL OR column_1 IS NOT NULL) AND column_2 = 'test'
        let predicate = FilterPredicate::And(
            Box::new(FilterPredicate::Or(
                Box::new(FilterPredicate::IsNull { column_idx: 0 }),
                Box::new(FilterPredicate::IsNotNull { column_idx: 1 }),
            )),
            Box::new(FilterPredicate::Equals {
                column_idx: 2,
                value: Value::Text(Text::from("test")),
            }),
        );
        let filter = FilterOperator::new(predicate);

        // Should match: column_0 IS NULL, column_2 = 'test'
        let values1 = vec![Value::Null, Value::Null, Value::Text(Text::from("test"))];
        assert!(filter.evaluate_predicate(&values1));

        // Should match: column_1 IS NOT NULL, column_2 = 'test'
        let values2 = vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Text(Text::from("test")),
        ];
        assert!(filter.evaluate_predicate(&values2));

        // Should not match: column_2 != 'test'
        let values3 = vec![
            Value::Null,
            Value::Integer(42),
            Value::Text(Text::from("other")),
        ];
        assert!(!filter.evaluate_predicate(&values3));

        // Should not match: column_0 IS NOT NULL AND column_1 IS NULL AND column_2 = 'test'
        let values4 = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text(Text::from("test")),
        ];
        assert!(!filter.evaluate_predicate(&values4));
    }
}
