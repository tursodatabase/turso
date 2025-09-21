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
