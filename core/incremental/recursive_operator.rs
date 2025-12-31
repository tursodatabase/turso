//! Recursive operator for DBSP fixed-point computation
//!
//! This operator wraps a recursive sub-circuit and iterates until a fixed-point is reached.
//! It implements the semantics described in the DBSP paper for incremental evaluation of
//! recursive queries.
//!
//! The fixed-point algorithm:
//! 1. Execute the base case to get initial values
//! 2. Initialize the delay operator with base case results
//! 3. Iterate:
//!    a. Execute the recursive step (reads from delay operator)
//!    b. If the result is empty, we've reached a fixed-point
//!    c. Otherwise, accumulate results and update delay operator
//!    d. Repeat until fixed-point or max iterations

use crate::incremental::dbsp::{Delta, DeltaPair, Hash128, HashableRow};
use crate::incremental::operator::{ComputationTracker, DbspStateCursors, EvalState};
use crate::types::IOResult;
use crate::Result;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

use super::operator::IncrementalOperator;

/// State machine for recursive fixed-point iteration
#[derive(Debug, Clone, PartialEq)]
pub enum RecursiveState {
    /// Starting a new fixed-point computation
    Init,
    /// Base case has been executed, ready for recursive iterations
    BaseComplete,
    /// Running recursive step iteration
    Iterating { iteration: usize },
    /// Fixed-point reached or max iterations hit
    Done,
}

/// Configuration for the recursive operator
#[derive(Debug, Clone)]
pub struct RecursiveConfig {
    /// Maximum number of iterations (prevents infinite loops)
    pub max_iterations: usize,
    /// Whether using UNION ALL (no deduplication) or UNION (distinct)
    pub union_all: bool,
}

impl Default for RecursiveConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
            union_all: false, // UNION (distinct) is safer default for recursion
        }
    }
}

/// Recursive operator that computes the fixed-point of a recursive query
///
/// This operator coordinates the fixed-point iteration:
/// - Executes the base case once
/// - Iterates the recursive step until no new values are produced
/// - Accumulates all results into the final output
pub struct RecursiveOperator {
    operator_id: i64,
    /// Configuration for the recursion
    config: RecursiveConfig,
    /// Current state of the recursion
    state: RecursiveState,
    /// Accumulated output from all iterations
    accumulated_output: Delta,
    /// Value hash -> canonical rowid for UNION distinct recursion
    seen_rows: HashMap<u64, i64>,
    /// Value hash -> net multiplicity for UNION distinct recursion
    seen_counts: HashMap<u64, isize>,
    /// Next rowid to assign for canonicalized rows
    next_rowid: i64,
    // The delay operator for feedback is managed by the circuit
}

/// Result of processing a recursive step
#[derive(Debug)]
pub struct RecursiveStepResult {
    pub done: bool,
    pub delta_for_delay: Delta,
}

impl RecursiveOperator {
    pub fn new(operator_id: i64, max_iterations: usize, union_all: bool) -> Self {
        Self {
            operator_id,
            config: RecursiveConfig {
                max_iterations,
                union_all,
            },
            state: RecursiveState::Init,
            accumulated_output: Delta::new(),
            seen_rows: HashMap::new(),
            seen_counts: HashMap::new(),
            next_rowid: 1,
        }
    }

    /// Get the current state
    pub fn state(&self) -> &RecursiveState {
        &self.state
    }

    /// Initialize with base case result
    pub fn initialize_with_base(&mut self, base_delta: Delta) -> Delta {
        self.state = RecursiveState::Init;
        self.accumulated_output = Delta::new();

        let normalized = self.normalize_delta(base_delta);
        let mut filtered = self.filter_new_rows(normalized);
        if !self.config.union_all {
            filtered.consolidate();
        }

        // Accumulate base case results
        self.accumulated_output = filtered.clone();

        self.state = RecursiveState::BaseComplete;
        filtered
    }

    /// Process the result of a recursive step iteration
    /// Returns true if fixed-point was reached, false if more iterations needed
    pub fn process_iteration_result(&mut self, step_delta: Delta) -> RecursiveStepResult {
        let iteration = match &self.state {
            RecursiveState::Iterating { iteration } => *iteration,
            _ => 1,
        };

        // Check max iterations first for fail-fast behavior
        if iteration >= self.config.max_iterations {
            tracing::warn!(
                "Recursive CTE hit max iterations limit ({}). Result may be incomplete.",
                self.config.max_iterations
            );
            self.state = RecursiveState::Done;
            return RecursiveStepResult {
                done: true,
                delta_for_delay: Delta::new(),
            };
        }

        let normalized = self.normalize_delta(step_delta);
        let mut filtered = self.filter_new_rows(normalized);
        if !self.config.union_all {
            filtered.consolidate();
        }

        // Check for fixed-point (empty delta after consolidation)
        if filtered.is_empty() {
            // Fixed-point reached!
            self.state = RecursiveState::Done;
            return RecursiveStepResult {
                done: true,
                delta_for_delay: Delta::new(),
            };
        }

        // Accumulate new results
        self.accumulated_output.merge(&filtered);
        if !self.config.union_all {
            self.accumulated_output.consolidate();
        }

        // Move to next iteration
        self.state = RecursiveState::Iterating {
            iteration: iteration + 1,
        };

        RecursiveStepResult {
            done: false,
            delta_for_delay: filtered,
        }
    }

    /// Start iteration phase (after base case is complete)
    pub fn start_iteration(&mut self) {
        assert!(matches!(self.state, RecursiveState::BaseComplete));
        self.state = RecursiveState::Iterating { iteration: 1 };
    }

    /// Finalize the result
    /// Consolidates if not UNION ALL
    pub fn finalize(&mut self) -> Delta {
        let mut result = std::mem::take(&mut self.accumulated_output);
        if !self.config.union_all {
            result.consolidate();
        }
        self.state = RecursiveState::Done;
        result
    }

    fn normalize_delta(&mut self, delta: Delta) -> Delta {
        if self.config.union_all {
            return delta;
        }

        let mut output = Delta::new();
        for (row, weight) in delta.changes {
            // We use a 128-bit hash of the row values (truncated to u64) to assign
            // stable row IDs for deduplication in non-UNION-ALL mode. In theory, a hash
            // collision could cause two distinct rows to share the same row ID and thus be
            // incorrectly merged. We accept this trade-off because Hash128 provides a
            // very large hash space (~2^64 after truncation), making collisions extremely
            // unlikely in practice, while keeping the deduplication state compact.
            let value_hash = Hash128::hash_values(&row.values).as_u64();
            let assigned_rowid = if let Some(&rowid) = self.seen_rows.get(&value_hash) {
                rowid
            } else {
                let rowid = self.next_rowid;
                self.next_rowid += 1;
                self.seen_rows.insert(value_hash, rowid);
                rowid
            };

            let final_row = HashableRow::new(assigned_rowid, row.values);
            output.changes.push((final_row, weight));
        }

        output
    }

    fn filter_new_rows(&mut self, delta: Delta) -> Delta {
        if self.config.union_all {
            return delta;
        }

        let mut output = Delta::new();
        for (row, weight) in delta.changes {
            if weight == 0 {
                continue;
            }
            let value_hash = Hash128::hash_values(&row.values).as_u64();
            let previous = *self.seen_counts.get(&value_hash).unwrap_or(&0);
            let new_count = previous + weight;
            let was_present = previous > 0;
            let is_present = new_count > 0;

            if !was_present && is_present {
                output.changes.push((row, 1));
            } else if was_present && !is_present {
                output.changes.push((row, -1));
            }

            if new_count == 0 {
                self.seen_counts.remove(&value_hash);
            } else {
                self.seen_counts.insert(value_hash, new_count);
            }
        }

        output
    }
}

impl Debug for RecursiveOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecursiveOperator")
            .field("operator_id", &self.operator_id)
            .field("state", &self.state)
            .field("max_iterations", &self.config.max_iterations)
            .field("union_all", &self.config.union_all)
            .field("accumulated_rows", &self.accumulated_output.changes.len())
            .finish()
    }
}

impl Display for RecursiveOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RecursiveOperator({}, state={:?})",
            self.operator_id, self.state
        )
    }
}

impl IncrementalOperator for RecursiveOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // The RecursiveOperator's eval returns the accumulated output
        // The actual fixed-point iteration is handled by execute_recursive_node()
        match state {
            EvalState::Init { .. } => {
                *state = EvalState::Done;
                Ok(IOResult::Done(self.accumulated_output.clone()))
            }
            EvalState::Done => Ok(IOResult::Done(Delta::new())),
            _ => unreachable!("RecursiveOperator only handles Init state"),
        }
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // During commit, we receive the final accumulated output
        // This is called after fixed-point iteration completes
        self.accumulated_output.merge(&deltas.left);
        if !self.config.union_all {
            self.accumulated_output.consolidate();
        }
        Ok(IOResult::Done(self.accumulated_output.clone()))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {
        // RecursiveOperator doesn't need computation tracking
        // (the sub-operators handle their own tracking)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental::dbsp::HashableRow;
    use crate::Value;

    #[test]
    fn test_recursive_operator_basic() {
        let mut op = RecursiveOperator::new(1, 100, false);

        assert!(matches!(op.state(), RecursiveState::Init));

        // Initialize with base case
        let mut base = Delta::new();
        base.changes
            .push((HashableRow::new(1, vec![Value::Integer(1)]), 1));
        base.changes
            .push((HashableRow::new(2, vec![Value::Integer(2)]), 1));
        let base_delta = op.initialize_with_base(base);

        assert!(matches!(op.state(), RecursiveState::BaseComplete));
        assert_eq!(op.accumulated_output.changes.len(), 2);
        assert_eq!(base_delta.changes.len(), 2);
    }

    #[test]
    fn test_recursive_operator_iteration() {
        let mut op = RecursiveOperator::new(1, 100, false);

        // Initialize with base case
        let mut base = Delta::new();
        base.changes
            .push((HashableRow::new(1, vec![Value::Integer(1)]), 1));
        op.initialize_with_base(base);

        // Start iteration
        op.start_iteration();
        assert!(matches!(
            op.state(),
            RecursiveState::Iterating { iteration: 1 }
        ));

        // Process first iteration result (not empty)
        let mut step1 = Delta::new();
        step1
            .changes
            .push((HashableRow::new(2, vec![Value::Integer(2)]), 1));
        let result = op.process_iteration_result(step1);
        assert!(!result.done);
        assert!(matches!(
            op.state(),
            RecursiveState::Iterating { iteration: 2 }
        ));
        assert_eq!(op.accumulated_output.changes.len(), 2);

        // Process second iteration result (empty = fixed-point)
        let step2 = Delta::new();
        let result = op.process_iteration_result(step2);
        assert!(result.done);
        assert!(matches!(op.state(), RecursiveState::Done));
    }

    #[test]
    fn test_recursive_operator_max_iterations() {
        let mut op = RecursiveOperator::new(1, 3, false);

        // Initialize with base case
        let mut base = Delta::new();
        base.changes
            .push((HashableRow::new(1, vec![Value::Integer(1)]), 1));
        op.initialize_with_base(base);

        op.start_iteration();

        // Simulate iterations that never converge
        for i in 1..=3 {
            let mut step = Delta::new();
            step.changes.push((
                HashableRow::new(i as i64 + 1, vec![Value::Integer(i as i64 + 1)]),
                1,
            ));
            let result = op.process_iteration_result(step);

            if i < 3 {
                assert!(!result.done, "Should not be done at iteration {i}");
            } else {
                // Should hit max iterations
                assert!(result.done, "Should be done at max iteration");
                assert!(matches!(op.state(), RecursiveState::Done));
            }
        }
    }

    #[test]
    fn test_recursive_operator_consolidation() {
        let mut op = RecursiveOperator::new(1, 100, false);

        // Initialize with base case
        let mut base = Delta::new();
        base.changes
            .push((HashableRow::new(1, vec![Value::Integer(1)]), 1));
        op.initialize_with_base(base);

        op.start_iteration();

        // Add same row again (should consolidate)
        let mut step = Delta::new();
        step.changes
            .push((HashableRow::new(1, vec![Value::Integer(1)]), 1)); // Duplicate
        step.changes
            .push((HashableRow::new(2, vec![Value::Integer(2)]), 1));
        op.process_iteration_result(step);

        // Finalize with consolidation
        let result = op.finalize();
        assert!(result.changes.len() <= 3); // May have fewer after consolidation
    }
}
