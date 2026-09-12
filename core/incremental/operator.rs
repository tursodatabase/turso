#![allow(dead_code)]
// Operator DAG for DBSP-style incremental computation
// Based on Feldera DBSP design but adapted for Turso's architecture

pub use crate::incremental::aggregate_operator::{
    AggregateEvalState, AggregateFunction, AggregateState,
};
pub use crate::incremental::filter_operator::{FilterOperator, FilterPredicate};
pub use crate::incremental::input_operator::InputOperator;
pub use crate::incremental::join_operator::{JoinEvalState, JoinOperator, JoinType};
pub use crate::incremental::project_operator::{ProjectColumn, ProjectOperator};

use crate::incremental::dbsp::{Delta, DeltaPair};
#[cfg(test)]
use crate::numeric::Numeric;
use crate::schema::{Index, IndexColumn};
use crate::storage::btree::BTreeCursor;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::types::IOResultOr;
use std::fmt::Debug;

/// Struct to hold both table and index cursors for DBSP state operations
pub struct DbspStateCursors {
    /// Cursor for the DBSP state table
    pub table_cursor: BTreeCursor,
    /// Cursor for the DBSP state table's primary key index
    pub index_cursor: BTreeCursor,
}

impl DbspStateCursors {
    /// Create a new DbspStateCursors with both table and index cursors
    pub fn new(table_cursor: BTreeCursor, index_cursor: BTreeCursor) -> Self {
        Self {
            table_cursor,
            index_cursor,
        }
    }
}

/// Create an index definition for the DBSP state table
/// This defines the primary key index on (operator_id, zset_id, element_id)
pub fn create_dbsp_state_index(root_page: i64) -> Index {
    Index {
        name: "dbsp_state_pk".to_string(),
        table_name: "dbsp_state".to_string(),
        root_page,
        columns: IndexColumn::new_many(vec!["operator_id", "zset_id", "element_id"]),
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    }
}

/// Generate a storage ID with column index and operation type encoding
/// Storage ID = (operator_id << 16) | (column_index << 2) | operation_type
/// Bit layout (64-bit integer):
/// - Bits 16-63 (48 bits): operator_id
/// - Bits 2-15 (14 bits): column_index (supports up to 16,384 columns)
/// - Bits 0-1 (2 bits): operation type (AGG_TYPE_REGULAR, AGG_TYPE_MINMAX, etc.)
pub fn generate_storage_id(operator_id: i64, column_index: usize, op_type: u8) -> i64 {
    assert!(op_type <= 3, "Invalid operation type");
    assert!(column_index < 16384, "Column index too large");

    ((operator_id) << 16) | ((column_index as i64) << 2) | (op_type as i64)
}

// Generic eval state that delegates to operator-specific states
#[derive(Debug)]
pub enum EvalState {
    Uninitialized,
    Init { deltas: DeltaPair },
    Aggregate(Box<AggregateEvalState>),
    Join(Box<JoinEvalState>),
    Done,
}

impl From<Delta> for EvalState {
    fn from(delta: Delta) -> Self {
        EvalState::Init {
            deltas: delta.into(),
        }
    }
}

impl From<DeltaPair> for EvalState {
    fn from(deltas: DeltaPair) -> Self {
        EvalState::Init { deltas }
    }
}

impl EvalState {
    pub fn from_delta(delta: Delta) -> Self {
        Self::Init {
            deltas: delta.into(),
        }
    }

    fn delta_ref(&self) -> &Delta {
        match self {
            EvalState::Init { deltas } => &deltas.left,
            _ => panic!("delta_ref() can only be called when in Init state",),
        }
    }
    pub fn extract_delta(&mut self) -> Delta {
        match self {
            EvalState::Init { deltas } => {
                let extracted = std::mem::take(&mut deltas.left);
                *self = EvalState::Uninitialized;
                extracted
            }
            _ => panic!("extract_delta() can only be called when in Init state"),
        }
    }
}

/// Tracks computation counts to verify incremental behavior (for tests now), and in the future
/// should be used to provide statistics.
#[derive(Debug, Default, Clone)]
pub struct ComputationTracker {
    pub filter_evaluations: usize,
    pub project_operations: usize,
    pub join_lookups: usize,
    pub aggregation_updates: usize,
    pub full_scans: usize,
}

impl ComputationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_filter(&mut self) {
        self.filter_evaluations += 1;
    }

    pub fn record_project(&mut self) {
        self.project_operations += 1;
    }

    pub fn record_join_lookup(&mut self) {
        self.join_lookups += 1;
    }

    pub fn record_aggregation(&mut self) {
        self.aggregation_updates += 1;
    }

    pub fn record_full_scan(&mut self) {
        self.full_scans += 1;
    }

    pub fn total_computations(&self) -> usize {
        self.filter_evaluations
            + self.project_operations
            + self.join_lookups
            + self.aggregation_updates
    }
}

/// Represents an operator in the dataflow graph
#[derive(Debug, Clone)]
pub enum QueryOperator {
    /// Table scan - source of data
    TableScan {
        table_name: String,
        column_names: Vec<String>,
    },

    /// Filter rows based on predicate
    Filter {
        predicate: FilterPredicate,
        input: usize, // Index of input operator
    },

    /// Project columns (select specific columns)
    Project {
        columns: Vec<ProjectColumn>,
        input: usize,
    },

    /// Join two inputs
    Join {
        join_type: JoinType,
        on_column: String,
        left_input: usize,
        right_input: usize,
    },

    /// Aggregate
    Aggregate {
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        input: usize,
    },
}

/// Operator DAG (Directed Acyclic Graph)
/// Base trait for incremental operators
// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
pub trait IncrementalOperator: Debug + Send {
    /// Evaluate the operator with a state, without modifying internal state
    /// This is used during query execution to compute results
    /// May need to read from storage to get current state (e.g., for aggregates)
    ///
    /// # Arguments
    /// * `state` - The evaluation state (may be in progress from a previous I/O operation)
    /// * `cursors` - Cursors for reading operator state from storage (table and optional index)
    ///
    /// # Returns
    /// The output delta from the evaluation
    fn eval(&mut self, state: &mut EvalState, cursors: &mut DbspStateCursors) -> IOResultOr<Delta>;

    /// Commit deltas to the operator's internal state and return the output
    /// This is called when a transaction commits, making changes permanent
    /// Returns the output delta (what downstream operators should see)
    /// The cursors parameter is for operators that need to persist state
    fn commit(&mut self, deltas: DeltaPair, cursors: &mut DbspStateCursors) -> IOResultOr<Delta>;

    /// Set computation tracker
    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>);
}

#[cfg(test)]
#[path = "../tests/unit/incremental/operator/tests.rs"]
mod tests;
