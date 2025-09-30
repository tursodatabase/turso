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
use crate::schema::{Index, IndexColumn};
use crate::storage::btree::BTreeCursor;
use crate::types::IOResult;
use crate::Result;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

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
        columns: vec![
            IndexColumn {
                name: "operator_id".to_string(),
                order: turso_parser::ast::SortOrder::Asc,
                collation: None,
                pos_in_table: 0,
                default: None,
            },
            IndexColumn {
                name: "zset_id".to_string(),
                order: turso_parser::ast::SortOrder::Asc,
                collation: None,
                pos_in_table: 1,
                default: None,
            },
            IndexColumn {
                name: "element_id".to_string(),
                order: turso_parser::ast::SortOrder::Asc,
                collation: None,
                pos_in_table: 2,
                default: None,
            },
        ],
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
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
pub trait IncrementalOperator: Debug {
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
    fn eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>>;

    /// Commit deltas to the operator's internal state and return the output
    /// This is called when a transaction commits, making changes permanent
    /// Returns the output delta (what downstream operators should see)
    /// The cursors parameter is for operators that need to persist state
    fn commit(
        &mut self,
        deltas: DeltaPair,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>>;

    /// Set computation tracker
    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental::aggregate_operator::{AggregateOperator, AGG_TYPE_REGULAR};
    use crate::incremental::dbsp::HashableRow;
    use crate::storage::pager::CreateBTreeFlags;
    use crate::types::Text;
    use crate::util::IOExt;
    use crate::Value;
    use crate::{Database, MemoryIO, IO};
    use std::sync::{Arc, Mutex};

    /// Create a test pager for operator tests with both table and index
    fn create_test_pager() -> (std::sync::Arc<crate::Pager>, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", false, false).unwrap();
        let conn = db.connect().unwrap();

        let pager = conn.pager.read().clone();

        // Allocate page 1 first (database header)
        let _ = pager.io.block(|| pager.allocate_page1());

        // Create a BTree for the table
        let table_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .expect("Failed to create BTree for aggregate state table")
            as i64;

        // Create a BTree for the index
        let index_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .expect("Failed to create BTree for aggregate state index")
            as i64;

        (pager, table_root_page_id, index_root_page_id)
    }

    /// Read the current state from the BTree (for testing)
    /// Returns a Delta with all the current aggregate values
    fn get_current_state_from_btree(
        agg: &AggregateOperator,
        pager: &std::sync::Arc<crate::Pager>,
        cursors: &mut DbspStateCursors,
    ) -> Delta {
        let mut result = Delta::new();

        // Rewind to start of table
        pager.io.block(|| cursors.table_cursor.rewind()).unwrap();

        loop {
            // Check if cursor is empty (no more rows)
            if cursors.table_cursor.is_empty() {
                break;
            }

            // Get the record at this position
            let record = pager
                .io
                .block(|| cursors.table_cursor.record())
                .unwrap()
                .unwrap()
                .to_owned();

            let values_ref = record.get_values();
            let values: Vec<Value> = values_ref.into_iter().map(|x| x.to_owned()).collect();

            // Parse the 5-column structure: operator_id, zset_id, element_id, value, weight
            if let Some(Value::Integer(op_id)) = values.first() {
                // For regular aggregates, use column_index=0 and AGG_TYPE_REGULAR
                let expected_op_id = generate_storage_id(agg.operator_id, 0, AGG_TYPE_REGULAR);

                // Skip if not our operator
                if *op_id != expected_op_id {
                    pager.io.block(|| cursors.table_cursor.next()).unwrap();
                    continue;
                }

                // Get the blob data from column 3 (value column)
                if let Some(Value::Blob(blob)) = values.get(3) {
                    // Deserialize the state
                    match AggregateState::from_blob(blob) {
                        Ok((state, group_key)) => {
                            // Should not have made it this far.
                            assert!(state.count != 0);
                            // Build output row: group_by columns + aggregate values
                            let mut output_values = group_key.clone();
                            output_values.extend(state.to_values(&agg.aggregates));

                            let group_key_str = AggregateOperator::group_key_to_string(&group_key);
                            let rowid = agg.generate_group_rowid(&group_key_str);
                            let output_row = HashableRow::new(rowid, output_values);
                            result.changes.push((output_row, 1));
                        }
                        Err(e) => {
                            // Log or handle the deserialization error
                            // For now, we'll skip this entry
                            eprintln!("Failed to deserialize aggregate state: {e}");
                        }
                    }
                }
            }

            pager.io.block(|| cursors.table_cursor.next()).unwrap();
        }

        result.consolidate();
        result
    }

    /// Assert that we're doing incremental work, not full recomputation
    fn assert_incremental(tracker: &ComputationTracker, expected_ops: usize, data_size: usize) {
        assert!(
            tracker.total_computations() <= expected_ops,
            "Expected <= {} operations for incremental update, got {}",
            expected_ops,
            tracker.total_computations()
        );
        assert!(
            tracker.total_computations() < data_size,
            "Computation count {} suggests full recomputation (data size: {})",
            tracker.total_computations(),
            data_size
        );
        assert_eq!(
            tracker.full_scans, 0,
            "Incremental computation should not perform full scans"
        );
    }

    // Aggregate tests
    #[test]
    fn test_aggregate_incremental_update_emits_retraction() {
        // This test verifies that when an aggregate value changes,
        // the operator emits both a retraction (-1) of the old value
        // and an insertion (+1) of the new value.

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Create an aggregate operator for SUM(age) with no GROUP BY
        let mut agg = AggregateOperator::new(
            1,                               // operator_id for testing
            vec![],                          // No GROUP BY
            vec![AggregateFunction::Sum(2)], // age is at index 2
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
        );

        // Initial data: 3 users
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".to_string().into()),
                Value::Integer(25),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".to_string().into()),
                Value::Integer(30),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".to_string().into()),
                Value::Integer(35),
            ],
        );

        // Initialize with initial data
        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify initial state: SUM(age) = 25 + 30 + 35 = 90
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 1, "Should have one aggregate row");
        let (row, weight) = &state.changes[0];
        assert_eq!(*weight, 1, "Aggregate row should have weight 1");
        assert_eq!(row.values[0], Value::Float(90.0), "SUM should be 90");

        // Now add a new user (incremental update)
        let mut update_delta = Delta::new();
        update_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".to_string().into()),
                Value::Integer(40),
            ],
        );

        // Process the incremental update
        let output_delta = pager
            .io
            .block(|| agg.commit((&update_delta).into(), &mut cursors))
            .unwrap();

        // CRITICAL: The output delta should contain TWO changes:
        // 1. Retraction of old aggregate value (90) with weight -1
        // 2. Insertion of new aggregate value (130) with weight +1
        assert_eq!(
            output_delta.changes.len(),
            2,
            "Expected 2 changes (retraction + insertion), got {}: {:?}",
            output_delta.changes.len(),
            output_delta.changes
        );

        // Verify the retraction comes first
        let (retraction_row, retraction_weight) = &output_delta.changes[0];
        assert_eq!(
            *retraction_weight, -1,
            "First change should be a retraction"
        );
        assert_eq!(
            retraction_row.values[0],
            Value::Float(90.0),
            "Retracted value should be the old sum (90)"
        );

        // Verify the insertion comes second
        let (insertion_row, insertion_weight) = &output_delta.changes[1];
        assert_eq!(*insertion_weight, 1, "Second change should be an insertion");
        assert_eq!(
            insertion_row.values[0],
            Value::Float(130.0),
            "Inserted value should be the new sum (130)"
        );

        // Both changes should have the same row ID (since it's the same aggregate group)
        assert_eq!(
            retraction_row.rowid, insertion_row.rowid,
            "Retraction and insertion should have the same row ID"
        );
    }

    #[test]
    fn test_aggregate_with_group_by_emits_retractions() {
        // This test verifies that when aggregate values change for grouped data,
        // the operator emits both retractions and insertions correctly for each group.

        // Create an aggregate operator for SUM(score) GROUP BY team
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,                               // operator_id for testing
            vec![1],                         // GROUP BY team (index 1)
            vec![AggregateFunction::Sum(3)], // score is at index 3
            vec![
                "id".to_string(),
                "team".to_string(),
                "player".to_string(),
                "score".to_string(),
            ],
        );

        // Initial data: players on different teams
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("red".to_string().into()),
                Value::Text("Alice".to_string().into()),
                Value::Integer(10),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("blue".to_string().into()),
                Value::Text("Bob".to_string().into()),
                Value::Integer(15),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("red".to_string().into()),
                Value::Text("Charlie".to_string().into()),
                Value::Integer(20),
            ],
        );

        // Initialize with initial data
        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify initial state: red team = 30, blue team = 15
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 2, "Should have two groups");

        // Find the red and blue team aggregates
        let mut red_sum = None;
        let mut blue_sum = None;
        for (row, weight) in &state.changes {
            assert_eq!(*weight, 1);
            if let Value::Text(team) = &row.values[0] {
                if team.as_str() == "red" {
                    red_sum = Some(&row.values[1]);
                } else if team.as_str() == "blue" {
                    blue_sum = Some(&row.values[1]);
                }
            }
        }
        assert_eq!(
            red_sum,
            Some(&Value::Float(30.0)),
            "Red team sum should be 30"
        );
        assert_eq!(
            blue_sum,
            Some(&Value::Float(15.0)),
            "Blue team sum should be 15"
        );

        // Now add a new player to the red team (incremental update)
        let mut update_delta = Delta::new();
        update_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("red".to_string().into()),
                Value::Text("David".to_string().into()),
                Value::Integer(25),
            ],
        );

        // Process the incremental update
        let output_delta = pager
            .io
            .block(|| agg.commit((&update_delta).into(), &mut cursors))
            .unwrap();

        // Should have 2 changes: retraction of old red team sum, insertion of new red team sum
        // Blue team should NOT be affected
        assert_eq!(
            output_delta.changes.len(),
            2,
            "Expected 2 changes for red team only, got {}: {:?}",
            output_delta.changes.len(),
            output_delta.changes
        );

        // Both changes should be for the red team
        let mut found_retraction = false;
        let mut found_insertion = false;

        for (row, weight) in &output_delta.changes {
            if let Value::Text(team) = &row.values[0] {
                assert_eq!(team.as_str(), "red", "Only red team should have changes");

                if *weight == -1 {
                    // Retraction of old value
                    assert_eq!(
                        row.values[1],
                        Value::Float(30.0),
                        "Should retract old sum of 30"
                    );
                    found_retraction = true;
                } else if *weight == 1 {
                    // Insertion of new value
                    assert_eq!(
                        row.values[1],
                        Value::Float(55.0),
                        "Should insert new sum of 55"
                    );
                    found_insertion = true;
                }
            }
        }

        assert!(found_retraction, "Should have found retraction");
        assert!(found_insertion, "Should have found insertion");
    }

    // Aggregation tests
    #[test]
    fn test_count_increments_not_recounts() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Create COUNT(*) GROUP BY category
        let mut agg = AggregateOperator::new(
            1,       // operator_id for testing
            vec![1], // category is at index 1
            vec![AggregateFunction::Count],
            vec![
                "item_id".to_string(),
                "category".to_string(),
                "price".to_string(),
            ],
        );
        agg.set_tracker(tracker.clone());

        // Initial: 100 items in 10 categories (10 items each)
        let mut initial = Delta::new();
        for i in 0..100 {
            let category = format!("cat_{}", i / 10);
            initial.insert(
                i,
                vec![
                    Value::Integer(i),
                    Value::Text(Text::new(&category)),
                    Value::Integer(i * 10),
                ],
            );
        }
        pager
            .io
            .block(|| agg.commit((&initial).into(), &mut cursors))
            .unwrap();

        // Reset tracker for delta processing
        tracker.lock().unwrap().aggregation_updates = 0;

        // Add one item to category 'cat_0'
        let mut delta = Delta::new();
        delta.insert(
            100,
            vec![
                Value::Integer(100),
                Value::Text(Text::new("cat_0")),
                Value::Integer(1000),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);

        // Check the final state - cat_0 should now have count 11
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let cat_0 = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("cat_0")))
            .unwrap();
        assert_eq!(cat_0.0.values[1], Value::Integer(11));

        // Verify incremental behavior - we process the delta twice (eval + commit)
        let t = tracker.lock().unwrap();
        assert_incremental(&t, 2, 101);
    }

    #[test]
    fn test_sum_updates_incrementally() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create SUM(amount) GROUP BY product
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,                               // operator_id for testing
            vec![1],                         // product is at index 1
            vec![AggregateFunction::Sum(2)], // amount is at index 2
            vec![
                "sale_id".to_string(),
                "product".to_string(),
                "amount".to_string(),
            ],
        );
        agg.set_tracker(tracker.clone());

        // Initial sales
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("Widget")),
                Value::Integer(100),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("Gadget")),
                Value::Integer(200),
            ],
        );
        initial.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text(Text::new("Widget")),
                Value::Integer(150),
            ],
        );
        pager
            .io
            .block(|| agg.commit((&initial).into(), &mut cursors))
            .unwrap();

        // Check initial state: Widget=250, Gadget=200
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let widget_sum = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("Widget")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(widget_sum.values[1], Value::Integer(250));

        // Reset tracker
        tracker.lock().unwrap().aggregation_updates = 0;

        // Add sale of 50 for Widget
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text(Text::new("Widget")),
                Value::Integer(50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);

        // Check final state - Widget should now be 300 (250 + 50)
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let widget = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("Widget")))
            .unwrap();
        assert_eq!(widget.0.values[1], Value::Integer(300));
    }

    #[test]
    fn test_count_and_sum_together() {
        // Test the example from DBSP_ROADMAP: COUNT(*) and SUM(amount) GROUP BY user_id
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,       // operator_id for testing
            vec![1], // user_id is at index 1
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum(2), // amount is at index 2
            ],
            vec![
                "order_id".to_string(),
                "user_id".to_string(),
                "amount".to_string(),
            ],
        );

        // Initial orders
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(100)],
        );
        initial.insert(
            2,
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(200)],
        );
        initial.insert(
            3,
            vec![Value::Integer(3), Value::Integer(2), Value::Integer(150)],
        );
        pager
            .io
            .block(|| agg.commit((&initial).into(), &mut cursors))
            .unwrap();

        // Check initial state
        // User 1: count=2, sum=300
        // User 2: count=1, sum=150
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 2);

        let user1 = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Integer(1))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(user1.values[1], Value::Integer(2)); // count
        assert_eq!(user1.values[2], Value::Integer(300)); // sum

        let user2 = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Integer(2))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(user2.values[1], Value::Integer(1)); // count
        assert_eq!(user2.values[2], Value::Integer(150)); // sum

        // Add order for user 1
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![Value::Integer(4), Value::Integer(1), Value::Integer(50)],
        );
        pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        // Check final state - user 1 should have updated count and sum
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let user1 = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Integer(1))
            .unwrap();
        assert_eq!(user1.0.values[1], Value::Integer(3)); // count: 2 + 1
        assert_eq!(user1.0.values[2], Value::Integer(350)); // sum: 300 + 50
    }

    #[test]
    fn test_avg_maintains_sum_and_count() {
        // Test AVG aggregation
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,                               // operator_id for testing
            vec![1],                         // category is at index 1
            vec![AggregateFunction::Avg(2)], // value is at index 2
            vec![
                "id".to_string(),
                "category".to_string(),
                "value".to_string(),
            ],
        );

        // Initial data
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(10),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("A")),
                Value::Integer(20),
            ],
        );
        initial.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text(Text::new("B")),
                Value::Integer(30),
            ],
        );
        pager
            .io
            .block(|| agg.commit((&initial).into(), &mut cursors))
            .unwrap();

        // Check initial averages
        // Category A: avg = (10 + 20) / 2 = 15
        // Category B: avg = 30 / 1 = 30
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let cat_a = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("A")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(cat_a.values[1], Value::Float(15.0));

        let cat_b = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("B")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(cat_b.values[1], Value::Float(30.0));

        // Add value to category A
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text(Text::new("A")),
                Value::Integer(30),
            ],
        );
        pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        // Check final state - Category A avg should now be (10 + 20 + 30) / 3 = 20
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let cat_a = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("A")))
            .unwrap();
        assert_eq!(cat_a.0.values[1], Value::Float(20.0));
    }

    #[test]
    fn test_delete_updates_aggregates() {
        // Test that deletes (negative weights) properly update aggregates
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,       // operator_id for testing
            vec![1], // category is at index 1
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum(2), // value is at index 2
            ],
            vec![
                "id".to_string(),
                "category".to_string(),
                "value".to_string(),
            ],
        );

        // Initial data
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(100),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("A")),
                Value::Integer(200),
            ],
        );
        pager
            .io
            .block(|| agg.commit((&initial).into(), &mut cursors))
            .unwrap();

        // Check initial state: count=2, sum=300
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert!(!state.changes.is_empty());
        let (row, _weight) = &state.changes[0];
        assert_eq!(row.values[1], Value::Integer(2)); // count
        assert_eq!(row.values[2], Value::Integer(300)); // sum

        // Delete one row
        let mut delta = Delta::new();
        delta.delete(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(100),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        // Check final state - should update to count=1, sum=200
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let cat_a = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("A")))
            .unwrap();
        assert_eq!(cat_a.0.values[1], Value::Integer(1)); // count: 2 - 1
        assert_eq!(cat_a.0.values[2], Value::Integer(200)); // sum: 300 - 100
    }

    #[test]
    fn test_count_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Count];
        let group_by = vec![0]; // category is at index 0
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial counts
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 2);

        // Find group A and B
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        let group_b = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("B".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2 for A
        assert_eq!(group_b.0.values[1], Value::Integer(1)); // COUNT = 1 for B

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(1, vec![Value::Text("A".into()), Value::Integer(10)]);

        let output = pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction for old count and insertion for new count
        assert_eq!(output.changes.len(), 2);

        // Check final state
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(group_a_final.0.values[1], Value::Integer(1)); // COUNT = 1 for A after deletion

        // Delete all rows from group B
        let mut delete_all_b = Delta::new();
        delete_all_b.delete(3, vec![Value::Text("B".into()), Value::Integer(30)]);

        let output_b = pager
            .io
            .block(|| agg.commit((&delete_all_b).into(), &mut cursors))
            .unwrap();
        assert_eq!(output_b.changes.len(), 1); // Only retraction, no new row
        assert_eq!(output_b.changes[0].1, -1); // Retraction

        // Final state should not have group B
        let final_state2 = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(final_state2.changes.len(), 1); // Only group A remains
        assert_eq!(final_state2.changes[0].0.values[0], Value::Text("A".into()));
    }

    #[test]
    fn test_sum_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Sum(1)]; // value is at index 1
        let group_by = vec![0]; // category is at index 0
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        init_data.insert(4, vec![Value::Text("B".into()), Value::Integer(15)]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial sums
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        let group_b = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("B".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(30)); // SUM = 30 for A (10+20)
        assert_eq!(group_b.0.values[1], Value::Integer(45)); // SUM = 45 for B (30+15)

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(2, vec![Value::Text("A".into()), Value::Integer(20)]);

        pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Check updated sum
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(group_a.0.values[1], Value::Integer(10)); // SUM = 10 for A after deletion

        // Delete all from group B
        let mut delete_all_b = Delta::new();
        delete_all_b.delete(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        delete_all_b.delete(4, vec![Value::Text("B".into()), Value::Integer(15)]);

        pager
            .io
            .block(|| agg.commit((&delete_all_b).into(), &mut cursors))
            .unwrap();

        // Group B should be gone
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(final_state.changes.len(), 1); // Only group A remains
        assert_eq!(final_state.changes[0].0.values[0], Value::Text("A".into()));
    }

    #[test]
    fn test_avg_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Avg(1)]; // value is at index 1
        let group_by = vec![0]; // category is at index 0
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("A".into()), Value::Integer(30)]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial average
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.values[1], Value::Float(20.0)); // AVG = (10+20+30)/3 = 20

        // Delete the middle value
        let mut delete_delta = Delta::new();
        delete_delta.delete(2, vec![Value::Text("A".into()), Value::Integer(20)]);

        pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Check updated average
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes[0].0.values[1], Value::Float(20.0)); // AVG = (10+30)/2 = 20 (same!)

        // Delete another to change the average
        let mut delete_another = Delta::new();
        delete_another.delete(3, vec![Value::Text("A".into()), Value::Integer(30)]);

        pager
            .io
            .block(|| agg.commit((&delete_another).into(), &mut cursors))
            .unwrap();

        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes[0].0.values[1], Value::Float(10.0)); // AVG = 10/1 = 10
    }

    #[test]
    fn test_multiple_aggregations_with_deletions() {
        // Test COUNT, SUM, and AVG together
        let aggregates = vec![
            AggregateFunction::Count,
            AggregateFunction::Sum(1), // value is at index 1
            AggregateFunction::Avg(1), // value is at index 1
        ];
        let group_by = vec![0]; // category is at index 0
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(100)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(200)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(50)]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial state
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2
        assert_eq!(group_a.0.values[2], Value::Integer(300)); // SUM = 300
        assert_eq!(group_a.0.values[3], Value::Float(150.0)); // AVG = 150

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(1, vec![Value::Text("A".into()), Value::Integer(100)]);

        pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Check all aggregates updated correctly
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(1)); // COUNT = 1
        assert_eq!(group_a.0.values[2], Value::Integer(200)); // SUM = 200
        assert_eq!(group_a.0.values[3], Value::Float(200.0)); // AVG = 200

        // Insert a new row with floating point value
        let mut insert_delta = Delta::new();
        insert_delta.insert(4, vec![Value::Text("A".into()), Value::Float(50.5)]);

        pager
            .io
            .block(|| agg.commit((&insert_delta).into(), &mut cursors))
            .unwrap();

        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2
        assert_eq!(group_a.0.values[2], Value::Float(250.5)); // SUM = 250.5
        assert_eq!(group_a.0.values[3], Value::Float(125.25)); // AVG = 125.25
    }

    #[test]
    fn test_filter_operator_rowid_update() {
        // When a row's rowid changes (e.g., UPDATE t SET a=1 WHERE a=3 on INTEGER PRIMARY KEY),
        // the operator should properly consolidate the state

        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut filter = FilterOperator::new(FilterPredicate::GreaterThan {
            column_idx: 1, // "b" is at index 1
            value: Value::Integer(2),
        });

        // Initialize with a row (rowid=3, values=[3, 3])
        let mut init_data = Delta::new();
        init_data.insert(3, vec![Value::Integer(3), Value::Integer(3)]);
        let state = pager
            .io
            .block(|| filter.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial state
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.rowid, 3);
        assert_eq!(
            state.changes[0].0.values,
            vec![Value::Integer(3), Value::Integer(3)]
        );

        // Simulate an UPDATE that changes rowid from 3 to 1
        // This is sent as: delete(3) + insert(1)
        let mut update_delta = Delta::new();
        update_delta.delete(3, vec![Value::Integer(3), Value::Integer(3)]);
        update_delta.insert(1, vec![Value::Integer(1), Value::Integer(3)]);

        let output = pager
            .io
            .block(|| filter.commit((&update_delta).into(), &mut cursors))
            .unwrap();

        // The output delta should have both changes (both pass the filter b > 2)
        assert_eq!(output.changes.len(), 2);
        assert_eq!(output.changes[0].1, -1); // delete weight
        assert_eq!(output.changes[1].1, 1); // insert weight
    }

    // ============================================================================
    // EVAL/COMMIT PATTERN TESTS
    // These tests verify that the eval/commit pattern works correctly:
    // - eval() computes results without modifying state
    // - eval() with uncommitted data returns correct results
    // - commit() updates internal state
    // - State remains unchanged when eval() is called with uncommitted data
    // ============================================================================

    #[test]
    fn test_filter_eval_with_uncommitted() {
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut filter = FilterOperator::new(FilterPredicate::GreaterThan {
            column_idx: 2, // "age" is at index 2
            value: Value::Integer(25),
        });

        // Initialize with some data
        let mut init_data = Delta::new();
        init_data.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(30),
            ],
        );
        init_data.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(20),
            ],
        );
        let state = pager
            .io
            .block(|| filter.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Verify initial state (only Alice passes filter)
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.rowid, 1);

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(35),
            ],
        );
        uncommitted.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(15),
            ],
        );

        // Eval with uncommitted - should return filtered uncommitted rows
        let mut eval_state = uncommitted.clone().into();
        let result = pager
            .io
            .block(|| filter.eval(&mut eval_state, &mut cursors))
            .unwrap();
        assert_eq!(
            result.changes.len(),
            1,
            "Only Charlie (35) should pass filter"
        );
        assert_eq!(result.changes[0].0.rowid, 3);

        // Now commit the changes
        let state = pager
            .io
            .block(|| filter.commit((&uncommitted).into(), &mut cursors))
            .unwrap();

        // State should now include Charlie (who passes filter)
        assert_eq!(
            state.changes.len(),
            1,
            "State should now have Alice and Charlie"
        );
    }

    #[test]
    fn test_aggregate_eval_with_uncommitted_preserves_state() {
        // This is the critical test - aggregations must not modify internal state during eval
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,       // operator_id for testing
            vec![1], // category is at index 1
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum(2), // amount is at index 2
            ],
            vec![
                "id".to_string(),
                "category".to_string(),
                "amount".to_string(),
            ],
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        init_data.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("A".into()),
                Value::Integer(200),
            ],
        );
        init_data.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("B".into()),
                Value::Integer(150),
            ],
        );
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Check initial state: A -> (count=2, sum=300), B -> (count=1, sum=150)
        let initial_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(initial_state.changes.len(), 2);

        // Store initial state for comparison
        let initial_a = initial_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(initial_a.0.values[1], Value::Integer(2)); // count
        assert_eq!(initial_a.0.values[2], Value::Float(300.0)); // sum

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("A".into()),
                Value::Integer(50),
            ],
        );
        uncommitted.insert(
            5,
            vec![
                Value::Integer(5),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        // Eval with uncommitted should return the delta (changes to aggregates)
        let mut eval_state = uncommitted.clone().into();
        let result = pager
            .io
            .block(|| agg.eval(&mut eval_state, &mut cursors))
            .unwrap();

        // Result should contain updates for A and new group C
        // For A: retraction of old (2, 300) and insertion of new (3, 350)
        // For C: insertion of (1, 75)
        assert!(!result.changes.is_empty(), "Should have aggregate changes");

        // CRITICAL: Verify internal state hasn't changed
        let state_after_eval = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(
            state_after_eval.changes.len(),
            2,
            "State should still have only A and B"
        );

        let a_after_eval = state_after_eval
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(
            a_after_eval.0.values[1],
            Value::Integer(2),
            "A count should still be 2"
        );
        assert_eq!(
            a_after_eval.0.values[2],
            Value::Float(300.0),
            "A sum should still be 300"
        );

        // Now commit the changes
        pager
            .io
            .block(|| agg.commit((&uncommitted).into(), &mut cursors))
            .unwrap();

        // State should now be updated
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(final_state.changes.len(), 3, "Should now have A, B, and C");

        let a_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(
            a_final.0.values[1],
            Value::Integer(3),
            "A count should now be 3"
        );
        assert_eq!(
            a_final.0.values[2],
            Value::Float(350.0),
            "A sum should now be 350"
        );

        let c_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("C".into()))
            .unwrap();
        assert_eq!(
            c_final.0.values[1],
            Value::Integer(1),
            "C count should be 1"
        );
        assert_eq!(
            c_final.0.values[2],
            Value::Float(75.0),
            "C sum should be 75"
        );
    }

    #[test]
    fn test_aggregate_eval_multiple_times_without_commit() {
        // Test that calling eval multiple times with different uncommitted data
        // doesn't pollute the internal state
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id for testing
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum(1), // value is at index 1
            ],
            vec!["id".to_string(), "value".to_string()],
        );

        // Initialize
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        init_data.insert(2, vec![Value::Integer(2), Value::Integer(200)]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Initial state: count=2, sum=300
        let initial_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(initial_state.changes.len(), 1);
        assert_eq!(initial_state.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(initial_state.changes[0].0.values[1], Value::Float(300.0));

        // First eval with uncommitted
        let mut uncommitted1 = Delta::new();
        uncommitted1.insert(3, vec![Value::Integer(3), Value::Integer(50)]);
        let mut eval_state1 = uncommitted1.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state1, &mut cursors))
            .unwrap();

        // State should be unchanged
        let state1 = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state1.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state1.changes[0].0.values[1], Value::Float(300.0));

        // Second eval with different uncommitted
        let mut uncommitted2 = Delta::new();
        uncommitted2.insert(4, vec![Value::Integer(4), Value::Integer(75)]);
        uncommitted2.insert(5, vec![Value::Integer(5), Value::Integer(25)]);
        let mut eval_state2 = uncommitted2.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state2, &mut cursors))
            .unwrap();

        // State should STILL be unchanged
        let state2 = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state2.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state2.changes[0].0.values[1], Value::Float(300.0));

        // Third eval with deletion as uncommitted
        let mut uncommitted3 = Delta::new();
        uncommitted3.delete(1, vec![Value::Integer(1), Value::Integer(100)]);
        let mut eval_state3 = uncommitted3.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state3, &mut cursors))
            .unwrap();

        // State should STILL be unchanged
        let state3 = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state3.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state3.changes[0].0.values[1], Value::Float(300.0));
    }

    #[test]
    fn test_aggregate_eval_with_mixed_committed_and_uncommitted() {
        // Test eval with both committed delta and uncommitted changes
        // Create a persistent pager for the test
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        // Create index cursor with proper index definition for DBSP state table
        let index_def = create_dbsp_state_index(index_root_page_id);
        // Index has 4 columns: operator_id, zset_id, element_id, rowid
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,       // operator_id for testing
            vec![1], // type is at index 1
            vec![AggregateFunction::Count],
            vec!["id".to_string(), "type".to_string()],
        );

        // Initialize
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Integer(1), Value::Text("X".into())]);
        init_data.insert(2, vec![Value::Integer(2), Value::Text("Y".into())]);
        pager
            .io
            .block(|| agg.commit((&init_data).into(), &mut cursors))
            .unwrap();

        // Create a committed delta (to be processed)
        let mut committed_delta = Delta::new();
        committed_delta.insert(3, vec![Value::Integer(3), Value::Text("X".into())]);

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(4, vec![Value::Integer(4), Value::Text("Y".into())]);
        uncommitted.insert(5, vec![Value::Integer(5), Value::Text("Z".into())]);

        // Eval with both - should process both but not commit
        let mut combined = committed_delta.clone();
        combined.merge(&uncommitted);
        let mut eval_state = combined.clone().into();
        let result = pager
            .io
            .block(|| agg.eval(&mut eval_state, &mut cursors))
            .unwrap();

        // Result should reflect changes from both
        assert!(!result.changes.is_empty(), "Result should not be empty");

        // Verify the DBSP pattern: retraction (-1) followed by insertion (1) for updates,
        // and just insertion (1) for new groups

        // We expect exactly 5 changes:
        // - X: retraction + insertion (was 1, now 2)
        // - Y: retraction + insertion (was 1, now 2)
        // - Z: insertion only (new group with count 1)
        assert_eq!(
            result.changes.len(),
            5,
            "Should have 5 changes (2 retractions + 3 insertions)"
        );

        // Sort by group name then by weight to get predictable order
        let mut sorted_changes: Vec<_> = result.changes.iter().collect();
        sorted_changes.sort_by(|a, b| {
            let a_group = &a.0.values[0];
            let b_group = &b.0.values[0];
            match a_group.partial_cmp(b_group).unwrap() {
                std::cmp::Ordering::Equal => a.1.cmp(&b.1), // Sort by weight if same group
                other => other,
            }
        });

        // Check X group: should have retraction (-1) for count=1, then insertion (1) for count=2
        assert_eq!(sorted_changes[0].0.values[0], Value::Text("X".into()));
        assert_eq!(sorted_changes[0].0.values[1], Value::Integer(1)); // old count
        assert_eq!(sorted_changes[0].1, -1); // retraction

        assert_eq!(sorted_changes[1].0.values[0], Value::Text("X".into()));
        assert_eq!(sorted_changes[1].0.values[1], Value::Integer(2)); // new count
        assert_eq!(sorted_changes[1].1, 1); // insertion

        // Check Y group: should have retraction (-1) for count=1, then insertion (1) for count=2
        assert_eq!(sorted_changes[2].0.values[0], Value::Text("Y".into()));
        assert_eq!(sorted_changes[2].0.values[1], Value::Integer(1)); // old count
        assert_eq!(sorted_changes[2].1, -1); // retraction

        assert_eq!(sorted_changes[3].0.values[0], Value::Text("Y".into()));
        assert_eq!(sorted_changes[3].0.values[1], Value::Integer(2)); // new count
        assert_eq!(sorted_changes[3].1, 1); // insertion

        // Check Z group: should only have insertion (1) for count=1 (new group)
        assert_eq!(sorted_changes[4].0.values[0], Value::Text("Z".into()));
        assert_eq!(sorted_changes[4].0.values[1], Value::Integer(1)); // new count
        assert_eq!(sorted_changes[4].1, 1); // insertion only (no retraction as it's new);

        // But internal state should be unchanged
        let state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        assert_eq!(state.changes.len(), 2, "Should still have only X and Y");

        // Now commit only the committed_delta
        pager
            .io
            .block(|| agg.commit((&committed_delta).into(), &mut cursors))
            .unwrap();

        // State should now have X count=2, Y count=1
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursors);
        let x = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("X".into()))
            .unwrap();
        assert_eq!(x.0.values[1], Value::Integer(2));
    }

    #[test]
    fn test_min_max_basic() {
        // Test basic MIN/MAX functionality
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Banana".into()),
                Value::Float(0.75),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify MIN and MAX
        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Float(0.75)); // MIN
        assert_eq!(row.values[1], Value::Float(3.50)); // MAX
    }

    #[test]
    fn test_min_max_deletion_updates_min() {
        // Test that deleting the MIN value updates to the next lowest
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Banana".into()),
                Value::Float(0.75),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Delete the MIN value (Banana at 0.75)
        let mut delete_delta = Delta::new();
        delete_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Banana".into()),
                Value::Float(0.75),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction of old values and new values
        assert_eq!(result.changes.len(), 2);

        // Find the retraction (weight = -1)
        let retraction = result.changes.iter().find(|(_, w)| *w == -1).unwrap();
        assert_eq!(retraction.0.values[0], Value::Float(0.75)); // Old MIN
        assert_eq!(retraction.0.values[1], Value::Float(3.50)); // Old MAX

        // Find the new values (weight = 1)
        let new_values = result.changes.iter().find(|(_, w)| *w == 1).unwrap();
        assert_eq!(new_values.0.values[0], Value::Float(1.50)); // New MIN (Apple)
        assert_eq!(new_values.0.values[1], Value::Float(3.50)); // MAX unchanged
    }

    #[test]
    fn test_min_max_deletion_updates_max() {
        // Test that deleting the MAX value updates to the next highest
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Banana".into()),
                Value::Float(0.75),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Delete the MAX value (Grape at 3.50)
        let mut delete_delta = Delta::new();
        delete_delta.delete(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&delete_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction of old values and new values
        assert_eq!(result.changes.len(), 2);

        // Find the retraction (weight = -1)
        let retraction = result.changes.iter().find(|(_, w)| *w == -1).unwrap();
        assert_eq!(retraction.0.values[0], Value::Float(0.75)); // Old MIN
        assert_eq!(retraction.0.values[1], Value::Float(3.50)); // Old MAX

        // Find the new values (weight = 1)
        let new_values = result.changes.iter().find(|(_, w)| *w == 1).unwrap();
        assert_eq!(new_values.0.values[0], Value::Float(0.75)); // MIN unchanged
        assert_eq!(new_values.0.values[1], Value::Float(2.00)); // New MAX (Orange)
    }

    #[test]
    fn test_min_max_insertion_updates_min() {
        // Test that inserting a new MIN value updates the aggregate
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Insert a new MIN value
        let mut insert_delta = Delta::new();
        insert_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Lemon".into()),
                Value::Float(0.50),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&insert_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction of old values and new values
        assert_eq!(result.changes.len(), 2);

        // Find the retraction (weight = -1)
        let retraction = result.changes.iter().find(|(_, w)| *w == -1).unwrap();
        assert_eq!(retraction.0.values[0], Value::Float(1.50)); // Old MIN
        assert_eq!(retraction.0.values[1], Value::Float(3.50)); // Old MAX

        // Find the new values (weight = 1)
        let new_values = result.changes.iter().find(|(_, w)| *w == 1).unwrap();
        assert_eq!(new_values.0.values[0], Value::Float(0.50)); // New MIN (Lemon)
        assert_eq!(new_values.0.values[1], Value::Float(3.50)); // MAX unchanged
    }

    #[test]
    fn test_min_max_insertion_updates_max() {
        // Test that inserting a new MAX value updates the aggregate
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Insert a new MAX value
        let mut insert_delta = Delta::new();
        insert_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Melon".into()),
                Value::Float(5.00),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&insert_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction of old values and new values
        assert_eq!(result.changes.len(), 2);

        // Find the retraction (weight = -1)
        let retraction = result.changes.iter().find(|(_, w)| *w == -1).unwrap();
        assert_eq!(retraction.0.values[0], Value::Float(1.50)); // Old MIN
        assert_eq!(retraction.0.values[1], Value::Float(3.50)); // Old MAX

        // Find the new values (weight = 1)
        let new_values = result.changes.iter().find(|(_, w)| *w == 1).unwrap();
        assert_eq!(new_values.0.values[0], Value::Float(1.50)); // MIN unchanged
        assert_eq!(new_values.0.values[1], Value::Float(5.00)); // New MAX (Melon)
    }

    #[test]
    fn test_min_max_update_changes_min() {
        // Test that updating a row to become the new MIN updates the aggregate
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Update Orange price to be the new MIN (update = delete + insert)
        let mut update_delta = Delta::new();
        update_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        update_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Orange".into()),
                Value::Float(0.25),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&update_delta).into(), &mut cursors))
            .unwrap();

        // Should emit retraction of old values and new values
        assert_eq!(result.changes.len(), 2);

        // Find the retraction (weight = -1)
        let retraction = result.changes.iter().find(|(_, w)| *w == -1).unwrap();
        assert_eq!(retraction.0.values[0], Value::Float(1.50)); // Old MIN
        assert_eq!(retraction.0.values[1], Value::Float(3.50)); // Old MAX

        // Find the new values (weight = 1)
        let new_values = result.changes.iter().find(|(_, w)| *w == 1).unwrap();
        assert_eq!(new_values.0.values[0], Value::Float(0.25)); // New MIN (updated Orange)
        assert_eq!(new_values.0.values[1], Value::Float(3.50)); // MAX unchanged
    }

    #[test]
    fn test_min_max_with_group_by() {
        // Test MIN/MAX with GROUP BY
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,       // operator_id
            vec![1], // GROUP BY category (index 1)
            vec![
                AggregateFunction::Min(3), // price is at index 3
                AggregateFunction::Max(3), // price is at index 3
            ],
            vec![
                "id".to_string(),
                "category".to_string(),
                "name".to_string(),
                "price".to_string(),
            ],
        );

        // Initial data with two categories
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("fruit".into()),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("fruit".into()),
                Value::Text("Banana".into()),
                Value::Float(0.75),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("fruit".into()),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("veggie".into()),
                Value::Text("Carrot".into()),
                Value::Float(0.50),
            ],
        );
        initial_delta.insert(
            5,
            vec![
                Value::Integer(5),
                Value::Text("veggie".into()),
                Value::Text("Lettuce".into()),
                Value::Float(1.25),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Should have two groups
        assert_eq!(result.changes.len(), 2);

        // Find fruit group
        let fruit = result
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("fruit".into()))
            .unwrap();
        assert_eq!(fruit.1, 1); // weight
        assert_eq!(fruit.0.values[1], Value::Float(0.75)); // MIN (Banana)
        assert_eq!(fruit.0.values[2], Value::Float(2.00)); // MAX (Orange)

        // Find veggie group
        let veggie = result
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("veggie".into()))
            .unwrap();
        assert_eq!(veggie.1, 1); // weight
        assert_eq!(veggie.0.values[1], Value::Float(0.50)); // MIN (Carrot)
        assert_eq!(veggie.0.values[2], Value::Float(1.25)); // MAX (Lettuce)
    }

    #[test]
    fn test_min_max_with_nulls() {
        // Test that NULL values are ignored in MIN/MAX
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // price is at index 2
                AggregateFunction::Max(2), // price is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        // Initial data with NULL values
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Apple".into()),
                Value::Float(1.50),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Unknown1".into()),
                Value::Null,
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Orange".into()),
                Value::Float(2.00),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Unknown2".into()),
                Value::Null,
            ],
        );
        initial_delta.insert(
            5,
            vec![
                Value::Integer(5),
                Value::Text("Grape".into()),
                Value::Float(3.50),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify MIN and MAX ignore NULLs
        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Float(1.50)); // MIN (Apple, ignoring NULLs)
        assert_eq!(row.values[1], Value::Float(3.50)); // MAX (Grape, ignoring NULLs)
    }

    #[test]
    fn test_min_max_integer_values() {
        // Test MIN/MAX with integer values instead of floats
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(2), // score is at index 2
                AggregateFunction::Max(2), // score is at index 2
            ],
            vec!["id".to_string(), "name".to_string(), "score".to_string()],
        );

        // Initial data with integer scores
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(85),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(92),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Carol".into()),
                Value::Integer(78),
            ],
        );
        initial_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("Dave".into()),
                Value::Integer(95),
            ],
        );

        let result = pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify MIN and MAX with integers
        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Integer(78)); // MIN (Carol)
        assert_eq!(row.values[1], Value::Integer(95)); // MAX (Dave)
    }

    #[test]
    fn test_min_max_text_values() {
        // Test MIN/MAX with text values (alphabetical ordering)
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(1), // name is at index 1
                AggregateFunction::Max(1), // name is at index 1
            ],
            vec!["id".to_string(), "name".to_string()],
        );

        // Initial data with text values
        let mut initial_delta = Delta::new();
        initial_delta.insert(1, vec![Value::Integer(1), Value::Text("Charlie".into())]);
        initial_delta.insert(2, vec![Value::Integer(2), Value::Text("Alice".into())]);
        initial_delta.insert(3, vec![Value::Integer(3), Value::Text("Bob".into())]);
        initial_delta.insert(4, vec![Value::Integer(4), Value::Text("David".into())]);

        let result = pager
            .io
            .block(|| agg.commit((&initial_delta).into(), &mut cursors))
            .unwrap();

        // Verify MIN and MAX with text (alphabetical)
        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Text("Alice".into())); // MIN alphabetically
        assert_eq!(row.values[1], Value::Text("David".into())); // MAX alphabetically
    }

    #[test]
    fn test_min_max_with_other_aggregates() {
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum(1), // value is at index 1
                AggregateFunction::Min(1), // value is at index 1
                AggregateFunction::Max(1), // value is at index 1
                AggregateFunction::Avg(1), // value is at index 1
            ],
            vec!["id".to_string(), "value".to_string()],
        );

        // Initial data
        let mut delta = Delta::new();
        delta.insert(1, vec![Value::Integer(1), Value::Integer(10)]);
        delta.insert(2, vec![Value::Integer(2), Value::Integer(5)]);
        delta.insert(3, vec![Value::Integer(3), Value::Integer(15)]);
        delta.insert(4, vec![Value::Integer(4), Value::Integer(20)]);

        let result = pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Integer(4)); // COUNT
        assert_eq!(row.values[1], Value::Integer(50)); // SUM
        assert_eq!(row.values[2], Value::Integer(5)); // MIN
        assert_eq!(row.values[3], Value::Integer(20)); // MAX
        assert_eq!(row.values[4], Value::Float(12.5)); // AVG (50/4)

        // Delete the MIN value
        let mut delta2 = Delta::new();
        delta2.delete(2, vec![Value::Integer(2), Value::Integer(5)]);

        let result2 = pager
            .io
            .block(|| agg.commit((&delta2).into(), &mut cursors))
            .unwrap();

        assert_eq!(result2.changes.len(), 2);
        let (row_del, weight_del) = &result2.changes[0];
        assert_eq!(*weight_del, -1);
        assert_eq!(row_del.values[0], Value::Integer(4)); // Old COUNT
        assert_eq!(row_del.values[1], Value::Integer(50)); // Old SUM
        assert_eq!(row_del.values[2], Value::Integer(5)); // Old MIN
        assert_eq!(row_del.values[3], Value::Integer(20)); // Old MAX
        assert_eq!(row_del.values[4], Value::Float(12.5)); // Old AVG

        let (row_ins, weight_ins) = &result2.changes[1];
        assert_eq!(*weight_ins, 1);
        assert_eq!(row_ins.values[0], Value::Integer(3)); // New COUNT
        assert_eq!(row_ins.values[1], Value::Integer(45)); // New SUM
        assert_eq!(row_ins.values[2], Value::Integer(10)); // New MIN
        assert_eq!(row_ins.values[3], Value::Integer(20)); // MAX unchanged
        assert_eq!(row_ins.values[4], Value::Float(15.0)); // New AVG (45/3)

        // Now delete the MAX value
        let mut delta3 = Delta::new();
        delta3.delete(4, vec![Value::Integer(4), Value::Integer(20)]);

        let result3 = pager
            .io
            .block(|| agg.commit((&delta3).into(), &mut cursors))
            .unwrap();

        assert_eq!(result3.changes.len(), 2);
        let (row_del2, weight_del2) = &result3.changes[0];
        assert_eq!(*weight_del2, -1);
        assert_eq!(row_del2.values[3], Value::Integer(20)); // Old MAX

        let (row_ins2, weight_ins2) = &result3.changes[1];
        assert_eq!(*weight_ins2, 1);
        assert_eq!(row_ins2.values[0], Value::Integer(2)); // COUNT
        assert_eq!(row_ins2.values[1], Value::Integer(25)); // SUM
        assert_eq!(row_ins2.values[2], Value::Integer(10)); // MIN unchanged
        assert_eq!(row_ins2.values[3], Value::Integer(15)); // New MAX
        assert_eq!(row_ins2.values[4], Value::Float(12.5)); // AVG (25/2)
    }

    #[test]
    fn test_min_max_multiple_columns() {
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut agg = AggregateOperator::new(
            1,      // operator_id
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Min(0), // col1 is at index 0
                AggregateFunction::Max(1), // col2 is at index 1
                AggregateFunction::Min(2), // col3 is at index 2
            ],
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
        );

        // Initial data
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(10),
                Value::Integer(100),
                Value::Integer(1000),
            ],
        );
        delta.insert(
            2,
            vec![Value::Integer(5), Value::Integer(200), Value::Integer(2000)],
        );
        delta.insert(
            3,
            vec![Value::Integer(15), Value::Integer(150), Value::Integer(500)],
        );

        let result = pager
            .io
            .block(|| agg.commit((&delta).into(), &mut cursors))
            .unwrap();

        assert_eq!(result.changes.len(), 1);
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values[0], Value::Integer(5)); // MIN(col1)
        assert_eq!(row.values[1], Value::Integer(200)); // MAX(col2)
        assert_eq!(row.values[2], Value::Integer(500)); // MIN(col3)

        // Delete the row with MIN(col1) and MAX(col2)
        let mut delta2 = Delta::new();
        delta2.delete(
            2,
            vec![Value::Integer(5), Value::Integer(200), Value::Integer(2000)],
        );

        let result2 = pager
            .io
            .block(|| agg.commit((&delta2).into(), &mut cursors))
            .unwrap();

        assert_eq!(result2.changes.len(), 2);
        // Should emit delete of old state and insert of new state
        let (row_del, weight_del) = &result2.changes[0];
        assert_eq!(*weight_del, -1);
        assert_eq!(row_del.values[0], Value::Integer(5)); // Old MIN(col1)
        assert_eq!(row_del.values[1], Value::Integer(200)); // Old MAX(col2)
        assert_eq!(row_del.values[2], Value::Integer(500)); // Old MIN(col3)

        let (row_ins, weight_ins) = &result2.changes[1];
        assert_eq!(*weight_ins, 1);
        assert_eq!(row_ins.values[0], Value::Integer(10)); // New MIN(col1)
        assert_eq!(row_ins.values[1], Value::Integer(150)); // New MAX(col2)
        assert_eq!(row_ins.values[2], Value::Integer(500)); // MIN(col3) unchanged
    }

    #[test]
    fn test_join_operator_inner() {
        // Test INNER JOIN with incremental updates
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);
        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on first column
            vec![0],
            vec!["customer_id".to_string(), "amount".to_string()],
            vec!["id".to_string(), "name".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Initialize with data
        let mut left_delta = Delta::new();
        left_delta.insert(1, vec![Value::Integer(1), Value::Float(100.0)]);
        left_delta.insert(2, vec![Value::Integer(2), Value::Float(200.0)]);
        left_delta.insert(3, vec![Value::Integer(3), Value::Float(300.0)]); // No match initially
        let mut right_delta = Delta::new();
        right_delta.insert(1, vec![Value::Integer(1), Value::Text("Alice".into())]);
        right_delta.insert(2, vec![Value::Integer(2), Value::Text("Bob".into())]);
        right_delta.insert(4, vec![Value::Integer(4), Value::Text("David".into())]); // No match initially

        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        // Should have 2 matches (customer 1 and 2)
        assert_eq!(
            result.changes.len(),
            2,
            "First commit should produce 2 matches"
        );

        let mut results: Vec<_> = result.changes.clone();
        results.sort_by_key(|r| r.0.values[0].clone());

        assert_eq!(results[0].0.values[0], Value::Integer(1));
        assert_eq!(results[0].0.values[3], Value::Text("Alice".into()));
        assert_eq!(results[1].0.values[0], Value::Integer(2));
        assert_eq!(results[1].0.values[3], Value::Text("Bob".into()));

        // SECOND COMMIT: Add incremental data that should join with persisted state
        // Add a new left row that should match existing right row (customer 4)
        let mut left_delta2 = Delta::new();
        left_delta2.insert(5, vec![Value::Integer(4), Value::Float(400.0)]); // Should match David from persisted state

        // Add a new right row that should match existing left row (customer 3)
        let mut right_delta2 = Delta::new();
        right_delta2.insert(6, vec![Value::Integer(3), Value::Text("Charlie".into())]); // Should match customer 3 from persisted state

        let delta_pair2 = DeltaPair::new(left_delta2, right_delta2);
        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // The second commit should produce:
        // 1. New left (customer_id=4) joins with persisted right (id=4, David)
        // 2. Persisted left (customer_id=3) joins with new right (id=3, Charlie)

        assert_eq!(
            result2.changes.len(),
            2,
            "Second commit should produce 2 new matches from incremental join. Got: {:?}",
            result2.changes
        );

        // Verify the incremental results
        let mut results2: Vec<_> = result2.changes.clone();
        results2.sort_by_key(|r| r.0.values[0].clone());

        // Check for customer 3 joined with Charlie (existing left + new right)
        let charlie_match = results2
            .iter()
            .find(|(row, _)| row.values[0] == Value::Integer(3))
            .expect("Should find customer 3 joined with new Charlie");
        assert_eq!(charlie_match.0.values[2], Value::Integer(3));
        assert_eq!(charlie_match.0.values[3], Value::Text("Charlie".into()));

        // Check for customer 4 joined with David (new left + existing right)
        let david_match = results2
            .iter()
            .find(|(row, _)| row.values[0] == Value::Integer(4))
            .expect("Should find new customer 4 joined with existing David");
        assert_eq!(david_match.0.values[0], Value::Integer(4));
        assert_eq!(david_match.0.values[3], Value::Text("David".into()));
    }

    #[test]
    fn test_join_operator_with_deletions() {
        // Test INNER JOIN with deletions (negative weights)
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on first column
            vec![0],
            vec!["customer_id".to_string(), "amount".to_string()],
            vec!["id".to_string(), "name".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Add initial data
        let mut left_delta = Delta::new();
        left_delta.insert(1, vec![Value::Integer(1), Value::Float(100.0)]);
        left_delta.insert(2, vec![Value::Integer(2), Value::Float(200.0)]);
        left_delta.insert(3, vec![Value::Integer(3), Value::Float(300.0)]);

        let mut right_delta = Delta::new();
        right_delta.insert(1, vec![Value::Integer(1), Value::Text("Alice".into())]);
        right_delta.insert(2, vec![Value::Integer(2), Value::Text("Bob".into())]);
        right_delta.insert(3, vec![Value::Integer(3), Value::Text("Charlie".into())]);

        let delta_pair = DeltaPair::new(left_delta, right_delta);

        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        assert_eq!(result.changes.len(), 3, "Should have 3 initial joins");

        // SECOND COMMIT: Delete customer 2 from left side
        let mut left_delta2 = Delta::new();
        left_delta2.delete(2, vec![Value::Integer(2), Value::Float(200.0)]);

        let empty_right = Delta::new();
        let delta_pair2 = DeltaPair::new(left_delta2, empty_right);

        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // Should produce 1 deletion (retraction) of the join for customer 2
        assert_eq!(
            result2.changes.len(),
            1,
            "Should produce 1 retraction for deleted customer 2"
        );
        assert_eq!(
            result2.changes[0].1, -1,
            "Should have weight -1 for deletion"
        );
        assert_eq!(result2.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(result2.changes[0].0.values[3], Value::Text("Bob".into()));

        // THIRD COMMIT: Delete customer 3 from right side
        let empty_left = Delta::new();
        let mut right_delta3 = Delta::new();
        right_delta3.delete(3, vec![Value::Integer(3), Value::Text("Charlie".into())]);

        let delta_pair3 = DeltaPair::new(empty_left, right_delta3);

        let result3 = pager
            .io
            .block(|| join.commit(delta_pair3.clone(), &mut cursors))
            .unwrap();

        // Should produce 1 deletion (retraction) of the join for customer 3
        assert_eq!(
            result3.changes.len(),
            1,
            "Should produce 1 retraction for deleted customer 3"
        );
        assert_eq!(
            result3.changes[0].1, -1,
            "Should have weight -1 for deletion"
        );
        assert_eq!(result3.changes[0].0.values[0], Value::Integer(3));
        assert_eq!(result3.changes[0].0.values[2], Value::Integer(3));
    }

    #[test]
    fn test_join_operator_one_to_many() {
        // Test one-to-many relationship: one customer with multiple orders
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on first column (customer_id for orders)
            vec![0], // Join on first column (id for customers)
            vec![
                "customer_id".to_string(),
                "order_id".to_string(),
                "amount".to_string(),
            ],
            vec!["id".to_string(), "name".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Add one customer
        let left_delta = Delta::new(); // Empty orders initially
        let mut right_delta = Delta::new();
        right_delta.insert(1, vec![Value::Integer(100), Value::Text("Alice".into())]);

        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        // No joins yet (customer exists but no orders)
        assert_eq!(
            result.changes.len(),
            0,
            "Should have no joins with customer but no orders"
        );

        // SECOND COMMIT: Add multiple orders for the same customer
        let mut left_delta2 = Delta::new();
        left_delta2.insert(
            1,
            vec![
                Value::Integer(100),
                Value::Integer(1001),
                Value::Float(50.0),
            ],
        ); // order 1001
        left_delta2.insert(
            2,
            vec![
                Value::Integer(100),
                Value::Integer(1002),
                Value::Float(75.0),
            ],
        ); // order 1002
        left_delta2.insert(
            3,
            vec![
                Value::Integer(100),
                Value::Integer(1003),
                Value::Float(100.0),
            ],
        ); // order 1003

        let right_delta2 = Delta::new(); // No new customers

        let delta_pair2 = DeltaPair::new(left_delta2, right_delta2);
        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // Should produce 3 joins (3 orders × 1 customer)
        assert_eq!(
            result2.changes.len(),
            3,
            "Should produce 3 joins for 3 orders with same customer. Got: {:?}",
            result2.changes
        );

        // Verify all three joins have the same customer but different orders
        for (row, weight) in &result2.changes {
            assert_eq!(*weight, 1, "Weight should be 1 for insertion");
            assert_eq!(
                row.values[0],
                Value::Integer(100),
                "Customer ID should be 100"
            );
            assert_eq!(
                row.values[4],
                Value::Text("Alice".into()),
                "Customer name should be Alice"
            );

            // Check order IDs are different
            let order_id = match &row.values[1] {
                Value::Integer(id) => *id,
                _ => panic!("Expected integer order ID"),
            };
            assert!(
                (1001..=1003).contains(&order_id),
                "Order ID {order_id} should be between 1001 and 1003"
            );
        }

        // THIRD COMMIT: Delete one order
        let mut left_delta3 = Delta::new();
        left_delta3.delete(
            2,
            vec![
                Value::Integer(100),
                Value::Integer(1002),
                Value::Float(75.0),
            ],
        );

        let delta_pair3 = DeltaPair::new(left_delta3, Delta::new());
        let result3 = pager
            .io
            .block(|| join.commit(delta_pair3.clone(), &mut cursors))
            .unwrap();

        // Should produce 1 retraction for the deleted order
        assert_eq!(result3.changes.len(), 1, "Should produce 1 retraction");
        assert_eq!(result3.changes[0].1, -1, "Should be a deletion");
        assert_eq!(
            result3.changes[0].0.values[1],
            Value::Integer(1002),
            "Should delete order 1002"
        );
    }

    #[test]
    fn test_join_operator_many_to_many() {
        // Test many-to-many: multiple rows with same key on both sides
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on category_id
            vec![0], // Join on id
            vec![
                "category_id".to_string(),
                "product_name".to_string(),
                "price".to_string(),
            ],
            vec!["id".to_string(), "category_name".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Add multiple products in same category
        let mut left_delta = Delta::new();
        left_delta.insert(
            1,
            vec![
                Value::Integer(10),
                Value::Text("Laptop".into()),
                Value::Float(1000.0),
            ],
        );
        left_delta.insert(
            2,
            vec![
                Value::Integer(10),
                Value::Text("Mouse".into()),
                Value::Float(50.0),
            ],
        );
        left_delta.insert(
            3,
            vec![
                Value::Integer(10),
                Value::Text("Keyboard".into()),
                Value::Float(100.0),
            ],
        );

        // Add multiple categories with same ID (simulating denormalized data or versioning)
        let mut right_delta = Delta::new();
        right_delta.insert(
            1,
            vec![Value::Integer(10), Value::Text("Electronics".into())],
        );
        right_delta.insert(2, vec![Value::Integer(10), Value::Text("Computers".into())]); // Same category ID, different name

        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        // Should produce 3 products × 2 categories = 6 joins
        assert_eq!(
            result.changes.len(),
            6,
            "Should produce 6 joins (3 products × 2 category records). Got: {:?}",
            result.changes
        );

        // Verify we have all combinations
        let mut found_combinations = std::collections::HashSet::new();
        for (row, weight) in &result.changes {
            assert_eq!(*weight, 1);
            let product = row.values[1].to_string();
            let category = row.values[4].to_string();
            found_combinations.insert((product, category));
        }

        assert_eq!(
            found_combinations.len(),
            6,
            "Should have 6 unique combinations"
        );

        // SECOND COMMIT: Add one more product in the same category
        let mut left_delta2 = Delta::new();
        left_delta2.insert(
            4,
            vec![
                Value::Integer(10),
                Value::Text("Monitor".into()),
                Value::Float(500.0),
            ],
        );

        let delta_pair2 = DeltaPair::new(left_delta2, Delta::new());
        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // New product should join with both existing category records
        assert_eq!(
            result2.changes.len(),
            2,
            "New product should join with 2 existing category records"
        );

        for (row, _) in &result2.changes {
            assert_eq!(row.values[1], Value::Text("Monitor".into()));
        }
    }

    #[test]
    fn test_join_operator_update_in_one_to_many() {
        // Test updates in one-to-many scenarios
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on customer_id
            vec![0], // Join on id
            vec![
                "customer_id".to_string(),
                "order_id".to_string(),
                "amount".to_string(),
            ],
            vec!["id".to_string(), "name".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Setup one customer with multiple orders
        let mut left_delta = Delta::new();
        left_delta.insert(
            1,
            vec![
                Value::Integer(100),
                Value::Integer(1001),
                Value::Float(50.0),
            ],
        );
        left_delta.insert(
            2,
            vec![
                Value::Integer(100),
                Value::Integer(1002),
                Value::Float(75.0),
            ],
        );
        left_delta.insert(
            3,
            vec![
                Value::Integer(100),
                Value::Integer(1003),
                Value::Float(100.0),
            ],
        );

        let mut right_delta = Delta::new();
        right_delta.insert(1, vec![Value::Integer(100), Value::Text("Alice".into())]);

        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        assert_eq!(result.changes.len(), 3, "Should have 3 initial joins");

        // SECOND COMMIT: Update the customer name (affects all 3 joins)
        let mut right_delta2 = Delta::new();
        // Delete old customer record
        right_delta2.delete(1, vec![Value::Integer(100), Value::Text("Alice".into())]);
        // Insert updated customer record
        right_delta2.insert(
            1,
            vec![Value::Integer(100), Value::Text("Alice Smith".into())],
        );

        let delta_pair2 = DeltaPair::new(Delta::new(), right_delta2);
        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // Should produce 3 deletions and 3 insertions (one for each order)
        assert_eq!(result2.changes.len(), 6,
            "Should produce 6 changes (3 deletions + 3 insertions) when updating customer with 3 orders");

        let deletions: Vec<_> = result2.changes.iter().filter(|(_, w)| *w == -1).collect();
        let insertions: Vec<_> = result2.changes.iter().filter(|(_, w)| *w == 1).collect();

        assert_eq!(deletions.len(), 3, "Should have 3 deletions");
        assert_eq!(insertions.len(), 3, "Should have 3 insertions");

        // Check all deletions have old name
        for (row, _) in &deletions {
            assert_eq!(
                row.values[4],
                Value::Text("Alice".into()),
                "Deletions should have old name"
            );
        }

        // Check all insertions have new name
        for (row, _) in &insertions {
            assert_eq!(
                row.values[4],
                Value::Text("Alice Smith".into()),
                "Insertions should have new name"
            );
        }

        // Verify we still have all three order IDs in the insertions
        let mut order_ids = std::collections::HashSet::new();
        for (row, _) in &insertions {
            if let Value::Integer(order_id) = &row.values[1] {
                order_ids.insert(*order_id);
            }
        }
        assert_eq!(
            order_ids.len(),
            3,
            "Should still have all 3 order IDs after update"
        );
        assert!(order_ids.contains(&1001));
        assert!(order_ids.contains(&1002));
        assert!(order_ids.contains(&1003));
    }

    #[test]
    fn test_join_operator_weight_accumulation_complex() {
        // Test complex weight accumulation with multiple identical rows
        let (pager, table_page_id, index_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_page_id, 10);
        let index_def = create_dbsp_state_index(index_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_page_id, &index_def, 10);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut join = JoinOperator::new(
            1, // operator_id
            JoinType::Inner,
            vec![0], // Join on first column
            vec![0],
            vec!["key".to_string(), "val_left".to_string()],
            vec!["key".to_string(), "val_right".to_string()],
        )
        .unwrap();

        // FIRST COMMIT: Add identical rows multiple times (simulating duplicates)
        let mut left_delta = Delta::new();
        // Same key-value pair inserted 3 times with different rowids
        left_delta.insert(1, vec![Value::Integer(10), Value::Text("A".into())]);
        left_delta.insert(2, vec![Value::Integer(10), Value::Text("A".into())]);
        left_delta.insert(3, vec![Value::Integer(10), Value::Text("A".into())]);

        let mut right_delta = Delta::new();
        // Same key-value pair inserted 2 times
        right_delta.insert(4, vec![Value::Integer(10), Value::Text("B".into())]);
        right_delta.insert(5, vec![Value::Integer(10), Value::Text("B".into())]);

        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let result = pager
            .io
            .block(|| join.commit(delta_pair.clone(), &mut cursors))
            .unwrap();

        // Should produce 3 × 2 = 6 join results (cartesian product)
        assert_eq!(
            result.changes.len(),
            6,
            "Should produce 6 joins (3 left rows × 2 right rows)"
        );

        // All should have weight 1
        for (_, weight) in &result.changes {
            assert_eq!(*weight, 1);
        }

        // SECOND COMMIT: Delete one instance from left
        let mut left_delta2 = Delta::new();
        left_delta2.delete(2, vec![Value::Integer(10), Value::Text("A".into())]);

        let delta_pair2 = DeltaPair::new(left_delta2, Delta::new());
        let result2 = pager
            .io
            .block(|| join.commit(delta_pair2.clone(), &mut cursors))
            .unwrap();

        // Should produce 2 retractions (1 deleted left row × 2 right rows)
        assert_eq!(
            result2.changes.len(),
            2,
            "Should produce 2 retractions when deleting 1 of 3 identical left rows"
        );

        for (_, weight) in &result2.changes {
            assert_eq!(*weight, -1, "Should be retractions");
        }
    }

    #[test]
    fn test_join_produces_all_expected_results() {
        // Test that a join produces ALL expected output rows
        // This reproduces the issue where only 1 of 3 expected rows appears in the final result

        // Create a join operator similar to: SELECT u.name, o.quantity FROM users u JOIN orders o ON u.id = o.user_id
        let mut join = JoinOperator::new(
            0,
            JoinType::Inner,
            vec![0], // Join on first column (id)
            vec![0], // Join on first column (user_id)
            vec!["id".to_string(), "name".to_string()],
            vec![
                "user_id".to_string(),
                "product_id".to_string(),
                "quantity".to_string(),
            ],
        )
        .unwrap();

        // Create test data matching the example that fails:
        // users: (1, 'Alice'), (2, 'Bob')
        // orders: (1, 5), (1, 3), (2, 7)  -- user_id, quantity
        let left_delta = Delta {
            changes: vec![
                (
                    HashableRow::new(1, vec![Value::Integer(1), Value::Text(Text::from("Alice"))]),
                    1,
                ),
                (
                    HashableRow::new(2, vec![Value::Integer(2), Value::Text(Text::from("Bob"))]),
                    1,
                ),
            ],
        };

        // Orders: Alice has 2 orders, Bob has 1
        let right_delta = Delta {
            changes: vec![
                (
                    HashableRow::new(
                        1,
                        vec![Value::Integer(1), Value::Integer(100), Value::Integer(5)],
                    ),
                    1,
                ),
                (
                    HashableRow::new(
                        2,
                        vec![Value::Integer(1), Value::Integer(101), Value::Integer(3)],
                    ),
                    1,
                ),
                (
                    HashableRow::new(
                        3,
                        vec![Value::Integer(2), Value::Integer(100), Value::Integer(7)],
                    ),
                    1,
                ),
            ],
        };

        // Evaluate the join
        let delta_pair = DeltaPair::new(left_delta, right_delta);
        let mut state = EvalState::Init { deltas: delta_pair };

        let (pager, table_root, index_root) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root, 5);
        let index_def = create_dbsp_state_index(index_root);
        let index_cursor = BTreeCursor::new_index(None, pager.clone(), index_root, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let result = pager
            .io
            .block(|| join.eval(&mut state, &mut cursors))
            .unwrap();

        // Should produce 3 results: Alice with 2 orders, Bob with 1 order
        assert_eq!(
            result.changes.len(),
            3,
            "Should produce 3 joined rows (Alice×2 + Bob×1)"
        );

        // Verify the actual content of the results
        let mut expected_results = std::collections::HashSet::new();
        // Expected: (Alice, 5), (Alice, 3), (Bob, 7)
        expected_results.insert(("Alice".to_string(), 5));
        expected_results.insert(("Alice".to_string(), 3));
        expected_results.insert(("Bob".to_string(), 7));

        let mut actual_results = std::collections::HashSet::new();
        for (row, weight) in &result.changes {
            assert_eq!(*weight, 1, "All results should have weight 1");

            // Extract name (column 1 from left) and quantity (column 3 from right)
            let name = match &row.values[1] {
                Value::Text(t) => t.as_str().to_string(),
                _ => panic!("Expected text value for name"),
            };
            let quantity = match &row.values[4] {
                Value::Integer(q) => *q,
                _ => panic!("Expected integer value for quantity"),
            };

            actual_results.insert((name, quantity));
        }

        assert_eq!(
            expected_results, actual_results,
            "Join should produce all expected results. Expected: {expected_results:?}, Got: {actual_results:?}",
        );

        // Also verify that rowids are unique (this is important for btree storage)
        let mut seen_rowids = std::collections::HashSet::new();
        for (row, _) in &result.changes {
            let was_new = seen_rowids.insert(row.rowid);
            assert!(was_new, "Duplicate rowid found: {}. This would cause rows to overwrite each other in btree storage!", row.rowid);
        }
    }

    // Merge operator tests
    use crate::incremental::merge_operator::{MergeOperator, UnionMode};

    #[test]
    fn test_merge_operator_basic() {
        let (_pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, _pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, _pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let mut merge_op = MergeOperator::new(
            1,
            UnionMode::All {
                left_table: "table1".to_string(),
                right_table: "table2".to_string(),
            },
        );

        // Create two deltas
        let mut left_delta = Delta::new();
        left_delta.insert(1, vec![Value::Integer(1)]);
        left_delta.insert(2, vec![Value::Integer(2)]);

        let mut right_delta = Delta::new();
        right_delta.insert(3, vec![Value::Integer(3)]);
        right_delta.insert(4, vec![Value::Integer(4)]);

        let delta_pair = DeltaPair::new(left_delta, right_delta);

        // Evaluate merge
        let result = merge_op.commit(delta_pair, &mut cursors).unwrap();

        if let IOResult::Done(merged) = result {
            // Should have all 4 entries
            assert_eq!(merged.len(), 4);

            // Check that all values are present
            let values: Vec<i64> = merged
                .changes
                .iter()
                .filter_map(|(row, weight)| {
                    if *weight > 0 && !row.values.is_empty() {
                        if let Value::Integer(n) = &row.values[0] {
                            Some(*n)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            assert!(values.contains(&1));
            assert!(values.contains(&2));
            assert!(values.contains(&3));
            assert!(values.contains(&4));
        } else {
            panic!("Expected Done result");
        }
    }

    #[test]
    fn test_merge_operator_stateful_distinct() {
        let (_pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, _pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, _pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Test that UNION (distinct) properly deduplicates across multiple operations
        let mut merge_op = MergeOperator::new(7, UnionMode::Distinct);

        // First operation: insert values 1, 2, 3 from left and 2, 3, 4 from right
        let mut left_delta1 = Delta::new();
        left_delta1.insert(1, vec![Value::Integer(1)]);
        left_delta1.insert(2, vec![Value::Integer(2)]);
        left_delta1.insert(3, vec![Value::Integer(3)]);

        let mut right_delta1 = Delta::new();
        right_delta1.insert(4, vec![Value::Integer(2)]); // Duplicate value 2
        right_delta1.insert(5, vec![Value::Integer(3)]); // Duplicate value 3
        right_delta1.insert(6, vec![Value::Integer(4)]);

        let result1 = merge_op
            .commit(DeltaPair::new(left_delta1, right_delta1), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged1) = result1 {
            // Should have 4 unique values (1, 2, 3, 4)
            // But 6 total entries (3 from left + 3 from right)
            assert_eq!(merged1.len(), 6);

            // Collect unique rowids - should be 4
            let unique_rowids: std::collections::HashSet<i64> =
                merged1.changes.iter().map(|(row, _)| row.rowid).collect();
            assert_eq!(
                unique_rowids.len(),
                4,
                "Should have 4 unique rowids for 4 unique values"
            );
        } else {
            panic!("Expected Done result");
        }

        // Second operation: insert value 2 again from left, and value 5 from right
        let mut left_delta2 = Delta::new();
        left_delta2.insert(7, vec![Value::Integer(2)]); // Duplicate of existing value

        let mut right_delta2 = Delta::new();
        right_delta2.insert(8, vec![Value::Integer(5)]); // New value

        let result2 = merge_op
            .commit(DeltaPair::new(left_delta2, right_delta2), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged2) = result2 {
            assert_eq!(merged2.len(), 2, "Should have 2 entries in delta");

            // Check that value 2 got the same rowid as before
            let has_existing_rowid = merged2
                .changes
                .iter()
                .any(|(row, _)| row.values == vec![Value::Integer(2)] && row.rowid <= 4);
            assert!(has_existing_rowid, "Value 2 should reuse existing rowid");

            // Check that value 5 got a new rowid
            let has_new_rowid = merged2
                .changes
                .iter()
                .any(|(row, _)| row.values == vec![Value::Integer(5)] && row.rowid > 4);
            assert!(has_new_rowid, "Value 5 should get a new rowid");
        } else {
            panic!("Expected Done result");
        }
    }

    #[test]
    fn test_merge_operator_single_sided_inputs_union_all() {
        let (_pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, _pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, _pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Test UNION ALL with inputs coming from only one side at a time
        let mut merge_op = MergeOperator::new(
            10,
            UnionMode::All {
                left_table: "orders".to_string(),
                right_table: "archived_orders".to_string(),
            },
        );

        // First: only left side (orders) has data
        let mut left_delta1 = Delta::new();
        left_delta1.insert(100, vec![Value::Integer(1001)]);
        left_delta1.insert(101, vec![Value::Integer(1002)]);

        let right_delta1 = Delta::new(); // Empty right side

        let result1 = merge_op
            .commit(DeltaPair::new(left_delta1, right_delta1), &mut cursors)
            .unwrap();

        let first_rowids = if let IOResult::Done(ref merged1) = result1 {
            assert_eq!(merged1.len(), 2, "Should have 2 entries from left only");
            merged1
                .changes
                .iter()
                .map(|(row, _)| row.rowid)
                .collect::<Vec<_>>()
        } else {
            panic!("Expected Done result");
        };

        // Second: only right side (archived_orders) has data
        let left_delta2 = Delta::new(); // Empty left side

        let mut right_delta2 = Delta::new();
        right_delta2.insert(100, vec![Value::Integer(2001)]); // Same rowid as left, different table
        right_delta2.insert(102, vec![Value::Integer(2002)]);

        let result2 = merge_op
            .commit(DeltaPair::new(left_delta2, right_delta2), &mut cursors)
            .unwrap();
        let second_result_rowid_100 = if let IOResult::Done(ref merged2) = result2 {
            assert_eq!(merged2.len(), 2, "Should have 2 entries from right only");

            // Rowids should be different from the left side even though original rowid 100 is the same
            let second_rowids: Vec<i64> =
                merged2.changes.iter().map(|(row, _)| row.rowid).collect();
            for rowid in &second_rowids {
                assert!(
                    !first_rowids.contains(rowid),
                    "Right side rowids should be different from left side rowids"
                );
            }

            // Save rowid for archived_orders.100
            merged2
                .changes
                .iter()
                .find(|(row, _)| row.values == vec![Value::Integer(2001)])
                .map(|(row, _)| row.rowid)
                .unwrap()
        } else {
            panic!("Expected Done result");
        };

        // Third: left side again with same rowids as before
        let mut left_delta3 = Delta::new();
        left_delta3.insert(100, vec![Value::Integer(1003)]); // Same rowid 100 from orders
        left_delta3.insert(101, vec![Value::Integer(1004)]); // Same rowid 101 from orders

        let right_delta3 = Delta::new(); // Empty right side

        let result3 = merge_op
            .commit(DeltaPair::new(left_delta3, right_delta3), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged3) = result3 {
            assert_eq!(merged3.len(), 2, "Should have 2 entries from left");

            // Should get the same assigned rowids as the first operation
            let third_rowids: Vec<i64> = merged3.changes.iter().map(|(row, _)| row.rowid).collect();
            assert_eq!(
                first_rowids, third_rowids,
                "Same (table, rowid) pairs should get same assigned rowids"
            );
        } else {
            panic!("Expected Done result");
        }

        // Fourth: right side again with rowid 100
        let left_delta4 = Delta::new(); // Empty left side

        let mut right_delta4 = Delta::new();
        right_delta4.insert(100, vec![Value::Integer(2003)]); // Same rowid 100 from archived_orders

        let result4 = merge_op
            .commit(DeltaPair::new(left_delta4, right_delta4), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged4) = result4 {
            assert_eq!(merged4.len(), 1, "Should have 1 entry from right");

            // Should get same assigned rowid as second operation for archived_orders.100
            let fourth_rowid = merged4.changes[0].0.rowid;
            assert_eq!(
                fourth_rowid, second_result_rowid_100,
                "archived_orders rowid 100 should consistently map to same assigned rowid"
            );
        } else {
            panic!("Expected Done result");
        }
    }

    #[test]
    fn test_merge_operator_both_sides_empty() {
        let (_pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, _pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, _pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Test that both sides being empty works correctly
        let mut merge_op = MergeOperator::new(
            12,
            UnionMode::All {
                left_table: "t1".to_string(),
                right_table: "t2".to_string(),
            },
        );

        // First: insert some data to establish state
        let mut left_delta1 = Delta::new();
        left_delta1.insert(1, vec![Value::Integer(100)]);
        let mut right_delta1 = Delta::new();
        right_delta1.insert(1, vec![Value::Integer(200)]);

        let result1 = merge_op
            .commit(DeltaPair::new(left_delta1, right_delta1), &mut cursors)
            .unwrap();
        let original_t1_rowid = if let IOResult::Done(ref merged1) = result1 {
            assert_eq!(merged1.len(), 2, "Should have 2 entries initially");
            // Save the rowid for t1.rowid=1
            merged1
                .changes
                .iter()
                .find(|(row, _)| row.values == vec![Value::Integer(100)])
                .map(|(row, _)| row.rowid)
                .unwrap()
        } else {
            panic!("Expected Done result");
        };

        // Second: both sides empty - should produce empty output
        let empty_left = Delta::new();
        let empty_right = Delta::new();

        let result2 = merge_op
            .commit(DeltaPair::new(empty_left, empty_right), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged2) = result2 {
            assert_eq!(
                merged2.len(),
                0,
                "Both empty sides should produce empty output"
            );
        } else {
            panic!("Expected Done result");
        }

        // Third: add more data to verify state is still intact
        let mut left_delta3 = Delta::new();
        left_delta3.insert(1, vec![Value::Integer(101)]); // Same rowid as before
        let right_delta3 = Delta::new();

        let result3 = merge_op
            .commit(DeltaPair::new(left_delta3, right_delta3), &mut cursors)
            .unwrap();
        if let IOResult::Done(merged3) = result3 {
            assert_eq!(merged3.len(), 1, "Should have 1 entry");
            // Should reuse the same assigned rowid for t1.rowid=1
            let rowid = merged3.changes[0].0.rowid;
            assert_eq!(
                rowid, original_t1_rowid,
                "Should maintain consistent rowid mapping after empty operation"
            );
        } else {
            panic!("Expected Done result");
        }
    }

    #[test]
    fn test_aggregate_serialization_with_different_column_indices() {
        // Test that aggregate state serialization correctly preserves column indices
        // when multiple aggregates operate on different columns
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(None, pager.clone(), table_root_page_id, 5);
        let index_def = create_dbsp_state_index(index_root_page_id);
        let index_cursor =
            BTreeCursor::new_index(None, pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        // Create first operator with SUM(col1), MIN(col3) GROUP BY col0
        let mut agg1 = AggregateOperator::new(
            1,
            vec![0],
            vec![AggregateFunction::Sum(1), AggregateFunction::Min(3)],
            vec![
                "group".to_string(),
                "val1".to_string(),
                "val2".to_string(),
                "val3".to_string(),
            ],
        );

        // Add initial data
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Text("A".into()),
                Value::Integer(10),
                Value::Integer(100),
                Value::Integer(5),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Text("A".into()),
                Value::Integer(15),
                Value::Integer(200),
                Value::Integer(3),
            ],
        );

        let result1 = pager
            .io
            .block(|| agg1.commit((&delta).into(), &mut cursors))
            .unwrap();

        assert_eq!(result1.changes.len(), 1);
        let (row1, _) = &result1.changes[0];
        assert_eq!(row1.values[0], Value::Text("A".into()));
        assert_eq!(row1.values[1], Value::Integer(25)); // SUM(val1) = 10 + 15
        assert_eq!(row1.values[2], Value::Integer(3)); // MIN(val3) = min(5, 3)

        // Create operator with same ID but different column mappings: SUM(col3), MIN(col1)
        let mut agg2 = AggregateOperator::new(
            1, // Same operator_id
            vec![0],
            vec![AggregateFunction::Sum(3), AggregateFunction::Min(1)],
            vec![
                "group".to_string(),
                "val1".to_string(),
                "val2".to_string(),
                "val3".to_string(),
            ],
        );

        // Process new data
        let mut delta2 = Delta::new();
        delta2.insert(
            3,
            vec![
                Value::Text("A".into()),
                Value::Integer(20),
                Value::Integer(300),
                Value::Integer(4),
            ],
        );

        let result2 = pager
            .io
            .block(|| agg2.commit((&delta2).into(), &mut cursors))
            .unwrap();

        // Find the positive weight row for group A (the updated aggregate)
        let row2 = result2
            .changes
            .iter()
            .find(|(row, weight)| row.values[0] == Value::Text("A".into()) && *weight > 0)
            .expect("Should have a positive weight row for group A");
        let (row2, _) = row2;

        // Verify that column indices are preserved correctly in serialization
        // When agg2 processes the data with different column mappings:
        // - It reads the existing state which has SUM(col1)=25 and MIN(col3)=3
        // - For SUM(col3), there's no existing state, so it starts fresh: 4
        // - For MIN(col1), there's no existing state, so it starts fresh: 20
        assert_eq!(
            row2.values[1],
            Value::Integer(4),
            "SUM(col3) should be 4 (new data only)"
        );
        assert_eq!(
            row2.values[2],
            Value::Integer(20),
            "MIN(col1) should be 20 (new data only)"
        );
    }
}
