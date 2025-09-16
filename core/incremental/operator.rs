#![allow(dead_code)]
// Operator DAG for DBSP-style incremental computation
// Based on Feldera DBSP design but adapted for Turso's architecture

use crate::function::{AggFunc, Func};
use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::persistence::{MinMaxPersistState, ReadRecord, RecomputeMinMax, WriteRow};
use crate::schema::{Index, IndexColumn};
use crate::storage::btree::BTreeCursor;
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult, Text};
use crate::{
    return_and_restore_if_io, return_if_io, Connection, Database, Result, SymbolTable, Value,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Display};
use std::sync::{Arc, Mutex};
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::{As, Expr, Literal, Name, OneSelect, Operator, ResultColumn};

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
pub fn create_dbsp_state_index(root_page: usize) -> Index {
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
    }
}

/// Constants for aggregate type encoding in storage IDs (2 bits)
pub const AGG_TYPE_REGULAR: u8 = 0b00; // COUNT/SUM/AVG
pub const AGG_TYPE_MINMAX: u8 = 0b01; // MIN/MAX (BTree ordering gives both)
pub const AGG_TYPE_RESERVED1: u8 = 0b10; // Reserved for future use
pub const AGG_TYPE_RESERVED2: u8 = 0b11; // Reserved for future use

/// Generate a storage ID with column index and operation type encoding
/// Storage ID = (operator_id << 16) | (column_index << 2) | operation_type
/// Bit layout (64-bit integer):
/// - Bits 16-63 (48 bits): operator_id
/// - Bits 2-15 (14 bits): column_index (supports up to 16,384 columns)
/// - Bits 0-1 (2 bits): operation type (AGG_TYPE_REGULAR, AGG_TYPE_MINMAX, etc.)
pub fn generate_storage_id(operator_id: usize, column_index: usize, op_type: u8) -> i64 {
    assert!(op_type <= 3, "Invalid operation type");
    assert!(column_index < 16384, "Column index too large");

    ((operator_id as i64) << 16) | ((column_index as i64) << 2) | (op_type as i64)
}

// group_key_str -> (group_key, state)
type ComputedStates = HashMap<String, (Vec<Value>, AggregateState)>;
// group_key_str -> (column_name, value_as_hashable_row) -> accumulated_weight
pub type MinMaxDeltas = HashMap<String, HashMap<(String, HashableRow), isize>>;

#[derive(Debug)]
enum AggregateCommitState {
    Idle,
    Eval {
        eval_state: EvalState,
    },
    PersistDelta {
        delta: Delta,
        computed_states: ComputedStates,
        current_idx: usize,
        write_row: WriteRow,
        min_max_deltas: MinMaxDeltas,
    },
    PersistMinMax {
        delta: Delta,
        min_max_persist_state: MinMaxPersistState,
    },
    Done {
        delta: Delta,
    },
    Invalid,
}

// Aggregate-specific eval states
#[derive(Debug)]
pub enum AggregateEvalState {
    FetchKey {
        delta: Delta, // Keep original delta for merge operation
        current_idx: usize,
        groups_to_read: Vec<(String, Vec<Value>)>, // Changed to Vec for index-based access
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
    },
    FetchData {
        delta: Delta, // Keep original delta for merge operation
        current_idx: usize,
        groups_to_read: Vec<(String, Vec<Value>)>, // Changed to Vec for index-based access
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
        rowid: Option<i64>, // Rowid found by FetchKey (None if not found)
        read_record_state: Box<ReadRecord>,
    },
    RecomputeMinMax {
        delta: Delta,
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
        recompute_state: Box<RecomputeMinMax>,
    },
    Done {
        output: (Delta, ComputedStates),
    },
}

// Helper function to read the next row from the BTree for joins
fn read_next_join_row(
    storage_id: i64,
    join_key: &HashableRow,
    last_element_id: i64,
    cursors: &mut DbspStateCursors,
) -> Result<IOResult<Option<(i64, HashableRow, isize)>>> {
    // Build the index key: (storage_id, zset_id, element_id)
    // zset_id is the hash of the join key
    let zset_id = join_key.cached_hash() as i64;

    let index_key_values = vec![
        Value::Integer(storage_id),
        Value::Integer(zset_id),
        Value::Integer(last_element_id),
    ];

    let index_record = ImmutableRecord::from_values(&index_key_values, index_key_values.len());
    let seek_result = return_if_io!(cursors
        .index_cursor
        .seek(SeekKey::IndexKey(&index_record), SeekOp::GT));

    if !matches!(seek_result, SeekResult::Found) {
        return Ok(IOResult::Done(None));
    }

    // Check if we're still in the same (storage_id, zset_id) range
    let current_record = return_if_io!(cursors.index_cursor.record());

    // Extract all needed values from the record before dropping it
    let (found_storage_id, found_zset_id, element_id) = if let Some(rec) = current_record {
        let values = rec.get_values();

        // Index has 4 values: storage_id, zset_id, element_id, rowid (appended by WriteRow)
        if values.len() >= 3 {
            let found_storage_id = match &values[0].to_owned() {
                Value::Integer(id) => *id,
                _ => return Ok(IOResult::Done(None)),
            };
            let found_zset_id = match &values[1].to_owned() {
                Value::Integer(id) => *id,
                _ => return Ok(IOResult::Done(None)),
            };
            let element_id = match &values[2].to_owned() {
                Value::Integer(id) => *id,
                _ => {
                    return Ok(IOResult::Done(None));
                }
            };
            (found_storage_id, found_zset_id, element_id)
        } else {
            return Ok(IOResult::Done(None));
        }
    } else {
        return Ok(IOResult::Done(None));
    };

    // Now we can safely check if we're in the right range
    // If we've moved to a different storage_id or zset_id, we're done
    if found_storage_id != storage_id || found_zset_id != zset_id {
        return Ok(IOResult::Done(None));
    }

    // Now get the actual row from the table using the rowid from the index
    let rowid = return_if_io!(cursors.index_cursor.rowid());
    if let Some(rowid) = rowid {
        return_if_io!(cursors
            .table_cursor
            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));

        let table_record = return_if_io!(cursors.table_cursor.record());
        if let Some(rec) = table_record {
            let table_values = rec.get_values();
            // Table format: [storage_id, zset_id, element_id, value_blob, weight]
            if table_values.len() >= 5 {
                // Deserialize the row from the blob
                let value_at_3 = table_values[3].to_owned();
                let blob = match value_at_3 {
                    Value::Blob(ref b) => b,
                    _ => return Ok(IOResult::Done(None)),
                };

                // The blob contains the serialized HashableRow
                // For now, let's deserialize it simply
                let row = deserialize_hashable_row(blob)?;

                let weight = match &table_values[4].to_owned() {
                    Value::Integer(w) => *w as isize,
                    _ => return Ok(IOResult::Done(None)),
                };

                return Ok(IOResult::Done(Some((element_id, row, weight))));
            }
        }
    }
    Ok(IOResult::Done(None))
}

// Join-specific eval states
#[derive(Debug)]
pub enum JoinEvalState {
    ProcessDeltaJoin {
        deltas: DeltaPair,
        output: Delta,
    },
    ProcessLeftJoin {
        deltas: DeltaPair,
        output: Delta,
        current_idx: usize,
        last_row_scanned: i64,
    },
    ProcessRightJoin {
        deltas: DeltaPair,
        output: Delta,
        current_idx: usize,
        last_row_scanned: i64,
    },
    Done {
        output: Delta,
    },
}

impl JoinEvalState {
    fn combine_rows(
        left_row: &HashableRow,
        left_weight: i64,
        right_row: &HashableRow,
        right_weight: i64,
        output: &mut Delta,
    ) {
        // Combine the rows
        let mut combined_values = left_row.values.clone();
        combined_values.extend(right_row.values.clone());
        // Use hash of the combined values as rowid to ensure uniqueness
        let temp_row = HashableRow::new(0, combined_values.clone());
        let joined_rowid = temp_row.cached_hash() as i64;
        let joined_row = HashableRow::new(joined_rowid, combined_values);

        // Add to output with combined weight
        let combined_weight = left_weight * right_weight;
        output.changes.push((joined_row, combined_weight as isize));
    }

    fn process_join_state(
        &mut self,
        cursors: &mut DbspStateCursors,
        left_key_indices: &[usize],
        right_key_indices: &[usize],
        left_storage_id: i64,
        right_storage_id: i64,
    ) -> Result<IOResult<Delta>> {
        loop {
            match self {
                JoinEvalState::ProcessDeltaJoin { deltas, output } => {
                    // Move to ProcessLeftJoin
                    *self = JoinEvalState::ProcessLeftJoin {
                        deltas: std::mem::take(deltas),
                        output: std::mem::take(output),
                        current_idx: 0,
                        last_row_scanned: i64::MIN,
                    };
                }
                JoinEvalState::ProcessLeftJoin {
                    deltas,
                    output,
                    current_idx,
                    last_row_scanned,
                } => {
                    if *current_idx >= deltas.left.changes.len() {
                        *self = JoinEvalState::ProcessRightJoin {
                            deltas: std::mem::take(deltas),
                            output: std::mem::take(output),
                            current_idx: 0,
                            last_row_scanned: i64::MIN,
                        };
                    } else {
                        let (left_row, left_weight) = &deltas.left.changes[*current_idx];
                        // Extract join key using provided indices
                        let key_values: Vec<Value> = left_key_indices
                            .iter()
                            .map(|&idx| left_row.values.get(idx).cloned().unwrap_or(Value::Null))
                            .collect();
                        let left_key = HashableRow::new(0, key_values);

                        let next_row = return_if_io!(read_next_join_row(
                            right_storage_id,
                            &left_key,
                            *last_row_scanned,
                            cursors
                        ));
                        match next_row {
                            Some((element_id, right_row, right_weight)) => {
                                Self::combine_rows(
                                    left_row,
                                    (*left_weight) as i64,
                                    &right_row,
                                    right_weight as i64,
                                    output,
                                );
                                // Continue scanning with this left row
                                *self = JoinEvalState::ProcessLeftJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx,
                                    last_row_scanned: element_id,
                                };
                            }
                            None => {
                                // No more matches for this left row, move to next
                                *self = JoinEvalState::ProcessLeftJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx + 1,
                                    last_row_scanned: i64::MIN,
                                };
                            }
                        }
                    }
                }
                JoinEvalState::ProcessRightJoin {
                    deltas,
                    output,
                    current_idx,
                    last_row_scanned,
                } => {
                    if *current_idx >= deltas.right.changes.len() {
                        *self = JoinEvalState::Done {
                            output: std::mem::take(output),
                        };
                    } else {
                        let (right_row, right_weight) = &deltas.right.changes[*current_idx];
                        // Extract join key using provided indices
                        let key_values: Vec<Value> = right_key_indices
                            .iter()
                            .map(|&idx| right_row.values.get(idx).cloned().unwrap_or(Value::Null))
                            .collect();
                        let right_key = HashableRow::new(0, key_values);

                        let next_row = return_if_io!(read_next_join_row(
                            left_storage_id,
                            &right_key,
                            *last_row_scanned,
                            cursors
                        ));
                        match next_row {
                            Some((element_id, left_row, left_weight)) => {
                                Self::combine_rows(
                                    &left_row,
                                    left_weight as i64,
                                    right_row,
                                    (*right_weight) as i64,
                                    output,
                                );
                                // Continue scanning with this right row
                                *self = JoinEvalState::ProcessRightJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx,
                                    last_row_scanned: element_id,
                                };
                            }
                            None => {
                                // No more matches for this right row, move to next
                                *self = JoinEvalState::ProcessRightJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx + 1,
                                    last_row_scanned: i64::MIN,
                                };
                            }
                        }
                    }
                }
                JoinEvalState::Done { output } => {
                    return Ok(IOResult::Done(std::mem::take(output)));
                }
            }
        }
    }
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

    fn advance_aggregate(&mut self, groups_to_read: BTreeMap<String, Vec<Value>>) {
        let delta = match self {
            EvalState::Init { deltas } => std::mem::take(&mut deltas.left),
            _ => panic!("advance_aggregate() can only be called when in Init state, current state: {self:?}"),
        };

        let _ = std::mem::replace(
            self,
            EvalState::Aggregate(Box::new(AggregateEvalState::FetchKey {
                delta,
                current_idx: 0,
                groups_to_read: groups_to_read.into_iter().collect(), // Convert BTreeMap to Vec
                existing_groups: HashMap::new(),
                old_values: HashMap::new(),
            })),
        );
    }
}

impl AggregateEvalState {
    fn process_delta(
        &mut self,
        operator: &mut AggregateOperator,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        loop {
            match self {
                AggregateEvalState::FetchKey {
                    delta,
                    current_idx,
                    groups_to_read,
                    existing_groups,
                    old_values,
                } => {
                    if *current_idx >= groups_to_read.len() {
                        // All groups have been fetched, move to RecomputeMinMax
                        // Extract MIN/MAX deltas from the input delta
                        let min_max_deltas = operator.extract_min_max_deltas(delta);

                        let recompute_state = Box::new(RecomputeMinMax::new(
                            min_max_deltas,
                            existing_groups,
                            operator,
                        ));

                        *self = AggregateEvalState::RecomputeMinMax {
                            delta: std::mem::take(delta),
                            existing_groups: std::mem::take(existing_groups),
                            old_values: std::mem::take(old_values),
                            recompute_state,
                        };
                    } else {
                        // Get the current group to read
                        let (group_key_str, _group_key) = &groups_to_read[*current_idx];

                        // Build the key for the index: (operator_id, zset_id, element_id)
                        // For regular aggregates, use column_index=0 and AGG_TYPE_REGULAR
                        let operator_storage_id =
                            generate_storage_id(operator.operator_id, 0, AGG_TYPE_REGULAR);
                        let zset_id = operator.generate_group_rowid(group_key_str);
                        let element_id = 0i64; // Always 0 for aggregators

                        // Create index key values
                        let index_key_values = vec![
                            Value::Integer(operator_storage_id),
                            Value::Integer(zset_id),
                            Value::Integer(element_id),
                        ];

                        // Create an immutable record for the index key
                        let index_record =
                            ImmutableRecord::from_values(&index_key_values, index_key_values.len());

                        // Seek in the index to find if this row exists
                        let seek_result = return_if_io!(cursors.index_cursor.seek(
                            SeekKey::IndexKey(&index_record),
                            SeekOp::GE { eq_only: true }
                        ));

                        let rowid = if matches!(seek_result, SeekResult::Found) {
                            // Found in index, get the table rowid
                            // The btree code handles extracting the rowid from the index record for has_rowid indexes
                            return_if_io!(cursors.index_cursor.rowid())
                        } else {
                            // Not found in index, no existing state
                            None
                        };

                        // Always transition to FetchData
                        let taken_existing = std::mem::take(existing_groups);
                        let taken_old_values = std::mem::take(old_values);
                        let next_state = AggregateEvalState::FetchData {
                            delta: std::mem::take(delta),
                            current_idx: *current_idx,
                            groups_to_read: std::mem::take(groups_to_read),
                            existing_groups: taken_existing,
                            old_values: taken_old_values,
                            rowid,
                            read_record_state: Box::new(ReadRecord::new()),
                        };
                        *self = next_state;
                    }
                }
                AggregateEvalState::FetchData {
                    delta,
                    current_idx,
                    groups_to_read,
                    existing_groups,
                    old_values,
                    rowid,
                    read_record_state,
                } => {
                    // Get the current group to read
                    let (group_key_str, group_key) = &groups_to_read[*current_idx];

                    // Only try to read if we have a rowid
                    if let Some(rowid) = rowid {
                        let key = SeekKey::TableRowId(*rowid);
                        let state = return_if_io!(read_record_state.read_record(
                            key,
                            &operator.aggregates,
                            &mut cursors.table_cursor
                        ));
                        // Process the fetched state
                        if let Some(state) = state {
                            let mut old_row = group_key.clone();
                            old_row.extend(state.to_values(&operator.aggregates));
                            old_values.insert(group_key_str.clone(), old_row);
                            existing_groups.insert(group_key_str.clone(), state.clone());
                        }
                    } else {
                        // No rowid for this group, skipping read
                    }
                    // If no rowid, there's no existing state for this group

                    // Move to next group
                    let next_idx = *current_idx + 1;
                    let taken_existing = std::mem::take(existing_groups);
                    let taken_old_values = std::mem::take(old_values);
                    let next_state = AggregateEvalState::FetchKey {
                        delta: std::mem::take(delta),
                        current_idx: next_idx,
                        groups_to_read: std::mem::take(groups_to_read),
                        existing_groups: taken_existing,
                        old_values: taken_old_values,
                    };
                    *self = next_state;
                }
                AggregateEvalState::RecomputeMinMax {
                    delta,
                    existing_groups,
                    old_values,
                    recompute_state,
                } => {
                    if operator.has_min_max() {
                        // Process MIN/MAX recomputation - this will update existing_groups with correct MIN/MAX
                        return_if_io!(recompute_state.process(existing_groups, operator, cursors));
                    }

                    // Now compute final output with updated MIN/MAX values
                    let (output_delta, computed_states) =
                        operator.merge_delta_with_existing(delta, existing_groups, old_values);

                    *self = AggregateEvalState::Done {
                        output: (output_delta, computed_states),
                    };
                }
                AggregateEvalState::Done { output } => {
                    return Ok(IOResult::Done(output.clone()));
                }
            }
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

#[cfg(test)]
mod dbsp_types_tests {
    use super::*;

    #[test]
    fn test_hashable_row_delta_operations() {
        let mut delta = Delta::new();

        // Test INSERT
        delta.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        assert_eq!(delta.len(), 1);

        // Test UPDATE (DELETE + INSERT) - order matters!
        delta.delete(1, vec![Value::Integer(1), Value::Integer(100)]);
        delta.insert(1, vec![Value::Integer(1), Value::Integer(200)]);
        assert_eq!(delta.len(), 3); // Should have 3 operations before consolidation

        // Verify order is preserved
        let ops: Vec<_> = delta.changes.iter().collect();
        assert_eq!(ops[0].1, 1); // First insert
        assert_eq!(ops[1].1, -1); // Delete
        assert_eq!(ops[2].1, 1); // Second insert

        // Test consolidation
        delta.consolidate();
        // After consolidation, the first insert and delete should cancel out
        // leaving only the second insert
        assert_eq!(delta.len(), 1);

        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 1);
        assert_eq!(
            final_row.0.values,
            vec![Value::Integer(1), Value::Integer(200)]
        );
        assert_eq!(final_row.1, 1);
    }

    #[test]
    fn test_duplicate_row_consolidation() {
        let mut delta = Delta::new();

        // Insert same row twice
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);

        assert_eq!(delta.len(), 2);

        delta.consolidate();
        assert_eq!(delta.len(), 1);

        // Weight should be 2 (sum of both inserts)
        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 2);
        assert_eq!(final_row.1, 2);
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

#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Column = value
    Equals { column: String, value: Value },
    /// Column != value
    NotEquals { column: String, value: Value },
    /// Column > value
    GreaterThan { column: String, value: Value },
    /// Column >= value
    GreaterThanOrEqual { column: String, value: Value },
    /// Column < value
    LessThan { column: String, value: Value },
    /// Column <= value
    LessThanOrEqual { column: String, value: Value },
    /// Logical AND of two predicates
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    /// Logical OR of two predicates
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    /// No predicate (accept all rows)
    None,
}

impl FilterPredicate {
    /// Parse a SQL AST expression into a FilterPredicate
    /// This centralizes all SQL-to-predicate parsing logic
    pub fn from_sql_expr(expr: &turso_parser::ast::Expr) -> crate::Result<Self> {
        let Expr::Binary(lhs, op, rhs) = expr else {
            return Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: not a binary expression"
                    .to_string(),
            ));
        };

        // Handle AND/OR logical operators
        match op {
            Operator::And => {
                let left = Self::from_sql_expr(lhs)?;
                let right = Self::from_sql_expr(rhs)?;
                return Ok(FilterPredicate::And(Box::new(left), Box::new(right)));
            }
            Operator::Or => {
                let left = Self::from_sql_expr(lhs)?;
                let right = Self::from_sql_expr(rhs)?;
                return Ok(FilterPredicate::Or(Box::new(left), Box::new(right)));
            }
            _ => {}
        }

        // Handle comparison operators
        let Expr::Id(column_name) = &**lhs else {
            return Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: left-hand-side is not a column reference".to_string(),
            ));
        };

        let column = column_name.as_str().to_string();

        // Parse the right-hand side value
        let value = match &**rhs {
            Expr::Literal(Literal::String(s)) => {
                // Strip quotes from string literals
                let cleaned = s.trim_matches('\'').trim_matches('"');
                Value::Text(Text::new(cleaned))
            }
            Expr::Literal(Literal::Numeric(n)) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    return Err(crate::LimboError::ParseError(
                        "Unsupported WHERE clause for incremental views: right-hand-side is not a numeric literal".to_string(),
                    ));
                }
            }
            Expr::Literal(Literal::Null) => Value::Null,
            Expr::Literal(Literal::Blob(_)) => {
                // Blob comparison not yet supported
                return Err(crate::LimboError::ParseError(
                    "Unsupported WHERE clause for incremental views: comparison with blob literals is not supported".to_string(),
                ));
            }
            other => {
                // Complex expressions not yet supported
                return Err(crate::LimboError::ParseError(
                    format!("Unsupported WHERE clause for incremental views: comparison with {other:?} is not supported"),
                ));
            }
        };

        // Create the appropriate predicate based on operator
        match op {
            Operator::Equals => Ok(FilterPredicate::Equals { column, value }),
            Operator::NotEquals => Ok(FilterPredicate::NotEquals { column, value }),
            Operator::Greater => Ok(FilterPredicate::GreaterThan { column, value }),
            Operator::GreaterEquals => Ok(FilterPredicate::GreaterThanOrEqual { column, value }),
            Operator::Less => Ok(FilterPredicate::LessThan { column, value }),
            Operator::LessEquals => Ok(FilterPredicate::LessThanOrEqual { column, value }),
            other => Err(crate::LimboError::ParseError(
                format!("Unsupported WHERE clause for incremental views: comparison operator {other:?} is not supported"),
            )),
        }
    }

    /// Parse a WHERE clause from a SELECT statement
    pub fn from_select(select: &turso_parser::ast::Select) -> crate::Result<Self> {
        if let OneSelect::Select {
            ref where_clause, ..
        } = select.body.select
        {
            if let Some(where_clause) = where_clause {
                Self::from_sql_expr(where_clause)
            } else {
                Ok(FilterPredicate::None)
            }
        } else {
            Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: not a single SELECT statement"
                    .to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProjectColumn {
    /// The original SQL expression (for debugging/fallback)
    pub expr: turso_parser::ast::Expr,
    /// Optional alias for the column
    pub alias: Option<String>,
    /// Compiled expression (handles both trivial columns and complex expressions)
    pub compiled: CompiledExpression,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT(*)"),
            AggregateFunction::Sum(col) => write!(f, "SUM({col})"),
            AggregateFunction::Avg(col) => write!(f, "AVG({col})"),
            AggregateFunction::Min(col) => write!(f, "MIN({col})"),
            AggregateFunction::Max(col) => write!(f, "MAX({col})"),
        }
    }
}

impl AggregateFunction {
    /// Get the default output column name for this aggregate function
    #[inline]
    pub fn default_output_name(&self) -> String {
        self.to_string()
    }

    /// Create an AggregateFunction from a SQL function and its arguments
    /// Returns None if the function is not a supported aggregate
    pub fn from_sql_function(
        func: &crate::function::Func,
        input_column: Option<String>,
    ) -> Option<Self> {
        match func {
            Func::Agg(agg_func) => {
                match agg_func {
                    AggFunc::Count | AggFunc::Count0 => Some(AggregateFunction::Count),
                    AggFunc::Sum => input_column.map(AggregateFunction::Sum),
                    AggFunc::Avg => input_column.map(AggregateFunction::Avg),
                    AggFunc::Min => input_column.map(AggregateFunction::Min),
                    AggFunc::Max => input_column.map(AggregateFunction::Max),
                    _ => None, // Other aggregate functions not yet supported in DBSP
                }
            }
            _ => None, // Not an aggregate function
        }
    }
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

/// Input operator - passes through input data unchanged
/// This operator is used for input nodes in the circuit to provide a uniform interface
#[derive(Debug)]
pub struct InputOperator {
    name: String,
}

impl InputOperator {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl IncrementalOperator for InputOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        match state {
            EvalState::Init { deltas } => {
                // Input operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "InputOperator expects right_delta to be empty"
                );
                let output = std::mem::take(&mut deltas.left);
                *state = EvalState::Done;
                Ok(IOResult::Done(output))
            }
            _ => unreachable!(
                "InputOperator doesn't execute the state machine. Should be in Init state"
            ),
        }
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Input operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "InputOperator expects right delta to be empty in commit"
        );
        // Input operator passes through the delta unchanged during commit
        Ok(IOResult::Done(deltas.left))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {
        // Input operator doesn't need tracking
    }
}

/// Filter operator - filters rows based on predicate
#[derive(Debug)]
pub struct FilterOperator {
    predicate: FilterPredicate,
    column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
}

impl FilterOperator {
    pub fn new(predicate: FilterPredicate, column_names: Vec<String>) -> Self {
        Self {
            predicate,
            column_names,
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
            FilterPredicate::Equals { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        return v == value;
                    }
                }
                false
            }
            FilterPredicate::NotEquals { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        return v != value;
                    }
                }
                false
            }
            FilterPredicate::GreaterThan { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        // Compare based on value types
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a > b,
                            (Value::Float(a), Value::Float(b)) => return a > b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() > b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::GreaterThanOrEqual { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a >= b,
                            (Value::Float(a), Value::Float(b)) => return a >= b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() >= b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::LessThan { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a < b,
                            (Value::Float(a), Value::Float(b)) => return a < b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() < b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::LessThanOrEqual { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a <= b,
                            (Value::Float(a), Value::Float(b)) => return a <= b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() <= b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::And(left, right) => {
                // Temporarily create sub-filters to evaluate
                let left_filter = FilterOperator::new((**left).clone(), self.column_names.clone());
                let right_filter =
                    FilterOperator::new((**right).clone(), self.column_names.clone());
                left_filter.evaluate_predicate(values) && right_filter.evaluate_predicate(values)
            }
            FilterPredicate::Or(left, right) => {
                let left_filter = FilterOperator::new((**left).clone(), self.column_names.clone());
                let right_filter =
                    FilterOperator::new((**right).clone(), self.column_names.clone());
                left_filter.evaluate_predicate(values) || right_filter.evaluate_predicate(values)
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

/// Project operator - selects/transforms columns
#[derive(Clone)]
pub struct ProjectOperator {
    columns: Vec<ProjectColumn>,
    input_column_names: Vec<String>,
    output_column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
    // Internal in-memory connection for expression evaluation
    // Programs are very dependent on having a connection, so give it one.
    //
    // We could in theory pass the current connection, but there are a host of problems with that.
    // For example: during a write transaction, where views are usually updated, we have autocommit
    // on. When the program we are executing calls Halt, it will try to commit the current
    // transaction, which is absolutely incorrect.
    //
    // There are other ways to solve this, but a read-only connection to an empty in-memory
    // database gives us the closest environment we need to execute expressions.
    internal_conn: Arc<Connection>,
}

impl std::fmt::Debug for ProjectOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectOperator")
            .field("columns", &self.columns)
            .field("input_column_names", &self.input_column_names)
            .field("output_column_names", &self.output_column_names)
            .field("tracker", &self.tracker)
            .finish_non_exhaustive()
    }
}

impl ProjectOperator {
    /// Create a new ProjectOperator from a SELECT statement, extracting projection columns
    pub fn from_select(
        select: &turso_parser::ast::Select,
        input_column_names: Vec<String>,
        schema: &crate::schema::Schema,
    ) -> crate::Result<Self> {
        // Set up internal connection for expression evaluation
        let io = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(
            io, ":memory:", false, // no MVCC needed for expression evaluation
            false, // no indexes needed
        )?;
        let internal_conn = db.connect()?;
        // Set to read-only mode and disable auto-commit since we're only evaluating expressions
        internal_conn.query_only.set(true);
        internal_conn.auto_commit.set(false);

        let temp_syms = SymbolTable::new();

        // Extract columns from SELECT statement
        let columns = if let OneSelect::Select {
            columns: ref select_columns,
            ..
        } = &select.body.select
        {
            let mut columns = Vec::new();
            for result_col in select_columns {
                match result_col {
                    ResultColumn::Expr(expr, alias) => {
                        let alias_str = if let Some(As::As(alias_name)) = alias {
                            Some(alias_name.as_str().to_string())
                        } else {
                            None
                        };
                        // Try to compile the expression (handles both columns and complex expressions)
                        let compiled = CompiledExpression::compile(
                            expr,
                            &input_column_names,
                            schema,
                            &temp_syms,
                            internal_conn.clone(),
                        )?;
                        columns.push(ProjectColumn {
                            expr: (**expr).clone(),
                            alias: alias_str,
                            compiled,
                        });
                    }
                    ResultColumn::Star => {
                        // Select all columns - create trivial column references
                        for name in &input_column_names {
                            // Create an Id expression for the column
                            let expr = Expr::Id(Name::Ident(name.clone()));
                            let compiled = CompiledExpression::compile(
                                &expr,
                                &input_column_names,
                                schema,
                                &temp_syms,
                                internal_conn.clone(),
                            )?;
                            columns.push(ProjectColumn {
                                expr,
                                alias: None,
                                compiled,
                            });
                        }
                    }
                    x => {
                        return Err(crate::LimboError::ParseError(format!(
                            "Unsupported {x:?} clause when compiling project operator",
                        )));
                    }
                }
            }

            if columns.is_empty() {
                return Err(crate::LimboError::ParseError(
                    "No columns found when compiling project operator".to_string(),
                ));
            }
            columns
        } else {
            return Err(crate::LimboError::ParseError(
                "Expression is not a valid SELECT expression".to_string(),
            ));
        };

        // Generate output column names based on aliases or expressions
        let output_column_names = columns
            .iter()
            .map(|c| {
                c.alias.clone().unwrap_or_else(|| match &c.expr {
                    Expr::Id(name) => name.as_str().to_string(),
                    Expr::Qualified(table, column) => {
                        format!("{}.{}", table.as_str(), column.as_str())
                    }
                    Expr::DoublyQualified(db, table, column) => {
                        format!("{}.{}.{}", db.as_str(), table.as_str(), column.as_str())
                    }
                    _ => c.expr.to_string(),
                })
            })
            .collect();

        Ok(Self {
            columns,
            input_column_names,
            output_column_names,
            tracker: None,
            internal_conn,
        })
    }

    /// Create a ProjectOperator from pre-compiled expressions
    pub fn from_compiled(
        compiled_exprs: Vec<CompiledExpression>,
        aliases: Vec<Option<String>>,
        input_column_names: Vec<String>,
        output_column_names: Vec<String>,
    ) -> crate::Result<Self> {
        // Set up internal connection for expression evaluation
        let io = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(
            io, ":memory:", false, // no MVCC needed for expression evaluation
            false, // no indexes needed
        )?;
        let internal_conn = db.connect()?;
        // Set to read-only mode and disable auto-commit since we're only evaluating expressions
        internal_conn.query_only.set(true);
        internal_conn.auto_commit.set(false);

        // Create ProjectColumn structs from compiled expressions
        let columns: Vec<ProjectColumn> = compiled_exprs
            .into_iter()
            .zip(aliases)
            .map(|(compiled, alias)| ProjectColumn {
                // Create a placeholder AST expression since we already have the compiled version
                expr: turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Null),
                alias,
                compiled,
            })
            .collect();

        Ok(Self {
            columns,
            input_column_names,
            output_column_names,
            tracker: None,
            internal_conn,
        })
    }

    /// Get the columns for this projection
    pub fn columns(&self) -> &[ProjectColumn] {
        &self.columns
    }

    fn project_values(&self, values: &[Value]) -> Vec<Value> {
        let mut output = Vec::new();

        for col in &self.columns {
            // Use the internal connection's pager for expression evaluation
            let internal_pager = self.internal_conn.pager.borrow().clone();

            // Execute the compiled expression (handles both columns and complex expressions)
            let result = col
                .compiled
                .execute(values, internal_pager)
                .expect("Failed to execute compiled expression for the Project operator");
            output.push(result);
        }

        output
    }

    fn evaluate_expression(&self, expr: &turso_parser::ast::Expr, values: &[Value]) -> Value {
        match expr {
            Expr::Id(name) => {
                if let Some(idx) = self
                    .input_column_names
                    .iter()
                    .position(|c| c == name.as_str())
                {
                    if let Some(v) = values.get(idx) {
                        return v.clone();
                    }
                }
                Value::Null
            }
            Expr::Literal(lit) => {
                match lit {
                    Literal::Numeric(n) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Value::Integer(i)
                        } else if let Ok(f) = n.parse::<f64>() {
                            Value::Float(f)
                        } else {
                            Value::Null
                        }
                    }
                    Literal::String(s) => {
                        let cleaned = s.trim_matches('\'').trim_matches('"');
                        Value::Text(Text::new(cleaned))
                    }
                    Literal::Null => Value::Null,
                    Literal::Blob(_)
                    | Literal::Keyword(_)
                    | Literal::CurrentDate
                    | Literal::CurrentTime
                    | Literal::CurrentTimestamp => Value::Null, // Not supported yet
                }
            }
            Expr::Binary(left, op, right) => {
                let left_val = self.evaluate_expression(left, values);
                let right_val = self.evaluate_expression(right, values);

                match op {
                    Operator::Add => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 + b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a + *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Subtract => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 - b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a - *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Multiply => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 * b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a * *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Divide => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if *b != 0 {
                                Value::Integer(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Float(a), Value::Float(b)) => {
                            if *b != 0.0 {
                                Value::Float(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Integer(a), Value::Float(b)) => {
                            if *b != 0.0 {
                                Value::Float(*a as f64 / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Float(a), Value::Integer(b)) => {
                            if *b != 0 {
                                Value::Float(a / *b as f64)
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    },
                    _ => Value::Null, // Other operators not supported yet
                }
            }
            Expr::FunctionCall { name, args, .. } => {
                let name_bytes = name.as_str().as_bytes();
                match_ignore_ascii_case!(match name_bytes {
                    b"hex" => {
                        if args.len() == 1 {
                            let arg_val = self.evaluate_expression(&args[0], values);
                            match arg_val {
                                Value::Integer(i) => Value::Text(Text::new(&format!("{i:X}"))),
                                _ => Value::Null,
                            }
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null, // Other functions not supported yet
                })
            }
            Expr::Parenthesized(inner) => {
                assert!(
                    inner.len() <= 1,
                    "Parenthesized expressions with multiple elements are not supported"
                );
                if !inner.is_empty() {
                    self.evaluate_expression(&inner[0], values)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null, // Other expression types not supported yet
        }
    }
}

impl IncrementalOperator for ProjectOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { deltas } => {
                // Project operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "ProjectOperator expects right_delta to be empty"
                );
                std::mem::take(&mut deltas.left)
            }
            _ => unreachable!(
                "ProjectOperator doesn't execute the state machine. Should be in Init state"
            ),
        };

        let mut output_delta = Delta::new();

        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }

            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, weight));
        }

        *state = EvalState::Done;
        Ok(IOResult::Done(output_delta))
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Project operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "ProjectOperator expects right delta to be empty in commit"
        );

        let mut output_delta = Delta::new();

        // Commit the delta to our internal state and build output
        for (row, weight) in &deltas.left.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }
            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, *weight));
        }

        Ok(crate::types::IOResult::Done(output_delta))
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

/// Aggregate operator - performs incremental aggregation with GROUP BY
/// Maintains running totals/counts that are updated incrementally
///
/// Information about a column that has MIN/MAX aggregations
#[derive(Debug, Clone)]
pub struct AggColumnInfo {
    /// Index used for storage key generation
    pub index: usize,
    /// Whether this column has a MIN aggregate
    pub has_min: bool,
    /// Whether this column has a MAX aggregate
    pub has_max: bool,
}

/// Note that the AggregateOperator essentially implements a ZSet, even
/// though the ZSet structure is never used explicitly. The on-disk btree
/// plays the role of the set!
#[derive(Debug)]
pub struct AggregateOperator {
    // Unique operator ID for indexing in persistent storage
    pub operator_id: usize,
    // GROUP BY columns
    group_by: Vec<String>,
    // Aggregate functions to compute (including MIN/MAX)
    pub aggregates: Vec<AggregateFunction>,
    // Column names from input
    pub input_column_names: Vec<String>,
    // Map from column name to aggregate info for quick lookup
    pub column_min_max: HashMap<String, AggColumnInfo>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    // State machine for commit operation
    commit_state: AggregateCommitState,
}

/// State for a single group's aggregates
#[derive(Debug, Clone, Default)]
pub struct AggregateState {
    // For COUNT: just the count
    count: i64,
    // For SUM: column_name -> sum value
    sums: HashMap<String, f64>,
    // For AVG: column_name -> (sum, count) for computing average
    avgs: HashMap<String, (f64, i64)>,
    // For MIN: column_name -> minimum value
    pub mins: HashMap<String, Value>,
    // For MAX: column_name -> maximum value
    pub maxs: HashMap<String, Value>,
}

/// Serialize a Value using SQLite's serial type format
/// This is used for MIN/MAX values that need to be stored in a compact, sortable format
pub fn serialize_value(value: &Value, blob: &mut Vec<u8>) {
    let serial_type = crate::types::SerialType::from(value);
    let serial_type_u64: u64 = serial_type.into();
    crate::storage::sqlite3_ondisk::write_varint_to_vec(serial_type_u64, blob);
    value.serialize_serial(blob);
}

/// Deserialize a Value using SQLite's serial type format
/// Returns the deserialized value and the number of bytes consumed
pub fn deserialize_value(blob: &[u8]) -> Option<(Value, usize)> {
    let mut cursor = 0;

    // Read the serial type
    let (serial_type, varint_size) = crate::storage::sqlite3_ondisk::read_varint(blob).ok()?;
    cursor += varint_size;

    let serial_type_obj = crate::types::SerialType::try_from(serial_type).ok()?;
    let expected_size = serial_type_obj.size();

    // Read the value
    let (value, actual_size) =
        crate::storage::sqlite3_ondisk::read_value(&blob[cursor..], serial_type_obj).ok()?;

    // Verify that the actual size matches what we expected from the serial type
    if actual_size != expected_size {
        return None; // Data corruption - size mismatch
    }

    cursor += actual_size;

    // Convert RefValue to Value
    Some((value.to_owned(), cursor))
}

impl AggregateState {
    pub fn new() -> Self {
        Self::default()
    }

    // Serialize the aggregate state to a binary blob including group key values
    // The reason we serialize it like this, instead of just writing the actual values, is that
    // The same table may have different aggregators in the circuit. They will all have different
    // columns.
    fn to_blob(&self, aggregates: &[AggregateFunction], group_key: &[Value]) -> Vec<u8> {
        let mut blob = Vec::new();

        // Write version byte for future compatibility
        blob.push(1u8);

        // Write number of group key values
        blob.extend_from_slice(&(group_key.len() as u32).to_le_bytes());

        // Write each group key value
        for value in group_key {
            // Write value type tag
            match value {
                Value::Null => blob.push(0u8),
                Value::Integer(i) => {
                    blob.push(1u8);
                    blob.extend_from_slice(&i.to_le_bytes());
                }
                Value::Float(f) => {
                    blob.push(2u8);
                    blob.extend_from_slice(&f.to_le_bytes());
                }
                Value::Text(s) => {
                    blob.push(3u8);
                    let text_str = s.as_str();
                    let bytes = text_str.as_bytes();
                    blob.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    blob.extend_from_slice(bytes);
                }
                Value::Blob(b) => {
                    blob.push(4u8);
                    blob.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    blob.extend_from_slice(b);
                }
            }
        }

        // Write count as 8 bytes (little-endian)
        blob.extend_from_slice(&self.count.to_le_bytes());

        // Write each aggregate's state
        for agg in aggregates {
            match agg {
                AggregateFunction::Sum(col_name) => {
                    let sum = self.sums.get(col_name).copied().unwrap_or(0.0);
                    blob.extend_from_slice(&sum.to_le_bytes());
                }
                AggregateFunction::Avg(col_name) => {
                    let (sum, count) = self.avgs.get(col_name).copied().unwrap_or((0.0, 0));
                    blob.extend_from_slice(&sum.to_le_bytes());
                    blob.extend_from_slice(&count.to_le_bytes());
                }
                AggregateFunction::Count => {
                    // Count is already written above
                }
                AggregateFunction::Min(col_name) => {
                    // Write whether we have a MIN value (1 byte)
                    if let Some(min_val) = self.mins.get(col_name) {
                        blob.push(1u8); // Has value
                        serialize_value(min_val, &mut blob);
                    } else {
                        blob.push(0u8); // No value
                    }
                }
                AggregateFunction::Max(col_name) => {
                    // Write whether we have a MAX value (1 byte)
                    if let Some(max_val) = self.maxs.get(col_name) {
                        blob.push(1u8); // Has value
                        serialize_value(max_val, &mut blob);
                    } else {
                        blob.push(0u8); // No value
                    }
                }
            }
        }

        blob
    }

    /// Deserialize aggregate state from a binary blob
    /// Returns the aggregate state and the group key values
    pub fn from_blob(blob: &[u8], aggregates: &[AggregateFunction]) -> Option<(Self, Vec<Value>)> {
        let mut cursor = 0;

        // Check version byte
        if blob.get(cursor) != Some(&1u8) {
            return None;
        }
        cursor += 1;

        // Read number of group key values
        let num_group_keys =
            u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
        cursor += 4;

        // Read group key values
        let mut group_key = Vec::new();
        for _ in 0..num_group_keys {
            let value_type = *blob.get(cursor)?;
            cursor += 1;

            let value = match value_type {
                0 => Value::Null,
                1 => {
                    let i = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    Value::Integer(i)
                }
                2 => {
                    let f = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    Value::Float(f)
                }
                3 => {
                    let len =
                        u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                    cursor += 4;
                    let bytes = blob.get(cursor..cursor + len)?;
                    cursor += len;
                    let text_str = std::str::from_utf8(bytes).ok()?;
                    Value::Text(text_str.to_string().into())
                }
                4 => {
                    let len =
                        u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                    cursor += 4;
                    let bytes = blob.get(cursor..cursor + len)?;
                    cursor += len;
                    Value::Blob(bytes.to_vec())
                }
                _ => return None,
            };
            group_key.push(value);
        }

        // Read count
        let count = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let mut state = Self::new();
        state.count = count;

        // Read each aggregate's state
        for agg in aggregates {
            match agg {
                AggregateFunction::Sum(col_name) => {
                    let sum = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    state.sums.insert(col_name.clone(), sum);
                }
                AggregateFunction::Avg(col_name) => {
                    let sum = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    let count = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    state.avgs.insert(col_name.clone(), (sum, count));
                }
                AggregateFunction::Count => {
                    // Count was already read above
                }
                AggregateFunction::Min(col_name) => {
                    // Read whether we have a MIN value
                    let has_value = *blob.get(cursor)?;
                    cursor += 1;

                    if has_value == 1 {
                        let (min_value, bytes_consumed) = deserialize_value(&blob[cursor..])?;
                        cursor += bytes_consumed;
                        state.mins.insert(col_name.clone(), min_value);
                    }
                }
                AggregateFunction::Max(col_name) => {
                    // Read whether we have a MAX value
                    let has_value = *blob.get(cursor)?;
                    cursor += 1;

                    if has_value == 1 {
                        let (max_value, bytes_consumed) = deserialize_value(&blob[cursor..])?;
                        cursor += bytes_consumed;
                        state.maxs.insert(col_name.clone(), max_value);
                    }
                }
            }
        }

        Some((state, group_key))
    }

    /// Apply a delta to this aggregate state
    fn apply_delta(
        &mut self,
        values: &[Value],
        weight: isize,
        aggregates: &[AggregateFunction],
        column_names: &[String],
    ) {
        // Update COUNT
        self.count += weight as i64;

        // Update other aggregates
        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    // Already handled above
                }
                AggregateFunction::Sum(col_name) => {
                    if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                        if let Some(val) = values.get(idx) {
                            let num_val = match val {
                                Value::Integer(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => 0.0,
                            };
                            *self.sums.entry(col_name.clone()).or_insert(0.0) +=
                                num_val * weight as f64;
                        }
                    }
                }
                AggregateFunction::Avg(col_name) => {
                    if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                        if let Some(val) = values.get(idx) {
                            let num_val = match val {
                                Value::Integer(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => 0.0,
                            };
                            let (sum, count) =
                                self.avgs.entry(col_name.clone()).or_insert((0.0, 0));
                            *sum += num_val * weight as f64;
                            *count += weight as i64;
                        }
                    }
                }
                AggregateFunction::Min(_col_name) | AggregateFunction::Max(_col_name) => {
                    // MIN/MAX cannot be handled incrementally in apply_delta because:
                    //
                    // 1. For insertions: We can't just keep the minimum/maximum value.
                    //    We need to track ALL values to handle future deletions correctly.
                    //
                    // 2. For deletions (retractions): If we delete the current MIN/MAX,
                    //    we need to find the next best value, which requires knowing all
                    //    other values in the group.
                    //
                    // Example: Consider MIN(price) with values [10, 20, 30]
                    // - Current MIN = 10
                    // - Delete 10 (weight = -1)
                    // - New MIN should be 20, but we can't determine this without
                    //   having tracked all values [20, 30]
                    //
                    // Therefore, MIN/MAX processing is handled separately:
                    // - All input values are persisted to the index via persist_min_max()
                    // - When aggregates have MIN/MAX, we unconditionally transition to
                    //   the RecomputeMinMax state machine (see EvalState::RecomputeMinMax)
                    // - RecomputeMinMax checks if the current MIN/MAX was deleted, and if so,
                    //   scans the index to find the new MIN/MAX from remaining values
                    //
                    // This ensures correctness for incremental computation at the cost of
                    // additional I/O for MIN/MAX operations.
                }
            }
        }
    }

    /// Convert aggregate state to output values
    pub fn to_values(&self, aggregates: &[AggregateFunction]) -> Vec<Value> {
        let mut result = Vec::new();

        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    result.push(Value::Integer(self.count));
                }
                AggregateFunction::Sum(col_name) => {
                    let sum = self.sums.get(col_name).copied().unwrap_or(0.0);
                    // Return as integer if it's a whole number, otherwise as float
                    if sum.fract() == 0.0 {
                        result.push(Value::Integer(sum as i64));
                    } else {
                        result.push(Value::Float(sum));
                    }
                }
                AggregateFunction::Avg(col_name) => {
                    if let Some((sum, count)) = self.avgs.get(col_name) {
                        if *count > 0 {
                            result.push(Value::Float(sum / *count as f64));
                        } else {
                            result.push(Value::Null);
                        }
                    } else {
                        result.push(Value::Null);
                    }
                }
                AggregateFunction::Min(col_name) => {
                    // Return the MIN value from our state
                    result.push(self.mins.get(col_name).cloned().unwrap_or(Value::Null));
                }
                AggregateFunction::Max(col_name) => {
                    // Return the MAX value from our state
                    result.push(self.maxs.get(col_name).cloned().unwrap_or(Value::Null));
                }
            }
        }

        result
    }
}

impl AggregateOperator {
    pub fn new(
        operator_id: usize,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        input_column_names: Vec<String>,
    ) -> Self {
        // Build map of column names to their MIN/MAX info with indices
        let mut column_min_max = HashMap::new();
        let mut column_indices = HashMap::new();
        let mut current_index = 0;

        // First pass: assign indices to unique MIN/MAX columns
        for agg in &aggregates {
            match agg {
                AggregateFunction::Min(col) | AggregateFunction::Max(col) => {
                    column_indices.entry(col.clone()).or_insert_with(|| {
                        let idx = current_index;
                        current_index += 1;
                        idx
                    });
                }
                _ => {}
            }
        }

        // Second pass: build the column info map
        for agg in &aggregates {
            match agg {
                AggregateFunction::Min(col) => {
                    let index = *column_indices.get(col).unwrap();
                    let entry = column_min_max.entry(col.clone()).or_insert(AggColumnInfo {
                        index,
                        has_min: false,
                        has_max: false,
                    });
                    entry.has_min = true;
                }
                AggregateFunction::Max(col) => {
                    let index = *column_indices.get(col).unwrap();
                    let entry = column_min_max.entry(col.clone()).or_insert(AggColumnInfo {
                        index,
                        has_min: false,
                        has_max: false,
                    });
                    entry.has_max = true;
                }
                _ => {}
            }
        }

        Self {
            operator_id,
            group_by,
            aggregates,
            input_column_names,
            column_min_max,
            tracker: None,
            commit_state: AggregateCommitState::Idle,
        }
    }

    pub fn has_min_max(&self) -> bool {
        !self.column_min_max.is_empty()
    }

    fn eval_internal(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        match state {
            EvalState::Uninitialized => {
                panic!("Cannot eval AggregateOperator with Uninitialized state");
            }
            EvalState::Init { deltas } => {
                // Aggregate operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "AggregateOperator expects right_delta to be empty"
                );

                if deltas.left.changes.is_empty() {
                    *state = EvalState::Done;
                    return Ok(IOResult::Done((Delta::new(), HashMap::new())));
                }

                let mut groups_to_read = BTreeMap::new();
                for (row, _weight) in &deltas.left.changes {
                    // Extract group key using cloned fields
                    let group_key = self.extract_group_key(&row.values);
                    let group_key_str = Self::group_key_to_string(&group_key);
                    groups_to_read.insert(group_key_str, group_key);
                }
                state.advance_aggregate(groups_to_read);
            }
            EvalState::Aggregate(_agg_state) => {
                // Already in progress, continue processing below.
            }
            EvalState::Done => {
                panic!("unreachable state! should have returned");
            }
            EvalState::Join(_) => {
                panic!("Join state should not appear in aggregate operator");
            }
        }

        // Process the delta through the aggregate state machine
        match state {
            EvalState::Aggregate(agg_state) => {
                let result = return_if_io!(agg_state.process_delta(self, cursors));
                Ok(IOResult::Done(result))
            }
            _ => panic!("Invalid state for aggregate processing"),
        }
    }

    fn merge_delta_with_existing(
        &mut self,
        delta: &Delta,
        existing_groups: &mut HashMap<String, AggregateState>,
        old_values: &mut HashMap<String, Vec<Value>>,
    ) -> (Delta, HashMap<String, (Vec<Value>, AggregateState)>) {
        let mut output_delta = Delta::new();
        let mut temp_keys: HashMap<String, Vec<Value>> = HashMap::new();

        // Process each change in the delta
        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_aggregation();
            }

            // Extract group key
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            let state = existing_groups.entry(group_key_str.clone()).or_default();

            temp_keys.insert(group_key_str.clone(), group_key.clone());

            // Apply the delta to the temporary state
            state.apply_delta(
                &row.values,
                *weight,
                &self.aggregates,
                &self.input_column_names,
            );
        }

        // Generate output delta from temporary states and collect final states
        let mut final_states = HashMap::new();

        for (group_key_str, state) in existing_groups {
            let group_key = temp_keys.get(group_key_str).cloned().unwrap_or_default();

            // Generate a unique rowid for this group
            let result_key = self.generate_group_rowid(group_key_str);

            if let Some(old_row_values) = old_values.get(group_key_str) {
                let old_row = HashableRow::new(result_key, old_row_values.clone());
                output_delta.changes.push((old_row, -1));
            }

            // Always store the state for persistence (even if count=0, we need to delete it)
            final_states.insert(group_key_str.clone(), (group_key.clone(), state.clone()));

            // Only include groups with count > 0 in the output delta
            if state.count > 0 {
                // Build output row: group_by columns + aggregate values
                let mut output_values = group_key.clone();
                let aggregate_values = state.to_values(&self.aggregates);
                output_values.extend(aggregate_values);

                let output_row = HashableRow::new(result_key, output_values.clone());
                output_delta.changes.push((output_row, 1));
            }
        }
        (output_delta, final_states)
    }

    /// Extract MIN/MAX values from delta changes for persistence to index
    fn extract_min_max_deltas(&self, delta: &Delta) -> MinMaxDeltas {
        let mut min_max_deltas: MinMaxDeltas = HashMap::new();

        for (row, weight) in &delta.changes {
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            for agg in &self.aggregates {
                match agg {
                    AggregateFunction::Min(col_name) | AggregateFunction::Max(col_name) => {
                        if let Some(idx) =
                            self.input_column_names.iter().position(|c| c == col_name)
                        {
                            if let Some(val) = row.values.get(idx) {
                                // Skip NULL values - they don't participate in MIN/MAX
                                if val == &Value::Null {
                                    continue;
                                }
                                // Create a HashableRow with just this value
                                // Use 0 as rowid since we only care about the value for comparison
                                let hashable_value = HashableRow::new(0, vec![val.clone()]);
                                let key = (col_name.clone(), hashable_value);

                                let group_entry =
                                    min_max_deltas.entry(group_key_str.clone()).or_default();

                                let value_entry = group_entry.entry(key).or_insert(0);

                                // Accumulate the weight
                                *value_entry += weight;
                            }
                        }
                    }
                    _ => {} // Ignore non-MIN/MAX aggregates
                }
            }
        }

        min_max_deltas
    }

    pub fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }

    /// Generate a rowid for a group
    /// For no GROUP BY: always returns 0
    /// For GROUP BY: returns a hash of the group key string
    pub fn generate_group_rowid(&self, group_key_str: &str) -> i64 {
        if self.group_by.is_empty() {
            0
        } else {
            group_key_str
                .bytes()
                .fold(0i64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as i64))
        }
    }

    /// Generate the composite key for BTree storage
    /// Combines operator_id and group hash
    fn generate_storage_key(&self, group_key_str: &str) -> i64 {
        let group_hash = self.generate_group_rowid(group_key_str);
        (self.operator_id as i64) << 32 | (group_hash & 0xFFFFFFFF)
    }

    /// Extract group key values from a row
    pub fn extract_group_key(&self, values: &[Value]) -> Vec<Value> {
        let mut key = Vec::new();

        for group_col in &self.group_by {
            if let Some(idx) = self.input_column_names.iter().position(|c| c == group_col) {
                if let Some(val) = values.get(idx) {
                    key.push(val.clone());
                } else {
                    key.push(Value::Null);
                }
            } else {
                key.push(Value::Null);
            }
        }

        key
    }

    /// Convert group key to string for indexing (since Value doesn't implement Hash)
    pub fn group_key_to_string(key: &[Value]) -> String {
        key.iter()
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn seek_key_from_str(&self, group_key_str: &str) -> SeekKey<'_> {
        // Calculate the composite key for seeking
        let key_i64 = self.generate_storage_key(group_key_str);
        SeekKey::TableRowId(key_i64)
    }

    fn seek_key(&self, row: HashableRow) -> SeekKey<'_> {
        // Extract group key for first row
        let group_key = self.extract_group_key(&row.values);
        let group_key_str = Self::group_key_to_string(&group_key);
        self.seek_key_from_str(&group_key_str)
    }
}

impl IncrementalOperator for AggregateOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let (delta, _) = return_if_io!(self.eval_internal(state, cursors));
        Ok(IOResult::Done(delta))
    }

    fn commit(
        &mut self,
        mut deltas: DeltaPair,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Aggregate operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "AggregateOperator expects right delta to be empty in commit"
        );
        let delta = std::mem::take(&mut deltas.left);
        loop {
            // Note: because we std::mem::replace here (without it, the borrow checker goes nuts,
            // because we call self.eval_interval, which requires a mutable borrow), we have to
            // restore the state if we return I/O. So we can't use return_if_io!
            let mut state =
                std::mem::replace(&mut self.commit_state, AggregateCommitState::Invalid);
            match &mut state {
                AggregateCommitState::Invalid => {
                    panic!("Reached invalid state! State was replaced, and not replaced back");
                }
                AggregateCommitState::Idle => {
                    let eval_state = EvalState::from_delta(delta.clone());
                    self.commit_state = AggregateCommitState::Eval { eval_state };
                }
                AggregateCommitState::Eval { ref mut eval_state } => {
                    // Extract input delta before eval for MIN/MAX processing
                    let input_delta = eval_state.extract_delta();

                    // Extract MIN/MAX deltas before any I/O operations
                    let min_max_deltas = self.extract_min_max_deltas(&input_delta);

                    // Create a new eval state with the same delta
                    *eval_state = EvalState::from_delta(input_delta.clone());

                    let (output_delta, computed_states) = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.eval_internal(eval_state, cursors)
                    );

                    self.commit_state = AggregateCommitState::PersistDelta {
                        delta: output_delta,
                        computed_states,
                        current_idx: 0,
                        write_row: WriteRow::new(),
                        min_max_deltas, // Store for later use
                    };
                }
                AggregateCommitState::PersistDelta {
                    delta,
                    computed_states,
                    current_idx,
                    write_row,
                    min_max_deltas,
                } => {
                    let states_vec: Vec<_> = computed_states.iter().collect();

                    if *current_idx >= states_vec.len() {
                        // Use the min_max_deltas we extracted earlier from the input delta
                        self.commit_state = AggregateCommitState::PersistMinMax {
                            delta: delta.clone(),
                            min_max_persist_state: MinMaxPersistState::new(min_max_deltas.clone()),
                        };
                    } else {
                        let (group_key_str, (group_key, agg_state)) = states_vec[*current_idx];

                        // Build the key components for the new table structure
                        // For regular aggregates, use column_index=0 and AGG_TYPE_REGULAR
                        let operator_storage_id =
                            generate_storage_id(self.operator_id, 0, AGG_TYPE_REGULAR);
                        let zset_id = self.generate_group_rowid(group_key_str);
                        let element_id = 0i64;

                        // Determine weight: -1 to delete (cancels existing weight=1), 1 to insert/update
                        let weight = if agg_state.count == 0 { -1 } else { 1 };

                        // Serialize the aggregate state with group key (even for deletion, we need a row)
                        let state_blob = agg_state.to_blob(&self.aggregates, group_key);
                        let blob_value = Value::Blob(state_blob);

                        // Build the aggregate storage format: [operator_id, zset_id, element_id, value, weight]
                        let operator_id_val = Value::Integer(operator_storage_id);
                        let zset_id_val = Value::Integer(zset_id);
                        let element_id_val = Value::Integer(element_id);
                        let blob_val = blob_value.clone();

                        // Create index key - the first 3 columns of our primary key
                        let index_key = vec![
                            operator_id_val.clone(),
                            zset_id_val.clone(),
                            element_id_val.clone(),
                        ];

                        // Record values (without weight)
                        let record_values =
                            vec![operator_id_val, zset_id_val, element_id_val, blob_val];

                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            write_row.write_row(cursors, index_key, record_values, weight)
                        );

                        let delta = std::mem::take(delta);
                        let computed_states = std::mem::take(computed_states);
                        let min_max_deltas = std::mem::take(min_max_deltas);

                        self.commit_state = AggregateCommitState::PersistDelta {
                            delta,
                            computed_states,
                            current_idx: *current_idx + 1,
                            write_row: WriteRow::new(), // Reset for next write
                            min_max_deltas,
                        };
                    }
                }
                AggregateCommitState::PersistMinMax {
                    delta,
                    min_max_persist_state,
                } => {
                    if !self.has_min_max() {
                        let delta = std::mem::take(delta);
                        self.commit_state = AggregateCommitState::Done { delta };
                    } else {
                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            min_max_persist_state.persist_min_max(
                                self.operator_id,
                                &self.column_min_max,
                                cursors,
                                |group_key_str| self.generate_group_rowid(group_key_str)
                            )
                        );

                        let delta = std::mem::take(delta);
                        self.commit_state = AggregateCommitState::Done { delta };
                    }
                }
                AggregateCommitState::Done { delta } => {
                    self.commit_state = AggregateCommitState::Idle;
                    let delta = std::mem::take(delta);
                    return Ok(IOResult::Done(delta));
                }
            }
        }
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

#[derive(Debug)]
enum JoinCommitState {
    Idle,
    Eval {
        eval_state: EvalState,
    },
    CommitLeftDelta {
        deltas: DeltaPair,
        output: Delta,
        current_idx: usize,
        write_row: WriteRow,
    },
    CommitRightDelta {
        deltas: DeltaPair,
        output: Delta,
        current_idx: usize,
        write_row: WriteRow,
    },
    Invalid,
}

/// Join operator - performs incremental join between two relations
/// Implements the DBSP formula: δ(R ⋈ S) = (δR ⋈ S) ∪ (R ⋈ δS) ∪ (δR ⋈ δS)
#[derive(Debug)]
pub struct JoinOperator {
    /// Unique operator ID for indexing in persistent storage
    operator_id: usize,
    /// Type of join to perform
    join_type: JoinType,
    /// Column indices for extracting join keys from left input
    left_key_indices: Vec<usize>,
    /// Column indices for extracting join keys from right input
    right_key_indices: Vec<usize>,
    /// Column names from left input
    left_columns: Vec<String>,
    /// Column names from right input
    right_columns: Vec<String>,
    /// Tracker for computation statistics
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    commit_state: JoinCommitState,
}

impl JoinOperator {
    pub fn new(
        operator_id: usize,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        left_columns: Vec<String>,
        right_columns: Vec<String>,
    ) -> Result<Self> {
        // Check for unsupported join types
        match join_type {
            JoinType::Left => {
                return Err(crate::LimboError::ParseError(
                    "LEFT OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Right => {
                return Err(crate::LimboError::ParseError(
                    "RIGHT OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Full => {
                return Err(crate::LimboError::ParseError(
                    "FULL OUTER JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Cross => {
                return Err(crate::LimboError::ParseError(
                    "CROSS JOIN is not yet supported in incremental views".to_string(),
                ))
            }
            JoinType::Inner => {} // Inner join is supported
        }

        Ok(Self {
            operator_id,
            join_type,
            left_key_indices,
            right_key_indices,
            left_columns,
            right_columns,
            tracker: None,
            commit_state: JoinCommitState::Idle,
        })
    }

    /// Extract join key from row values using the specified indices
    fn extract_join_key(&self, values: &[Value], indices: &[usize]) -> HashableRow {
        let key_values: Vec<Value> = indices
            .iter()
            .map(|&idx| values.get(idx).cloned().unwrap_or(Value::Null))
            .collect();
        // Use 0 as a dummy rowid for join keys. They don't come from a table,
        // so they don't need a rowid. Their key will be the hash of the row values.
        HashableRow::new(0, key_values)
    }

    /// Generate storage ID for left table
    fn left_storage_id(&self) -> i64 {
        // Use column_index=0 for left side
        generate_storage_id(self.operator_id, 0, 0)
    }

    /// Generate storage ID for right table
    fn right_storage_id(&self) -> i64 {
        // Use column_index=1 for right side
        generate_storage_id(self.operator_id, 1, 0)
    }

    /// SQL-compliant comparison for join keys
    /// Returns true if keys match according to SQL semantics (NULL != NULL)
    fn sql_keys_equal(left_key: &HashableRow, right_key: &HashableRow) -> bool {
        if left_key.values.len() != right_key.values.len() {
            return false;
        }

        for (left_val, right_val) in left_key.values.iter().zip(right_key.values.iter()) {
            // In SQL, NULL never equals NULL
            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return false;
            }

            // For non-NULL values, use regular comparison
            if left_val != right_val {
                return false;
            }
        }

        true
    }

    fn process_join_state(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Get the join state out of the enum
        match state {
            EvalState::Join(js) => js.process_join_state(
                cursors,
                &self.left_key_indices,
                &self.right_key_indices,
                self.left_storage_id(),
                self.right_storage_id(),
            ),
            _ => panic!("process_join_state called with non-join state"),
        }
    }

    fn eval_internal(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        loop {
            let loop_state = std::mem::replace(state, EvalState::Uninitialized);
            match loop_state {
                EvalState::Uninitialized => {
                    panic!("Cannot eval JoinOperator with Uninitialized state");
                }
                EvalState::Init { deltas } => {
                    let mut output = Delta::new();

                    // Component 3: δR ⋈ δS (left delta join right delta)
                    for (left_row, left_weight) in &deltas.left.changes {
                        let left_key =
                            self.extract_join_key(&left_row.values, &self.left_key_indices);

                        for (right_row, right_weight) in &deltas.right.changes {
                            let right_key =
                                self.extract_join_key(&right_row.values, &self.right_key_indices);

                            if Self::sql_keys_equal(&left_key, &right_key) {
                                if let Some(tracker) = &self.tracker {
                                    tracker.lock().unwrap().record_join_lookup();
                                }

                                // Combine the rows
                                let mut combined_values = left_row.values.clone();
                                combined_values.extend(right_row.values.clone());

                                // Create the joined row with a unique rowid
                                // Use hash of the combined values to ensure uniqueness
                                let temp_row = HashableRow::new(0, combined_values.clone());
                                let joined_rowid = temp_row.cached_hash() as i64;
                                let joined_row =
                                    HashableRow::new(joined_rowid, combined_values.clone());

                                // Add to output with combined weight
                                let combined_weight = left_weight * right_weight;
                                output.changes.push((joined_row, combined_weight));
                            }
                        }
                    }

                    *state = EvalState::Join(Box::new(JoinEvalState::ProcessDeltaJoin {
                        deltas,
                        output,
                    }));
                }
                EvalState::Join(join_state) => {
                    *state = EvalState::Join(join_state);
                    let output = return_if_io!(self.process_join_state(state, cursors));
                    return Ok(IOResult::Done(output));
                }
                EvalState::Done => {
                    return Ok(IOResult::Done(Delta::new()));
                }
                EvalState::Aggregate(_) => {
                    panic!("Aggregate state should not appear in join operator");
                }
            }
        }
    }
}

// Helper to deserialize a HashableRow from a blob
fn deserialize_hashable_row(blob: &[u8]) -> Result<HashableRow> {
    // Simple deserialization - this needs to match how we serialize in commit
    // Format: [rowid:8 bytes][num_values:4 bytes][values...]
    if blob.len() < 12 {
        return Err(crate::LimboError::InternalError(
            "Invalid blob size".to_string(),
        ));
    }

    let rowid = i64::from_le_bytes(blob[0..8].try_into().unwrap());
    let num_values = u32::from_le_bytes(blob[8..12].try_into().unwrap()) as usize;

    let mut values = Vec::new();
    let mut offset = 12;

    for _ in 0..num_values {
        if offset >= blob.len() {
            break;
        }

        let type_tag = blob[offset];
        offset += 1;

        match type_tag {
            0 => values.push(Value::Null),
            1 => {
                if offset + 8 <= blob.len() {
                    let i = i64::from_le_bytes(blob[offset..offset + 8].try_into().unwrap());
                    values.push(Value::Integer(i));
                    offset += 8;
                }
            }
            2 => {
                if offset + 8 <= blob.len() {
                    let f = f64::from_le_bytes(blob[offset..offset + 8].try_into().unwrap());
                    values.push(Value::Float(f));
                    offset += 8;
                }
            }
            3 => {
                if offset + 4 <= blob.len() {
                    let len =
                        u32::from_le_bytes(blob[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + len < blob.len() {
                        let text_bytes = blob[offset..offset + len].to_vec();
                        offset += len;
                        let subtype = match blob[offset] {
                            0 => crate::types::TextSubtype::Text,
                            1 => crate::types::TextSubtype::Json,
                            _ => crate::types::TextSubtype::Text,
                        };
                        offset += 1;
                        values.push(Value::Text(crate::types::Text {
                            value: text_bytes,
                            subtype,
                        }));
                    }
                }
            }
            4 => {
                if offset + 4 <= blob.len() {
                    let len =
                        u32::from_le_bytes(blob[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + len <= blob.len() {
                        let blob_data = blob[offset..offset + len].to_vec();
                        values.push(Value::Blob(blob_data));
                        offset += len;
                    }
                }
            }
            _ => break, // Unknown type tag
        }
    }

    Ok(HashableRow::new(rowid, values))
}

// Helper to serialize a HashableRow to a blob
fn serialize_hashable_row(row: &HashableRow) -> Vec<u8> {
    let mut blob = Vec::new();

    // Write rowid
    blob.extend_from_slice(&row.rowid.to_le_bytes());

    // Write number of values
    blob.extend_from_slice(&(row.values.len() as u32).to_le_bytes());

    // Write each value directly with type tags (like AggregateState does)
    for value in &row.values {
        match value {
            Value::Null => blob.push(0u8),
            Value::Integer(i) => {
                blob.push(1u8);
                blob.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                blob.push(2u8);
                blob.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                blob.push(3u8);
                let bytes = &s.value;
                blob.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                blob.extend_from_slice(bytes);
                blob.push(s.subtype as u8);
            }
            Value::Blob(b) => {
                blob.push(4u8);
                blob.extend_from_slice(&(b.len() as u32).to_le_bytes());
                blob.extend_from_slice(b);
            }
        }
    }

    blob
}

impl IncrementalOperator for JoinOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let delta = return_if_io!(self.eval_internal(state, cursors));
        Ok(IOResult::Done(delta))
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        loop {
            let mut state = std::mem::replace(&mut self.commit_state, JoinCommitState::Invalid);
            match &mut state {
                JoinCommitState::Idle => {
                    self.commit_state = JoinCommitState::Eval {
                        eval_state: deltas.clone().into(),
                    }
                }
                JoinCommitState::Eval { ref mut eval_state } => {
                    let output = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.eval(eval_state, cursors)
                    );
                    self.commit_state = JoinCommitState::CommitLeftDelta {
                        deltas: deltas.clone(),
                        output,
                        current_idx: 0,
                        write_row: WriteRow::new(),
                    };
                }
                JoinCommitState::CommitLeftDelta {
                    deltas,
                    output,
                    current_idx,
                    ref mut write_row,
                } => {
                    if *current_idx >= deltas.left.changes.len() {
                        self.commit_state = JoinCommitState::CommitRightDelta {
                            deltas: std::mem::take(deltas),
                            output: std::mem::take(output),
                            current_idx: 0,
                            write_row: WriteRow::new(),
                        };
                        continue;
                    }

                    let (row, weight) = &deltas.left.changes[*current_idx];
                    // Extract join key from the left row
                    let join_key = self.extract_join_key(&row.values, &self.left_key_indices);

                    // The index key: (storage_id, zset_id, element_id)
                    // zset_id is the hash of the join key, element_id is hash of the row
                    let storage_id = self.left_storage_id();
                    let zset_id = join_key.cached_hash() as i64;
                    let element_id = row.cached_hash() as i64;
                    let index_key = vec![
                        Value::Integer(storage_id),
                        Value::Integer(zset_id),
                        Value::Integer(element_id),
                    ];

                    // The record values: we'll store the serialized row as a blob
                    let row_blob = serialize_hashable_row(row);
                    let record_values = vec![
                        Value::Integer(self.left_storage_id()),
                        Value::Integer(join_key.cached_hash() as i64),
                        Value::Integer(row.cached_hash() as i64),
                        Value::Blob(row_blob),
                    ];

                    // Use return_and_restore_if_io to handle I/O properly
                    return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        write_row.write_row(cursors, index_key, record_values, *weight)
                    );

                    self.commit_state = JoinCommitState::CommitLeftDelta {
                        deltas: deltas.clone(),
                        output: output.clone(),
                        current_idx: *current_idx + 1,
                        write_row: WriteRow::new(),
                    };
                }
                JoinCommitState::CommitRightDelta {
                    deltas,
                    output,
                    current_idx,
                    ref mut write_row,
                } => {
                    if *current_idx >= deltas.right.changes.len() {
                        // Reset to Idle state for next commit
                        self.commit_state = JoinCommitState::Idle;
                        return Ok(IOResult::Done(output.clone()));
                    }

                    let (row, weight) = &deltas.right.changes[*current_idx];
                    // Extract join key from the right row
                    let join_key = self.extract_join_key(&row.values, &self.right_key_indices);

                    // The index key: (storage_id, zset_id, element_id)
                    let index_key = vec![
                        Value::Integer(self.right_storage_id()),
                        Value::Integer(join_key.cached_hash() as i64),
                        Value::Integer(row.cached_hash() as i64),
                    ];

                    // The record values: we'll store the serialized row as a blob
                    let row_blob = serialize_hashable_row(row);
                    let record_values = vec![
                        Value::Integer(self.right_storage_id()),
                        Value::Integer(join_key.cached_hash() as i64),
                        Value::Integer(row.cached_hash() as i64),
                        Value::Blob(row_blob),
                    ];

                    // Use return_and_restore_if_io to handle I/O properly
                    return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        write_row.write_row(cursors, index_key, record_values, *weight)
                    );

                    self.commit_state = JoinCommitState::CommitRightDelta {
                        deltas: std::mem::take(deltas),
                        output: std::mem::take(output),
                        current_idx: *current_idx + 1,
                        write_row: WriteRow::new(),
                    };
                }
                JoinCommitState::Invalid => {
                    panic!("Invalid join commit state");
                }
            }
        }
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pager::CreateBTreeFlags;
    use crate::types::Text;
    use crate::util::IOExt;
    use crate::Value;
    use crate::{Database, MemoryIO, IO};
    use std::sync::{Arc, Mutex};

    /// Create a test pager for operator tests with both table and index
    fn create_test_pager() -> (std::sync::Arc<crate::Pager>, usize, usize) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", false, false).unwrap();
        let conn = db.connect().unwrap();

        let pager = conn.pager.borrow().clone();

        // Allocate page 1 first (database header)
        let _ = pager.io.block(|| pager.allocate_page1());

        // Create a BTree for the table
        let table_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .expect("Failed to create BTree for aggregate state table")
            as usize;

        // Create a BTree for the index
        let index_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .expect("Failed to create BTree for aggregate state index")
            as usize;

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
                    if let Some((state, group_key)) =
                        AggregateState::from_blob(blob, &agg.aggregates)
                    {
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
            1,      // operator_id for testing
            vec![], // No GROUP BY
            vec![AggregateFunction::Sum("age".to_string())],
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
            1,                        // operator_id for testing
            vec!["team".to_string()], // GROUP BY team
            vec![AggregateFunction::Sum("score".to_string())],
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
            1, // operator_id for testing
            vec!["category".to_string()],
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
            1, // operator_id for testing
            vec!["product".to_string()],
            vec![AggregateFunction::Sum("amount".to_string())],
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
            1, // operator_id for testing
            vec!["user_id".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("amount".to_string()),
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
            1, // operator_id for testing
            vec!["category".to_string()],
            vec![AggregateFunction::Avg("value".to_string())],
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
            1, // operator_id for testing
            vec!["category".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("value".to_string()),
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
        let group_by = vec!["category".to_string()];
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
        let aggregates = vec![AggregateFunction::Sum("value".to_string())];
        let group_by = vec!["category".to_string()];
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
        let aggregates = vec![AggregateFunction::Avg("value".to_string())];
        let group_by = vec!["category".to_string()];
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
            AggregateFunction::Sum("value".to_string()),
            AggregateFunction::Avg("value".to_string()),
        ];
        let group_by = vec!["category".to_string()];
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

        let mut filter = FilterOperator::new(
            FilterPredicate::GreaterThan {
                column: "b".to_string(),
                value: Value::Integer(2),
            },
            vec!["a".to_string(), "b".to_string()],
        );

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

        let mut filter = FilterOperator::new(
            FilterPredicate::GreaterThan {
                column: "age".to_string(),
                value: Value::Integer(25),
            },
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
        );

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
            1, // operator_id for testing
            vec!["category".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("amount".to_string()),
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
                AggregateFunction::Sum("value".to_string()),
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
            1, // operator_id for testing
            vec!["type".to_string()],
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
            1,                            // operator_id
            vec!["category".to_string()], // GROUP BY category
            vec![
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("price".to_string()),
                AggregateFunction::Max("price".to_string()),
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
                AggregateFunction::Min("score".to_string()),
                AggregateFunction::Max("score".to_string()),
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
                AggregateFunction::Min("name".to_string()),
                AggregateFunction::Max("name".to_string()),
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
                AggregateFunction::Sum("value".to_string()),
                AggregateFunction::Min("value".to_string()),
                AggregateFunction::Max("value".to_string()),
                AggregateFunction::Avg("value".to_string()),
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
                AggregateFunction::Min("col1".to_string()),
                AggregateFunction::Max("col2".to_string()),
                AggregateFunction::Min("col3".to_string()),
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
}
