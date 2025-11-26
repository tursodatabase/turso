#![allow(dead_code)]

use crate::incremental::dbsp::Hash128;
use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    generate_storage_id, ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::incremental::persistence::WriteRow;
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use crate::{return_and_restore_if_io, return_if_io, Result, Value};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// Helper function to read the next row from the BTree for joins
fn read_next_join_row(
    storage_id: i64,
    join_key: &HashableRow,
    last_element_hash: Option<Hash128>,
    cursors: &mut DbspStateCursors,
) -> Result<IOResult<Option<(Hash128, HashableRow, isize)>>> {
    // Build the index key: (storage_id, zset_id, element_id)
    // zset_id is the hash of the join key
    let zset_hash = join_key.cached_hash();

    // For iteration, use the last element hash if we have one, or NULL to start
    let index_key_values = match last_element_hash {
        Some(last_hash) => vec![
            Value::Integer(storage_id),
            zset_hash.to_value(),
            last_hash.to_value(),
        ],
        None => vec![
            Value::Integer(storage_id),
            zset_hash.to_value(),
            Value::Null, // Start iteration from beginning
        ],
    };

    let index_record = ImmutableRecord::from_values(&index_key_values, index_key_values.len());

    // Use GE (>=) for initial seek with NULL, GT (>) for continuation
    let seek_op = if last_element_hash.is_none() {
        SeekOp::GE { eq_only: false }
    } else {
        SeekOp::GT
    };

    let seek_result = return_if_io!(cursors
        .index_cursor
        .seek(SeekKey::IndexKey(&index_record), seek_op));

    if !matches!(seek_result, SeekResult::Found) {
        return Ok(IOResult::Done(None));
    }

    // Check if we're still in the same (storage_id, zset_id) range
    let current_record = return_if_io!(cursors.index_cursor.record());

    // Extract all needed values from the record before dropping it
    let (found_storage_id, found_zset_hash, element_hash) = if let Some(rec) = current_record {
        let values = rec.get_values();

        // Index has 4 values: storage_id, zset_id, element_id, rowid (appended by WriteRow)
        if values.len() >= 3 {
            let found_storage_id = match &values[0].to_owned() {
                Value::Integer(id) => *id,
                _ => return Ok(IOResult::Done(None)),
            };
            let found_zset_hash = match &values[1].to_owned() {
                Value::Blob(blob) => Hash128::from_blob(blob).ok_or_else(|| {
                    crate::LimboError::InternalError("Invalid zset_hash blob".to_string())
                })?,
                _ => return Ok(IOResult::Done(None)),
            };
            let element_hash = match &values[2].to_owned() {
                Value::Blob(blob) => Hash128::from_blob(blob).ok_or_else(|| {
                    crate::LimboError::InternalError("Invalid element_hash blob".to_string())
                })?,
                _ => {
                    return Ok(IOResult::Done(None));
                }
            };
            (found_storage_id, found_zset_hash, element_hash)
        } else {
            return Ok(IOResult::Done(None));
        }
    } else {
        return Ok(IOResult::Done(None));
    };

    // Now we can safely check if we're in the right range
    // If we've moved to a different storage_id or zset_id, we're done
    if found_storage_id != storage_id || found_zset_hash != zset_hash {
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

                return Ok(IOResult::Done(Some((element_hash, row, weight))));
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
        last_row_scanned: Option<Hash128>,
    },
    ProcessRightJoin {
        deltas: DeltaPair,
        output: Delta,
        current_idx: usize,
        last_row_scanned: Option<Hash128>,
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
        // Use hash of combined values as synthetic rowid
        let temp_row = HashableRow::new(0, combined_values.clone());
        let joined_rowid = temp_row.cached_hash().as_i64();
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
                        last_row_scanned: None,
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
                            last_row_scanned: None,
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
                            Some((element_hash, right_row, right_weight)) => {
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
                                    last_row_scanned: Some(element_hash),
                                };
                            }
                            None => {
                                // No more matches for this left row, move to next
                                *self = JoinEvalState::ProcessLeftJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx + 1,
                                    last_row_scanned: None,
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
                            Some((element_hash, left_row, left_weight)) => {
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
                                    last_row_scanned: Some(element_hash),
                                };
                            }
                            None => {
                                // No more matches for this right row, move to next
                                *self = JoinEvalState::ProcessRightJoin {
                                    deltas: std::mem::take(deltas),
                                    output: std::mem::take(output),
                                    current_idx: *current_idx + 1,
                                    last_row_scanned: None,
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
    operator_id: i64,
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
        operator_id: i64,
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

        let result = Self {
            operator_id,
            join_type,
            left_key_indices,
            right_key_indices,
            left_columns,
            right_columns,
            tracker: None,
            commit_state: JoinCommitState::Idle,
        };
        Ok(result)
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
                                // Use hash of combined values as synthetic rowid
                                let temp_row = HashableRow::new(0, combined_values.clone());
                                let joined_rowid = temp_row.cached_hash().as_i64();
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

fn deserialize_hashable_row(blob: &[u8]) -> Result<HashableRow> {
    use crate::types::ImmutableRecord;

    let record = ImmutableRecord::from_bin_record(blob.to_vec());
    let ref_values = record.get_values();
    let all_values: Vec<Value> = ref_values.into_iter().map(|rv| rv.to_owned()).collect();

    if all_values.is_empty() {
        return Err(crate::LimboError::InternalError(
            "HashableRow blob must contain at least rowid".to_string(),
        ));
    }

    // First value is the rowid
    let rowid = match &all_values[0] {
        Value::Integer(i) => *i,
        _ => {
            return Err(crate::LimboError::InternalError(
                "First value must be rowid (integer)".to_string(),
            ))
        }
    };

    // Rest are the row values
    let values = all_values[1..].to_vec();

    Ok(HashableRow::new(rowid, values))
}

fn serialize_hashable_row(row: &HashableRow) -> Vec<u8> {
    use crate::types::ImmutableRecord;

    let mut all_values = Vec::with_capacity(row.values.len() + 1);
    all_values.push(Value::Integer(row.rowid));
    all_values.extend_from_slice(&row.values);

    let record = ImmutableRecord::from_values(&all_values, all_values.len());
    record.as_blob().clone()
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
                    let zset_hash = join_key.cached_hash();
                    let element_hash = row.cached_hash();
                    let index_key = vec![
                        Value::Integer(storage_id),
                        zset_hash.to_value(),
                        element_hash.to_value(),
                    ];

                    // The record values: we'll store the serialized row as a blob
                    let row_blob = serialize_hashable_row(row);
                    let record_values = vec![
                        Value::Integer(self.left_storage_id()),
                        zset_hash.to_value(),
                        element_hash.to_value(),
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
                    let zset_hash = join_key.cached_hash();
                    let element_hash = row.cached_hash();
                    let index_key = vec![
                        Value::Integer(self.right_storage_id()),
                        zset_hash.to_value(),
                        element_hash.to_value(),
                    ];

                    // The record values: we'll store the serialized row as a blob
                    let row_blob = serialize_hashable_row(row);
                    let record_values = vec![
                        Value::Integer(self.right_storage_id()),
                        zset_hash.to_value(),
                        element_hash.to_value(),
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
