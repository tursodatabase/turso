use crate::incremental::dbsp::HashableRow;
use crate::incremental::operator::{
    generate_storage_id, AggColumnInfo, AggregateFunction, AggregateOperator, AggregateState,
    DbspStateCursors, MinMaxDeltas, AGG_TYPE_MINMAX,
};
use crate::storage::btree::{BTreeCursor, BTreeKey};
use crate::types::{IOResult, ImmutableRecord, RefValue, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, LimboError, Result, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub enum ReadRecord {
    #[default]
    GetRecord,
    Done {
        state: Option<AggregateState>,
    },
}

impl ReadRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_record(
        &mut self,
        key: SeekKey,
        aggregates: &[AggregateFunction],
        cursor: &mut BTreeCursor,
    ) -> Result<IOResult<Option<AggregateState>>> {
        loop {
            match self {
                ReadRecord::GetRecord => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        *self = ReadRecord::Done { state: None };
                    } else {
                        let record = return_if_io!(cursor.record());
                        let r = record.ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "Found key {key:?} in aggregate storage but could not read record"
                            ))
                        })?;
                        let values = r.get_values();
                        // The blob is in column 3: operator_id, zset_id, element_id, value, weight
                        let blob = values[3].to_owned();

                        let (state, _group_key) = match blob {
                            Value::Blob(blob) => AggregateState::from_blob(&blob, aggregates)
                                .ok_or_else(|| {
                                    LimboError::InternalError(format!(
                                        "Cannot deserialize aggregate state {blob:?}",
                                    ))
                                }),
                            _ => Err(LimboError::ParseError(
                                "Value in aggregator not blob".to_string(),
                            )),
                        }?;
                        *self = ReadRecord::Done { state: Some(state) }
                    }
                }
                ReadRecord::Done { state } => return Ok(IOResult::Done(state.clone())),
            }
        }
    }
}

#[derive(Debug, Default)]
pub enum WriteRow {
    #[default]
    GetRecord,
    Delete {
        rowid: i64,
    },
    DeleteIndex,
    ComputeNewRowId {
        final_weight: isize,
    },
    InsertNew {
        rowid: i64,
        final_weight: isize,
    },
    InsertIndex {
        rowid: i64,
    },
    UpdateExisting {
        rowid: i64,
        final_weight: isize,
    },
    Done,
}

impl WriteRow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write a row with weight management using index for lookups.
    ///
    /// # Arguments
    /// * `cursors` - DBSP state cursors (table and index)
    /// * `index_key` - The key to seek in the index
    /// * `record_values` - The record values (without weight) to insert
    /// * `weight` - The weight delta to apply
    pub fn write_row(
        &mut self,
        cursors: &mut DbspStateCursors,
        index_key: Vec<Value>,
        record_values: Vec<Value>,
        weight: isize,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                WriteRow::GetRecord => {
                    // First, seek in the index to find if the row exists
                    let index_values = index_key.clone();
                    let index_record =
                        ImmutableRecord::from_values(&index_values, index_values.len());

                    let res = return_if_io!(cursors.index_cursor.seek(
                        SeekKey::IndexKey(&index_record),
                        SeekOp::GE { eq_only: true }
                    ));

                    if !matches!(res, SeekResult::Found) {
                        // Row doesn't exist, we'll insert a new one
                        *self = WriteRow::ComputeNewRowId {
                            final_weight: weight,
                        };
                    } else {
                        // Found in index, get the rowid it points to
                        let rowid = return_if_io!(cursors.index_cursor.rowid());
                        let rowid = rowid.ok_or_else(|| {
                            LimboError::InternalError(
                                "Index cursor does not have a valid rowid".to_string(),
                            )
                        })?;

                        // Now seek in the table using the rowid
                        let table_res = return_if_io!(cursors
                            .table_cursor
                            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));

                        if !matches!(table_res, SeekResult::Found) {
                            return Err(LimboError::InternalError(
                                "Index points to non-existent table row".to_string(),
                            ));
                        }

                        let existing_record = return_if_io!(cursors.table_cursor.record());
                        let r = existing_record.ok_or_else(|| {
                            LimboError::InternalError(
                                "Found rowid in table but could not read record".to_string(),
                            )
                        })?;
                        let values = r.get_values();

                        // Weight is always the last value (column 4 in our 5-column structure)
                        let existing_weight = match values.get(4) {
                            Some(val) => match val.to_owned() {
                                Value::Integer(w) => w as isize,
                                _ => {
                                    return Err(LimboError::InternalError(
                                        "Invalid weight value in storage".to_string(),
                                    ))
                                }
                            },
                            None => {
                                return Err(LimboError::InternalError(
                                    "No weight value found in storage".to_string(),
                                ))
                            }
                        };

                        let final_weight = existing_weight + weight;
                        if final_weight <= 0 {
                            // Store index_key for later deletion of index entry
                            *self = WriteRow::Delete { rowid }
                        } else {
                            // Store the rowid for update
                            *self = WriteRow::UpdateExisting {
                                rowid,
                                final_weight,
                            }
                        }
                    }
                }
                WriteRow::Delete { rowid } => {
                    // Seek to the row and delete it
                    return_if_io!(cursors
                        .table_cursor
                        .seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true }));

                    // Transition to DeleteIndex to also delete the index entry
                    *self = WriteRow::DeleteIndex;
                    return_if_io!(cursors.table_cursor.delete());
                }
                WriteRow::DeleteIndex => {
                    // Mark as Done before delete to avoid retry on I/O
                    *self = WriteRow::Done;
                    return_if_io!(cursors.index_cursor.delete());
                }
                WriteRow::ComputeNewRowId { final_weight } => {
                    // Find the last rowid to compute the next one
                    return_if_io!(cursors.table_cursor.last());
                    let rowid = if cursors.table_cursor.is_empty() {
                        1
                    } else {
                        match return_if_io!(cursors.table_cursor.rowid()) {
                            Some(id) => id + 1,
                            None => {
                                return Err(LimboError::InternalError(
                                    "Table cursor has rows but no valid rowid".to_string(),
                                ))
                            }
                        }
                    };

                    // Transition to InsertNew with the computed rowid
                    *self = WriteRow::InsertNew {
                        rowid,
                        final_weight: *final_weight,
                    };
                }
                WriteRow::InsertNew {
                    rowid,
                    final_weight,
                } => {
                    let rowid_val = *rowid;
                    let final_weight_val = *final_weight;

                    // Seek to where we want to insert
                    // The insert will position the cursor correctly
                    return_if_io!(cursors.table_cursor.seek(
                        SeekKey::TableRowId(rowid_val),
                        SeekOp::GE { eq_only: false }
                    ));

                    // Build the complete record with weight
                    // Use the function parameter record_values directly
                    let mut complete_record = record_values.clone();
                    complete_record.push(Value::Integer(final_weight_val as i64));

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        ImmutableRecord::from_values(&complete_record, complete_record.len());
                    let btree_key = BTreeKey::new_table_rowid(rowid_val, Some(&immutable_record));

                    // Transition to InsertIndex state after table insertion
                    *self = WriteRow::InsertIndex { rowid: rowid_val };
                    return_if_io!(cursors.table_cursor.insert(&btree_key));
                }
                WriteRow::InsertIndex { rowid } => {
                    // For has_rowid indexes, we need to append the rowid to the index key
                    // Use the function parameter index_key directly
                    let mut index_values = index_key.clone();
                    index_values.push(Value::Integer(*rowid));

                    // Create the index record with the rowid appended
                    let index_record =
                        ImmutableRecord::from_values(&index_values, index_values.len());
                    let index_btree_key = BTreeKey::new_index_key(&index_record);

                    // Mark as Done before index insert to avoid retry on I/O
                    *self = WriteRow::Done;
                    return_if_io!(cursors.index_cursor.insert(&index_btree_key));
                }
                WriteRow::UpdateExisting {
                    rowid,
                    final_weight,
                } => {
                    // Build the complete record with weight
                    let mut complete_record = record_values.clone();
                    complete_record.push(Value::Integer(*final_weight as i64));

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        ImmutableRecord::from_values(&complete_record, complete_record.len());
                    let btree_key = BTreeKey::new_table_rowid(*rowid, Some(&immutable_record));

                    // Mark as Done before insert to avoid retry on I/O
                    *self = WriteRow::Done;
                    // BTree insert with existing key will replace the old value
                    return_if_io!(cursors.table_cursor.insert(&btree_key));
                }
                WriteRow::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

/// State machine for recomputing MIN/MAX values after deletion
#[derive(Debug)]
pub enum RecomputeMinMax {
    ProcessElements {
        /// Current column being processed
        current_column_idx: usize,
        /// Columns to process (combined MIN and MAX)
        columns_to_process: Vec<(String, String, bool)>, // (group_key, column_name, is_min)
        /// MIN/MAX deltas for checking values and weights
        min_max_deltas: MinMaxDeltas,
    },
    Scan {
        /// Columns still to process
        columns_to_process: Vec<(String, String, bool)>,
        /// Current index in columns_to_process (will resume from here)
        current_column_idx: usize,
        /// MIN/MAX deltas for checking values and weights
        min_max_deltas: MinMaxDeltas,
        /// Current group key being processed
        group_key: String,
        /// Current column name being processed
        column_name: String,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
        /// The scan state machine for finding the new MIN/MAX
        scan_state: Box<ScanState>,
    },
    Done,
}

impl RecomputeMinMax {
    pub fn new(
        min_max_deltas: MinMaxDeltas,
        existing_groups: &HashMap<String, AggregateState>,
        operator: &AggregateOperator,
    ) -> Self {
        let mut groups_to_check: HashSet<(String, String, bool)> = HashSet::new();

        // Remember the min_max_deltas are essentially just the only column that is affected by
        // this min/max, in delta (actually ZSet - consolidated delta) format. This makes it easier
        // for us to consume it in here.
        //
        // The most challenging case is the case where there is a retraction, since we need to go
        // back to the index.
        for (group_key_str, values) in &min_max_deltas {
            for ((col_name, hashable_row), weight) in values {
                let col_info = operator.column_min_max.get(col_name);

                let value = &hashable_row.values[0];

                if *weight < 0 {
                    // Deletion detected - check if it's the current MIN/MAX
                    if let Some(state) = existing_groups.get(group_key_str) {
                        // Check for MIN
                        if let Some(current_min) = state.mins.get(col_name) {
                            if current_min == value {
                                groups_to_check.insert((
                                    group_key_str.clone(),
                                    col_name.clone(),
                                    true,
                                ));
                            }
                        }
                        // Check for MAX
                        if let Some(current_max) = state.maxs.get(col_name) {
                            if current_max == value {
                                groups_to_check.insert((
                                    group_key_str.clone(),
                                    col_name.clone(),
                                    false,
                                ));
                            }
                        }
                    }
                } else if *weight > 0 {
                    // If it is not found in the existing groups, then we only need to care
                    // about this if this is a new record being inserted
                    if let Some(info) = col_info {
                        if info.has_min {
                            groups_to_check.insert((group_key_str.clone(), col_name.clone(), true));
                        }
                        if info.has_max {
                            groups_to_check.insert((
                                group_key_str.clone(),
                                col_name.clone(),
                                false,
                            ));
                        }
                    }
                }
            }
        }

        if groups_to_check.is_empty() {
            // No recomputation or initialization needed
            Self::Done
        } else {
            // Convert HashSet to Vec for indexed processing
            let groups_to_check_vec: Vec<_> = groups_to_check.into_iter().collect();
            Self::ProcessElements {
                current_column_idx: 0,
                columns_to_process: groups_to_check_vec,
                min_max_deltas,
            }
        }
    }

    pub fn process(
        &mut self,
        existing_groups: &mut HashMap<String, AggregateState>,
        operator: &AggregateOperator,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                RecomputeMinMax::ProcessElements {
                    current_column_idx,
                    columns_to_process,
                    min_max_deltas,
                } => {
                    if *current_column_idx >= columns_to_process.len() {
                        *self = RecomputeMinMax::Done;
                        return Ok(IOResult::Done(()));
                    }

                    let (group_key, column_name, is_min) =
                        columns_to_process[*current_column_idx].clone();

                    // Get column index from pre-computed info
                    let column_index = operator
                        .column_min_max
                        .get(&column_name)
                        .map(|info| info.index)
                        .unwrap(); // Should always exist since we're processing known columns

                    // Get current value from existing state
                    let current_value = existing_groups.get(&group_key).and_then(|state| {
                        if is_min {
                            state.mins.get(&column_name).cloned()
                        } else {
                            state.maxs.get(&column_name).cloned()
                        }
                    });

                    // Create storage keys for index lookup
                    let storage_id =
                        generate_storage_id(operator.operator_id, column_index, AGG_TYPE_MINMAX);
                    let zset_id = operator.generate_group_rowid(&group_key);

                    // Get the values for this group from min_max_deltas
                    let group_values = min_max_deltas.get(&group_key).cloned().unwrap_or_default();

                    let columns_to_process = std::mem::take(columns_to_process);
                    let min_max_deltas = std::mem::take(min_max_deltas);

                    let scan_state = if is_min {
                        Box::new(ScanState::new_for_min(
                            current_value,
                            group_key.clone(),
                            column_name.clone(),
                            storage_id,
                            zset_id,
                            group_values,
                        ))
                    } else {
                        Box::new(ScanState::new_for_max(
                            current_value,
                            group_key.clone(),
                            column_name.clone(),
                            storage_id,
                            zset_id,
                            group_values,
                        ))
                    };

                    *self = RecomputeMinMax::Scan {
                        columns_to_process,
                        current_column_idx: *current_column_idx,
                        min_max_deltas,
                        group_key,
                        column_name,
                        is_min,
                        scan_state,
                    };
                }
                RecomputeMinMax::Scan {
                    columns_to_process,
                    current_column_idx,
                    min_max_deltas,
                    group_key,
                    column_name,
                    is_min,
                    scan_state,
                } => {
                    // Find new value using the scan state machine
                    let new_value = return_if_io!(scan_state.find_new_value(cursors));

                    // Update the state with new value (create if doesn't exist)
                    let state = existing_groups.entry(group_key.clone()).or_default();

                    if *is_min {
                        if let Some(min_val) = new_value {
                            state.mins.insert(column_name.clone(), min_val);
                        } else {
                            state.mins.remove(column_name);
                        }
                    } else if let Some(max_val) = new_value {
                        state.maxs.insert(column_name.clone(), max_val);
                    } else {
                        state.maxs.remove(column_name);
                    }

                    // Move to next column
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let columns_to_process = std::mem::take(columns_to_process);
                    *self = RecomputeMinMax::ProcessElements {
                        current_column_idx: *current_column_idx + 1,
                        columns_to_process,
                        min_max_deltas,
                    };
                }
                RecomputeMinMax::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

/// State machine for scanning through the index to find new MIN/MAX values
#[derive(Debug)]
pub enum ScanState {
    CheckCandidate {
        /// Current candidate value for MIN/MAX
        candidate: Option<Value>,
        /// Group key being processed
        group_key: String,
        /// Column name being processed
        column_name: String,
        /// Storage ID for the index seek
        storage_id: i64,
        /// ZSet ID for the group
        zset_id: i64,
        /// Group values from MinMaxDeltas: (column_name, HashableRow) -> weight
        group_values: HashMap<(String, HashableRow), isize>,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
    },
    FetchNextCandidate {
        /// Current candidate to seek past
        current_candidate: Value,
        /// Group key being processed
        group_key: String,
        /// Column name being processed
        column_name: String,
        /// Storage ID for the index seek
        storage_id: i64,
        /// ZSet ID for the group
        zset_id: i64,
        /// Group values from MinMaxDeltas: (column_name, HashableRow) -> weight
        group_values: HashMap<(String, HashableRow), isize>,
        /// Whether we're looking for MIN (true) or MAX (false)
        is_min: bool,
    },
    Done {
        /// The final MIN/MAX value found
        result: Option<Value>,
    },
}

impl ScanState {
    pub fn new_for_min(
        current_min: Option<Value>,
        group_key: String,
        column_name: String,
        storage_id: i64,
        zset_id: i64,
        group_values: HashMap<(String, HashableRow), isize>,
    ) -> Self {
        Self::CheckCandidate {
            candidate: current_min,
            group_key,
            column_name,
            storage_id,
            zset_id,
            group_values,
            is_min: true,
        }
    }

    // Extract a new candidate from the index. It is possible that, when searching,
    // we end up going into a different operator altogether. That means we have
    // exhausted this operator (or group) entirely, and no good candidate was found
    fn extract_new_candidate(
        cursors: &mut DbspStateCursors,
        index_record: &ImmutableRecord,
        seek_op: SeekOp,
        storage_id: i64,
        zset_id: i64,
    ) -> Result<IOResult<Option<Value>>> {
        let seek_result = return_if_io!(cursors
            .index_cursor
            .seek(SeekKey::IndexKey(index_record), seek_op));
        if !matches!(seek_result, SeekResult::Found) {
            return Ok(IOResult::Done(None));
        }

        let record = return_if_io!(cursors.index_cursor.record()).ok_or_else(|| {
            LimboError::InternalError(
                "Record found on the cursor, but could not be read".to_string(),
            )
        })?;

        let values = record.get_values();
        if values.len() < 3 {
            return Ok(IOResult::Done(None));
        }

        let Some(rec_storage_id) = values.first() else {
            return Ok(IOResult::Done(None));
        };
        let Some(rec_zset_id) = values.get(1) else {
            return Ok(IOResult::Done(None));
        };

        // Check if we're still in the same group
        if let (RefValue::Integer(rec_sid), RefValue::Integer(rec_zid)) =
            (rec_storage_id, rec_zset_id)
        {
            if *rec_sid != storage_id || *rec_zid != zset_id {
                return Ok(IOResult::Done(None));
            }
        } else {
            return Ok(IOResult::Done(None));
        }

        // Get the value (3rd element)
        Ok(IOResult::Done(values.get(2).map(|v| v.to_owned())))
    }

    pub fn new_for_max(
        current_max: Option<Value>,
        group_key: String,
        column_name: String,
        storage_id: i64,
        zset_id: i64,
        group_values: HashMap<(String, HashableRow), isize>,
    ) -> Self {
        Self::CheckCandidate {
            candidate: current_max,
            group_key,
            column_name,
            storage_id,
            zset_id,
            group_values,
            is_min: false,
        }
    }

    pub fn find_new_value(
        &mut self,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Option<Value>>> {
        loop {
            match self {
                ScanState::CheckCandidate {
                    candidate,
                    group_key,
                    column_name,
                    storage_id,
                    zset_id,
                    group_values,
                    is_min,
                } => {
                    // First, check if we have a candidate
                    if let Some(cand_val) = candidate {
                        // Check if the candidate is retracted (weight <= 0)
                        // Create a HashableRow to look up the weight
                        let hashable_cand = HashableRow::new(0, vec![cand_val.clone()]);
                        let key = (column_name.clone(), hashable_cand);
                        let is_retracted =
                            group_values.get(&key).is_some_and(|weight| *weight <= 0);

                        if is_retracted {
                            // Candidate is retracted, need to fetch next from index
                            *self = ScanState::FetchNextCandidate {
                                current_candidate: cand_val.clone(),
                                group_key: std::mem::take(group_key),
                                column_name: std::mem::take(column_name),
                                storage_id: *storage_id,
                                zset_id: *zset_id,
                                group_values: std::mem::take(group_values),
                                is_min: *is_min,
                            };
                            continue;
                        }
                    }

                    // Candidate is valid or we have no candidate
                    // Now find the best value from insertions in group_values
                    let mut best_from_zset = None;
                    for ((col, hashable_val), weight) in group_values.iter() {
                        if col == column_name && *weight > 0 {
                            let value = &hashable_val.values[0];
                            // Skip NULL values - they don't participate in MIN/MAX
                            if value == &Value::Null {
                                continue;
                            }
                            // This is an insertion for our column
                            if let Some(ref current_best) = best_from_zset {
                                if *is_min {
                                    if value.cmp(current_best) == std::cmp::Ordering::Less {
                                        best_from_zset = Some(value.clone());
                                    }
                                } else if value.cmp(current_best) == std::cmp::Ordering::Greater {
                                    best_from_zset = Some(value.clone());
                                }
                            } else {
                                best_from_zset = Some(value.clone());
                            }
                        }
                    }

                    // Compare candidate with best from ZSet, filtering out NULLs
                    let result = match (&candidate, &best_from_zset) {
                        (Some(cand), Some(zset_val)) if cand != &Value::Null => {
                            if *is_min {
                                if zset_val.cmp(cand) == std::cmp::Ordering::Less {
                                    Some(zset_val.clone())
                                } else {
                                    Some(cand.clone())
                                }
                            } else if zset_val.cmp(cand) == std::cmp::Ordering::Greater {
                                Some(zset_val.clone())
                            } else {
                                Some(cand.clone())
                            }
                        }
                        (Some(cand), None) if cand != &Value::Null => Some(cand.clone()),
                        (None, Some(zset_val)) => Some(zset_val.clone()),
                        (Some(cand), Some(_)) if cand == &Value::Null => best_from_zset,
                        _ => None,
                    };

                    *self = ScanState::Done { result };
                }

                ScanState::FetchNextCandidate {
                    current_candidate,
                    group_key,
                    column_name,
                    storage_id,
                    zset_id,
                    group_values,
                    is_min,
                } => {
                    // Seek to the next value in the index
                    let index_key = vec![
                        Value::Integer(*storage_id),
                        Value::Integer(*zset_id),
                        current_candidate.clone(),
                    ];
                    let index_record = ImmutableRecord::from_values(&index_key, index_key.len());

                    let seek_op = if *is_min {
                        SeekOp::GT // For MIN, seek greater than current
                    } else {
                        SeekOp::LT // For MAX, seek less than current
                    };

                    let new_candidate = return_if_io!(Self::extract_new_candidate(
                        cursors,
                        &index_record,
                        seek_op,
                        *storage_id,
                        *zset_id
                    ));

                    *self = ScanState::CheckCandidate {
                        candidate: new_candidate,
                        group_key: std::mem::take(group_key),
                        column_name: std::mem::take(column_name),
                        storage_id: *storage_id,
                        zset_id: *zset_id,
                        group_values: std::mem::take(group_values),
                        is_min: *is_min,
                    };
                }

                ScanState::Done { result } => {
                    return Ok(IOResult::Done(result.clone()));
                }
            }
        }
    }
}

/// State machine for persisting Min/Max values to storage
#[derive(Debug)]
pub enum MinMaxPersistState {
    Init {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
    },
    ProcessGroup {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
        group_idx: usize,
        value_idx: usize,
    },
    WriteValue {
        min_max_deltas: MinMaxDeltas,
        group_keys: Vec<String>,
        group_idx: usize,
        value_idx: usize,
        value: Value,
        column_name: String,
        weight: isize,
        write_row: WriteRow,
    },
    Done,
}

impl MinMaxPersistState {
    pub fn new(min_max_deltas: MinMaxDeltas) -> Self {
        let group_keys: Vec<String> = min_max_deltas.keys().cloned().collect();
        Self::Init {
            min_max_deltas,
            group_keys,
        }
    }

    pub fn persist_min_max(
        &mut self,
        operator_id: usize,
        column_min_max: &HashMap<String, AggColumnInfo>,
        cursors: &mut DbspStateCursors,
        generate_group_rowid: impl Fn(&str) -> i64,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                MinMaxPersistState::Init {
                    min_max_deltas,
                    group_keys,
                } => {
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::ProcessGroup {
                        min_max_deltas,
                        group_keys,
                        group_idx: 0,
                        value_idx: 0,
                    };
                }
                MinMaxPersistState::ProcessGroup {
                    min_max_deltas,
                    group_keys,
                    group_idx,
                    value_idx,
                } => {
                    // Check if we're past all groups
                    if *group_idx >= group_keys.len() {
                        *self = MinMaxPersistState::Done;
                        continue;
                    }

                    let group_key_str = &group_keys[*group_idx];
                    let values = &min_max_deltas[group_key_str]; // This should always exist

                    // Convert HashMap to Vec for indexed access
                    let values_vec: Vec<_> = values.iter().collect();

                    // Check if we have more values in current group
                    if *value_idx >= values_vec.len() {
                        *group_idx += 1;
                        *value_idx = 0;
                        // Continue to check if we're past all groups now
                        continue;
                    }

                    // Process current value and extract what we need before taking ownership
                    let ((column_name, hashable_row), weight) = values_vec[*value_idx];
                    let column_name = column_name.clone();
                    let value = hashable_row.values[0].clone(); // Extract the Value from HashableRow
                    let weight = *weight;

                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::WriteValue {
                        min_max_deltas,
                        group_keys,
                        group_idx: *group_idx,
                        value_idx: *value_idx,
                        column_name,
                        value,
                        weight,
                        write_row: WriteRow::new(),
                    };
                }
                MinMaxPersistState::WriteValue {
                    min_max_deltas,
                    group_keys,
                    group_idx,
                    value_idx,
                    value,
                    column_name,
                    weight,
                    write_row,
                } => {
                    // Should have exited in the previous state
                    assert!(*group_idx < group_keys.len());

                    let group_key_str = &group_keys[*group_idx];

                    // Get the column index from the pre-computed map
                    let column_info = column_min_max
                        .get(&*column_name)
                        .expect("Column should exist in column_min_max map");
                    let column_index = column_info.index;

                    // Build the key components for MinMax storage using new encoding
                    let storage_id =
                        generate_storage_id(operator_id, column_index, AGG_TYPE_MINMAX);
                    let zset_id = generate_group_rowid(group_key_str);

                    // element_id is the actual value for Min/Max
                    let element_id_val = value.clone();

                    // Create index key
                    let index_key = vec![
                        Value::Integer(storage_id),
                        Value::Integer(zset_id),
                        element_id_val.clone(),
                    ];

                    // Record values (operator_id, zset_id, element_id, unused_placeholder)
                    // For MIN/MAX, the element_id IS the value, so we use NULL for the 4th column
                    let record_values = vec![
                        Value::Integer(storage_id),
                        Value::Integer(zset_id),
                        element_id_val.clone(),
                        Value::Null, // Placeholder - not used for MIN/MAX
                    ];

                    return_if_io!(write_row.write_row(
                        cursors,
                        index_key.clone(),
                        record_values,
                        *weight
                    ));

                    // Move to next value
                    let min_max_deltas = std::mem::take(min_max_deltas);
                    let group_keys = std::mem::take(group_keys);
                    *self = MinMaxPersistState::ProcessGroup {
                        min_max_deltas,
                        group_keys,
                        group_idx: *group_idx,
                        value_idx: *value_idx + 1,
                    };
                }
                MinMaxPersistState::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}
