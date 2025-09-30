use crate::incremental::operator::{AggregateState, DbspStateCursors};
use crate::storage::btree::{BTreeCursor, BTreeKey};
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, LimboError, Result, Value};

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
                            Value::Blob(blob) => AggregateState::from_blob(&blob),
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
