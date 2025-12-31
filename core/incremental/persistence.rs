use crate::incremental::operator::{AggregateState, DbspStateCursors};
use crate::numeric::Numeric;
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, LimboError, Result, Value};

#[derive(Debug, Default)]
pub enum ReadRecord {
    #[default]
    GetRecord,
    Done {
        state: Box<Option<AggregateState>>,
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
                        *self = ReadRecord::Done {
                            state: Box::new(None),
                        };
                    } else {
                        let record = return_if_io!(cursor.record());
                        let r = record.ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "Found key {key:?} in aggregate storage but could not read record"
                            ))
                        })?;
                        // The blob is in column 3: operator_id, zset_id, element_id, value, weight
                        let blob = r.get_value(3)?.to_owned();

                        let (state, _group_key) = match blob {
                            Value::Blob(blob) => AggregateState::from_blob(&blob),
                            Value::Null => {
                                // For plain DISTINCT, we store null value and just track weight
                                // Return a minimal state indicating existence
                                Ok((AggregateState::default(), vec![]))
                            }
                            _ => Err(LimboError::ParseError(
                                "Value in aggregator not blob or null".to_string(),
                            )),
                        }?;
                        *self = ReadRecord::Done {
                            state: Box::new(Some(state)),
                        }
                    }
                }
                ReadRecord::Done { state } => return Ok(IOResult::Done((**state).clone())),
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
                        // Row doesn't exist
                        if weight <= 0 {
                            // Can't delete/subtract from a non-existent row - this is a no-op.
                            // This can happen in recursive CTEs where intermediate results
                            // are computed and then retracted before being persisted.
                            *self = WriteRow::Done;
                        } else {
                            // Insert a new row with positive weight
                            *self = WriteRow::ComputeNewRowId {
                                final_weight: weight,
                            };
                        }
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
                        let weight_opt = r.get_value_opt(4);

                        // Weight is always the last value (column 4 in our 5-column structure)
                        let existing_weight = match weight_opt {
                            Some(val) => match val.to_owned() {
                                Value::Numeric(Numeric::Integer(w)) => w as isize,
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
                    // Re-seek the index cursor to ensure it's positioned correctly.
                    // The cursor position may have been invalidated after I/O yields.
                    let index_values = index_key.clone();
                    let index_record =
                        ImmutableRecord::from_values(&index_values, index_values.len());
                    let res = return_if_io!(cursors.index_cursor.seek(
                        SeekKey::IndexKey(&index_record),
                        SeekOp::GE { eq_only: true }
                    ));

                    // Mark as Done before delete to avoid retry on I/O
                    *self = WriteRow::Done;

                    // Only delete if we found the entry (it might have been deleted already)
                    if matches!(res, SeekResult::Found) {
                        return_if_io!(cursors.index_cursor.delete());
                    }
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
                    complete_record.push(Value::from_i64(final_weight_val as i64));

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
                    index_values.push(Value::from_i64(*rowid));

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
                    complete_record.push(Value::from_i64(*final_weight as i64));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryIO;
    use crate::pager::CreateBTreeFlags;
    use crate::util::IOExt;
    use crate::{Database, Pager, IO};
    use std::sync::Arc;

    use crate::incremental::operator::{create_dbsp_state_index, DbspStateCursors};

    fn create_test_pager() -> (Arc<Pager>, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();
        let _ = pager.io.block(|| pager.allocate_page1());
        let table_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .expect("Failed to create table BTree") as i64;
        let index_root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .expect("Failed to create index BTree") as i64;
        (pager, table_root_page_id, index_root_page_id)
    }

    /// Test that calling write_row with a negative weight for a key that
    /// doesn't exist in the btree is a no-op (WriteRow::Done) rather than
    /// inserting a row with negative weight.
    ///
    /// This can happen in recursive CTEs where intermediate results are
    /// computed and then retracted before being persisted.
    #[test]
    fn test_write_row_negative_weight_nonexistent_key_is_noop() {
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let index_def = create_dbsp_state_index(index_root_page_id);
        let table_cursor = BTreeCursor::new_table(pager.clone(), table_root_page_id, 5);
        let index_cursor = BTreeCursor::new_index(pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let index_key = vec![Value::from_i64(1), Value::from_i64(100), Value::from_i64(0)];
        let record_values = vec![
            Value::from_i64(1),
            Value::from_i64(100),
            Value::from_i64(0),
            Value::Null,
        ];

        let mut write_row = WriteRow::new();
        pager
            .io
            .block(|| {
                write_row.write_row(&mut cursors, index_key.clone(), record_values.clone(), -1)
            })
            .unwrap();

        // Verify the btree is still empty - no row with negative weight was inserted.
        // We check via the index cursor: seek for the key and verify it's not found.
        let index_record = ImmutableRecord::from_values(&index_key, index_key.len());
        let res = pager.io.block(|| {
            cursors.index_cursor.seek(
                SeekKey::IndexKey(&index_record),
                SeekOp::GE { eq_only: true },
            )
        });
        assert!(
            !matches!(res, Ok(SeekResult::Found)),
            "Expected no index entry after negative weight write for non-existent key, \
             but found one. This means a row with negative weight was incorrectly inserted."
        );
    }

    /// Test that writing a positive weight for a new key works, and then
    /// writing a negative weight that brings it to zero removes the row.
    #[test]
    fn test_write_row_positive_then_negative_removes_row() {
        let (pager, table_root_page_id, index_root_page_id) = create_test_pager();
        let index_def = create_dbsp_state_index(index_root_page_id);
        let table_cursor = BTreeCursor::new_table(pager.clone(), table_root_page_id, 5);
        let index_cursor = BTreeCursor::new_index(pager.clone(), index_root_page_id, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let index_key = vec![Value::from_i64(1), Value::from_i64(200), Value::from_i64(0)];
        let record_values = vec![
            Value::from_i64(1),
            Value::from_i64(200),
            Value::from_i64(0),
            Value::Null,
        ];

        let index_record = ImmutableRecord::from_values(&index_key, index_key.len());

        // Insert with positive weight
        let mut write_row = WriteRow::new();
        pager
            .io
            .block(|| {
                write_row.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1)
            })
            .unwrap();

        // Verify the row exists via index seek
        let res = pager.io.block(|| {
            cursors.index_cursor.seek(
                SeekKey::IndexKey(&index_record),
                SeekOp::GE { eq_only: true },
            )
        });
        assert!(
            matches!(res, Ok(SeekResult::Found)),
            "Row should exist after positive weight insert"
        );

        // Now apply negative weight to remove it
        let mut write_row2 = WriteRow::new();
        pager
            .io
            .block(|| {
                write_row2.write_row(&mut cursors, index_key.clone(), record_values.clone(), -1)
            })
            .unwrap();

        // Verify the row is gone
        let res2 = pager.io.block(|| {
            cursors.index_cursor.seek(
                SeekKey::IndexKey(&index_record),
                SeekOp::GE { eq_only: true },
            )
        });
        assert!(
            !matches!(res2, Ok(SeekResult::Found)),
            "Row should be removed after net-zero weight"
        );
    }
}
