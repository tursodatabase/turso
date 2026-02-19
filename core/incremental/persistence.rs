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
    use crate::incremental::operator::{create_dbsp_state_index, DbspStateCursors};
    use crate::storage::btree::{BTreeCursor, CursorTrait};
    use crate::storage::pager::CreateBTreeFlags;
    use crate::sync::Arc;
    use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult};
    use crate::util::IOExt;
    use crate::{Database, MemoryIO, IO};

    fn create_test_pager() -> (Arc<crate::Pager>, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();
        let _ = pager.io.block(|| pager.allocate_page1());

        let table_root = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as i64;
        let index_root = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;

        (pager, table_root, index_root)
    }

    /// Verify that every table row has a matching index entry by scanning the
    /// table and seeking each row's key in the index. Returns the number of
    /// verified rows.
    fn verify_index_integrity(pager: &Arc<crate::Pager>, cursors: &mut DbspStateCursors) -> usize {
        pager.io.block(|| cursors.table_cursor.rewind()).unwrap();
        let mut count = 0;

        loop {
            if cursors.table_cursor.is_empty() {
                break;
            }

            let record = loop {
                match cursors.table_cursor.record().unwrap() {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            };
            let record = record.unwrap().to_owned();
            let values: Vec<Value> = record.get_values_owned().unwrap();

            let rowid = loop {
                match cursors.table_cursor.rowid().unwrap() {
                    IOResult::Done(r) => break r.unwrap(),
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            };

            // Build the index key: first 3 columns (operator_id, zset_id, element_id)
            let index_key: Vec<Value> = values[..3].to_vec();
            let index_record = ImmutableRecord::from_values(&index_key, index_key.len());

            let seek_result = pager
                .io
                .block(|| {
                    cursors.index_cursor.seek(
                        SeekKey::IndexKey(&index_record),
                        SeekOp::GE { eq_only: true },
                    )
                })
                .unwrap();

            assert!(
                matches!(seek_result, SeekResult::Found),
                "Index entry not found for table rowid={rowid}, key={index_key:?}"
            );

            // Verify the index entry points to the correct rowid
            let index_rowid = pager
                .io
                .block(|| cursors.index_cursor.rowid())
                .unwrap()
                .unwrap();
            assert_eq!(
                index_rowid, rowid,
                "Index rowid mismatch: expected {rowid}, got {index_rowid}"
            );

            count += 1;
            pager.io.block(|| cursors.table_cursor.next()).unwrap();
        }
        count
    }

    /// Simulates the scenario where an I/O yield occurs during the table insert
    /// in InsertNew, and the index cursor is left in a stale position.
    ///
    /// Without the SeekForInsertIndex fix, InsertIndex uses the cursor position
    /// left over from the GetRecord seek, which may be wrong after an I/O yield.
    /// With the fix, SeekForInsertIndex re-seeks the index cursor before insert.
    ///
    /// This test reproduces the issue by:
    /// 1. Inserting several entries to populate the B-tree
    /// 2. For a new entry, manually performing the GetRecord + table insert steps
    /// 3. Moving the index cursor to a different position (simulating stale state
    ///    from I/O yield)
    /// 4. Continuing from the InsertIndex state
    /// 5. Verifying the index is correct
    #[test]
    fn test_write_row_stale_index_cursor_after_simulated_io_yield() {
        let (pager, table_root, index_root) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        let index_def = create_dbsp_state_index(index_root);
        let index_cursor = BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let op_id = 1i64;
        let zset_id = 1i64;

        // Insert some entries normally to populate the B-tree
        for i in 1..=10 {
            let mut wr = WriteRow::new();
            let ik = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
            ];
            let rv = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
                Value::Null,
            ];
            pager
                .io
                .block(|| wr.write_row(&mut cursors, ik.clone(), rv.clone(), 1))
                .unwrap();
        }

        // Now simulate inserting element_id=20 with a simulated I/O yield.
        // Step 1: Manually perform the table insert (what InsertNew does)
        let new_elem_id = 20i64;
        let rowid = 11i64; // next rowid after 10 entries

        // Seek and insert into table
        pager
            .io
            .block(|| {
                cursors
                    .table_cursor
                    .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: false })
            })
            .unwrap();
        let complete_record = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(new_elem_id),
            Value::Null,
            Value::from_i64(1), // weight=1
        ];
        let immutable_record =
            ImmutableRecord::from_values(&complete_record, complete_record.len());
        let btree_key = BTreeKey::new_table_rowid(rowid, Some(&immutable_record));
        pager
            .io
            .block(|| cursors.table_cursor.insert(&btree_key))
            .unwrap();

        // Step 2: Simulate I/O yield invalidating the index cursor.
        // Move the index cursor to element_id=1 (a wrong position for inserting element_id=20).
        let wrong_key = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(1i64),
        ];
        let wrong_record = ImmutableRecord::from_values(&wrong_key, wrong_key.len());
        pager
            .io
            .block(|| {
                cursors.index_cursor.seek(
                    SeekKey::IndexKey(&wrong_record),
                    SeekOp::GE { eq_only: false },
                )
            })
            .unwrap();

        // Step 3: Continue from InsertIndex state (what happens after I/O yield resumes)
        let index_key = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(new_elem_id),
        ];
        let record_values = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(new_elem_id),
            Value::Null,
        ];
        let mut wr = WriteRow::InsertIndex { rowid };
        pager
            .io
            .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
            .unwrap();

        // Step 4: Verify the index entry for element_id=20 exists and points to
        // the correct rowid
        let search_key = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(new_elem_id),
        ];
        let search_record = ImmutableRecord::from_values(&search_key, search_key.len());
        let seek_result = pager
            .io
            .block(|| {
                cursors.index_cursor.seek(
                    SeekKey::IndexKey(&search_record),
                    SeekOp::GE { eq_only: true },
                )
            })
            .unwrap();
        assert!(
            matches!(seek_result, SeekResult::Found),
            "Index entry not found for element_id={new_elem_id} after simulated I/O yield"
        );
        let found_rowid = pager
            .io
            .block(|| cursors.index_cursor.rowid())
            .unwrap()
            .unwrap();
        assert_eq!(
            found_rowid, rowid,
            "Index entry for element_id={new_elem_id} points to wrong rowid: expected {rowid}, got {found_rowid}"
        );

        // Also verify ALL entries are intact (the wrong cursor position might have
        // corrupted an existing entry)
        let count = verify_index_integrity(&pager, &mut cursors);
        assert_eq!(
            count, 11,
            "Expected 11 rows (10 original + 1 new), found {count}"
        );
    }

    #[test]
    fn test_write_row_many_entries_index_integrity() {
        let (pager, table_root, index_root) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        let index_def = create_dbsp_state_index(index_root);
        let index_cursor = BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let op_id = 1i64;
        let zset_id = 1i64;

        // Insert 200+ entries with distinct element_ids to force multi-page B-tree.
        // Each entry is a new group, so WriteRow takes the InsertNew path every time.
        let n = 250;
        for i in 1..=n {
            let mut wr = WriteRow::new();
            let index_key = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
            ];
            let record_values = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
                Value::Null,
            ];
            pager
                .io
                .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
                .unwrap();
        }

        let count = verify_index_integrity(&pager, &mut cursors);
        assert_eq!(count, n as usize, "Expected {n} rows, found {count}");
    }

    /// Test WriteRow with interleaved inserts and updates across many groups.
    /// First inserts create new entries (InsertNew path), then updates modify
    /// existing entries (UpdateExisting path). The alternation stresses cursor
    /// positioning between the "found" and "not found" index seek paths.
    #[test]
    fn test_write_row_interleaved_insert_update_index_integrity() {
        let (pager, table_root, index_root) = create_test_pager();
        let table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        let index_def = create_dbsp_state_index(index_root);
        let index_cursor = BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let op_id = 1i64;
        let zset_id = 1i64;
        let n = 150;

        // Phase 1: Insert n entries
        for i in 1..=n {
            let mut wr = WriteRow::new();
            let index_key = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
            ];
            let record_values = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
                Value::Null,
            ];
            pager
                .io
                .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
                .unwrap();
        }

        // Phase 2: Interleave updates to existing entries with inserts of new entries
        for i in 1..=n {
            // Update existing entry i (weight +1 → UpdateExisting path)
            let mut wr = WriteRow::new();
            let index_key = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
            ];
            let record_values = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(i),
                Value::Null,
            ];
            pager
                .io
                .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
                .unwrap();

            // Insert a new entry n+i (InsertNew path)
            let mut wr2 = WriteRow::new();
            let new_id = n + i;
            let index_key2 = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(new_id),
            ];
            let record_values2 = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(new_id),
                Value::Null,
            ];
            pager
                .io
                .block(|| {
                    wr2.write_row(&mut cursors, index_key2.clone(), record_values2.clone(), 1)
                })
                .unwrap();
        }

        let count = verify_index_integrity(&pager, &mut cursors);
        assert_eq!(
            count,
            (2 * n) as usize,
            "Expected {} rows, found {count}",
            2 * n
        );
    }
}
