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
                        let blob = r.get_value(3)?.to_owned()?;

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

/// Each arm issues exactly one cursor op and advances only after it returns `Done`:
/// `IOResult::IO` means "call me again", so advancing first abandons an in-flight
/// balance. Seeks therefore get their own arm.
#[derive(Debug, Default)]
pub enum WriteRow {
    #[default]
    GetRecord,
    Delete {
        rowid: i64,
    },
    DeleteTable,
    DeleteIndex,
    ComputeNewRowId {
        final_weight: isize,
    },
    InsertNew {
        rowid: i64,
        final_weight: isize,
    },
    InsertNewRow {
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
                        ImmutableRecord::from_values(&index_values, index_values.len())?;

                    let res = return_if_io!(cursors.index_cursor.seek(
                        SeekKey::IndexKey(index_record.as_record_ref()),
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
                            Some(val) => match val.to_owned()? {
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
                    return_if_io!(cursors
                        .table_cursor
                        .seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true }));
                    *self = WriteRow::DeleteTable;
                }
                WriteRow::DeleteTable => {
                    return_if_io!(cursors.table_cursor.delete());
                    *self = WriteRow::DeleteIndex;
                }
                WriteRow::DeleteIndex => {
                    return_if_io!(cursors.index_cursor.delete());
                    *self = WriteRow::Done;
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
                    return_if_io!(cursors
                        .table_cursor
                        .seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: false }));
                    *self = WriteRow::InsertNewRow {
                        rowid: *rowid,
                        final_weight: *final_weight,
                    };
                }
                WriteRow::InsertNewRow {
                    rowid,
                    final_weight,
                } => {
                    let rowid_val = *rowid;
                    let final_weight_val = *final_weight;

                    // Build the complete record with weight
                    // Use the function parameter record_values directly
                    let mut complete_record = record_values.clone();
                    complete_record.push(Value::from_i64(final_weight_val as i64));

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        ImmutableRecord::from_values(&complete_record, complete_record.len())?;
                    let btree_key = BTreeKey::new_table_rowid(rowid_val, Some(&immutable_record));

                    return_if_io!(cursors.table_cursor.insert(&btree_key));
                    *self = WriteRow::InsertIndex { rowid: rowid_val };
                }
                WriteRow::InsertIndex { rowid } => {
                    // For has_rowid indexes, we need to append the rowid to the index key
                    // Use the function parameter index_key directly
                    let mut index_values = index_key.clone();
                    index_values.push(Value::from_i64(*rowid));

                    // Create the index record with the rowid appended
                    let index_record =
                        ImmutableRecord::from_values(&index_values, index_values.len())?;
                    let index_btree_key = BTreeKey::new_index_key(index_record.as_record_ref());

                    return_if_io!(cursors.index_cursor.insert(&index_btree_key));
                    *self = WriteRow::Done;
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
                        ImmutableRecord::from_values(&complete_record, complete_record.len())?;
                    let btree_key = BTreeKey::new_table_rowid(*rowid, Some(&immutable_record));

                    // BTree insert with existing key will replace the old value
                    return_if_io!(cursors.table_cursor.insert(&btree_key));
                    *self = WriteRow::Done;
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
    use crate::incremental::yield_test_support::OneShotYieldInjector;
    use crate::mvcc::yield_hooks::YieldPointMarker;
    use crate::storage::btree::{
        BTreeCursor, BTreeWriteYieldPoint, CursorTrait, BTREE_WRITE_YIELD_FAMILY,
    };
    use crate::storage::pager::CreateBTreeFlags;
    use crate::sync::Arc;
    use crate::util::IOExt;
    use crate::{Connection, Database, MemoryIO, SqliteDialect, IO};

    fn setup() -> (Arc<Connection>, Arc<crate::Pager>, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
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
        (conn, pager, table_root, index_root)
    }

    // ~1200-byte cells stay on the leaf, so enough of them overflow the page and trip
    // AfterInsertOverflowCellBeforeBalance.
    fn page_filling_record(op_id: i64, zset_id: i64, elem_id: i64) -> Vec<Value> {
        vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(elem_id),
            Value::from_slice(&[0xcd_u8; 1200]).unwrap(),
        ]
    }

    /// If the table insert yields mid-balance, `WriteRow` must re-drive it before
    /// advancing; advancing first strands the overflow cell and the row vanishes.
    #[test]
    fn write_row_completes_yielded_overflowing_table_insert() {
        let (conn, pager, table_root, index_root) = setup();

        let injector = OneShotYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            BTREE_WRITE_YIELD_FAMILY ^ table_root as u64,
        );
        conn.set_yield_injector(Some(injector.clone()));

        let mut table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        table_cursor.install_yield_context(&conn);
        let index_def = create_dbsp_state_index(index_root);
        let mut index_cursor =
            BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4).unwrap();
        index_cursor.install_yield_context(&conn);
        let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

        let (op_id, zset_id) = (1i64, 1i64);
        // rowid == elem_id here: ComputeNewRowId assigns last+1.
        let mut victim_rowid = None;
        for elem_id in 1i64..=200 {
            let index_key = vec![
                Value::from_i64(op_id),
                Value::from_i64(zset_id),
                Value::from_i64(elem_id),
            ];
            let record_values = page_filling_record(op_id, zset_id, elem_id);

            let mut wr = WriteRow::new();
            pager
                .io
                .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
                .unwrap();

            if injector.fired() {
                victim_rowid = Some(elem_id);
                break;
            }
        }
        let victim_rowid =
            victim_rowid.expect("no insert ever overflowed a page; test does not exercise the bug");
        conn.set_yield_injector(None);

        // Fresh cursor: the working one may be parked mid-balance.
        let mut verify_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
        let found = pager
            .io
            .block(|| {
                verify_cursor.seek(
                    SeekKey::TableRowId(victim_rowid),
                    SeekOp::GE { eq_only: true },
                )
            })
            .unwrap();
        assert!(
            matches!(found, SeekResult::Found),
            "table row {victim_rowid} lost: WriteRow advanced past a yielded (mid-balance) insert"
        );
    }
}
