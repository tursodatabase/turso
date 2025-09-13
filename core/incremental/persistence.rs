use crate::incremental::operator::{AggregateFunction, AggregateState};
use crate::storage::btree::{BTreeCursor, BTreeKey};
use crate::types::{IOResult, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, Result, Value};

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
                            crate::LimboError::InternalError(format!(
                                "Found key {key:?} in aggregate storage but could not read record"
                            ))
                        })?;
                        let values = r.get_values();
                        let blob = values[1].to_owned();

                        let (state, _group_key) = match blob {
                            Value::Blob(blob) => AggregateState::from_blob(&blob, aggregates)
                                .ok_or_else(|| {
                                    crate::LimboError::InternalError(format!(
                                        "Cannot deserialize aggregate state {blob:?}",
                                    ))
                                }),
                            _ => Err(crate::LimboError::ParseError(
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
    Delete,
    Insert {
        final_weight: isize,
    },
    Done,
}

impl WriteRow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write a row with weight management.
    ///
    /// # Arguments
    /// * `cursor` - BTree cursor for the storage
    /// * `key` - The key to seek (TableRowId)
    /// * `build_record` - Function that builds the record values to insert.
    ///   Takes the final_weight and returns the complete record values.
    /// * `weight` - The weight delta to apply
    pub fn write_row<F>(
        &mut self,
        cursor: &mut BTreeCursor,
        key: SeekKey,
        build_record: F,
        weight: isize,
    ) -> Result<IOResult<()>>
    where
        F: Fn(isize) -> Vec<Value>,
    {
        loop {
            match self {
                WriteRow::GetRecord => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        *self = WriteRow::Insert {
                            final_weight: weight,
                        };
                    } else {
                        let existing_record = return_if_io!(cursor.record());
                        let r = existing_record.ok_or_else(|| {
                            crate::LimboError::InternalError(format!(
                                "Found key {key:?} in storage but could not read record"
                            ))
                        })?;
                        let values = r.get_values();

                        // Weight is always the last value
                        let existing_weight = match values.last() {
                            Some(val) => match val.to_owned() {
                                Value::Integer(w) => w as isize,
                                _ => {
                                    return Err(crate::LimboError::InternalError(format!(
                                        "Invalid weight value in storage for key {key:?}"
                                    )))
                                }
                            },
                            None => {
                                return Err(crate::LimboError::InternalError(format!(
                                    "No weight value found in storage for key {key:?}"
                                )))
                            }
                        };

                        let final_weight = existing_weight + weight;
                        if final_weight <= 0 {
                            *self = WriteRow::Delete
                        } else {
                            *self = WriteRow::Insert { final_weight }
                        }
                    }
                }
                WriteRow::Delete => {
                    // Mark as Done before delete to avoid retry on I/O
                    *self = WriteRow::Done;
                    return_if_io!(cursor.delete());
                }
                WriteRow::Insert { final_weight } => {
                    return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));

                    // Extract the row ID from the key
                    let key_i64 = match key {
                        SeekKey::TableRowId(id) => id,
                        _ => {
                            return Err(crate::LimboError::InternalError(
                                "Expected TableRowId for storage".to_string(),
                            ))
                        }
                    };

                    // Build the record values using the provided function
                    let record_values = build_record(*final_weight);

                    // Create an ImmutableRecord from the values
                    let immutable_record = crate::types::ImmutableRecord::from_values(
                        &record_values,
                        record_values.len(),
                    );
                    let btree_key = BTreeKey::new_table_rowid(key_i64, Some(&immutable_record));

                    // Mark as Done before insert to avoid retry on I/O
                    *self = WriteRow::Done;
                    return_if_io!(cursor.insert(&btree_key));
                }
                WriteRow::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}
