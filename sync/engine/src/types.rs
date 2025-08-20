use serde::{Deserialize, Serialize};

use crate::{errors::Error, Result};

pub type Coro = genawaiter::sync::Co<ProtocolCommand, Result<()>>;

#[derive(Debug, Deserialize, Serialize)]
pub struct DbSyncInfo {
    pub current_generation: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DbSyncStatus {
    pub baton: Option<String>,
    pub status: String,
    pub generation: u64,
    pub max_frame_no: u64,
}

#[derive(Debug)]
pub struct DbChangesStatus {
    pub generation: u64,
    pub max_frame_no: u64,
    pub file_path: String,
}

pub struct DbLocalChangesCount {
    pub cdc_operations: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatabaseChangeType {
    Delete,
    Update,
    Insert,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatabaseMetadata {
    /// Unique identifier of the client - generated on sync startup
    pub client_unique_id: String,
    /// Latest generation from remote which was pulled locally to the Synced DB
    pub synced_generation: u64,
    /// Latest frame number from remote which was pulled locally to the Synced DB
    pub synced_frame_no: Option<u64>,
    /// pair of frame_no for Draft and Synced DB such that content of the database file up to these frames is identical
    pub revert_since_wal_checkpoint_seq: u32,
    pub revert_since_wal_watermark: u64,
}

impl DatabaseMetadata {
    pub fn load(data: &[u8]) -> Result<Self> {
        let meta = serde_json::from_slice::<DatabaseMetadata>(data)?;
        Ok(meta)
    }
    pub fn dump(&self) -> Result<Vec<u8>> {
        let data = serde_json::to_string(self)?;
        Ok(data.into_bytes())
    }
}

/// [DatabaseChange] struct represents data from CDC table as-i
/// (see `turso_cdc_table_columns` definition in turso-core)
#[derive(Clone)]
pub struct DatabaseChange {
    /// Monotonically incrementing change number
    pub change_id: i64,
    /// Unix timestamp of the change (not guaranteed to be strictly monotonic as host clocks can drift)
    pub change_time: u64,
    /// Type of the change
    pub change_type: DatabaseChangeType,
    /// Table of the change
    pub table_name: String,
    /// Rowid of changed row
    pub id: i64,
    /// Binary record of the row before the change, if CDC pragma set to either 'before' or 'full'
    pub before: Option<Vec<u8>>,
    /// Binary record of the row after the change, if CDC pragma set to either 'after' or 'full'
    pub after: Option<Vec<u8>>,
    /// Binary record from "updates" column, if CDC pragma set to 'full'
    pub updates: Option<Vec<u8>>,
}

impl DatabaseChange {
    /// Converts [DatabaseChange] into the operation which effect will be the application of the change
    pub fn into_apply(self) -> Result<DatabaseTapeRowChange> {
        let tape_change = match self.change_type {
            DatabaseChangeType::Delete => DatabaseTapeRowChangeType::Delete {
                before: parse_bin_record(self.before.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'before'".to_string(),
                    )
                })?)?,
            },
            DatabaseChangeType::Update => DatabaseTapeRowChangeType::Update {
                before: parse_bin_record(self.before.ok_or_else(|| {
                    Error::DatabaseTapeError("cdc_mode must be set to 'full'".to_string())
                })?)?,
                after: parse_bin_record(self.after.ok_or_else(|| {
                    Error::DatabaseTapeError("cdc_mode must be set to 'full'".to_string())
                })?)?,
                updates: if let Some(updates) = self.updates {
                    Some(parse_bin_record(updates)?)
                } else {
                    None
                },
            },
            DatabaseChangeType::Insert => DatabaseTapeRowChangeType::Insert {
                after: parse_bin_record(self.after.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'after'".to_string(),
                    )
                })?)?,
            },
        };
        Ok(DatabaseTapeRowChange {
            change_id: self.change_id,
            change_time: self.change_time,
            change: tape_change,
            table_name: self.table_name,
            id: self.id,
        })
    }
    /// Converts [DatabaseChange] into the operation which effect will be the revert of the change
    pub fn into_revert(self) -> Result<DatabaseTapeRowChange> {
        let tape_change = match self.change_type {
            DatabaseChangeType::Delete => DatabaseTapeRowChangeType::Insert {
                after: parse_bin_record(self.before.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'before'".to_string(),
                    )
                })?)?,
            },
            DatabaseChangeType::Update => DatabaseTapeRowChangeType::Update {
                before: parse_bin_record(self.after.ok_or_else(|| {
                    Error::DatabaseTapeError("cdc_mode must be set to 'full'".to_string())
                })?)?,
                after: parse_bin_record(self.before.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'before'".to_string(),
                    )
                })?)?,
                updates: None,
            },
            DatabaseChangeType::Insert => DatabaseTapeRowChangeType::Delete {
                before: parse_bin_record(self.after.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'after'".to_string(),
                    )
                })?)?,
            },
        };
        Ok(DatabaseTapeRowChange {
            change_id: self.change_id,
            change_time: self.change_time,
            change: tape_change,
            table_name: self.table_name,
            id: self.id,
        })
    }
}

impl std::fmt::Debug for DatabaseChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseChange")
            .field("change_id", &self.change_id)
            .field("change_time", &self.change_time)
            .field("change_type", &self.change_type)
            .field("table_name", &self.table_name)
            .field("id", &self.id)
            .field("before.len()", &self.before.as_ref().map(|x| x.len()))
            .field("after.len()", &self.after.as_ref().map(|x| x.len()))
            .finish()
    }
}

impl TryFrom<&turso_core::Row> for DatabaseChange {
    type Error = Error;

    fn try_from(row: &turso_core::Row) -> Result<Self> {
        let change_id = get_core_value_i64(row, 0)?;
        let change_time = get_core_value_i64(row, 1)? as u64;
        let change_type = get_core_value_i64(row, 2)?;
        let table_name = get_core_value_text(row, 3)?;
        let id = get_core_value_i64(row, 4)?;
        let before = get_core_value_blob_or_null(row, 5)?;
        let after = get_core_value_blob_or_null(row, 6)?;
        let updates = get_core_value_blob_or_null(row, 7)?;

        let change_type = match change_type {
            -1 => DatabaseChangeType::Delete,
            0 => DatabaseChangeType::Update,
            1 => DatabaseChangeType::Insert,
            v => {
                return Err(Error::DatabaseTapeError(format!(
                    "unexpected change type: expected -1|0|1, got '{v:?}'"
                )))
            }
        };
        Ok(Self {
            change_id,
            change_time,
            change_type,
            table_name,
            id,
            before,
            after,
            updates,
        })
    }
}

pub enum DatabaseTapeRowChangeType {
    Delete {
        before: Vec<turso_core::Value>,
    },
    Update {
        before: Vec<turso_core::Value>,
        after: Vec<turso_core::Value>,
        updates: Option<Vec<turso_core::Value>>,
    },
    Insert {
        after: Vec<turso_core::Value>,
    },
}

impl From<&DatabaseTapeRowChangeType> for DatabaseChangeType {
    fn from(value: &DatabaseTapeRowChangeType) -> Self {
        match value {
            DatabaseTapeRowChangeType::Delete { .. } => DatabaseChangeType::Delete,
            DatabaseTapeRowChangeType::Update { .. } => DatabaseChangeType::Update,
            DatabaseTapeRowChangeType::Insert { .. } => DatabaseChangeType::Insert,
        }
    }
}

/// [DatabaseTapeOperation] extends [DatabaseTapeRowChange] by adding information about transaction boundary
///
/// This helps [crate::database_tape::DatabaseTapeSession] to properly maintain transaction state and COMMIT or ROLLBACK changes in appropriate time
/// by consuming events from [crate::database_tape::DatabaseChangesIterator]
#[derive(Debug)]
pub enum DatabaseTapeOperation {
    RowChange(DatabaseTapeRowChange),
    Commit,
}

/// [DatabaseTapeRowChange] is the specific operation over single row which can be performed on database
#[derive(Debug)]
pub struct DatabaseTapeRowChange {
    pub change_id: i64,
    pub change_time: u64,
    pub change: DatabaseTapeRowChangeType,
    pub table_name: String,
    pub id: i64,
}

impl std::fmt::Debug for DatabaseTapeRowChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete { before } => f
                .debug_struct("Delete")
                .field("before.len()", &before.len())
                .finish(),
            Self::Update {
                before,
                after,
                updates,
            } => f
                .debug_struct("Update")
                .field("before.len()", &before.len())
                .field("after.len()", &after.len())
                .field("updates.len()", &updates.as_ref().map(|x| x.len()))
                .finish(),
            Self::Insert { after } => f
                .debug_struct("Insert")
                .field("after.len()", &after.len())
                .finish(),
        }
    }
}

fn get_core_value_i64(row: &turso_core::Row, index: usize) -> Result<i64> {
    match row.get_value(index) {
        turso_core::Value::Integer(v) => Ok(*v),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected integer, got '{v:?}'"
        ))),
    }
}

fn get_core_value_text(row: &turso_core::Row, index: usize) -> Result<String> {
    match row.get_value(index) {
        turso_core::Value::Text(x) => Ok(x.to_string()),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected string, got '{v:?}'"
        ))),
    }
}

fn get_core_value_blob_or_null(row: &turso_core::Row, index: usize) -> Result<Option<Vec<u8>>> {
    match row.get_value(index) {
        turso_core::Value::Null => Ok(None),
        turso_core::Value::Blob(x) => Ok(Some(x.clone())),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected blob, got '{v:?}'"
        ))),
    }
}

pub enum ProtocolCommand {
    // Protocol waits for some IO - caller must spin turso-db IO event loop and also drive ProtocolIO
    IO,
}

pub fn parse_bin_record(bin_record: Vec<u8>) -> Result<Vec<turso_core::Value>> {
    let record = turso_core::types::ImmutableRecord::from_bin_record(bin_record);
    let mut cursor = turso_core::types::RecordCursor::new();
    let columns = cursor.count(&record);
    let mut values = Vec::with_capacity(columns);
    for i in 0..columns {
        let value = cursor.get_value(&record, i)?;
        values.push(value.to_owned());
    }
    Ok(values)
}
