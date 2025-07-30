use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{errors::Error, Result};

pub type Coro = genawaiter::sync::Co<ProtocolYield, Result<ProtocolResume>>;

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncInfo {
    pub current_generation: usize,
}

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncStatus {
    pub baton: Option<String>,
    pub status: String,
    pub generation: usize,
    pub max_frame_no: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatabaseChangeType {
    Delete,
    Update,
    Insert,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatabaseMetadata {
    /// Latest generation from remote which was pulled locally to the Synced DB
    pub synced_generation: usize,
    /// Latest frame number from remote which was pulled locally to the Synced DB
    pub synced_frame_no: Option<usize>,
    /// Latest change_id from CDC table in Draft DB which was successfully pushed to the remote through Synced DB
    pub synced_change_id: Option<i64>,
    /// Optional field which will store change_id from CDC table in Draft DB which was successfully transferred to the SyncedDB
    /// but not durably pushed to the remote yet
    ///
    /// This can happen if WAL push will abort in the middle due to network partition, application crash, etc
    pub transferred_change_id: Option<i64>,
    /// pair of frame_no for Draft and Synced DB such that content of the database file up to these frames is identical
    pub draft_wal_match_watermark: u64,
    pub synced_wal_match_watermark: u64,
}

impl DatabaseMetadata {
    pub fn load(data: &[u8]) -> Result<Self> {
        let meta = serde_json::from_slice::<DatabaseMetadata>(&data[..])?;
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
}

impl DatabaseChange {
    /// Converts [DatabaseChange] into the operation which effect will be the application of the change
    pub fn into_apply(self) -> Result<DatabaseTapeRowChange> {
        let tape_change = match self.change_type {
            DatabaseChangeType::Delete => DatabaseTapeRowChangeType::Delete,
            DatabaseChangeType::Update => DatabaseTapeRowChangeType::Update {
                bin_record: self.after.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'after'".to_string(),
                    )
                })?,
            },
            DatabaseChangeType::Insert => DatabaseTapeRowChangeType::Insert {
                bin_record: self.after.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'after'".to_string(),
                    )
                })?,
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
                bin_record: self.before.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'before'".to_string(),
                    )
                })?,
            },
            DatabaseChangeType::Update => DatabaseTapeRowChangeType::Update {
                bin_record: self.before.ok_or_else(|| {
                    Error::DatabaseTapeError(
                        "cdc_mode must be set to either 'full' or 'before'".to_string(),
                    )
                })?,
            },
            DatabaseChangeType::Insert => DatabaseTapeRowChangeType::Delete,
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

impl TryFrom<turso::Row> for DatabaseChange {
    type Error = Error;

    fn try_from(row: turso::Row) -> Result<Self> {
        let change_id = get_value_i64(&row, 0)?;
        let change_time = get_value_i64(&row, 1)? as u64;
        let change_type = get_value_i64(&row, 2)?;
        let table_name = get_value_text(&row, 3)?;
        let id = get_value_i64(&row, 4)?;
        let before = get_value_blob_or_null(&row, 5)?;
        let after = get_value_blob_or_null(&row, 6)?;

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
        })
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
        })
    }
}

pub enum DatabaseTapeRowChangeType {
    Delete,
    Update { bin_record: Vec<u8> },
    Insert { bin_record: Vec<u8> },
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
            Self::Delete => write!(f, "Delete"),
            Self::Update { bin_record } => f
                .debug_struct("Update")
                .field("bin_record.len()", &bin_record.len())
                .finish(),
            Self::Insert { bin_record } => f
                .debug_struct("Insert")
                .field("bin_record.len()", &bin_record.len())
                .finish(),
        }
    }
}

fn get_value(row: &turso::Row, index: usize) -> Result<turso::Value> {
    row.get_value(index).map_err(Error::TursoError)
}

fn get_value_i64(row: &turso::Row, index: usize) -> Result<i64> {
    let v = get_value(row, index)?;
    match v {
        turso::Value::Integer(v) => Ok(v),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected integer, got '{v:?}'"
        ))),
    }
}

fn get_value_text(row: &turso::Row, index: usize) -> Result<String> {
    let v = get_value(row, index)?;
    match v {
        turso::Value::Text(x) => Ok(x),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected string, got '{v:?}'"
        ))),
    }
}

fn get_value_blob_or_null(row: &turso::Row, index: usize) -> Result<Option<Vec<u8>>> {
    let v = get_value(row, index)?;
    match v {
        turso::Value::Null => Ok(None),
        turso::Value::Blob(x) => Ok(Some(x)),
        v => Err(Error::DatabaseTapeError(format!(
            "column {index} type mismatch: expected blob, got '{v:?}'"
        ))),
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

#[derive(Debug)]
pub enum ProtocolYield {
    IO,
    DbInfo,
    DbBootstrap {
        generation: usize,
    },
    WalPush {
        baton: Option<String>,
        generation: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    },
    WalPull {
        generation: usize,
        start_frame: usize,
    },
    ExistsFile {
        path: PathBuf,
    },
    ReadFile {
        path: PathBuf,
    },
    WriteFile {
        path: PathBuf,
        data: Vec<u8>,
    },
    RemoveFile {
        path: PathBuf,
    },
    Continue,
}

#[derive(Debug)]
pub enum ProtocolResume {
    None,
    Exists(bool),
    Content(Option<Vec<u8>>),
    DbInfo { generation: usize },
    DbStatus(DbSyncStatus),
}

impl ProtocolResume {
    pub fn none(self) -> () {
        match self {
            ProtocolResume::None => {}
            v @ _ => panic!(
                "unexpected ProtocolResume value: expected None, got {:?}",
                v
            ),
        }
    }
    pub fn content(self) -> Option<Vec<u8>> {
        match self {
            ProtocolResume::Content(data) => data,
            v @ _ => panic!(
                "unexpected ProtocolResume value: expected Content, got {:?}",
                v
            ),
        }
    }
    pub fn exists(self) -> bool {
        match self {
            ProtocolResume::Exists(data) => data,
            v @ _ => panic!(
                "unexpected ProtocolResume value: expected Exists, got {:?}",
                v
            ),
        }
    }
    pub fn db_info(self) -> usize {
        match self {
            ProtocolResume::DbInfo { generation } => generation,
            v @ _ => panic!(
                "unexpected ProtocolResume value: expected DbInfo, got {:?}",
                v
            ),
        }
    }
}
