use crate::{
    mvcc::database::{LogRecord, RowVersion},
    storage::sqlite3_ondisk::{write_varint, write_varint_to_vec},
    turso_assert,
    types::IOCompletions,
    Buffer, Completion, CompletionError, Result,
};
use std::sync::Arc;

use crate::{types::IOResult, File};

pub struct LogicalLog {
    file: Arc<dyn File>,
    offset: u64,
}

const TOMBSTONE: u8 = 1;
const NOT_TOMBSTONE: u8 = 0;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum LogRowType {
    Delete = 0,
    Insert = 1,
}

impl LogRowType {
    fn from_row_version(row_version: &RowVersion) -> Self {
        if row_version.end.is_some() {
            Self::Delete
        } else {
            Self::Insert
        }
    }

    #[allow(dead_code)]
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(LogRowType::Delete),
            1 => Some(LogRowType::Insert),
            _ => None,
        }
    }

    fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Serialize a row_version into on disk format.
    /// Format of a "row" (maybe we could change the name because row is not general enough for
    /// future type of values):
    ///
    /// * table_id (root page) -> u64
    /// * row type -> u8
    ///
    /// (by row type)
    /// Delete:
    /// * Payload length -> u64
    /// * Rowid -> varint
    ///
    /// Insert:
    /// * Payload length -> u64
    /// * Data size -> varint
    /// * Rowid -> varint
    /// * Data -> [u8] (data size length)
    fn serialize(&self, buffer: &mut Vec<u8>, row_version: &RowVersion) {
        buffer.extend_from_slice(&row_version.row.id.table_id.to_be_bytes());
        buffer.extend_from_slice(&self.as_u8().to_be_bytes());
        let size_before_payload = buffer.len();
        match self {
            LogRowType::Delete => {
                write_varint_to_vec(row_version.row.id.row_id as u64, buffer);
            }
            LogRowType::Insert => {
                write_varint_to_vec(row_version.row.id.row_id as u64, buffer);
                write_varint_to_vec(row_version.row.column_count as u64, buffer);

                let data = &row_version.row.data;
                // Maybe this isn't needed? We already might infer data size with payload size
                // anyways.
                write_varint_to_vec(data.len() as u64, buffer);
                buffer.extend_from_slice(data);
            }
        }
        // FIXME: remove shifting of bytes that we do by inserting payload sizes before everything
        // Should payload_size be varint?
        let payload_size = (buffer.len() - size_before_payload) as u64;
        buffer.splice(
            size_before_payload..size_before_payload,
            payload_size.to_be_bytes(),
        );
    }
}

impl LogicalLog {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<IOResult<()>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&tx.tx_timestamp.to_be_bytes());
        tx.row_versions.iter().for_each(|row_version| {
            let row_type = LogRowType::from_row_version(row_version);
            row_type.serialize(&mut buffer, row_version);
        });

        let buffer = Arc::new(Buffer::new(buffer));
        let c = Completion::new_write({
            let buffer = buffer.clone();
            let buffer_len = buffer.len();
            move |res: Result<i32, CompletionError>| {
                let Ok(bytes_written) = res else {
                    return;
                };
                turso_assert!(
                    bytes_written == buffer_len as i32,
                    "wrote({bytes_written}) != expected({buffer_len})"
                );
            }
        });
        let buffer_len = buffer.len();
        let c = self.file.pwrite(self.offset, buffer, c)?;
        self.offset += buffer_len as u64;
        Ok(IOResult::IO(IOCompletions::Single(c)))
    }

    pub fn sync(&mut self) -> Result<IOResult<()>> {
        let completion = Completion::new_sync(move |_| {
            tracing::debug!("logical_log_sync finish");
        });
        let c = self.file.sync(completion)?;
        Ok(IOResult::IO(IOCompletions::Single(c)))
    }
}
