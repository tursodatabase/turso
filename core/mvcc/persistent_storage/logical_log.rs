use crate::{
    mvcc::database::LogRecord, turso_assert, types::IOCompletions, Buffer, Completion,
    CompletionError, Result,
};
use std::sync::Arc;

use crate::{types::IOResult, File};

pub struct LogicalLog {
    file: Arc<dyn File>,
    offset: u64,
}

const TOMBSTONE: u8 = 1;
const NOT_TOMBSTONE: u8 = 0;

impl LogicalLog {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<IOResult<()>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&tx.tx_timestamp.to_be_bytes());
        tx.row_versions.iter().for_each(|row_version| {
            let data = &row_version.row.data;
            buffer.extend_from_slice(&row_version.row.id.table_id.to_be_bytes());
            buffer.extend_from_slice(&row_version.row.id.row_id.to_be_bytes());
            if row_version.end.is_some() {
                buffer.extend_from_slice(&TOMBSTONE.to_be_bytes());
            } else {
                buffer.extend_from_slice(&NOT_TOMBSTONE.to_be_bytes());
                buffer.extend_from_slice(&row_version.row.column_count.to_be_bytes());
                buffer.extend_from_slice(data);
            }
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
