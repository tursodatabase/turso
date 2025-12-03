// FIXME: remove this once we add recovery
#![allow(dead_code)]
use crate::{
    io::ReadComplete,
    mvcc::database::{LogRecord, MVTableId, Row, RowID, RowKey, RowVersion, SortableIndexKey},
    storage::sqlite3_ondisk::{read_varint, write_varint_to_vec},
    turso_assert,
    types::{ImmutableRecord, IndexInfo},
    Buffer, Completion, CompletionError, LimboError, Result,
};
use parking_lot::RwLock;
use std::sync::Arc;

use crate::File;

pub const DEFAULT_LOG_CHECKPOINT_THRESHOLD: i64 = -1; // Disabled by default

pub struct LogicalLog {
    pub file: Arc<dyn File>,
    pub offset: u64,
    /// Size at which we start performing a checkpoint on the logical log.
    /// Set to -1 to disable automatic checkpointing.
    checkpoint_threshold: i64,
}

/// Log's Header, this will be the 64 bytes in any logical log file.
/// Log header is 64 bytes at maximum, fields added must not exceed that size. If it doesn't exceed
/// it, any bytes missing will be padded with zeroes.
#[derive(Debug)]
struct LogHeader {
    version: u8,
    salt: u64,
    encrypted: u8, // 0 is no
}

const LOG_HEADER_MAX_SIZE: usize = 64;
const LOG_HEADER_PADDING: [u8; LOG_HEADER_MAX_SIZE] = [0; LOG_HEADER_MAX_SIZE];
/// u64 tx id
/// u64 checksum
/// u64 payload length
const TRANSACTION_HEAD_SIZE: usize = 8 + 8 + 8;

impl LogHeader {
    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        let buffer_size_start = buffer.len();
        buffer.push(self.version);
        buffer.extend_from_slice(&self.salt.to_be_bytes());
        buffer.push(self.encrypted);

        let header_size_before_padding = buffer.len() - buffer_size_start;
        let padding = 64 - header_size_before_padding;
        debug_assert!(header_size_before_padding <= LOG_HEADER_MAX_SIZE);
        buffer.extend_from_slice(&LOG_HEADER_PADDING[0..padding]);
        assert_eq!(buffer.len() - buffer_size_start, LOG_HEADER_MAX_SIZE);
    }
}

impl Default for LogHeader {
    fn default() -> Self {
        Self {
            version: 1,
            salt: 0,
            encrypted: 0,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum LogRecordType {
    DeleteRow = 0,
    InsertRow = 1,
}

impl LogRecordType {
    fn from_row_version(row_version: &RowVersion) -> Self {
        if row_version.end.is_some() {
            Self::DeleteRow
        } else {
            Self::InsertRow
        }
    }

    #[allow(dead_code)]
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(LogRecordType::DeleteRow),
            1 => Some(LogRecordType::InsertRow),
            _ => None,
        }
    }

    fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Serialize a row_version into on disk format.
    /// Format of a "row":
    ///
    /// * table_id (i64, 8 bytes) - always negative for MVCC table IDs
    /// * row_kind (u8, 1 byte) - 0 = table row, 1 = index row
    /// * record_type (u8, 1 byte) - 0 = DeleteRow, 1 = InsertRow
    /// * payload_length (u64, 8 bytes)
    ///
    /// For table rows (row_kind = 0):
    ///   * rowid (varint)
    ///   * data_size (varint)
    ///   * data (bytes)
    ///
    /// For index rows (row_kind = 1):
    ///   * key_data_size (varint)
    ///   * key_data (bytes) - the SortableIndexKey.key.as_blob()
    fn serialize(&self, buffer: &mut Vec<u8>, row_version: &RowVersion) {
        let table_id_i64: i64 = row_version.row.id.table_id.into();
        assert!(
            table_id_i64 < 0,
            "table_id_i64 should be negative, but got {table_id_i64}"
        );
        buffer.extend_from_slice(&table_id_i64.to_be_bytes());

        // Determine row kind: 0 = table row, 1 = index row
        let row_kind = match &row_version.row.id.row_id {
            RowKey::Int(_) => 0u8,
            RowKey::Record(_) => 1u8,
        };
        buffer.push(row_kind);
        buffer.push(self.as_u8());

        let size_before_payload = buffer.len();

        match &row_version.row.id.row_id {
            RowKey::Int(rowid) => {
                // Table row
                write_varint_to_vec(*rowid as u64, buffer);
                let data = row_version.row.payload();
                write_varint_to_vec(data.len() as u64, buffer);
                buffer.extend_from_slice(data);
            }
            RowKey::Record(sortable_key) => {
                // Index row - serialize just the ImmutableRecord bytes (the key data)
                // The IndexInfo metadata will be reconstructed from schema during recovery
                let key_bytes = sortable_key.key.as_blob();
                write_varint_to_vec(key_bytes.len() as u64, buffer);
                buffer.extend_from_slice(key_bytes);
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
        Self {
            file,
            offset: 0,
            checkpoint_threshold: DEFAULT_LOG_CHECKPOINT_THRESHOLD,
        }
    }

    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<Completion> {
        let mut buffer = Vec::new();

        // 1. Serialize log header if it's first write
        let is_first_write = self.offset == 0;
        if is_first_write {
            let header = LogHeader::default();
            header.serialize(&mut buffer);
        }

        // 2. Serialize Transaction
        buffer.extend_from_slice(&tx.tx_timestamp.to_be_bytes());
        // TODO: checksum
        buffer.extend_from_slice(&[0; 8]);
        let buffer_pos_for_rows_size = buffer.len();

        // 3. Serialize rows (both table and index rows)
        tx.row_versions.iter().for_each(|row_version| {
            let row_type = LogRecordType::from_row_version(row_version);
            row_type.serialize(&mut buffer, row_version);
        });

        // 4. Serialize transaction's end marker and rows size. This marker will be the position of the offset
        //    after writing the buffer.
        let rows_size = (buffer.len() - buffer_pos_for_rows_size) as u64;
        buffer.splice(
            buffer_pos_for_rows_size..buffer_pos_for_rows_size,
            rows_size.to_be_bytes(),
        );
        let offset_after_buffer = self.offset + buffer.len() as u64 + size_of::<u64>() as u64;
        buffer.extend_from_slice(&offset_after_buffer.to_be_bytes());

        // 5. Write to disk
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
        Ok(c)
    }

    pub fn sync(&mut self) -> Result<Completion> {
        let completion = Completion::new_sync(move |_| {
            tracing::debug!("logical_log_sync finish");
        });
        let c = self.file.sync(completion)?;
        Ok(c)
    }

    pub fn truncate(&mut self) -> Result<Completion> {
        let completion = Completion::new_trunc(move |result| {
            if let Err(err) = result {
                tracing::error!("logical_log_truncate failed: {}", err);
            }
        });
        let c = self.file.truncate(0, completion)?;
        self.offset = 0;
        Ok(c)
    }

    pub fn should_checkpoint(&self) -> bool {
        if self.checkpoint_threshold < 0 {
            return false;
        }
        self.offset >= self.checkpoint_threshold as u64
    }

    pub fn set_checkpoint_threshold(&mut self, threshold: i64) {
        self.checkpoint_threshold = threshold;
    }

    pub fn checkpoint_threshold(&self) -> i64 {
        self.checkpoint_threshold
    }
}

#[derive(Debug)]
pub enum StreamingResult {
    InsertTableRow { row: Row, rowid: RowID },
    DeleteTableRow { row: Row, rowid: RowID },
    InsertIndexRow { row: Row, rowid: RowID },
    DeleteIndexRow { row: Row, rowid: RowID },
    Eof,
}

#[derive(Clone, Copy, Debug)]
enum StreamingState {
    NeedTransactionStart,
    NeedRow {
        transaction_size: u64,
        transaction_read_bytes: usize,
    },
}

pub struct StreamingLogicalLogReader {
    file: Arc<dyn File>,
    /// Offset to read from file
    pub offset: usize,
    /// Log Header
    header: Option<Arc<LogHeader>>,
    /// Cached buffer after io read
    buffer: Arc<RwLock<Vec<u8>>>,
    /// Position to read from loaded buffer
    buffer_offset: usize,
    file_size: usize,
    state: StreamingState,
}

impl StreamingLogicalLogReader {
    pub fn new(file: Arc<dyn File>) -> Self {
        let file_size = file.size().expect("failed to get file size") as usize;
        Self {
            file,
            offset: 0,
            header: None,
            buffer: Arc::new(RwLock::new(Vec::with_capacity(4096))),
            buffer_offset: 0,
            file_size,
            state: StreamingState::NeedTransactionStart,
        }
    }

    pub fn read_header(&mut self) -> Result<Completion> {
        let header_buf = Arc::new(Buffer::new_temporary(LOG_HEADER_MAX_SIZE));
        let header = Arc::new(RwLock::new(LogHeader::default()));
        let completion: Box<ReadComplete> = Box::new(move |res| {
            let header = header.clone();
            let mut header = header.write();
            let Ok((buf, bytes_read)) = res else {
                tracing::error!("couldn't ready log err={:?}", res,);
                return;
            };
            if bytes_read != LOG_HEADER_MAX_SIZE as i32 {
                tracing::error!(
                    "couldn't ready log header read={}, expected={}",
                    bytes_read,
                    LOG_HEADER_MAX_SIZE
                );
                return;
            }
            let buf = buf.as_slice();
            header.version = buf[0];
            header.salt = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            header.encrypted = buf[10];
            tracing::trace!("LogicalLog header={:?}", header);
        });
        let c = Completion::new_read(header_buf, completion);
        self.offset += LOG_HEADER_MAX_SIZE;
        self.file.pread(0, c)
    }

    /// Reads next record in log.
    /// 1. Read start of transaction
    /// 2. Read next row
    /// 3. Check transaction marker
    pub fn next_record(
        &mut self,
        io: &Arc<dyn crate::IO>,
        mut get_index_info: impl FnMut(MVTableId) -> Result<Arc<IndexInfo>>,
    ) -> Result<StreamingResult> {
        loop {
            match self.state {
                StreamingState::NeedTransactionStart => {
                    if self.is_eof() {
                        return Ok(StreamingResult::Eof);
                    }
                    let _tx_id = self.consume_u64(io)?;
                    let _checksum = self.consume_u64(io)?;
                    let transaction_size = self.consume_u64(io)?;
                    self.state = StreamingState::NeedRow {
                        transaction_size,
                        transaction_read_bytes: 0,
                    };
                }
                StreamingState::NeedRow {
                    transaction_size,
                    transaction_read_bytes,
                } => {
                    if transaction_read_bytes > transaction_size as usize {
                        return Err(LimboError::Corrupt(format!("streaming log read more bytes than expected from a transaction expected={transaction_size} read={transaction_size}")));
                    } else if transaction_size as usize == transaction_read_bytes {
                        // TODO: offset verification
                        let _offset_after = self.consume_u64(io)?;
                        self.state = StreamingState::NeedTransactionStart;
                        continue;
                    }
                    let table_id_i64 = self.consume_i64(io)?;
                    let table_id = MVTableId::from(table_id_i64);
                    let row_kind = self.consume_u8(io)?;
                    let record_type = self.consume_u8(io)?;
                    let _payload_size = self.consume_u64(io)?;
                    let mut bytes_read_on_row = 18; // table_id, row_kind, record_type and payload_size

                    let is_table_row = row_kind == 0;
                    let is_index_row = row_kind == 1;

                    if !is_table_row && !is_index_row {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid row_kind: {row_kind}, expected 0 (table) or 1 (index)"
                        )));
                    }

                    match LogRecordType::from_u8(record_type)
                        .unwrap_or_else(|| panic!("invalid record type: {record_type}"))
                    {
                        LogRecordType::DeleteRow => {
                            if is_table_row {
                                let (rowid, nrowid) = self.consume_varint(io)?;
                                let (payload_size, npayload) = self.consume_varint(io)?;
                                let buffer = self.consume_buffer(io, payload_size as usize)?;
                                let record = ImmutableRecord::from_bin_record(buffer.clone());
                                let column_count = record.column_count();

                                bytes_read_on_row += npayload + nrowid + payload_size as usize;
                                self.state = StreamingState::NeedRow {
                                    transaction_size,
                                    transaction_read_bytes: transaction_read_bytes
                                        + bytes_read_on_row,
                                };
                                let row = Row::new_table_row(
                                    RowID::new(table_id, RowKey::Int(rowid as i64)),
                                    buffer,
                                    column_count,
                                );
                                return Ok(StreamingResult::DeleteTableRow {
                                    rowid: RowID::new(table_id, RowKey::Int(rowid as i64)),
                                    row,
                                });
                            } else {
                                // Index row
                                let (key_size, nkey) = self.consume_varint(io)?;
                                let key_bytes = self.consume_buffer(io, key_size as usize)?;

                                let key_record = ImmutableRecord::from_bin_record(key_bytes);
                                let column_count = key_record.column_count();

                                let index_info = get_index_info(table_id)?;
                                let key = SortableIndexKey::new_from_record(key_record, index_info);

                                bytes_read_on_row += nkey + key_size as usize;
                                self.state = StreamingState::NeedRow {
                                    transaction_size,
                                    transaction_read_bytes: transaction_read_bytes
                                        + bytes_read_on_row,
                                };
                                let row = Row::new_index_row(
                                    RowID::new(table_id, RowKey::Record(key.clone())),
                                    column_count,
                                );
                                return Ok(StreamingResult::DeleteIndexRow {
                                    rowid: RowID::new(table_id, RowKey::Record(key)),
                                    row,
                                });
                            }
                        }
                        LogRecordType::InsertRow => {
                            if is_table_row {
                                let (rowid, nrowid) = self.consume_varint(io)?;
                                let (payload_size, npayload) = self.consume_varint(io)?;
                                let buffer = self.consume_buffer(io, payload_size as usize)?;
                                let record = ImmutableRecord::from_bin_record(buffer.clone());
                                let column_count = record.column_count();

                                bytes_read_on_row += npayload + nrowid + payload_size as usize;
                                self.state = StreamingState::NeedRow {
                                    transaction_size,
                                    transaction_read_bytes: transaction_read_bytes
                                        + bytes_read_on_row,
                                };
                                let row = Row::new_table_row(
                                    RowID::new(table_id, RowKey::Int(rowid as i64)),
                                    buffer,
                                    column_count,
                                );
                                return Ok(StreamingResult::InsertTableRow {
                                    rowid: RowID::new(table_id, RowKey::Int(rowid as i64)),
                                    row,
                                });
                            } else {
                                // Index row
                                let (key_size, nkey) = self.consume_varint(io)?;
                                let key_bytes = self.consume_buffer(io, key_size as usize)?;

                                let key_record = ImmutableRecord::from_bin_record(key_bytes);
                                let column_count = key_record.column_count();

                                let index_info = get_index_info(table_id)?;
                                let key = SortableIndexKey::new_from_record(key_record, index_info);

                                bytes_read_on_row += nkey + key_size as usize;
                                self.state = StreamingState::NeedRow {
                                    transaction_size,
                                    transaction_read_bytes: transaction_read_bytes
                                        + bytes_read_on_row,
                                };
                                let row = Row::new_index_row(
                                    RowID::new(table_id, RowKey::Record(key.clone())),
                                    column_count,
                                );
                                return Ok(StreamingResult::InsertIndexRow {
                                    rowid: RowID::new(table_id, RowKey::Record(key)),
                                    row,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn is_eof(&self) -> bool {
        // if we are positioned at the end of file and we have consumed all bytes
        self.offset >= self.file_size && self.bytes_can_read() == 0
    }

    fn consume_u8(&mut self, io: &Arc<dyn crate::IO>) -> Result<u8> {
        self.read_more_data(io, 1)?;
        let r = self.buffer.read()[self.buffer_offset];
        self.buffer_offset += 1;
        Ok(r)
    }

    fn consume_i64(&mut self, io: &Arc<dyn crate::IO>) -> Result<i64> {
        self.read_more_data(io, 8)?;
        let buf = self.buffer.read();
        let offset = self.buffer_offset;
        let r = i64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        self.buffer_offset += 8;
        Ok(r)
    }

    fn consume_u64(&mut self, io: &Arc<dyn crate::IO>) -> Result<u64> {
        self.read_more_data(io, 8)?;
        let buf = self.buffer.read();
        let offset = self.buffer_offset;
        let r = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        self.buffer_offset += 8;
        Ok(r)
    }

    fn consume_varint(&mut self, io: &Arc<dyn crate::IO>) -> Result<(u64, usize)> {
        self.read_more_data(io, 9)?;
        let buffer_guard = self.buffer.read();
        let buffer = &buffer_guard[self.buffer_offset..];
        let (v, n) = read_varint(buffer)?;
        self.buffer_offset += n;
        Ok((v, n))
    }

    fn consume_buffer(&mut self, io: &Arc<dyn crate::IO>, amount: usize) -> Result<Vec<u8>> {
        self.read_more_data(io, amount)?;
        let buffer = self.buffer.read()[self.buffer_offset..self.buffer_offset + amount].to_vec();
        self.buffer_offset += amount;
        Ok(buffer)
    }

    fn get_buffer(&self) -> parking_lot::RwLockReadGuard<'_, Vec<u8>> {
        self.buffer.read()
    }

    /// Read at least `need` bytes from the logical log, issuing multiple reads if necessary.
    /// If at any point 0 bytes are read, that indicates corruption.
    pub fn read_more_data(&mut self, io: &Arc<dyn crate::IO>, need: usize) -> Result<()> {
        let bytes_can_read = self.bytes_can_read();
        if bytes_can_read >= need {
            return Ok(());
        }

        let initial_buffer_offset = self.buffer_offset;

        loop {
            let buffer_size_before_read = self.buffer.read().len();
            turso_assert!(
                buffer_size_before_read >= self.buffer_offset,
                "buffer_size_before_read={buffer_size_before_read} < buffer_offset={}",
                self.buffer_offset
            );
            let bytes_available_in_buffer = buffer_size_before_read - self.buffer_offset;
            let still_need = need.saturating_sub(bytes_available_in_buffer);

            if still_need == 0 {
                break;
            }

            turso_assert!(
                self.file_size >= self.offset,
                "file_size={} < offset={}",
                self.file_size,
                self.offset
            );
            let to_read = 4096.max(still_need).min(self.file_size - self.offset);

            if to_read == 0 {
                // No more data available in file even though we need more -> corrupt
                return Err(LimboError::Corrupt(format!(
                    "Expected to read {still_need} bytes more but reached end of file at offset {}",
                    self.offset
                )));
            }

            let header_buf = Arc::new(Buffer::new_temporary(to_read));
            let buffer = self.buffer.clone();
            let completion: Box<ReadComplete> = Box::new(move |res| {
                let buffer = buffer.clone();
                let mut buffer = buffer.write();
                let Ok((buf, bytes_read)) = res else {
                    panic!("Logical log read failed: {res:?}");
                };
                let buf = buf.as_slice();
                if bytes_read > 0 {
                    buffer.extend_from_slice(&buf[..bytes_read as usize]);
                }
            });
            let c = Completion::new_read(header_buf, completion);
            let c = self.file.pread(self.offset as u64, c)?;
            io.wait_for_completion(c)?;

            let buffer_size_after_read = self.buffer.read().len();
            let bytes_read = buffer_size_after_read - buffer_size_before_read;

            if bytes_read == 0 {
                return Err(LimboError::Corrupt(format!(
                    "Expected to read {still_need} bytes more but read 0 bytes at offset {}",
                    self.offset
                )));
            }

            self.offset += bytes_read;
        }

        // cleanup consumed bytes
        // this could be better for sure
        let _ = self.buffer.write().drain(0..initial_buffer_offset);
        self.buffer_offset = 0;
        Ok(())
    }

    fn bytes_can_read(&self) -> usize {
        self.buffer.read().len() - self.buffer_offset
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::{rng, Rng};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    use crate::{
        mvcc::database::{
            tests::{commit_tx, generate_simple_string_row, MvccTestDbNoConn},
            Row, RowID, RowKey, SortableIndexKey,
        },
        types::{ImmutableRecord, IndexInfo, Text},
        Value, ValueRef,
    };
    use std::sync::Arc;

    use super::LogRecordType;

    #[test]
    fn test_logical_log_read() {
        // Load a transaction
        // let's not drop db as we don't want files to be removed
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            let pager = conn.pager.load().clone();
            let mvcc_store = db.get_mvcc_store();
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            // insert table id -2 into sqlite_schema table (table_id -1)
            let data = ImmutableRecord::from_values(
                &[
                    Value::Text(Text::new("table")), // type
                    Value::Text(Text::new("test")),  // name
                    Value::Text(Text::new("test")),  // tbl_name
                    Value::Integer(-2),              // rootpage
                    Value::Text(Text::new(
                        "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                    )), // sql
                ],
                5,
            );
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1)),
                        data.as_blob().to_vec(),
                        5,
                    ),
                )
                .unwrap();
            // now insert a row into table -2
            let row = generate_simple_string_row((-2).into(), 1, "foo");
            mvcc_store.insert(tx_id, row).unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        }

        // Restart the database to trigger recovery
        db.restart();

        // Now try to read it back - recovery happens automatically during bootstrap
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        let row = mvcc_store
            .read(tx, RowID::new((-2).into(), RowKey::Int(1)))
            .unwrap()
            .unwrap();
        let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
        let values = record.get_values();
        let foo = values.first().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };
        assert_eq!(foo.as_str(), "foo");
    }

    #[test]
    fn test_logical_log_read_multiple_transactions() {
        let values = (0..100)
            .map(|i| {
                (
                    RowID::new((-2).into(), RowKey::Int(i as i64)),
                    format!("foo_{i}"),
                )
            })
            .collect::<Vec<(RowID, String)>>();
        // let's not drop db as we don't want files to be removed
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            let pager = conn.pager.load().clone();
            let mvcc_store = db.get_mvcc_store();

            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            // insert table id -2 into sqlite_schema table (table_id -1)
            let data = ImmutableRecord::from_values(
                &[
                    Value::Text(Text::new("table")), // type
                    Value::Text(Text::new("test")),  // name
                    Value::Text(Text::new("test")),  // tbl_name
                    Value::Integer(-2),              // rootpage
                    Value::Text(Text::new(
                        "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                    )), // sql
                ],
                5,
            );
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1)),
                        data.as_blob().to_vec(),
                        5,
                    ),
                )
                .unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
            // now insert a row into table -2
            // generate insert per transaction
            for (rowid, value) in &values {
                let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
                let row = generate_simple_string_row(
                    rowid.table_id,
                    rowid.row_id.to_int_or_panic(),
                    value,
                );
                mvcc_store.insert(tx_id, row).unwrap();
                commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
            }
        }

        // Restart the database to trigger recovery
        db.restart();

        // Now try to read it back - recovery happens automatically during bootstrap
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();
        for (rowid, value) in &values {
            let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
            let row = mvcc_store.read(tx, rowid.clone()).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let values = record.get_values();
            let foo = values.first().unwrap();
            let ValueRef::Text(foo) = foo else {
                unreachable!()
            };
            assert_eq!(foo.as_str(), value.as_str());
        }
    }

    #[test]
    fn test_logical_log_read_fuzz() {
        let seed = rng().random();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let num_transactions = rng.next_u64() % 128;
        let mut txns = vec![];
        let mut present_rowids = BTreeSet::new();
        let mut non_present_rowids = BTreeSet::new();
        for _ in 0..num_transactions {
            let num_operations = rng.next_u64() % 8;
            let mut ops = vec![];
            for _ in 0..num_operations {
                let op_type = rng.next_u64() % 2;
                match op_type {
                    0 => {
                        // Generate a positive rowid that fits in i64
                        let row_id = (rng.next_u64() % (i64::MAX as u64)) as i64;
                        let rowid = RowID::new((-2).into(), RowKey::Int(row_id));
                        let row = generate_simple_string_row(
                            rowid.table_id,
                            rowid.row_id.to_int_or_panic(),
                            &format!("row_{row_id}"),
                        );
                        ops.push((LogRecordType::InsertRow, Some(row), rowid.clone()));
                        present_rowids.insert(rowid.clone());
                        non_present_rowids.remove(&rowid);
                        tracing::debug!("insert {rowid:?}");
                    }
                    1 => {
                        if present_rowids.is_empty() {
                            continue;
                        }
                        let row_id_pos = rng.next_u64() as usize % present_rowids.len();
                        let row_id = present_rowids.iter().nth(row_id_pos).unwrap().clone();
                        ops.push((LogRecordType::DeleteRow, None, row_id.clone()));
                        present_rowids.remove(&row_id);
                        non_present_rowids.insert(row_id.clone());
                        tracing::debug!("removed {row_id:?}");
                    }
                    _ => unreachable!(),
                }
            }
            txns.push(ops);
        }
        // let's not drop db as we don't want files to be removed
        let mut db = MvccTestDbNoConn::new_with_random_db();
        let pager = {
            let conn = db.connect();
            let pager = conn.pager.load().clone();
            let mvcc_store = db.get_mvcc_store();

            // insert table id -2 into sqlite_schema table (table_id -1)
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            let data = ImmutableRecord::from_values(
                &[
                    Value::Text(Text::new("table")), // type
                    Value::Text(Text::new("test")),  // name
                    Value::Text(Text::new("test")),  // tbl_name
                    Value::Integer(-2),              // rootpage
                    Value::Text(Text::new(
                        "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                    )), // sql
                ],
                5,
            );
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1)),
                        data.as_blob().to_vec(),
                        5,
                    ),
                )
                .unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

            // insert rows
            for ops in &txns {
                let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
                for (op_type, maybe_row, rowid) in ops {
                    match op_type {
                        LogRecordType::DeleteRow => {
                            mvcc_store.delete(tx_id, rowid.clone()).unwrap();
                        }
                        LogRecordType::InsertRow => {
                            mvcc_store
                                .insert(tx_id, maybe_row.as_ref().unwrap().clone())
                                .unwrap();
                        }
                    }
                }
                commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
            }

            conn.close().unwrap();
            pager
        };

        db.restart();

        // connect after restart should recover log.
        let _conn = db.connect();
        let mvcc_store = db.get_mvcc_store();

        // Check rowids that weren't deleted
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        for present_rowid in present_rowids {
            let row = mvcc_store.read(tx, present_rowid.clone()).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let values = record.get_values();
            let foo = values.first().unwrap();
            let ValueRef::Text(foo) = foo else {
                unreachable!()
            };

            assert_eq!(
                foo.as_str(),
                format!("row_{}", present_rowid.row_id.to_int_or_panic())
            );
        }

        // Check rowids that were deleted
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        for present_rowid in non_present_rowids {
            let row = mvcc_store.read(tx, present_rowid.clone()).unwrap();
            assert!(
                row.is_none(),
                "row {present_rowid:?} should have been removed"
            );
        }
    }

    #[test]
    fn test_logical_log_read_table_and_index_rows() {
        // Test that both table rows and index rows can be read back after recovery
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();

            // Create a table with an index
            conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
                .unwrap();
            conn.execute("CREATE INDEX idx_data ON test(data)").unwrap();

            // Checkpoint to ensure the index has a root_page mapping
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

            // Insert some data - this will create both table rows and index rows in the logical log
            // Don't checkpoint after inserts so they remain in the logical log for recovery testing
            conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
                .unwrap();
            conn.execute("INSERT INTO test(id, data) VALUES (2, 'bar')")
                .unwrap();
            conn.execute("INSERT INTO test(id, data) VALUES (3, 'baz')")
                .unwrap();
        }

        // Restart the database to trigger recovery
        db.restart();

        // Now verify that both table rows and index rows can be read back
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();
        let schema = conn.schema.read();

        // Get the index from schema
        let index = schema
            .get_index("test", "idx_data")
            .expect("Index should exist");
        // Use get_table_id_from_root_page to get the correct index_id (handles both checkpointed and non-checkpointed)
        let index_id = mvcc_store.get_table_id_from_root_page(index.root_page);
        let index_info = Arc::new(IndexInfo::new_from_index(index));

        // Verify table rows can be read
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        for (row_id, expected_data) in [(1, "foo"), (2, "bar"), (3, "baz")] {
            let row = mvcc_store
                .read(tx, RowID::new((-2).into(), RowKey::Int(row_id)))
                .unwrap()
                .expect("Table row should exist");
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let values = record.get_values();
            let data_value = values.get(1).expect("Should have data column");
            let ValueRef::Text(data_text) = data_value else {
                panic!("Data column should be text");
            };
            assert_eq!(data_text.as_str(), expected_data);
        }

        // Verify index rows can be read
        // Note: Index rows are written to the logical log, but we need to construct the correct key format
        // The index key format is (indexed_column_value, table_rowid)
        for (row_id, data_value) in [(1, "foo"), (2, "bar"), (3, "baz")] {
            // Create the index key: (data_value, rowid)
            // The index on data column stores (data_value, table_rowid) as the key
            let key_record = ImmutableRecord::from_values(
                &[
                    Value::Text(Text::new(data_value.to_string())),
                    Value::Integer(row_id),
                ],
                2,
            );
            let sortable_key = SortableIndexKey::new_from_record(key_record, index_info.clone());
            let index_rowid = RowID::new(index_id, RowKey::Record(sortable_key));

            // Use read_from_table_or_index to read the index row
            // This verifies that index rows were properly serialized and deserialized from the logical log
            let index_row_opt = mvcc_store
                .read_from_table_or_index(tx, index_rowid.clone(), Some(index_id))
                .unwrap_or_else(|e| {
                    panic!("Failed to read index row for ({}, {}): {:?}. Index ID: {:?}, root_page: {}", 
                           data_value, row_id, e, index_id, index.root_page)
                });

            let Some(index_row) = index_row_opt else {
                panic!("Index row for ({data_value}, {row_id}) not found after recovery. Index rows should be in the logical log.");
            };
            // Verify the index row contains the correct data
            let RowKey::Record(sortable_key) = index_row.id.row_id else {
                panic!("Index row should have a record row_id");
            };
            let record = sortable_key.key.clone();
            let values = record.get_values();
            assert_eq!(
                values.len(),
                2,
                "Index row should have 2 columns (data, rowid)"
            );
            let ValueRef::Text(index_data) = values[0] else {
                panic!("First index column should be text");
            };
            assert_eq!(index_data.as_str(), data_value, "Index data should match");
            let ValueRef::Integer(index_rowid_val) = values[1] else {
                panic!("Second index column should be integer (rowid)");
            };
            assert_eq!(index_rowid_val, row_id, "Index rowid should match");
        }
    }
}
