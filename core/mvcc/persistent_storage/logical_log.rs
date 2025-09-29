// FIXME: remove this once we add recovery
#![allow(dead_code)]
use crate::{
    io::ReadComplete,
    mvcc::database::{LogRecord, Row, RowID, RowVersion},
    storage::sqlite3_ondisk::{read_varint, write_varint_to_vec},
    turso_assert,
    types::{IOCompletions, ImmutableRecord},
    Buffer, Completion, CompletionError, LimboError, Result,
};
use std::sync::{Arc, RwLock};

use crate::{types::IOResult, File};

pub struct LogicalLog {
    pub file: Arc<dyn File>,
    offset: u64,
    needs_recovery: bool,
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
    /// * Rowid -> varint
    /// * Data size -> varint
    /// * Data -> [u8] (data size length)
    fn serialize(&self, buffer: &mut Vec<u8>, row_version: &RowVersion) {
        buffer.extend_from_slice(&row_version.row.id.table_id.to_be_bytes());
        buffer.extend_from_slice(&self.as_u8().to_be_bytes());
        let size_before_payload = buffer.len();
        match self {
            LogRecordType::DeleteRow => {
                write_varint_to_vec(row_version.row.id.row_id as u64, buffer);
            }
            LogRecordType::InsertRow => {
                write_varint_to_vec(row_version.row.id.row_id as u64, buffer);

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
        let recover = file.size().unwrap() > 0;
        Self {
            file,
            offset: 0,
            needs_recovery: recover,
        }
    }

    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<IOResult<()>> {
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

        // 3. Serialize rows
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
        Ok(IOResult::IO(IOCompletions::Single(c)))
    }

    pub fn sync(&mut self) -> Result<IOResult<()>> {
        let completion = Completion::new_sync(move |_| {
            tracing::debug!("logical_log_sync finish");
        });
        let c = self.file.sync(completion)?;
        Ok(IOResult::IO(IOCompletions::Single(c)))
    }

    pub fn truncate(&mut self) -> Result<IOResult<()>> {
        let completion = Completion::new_trunc(move |result| {
            if let Err(err) = result {
                tracing::error!("logical_log_truncate failed: {}", err);
            }
        });
        let c = self.file.truncate(0, completion)?;
        self.offset = 0;
        Ok(IOResult::IO(IOCompletions::Single(c)))
    }

    pub fn needs_recover(&self) -> bool {
        self.needs_recovery
    }

    pub fn mark_recovered(&mut self) {
        self.needs_recovery = false;
    }
}

pub enum StreamingResult {
    InsertRow { row: Row, rowid: RowID },
    DeleteRow { rowid: RowID },
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
    offset: usize,
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
        let file_size = file.size().unwrap() as usize;
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
            let mut header = header.write().unwrap();
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
            header.salt = u64::from_be_bytes(buf[1..9].try_into().unwrap());
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
    pub fn next_record(&mut self, io: &Arc<dyn crate::IO>) -> Result<StreamingResult> {
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
                    let table_id = self.consume_u64(io)? as i64;
                    let record_type = self.consume_u8(io)?;
                    let _payload_size = self.consume_u64(io)?;
                    let mut bytes_read_on_row = 17; // table_id, record_type and payload_size
                    match LogRecordType::from_u8(record_type).unwrap() {
                        LogRecordType::DeleteRow => {
                            let (rowid, n) = self.consume_varint(io)?;
                            bytes_read_on_row += n;
                            self.state = StreamingState::NeedRow {
                                transaction_size,
                                transaction_read_bytes: transaction_read_bytes + bytes_read_on_row,
                            };
                            return Ok(StreamingResult::DeleteRow {
                                rowid: RowID::new(table_id, rowid as i64),
                            });
                        }
                        LogRecordType::InsertRow => {
                            let (rowid, nrowid) = self.consume_varint(io)?;
                            let (payload_size, npayload) = self.consume_varint(io)?;
                            let buffer = self.consume_buffer(io, payload_size as usize)?;
                            let record = ImmutableRecord::from_bin_record(buffer.clone());
                            let column_count = record.column_count();

                            bytes_read_on_row += npayload + nrowid + payload_size as usize;
                            self.state = StreamingState::NeedRow {
                                transaction_size,
                                transaction_read_bytes: transaction_read_bytes + bytes_read_on_row,
                            };
                            let row =
                                Row::new(RowID::new(table_id, rowid as i64), buffer, column_count);
                            return Ok(StreamingResult::InsertRow {
                                rowid: RowID::new(table_id, rowid as i64),
                                row,
                            });
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
        let r = self.buffer.read().unwrap()[self.buffer_offset];
        self.buffer_offset += 1;
        Ok(r)
    }

    fn consume_u64(&mut self, io: &Arc<dyn crate::IO>) -> Result<u64> {
        self.read_more_data(io, 8)?;
        let r = u64::from_be_bytes(
            self.buffer.read().unwrap()[self.buffer_offset..self.buffer_offset + 8]
                .try_into()
                .unwrap(),
        );
        self.buffer_offset += 8;
        Ok(r)
    }

    fn consume_varint(&mut self, io: &Arc<dyn crate::IO>) -> Result<(u64, usize)> {
        self.read_more_data(io, 9)?;
        let buffer_guard = self.buffer.read().unwrap();
        let buffer = &buffer_guard[self.buffer_offset..];
        let (v, n) = read_varint(buffer)?;
        self.buffer_offset += n;
        Ok((v, n))
    }

    fn consume_buffer(&mut self, io: &Arc<dyn crate::IO>, amount: usize) -> Result<Vec<u8>> {
        self.read_more_data(io, amount)?;
        let buffer =
            self.buffer.read().unwrap()[self.buffer_offset..self.buffer_offset + amount].to_vec();
        self.buffer_offset += amount;
        Ok(buffer)
    }

    fn get_buffer(&self) -> std::sync::RwLockReadGuard<'_, Vec<u8>> {
        self.buffer.read().unwrap()
    }

    pub fn read_more_data(&mut self, io: &Arc<dyn crate::IO>, need: usize) -> Result<()> {
        let bytes_can_read = self.bytes_can_read();
        if bytes_can_read >= need {
            return Ok(());
        }
        let to_read = 4096;
        let to_read = to_read.min(self.file_size - self.offset);
        let header_buf = Arc::new(Buffer::new_temporary(to_read));
        let buffer = self.buffer.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            let buffer = buffer.clone();
            let mut buffer = buffer.write().unwrap();
            let Ok((buf, _bytes_read)) = res else {
                tracing::trace!("couldn't ready log err={:?}", res,);
                return;
            };
            let buf = buf.as_slice();
            buffer.extend_from_slice(buf);
        });
        let c = Completion::new_read(header_buf, completion);
        let c = self.file.pread(self.offset as u64, c)?;
        io.wait_for_completion(c)?;
        self.offset += to_read;
        // cleanup consumed bytes
        // this could be better for sure
        let _ = self.buffer.write().unwrap().drain(0..self.buffer_offset);
        self.buffer_offset = 0;
        Ok(())
    }

    fn bytes_can_read(&self) -> usize {
        self.buffer.read().unwrap().len() - self.buffer_offset
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use rand::{thread_rng, Rng};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    use crate::{
        mvcc::{
            database::{
                tests::{commit_tx, generate_simple_string_row, MvccTestDbNoConn},
                RowID,
            },
            persistent_storage::Storage,
            LocalClock, MvStore,
        },
        types::ImmutableRecord,
        OpenFlags, RefValue,
    };

    use super::LogRecordType;

    #[test]
    fn test_logical_log_read() {
        // Load a transaction
        // let's not drop db as we don't want files to be removed
        let db = MvccTestDbNoConn::new_with_random_db();
        let (io, pager) = {
            let conn = db.connect();
            let pager = conn.pager.read().clone();
            let mvcc_store = db.get_mvcc_store();
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();

            let row = generate_simple_string_row(1, 1, "foo");
            mvcc_store.insert(tx_id, row).unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
            conn.close().unwrap();
            let db = db.get_db();
            (db.io.clone(), pager)
        };

        // Now try to read it back
        let log_file = db.get_log_path();

        let file = io.open_file(log_file, OpenFlags::ReadOnly, false).unwrap();
        let mvcc_store = Arc::new(MvStore::new(LocalClock::new(), Storage::new(file.clone())));
        mvcc_store.recover_logical_log(&io, &pager).unwrap();
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        let row = mvcc_store.read(tx, RowID::new(1, 1)).unwrap().unwrap();
        let record = ImmutableRecord::from_bin_record(row.data.clone());
        let values = record.get_values();
        let foo = values.first().unwrap();
        let RefValue::Text(foo) = foo else {
            unreachable!()
        };
        assert_eq!(foo.as_str(), "foo");
    }

    #[test]
    fn test_logical_log_read_multiple_transactions() {
        let values = (0..100)
            .map(|i| (RowID::new(1, i), format!("foo_{i}")))
            .collect::<Vec<(RowID, String)>>();
        // let's not drop db as we don't want files to be removed
        let db = MvccTestDbNoConn::new_with_random_db();
        let (io, pager) = {
            let conn = db.connect();
            let pager = conn.pager.read().clone();
            let mvcc_store = db.get_mvcc_store();

            // generate insert per transaction
            for (rowid, value) in &values {
                let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
                let row = generate_simple_string_row(rowid.table_id, rowid.row_id, value);
                mvcc_store.insert(tx_id, row).unwrap();
                commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
            }

            conn.close().unwrap();
            let db = db.get_db();
            (db.io.clone(), pager)
        };

        // Now try to read it back
        let log_file = db.get_log_path();

        let file = io.open_file(log_file, OpenFlags::ReadOnly, false).unwrap();
        let mvcc_store = Arc::new(MvStore::new(LocalClock::new(), Storage::new(file.clone())));
        mvcc_store.recover_logical_log(&io, &pager).unwrap();
        for (rowid, value) in &values {
            let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
            let row = mvcc_store.read(tx, *rowid).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.data.clone());
            let values = record.get_values();
            let foo = values.first().unwrap();
            let RefValue::Text(foo) = foo else {
                unreachable!()
            };
            assert_eq!(foo.as_str(), value.as_str());
        }
    }

    #[test]
    fn test_logical_log_read_fuzz() {
        let seed = thread_rng().gen();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let num_transactions = rng.next_u64() % 128;
        let mut txns = vec![];
        let mut present_rowids = HashSet::new();
        let mut non_present_rowids = HashSet::new();
        for _ in 0..num_transactions {
            let num_operations = rng.next_u64() % 8;
            let mut ops = vec![];
            for _ in 0..num_operations {
                let op_type = rng.next_u64() % 2;
                match op_type {
                    0 => {
                        let row_id = rng.next_u64();
                        let rowid = RowID::new(1, row_id as i64);
                        let row = generate_simple_string_row(
                            rowid.table_id,
                            rowid.row_id,
                            &format!("row_{row_id}"),
                        );
                        ops.push((LogRecordType::InsertRow, Some(row), rowid));
                        present_rowids.insert(rowid);
                        non_present_rowids.remove(&rowid);
                        tracing::debug!("insert {rowid:?}");
                    }
                    1 => {
                        if present_rowids.is_empty() {
                            continue;
                        }
                        let row_id_pos = rng.next_u64() as usize % present_rowids.len();
                        let row_id = *present_rowids.iter().nth(row_id_pos).unwrap();
                        ops.push((LogRecordType::DeleteRow, None, row_id));
                        present_rowids.remove(&row_id);
                        non_present_rowids.insert(row_id);
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
            let pager = conn.pager.read().clone();
            let mvcc_store = db.get_mvcc_store();

            for ops in &txns {
                let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
                for (op_type, maybe_row, rowid) in ops {
                    match op_type {
                        LogRecordType::DeleteRow => {
                            mvcc_store.delete(tx_id, *rowid).unwrap();
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
            let row = mvcc_store.read(tx, present_rowid).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.data.clone());
            let values = record.get_values();
            let foo = values.first().unwrap();
            let RefValue::Text(foo) = foo else {
                unreachable!()
            };

            assert_eq!(foo.as_str(), format!("row_{}", present_rowid.row_id as u64));
        }

        // Check rowids that were deleted
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        for present_rowid in non_present_rowids {
            let row = mvcc_store.read(tx, present_rowid).unwrap();
            assert!(
                row.is_none(),
                "row {present_rowid:?} should have been removed"
            );
        }
    }
}
