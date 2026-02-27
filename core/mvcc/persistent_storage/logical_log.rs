//! MVCC logical log: file format, recovery rules, and durability contract.
//!
//! ## What this file is for
//!
//! The logical log stores committed MVCC operations that are not checkpointed into the main
//! SQLite database file yet. On restart, recovery replays those operations.
//!
//! In normal operation:
//! - commits append transaction frames to `.db-log`;
//! - checkpoint copies data into the DB file, then truncates `.db-log` to 0.
//!
//! ## File layout
//!
//! A logical log file has:
//! - one fixed-size header (`LOG_HDR_SIZE = 56` bytes), then
//! - zero or more transaction frames.
//!
//! ### Header fields (56 bytes, little-endian)
//! - `magic: u32` (`LOG_MAGIC`)
//! - `version: u8` (`LOG_VERSION`)
//! - `flags: u8` (bits 1..7 must be zero; bit 0 is currently reserved/ignored)
//! - `hdr_len: u16` (`>= 56`)
//! - `salt: u64` (random salt, regenerated on each log truncation)
//! - `reserved: [u8; 36]` (must be zero for current format)
//! - `hdr_crc32c: u32` (CRC32C of the header with this field zeroed)
//!
//! ### Transaction frame
//!
//! Header (`TX_HEADER_SIZE = 16`):
//! - `frame_magic: u32` (`FRAME_MAGIC`)
//! - `op_count: u32`
//! - `commit_ts: u64`
//!
//! Payload:
//! - `op_count` operation entries:
//!   - `tag: u8` (`OP_*`)
//!   - `flags: u8` (`OP_FLAG_BTREE_RESIDENT` currently defined)
//!   - `table_id: i32` (must be negative)
//!   - `payload_len: sqlite varint`
//!   - `payload: [u8; payload_len]`
//!
//! Trailer (`TX_TRAILER_SIZE = 16`):
//! - `payload_size: u64` (total bytes of all op entries)
//! - `crc32c: u32` (chained CRC32C: `crc32c_append(prev_frame_crc, tx_header || payload)`;
//!   the first frame uses `crc32c(salt.to_le_bytes())` as its seed)
//! - `end_magic: u32` (`END_MAGIC`)
//!
//! ## Operation encoding
//!
//! - `OP_UPSERT_TABLE`: `rowid_varint || table_record_bytes`
//! - `OP_DELETE_TABLE`: `rowid_varint`
//! - `OP_UPSERT_INDEX`: serialized index key record
//! - `OP_DELETE_INDEX`: serialized index key record
//!
//! `OP_FLAG_BTREE_RESIDENT` means the row existed in the B-tree before MVCC started tracking it.
//! Recovery preserves this bit because checkpoint/GC logic depends on it.
//!
//! ## Validation behavior
//!
//! The read path (`parse_next_transaction`) performs strict structural validation (header/trailer
//! fields, reserved bits, table-id sign, op payload shape) plus chained CRC verification.
//!
//! Validation is availability-focused, mirroring SQLite WAL prefix semantics:
//! - torn/incomplete tail at end-of-file is accepted as EOF (previous validated frames remain);
//! - first invalid frame encountered during forward scan is treated as an invalid tail and ignored;
//! - only header corruption fails closed.
//!
//! ## Recovery behavior
//!
//! Recovery (reader + MVCC replay) does this:
//! - validates header first (empty/0-byte file treated as no log);
//! - accepts a valid header with no frames (size `<= LOG_HDR_SIZE`);
//! - reads `persistent_tx_ts_max` from `__turso_internal_mvcc_meta` (the durable replay boundary);
//! - streams frames in commit order until first torn tail;
//! - applies only validated frames whose `commit_ts > persistent_tx_ts_max`;
//! - sets clock to `max(persistent_tx_ts_max, max_replayed_commit_ts) + 1`;
//! - restores writer offset to `last_valid_offset` so torn-tail bytes are overwritten.
//!
//! ## Durability and checkpoint ordering
//!
//! Commit durability:
//! - Append completion must succeed.
//! - Fsync behavior depends on sync mode (`Full` fsyncs per commit; lower modes may defer).
//!
//! Checkpoint ordering (enforced by checkpoint state machine):
//! 1. write committed MVCC versions into pager (WAL);
//! 2. commit pager transaction (data + metadata row in same WAL txn);
//! 3. checkpoint WAL pages into DB file;
//! 4. fsync DB file (unless `SyncMode::Off`);
//! 5. truncate logical log to 0 (regenerates salt in memory; header written with next frame);
//! 6. fsync logical log (unless `SyncMode::Off`);
//! 7. truncate WAL last.
//!
//! WAL-last is intentional: if crash happens mid-checkpoint, WAL remains a safety net until
//! logical-log cleanup is complete.
//!
//! ## Non-goal
//!
//! Frame-level atomicity only: torn tails are discarded; partially written frames are not salvaged.
#![allow(dead_code)]

use crate::io::FileSyncType;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::turso_assert;
use crate::{
    io::ReadComplete,
    mvcc::database::{LogRecord, MVTableId, Row, RowID, RowKey, RowVersion, SortableIndexKey},
    storage::sqlite3_ondisk::{read_varint, varint_len, write_varint_to_vec, DatabaseHeader},
    types::IndexInfo,
    Buffer, Completion, CompletionError, LimboError, Result,
};

use crate::File;

/// Logical log size in bytes at which a committing transaction will trigger a checkpoint.
/// Default to the size of 1000 SQLite WAL frames; disable by setting a negative value.
pub const DEFAULT_LOG_CHECKPOINT_THRESHOLD: i64 = 4120 * 1000;

const LOG_MAGIC: u32 = 0x4C4D4C32; // "LML2" in LE
const LOG_VERSION: u8 = 2;
pub const LOG_HDR_SIZE: usize = 56;
const LOG_HDR_SALT_START: usize = 8;
const LOG_HDR_SALT_SIZE: usize = 8;
const LOG_HDR_RESERVED_START: usize = LOG_HDR_SALT_START + LOG_HDR_SALT_SIZE; // 16
const LOG_HDR_CRC_START: usize = 52;
const LOG_HDR_RESERVED_SIZE: usize = LOG_HDR_CRC_START - LOG_HDR_RESERVED_START; // 36
const FRAME_MAGIC: u32 = 0x5854564D; // "MVTX" in LE
const END_MAGIC: u32 = 0x4554564D; // "MVTE" in LE

const OP_UPSERT_TABLE: u8 = 0;
const OP_DELETE_TABLE: u8 = 1;
const OP_UPSERT_INDEX: u8 = 2;
const OP_DELETE_INDEX: u8 = 3;
/// Frame-local database-header mutation (payload = serialized `DatabaseHeader`).
const OP_UPDATE_HEADER: u8 = 4;

const OP_FLAG_BTREE_RESIDENT: u8 = 1 << 0;

const TX_HEADER_SIZE: usize = 16;
const TX_TRAILER_SIZE: usize = 16;
const TX_MIN_FRAME_SIZE: usize = TX_HEADER_SIZE + TX_TRAILER_SIZE;

/// Log's Header, the first 56 bytes of any logical log file.
#[derive(Clone, Debug)]
pub(crate) struct LogHeader {
    version: u8,
    flags: u8,
    hdr_len: u16,
    pub(crate) salt: u64,
    hdr_crc32c: u32,
    reserved: [u8; LOG_HDR_RESERVED_SIZE],
}

impl LogHeader {
    pub(crate) fn new(io: &Arc<dyn crate::IO>) -> Self {
        Self {
            version: LOG_VERSION,
            flags: 0,
            hdr_len: LOG_HDR_SIZE as u16,
            salt: io.generate_random_number() as u64,
            hdr_crc32c: 0,
            reserved: [0; LOG_HDR_RESERVED_SIZE],
        }
    }

    fn encode(&self) -> [u8; LOG_HDR_SIZE] {
        let mut buf = [0u8; LOG_HDR_SIZE];
        buf[0..4].copy_from_slice(&LOG_MAGIC.to_le_bytes());
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..8].copy_from_slice(&self.hdr_len.to_le_bytes());
        buf[LOG_HDR_SALT_START..LOG_HDR_SALT_START + LOG_HDR_SALT_SIZE]
            .copy_from_slice(&self.salt.to_le_bytes());
        buf[LOG_HDR_RESERVED_START..LOG_HDR_CRC_START].copy_from_slice(&self.reserved);

        let crc = crc32c::crc32c(&buf);
        buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < LOG_HDR_SIZE {
            return Err(LimboError::Corrupt(
                "Logical log header too small".to_string(),
            ));
        }
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != LOG_MAGIC {
            return Err(LimboError::Corrupt("Invalid logical log magic".to_string()));
        }
        let version = buf[4];
        if version != LOG_VERSION {
            return Err(LimboError::Corrupt(format!(
                "Unsupported logical log version {version}"
            )));
        }
        let flags = buf[5];
        if flags & 0b1111_1110 != 0 {
            return Err(LimboError::Corrupt(
                "Invalid logical log header flags".to_string(),
            ));
        }
        let hdr_len = u16::from_le_bytes([buf[6], buf[7]]);
        if hdr_len as usize != LOG_HDR_SIZE {
            return Err(LimboError::Corrupt(format!(
                "Invalid logical log header length {hdr_len}"
            )));
        }
        if buf.len() < hdr_len as usize {
            return Err(LimboError::Corrupt(
                "Logical log header shorter than hdr_len".to_string(),
            ));
        }
        let hdr_crc32c = u32::from_le_bytes([
            buf[LOG_HDR_CRC_START],
            buf[LOG_HDR_CRC_START + 1],
            buf[LOG_HDR_CRC_START + 2],
            buf[LOG_HDR_CRC_START + 3],
        ]);
        let mut crc_buf = [0u8; LOG_HDR_SIZE];
        crc_buf.copy_from_slice(&buf[..LOG_HDR_SIZE]);
        crc_buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
        let expected_crc = crc32c::crc32c(&crc_buf);
        if expected_crc != hdr_crc32c {
            return Err(LimboError::Corrupt(
                "Logical log header checksum mismatch".to_string(),
            ));
        }

        let salt = u64::from_le_bytes([
            buf[LOG_HDR_SALT_START],
            buf[LOG_HDR_SALT_START + 1],
            buf[LOG_HDR_SALT_START + 2],
            buf[LOG_HDR_SALT_START + 3],
            buf[LOG_HDR_SALT_START + 4],
            buf[LOG_HDR_SALT_START + 5],
            buf[LOG_HDR_SALT_START + 6],
            buf[LOG_HDR_SALT_START + 7],
        ]);

        let mut reserved = [0u8; LOG_HDR_RESERVED_SIZE];
        reserved.copy_from_slice(&buf[LOG_HDR_RESERVED_START..LOG_HDR_CRC_START]);
        if reserved.iter().any(|b| *b != 0) {
            return Err(LimboError::Corrupt(
                "Logical log header reserved bytes must be zero".to_string(),
            ));
        }

        Ok(Self {
            version,
            flags,
            hdr_len,
            salt,
            hdr_crc32c,
            reserved,
        })
    }
}

/// Derives the initial CRC seed from the header salt.
/// The salt is mixed into a 32-bit CRC state that seeds the first frame's checksum.
fn derive_initial_crc(salt: u64) -> u32 {
    crc32c::crc32c(&salt.to_le_bytes())
}

pub struct LogicalLog {
    pub file: Arc<dyn File>,
    io: Arc<dyn crate::IO>,
    pub offset: u64,
    write_buf: Vec<u8>,
    header: Option<LogHeader>,
    /// Running CRC state for chained checksums. Seeded from the header salt;
    /// updated after each committed frame. The next frame's CRC is computed as
    /// `crc32c_append(running_crc, frame_bytes)`.
    pub running_crc: u32,
    /// Pending CRC from a deferred-offset write. Applied by
    /// `advance_offset_after_success` so that an abandoned write
    /// doesn't corrupt the chain.
    pending_running_crc: Option<u32>,
}

impl LogicalLog {
    pub fn new(file: Arc<dyn File>, io: Arc<dyn crate::IO>) -> Self {
        Self {
            file,
            io,
            offset: 0,
            write_buf: Vec::new(),
            header: None,
            running_crc: 0,
            pending_running_crc: None,
        }
    }

    pub(crate) fn set_header(&mut self, header: LogHeader) {
        self.running_crc = derive_initial_crc(header.salt);
        self.header = Some(header);
    }

    pub(crate) fn header(&self) -> Option<&LogHeader> {
        self.header.as_ref()
    }

    /// Serializes a transaction record and writes it to the log file.
    /// `advance_offset_immediately`: when true, the writer offset advances right after
    /// issuing the pwrite (used by `log_tx` for fire-and-forget appends). When false,
    /// the offset stays behind until the caller confirms success via
    /// `advance_offset_after_success` (used by `log_tx_deferred_offset` for two-phase
    /// commit, where the offset must not advance if the commit is later aborted).
    fn serialize_and_pwrite_tx(
        &mut self,
        tx: &LogRecord,
        advance_offset_immediately: bool,
    ) -> Result<(Completion, u64)> {
        self.write_buf.clear();

        // 1. Serialize log header if it's first write
        let is_first_write = self.offset == 0;
        if is_first_write {
            if self.header.is_none() {
                let header = LogHeader::new(&self.io);
                self.running_crc = derive_initial_crc(header.salt);
                self.header = Some(header);
            }
            let header_bytes = self.header.as_ref().unwrap().encode();
            self.write_buf.extend_from_slice(&header_bytes);
        }

        // 2. Serialize Transaction header
        // A header-only transaction is encoded as a single OP_UPDATE_HEADER op.
        let op_count = u32::try_from(tx.row_versions.len() + usize::from(tx.header.is_some()))
            .map_err(|_| {
                LimboError::InternalError("Logical log op_count exceeds u32".to_string())
            })?;
        let tx_header_start = self.write_buf.len();
        self.write_buf.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
        self.write_buf.extend_from_slice(&op_count.to_le_bytes());
        self.write_buf
            .extend_from_slice(&tx.tx_timestamp.to_le_bytes());

        let payload_start = self.write_buf.len();

        // 3. Serialize ops (both table and index rows)
        for row_version in &tx.row_versions {
            serialize_op_entry(&mut self.write_buf, row_version)?;
        }
        if let Some(header) = tx.header {
            serialize_header_entry(&mut self.write_buf, &header);
        }

        let payload_end = self.write_buf.len();
        let payload_size = (payload_end - payload_start) as u64;
        // Chained CRC: seed from running_crc (derived from salt, or previous frame's CRC)
        let crc = crc32c::crc32c_append(
            self.running_crc,
            &self.write_buf[tx_header_start..payload_end],
        );

        // 4. Serialize trailer
        self.write_buf
            .extend_from_slice(&payload_size.to_le_bytes());
        self.write_buf.extend_from_slice(&crc.to_le_bytes());
        self.write_buf.extend_from_slice(&END_MAGIC.to_le_bytes());

        // 5. Write to disk
        let buffer = Arc::new(Buffer::new(self.write_buf.clone()));
        let c = Completion::new_write({
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
        if advance_offset_immediately {
            self.offset += buffer_len as u64;
            self.running_crc = crc;
        } else {
            self.pending_running_crc = Some(crc);
        }
        Ok((c, buffer_len as u64))
    }

    /// Writes a transaction to the log and immediately advances the writer offset.
    /// Used for checkpoint-initiated writes where no two-phase commit is needed.
    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<Completion> {
        let (c, _) = self.serialize_and_pwrite_tx(tx, true)?;
        Ok(c)
    }

    /// Writes a transaction to the log but does NOT advance the writer offset.
    /// Returns `(completion, bytes_written)`. The caller must call
    /// `advance_offset_after_success(bytes)` after confirming the commit succeeded.
    /// Used by the MVCC commit path where the offset must not advance if the
    /// transaction is later aborted (the un-advanced bytes get overwritten by the next write).
    pub fn log_tx_deferred_offset(&mut self, tx: &LogRecord) -> Result<(Completion, u64)> {
        self.serialize_and_pwrite_tx(tx, false)
    }

    pub fn advance_offset_after_success(&mut self, bytes: u64) {
        self.offset = self
            .offset
            .checked_add(bytes)
            .expect("logical log offset overflow");
        self.running_crc = self
            .pending_running_crc
            .take()
            .expect("advance_offset_after_success called without pending deferred write");
    }

    pub fn sync(&mut self, sync_type: FileSyncType) -> Result<Completion> {
        let completion = Completion::new_sync(move |_| {
            tracing::debug!("logical_log_sync finish");
        });
        let c = self.file.sync(completion, sync_type)?;
        Ok(c)
    }

    fn current_or_new_header(&self) -> Result<LogHeader> {
        if let Some(header) = self.header.clone() {
            return Ok(header);
        }
        if self.offset == 0 {
            // Valid path: checkpoint can run before the first logical-log append.
            return Ok(LogHeader::new(&self.io));
        }
        Err(LimboError::InternalError(
            "Logical log header not initialized".to_string(),
        ))
    }

    fn write_header(&mut self, mut header: LogHeader) -> Result<Completion> {
        let header_bytes = header.encode();
        header.hdr_crc32c = u32::from_le_bytes([
            header_bytes[LOG_HDR_CRC_START],
            header_bytes[LOG_HDR_CRC_START + 1],
            header_bytes[LOG_HDR_CRC_START + 2],
            header_bytes[LOG_HDR_CRC_START + 3],
        ]);
        self.header = Some(header);

        let buffer = Arc::new(Buffer::new(header_bytes.to_vec()));
        let c = Completion::new_write({
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
        self.file.pwrite(0, buffer, c)
    }

    pub fn update_header(&mut self) -> Result<Completion> {
        let header = self.current_or_new_header()?;
        self.write_header(header)
    }

    pub fn truncate(&mut self) -> Result<Completion> {
        // Regenerate salt so stale frames (from before truncation) cannot validate
        // against the new CRC chain.
        let mut header = self.current_or_new_header()?;
        header.salt = self.io.generate_random_number() as u64;
        self.running_crc = derive_initial_crc(header.salt);
        self.pending_running_crc = None;
        self.header = Some(header);

        let completion = Completion::new_trunc(move |result| {
            if let Err(err) = result {
                tracing::error!("logical_log_truncate failed: {}", err);
            }
        });
        let c = self.file.truncate(0, completion)?;
        self.offset = 0;
        Ok(c)
    }
}

fn serialize_op_entry(buffer: &mut Vec<u8>, row_version: &RowVersion) -> Result<()> {
    let is_delete = row_version.end.is_some();
    let tag = match (&row_version.row.id.row_id, is_delete) {
        (RowKey::Int(_), false) => OP_UPSERT_TABLE,
        (RowKey::Int(_), true) => OP_DELETE_TABLE,
        (RowKey::Record(_), false) => OP_UPSERT_INDEX,
        (RowKey::Record(_), true) => OP_DELETE_INDEX,
    };

    let mut flags = 0u8;
    if row_version.btree_resident {
        flags |= OP_FLAG_BTREE_RESIDENT;
    }

    let table_id_i64: i64 = row_version.row.id.table_id.into();
    turso_assert!(
        table_id_i64 < 0,
        "table_id_i64 should be negative, but got {table_id_i64}"
    );
    turso_assert!(
        (i32::MIN as i64..=i32::MAX as i64).contains(&table_id_i64),
        "table_id_i64 out of i32 range: {table_id_i64}"
    );
    let table_id_i32 = table_id_i64 as i32;

    buffer.push(tag);
    buffer.push(flags);
    buffer.extend_from_slice(&table_id_i32.to_le_bytes());

    match tag {
        OP_UPSERT_TABLE => {
            let RowKey::Int(rowid) = row_version.row.id.row_id else {
                unreachable!("table ops must have RowKey::Int")
            };
            let record_bytes = row_version.row.payload();
            let rowid_u64 = rowid as u64;
            let rowid_len = varint_len(rowid_u64);
            let payload_len = rowid_len + record_bytes.len();
            write_varint_to_vec(payload_len as u64, buffer);
            write_varint_to_vec(rowid_u64, buffer);
            buffer.extend_from_slice(record_bytes);
        }
        OP_DELETE_TABLE => {
            let RowKey::Int(rowid) = row_version.row.id.row_id else {
                unreachable!("table ops must have RowKey::Int")
            };
            let rowid_u64 = rowid as u64;
            let rowid_len = varint_len(rowid_u64);
            write_varint_to_vec(rowid_len as u64, buffer);
            write_varint_to_vec(rowid_u64, buffer);
        }
        OP_UPSERT_INDEX | OP_DELETE_INDEX => {
            let key_bytes = row_version.row.payload();
            write_varint_to_vec(key_bytes.len() as u64, buffer);
            buffer.extend_from_slice(key_bytes);
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "invalid logical log op tag: {tag}"
            )));
        }
    }

    Ok(())
}

fn serialize_header_entry(buffer: &mut Vec<u8>, header: &DatabaseHeader) {
    // Header op uses tag-only addressing (table_id=0, flags=0) and fixed payload length.
    buffer.push(OP_UPDATE_HEADER);
    buffer.push(0);
    buffer.extend_from_slice(&0i32.to_le_bytes());
    write_varint_to_vec(DatabaseHeader::SIZE as u64, buffer);
    buffer.extend_from_slice(bytemuck::bytes_of(header));
}

#[derive(Debug)]
pub enum StreamingResult {
    UpsertTableRow {
        row: Row,
        rowid: RowID,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteTableRow {
        rowid: RowID,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpsertIndexRow {
        row: Row,
        rowid: RowID,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteIndexRow {
        row: Row,
        rowid: RowID,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpdateHeader {
        header: DatabaseHeader,
        commit_ts: u64,
    },
    Eof,
}

#[derive(Clone, Copy, Debug)]
enum StreamingState {
    NeedTransactionStart,
}

/// Result of attempting to read and validate the logical log file header.
#[derive(Debug, Clone)]
pub(crate) enum HeaderReadResult {
    /// Header is well-formed: magic, version, flags, reserved, and CRC all valid.
    Valid(LogHeader),
    /// File is smaller than `LOG_HDR_SIZE` — no log exists (first run or truncated to zero).
    NoLog,
    /// Header exists but is corrupt (bad magic, version, flags, CRC, non-zero reserved, or truncated).
    Invalid,
}

pub struct StreamingLogicalLogReader {
    file: Arc<dyn File>,
    /// Offset to read from file
    pub offset: usize,
    /// Log Header
    header: Option<LogHeader>,
    /// Cached buffer after io read
    buffer: Arc<RwLock<Vec<u8>>>,
    /// Position to read from loaded buffer
    buffer_offset: usize,
    file_size: usize,
    state: StreamingState,
    /// Buffer of parsed ops from the current transaction frame. `parse_next_transaction`
    /// fills this; `next_record` drains one op at a time. Empty between transactions.
    pending_ops: std::collections::VecDeque<ParsedOp>,
    /// Byte offset of the end of the last fully validated transaction frame. Used during
    /// recovery to set the writer offset so that torn-tail bytes are overwritten on next append.
    last_valid_offset: usize,
    /// Running CRC state for chained checksum validation. Seeded from the header salt;
    /// updated after each successfully validated frame.
    running_crc: u32,
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
            pending_ops: std::collections::VecDeque::new(),
            last_valid_offset: 0,
            running_crc: 0,
        }
    }

    pub(crate) fn header(&self) -> Option<&LogHeader> {
        self.header.as_ref()
    }

    /// Returns the byte offset just past the last fully validated transaction frame.
    /// After recovery, the log writer should resume from this offset so any torn-tail
    /// bytes beyond it are overwritten by the next append.
    pub fn last_valid_offset(&self) -> usize {
        self.last_valid_offset
    }

    /// Returns the running CRC state after all validated frames. Used during recovery
    /// to hand off the chain state to the writer so it can continue appending.
    pub fn running_crc(&self) -> u32 {
        self.running_crc
    }

    pub fn read_header(&mut self, io: &Arc<dyn crate::IO>) -> Result<()> {
        match self.try_read_header(io)? {
            HeaderReadResult::Valid(_) => Ok(()),
            HeaderReadResult::NoLog => Err(LimboError::Corrupt(
                "Logical log header incomplete".to_string(),
            )),
            HeaderReadResult::Invalid => Err(LimboError::Corrupt(
                "Logical log header corrupt".to_string(),
            )),
        }
    }

    pub(crate) fn try_read_header(&mut self, io: &Arc<dyn crate::IO>) -> Result<HeaderReadResult> {
        self.file_size = self.file.size()? as usize;
        if self.file_size < LOG_HDR_SIZE {
            return Ok(HeaderReadResult::NoLog);
        }

        let header_bytes = self.read_exact_at(io, 0, LOG_HDR_SIZE)?;
        let hdr_len = u16::from_le_bytes([header_bytes[6], header_bytes[7]]) as usize;
        if hdr_len != LOG_HDR_SIZE {
            self.set_invalid_header_state();
            return Ok(HeaderReadResult::Invalid);
        }

        match LogHeader::decode(&header_bytes) {
            Ok(header) => {
                self.running_crc = derive_initial_crc(header.salt);
                self.header = Some(header.clone());
                self.offset = hdr_len;
                self.buffer.write().clear();
                self.buffer_offset = 0;
                self.last_valid_offset = hdr_len;
                Ok(HeaderReadResult::Valid(header))
            }
            Err(LimboError::Corrupt(_)) => {
                self.set_invalid_header_state();
                Ok(HeaderReadResult::Invalid)
            }
            Err(err) => Err(err),
        }
    }

    fn set_invalid_header_state(&mut self) {
        self.header = None;
        self.offset = LOG_HDR_SIZE;
        self.buffer.write().clear();
        self.buffer_offset = 0;
        self.last_valid_offset = LOG_HDR_SIZE;
    }

    /// Reads next record in log.
    pub fn next_record(
        &mut self,
        io: &Arc<dyn crate::IO>,
        mut get_index_info: impl FnMut(MVTableId) -> Result<Arc<IndexInfo>>,
    ) -> Result<StreamingResult> {
        if let Some(op) = self.pending_ops.pop_front() {
            return self.parsed_op_to_streaming(op, &mut get_index_info);
        }

        loop {
            match self.state {
                StreamingState::NeedTransactionStart => {
                    if self.remaining_bytes() < TX_MIN_FRAME_SIZE {
                        return Ok(StreamingResult::Eof);
                    }

                    let ops = match self.parse_next_transaction(io)? {
                        ParseResult::Ops(ops) => ops,
                        ParseResult::Eof | ParseResult::InvalidFrame => {
                            return Ok(StreamingResult::Eof);
                        }
                    };

                    if ops.is_empty() {
                        continue;
                    }
                    self.pending_ops = ops.into();
                    let op = self
                        .pending_ops
                        .pop_front()
                        .expect("ops queue should not be empty");
                    return self.parsed_op_to_streaming(op, &mut get_index_info);
                }
            }
        }
    }

    pub fn is_eof(&self) -> bool {
        self.remaining_bytes() == 0
    }

    fn parse_next_transaction(&mut self, io: &Arc<dyn crate::IO>) -> Result<ParseResult> {
        if self.remaining_bytes() < TX_MIN_FRAME_SIZE {
            return Ok(ParseResult::Eof);
        }
        let frame_start = self.offset.saturating_sub(self.bytes_can_read());

        let header_bytes = match self.try_consume_fixed::<TX_HEADER_SIZE>(io)? {
            Some(bytes) => bytes,
            None => return Ok(ParseResult::Eof),
        };

        let frame_magic = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);
        if frame_magic != FRAME_MAGIC {
            self.last_valid_offset = frame_start;
            return Ok(ParseResult::InvalidFrame);
        }
        let op_count = u32::from_le_bytes([
            header_bytes[4],
            header_bytes[5],
            header_bytes[6],
            header_bytes[7],
        ]);
        let commit_ts = u64::from_le_bytes([
            header_bytes[8],
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
            header_bytes[12],
            header_bytes[13],
            header_bytes[14],
            header_bytes[15],
        ]);

        // Chained CRC: seed from running_crc (derived from salt, or previous frame's CRC)
        let mut running_crc = crc32c::crc32c_append(self.running_crc, &header_bytes);
        let mut payload_bytes_read: u64 = 0;
        let mut parsed_ops = Vec::with_capacity((op_count as usize).min(1024));

        for _ in 0..op_count {
            let op_bytes = match self.try_consume_fixed::<6>(io)? {
                Some(bytes) => bytes,
                None => return Ok(ParseResult::Eof),
            };
            running_crc = crc32c::crc32c_append(running_crc, &op_bytes);
            let tag = op_bytes[0];
            let flags = op_bytes[1];
            let table_id_i32 =
                i32::from_le_bytes([op_bytes[2], op_bytes[3], op_bytes[4], op_bytes[5]]);
            let table_id = match tag {
                OP_UPSERT_TABLE | OP_DELETE_TABLE | OP_UPSERT_INDEX | OP_DELETE_INDEX => {
                    if flags & !OP_FLAG_BTREE_RESIDENT != 0 || table_id_i32 >= 0 {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    Some(MVTableId::from(table_id_i32 as i64))
                }
                OP_UPDATE_HEADER => {
                    // Header op must not carry row-level bits or table addressing.
                    if flags != 0 || table_id_i32 != 0 {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    None
                }
                _ => {
                    self.last_valid_offset = frame_start;
                    return Ok(ParseResult::InvalidFrame);
                }
            };
            let btree_resident = (flags & OP_FLAG_BTREE_RESIDENT) != 0;

            let (payload_len, payload_len_bytes, payload_len_bytes_len) =
                match self.consume_varint_bytes(io)? {
                    Some((value, bytes, len)) => (value, bytes, len),
                    None => return Ok(ParseResult::Eof),
                };
            running_crc =
                crc32c::crc32c_append(running_crc, &payload_len_bytes[..payload_len_bytes_len]);
            let payload_len = match usize::try_from(payload_len) {
                Ok(v) => v,
                Err(_) => {
                    self.last_valid_offset = frame_start;
                    return Ok(ParseResult::InvalidFrame);
                }
            };

            let payload = match self.try_consume_bytes(io, payload_len)? {
                Some(bytes) => bytes,
                None => return Ok(ParseResult::Eof),
            };
            running_crc = crc32c::crc32c_append(running_crc, &payload);

            let op_total_bytes = 6 + payload_len_bytes_len + payload_len;
            payload_bytes_read = match u64::try_from(op_total_bytes)
                .ok()
                .and_then(|op_size| payload_bytes_read.checked_add(op_size))
            {
                Some(v) => v,
                None => {
                    self.last_valid_offset = frame_start;
                    return Ok(ParseResult::InvalidFrame);
                }
            };

            let parsed_op = match tag {
                OP_UPSERT_TABLE => {
                    let table_id = table_id.expect("table op must carry table id");
                    let (rowid_u64, rowid_len) = match read_varint(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            self.last_valid_offset = frame_start;
                            return Ok(ParseResult::InvalidFrame);
                        }
                    };
                    let rowid_i64 = rowid_u64 as i64;
                    if rowid_len > payload.len() {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    let mut payload = payload;
                    let record_bytes = payload.split_off(rowid_len);
                    let rowid = RowID::new(table_id, RowKey::Int(rowid_i64));
                    ParsedOp::UpsertTable {
                        table_id,
                        rowid,
                        record_bytes,
                        commit_ts,
                        btree_resident,
                    }
                }
                OP_DELETE_TABLE => {
                    let table_id = table_id.expect("table op must carry table id");
                    let (rowid_u64, rowid_len) = match read_varint(&payload) {
                        Ok(v) => v,
                        Err(_) => {
                            self.last_valid_offset = frame_start;
                            return Ok(ParseResult::InvalidFrame);
                        }
                    };
                    if rowid_len != payload.len() {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    let rowid_i64 = rowid_u64 as i64;
                    let rowid = RowID::new(table_id, RowKey::Int(rowid_i64));
                    ParsedOp::DeleteTable {
                        rowid,
                        commit_ts,
                        btree_resident,
                    }
                }
                OP_UPSERT_INDEX => {
                    let table_id = table_id.expect("index op must carry table id");
                    ParsedOp::UpsertIndex {
                        table_id,
                        payload,
                        commit_ts,
                        btree_resident,
                    }
                }
                OP_DELETE_INDEX => {
                    let table_id = table_id.expect("index op must carry table id");
                    ParsedOp::DeleteIndex {
                        table_id,
                        payload,
                        commit_ts,
                        btree_resident,
                    }
                }
                OP_UPDATE_HEADER => {
                    if payload.len() != DatabaseHeader::SIZE {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    let mut bytes = [0u8; DatabaseHeader::SIZE];
                    bytes.copy_from_slice(&payload);
                    let header = *bytemuck::from_bytes::<DatabaseHeader>(&bytes);
                    // Fail closed on clearly invalid header payloads before handing it to recovery.
                    if header.magic != *b"SQLite format 3\0" {
                        self.last_valid_offset = frame_start;
                        return Ok(ParseResult::InvalidFrame);
                    }
                    ParsedOp::UpdateHeader { header, commit_ts }
                }
                _ => {
                    self.last_valid_offset = frame_start;
                    return Ok(ParseResult::InvalidFrame);
                }
            };

            parsed_ops.push(parsed_op);
        }

        let trailer_bytes = match self.try_consume_fixed::<TX_TRAILER_SIZE>(io)? {
            Some(bytes) => bytes,
            None => return Ok(ParseResult::Eof),
        };

        let payload_size = u64::from_le_bytes([
            trailer_bytes[0],
            trailer_bytes[1],
            trailer_bytes[2],
            trailer_bytes[3],
            trailer_bytes[4],
            trailer_bytes[5],
            trailer_bytes[6],
            trailer_bytes[7],
        ]);
        let crc32c_expected = u32::from_le_bytes([
            trailer_bytes[8],
            trailer_bytes[9],
            trailer_bytes[10],
            trailer_bytes[11],
        ]);
        let end_magic = u32::from_le_bytes([
            trailer_bytes[12],
            trailer_bytes[13],
            trailer_bytes[14],
            trailer_bytes[15],
        ]);

        if payload_size != payload_bytes_read {
            self.last_valid_offset = frame_start;
            return Ok(ParseResult::InvalidFrame);
        }
        if crc32c_expected != running_crc {
            self.last_valid_offset = frame_start;
            return Ok(ParseResult::InvalidFrame);
        }
        if end_magic != END_MAGIC {
            self.last_valid_offset = frame_start;
            return Ok(ParseResult::InvalidFrame);
        }

        self.last_valid_offset = self.offset.saturating_sub(self.bytes_can_read());
        // Advance the chain: this frame's CRC becomes the seed for the next frame.
        self.running_crc = running_crc;
        Ok(ParseResult::Ops(parsed_ops))
    }

    fn parsed_op_to_streaming(
        &self,
        parsed_op: ParsedOp,
        get_index_info: &mut impl FnMut(MVTableId) -> Result<Arc<IndexInfo>>,
    ) -> Result<StreamingResult> {
        match parsed_op {
            ParsedOp::UpsertTable {
                table_id,
                rowid,
                record_bytes,
                commit_ts,
                btree_resident,
            } => {
                // Compute column_count from the serialized record so recovered rows keep
                // the same shape metadata as non-recovered rows.
                let column_count =
                    crate::types::ImmutableRecord::from_bin_record(record_bytes.clone())
                        .column_count();
                let row = Row::new_table_row(
                    RowID::new(table_id, rowid.row_id.clone()),
                    record_bytes,
                    column_count,
                );
                Ok(StreamingResult::UpsertTableRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                })
            }
            ParsedOp::DeleteTable {
                rowid,
                commit_ts,
                btree_resident,
            } => Ok(StreamingResult::DeleteTableRow {
                rowid,
                commit_ts,
                btree_resident,
            }),
            ParsedOp::UpsertIndex {
                table_id,
                payload,
                commit_ts,
                btree_resident,
            } => {
                let key_record = crate::types::ImmutableRecord::from_bin_record(payload);
                let column_count = key_record.column_count();
                let index_info = get_index_info(table_id)?;
                let key = SortableIndexKey::new_from_record(key_record, index_info);
                let rowid = RowID::new(table_id, RowKey::Record(key));
                let row = Row::new_index_row(rowid.clone(), column_count);
                Ok(StreamingResult::UpsertIndexRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                })
            }
            ParsedOp::DeleteIndex {
                table_id,
                payload,
                commit_ts,
                btree_resident,
            } => {
                let key_record = crate::types::ImmutableRecord::from_bin_record(payload);
                let column_count = key_record.column_count();
                let index_info = get_index_info(table_id)?;
                let key = SortableIndexKey::new_from_record(key_record, index_info);
                let rowid = RowID::new(table_id, RowKey::Record(key));
                let row = Row::new_index_row(rowid.clone(), column_count);
                Ok(StreamingResult::DeleteIndexRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                })
            }
            ParsedOp::UpdateHeader { header, commit_ts } => {
                Ok(StreamingResult::UpdateHeader { header, commit_ts })
            }
        }
    }

    fn remaining_bytes(&self) -> usize {
        let bytes_in_buffer = self.bytes_can_read();
        let bytes_in_file = self.file_size.saturating_sub(self.offset);
        bytes_in_buffer + bytes_in_file
    }

    fn try_consume_bytes(
        &mut self,
        io: &Arc<dyn crate::IO>,
        amount: usize,
    ) -> Result<Option<Vec<u8>>> {
        if self.remaining_bytes() < amount {
            return Ok(None);
        }
        self.read_more_data(io, amount)?;
        let buffer = self.buffer.read();
        let start = self.buffer_offset;
        let end = start + amount;
        let bytes = buffer[start..end].to_vec();
        self.buffer_offset = end;
        Ok(Some(bytes))
    }

    fn try_consume_fixed<const N: usize>(
        &mut self,
        io: &Arc<dyn crate::IO>,
    ) -> Result<Option<[u8; N]>> {
        if self.remaining_bytes() < N {
            return Ok(None);
        }
        self.read_more_data(io, N)?;
        let buffer = self.buffer.read();
        let start = self.buffer_offset;
        let end = start + N;
        let mut out = [0u8; N];
        out.copy_from_slice(&buffer[start..end]);
        self.buffer_offset = end;
        Ok(Some(out))
    }

    fn try_consume_u8(&mut self, io: &Arc<dyn crate::IO>) -> Result<Option<u8>> {
        if self.remaining_bytes() == 0 {
            return Ok(None);
        }
        self.read_more_data(io, 1)?;
        let r = self.buffer.read()[self.buffer_offset];
        self.buffer_offset += 1;
        Ok(Some(r))
    }

    /// Reads a SQLite-format varint one byte at a time from the streaming reader.
    /// Returns `(decoded_value, raw_bytes, byte_count)`. The raw bytes are returned
    /// so callers can feed them into the CRC computation without re-encoding.
    /// Unlike `read_varint` from sqlite3_ondisk (which requires a contiguous buffer),
    /// this reads byte-by-byte via `try_consume_u8` to handle streaming I/O where
    /// the varint may span a buffer boundary. Returns `None` on EOF (short read).
    fn consume_varint_bytes(
        &mut self,
        io: &Arc<dyn crate::IO>,
    ) -> Result<Option<(u64, [u8; 9], usize)>> {
        let mut v: u64 = 0;
        let mut bytes = [0u8; 9];
        let mut len = 0usize;
        for _ in 0..8 {
            let Some(c) = self.try_consume_u8(io)? else {
                return Ok(None);
            };
            bytes[len] = c;
            len += 1;
            v = (v << 7) + (c & 0x7f) as u64;
            if (c & 0x80) == 0 {
                return Ok(Some((v, bytes, len)));
            }
        }
        let Some(c) = self.try_consume_u8(io)? else {
            return Ok(None);
        };
        bytes[len] = c;
        len += 1;
        if (v >> 48) == 0 {
            return Err(LimboError::Corrupt("Invalid varint".to_string()));
        }
        v = (v << 8) + c as u64;
        Ok(Some((v, bytes, len)))
    }

    fn read_exact_at(&self, io: &Arc<dyn crate::IO>, pos: u64, len: usize) -> Result<Vec<u8>> {
        let header_buf = Arc::new(Buffer::new_temporary(len));
        let out = Arc::new(RwLock::new(Vec::with_capacity(len)));
        let out_clone = out.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            let out = out_clone.clone();
            let mut out = out.write();
            let Ok((buf, bytes_read)) = res else {
                tracing::error!("couldn't read logical log header err={:?}", res);
                return None;
            };
            if bytes_read > 0 {
                out.extend_from_slice(&buf.as_slice()[..bytes_read as usize]);
            }
            None
        });
        let c = Completion::new_read(header_buf, completion);
        let c = self.file.pread(pos, c)?;
        io.wait_for_completion(c)?;
        let out = out.read().clone();
        if out.len() != len {
            return Err(LimboError::Corrupt(format!(
                "Logical log short read: expected {len}, got {}",
                out.len()
            )));
        }
        Ok(out)
    }

    fn get_buffer(&self) -> crate::sync::RwLockReadGuard<'_, Vec<u8>> {
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
                "buffer_size_before_read < buffer_offset",
                { "buffer_size_before_read": buffer_size_before_read, "buffer_offset": self.buffer_offset }
            );
            let bytes_available_in_buffer = buffer_size_before_read - self.buffer_offset;
            let still_need = need.saturating_sub(bytes_available_in_buffer);

            if still_need == 0 {
                break;
            }

            turso_assert!(
                self.file_size >= self.offset,
                "file_size < offset",
                { "file_size": self.file_size, "offset": self.offset }
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
            let completion: Box<ReadComplete> = Box::new(move |res| match res {
                Ok((buf, bytes_read)) => {
                    let mut buffer = buffer.write();
                    let buf = buf.as_slice();
                    if bytes_read > 0 {
                        buffer.extend_from_slice(&buf[..bytes_read as usize]);
                    }
                    None
                }
                Err(err) => Some(err),
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

        // Cleanup consumed bytes. If everything was consumed, clear avoids memmove.
        let mut buffer = self.buffer.write();
        if initial_buffer_offset >= buffer.len() {
            buffer.clear();
        } else if initial_buffer_offset > 0 {
            let _ = buffer.drain(0..initial_buffer_offset);
        }
        self.buffer_offset = 0;
        Ok(())
    }

    fn bytes_can_read(&self) -> usize {
        self.buffer.read().len().saturating_sub(self.buffer_offset)
    }
}

#[cfg_attr(test, derive(Debug))]
enum ParseResult {
    /// A fully validated transaction frame was parsed.
    Ops(Vec<ParsedOp>),
    /// True end-of-file: not enough bytes remain to form a complete frame.
    Eof,
    /// An invalid frame was encountered (bad magic, CRC mismatch, structural error).
    /// Handled the same as EOF (stop scanning, keep previously validated frames),
    /// but semantically distinct: the data exists but is not a valid frame.
    /// `last_valid_offset` is set to the start of the invalid frame before returning this.
    InvalidFrame,
}

#[cfg_attr(test, derive(Debug))]
enum ParsedOp {
    UpsertTable {
        table_id: MVTableId,
        rowid: RowID,
        record_bytes: Vec<u8>,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteTable {
        rowid: RowID,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpsertIndex {
        table_id: MVTableId,
        payload: Vec<u8>,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteIndex {
        table_id: MVTableId,
        payload: Vec<u8>,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpdateHeader {
        header: DatabaseHeader,
        commit_ts: u64,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Once;

    use quickcheck_macros::quickcheck;
    use rand::{rng, Rng};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    use crate::io::MemoryIO;
    use crate::sync::Arc;
    use crate::{
        mvcc::database::{
            tests::{commit_tx, generate_simple_string_row, MvccTestDbNoConn},
            MVTableId, Row, RowID, RowKey, SortableIndexKey,
        },
        schema::Table,
        storage::sqlite3_ondisk::{read_varint, varint_len, write_varint},
        types::{ImmutableRecord, IndexInfo, Text},
        Buffer, Completion, LimboError, Value, ValueRef,
    };

    use super::{
        HeaderReadResult, LogHeader, LogicalLog, LOG_HDR_CRC_START, LOG_HDR_RESERVED_START,
        LOG_HDR_SIZE, TX_HEADER_SIZE, TX_TRAILER_SIZE,
    };
    use super::{ParseResult, StreamingLogicalLogReader, StreamingResult};
    use crate::OpenFlags;
    use tracing_subscriber::EnvFilter;

    fn init_tracing() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::from_default_env())
                .try_init();
        });
    }

    fn write_single_table_tx(
        io: &Arc<dyn crate::IO>,
        file_name: &str,
        commit_ts: u64,
    ) -> (Arc<dyn crate::File>, usize) {
        let file = io.open_file(file_name, OpenFlags::Create, false).unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let mut tx = crate::mvcc::database::LogRecord {
            tx_timestamp: commit_ts,
            row_versions: Vec::new(),
            header: None,
        };
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts)),
            end: None,
            row: row.clone(),
            btree_resident: false,
        };
        tx.row_versions.push(version);
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let rowid_len = varint_len(1);
        let payload_len = rowid_len + row.payload().len();
        let payload_len_len = varint_len(payload_len as u64);
        let op_size = 6 + payload_len_len + payload_len;
        (file, op_size)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ExpectedTableOp {
        Upsert {
            rowid: i64,
            payload: Vec<u8>,
            commit_ts: u64,
            btree_resident: bool,
        },
        Delete {
            rowid: i64,
            commit_ts: u64,
            btree_resident: bool,
        },
    }

    fn read_table_ops(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>) -> Vec<ExpectedTableOp> {
        let mut reader = StreamingLogicalLogReader::new(file);
        reader.read_header(io).unwrap();
        let mut ops = Vec::new();
        loop {
            match reader
                .next_record(io, |_id| {
                    Err(LimboError::InternalError("no index".to_string()))
                })
                .unwrap()
            {
                StreamingResult::UpsertTableRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    ops.push(ExpectedTableOp::Upsert {
                        rowid: rowid.row_id.to_int_or_panic(),
                        payload: row.payload().to_vec(),
                        commit_ts,
                        btree_resident,
                    });
                }
                StreamingResult::DeleteTableRow {
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    ops.push(ExpectedTableOp::Delete {
                        rowid: rowid.row_id.to_int_or_panic(),
                        commit_ts,
                        btree_resident,
                    });
                }
                StreamingResult::Eof => break,
                other => panic!("unexpected record: {other:?}"),
            }
        }
        ops
    }

    #[allow(clippy::too_many_arguments)]
    fn append_single_table_op_tx(
        log: &mut LogicalLog,
        io: &Arc<dyn crate::IO>,
        table_id: crate::mvcc::database::MVTableId,
        rowid: i64,
        commit_ts: u64,
        is_delete: bool,
        btree_resident: bool,
        payload_text: &str,
    ) {
        let row = generate_simple_string_row(table_id, rowid, payload_text);
        let row_version = crate::mvcc::database::RowVersion {
            id: commit_ts,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts)),
            end: if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            },
            row,
            btree_resident,
        };
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: commit_ts,
            row_versions: vec![row_version],
            header: None,
        };
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    fn decode_streaming_varint(bytes: &[u8]) -> crate::Result<Option<(u64, [u8; 9], usize)>> {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("logical_log_varint_decode_tmp", OpenFlags::Create, false)
            .unwrap();
        let mut reader = StreamingLogicalLogReader::new(file);
        reader.buffer.write().extend_from_slice(bytes);
        reader.consume_varint_bytes(&io)
    }

    /// What this test checks: A committed transaction written to the logical log is replayed correctly after restart.
    /// Why this matters: This is the baseline durability/recovery guarantee for MVCC commits.
    #[test]
    fn test_logical_log_read() {
        init_tracing();
        // Load a transaction
        // let's not drop db as we don't want files to be removed
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            let pager = conn.pager.load().clone();
            let mvcc_store = db.get_mvcc_store();
            let table_id: MVTableId = (-100).into();
            let tx_id = mvcc_store.begin_tx(pager).unwrap();
            // insert table id -2 into sqlite_schema table (table_id -1)
            let data = ImmutableRecord::from_values(
                &[
                    Value::Text(Text::new("table")),  // type
                    Value::Text(Text::new("test")),   // name
                    Value::Text(Text::new("test")),   // tbl_name
                    Value::from_i64(table_id.into()), // rootpage
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
                        RowID::new((-1).into(), RowKey::Int(1000)),
                        data.as_blob().to_vec(),
                        5,
                    ),
                )
                .unwrap();
            // now insert a row into table -2
            let row = generate_simple_string_row(table_id, 1, "foo");
            mvcc_store.insert(tx_id, row).unwrap();
            commit_tx(mvcc_store, &conn, tx_id).unwrap();
        }

        // Restart the database to trigger recovery
        db.restart();

        // Now try to read it back - recovery happens automatically during bootstrap
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();
        let tx = mvcc_store.begin_tx(pager).unwrap();
        let row = mvcc_store
            .read(tx, &RowID::new((-100).into(), RowKey::Int(1)))
            .unwrap()
            .unwrap();
        let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
        let foo = record.iter().unwrap().next().unwrap().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };
        assert_eq!(foo.as_str(), "foo");
    }

    /// What this test checks: A long sequence of committed frames is replayed in order without dropping or reordering transactions.
    /// Why this matters: Recovery must preserve commit order to maintain MVCC visibility semantics.
    #[test]
    fn test_logical_log_read_multiple_transactions() {
        init_tracing();
        let table_id: MVTableId = (-100).into();
        let values = (0..100)
            .map(|i| {
                (
                    RowID::new(table_id, RowKey::Int(i as i64)),
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
                    Value::Text(Text::new("table")),  // type
                    Value::Text(Text::new("test")),   // name
                    Value::Text(Text::new("test")),   // tbl_name
                    Value::from_i64(table_id.into()), // rootpage
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
                        RowID::new((-1).into(), RowKey::Int(1000)),
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
            let row = mvcc_store.read(tx, rowid).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let foo = record.iter().unwrap().next().unwrap().unwrap();
            let ValueRef::Text(foo) = foo else {
                unreachable!()
            };
            assert_eq!(foo.as_str(), value.as_str());
        }
    }

    /// What this test checks: Randomized insert/delete workloads round-trip through write + restart replay with matching final contents.
    /// Why this matters: Fuzz-style coverage catches edge combinations that hand-written examples miss.
    #[test]
    fn test_logical_log_read_fuzz() {
        init_tracing();
        let table_id: MVTableId = (-100).into();
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
                        let rowid = RowID::new(table_id, RowKey::Int(row_id));
                        let row = generate_simple_string_row(
                            rowid.table_id,
                            rowid.row_id.to_int_or_panic(),
                            &format!("row_{row_id}"),
                        );
                        ops.push((true, Some(row), rowid.clone()));
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
                        ops.push((false, None, row_id.clone()));
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
                    Value::Text(Text::new("table")),  // type
                    Value::Text(Text::new("test")),   // name
                    Value::Text(Text::new("test")),   // tbl_name
                    Value::from_i64(table_id.into()), // rootpage
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
                        RowID::new((-1).into(), RowKey::Int(1000)),
                        data.as_blob().to_vec(),
                        5,
                    ),
                )
                .unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

            // insert rows
            for ops in &txns {
                let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
                for (is_insert, maybe_row, rowid) in ops {
                    if *is_insert {
                        mvcc_store
                            .insert(tx_id, maybe_row.as_ref().unwrap().clone())
                            .unwrap();
                    } else {
                        mvcc_store.delete(tx_id, rowid.clone()).unwrap();
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
            let row = mvcc_store.read(tx, &present_rowid).unwrap().unwrap();
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let foo = record.iter().unwrap().next().unwrap().unwrap();
            let ValueRef::Text(foo) = foo else {
                unreachable!()
            };

            assert_eq!(
                foo.as_str(),
                format!("row_{}", present_rowid.row_id.to_int_or_panic())
            );
        }

        // Check rowids that were deleted
        let tx = mvcc_store.begin_tx(pager).unwrap();
        for present_rowid in non_present_rowids {
            let row = mvcc_store.read(tx, &present_rowid).unwrap();
            assert!(
                row.is_none(),
                "row {present_rowid:?} should have been removed"
            );
        }
    }

    /// What this test checks: Recovery rebuilds both table rows and index rows from logical-log operations.
    /// Why this matters: Table/index divergence after restart would break query correctness.
    #[test]
    fn test_logical_log_read_table_and_index_rows() {
        init_tracing();
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
        let table = schema.get_table("test").expect("table test should exist");
        let Table::BTree(table) = table.as_ref() else {
            panic!("table test should be btree");
        };
        let table_id = mvcc_store.get_table_id_from_root_page(table.root_page);

        // Get the index from schema
        let index = schema
            .get_index("test", "idx_data")
            .expect("Index should exist");
        // Use get_table_id_from_root_page to get the correct index_id (handles both checkpointed and non-checkpointed)
        let index_id = mvcc_store.get_table_id_from_root_page(index.root_page);
        let index_info = Arc::new(IndexInfo::new_from_index(index));

        // Verify table rows can be read
        let tx = mvcc_store.begin_tx(pager).unwrap();
        for (row_id, expected_data) in [(1, "foo"), (2, "bar"), (3, "baz")] {
            let row = mvcc_store
                .read(tx, &RowID::new(table_id, RowKey::Int(row_id)))
                .unwrap()
                .expect("Table row should exist");
            let record = ImmutableRecord::from_bin_record(row.payload().to_vec());
            let values = record.get_values().unwrap();
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
                    Value::from_i64(row_id),
                ],
                2,
            );
            let sortable_key = SortableIndexKey::new_from_record(key_record, index_info.clone());
            let index_rowid = RowID::new(index_id, RowKey::Record(sortable_key));

            // Use read_from_table_or_index to read the index row
            // This verifies that index rows were properly serialized and deserialized from the logical log
            let index_row_opt = mvcc_store
                .read_from_table_or_index(tx, &index_rowid, Some(index_id))
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
            let values = record.get_values().unwrap();
            assert_eq!(
                values.len(),
                2,
                "Index row should have 2 columns (data, rowid)"
            );
            let ValueRef::Text(index_data) = values[0] else {
                panic!("First index column should be text");
            };
            assert_eq!(index_data.as_str(), data_value, "Index data should match");
            let ValueRef::Numeric(crate::numeric::Numeric::Integer(index_rowid_val)) = values[1]
            else {
                panic!("Second index column should be integer (rowid)");
            };
            assert_eq!(index_rowid_val, row_id, "Index rowid should match");
        }
    }

    /// What this test checks: If the last frame is torn, recovery keeps the valid prefix and ignores only the incomplete tail.
    /// Why this matters: Crashes commonly leave partial EOF writes; we need safe prefix recovery instead of full failure.
    #[test]
    fn test_logical_log_torn_tail_stops_cleanly() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("test.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let rowid_len = varint_len(1);
        let payload_len = rowid_len + row.payload().len();
        let payload_len_len = varint_len(payload_len as u64);
        let op_size = 6 + payload_len_len + payload_len;
        let frame_size = TX_HEADER_SIZE + op_size + TX_TRAILER_SIZE;

        let mut tx1 = crate::mvcc::database::LogRecord {
            tx_timestamp: 10,
            row_versions: Vec::new(),
            header: None,
        };
        tx1.row_versions.push(crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(10)),
            end: None,
            row: row.clone(),
            btree_resident: false,
        });
        let c = log.log_tx(&tx1).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut tx2 = crate::mvcc::database::LogRecord {
            tx_timestamp: 20,
            row_versions: Vec::new(),
            header: None,
        };
        tx2.row_versions.push(crate::mvcc::database::RowVersion {
            id: 2,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(20)),
            end: None,
            row,
            btree_resident: false,
        });
        let c = log.log_tx(&tx2).unwrap();
        io.wait_for_completion(c).unwrap();

        let file_size = file.size().unwrap() as usize;
        let last_frame_start = LOG_HDR_SIZE + frame_size;

        // Truncate the file at every offset within the last frame.
        for cut in (last_frame_start..file_size).rev() {
            let c = file
                .truncate(cut as u64, Completion::new_trunc(|_| {}))
                .unwrap();
            io.wait_for_completion(c).unwrap();

            let mut reader = StreamingLogicalLogReader::new(file.clone());
            reader.read_header(&io).unwrap();
            let mut seen = 0;
            loop {
                match reader.next_record(&io, |_id| {
                    Err(LimboError::InternalError("no index".to_string()))
                }) {
                    Ok(StreamingResult::UpsertTableRow { .. }) => seen += 1,
                    Ok(StreamingResult::Eof) => break,
                    Ok(other) => panic!("unexpected record: {other:?}"),
                    Err(err) => panic!("unexpected error: {err:?}"),
                }
            }
            assert_eq!(seen, 1, "should apply only the first transaction");
        }
    }

    /// What this test checks: With many frames, a torn tail still preserves all earlier complete frames.
    /// Why this matters: Durable commits before the crash boundary must survive regardless of tail damage.
    #[test]
    fn test_logical_log_torn_tail_multiple_frames_stops_cleanly() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "logical_log_torn_tail_multi_frame",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 1, false, false, "a");
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 2, false, false, "b");
        let after_tx2 = log.offset as usize;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 3, 3, false, false, "c");
        let after_tx3 = log.offset as usize;

        let partial_tail_len = (after_tx3 - after_tx2) / 2;
        let trunc_offset = (after_tx2 + partial_tail_len) as u64;
        let c = file
            .truncate(trunc_offset, Completion::new_trunc(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let read_back = read_table_ops(file.clone(), &io);
        assert_eq!(read_back.len(), 2);
        assert_eq!(
            read_back[0],
            ExpectedTableOp::Upsert {
                rowid: 1,
                payload: generate_simple_string_row((-2).into(), 1, "a")
                    .payload()
                    .to_vec(),
                commit_ts: 1,
                btree_resident: false,
            }
        );
        assert_eq!(
            read_back[1],
            ExpectedTableOp::Upsert {
                rowid: 2,
                payload: generate_simple_string_row((-2).into(), 2, "b")
                    .payload()
                    .to_vec(),
                commit_ts: 2,
                btree_resident: false,
            }
        );
    }

    /// What this test checks: The parser accepts the full valid negative table-id range, including i32::MIN.
    /// Why this matters: Edge ID handling must be stable to avoid replay panics/corruption on valid inputs.
    #[test]
    fn test_logical_log_read_i32_min_table_id() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("logical_log_i32_min_table_id", OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());
        let table_id = crate::mvcc::database::MVTableId::from(i32::MIN as i64);

        append_single_table_op_tx(&mut log, &io, table_id, 7, 11, false, false, "min");

        let mut reader = StreamingLogicalLogReader::new(file);
        reader.read_header(&io).unwrap();
        match reader
            .next_record(&io, |_id| {
                Err(LimboError::InternalError("no index".to_string()))
            })
            .unwrap()
        {
            StreamingResult::UpsertTableRow { rowid, .. } => {
                assert_eq!(rowid.table_id, table_id);
                assert_eq!(rowid.row_id.to_int_or_panic(), 7);
            }
            other => panic!("unexpected record: {other:?}"),
        }
    }

    /// What this test checks: Rowid varint encoding/decoding is consistent for negative i64-style values used by this path.
    /// Why this matters: Rowid decoding mismatches would replay to the wrong keys.
    #[test]
    fn test_logical_log_rowid_negative_varint_roundtrip() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "logical_log_negative_rowid_roundtrip",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 1, false, false, "neg");
        append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 2, true, false, "neg");

        let read_back = read_table_ops(file, &io);
        assert_eq!(read_back.len(), 2);
        match &read_back[0] {
            ExpectedTableOp::Upsert { rowid, .. } => assert_eq!(*rowid, -1),
            other => panic!("unexpected op: {other:?}"),
        }
        match &read_back[1] {
            ExpectedTableOp::Delete { rowid, .. } => assert_eq!(*rowid, -1),
            other => panic!("unexpected op: {other:?}"),
        }
    }

    /// What this test checks: A payload bit flip in a fully present tail frame is ignored as invalid tail.
    /// Why this matters: Availability-focused recovery keeps the valid prefix even when newest tail bytes are bad.
    #[test]
    fn test_logical_log_corruption_detected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("corrupt.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let mut tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 123,
            row_versions: Vec::new(),
            header: None,
        };
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(123)),
            end: None,
            row,
            btree_resident: false,
        };
        tx.row_versions.push(version);
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        // Flip one byte in the op payload.
        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let offset = reader.offset + TX_HEADER_SIZE + 6 + 1; // header + fixed op + first payload byte
        let buf = Arc::new(Buffer::new(vec![0xFF]));
        let c = file
            .pwrite(offset as u64, buf, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Frames with invalid trailer end-magic are treated as invalid tail.
    /// Why this matters: End-magic damage in newest bytes should not fail startup.
    #[test]
    fn test_logical_log_end_magic_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, op_size) = write_single_table_tx(&io, "end-magic.db-log", 100);
        let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(
                (trailer_offset + 8) as u64,
                bad,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Trailer payload-size mismatch in the newest frame is treated as invalid tail.
    /// Why this matters: Prefix-preserving recovery should not hard-fail on newest damaged frame.
    #[test]
    fn test_logical_log_payload_size_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, op_size) = write_single_table_tx(&io, "payload-size.db-log", 101);
        let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
        let bad_payload_size = (op_size as u64 + 1).to_le_bytes().to_vec();
        let bad = Arc::new(Buffer::new(bad_payload_size));
        let c = file
            .pwrite(trailer_offset as u64, bad, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Invalid frame-magic at newest frame boundary is treated as invalid tail.
    /// Why this matters: Recovery should stop at last valid frame instead of failing startup.
    #[test]
    fn test_logical_log_frame_magic_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "frame-magic.db-log", 103);

        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(LOG_HDR_SIZE as u64, bad, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Corrupting only the stored CRC field turns newest frame into invalid tail.
    /// Why this matters: Prefix must remain replayable under tail checksum damage.
    #[test]
    fn test_logical_log_crc_field_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, op_size) = write_single_table_tx(&io, "crc-field.db-log", 104);
        let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(
                (trailer_offset + 8) as u64,
                bad,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: A corrupted newest frame is dropped while older valid frames still replay.
    /// Why this matters: Prefix-preserving behavior is required for SQLite-style availability recovery.
    #[test]
    fn test_logical_log_corrupt_tail_keeps_valid_prefix() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "corrupt-tail-prefix.db-log",
                crate::OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
        let after_first = log.offset as usize;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");
        let after_second = log.offset as usize;
        let second_frame_len = after_second - after_first;

        let second_trailer_crc_offset = after_first + second_frame_len - TX_TRAILER_SIZE + 8;
        let c = file
            .pwrite(
                second_trailer_crc_offset as u64,
                Arc::new(Buffer::new(vec![0xDE, 0xAD, 0xBE, 0xEF])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let ops = read_table_ops(file, &io);
        assert_eq!(
            ops,
            vec![ExpectedTableOp::Upsert {
                rowid: 1,
                payload: generate_simple_string_row((-2).into(), 1, "a")
                    .payload()
                    .to_vec(),
                commit_ts: 10,
                btree_resident: false,
            }]
        );
    }

    /// What this test checks: Corrupted file-header bytes are detected before replay starts.
    /// Why this matters: Header trust is foundational for offsets and version checks.
    #[test]
    fn test_logical_log_header_corruption_detected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("header-corrupt.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 77,
            row_versions: vec![],
            header: None,
        };
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        // Corrupt magic bytes in the file header.
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file.pwrite(0, bad, Completion::new_write(|_| {})).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        let res = reader.read_header(&io);
        assert!(res.is_err());
    }

    /// What this test checks: Unknown/invalid header flag bits are rejected.
    /// Why this matters: Fail-closed flag handling prevents old readers from misinterpreting new format states.
    #[test]
    fn test_logical_log_header_flags_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "header-flags.db-log", 105);

        // Header flags byte at offset 5 must not have reserved bits set.
        let c = file
            .pwrite(
                5,
                Arc::new(Buffer::new(vec![0b0000_0010])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        let res = reader.read_header(&io);
        assert!(res.is_err());
    }

    /// What this test checks: v2 headers must use the fixed 56-byte length.
    /// Why this matters: Accepting larger lengths can misalign frame parsing and drop valid commits.
    #[test]
    fn test_logical_log_header_non_default_len_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "header-len.db-log", 106);

        let header_buf = Arc::new(Buffer::new_temporary(LOG_HDR_SIZE));
        let c = file
            .pread(0, Completion::new_read(header_buf.clone(), |_| None))
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let mut header_bytes = header_buf.as_slice()[..LOG_HDR_SIZE].to_vec();

        header_bytes[6..8].copy_from_slice(&(LOG_HDR_SIZE as u16 + 1).to_le_bytes());
        header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
        let new_crc = crc32c::crc32c(&header_bytes);
        header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        let c = file
            .pwrite(
                0,
                Arc::new(Buffer::new(header_bytes)),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file);
        let res = reader.read_header(&io);
        assert!(res.is_err());
    }

    /// What this test checks: Non-zero reserved bytes in the file header are rejected for this format version.
    /// Why this matters: Reserved-region discipline preserves forward-compatibility and corruption detection.
    #[test]
    fn test_logical_log_header_reserved_bytes_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "header-reserved.db-log", 106);

        // Read existing header bytes so we can corrupt reserved and recompute CRC.
        let header_buf = Arc::new(Buffer::new_temporary(LOG_HDR_SIZE));
        let c = file
            .pread(0, Completion::new_read(header_buf.clone(), |_| None))
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let mut header_bytes = header_buf.as_slice()[..LOG_HDR_SIZE].to_vec();

        // Corrupt reserved region (bytes 16-51). Reserved region starts at offset 16 (after salt at 8-15).
        header_bytes[LOG_HDR_RESERVED_START] = 1;

        // Recompute CRC with CRC field zeroed, then fill in the new CRC.
        header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
        let new_crc = crc32c::crc32c(&header_bytes);
        header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        // Write the corrupted header back.
        let c = file
            .pwrite(
                0,
                Arc::new(Buffer::new(header_bytes)),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        let res = reader.read_header(&io);
        assert!(res.is_err());
    }

    /// What this test checks: Unknown op reserved-flag bits in newest frame are treated as invalid tail.
    /// Why this matters: Prefix frames must remain usable after tail damage.
    #[test]
    fn test_logical_log_op_reserved_flags_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "op-flags.db-log", 108);

        // First op flags byte at frame offset: TX header + tag byte.
        let c = file
            .pwrite(
                (LOG_HDR_SIZE + TX_HEADER_SIZE + 1) as u64,
                Arc::new(Buffer::new(vec![0b0000_0010])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Non-negative table_id in newest frame is treated as invalid tail.
    /// Why this matters: Bad tail metadata should not make the entire log unreadable.
    #[test]
    fn test_logical_log_non_negative_table_id_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, _) = write_single_table_tx(&io, "table-id-sign.db-log", 109);

        // First op table_id starts after tag+flags.
        let c = file
            .pwrite(
                (LOG_HDR_SIZE + TX_HEADER_SIZE + 2) as u64,
                Arc::new(Buffer::new(1i32.to_le_bytes().to_vec())),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Zero-operation frames are valid and round-trip correctly.
    /// Why this matters: Edge-case frame shapes must remain parseable to keep format handling robust.
    #[test]
    fn test_logical_log_empty_transaction_frame() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("empty-tx.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 200,
            row_versions: vec![],
            header: None,
        };
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let rec = reader
            .next_record(&io, |_id| {
                Err(LimboError::InternalError("no index".to_string()))
            })
            .unwrap();
        assert!(matches!(rec, StreamingResult::Eof));
    }

    /// What this test checks: Every single-bit flip in a full frame is either detected or safely rejected.
    /// Why this matters: This gives strong confidence that integrity checks catch realistic media faults.
    #[test]
    fn test_logical_log_bitflip_integrity_exhaustive_single_frame() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("bitflip.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());
        let mut tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 300,
            row_versions: Vec::new(),
            header: None,
        };
        tx.row_versions.push(crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(300)),
            end: None,
            row: generate_simple_string_row((-2).into(), 42, "flip"),
            btree_resident: false,
        });
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let size = file.size().unwrap() as usize;
        let mut original = vec![0u8; size];
        let read_buf = Arc::new(Buffer::new_temporary(size));
        let c = file
            .pread(0, Completion::new_read(read_buf.clone(), |_| None))
            .unwrap();
        io.wait_for_completion(c).unwrap();
        original.copy_from_slice(&read_buf.as_slice()[..size]);

        for (i, original_byte) in original.iter().enumerate().take(size).skip(LOG_HDR_SIZE) {
            for bit in 0..8u8 {
                let mutated = original_byte ^ (1 << bit);
                let c = file
                    .pwrite(
                        i as u64,
                        Arc::new(Buffer::new(vec![mutated])),
                        Completion::new_write(|_| {}),
                    )
                    .unwrap();
                io.wait_for_completion(c).unwrap();

                let mut reader = StreamingLogicalLogReader::new(file.clone());
                reader.read_header(&io).unwrap();
                let res = reader.next_record(&io, |_id| {
                    Err(LimboError::InternalError("no index".to_string()))
                });
                match res {
                    Err(_) | Ok(StreamingResult::Eof) => {}
                    Ok(other) => {
                        panic!("bit flip at offset={i}, bit={bit} produced valid record: {other:?}")
                    }
                }

                let c = file
                    .pwrite(
                        i as u64,
                        Arc::new(Buffer::new(vec![*original_byte])),
                        Completion::new_write(|_| {}),
                    )
                    .unwrap();
                io.wait_for_completion(c).unwrap();
            }
        }
    }

    /// What this test checks: Random table upsert/delete sequences round-trip through serialize + parse.
    /// Why this matters: Randomized coverage validates invariants across many payload/order combinations.
    #[test]
    fn test_logical_log_roundtrip_random_table_ops() {
        init_tracing();
        let seed = 0xA11CE55u64;
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("roundtrip-rand.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let mut expected = Vec::new();
        for tx_i in 0..128u64 {
            let mut tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 1_000 + tx_i,
                row_versions: Vec::new(),
                header: None,
            };
            let op_count = (rng.next_u64() % 4) as usize;
            for _ in 0..op_count {
                let rowid = (rng.next_u64() % 64) as i64 + 1;
                let btree_resident = (rng.next_u32() & 1) == 1;
                let is_delete = (rng.next_u32() & 1) == 1;
                if is_delete {
                    tx.row_versions.push(crate::mvcc::database::RowVersion {
                        id: 0,
                        begin: None,
                        end: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(
                            tx.tx_timestamp,
                        )),
                        row: Row::new_table_row(
                            RowID::new((-2).into(), RowKey::Int(rowid)),
                            Vec::new(),
                            0,
                        ),
                        btree_resident,
                    });
                    expected.push(ExpectedTableOp::Delete {
                        rowid,
                        commit_ts: tx.tx_timestamp,
                        btree_resident,
                    });
                } else {
                    let payload = format!("r-{tx_i}-{rowid}");
                    let row = generate_simple_string_row((-2).into(), rowid, &payload);
                    tx.row_versions.push(crate::mvcc::database::RowVersion {
                        id: 0,
                        begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(
                            tx.tx_timestamp,
                        )),
                        end: None,
                        row: row.clone(),
                        btree_resident,
                    });
                    expected.push(ExpectedTableOp::Upsert {
                        rowid,
                        payload: row.payload().to_vec(),
                        commit_ts: tx.tx_timestamp,
                        btree_resident,
                    });
                }
            }
            let c = log.log_tx(&tx).unwrap();
            io.wait_for_completion(c).unwrap();
        }

        let got = read_table_ops(file.clone(), &io);
        assert_eq!(got, expected);
    }

    /// What this property checks: For arbitrary event sequences, write/read round-trip preserves operation intent.
    /// Why this matters: Property checks broaden coverage beyond hand-crafted examples.
    #[quickcheck]
    fn prop_logical_log_roundtrip_sequence(events: Vec<(bool, i64, bool)>) -> bool {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = match io.open_file(
            "logical_log_prop_roundtrip_sequence",
            OpenFlags::Create,
            false,
        ) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mut log = LogicalLog::new(file.clone(), io.clone());
        let mut expected = Vec::new();

        for (idx, (is_delete, rowid, btree_resident)) in events.into_iter().take(64).enumerate() {
            let commit_ts = (idx + 1) as u64;
            let payload_text = format!("v{idx}");
            let row = generate_simple_string_row((-2).into(), rowid, &payload_text);
            let row_version = crate::mvcc::database::RowVersion {
                id: commit_ts,
                begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts)),
                end: if is_delete {
                    Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
                } else {
                    None
                },
                row: row.clone(),
                btree_resident,
            };
            expected.push(if is_delete {
                ExpectedTableOp::Delete {
                    rowid,
                    commit_ts,
                    btree_resident,
                }
            } else {
                ExpectedTableOp::Upsert {
                    rowid,
                    payload: row.payload().to_vec(),
                    commit_ts,
                    btree_resident,
                }
            });
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: commit_ts,
                row_versions: vec![row_version],
                header: None,
            };
            let Ok(c) = log.log_tx(&tx) else {
                return false;
            };
            if io.wait_for_completion(c).is_err() {
                return false;
            }
        }

        if expected.is_empty() {
            return file.size().expect("file.size() failed") == 0;
        }

        read_table_ops(file, &io) == expected
    }

    /// What this property checks: Streaming varint decode returns the original value for encoded inputs.
    /// Why this matters: Varint correctness is required for rowid and payload-length decoding.
    #[quickcheck]
    fn prop_streaming_varint_roundtrip(value: u64) -> bool {
        let mut encoded = [0u8; 9];
        let len = write_varint(&mut encoded, value);
        if len == 0 || len > 9 {
            return false;
        }
        let encoded = &encoded[..len];

        let parsed_streaming = match decode_streaming_varint(encoded) {
            Ok(Some(v)) => v,
            _ => return false,
        };
        let parsed_read = match read_varint(encoded) {
            Ok(v) => v,
            Err(_) => return false,
        };

        parsed_streaming.0 == value
            && parsed_streaming.2 == len
            && parsed_streaming.1[..len] == encoded[..]
            && parsed_read.0 == value
            && parsed_read.1 == len
    }

    /// What this property checks: The streaming varint decoder agrees with the reference decoder on the same bytes.
    /// Why this matters: Decoder agreement reduces risk of split-brain parsing behavior.
    #[quickcheck]
    fn prop_streaming_varint_matches_read_varint(bytes: Vec<u8>) -> bool {
        let bytes = if bytes.len() > 16 {
            &bytes[..16]
        } else {
            bytes.as_slice()
        };
        let streaming = decode_streaming_varint(bytes);
        let plain = read_varint(bytes);

        match (streaming, plain) {
            (Ok(Some((v1, b1, l1))), Ok((v2, l2))) => {
                v1 == v2 && l1 == l2 && b1[..l1] == bytes[..l1]
            }
            (Ok(None), Err(_)) => true, // truncated varint in streaming path
            (Err(_), Err(_)) => true,   // malformed varint in both paths
            _ => false,
        }
    }

    /// What this test checks: The btree_resident flag survives write/read round-trip unchanged.
    /// Why this matters: This flag affects tombstone and checkpoint behavior after recovery.
    #[test]
    fn test_logical_log_btree_resident_roundtrip() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("btree.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let mut tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 55,
            row_versions: Vec::new(),
            header: None,
        };
        let mut row = generate_simple_string_row((-2).into(), 1, "foo");
        row.id.table_id = (-2).into();
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(55)),
            end: None,
            row,
            btree_resident: true,
        };
        tx.row_versions.push(version);
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let rec = reader
            .next_record(&io, |_id| {
                Err(LimboError::InternalError("no index".to_string()))
            })
            .unwrap();
        match rec {
            StreamingResult::UpsertTableRow { btree_resident, .. } => {
                assert!(btree_resident);
            }
            _ => panic!("unexpected record"),
        }
    }

    /// What this test checks: Header rewrites remain durable and parseable across truncate/reopen cycles.
    /// Why this matters: Recovery depends on header validity even when the log body is empty.
    #[test]
    fn test_logical_log_header_persistence() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("header.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        let mut tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 10,
            row_versions: Vec::new(),
            header: None,
        };
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(10)),
            end: None,
            row,
            btree_resident: false,
        };
        tx.row_versions.push(version);
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let c = file
            .truncate(LOG_HDR_SIZE as u64, Completion::new_trunc(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone());
        reader.read_header(&io).unwrap();
        let header = reader.header().unwrap();
        // Verify the on-disk CRC matches a fresh computation over the header bytes
        let encoded = header.encode();
        let mut check_buf = [0u8; LOG_HDR_SIZE];
        check_buf.copy_from_slice(&encoded);
        check_buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&[0; 4]);
        let expected_crc = crc32c::crc32c(&check_buf);
        assert_eq!(header.hdr_crc32c, expected_crc);
    }

    /// What this test checks: Header encode/decode with CRC validation round-trips cleanly, including salt.
    /// Why this matters: Header integrity verification must be deterministic across writes/restarts.
    #[test]
    fn test_logical_log_header_crc_roundtrip() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let header = LogHeader::new(&io);
        assert_ne!(header.salt, 0, "salt should be non-zero from IO RNG");
        let bytes = header.encode();
        // Verify CRC: zero out the CRC field and recompute
        let mut check_buf = bytes;
        check_buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&[0; 4]);
        let expected_crc = crc32c::crc32c(&check_buf);
        let decoded = LogHeader::decode(&bytes).unwrap();
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.salt, header.salt);
        assert_eq!(decoded.hdr_crc32c, expected_crc);
    }

    /// What this test checks: try_read_header classifies malformed headers as Invalid (recoverable path) instead of hard-failing immediately.
    /// Why this matters: Bootstrap logic needs this distinction to decide between body-scan fallback and fatal errors.
    #[test]
    fn test_try_read_header_reports_invalid_not_corrupt() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "try-read-header-invalid.db-log",
                crate::OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 11, false, false, "foo");
        let c = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![0])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file);
        let result = reader.try_read_header(&io).unwrap();
        assert!(matches!(result, HeaderReadResult::Invalid));
    }

    /// What this test checks: Truncation regenerates the salt and old frames can't validate with the new salt.
    /// Why this matters: Salt rotation on truncation ensures stale data from a previous log epoch
    /// cannot accidentally validate against the new CRC chain.
    #[test]
    fn test_truncation_regenerates_salt() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("salt-regen.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        // Write a frame and capture the salt
        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
        let salt_before = log.header.as_ref().unwrap().salt;

        // Truncate to 0 (simulates checkpoint truncation); header with new salt
        // will be written together with the next frame.
        let c = log.truncate().unwrap();
        io.wait_for_completion(c).unwrap();

        let salt_after = log.header.as_ref().unwrap().salt;
        assert_ne!(salt_before, salt_after, "salt must change on truncation");
        assert_eq!(log.offset, 0, "offset must be 0 after truncation");

        // Write a new frame — this also writes the header with the new salt
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");

        // Reader should see only the new frame (old data was truncated)
        let mut reader = StreamingLogicalLogReader::new(file);
        assert!(matches!(
            reader.try_read_header(&io).unwrap(),
            HeaderReadResult::Valid(_)
        ));
        let header = reader.header().unwrap();
        assert_eq!(header.salt, salt_after);

        match reader.parse_next_transaction(&io) {
            Ok(ParseResult::Ops(ops)) => {
                assert!(!ops.is_empty(), "expected at least one op");
            }
            Ok(ParseResult::Eof) => panic!("expected ops, got EOF"),
            Ok(ParseResult::InvalidFrame) => panic!("expected ops, got InvalidFrame"),
            Err(e) => panic!("expected ops, got error: {e:?}"),
        }
        assert!(matches!(
            reader.parse_next_transaction(&io),
            Ok(ParseResult::Eof)
        ));
    }

    /// What this test checks: Corrupting frame 1 in a multi-frame log invalidates frame 2 even
    /// though frame 2's bytes are intact, because the CRC chain is broken.
    /// Why this matters: Chained CRC guarantees prefix integrity — any corruption stops the entire
    /// suffix from validating, not just the corrupted frame.
    #[test]
    fn test_crc_chain_invalidates_suffix_on_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("crc-chain.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone());

        // Write 3 frames
        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "aaa");
        let after_first = log.offset as usize;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "bbb");
        append_single_table_op_tx(&mut log, &io, (-2).into(), 3, 30, false, false, "ccc");

        // Without corruption, all 3 frames should read back
        let mut reader = StreamingLogicalLogReader::new(file.clone());
        assert!(matches!(
            reader.try_read_header(&io).unwrap(),
            HeaderReadResult::Valid(_)
        ));
        let mut count = 0;
        while let Ok(ParseResult::Ops(_)) = reader.parse_next_transaction(&io) {
            count += 1;
        }
        assert_eq!(count, 3);

        // Corrupt one byte in frame 1's payload (not the CRC field itself)
        let corrupt_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + 1; // inside frame 1 payload
        let c = file
            .pwrite(
                corrupt_offset as u64,
                Arc::new(Buffer::new(vec![0xFF])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        // Now frame 1 should fail CRC, and frames 2+3 should NOT be returned
        // (chained CRC means the reader stops at the first invalid frame)
        let mut reader = StreamingLogicalLogReader::new(file);
        assert!(matches!(
            reader.try_read_header(&io).unwrap(),
            HeaderReadResult::Valid(_)
        ));
        // Frame 1 is corrupted — CRC mismatch on structurally complete frame
        match reader.parse_next_transaction(&io) {
            Ok(ParseResult::InvalidFrame) => {}
            other => panic!("expected InvalidFrame after corrupted frame 1, got {other:?}"),
        }
        // Verify we didn't somehow get frame 2 or 3
        let valid_offset = reader.last_valid_offset();
        assert!(
            valid_offset <= after_first,
            "valid offset {valid_offset} should be <= first frame end {after_first}",
        );
    }

    /// What this test checks: A structurally valid tx frame from one log cannot be spliced
    /// into another log and pass CRC validation, because the two logs have different salts
    /// and therefore different CRC chains.
    /// Why this matters: Salt-seeded chained CRC prevents cross-log frame replay attacks —
    /// an adversary cannot copy frames between logs to forge commit history.
    #[test]
    fn test_splice_frame_from_different_log_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());

        // --- Log A: write one frame ---
        let file_a = io
            .open_file("splice-a.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log_a = LogicalLog::new(file_a.clone(), io.clone());
        append_single_table_op_tx(&mut log_a, &io, (-2).into(), 1, 10, false, false, "aaa");
        let log_a_end = log_a.offset as usize;

        // --- Log B: write one frame (different salt → different CRC chain) ---
        let file_b = io
            .open_file("splice-b.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log_b = LogicalLog::new(file_b.clone(), io.clone());
        append_single_table_op_tx(&mut log_b, &io, (-2).into(), 2, 20, false, false, "bbb");
        let log_b_end = log_b.offset as usize;

        // Verify the two logs have different salts
        let salt_a = log_a.header.as_ref().unwrap().salt;
        let salt_b = log_b.header.as_ref().unwrap().salt;
        assert_ne!(
            salt_a, salt_b,
            "two independent logs should have different salts"
        );

        // Read raw frame bytes from log B (everything after the header)
        let frame_b_len = log_b_end - LOG_HDR_SIZE;
        let read_buf = Arc::new(Buffer::new_temporary(frame_b_len));
        let c = file_b
            .pread(
                LOG_HDR_SIZE as u64,
                Completion::new_read(read_buf.clone(), |_| None),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let frame_b_bytes: Vec<u8> = read_buf.as_slice()[..frame_b_len].to_vec();

        // Splice log B's frame onto the end of log A
        let c = file_a
            .pwrite(
                log_a_end as u64,
                Arc::new(Buffer::new(frame_b_bytes)),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        // Read log A — should get 1 valid frame (A's own), then reject the spliced frame
        let mut reader = StreamingLogicalLogReader::new(file_a);
        assert!(matches!(
            reader.try_read_header(&io).unwrap(),
            HeaderReadResult::Valid(_)
        ));

        // Frame 1 from log A should validate fine
        match reader.parse_next_transaction(&io) {
            Ok(ParseResult::Ops(ops)) => assert!(!ops.is_empty()),
            other => panic!("expected log A's frame to parse, got {other:?}"),
        }

        // The spliced frame from log B should fail CRC validation
        match reader.parse_next_transaction(&io) {
            Ok(ParseResult::InvalidFrame) => {}
            other => {
                panic!("spliced frame from a different log should NOT validate, got {other:?}")
            }
        }
    }
}
