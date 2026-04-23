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
//! ```text
//!     ┌─────────────────────────────────────────┐
//!     │         Log Header (56 bytes)           │
//!     │  magic(4) | ver(1) | flags(1) | len(2)  │
//!     │  salt(8) | reserved(36) | crc32c(4)     │
//!     ├─────────────────────────────────────────┤
//!     │         TX Frame 0                      │
//!     ├─────────────────────────────────────────┤
//!     │         TX Frame 1                      │
//!     ├─────────────────────────────────────────┤
//!     │         ...                             │
//!     └─────────────────────────────────────────┘
//! ```
//!
//! ### Transaction frame (TX Frame)
//!
//! ```text
//!     ┌─────────────────────────────────────────┐
//!     │       TX Header (24 bytes)              │
//!     │  frame_magic(4) | payload_size(8)       │
//!     │  op_count(4) | commit_ts(8)             │
//!     ├─────────────────────────────────────────┤
//!     │       Payload (variable)                │
//!     │                                         │
//!     │  Unencrypted:                           │
//!     │    op entries serialized directly       │
//!     │                                         │
//!     │  Encrypted:                             │
//!     │    chunk_0(ciphertext+tag | nonce)      │
//!     │    chunk_1(ciphertext+tag | nonce)      │
//!     │    ...                                  │
//!     ├─────────────────────────────────────────┤
//!     │       TX Trailer (8 bytes)              │
//!     │  crc32c(4) | end_magic(4)               │
//!     └─────────────────────────────────────────┘
//! ```
//!
//! When encryption is enabled, only the payload is encrypted. The log header,
//! TX header, and TX trailer are always written in plaintext. The log header's salt and TX header
//! fields (op_count, commit_ts, and the final chunk's payload_size) are bound to the ciphertext
//! as AEAD additional data, so tampering with them will cause decryption to fail.
//! The CRC in the trailer covers the TX header and the payload as written on disk
//! (i.e. the ciphertext when encrypted).
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
//! ### TX Header (`TX_HEADER_SIZE = 24`)
//! - `frame_magic: u32` (`FRAME_MAGIC`)
//! - `payload_size: u64` (total bytes of all op entries, pre-encryption)
//! - `op_count: u32`
//! - `commit_ts: u64`
//!
//! ### Payload
//! - When **unencrypted**: `op_count` operation entries serialized directly:
//!   - `tag: u8` (`OP_*`)
//!   - `flags: u8` (`OP_FLAG_BTREE_RESIDENT` currently defined)
//!   - `table_id: i32` (must be negative)
//!   - `payload_len: sqlite varint`
//!   - `payload: [u8; payload_len]`
//! - When **encrypted**: payload is split into fixed-size plaintext chunks
//!   (`ENCRYPTED_PAYLOAD_CHUNK_SIZE`, except the final remainder chunk)
//!   - each chunk is written as `ciphertext(chunk_plain_len + tag_size) | nonce(nonce_size)`
//!   - AEAD additional data:
//!     `salt(8) || payload_size_or_zero(8) || op_count(4) || commit_ts(8) || chunk_index(4)` (little-endian)
//!     where the payload-size slot is zero for non-final chunks and carries the real payload size
//!     only in the final chunk
//!
//! ### TX Trailer (`TX_TRAILER_SIZE = 8`)
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
//! ### Frame Layout: Unencrypted vs Encrypted
//!
//! ```text
//! Unencrypted:
//! ┌──────────────┬──────────────────────────────┬───────────┐
//! │ TX Header    │ Payload                      │ Trailer   │
//! │ (24B plain)  │ Op₀ | Op₁ | Op₂ | ...        │ CRC + End │
//! └──────────────┴──────────────────────────────┴───────────┘
//!
//! Encrypted (chunked):
//! ┌──────────────┬──────────┬──────────┬──────────┬───────────┐
//! │ TX Header    │ Chunk 0  │ Chunk 1  │ Chunk N  │ Trailer   │
//! │ (24B plain)  │ ct|n     │ ct|n     │ ct|n     │ CRC + End │
//! └──────────────┴──────────┴──────────┴──────────┴───────────┘
//!                     │
//!                     ▼
//!               ┌───────────────────────────┬───────┐
//!               │ ciphertext (plain + tag)  │ nonce │
//!               └───────────────────────────┴───────┘
//! ```
//!
//! Each chunk encrypted with AAD (32B):
//! ```text
//! ┌────────┬────────────────────┬──────────┬────────────┬─────────────┐
//! │salt (8)│payload_size_or_0(8)│op_cnt (4)│commit_ts(8)│chunk_idx (4)│
//! └────────┴────────────────────┴──────────┴────────────┴─────────────┘
//!           ↑
//!           └── payload_size only in final chunk; zero for all others
//! ```
//!
//! ### How Plaintext Payload Is Split Into Chunks
//!
//! ```text
//! Plaintext payload (serialized ops, payload_size bytes):
//!
//! ┌──────┬──────┬────────────┬──────────┬──────┬────────────┬──────┬──────┬──────┬───────┐
//! │ Op₀  │ Op₁  │    Op₂     │   Op₃    │ Op₄  │    Op₅     │ Op₆  │ Op₇  │ Op₈  │ Op₉   │
//! └──────┴──────┴─────┼──────┴──────────┴──────┴──────┼─────┴──────┴──────┴──────┴───────┘
//!                     │                               │
//!               32 KB boundary                   64 KB boundary
//!
//! Chunking splits at fixed 32 KB boundaries — ops may straddle them:
//!
//!   Chunk 0 (32 KB)              Chunk 1 (32 KB)              Chunk 2 (remainder)
//! ┌──────┬──────┬──────┐     ┌──────┬──────┬──────┬──────┐   ┌──────┬──────┬──────┬──────┐
//! │ Op₀  │ Op₁  │ Op₂▌ │     │▐Op₂  │ Op₃  │ Op₄  │ Op₅▌ │   │▐Op₅  │ Op₆  │ Op₇  │ ...  │
//! └──────┴──────┴──────┘     └──────┴──────┴──────┴──────┘   └──────┴──────┴──────┴──────┘
//!                ├─── Op₂ split across chunks 0 & 1 ───┤              │
//!                                          ├── Op₅ split across chunks 1 & 2 ──┤
//!
//!   Op₂ starts in chunk 0, ends in chunk 1.  The reader uses a "carry buffer"
//!   to accumulate the partial op across chunk boundaries before parsing.
//!
//!             │                          │                       │
//!             ▼                          ▼                       ▼
//!       ┌───────────┬────┐         ┌───────────┬────┐     ┌───────────┬────┐
//!       │ciphertext₀│ N₀ │         │ciphertext₁│ N₁ │     │ciphertext₂│ N₂ │
//!       │(32KB+tag) │    │         │(32KB+tag) │    │     │(rem+tag)  │    │
//!       └───────────┴────┘         └───────────┴────┘     └───────────┴────┘
//!        on-disk chunk blob         on-disk chunk blob     on-disk chunk blob
//!
//! Each chunk is encrypted independently with AEAD. The reader decrypts one chunk
//! at a time. If an op is incomplete at the end of a chunk, the leftover bytes go
//! into a carry buffer and are joined with bytes from the next decrypted chunk.
//! ```
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
    storage::sqlite3_ondisk::{
        read_varint, read_varint_partial, varint_len, write_varint_to_vec, DatabaseHeader,
    },
    types::IndexInfo,
    Buffer, Completion, CompletionError, LimboError, Result,
};

use crate::storage::encryption::EncryptionContext;
use crate::File;

/// Logical log size in bytes at which a committing transaction will trigger a checkpoint.
/// Default to the size of 1000 SQLite WAL frames; disable by setting a negative value.
pub const DEFAULT_LOG_CHECKPOINT_THRESHOLD: i64 = 4120 * 1000;

/// Optional callback invoked after serialization with a zero-copy reference to
/// the serialized frame bytes and the running CRC, before the disk write.
pub type OnSerializationComplete<'a> = Option<&'a dyn Fn(&[u8], u32) -> crate::Result<()>>;

const LOG_MAGIC: u32 = 0x4C4D4C32; // "LML2" in LE
const LOG_VERSION: u8 = 2;
pub const LOG_HDR_SIZE: usize = 56;
const LOG_HDR_SALT_START: usize = 8;
const LOG_HDR_SALT_SIZE: usize = 8;
const LOG_HDR_RESERVED_START: usize = LOG_HDR_SALT_START + LOG_HDR_SALT_SIZE; // 16
const LOG_HDR_CRC_START: usize = 52;
const LOG_HDR_RESERVED_SIZE: usize = LOG_HDR_CRC_START - LOG_HDR_RESERVED_START; // 36
pub(crate) const FRAME_MAGIC: u32 = 0x5854564D; // "MVTX" in LE
const END_MAGIC: u32 = 0x4554564D; // "MVTE" in LE

// Size of each chunk before encryption (i.e. before tag/nonce overhead is added)
pub(crate) const ENCRYPTED_PAYLOAD_CHUNK_SIZE: usize = 32 * 1024;
// Fixed AAD width for one encrypted chunk:
// salt(8) + payload_size_or_zero(8) + op_count(4) + commit_ts(8) + chunk_index(4).
const ENCRYPTED_CHUNK_AAD_SIZE: usize = 32;

const OP_UPSERT_TABLE: u8 = 0;
const OP_DELETE_TABLE: u8 = 1;
const OP_UPSERT_INDEX: u8 = 2;
const OP_DELETE_INDEX: u8 = 3;
/// Frame-local database-header mutation (payload = serialized `DatabaseHeader`).
const OP_UPDATE_HEADER: u8 = 4;

const OP_FLAG_BTREE_RESIDENT: u8 = 1 << 0;

const TX_HEADER_SIZE: usize = 24; // FRAME_MAGIC(4) + payload_size(8) + op_count(4) + commit_ts(8)
const TX_TRAILER_SIZE: usize = 8; // crc32c(4) + END_MAGIC(4)
const TX_MIN_FRAME_SIZE: usize = TX_HEADER_SIZE + TX_TRAILER_SIZE; // 32

fn encrypted_payload_chunk_count(payload_size: usize, chunk_size: usize) -> usize {
    if payload_size == 0 {
        0
    } else {
        payload_size.div_ceil(chunk_size)
    }
}

/// Returns how many plaintext bytes belong to `chunk_index` before encryption.
/// If the payload fits within a chunk, then that is the length.
/// If a payload spans over multiple chunks, then except the last chunk rest of the chunks
/// will have `chunk_size` plaintext and the last one will have the remainder.
fn encrypted_chunk_plaintext_len(
    payload_size: usize,
    chunk_index: usize,
    chunk_size: usize,
) -> Result<usize> {
    let chunk_start = chunk_index.checked_mul(chunk_size).ok_or_else(|| {
        LimboError::Corrupt(format!(
            "encrypted chunk offset overflow: chunk_index={chunk_index}, chunk_size={chunk_size}"
        ))
    })?;
    if chunk_start >= payload_size {
        return Err(LimboError::Corrupt(format!(
            "encrypted chunk index {chunk_index} out of range for payload_size={payload_size}"
        )));
    }
    Ok((payload_size - chunk_start).min(chunk_size))
}

/// On-disk size of one encrypted chunk: `plaintext_len + tag + nonce`.
fn encrypted_chunk_blob_size(
    plaintext_len: usize,
    tag_size: usize,
    nonce_size: usize,
) -> Result<usize> {
    plaintext_len
        .checked_add(tag_size)
        .and_then(|size| size.checked_add(nonce_size))
        .ok_or_else(|| {
            LimboError::Corrupt(format!(
                "encrypted chunk size overflow: plaintext={plaintext_len}, tag={tag_size}, nonce={nonce_size}"
            ))
        })
}

/// Total on-disk size of an encrypted payload: the sum of every chunk's
/// `plaintext_len + tag + nonce`. The last chunk may be shorter than `chunk_size`.
fn encrypted_payload_blob_size(
    payload_size: usize,
    chunk_size: usize,
    tag_size: usize,
    nonce_size: usize,
) -> Result<usize> {
    let chunk_count = encrypted_payload_chunk_count(payload_size, chunk_size);
    if chunk_count == 0 {
        return Ok(0);
    }

    let full_chunk_on_disk = encrypted_chunk_blob_size(chunk_size, tag_size, nonce_size)?;
    let full_chunks_total = full_chunk_on_disk
        .checked_mul(chunk_count.saturating_sub(1))
        .ok_or_else(|| LimboError::Corrupt("encrypted payload total size overflow".to_string()))?;
    let last_plaintext_len =
        encrypted_chunk_plaintext_len(payload_size, chunk_count - 1, chunk_size)?;
    let last_chunk_on_disk = encrypted_chunk_blob_size(last_plaintext_len, tag_size, nonce_size)?;
    full_chunks_total
        .checked_add(last_chunk_on_disk)
        .ok_or_else(|| LimboError::Corrupt("encrypted payload total size overflow".to_string()))
}

fn build_encrypted_chunk_aad(
    salt: u64,
    payload_size_in_aad: Option<u64>,
    op_count: u32,
    commit_ts: u64,
    chunk_index: u32,
) -> [u8; ENCRYPTED_CHUNK_AAD_SIZE] {
    let mut aad = [0u8; ENCRYPTED_CHUNK_AAD_SIZE];
    aad[..8].copy_from_slice(&salt.to_le_bytes());
    if let Some(payload_size) = payload_size_in_aad {
        aad[8..16].copy_from_slice(&payload_size.to_le_bytes());
    }
    aad[16..20].copy_from_slice(&op_count.to_le_bytes());
    aad[20..28].copy_from_slice(&commit_ts.to_le_bytes());
    aad[28..32].copy_from_slice(&chunk_index.to_le_bytes());
    aad
}

/// Log's Header, the first 56 bytes of any logical log file.
#[derive(Clone, Debug)]
pub struct LogHeader {
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
    encryption_ctx: Option<EncryptionContext>,
    /// Plaintext bytes per encrypted payload chunk. Production uses the fixed format constant;
    /// tests may override via `new_with_encrypted_payload_chunk_size_for_test`.
    encrypted_payload_chunk_size: usize,
    /// Reusable scratch buffer for ops serialization on the encrypted write path.
    encryption_scratch_buffer: Vec<u8>,
}

impl LogicalLog {
    fn new_internal(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
        encrypted_payload_chunk_size: usize,
    ) -> Self {
        Self {
            file,
            io,
            offset: 0,
            write_buf: Vec::new(),
            header: None,
            running_crc: 0,
            pending_running_crc: None,
            encryption_ctx,
            encrypted_payload_chunk_size,
            encryption_scratch_buffer: Vec::new(),
        }
    }

    pub fn new(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
    ) -> Self {
        Self::new_internal(file, io, encryption_ctx, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
    }

    #[cfg(test)]
    fn new_with_payload_chunk_size(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
        encrypted_payload_chunk_size: usize,
    ) -> Self {
        Self::new_internal(file, io, encryption_ctx, encrypted_payload_chunk_size)
    }

    pub(crate) fn set_header(&mut self, header: LogHeader) {
        self.running_crc = derive_initial_crc(header.salt);
        self.header = Some(header);
    }

    pub(crate) fn header(&self) -> Option<&LogHeader> {
        self.header.as_ref()
    }

    pub(crate) fn encryption_ctx(&self) -> Option<&EncryptionContext> {
        self.encryption_ctx.as_ref()
    }

    /// Serializes a transaction into `write_buf`, optionally calls
    /// `on_serialization_complete` with a zero-copy reference to the frame bytes,
    /// then writes to disk. `write_buf` retains its allocation across calls.
    ///
    /// `advance_offset_immediately`: when true, the writer offset advances right
    /// after the pwrite (checkpoint path). When false, the offset stays behind
    /// until `advance_offset_after_success` is called (MVCC commit path).
    fn serialize_and_pwrite_tx(
        &mut self,
        tx: &LogRecord,
        advance_offset_immediately: bool,
        on_serialization_complete: OnSerializationComplete<'_>,
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

        // 2. Serialize Transaction header.
        // A header-only transaction is encoded as a single OP_UPDATE_HEADER op.
        // payload_size is only known after serializing all ops. We reserve TX_HEADER_SIZE bytes
        // as a placeholder and backfill all header fields in step 4.
        let op_count = u32::try_from(tx.row_versions.len() + usize::from(tx.header.is_some()))
            .map_err(|_| {
                LimboError::InternalError("Logical log op_count exceeds u32".to_string())
            })?;
        let commit_ts = tx.tx_timestamp;
        let tx_header_start = self.write_buf.len();
        self.write_buf.resize(tx_header_start + TX_HEADER_SIZE, 0);

        // 3. Serialize ops into write_buf (encrypted or plaintext).
        let payload_size = self.serialize_ops_into_write_buf(tx, op_count, commit_ts)?;
        let payload_end = self.write_buf.len();

        // 4. Backfill TX HEADER: FRAME_MAGIC(4) | payload_size(8) | op_count(4) | commit_ts(8)
        self.write_buf[tx_header_start..tx_header_start + 4]
            .copy_from_slice(&FRAME_MAGIC.to_le_bytes());
        self.write_buf[tx_header_start + 4..tx_header_start + 12]
            .copy_from_slice(&payload_size.to_le_bytes());
        self.write_buf[tx_header_start + 12..tx_header_start + 16]
            .copy_from_slice(&op_count.to_le_bytes());
        self.write_buf[tx_header_start + 16..tx_header_start + 24]
            .copy_from_slice(&commit_ts.to_le_bytes());

        // 5. TX TRAILER layout (8 bytes): crc32c(4, le u32) | END_MAGIC(4)
        // CRC is chained: seeded from running_crc (salt-derived, or previous frame's CRC),
        // covers TX_HEADER (24 B) + payload (encrypted or plaintext).
        let crc = crc32c::crc32c_append(
            self.running_crc,
            &self.write_buf[tx_header_start..payload_end],
        );
        self.write_buf.extend_from_slice(&crc.to_le_bytes());
        self.write_buf.extend_from_slice(&END_MAGIC.to_le_bytes());

        // 6. Call observer before writing — zero-copy reference into write_buf.
        if let Some(cb) = on_serialization_complete {
            cb(&self.write_buf, crc)?;
        }

        // 7. Copy write_buf into an I/O buffer and pwrite. write_buf keeps its allocation.
        let buffer = Arc::new(Buffer::new(self.write_buf.to_vec()));
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

    /// Serializes ops into `write_buf`, encrypting if an encryption context is set.
    /// Returns the plaintext payload size (used in the TX header's `payload_size` field).
    ///
    /// Encrypted on-disk payload layout: repeated
    /// `ciphertext(chunk_plain_len + tag_size) | nonce(nonce_size)` chunks.
    fn serialize_ops_into_write_buf(
        &mut self,
        tx: &LogRecord,
        op_count: u32,
        commit_ts: u64,
    ) -> Result<u64> {
        if let Some(enc_ctx) = &self.encryption_ctx {
            self.encryption_scratch_buffer.clear();
            for row_version in &tx.row_versions {
                serialize_op_entry(&mut self.encryption_scratch_buffer, row_version)?;
            }
            if let Some(hdr) = tx.header {
                serialize_header_entry(&mut self.encryption_scratch_buffer, &hdr);
            }
            let payload_size = self.encryption_scratch_buffer.len();

            let salt = self
                .header
                .as_ref()
                .expect("log header must be set before writing")
                .salt;
            let total_on_disk_size = encrypted_payload_blob_size(
                payload_size,
                self.encrypted_payload_chunk_size,
                enc_ctx.tag_size(),
                enc_ctx.nonce_size(),
            )?;
            let write_buf_start = self.write_buf.len();
            self.write_buf.reserve(total_on_disk_size);
            let chunk_count =
                encrypted_payload_chunk_count(payload_size, self.encrypted_payload_chunk_size);

            let payload_size = payload_size as u64;
            for (chunk_index, plaintext_chunk) in self
                .encryption_scratch_buffer
                .chunks(self.encrypted_payload_chunk_size)
                .enumerate()
            {
                let is_last_chunk = chunk_index + 1 == chunk_count;
                let aad = build_encrypted_chunk_aad(
                    salt,
                    is_last_chunk.then_some(payload_size),
                    op_count,
                    commit_ts,
                    u32::try_from(chunk_index).map_err(|_| {
                        LimboError::InternalError(
                            "encrypted payload chunk index exceeds u32".to_string(),
                        )
                    })?,
                );

                let (ciphertext, nonce) = enc_ctx.encrypt_chunk(plaintext_chunk, &aad)?;
                // encrypt_chunk returns ciphertext with the auth tag appended, so its
                // length must be exactly plaintext_len + tag_size. The read path relies
                // on this to split each chunk back into (ciphertext+tag, nonce).
                debug_assert_eq!(
                    ciphertext.len(),
                    plaintext_chunk.len() + enc_ctx.tag_size(),
                    "encrypt_chunk output size mismatch: expected plaintext({}) + tag({}), got {}",
                    plaintext_chunk.len(),
                    enc_ctx.tag_size(),
                    ciphertext.len(),
                );
                self.write_buf.extend_from_slice(&ciphertext);
                self.write_buf.extend_from_slice(&nonce);
            }
            turso_assert!(
                self.write_buf.len() - write_buf_start == total_on_disk_size,
                "encrypted write_buf size mismatch"
            );
            Ok(payload_size)
        } else {
            let payload_start = self.write_buf.len();
            for row_version in &tx.row_versions {
                serialize_op_entry(&mut self.write_buf, row_version)?;
            }
            if let Some(header) = tx.header {
                serialize_header_entry(&mut self.write_buf, &header);
            }
            Ok((self.write_buf.len() - payload_start) as u64)
        }
    }

    /// Writes a transaction to the log and immediately advances the writer offset.
    /// Used for checkpoint-initiated writes where no two-phase commit is needed.
    pub fn log_tx(&mut self, tx: &LogRecord) -> Result<Completion> {
        let (c, _) = self.serialize_and_pwrite_tx(tx, true, None)?;
        Ok(c)
    }

    /// Writes a transaction to the log but does NOT advance the writer offset.
    /// Returns `(completion, bytes_written)`. The caller must call
    /// `advance_offset_after_success(bytes)` after confirming the commit succeeded.
    ///
    /// If `on_serialization_complete` is provided, it is called with a zero-copy
    /// reference to the serialized frame bytes and the running CRC after
    /// serialization but before the disk write.
    pub fn log_tx_deferred_offset(
        &mut self,
        tx: &LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.serialize_and_pwrite_tx(tx, false, on_serialization_complete)
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

/// Serialize one op into `buffer`.
/// Op layout: tag(1) | flags(1) | table_id(4, le i32) | payload_len(varint) | payload(variable)
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

/// Parse all ops from a decrypted plaintext buffer.
/// Validates that `plaintext.len() == payload_size` and that every byte is consumed.
fn parse_ops_from_plaintext(
    plaintext: &[u8],
    payload_size: usize,
    op_count: u32,
    commit_ts: u64,
) -> Result<Vec<ParsedOp>> {
    if plaintext.len() != payload_size {
        return Err(LimboError::Corrupt(format!(
            "decrypted size ({}) != payload_size ({payload_size})",
            plaintext.len()
        )));
    }
    let mut ops = Vec::with_capacity((op_count as usize).min(1024));
    let mut cursor = 0usize;
    for _ in 0..op_count {
        match try_parse_one_op_from_buf(&plaintext[cursor..], commit_ts)? {
            Some((op, consumed)) => {
                cursor += consumed;
                ops.push(op);
            }
            None => {
                return Err(LimboError::Corrupt(
                    "incomplete op in decrypted payload".into(),
                ));
            }
        }
    }
    if cursor != plaintext.len() {
        return Err(LimboError::Corrupt(format!(
            "trailing bytes after ops: consumed {cursor}, total {}",
            plaintext.len()
        )));
    }
    Ok(ops)
}

/// Parse one op entry from a contiguous byte slice (no IO).
/// Returns `Ok(Some((parsed_op, bytes_consumed)))` on success,
/// `Ok(None)` when not enough bytes, or `Err` on structural corruption.
///
/// Op layout: tag(1) | flags(1) | table_id(4, le i32) | payload_len(varint) | payload(variable)
fn try_parse_one_op_from_buf(buf: &[u8], commit_ts: u64) -> Result<Option<(ParsedOp, usize)>> {
    if buf.len() < 6 {
        return Ok(None);
    }

    let tag = buf[0];
    let flags = buf[1];
    let table_id_i32 = i32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]);

    let table_id: Option<MVTableId> = match tag {
        OP_UPSERT_TABLE | OP_DELETE_TABLE | OP_UPSERT_INDEX | OP_DELETE_INDEX => {
            if flags & !OP_FLAG_BTREE_RESIDENT != 0 || table_id_i32 >= 0 {
                return Err(LimboError::Corrupt(
                    "Invalid op flags or non-negative table_id".into(),
                ));
            }
            Some(MVTableId::from(table_id_i32 as i64))
        }
        OP_UPDATE_HEADER => {
            if flags != 0 || table_id_i32 != 0 {
                return Err(LimboError::Corrupt(
                    "Invalid UPDATE_HEADER flags/table_id".into(),
                ));
            }
            None
        }
        _ => return Err(LimboError::Corrupt(format!("Unknown op tag: {tag}"))),
    };
    let btree_resident = (flags & OP_FLAG_BTREE_RESIDENT) != 0;

    let Some((payload_len_u64, varint_bytes)) = read_varint_partial(&buf[6..])? else {
        return Ok(None);
    };
    let payload_len = match usize::try_from(payload_len_u64) {
        Ok(v) => v,
        Err(_) => return Err(LimboError::Corrupt("payload_len overflows usize".into())),
    };

    let fixed = 6 + varint_bytes;
    let total = fixed + payload_len;
    if buf.len() < total {
        return Ok(None);
    }

    let payload = &buf[fixed..total];

    let parsed_op = match tag {
        OP_UPSERT_TABLE => {
            let table_id = table_id.expect("table op must have table_id");
            let (rowid_u64, rowid_len) = read_varint(payload)
                .map_err(|_| LimboError::Corrupt("Bad rowid varint in UPSERT_TABLE".into()))?;
            if rowid_len > payload.len() {
                return Err(LimboError::Corrupt("rowid_len > payload".into()));
            }
            let record_bytes = payload[rowid_len..].to_vec();
            let rowid = RowID::new(table_id, RowKey::Int(rowid_u64 as i64));
            ParsedOp::UpsertTable {
                table_id,
                rowid,
                record_bytes,
                commit_ts,
                btree_resident,
            }
        }
        OP_DELETE_TABLE => {
            let table_id = table_id.expect("table op must have table_id");
            let (rowid_u64, rowid_len) = read_varint(payload)
                .map_err(|_| LimboError::Corrupt("Bad rowid varint in DELETE_TABLE".into()))?;
            if rowid_len != payload.len() {
                return Err(LimboError::Corrupt(
                    "DELETE_TABLE payload size mismatch".into(),
                ));
            }
            let rowid = RowID::new(table_id, RowKey::Int(rowid_u64 as i64));
            ParsedOp::DeleteTable {
                rowid,
                commit_ts,
                btree_resident,
            }
        }
        OP_UPSERT_INDEX => ParsedOp::UpsertIndex {
            table_id: table_id.expect("index op must have table_id"),
            payload: payload.to_vec(),
            commit_ts,
            btree_resident,
        },
        OP_DELETE_INDEX => ParsedOp::DeleteIndex {
            table_id: table_id.expect("index op must have table_id"),
            payload: payload.to_vec(),
            commit_ts,
            btree_resident,
        },
        OP_UPDATE_HEADER => {
            if payload.len() != DatabaseHeader::SIZE {
                return Err(LimboError::Corrupt(
                    "UPDATE_HEADER wrong payload size".into(),
                ));
            }
            let mut bytes = [0u8; DatabaseHeader::SIZE];
            bytes.copy_from_slice(payload);
            let header = *bytemuck::from_bytes::<DatabaseHeader>(&bytes);
            if header.magic != *b"SQLite format 3\0" {
                return Err(LimboError::Corrupt("UPDATE_HEADER bad SQLite magic".into()));
            }
            ParsedOp::UpdateHeader { header, commit_ts }
        }
        _ => unreachable!("tag validated above"),
    };

    Ok(Some((parsed_op, total)))
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
    encryption_ctx: Option<EncryptionContext>,
    /// Plaintext bytes per encrypted payload chunk. Production uses the fixed format constant;
    /// tests may override via `new_with_encrypted_payload_chunk_size_for_test`.
    encrypted_payload_chunk_size: usize,
    // Reused scratch buffer for decrypted chunk plaintext. Kept on the reader so encrypted
    // recovery can reuse the allocation across chunks and transaction frames.
    decrypt_scratch: Vec<u8>,
}

impl StreamingLogicalLogReader {
    fn new_internal(
        file: Arc<dyn File>,
        encryption_ctx: Option<EncryptionContext>,
        encrypted_payload_chunk_size: usize,
    ) -> Self {
        let file_size = file.size().expect("failed to get file size") as usize;
        let decrypt_scratch = encryption_ctx
            .as_ref()
            .map(|enc_ctx| Vec::with_capacity(encrypted_payload_chunk_size + enc_ctx.tag_size()))
            .unwrap_or_default();
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
            encryption_ctx,
            encrypted_payload_chunk_size,
            decrypt_scratch,
        }
    }

    pub fn new(file: Arc<dyn File>, encryption_ctx: Option<EncryptionContext>) -> Self {
        Self::new_internal(file, encryption_ctx, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
    }

    #[cfg(test)]
    fn new_with_payload_chunk_size(
        file: Arc<dyn File>,
        encryption_ctx: Option<EncryptionContext>,
        encrypted_payload_chunk_size: usize,
    ) -> Self {
        Self::new_internal(file, encryption_ctx, encrypted_payload_chunk_size)
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

    /// Parse as many complete ops as possible from decrypted plaintext, up to `op_count` and
    /// starting at `start`.
    /// Returns how many plaintext bytes were fully consumed into `parsed_ops`.
    fn parse_decrypted_chunk_ops(
        plaintext: &[u8],
        start: usize,
        parsed_ops: &mut Vec<ParsedOp>,
        op_count: u32,
        commit_ts: u64,
    ) -> Result<usize> {
        let mut consumed = 0usize;
        while parsed_ops.len() < op_count as usize {
            match try_parse_one_op_from_buf(&plaintext[start + consumed..], commit_ts)? {
                Some((op, bytes_consumed)) => {
                    consumed += bytes_consumed;
                    parsed_ops.push(op);
                }
                None => break,
            }
        }
        Ok(consumed)
    }

    fn carried_op_total_len_if_known(buf: &[u8]) -> Result<Option<usize>> {
        // we need minimum of 6 bytes to read the length field
        // 1 byte op tag + 1 byte flags + 4 bytes table id
        if buf.len() < 6 {
            return Ok(None);
        }

        match buf[0] {
            OP_UPSERT_TABLE | OP_DELETE_TABLE | OP_UPSERT_INDEX | OP_DELETE_INDEX
            | OP_UPDATE_HEADER => {}
            tag => return Err(LimboError::Corrupt(format!("Unknown op tag: {tag}"))),
        }

        let Some((payload_len_u64, varint_bytes)) = read_varint_partial(&buf[6..])? else {
            // we don't have enough data to read the varint
            return Ok(None);
        };
        let payload_len = usize::try_from(payload_len_u64)
            .map_err(|_| LimboError::Corrupt("payload_len overflows usize".into()))?;
        let fixed = 6usize
            .checked_add(varint_bytes)
            .ok_or_else(|| LimboError::Corrupt("op header length overflow".into()))?;
        let total = fixed
            .checked_add(payload_len)
            .ok_or_else(|| LimboError::Corrupt("op payload length overflow".into()))?;
        Ok(Some(total))
    }

    // fixed 6-byte prelude + max 9-byte varint (payload_len)
    // (prelude = 1 byte op tag + 1 byte flags + 4 bytes table_id)
    // This is the maximum prefix length needed to determine total_len for a partial op.
    const MAX_SERIALIZED_OP_PREFIX_LEN: usize = 15;

    /// given the chunk index, read the chunk off the disk and decrypt it
    fn read_and_decrypt_encrypted_chunk(
        &mut self,
        io: &Arc<dyn crate::IO>,
        payload_ctx: &EncryptedPayloadReadContext,
        chunk_index: usize,
        running_crc: u32,
    ) -> Result<EncryptedChunkReadResult> {
        // first we gotta figure out, how many bytes to read off the disk, its either
        // `self.encrypted_payload_chunk_size` or the remainder in the last chunk
        let plaintext_len = encrypted_chunk_plaintext_len(
            payload_ctx.payload_size,
            chunk_index,
            self.encrypted_payload_chunk_size,
        )?;
        let on_disk_size =
            encrypted_chunk_blob_size(plaintext_len, payload_ctx.tag_size, payload_ctx.nonce_size)?;
        let chunk_count = encrypted_payload_chunk_count(
            payload_ctx.payload_size,
            self.encrypted_payload_chunk_size,
        );
        let is_last_chunk = chunk_index + 1 == chunk_count;

        let aad = build_encrypted_chunk_aad(
            payload_ctx.salt,
            is_last_chunk.then_some(payload_ctx.payload_size as u64),
            payload_ctx.op_count,
            payload_ctx.commit_ts,
            u32::try_from(chunk_index).map_err(|_| {
                LimboError::Corrupt("encrypted payload chunk index exceeds u32".to_string())
            })?,
        );

        if self.remaining_bytes() < on_disk_size {
            return Ok(EncryptedChunkReadResult::Eof);
        }
        self.read_more_data(io, on_disk_size)?;
        let start = self.buffer_offset;
        let end = start + on_disk_size;

        let (next_crc, decrypted_plaintext_len) = {
            let encryption_ctx = self
                .encryption_ctx
                .as_ref()
                .expect("encryption_ctx must be set for encrypted payload");
            let decrypt_scratch = &mut self.decrypt_scratch;
            let buffer = self.buffer.read();
            let blob = &buffer[start..end];
            let next_crc = crc32c::crc32c_append(running_crc, blob);
            let ciphertext = &blob[..plaintext_len + payload_ctx.tag_size];
            let nonce = &blob[plaintext_len + payload_ctx.tag_size..];
            encryption_ctx
                .decrypt_chunk_into(ciphertext, nonce, &aad, decrypt_scratch)
                .map_err(|e| {
                    LimboError::Corrupt(format!(
                        "decrypt_chunk failed for chunk {chunk_index}: {e}"
                    ))
                })?;
            (next_crc, decrypt_scratch.len())
        };

        self.buffer_offset = end;
        if decrypted_plaintext_len != plaintext_len {
            return Err(LimboError::Corrupt(format!(
                "decrypted chunk length mismatch: expected {plaintext_len}, got {decrypted_plaintext_len}"
            )));
        }

        Ok(EncryptedChunkReadResult::Ok {
            running_crc: next_crc,
        })
    }

    /// Extend the carried partial op with enough bytes from the current plaintext chunk to decode
    /// its total serialized length. Returns `Ok(None)` if this chunk still does not provide enough
    /// prefix bytes and the caller must continue with the next chunk.
    fn try_resolve_carried_encrypted_op_total_len(
        carry: &mut Vec<u8>,
        plaintext: &[u8],
        plaintext_start: &mut usize,
    ) -> Result<Option<usize>> {
        loop {
            if let Some(total_len) = Self::carried_op_total_len_if_known(carry)? {
                return Ok(Some(total_len));
            }

            let available = plaintext.len().saturating_sub(*plaintext_start);
            if available == 0 {
                // i.e. no more bytes left in the current plaintext chunk to read more.
                return Ok(None);
            }

            if carry.len() >= Self::MAX_SERIALIZED_OP_PREFIX_LEN {
                return Err(LimboError::Corrupt(
                    "carried encrypted op prefix could not resolve total length".into(),
                ));
            }

            carry.push(plaintext[*plaintext_start]);
            *plaintext_start += 1;
        }
    }

    /// This is part of decryption of a chunk when reading the log file. `carry` contains the
    /// partial op suffix from the previous chunk and `plaintext` is the current decrypted chunk.
    /// Return `Ok(true)` when the carried op is completed and parsed; `Ok(false)` when more
    /// chunk bytes are still needed.
    fn try_finish_carried_encrypted_op(
        carry: &mut Vec<u8>,
        plaintext: &[u8],
        plaintext_start: &mut usize,
        parsed_ops: &mut Vec<ParsedOp>,
        op_count: u32,
        commit_ts: u64,
    ) -> Result<bool> {
        turso_assert!(!carry.is_empty());
        turso_assert!(parsed_ops.len() < op_count as usize);

        // lets try to parse the length of this op
        let Some(carried_op_total_len) =
            Self::try_resolve_carried_encrypted_op_total_len(carry, plaintext, plaintext_start)?
        else {
            return Ok(false);
        };

        // carry buffer must never have more than the op total length. it carries bytes from a
        // previous chunk which is incomplete.
        if carry.len() > carried_op_total_len {
            return Err(LimboError::Corrupt(format!(
                "carried encrypted op exceeded computed length: len={} total={carried_op_total_len}",
                carry.len()
            )));
        }
        // if the carry does not have enough bytes right now, then we consume from plaintext
        // and try to parse. if not, we return so that next chunk can be read and decrypted.
        // this scenario can happen when carry contains the prefix, but the op spans over current
        // chunk and then on multiple chunks.
        if carry.len() < carried_op_total_len {
            let available = plaintext.len().saturating_sub(*plaintext_start);
            if available == 0 {
                return Ok(false);
            }
            let take = (carried_op_total_len - carry.len()).min(available);
            carry.extend_from_slice(&plaintext[*plaintext_start..*plaintext_start + take]);
            *plaintext_start += take;
            if carry.len() < carried_op_total_len {
                return Ok(false);
            }
        }

        // carry must have the total data now and then we can parse
        turso_assert!(carry.len() == carried_op_total_len);
        match try_parse_one_op_from_buf(carry, commit_ts)? {
            Some((op, bytes_consumed)) if bytes_consumed == carry.len() => {
                parsed_ops.push(op);
                carry.clear();
                Ok(true)
            }
            Some((_, bytes_consumed)) => Err(LimboError::Corrupt(format!(
                "carried encrypted op consumed {bytes_consumed} bytes but carry holds {}",
                carry.len()
            ))),
            None => Err(LimboError::Corrupt(
                "carried encrypted op remained incomplete after reaching computed length".into(),
            )),
        }
    }

    /// Parse an encrypted payload by reading and decrypting fixed-size plaintext chunks,
    /// then incrementally parsing ops from the resulting plaintext.
    /// Encrypted on-disk payload layout is a concatenation of chunk blobs:
    /// ciphertext(chunk_plain_len + tag_size) | nonce(nonce_size), one blob per chunk.
    fn parse_encrypted_payload(
        &mut self,
        io: &Arc<dyn crate::IO>,
        op_count: u32,
        payload_size: usize,
        commit_ts: u64,
        running_crc: u32,
    ) -> Result<PayloadParseResult> {
        let (nonce_size, tag_size) = {
            let enc = self
                .encryption_ctx
                .as_ref()
                .expect("encryption_ctx must be set for encrypted payload");
            (enc.nonce_size(), enc.tag_size())
        };
        let salt = self
            .header
            .as_ref()
            .expect("log header must be read before parsing")
            .salt;
        let payload_ctx = EncryptedPayloadReadContext {
            payload_size,
            op_count,
            commit_ts,
            salt,
            nonce_size,
            tag_size,
        };
        let mut running_crc = running_crc;
        // carry contains the payload from previous chunk.
        // it is possible that op might split between two chunks (or even multiple), in that case
        // we need to keep the previous payload, then decrypt the next chunk. Only when we have the
        // full payload, we parse it.
        let mut carry = Vec::with_capacity(self.encrypted_payload_chunk_size);
        // we allocate some space to keep a vector of parsed ops, we set the 1024 as upper bound
        // size and extend the vector as required.
        let mut parsed_ops = Vec::with_capacity((op_count as usize).min(1024));
        let chunk_count =
            encrypted_payload_chunk_count(payload_size, self.encrypted_payload_chunk_size);

        for chunk_index in 0..chunk_count {
            // lets decrypt the log file, chunk by chunk
            running_crc = match self.read_and_decrypt_encrypted_chunk(
                io,
                &payload_ctx,
                chunk_index,
                running_crc,
            )? {
                EncryptedChunkReadResult::Ok { running_crc } => running_crc,
                EncryptedChunkReadResult::Eof => return Ok(PayloadParseResult::Eof),
            };

            let mut plaintext_start = 0usize;
            let plaintext = self.decrypt_scratch.as_slice();

            turso_assert!(
                parsed_ops.len() <= op_count as usize,
                "parsed_ops.len() exceeded declared op_count"
            );
            if !carry.is_empty() {
                if parsed_ops.len() == op_count as usize {
                    return Err(LimboError::Corrupt(format!(
                        "encrypted payload has trailing carried bytes after parsing all {op_count} ops"
                    )));
                }
                // carry holds the prefix of an op that was split by the previous chunk boundary.
                // Try to finish that carried op using bytes from the current decrypted chunk.
                // If this chunk still does not complete the op, keep it in carry and continue
                // with the next chunk
                match Self::try_finish_carried_encrypted_op(
                    &mut carry,
                    plaintext,
                    &mut plaintext_start,
                    &mut parsed_ops,
                    op_count,
                    commit_ts,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        return Err(LimboError::Corrupt(format!(
                            "encrypted carried-op parse error: {e}"
                        )));
                    }
                }
            }
            // if we are here, then we have successfully emptied the carry
            turso_assert!(
                carry.is_empty(),
                "carry must be empty before parsing fresh ops from the current decrypted chunk"
            );

            // we don't have any carry bytes, so lets just parse the plaintext
            let consumed = Self::parse_decrypted_chunk_ops(
                plaintext,
                plaintext_start,
                &mut parsed_ops,
                op_count,
                commit_ts,
            )?;
            plaintext_start += consumed;
            if plaintext_start < plaintext.len() {
                // IOW we still have some bytes left over, so lets add that to carry so that
                // in the next iteration it is parsed.
                // it is safe to add it to carry buffer since we have already asserted that it is
                // empty
                carry.extend_from_slice(&plaintext[plaintext_start..]);
            }
        }

        // at this point, we must have parsed the full payload
        if parsed_ops.len() != op_count as usize {
            return Err(LimboError::Corrupt(format!(
                "encrypted payload ended after {} parsed ops, expected {op_count}",
                parsed_ops.len()
            )));
        }

        // once we have parsed the full payload, carry must be empty
        if !carry.is_empty() {
            return Err(LimboError::Corrupt(format!(
                "encrypted payload has {} trailing plaintext bytes after parsing all ops",
                carry.len()
            )));
        }

        Ok(PayloadParseResult::Ok(parsed_ops, running_crc))
    }

    /// Parse an unencrypted payload via field-by-field streaming IO reads.
    fn parse_streaming_payload(
        &mut self,
        io: &Arc<dyn crate::IO>,
        op_count: u32,
        payload_size: usize,
        commit_ts: u64,
        mut running_crc: u32,
    ) -> Result<PayloadParseResult> {
        let mut parsed_ops = Vec::with_capacity((op_count as usize).min(1024));
        let mut payload_bytes_read: u64 = 0;

        for _ in 0..op_count {
            // Op header (6 bytes): tag(1) | flags(1) | table_id(4, little-endian i32)
            let op_bytes = match self.try_consume_fixed::<6>(io)? {
                Some(bytes) => bytes,
                None => return Ok(PayloadParseResult::Eof),
            };
            running_crc = crc32c::crc32c_append(running_crc, &op_bytes);
            let tag = op_bytes[0];
            let flags = op_bytes[1];
            let table_id_i32 =
                i32::from_le_bytes([op_bytes[2], op_bytes[3], op_bytes[4], op_bytes[5]]);
            let table_id = match tag {
                OP_UPSERT_TABLE | OP_DELETE_TABLE | OP_UPSERT_INDEX | OP_DELETE_INDEX => {
                    if flags & !OP_FLAG_BTREE_RESIDENT != 0 || table_id_i32 >= 0 {
                        return Err(LimboError::Corrupt(format!(
                            "invalid op flags={flags:#x} or table_id={table_id_i32} for tag={tag}"
                        )));
                    }
                    Some(MVTableId::from(table_id_i32 as i64))
                }
                OP_UPDATE_HEADER => {
                    if flags != 0 || table_id_i32 != 0 {
                        return Err(LimboError::Corrupt(format!(
                            "OP_UPDATE_HEADER has non-zero flags={flags:#x} or table_id={table_id_i32}"
                        )));
                    }
                    None
                }
                _ => {
                    return Err(LimboError::Corrupt(format!("unknown op tag {tag}")));
                }
            };
            let btree_resident = (flags & OP_FLAG_BTREE_RESIDENT) != 0;

            let (payload_len, payload_len_bytes, payload_len_bytes_len) =
                match self.consume_varint_bytes(io) {
                    Ok(Some((value, bytes, len))) => (value, bytes, len),
                    Ok(None) => return Ok(PayloadParseResult::Eof),
                    Err(err) => return Err(err),
                };
            running_crc =
                crc32c::crc32c_append(running_crc, &payload_len_bytes[..payload_len_bytes_len]);
            let payload_len = usize::try_from(payload_len)
                .map_err(|e| LimboError::Corrupt(format!("payload_len overflows usize: {e}")))?;

            let payload = match self.try_consume_bytes(io, payload_len)? {
                Some(bytes) => bytes,
                None => return Ok(PayloadParseResult::Eof),
            };
            running_crc = crc32c::crc32c_append(running_crc, &payload);

            let op_total_bytes = 6 + payload_len_bytes_len + payload_len;
            payload_bytes_read = u64::try_from(op_total_bytes)
                .ok()
                .and_then(|op_size| payload_bytes_read.checked_add(op_size))
                .ok_or_else(|| LimboError::Corrupt("payload_bytes_read overflow".to_string()))?;

            let parsed_op = match tag {
                OP_UPSERT_TABLE => {
                    let table_id = table_id.expect("table op must carry table id");
                    let (rowid_u64, rowid_len) = read_varint(&payload).map_err(|e| {
                        LimboError::Corrupt(format!(
                            "failed to read rowid varint in upsert op: {e}"
                        ))
                    })?;
                    let rowid_i64 = rowid_u64 as i64;
                    if rowid_len > payload.len() {
                        return Err(LimboError::Corrupt(
                            "upsert op rowid varint extends beyond payload".to_string(),
                        ));
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
                    let (rowid_u64, rowid_len) = read_varint(&payload).map_err(|e| {
                        LimboError::Corrupt(format!(
                            "failed to read rowid varint in delete op: {e}"
                        ))
                    })?;
                    if rowid_len != payload.len() {
                        return Err(LimboError::Corrupt(format!(
                            "delete op rowid varint len {rowid_len} != payload len {}",
                            payload.len()
                        )));
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
                        return Err(LimboError::Corrupt(format!(
                            "OP_UPDATE_HEADER payload len {} != DatabaseHeader::SIZE {}",
                            payload.len(),
                            DatabaseHeader::SIZE
                        )));
                    }
                    let mut bytes = [0u8; DatabaseHeader::SIZE];
                    bytes.copy_from_slice(&payload);
                    let header = *bytemuck::from_bytes::<DatabaseHeader>(&bytes);
                    if header.magic != *b"SQLite format 3\0" {
                        return Err(LimboError::Corrupt(
                            "OP_UPDATE_HEADER has invalid SQLite magic".to_string(),
                        ));
                    }
                    ParsedOp::UpdateHeader { header, commit_ts }
                }
                _ => {
                    return Err(LimboError::Corrupt(format!(
                        "unknown op tag {tag} in payload"
                    )));
                }
            };

            parsed_ops.push(parsed_op);
        }

        if payload_size as u64 != payload_bytes_read {
            return Err(LimboError::Corrupt(format!(
                "payload_size ({payload_size}) != payload_bytes_read ({payload_bytes_read})"
            )));
        }

        Ok(PayloadParseResult::Ok(parsed_ops, running_crc))
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

        // TX HEADER layout (24 bytes): FRAME_MAGIC(4) | payload_size(8) | op_count(4) | commit_ts(8)
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
        let payload_size_u64 = u64::from_le_bytes([
            header_bytes[4],
            header_bytes[5],
            header_bytes[6],
            header_bytes[7],
            header_bytes[8],
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
        ]);
        let op_count = u32::from_le_bytes([
            header_bytes[12],
            header_bytes[13],
            header_bytes[14],
            header_bytes[15],
        ]);
        let commit_ts = u64::from_le_bytes([
            header_bytes[16],
            header_bytes[17],
            header_bytes[18],
            header_bytes[19],
            header_bytes[20],
            header_bytes[21],
            header_bytes[22],
            header_bytes[23],
        ]);

        let payload_size = match usize::try_from(payload_size_u64) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("payload_size overflows usize: {e}");
                self.last_valid_offset = frame_start;
                return Ok(ParseResult::InvalidFrame);
            }
        };

        // Chained CRC: seed from running_crc (derived from salt, or previous frame's CRC)
        let running_crc = crc32c::crc32c_append(self.running_crc, &header_bytes);

        // 2. Parse payload — branches for encrypted vs unencrypted.
        //    Corrupt errors from payload parsing are treated as an invalid frame
        //    (stop scanning, keep previously validated frames).
        let (parsed_ops, running_crc) = match if self.encryption_ctx.is_some() {
            self.parse_encrypted_payload(io, op_count, payload_size, commit_ts, running_crc)
        } else {
            self.parse_streaming_payload(io, op_count, payload_size, commit_ts, running_crc)
        } {
            Ok(PayloadParseResult::Ok(ops, crc)) => (ops, crc),
            Ok(PayloadParseResult::Eof) => return Ok(ParseResult::Eof),
            Err(LimboError::Corrupt(msg)) => {
                tracing::warn!("corrupt payload: {msg}");
                self.last_valid_offset = frame_start;
                return Ok(ParseResult::InvalidFrame);
            }
            Err(e) => return Err(e),
        };

        // 3. TX TRAILER layout (8 bytes): crc32c(4, le u32) | END_MAGIC(4)
        let trailer_bytes = match self.try_consume_fixed::<TX_TRAILER_SIZE>(io)? {
            Some(bytes) => bytes,
            None => return Ok(ParseResult::Eof),
        };

        let crc32c_expected = u32::from_le_bytes([
            trailer_bytes[0],
            trailer_bytes[1],
            trailer_bytes[2],
            trailer_bytes[3],
        ]);
        let end_magic = u32::from_le_bytes([
            trailer_bytes[4],
            trailer_bytes[5],
            trailer_bytes[6],
            trailer_bytes[7],
        ]);

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

/// Metadata shared by every encrypted chunk in the current frame.
struct EncryptedPayloadReadContext {
    payload_size: usize,
    op_count: u32,
    commit_ts: u64,
    salt: u64,
    nonce_size: usize,
    tag_size: usize,
}

/// Result of parsing just the payload portion of a transaction frame.
/// Used by `parse_encrypted_payload` and `parse_streaming_payload` to communicate
/// back to `parse_next_transaction` without duplicating control flow.
///
/// Corruption is signalled via `Err(LimboError::Corrupt(...))`, not a variant here.
/// The caller (`parse_next_transaction`) catches those errors and converts them to
/// `ParseResult::InvalidFrame` to preserve the WAL-prefix "stop scanning" semantics.
enum PayloadParseResult {
    /// Successfully parsed ops and updated running CRC.
    Ok(Vec<ParsedOp>, u32),
    /// Not enough bytes to complete the payload.
    Eof,
}

/// Result of reading and decrypting one encrypted chunk into `decrypt_scratch`.
/// Corruption (decryption failure, length mismatch) is returned as
/// `Err(LimboError::Corrupt(...))`.
enum EncryptedChunkReadResult {
    Ok { running_crc: u32 },
    Eof,
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

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
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
    use rand::{random_range, rng, Rng};
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
        storage::sqlite3_ondisk::{
            read_varint, read_varint_partial, varint_len, write_varint, DatabaseHeader,
        },
        types::{ImmutableRecord, IndexInfo, Text},
        Buffer, Completion, LimboError, Value, ValueRef,
    };

    use super::{
        build_encrypted_chunk_aad, encrypted_chunk_blob_size, encrypted_chunk_plaintext_len,
        encrypted_payload_blob_size, encrypted_payload_chunk_count, serialize_header_entry,
        serialize_op_entry, HeaderReadResult, LogHeader, LogicalLog, ParseResult, ParsedOp,
        StreamingLogicalLogReader, StreamingResult, ENCRYPTED_CHUNK_AAD_SIZE,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE, END_MAGIC, FRAME_MAGIC, LOG_HDR_CRC_START,
        LOG_HDR_RESERVED_START, LOG_HDR_SIZE, LOG_VERSION, TX_HEADER_SIZE, TX_TRAILER_SIZE,
    };
    use crate::OpenFlags;
    use crate::{turso_assert, turso_assert_less_than};
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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
        let mut reader = StreamingLogicalLogReader::new(file, None);
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
        let mut reader = StreamingLogicalLogReader::new(file, None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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

            let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
        let table_id = crate::mvcc::database::MVTableId::from(i32::MIN as i64);

        append_single_table_op_tx(&mut log, &io, table_id, 7, 11, false, false, "min");

        let mut reader = StreamingLogicalLogReader::new(file, None);
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

    /// What this test checks: Rowid varint encoding/decoding is consistent for negative i64-style
    /// values, and the deferred-offset write path (log_tx_deferred_offset) does not advance the
    /// writer offset until advance_offset_after_success is called, after which all frames are
    /// readable with a valid CRC chain.
    /// Why this matters: Rowid decoding mismatches would replay to the wrong keys.
    ///   The MVCC commit path uses deferred writes so an aborted commit can be silently overwritten;
    ///   the offset must not advance before confirmation.
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 1, false, false, "neg");
        append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 2, true, false, "neg");
        let offset_after_frame2 = log.offset;

        // Frame 3: deferred path — offset must not advance until confirmed.
        let row3 = generate_simple_string_row((-2).into(), 3, "deferred");
        let tx3 = crate::mvcc::database::LogRecord {
            tx_timestamp: 3,
            row_versions: vec![crate::mvcc::database::RowVersion {
                id: 3,
                begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(3)),
                end: None,
                row: row3,
                btree_resident: false,
            }],
            header: None,
        };
        let (c, bytes_written) = log.log_tx_deferred_offset(&tx3, None).unwrap();
        io.wait_for_completion(c).unwrap();

        assert_eq!(
            log.offset, offset_after_frame2,
            "deferred write must not advance offset before advance_offset_after_success"
        );
        log.advance_offset_after_success(bytes_written);
        assert_eq!(
            log.offset,
            offset_after_frame2 + bytes_written,
            "offset must advance by exactly bytes_written after confirmation"
        );

        let read_back = read_table_ops(file, &io);
        assert_eq!(read_back.len(), 3);
        match &read_back[0] {
            ExpectedTableOp::Upsert { rowid, .. } => assert_eq!(*rowid, -1),
            other => panic!("unexpected op: {other:?}"),
        }
        match &read_back[1] {
            ExpectedTableOp::Delete { rowid, .. } => assert_eq!(*rowid, -1),
            other => panic!("unexpected op: {other:?}"),
        }
        match &read_back[2] {
            ExpectedTableOp::Upsert { rowid, .. } => assert_eq!(*rowid, 3),
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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

        // Flip one byte in the op data (varint payload_len).
        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        // After read_header, reader.offset = LOG_HDR_SIZE.
        // Skip frame header (TX_HEADER_SIZE) + fixed op prefix (tag+flags+table_id = 6 bytes).
        let offset = reader.offset + TX_HEADER_SIZE + 6; // first byte of varint payload_len
        let buf = Arc::new(Buffer::new(vec![0xFF]));
        let c = file
            .pwrite(offset as u64, buf, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Malformed payload-length varint in newest frame is treated as invalid tail.
    /// Why this matters: Recovery must preserve already-validated commits instead of failing hard.
    #[test]
    fn test_logical_log_payload_len_varint_corrupt_tail_keeps_prefix() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "payload-len-varint-corrupt.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 1, false, false, "first");
        let frame2_start = log.offset;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 2, false, false, "second");

        // Corrupt frame-2 payload_len varint into an invalid 9-byte varint sequence.
        let payload_len_offset = frame2_start + (TX_HEADER_SIZE + 6) as u64;
        let mut bad_varint = vec![0x80; 8];
        bad_varint.push(0x00);
        let c = file
            .pwrite(
                payload_len_offset,
                Arc::new(Buffer::new(bad_varint)),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let read_back = read_table_ops(file, &io);
        assert_eq!(read_back.len(), 1);
        assert_eq!(
            read_back[0],
            ExpectedTableOp::Upsert {
                rowid: 1,
                payload: generate_simple_string_row((-2).into(), 1, "first")
                    .payload()
                    .to_vec(),
                commit_ts: 1,
                btree_resident: false,
            }
        );
    }

    /// What this test checks: Frames with invalid trailer end-magic are treated as invalid tail.
    /// Why this matters: End-magic damage in newest bytes should not fail startup.
    #[test]
    fn test_logical_log_end_magic_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, op_size) = write_single_table_tx(&io, "end-magic.db-log", 100);
        let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
        // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; END_MAGIC is at offset +4.
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(
                (trailer_offset + 4) as u64,
                bad,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Header payload-size mismatch in the newest frame is treated as invalid tail.
    /// Why this matters: Prefix-preserving recovery should not hard-fail on newest damaged frame.
    #[test]
    fn test_logical_log_payload_size_corruption() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let (file, op_size) = write_single_table_tx(&io, "payload-size.db-log", 101);
        // TX header layout: [FRAME_MAGIC(4)][payload_size(8)][op_count(4)][commit_ts(8)]
        // payload_size is at byte 4 of the frame (right after FRAME_MAGIC).
        let bad_payload_size = (op_size as u64 + 1).to_le_bytes().to_vec();
        let bad = Arc::new(Buffer::new(bad_payload_size));
        let c = file
            .pwrite(
                (LOG_HDR_SIZE + 4) as u64,
                bad,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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

        // TX header layout: [FRAME_MAGIC(4)][payload_size(8)][op_count(4)][commit_ts(8)]
        // FRAME_MAGIC is at offset +0 from frame start.
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(LOG_HDR_SIZE as u64, bad, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; crc32c is at offset +0.
        let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
        let c = file
            .pwrite(trailer_offset as u64, bad, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
        let after_first = log.offset as usize;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");
        let after_second = log.offset as usize;
        let second_frame_len = after_second - after_first;

        // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; crc32c is at trailer offset +0.
        let second_trailer_crc_offset = after_first + second_frame_len - TX_TRAILER_SIZE;
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        let res = reader.read_header(&io);
        assert!(res.is_err());
    }

    /// What this test checks: v2 headers must use the fixed 56-byte length and a known version byte.
    /// Why this matters: Accepting larger lengths can misalign frame parsing and drop valid commits.
    ///   Unknown versions must not be silently misread.
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
        let original_header_bytes = header_buf.as_slice()[..LOG_HDR_SIZE].to_vec();

        // Test 1: non-default header length (LOG_HDR_SIZE + 1) with valid CRC is rejected.
        let mut header_bytes = original_header_bytes.clone();
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        let res = reader.read_header(&io);
        assert!(res.is_err());

        // Test 2: unknown version byte (99) with valid CRC is rejected as Invalid.
        let mut header_bytes = original_header_bytes;
        header_bytes[4] = 99; // unknown version
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

        let mut reader = StreamingLogicalLogReader::new(file, None);
        let result = reader.try_read_header(&io).unwrap();
        assert!(
            matches!(result, HeaderReadResult::Invalid),
            "unknown version header must be rejected as Invalid, got {result:?}"
        );
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        let res = reader.next_record(&io, |_id| {
            Err(LimboError::InternalError("no index".to_string()))
        });
        assert!(matches!(res.unwrap(), StreamingResult::Eof));
    }

    /// What this test checks: Zero-operation frames are silently skipped by the reader, and a
    /// LogRecord carrying a DatabaseHeader round-trips as UpdateHeader with all fields intact.
    /// Why this matters: Edge-case frame shapes must remain parseable to keep format handling robust.
    ///   UPDATE_HEADER is a distinct op type with its own fixed-size payload, zero-flags constraint,
    ///   zero-table_id constraint, and magic validation — none of which the table/index op tests cover.
    #[test]
    fn test_logical_log_empty_transaction_frame() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("empty-tx.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        // Frame 1: empty tx (no ops). The reader must skip it silently (ops.is_empty() → continue).
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 200,
            row_versions: vec![],
            header: None,
        };
        let c = log.log_tx(&tx).unwrap();
        io.wait_for_completion(c).unwrap();

        // Frame 2: header-only tx. DatabaseHeader::default() has the SQLite magic that passes
        // the reader's magic validation check.
        let commit_ts = 201u64;
        let db_header = DatabaseHeader::default();
        let header_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: commit_ts,
            row_versions: vec![],
            header: Some(db_header),
        };
        let c = log.log_tx(&header_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();

        // The reader skips the empty frame and returns the UpdateHeader from frame 2.
        let rec = reader
            .next_record(&io, |_id| {
                Err(LimboError::InternalError("no index".to_string()))
            })
            .unwrap();
        match rec {
            StreamingResult::UpdateHeader {
                header: recovered,
                commit_ts: recovered_ts,
            } => {
                assert_eq!(recovered_ts, commit_ts);
                assert_eq!(recovered.magic, db_header.magic);
            }
            other => panic!("expected UpdateHeader, got {other:?}"),
        }

        // Nothing left after frame 2.
        let eof = reader
            .next_record(&io, |_id| {
                Err(LimboError::InternalError("no index".to_string()))
            })
            .unwrap();
        assert!(matches!(eof, StreamingResult::Eof));
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
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

                let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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

        // Large-payload frame: 30 rows × 200 bytes ≈ 6 KB — well above the 4096-byte internal
        // read-chunk boundary. This verifies the reader stitches together multiple pread results
        // correctly when a single frame spans chunk boundaries.
        let large_commit_ts = 1_000 + 128u64;
        let large_text: String = "x".repeat(200);
        let mut large_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: large_commit_ts,
            row_versions: Vec::new(),
            header: None,
        };
        for rowid in 1..=30i64 {
            let row = generate_simple_string_row((-3).into(), rowid, &large_text);
            expected.push(ExpectedTableOp::Upsert {
                rowid,
                payload: row.payload().to_vec(),
                commit_ts: large_commit_ts,
                btree_resident: false,
            });
            large_tx
                .row_versions
                .push(crate::mvcc::database::RowVersion {
                    id: rowid as u64,
                    begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(
                        large_commit_ts,
                    )),
                    end: None,
                    row,
                    btree_resident: false,
                });
        }
        let c = log.log_tx(&large_tx).unwrap();
        io.wait_for_completion(c).unwrap();

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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
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

    /// What this test checks: The btree_resident flag survives write/read round-trip unchanged,
    /// and the on-disk frame header has the correct binary layout (FRAME_MAGIC at [0..4],
    /// payload_size as u64 at [4..12]).
    /// Why this matters: This flag affects tombstone and checkpoint behavior after recovery.
    ///   The frame layout check is baseline confirmation that the serialized format is self-consistent.
    #[test]
    fn test_logical_log_btree_resident_roundtrip() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("btree.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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

        // Verify the on-disk frame header binary layout.
        let frame_hdr_buf = Arc::new(Buffer::new_temporary(TX_HEADER_SIZE));
        let c = file
            .pread(
                LOG_HDR_SIZE as u64,
                Completion::new_read(frame_hdr_buf.clone(), |_| None),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let frame_hdr = frame_hdr_buf.as_slice()[..TX_HEADER_SIZE].to_vec();
        assert_eq!(
            u32::from_le_bytes(frame_hdr[0..4].try_into().unwrap()),
            FRAME_MAGIC,
            "FRAME_MAGIC at bytes [0..4]"
        );
        assert!(
            u64::from_le_bytes(frame_hdr[4..12].try_into().unwrap()) > 0,
            "payload_size at bytes [4..12] must be non-zero for a non-empty op"
        );

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 11, false, false, "foo");
        let c = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![0])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

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
        let mut reader = StreamingLogicalLogReader::new(file, None);
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
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        // Write 3 frames
        append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "aaa");
        let after_first = log.offset as usize;
        append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "bbb");
        append_single_table_op_tx(&mut log, &io, (-2).into(), 3, 30, false, false, "ccc");

        // Without corruption, all 3 frames should read back
        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
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
        let mut reader = StreamingLogicalLogReader::new(file, None);
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
        let mut log_a = LogicalLog::new(file_a.clone(), io.clone(), None);
        append_single_table_op_tx(&mut log_a, &io, (-2).into(), 1, 10, false, false, "aaa");
        let log_a_end = log_a.offset as usize;

        // --- Log B: write one frame (different salt → different CRC chain) ---
        let file_b = io
            .open_file("splice-b.db-log", crate::OpenFlags::Create, false)
            .unwrap();
        let mut log_b = LogicalLog::new(file_b.clone(), io.clone(), None);
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
        let mut reader = StreamingLogicalLogReader::new(file_a, None);
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

    fn test_enc_ctx() -> crate::storage::encryption::EncryptionContext {
        use crate::storage::encryption::{CipherMode, EncryptionKey};
        let key = EncryptionKey::Key128([0x42u8; 16]);
        crate::storage::encryption::EncryptionContext::new(CipherMode::Aes128Gcm, &key, 4096)
            .unwrap()
    }

    fn wrong_key_enc_ctx() -> crate::storage::encryption::EncryptionContext {
        use crate::storage::encryption::{CipherMode, EncryptionKey};
        let key = EncryptionKey::Key128([0xFFu8; 16]);
        crate::storage::encryption::EncryptionContext::new(CipherMode::Aes128Gcm, &key, 4096)
            .unwrap()
    }

    fn make_test_row_version(
        table_id: MVTableId,
        rowid: i64,
        value: &str,
        commit_ts: u64,
    ) -> crate::mvcc::database::RowVersion {
        let row = generate_simple_string_row(table_id, rowid, value);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts)),
            end: None,
            row,
            btree_resident: false,
        }
    }

    fn make_test_index_row_version(
        table_id: MVTableId,
        rowid: i64,
        value: &str,
        commit_ts: u64,
    ) -> crate::mvcc::database::RowVersion {
        let key_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(value.to_string())),
                Value::from_i64(rowid),
            ],
            2,
        );
        let sortable_key = SortableIndexKey::new_from_record(
            key_record,
            Arc::new(IndexInfo {
                has_rowid: true,
                num_cols: 2,
                is_unique: false,
                ..Default::default()
            }),
        );
        let row_id = RowID::new(table_id, RowKey::Record(sortable_key));
        let row = Row::new_index_row(row_id, 2);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts)),
            end: None,
            row,
            btree_resident: false,
        }
    }

    fn test_index_info() -> Arc<IndexInfo> {
        Arc::new(IndexInfo {
            has_rowid: true,
            num_cols: 2,
            is_unique: false,
            ..Default::default()
        })
    }

    fn make_test_raw_table_row_version(
        table_id: MVTableId,
        rowid: i64,
        record_bytes: Vec<u8>,
        commit_ts: u64,
        is_delete: bool,
    ) -> crate::mvcc::database::RowVersion {
        let row = Row::new_table_row(RowID::new(table_id, RowKey::Int(rowid)), record_bytes, 1);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: if is_delete {
                None
            } else {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            },
            end: if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            },
            row,
            btree_resident: false,
        }
    }

    fn make_test_raw_index_row_version(
        table_id: MVTableId,
        rowid: i64,
        payload_bytes: Vec<u8>,
        commit_ts: u64,
        is_delete: bool,
    ) -> crate::mvcc::database::RowVersion {
        let sortable_key = SortableIndexKey::new_from_bytes(payload_bytes, test_index_info());
        let row_id = RowID::new(table_id, RowKey::Record(sortable_key));
        let row = Row::new_index_row(row_id, 2);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: if is_delete {
                None
            } else {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            },
            end: if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            },
            row,
            btree_resident: false,
        }
    }

    fn single_upsert_table_op_size_for_text_len(rowid: i64, text_len: usize) -> usize {
        let mut encoded = Vec::new();
        let value = "x".repeat(text_len);
        let row_version = make_test_row_version((-2).into(), rowid, &value, 100);
        serialize_op_entry(&mut encoded, &row_version).unwrap();
        encoded.len()
    }

    fn try_text_len_for_single_upsert_table_op_size(
        rowid: i64,
        target_op_size: usize,
    ) -> Option<usize> {
        (0..=target_op_size).find(|&text_len| {
            single_upsert_table_op_size_for_text_len(rowid, text_len) == target_op_size
        })
    }

    fn text_len_for_single_upsert_table_op_size(target_op_size: usize) -> usize {
        if let Some(text_len) = try_text_len_for_single_upsert_table_op_size(1, target_op_size) {
            return text_len;
        }
        panic!("could not find text length for op size {target_op_size}");
    }

    fn try_record_bytes_len_for_upsert_table_op_size(
        rowid: i64,
        target_op_size: usize,
    ) -> Option<usize> {
        let rowid_len = varint_len(rowid as u64);
        for payload_len_varint_len in 1..=9usize {
            let record_bytes_len =
                target_op_size.checked_sub(6 + payload_len_varint_len + rowid_len)?;
            let payload_len = rowid_len + record_bytes_len;
            if varint_len(payload_len as u64) == payload_len_varint_len {
                return Some(record_bytes_len);
            }
        }
        None
    }

    fn read_file_bytes(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>) -> Vec<u8> {
        let file_size = file.size().unwrap() as usize;
        if file_size == 0 {
            return Vec::new();
        }
        let reader = StreamingLogicalLogReader::new(file, None);
        reader.read_exact_at(io, 0, file_size).unwrap()
    }

    fn overwrite_file_bytes(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>, bytes: &[u8]) {
        let c = file.truncate(0, Completion::new_trunc(|_| {})).unwrap();
        io.wait_for_completion(c).unwrap();
        if bytes.is_empty() {
            return;
        }
        let c = file
            .pwrite(
                0,
                Arc::new(Buffer::new(bytes.to_vec())),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
    }

    fn open_test_file(io: &Arc<dyn crate::IO>, file_name: &str) -> Arc<dyn crate::File> {
        io.open_file(file_name, OpenFlags::Create, false).unwrap()
    }

    fn append_encrypted_tx(
        log: &mut LogicalLog,
        io: &Arc<dyn crate::IO>,
        tx: &crate::mvcc::database::LogRecord,
    ) {
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    fn write_first_encrypted_tx(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        tx: &crate::mvcc::database::LogRecord,
    ) {
        assert_eq!(
            file.size().unwrap(),
            0,
            "write_first_encrypted_tx only supports writing the first frame to a fresh file"
        );
        let mut log = LogicalLog::new(file, io.clone(), Some(enc_ctx.clone()));
        append_encrypted_tx(&mut log, io, tx);
    }

    fn write_first_encrypted_tx_with_chunk_size_for_test(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        encrypted_payload_chunk_size: usize,
        tx: &crate::mvcc::database::LogRecord,
    ) {
        assert_eq!(
            file.size().unwrap(),
            0,
            "write_first_encrypted_tx_with_chunk_size_for_test only supports writing the first frame to a fresh file"
        );
        let mut log = LogicalLog::new_with_payload_chunk_size(
            file,
            io.clone(),
            Some(enc_ctx.clone()),
            encrypted_payload_chunk_size,
        );
        append_encrypted_tx(&mut log, io, tx);
    }

    fn write_single_encrypted_tx(
        io: &Arc<dyn crate::IO>,
        file_name: &str,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        tx: &crate::mvcc::database::LogRecord,
    ) -> Arc<dyn crate::File> {
        let file = open_test_file(io, file_name);
        write_first_encrypted_tx(file.clone(), io, enc_ctx, tx);
        file
    }

    fn write_single_encrypted_tx_with_chunk_size_for_test(
        io: &Arc<dyn crate::IO>,
        file_name: &str,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        encrypted_payload_chunk_size: usize,
        tx: &crate::mvcc::database::LogRecord,
    ) -> Arc<dyn crate::File> {
        let file = open_test_file(io, file_name);
        write_first_encrypted_tx_with_chunk_size_for_test(
            file.clone(),
            io,
            enc_ctx,
            encrypted_payload_chunk_size,
            tx,
        );
        file
    }

    fn write_encrypted_txs_with_chunk_size_for_test(
        io: &Arc<dyn crate::IO>,
        file_name: &str,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        encrypted_payload_chunk_size: usize,
        txs: &[crate::mvcc::database::LogRecord],
    ) -> Arc<dyn crate::File> {
        let file = open_test_file(io, file_name);
        let mut log = LogicalLog::new_with_payload_chunk_size(
            file.clone(),
            io.clone(),
            Some(enc_ctx.clone()),
            encrypted_payload_chunk_size,
        );
        for tx in txs {
            append_encrypted_tx(&mut log, io, tx);
        }
        file
    }

    fn parse_only_encrypted_tx_ops(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
    ) -> Vec<ParsedOp> {
        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
        reader.read_header(io).unwrap();
        let ops = match reader.parse_next_transaction(io).unwrap() {
            ParseResult::Ops(ops) => ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert!(matches!(
            reader.parse_next_transaction(io).unwrap(),
            ParseResult::Eof
        ));
        ops
    }

    fn parse_only_encrypted_tx_ops_with_chunk_size_for_test(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        encrypted_payload_chunk_size: usize,
    ) -> Vec<ParsedOp> {
        let mut reader = StreamingLogicalLogReader::new_with_payload_chunk_size(
            file,
            Some(enc_ctx.clone()),
            encrypted_payload_chunk_size,
        );
        reader.read_header(io).unwrap();
        let ops = match reader.parse_next_transaction(io).unwrap() {
            ParseResult::Ops(ops) => ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert!(matches!(
            reader.parse_next_transaction(io).unwrap(),
            ParseResult::Eof
        ));
        ops
    }

    fn parse_all_encrypted_tx_ops_with_chunk_size_for_test(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        encrypted_payload_chunk_size: usize,
    ) -> std::result::Result<Vec<Vec<ParsedOp>>, String> {
        let mut reader = StreamingLogicalLogReader::new_with_payload_chunk_size(
            file,
            Some(enc_ctx.clone()),
            encrypted_payload_chunk_size,
        );
        reader
            .read_header(io)
            .map_err(|e| format!("failed to read fuzz log header: {e}"))?;
        let mut frames = Vec::new();
        let mut tx_index = 0usize;
        loop {
            match reader
                .parse_next_transaction(io)
                .map_err(|e| format!("failed to parse fuzz frame {tx_index}: {e}"))?
            {
                ParseResult::Ops(ops) => frames.push(ops),
                ParseResult::Eof => break,
                ParseResult::InvalidFrame => {
                    return Err(format!("invalid fuzz frame at tx_index={tx_index}"));
                }
            }
            tx_index += 1;
        }
        Ok(frames)
    }

    fn assert_upsert_table_op(
        op: &ParsedOp,
        expected_table_id: MVTableId,
        expected_rowid: i64,
        expected_record_bytes: &[u8],
        expected_commit_ts: u64,
    ) {
        match op {
            ParsedOp::UpsertTable {
                table_id,
                rowid,
                record_bytes,
                commit_ts,
                btree_resident,
            } => {
                assert_eq!(*table_id, expected_table_id);
                assert_eq!(rowid.row_id, RowKey::Int(expected_rowid));
                assert_eq!(record_bytes, expected_record_bytes);
                assert_eq!(*commit_ts, expected_commit_ts);
                assert!(!btree_resident);
            }
            other => panic!("expected UpsertTable, got {other:?}"),
        }
    }

    fn assert_upsert_index_op(
        op: &ParsedOp,
        expected_table_id: MVTableId,
        expected_payload: &[u8],
        expected_commit_ts: u64,
    ) {
        match op {
            ParsedOp::UpsertIndex {
                table_id,
                payload,
                commit_ts,
                btree_resident,
            } => {
                assert_eq!(*table_id, expected_table_id);
                assert_eq!(payload, expected_payload);
                assert_eq!(*commit_ts, expected_commit_ts);
                assert!(!btree_resident);
            }
            other => panic!("expected UpsertIndex, got {other:?}"),
        }
    }

    fn assert_update_header_op(
        op: &ParsedOp,
        expected_header: &DatabaseHeader,
        expected_commit_ts: u64,
    ) {
        match op {
            ParsedOp::UpdateHeader { header, commit_ts } => {
                assert_eq!(*commit_ts, expected_commit_ts);
                assert_eq!(
                    bytemuck::bytes_of(header),
                    bytemuck::bytes_of(expected_header)
                );
            }
            other => panic!("expected UpdateHeader, got {other:?}"),
        }
    }

    // Generate one record-bytes length from buckets that bias heavily toward
    // chunk boundaries, while still mixing in smaller values.
    fn encrypted_carry_fuzz_record_bytes_len(
        rng: &mut ChaCha8Rng,
        rowid: i64,
        chunk_size: usize,
    ) -> usize {
        // Sometimes force the whole serialized upsert op to land exactly on a chunk multiple.
        if rng.random_range(0..4) == 0 {
            let exact_op_size = rng.random_range(1..=3) * chunk_size;
            if let Some(record_bytes_len) =
                try_record_bytes_len_for_upsert_table_op_size(rowid, exact_op_size)
            {
                return record_bytes_len;
            }
        }

        let jitter = rng.random_range(0..=16) as isize - 8;
        let base = match rng.random_range(0..15) {
            0 => 1usize,
            1 => 16usize,
            2 => chunk_size,
            3 => chunk_size + 1,
            4 => chunk_size - 1,
            5 => 2 * chunk_size,
            6 => 2 * chunk_size + 1,
            7 => 2 * chunk_size - 1,
            8 => 3 * chunk_size,
            9 => 3 * chunk_size + 1,
            10 => chunk_size / 2,
            11 => chunk_size + chunk_size / 2,
            12 => 2 * chunk_size + chunk_size / 2,
            13 => random_range(1..=16usize) + random_range(0..=chunk_size),
            14 => random_range(1..=chunk_size),
            _ => rng.random_range(1..=3) * chunk_size,
        } as isize;
        (base + jitter).max(1) as usize
    }

    fn expected_upsert_table_fuzz_op(
        row_version: &crate::mvcc::database::RowVersion,
        rowid: i64,
        commit_ts: u64,
    ) -> ParsedOp {
        ParsedOp::UpsertTable {
            table_id: (-2).into(),
            rowid: RowID::new((-2).into(), RowKey::Int(rowid)),
            record_bytes: row_version.row.payload().to_vec(),
            commit_ts,
            btree_resident: false,
        }
    }

    fn assert_forced_upsert_carry_prefix_layout(
        short_filler: &crate::mvcc::database::RowVersion,
        short_upsert: &crate::mvcc::database::RowVersion,
        long_upsert: &crate::mvcc::database::RowVersion,
        chunk_size: usize,
    ) {
        let mut filler_buf = Vec::new();
        serialize_op_entry(&mut filler_buf, short_filler).unwrap();
        let mut short_upsert_buf = Vec::new();
        serialize_op_entry(&mut short_upsert_buf, short_upsert).unwrap();
        let mut long_upsert_buf = Vec::new();
        serialize_op_entry(&mut long_upsert_buf, long_upsert).unwrap();

        turso_assert_less_than!(
            filler_buf.len(),
            chunk_size,
            "forced short-carry filler upsert must fit before the first chunk boundary"
        );
        let short_split_offset = chunk_size - filler_buf.len();
        turso_assert!(
            short_split_offset > 0 && short_split_offset < short_upsert_buf.len(),
            "forced short carry must end the first chunk inside the short upsert"
        );
        turso_assert_less_than!(
            short_upsert_buf.len(),
            StreamingLogicalLogReader::MAX_SERIALIZED_OP_PREFIX_LEN,
            "forced short carry upsert must remain below MAX_SERIALIZED_OP_PREFIX_LEN"
        );

        let long_start_offset = (filler_buf.len() + short_upsert_buf.len()) % chunk_size;
        turso_assert!(
            long_start_offset > 0,
            "forced long carry upsert must begin inside a chunk, not on a chunk boundary"
        );
        turso_assert!(
            long_upsert_buf.len() > 2 * chunk_size,
            "forced long carry upsert must span more than two chunk widths"
        );
    }

    fn append_forced_upsert_carry_prefix(
        rng: &mut ChaCha8Rng,
        chunk_size: usize,
        commit_ts: u64,
        row_versions: &mut Vec<crate::mvcc::database::RowVersion>,
        expected_ops: &mut Vec<ParsedOp>,
    ) {
        // Every forced case starts with:
        // 1. an upsert filler that lands the chunk boundary inside the next upsert
        // 2. a short carried upsert whose total size is below MAX_SERIALIZED_OP_PREFIX_LEN
        // 3. a long carried upsert that spans more than two later chunks
        let short_rowid = 0i64;
        let short_record_bytes = vec![0x11];
        let short_upsert = make_test_raw_table_row_version(
            (-2).into(),
            short_rowid,
            short_record_bytes,
            commit_ts,
            false,
        );
        let mut short_upsert_buf = Vec::new();
        serialize_op_entry(&mut short_upsert_buf, &short_upsert).unwrap();
        turso_assert_less_than!(
            short_upsert_buf.len(),
            StreamingLogicalLogReader::MAX_SERIALIZED_OP_PREFIX_LEN,
            "forced short carry upsert must remain below MAX_SERIALIZED_OP_PREFIX_LEN"
        );

        let split_offset = rng.random_range(1..short_upsert_buf.len());
        let filler_op_size = chunk_size - split_offset;
        let filler_record_bytes_len =
            try_record_bytes_len_for_upsert_table_op_size(1, filler_op_size)
                .expect("forced filler upsert size must map to a valid record_bytes length");
        let short_filler = make_test_raw_table_row_version(
            (-2).into(),
            1,
            vec![0x22; filler_record_bytes_len],
            commit_ts,
            false,
        );
        let long_upsert = make_test_raw_table_row_version(
            (-2).into(),
            2,
            vec![0x5A; 2 * chunk_size + rng.random_range(64..=256)],
            commit_ts,
            false,
        );
        assert_forced_upsert_carry_prefix_layout(
            &short_filler,
            &short_upsert,
            &long_upsert,
            chunk_size,
        );

        expected_ops.push(expected_upsert_table_fuzz_op(&short_filler, 1, commit_ts));
        row_versions.push(short_filler);

        expected_ops.push(expected_upsert_table_fuzz_op(
            &short_upsert,
            short_rowid,
            commit_ts,
        ));
        row_versions.push(short_upsert);

        expected_ops.push(expected_upsert_table_fuzz_op(&long_upsert, 2, commit_ts));
        row_versions.push(long_upsert);
    }

    fn generate_random_encrypted_carry_fuzz_upsert(
        rng: &mut ChaCha8Rng,
        rowid: i64,
        chunk_size: usize,
        commit_ts: u64,
    ) -> (crate::mvcc::database::RowVersion, ParsedOp) {
        // first generate a random payload size
        let record_bytes_len = encrypted_carry_fuzz_record_bytes_len(rng, rowid, chunk_size);
        let row_version = make_test_raw_table_row_version(
            (-2).into(),
            rowid,
            vec![(rowid as u8).wrapping_add(1); record_bytes_len],
            commit_ts,
            false,
        );
        let expected = expected_upsert_table_fuzz_op(&row_version, rowid, commit_ts);
        (row_version, expected)
    }

    /// given a seed, generate fuzz plan with all kinds of random payload sizes.
    fn generate_encrypted_carry_fuzz_case(
        case_seed: u64,
        chunk_size: usize,
        include_forced_prefix: bool,
    ) -> (Vec<crate::mvcc::database::LogRecord>, Vec<Vec<ParsedOp>>) {
        let mut rng = ChaCha8Rng::seed_from_u64(case_seed);
        let tx_count = rng.random_range(1..=3);
        let mut txs = Vec::with_capacity(tx_count);
        let mut expected_frames = Vec::with_capacity(tx_count);

        for tx_index in 0..tx_count {
            let commit_ts = 1_000 + (rng.next_u64() % 1_000_000) + tx_index as u64;
            let op_count = rng.random_range(1..=20);

            let mut row_versions = Vec::with_capacity(op_count);
            let mut expected_ops = Vec::with_capacity(op_count);
            // When requested, the first tx begins with two deliberate upsert carry scenarios:
            // - a short carried upsert that ends the first chunk inside a sub-15-byte op
            // - a long carried upsert that starts mid-chunk and spans more than two later chunks
            if tx_index == 0 && include_forced_prefix {
                append_forced_upsert_carry_prefix(
                    &mut rng,
                    chunk_size,
                    commit_ts,
                    &mut row_versions,
                    &mut expected_ops,
                );
            }

            while row_versions.len() < op_count {
                let rowid = (row_versions.len() + 1) as i64;
                let (row_version, expected_op) = generate_random_encrypted_carry_fuzz_upsert(
                    &mut rng, rowid, chunk_size, commit_ts,
                );
                row_versions.push(row_version);
                expected_ops.push(expected_op);
            }

            txs.push(crate::mvcc::database::LogRecord {
                tx_timestamp: commit_ts,
                row_versions,
                header: None,
            });
            expected_frames.push(expected_ops);
        }

        (txs, expected_frames)
    }

    // Returns the byte ranges of each encrypted chunk within a frame's payload blob,
    // where every chunk occupies plaintext_len + tag_size + nonce_size bytes on disk.
    fn encrypted_chunk_ranges(
        payload_size: usize,
        tag_size: usize,
        nonce_size: usize,
    ) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::new();
        let mut offset = 0usize;
        for chunk_index in
            0..encrypted_payload_chunk_count(payload_size, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
        {
            let plaintext_len = encrypted_chunk_plaintext_len(
                payload_size,
                chunk_index,
                ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            )
            .unwrap();
            let chunk_len = encrypted_chunk_blob_size(plaintext_len, tag_size, nonce_size).unwrap();
            ranges.push(offset..offset + chunk_len);
            offset += chunk_len;
        }
        ranges
    }

    fn assert_single_frame_invalid(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: crate::storage::encryption::EncryptionContext,
    ) {
        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(io).unwrap();
        match reader.parse_next_transaction(io).unwrap() {
            ParseResult::InvalidFrame => {}
            other => panic!("expected InvalidFrame, got {other:?}"),
        }
    }

    /// Write an encrypted frame, verify the on-disk layout invariant
    /// (`plaintext + per-chunk tag/nonce metadata`), then read back and
    /// verify roundtrip correctness with multiple ops.
    #[test]
    fn test_encrypted_log_roundtrip_and_layout() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = open_test_file(&io, "enc-roundtrip.db-log");
        let table_id: MVTableId = (-2).into();
        let enc_ctx = test_enc_ctx();
        let tag_size = enc_ctx.tag_size();
        let nonce_size = enc_ctx.nonce_size();
        let expected_hello_record_bytes = generate_simple_string_row(table_id, 1, "hello")
            .payload()
            .to_vec();
        let expected_world_record_bytes = generate_simple_string_row(table_id, 2, "world")
            .payload()
            .to_vec();

        // Write one encrypted frame with 2 ops.
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![
                make_test_row_version(table_id, 1, "hello", 100),
                make_test_row_version(table_id, 2, "world", 100),
            ],
            header: None,
        };
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, &tx);

        // ── Layout invariant check ──
        // Read the raw TX header to extract payload_size.
        let frame_hdr_buf = Arc::new(Buffer::new_temporary(TX_HEADER_SIZE));
        let frame_hdr_out = Arc::new(crate::sync::RwLock::new(Vec::new()));
        let out = frame_hdr_out.clone();
        let c = Completion::new_read(
            frame_hdr_buf,
            Box::new(
                move |res: std::result::Result<(Arc<Buffer>, i32), crate::CompletionError>| {
                    let Ok((buf, n)) = res else { return None };
                    out.write().extend_from_slice(&buf.as_slice()[..n as usize]);
                    None
                },
            ),
        );
        let c = file.pread(LOG_HDR_SIZE as u64, c).unwrap();
        io.wait_for_completion(c).unwrap();

        let frame_hdr = frame_hdr_out.read();
        assert_eq!(frame_hdr.len(), TX_HEADER_SIZE);
        let payload_size = u64::from_le_bytes(frame_hdr[4..12].try_into().unwrap()) as usize;

        let file_size = file.size().unwrap() as usize;
        let encrypted_blob_size = file_size - LOG_HDR_SIZE - TX_HEADER_SIZE - TX_TRAILER_SIZE;
        let expected_blob_size = encrypted_payload_blob_size(
            payload_size,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            tag_size,
            nonce_size,
        )
        .unwrap();
        assert_eq!(
            encrypted_blob_size,
            expected_blob_size,
            "on-disk blob size ({encrypted_blob_size}) != expected chunked encrypted size({expected_blob_size})"
        );

        // ── Roundtrip read ──
        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        let ops = match reader.parse_next_transaction(&io).unwrap() {
            ParseResult::Ops(ops) => ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert_eq!(ops.len(), 2);
        assert_upsert_table_op(&ops[0], table_id, 1, &expected_hello_record_bytes, 100);
        assert_upsert_table_op(&ops[1], table_id, 2, &expected_world_record_bytes, 100);

        assert!(matches!(
            reader.parse_next_transaction(&io).unwrap(),
            ParseResult::Eof
        ));
    }

    /// What this test checks: Test-only chunk-size overrides affect both encrypted writing and
    /// streaming recovery, so fuzz tests can exercise smaller chunk boundaries without changing
    /// the production format constant.
    #[test]
    fn test_encrypted_log_roundtrip_with_test_chunk_size_override() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        const TEST_CHUNK_SIZE: usize = 2 * 1024;

        let target_op_size = TEST_CHUNK_SIZE + 257;
        let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
        let value = "t".repeat(text_len);
        let row_version = make_test_row_version((-2).into(), 1, &value, 100);
        let expected_record_bytes = row_version.row.payload().to_vec();
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![row_version],
            header: None,
        };

        let file = write_single_encrypted_tx_with_chunk_size_for_test(
            &io,
            "enc-roundtrip-test-chunk-size.db-log",
            &enc_ctx,
            TEST_CHUNK_SIZE,
            &tx,
        );

        assert_eq!(
            encrypted_payload_chunk_count(target_op_size, TEST_CHUNK_SIZE),
            2,
            "test payload should span exactly two test-sized chunks"
        );
        let expected_blob_size = encrypted_payload_blob_size(
            target_op_size,
            TEST_CHUNK_SIZE,
            enc_ctx.tag_size(),
            enc_ctx.nonce_size(),
        )
        .unwrap();
        assert_eq!(
            file.size().unwrap() as usize,
            LOG_HDR_SIZE + TX_HEADER_SIZE + expected_blob_size + TX_TRAILER_SIZE
        );

        let ops = parse_only_encrypted_tx_ops_with_chunk_size_for_test(
            file,
            &io,
            &enc_ctx,
            TEST_CHUNK_SIZE,
        );
        assert_eq!(ops.len(), 1);
        assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
    }

    /// Random fuzzer to test encrypted chunking logic, especially carry.
    /// We create a plan from a seed, then generate ops, write to encrypted log file and read it back
    #[test]
    fn test_encrypted_log_carry_fuzz() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        const TEST_CHUNK_SIZE: usize = 2 * 1024;

        let seed = std::env::var("TURSO_ENCRYPTED_CARRY_FUZZ_SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| rng().random::<u64>());
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let case_count = rng.random_range(1..=8);
        let forced_case_index = rng.random_range(0..case_count);
        eprintln!(
            "encrypted carry fuzz root_seed={seed} case_count={case_count} forced_case_index={forced_case_index} test_chunk_size={TEST_CHUNK_SIZE}"
        );

        for case_index in 0..case_count {
            let case_seed = rng.next_u64();
            let include_forced_prefix = case_index == forced_case_index;
            let (txs, expected_frames) = generate_encrypted_carry_fuzz_case(
                case_seed,
                TEST_CHUNK_SIZE,
                include_forced_prefix,
            );

            let file = write_encrypted_txs_with_chunk_size_for_test(
                &io,
                &format!("enc-carry-fuzz-{seed}-{case_index}.db-log"),
                &enc_ctx,
                TEST_CHUNK_SIZE,
                &txs,
            );
            let actual_frames = parse_all_encrypted_tx_ops_with_chunk_size_for_test(
                file,
                &io,
                &enc_ctx,
                TEST_CHUNK_SIZE,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "encrypted carry fuzz failed while parsing frames: root_seed={seed} case_index={case_index} forced_case_index={forced_case_index} include_forced_prefix={include_forced_prefix} case_seed={case_seed} err={err}"
                )
            });

            assert_eq!(
                actual_frames,
                expected_frames,
                "encrypted carry fuzz failed: root_seed={seed} case_index={case_index} forced_case_index={forced_case_index} include_forced_prefix={include_forced_prefix} case_seed={case_seed}"
            );
        }
    }

    #[test]
    fn test_encrypted_log_format_assumptions_are_pinned() {
        assert_eq!(LOG_VERSION, 2);
        assert_eq!(LOG_HDR_SIZE, 56);
        assert_eq!(ENCRYPTED_PAYLOAD_CHUNK_SIZE, 32 * 1024);
        assert_eq!(ENCRYPTED_CHUNK_AAD_SIZE, 32);
        assert_eq!(FRAME_MAGIC, 0x5854_564D);
        assert_eq!(END_MAGIC, 0x4554_564D);
        assert_eq!(TX_HEADER_SIZE, 24);
        assert_eq!(TX_TRAILER_SIZE, 8);
    }

    #[test]
    fn test_encrypted_chunk_aad_layout_is_pinned() {
        let non_last_aad = build_encrypted_chunk_aad(
            0x0102_0304_0506_0708,
            None,
            0x2122_2324,
            0x3132_3334_3536_3738,
            0x4142_4344,
        );
        assert_eq!(
            non_last_aad,
            [
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // salt
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, // payload_size omitted for non-final chunk
                0x24, 0x23, 0x22, 0x21, // op_count
                0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, // commit_ts
                0x44, 0x43, 0x42, 0x41, // chunk_index
            ]
        );

        let last_aad = build_encrypted_chunk_aad(
            0x0102_0304_0506_0708,
            Some(0x1112_1314_1516_1718),
            0x2122_2324,
            0x3132_3334_3536_3738,
            0x4142_4344,
        );

        assert_eq!(
            last_aad,
            [
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // salt
                0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12,
                0x11, // payload_size (final chunk only)
                0x24, 0x23, 0x22, 0x21, // op_count
                0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, // commit_ts
                0x44, 0x43, 0x42, 0x41, // chunk_index
            ]
        );
    }

    #[test]
    fn test_encrypted_log_aes128_chunk_layout_assumptions_are_pinned() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();

        assert_eq!(
            enc_ctx.cipher_mode(),
            crate::storage::encryption::CipherMode::Aes128Gcm
        );
        assert_eq!(enc_ctx.tag_size(), 16);
        assert_eq!(enc_ctx.nonce_size(), 12);

        for (payload_size, expected_chunk_ranges, expected_file_size) in [
            (
                32_767usize,
                std::iter::once(0..32_795).collect::<Vec<_>>(),
                32_883usize,
            ),
            (
                32_768usize,
                std::iter::once(0..32_796).collect::<Vec<_>>(),
                32_884usize,
            ),
            (32_769usize, vec![0..32_796, 32_796..32_825], 32_913usize),
            (65_536usize, vec![0..32_796, 32_796..65_592], 65_680usize),
            (
                65_537usize,
                vec![0..32_796, 32_796..65_592, 65_592..65_621],
                65_709usize,
            ),
        ] {
            let text_len = text_len_for_single_upsert_table_op_size(payload_size);
            let value = "p".repeat(text_len);
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100,
                row_versions: vec![make_test_row_version((-2).into(), 1, &value, 100)],
                header: None,
            };
            let file = write_single_encrypted_tx(
                &io,
                &format!("enc-layout-pinned-{payload_size}.db-log"),
                &enc_ctx,
                &tx,
            );

            let frame_bytes = read_file_bytes(file.clone(), &io);
            let actual_payload_size = u64::from_le_bytes(
                frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                    .try_into()
                    .unwrap(),
            ) as usize;
            assert_eq!(actual_payload_size, payload_size);
            assert_eq!(file.size().unwrap() as usize, expected_file_size);
            assert_eq!(
                encrypted_chunk_ranges(payload_size, 16, 12),
                expected_chunk_ranges
            );
        }
    }

    // Verifies the final chunk authenticates payload_size: tampering the TX header's
    // payload_size field must still reject the encrypted frame.
    #[test]
    fn test_encrypted_log_payload_size_tamper_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-payload-size-tamper.db-log", OpenFlags::Create, false)
            .unwrap();
        let enc_ctx = test_enc_ctx();
        let table_id: MVTableId = (-2).into();
        let text_len =
            text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
        let value = "s".repeat(text_len);

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 444,
            row_versions: vec![make_test_row_version(table_id, 1, &value, 444)],
            header: None,
        };
        append_encrypted_tx(&mut log, &io, &tx);

        let frame_bytes = read_file_bytes(file.clone(), &io);
        let payload_size = u64::from_le_bytes(
            frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        );
        let bad_payload_size = Arc::new(Buffer::new((payload_size + 1).to_le_bytes().to_vec()));
        let c = Completion::new_write(|_| {});
        io.wait_for_completion(
            file.pwrite((LOG_HDR_SIZE + 4) as u64, bad_payload_size, c)
                .unwrap(),
        )
        .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();
        match reader.parse_next_transaction(&io).unwrap() {
            ParseResult::InvalidFrame => {}
            other => panic!("expected InvalidFrame after payload_size tamper, got {other:?}"),
        }
    }

    #[test]
    fn test_encrypted_log_chunk_layout_boundaries() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        let tag_size = enc_ctx.tag_size();
        let nonce_size = enc_ctx.nonce_size();

        for target_op_size in [
            ENCRYPTED_PAYLOAD_CHUNK_SIZE - 1,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE + 1,
            2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 1,
        ] {
            let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
            let value = "x".repeat(text_len);
            let row_version = make_test_row_version((-2).into(), 1, &value, 100);
            let expected_record_bytes = row_version.row.payload().to_vec();
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100,
                row_versions: vec![row_version],
                header: None,
            };
            let file = write_single_encrypted_tx(
                &io,
                &format!("enc-layout-{target_op_size}.db-log"),
                &enc_ctx,
                &tx,
            );

            let frame_hdr = read_file_bytes(file.clone(), &io);
            let payload_size = u64::from_le_bytes(
                frame_hdr[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                    .try_into()
                    .unwrap(),
            ) as usize;
            assert_eq!(payload_size, target_op_size);

            let file_size = file.size().unwrap() as usize;
            let encrypted_blob_size = file_size - LOG_HDR_SIZE - TX_HEADER_SIZE - TX_TRAILER_SIZE;
            let expected_blob_size = encrypted_payload_blob_size(
                payload_size,
                ENCRYPTED_PAYLOAD_CHUNK_SIZE,
                tag_size,
                nonce_size,
            )
            .unwrap();
            assert_eq!(encrypted_blob_size, expected_blob_size);

            let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
            assert_eq!(ops.len(), 1);
            assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
        }
    }

    #[test]
    fn test_encrypted_log_single_op_crosses_chunk_boundary() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        let target_op_size = ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257;
        let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
        let value = "x".repeat(text_len);
        let row_version = make_test_row_version((-2).into(), 1, &value, 100);
        let expected_record_bytes = row_version.row.payload().to_vec();

        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![row_version],
            header: None,
        };
        let file = write_single_encrypted_tx(&io, "enc-cross-boundary.db-log", &enc_ctx, &tx);
        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 1);
        assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
    }

    // Verifies the reader can reconstruct a payload_len varint that is split across
    // two encrypted chunks, without changing either row payload.
    #[test]
    fn test_encrypted_log_varint_crosses_chunk_boundary() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-varint-boundary.db-log", OpenFlags::Create, false)
            .unwrap();
        let enc_ctx = test_enc_ctx();
        // Keep the first op 7 bytes short of a full chunk so the second op begins with:
        // 6-byte op prelude (tag + flags + table_id) and then 1 byte of payload_len varint.
        // That places the chunk boundary immediately after the first varint byte.
        let filler_len = text_len_for_single_upsert_table_op_size(ENCRYPTED_PAYLOAD_CHUNK_SIZE - 7);
        let filler_value = "a".repeat(filler_len);
        let second_value = "b".repeat(200);
        let filler = make_test_row_version((-2).into(), 1, &filler_value, 100);
        let second = make_test_row_version((-2).into(), 2, &second_value, 100);
        let expected_filler_record_bytes = filler.row.payload().to_vec();
        let expected_second_record_bytes = second.row.payload().to_vec();

        let mut filler_buf = Vec::new();
        serialize_op_entry(&mut filler_buf, &filler).unwrap();
        assert_eq!(filler_buf.len(), ENCRYPTED_PAYLOAD_CHUNK_SIZE - 7);

        let mut second_buf = Vec::new();
        serialize_op_entry(&mut second_buf, &second).unwrap();
        // Table ops begin with a fixed 6-byte prelude:
        // 1 byte op tag + 1 byte flags + 4 bytes table_id.
        // The payload_len varint begins immediately after that prefix.
        let (_, varint_bytes) = read_varint_partial(&second_buf[6..]).unwrap().unwrap();
        assert!(
            varint_bytes >= 2,
            "second op payload_len must use a multi-byte varint so the chunk boundary can split it"
        );
        // filler_buf.len() consumes the prefix of the chunk, then the second op contributes:
        // 6 bytes of fixed prelude + exactly 1 byte of payload_len varint before the boundary.
        // That forces the remaining varint bytes into the next encrypted chunk.
        assert_eq!(
            filler_buf.len() + 6 + 1,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            "chunk boundary should fall after the first payload_len varint byte"
        );

        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![filler, second],
            header: None,
        };
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, &tx);
        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 2);
        assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_filler_record_bytes, 100);
        assert_upsert_table_op(&ops[1], (-2).into(), 2, &expected_second_record_bytes, 100);
    }

    // Verifies a transaction header update still round-trips when the OP_UPDATE_HEADER
    // entry itself is split across an encrypted chunk boundary.
    #[test]
    fn test_encrypted_log_header_op_crosses_chunk_boundary() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-header-boundary.db-log", OpenFlags::Create, false)
            .unwrap();
        let enc_ctx = test_enc_ctx();
        let mut header_buf = Vec::new();
        let mut header = DatabaseHeader::default();
        header.database_size = 123.into();
        header.schema_cookie = 456.into();
        serialize_header_entry(&mut header_buf, &header);

        let filler_payload_size = ENCRYPTED_PAYLOAD_CHUNK_SIZE - (header_buf.len() - 1);
        let filler_len = text_len_for_single_upsert_table_op_size(filler_payload_size);
        let filler_value = "h".repeat(filler_len);
        let filler = make_test_row_version((-2).into(), 1, &filler_value, 100);
        let expected_filler_record_bytes = filler.row.payload().to_vec();

        let mut filler_buf = Vec::new();
        serialize_op_entry(&mut filler_buf, &filler).unwrap();
        assert_eq!(filler_buf.len(), filler_payload_size);
        assert_eq!(
            filler_buf.len() + header_buf.len() - 1,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            "chunk boundary should split the header op after its first byte"
        );

        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![filler],
            header: Some(header),
        };
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, &tx);
        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 2);
        assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_filler_record_bytes, 100);
        assert_update_header_op(&ops[1], &header, 100);
    }

    // Verifies the chunked reader can walk a long sequence of table upserts whose
    // boundaries land both between ops and in the middle of serialized row payloads.
    #[test]
    fn test_encrypted_log_many_ops_cross_chunk_boundaries() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-many-ops.db-log", OpenFlags::Create, false)
            .unwrap();
        let enc_ctx = test_enc_ctx();
        let table_id: MVTableId = (-2).into();

        let row_versions = (0..96)
            .map(|rowid| {
                let value = format!("row-{rowid}-{}", "x".repeat(900));
                make_test_row_version(table_id, rowid + 1, &value, 200)
            })
            .collect::<Vec<_>>();
        let expected_record_bytes = row_versions
            .iter()
            .map(|row_version| row_version.row.payload().to_vec())
            .collect::<Vec<_>>();
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 200,
            row_versions,
            header: None,
        };
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, &tx);
        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 96);
        for (idx, op) in ops.iter().enumerate() {
            assert_upsert_table_op(
                op,
                table_id,
                (idx + 1) as i64,
                &expected_record_bytes[idx],
                200,
            );
        }
    }

    // Verifies a large index-key payload is chunked, decrypted, and parsed back as an
    // UpsertIndex op without changing the serialized key bytes.
    #[test]
    fn test_encrypted_log_upsert_index_crosses_chunk_boundary() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        let index_id: MVTableId = (-3).into();
        let value = "i".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 2);
        let row_version = make_test_index_row_version(index_id, 42, &value, 250);
        let expected_payload = row_version.row.payload().to_vec();

        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 250,
            row_versions: vec![row_version],
            header: None,
        };
        let file = write_single_encrypted_tx(&io, "enc-index-boundary.db-log", &enc_ctx, &tx);

        let frame_bytes = read_file_bytes(file.clone(), &io);
        let payload_size = u64::from_le_bytes(
            frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(
            payload_size > ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            "index payload should span multiple encrypted chunks"
        );

        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 1);
        assert_upsert_index_op(&ops[0], index_id, &expected_payload, 250);
    }

    // Verifies CRC chaining across multiple encrypted frames while still preserving the
    // exact row payload bytes in every successfully parsed frame.
    #[test]
    fn test_encrypted_log_multiple_frames_crc_chain() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-multi.db-log", OpenFlags::Create, false)
            .unwrap();
        let table_id: MVTableId = (-2).into();
        let enc_ctx = test_enc_ctx();
        let expected_record_bytes = (0..5u64)
            .map(|i| generate_simple_string_row(table_id, i as i64, &format!("val_{i}")))
            .map(|row| row.payload().to_vec())
            .collect::<Vec<_>>();

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        for i in 0..5u64 {
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100 + i,
                row_versions: vec![make_test_row_version(
                    table_id,
                    i as i64,
                    &format!("val_{i}"),
                    100 + i,
                )],
                header: None,
            };
            append_encrypted_tx(&mut log, &io, &tx);
        }

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        for i in 0..5u64 {
            let ops = match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::Ops(ops) => ops,
                other => panic!("frame {i}: expected Ops, got {other:?}"),
            };
            assert_eq!(ops.len(), 1, "frame {i}");
            assert_upsert_table_op(
                &ops[0],
                table_id,
                i as i64,
                &expected_record_bytes[i as usize],
                100 + i,
            );
        }

        assert!(matches!(
            reader.parse_next_transaction(&io).unwrap(),
            ParseResult::Eof
        ));
    }

    /// AEAD integrity: wrong key and tampered ciphertext must both be rejected.
    #[test]
    fn test_encrypted_log_integrity_rejection() {
        init_tracing();
        let table_id: MVTableId = (-2).into();
        let enc_ctx = test_enc_ctx();

        // ── Wrong key ──
        {
            let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
            let file = io
                .open_file("enc-wrongkey.db-log", OpenFlags::Create, false)
                .unwrap();

            let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100,
                row_versions: vec![make_test_row_version(table_id, 1, "secret", 100)],
                header: None,
            };
            append_encrypted_tx(&mut log, &io, &tx);

            let mut reader = StreamingLogicalLogReader::new(file, Some(wrong_key_enc_ctx()));
            reader.read_header(&io).unwrap();

            match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::InvalidFrame => {}
                other => panic!("expected InvalidFrame with wrong key, got {other:?}"),
            }
        }

        // ── Tampered TX header (commit_ts) ──
        // commit_ts is part of the AAD, so flipping a byte in it causes AEAD
        // decryption to fail even though the ciphertext itself is untouched.
        {
            let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
            let file = io
                .open_file("enc-hdr-tamper.db-log", OpenFlags::Create, false)
                .unwrap();

            let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100,
                row_versions: vec![make_test_row_version(table_id, 1, "hdr_tamper", 100)],
                header: None,
            };
            append_encrypted_tx(&mut log, &io, &tx);

            // Flip a byte in the commit_ts field (TX header offset 16..24, file offset = LOG_HDR + 16).
            let corrupt_offset = (LOG_HDR_SIZE + 16) as u64;
            let byte_buf = Arc::new(Buffer::new(vec![0xFF]));
            let c = Completion::new_write(move |_| {});
            io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
                .unwrap();

            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
            reader.read_header(&io).unwrap();

            match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::InvalidFrame => {}
                other => panic!("expected InvalidFrame after TX header tamper, got {other:?}"),
            }
        }

        // ── Tampered ciphertext ──
        {
            let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
            let file = io
                .open_file("enc-tamper.db-log", OpenFlags::Create, false)
                .unwrap();

            let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
            let tx = crate::mvcc::database::LogRecord {
                tx_timestamp: 100,
                row_versions: vec![make_test_row_version(table_id, 1, "tamper_me", 100)],
                header: None,
            };
            append_encrypted_tx(&mut log, &io, &tx);

            // Flip a byte in the ciphertext (after log header + TX header).
            let corrupt_offset = (LOG_HDR_SIZE + TX_HEADER_SIZE + 1) as u64;
            let byte_buf = Arc::new(Buffer::new(vec![0xFF]));
            let c = Completion::new_write(move |_| {});
            io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
                .unwrap();

            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
            reader.read_header(&io).unwrap();

            match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::InvalidFrame => {}
                other => panic!("expected InvalidFrame after ciphertext tamper, got {other:?}"),
            }
        }
    }

    // Verifies a torn final frame is ignored while the last fully written prefix frame
    // still decrypts to the exact bytes that were committed before the tear.
    #[test]
    fn test_encrypted_log_torn_tail_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = open_test_file(&io, "enc-torn.db-log");
        let table_id: MVTableId = (-2).into();
        let enc_ctx = test_enc_ctx();
        let first_row_version = make_test_row_version(table_id, 0, "data", 100);
        let expected_first_record_bytes = first_row_version.row.payload().to_vec();

        // Write 2 frames.
        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let first_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 100,
            row_versions: vec![first_row_version],
            header: None,
        };
        append_encrypted_tx(&mut log, &io, &first_tx);
        let second_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 101,
            row_versions: vec![make_test_row_version(table_id, 1, "data", 101)],
            header: None,
        };
        append_encrypted_tx(&mut log, &io, &second_tx);

        // Truncate mid-way through the second frame.
        let file_size = file.size().unwrap();
        let truncate_at = file_size - 5; // remove last 5 bytes
        let c = Completion::new_trunc(|_| {});
        io.wait_for_completion(file.truncate(truncate_at, c).unwrap())
            .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        // First frame should parse fine.
        match reader.parse_next_transaction(&io).unwrap() {
            ParseResult::Ops(ops) => {
                assert_eq!(ops.len(), 1);
                assert_upsert_table_op(&ops[0], (-2).into(), 0, &expected_first_record_bytes, 100);
            }
            other => panic!("expected Ops for frame 1, got {other:?}"),
        }

        // Second frame is torn — should be EOF.
        match reader.parse_next_transaction(&io).unwrap() {
            ParseResult::Eof => {}
            other => panic!("expected Eof for torn frame 2, got {other:?}"),
        }
    }

    // Verifies chunk-level tampering is rejected: any corruption, reorder, drop, or
    // duplicate in the encrypted chunk stream must fail closed instead of replaying data.
    #[test]
    fn test_encrypted_log_chunk_integrity_rejection() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        let text_len =
            text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
        let value = "z".repeat(text_len);

        let base_file = open_test_file(&io, "enc-chunk-integrity-base.db-log");
        let row_version = make_test_row_version((-2).into(), 1, &value, 333);
        let expected_record_bytes = row_version.row.payload().to_vec();
        let tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 333,
            row_versions: vec![row_version],
            header: None,
        };
        write_first_encrypted_tx(base_file.clone(), &io, &enc_ctx, &tx);
        let base_ops = parse_only_encrypted_tx_ops(base_file.clone(), &io, &enc_ctx);
        assert_eq!(base_ops.len(), 1);
        assert_upsert_table_op(&base_ops[0], (-2).into(), 1, &expected_record_bytes, 333);

        let base_bytes = read_file_bytes(base_file, &io);
        let payload_size = u64::from_le_bytes(
            base_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let chunk_ranges =
            encrypted_chunk_ranges(payload_size, enc_ctx.tag_size(), enc_ctx.nonce_size());
        assert!(
            chunk_ranges.len() >= 3,
            "expected at least 3 encrypted chunks for corruption coverage"
        );
        let frame_payload_start = LOG_HDR_SIZE + TX_HEADER_SIZE;
        let full_chunk_plaintext_len =
            encrypted_chunk_plaintext_len(payload_size, 1, ENCRYPTED_PAYLOAD_CHUNK_SIZE).unwrap();

        let mut cases: Vec<(&str, Vec<u8>, bool)> = Vec::new();

        // Corrupt ciphertext in chunk 2.
        {
            let mut bytes = base_bytes.clone();
            let offset = frame_payload_start + chunk_ranges[1].start + 1;
            bytes[offset] ^= 0xFF;
            cases.push(("ciphertext", bytes, false));
        }

        // Corrupt tag in chunk 2.
        {
            let mut bytes = base_bytes.clone();
            let offset = frame_payload_start + chunk_ranges[1].start + full_chunk_plaintext_len + 1;
            bytes[offset] ^= 0xFF;
            cases.push(("tag", bytes, false));
        }

        // Corrupt nonce in chunk 2.
        {
            let mut bytes = base_bytes.clone();
            let offset = frame_payload_start
                + chunk_ranges[1].start
                + full_chunk_plaintext_len
                + enc_ctx.tag_size();
            bytes[offset] ^= 0xFF;
            cases.push(("nonce", bytes, false));
        }

        // Reorder the first two full-size chunks.
        {
            let mut bytes = base_bytes.clone();
            let first = chunk_ranges[0].clone();
            let second = chunk_ranges[1].clone();
            let first_bytes =
                bytes[frame_payload_start + first.start..frame_payload_start + first.end].to_vec();
            let second_bytes = bytes
                [frame_payload_start + second.start..frame_payload_start + second.end]
                .to_vec();
            bytes[frame_payload_start + first.start..frame_payload_start + first.end]
                .copy_from_slice(&second_bytes);
            bytes[frame_payload_start + second.start..frame_payload_start + second.end]
                .copy_from_slice(&first_bytes);
            cases.push(("reorder", bytes, false));
        }

        // Drop the middle chunk entirely.
        {
            let mut bytes = base_bytes.clone();
            let second = chunk_ranges[1].clone();
            bytes.drain(frame_payload_start + second.start..frame_payload_start + second.end);
            cases.push(("drop", bytes, true));
        }

        // Duplicate chunk 1 over chunk 2.
        {
            let mut bytes = base_bytes;
            let first = chunk_ranges[0].clone();
            let second = chunk_ranges[1].clone();
            let first_bytes =
                bytes[frame_payload_start + first.start..frame_payload_start + first.end].to_vec();
            bytes[frame_payload_start + second.start..frame_payload_start + second.end]
                .copy_from_slice(&first_bytes);
            cases.push(("duplicate", bytes, false));
        }

        for (label, bytes, allow_eof) in cases {
            let file = io
                .open_file(
                    &format!("enc-chunk-integrity-{label}.db-log"),
                    OpenFlags::Create,
                    false,
                )
                .unwrap();
            overwrite_file_bytes(file.clone(), &io, &bytes);
            if allow_eof {
                let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
                reader.read_header(&io).unwrap();
                match reader.parse_next_transaction(&io).unwrap() {
                    ParseResult::InvalidFrame | ParseResult::Eof => {}
                    other => panic!("expected rejection for {label}, got {other:?}"),
                }
            } else {
                assert_single_frame_invalid(file, &io, enc_ctx.clone());
            }
        }
    }

    // Verifies a torn multi-chunk tail is ignored without losing the last fully written
    // prefix frame that appears before the truncation point.
    #[test]
    fn test_encrypted_log_chunk_torn_tail_rejected() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-chunk-torn-tail.db-log", OpenFlags::Create, false)
            .unwrap();
        let enc_ctx = test_enc_ctx();
        let table_id: MVTableId = (-2).into();
        let text_len =
            text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
        let value = "q".repeat(text_len);

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let first_row_version = make_test_row_version(table_id, 1, "prefix", 500);
        let expected_prefix_record_bytes = first_row_version.row.payload().to_vec();
        let first_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 500,
            row_versions: vec![first_row_version],
            header: None,
        };
        let c = log.log_tx(&first_tx).unwrap();
        io.wait_for_completion(c).unwrap();
        let second_frame_start = log.offset as usize;

        let second_tx = crate::mvcc::database::LogRecord {
            tx_timestamp: 600,
            row_versions: vec![make_test_row_version(table_id, 2, &value, 600)],
            header: None,
        };
        let c = log.log_tx(&second_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let base_bytes = read_file_bytes(file.clone(), &io);
        let second_payload_size = u64::from_le_bytes(
            base_bytes[second_frame_start + 4..second_frame_start + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let chunk_ranges = encrypted_chunk_ranges(
            second_payload_size,
            enc_ctx.tag_size(),
            enc_ctx.nonce_size(),
        );
        assert!(chunk_ranges.len() >= 3);
        let second_payload_start = second_frame_start + TX_HEADER_SIZE;
        let second_chunk_plaintext_len =
            encrypted_chunk_plaintext_len(second_payload_size, 1, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .unwrap();
        let second_chunk = chunk_ranges[1].clone();
        let last_chunk = chunk_ranges.last().unwrap().clone();
        let second_frame_end = base_bytes.len();

        let cuts = [
            second_payload_start + second_chunk.start + 17,
            second_payload_start
                + second_chunk.start
                + second_chunk_plaintext_len
                + enc_ctx.tag_size(),
            second_payload_start + second_chunk.end,
            second_frame_end - TX_TRAILER_SIZE + 3,
        ];

        for (idx, cut) in cuts.into_iter().enumerate() {
            let file = io
                .open_file(
                    &format!("enc-chunk-torn-tail-{idx}.db-log"),
                    OpenFlags::Create,
                    false,
                )
                .unwrap();
            overwrite_file_bytes(file.clone(), &io, &base_bytes[..cut]);

            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
            reader.read_header(&io).unwrap();
            match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::Ops(ops) => {
                    assert_eq!(ops.len(), 1);
                    assert_upsert_table_op(
                        &ops[0],
                        (-2).into(),
                        1,
                        &expected_prefix_record_bytes,
                        500,
                    );
                }
                other => panic!("expected prefix frame to survive, got {other:?}"),
            }
            match reader.parse_next_transaction(&io).unwrap() {
                ParseResult::Eof => {}
                other => panic!("expected Eof for torn multi-chunk frame, got {other:?}"),
            }
        }

        // Keep the last chunk variable used so the compiler notices if the range math changes.
        assert!(last_chunk.end > last_chunk.start);
    }
}
