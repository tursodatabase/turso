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
//! When encryption is enabled, the recovery payload and any extension block are
//! encrypted together. The log header, TX header, and TX trailer are always
//! written in plaintext. The log header's salt and TX header fields (op_count,
//! commit_ts, and the final chunk's encrypted plaintext size) are bound to the
//! ciphertext as AEAD additional data, so tampering with them will cause
//! decryption to fail. The CRC in the trailer covers the TX header and the body
//! as written on disk (i.e. the ciphertext when encrypted).
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
//! ### TX Header (`TX_HEADER_SIZE = 24`, `TX_EXT_HEADER_SIZE = 40`)
//! - `frame_magic: u32` (`FRAME_MAGIC` for compact recovery frames,
//!   `EXT_FRAME_MAGIC` when a portable extension block precedes the recovery
//!   payload)
//! - `payload_size: u64` (total bytes of all op entries, pre-encryption)
//! - `op_count: u32`
//! - `commit_ts: u64`
//! - `extension_size: u64` (extension frames only)
//! - `extension_record_count: u32` (extension frames only)
//! - `frame_flags: u32` (extension frames only)
//!
//! ### Payload
//! - When **unencrypted** and no extension block is present: `op_count` operation
//!   entries serialized directly:
//!   - `tag: u8` (`OP_*`)
//!   - `flags: u8` (`OP_FLAG_BTREE_RESIDENT`, `OP_FLAG_PORTABLE_EXTENSION`)
//!   - `table_id: i32` (must be negative)
//!   - `payload_len: sqlite varint`
//!   - `payload: [u8; payload_len]`
//!   - if `OP_FLAG_PORTABLE_EXTENSION` is set:
//!     `extension_len: sqlite varint || extension: [u8; extension_len]`
//! - When an extension block is present, the transaction body is:
//!   `extension_block || recovery_payload`
//! - When **encrypted**: extension block plus recovery payload is split into
//!   fixed-size plaintext chunks
//!   (`ENCRYPTED_PAYLOAD_CHUNK_SIZE`, except the final remainder chunk)
//!   - each chunk is written as `ciphertext(chunk_plain_len + tag_size) | nonce(nonce_size)`
//!   - AEAD additional data:
//!     `salt(8) || plaintext_size_or_zero(8) || op_count(4) || commit_ts(8) || chunk_index(4)` (little-endian)
//!     where the plaintext-size slot is zero for non-final chunks and carries the encrypted
//!     plaintext size only in the final chunk
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
//! `OP_FLAG_PORTABLE_EXTENSION` means the op has protobuf-style extension bytes immediately after
//! its main recovery payload. Recovery may ignore those bytes, but the parser must consume them as
//! part of the op.
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
//! │salt (8)│plaintext_size_or_0 │op_cnt (4)│commit_ts(8)│chunk_idx (4)│
//! └────────┴────────────────────┴──────────┴────────────┴─────────────┘
//!           ↑
//!           └── encrypted plaintext size only in final chunk; zero for all others
//! ```
//!
//! ### How Plaintext Payload Is Split Into Chunks
//!
//! ```text
//! Plaintext payload for a frame without a transaction extension
//! (serialized ops, payload_size bytes):
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

use crate::io::{FileSyncType, SharedBufferData};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::turso_assert;
use crate::types::IOResultOr;
use crate::{
    alloc::{ConcurrentAllocator, TursoAllocator},
    io::{CompletionGroup, ReadComplete},
    io_yield_one,
    mvcc::database::{LogRecord, MVTableId, Row, RowID, RowKey, RowVersion, SortableIndexKey},
    return_if_io,
    storage::sqlite3_ondisk::{
        read_varint, read_varint_partial, varint_len, write_varint, DatabaseHeader,
    },
    types::{IOCompletions, IOResult, IndexInfo},
    util::IOExt as _,
    Buffer, Completion, CompletionError, LimboError, Result,
};

use crate::storage::encryption::EncryptionContext;
use crate::File;

mod serializer;
use serializer::EncryptedPayload;
#[cfg(feature = "conn_raw_api")]
use serializer::{
    extension_record_len, ExtensionRecord, PortableChangePayload, PortableEndOffsetCtx,
};
pub(crate) use serializer::{log_write, LogBufferWrite, LogChunkStream, LogSerializer};
#[cfg(feature = "conn_raw_api")]
pub(crate) use serializer::{
    ProtoKey, ProtoSint64, ProtoVarint, PROTO_WIRE_LENGTH_DELIMITED, PROTO_WIRE_VARINT,
};

/// Logical log size in bytes at which a committing transaction will trigger a checkpoint.
/// Default to the size of 1000 SQLite WAL frames; disable by setting a negative value.
pub const DEFAULT_LOG_CHECKPOINT_THRESHOLD: i64 = 4120 * 1000;

/// Chain state of a serialized logical-log frame, delivered to
/// [`OnSerializationComplete`] observers.
///
/// `start_crc32c` is the committed running CRC immediately before this frame
/// (the chain seed for the frame), and `end_crc32c` is the running CRC after
/// it. For a deferred write these describe the *pending* chain position: the
/// log's own running CRC only advances to `end_crc32c` once the commit is
/// accepted via `advance_offset_after_success`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogTxFrameInfo {
    /// Writer offset of the frame's first byte (the log's `offset` before this
    /// write; includes the 56-byte log-header region for the first write).
    pub logical_start_offset: u64,
    /// Committed running CRC immediately before this frame.
    pub start_crc32c: u32,
    /// Running CRC after this frame (equals the frame's trailer CRC).
    pub end_crc32c: u32,
}

/// Optional callback invoked after serialization with shared ownership of the
/// serialized frame bytes and the frame's chain state, before the disk write.
pub type OnSerializationComplete<'a> =
    Option<&'a dyn Fn(SharedBufferData, LogTxFrameInfo) -> crate::Result<()>>;

const LOG_MAGIC: u32 = 0x4C4D4C32; // "LML2" in LE
const LOG_VERSION_V2: u8 = 2;
const LOG_VERSION: u8 = 3;
pub const LOG_HDR_SIZE: usize = 56;
const LOG_HDR_SALT_START: usize = 8;
const LOG_HDR_SALT_SIZE: usize = 8;
const LOG_HDR_RESERVED_START: usize = LOG_HDR_SALT_START + LOG_HDR_SALT_SIZE; // 16
const LOG_HDR_CRC_START: usize = 52;
const LOG_HDR_RESERVED_SIZE: usize = LOG_HDR_CRC_START - LOG_HDR_RESERVED_START; // 36
pub(crate) const FRAME_MAGIC: u32 = 0x5854564D; // "MVTX" in LE
pub(crate) const EXT_FRAME_MAGIC: u32 = 0x5845564D; // "MVEX" in LE
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
const OP_FLAG_PORTABLE_EXTENSION: u8 = 1 << 1;
const OP_ALLOWED_FLAGS: u8 = OP_FLAG_BTREE_RESIDENT | OP_FLAG_PORTABLE_EXTENSION;
const OP_EXT_FIELD_DELETE_IDENTITY_RECORD: u64 = 1;
const OP_EXT_FIELD_DELETE_PK_RECORD: u64 = 2;
const OP_EXT_FIELD_DELETE_ROWID: u64 = 3;

struct DeletePortableExtension {
    identity_record: crate::ValueBlob,
    pk_record: crate::ValueBlob,
}

impl Default for DeletePortableExtension {
    fn default() -> Self {
        Self {
            identity_record: crate::alloc::vec![],
            pk_record: crate::alloc::vec![],
        }
    }
}

const TX_HEADER_SIZE_V2: usize = 24; // FRAME_MAGIC(4) + payload_size(8) + op_count(4) + commit_ts(8)
const TX_HEADER_SIZE: usize = TX_HEADER_SIZE_V2;
// LML3 extension frames keep the recovery fields first, then append portable
// metadata. Compact frames use the 24-byte recovery header and normal
// FRAME_MAGIC; extension frames use EXT_FRAME_MAGIC and this 40-byte header.
pub(crate) const TX_EXT_HEADER_SIZE: usize =
    TX_HEADER_SIZE + 8 /* extension_size */ + 4 /* extension_record_count */ + 4 /* frame_flags */;
const TX_TRAILER_SIZE: usize = 8; // crc32c(4) + END_MAGIC(4)
const TX_MIN_FRAME_SIZE_V2: usize = TX_HEADER_SIZE_V2 + TX_TRAILER_SIZE; // 32
const TX_MIN_FRAME_SIZE: usize = TX_HEADER_SIZE + TX_TRAILER_SIZE; // 32
const TX_FRAME_FLAG_HAS_EXTENSION_BLOCK: u32 = 1 << 0;
const EXTENSION_RECORD_HEADER_SIZE: usize = 8; // type(u16) + flags(u16) + len(u32)
const EXTENSION_TYPE_PORTABLE_CHANGES: u16 = 1;

/// Total bytes pre-reserved at the front of a `LogRecord::buf`.
pub(crate) const LOG_RECORD_PREFIX_SIZE: usize = LOG_HDR_SIZE + TX_HEADER_SIZE;

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
        Self::new_with_version(io, LOG_VERSION_V2)
    }

    fn new_with_version(io: &Arc<dyn crate::IO>, version: u8) -> Self {
        turso_assert!(
            version == LOG_VERSION_V2 || version == LOG_VERSION,
            "unsupported logical log header version: {version}"
        );
        Self {
            version,
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
        if version != LOG_VERSION && version != LOG_VERSION_V2 {
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

#[cfg_attr(feature = "aristo-instr", derive(aristo::instrument::Inspect))]
pub struct LogicalLog {
    pub file: Arc<dyn File>,
    io: Arc<dyn crate::IO>,
    pub offset: u64,
    #[cfg_attr(feature = "aristo-instr", inspect(ret = Option<u8>, with = |h| h.as_ref().map(|x| x.version), name = "header_version"))]
    header: Option<LogHeader>,
    /// Running CRC state for chained checksums. Seeded from the header salt;
    /// updated after each committed frame. The next frame's CRC is computed as
    /// `crc32c_append(running_crc, frame_bytes)`.
    pub running_crc: u32,
    /// Pending CRC from a deferred-offset write. Applied by
    /// `advance_offset_after_success` so that an abandoned write
    /// doesn't corrupt the chain.
    #[cfg_attr(feature = "aristo-instr", inspect(name = "pending_running_crc"))]
    pending_running_crc: Option<u32>,
    encryption_ctx: Option<EncryptionContext>,
    /// Plaintext bytes per encrypted payload chunk. Production uses the fixed format constant;
    /// tests may override via `new_with_encrypted_payload_chunk_size_for_test`.
    encrypted_payload_chunk_size: usize,
    max_appended_commit_ts: u64,
}

#[cfg(feature = "aristo-instr")]
impl LogicalLog {
    /// Harness accessor: the logical-log write cursor and running CRC as one
    /// owned snapshot, read by the durability-ordering (DOI) differential tests.
    /// Both underlying fields are already `pub`; this pairs them under the exact
    /// symbol the routed conformance tests expect.
    pub fn read_logicallog_offset_crc(&self) -> (u64, u32) {
        (self.offset, self.running_crc)
    }
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
            header: None,
            running_crc: 0,
            pending_running_crc: None,
            encryption_ctx,
            encrypted_payload_chunk_size,
            max_appended_commit_ts: 0,
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

    /// Wraps the pre-serialized payload (`tx.buf`) with the log/TX framing
    /// — optional log header, TX header, optional chunked encryption, CRC
    /// trailer — and pwrites the resulting frame to disk.
    ///
    /// `advance_offset_immediately`: when true, the writer offset advances right
    /// after the pwrite (checkpoint path). When false, the offset stays behind
    /// until `advance_offset_after_success` is called (MVCC commit path).
    fn frame_and_pwrite_tx(
        &mut self,
        mut tx: LogRecord,
        advance_offset_immediately: bool,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        let op_count = tx.op_count;
        let commit_ts = tx.tx_timestamp;
        self.max_appended_commit_ts = self.max_appended_commit_ts.max(commit_ts);
        // `tx.buf` is laid out as:
        //   [LOG_HDR slot (56B, zeros)] [TX_HEADER slot (24B, zeros)] [payload]
        debug_assert!(
            tx.buf.len() >= LOG_RECORD_PREFIX_SIZE,
            "LogRecord buf missing pre-reserved framing prefix"
        );
        let payload_size = tx.buf.len() - LOG_RECORD_PREFIX_SIZE;
        let payload_size_u64 = payload_size as u64;

        // Every commit from a portable-enabled writer gets an extension block,
        // including one whose portable object map is empty. A transaction that
        // touches only internal objects (`turso_sync_*`, `turso_cdc*`,
        // `sqlite_*`, indexes) would otherwise be written as a plain frame with
        // recovery ops, which is byte-identical to pre-portable LML2 history: a
        // reader planning a logical sync range cannot tell "no user-visible
        // changes here" from "user data this reader cannot replay", so it must
        // refuse the range. Emitting the block makes the empty change set
        // explicit; readers that decode it produce no ops for the frame.
        #[cfg(feature = "conn_raw_api")]
        let portable_changes_enabled = tx.portable_changes_enabled
            || tx.portable_changes_required
            || !tx.portable_changes.is_empty();
        #[cfg(not(feature = "conn_raw_api"))]
        let portable_changes_enabled = false;
        let has_portable_changes = portable_changes_enabled;

        // 1. Ensure we have a log header object (created lazily on first write).
        // Non-portable logs remain LML2 so a deployment that does not enable
        // portable extensions can still roll back to readers that only know LML2.
        let is_first_write = self.offset == 0;
        if is_first_write && self.header.is_none() {
            let version = if portable_changes_enabled {
                LOG_VERSION
            } else {
                LOG_VERSION_V2
            };
            let header = LogHeader::new_with_version(&self.io, version);
            self.running_crc = derive_initial_crc(header.salt);
            self.header = Some(header);
        }
        if portable_changes_enabled {
            let header = self
                .header
                .as_mut()
                .expect("log header must be set before writing");
            if header.version == LOG_VERSION_V2 {
                if !is_first_write {
                    return Err(LimboError::InternalError(
                        "portable logical changes require logical log header upgrade before append"
                            .to_string(),
                    ));
                }
                header.version = LOG_VERSION;
            }
        }
        if has_portable_changes {
            LogSerializer::new(&mut tx.buf).insert(
                LOG_RECORD_PREFIX_SIZE,
                [0u8; TX_EXT_HEADER_SIZE - TX_HEADER_SIZE],
            )?;
        }

        let tx_header_size = if has_portable_changes {
            TX_EXT_HEADER_SIZE
        } else {
            TX_HEADER_SIZE
        };
        let frame_payload_start = LOG_HDR_SIZE + tx_header_size;

        #[cfg(feature = "conn_raw_api")]
        let extension_size = if has_portable_changes {
            let encryption_overhead = self
                .encryption_ctx
                .as_ref()
                .map(|enc_ctx| (enc_ctx.tag_size(), enc_ctx.nonce_size()));
            let portable_changes = PortableChangePayload::with_stable_end_offset(
                PortableEndOffsetCtx {
                    write_offset: self.offset,
                    includes_log_header: is_first_write,
                    tx_header_size,
                    recovery_payload_size: payload_size,
                    encrypted_payload_chunk_size: self.encrypted_payload_chunk_size,
                    encryption_overhead,
                },
                tx.tx_timestamp,
                &tx.portable_changes,
            )?;
            let extension =
                ExtensionRecord::new(EXTENSION_TYPE_PORTABLE_CHANGES, 0, portable_changes);
            let extension_size =
                u64::try_from(extension_record_len(&extension)?).map_err(|_| {
                    LimboError::InternalError("Logical log extension size exceeds u64".to_string())
                })?;
            LogSerializer::new(&mut tx.buf)
                .insert_portable_extension(frame_payload_start, extension)?;
            extension_size
        } else {
            0
        };
        #[cfg(not(feature = "conn_raw_api"))]
        let extension_size = 0u64;
        let plaintext_size = tx.buf.len() - frame_payload_start;

        // 2. Build the on-disk payload. Unencrypted is the zero-shift fast
        // path: plaintext is already after the TX header. Extension frames are
        // laid out as `extension_block || recovery_payload`, so raw-log
        // consumers can load transaction metadata before scanning recovery ops.
        // Encrypted frames encrypt both parts as one authenticated body.
        if let Some(enc_ctx) = &self.encryption_ctx {
            let salt = self
                .header
                .as_ref()
                .expect("log header must be set before writing")
                .salt;
            LogSerializer::new(&mut tx.buf).encrypt_payload_in_place(EncryptedPayload {
                enc_ctx,
                payload_start: frame_payload_start,
                plaintext_size,
                chunk_size: self.encrypted_payload_chunk_size,
                salt,
                op_count,
                commit_ts,
            })?;
        }
        // Unencrypted: plaintext bytes are already in place after the TX header.

        // 3. Backfill TX HEADER at offset LOG_HDR_SIZE:
        //    FRAME_MAGIC(4) | payload_size(8) | op_count(4) | commit_ts(8)
        // Extension frames use EXT_FRAME_MAGIC and append:
        //    | extension_size(8) | extension_record_count(4) | frame_flags(4)
        let tx_header_start = LOG_HDR_SIZE;
        let frame_magic = if has_portable_changes {
            EXT_FRAME_MAGIC
        } else {
            FRAME_MAGIC
        };
        tx.buf[tx_header_start..tx_header_start + 4].copy_from_slice(&frame_magic.to_le_bytes());
        tx.buf[tx_header_start + 4..tx_header_start + 12]
            .copy_from_slice(&payload_size_u64.to_le_bytes());
        tx.buf[tx_header_start + 12..tx_header_start + 16].copy_from_slice(&op_count.to_le_bytes());
        tx.buf[tx_header_start + 16..tx_header_start + 24]
            .copy_from_slice(&commit_ts.to_le_bytes());
        if has_portable_changes {
            tx.buf[tx_header_start + 24..tx_header_start + 32]
                .copy_from_slice(&extension_size.to_le_bytes());
            tx.buf[tx_header_start + 32..tx_header_start + 36].copy_from_slice(&1u32.to_le_bytes());
            tx.buf[tx_header_start + 36..tx_header_start + 40]
                .copy_from_slice(&TX_FRAME_FLAG_HAS_EXTENSION_BLOCK.to_le_bytes());
        }

        // 4. TX TRAILER (8 bytes): crc32c(4, le u32) | END_MAGIC(4)
        // CRC is chained: seeded from running_crc (salt-derived, or previous
        // frame's CRC), covers TX_HEADER (24 B) + payload (encrypted or plain).
        // The log header is NOT part of the CRC chain — it has its own header
        // CRC stored within its 56 bytes.
        let payload_end = tx.buf.len();
        let crc = crc32c::crc32c_append(self.running_crc, &tx.buf[tx_header_start..payload_end]);
        LogSerializer::new(&mut tx.buf).serialize_tx_trailer(crc)?;

        // 5. Fill the LOG_HDR slot (first-write only). Non-first-write
        // commits leave it as zeros; those bytes never reach disk because
        // the shared view exposes only `data[LOG_HDR_SIZE..]` below.
        if is_first_write {
            let header_bytes = self.header.as_ref().unwrap().encode();
            tx.buf[..LOG_HDR_SIZE].copy_from_slice(&header_bytes);
        }

        // 6. Observer hook: gets shared ownership of a zero-copy view into the
        // on-disk bytes.
        let raw = Arc::new(tx.buf.into_boxed_slice());
        let shared = if is_first_write {
            SharedBufferData::new(raw)
        } else {
            SharedBufferData::new_view(raw, LOG_HDR_SIZE)
        };
        if let Some(cb) = on_serialization_complete {
            // `self.running_crc` has not been touched yet in this write: it is
            // still the committed pre-frame chain value (seeded from the salt
            // above on first write), so it is the frame's start CRC.
            cb(
                shared.clone(),
                LogTxFrameInfo {
                    logical_start_offset: self.offset,
                    start_crc32c: self.running_crc,
                    end_crc32c: crc,
                },
            )?;
        }

        // 7. Hand off `tx.buf` to the I/O layer without copying. For
        // non-first-write commits, the Buffer wrapper exposes only
        // `data[LOG_HDR_SIZE..]` so the unused 56-byte prefix never reaches
        // disk: a single pwrite, no shift.
        let buffer = Arc::new(Buffer::new_shared_data(shared));
        let buffer_len = buffer.len();
        let c = Completion::new_write(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            turso_assert!(
                bytes_written == buffer_len as i32,
                "wrote({bytes_written}) != expected({buffer_len})"
            );
        });

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
    pub fn log_tx(&mut self, tx: LogRecord) -> Result<Completion> {
        let (c, _) = self.frame_and_pwrite_tx(tx, true, None)?;
        Ok(c)
    }

    pub fn upgrade_header_for_log_tx(&mut self, tx: &LogRecord) -> Result<Option<Completion>> {
        #[cfg(feature = "conn_raw_api")]
        let portable_changes_enabled =
            tx.portable_changes_enabled || !tx.portable_changes.is_empty();
        #[cfg(not(feature = "conn_raw_api"))]
        let portable_changes_enabled = {
            let _ = tx;
            false
        };

        if !portable_changes_enabled || self.offset == 0 {
            return Ok(None);
        }

        let upgraded_header = {
            let header = self.header.as_mut().ok_or_else(|| {
                LimboError::InternalError(
                    "Logical log header not initialized before portable upgrade".to_string(),
                )
            })?;
            if header.version != LOG_VERSION_V2 {
                return Ok(None);
            }
            header.version = LOG_VERSION;
            header.clone()
        };

        Ok(Some(self.write_header(upgraded_header, None)?))
    }

    /// Writes a transaction to the log but does NOT advance the writer offset.
    /// Returns `(completion, bytes_written)`. The caller must call
    /// `advance_offset_after_success(bytes)` after confirming the commit succeeded.
    ///
    /// If `on_serialization_complete` is provided, it is called with shared
    /// ownership of the framed bytes and the frame's [`LogTxFrameInfo`] chain
    /// state after framing but before the disk write.
    pub fn log_tx_deferred_offset(
        &mut self,
        tx: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.frame_and_pwrite_tx(tx, false, on_serialization_complete)
    }

    #[aristo::intent("the in-memory log offset advances only after the corresponding frame pwrite has completed durably", id = "aristos:logical_log_inmemory_offset_advances_after_durable_write", verify = "full")]
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

    /// Discard the pending running CRC staged by a deferred write whose
    /// two-phase commit aborted before the offset advanced.
    ///
    /// This must be called on the abort path so no later write chains its
    /// running CRC from a value staged for a write that never confirmed.
    pub fn discard_pending_write(&mut self) {
        self.pending_running_crc = None;
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

    #[aristo::intent("the in-memory log header is published only after the on-disk header pwrite has completed durably", id = "aristos:logical_log_header_publish_after_fsync", verify = "full")]
    /// Writes the header. The write is added to `group`, when given,
    /// before it is submitted.
    fn write_header(
        &mut self,
        mut header: LogHeader,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion> {
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
        if let Some(group) = group {
            group.add(&c);
        }
        self.file.pwrite(0, buffer, c)
    }

    pub fn update_header(&mut self) -> Result<Completion> {
        let header = self.current_or_new_header()?;
        self.write_header(header, None)
    }

    #[aristo::intent("the running CRC of the log is reseeded only after the truncate operation has completed durably", id = "aristos:logical_log_truncate_crc_reseed_after_completion", verify = "full")]
    fn truncate_to_zero(&mut self) -> Result<Completion> {
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
        self.max_appended_commit_ts = 0;
        Ok(c)
    }

    /// Truncate when `max_appended_commit_ts <= boundary`; passive uses `durable_txid_max_new`,
    /// truncate mode uses `u64::MAX` (always empty after checkpoint).
    pub fn truncate(
        &mut self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, super::LogicalLogTruncateOutcome)> {
        use super::LogicalLogTruncateOutcome;
        if self.max_appended_commit_ts > checkpointed_through_ts {
            // Uncheckpointed frames remain — skip truncation.
            let c = Completion::new_trunc(|_| {});
            c.complete(0);
            return Ok((c, LogicalLogTruncateOutcome::Retained));
        }
        let c = self.truncate_to_zero()?;
        Ok((c, LogicalLogTruncateOutcome::Truncated))
    }

    /// Reset the log to a header-only file and return one completion for the
    /// header write plus truncate.
    ///
    /// This intentionally truncates to `LOG_HDR_SIZE`, not zero, so the header
    /// write and truncate can run as a group without an ordering dependency.
    /// Either completion order leaves a header-sized file with the fresh header
    /// bytes at offset zero.
    pub fn reset_to_fresh_header(&mut self) -> Result<Completion> {
        // Regenerate salt so stale frames from before the reset cannot validate
        // against this new CRC chain.
        let mut header = self.current_or_new_header()?;
        header.salt = self.io.generate_random_number() as u64;
        self.running_crc = derive_initial_crc(header.salt);
        self.pending_running_crc = None;
        self.header = Some(header.clone());

        let mut group = CompletionGroup::new(|_| {});
        let _header_c = self.write_header(header, Some(&mut group))?;
        let c = Completion::new_trunc(move |result| {
            if let Err(err) = result {
                tracing::error!("logical_log_truncate failed: {}", err);
            }
        });
        group.add(&c);
        let _truncate_c = self.file.truncate(LOG_HDR_SIZE as u64, c)?;
        self.offset = 0;
        Ok(group.build())
    }
}

/// Serializes one logical-log operation for the serialization benchmark.
#[cfg(feature = "bench")]
pub fn benchmark_serialize_op_entry(
    buffer: &mut Vec<u8>,
    row_version: &RowVersion,
    portable_extension: Option<&[u8]>,
) -> Result<()> {
    LogSerializer::new(buffer).serialize_op_entry(row_version, portable_extension)
}

fn read_proto_varint_from_buf(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    while *offset < bytes.len() {
        let byte = bytes[*offset];
        *offset += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(LimboError::Corrupt("protobuf varint overflows u64".into()));
        }
    }
    Err(LimboError::Corrupt("truncated protobuf varint".into()))
}

fn skip_proto_field(bytes: &[u8], offset: &mut usize, wire_type: u64) -> Result<()> {
    match wire_type {
        0 => {
            let _ = read_proto_varint_from_buf(bytes, offset)?;
        }
        2 => {
            let len = read_proto_varint_from_buf(bytes, offset)?;
            let len = usize::try_from(len)
                .map_err(|_| LimboError::Corrupt("protobuf field length overflows usize".into()))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| LimboError::Corrupt("protobuf field length overflow".into()))?;
            if end > bytes.len() {
                return Err(LimboError::Corrupt(
                    "protobuf length-delimited field exceeds extension".into(),
                ));
            }
            *offset = end;
        }
        other => {
            return Err(LimboError::Corrupt(format!(
                "unsupported protobuf wire type in op extension: {other}"
            )));
        }
    }
    Ok(())
}

fn read_proto_sint64_from_buf(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let value = read_proto_varint_from_buf(bytes, offset)?;
    Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)))
}

fn decode_delete_portable_extension(extension: &[u8]) -> Result<DeletePortableExtension> {
    let mut offset = 0usize;
    let mut decoded = DeletePortableExtension::default();
    while offset < extension.len() {
        let key = read_proto_varint_from_buf(extension, &mut offset)?;
        let field = key >> 3;
        let wire_type = key & 7;
        match (field, wire_type) {
            (OP_EXT_FIELD_DELETE_IDENTITY_RECORD, 2) => {
                let len = read_proto_varint_from_buf(extension, &mut offset)?;
                let len = usize::try_from(len).map_err(|_| {
                    LimboError::Corrupt("delete identity record length overflows usize".into())
                })?;
                let end = offset.checked_add(len).ok_or_else(|| {
                    LimboError::Corrupt("delete identity record length overflow".into())
                })?;
                if end > extension.len() {
                    return Err(LimboError::Corrupt(
                        "delete identity record exceeds op extension".into(),
                    ));
                }
                decoded.identity_record =
                    crate::types::value_blob_from_slice(&extension[offset..end])?;
                offset = end;
            }
            (OP_EXT_FIELD_DELETE_PK_RECORD, 2) => {
                let len = read_proto_varint_from_buf(extension, &mut offset)?;
                let len = usize::try_from(len).map_err(|_| {
                    LimboError::Corrupt("delete PK record length overflows usize".into())
                })?;
                let end = offset.checked_add(len).ok_or_else(|| {
                    LimboError::Corrupt("delete PK record length overflow".into())
                })?;
                if end > extension.len() {
                    return Err(LimboError::Corrupt(
                        "delete PK record exceeds op extension".into(),
                    ));
                }
                decoded.pk_record = crate::types::value_blob_from_slice(&extension[offset..end])?;
                offset = end;
            }
            (OP_EXT_FIELD_DELETE_ROWID, 0) => {
                let _ = read_proto_sint64_from_buf(extension, &mut offset)?;
            }
            _ => skip_proto_field(extension, &mut offset, wire_type)?,
        }
    }
    Ok(decoded)
}

fn find_extension_payload(
    extension_block: &[u8],
    extension_record_count: u32,
    wanted_type: u16,
) -> Result<Vec<u8>> {
    let mut offset = 0usize;
    let mut payload = Vec::new();
    for _ in 0..extension_record_count {
        let Some(header_end) = offset.checked_add(EXTENSION_RECORD_HEADER_SIZE) else {
            return Err(LimboError::Corrupt(
                "extension record header offset overflow".to_string(),
            ));
        };
        if header_end > extension_block.len() {
            return Err(LimboError::Corrupt(
                "extension record header is truncated".to_string(),
            ));
        }
        let extension_type =
            u16::from_le_bytes(extension_block[offset..offset + 2].try_into().unwrap());
        let extension_flags =
            u16::from_le_bytes(extension_block[offset + 2..offset + 4].try_into().unwrap());
        if extension_flags != 0 {
            return Err(LimboError::Corrupt(format!(
                "unsupported extension flags for type {extension_type}: {extension_flags:#x}"
            )));
        }
        let extension_len = u32::from_le_bytes(
            extension_block[offset + 4..offset + EXTENSION_RECORD_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        let payload_start = header_end;
        let Some(payload_end) = payload_start.checked_add(extension_len) else {
            return Err(LimboError::Corrupt(
                "extension record payload offset overflow".to_string(),
            ));
        };
        if payload_end > extension_block.len() {
            return Err(LimboError::Corrupt(
                "extension record payload is truncated".to_string(),
            ));
        }
        if extension_type == wanted_type {
            payload.extend_from_slice(&extension_block[payload_start..payload_end]);
        }
        offset = payload_end;
    }
    if offset != extension_block.len() {
        return Err(LimboError::Corrupt(
            "extension block has trailing bytes".to_string(),
        ));
    }
    Ok(payload)
}

/// Parse all ops from a decrypted plaintext buffer.
/// Validates that `plaintext.len() == payload_size` and that every byte is consumed.
pub(crate) fn parse_ops_from_plaintext(
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
            if flags & !OP_ALLOWED_FLAGS != 0 || table_id_i32 >= 0 {
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
    let (extension, total) = if flags & OP_FLAG_PORTABLE_EXTENSION == 0 {
        (&[][..], total)
    } else {
        let Some((extension_len_u64, extension_len_bytes)) = read_varint_partial(&buf[total..])?
        else {
            return Ok(None);
        };
        let extension_len = usize::try_from(extension_len_u64)
            .map_err(|_| LimboError::Corrupt("op extension length overflows usize".into()))?;
        let extension_start = total + extension_len_bytes;
        let extension_end = extension_start
            .checked_add(extension_len)
            .ok_or_else(|| LimboError::Corrupt("op extension length overflow".into()))?;
        if buf.len() < extension_end {
            return Ok(None);
        }
        (&buf[extension_start..extension_end], extension_end)
    };

    let parsed_op = match tag {
        OP_UPSERT_TABLE => {
            let table_id = table_id.expect("table op must have table_id");
            let (rowid_u64, rowid_len) = read_varint(payload)
                .map_err(|_| LimboError::Corrupt("Bad rowid varint in UPSERT_TABLE".into()))?;
            if rowid_len > payload.len() {
                return Err(LimboError::Corrupt("rowid_len > payload".into()));
            }
            let record_bytes = crate::types::value_blob_from_slice(&payload[rowid_len..])?;
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
            if rowid_len > payload.len() {
                return Err(LimboError::Corrupt(
                    "DELETE_TABLE payload size mismatch".into(),
                ));
            }
            let mut record_bytes = crate::types::value_blob_from_slice(&payload[rowid_len..])?;
            let mut pk_record_bytes = crate::alloc::vec![];
            if !extension.is_empty() {
                let decoded = decode_delete_portable_extension(extension)?;
                if record_bytes.is_empty() {
                    record_bytes = decoded.identity_record;
                }
                pk_record_bytes = decoded.pk_record;
            }
            let rowid = RowID::new(table_id, RowKey::Int(rowid_u64 as i64));
            ParsedOp::DeleteTable {
                rowid,
                record_bytes,
                pk_record_bytes,
                commit_ts,
                btree_resident,
            }
        }
        OP_UPSERT_INDEX => ParsedOp::UpsertIndex {
            table_id: table_id.expect("index op must have table_id"),
            payload: crate::types::value_blob_from_slice(payload)?,
            commit_ts,
            btree_resident,
        },
        OP_DELETE_INDEX => ParsedOp::DeleteIndex {
            table_id: table_id.expect("index op must have table_id"),
            payload: crate::types::value_blob_from_slice(payload)?,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableChangeFrame {
    pub end_offset: u64,
    pub commit_ts: u64,
    pub extension_record_count: u32,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
enum StreamingState {
    NeedTransactionStart,
}

/// Phase of the in-progress transaction frame parse. Each phase corresponds to a
/// re-entrant unit: the header (atomic), an optional unencrypted extension block,
/// the payload, and the trailer. See [`FrameInProgress`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FramePhase {
    Header,
    ExtensionBlock,
    Payload,
    Trailer,
}

/// Progress carried across IO yields while parsing one transaction frame.
///
/// The reader yields mid-frame (any `try_consume_*` can need more data). Instead
/// of re-parsing the whole frame from its start on every re-entry (the previous
/// model), we checkpoint at unit boundaries: when a unit (header / extension
/// block / one op / trailer) is fully consumed *and* its bytes are folded into
/// `running_crc`, we record progress here and advance `frame_anchor` to the
/// consume cursor. Re-entry then rewinds only to the latest checkpoint and
/// re-parses just the in-flight unit, so the buffer compacts as units are
/// consumed (memory bounded by the largest single op, not the whole frame) and
/// no unit is parsed more than its own yields require.
///
/// `running_crc` is the chained CRC *up to and excluding* the in-flight unit; the
/// in-flight unit folds onto a local copy that is committed back here only at its
/// checkpoint. `frame_start` is captured once at frame open and is the value used
/// for `last_valid_offset` when the frame turns out to be invalid — it must never
/// be recomputed from the (mid-frame) consume cursor.
struct FrameInProgress {
    frame_start: usize,
    phase: FramePhase,
    // Header fields (filled once the Header phase completes):
    payload_size: usize,
    op_count: u32,
    commit_ts: u64,
    extension_size: usize,
    extension_record_count: u32,
    frame_flags: u32,
    // Accumulators carried across op/unit yields:
    running_crc: u32,
    parsed_ops: Vec<ParsedOp>,
    portable_changes: Vec<u8>,
    payload_bytes_read: u64,
    op_index: u32,
}

impl FrameInProgress {
    fn new(frame_start: usize) -> Self {
        Self {
            frame_start,
            phase: FramePhase::Header,
            payload_size: 0,
            op_count: 0,
            commit_ts: 0,
            extension_size: 0,
            extension_record_count: 0,
            frame_flags: 0,
            running_crc: 0,
            parsed_ops: Vec::new(),
            portable_changes: Vec::new(),
            payload_bytes_read: 0,
            op_index: 0,
        }
    }
}

/// Parsed transaction-header fields returned by `parse_frame_header`.
struct FrameHeader {
    payload_size: usize,
    op_count: u32,
    commit_ts: u64,
    extension_size: usize,
    extension_record_count: u32,
    frame_flags: u32,
    /// Chained CRC seeded from `self.running_crc` and folded over the header bytes.
    running_crc: u32,
}

/// Outcome of parsing the transaction header (a re-entrant atomic unit).
enum HeaderParseOutcome {
    Ok(FrameHeader),
    Eof,
    Invalid,
}

/// Outcome of parsing a payload phase. Corruption is signalled via
/// `Err(LimboError::Corrupt(..))` and translated to `Invalid` by the caller,
/// mirroring the previous control flow.
enum PayloadOutcome {
    Ok,
    Eof,
}

/// Result of attempting to read and validate the logical log file header.
#[derive(Debug, Clone)]
pub enum HeaderReadResult {
    /// Header is well-formed: magic, version, flags, reserved, and CRC all valid.
    Valid(LogHeader),
    /// File is smaller than `LOG_HDR_SIZE` — no log exists (first run or truncated to zero).
    NoLog,
    /// Header exists but is corrupt (bad magic, version, flags, CRC, non-zero reserved, or truncated).
    Invalid,
}

/// In-flight read state for [`StreamingLogicalLogReader`]. Tracks a pread
/// that has been issued but not yet completed, so the reader's IO-returning
/// methods can yield through their completion and resume on re-entry without
/// re-issuing the read.
#[derive(Debug)]
enum InFlightRead {
    /// A `read_more_data` chunk read whose callback appends to `self.buffer`.
    /// `pre_size` is the buffer length at the moment the read was issued, so
    /// `bytes_read = buffer.len() - pre_size` once the completion finishes.
    Chunk {
        completion: Completion,
        pre_size: usize,
    },
    /// A `read_exact_at` one-shot read whose callback appends to `out`.
    Exact {
        completion: Completion,
        out: Arc<RwLock<Vec<u8>>>,
        expected_len: usize,
    },
}

/// Plaintext + crc32 result to appease clippy for type complexity
type ReadEncryptedResult = Option<(Vec<u8>, u32)>;

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
    /// Buffer index of the start of the transaction frame currently being
    /// parsed. The reader yields mid-frame (any `try_consume_*` can need more
    /// data), and `next_frame`/`parse_next_transaction` restart from the top on
    /// re-entry — so on each parse entry we rewind `buffer_offset` to this
    /// anchor and re-parse the whole frame, and the buffer is only compacted
    /// (drained) up to this anchor, never mid-frame. Advanced to the new frame
    /// boundary only once a frame is fully validated.
    frame_anchor: usize,
    file_size: usize,
    state: StreamingState,
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
    #[cfg(test)]
    pending_ops: std::collections::VecDeque<ParsedOp>,
    // Reused scratch buffer for decrypted chunk plaintext. Kept on the reader so encrypted
    // recovery can reuse the allocation across chunks and transaction frames.
    decrypt_scratch: Vec<u8>,
    /// Set when a read has been issued but its completion has not yet been
    /// observed by the calling IOResult method. Cleared on completion.
    in_flight_read: Option<InFlightRead>,
    /// Progress of the transaction frame currently being parsed by
    /// `parse_next_transaction`, carried across IO yields. `None` between frames.
    /// See [`FrameInProgress`]. (The portable-changes reader uses the
    /// rewind-and-rebuild model and does not populate this.)
    frame_in_progress: Option<FrameInProgress>,
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
            frame_anchor: 0,
            file_size,
            state: StreamingState::NeedTransactionStart,
            last_valid_offset: 0,
            running_crc: 0,
            encryption_ctx,
            encrypted_payload_chunk_size,
            #[cfg(test)]
            pending_ops: std::collections::VecDeque::new(),
            decrypt_scratch,
            in_flight_read: None,
            frame_in_progress: None,
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

    #[cfg(test)]
    pub fn has_pending_ops(&self) -> bool {
        !self.pending_ops.is_empty()
    }

    /// Returns the running CRC state after all validated frames. Used during recovery
    /// to hand off the chain state to the writer so it can continue appending.
    pub fn running_crc(&self) -> u32 {
        self.running_crc
    }

    fn tx_min_frame_size(&self) -> usize {
        match self.header.as_ref().map(|header| header.version) {
            Some(LOG_VERSION_V2) => TX_MIN_FRAME_SIZE_V2,
            _ => TX_MIN_FRAME_SIZE,
        }
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

    /// Blocking shim — retained for tests and the synchronous
    /// `MvStore::bootstrap` callers that have not yet been lifted to
    /// IOResult. The open state machine prefers
    /// [`StreamingLogicalLogReader::try_read_header_nonblock`] so the
    /// MVCC log-header read on open does not block.
    pub(crate) fn try_read_header(&mut self, io: &Arc<dyn crate::IO>) -> Result<HeaderReadResult> {
        let io = io.clone();
        io.block(|| self.try_read_header_nonblock())
    }

    pub(crate) fn try_read_header_nonblock(&mut self) -> IOResultOr<HeaderReadResult> {
        self.file_size = self.file.size()? as usize;
        if self.file_size < LOG_HDR_SIZE {
            return Ok(IOResult::Done(HeaderReadResult::NoLog));
        }

        let header_bytes = return_if_io!(self.read_exact_at(0, LOG_HDR_SIZE));
        // All-zero header means no durable log header yet (pre-fsync crash), not corruption.
        if header_bytes.iter().all(|&b| b == 0) {
            return Ok(IOResult::Done(HeaderReadResult::NoLog));
        }
        let hdr_len = u16::from_le_bytes([header_bytes[6], header_bytes[7]]) as usize;
        if hdr_len != LOG_HDR_SIZE {
            self.set_invalid_header_state();
            return Ok(IOResult::Done(HeaderReadResult::Invalid));
        }

        match LogHeader::decode(&header_bytes) {
            Ok(header) => {
                self.running_crc = derive_initial_crc(header.salt);
                self.header = Some(header.clone());
                self.offset = hdr_len;
                self.buffer.write().clear();
                self.buffer_offset = 0;
                self.frame_anchor = 0;
                self.frame_in_progress = None;
                self.last_valid_offset = hdr_len;
                Ok(IOResult::Done(HeaderReadResult::Valid(header)))
            }
            Err(LimboError::Corrupt(_)) => {
                self.set_invalid_header_state();
                Ok(IOResult::Done(HeaderReadResult::Invalid))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn set_invalid_header_state(&mut self) {
        self.header = None;
        self.offset = LOG_HDR_SIZE;
        self.buffer.write().clear();
        self.buffer_offset = 0;
        self.frame_anchor = 0;
        self.frame_in_progress = None;
        self.last_valid_offset = LOG_HDR_SIZE;
    }

    #[cfg(test)]
    pub(crate) fn next_frame_blocking(
        &mut self,
        io: &Arc<dyn crate::IO>,
    ) -> Result<Option<Vec<ParsedOp>>> {
        let io = io.clone();
        io.block(|| self.next_frame())
    }

    /// Reads the next complete transaction frame.
    ///
    /// Recovery needs the whole frame so it can decide which schema snapshot should decode each
    /// index op. Empty parsed frames are skipped, so callers that receive Some(frame) can
    /// rely on `frame` being non-empty.
    pub(crate) fn next_frame(&mut self) -> IOResultOr<Option<Vec<ParsedOp>>> {
        loop {
            match self.state {
                StreamingState::NeedTransactionStart => {
                    // EOF fast-path, only meaningful when starting a fresh frame.
                    // When a frame is in progress we must resume it regardless of
                    // how few bytes remain from the latest checkpoint (e.g. only
                    // the 8-byte trailer is left), so gate the guard on
                    // `frame_in_progress.is_none()`. Rewind to the frame anchor
                    // first so a mid-frame `buffer_offset` does not undercount
                    // `remaining_bytes()`. `parse_next_transaction` rewinds again
                    // (idempotent).
                    if self.frame_in_progress.is_none() {
                        self.buffer_offset = self.frame_anchor;
                        if self.remaining_bytes() < TX_MIN_FRAME_SIZE {
                            return Ok(IOResult::Done(None));
                        }
                    }

                    let ops = match return_if_io!(self.parse_next_transaction()) {
                        ParseResult::Frame(frame) => frame.ops,
                        ParseResult::Eof | ParseResult::InvalidFrame => {
                            return Ok(IOResult::Done(None));
                        }
                    };

                    if ops.is_empty() {
                        continue;
                    }
                    return Ok(IOResult::Done(Some(ops)));
                }
            }
        }
    }

    /// Reads next record in log.
    ///
    /// This is a test-only version of [Self::next_frame], and it could eventually be replaced
    /// in tests by [Self::next_frame], which didn't exist when [Self::next_record] was written.
    #[cfg(test)]
    pub fn next_record(
        &mut self,
        io: &Arc<dyn crate::IO>,
        mut get_index_info: impl FnMut(MVTableId) -> Result<Arc<IndexInfo>>,
    ) -> Result<StreamingResult> {
        let mut get_index_info = |index_id, _op_kind| get_index_info(index_id);
        self.file_size = self.file.size()? as usize;
        if let Some(op) = self.pending_ops.pop_front() {
            return self.parsed_op_to_streaming(op, &mut get_index_info);
        }

        loop {
            match self.state {
                StreamingState::NeedTransactionStart => {
                    if self.remaining_bytes() < self.tx_min_frame_size() {
                        return Ok(StreamingResult::Eof);
                    }

                    let ops = match io.block(|| self.parse_next_transaction())? {
                        ParseResult::Frame(frame) => frame.ops,
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

    /// Reads the next transaction frame and returns its portable logical-change
    /// payload. This validates the LML3 frame envelope and chained CRC while
    /// treating the recovery payload as opaque bytes.
    ///
    /// Empty payloads are returned because internal-only commits still
    /// advance the logical-log offset even though clients have no operation to
    /// apply.
    pub fn next_portable_change_frame(&mut self) -> IOResultOr<Option<PortableChangeFrame>> {
        self.file_size = self.file.size()? as usize;
        match return_if_io!(self.parse_next_portable_changes_frame()) {
            ParseResult::Frame(frame) => Ok(IOResult::Done(Some(PortableChangeFrame {
                end_offset: frame.end_offset as u64,
                commit_ts: frame.commit_ts,
                extension_record_count: frame.extension_record_count,
                payload: frame.portable_changes,
            }))),
            ParseResult::Eof | ParseResult::InvalidFrame => Ok(IOResult::Done(None)),
        }
    }

    /// Reads the next portable logical-change payload, skipping internal-only
    /// frames.
    ///
    /// Empty payloads are valid: internal-only commits still need recovery
    /// log frames, but they do not produce client-visible logical operations.
    pub fn next_portable_changes(&mut self) -> IOResultOr<Option<PortableChangeFrame>> {
        loop {
            let Some(frame) = return_if_io!(self.next_portable_change_frame()) else {
                return Ok(IOResult::Done(None));
            };
            if !frame.payload.is_empty() {
                return Ok(IOResult::Done(Some(frame)));
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
        payload_ctx: &EncryptedPayloadReadContext,
        chunk_index: usize,
        running_crc: u32,
    ) -> IOResultOr<EncryptedChunkReadResult> {
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
            return Ok(IOResult::Done(EncryptedChunkReadResult::Eof));
        }
        return_if_io!(self.read_more_data(on_disk_size));
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
            )).into());
        }

        Ok(IOResult::Done(EncryptedChunkReadResult::Ok {
            running_crc: next_crc,
        }))
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
        op_count: u32,
        payload_size: usize,
        commit_ts: u64,
        running_crc: u32,
    ) -> IOResultOr<PayloadParseResult> {
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
            running_crc = match return_if_io!(self.read_and_decrypt_encrypted_chunk(
                &payload_ctx,
                chunk_index,
                running_crc,
            )) {
                EncryptedChunkReadResult::Ok { running_crc } => running_crc,
                EncryptedChunkReadResult::Eof => {
                    return Ok(IOResult::Done(PayloadParseResult::Eof));
                }
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
                    )).into());
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
                        ))
                        .into());
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
            ))
            .into());
        }

        // once we have parsed the full payload, carry must be empty
        if !carry.is_empty() {
            return Err(LimboError::Corrupt(format!(
                "encrypted payload has {} trailing plaintext bytes after parsing all ops",
                carry.len()
            ))
            .into());
        }

        Ok(IOResult::Done(PayloadParseResult::Ok(
            parsed_ops,
            running_crc,
        )))
    }

    fn read_encrypted_plaintext(
        &mut self,
        plaintext_size: usize,
        op_count: u32,
        commit_ts: u64,
        running_crc: u32,
    ) -> IOResultOr<ReadEncryptedResult> {
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
            payload_size: plaintext_size,
            op_count,
            commit_ts,
            salt,
            nonce_size,
            tag_size,
        };
        let chunk_count =
            encrypted_payload_chunk_count(plaintext_size, self.encrypted_payload_chunk_size);
        let mut running_crc = running_crc;
        let mut plaintext = Vec::with_capacity(plaintext_size);
        for chunk_index in 0..chunk_count {
            running_crc = match return_if_io!(self.read_and_decrypt_encrypted_chunk(
                &payload_ctx,
                chunk_index,
                running_crc,
            )) {
                EncryptedChunkReadResult::Ok { running_crc } => running_crc,
                EncryptedChunkReadResult::Eof => return Ok(IOResult::Done(None)),
            };
            plaintext.extend_from_slice(&self.decrypt_scratch);
        }
        if plaintext.len() != plaintext_size {
            return Err(LimboError::Corrupt(format!(
                "encrypted plaintext size mismatch: expected {plaintext_size}, got {}",
                plaintext.len()
            ))
            .into());
        }
        Ok(IOResult::Done(Some((plaintext, running_crc))))
    }

    /// Parse an unencrypted payload via field-by-field streaming IO reads.
    ///
    /// Resumable: progress lives in `self.frame_in_progress` (op index, parsed
    /// ops, chained CRC, payload byte count). Each fully consumed op is committed
    /// there and the consume cursor is checkpointed (`advance_checkpoint`), so a
    /// mid-op IO yield re-parses only the in-flight op on re-entry and the buffer
    /// compacts as ops are consumed. Corruption is reported as
    /// `Err(LimboError::Corrupt(..))`; the caller maps it to an invalid frame.
    fn parse_streaming_payload(&mut self) -> IOResultOr<PayloadOutcome> {
        loop {
            let (op_index, op_count, commit_ts) = {
                let fip = self
                    .frame_in_progress
                    .as_ref()
                    .expect("frame in progress while parsing streaming payload");
                (fip.op_index, fip.op_count, fip.commit_ts)
            };

            if op_index >= op_count {
                let (payload_size, payload_bytes_read) = {
                    let fip = self
                        .frame_in_progress
                        .as_ref()
                        .expect("frame in progress while parsing streaming payload");
                    (fip.payload_size, fip.payload_bytes_read)
                };
                if payload_size as u64 != payload_bytes_read {
                    return Err(LimboError::Corrupt(format!(
                        "payload_size ({payload_size}) != payload_bytes_read ({payload_bytes_read})"
                    ))
                    .into());
                }
                return Ok(IOResult::Done(PayloadOutcome::Ok));
            }

            // Seed this op's accumulators from the last committed op; they fold
            // this op's bytes and are written back only once it is fully parsed.
            let mut running_crc = self
                .frame_in_progress
                .as_ref()
                .expect("frame in progress while parsing streaming payload")
                .running_crc;
            let mut payload_bytes_read = self
                .frame_in_progress
                .as_ref()
                .expect("frame in progress while parsing streaming payload")
                .payload_bytes_read;

            // Op header (6 bytes): tag(1) | flags(1) | table_id(4, little-endian i32)
            let op_bytes = match return_if_io!(self.try_consume_fixed::<6>()) {
                Some(bytes) => bytes,
                None => return Ok(IOResult::Done(PayloadOutcome::Eof)),
            };
            running_crc = crc32c::crc32c_append(running_crc, &op_bytes);
            let tag = op_bytes[0];
            let flags = op_bytes[1];
            let table_id_i32 =
                i32::from_le_bytes([op_bytes[2], op_bytes[3], op_bytes[4], op_bytes[5]]);
            let table_id = match tag {
                OP_UPSERT_TABLE | OP_DELETE_TABLE | OP_UPSERT_INDEX | OP_DELETE_INDEX => {
                    if flags & !OP_ALLOWED_FLAGS != 0 || table_id_i32 >= 0 {
                        return Err(LimboError::Corrupt(format!(
                            "invalid op flags={flags:#x} or table_id={table_id_i32} for tag={tag}"
                        ))
                        .into());
                    }
                    Some(MVTableId::from(table_id_i32 as i64))
                }
                OP_UPDATE_HEADER => {
                    if flags != 0 || table_id_i32 != 0 {
                        return Err(LimboError::Corrupt(format!(
                            "OP_UPDATE_HEADER has non-zero flags={flags:#x} or table_id={table_id_i32}"
                        )).into());
                    }
                    None
                }
                _ => {
                    return Err(LimboError::Corrupt(format!("unknown op tag {tag}")).into());
                }
            };
            let btree_resident = (flags & OP_FLAG_BTREE_RESIDENT) != 0;
            let has_portable_extension = (flags & OP_FLAG_PORTABLE_EXTENSION) != 0;

            let (payload_len, payload_len_bytes, payload_len_bytes_len) =
                match return_if_io!(self.consume_varint_bytes()) {
                    Some((value, bytes, len)) => (value, bytes, len),
                    None => return Ok(IOResult::Done(PayloadOutcome::Eof)),
                };
            running_crc =
                crc32c::crc32c_append(running_crc, &payload_len_bytes[..payload_len_bytes_len]);
            let payload_len = usize::try_from(payload_len)
                .map_err(|e| LimboError::Corrupt(format!("payload_len overflows usize: {e}")))?;

            let payload = match return_if_io!(self.try_consume_bytes(payload_len)) {
                Some(bytes) => bytes,
                None => return Ok(IOResult::Done(PayloadOutcome::Eof)),
            };
            running_crc = crc32c::crc32c_append(running_crc, &payload);

            let (portable_extension, extension_total_bytes) = if has_portable_extension {
                let (extension_len, extension_len_bytes, extension_len_bytes_len) =
                    match return_if_io!(self.consume_varint_bytes()) {
                        Some((value, bytes, len)) => (value, bytes, len),
                        None => return Ok(IOResult::Done(PayloadOutcome::Eof)),
                    };
                running_crc = crc32c::crc32c_append(
                    running_crc,
                    &extension_len_bytes[..extension_len_bytes_len],
                );
                let extension_len = usize::try_from(extension_len).map_err(|e| {
                    LimboError::Corrupt(format!("op extension length overflows usize: {e}"))
                })?;
                let extension = match return_if_io!(self.try_consume_bytes(extension_len)) {
                    Some(bytes) => bytes,
                    None => return Ok(IOResult::Done(PayloadOutcome::Eof)),
                };
                running_crc = crc32c::crc32c_append(running_crc, &extension);
                (extension, extension_len_bytes_len + extension_len)
            } else {
                (crate::alloc::vec![], 0)
            };

            let op_total_bytes = 6 + payload_len_bytes_len + payload_len + extension_total_bytes;
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
                        )
                        .into());
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
                    if rowid_len > payload.len() {
                        return Err(LimboError::Corrupt(format!(
                            "delete op rowid varint len {rowid_len} > payload len {}",
                            payload.len()
                        ))
                        .into());
                    }
                    let rowid_i64 = rowid_u64 as i64;
                    let mut payload = payload;
                    let mut record_bytes = payload.split_off(rowid_len);
                    let mut pk_record_bytes = crate::alloc::vec![];
                    if !portable_extension.is_empty() {
                        let decoded = decode_delete_portable_extension(&portable_extension)?;
                        if record_bytes.is_empty() {
                            record_bytes = decoded.identity_record;
                        }
                        pk_record_bytes = decoded.pk_record;
                    }
                    let rowid = RowID::new(table_id, RowKey::Int(rowid_i64));
                    ParsedOp::DeleteTable {
                        rowid,
                        record_bytes,
                        pk_record_bytes,
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
                        ))
                        .into());
                    }
                    let mut bytes = [0u8; DatabaseHeader::SIZE];
                    bytes.copy_from_slice(&payload);
                    let header = *bytemuck::from_bytes::<DatabaseHeader>(&bytes);
                    if header.magic != *b"SQLite format 3\0" {
                        return Err(LimboError::Corrupt(
                            "OP_UPDATE_HEADER has invalid SQLite magic".to_string(),
                        )
                        .into());
                    }
                    ParsedOp::UpdateHeader { header, commit_ts }
                }
                _ => {
                    return Err(
                        LimboError::Corrupt(format!("unknown op tag {tag} in payload")).into(),
                    );
                }
            };

            // Op fully parsed and folded: commit it and checkpoint the cursor so
            // re-entry resumes at the next op and the buffer can compact this one.
            {
                let fip = self
                    .frame_in_progress
                    .as_mut()
                    .expect("frame in progress while parsing streaming payload");
                fip.parsed_ops.push(parsed_op);
                fip.running_crc = running_crc;
                fip.payload_bytes_read = payload_bytes_read;
                fip.op_index += 1;
            }
            self.advance_checkpoint();
        }
    }

    /// Parse the next transaction frame as a re-entrant phase machine.
    ///
    /// Progress is carried in `self.frame_in_progress` across IO yields. Each
    /// phase (header, optional unencrypted extension block, payload, trailer) is
    /// a re-entrant unit: once fully consumed and folded into the chained CRC it
    /// checkpoints (`advance_checkpoint`), advancing `frame_anchor` to the
    /// consume cursor so `read_more_data` can compact everything before it and a
    /// later yield rewinds only to the latest checkpoint. Re-entry rewinds
    /// `buffer_offset` to `frame_anchor` and re-runs just the in-flight unit from
    /// local state. `frame_start` is captured once at frame open (never
    /// recomputed) so an invalid frame reports the correct `last_valid_offset`.
    fn parse_next_transaction(&mut self) -> IOResultOr<ParseResult> {
        loop {
            if self.frame_in_progress.is_none() {
                // Start a fresh frame at the current consume cursor.
                self.buffer_offset = self.frame_anchor;
                if self.remaining_bytes() < self.tx_min_frame_size() {
                    return Ok(IOResult::Done(ParseResult::Eof));
                }
                let frame_start = self.offset.saturating_sub(self.bytes_can_read());
                self.frame_in_progress = Some(FrameInProgress::new(frame_start));
            } else {
                // Resume the in-flight frame: rewind the consume cursor to the
                // latest checkpoint and re-run the current phase from there.
                self.buffer_offset = self.frame_anchor;
            }

            let phase = self
                .frame_in_progress
                .as_ref()
                .expect("frame in progress")
                .phase;
            match phase {
                FramePhase::Header => {
                    let header = match return_if_io!(self.parse_frame_header()) {
                        HeaderParseOutcome::Ok(header) => header,
                        HeaderParseOutcome::Eof => return self.abort_frame_eof(),
                        HeaderParseOutcome::Invalid => return self.invalidate_frame(),
                    };
                    // Unencrypted frames with an extension block consume it as a
                    // separate phase; encrypted frames carry the extension inside
                    // the encrypted plaintext (handled in the payload phase).
                    let next_phase = if self.encryption_ctx.is_none() && header.extension_size > 0 {
                        FramePhase::ExtensionBlock
                    } else {
                        FramePhase::Payload
                    };
                    {
                        let fip = self.frame_in_progress.as_mut().expect("frame in progress");
                        fip.payload_size = header.payload_size;
                        fip.op_count = header.op_count;
                        fip.commit_ts = header.commit_ts;
                        fip.extension_size = header.extension_size;
                        fip.extension_record_count = header.extension_record_count;
                        fip.frame_flags = header.frame_flags;
                        fip.running_crc = header.running_crc;
                        fip.phase = next_phase;
                    }
                    self.advance_checkpoint();
                }
                FramePhase::ExtensionBlock => {
                    let (extension_size, extension_record_count, running_crc) = {
                        let fip = self.frame_in_progress.as_ref().expect("frame in progress");
                        (
                            fip.extension_size,
                            fip.extension_record_count,
                            fip.running_crc,
                        )
                    };
                    let bytes = match return_if_io!(self.try_consume_bytes(extension_size)) {
                        Some(bytes) => bytes,
                        None => return self.abort_frame_eof(),
                    };
                    let running_crc = crc32c::crc32c_append(running_crc, &bytes);
                    let portable_changes = match find_extension_payload(
                        &bytes,
                        extension_record_count,
                        EXTENSION_TYPE_PORTABLE_CHANGES,
                    ) {
                        Ok(payload) => payload,
                        Err(LimboError::Corrupt(msg)) => {
                            tracing::warn!("corrupt extension block: {msg}");
                            return self.invalidate_frame();
                        }
                        Err(e) => return Err(e.into()),
                    };
                    {
                        let fip = self.frame_in_progress.as_mut().expect("frame in progress");
                        fip.portable_changes = portable_changes;
                        fip.running_crc = running_crc;
                        fip.phase = FramePhase::Payload;
                    }
                    self.advance_checkpoint();
                }
                FramePhase::Payload => match self.parse_payload_phase() {
                    Ok(IOResult::Done(PayloadOutcome::Ok)) => {
                        self.frame_in_progress
                            .as_mut()
                            .expect("frame in progress")
                            .phase = FramePhase::Trailer;
                        self.advance_checkpoint();
                    }
                    Ok(IOResult::Done(PayloadOutcome::Eof)) => return self.abort_frame_eof(),
                    Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                    Err(err) if matches!(*err, LimboError::Corrupt(_)) => {
                        tracing::warn!("corrupt payload: {err}");
                        return self.invalidate_frame();
                    }
                    Err(e) => return Err(e),
                },
                FramePhase::Trailer => {
                    // TX TRAILER layout (8 bytes): crc32c(4, le u32) | END_MAGIC(4)
                    let trailer_bytes =
                        match return_if_io!(self.try_consume_fixed::<TX_TRAILER_SIZE>()) {
                            Some(bytes) => bytes,
                            None => return self.abort_frame_eof(),
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
                    let running_crc = self
                        .frame_in_progress
                        .as_ref()
                        .expect("frame in progress")
                        .running_crc;
                    if crc32c_expected != running_crc {
                        return self.invalidate_frame();
                    }
                    if end_magic != END_MAGIC {
                        return self.invalidate_frame();
                    }
                    return self.commit_frame();
                }
            }
        }
    }

    /// Parse and validate the transaction header (a re-entrant atomic unit).
    /// Reads from the current consume cursor and mutates only local state plus
    /// the consume cursor, so it is safe to re-run from the frame anchor on
    /// re-entry. The chained CRC is seeded from `self.running_crc` and folded
    /// over the header bytes. Field/structural problems return `Invalid`; the
    /// caller sets `last_valid_offset` from the captured `frame_start`.
    fn parse_frame_header(&mut self) -> IOResultOr<HeaderParseOutcome> {
        // TX HEADER v2 layout (24 bytes):
        // FRAME_MAGIC(4) | payload_size(8) | op_count(4) | commit_ts(8)
        //
        // TX HEADER v3 extension frames append:
        // extension_size(8) | extension_record_count(4) | frame_flags(4)
        let mut header_bytes = match return_if_io!(self.try_consume_bytes(TX_HEADER_SIZE)) {
            Some(bytes) => bytes,
            None => return Ok(IOResult::Done(HeaderParseOutcome::Eof)),
        };

        let frame_magic = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);
        let is_v2 = self
            .header
            .as_ref()
            .is_some_and(|header| header.version == LOG_VERSION_V2);
        let has_extension_header = !is_v2 && frame_magic == EXT_FRAME_MAGIC;
        if frame_magic != FRAME_MAGIC && !has_extension_header {
            return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
        }
        if is_v2 && frame_magic != FRAME_MAGIC {
            return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
        }
        if has_extension_header {
            let Some(extension_header) =
                return_if_io!(self.try_consume_bytes(TX_EXT_HEADER_SIZE - TX_HEADER_SIZE))
            else {
                return Ok(IOResult::Done(HeaderParseOutcome::Eof));
            };
            header_bytes.extend_from_slice(&extension_header);
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
        let (extension_size, extension_record_count, frame_flags) = if has_extension_header {
            let extension_size_u64 = u64::from_le_bytes([
                header_bytes[24],
                header_bytes[25],
                header_bytes[26],
                header_bytes[27],
                header_bytes[28],
                header_bytes[29],
                header_bytes[30],
                header_bytes[31],
            ]);
            let extension_size = match usize::try_from(extension_size_u64) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("extension_size overflows usize: {e}");
                    return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
                }
            };
            let extension_record_count = u32::from_le_bytes([
                header_bytes[32],
                header_bytes[33],
                header_bytes[34],
                header_bytes[35],
            ]);
            let frame_flags = u32::from_le_bytes([
                header_bytes[36],
                header_bytes[37],
                header_bytes[38],
                header_bytes[39],
            ]);
            if frame_flags & !TX_FRAME_FLAG_HAS_EXTENSION_BLOCK != 0 {
                return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
            }
            if extension_size == 0 && extension_record_count != 0 {
                return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
            }
            if extension_size > 0 && frame_flags & TX_FRAME_FLAG_HAS_EXTENSION_BLOCK == 0 {
                return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
            }
            (extension_size, extension_record_count, frame_flags)
        } else {
            (0, 0, 0)
        };

        let payload_size = match usize::try_from(payload_size_u64) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("payload_size overflows usize: {e}");
                return Ok(IOResult::Done(HeaderParseOutcome::Invalid));
            }
        };

        // Chained CRC: seed from running_crc (derived from salt, or previous frame's CRC).
        let running_crc = crc32c::crc32c_append(self.running_crc, &header_bytes);

        Ok(IOResult::Done(HeaderParseOutcome::Ok(FrameHeader {
            payload_size,
            op_count,
            commit_ts,
            extension_size,
            extension_record_count,
            frame_flags,
            running_crc,
        })))
    }

    /// Parse the payload phase, dispatching on encryption. The unencrypted path
    /// is the resumable per-op machine (`parse_streaming_payload`) that
    /// checkpoints into `frame_in_progress`; the encrypted paths are parsed
    /// wholesale from the payload start (rewind-and-rebuild from `frame_anchor`)
    /// and store their result into `frame_in_progress` once complete. Corruption
    /// propagates as `Err(LimboError::Corrupt(..))` for the caller to map to an
    /// invalid frame.
    fn parse_payload_phase(&mut self) -> IOResultOr<PayloadOutcome> {
        let (payload_size, op_count, commit_ts, extension_size, extension_record_count, header_crc) = {
            let fip = self.frame_in_progress.as_ref().expect("frame in progress");
            (
                fip.payload_size,
                fip.op_count,
                fip.commit_ts,
                fip.extension_size,
                fip.extension_record_count,
                fip.running_crc,
            )
        };
        let encrypted_extension_size = if self.encryption_ctx.is_some() {
            extension_size
        } else {
            0
        };

        if encrypted_extension_size > 0 {
            let plaintext_size = payload_size
                .checked_add(encrypted_extension_size)
                .ok_or_else(|| {
                    LimboError::Corrupt("encrypted plaintext size overflows usize".into())
                })?;
            let Some((plaintext, running_crc)) = return_if_io!(self.read_encrypted_plaintext(
                plaintext_size,
                op_count,
                commit_ts,
                header_crc,
            )) else {
                return Ok(IOResult::Done(PayloadOutcome::Eof));
            };
            let recovery_start = extension_size;
            let recovery_end = recovery_start
                .checked_add(payload_size)
                .ok_or_else(|| LimboError::Corrupt("recovery payload offset overflow".into()))?;
            let portable_changes = find_extension_payload(
                &plaintext[..extension_size],
                extension_record_count,
                EXTENSION_TYPE_PORTABLE_CHANGES,
            )?;
            let parsed_ops = parse_ops_from_plaintext(
                &plaintext[recovery_start..recovery_end],
                payload_size,
                op_count,
                commit_ts,
            )?;
            let fip = self.frame_in_progress.as_mut().expect("frame in progress");
            fip.parsed_ops = parsed_ops;
            fip.portable_changes = portable_changes;
            fip.running_crc = running_crc;
            return Ok(IOResult::Done(PayloadOutcome::Ok));
        }

        if self.encryption_ctx.is_some() {
            let (parsed_ops, running_crc) = match self.parse_encrypted_payload(
                op_count,
                payload_size,
                commit_ts,
                header_crc,
            )? {
                IOResult::Done(PayloadParseResult::Ok(ops, crc)) => (ops, crc),
                IOResult::Done(PayloadParseResult::Eof) => {
                    return Ok(IOResult::Done(PayloadOutcome::Eof));
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let fip = self.frame_in_progress.as_mut().expect("frame in progress");
            fip.parsed_ops = parsed_ops;
            fip.running_crc = running_crc;
            return Ok(IOResult::Done(PayloadOutcome::Ok));
        }

        // Unencrypted: resumable per-op machine that checkpoints into
        // `frame_in_progress` (parsed ops + CRC + byte count) as it goes.
        self.parse_streaming_payload()
    }

    /// Commit the consume cursor as the new rewind point. Called once a unit
    /// (header / extension block / op / payload) is fully consumed and folded
    /// into the in-progress chained CRC, so `read_more_data` may compact every
    /// byte before it and re-entry resumes here rather than at the frame start.
    fn advance_checkpoint(&mut self) {
        self.frame_anchor = self.buffer_offset;
    }

    /// Torn tail: not enough bytes remain to finish the in-progress frame. Drop
    /// it without advancing the chain — `last_valid_offset`/`running_crc` stay at
    /// the last fully committed frame. EOF is terminal for a recovery pass
    /// (`file_size` is fixed once recovery starts).
    fn abort_frame_eof(&mut self) -> IOResultOr<ParseResult> {
        self.frame_in_progress = None;
        Ok(IOResult::Done(ParseResult::Eof))
    }

    /// The in-progress frame is structurally invalid (bad magic/flags/CRC/op).
    /// Set `last_valid_offset` to the captured frame start so the writer
    /// overwrites the torn frame on the next append, and drop the frame without
    /// advancing the chain.
    fn invalidate_frame(&mut self) -> IOResultOr<ParseResult> {
        let frame_start = self
            .frame_in_progress
            .as_ref()
            .expect("frame in progress")
            .frame_start;
        self.last_valid_offset = frame_start;
        self.frame_in_progress = None;
        Ok(IOResult::Done(ParseResult::InvalidFrame))
    }

    /// Commit a fully validated frame: advance `last_valid_offset` to the byte
    /// past the trailer, carry this frame's CRC as the seed for the next frame,
    /// and move the frame anchor past the trailer.
    fn commit_frame(&mut self) -> IOResultOr<ParseResult> {
        let fip = self.frame_in_progress.take().expect("frame in progress");
        self.last_valid_offset = self.offset.saturating_sub(self.bytes_can_read());
        self.running_crc = fip.running_crc;
        self.frame_anchor = self.buffer_offset;
        Ok(IOResult::Done(ParseResult::Frame(ParsedFrame {
            ops: fip.parsed_ops,
            portable_changes: fip.portable_changes,
            extension_record_count: fip.extension_record_count,
            frame_flags: fip.frame_flags,
            commit_ts: fip.commit_ts,
            end_offset: self.last_valid_offset,
        })))
    }

    fn consume_and_crc_bytes(
        &mut self,
        mut amount: usize,
        mut running_crc: u32,
    ) -> IOResultOr<Option<u32>> {
        const CHUNK_SIZE: usize = 64 * 1024;
        while amount > 0 {
            let chunk_len = amount.min(CHUNK_SIZE);
            let Some(bytes) = return_if_io!(self.try_consume_bytes(chunk_len)) else {
                return Ok(IOResult::Done(None));
            };
            running_crc = crc32c::crc32c_append(running_crc, &bytes);
            amount -= chunk_len;
        }
        Ok(IOResult::Done(Some(running_crc)))
    }

    fn encrypted_payload_on_disk_size(&self, payload_size: usize) -> Result<usize> {
        let Some(encryption_ctx) = self.encryption_ctx.as_ref() else {
            return Ok(payload_size);
        };
        let mut on_disk_size = 0usize;
        for chunk_index in
            0..encrypted_payload_chunk_count(payload_size, self.encrypted_payload_chunk_size)
        {
            let plaintext_len = encrypted_chunk_plaintext_len(
                payload_size,
                chunk_index,
                self.encrypted_payload_chunk_size,
            )?;
            on_disk_size = on_disk_size
                .checked_add(encrypted_chunk_blob_size(
                    plaintext_len,
                    encryption_ctx.tag_size(),
                    encryption_ctx.nonce_size(),
                )?)
                .ok_or_else(|| {
                    LimboError::Corrupt("encrypted payload size overflows usize".to_string())
                })?;
        }
        Ok(on_disk_size)
    }

    fn parse_next_portable_changes_frame(&mut self) -> IOResultOr<ParseResult> {
        // See `parse_next_transaction`: rewind to the frame anchor so a mid-frame
        // IO yield resumes correctly on re-entry.
        self.buffer_offset = self.frame_anchor;
        if self
            .header
            .as_ref()
            .is_some_and(|h| h.version == LOG_VERSION_V2)
        {
            return Ok(IOResult::Done(ParseResult::Eof));
        }
        if self.remaining_bytes() < TX_MIN_FRAME_SIZE {
            return Ok(IOResult::Done(ParseResult::Eof));
        }
        let frame_start = self.offset.saturating_sub(self.bytes_can_read());

        let mut header_bytes = match return_if_io!(self.try_consume_bytes(TX_HEADER_SIZE)) {
            Some(bytes) => bytes,
            None => return Ok(IOResult::Done(ParseResult::Eof)),
        };

        let frame_magic = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);
        let has_extension_header = frame_magic == EXT_FRAME_MAGIC;
        if frame_magic != FRAME_MAGIC && !has_extension_header {
            self.last_valid_offset = frame_start;
            return Ok(IOResult::Done(ParseResult::InvalidFrame));
        }
        if has_extension_header {
            let Some(extension_header) =
                return_if_io!(self.try_consume_bytes(TX_EXT_HEADER_SIZE - TX_HEADER_SIZE))
            else {
                return Ok(IOResult::Done(ParseResult::Eof));
            };
            header_bytes.extend_from_slice(&extension_header);
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
        let (extension_size_u64, extension_record_count, frame_flags) = if has_extension_header {
            let extension_size_u64 = u64::from_le_bytes([
                header_bytes[24],
                header_bytes[25],
                header_bytes[26],
                header_bytes[27],
                header_bytes[28],
                header_bytes[29],
                header_bytes[30],
                header_bytes[31],
            ]);
            let extension_record_count = u32::from_le_bytes([
                header_bytes[32],
                header_bytes[33],
                header_bytes[34],
                header_bytes[35],
            ]);
            let frame_flags = u32::from_le_bytes([
                header_bytes[36],
                header_bytes[37],
                header_bytes[38],
                header_bytes[39],
            ]);
            if frame_flags & !TX_FRAME_FLAG_HAS_EXTENSION_BLOCK != 0 {
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
            if extension_size_u64 == 0 && extension_record_count != 0 {
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
            if extension_size_u64 > 0 && frame_flags & TX_FRAME_FLAG_HAS_EXTENSION_BLOCK == 0 {
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
            (extension_size_u64, extension_record_count, frame_flags)
        } else {
            (0, 0, 0)
        };

        let payload_size = match usize::try_from(payload_size_u64) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("payload_size overflows usize: {e}");
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
        };
        let extension_size = match usize::try_from(extension_size_u64) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("extension_size overflows usize: {e}");
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
        };

        let running_crc = crc32c::crc32c_append(self.running_crc, &header_bytes);
        let encrypted_extension_size = if self.encryption_ctx.is_some() {
            extension_size
        } else {
            0
        };
        let payload_on_disk_size = match self.encrypted_payload_on_disk_size(
            payload_size
                .checked_add(encrypted_extension_size)
                .ok_or_else(|| {
                    LimboError::Corrupt(
                        "payload plus encrypted extension size overflows usize".to_string(),
                    )
                })?,
        ) {
            Ok(size) => size,
            Err(LimboError::Corrupt(msg)) => {
                tracing::warn!("corrupt payload size: {msg}");
                self.last_valid_offset = frame_start;
                return Ok(IOResult::Done(ParseResult::InvalidFrame));
            }
            Err(e) => return Err(e.into()),
        };
        let (portable_changes, running_crc) = if encrypted_extension_size > 0 {
            let plaintext_size = payload_size
                .checked_add(encrypted_extension_size)
                .ok_or_else(|| {
                    LimboError::Corrupt("encrypted plaintext size overflows usize".into())
                })?;
            let Some((plaintext, running_crc)) = return_if_io!(self.read_encrypted_plaintext(
                plaintext_size,
                op_count,
                commit_ts,
                running_crc,
            )) else {
                return Ok(IOResult::Done(ParseResult::Eof));
            };
            let portable_changes = match find_extension_payload(
                &plaintext[..extension_size],
                extension_record_count,
                EXTENSION_TYPE_PORTABLE_CHANGES,
            ) {
                Ok(payload) => payload,
                Err(LimboError::Corrupt(msg)) => {
                    tracing::warn!("corrupt extension block: {msg}");
                    self.last_valid_offset = frame_start;
                    return Ok(IOResult::Done(ParseResult::InvalidFrame));
                }
                Err(e) => return Err(e.into()),
            };
            (portable_changes, running_crc)
        } else {
            let (portable_changes, running_crc) = if extension_size > 0 {
                match return_if_io!(self.try_consume_bytes(extension_size)) {
                    Some(bytes) => {
                        let running_crc = crc32c::crc32c_append(running_crc, &bytes);
                        let portable_changes = match find_extension_payload(
                            &bytes,
                            extension_record_count,
                            EXTENSION_TYPE_PORTABLE_CHANGES,
                        ) {
                            Ok(payload) => payload,
                            Err(LimboError::Corrupt(msg)) => {
                                tracing::warn!("corrupt extension block: {msg}");
                                self.last_valid_offset = frame_start;
                                return Ok(IOResult::Done(ParseResult::InvalidFrame));
                            }
                            Err(e) => return Err(e.into()),
                        };
                        (portable_changes, running_crc)
                    }
                    None => return Ok(IOResult::Done(ParseResult::Eof)),
                }
            } else {
                (Vec::new(), running_crc)
            };
            let Some(running_crc) =
                return_if_io!(self.consume_and_crc_bytes(payload_on_disk_size, running_crc))
            else {
                return Ok(IOResult::Done(ParseResult::Eof));
            };
            (portable_changes, running_crc)
        };

        let trailer_bytes = match return_if_io!(self.try_consume_fixed::<TX_TRAILER_SIZE>()) {
            Some(bytes) => bytes,
            None => return Ok(IOResult::Done(ParseResult::Eof)),
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
            return Ok(IOResult::Done(ParseResult::InvalidFrame));
        }
        if end_magic != END_MAGIC {
            self.last_valid_offset = frame_start;
            return Ok(IOResult::Done(ParseResult::InvalidFrame));
        }

        self.last_valid_offset = self.offset.saturating_sub(self.bytes_can_read());
        self.running_crc = running_crc;
        self.frame_anchor = self.buffer_offset;
        Ok(IOResult::Done(ParseResult::Frame(ParsedFrame {
            ops: Vec::new(),
            portable_changes,
            extension_record_count,
            frame_flags,
            commit_ts,
            end_offset: self.last_valid_offset,
        })))
    }

    pub(crate) fn parsed_op_to_streaming(
        &self,
        parsed_op: ParsedOp,
        get_index_info: &mut impl FnMut(MVTableId, IndexOpKind) -> Result<Arc<IndexInfo>>,
    ) -> Result<StreamingResult> {
        self.parsed_op_to_streaming_in(parsed_op, get_index_info, TursoAllocator)
    }

    pub(crate) fn parsed_op_to_streaming_in<A: ConcurrentAllocator>(
        &self,
        parsed_op: ParsedOp,
        get_index_info: &mut impl FnMut(MVTableId, IndexOpKind) -> Result<Arc<IndexInfo>>,
        alloc: A,
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
                // Decode shape metadata by reference; ownership is only needed for the row payload.
                let column_count =
                    crate::types::ImmutableRecordRef::from_bin_record(&record_bytes).column_count();
                let row = crate::with_mv_store_allocation_site!(
                    RowPayload,
                    Row::new_table_row_in(
                        RowID::new(table_id, rowid.row_id.clone()),
                        &record_bytes,
                        column_count,
                        alloc,
                    )?
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
                record_bytes: _,
                pk_record_bytes: _,
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
                let key_record = crate::types::ImmutableRecordRef::from_bin_record(&payload);
                let column_count = key_record.column_count();
                let index_info = get_index_info(table_id, IndexOpKind::Upsert)?;
                let key = Arc::new(SortableIndexKey::new_from_payload_in(
                    key_record, index_info, alloc,
                )?);
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
                let key_record = crate::types::ImmutableRecordRef::from_bin_record(&payload);
                let column_count = key_record.column_count();
                let index_info = get_index_info(table_id, IndexOpKind::Delete)?;
                let key = Arc::new(SortableIndexKey::new_from_payload_in(
                    key_record, index_info, alloc,
                )?);
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

    fn try_consume_bytes(&mut self, amount: usize) -> IOResultOr<Option<crate::ValueBlob>> {
        if self.remaining_bytes() < amount {
            return Ok(IOResult::Done(None));
        }
        return_if_io!(self.read_more_data(amount));
        let buffer = self.buffer.read();
        let start = self.buffer_offset;
        let end = start + amount;
        let bytes = crate::types::value_blob_from_slice(&buffer[start..end])?;
        self.buffer_offset = end;
        Ok(IOResult::Done(Some(bytes)))
    }

    fn try_consume_fixed<const N: usize>(&mut self) -> IOResultOr<Option<[u8; N]>> {
        if self.remaining_bytes() < N {
            return Ok(IOResult::Done(None));
        }
        return_if_io!(self.read_more_data(N));
        let buffer = self.buffer.read();
        let start = self.buffer_offset;
        let end = start + N;
        let mut out = [0u8; N];
        out.copy_from_slice(&buffer[start..end]);
        self.buffer_offset = end;
        Ok(IOResult::Done(Some(out)))
    }

    fn try_consume_u8(&mut self) -> IOResultOr<Option<u8>> {
        if self.remaining_bytes() == 0 {
            return Ok(IOResult::Done(None));
        }
        return_if_io!(self.read_more_data(1));
        let r = self.buffer.read()[self.buffer_offset];
        self.buffer_offset += 1;
        Ok(IOResult::Done(Some(r)))
    }

    /// Reads a SQLite-format varint one byte at a time from the streaming reader.
    /// Returns `(decoded_value, raw_bytes, byte_count)`. The raw bytes are returned
    /// so callers can feed them into the CRC computation without re-encoding.
    /// Unlike `read_varint` from sqlite3_ondisk (which requires a contiguous buffer),
    /// this reads byte-by-byte via `try_consume_u8` to handle streaming I/O where
    /// the varint may span a buffer boundary. Returns `None` on EOF (short read).
    #[allow(clippy::type_complexity)]
    fn consume_varint_bytes(&mut self) -> IOResultOr<Option<(u64, [u8; 9], usize)>> {
        let mut v: u64 = 0;
        let mut bytes = [0u8; 9];
        let mut len = 0usize;
        for _ in 0..8 {
            let Some(c) = return_if_io!(self.try_consume_u8()) else {
                return Ok(IOResult::Done(None));
            };
            bytes[len] = c;
            len += 1;
            v = (v << 7) + (c & 0x7f) as u64;
            if (c & 0x80) == 0 {
                return Ok(IOResult::Done(Some((v, bytes, len))));
            }
        }
        let Some(c) = return_if_io!(self.try_consume_u8()) else {
            return Ok(IOResult::Done(None));
        };
        bytes[len] = c;
        len += 1;
        if (v >> 48) == 0 {
            return Err(LimboError::Corrupt("Invalid varint".to_string()).into());
        }
        v = (v << 8) + c as u64;
        Ok(IOResult::Done(Some((v, bytes, len))))
    }

    /// Non-blocking read of exactly `len` bytes at file offset `pos`.
    ///
    /// On entry: if an in-flight `Exact` read is already pending, resume it
    /// (yield until done, then return its accumulated buffer). Otherwise
    /// issue a fresh pread, stash it in `self.in_flight_read`, and either
    /// yield the completion (when not synchronously done) or loop to take
    /// the resume branch.
    fn read_exact_at(&mut self, pos: u64, len: usize) -> IOResultOr<Vec<u8>> {
        loop {
            if let Some(InFlightRead::Exact { completion, .. }) = &self.in_flight_read {
                if !completion.succeeded() {
                    let c = completion.clone();
                    io_yield_one!(c);
                }
                let Some(InFlightRead::Exact {
                    out, expected_len, ..
                }) = self.in_flight_read.take()
                else {
                    unreachable!("in_flight_read variant just matched Exact");
                };
                let result = out.read().clone();
                if result.len() != expected_len {
                    return Err(LimboError::Corrupt(format!(
                        "Logical log short read: expected {expected_len}, got {}",
                        result.len()
                    ))
                    .into());
                }
                return Ok(IOResult::Done(result));
            }

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
            self.in_flight_read = Some(InFlightRead::Exact {
                completion: c,
                out,
                expected_len: len,
            });
            // Loop to take the resume branch — handles both the synchronous-
            // completion and not-finished cases uniformly.
        }
    }

    fn get_buffer(&self) -> crate::sync::RwLockReadGuard<'_, Vec<u8>> {
        self.buffer.read()
    }

    /// Read at least `need` bytes from the logical log, issuing multiple
    /// reads if necessary. If at any point 0 bytes are read, that indicates
    /// corruption.
    ///
    /// Non-blocking: a pread in flight is tracked in `self.in_flight_read`
    /// and the method yields its completion until done. Re-entry picks up
    /// where it left off without re-issuing the read.
    pub fn read_more_data(&mut self, need: usize) -> IOResultOr<()> {
        loop {
            // Resume hook: a pread that was issued by a previous call to
            // this method completed; observe its result and advance.
            if let Some(InFlightRead::Chunk { completion, .. }) = &self.in_flight_read {
                if !completion.succeeded() {
                    let c = completion.clone();
                    io_yield_one!(c);
                }
                let Some(InFlightRead::Chunk { pre_size, .. }) = self.in_flight_read.take() else {
                    unreachable!("in_flight_read variant just matched Chunk");
                };
                let buffer_size_after_read = self.buffer.read().len();
                let bytes_read = buffer_size_after_read - pre_size;
                if bytes_read == 0 {
                    return Err(LimboError::Corrupt(format!(
                        "Expected to read more bytes but read 0 bytes at offset {}",
                        self.offset
                    ))
                    .into());
                }
                self.offset += bytes_read;
            }

            let buffer_size_before_read = self.buffer.read().len();
            turso_assert!(
                buffer_size_before_read >= self.buffer_offset,
                "buffer_size_before_read < buffer_offset",
                { "buffer_size_before_read": buffer_size_before_read, "buffer_offset": self.buffer_offset }
            );
            let bytes_available_in_buffer = buffer_size_before_read - self.buffer_offset;
            let still_need = need.saturating_sub(bytes_available_in_buffer);

            if still_need == 0 {
                // Data is already buffered — return without touching the buffer.
                // Compaction happens only on the disk-read path below: draining
                // here would memmove the buffer tail on every consume (i.e. once
                // per frame for tiny frames), which is a large recovery
                // regression with no benefit, since the buffer only grows when we
                // actually read from disk.
                return Ok(IOResult::Done(()));
            }

            // We must read from disk. Compact the consumed bytes *before* the
            // latest checkpoint (`frame_anchor`) first, so the buffer doesn't
            // grow without bound. `frame_anchor` advances at every parse
            // checkpoint (each header / extension block / op), so for the
            // streaming path this drains fully-consumed ops and bounds the buffer
            // to roughly the in-flight unit rather than the whole frame. Bytes at
            // `frame_anchor..` must stay buffered so a mid-unit IO yield can be
            // resumed by rewinding `buffer_offset` back to `frame_anchor` and
            // re-parsing that unit. Draining up to `buffer_offset` (the consume
            // cursor) instead would discard the in-flight unit's bytes and
            // corrupt its re-parse.
            let drain_to = self.frame_anchor.min(self.buffer.read().len());
            if drain_to > 0 {
                let _ = self.buffer.write().drain(0..drain_to);
                self.buffer_offset -= drain_to;
                self.frame_anchor -= drain_to;
            }

            turso_assert!(
                self.file_size >= self.offset,
                "file_size < offset",
                { "file_size": self.file_size, "offset": self.offset }
            );
            // Recompute after draining: `buffer.len()` shrank, and `pre_size`
            // (captured below) must reflect the post-drain length so the
            // completion's `bytes_read = buffer.len() - pre_size` is correct.
            let buffer_size_before_read = self.buffer.read().len();
            let to_read = 4096.max(still_need).min(self.file_size - self.offset);

            if to_read == 0 {
                // No more data available in file even though we need more -> corrupt
                return Err(LimboError::Corrupt(format!(
                    "Expected to read {still_need} bytes more but reached end of file at offset {}",
                    self.offset
                ))
                .into());
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
            self.in_flight_read = Some(InFlightRead::Chunk {
                completion: c,
                pre_size: buffer_size_before_read,
            });
            // Loop to take the resume branch — covers both synchronous and
            // asynchronous completion paths.
        }
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
    Frame(ParsedFrame),
    /// True end-of-file: not enough bytes remain to form a complete frame.
    Eof,
    /// An invalid frame was encountered (bad magic, CRC mismatch, structural error).
    /// Handled the same as EOF (stop scanning, keep previously validated frames),
    /// but semantically distinct: the data exists but is not a valid frame.
    /// `last_valid_offset` is set to the start of the invalid frame before returning this.
    InvalidFrame,
}

#[cfg_attr(test, derive(Debug))]
pub struct ParsedFrame {
    ops: Vec<ParsedOp>,
    pub portable_changes: Vec<u8>,
    pub extension_record_count: u32,
    pub frame_flags: u32,
    pub commit_ts: u64,
    pub end_offset: usize,
}

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) enum ParsedOp {
    UpsertTable {
        table_id: MVTableId,
        rowid: RowID,
        record_bytes: crate::ValueBlob,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteTable {
        rowid: RowID,
        record_bytes: crate::ValueBlob,
        pk_record_bytes: crate::ValueBlob,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpsertIndex {
        table_id: MVTableId,
        payload: crate::ValueBlob,
        commit_ts: u64,
        btree_resident: bool,
    },
    DeleteIndex {
        table_id: MVTableId,
        payload: crate::ValueBlob,
        commit_ts: u64,
        btree_resident: bool,
    },
    UpdateHeader {
        header: DatabaseHeader,
        commit_ts: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IndexOpKind {
    Upsert,
    Delete,
}

#[cfg(test)]
#[path = "../../tests/unit/mvcc/persistent_storage/logical_log/tests.rs"]
mod tests;
