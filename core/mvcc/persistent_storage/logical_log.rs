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
use crate::{
    alloc::{ConcurrentAllocator, TursoAllocator},
    io::{CompletionGroup, ReadComplete},
    io_yield_one,
    mvcc::database::{LogRecord, MVTableId, Row, RowID, RowKey, RowVersion, SortableIndexKey},
    return_if_io,
    storage::sqlite3_ondisk::{
        read_varint, read_varint_partial, varint_len, write_varint_to_vec, DatabaseHeader,
    },
    types::{IOCompletions, IOResult, IndexInfo},
    util::IOExt as _,
    Buffer, Completion, CompletionError, LimboError, Result,
};

use crate::storage::encryption::EncryptionContext;
use crate::File;

/// Logical log size in bytes at which a committing transaction will trigger a checkpoint.
/// Default to the size of 1000 SQLite WAL frames; disable by setting a negative value.
pub const DEFAULT_LOG_CHECKPOINT_THRESHOLD: i64 = 4120 * 1000;

/// Optional callback invoked after serialization with shared ownership of the
/// serialized frame bytes and the running CRC, before the disk write.
pub type OnSerializationComplete<'a> =
    Option<&'a dyn Fn(SharedBufferData, u32) -> crate::Result<()>>;

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

pub struct LogicalLog {
    pub file: Arc<dyn File>,
    io: Arc<dyn crate::IO>,
    pub offset: u64,
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
    max_appended_commit_ts: u64,
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

        #[cfg(feature = "conn_raw_api")]
        let has_portable_changes = tx.portable_changes_required || !tx.portable_changes.is_empty();
        #[cfg(not(feature = "conn_raw_api"))]
        let has_portable_changes = false;
        #[cfg(feature = "conn_raw_api")]
        let portable_changes_enabled = tx.portable_changes_enabled || has_portable_changes;
        #[cfg(not(feature = "conn_raw_api"))]
        let portable_changes_enabled = false;

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
            tx.buf.splice(
                LOG_RECORD_PREFIX_SIZE..LOG_RECORD_PREFIX_SIZE,
                [0u8; TX_EXT_HEADER_SIZE - TX_HEADER_SIZE],
            );
        }

        let tx_header_size = if has_portable_changes {
            TX_EXT_HEADER_SIZE
        } else {
            TX_HEADER_SIZE
        };
        let frame_payload_start = LOG_HDR_SIZE + tx_header_size;

        #[cfg(feature = "conn_raw_api")]
        let extension_block = if !has_portable_changes {
            Vec::new()
        } else {
            let encryption_overhead = self
                .encryption_ctx
                .as_ref()
                .map(|enc_ctx| (enc_ctx.tag_size(), enc_ctx.nonce_size()));
            let portable_changes = encode_portable_change_payload_with_stable_end_offset(
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
            encode_extension_record(EXTENSION_TYPE_PORTABLE_CHANGES, 0, &portable_changes)?
        };
        #[cfg(not(feature = "conn_raw_api"))]
        let extension_block = Vec::new();

        let extension_size = u64::try_from(extension_block.len()).map_err(|_| {
            LimboError::InternalError("Logical log extension size exceeds u64".to_string())
        })?;
        if !extension_block.is_empty() {
            tx.buf
                .splice(frame_payload_start..frame_payload_start, extension_block);
        }
        let plaintext_size = tx.buf.len() - frame_payload_start;
        let plaintext_size_u64 = plaintext_size as u64;

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
            let on_disk_payload_size = encrypted_payload_blob_size(
                plaintext_size,
                self.encrypted_payload_chunk_size,
                enc_ctx.tag_size(),
                enc_ctx.nonce_size(),
            )?;
            let total = frame_payload_start + on_disk_payload_size + TX_TRAILER_SIZE;
            // Move the plaintext out (`split_off` returns the tail past the
            // framing prefix; `tx.buf` is left with just the header prefix
            // to grow back into with encrypted chunks).
            let plaintext = tx.buf.split_off(frame_payload_start);
            debug_assert_eq!(plaintext.len(), plaintext_size);
            tx.buf.reserve(total - tx.buf.len());

            let chunk_count =
                encrypted_payload_chunk_count(plaintext_size, self.encrypted_payload_chunk_size);
            let payload_start = tx.buf.len();
            for (chunk_index, plaintext_chunk) in plaintext
                .chunks(self.encrypted_payload_chunk_size)
                .enumerate()
            {
                let is_last_chunk = chunk_index + 1 == chunk_count;
                let aad = build_encrypted_chunk_aad(
                    salt,
                    is_last_chunk.then_some(plaintext_size_u64),
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
                tx.buf.extend_from_slice(&ciphertext);
                tx.buf.extend_from_slice(&nonce);
            }
            turso_assert!(
                tx.buf.len() - payload_start == on_disk_payload_size,
                "encrypted on-disk payload size mismatch"
            );
            // `plaintext` is dropped here, freeing its allocation before pwrite.
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
        tx.buf.extend_from_slice(&crc.to_le_bytes());
        tx.buf.extend_from_slice(&END_MAGIC.to_le_bytes());

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
            cb(shared.clone(), crc)?;
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

        Ok(Some(self.write_header(upgraded_header)?))
    }

    /// Writes a transaction to the log but does NOT advance the writer offset.
    /// Returns `(completion, bytes_written)`. The caller must call
    /// `advance_offset_after_success(bytes)` after confirming the commit succeeded.
    ///
    /// If `on_serialization_complete` is provided, it is called with shared
    /// ownership of the framed bytes and the running CRC after framing but
    /// before the disk write.
    pub fn log_tx_deferred_offset(
        &mut self,
        tx: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.frame_and_pwrite_tx(tx, false, on_serialization_complete)
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
    pub fn truncate(&mut self, checkpointed_through_ts: u64) -> Result<Completion> {
        if self.max_appended_commit_ts > checkpointed_through_ts {
            // Uncheckpointed frames remain — skip truncation.
            let c = Completion::new_trunc(|_| {});
            c.complete(0);
            return Ok(c);
        }
        self.truncate_to_zero()
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

        let header_c = self.write_header(header)?;
        let truncate_c = self.file.truncate(
            LOG_HDR_SIZE as u64,
            Completion::new_trunc(move |result| {
                if let Err(err) = result {
                    tracing::error!("logical_log_truncate failed: {}", err);
                }
            }),
        )?;
        self.offset = 0;

        let mut group = CompletionGroup::new(|_| {});
        group.add(&header_c);
        group.add(&truncate_c);
        Ok(group.build())
    }
}

/// Serialize one op into `buffer`.
/// Op layout: tag(1) | flags(1) | table_id(4, le i32) | payload_len(varint) | payload(variable)
pub(crate) fn serialize_op_entry(
    buffer: &mut Vec<u8>,
    row_version: &RowVersion,
    portable_extension: Option<&[u8]>,
) -> Result<()> {
    let is_delete = row_version.end().is_some();

    let mut flags = 0u8;
    if row_version.btree_resident {
        flags |= OP_FLAG_BTREE_RESIDENT;
    }
    if portable_extension.is_some_and(|extension| !extension.is_empty()) {
        flags |= OP_FLAG_PORTABLE_EXTENSION;
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

    let write_header = |buf: &mut Vec<u8>, tag: u8| {
        buf.push(tag);
        buf.push(flags);
        buf.extend_from_slice(&table_id_i32.to_le_bytes());
    };

    match (&row_version.row.id.row_id, is_delete) {
        (&RowKey::Int(rowid), false) => {
            write_header(buffer, OP_UPSERT_TABLE);
            let record_bytes = row_version.row.payload();
            let rowid_u64 = rowid as u64;
            let rowid_len = varint_len(rowid_u64);
            let payload_len = rowid_len + record_bytes.len();
            write_varint_to_vec(payload_len as u64, buffer);
            write_varint_to_vec(rowid_u64, buffer);
            buffer.extend_from_slice(record_bytes);
        }
        (&RowKey::Int(rowid), true) => {
            write_header(buffer, OP_DELETE_TABLE);
            let rowid_u64 = rowid as u64;
            let rowid_len = varint_len(rowid_u64);
            write_varint_to_vec(rowid_len as u64, buffer);
            write_varint_to_vec(rowid_u64, buffer);
        }
        (RowKey::Record(_), is_delete) => {
            write_header(
                buffer,
                if is_delete {
                    OP_DELETE_INDEX
                } else {
                    OP_UPSERT_INDEX
                },
            );
            let key_bytes = row_version.row.payload();
            write_varint_to_vec(key_bytes.len() as u64, buffer);
            buffer.extend_from_slice(key_bytes);
        }
    }

    if let Some(portable_extension) =
        portable_extension.filter(|portable_extension| !portable_extension.is_empty())
    {
        write_varint_to_vec(portable_extension.len() as u64, buffer);
        buffer.extend_from_slice(portable_extension);
    }

    Ok(())
}

/// Serializes one logical-log operation for the serialization benchmark.
#[cfg(feature = "bench")]
pub fn benchmark_serialize_op_entry(
    buffer: &mut Vec<u8>,
    row_version: &RowVersion,
    portable_extension: Option<&[u8]>,
) -> Result<()> {
    serialize_op_entry(buffer, row_version, portable_extension)
}

pub(crate) fn serialize_header_entry(buffer: &mut Vec<u8>, header: &DatabaseHeader) {
    // Header op uses tag-only addressing (table_id=0, flags=0) and fixed payload length.
    buffer.push(OP_UPDATE_HEADER);
    buffer.push(0);
    buffer.extend_from_slice(&0i32.to_le_bytes());
    write_varint_to_vec(DatabaseHeader::SIZE as u64, buffer);
    buffer.extend_from_slice(bytemuck::bytes_of(header));
}

fn write_proto_varint(mut value: u64, buffer: &mut Vec<u8>) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

fn write_proto_key(field: u64, wire_type: u64, buffer: &mut Vec<u8>) {
    write_proto_varint((field << 3) | wire_type, buffer);
}

fn write_proto_sint64(field: u64, value: i64, buffer: &mut Vec<u8>) {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_proto_key(field, 0, buffer);
    write_proto_varint(zigzag, buffer);
}

fn write_proto_bytes(field: u64, value: &[u8], buffer: &mut Vec<u8>) {
    write_proto_key(field, 2, buffer);
    write_proto_varint(value.len() as u64, buffer);
    buffer.extend_from_slice(value);
}

pub(crate) fn encode_delete_portable_extension(
    identity_record: Option<&[u8]>,
    pk_record: Option<&[u8]>,
    rowid: Option<i64>,
) -> Vec<u8> {
    let mut extension = Vec::new();
    if let Some(identity_record) = identity_record.filter(|record| !record.is_empty()) {
        write_proto_bytes(
            OP_EXT_FIELD_DELETE_IDENTITY_RECORD,
            identity_record,
            &mut extension,
        );
    }
    if let Some(pk_record) = pk_record.filter(|record| !record.is_empty()) {
        write_proto_bytes(OP_EXT_FIELD_DELETE_PK_RECORD, pk_record, &mut extension);
    }
    if let Some(rowid) = rowid {
        write_proto_sint64(OP_EXT_FIELD_DELETE_ROWID, rowid, &mut extension);
    }
    extension
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

fn proto_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

fn encode_portable_change_payload(
    end_offset: u64,
    commit_ts: u64,
    encoded_metadata: &[u8],
) -> Vec<u8> {
    let body_len =
        2 + proto_varint_len(end_offset) + proto_varint_len(commit_ts) + encoded_metadata.len();
    let mut out = Vec::with_capacity(proto_varint_len(body_len as u64) + body_len);
    write_proto_varint(body_len as u64, &mut out);
    // PortableLogicalTxn.end_offset, field 1, varint.
    write_proto_varint(1 << 3, &mut out);
    write_proto_varint(end_offset, &mut out);
    // PortableLogicalTxn.commit_ts, field 2, varint.
    write_proto_varint(2 << 3, &mut out);
    write_proto_varint(commit_ts, &mut out);
    out.extend_from_slice(encoded_metadata);
    out
}

/// Wraps commit-built logical op messages in one length-delimited
/// portable MVCC logical transaction payload and iterates until the embedded
/// `end_offset` matches the final frame size.
///
/// `end_offset` is part of the raw-log replay cursor, but its varint width can
/// change the payload length. The fixed-point loop converges after the varint
/// width stops changing.
struct PortableEndOffsetCtx {
    write_offset: u64,
    includes_log_header: bool,
    tx_header_size: usize,
    recovery_payload_size: usize,
    encrypted_payload_chunk_size: usize,
    encryption_overhead: Option<(usize, usize)>,
}

fn encode_portable_change_payload_with_stable_end_offset(
    ctx: PortableEndOffsetCtx,
    tx_timestamp: u64,
    portable_changes: &[u8],
) -> Result<Vec<u8>> {
    let frame_end_offset = |portable_payload_len: usize| -> Result<u64> {
        let extension_size = EXTENSION_RECORD_HEADER_SIZE
            .checked_add(portable_payload_len)
            .ok_or_else(|| {
                LimboError::InternalError("portable logical extension size overflow".to_string())
            })?;
        let plaintext_size = ctx
            .recovery_payload_size
            .checked_add(extension_size)
            .ok_or_else(|| {
                LimboError::InternalError("portable logical plaintext size overflow".to_string())
            })?;
        let body_size = if let Some((tag_size, nonce_size)) = ctx.encryption_overhead {
            encrypted_payload_blob_size(
                plaintext_size,
                ctx.encrypted_payload_chunk_size,
                tag_size,
                nonce_size,
            )?
        } else {
            plaintext_size
        };
        let prefix_size = if ctx.includes_log_header {
            LOG_HDR_SIZE
        } else {
            0
        };
        let frame_bytes = prefix_size
            .checked_add(ctx.tx_header_size)
            .and_then(|value| value.checked_add(body_size))
            .and_then(|value| value.checked_add(TX_TRAILER_SIZE))
            .ok_or_else(|| {
                LimboError::InternalError("portable logical frame size overflow".to_string())
            })?;
        ctx.write_offset
            .checked_add(frame_bytes as u64)
            .ok_or_else(|| {
                LimboError::InternalError("portable logical frame offset overflow".to_string())
            })
    };

    let mut end_offset = frame_end_offset(0)?;
    loop {
        let payload = encode_portable_change_payload(end_offset, tx_timestamp, portable_changes);
        let next_end_offset = frame_end_offset(payload.len())?;
        if next_end_offset == end_offset {
            return Ok(payload);
        }
        end_offset = next_end_offset;
    }
}

fn encode_extension_record(
    extension_type: u16,
    extension_flags: u16,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        LimboError::InternalError("Logical log extension record exceeds u32".to_string())
    })?;
    let mut record = Vec::with_capacity(EXTENSION_RECORD_HEADER_SIZE + payload.len());
    record.extend_from_slice(&extension_type.to_le_bytes());
    record.extend_from_slice(&extension_flags.to_le_bytes());
    record.extend_from_slice(&payload_len.to_le_bytes());
    record.extend_from_slice(payload);
    Ok(record)
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

    pub(crate) fn try_read_header_nonblock(&mut self) -> Result<IOResult<HeaderReadResult>> {
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
            Err(err) => Err(err),
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
    pub(crate) fn next_frame(&mut self) -> Result<IOResult<Option<Vec<ParsedOp>>>> {
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
    pub fn next_portable_change_frame(&mut self) -> Result<IOResult<Option<PortableChangeFrame>>> {
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
    pub fn next_portable_changes(&mut self) -> Result<IOResult<Option<PortableChangeFrame>>> {
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
    ) -> Result<IOResult<EncryptedChunkReadResult>> {
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
            )));
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
    ) -> Result<IOResult<PayloadParseResult>> {
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
    ) -> Result<IOResult<ReadEncryptedResult>> {
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
            )));
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
    fn parse_streaming_payload(&mut self) -> Result<IOResult<PayloadOutcome>> {
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
                    )));
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
                    if rowid_len > payload.len() {
                        return Err(LimboError::Corrupt(format!(
                            "delete op rowid varint len {rowid_len} > payload len {}",
                            payload.len()
                        )));
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
    fn parse_next_transaction(&mut self) -> Result<IOResult<ParseResult>> {
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
                        Err(e) => return Err(e),
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
                    Err(LimboError::Corrupt(msg)) => {
                        tracing::warn!("corrupt payload: {msg}");
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
    fn parse_frame_header(&mut self) -> Result<IOResult<HeaderParseOutcome>> {
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
    fn parse_payload_phase(&mut self) -> Result<IOResult<PayloadOutcome>> {
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
    fn abort_frame_eof(&mut self) -> Result<IOResult<ParseResult>> {
        self.frame_in_progress = None;
        Ok(IOResult::Done(ParseResult::Eof))
    }

    /// The in-progress frame is structurally invalid (bad magic/flags/CRC/op).
    /// Set `last_valid_offset` to the captured frame start so the writer
    /// overwrites the torn frame on the next append, and drop the frame without
    /// advancing the chain.
    fn invalidate_frame(&mut self) -> Result<IOResult<ParseResult>> {
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
    fn commit_frame(&mut self) -> Result<IOResult<ParseResult>> {
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
    ) -> Result<IOResult<Option<u32>>> {
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

    fn parse_next_portable_changes_frame(&mut self) -> Result<IOResult<ParseResult>> {
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
            Err(e) => return Err(e),
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
                Err(e) => return Err(e),
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
                            Err(e) => return Err(e),
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

    fn try_consume_bytes(&mut self, amount: usize) -> Result<IOResult<Option<crate::ValueBlob>>> {
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

    fn try_consume_fixed<const N: usize>(&mut self) -> Result<IOResult<Option<[u8; N]>>> {
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

    fn try_consume_u8(&mut self) -> Result<IOResult<Option<u8>>> {
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
    fn consume_varint_bytes(&mut self) -> Result<IOResult<Option<(u64, [u8; 9], usize)>>> {
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
            return Err(LimboError::Corrupt("Invalid varint".to_string()));
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
    fn read_exact_at(&mut self, pos: u64, len: usize) -> Result<IOResult<Vec<u8>>> {
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
                    )));
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
    pub fn read_more_data(&mut self, need: usize) -> Result<IOResult<()>> {
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
                    )));
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
mod tests {
    use crate::types::IOResult;
    use crate::util::IOExt as _;
    use std::cell::RefCell;
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
        types::{ImmutableRecord, ImmutableRecordRef, IndexInfo, Text},
        Buffer, Completion, SharedBufferData, Value, ValueRef,
    };

    use super::{
        build_encrypted_chunk_aad, encrypted_chunk_blob_size, encrypted_chunk_plaintext_len,
        encrypted_payload_blob_size, encrypted_payload_chunk_count, serialize_header_entry,
        serialize_op_entry, HeaderReadResult, LogHeader, LogicalLog, ParseResult, ParsedOp,
        StreamingLogicalLogReader, ENCRYPTED_CHUNK_AAD_SIZE, ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        END_MAGIC, EXT_FRAME_MAGIC, FRAME_MAGIC, LOG_HDR_CRC_START, LOG_HDR_RESERVED_START,
        LOG_HDR_SIZE, LOG_VERSION, LOG_VERSION_V2, TX_EXT_HEADER_SIZE, TX_HEADER_SIZE,
        TX_HEADER_SIZE_V2, TX_TRAILER_SIZE,
    };
    #[cfg(feature = "conn_raw_api")]
    use super::{EXTENSION_RECORD_HEADER_SIZE, EXTENSION_TYPE_PORTABLE_CHANGES, OP_UPSERT_TABLE};
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

        let mut tx = crate::mvcc::database::LogRecord::new(commit_ts);
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: row.clone(),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tx.push_row_version_for_test(&version);
        let c = log.log_tx(tx).unwrap();
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
            payload: crate::ValueBlob,
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
        while let Some(frame) = reader.next_frame_blocking(io).unwrap() {
            for op in frame {
                match op {
                    ParsedOp::UpsertTable {
                        rowid,
                        record_bytes,
                        commit_ts,
                        btree_resident,
                        ..
                    } => {
                        ops.push(ExpectedTableOp::Upsert {
                            rowid: rowid.row_id.to_int_or_panic(),
                            payload: record_bytes,
                            commit_ts,
                            btree_resident,
                        });
                    }
                    ParsedOp::DeleteTable {
                        rowid,
                        commit_ts,
                        btree_resident,
                        ..
                    } => {
                        ops.push(ExpectedTableOp::Delete {
                            rowid: rowid.row_id.to_int_or_panic(),
                            commit_ts,
                            btree_resident,
                        });
                    }
                    other => panic!("unexpected op: {other:?}"),
                }
            }
        }
        ops
    }

    fn read_file_range_bytes(
        file: &Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        pos: u64,
        len: usize,
    ) -> Vec<u8> {
        let buf = Arc::new(Buffer::new_temporary(len));
        let c = file
            .pread(pos, Completion::new_read(buf.clone(), |_| None))
            .unwrap();
        io.wait_for_completion(c).unwrap();
        buf.as_slice().to_vec()
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
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            }),
            row,
            btree_resident,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[row_version], None);
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    fn decode_streaming_varint(bytes: &[u8]) -> crate::Result<Option<(u64, [u8; 9], usize)>> {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("logical_log_varint_decode_tmp", OpenFlags::Create, false)
            .unwrap();
        let mut reader = StreamingLogicalLogReader::new(file, None);
        reader.buffer.write().extend_from_slice(bytes);
        io.block(|| reader.consume_varint_bytes())
    }

    /// A test `File` that DEFERS every pread and SHORT-READS at most `max_read`
    /// bytes per call, so the streaming reader actually yields mid-frame (and is
    /// re-entered) and a single op spans many reads. Owns the full log bytes;
    /// reads complete only via an explicit `step()`, letting the test drive the
    /// reader's state machine one IO at a time and observe peak buffer usage.
    /// `MemoryIO` completes preads synchronously and fully, so it cannot exercise
    /// the mid-frame yield/resume paths — this can.
    struct SlowReadFile {
        data: Vec<u8>,
        max_read: usize,
        pending: std::sync::Mutex<std::collections::VecDeque<(u64, Completion)>>,
    }

    impl SlowReadFile {
        fn new(data: Vec<u8>, max_read: usize) -> Self {
            Self {
                data,
                max_read,
                pending: std::sync::Mutex::new(std::collections::VecDeque::new()),
            }
        }

        /// Complete the oldest pending pread with a short read. Returns false if
        /// nothing is pending (a stall — the reader expected more IO).
        fn step(&self) -> bool {
            let Some((pos, c)) = self.pending.lock().unwrap().pop_front() else {
                return false;
            };
            let pos = pos as usize;
            let read = c.as_read();
            let cap = read.buf().len();
            let avail = self.data.len().saturating_sub(pos);
            let n = cap.min(self.max_read).min(avail);
            if n > 0 {
                read.buf().as_mut_slice()[..n].copy_from_slice(&self.data[pos..pos + n]);
            }
            c.complete(n as i32);
            true
        }
    }

    impl crate::File for SlowReadFile {
        fn lock_file(&self, _exclusive: bool) -> crate::Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> crate::Result<()> {
            Ok(())
        }
        fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
            self.pending.lock().unwrap().push_back((pos, c.clone()));
            Ok(c)
        }
        fn pwrite(
            &self,
            _pos: u64,
            _buffer: Arc<Buffer>,
            _c: Completion,
        ) -> crate::Result<Completion> {
            unimplemented!("SlowReadFile is read-only")
        }
        fn sync(
            &self,
            _c: Completion,
            _sync_type: crate::io::FileSyncType,
        ) -> crate::Result<Completion> {
            unimplemented!("SlowReadFile is read-only")
        }
        fn size(&self) -> crate::Result<u64> {
            Ok(self.data.len() as u64)
        }
        fn truncate(&self, _len: u64, _c: Completion) -> crate::Result<Completion> {
            unimplemented!("SlowReadFile is read-only")
        }
    }

    /// Read an entire (synchronous) file into a `Vec`.
    fn read_file_to_vec(file: &Arc<dyn crate::File>) -> Vec<u8> {
        let size = file.size().unwrap() as usize;
        let out = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = out.clone();
        let buf = Arc::new(Buffer::new_temporary(size));
        let c = Completion::new_read(buf, move |res| {
            if let Ok((b, n)) = res {
                sink.lock()
                    .unwrap()
                    .extend_from_slice(&b.as_slice()[..n as usize]);
            }
            None
        });
        let _completion = file.pread(0, c).unwrap();
        let bytes = out.lock().unwrap().clone();
        assert_eq!(bytes.len(), size, "expected a synchronous full read");
        bytes
    }

    fn table_row_version(
        table_id: MVTableId,
        rowid: i64,
        commit_ts: u64,
        data: &str,
    ) -> crate::mvcc::database::RowVersion {
        crate::mvcc::database::RowVersion {
            id: commit_ts,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: generate_simple_string_row(table_id, rowid, data),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    /// Recover all ops through `SlowReadFile`, driving the reader one deferred IO
    /// at a time. Returns the recovered ops and the peak `buffer` length observed
    /// across the whole recovery.
    fn recover_with_forced_yields(data: Vec<u8>, max_read: usize) -> (Vec<ParsedOp>, usize) {
        recover_with_forced_yields_inner(data, max_read, None)
    }

    fn recover_with_forced_yields_inner(
        data: Vec<u8>,
        max_read: usize,
        encryption: Option<(crate::storage::encryption::EncryptionContext, usize)>,
    ) -> (Vec<ParsedOp>, usize) {
        let slow = Arc::new(SlowReadFile::new(data, max_read));
        let file: Arc<dyn crate::File> = slow.clone();
        let mut reader = match encryption {
            Some((ctx, chunk_size)) => {
                StreamingLogicalLogReader::new_with_payload_chunk_size(file, Some(ctx), chunk_size)
            }
            None => StreamingLogicalLogReader::new(file, None),
        };
        let mut peak = 0usize;

        // Header (read in one shot; max_read must exceed LOG_HDR_SIZE).
        loop {
            match reader.try_read_header_nonblock().unwrap() {
                IOResult::Done(HeaderReadResult::Valid(_)) => break,
                IOResult::Done(other) => panic!("unexpected header result: {other:?}"),
                IOResult::IO(_) => assert!(slow.step(), "stalled reading header"),
            }
            peak = peak.max(reader.buffer.read().len());
        }

        // Frames.
        let mut ops = Vec::new();
        loop {
            let result = reader.next_frame().unwrap();
            peak = peak.max(reader.buffer.read().len());
            match result {
                IOResult::Done(Some(frame_ops)) => ops.extend(frame_ops),
                IOResult::Done(None) => break,
                IOResult::IO(_) => {
                    assert!(slow.step(), "stalled reading frame");
                    peak = peak.max(reader.buffer.read().len());
                }
            }
        }
        (ops, peak)
    }

    /// What this test checks: streaming recovery is correctly re-entrant when IO
    /// yields at every read boundary, and the read buffer compacts per-op so a
    /// large multi-op frame is not forced wholesale into memory.
    /// Why this matters: recovery runs on a cooperative event loop, so a frame
    /// must resume identically across yields; and a huge transaction must not
    /// blow up memory during replay (the buffer is bounded to ~one op, not the
    /// whole frame).
    #[test]
    fn test_logical_log_streaming_recovery_forced_yields_bounded_memory() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("logical_log_forced_yields", OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        let table_id: MVTableId = (-100).into();

        // A small frame, then one large frame with many sizable ops, then a
        // multi-op small frame. The large frame is what would dominate memory if
        // the whole frame had to stay buffered for re-parse.
        let big_payload = "x".repeat(3000);
        let frames: Vec<Vec<crate::mvcc::database::RowVersion>> = vec![
            vec![table_row_version(table_id, 1, 10, "first")],
            (0..40)
                .map(|i| table_row_version(table_id, 100 + i as i64, 20, &big_payload))
                .collect(),
            vec![
                table_row_version(table_id, 2, 30, "a"),
                table_row_version(table_id, 3, 30, "b"),
            ],
        ];
        for (idx, rows) in frames.iter().enumerate() {
            let commit_ts = (idx as u64 + 1) * 10;
            let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, rows, None);
            let c = log.log_tx(tx).unwrap();
            io.wait_for_completion(c).unwrap();
        }

        // Expected ops via the straightforward synchronous (full-read) path.
        let mut expected_reader = StreamingLogicalLogReader::new(file.clone(), None);
        expected_reader.read_header(&io).unwrap();
        let mut expected = Vec::new();
        while let Some(frame) = expected_reader.next_frame_blocking(&io).unwrap() {
            expected.extend(frame);
        }
        assert_eq!(expected.len(), 1 + 40 + 2);

        // Recover the same bytes with deferred, short (64-byte) reads so every
        // `try_consume_*` yields and a single op spans dozens of reads.
        let bytes = read_file_to_vec(&file);
        let (recovered, peak) = recover_with_forced_yields(bytes, 64);

        assert_eq!(
            recovered, expected,
            "forced-yield recovery must match the synchronous path exactly"
        );

        // The large frame is ~40 * ~3KB ≈ 120KB. With per-op checkpointing the
        // buffer holds at most ~one op plus a read chunk; the whole-frame rewind
        // model would keep the entire frame resident. Assert a bound far below
        // the frame size.
        assert!(
            peak < 16 * 1024,
            "peak buffer {peak} bytes should be bounded to ~one op, not the whole frame"
        );
    }

    /// What this test checks: encrypted recovery is correctly re-entrant when IO
    /// yields at every read boundary. The encrypted payload is parsed wholesale
    /// (rewind-and-rebuild from the payload start), so this exercises that the
    /// post-header checkpoint + payload-start resume decrypts identically across
    /// yields. (Memory is intentionally not bounded for encrypted frames — the
    /// plaintext is accumulated contiguously by design — so only correctness is
    /// asserted here.)
    /// Why this matters: the refactor changed the encrypted resume point from the
    /// frame start to the payload start; this guards that change under yields,
    /// which `MemoryIO`-based tests cannot reach.
    #[test]
    fn test_encrypted_log_recovery_forced_yields() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let enc_ctx = test_enc_ctx();
        const TEST_CHUNK_SIZE: usize = 2 * 1024;

        // Two transactions; the first has a multi-op payload large enough to span
        // several encrypted chunks, the second is small.
        let big = "z".repeat(2500);
        let tx0 = crate::mvcc::database::LogRecord::for_test(
            100,
            &[
                make_test_row_version((-2).into(), 1, &big, 100),
                make_test_row_version((-2).into(), 2, &big, 100),
                make_test_row_version((-2).into(), 3, "small", 100),
            ],
            None,
        );
        let tx1 = crate::mvcc::database::LogRecord::for_test(
            200,
            &[make_test_row_version((-2).into(), 4, "tail", 200)],
            None,
        );
        let file = write_encrypted_txs_with_chunk_size_for_test(
            &io,
            "enc-forced-yields.db-log",
            &enc_ctx,
            TEST_CHUNK_SIZE,
            vec![tx0, tx1],
        );

        let expected: Vec<ParsedOp> = parse_all_encrypted_tx_ops_with_chunk_size_for_test(
            file.clone(),
            &io,
            &enc_ctx,
            TEST_CHUNK_SIZE,
        )
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
        assert_eq!(expected.len(), 4);

        let bytes = read_file_to_vec(&file);
        let (recovered, _peak) =
            recover_with_forced_yields_inner(bytes, 64, Some((enc_ctx.clone(), TEST_CHUNK_SIZE)));

        assert_eq!(
            recovered, expected,
            "encrypted forced-yield recovery must match the synchronous path exactly"
        );
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
            )
            .unwrap();
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1000)),
                        data.as_blob(),
                        5,
                    )
                    .unwrap(),
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
        let record = ImmutableRecordRef::from_bin_record(row.payload());
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
            )
            .unwrap();
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1000)),
                        data.as_blob(),
                        5,
                    )
                    .unwrap(),
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
            let record = ImmutableRecordRef::from_bin_record(row.payload());
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
            )
            .unwrap();
            mvcc_store
                .insert(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-1).into(), RowKey::Int(1000)),
                        data.as_blob(),
                        5,
                    )
                    .unwrap(),
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
            let record = ImmutableRecordRef::from_bin_record(row.payload());
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
        let index_info = Arc::new(IndexInfo::new_from_index(index).unwrap());

        // Verify table rows can be read
        let tx = mvcc_store.begin_tx(pager).unwrap();
        for (row_id, expected_data) in [(1, "foo"), (2, "bar"), (3, "baz")] {
            let row = mvcc_store
                .read(tx, &RowID::new(table_id, RowKey::Int(row_id)))
                .unwrap()
                .expect("Table row should exist");
            let record = ImmutableRecordRef::from_bin_record(row.payload());
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
            )
            .unwrap();
            let sortable_key = SortableIndexKey::new_from_payload_in(
                &key_record,
                index_info.clone(),
                crate::alloc::TursoAllocator,
            )
            .unwrap();
            let index_rowid = RowID::new(index_id, RowKey::Record(Arc::new(sortable_key)));

            // Use read_from_table_or_index to read the index row
            // This verifies that index rows were properly serialized and deserialized from the logical log
            let index_row_opt = mvcc_store
                .read_from_table_or_index(tx, &index_rowid, Some(index_id))
                .unwrap_or_else(|e| {
                    panic!("Failed to read index row for ({}, {}): {:?}. Index ID: {:?}, root_page: {}",
                           data_value, row_id, e, index_id, index.root_page)
                });

            let Some(index_row) = index_row_opt else {
                panic!(
                    "Index row for ({data_value}, {row_id}) not found after recovery. Index rows should be in the logical log."
                );
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

        let mut tx1 = crate::mvcc::database::LogRecord::new(10);
        tx1.push_row_version_for_test(&crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(10),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: row.clone(),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        });
        let c = log.log_tx(tx1).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut tx2 = crate::mvcc::database::LogRecord::new(20);
        tx2.push_row_version_for_test(&crate::mvcc::database::RowVersion {
            id: 2,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(20),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        });
        let c = log.log_tx(tx2).unwrap();
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
                match reader.next_frame_blocking(&io) {
                    Ok(Some(frame)) => {
                        for op in frame {
                            match op {
                                ParsedOp::UpsertTable { .. } => seen += 1,
                                other => panic!("unexpected op: {other:?}"),
                            }
                        }
                    }
                    Ok(None) => break,
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
                payload: crate::types::value_blob_from_slice(
                    generate_simple_string_row((-2).into(), 1, "a").payload(),
                )
                .expect(crate::alloc::ALLOC_ERR_MSG),
                commit_ts: 1,
                btree_resident: false,
            }
        );
        assert_eq!(
            read_back[1],
            ExpectedTableOp::Upsert {
                rowid: 2,
                payload: crate::types::value_blob_from_slice(
                    generate_simple_string_row((-2).into(), 2, "b").payload(),
                )
                .expect(crate::alloc::ALLOC_ERR_MSG),
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
        let frame = reader
            .next_frame_blocking(&io)
            .unwrap()
            .expect("expected one frame");
        assert_eq!(frame.len(), 1);
        match &frame[0] {
            ParsedOp::UpsertTable { rowid, .. } => {
                assert_eq!(rowid.table_id, table_id);
                assert_eq!(rowid.row_id.to_int_or_panic(), 7);
            }
            other => panic!("unexpected op: {other:?}"),
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
        let tx3 = crate::mvcc::database::LogRecord::for_test(
            3,
            &[crate::mvcc::database::RowVersion {
                id: 3,
                begin: crate::mvcc::database::PackedTs::pack(Some(
                    crate::mvcc::database::TxTimestampOrID::Timestamp(3),
                )),
                end: crate::mvcc::database::PackedTs::pack(None),
                row: row3,
                btree_resident: false,
                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
            }],
            None,
        );
        let (c, bytes_written) = log.log_tx_deferred_offset(tx3, None).unwrap();
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

    #[test]
    fn test_on_serialization_complete_gets_shared_write_bytes() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "serialization-callback-shared.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);
        let captured = RefCell::new(Vec::<(SharedBufferData, u32)>::new());
        let callback = |bytes: SharedBufferData, crc: u32| {
            captured.borrow_mut().push((bytes, crc));
            Ok(())
        };

        let tx1 = crate::mvcc::database::LogRecord::for_test(
            1,
            &[crate::mvcc::database::RowVersion {
                id: 1,
                begin: crate::mvcc::database::PackedTs::pack(Some(
                    crate::mvcc::database::TxTimestampOrID::Timestamp(1),
                )),
                end: crate::mvcc::database::PackedTs::pack(None),
                row: generate_simple_string_row((-2).into(), 1, "first"),
                btree_resident: false,
                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
            }],
            None,
        );
        let (c, first_len) = log.log_tx_deferred_offset(tx1, Some(&callback)).unwrap();
        io.wait_for_completion(c).unwrap();
        log.advance_offset_after_success(first_len);

        let tx2 = crate::mvcc::database::LogRecord::for_test(
            2,
            &[crate::mvcc::database::RowVersion {
                id: 2,
                begin: crate::mvcc::database::PackedTs::pack(Some(
                    crate::mvcc::database::TxTimestampOrID::Timestamp(2),
                )),
                end: crate::mvcc::database::PackedTs::pack(None),
                row: generate_simple_string_row((-2).into(), 2, "second"),
                btree_resident: false,
                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
            }],
            None,
        );
        let (c, second_len) = log.log_tx_deferred_offset(tx2, Some(&callback)).unwrap();
        io.wait_for_completion(c).unwrap();
        log.advance_offset_after_success(second_len);

        let captured = captured.borrow();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0.len(), first_len as usize);
        assert_eq!(captured[1].0.len(), second_len as usize);
        assert!(matches!(&captured[0].0, SharedBufferData::Full(_)));
        assert!(matches!(&captured[1].0, SharedBufferData::View(_)));

        let first_on_disk = read_file_range_bytes(&file, &io, 0, first_len as usize);
        let second_on_disk = read_file_range_bytes(&file, &io, first_len, second_len as usize);
        assert_eq!(captured[0].0.as_slice(), first_on_disk.as_slice());
        assert_eq!(captured[1].0.as_slice(), second_on_disk.as_slice());
        assert_eq!(
            captured[0].1,
            u32::from_le_bytes(
                captured[0].0.as_slice()[captured[0].0.len() - TX_TRAILER_SIZE
                    ..captured[0].0.len() - TX_TRAILER_SIZE + 4]
                    .try_into()
                    .unwrap()
            )
        );
        assert_eq!(
            captured[1].1,
            u32::from_le_bytes(
                captured[1].0.as_slice()[captured[1].0.len() - TX_TRAILER_SIZE
                    ..captured[1].0.len() - TX_TRAILER_SIZE + 4]
                    .try_into()
                    .unwrap()
            )
        );
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

        let mut tx = crate::mvcc::database::LogRecord::new(123);
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(123),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tx.push_row_version_for_test(&version);
        let c = log.log_tx(tx).unwrap();
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
                payload: crate::types::value_blob_from_slice(
                    generate_simple_string_row((-2).into(), 1, "first").payload(),
                )
                .expect(crate::alloc::ALLOC_ERR_MSG),
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
                payload: crate::types::value_blob_from_slice(
                    generate_simple_string_row((-2).into(), 1, "a").payload(),
                )
                .expect(crate::alloc::ALLOC_ERR_MSG),
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
        let tx = crate::mvcc::database::LogRecord::for_test(77, &[], None);
        let c = log.log_tx(tx).unwrap();
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
        let res = reader.next_frame_blocking(&io);
        assert!(res.unwrap().is_none());
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
        let tx = crate::mvcc::database::LogRecord::for_test(200, &[], None);
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();

        // Frame 2: header-only tx. DatabaseHeader::default() has the SQLite magic that passes
        // the reader's magic validation check.
        let commit_ts = 201u64;
        let db_header = DatabaseHeader::default();
        let header_tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[], Some(db_header));
        let c = log.log_tx(header_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();

        // The reader skips the empty frame and returns the UpdateHeader from frame 2.
        let frame = reader
            .next_frame_blocking(&io)
            .unwrap()
            .expect("expected UpdateHeader frame after empty tx");
        assert_eq!(frame.len(), 1);
        match &frame[0] {
            ParsedOp::UpdateHeader {
                header: recovered,
                commit_ts: recovered_ts,
            } => {
                assert_eq!(*recovered_ts, commit_ts);
                assert_eq!(recovered.magic, db_header.magic);
            }
            other => panic!("expected UpdateHeader, got {other:?}"),
        }

        // Nothing left after frame 2.
        assert!(reader.next_frame_blocking(&io).unwrap().is_none());
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
        let mut tx = crate::mvcc::database::LogRecord::new(300);
        tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(300),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: generate_simple_string_row((-2).into(), 42, "flip"),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        });
        let c = log.log_tx(tx).unwrap();
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
                let res = reader.next_frame_blocking(&io);
                match res {
                    Err(_) | Ok(None) => {}
                    Ok(Some(frame)) => {
                        panic!("bit flip at offset={i}, bit={bit} produced valid frame: {frame:?}")
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
            let mut tx = crate::mvcc::database::LogRecord::new(1_000 + tx_i);
            let op_count = (rng.next_u64() % 4) as usize;
            for _ in 0..op_count {
                let rowid = (rng.next_u64() % 64) as i64 + 1;
                let btree_resident = (rng.next_u32() & 1) == 1;
                let is_delete = (rng.next_u32() & 1) == 1;
                if is_delete {
                    tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
                        id: 0,
                        begin: crate::mvcc::database::PackedTs::pack(None),
                        end: crate::mvcc::database::PackedTs::pack(Some(
                            crate::mvcc::database::TxTimestampOrID::Timestamp(tx.tx_timestamp),
                        )),
                        row: Row::new_table_row(
                            RowID::new((-2).into(), RowKey::Int(rowid)),
                            &[],
                            0,
                        )
                        .unwrap(),
                        btree_resident,
                        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                    });
                    expected.push(ExpectedTableOp::Delete {
                        rowid,
                        commit_ts: tx.tx_timestamp,
                        btree_resident,
                    });
                } else {
                    let payload = format!("r-{tx_i}-{rowid}");
                    let row = generate_simple_string_row((-2).into(), rowid, &payload);
                    tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
                        id: 0,
                        begin: crate::mvcc::database::PackedTs::pack(Some(
                            crate::mvcc::database::TxTimestampOrID::Timestamp(tx.tx_timestamp),
                        )),
                        end: crate::mvcc::database::PackedTs::pack(None),
                        row: row.clone(),
                        btree_resident,
                        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                    });
                    expected.push(ExpectedTableOp::Upsert {
                        rowid,
                        payload: crate::types::value_blob_from_slice(row.payload())
                            .expect(crate::alloc::ALLOC_ERR_MSG),
                        commit_ts: tx.tx_timestamp,
                        btree_resident,
                    });
                }
            }
            let c = log.log_tx(tx).unwrap();
            io.wait_for_completion(c).unwrap();
        }

        // Large-payload frame: 30 rows × 200 bytes ≈ 6 KB — well above the 4096-byte internal
        // read-chunk boundary. This verifies the reader stitches together multiple pread results
        // correctly when a single frame spans chunk boundaries.
        let large_commit_ts = 1_000 + 128u64;
        let large_text: String = "x".repeat(200);
        let mut large_tx = crate::mvcc::database::LogRecord::new(large_commit_ts);
        for rowid in 1..=30i64 {
            let row = generate_simple_string_row((-3).into(), rowid, &large_text);
            expected.push(ExpectedTableOp::Upsert {
                rowid,
                payload: crate::types::value_blob_from_slice(row.payload())
                    .expect(crate::alloc::ALLOC_ERR_MSG),
                commit_ts: large_commit_ts,
                btree_resident: false,
            });
            large_tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
                id: rowid as u64,
                begin: crate::mvcc::database::PackedTs::pack(Some(
                    crate::mvcc::database::TxTimestampOrID::Timestamp(large_commit_ts),
                )),
                end: crate::mvcc::database::PackedTs::pack(None),
                row,
                btree_resident: false,
                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
            });
        }
        let c = log.log_tx(large_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let got = read_table_ops(file.clone(), &io);
        assert_eq!(got, expected);
    }

    /// What this property checks: For arbitrary event sequences, write/read round-trip preserves operation intent.
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
                begin: crate::mvcc::database::PackedTs::pack(Some(
                    crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
                )),
                end: crate::mvcc::database::PackedTs::pack(if is_delete {
                    Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
                } else {
                    None
                }),
                row: row.clone(),
                btree_resident,
                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
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
                    payload: crate::types::value_blob_from_slice(row.payload())
                        .expect(crate::alloc::ALLOC_ERR_MSG),
                    commit_ts,
                    btree_resident,
                }
            });
            let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[row_version], None);
            let Ok(c) = log.log_tx(tx) else {
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

        let mut tx = crate::mvcc::database::LogRecord::new(55);
        let mut row = generate_simple_string_row((-2).into(), 1, "foo");
        row.id.table_id = (-2).into();
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(55),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: true,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tx.push_row_version_for_test(&version);
        let c = log.log_tx(tx).unwrap();
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
        let frame = reader
            .next_frame_blocking(&io)
            .unwrap()
            .expect("expected one frame");
        assert_eq!(frame.len(), 1);
        match &frame[0] {
            ParsedOp::UpsertTable { btree_resident, .. } => {
                assert!(*btree_resident);
            }
            other => panic!("unexpected op: {other:?}"),
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

        let mut tx = crate::mvcc::database::LogRecord::new(10);
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        let version = crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(10),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tx.push_row_version_for_test(&version);
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let c = file
            .truncate(LOG_HDR_SIZE as u64, Completion::new_trunc(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        let header = reader.header().unwrap();
        assert_eq!(header.version, LOG_VERSION_V2);
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
        assert_eq!(header.version, LOG_VERSION_V2);
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
        // will be written together with the next frame. u64::MAX boundary => all
        // frames are considered checkpointed, so it truncates unconditionally.
        let c = log.truncate(u64::MAX).unwrap();
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

        match io.block(|| reader.parse_next_transaction()) {
            Ok(ParseResult::Frame(frame)) => {
                let ops = frame.ops;
                assert!(!ops.is_empty(), "expected at least one op");
            }
            Ok(ParseResult::Eof) => panic!("expected ops, got EOF"),
            Ok(ParseResult::InvalidFrame) => panic!("expected ops, got InvalidFrame"),
            Err(e) => panic!("expected ops, got error: {e:?}"),
        }
        assert!(matches!(
            io.block(|| reader.parse_next_transaction()),
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
        while let Ok(ParseResult::Frame(_)) = io.block(|| reader.parse_next_transaction()) {
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
        match io.block(|| reader.parse_next_transaction()) {
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
        match io.block(|| reader.parse_next_transaction()) {
            Ok(ParseResult::Frame(frame)) => assert!(!frame.ops.is_empty()),
            other => panic!("expected log A's frame to parse, got {other:?}"),
        }

        // The spliced frame from log B should fail CRC validation
        match io.block(|| reader.parse_next_transaction()) {
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
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
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
        )
        .unwrap();
        let sortable_key = SortableIndexKey::new_from_payload_in(
            &key_record,
            test_index_info(),
            crate::alloc::TursoAllocator,
        )
        .unwrap();
        let row_id = RowID::new(table_id, RowKey::Record(Arc::new(sortable_key)));
        let row = Row::new_index_row(row_id, 2);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    fn test_index_info() -> Arc<IndexInfo> {
        Arc::new(
            IndexInfo::new(
                [
                    crate::types::KeyInfo {
                        sort_order: turso_parser::ast::SortOrder::Asc,
                        collation: crate::translate::collate::CollationSeq::Binary,
                        nulls_order: None,
                    },
                    crate::types::KeyInfo {
                        sort_order: turso_parser::ast::SortOrder::Asc,
                        collation: crate::translate::collate::CollationSeq::Binary,
                        nulls_order: None,
                    },
                ],
                true,
                2,
                false,
            )
            .unwrap(),
        )
    }

    fn make_test_raw_table_row_version(
        table_id: MVTableId,
        rowid: i64,
        record_bytes: Vec<u8>,
        commit_ts: u64,
        is_delete: bool,
    ) -> crate::mvcc::database::RowVersion {
        let row =
            Row::new_table_row(RowID::new(table_id, RowKey::Int(rowid)), &record_bytes, 1).unwrap();
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: crate::mvcc::database::PackedTs::pack(if is_delete {
                None
            } else {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            }),
            end: crate::mvcc::database::PackedTs::pack(if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            }),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    fn make_test_raw_index_row_version(
        table_id: MVTableId,
        rowid: i64,
        payload_bytes: Vec<u8>,
        commit_ts: u64,
        is_delete: bool,
    ) -> crate::mvcc::database::RowVersion {
        let sortable_key = SortableIndexKey::new_from_payload_in(
            &payload_bytes,
            test_index_info(),
            crate::alloc::TursoAllocator,
        )
        .unwrap();
        let row_id = RowID::new(table_id, RowKey::Record(Arc::new(sortable_key)));
        let row = Row::new_index_row(row_id, 2);
        crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: crate::mvcc::database::PackedTs::pack(if is_delete {
                None
            } else {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            }),
            end: crate::mvcc::database::PackedTs::pack(if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            }),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    fn single_upsert_table_op_size_for_text_len(rowid: i64, text_len: usize) -> usize {
        let mut encoded = Vec::new();
        let value = "x".repeat(text_len);
        let row_version = make_test_row_version((-2).into(), rowid, &value, 100);
        serialize_op_entry(&mut encoded, &row_version, None).unwrap();
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
        let mut reader = StreamingLogicalLogReader::new(file, None);
        io.block(|| reader.read_exact_at(0, file_size)).unwrap()
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
        tx: crate::mvcc::database::LogRecord,
    ) {
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    fn write_first_encrypted_tx(
        file: Arc<dyn crate::File>,
        io: &Arc<dyn crate::IO>,
        enc_ctx: &crate::storage::encryption::EncryptionContext,
        tx: crate::mvcc::database::LogRecord,
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
        tx: crate::mvcc::database::LogRecord,
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
        tx: crate::mvcc::database::LogRecord,
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
        tx: crate::mvcc::database::LogRecord,
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
        txs: Vec<crate::mvcc::database::LogRecord>,
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
        let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => frame.ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert!(matches!(
            io.block(|| reader.parse_next_transaction()).unwrap(),
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
        let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => frame.ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert!(matches!(
            io.block(|| reader.parse_next_transaction()).unwrap(),
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
            match io
                .block(|| reader.parse_next_transaction())
                .map_err(|e| format!("failed to parse fuzz frame {tx_index}: {e}"))?
            {
                ParseResult::Frame(frame) => frames.push(frame.ops),
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
            record_bytes: crate::types::value_blob_from_slice(row_version.row.payload())
                .expect(crate::alloc::ALLOC_ERR_MSG),
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
        serialize_op_entry(&mut filler_buf, short_filler, None).unwrap();
        let mut short_upsert_buf = Vec::new();
        serialize_op_entry(&mut short_upsert_buf, short_upsert, None).unwrap();
        let mut long_upsert_buf = Vec::new();
        serialize_op_entry(&mut long_upsert_buf, long_upsert, None).unwrap();

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
        serialize_op_entry(&mut short_upsert_buf, &short_upsert, None).unwrap();
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

            txs.push(crate::mvcc::database::LogRecord::for_test(
                commit_ts,
                &row_versions,
                None,
            ));
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
        match io.block(|| reader.parse_next_transaction()).unwrap() {
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
        let tx = crate::mvcc::database::LogRecord::for_test(
            100,
            &[
                make_test_row_version(table_id, 1, "hello", 100),
                make_test_row_version(table_id, 2, "world", 100),
            ],
            None,
        );
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);

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
            encrypted_blob_size, expected_blob_size,
            "on-disk blob size ({encrypted_blob_size}) != expected chunked encrypted size({expected_blob_size})"
        );

        // ── Roundtrip read ──
        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => frame.ops,
            other => panic!("expected Ops, got {other:?}"),
        };
        assert_eq!(ops.len(), 2);
        assert_upsert_table_op(&ops[0], table_id, 1, &expected_hello_record_bytes, 100);
        assert_upsert_table_op(&ops[1], table_id, 2, &expected_world_record_bytes, 100);

        assert!(matches!(
            io.block(|| reader.parse_next_transaction()).unwrap(),
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
        let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);

        let file = write_single_encrypted_tx_with_chunk_size_for_test(
            &io,
            "enc-roundtrip-test-chunk-size.db-log",
            &enc_ctx,
            TEST_CHUNK_SIZE,
            tx,
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
                txs,
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
                actual_frames, expected_frames,
                "encrypted carry fuzz failed: root_seed={seed} case_index={case_index} forced_case_index={forced_case_index} include_forced_prefix={include_forced_prefix} case_seed={case_seed}"
            );
        }
    }

    #[test]
    fn test_encrypted_log_format_assumptions_are_pinned() {
        assert_eq!(LOG_VERSION_V2, 2);
        assert_eq!(LOG_VERSION, 3);
        assert_eq!(LOG_HDR_SIZE, 56);
        assert_eq!(ENCRYPTED_PAYLOAD_CHUNK_SIZE, 32 * 1024);
        assert_eq!(ENCRYPTED_CHUNK_AAD_SIZE, 32);
        assert_eq!(FRAME_MAGIC, 0x5854_564D);
        assert_eq!(EXT_FRAME_MAGIC, 0x5845_564D);
        assert_eq!(END_MAGIC, 0x4554_564D);
        assert_eq!(TX_HEADER_SIZE_V2, 24);
        assert_eq!(TX_HEADER_SIZE, 24);
        assert_eq!(TX_EXT_HEADER_SIZE, 40);
        assert_eq!(TX_TRAILER_SIZE, 8);
    }

    #[test]
    fn test_non_portable_first_write_uses_lml2_header_and_v2_frame() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "non-portable-first-write-lml2.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        let tx = crate::mvcc::database::LogRecord::for_test(
            10,
            &[make_test_row_version((-2).into(), 1, "visible", 10)],
            None,
        );
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let frame = read_file_bytes(file, &io);
        let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
        assert_eq!(header.version, LOG_VERSION_V2);
        assert_eq!(
            u32::from_le_bytes(frame[LOG_HDR_SIZE..LOG_HDR_SIZE + 4].try_into().unwrap()),
            FRAME_MAGIC
        );
    }

    #[test]
    fn test_non_portable_appends_keep_lml2_header_and_v2_frames() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("non-portable-appends-lml2.db-log", OpenFlags::Create, false)
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        for (commit_ts, rowid) in [(10, 1), (20, 2)] {
            let tx = crate::mvcc::database::LogRecord::for_test(
                commit_ts,
                &[make_test_row_version(
                    (-2).into(),
                    rowid,
                    "visible",
                    commit_ts,
                )],
                None,
            );
            let c = log.log_tx(tx).unwrap();
            io.wait_for_completion(c).unwrap();
        }

        let frame = read_file_bytes(file, &io);
        let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
        assert_eq!(header.version, LOG_VERSION_V2);
        assert_eq!(
            u32::from_le_bytes(frame[LOG_HDR_SIZE..LOG_HDR_SIZE + 4].try_into().unwrap()),
            FRAME_MAGIC
        );

        let first_payload_size = u64::from_le_bytes(
            frame[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let second_frame_start =
            LOG_HDR_SIZE + TX_HEADER_SIZE + first_payload_size + TX_TRAILER_SIZE;
        assert_eq!(
            u32::from_le_bytes(
                frame[second_frame_start..second_frame_start + 4]
                    .try_into()
                    .unwrap()
            ),
            FRAME_MAGIC
        );
    }

    #[cfg(feature = "conn_raw_api")]
    #[test]
    fn test_portable_changes_upgrade_non_empty_lml2_log_to_lml3() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "portable-after-lml2-upgrade.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        let tx = crate::mvcc::database::LogRecord::for_test(
            10,
            &[make_test_row_version((-2).into(), 1, "visible", 10)],
            None,
        );
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut portable_tx = crate::mvcc::database::LogRecord::for_test(
            20,
            &[make_test_row_version((-2).into(), 2, "visible", 20)],
            None,
        );
        portable_tx.portable_changes_enabled = true;
        portable_tx.portable_changes = vec![0x1a, 0x00];

        let c = log
            .upgrade_header_for_log_tx(&portable_tx)
            .unwrap()
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let c = log.log_tx(portable_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let frame = read_file_bytes(file, &io);
        let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
        assert_eq!(header.version, LOG_VERSION);

        let first_payload_size = u64::from_le_bytes(
            frame[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let second_frame_start =
            LOG_HDR_SIZE + TX_HEADER_SIZE + first_payload_size + TX_TRAILER_SIZE;
        assert_eq!(
            u32::from_le_bytes(
                frame[second_frame_start..second_frame_start + 4]
                    .try_into()
                    .unwrap()
            ),
            EXT_FRAME_MAGIC
        );
    }

    #[cfg(feature = "conn_raw_api")]
    #[test]
    fn test_next_portable_change_frame_returns_empty_and_nonempty_lml3_frames() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "sync-frame-empty-and-nonempty.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        let mut empty_sync_tx = crate::mvcc::database::LogRecord::for_test(
            10,
            &[make_test_row_version((-2).into(), 1, "internal", 10)],
            None,
        );
        empty_sync_tx.portable_changes_enabled = true;
        let c = log.log_tx(empty_sync_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let encoded_empty_logical_op = vec![0x1a, 0x00];
        let mut sync_tx = crate::mvcc::database::LogRecord::for_test(
            20,
            &[make_test_row_version((-2).into(), 2, "visible", 20)],
            None,
        );
        sync_tx.portable_changes = encoded_empty_logical_op;
        let c = log.log_tx(sync_tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, None);
        reader.read_header(&io).unwrap();
        assert_eq!(reader.header().unwrap().version, LOG_VERSION);
        let first = io
            .block(|| reader.next_portable_change_frame())
            .unwrap()
            .unwrap();
        assert_eq!(first.commit_ts, 10);
        assert_eq!(first.extension_record_count, 0);
        assert!(first.payload.is_empty());

        let second = io
            .block(|| reader.next_portable_change_frame())
            .unwrap()
            .unwrap();
        assert_eq!(second.commit_ts, 20);
        assert_eq!(second.extension_record_count, 1);
        assert!(!second.payload.is_empty());
        assert_eq!(second.end_offset, reader.last_valid_offset() as u64);

        assert!(io
            .block(|| reader.next_portable_change_frame())
            .unwrap()
            .is_none());
    }

    #[cfg(feature = "conn_raw_api")]
    #[test]
    fn test_portable_extension_block_precedes_recovery_payload() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file(
                "portable-extension-before-payload.db-log",
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let mut log = LogicalLog::new(file.clone(), io.clone(), None);

        let portable_metadata = vec![0x1a, 0x00];
        let mut tx = crate::mvcc::database::LogRecord::for_test(
            20,
            &[make_test_row_version((-2).into(), 2, "visible", 20)],
            None,
        );
        tx.portable_changes = portable_metadata.clone();
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();

        let frame = read_file_bytes(file, &io);
        let tx_header_start = LOG_HDR_SIZE;
        let body_start = LOG_HDR_SIZE + TX_EXT_HEADER_SIZE;
        assert_eq!(
            u32::from_le_bytes(
                frame[tx_header_start..tx_header_start + 4]
                    .try_into()
                    .unwrap()
            ),
            EXT_FRAME_MAGIC
        );
        let extension_size = u64::from_le_bytes(
            frame[tx_header_start + 24..tx_header_start + 32]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(extension_size >= EXTENSION_RECORD_HEADER_SIZE);

        let extension_type =
            u16::from_le_bytes(frame[body_start..body_start + 2].try_into().unwrap());
        assert_eq!(extension_type, EXTENSION_TYPE_PORTABLE_CHANGES);
        let extension_payload_len = u32::from_le_bytes(
            frame[body_start + 4..body_start + EXTENSION_RECORD_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        let extension_payload = &frame[body_start + EXTENSION_RECORD_HEADER_SIZE
            ..body_start + EXTENSION_RECORD_HEADER_SIZE + extension_payload_len];
        assert!(extension_payload.ends_with(&portable_metadata));

        let recovery_start = body_start + extension_size;
        assert_eq!(frame[recovery_start], OP_UPSERT_TABLE);
    }

    #[test]
    fn test_next_portable_change_frame_does_not_advance_lml2_logs() {
        init_tracing();
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("sync-frame-lml2.db-log", OpenFlags::Create, false)
            .unwrap();

        let mut header = LogHeader::new(&io);
        header.version = LOG_VERSION_V2;
        let buffer = Arc::new(Buffer::new(header.encode().to_vec()));
        let c = Completion::new_write(|_| {});
        io.wait_for_completion(file.pwrite(0, buffer, c).unwrap())
            .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, None);
        reader.read_header(&io).unwrap();
        assert_eq!(reader.last_valid_offset(), LOG_HDR_SIZE);
        assert!(io
            .block(|| reader.next_portable_change_frame())
            .unwrap()
            .is_none());
        assert_eq!(reader.last_valid_offset(), LOG_HDR_SIZE);
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
            let tx = crate::mvcc::database::LogRecord::for_test(
                100,
                &[make_test_row_version((-2).into(), 1, &value, 100)],
                None,
            );
            let file = write_single_encrypted_tx(
                &io,
                &format!("enc-layout-pinned-{payload_size}.db-log"),
                &enc_ctx,
                tx,
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
        let tx = crate::mvcc::database::LogRecord::for_test(
            444,
            &[make_test_row_version(table_id, 1, &value, 444)],
            None,
        );
        append_encrypted_tx(&mut log, &io, tx);

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
        match io.block(|| reader.parse_next_transaction()).unwrap() {
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
            let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);
            let file = write_single_encrypted_tx(
                &io,
                &format!("enc-layout-{target_op_size}.db-log"),
                &enc_ctx,
                tx,
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

        let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);
        let file = write_single_encrypted_tx(&io, "enc-cross-boundary.db-log", &enc_ctx, tx);
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
        serialize_op_entry(&mut filler_buf, &filler, None).unwrap();
        assert_eq!(filler_buf.len(), ENCRYPTED_PAYLOAD_CHUNK_SIZE - 7);

        let mut second_buf = Vec::new();
        serialize_op_entry(&mut second_buf, &second, None).unwrap();
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

        let tx = crate::mvcc::database::LogRecord::for_test(100, &[filler, second], None);
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
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
        serialize_op_entry(&mut filler_buf, &filler, None).unwrap();
        assert_eq!(filler_buf.len(), filler_payload_size);
        assert_eq!(
            filler_buf.len() + header_buf.len() - 1,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            "chunk boundary should split the header op after its first byte"
        );

        let tx = crate::mvcc::database::LogRecord::for_test(100, &[filler], Some(header));
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
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
        let tx = crate::mvcc::database::LogRecord::for_test(200, &row_versions, None);
        write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
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

        let tx = crate::mvcc::database::LogRecord::for_test(250, &[row_version], None);
        let file = write_single_encrypted_tx(&io, "enc-index-boundary.db-log", &enc_ctx, tx);

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
            let tx = crate::mvcc::database::LogRecord::for_test(
                100 + i,
                &[make_test_row_version(
                    table_id,
                    i as i64,
                    &format!("val_{i}"),
                    100 + i,
                )],
                None,
            );
            append_encrypted_tx(&mut log, &io, tx);
        }

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        for i in 0..5u64 {
            let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
                ParseResult::Frame(frame) => frame.ops,
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
            io.block(|| reader.parse_next_transaction()).unwrap(),
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
            let tx = crate::mvcc::database::LogRecord::for_test(
                100,
                &[make_test_row_version(table_id, 1, "secret", 100)],
                None,
            );
            append_encrypted_tx(&mut log, &io, tx);

            let mut reader = StreamingLogicalLogReader::new(file, Some(wrong_key_enc_ctx()));
            reader.read_header(&io).unwrap();

            match io.block(|| reader.parse_next_transaction()).unwrap() {
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
            let tx = crate::mvcc::database::LogRecord::for_test(
                100,
                &[make_test_row_version(table_id, 1, "hdr_tamper", 100)],
                None,
            );
            append_encrypted_tx(&mut log, &io, tx);

            // Flip a byte in the commit_ts field (TX header offset 16..24, file offset = LOG_HDR + 16).
            let corrupt_offset = (LOG_HDR_SIZE + 16) as u64;
            let byte_buf = Arc::new(Buffer::new(vec![0xFF]));
            let c = Completion::new_write(move |_| {});
            io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
                .unwrap();

            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
            reader.read_header(&io).unwrap();

            match io.block(|| reader.parse_next_transaction()).unwrap() {
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
            let tx = crate::mvcc::database::LogRecord::for_test(
                100,
                &[make_test_row_version(table_id, 1, "tamper_me", 100)],
                None,
            );
            append_encrypted_tx(&mut log, &io, tx);

            // Tamper a 16-byte window in the ciphertext (after log header
            // + TX header). A single-byte overwrite with 0xFF can be a no-op
            // when the cipher output at that offset already equals 0xFF
            // (~1/256 per run); tampering a 16-byte run with a fixed
            // alternating pattern makes the no-op probability 1/256^16,
            // which is effectively never.
            let corrupt_offset = (LOG_HDR_SIZE + TX_HEADER_SIZE + 1) as u64;
            let pattern: Vec<u8> = (0..16)
                .map(|i| if i & 1 == 0 { 0x00 } else { 0xFF })
                .collect();
            let byte_buf = Arc::new(Buffer::new(pattern));
            let c = Completion::new_write(move |_| {});
            io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
                .unwrap();

            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
            reader.read_header(&io).unwrap();

            match io.block(|| reader.parse_next_transaction()).unwrap() {
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
        let first_tx = crate::mvcc::database::LogRecord::for_test(100, &[first_row_version], None);
        append_encrypted_tx(&mut log, &io, first_tx);
        let second_tx = crate::mvcc::database::LogRecord::for_test(
            101,
            &[make_test_row_version(table_id, 1, "data", 101)],
            None,
        );
        append_encrypted_tx(&mut log, &io, second_tx);

        // Truncate mid-way through the second frame.
        let file_size = file.size().unwrap();
        let truncate_at = file_size - 5; // remove last 5 bytes
        let c = Completion::new_trunc(|_| {});
        io.wait_for_completion(file.truncate(truncate_at, c).unwrap())
            .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        // First frame should parse fine.
        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => {
                let ops = frame.ops;
                assert_eq!(ops.len(), 1);
                assert_upsert_table_op(&ops[0], (-2).into(), 0, &expected_first_record_bytes, 100);
            }
            other => panic!("expected Ops for frame 1, got {other:?}"),
        }

        // Second frame is torn — should be EOF.
        match io.block(|| reader.parse_next_transaction()).unwrap() {
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
        let tx = crate::mvcc::database::LogRecord::for_test(333, &[row_version], None);
        write_first_encrypted_tx(base_file.clone(), &io, &enc_ctx, tx);
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
                match io.block(|| reader.parse_next_transaction()).unwrap() {
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
        let first_tx = crate::mvcc::database::LogRecord::for_test(500, &[first_row_version], None);
        let c = log.log_tx(first_tx).unwrap();
        io.wait_for_completion(c).unwrap();
        let second_frame_start = log.offset as usize;

        let second_tx = crate::mvcc::database::LogRecord::for_test(
            600,
            &[make_test_row_version(table_id, 2, &value, 600)],
            None,
        );
        let c = log.log_tx(second_tx).unwrap();
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
            match io.block(|| reader.parse_next_transaction()).unwrap() {
                ParseResult::Frame(frame) => {
                    let ops = frame.ops;
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
            match io.block(|| reader.parse_next_transaction()).unwrap() {
                ParseResult::Eof => {}
                other => panic!("expected Eof for torn multi-chunk frame, got {other:?}"),
            }
        }

        // Keep the last chunk variable used so the compiler notices if the range math changes.
        assert!(last_chunk.end > last_chunk.start);
    }
}
