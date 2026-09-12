use crate::alloc::vec;
use crate::alloc::*;
use crate::turso_assert;
use crate::types::IOResultOr;
use crate::{
    error::LimboError,
    io::{Buffer, Completion, TempFile, IO},
    io_yield_one, return_if_io,
    storage::sqlite3_ondisk::{read_varint, read_varint_partial, varint_len, write_varint},
    sync::{
        atomic::{self, AtomicUsize},
        Arc, RwLock,
    },
    translate::collate::CollationSeq,
    types::{IOCompletions, IOResult, Value, ValueRef},
    vdbe::metrics::HashJoinMetrics,
    CompletionError, Numeric, Result,
};
use branches::{mark_unlikely, unlikely};
use rapidhash::fast::RapidHasher;
use std::{cell::RefCell, cmp::Ordering, hash::Hasher};
use turso_macros::{turso_assert_eq, AtomicEnum};

const DEFAULT_SEED: u64 = 1337;

// set to a *very* small 32KB, intentionally to trigger frequent spilling during tests
#[cfg(debug_assertions)]
pub const DEFAULT_MEM_BUDGET: usize = 32 * 1024;

/// 64MB default memory budget for hash joins.
/// TODO: make configurable via PRAGMA
#[cfg(not(debug_assertions))]
pub const DEFAULT_MEM_BUDGET: usize = 64 * 1024 * 1024;
const DEFAULT_BUCKETS: usize = 1024;
/// Minimum number of partitions for grace hash join.
pub const MIN_PARTITIONS: usize = 16;
/// Maximum number of partitions for adaptive partitioning.
pub const MAX_PARTITIONS: usize = 128;
const NULL_HASH: u8 = 0;
const INT_HASH: u8 = 1;
const FLOAT_HASH: u8 = 2;
const TEXT_HASH: u8 = 3;
const BLOB_HASH: u8 = 4;

#[inline]
/// Hash text case-insensitively without allocation (ASCII-only for SQLite NOCASE).
/// SQLite's NOCASE collation only considers ASCII case, so to_ascii_lowercase() is correct.
fn hash_text_nocase(hasher: &mut impl Hasher, text: &str) {
    hasher.write_usize(text.len());
    for byte in text.bytes() {
        hasher.write_u8(byte.to_ascii_lowercase());
        if byte == 0 {
            break;
        }
    }
}

/// Hash function for join keys using rapidhash
/// Takes collation into account when hashing text values
fn hash_join_key(key_values: &[ValueRef], collations: &[CollationSeq]) -> u64 {
    let mut hasher = RapidHasher::new(DEFAULT_SEED);

    for (idx, value) in key_values.iter().enumerate() {
        match value {
            ValueRef::Null => {
                hasher.write_u8(NULL_HASH);
            }
            ValueRef::Numeric(Numeric::Integer(i)) => {
                // Hash integers in the same bucket as numerically equivalent REALs so e.g. 10 and 10.0 have the same hash.
                let f = *i as f64;
                if (f as i64) == *i && f.is_finite() {
                    hasher.write_u8(FLOAT_HASH);
                    let bits = normalized_f64_bits(f);
                    hasher.write(&bits.to_le_bytes());
                } else {
                    // Fallback to the integer domain when the float representation would lose precision.
                    hasher.write_u8(INT_HASH);
                    hasher.write_i64(*i);
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                hasher.write_u8(FLOAT_HASH);
                let bits = normalized_f64_bits(f64::from(*f));
                hasher.write(&bits.to_le_bytes());
            }
            ValueRef::Text(text) => {
                let collation = collations.get(idx).unwrap_or(&CollationSeq::Binary);
                hasher.write_u8(TEXT_HASH);
                match *collation {
                    CollationSeq::NoCase => {
                        hash_text_nocase(&mut hasher, text.as_str());
                    }
                    CollationSeq::Rtrim => {
                        let trimmed = text.as_str().trim_end_matches(' ');
                        hasher.write(trimmed.as_bytes());
                    }
                    CollationSeq::Binary | CollationSeq::Unset => {
                        hasher.write(text.as_bytes());
                    }
                    CollationSeq::Locale(_) => {
                        hasher.write(&collation.hash_key(text.as_str()));
                    }
                    CollationSeq::Custom(_) => {
                        unreachable!(
                            "custom collations are rejected before hash table construction"
                        )
                    }
                }
            }
            ValueRef::Blob(blob) => {
                hasher.write_u8(BLOB_HASH);
                hasher.write(blob);
            }
        }
    }
    hasher.finish()
}

/// Normalize signed zero so 0.0 and -0.0 hash the same.
#[inline]
const fn normalized_f64_bits(f: f64) -> u64 {
    if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    }
}

/// Check if any of the key values is NULL.
/// Rows with NULL join keys should be skipped in hash joins since NULL != NULL in SQL.
fn has_null_key(key_values: &[Value]) -> bool {
    key_values.iter().any(|v| matches!(v, Value::Null))
}

/// Check if any of the key value refs is NULL.
fn has_null_key_ref(key_values: &[ValueRef]) -> bool {
    key_values.iter().any(|v| matches!(v, ValueRef::Null))
}

/// Check if two key value arrays are equal, taking collation into account.
fn keys_equal(key1: &[Value], key2: &[ValueRef], collations: &[CollationSeq]) -> bool {
    if key1.len() != key2.len() {
        return false;
    }
    for (idx, (v1, v2)) in key1.iter().zip(key2.iter()).enumerate() {
        let collation = collations.get(idx).copied().unwrap_or(CollationSeq::Binary);
        if !values_equal(v1.as_ref(), *v2, collation) {
            return false;
        }
    }
    true
}

/// Check if two values are equal, using the specified collation for text comparison.
/// NOTE: In SQL, NULL = NULL evaluates to NULL (falsy), so this returns false for NULL comparisons.
fn values_equal(v1: ValueRef, v2: ValueRef, collation: CollationSeq) -> bool {
    match (v1, v2) {
        // NULL = NULL is false in SQL (actually NULL, which is falsy)
        (ValueRef::Null, _) | (_, ValueRef::Null) => false,
        (ValueRef::Numeric(n1), ValueRef::Numeric(n2)) => {
            ValueRef::Numeric(n1) == ValueRef::Numeric(n2)
        }
        (ValueRef::Blob(b1), ValueRef::Blob(b2)) => b1 == b2,
        (ValueRef::Text(t1), ValueRef::Text(t2)) => {
            // Use collation for text comparison
            collation.compare_strings(t1.as_str(), t2.as_str()) == Ordering::Equal
        }
        _ => false,
    }
}

/// DISTINCT equality: NULLs compare equal to NULL.
fn values_equal_distinct(v1: ValueRef, v2: ValueRef, collation: CollationSeq) -> bool {
    match (v1, v2) {
        (ValueRef::Null, ValueRef::Null) => true,
        (ValueRef::Null, _) | (_, ValueRef::Null) => false,
        _ => values_equal(v1, v2, collation),
    }
}

fn keys_equal_distinct(key1: &[Value], key2: &[ValueRef], collations: &[CollationSeq]) -> bool {
    if key1.len() != key2.len() {
        return false;
    }
    for (idx, (v1, v2)) in key1.iter().zip(key2.iter()).enumerate() {
        let collation = collations.get(idx).copied().unwrap_or(CollationSeq::Binary);
        if !values_equal_distinct(v1.as_ref(), *v2, collation) {
            return false;
        }
    }
    true
}

/// State machine states for hash table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashTableState {
    Building,
    Probing,
    Spilled,
    GraceProcessing,
    Closed,
}

/// A probe entry returned by `grace_next_probe_entry()`.
/// The VDBE writes these to registers for HashProbe to use.
#[derive(Debug)]
pub struct GraceProbeEntry {
    pub key_values: Vec<Value>,
    pub probe_rowid: i64,
}

/// A single entry in a hash table bucket.
#[derive(Debug, Clone)]
pub struct HashEntry {
    /// Hash value of the join keys.
    pub hash: u64,
    /// The join key values.
    pub key_values: Vec<Value>,
    /// The rowid of the row in the build table.
    /// During probe phase, we'll use SeekRowid to fetch the full row
    /// (unless payload_values contains all needed columns).
    pub rowid: i64,
    /// Optional payload values - columns from the build table that are stored
    /// directly in the hash entry to avoid SeekRowid during probe phase.
    /// When populated, these are the result columns needed from the build table,
    /// stored in column index order as specified during hash table construction.
    pub payload_values: Vec<Value>,
}

#[derive(Debug)]
pub(crate) struct PendingHashInsert {
    pub(crate) key_values: Vec<Value>,
    pub(crate) rowid: i64,
    pub(crate) payload_values: Vec<Value>,
}

#[derive(Debug)]
pub(crate) enum HashInsertResult {
    Done,
    IO {
        io: IOCompletions,
        pending: PendingHashInsert,
    },
}

impl HashEntry {
    fn new(hash: u64, key_values: Vec<Value>, rowid: i64) -> Self {
        Self {
            hash,
            key_values,
            rowid,
            payload_values: vec![],
        }
    }

    const fn new_with_payload(
        hash: u64,
        key_values: Vec<Value>,
        rowid: i64,
        payload_values: Vec<Value>,
    ) -> Self {
        Self {
            hash,
            key_values,
            rowid,
            payload_values,
        }
    }

    /// Returns true if this entry has payload values stored.
    pub const fn has_payload(&self) -> bool {
        !self.payload_values.is_empty()
    }

    /// Get the size of this entry in bytes (approximate).
    /// This is a lightweight estimate for memory budgeting, not a precise measurement.
    fn size_bytes(&self) -> usize {
        Self::size_from_values(&self.key_values, &self.payload_values)
    }

    fn size_from_values(key_values: &[Value], payload_values: &[Value]) -> usize {
        let value_size = |v: &Value| match v {
            Value::Null => 1,
            Value::Numeric(_) => 8,
            Value::Text(t) => t.as_str().len(),
            Value::Blob(b) => b.len(),
        };
        let key_size: usize = key_values.iter().map(value_size).sum();
        let payload_size: usize = payload_values.iter().map(value_size).sum();
        key_size + payload_size + 8 + 8 // +8 for hash, +8 for rowid
    }

    /// Calculate the serialized size of a single Value.
    #[inline]
    fn value_serialized_size(v: &Value) -> usize {
        1 + match v {
            Value::Null => 0,
            Value::Numeric(_) => 8,
            Value::Text(t) => {
                let len = t.as_str().len();
                varint_len(len as u64) + len
            }
            Value::Blob(b) => varint_len(b.len() as u64) + b.len(),
        }
    }

    /// Calculate the exact serialized size of this entry.
    fn serialized_size(&self) -> usize {
        8 + 8 // hash + rowid
            + varint_len(self.key_values.len() as u64)
            + self.key_values.iter().map(Self::value_serialized_size).sum::<usize>()
            + varint_len(self.payload_values.len() as u64)
            + self.payload_values.iter().map(Self::value_serialized_size).sum::<usize>()
    }

    /// Serialize this entry directly to a slice, returns bytes written.
    /// The caller must ensure the slice is large enough (use serialized_size()).
    fn serialize_to_slice(&self, buf: &mut [u8]) -> usize {
        let mut offset = 0;

        // Write hash and rowid
        buf[offset..offset + 8].copy_from_slice(&self.hash.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.rowid.to_le_bytes());
        offset += 8;

        // Write number of keys and key values
        offset += write_varint(&mut buf[offset..], self.key_values.len() as u64);
        for value in &self.key_values {
            offset += Self::serialize_value_to_slice(value, &mut buf[offset..]);
        }

        // Write number of payload values and payload values
        offset += write_varint(&mut buf[offset..], self.payload_values.len() as u64);
        for value in &self.payload_values {
            offset += Self::serialize_value_to_slice(value, &mut buf[offset..]);
        }

        offset
    }

    /// Helper to serialize a single Value directly to a slice. Returns bytes written.
    #[inline]
    fn serialize_value_to_slice(value: &Value, buf: &mut [u8]) -> usize {
        let mut offset = 0;
        match value {
            Value::Null => {
                buf[offset] = NULL_HASH;
                offset += 1;
            }
            Value::Numeric(Numeric::Integer(i)) => {
                buf[offset] = INT_HASH;
                offset += 1;
                buf[offset..offset + 8].copy_from_slice(&i.to_le_bytes());
                offset += 8;
            }
            Value::Numeric(Numeric::Float(f)) => {
                buf[offset] = FLOAT_HASH;
                offset += 1;
                buf[offset..offset + 8].copy_from_slice(&f64::from(*f).to_le_bytes());
                offset += 8;
            }
            Value::Text(t) => {
                buf[offset] = TEXT_HASH;
                offset += 1;
                let bytes = t.as_str().as_bytes();
                offset += write_varint(&mut buf[offset..], bytes.len() as u64);
                buf[offset..offset + bytes.len()].copy_from_slice(bytes);
                offset += bytes.len();
            }
            Value::Blob(b) => {
                buf[offset] = BLOB_HASH;
                offset += 1;
                offset += write_varint(&mut buf[offset..], b.len() as u64);
                buf[offset..offset + b.len()].copy_from_slice(b);
                offset += b.len();
            }
        }
        offset
    }

    /// Serialize this entry to bytes for disk storage.
    /// Format: [hash:8][rowid:8][num_keys:varint][keys...][num_payload:varint][payload...]
    /// Each value is: [type:1][len:varint (for text/blob)][data]
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.try_reserve(self.serialized_size())?;
        buf.extend_from_slice(&self.hash.to_le_bytes());
        buf.extend_from_slice(&self.rowid.to_le_bytes());

        // Write number of keys and key values
        let varint_buf = &mut [0u8; 9];
        let len = write_varint(varint_buf, self.key_values.len() as u64);
        buf.extend_from_slice(&varint_buf[..len]);
        for value in &self.key_values {
            Self::serialize_value(value, buf, varint_buf);
        }

        // Write number of payload values and payload values
        let len = write_varint(varint_buf, self.payload_values.len() as u64);
        buf.extend_from_slice(&varint_buf[..len]);
        for value in &self.payload_values {
            Self::serialize_value(value, buf, varint_buf);
        }
        Ok(())
    }

    /// Helper to serialize a single Value to bytes.
    fn serialize_value(value: &Value, buf: &mut Vec<u8>, varint_buf: &mut [u8; 9]) {
        match value {
            Value::Null => {
                buf.push(NULL_HASH);
            }
            Value::Numeric(Numeric::Integer(i)) => {
                buf.push(INT_HASH);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Numeric(Numeric::Float(f)) => {
                buf.push(FLOAT_HASH);
                buf.extend_from_slice(&f64::from(*f).to_le_bytes());
            }
            Value::Text(t) => {
                buf.push(TEXT_HASH);
                let bytes = t.as_str().as_bytes();
                let len = write_varint(varint_buf, bytes.len() as u64);
                buf.extend_from_slice(&varint_buf[..len]);
                buf.extend_from_slice(bytes);
            }
            Value::Blob(b) => {
                buf.push(BLOB_HASH);
                let len = write_varint(varint_buf, b.len() as u64);
                buf.extend_from_slice(&varint_buf[..len]);
                buf.extend_from_slice(b);
            }
        }
    }

    /// Deserialize an entry from bytes, returning (entry, bytes_consumed) or error.
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if unlikely(buf.len() < 16) {
            return Err(LimboError::Corrupt(
                "HashEntry: buffer too small for header".to_string(),
            ));
        }

        // buffer len checked above
        let hash = u64::from_le_bytes(buf[0..8].try_into().expect("expect 8 bytes"));
        let rowid = i64::from_le_bytes(buf[8..16].try_into().expect("expect 8 bytes"));
        let mut offset = 16;

        // Read number of keys and key values
        let (num_keys, varint_len) = read_varint(&buf[offset..])?;
        offset += varint_len;

        let mut key_values = Vec::try_with_capacity_ext(num_keys as usize)?;
        for _ in 0..num_keys {
            let (value, consumed) = Self::deserialize_value(&buf[offset..])?;
            key_values
                .push_within_capacity(value)
                .expect("key values vector was preallocated");
            offset += consumed;
        }

        // Read number of payload values and payload values
        let (num_payload, varint_len) = read_varint(&buf[offset..])?;
        offset += varint_len;

        let mut payload_values = Vec::try_with_capacity_ext(num_payload as usize)?;
        for _ in 0..num_payload {
            let (value, consumed) = Self::deserialize_value(&buf[offset..])?;
            payload_values
                .push_within_capacity(value)
                .expect("payload values vector was preallocated");
            offset += consumed;
        }

        Ok((
            Self {
                hash,
                key_values,
                rowid,
                payload_values,
            },
            offset,
        ))
    }

    /// Helper to deserialize a single Value from bytes.
    /// Returns (Value, bytes_consumed).
    fn deserialize_value(buf: &[u8]) -> Result<(Value, usize)> {
        if unlikely(buf.is_empty()) {
            return Err(LimboError::Corrupt(
                "HashEntry: unexpected end of buffer".to_string(),
            ));
        }
        let value_type = buf[0];
        let mut offset = 1;

        let value = match value_type {
            NULL_HASH => Value::Null,
            INT_HASH => {
                if unlikely(offset + 8 > buf.len()) {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for integer".to_string(),
                    ));
                }
                let i =
                    i64::from_le_bytes(buf[offset..offset + 8].try_into().expect("expect 8 bytes"));
                offset += 8;
                Value::from_i64(i)
            }
            FLOAT_HASH => {
                if unlikely(offset + 8 > buf.len()) {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for float".to_string(),
                    ));
                }
                let f =
                    f64::from_le_bytes(buf[offset..offset + 8].try_into().expect("expect 8 bytes"));
                offset += 8;
                Value::from_f64(f)
            }
            TEXT_HASH => {
                let (str_len, varint_len) = read_varint(&buf[offset..])?;
                offset += varint_len;
                if unlikely(offset + str_len as usize > buf.len()) {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for text".to_string(),
                    ));
                }
                // SAFETY: We serialized this data ourselves, so it should be valid UTF-8.
                // Skipping validation here for performance in the spill/reload path.
                // Doing checked utf8 construction here is a massive performance hit.
                let bytes = buf[offset..offset + str_len as usize].to_vec();
                let s = unsafe { String::from_utf8_unchecked(bytes) };
                offset += str_len as usize;
                Value::Text(s.into())
            }
            BLOB_HASH => {
                let (blob_len, varint_len) = read_varint(&buf[offset..])?;
                offset += varint_len;
                if unlikely(offset + blob_len as usize > buf.len()) {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for blob".to_string(),
                    ));
                }
                let b =
                    crate::types::value_blob_from_slice(&buf[offset..offset + blob_len as usize])?;
                offset += blob_len as usize;
                Value::Blob(b)
            }
            _ => {
                mark_unlikely();
                return Err(LimboError::Corrupt(format!(
                    "HashEntry: unknown value type {value_type}",
                )));
            }
        };
        Ok((value, offset))
    }
}

#[derive(Debug, Clone, Copy)]
struct Partitioning {
    count: usize,
    mask: usize,
    shift: u32,
}

impl Partitioning {
    fn new(count: usize) -> Self {
        turso_assert!(
            count.is_power_of_two(),
            "partition count must be a power of two"
        );
        let bits = count.trailing_zeros();
        Self {
            count,
            mask: count - 1,
            shift: 64 - bits,
        }
    }

    #[inline(always)]
    fn index(&self, hash: u64) -> usize {
        ((hash >> self.shift) as usize) & self.mask
    }
}

/// A bucket in the hash table. Uses chaining for collision resolution.
#[derive(Debug, Clone)]
pub struct HashBucket {
    entries: Vec<HashEntry>,
}

impl HashBucket {
    fn new() -> Self {
        Self { entries: vec![] }
    }

    fn insert(&mut self, entry: HashEntry) -> Result<()> {
        self.entries.try_push(entry)?;
        Ok(())
    }

    const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn size_bytes(&self) -> usize {
        self.entries.iter().map(|e| e.size_bytes()).sum()
    }
}

/// I/O state for spilled partition operations
#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq)]
pub enum SpillIOState {
    None,
    WaitingForWrite,
    WriteComplete,
    WaitingForRead,
    ReadComplete,
    Error,
}

/// State of a partition in a spilled hash table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionState {
    /// data is in partition_buffers
    InMemory,
    /// Has been written to disk, not yet loaded
    OnDisk,
    /// Is being loaded from disk (I/O in progress)
    Loading,
    /// Has been loaded from disk and is ready for probing
    Loaded,
}

/// A chunk of partition data spilled to disk.
/// A partition may be spilled multiple times, creating multiple chunks.
#[derive(Debug)]
struct SpillChunk {
    /// File offset where this chunk's data starts
    file_offset: u64,
    /// Size in bytes of this chunk on disk
    size_bytes: usize,
    /// Number of entries in this chunk
    num_entries: usize,
}

/// Tracks a partition that has been spilled to disk during grace hash join.
pub struct SpilledPartition {
    /// Partition index (0 to partition_count - 1)
    pub partition_idx: usize,
    /// Chunks of data belonging to this partition (may have multiple spills)
    chunks: Vec<SpillChunk>,
    /// Current state of the partition
    state: PartitionState,
    /// I/O state for async operations
    io_state: Arc<AtomicSpillIOState>,
    /// Read buffer for loading partition back
    read_buffer: Arc<RwLock<Vec<u8>>>,
    /// Length of data in read buffer
    buffer_len: Arc<AtomicUsize>,
    /// Hash buckets for this partition (populated after loading)
    buckets: Vec<HashBucket>,
    /// Current chunk being loaded (for multi-chunk reads)
    current_chunk_idx: usize,
    /// Approximate memory used by the resident buckets for this partition
    resident_mem: usize,
    /// Parallel to `buckets`: tracks which entries have been matched (for FULL OUTER JOIN).
    matched_bits: Vec<Vec<bool>>,
    /// Partial entry bytes spanning chunk boundaries
    partial_entry: Vec<u8>,
    /// Parsed entries for validation
    parsed_entries: usize,
}

impl SpilledPartition {
    fn new(partition_idx: usize) -> Self {
        Self {
            partition_idx,
            chunks: vec![],
            state: PartitionState::OnDisk,
            io_state: Arc::new(AtomicSpillIOState::new(SpillIOState::None)),
            read_buffer: Arc::new(RwLock::new(vec![])),
            buffer_len: Arc::new(atomic::AtomicUsize::new(0)),
            buckets: vec![],
            current_chunk_idx: 0,
            resident_mem: 0,
            matched_bits: vec![],
            partial_entry: vec![],
            parsed_entries: 0,
        }
    }

    /// Add a new chunk of data to this partition
    fn add_chunk(&mut self, file_offset: u64, size_bytes: usize, num_entries: usize) -> Result<()> {
        self.chunks.try_push(SpillChunk {
            file_offset,
            size_bytes,
            num_entries,
        })?;
        Ok(())
    }

    /// Get total size in bytes across all chunks
    fn total_size_bytes(&self) -> usize {
        self.chunks.iter().map(|c| c.size_bytes).sum()
    }

    /// Get total number of entries across all chunks
    fn total_num_entries(&self) -> usize {
        self.chunks.iter().map(|c| c.num_entries).sum()
    }

    fn buffer_len(&self) -> usize {
        self.buffer_len.load(atomic::Ordering::Acquire)
    }

    /// Check if partition is ready for probing
    pub const fn is_loaded(&self) -> bool {
        matches!(
            self.state,
            PartitionState::Loaded | PartitionState::InMemory
        )
    }

    /// Check if there are more chunks to load
    const fn has_more_chunks(&self) -> bool {
        self.current_chunk_idx < self.chunks.len()
    }

    /// Get the current chunk to load, if any
    fn current_chunk(&self) -> Option<&SpillChunk> {
        self.chunks.get(self.current_chunk_idx)
    }
}

/// In-memory partition buffer for grace hash join.
/// During build phase, entries are first accumulated here before spilling.
struct PartitionBuffer {
    /// Entries in this partition
    entries: Vec<HashEntry>,
    /// Total memory used by entries in this partition
    mem_used: usize,
}

impl PartitionBuffer {
    fn new() -> Self {
        Self {
            entries: vec![],
            mem_used: 0,
        }
    }

    fn insert(&mut self, entry: HashEntry) -> Result<()> {
        self.mem_used += entry.size_bytes();
        self.entries.try_push(entry)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.mem_used = 0;
    }

    const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Configuration for the hash table.
#[derive(Debug, Clone)]
pub struct HashTableConfig {
    /// Initial number of buckets (must be power of 2).
    pub initial_buckets: usize,
    /// Maximum memory budget in bytes.
    pub mem_budget: usize,
    /// Number of keys in the join condition.
    pub num_keys: usize,
    /// Collation sequences for each join key.
    pub collations: Vec<CollationSeq>,
    /// Only spill to a file when != TempStore::Memory
    pub temp_store: crate::TempStore,
    /// Whether to track which entries have been matched during probing (for FULL OUTER JOIN).
    pub track_matched: bool,
    /// Optional override for the number of partitions (must be power of two).
    pub partition_count: Option<usize>,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            initial_buckets: DEFAULT_BUCKETS,
            mem_budget: DEFAULT_MEM_BUDGET,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
            temp_store: crate::TempStore::Default,
            track_matched: false,
            partition_count: None,
        }
    }
}

struct SpillState {
    /// In-memory partition buffers for grace hash join.
    /// When spilling is triggered, entries are partitioned by hash before writing.
    partition_buffers: Vec<PartitionBuffer>,
    /// Spilled partitions metadata, tracks what's on disk
    partitions: Vec<SpilledPartition>,
    /// Current file offset for next spill write
    next_spill_offset: u64,
    /// Temporary file for spilled data.
    temp_file: TempFile,
    /// Partitioning strategy for this spill.
    partitioning: Partitioning,
}

impl SpillState {
    fn new(
        io: &Arc<dyn IO>,
        temp_store: crate::TempStore,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(SpillState {
            partition_buffers: (0..partitioning.count)
                .map(|_| PartitionBuffer::new())
                .try_collect()?,
            partitions: vec![],
            next_spill_offset: 0,
            temp_file: TempFile::with_temp_store(io, temp_store)?,
            partitioning,
        })
    }

    fn find_partition_mut(&mut self, logical_idx: usize) -> Option<&mut SpilledPartition> {
        self.partitions
            .iter_mut()
            .find(|p| p.partition_idx == logical_idx)
    }

    fn find_partition(&self, logical_idx: usize) -> Option<&SpilledPartition> {
        self.partitions
            .iter()
            .find(|p| p.partition_idx == logical_idx)
    }
}

/// Probe-side buffering/spilling state for grace hash join.
/// Reuses the same serialization format and types as build-side spilling.
struct ProbeSpillState {
    /// In-memory partition buffers for probe rows targeting spilled build partitions.
    partition_buffers: Vec<PartitionBuffer>,
    /// Spilled probe partition metadata.
    partitions: Vec<SpilledPartition>,
    /// Current file offset for next probe spill write.
    next_spill_offset: u64,
    /// Separate temp file for probe-side spills.
    temp_file: TempFile,
    /// Same partitioning as build side, so partition indices correspond.
    partitioning: Partitioning,
    /// Current memory used by probe buffers.
    mem_used: usize,
    /// Memory budget for probe-side buffers.
    mem_budget: usize,
}

impl ProbeSpillState {
    fn new(
        io: &Arc<dyn IO>,
        temp_store: crate::TempStore,
        partitioning: Partitioning,
        mem_budget: usize,
    ) -> Result<Self> {
        Ok(Self {
            partition_buffers: (0..partitioning.count)
                .map(|_| PartitionBuffer::new())
                .try_collect()?,
            partitions: vec![],
            next_spill_offset: 0,
            temp_file: TempFile::with_temp_store(io, temp_store)?,
            partitioning,
            mem_used: 0,
            mem_budget,
        })
    }

    fn find_partition_mut(&mut self, logical_idx: usize) -> Option<&mut SpilledPartition> {
        self.partitions
            .iter_mut()
            .find(|p| p.partition_idx == logical_idx)
    }

    fn find_partition(&self, logical_idx: usize) -> Option<&SpilledPartition> {
        self.partitions
            .iter()
            .find(|p| p.partition_idx == logical_idx)
    }
}

/// State for grace hash join partition-by-partition processing.
/// The VDBE drives the loop; this tracks partition iteration and probe chunk loading.
struct GraceState {
    /// Current probe entries loaded from disk (one chunk at a time).
    probe_entries: Vec<HashEntry>,
    /// Cursor into probe_entries.
    probe_entry_cursor: usize,
    /// Ordered list of partition indices to process (only spilled ones).
    partitions_to_process: Vec<usize>,
    /// Index into partitions_to_process.
    partition_list_idx: usize,
    /// Current load state for the active grace partition.
    load_state: GracePartitionLoadState,
}

impl GraceState {
    fn current_partition_idx(&self) -> Option<usize> {
        self.partitions_to_process
            .get(self.partition_list_idx)
            .copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GracePartitionLoadState {
    NeedBuildLoad,
    NeedProbeLoad,
    Ready,
}

/// HashTable is the build-side data structure used for hash joins and DISTINCT. It behaves like a
/// standard in-memory hash table until a configurable memory budget is exceeded, at
/// which point it transparently switches to a grace-hash-join style layout and spills
/// partitions to disk.
///
/// # Overview
///
/// The table is keyed by an N-column join key. Keys are hashed using a stable
/// rapidhash hasher that is aware of SQLite-style collations for text values.
/// Each entry stores:
///
/// - the precomputed hash value,
/// - a owned copy of the join key values, and
/// - the rowid of the build-side row, used later to SeekRowid into the build table.
///
/// Collisions within a hash bucket are resolved using simple chaining (a `Vec<HashEntry>`),
/// and equality is determined by comparing the stored key values against probe keys using
/// the same collation-aware comparison logic that was used when hashing.
pub struct HashTable {
    /// Initial bucket count used to reinitialize after spills.
    initial_buckets: usize,
    /// The hash buckets (used when not spilled).
    buckets: Vec<HashBucket>,
    /// Number of entries in the table.
    num_entries: usize,
    /// Current memory usage in bytes.
    mem_used: usize,
    /// Memory budget in bytes.
    mem_budget: usize,
    /// Number of join keys.
    num_keys: usize,
    /// Collation sequences for each join key.
    collations: Vec<CollationSeq>,
    /// Current state of the hash table.
    state: HashTableState,
    /// IO object for disk operations.
    io: Arc<dyn IO>,
    /// Current probe position bucket index.
    probe_bucket_idx: usize,
    /// Current probe entry index within bucket.
    probe_entry_idx: usize,
    /// Cached hash of current probe keys (to avoid recomputing)
    current_probe_hash: Option<u64>,
    /// Current probe key values being searched.
    current_probe_keys: Option<Vec<Value>>,
    spill_state: Option<SpillState>,
    /// Index of current spilled partition being probed
    current_spill_partition_idx: usize,
    /// Track non-empty buckets for fast clear in distinct/group-by usage.
    non_empty_buckets: Vec<usize>,
    /// LRU of resident spilled partitions to cap memory for DISTINCT, grace,
    /// and unmatched-scan partition loads.
    loaded_partitions_lru: RefCell<VecDeque<usize>>,
    /// Memory used by resident (loaded or in-memory) partitions
    loaded_partitions_mem: usize,
    /// Temp storage mode (memory vs file) for spilled data
    temp_store: crate::TempStore,
    /// Whether to track matched entries (for FULL OUTER JOIN).
    track_matched: bool,
    /// Parallel to `buckets`: one Vec<bool> per bucket tracking which entries were matched.
    matched_bits: Vec<Vec<bool>>,
    /// Bucket index for iterating unmatched entries.
    unmatched_scan_bucket: usize,
    /// Entry index within bucket for iterating unmatched entries.
    unmatched_scan_entry: usize,
    /// Partition index for iterating unmatched entries in spilled mode.
    unmatched_scan_partition: usize,
    /// Optional override for partition count selection
    partition_count_override: Option<usize>,
    /// Probe-side spill state for grace hash join.
    probe_spill_state: Option<ProbeSpillState>,
    /// Grace processing state machine.
    grace_state: Option<GraceState>,
}

crate::assert::assert_send!(HashTable);

enum SpillAction {
    AlreadyLoaded,
    ParseChunk {
        partition_idx: usize,
    },
    WaitingForIO,
    NoChunks,
    LoadChunk {
        read_size: usize,
        file_offset: u64,
        io_state: Arc<AtomicSpillIOState>,
        buffer_len: Arc<AtomicUsize>,
        read_buffer_ref: Arc<RwLock<Vec<u8>>>,
    },
    Restart,
    NotFound,
}

enum GraceProbeChunkAction {
    WaitingForIO,
    ParseChunk {
        partition_idx: usize,
    },
    LoadChunk {
        read_size: usize,
        file_offset: u64,
        io_state: Arc<AtomicSpillIOState>,
        buffer_len: Arc<AtomicUsize>,
        read_buffer_ref: Arc<RwLock<Vec<u8>>>,
    },
    Restart,
    NoMoreChunks,
}

enum ParseChunkResult {
    MoreChunks,
    Done { resident_mem: usize },
}

impl HashTable {
    /// Create a new hash table.
    pub fn new(config: HashTableConfig, io: Arc<dyn IO>) -> Result<Self> {
        if config
            .collations
            .iter()
            .any(|collation| collation.is_custom())
        {
            // Custom collation equality is a connection-owned callback. Hash tables do
            // not carry connection context, so hash/equality cannot be made consistent here.
            return Err(LimboError::InternalError(
                "custom collations are not supported by hash tables".to_string(),
            ));
        }
        let num_buckets = config.initial_buckets;
        let buckets = (0..num_buckets).map(|_| HashBucket::new()).try_collect()?;
        let matched_bits = if config.track_matched {
            (0..num_buckets).map(|_| vec![]).try_collect()?
        } else {
            vec![]
        };
        Ok(Self {
            initial_buckets: config.initial_buckets,
            buckets,
            num_entries: 0,
            mem_used: 0,
            mem_budget: config.mem_budget,
            num_keys: config.num_keys,
            collations: config.collations,
            state: HashTableState::Building,
            io,
            probe_bucket_idx: 0,
            probe_entry_idx: 0,
            current_probe_keys: None,
            current_probe_hash: None,
            spill_state: None,
            current_spill_partition_idx: 0,
            loaded_partitions_lru: VecDeque::new().into(),
            loaded_partitions_mem: 0,
            non_empty_buckets: vec![],
            temp_store: config.temp_store,
            track_matched: config.track_matched,
            matched_bits,
            unmatched_scan_bucket: 0,
            unmatched_scan_entry: 0,
            unmatched_scan_partition: 0,
            partition_count_override: config.partition_count,
            probe_spill_state: None,
            grace_state: None,
        })
    }

    /// Get the current state of the hash table.
    pub fn get_state(&self) -> &HashTableState {
        &self.state
    }

    /// Based on average entry size and number of entries,
    /// determine the number of partitions to use for spilling.
    fn choose_partition_count(&self, entry_size: usize) -> usize {
        if let Some(count) = self.partition_count_override {
            turso_assert!(
                count.is_power_of_two(),
                "partition count override must be a power of two"
            );
            return count;
        }

        let avg_entry_size = if self.num_entries > 0 {
            (self.mem_used / self.num_entries).max(entry_size)
        } else {
            entry_size.max(1)
        };
        let target_partition_bytes = (self.mem_budget / 2).max(avg_entry_size);
        let target_entries_per_partition = (target_partition_bytes / avg_entry_size).max(1);
        let estimated_total_entries = self.num_entries.saturating_add(1);
        let mut partitions = estimated_total_entries.div_ceil(target_entries_per_partition);
        partitions = partitions.clamp(MIN_PARTITIONS, MAX_PARTITIONS);
        partitions.next_power_of_two()
    }

    /// For a given hash value, get the partition index.
    /// SAFETY: only call this when spill_state is Some.
    fn partition_index(&self, hash: u64) -> usize {
        let spill_state = self.spill_state.as_ref().expect("spill state must exist");
        spill_state.partitioning.index(hash)
    }

    fn record_probe_call(&mut self, metrics: Option<&mut HashJoinMetrics>) {
        if let Some(metrics) = metrics {
            metrics.probe_calls = metrics.probe_calls.saturating_add(1);
        }
    }

    /// Insert a row into the hash table, returns IOResult because this may spill to disk.
    /// When memory budget is exceeded, triggers grace hash join by partitioning and spilling.
    /// Rows with NULL join keys are skipped since NULL != NULL in SQL.
    /// (This is specific to hash join semantics, not DISTINCT.)
    pub fn insert(
        &mut self,
        key_values: Vec<Value>,
        rowid: i64,
        payload_values: Vec<Value>,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<()> {
        let pending = PendingHashInsert {
            key_values,
            rowid,
            payload_values,
        };
        match self.insert_pending(pending, metrics)? {
            HashInsertResult::Done => Ok(IOResult::Done(())),
            HashInsertResult::IO { io, .. } => Ok(IOResult::IO(io)),
        }
    }

    pub(crate) fn insert_pending(
        &mut self,
        pending: PendingHashInsert,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<HashInsertResult> {
        turso_assert!(
            matches!(
                self.state,
                HashTableState::Building | HashTableState::Spilled
            ),
            "Cannot insert into hash table in unexpected state",
            { "state": format!("{:?}", self.state) }
        );

        // Skip rows with NULL join keys - they can never match anything since NULL != NULL in SQL.
        // However, when track_matched is enabled (outer joins), we must keep NULL-key entries
        // so they appear as unmatched in the unmatched scan.
        if has_null_key(&pending.key_values) && !self.track_matched {
            return Ok(HashInsertResult::Done);
        }

        // Compute hash of the join keys using collations
        let key_refs: Vec<ValueRef> = pending
            .key_values
            .iter()
            .map(|value| value.as_ref())
            .try_collect()?;
        let hash = hash_join_key(&key_refs, &self.collations);
        let entry_size = HashEntry::size_from_values(&pending.key_values, &pending.payload_values);

        // Check if we would exceed memory budget
        if self.mem_used + entry_size > self.mem_budget {
            if self.spill_state.is_none() {
                tracing::debug!(
                    "Hash table memory budget exceeded (used: {}, budget: {}), spilling to disk",
                    self.mem_used,
                    self.mem_budget
                );
                // First time exceeding budget, trigger spill
                // Move all existing bucket entries into partition buffers
                let partition_count = self.choose_partition_count(entry_size);
                let partitioning = Partitioning::new(partition_count);
                self.spill_state = Some(SpillState::new(&self.io, self.temp_store, partitioning)?);
                self.redistribute_to_partitions()?;
                self.state = HashTableState::Spilled;
            };

            // Spill whole partitions until the new entry fits
            if let Some(c) = self.spill_partitions_for_entry(entry_size, metrics)? {
                // I/O pending, caller will re-enter after completion and retry the insert.
                if !c.finished() {
                    return Ok(HashInsertResult::IO {
                        io: IOCompletions(c),
                        pending,
                    });
                }
            }
        }

        let PendingHashInsert {
            key_values,
            rowid,
            payload_values,
        } = pending;
        let entry = if payload_values.is_empty() {
            HashEntry::new(hash, key_values, rowid)
        } else {
            HashEntry::new_with_payload(hash, key_values, rowid, payload_values)
        };

        if self.spill_state.is_some() {
            let partition_idx = {
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                spill_state.partitioning.index(hash)
            };
            let spill_state = self.spill_state.as_mut().expect("spill state must exist");
            // In spilled mode, insert into partition buffer
            spill_state.partition_buffers[partition_idx].insert(entry)?;
        } else {
            // Normal mode, insert into hash bucket
            let bucket_idx = (hash as usize) % self.buckets.len();
            if self.buckets[bucket_idx].entries.is_empty() {
                self.non_empty_buckets.try_push(bucket_idx)?;
            }
            self.buckets[bucket_idx].insert(entry)?;
            if self.track_matched {
                self.matched_bits[bucket_idx].try_push(false)?;
            }
        }

        self.num_entries += 1;
        self.mem_used += entry_size;

        Ok(HashInsertResult::Done)
    }

    /// Insert keys into the hash table if not already present.
    /// Returns true if inserted, false if duplicate found.
    /// Unlike hash join inserts, DISTINCT keeps NULLs and treats NULL==NULL.
    pub fn insert_distinct(
        &mut self,
        key_values: &[Value],
        key_refs: &[ValueRef],
        mut metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<bool> {
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot insert_distinct into hash table in unexpected state",
            { "state": format!("{:?}", self.state) }
        );

        let hash = hash_join_key(key_refs, &self.collations);

        if self.spill_state.is_some() {
            let partition_idx = self.partition_index(hash);
            // Check partition buffer for duplicates
            let has_buffer_dup = {
                let spill_state = self.spill_state.as_ref().expect("spill state exists");
                let buffer = &spill_state.partition_buffers[partition_idx];
                buffer.entries.iter().any(|entry| {
                    entry.hash == hash
                        && keys_equal_distinct(&entry.key_values, key_refs, &self.collations)
                })
            };
            if has_buffer_dup {
                return Ok(IOResult::Done(false));
            }

            // Ensure spilled partition is loaded before checking
            let has_partition = {
                let spill_state = self.spill_state.as_ref().expect("spill state exists");
                spill_state.find_partition(partition_idx).is_some()
            };
            if has_partition && !self.is_partition_loaded(partition_idx) {
                return_if_io!(self.load_spilled_partition(partition_idx, metrics.as_deref_mut()));
            }

            // Check loaded partition for duplicates
            let has_spilled_dup = 'has_spilled_dup: {
                let spill_state = self.spill_state.as_ref().expect("spill state exists");
                let Some(partition) = spill_state.find_partition(partition_idx) else {
                    break 'has_spilled_dup false;
                };
                if partition.buckets.is_empty() {
                    break 'has_spilled_dup false;
                }
                let bucket_idx = (hash as usize) % partition.buckets.len();
                let bucket = &partition.buckets[bucket_idx];
                bucket.entries.iter().any(|entry| {
                    entry.hash == hash
                        && keys_equal_distinct(&entry.key_values, key_refs, &self.collations)
                })
            };
            if has_spilled_dup {
                return Ok(IOResult::Done(false));
            }

            let entry_size = HashEntry::size_from_values(key_values, &[]);
            if let Some(c) = self.spill_partitions_for_entry(entry_size, metrics.as_deref_mut())? {
                if !c.succeeded() {
                    return Ok(IOResult::IO(IOCompletions(c)));
                }
            }

            {
                let spill_state = self.spill_state.as_mut().expect("spill state exists");
                spill_state.partition_buffers[partition_idx].insert(HashEntry::new(
                    hash,
                    key_values.iter().cloned().try_collect()?,
                    0,
                ))?;
            }
            self.num_entries += 1;
            self.mem_used += entry_size;
            return Ok(IOResult::Done(true));
        }

        // Non-spilled mode: check main buckets
        let bucket_idx = (hash as usize) % self.buckets.len();
        let bucket = &self.buckets[bucket_idx];
        for entry in &bucket.entries {
            if entry.hash == hash
                && keys_equal_distinct(&entry.key_values, key_refs, &self.collations)
            {
                return Ok(IOResult::Done(false));
            }
        }

        let entry_size = HashEntry::size_from_values(key_values, &[]);
        if self.mem_used + entry_size > self.mem_budget {
            if self.spill_state.is_none() {
                let partition_count = self.choose_partition_count(entry_size);
                let partitioning = Partitioning::new(partition_count);
                self.spill_state = Some(SpillState::new(&self.io, self.temp_store, partitioning)?);
                self.redistribute_to_partitions()?;
                self.state = HashTableState::Spilled;
            }
            return self.insert_distinct(key_values, key_refs, metrics);
        }

        if self.buckets[bucket_idx].entries.is_empty() {
            self.non_empty_buckets.try_push(bucket_idx)?;
        }
        self.buckets[bucket_idx].insert(HashEntry::new(
            hash,
            key_values.iter().cloned().try_collect()?,
            0,
        ))?;
        self.num_entries += 1;
        self.mem_used += entry_size;
        Ok(IOResult::Done(true))
    }

    /// Clear all entries and reset spill state.
    pub fn clear(&mut self) -> Result<()> {
        if self.num_entries == 0 && self.spill_state.is_none() {
            self.state = HashTableState::Building;
            self.current_probe_keys = None;
            self.current_probe_hash = None;
            self.probe_bucket_idx = 0;
            self.probe_entry_idx = 0;
            self.current_spill_partition_idx = 0;
            self.loaded_partitions_lru.borrow_mut().clear();
            self.loaded_partitions_mem = 0;
            self.non_empty_buckets.clear();
            self.probe_spill_state = None;
            self.grace_state = None;
            return Ok(());
        }

        if self.spill_state.is_some() {
            // Drop spilled partitions and reset buckets.
            self.spill_state = None;
            let bucket_count = self.initial_buckets.max(1);
            self.buckets = (0..bucket_count).map(|_| HashBucket::new()).try_collect()?;
            self.non_empty_buckets.clear();
        } else {
            for &idx in &self.non_empty_buckets {
                self.buckets[idx].entries.clear();
            }
            self.non_empty_buckets.clear();
        }

        self.num_entries = 0;
        self.mem_used = 0;
        self.state = HashTableState::Building;
        self.current_probe_keys = None;
        self.current_probe_hash = None;
        self.probe_bucket_idx = 0;
        self.probe_entry_idx = 0;
        self.current_spill_partition_idx = 0;
        self.loaded_partitions_lru.borrow_mut().clear();
        self.loaded_partitions_mem = 0;
        Ok(())
    }

    /// Redistribute existing bucket entries into partition buffers for grace hash join.
    fn redistribute_to_partitions(&mut self) -> Result<()> {
        let partitioning = {
            let spill_state = self.spill_state.as_ref().expect("spill state must exist");
            spill_state.partitioning
        };
        for bucket in self.buckets.drain(..) {
            for entry in bucket.entries {
                let partition_idx = partitioning.index(entry.hash);
                self.spill_state
                    .as_mut()
                    .expect("spill state must exist")
                    .partition_buffers[partition_idx]
                    .insert(entry)?;
            }
        }
        // Clear in-memory matched bits; spilled partitions will have their own.
        self.matched_bits.clear();
        Ok(())
    }

    /// Return the next partition which should be spilled to disk, for simplicity,
    /// we always select the largest non-empty partition buffer.
    fn next_partition_to_spill(&self, _required_free: usize) -> Option<usize> {
        let spill_state = self.spill_state.as_ref()?;
        spill_state
            .partition_buffers
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_empty())
            .max_by_key(|(_, p)| p.mem_used)
            .map(|(idx, _)| idx)
    }

    /// Spill the given partition buffer to disk and return the pending completion.
    /// Uses single-pass serialization directly into the I/O buffer to avoid intermediate copies.
    fn spill_partition(
        &mut self,
        partition_idx: usize,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<Completion>> {
        let mut metrics = metrics;
        let spill_state = self.spill_state.as_mut().expect("Spill state must exist");
        let partition = &spill_state.partition_buffers[partition_idx];
        if partition.is_empty() {
            return Ok(None);
        }

        // Phase 1: Calculate sizes and cache them to avoid recomputation
        let num_entries = partition.entries.len();
        let mut entry_sizes = Vec::try_with_capacity_ext(num_entries)?;
        let mut total_size = 0usize;
        for entry in &partition.entries {
            let entry_size = entry.serialized_size();
            entry_sizes
                .push_within_capacity(entry_size)
                .expect("entry sizes vector was preallocated");
            total_size += varint_len(entry_size as u64) + entry_size;
        }

        // Allocate I/O buffer and serialize using cached sizes
        let buffer = Buffer::new_temporary(total_size);
        let buf = buffer.as_mut_slice();
        let mut offset = 0;

        for (entry, &entry_size) in partition.entries.iter().zip(entry_sizes.iter()) {
            offset += write_varint(&mut buf[offset..], entry_size as u64);
            offset += entry.serialize_to_slice(&mut buf[offset..]);
        }

        turso_assert!(offset == total_size, "serialized size mismatch");

        let file_offset = spill_state.next_spill_offset;
        let data_size = total_size;
        let num_entries = spill_state.partition_buffers[partition_idx].entries.len();
        let mem_freed = spill_state.partition_buffers[partition_idx].mem_used;

        spill_state.partition_buffers[partition_idx].clear();

        // Find existing partition or create new one
        let io_state = if let Some(existing) = spill_state.find_partition_mut(partition_idx) {
            existing.add_chunk(file_offset, data_size, num_entries)?;
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.spill_bytes_written =
                    metrics.spill_bytes_written.saturating_add(data_size as u64);
                metrics.spill_chunks = metrics.spill_chunks.saturating_add(1);
                metrics.spill_max_chunks_per_partition = metrics
                    .spill_max_chunks_per_partition
                    .max(existing.chunks.len() as u64);
                metrics.spill_max_partition_bytes = metrics
                    .spill_max_partition_bytes
                    .max(existing.total_size_bytes() as u64);
            }
            existing.io_state.clone()
        } else {
            let mut new_partition = SpilledPartition::new(partition_idx);
            new_partition.add_chunk(file_offset, data_size, num_entries)?;
            if let Some(metrics) = metrics {
                metrics.spill_bytes_written =
                    metrics.spill_bytes_written.saturating_add(data_size as u64);
                metrics.spill_chunks = metrics.spill_chunks.saturating_add(1);
                metrics.spill_max_chunks_per_partition = metrics
                    .spill_max_chunks_per_partition
                    .max(new_partition.chunks.len() as u64);
                metrics.spill_max_partition_bytes = metrics
                    .spill_max_partition_bytes
                    .max(new_partition.total_size_bytes() as u64);
            }
            let io_state = new_partition.io_state.clone();
            spill_state.partitions.try_push(new_partition)?;
            io_state
        };

        io_state.set(SpillIOState::WaitingForWrite);

        let buffer_ref = Arc::new(buffer);
        let write_complete = Box::new(move |res: Result<i32, crate::CompletionError>| match res {
            Ok(_) => {
                tracing::trace!("Successfully wrote spilled partition to disk");
                io_state.set(SpillIOState::WriteComplete);
            }
            Err(e) => {
                tracing::error!("Error writing spilled partition to disk: {e:?}");
                io_state.set(SpillIOState::Error);
            }
        });

        let completion = Completion::new_write(write_complete);
        let file = spill_state.temp_file.file.clone();
        let completion = file.pwrite(file_offset, buffer_ref, completion)?;

        // Update state
        self.mem_used -= mem_freed;
        spill_state.next_spill_offset += data_size as u64;
        Ok(Some(completion))
    }

    /// Spill multiple partitions in a single I/O operation.
    /// This batches the work to reduce syscall overhead when freeing large amounts of memory.
    fn spill_multiple_partitions(
        &mut self,
        partition_indices: &[usize],
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<Completion>> {
        let mut metrics = metrics;
        if partition_indices.is_empty() {
            return Ok(None);
        }

        // If only one partition, use the simpler single-partition path
        if partition_indices.len() == 1 {
            return self.spill_partition(partition_indices[0], metrics);
        }

        let spill_state = self.spill_state.as_mut().expect("Spill state must exist");

        // Phase 1: Calculate total size and per-partition metadata
        struct PartitionMeta {
            idx: usize,
            num_entries: usize,
            data_size: usize,
            mem_freed: usize,
            entry_sizes: Vec<usize>,
        }

        let mut metas = Vec::try_with_capacity_ext(partition_indices.len())?;
        let mut total_size = 0usize;

        for &partition_idx in partition_indices {
            let partition = &spill_state.partition_buffers[partition_idx];
            if partition.is_empty() {
                continue;
            }

            let mut entry_sizes = Vec::try_with_capacity_ext(partition.entries.len())?;
            let mut partition_size = 0usize;
            for entry in &partition.entries {
                let entry_size = entry.serialized_size();
                entry_sizes
                    .push_within_capacity(entry_size)
                    .expect("entry sizes vector was preallocated");
                partition_size += varint_len(entry_size as u64) + entry_size;
            }

            metas.try_push(PartitionMeta {
                idx: partition_idx,
                num_entries: partition.entries.len(),
                data_size: partition_size,
                mem_freed: partition.mem_used,
                entry_sizes,
            })?;
            total_size += partition_size;
        }

        if metas.is_empty() {
            return Ok(None);
        }

        // Allocate single I/O buffer and serialize all partitions
        let buffer = Buffer::new_temporary(total_size);
        let buf = buffer.as_mut_slice();
        let mut offset = 0;
        let base_file_offset = spill_state.next_spill_offset;

        // Track where each partition's data starts in the buffer
        let mut partition_offsets = Vec::try_with_capacity_ext(metas.len())?;

        for meta in &metas {
            partition_offsets
                .push_within_capacity(offset)
                .expect("partition offsets vector was preallocated");
            let partition = &spill_state.partition_buffers[meta.idx];

            for (entry, &entry_size) in partition.entries.iter().zip(meta.entry_sizes.iter()) {
                offset += write_varint(&mut buf[offset..], entry_size as u64);
                offset += entry.serialize_to_slice(&mut buf[offset..]);
            }
        }

        turso_assert!(offset == total_size, "serialized size mismatch");

        // Update partition metadata and clear buffers
        let mut total_mem_freed = 0usize;
        let mut io_states = Vec::try_with_capacity_ext(metas.len())?;

        for (meta, &partition_offset) in metas.iter().zip(partition_offsets.iter()) {
            let file_offset = base_file_offset + partition_offset as u64;

            spill_state.partition_buffers[meta.idx].clear();
            total_mem_freed += meta.mem_freed;

            // Find existing partition or create new one
            let io_state = if let Some(existing) = spill_state.find_partition_mut(meta.idx) {
                existing.add_chunk(file_offset, meta.data_size, meta.num_entries)?;
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.spill_bytes_written = metrics
                        .spill_bytes_written
                        .saturating_add(meta.data_size as u64);
                    metrics.spill_chunks = metrics.spill_chunks.saturating_add(1);
                    metrics.spill_max_chunks_per_partition = metrics
                        .spill_max_chunks_per_partition
                        .max(existing.chunks.len() as u64);
                    metrics.spill_max_partition_bytes = metrics
                        .spill_max_partition_bytes
                        .max(existing.total_size_bytes() as u64);
                }
                existing.io_state.clone()
            } else {
                let mut new_partition = SpilledPartition::new(meta.idx);
                new_partition.add_chunk(file_offset, meta.data_size, meta.num_entries)?;
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.spill_bytes_written = metrics
                        .spill_bytes_written
                        .saturating_add(meta.data_size as u64);
                    metrics.spill_chunks = metrics.spill_chunks.saturating_add(1);
                    metrics.spill_max_chunks_per_partition = metrics
                        .spill_max_chunks_per_partition
                        .max(new_partition.chunks.len() as u64);
                    metrics.spill_max_partition_bytes = metrics
                        .spill_max_partition_bytes
                        .max(new_partition.total_size_bytes() as u64);
                }
                let io_state = new_partition.io_state.clone();
                spill_state.partitions.try_push(new_partition)?;
                io_state
            };

            io_state.set(SpillIOState::WaitingForWrite);
            io_states.try_push(io_state)?;
        }

        // Submit single I/O write
        let buffer_ref = Arc::new(buffer);
        let _buffer_ref_clone = buffer_ref.clone();
        let write_complete = Box::new(move |res: Result<i32, crate::CompletionError>| match res {
            Ok(_) => {
                let _buf = _buffer_ref_clone.clone();
                tracing::trace!(
                    "Successfully wrote {} batched partitions to disk",
                    io_states.len()
                );
                for io_state in &io_states {
                    io_state.set(SpillIOState::WriteComplete);
                }
            }
            Err(e) => {
                tracing::error!("Error writing batched partitions to disk: {e:?}");
                for io_state in &io_states {
                    io_state.set(SpillIOState::Error);
                }
            }
        });

        let completion = Completion::new_write(write_complete);
        let file = spill_state.temp_file.file.clone();
        let completion = file.pwrite(base_file_offset, buffer_ref, completion)?;

        // Update state
        self.mem_used -= total_mem_freed;
        spill_state.next_spill_offset += total_size as u64;
        Ok(Some(completion))
    }

    /// Spill as many whole partitions as needed to keep the incoming entry within budget.
    /// Uses batch spilling to combine multiple partitions into a single I/O operation.
    fn spill_partitions_for_entry(
        &mut self,
        entry_size: usize,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<Completion>> {
        if self.mem_used + entry_size <= self.mem_budget {
            return Ok(None);
        }

        // Collect all partitions that need to be spilled
        let mut partitions_to_spill = vec![];
        let mut projected_mem_used = self.mem_used;

        let spill_state = self.spill_state.as_ref().expect("spill state must exist");

        // Sort partitions by size (largest first) to minimize number of spills
        let mut candidates: Vec<(usize, usize)> = spill_state
            .partition_buffers
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_empty())
            .map(|(idx, p)| (idx, p.mem_used))
            .try_collect()?;
        candidates.sort_by(|a, b| b.1.cmp(&a.1)); // Sort descending by mem_used

        for (partition_idx, mem_used) in candidates {
            if projected_mem_used + entry_size <= self.mem_budget {
                break;
            }
            partitions_to_spill.try_push(partition_idx)?;
            projected_mem_used -= mem_used;
        }

        if partitions_to_spill.is_empty() {
            return Ok(None);
        }

        self.spill_multiple_partitions(&partitions_to_spill, metrics)
    }

    /// Convert a never-spilled partition buffer into in-memory buckets for probing.
    fn materialize_partition_in_memory(&mut self, partition_idx: usize) -> Result<()> {
        let spill_state = self.spill_state.as_mut().expect("spill state must exist");
        if spill_state.find_partition(partition_idx).is_some() {
            return Ok(());
        }

        let partition_buffer = &mut spill_state.partition_buffers[partition_idx];
        if partition_buffer.is_empty() {
            return Ok(());
        }

        let entries = std::mem::replace(&mut partition_buffer.entries, vec![]);
        // we don't change self.mem_used here, as these entries
        // were always in memory. we’re just changing their layout
        partition_buffer.mem_used = 0;

        let bucket_count = entries.len().next_power_of_two().max(64);
        let mut buckets: Vec<_> = (0..bucket_count).map(|_| HashBucket::new()).try_collect()?;
        for entry in entries {
            let bucket_idx = (entry.hash as usize) % bucket_count;
            buckets[bucket_idx].insert(entry)?;
        }

        let matched_bits = if self.track_matched {
            buckets
                .iter()
                .map(|b| vec![false; b.entries.len()])
                .try_collect()?
        } else {
            vec![]
        };
        let mut partition = SpilledPartition::new(partition_idx);
        partition.state = PartitionState::InMemory;
        partition.buckets = buckets;
        partition.matched_bits = matched_bits;
        partition.resident_mem = 0;
        spill_state.partitions.try_push(partition)?;
        Ok(())
    }

    /// Finalize the build phase and prepare for probing.
    /// If spilled, flushes remaining in-memory partition entries to disk.
    pub fn finalize_build(&mut self, metrics: Option<&mut HashJoinMetrics>) -> IOResultOr<()> {
        let mut metrics = metrics;
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot finalize build in unexpected state",
            { "state": format!("{:?}", self.state) }
        );

        if self.spill_state.is_some() {
            {
                // Check for pending writes from previous call
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                for spilled in &spill_state.partitions {
                    if matches!(spilled.io_state.get(), SpillIOState::WaitingForWrite) {
                        io_yield_one!(Completion::new_yield());
                    }
                }
            }
            // Determine which partitions need to spill vs stay in memory without holding
            // a mutable borrow across the spill/materialize calls.
            let mut spill_targets = vec![];
            let mut materialize_targets = vec![];
            {
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                for partition_idx in 0..spill_state.partitioning.count {
                    let partition = &spill_state.partition_buffers[partition_idx];
                    if partition.is_empty() {
                        continue;
                    }
                    if spill_state.find_partition(partition_idx).is_some() {
                        spill_targets.try_push(partition_idx)?;
                    } else {
                        materialize_targets.try_push(partition_idx)?;
                    }
                }
            }
            for partition_idx in spill_targets {
                if let Some(completion) =
                    self.spill_partition(partition_idx, metrics.as_deref_mut())?
                {
                    // Return I/O completion to caller, they will re-enter after completion
                    if !completion.finished() {
                        io_yield_one!(completion);
                    }
                }
            }
            for partition_idx in materialize_targets {
                self.materialize_partition_in_memory(partition_idx)?;
            }
        }
        self.current_spill_partition_idx = 0;
        self.state = HashTableState::Probing;
        Ok(IOResult::Done(()))
    }

    /// Probe the hash table with the given keys, returns the first matching entry if found.
    /// NOTE: Calling `probe` on a spilled table requires the relevant partition to be loaded.
    /// Returns None immediately if any probe key is NULL since NULL != NULL in SQL.
    pub fn probe(
        &mut self,
        probe_keys: Vec<Value>,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<&HashEntry>> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot probe hash table in unexpected state",
            { "state": format!("{:?}", self.state) }
        );

        // Skip probing if any key is NULL - NULL can never match anything in SQL
        if has_null_key(&probe_keys) {
            self.current_probe_keys = Some(probe_keys);
            self.current_probe_hash = None;
            return Ok(None);
        }

        // Compute hash of probe keys using collations
        let hash = {
            let key_refs: Vec<ValueRef> = probe_keys
                .iter()
                .map(|value| value.as_ref())
                .try_collect()?;
            hash_join_key(&key_refs, &self.collations)
        };
        self.current_probe_keys = Some(probe_keys);
        self.current_probe_hash = Some(hash);

        // Reset probe state
        self.probe_entry_idx = 0;

        if self.spill_state.is_some() {
            // In spilled mode, search through loaded entries from spilled partitions
            // that match this probe key's partition
            let partitioning = {
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                spill_state.partitioning
            };
            let target_partition = partitioning.index(hash);
            self.record_probe_call(metrics);
            self.touch_partition_lru(target_partition);

            let bucket_idx = {
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                let Some(partition) = spill_state.find_partition(target_partition) else {
                    return Ok(None);
                };
                if partition.buckets.is_empty() {
                    return Ok(None);
                }
                (hash as usize) % partition.buckets.len()
            };

            self.probe_bucket_idx = bucket_idx;
            self.current_spill_partition_idx = target_partition;

            let match_idx = {
                let key_refs: Vec<ValueRef> = self
                    .current_probe_keys
                    .as_ref()
                    .expect("probe keys were set")
                    .iter()
                    .map(|v| v.as_ref())
                    .try_collect()?;
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                let Some(partition) = spill_state.find_partition(target_partition) else {
                    return Ok(None);
                };
                let bucket = &partition.buckets[bucket_idx];
                let mut found = None;
                for (idx, entry) in bucket.entries.iter().enumerate() {
                    if entry.hash == hash
                        && keys_equal(&entry.key_values, &key_refs, &self.collations)
                    {
                        found = Some(idx);
                        break;
                    }
                }
                found
            };

            if let Some(idx) = match_idx {
                self.probe_entry_idx = idx + 1;
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                let Some(partition) = spill_state.find_partition(target_partition) else {
                    return Ok(None);
                };
                let bucket = &partition.buckets[bucket_idx];
                return Ok(bucket.entries.get(idx));
            }
            Ok(None)
        } else {
            // Normal mode - search in hash buckets
            self.record_probe_call(metrics);
            let bucket_idx = (hash as usize) % self.buckets.len();
            self.probe_bucket_idx = bucket_idx;
            let match_idx = {
                let key_refs: Vec<ValueRef> = self
                    .current_probe_keys
                    .as_ref()
                    .expect("probe keys were set")
                    .iter()
                    .map(|v| v.as_ref())
                    .try_collect()?;
                let bucket = &self.buckets[bucket_idx];
                let mut found = None;
                for (idx, entry) in bucket.entries.iter().enumerate() {
                    if entry.hash == hash
                        && keys_equal(&entry.key_values, &key_refs, &self.collations)
                    {
                        found = Some(idx);
                        break;
                    }
                }
                found
            };

            if let Some(idx) = match_idx {
                self.probe_entry_idx = idx + 1;
                return Ok(self.buckets[bucket_idx].entries.get(idx));
            }
            Ok(None)
        }
    }

    /// Get the next matching entry for the current probe keys.
    pub fn next_match(&mut self) -> Result<Option<&HashEntry>> {
        turso_assert!(
            self.state == HashTableState::Probing || self.state == HashTableState::GraceProcessing,
            "Cannot get next match in unexpected state",
            { "state": format!("{:?}", self.state) }
        );

        turso_assert!(self.current_probe_keys.is_some(), "probe keys must be set");
        let Some(probe_keys) = self.current_probe_keys.as_ref() else {
            return Ok(None);
        };
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).try_collect()?;
        let hash = match self.current_probe_hash {
            Some(h) => h,
            None => {
                let h = hash_join_key(&key_refs, &self.collations);
                self.current_probe_hash = Some(h);
                h
            }
        };

        if let Some(spill_state) = self.spill_state.as_ref() {
            let partition_idx = self.current_spill_partition_idx;

            turso_assert_eq!(partition_idx, self.partition_index(hash));
            let Some(partition) = spill_state.find_partition(partition_idx) else {
                return Ok(None);
            };
            if partition.buckets.is_empty() {
                return Ok(None);
            }

            let bucket = &partition.buckets[self.probe_bucket_idx];
            // Continue from where we left off
            for idx in self.probe_entry_idx..bucket.entries.len() {
                let entry = &bucket.entries[idx];
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    self.probe_entry_idx = idx + 1;
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        } else {
            // non-spilled case, seach in main buckets
            let bucket = &self.buckets[self.probe_bucket_idx];
            for idx in self.probe_entry_idx..bucket.entries.len() {
                let entry = &bucket.entries[idx];
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    // update probe entry index for next call
                    self.probe_entry_idx = idx + 1;
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        }
    }

    /// Mark the current matched entry as "matched" for outer join tracking.
    /// Must be called after a successful probe/next_match.
    pub fn mark_current_matched(&mut self) {
        if !self.track_matched {
            return;
        }
        let entry_idx = self
            .probe_entry_idx
            .checked_sub(1)
            .expect("mark_current_matched called without prior probe match");
        let bucket_idx = self.probe_bucket_idx;
        if let Some(spill_state) = &mut self.spill_state {
            let partition_idx = self.current_spill_partition_idx;
            let partition = spill_state
                .find_partition_mut(partition_idx)
                .expect("spilled partition missing during mark_current_matched");
            partition.matched_bits[bucket_idx][entry_idx] = true;
        } else {
            self.matched_bits[bucket_idx][entry_idx] = true;
        }
    }

    /// Reset all matched_bits to false. Called at the start of each outer-loop
    /// iteration so that marks from a previous iteration don't suppress NULL-fill
    /// rows in the current one.
    pub fn reset_matched_bits(&mut self) {
        if let Some(spill_state) = self.spill_state.as_mut() {
            for partition in &mut spill_state.partitions {
                for bits in &mut partition.matched_bits {
                    bits.fill(false);
                }
            }
        }
        for bits in &mut self.matched_bits {
            bits.fill(false);
        }
    }

    /// Reset the unmatched scan state to the beginning.
    pub fn begin_unmatched_scan(&mut self) {
        self.unmatched_scan_bucket = 0;
        self.unmatched_scan_entry = 0;
        self.unmatched_scan_partition = 0;
    }

    /// Advance to the next unmatched entry in the hash table.
    /// Returns the entry if found, or None when the scan is complete
    /// (or a spilled partition needs loading).
    pub fn next_unmatched(&mut self) -> Option<&HashEntry> {
        let has_spill_state = self.spill_state.is_some();
        let in_grace = self.grace_state.is_some();
        let has_pending_grace = !in_grace && self.has_grace_partitions();

        if has_spill_state {
            if in_grace {
                // During grace: scan only the partition currently owned by grace.
                return self.next_unmatched_current_grace_partition();
            }
            if has_pending_grace {
                // Before grace starts, emit unmatched rows from never-spilled in-memory
                // partitions here. Grace will handle only partitions that actually have
                // disk chunks, so skipping these would lose unmatched build rows.
                return self.next_unmatched_spilled_in_memory_only();
            }
            return self.next_unmatched_spilled();
        }
        self.next_unmatched_main_buckets()
    }

    fn next_unmatched_main_buckets(&mut self) -> Option<&HashEntry> {
        while self.unmatched_scan_bucket < self.buckets.len() {
            let bucket = &self.buckets[self.unmatched_scan_bucket];
            let matched = &self.matched_bits[self.unmatched_scan_bucket];
            while self.unmatched_scan_entry < bucket.entries.len() {
                let idx = self.unmatched_scan_entry;
                self.unmatched_scan_entry += 1;
                if !matched[idx] {
                    return Some(&bucket.entries[idx]);
                }
            }
            self.unmatched_scan_bucket += 1;
            self.unmatched_scan_entry = 0;
        }
        None
    }

    /// Advance to the next unmatched entry across spilled partitions.
    /// Returns None when a partition needs loading (caller must load and retry).
    fn next_unmatched_spilled(&mut self) -> Option<&HashEntry> {
        let spill_state = self.spill_state.as_ref()?;
        while self.unmatched_scan_partition < spill_state.partitions.len() {
            let partition = &spill_state.partitions[self.unmatched_scan_partition];
            if !partition.is_loaded() {
                // Partition not loaded yet — caller needs to load it.
                // Return None to signal we need I/O; caller will re-enter.
                return None;
            }
            while self.unmatched_scan_bucket < partition.buckets.len() {
                let bucket = &partition.buckets[self.unmatched_scan_bucket];
                let matched = &partition.matched_bits[self.unmatched_scan_bucket];
                while self.unmatched_scan_entry < bucket.entries.len() {
                    let idx = self.unmatched_scan_entry;
                    self.unmatched_scan_entry += 1;
                    if !matched[idx] {
                        return Some(&bucket.entries[idx]);
                    }
                }
                self.unmatched_scan_bucket += 1;
                self.unmatched_scan_entry = 0;
            }
            self.unmatched_scan_partition += 1;
            self.unmatched_scan_bucket = 0;
            self.unmatched_scan_entry = 0;
        }
        None
    }

    /// Scan unmatched rows from partitions that stayed resident in memory after spilling.
    /// These partitions have no disk chunks, so grace never revisits them.
    fn next_unmatched_spilled_in_memory_only(&mut self) -> Option<&HashEntry> {
        let spill_state = self.spill_state.as_ref()?;
        while self.unmatched_scan_partition < spill_state.partitions.len() {
            let partition = &spill_state.partitions[self.unmatched_scan_partition];
            if !partition.chunks.is_empty() {
                self.unmatched_scan_partition += 1;
                self.unmatched_scan_bucket = 0;
                self.unmatched_scan_entry = 0;
                continue;
            }

            turso_assert!(
                partition.is_loaded(),
                "in-memory partition unexpectedly unavailable during unmatched scan",
                { "partition_idx": partition.partition_idx }
            );

            while self.unmatched_scan_bucket < partition.buckets.len() {
                let bucket = &partition.buckets[self.unmatched_scan_bucket];
                let matched = &partition.matched_bits[self.unmatched_scan_bucket];
                while self.unmatched_scan_entry < bucket.entries.len() {
                    let idx = self.unmatched_scan_entry;
                    self.unmatched_scan_entry += 1;
                    if !matched[idx] {
                        return Some(&bucket.entries[idx]);
                    }
                }
                self.unmatched_scan_bucket += 1;
                self.unmatched_scan_entry = 0;
            }

            self.unmatched_scan_partition += 1;
            self.unmatched_scan_bucket = 0;
            self.unmatched_scan_entry = 0;
        }
        None
    }

    /// Scan unmatched entries only for the active grace partition.
    /// Grace unmatched emission happens partition-by-partition before eviction, so
    /// scanning any other loaded partition can duplicate or suppress rows.
    fn next_unmatched_current_grace_partition(&mut self) -> Option<&HashEntry> {
        let partition_idx = self.grace_state.as_ref()?.current_partition_idx()?;
        let spill_state = self.spill_state.as_ref()?;
        let partition = spill_state
            .find_partition(partition_idx)
            .expect("current grace partition missing from spill state");

        if !partition.is_loaded() {
            return None;
        }

        while self.unmatched_scan_bucket < partition.buckets.len() {
            let bucket = &partition.buckets[self.unmatched_scan_bucket];
            let matched = &partition.matched_bits[self.unmatched_scan_bucket];
            while self.unmatched_scan_entry < bucket.entries.len() {
                let idx = self.unmatched_scan_entry;
                self.unmatched_scan_entry += 1;
                if !matched[idx] {
                    return Some(&bucket.entries[idx]);
                }
            }
            self.unmatched_scan_bucket += 1;
            self.unmatched_scan_entry = 0;
        }
        None
    }

    /// Get the current partition index for unmatched scan (for spilled partition loading).
    pub fn unmatched_scan_current_partition(&self) -> Option<usize> {
        if let Some(grace_state) = self.grace_state.as_ref() {
            return grace_state.current_partition_idx();
        }
        if self.has_grace_partitions() {
            return None;
        }
        let spill_state = self.spill_state.as_ref()?;
        if self.unmatched_scan_partition < spill_state.partitions.len() {
            Some(spill_state.partitions[self.unmatched_scan_partition].partition_idx)
        } else {
            None
        }
    }

    /// Get the number of spilled partitions.
    pub fn num_partitions(&self) -> usize {
        self.spill_state
            .as_ref()
            .map(|s| s.partitions.len())
            .unwrap_or(0)
    }

    /// Check if a specific partition is loaded and ready for probing.
    pub fn is_partition_loaded(&self, partition_idx: usize) -> bool {
        self.spill_state
            .as_ref()
            .and_then(|s| s.find_partition(partition_idx))
            .is_some_and(|p| p.is_loaded())
    }

    /// Re-entrantly load spilled partitions from disk
    pub fn load_spilled_partition(
        &mut self,
        partition_idx: usize,
        mut metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<()> {
        loop {
            // to avoid holding mut borrows, split this into two phases.
            let action = {
                let spill_state = match &mut self.spill_state {
                    Some(s) => s,
                    None => return Ok(IOResult::Done(())),
                };

                let spilled = match spill_state.find_partition_mut(partition_idx) {
                    Some(p) => p,
                    None => return Ok(IOResult::Done(())),
                };
                let io_state = spilled.io_state.get();

                if unlikely(matches!(io_state, SpillIOState::Error)) {
                    return Err(
                        LimboError::InternalError("hash join spill I/O failure".into()).into(),
                    );
                }
                // Already fully loaded
                if spilled.is_loaded() {
                    SpillAction::AlreadyLoaded
                } else if matches!(io_state, SpillIOState::WaitingForRead) {
                    // We've scheduled a read, caller must wait for completion.
                    SpillAction::WaitingForIO
                } else if matches!(io_state, SpillIOState::ReadComplete) {
                    SpillAction::ParseChunk { partition_idx }
                } else {
                    match spilled.current_chunk() {
                        Some(chunk) => {
                            let read_size = chunk.size_bytes;
                            let file_offset = chunk.file_offset;
                            let is_first_load = matches!(spilled.state, PartitionState::OnDisk)
                                && spilled.current_chunk_idx == 0;
                            if is_first_load {
                                let total_entries = spilled.total_num_entries();
                                let bucket_count = total_entries.next_power_of_two().max(64);
                                spilled.buckets =
                                    (0..bucket_count).map(|_| HashBucket::new()).try_collect()?;
                                spilled.parsed_entries = 0;
                                spilled.partial_entry.clear();
                            }

                            if read_size == 0 {
                                // Empty chunk: skip it and move to the next.
                                spilled.current_chunk_idx += 1;
                                if spilled.has_more_chunks() {
                                    SpillAction::Restart
                                } else {
                                    // No more chunks, but nothing to read; mark loaded.
                                    spilled.state = PartitionState::Loaded;
                                    SpillAction::NoChunks
                                }
                            } else {
                                // Non-empty chunk, schedule a read for it.
                                let buffer_len = spilled.buffer_len.clone();
                                let read_buffer_ref = spilled.read_buffer.clone();

                                spilled.io_state.set(SpillIOState::WaitingForRead);
                                spilled.state = PartitionState::Loading;

                                SpillAction::LoadChunk {
                                    read_size,
                                    file_offset,
                                    io_state: spilled.io_state.clone(),
                                    buffer_len,
                                    read_buffer_ref,
                                }
                            }
                        }
                        None => {
                            // No chunks at all: partition is logically empty, mark as loaded.
                            spilled.state = PartitionState::Loaded;
                            SpillAction::NoChunks
                        }
                    }
                }
            };

            match action {
                SpillAction::AlreadyLoaded => {
                    self.touch_partition_lru(partition_idx);
                    return Ok(IOResult::Done(()));
                }
                SpillAction::NoChunks => {
                    self.evict_partitions_to_fit(0, partition_idx);
                    self.record_partition_resident(partition_idx, 0);
                    return Ok(IOResult::Done(()));
                }
                SpillAction::NotFound => {
                    return Ok(IOResult::Done(()));
                }
                SpillAction::ParseChunk { partition_idx } => {
                    match self.parse_partition_chunk(partition_idx, metrics.as_deref_mut())? {
                        ParseChunkResult::MoreChunks => continue,
                        ParseChunkResult::Done { resident_mem } => {
                            self.evict_partitions_to_fit(resident_mem, partition_idx);
                            self.record_partition_resident(partition_idx, resident_mem);
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
                SpillAction::WaitingForIO => {
                    io_yield_one!(Completion::new_yield());
                }
                SpillAction::Restart => {
                    // We advanced state (e.g., moved past an empty chunk or completed a chunk),
                    // so just loop again and recompute the next action.
                    continue;
                }
                SpillAction::LoadChunk {
                    read_size,
                    file_offset,
                    io_state,
                    buffer_len,
                    read_buffer_ref,
                } => {
                    let read_buffer = Arc::new(Buffer::new_temporary(read_size));
                    let read_complete = Box::new(
                        move |res: Result<(Arc<Buffer>, i32), CompletionError>| match res {
                            Ok((buf, bytes_read)) => {
                                tracing::trace!(
                                    "Completed read of spilled partition chunk: bytes_read={}",
                                    bytes_read
                                );
                                let mut persistent_buf = read_buffer_ref.write();
                                persistent_buf.clear();
                                persistent_buf
                                    .extend_from_slice(&buf.as_slice()[..bytes_read as usize]);
                                buffer_len.store(bytes_read as usize, atomic::Ordering::Release);
                                io_state.set(SpillIOState::ReadComplete);
                                None
                            }
                            Err(e) => {
                                tracing::error!("Error reading spilled partition chunk: {e:?}");
                                io_state.set(SpillIOState::Error);
                                None
                            }
                        },
                    );
                    let completion = Completion::new_read(read_buffer, read_complete);
                    let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                    let c = spill_state.temp_file.file.pread(file_offset, completion)?;
                    if !c.finished() {
                        io_yield_one!(c);
                    }
                }
            }
        }
    }

    /// Parse entries from the current chunk buffer into buckets for a partition.
    fn parse_partition_chunk(
        &mut self,
        partition_idx: usize,
        mut metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<ParseChunkResult> {
        let (has_more_chunks, resident_mem) = {
            let spill_state = self.spill_state.as_mut().expect("spill state must exist");
            let partition = spill_state
                .find_partition_mut(partition_idx)
                .expect("partition must exist for parsing");

            let data_len = partition.buffer_len();
            if let Some(metrics) = metrics.as_mut() {
                metrics.load_bytes_read = metrics.load_bytes_read.saturating_add(data_len as u64);
            }

            let data_guard = partition.read_buffer.read();
            let data = &data_guard[..data_len];
            let parse_buf = if partition.partial_entry.is_empty() {
                data.iter().copied().try_collect()?
            } else {
                let mut combined =
                    Vec::try_with_capacity_ext(partition.partial_entry.len() + data.len())?;
                combined.extend_from_slice(&partition.partial_entry);
                combined.extend_from_slice(data);
                combined
            };
            drop(data_guard);

            partition.partial_entry.clear();
            partition.buffer_len.store(0, atomic::Ordering::Release);
            partition.read_buffer.write().clear();
            partition.io_state.set(SpillIOState::None);

            let mut offset = 0;
            while offset < parse_buf.len() {
                let Some((entry_len, varint_size)) = read_varint_partial(&parse_buf[offset..])?
                else {
                    partition
                        .partial_entry
                        .try_reserve(parse_buf.len() - offset)?;
                    partition
                        .partial_entry
                        .extend_from_slice(&parse_buf[offset..]);
                    break;
                };

                let total_needed = varint_size + entry_len as usize;
                if offset + total_needed > parse_buf.len() {
                    partition
                        .partial_entry
                        .try_reserve(parse_buf.len() - offset)?;
                    partition
                        .partial_entry
                        .extend_from_slice(&parse_buf[offset..]);
                    break;
                }

                let start = offset + varint_size;
                let end = start + entry_len as usize;
                let (entry, consumed) = HashEntry::deserialize(&parse_buf[start..end])?;
                turso_assert!(
                    consumed == entry_len as usize,
                    "expected to consume entire entry"
                );

                let bucket_idx = (entry.hash as usize) % partition.buckets.len();
                partition.buckets[bucket_idx].insert(entry)?;
                partition.parsed_entries += 1;
                offset += total_needed;
            }

            partition.current_chunk_idx += 1;

            if partition.has_more_chunks() {
                (true, 0)
            } else {
                if unlikely(!partition.partial_entry.is_empty()) {
                    return Err(LimboError::Corrupt("HashEntry: truncated entry".into()));
                }
                let total_num_entries = partition.total_num_entries();
                turso_assert!(
                    partition.parsed_entries == total_num_entries,
                    "parsed entry count mismatch"
                );
                if self.track_matched && partition.matched_bits.is_empty() {
                    // Only initialize matched_bits on the first load. On subsequent
                    // reloads (after eviction), the existing bits are preserved so that
                    // probe marks set during earlier passes are not lost.
                    partition.matched_bits = partition
                        .buckets
                        .iter()
                        .map(|b| vec![false; b.entries.len()])
                        .try_collect()?;
                }
                partition.state = PartitionState::Loaded;
                partition.resident_mem = Self::partition_bucket_mem(&partition.buckets);
                // Release staging buffer to free memory now that buckets are built.
                partition.buffer_len.store(0, atomic::Ordering::SeqCst);
                partition.read_buffer.write().clear();
                (false, partition.resident_mem)
            }
        };

        if has_more_chunks {
            Ok(ParseChunkResult::MoreChunks)
        } else {
            Ok(ParseChunkResult::Done { resident_mem })
        }
    }

    /// Probe a specific partition with the given keys. The partition must be loaded first via `load_spilled_partition`.
    /// VDBE *must* call load_spilled_partition(partition_idx) and get IOResult::Done before calling probe.
    /// Returns None immediately if any probe key is NULL since NULL != NULL in SQL.
    pub fn probe_partition(
        &mut self,
        partition_idx: usize,
        probe_keys: &[Value],
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<&HashEntry>> {
        // Skip probing if any key is NULL - NULL can never match anything in SQL
        if has_null_key(probe_keys) {
            self.current_probe_keys = Some(probe_keys.iter().cloned().try_collect()?);
            self.current_probe_hash = None;
            return Ok(None);
        }

        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).try_collect()?;
        let hash = hash_join_key(&key_refs, &self.collations);

        // Store probe keys for subsequent next_match calls
        self.current_probe_keys = Some(probe_keys.iter().cloned().try_collect()?);
        self.current_probe_hash = Some(hash);

        self.record_probe_call(metrics);
        self.touch_partition_lru(partition_idx);
        let Some(spill_state) = self.spill_state.as_ref() else {
            return Ok(None);
        };
        let Some(partition) = spill_state.find_partition(partition_idx) else {
            return Ok(None);
        };

        if !partition.is_loaded() || partition.buckets.is_empty() {
            return Ok(None);
        }

        let bucket_idx = (hash as usize) % partition.buckets.len();
        let bucket = &partition.buckets[bucket_idx];

        self.probe_bucket_idx = bucket_idx;
        self.current_spill_partition_idx = partition_idx;

        for (idx, entry) in bucket.entries.iter().enumerate() {
            if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations) {
                self.probe_entry_idx = idx + 1;
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Get the partition index for a given probe key hash.
    pub fn partition_for_keys(&self, probe_keys: &[Value]) -> Result<usize> {
        turso_assert!(
            self.spill_state.is_some(),
            "partition_for_keys requires spill state"
        );
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).try_collect()?;
        let hash = hash_join_key(&key_refs, &self.collations);
        Ok(self.partition_index(hash))
    }

    /// Returns true if the hash table has spilled to disk.
    pub fn has_spilled(&self) -> bool {
        self.spill_state.is_some()
    }

    /// Approximate memory used by a partition's buckets.
    fn partition_bucket_mem(buckets: &[HashBucket]) -> usize {
        buckets.iter().map(|b| b.size_bytes()).sum()
    }

    /// Touch a resident spilled partition for LRU ordering without changing its
    /// accounted memory.
    fn touch_partition_lru(&self, partition_idx: usize) {
        let mut lru = self.loaded_partitions_lru.borrow_mut();
        if let Some(pos) = lru.iter().position(|p| *p == partition_idx) {
            lru.remove(pos);
        }
        lru.push_back(partition_idx);
    }

    /// Record that a spilled partition is resident with the given memory
    /// footprint and update LRU ordering.
    fn record_partition_resident(&mut self, partition_idx: usize, mem_used: usize) {
        if let Some(spill_state) = self.spill_state.as_mut() {
            if let Some(partition) = spill_state.find_partition_mut(partition_idx) {
                self.loaded_partitions_mem = self
                    .loaded_partitions_mem
                    .saturating_sub(partition.resident_mem);
                partition.resident_mem = mem_used;
                self.loaded_partitions_mem += mem_used;
                self.touch_partition_lru(partition_idx);
            }
        }
    }

    fn evict_partitions_to_fit(&mut self, incoming_mem: usize, protect_idx: usize) {
        while self.mem_used + self.loaded_partitions_mem + incoming_mem > self.mem_budget {
            let Some(victim_idx) = self.next_evictable(protect_idx) else {
                break;
            };

            let mut freed = 0;
            if let Some(spill_state) = self.spill_state.as_mut() {
                if let Some(victim) = spill_state.find_partition_mut(victim_idx) {
                    if matches!(victim.state, PartitionState::Loaded) {
                        freed = victim.resident_mem;
                        victim.buckets.clear();
                        victim.state = PartitionState::OnDisk;
                        victim.resident_mem = 0;
                        victim.current_chunk_idx = 0;
                        victim.buffer_len.store(0, atomic::Ordering::Release);
                        victim.read_buffer.write().clear();
                        victim.partial_entry.clear();
                        victim.parsed_entries = 0;
                        victim.io_state.set(SpillIOState::None);
                    }
                }
            }

            self.loaded_partitions_mem = self.loaded_partitions_mem.saturating_sub(freed);
        }
    }

    /// Find the next evictable resident spilled partition (LRU) that is not
    /// protected and has backing spill data.
    fn next_evictable(&mut self, protect_idx: usize) -> Option<usize> {
        let spill_state = self.spill_state.as_ref()?;

        let len = self.loaded_partitions_lru.borrow().len();
        for i in 0..len {
            let lru = self.loaded_partitions_lru.borrow();
            let candidate = lru[i];
            if candidate == protect_idx {
                continue;
            }
            if let Some(p) = spill_state.find_partition(candidate) {
                let has_disk = !p.chunks.is_empty();
                drop(lru);
                if matches!(p.state, PartitionState::Loaded) && has_disk {
                    self.loaded_partitions_lru.borrow_mut().remove(i);
                    return Some(candidate);
                }
            }
        }
        None
    }

    /// Buffer a probe row whose target build partition is on disk.
    /// Called from op_hash_probe when `probe_rowid_reg` is Some and the
    /// partition is OnDisk.
    pub fn buffer_probe_row(
        &mut self,
        key_values: Vec<Value>,
        probe_rowid: i64,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<()> {
        let spill_state = self
            .spill_state
            .as_ref()
            .expect("buffer_probe_row requires build-side spill state");
        let partitioning = spill_state.partitioning;

        // Lazily initialize probe spill state on first call
        if self.probe_spill_state.is_none() {
            self.probe_spill_state = Some(ProbeSpillState::new(
                &self.io,
                self.temp_store,
                partitioning,
                self.mem_budget / 2,
            )?);
        }

        let key_refs: Vec<ValueRef> = key_values.iter().map(|v| v.as_ref()).try_collect()?;
        let hash = hash_join_key(&key_refs, &self.collations);
        let partition_idx = partitioning.index(hash);

        let entry = HashEntry::new(hash, key_values, probe_rowid);
        let entry_size = entry.size_bytes();

        let probe_state = self
            .probe_spill_state
            .as_mut()
            .expect("probe spill state just initialized");

        probe_state.partition_buffers[partition_idx].insert(entry)?;
        probe_state.mem_used += entry_size;

        if let Some(metrics) = metrics {
            metrics.grace_probe_rows_buffered = metrics.grace_probe_rows_buffered.saturating_add(1);
        }

        // If probe buffers exceed budget, spill the largest one
        if probe_state.mem_used > probe_state.mem_budget {
            if let Some(c) = self.spill_largest_probe_partition(None)? {
                if !c.finished() {
                    return Ok(IOResult::IO(IOCompletions(c)));
                }
            }
        }

        Ok(IOResult::Done(()))
    }

    /// Spill the largest probe partition buffer to disk.
    fn spill_largest_probe_partition(
        &mut self,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<Completion>> {
        let probe_state = self
            .probe_spill_state
            .as_mut()
            .expect("probe spill state must exist");

        let largest_idx = probe_state
            .partition_buffers
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_empty())
            .max_by_key(|(_, p)| p.mem_used)
            .map(|(idx, _)| idx);

        let Some(partition_idx) = largest_idx else {
            return Ok(None);
        };

        Self::spill_probe_partition(probe_state, partition_idx, &self.io, metrics)
    }

    /// Spill a probe partition buffer to its temp file.
    fn spill_probe_partition(
        probe_state: &mut ProbeSpillState,
        partition_idx: usize,
        io: &Arc<dyn IO>,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> Result<Option<Completion>> {
        let partition = &probe_state.partition_buffers[partition_idx];
        if partition.is_empty() {
            return Ok(None);
        }

        // Calculate total serialized size
        let mut total_size = 0usize;
        let mut entry_sizes = Vec::try_with_capacity_ext(partition.entries.len())?;
        for entry in &partition.entries {
            let s = entry.serialized_size();
            entry_sizes.try_push(s)?;
            total_size += varint_len(s as u64) + s;
        }

        // Serialize into I/O buffer
        let buffer = Buffer::new_temporary(total_size);
        let buf = buffer.as_mut_slice();
        let mut offset = 0;
        for (entry, &entry_size) in partition.entries.iter().zip(entry_sizes.iter()) {
            offset += write_varint(&mut buf[offset..], entry_size as u64);
            offset += entry.serialize_to_slice(&mut buf[offset..]);
        }
        turso_assert!(offset == total_size, "serialized size mismatch");

        let file_offset = probe_state.next_spill_offset;
        let num_entries = partition.entries.len();
        let mem_freed = partition.mem_used;

        probe_state.partition_buffers[partition_idx].clear();

        // Record chunk
        let io_state = if let Some(existing) = probe_state.find_partition_mut(partition_idx) {
            existing.add_chunk(file_offset, total_size, num_entries)?;
            existing.io_state.clone()
        } else {
            let mut new_partition = SpilledPartition::new(partition_idx);
            new_partition.add_chunk(file_offset, total_size, num_entries)?;
            let io_state = new_partition.io_state.clone();
            probe_state.partitions.try_push(new_partition)?;
            io_state
        };

        if let Some(metrics) = metrics {
            metrics.probe_spill_bytes_written = metrics
                .probe_spill_bytes_written
                .saturating_add(total_size as u64);
            metrics.probe_spill_chunks = metrics.probe_spill_chunks.saturating_add(1);
        }

        io_state.set(SpillIOState::WaitingForWrite);
        let buffer_ref = Arc::new(buffer);
        let write_complete = Box::new(move |res: Result<i32, crate::CompletionError>| match res {
            Ok(_) => io_state.set(SpillIOState::WriteComplete),
            Err(e) => {
                tracing::error!("Error writing probe partition to disk: {e:?}");
                io_state.set(SpillIOState::Error);
            }
        });

        let completion = Completion::new_write(write_complete);
        let file = probe_state.temp_file.file.clone();
        let completion = file.pwrite(file_offset, buffer_ref, completion)?;

        probe_state.mem_used -= mem_freed;
        probe_state.next_spill_offset += total_size as u64;
        let _ = io;
        Ok(Some(completion))
    }

    /// Finalize probe-side spilling after the probe cursor is exhausted.
    /// Flushes remaining in-memory probe buffers for partitions that have spill
    /// chunks, keeps purely in-memory probe buffers as-is.
    pub fn finalize_probe_spill(
        &mut self,
        metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<()> {
        let mut metrics = metrics;
        let Some(probe_state) = self.probe_spill_state.as_ref() else {
            return Ok(IOResult::Done(()));
        };

        // Wait for any pending probe writes
        for spilled in &probe_state.partitions {
            if matches!(spilled.io_state.get(), SpillIOState::WaitingForWrite) {
                io_yield_one!(Completion::new_yield());
            }
        }

        // Collect partition indices that need flushing
        let partition_count = probe_state.partitioning.count;
        let mut flush_targets = vec![];
        for partition_idx in 0..partition_count {
            if probe_state.partition_buffers[partition_idx].is_empty() {
                continue;
            }
            if probe_state.find_partition(partition_idx).is_some() {
                flush_targets.try_push(partition_idx)?;
            }
        }

        for partition_idx in flush_targets {
            if let Some(c) = Self::spill_probe_partition(
                self.probe_spill_state.as_mut().expect("probe state exists"),
                partition_idx,
                &self.io,
                metrics.as_deref_mut(),
            )? {
                if !c.finished() {
                    io_yield_one!(c);
                }
            }
        }

        Ok(IOResult::Done(()))
    }

    /// Initialize grace processing. Builds partition list, frees in-memory build partitions.
    /// Returns true if there are partitions to process. No IO.
    pub fn grace_begin(&mut self) -> Result<bool> {
        if self.probe_spill_state.is_none() || self.spill_state.is_none() {
            return Ok(false);
        }

        // Build list of partitions that were spilled on the build side
        let partitions_to_process: Vec<usize> = {
            let spill_state = self.spill_state.as_ref().expect("spill state exists");
            spill_state
                .partitions
                .iter()
                .filter(|p| !p.chunks.is_empty())
                .map(|p| p.partition_idx)
                .try_collect()?
        };

        if partitions_to_process.is_empty() {
            return Ok(false);
        }

        // Free in-memory build partitions -- initial probe is done with them
        self.free_in_memory_build_partitions();

        self.grace_state = Some(GraceState {
            probe_entries: vec![],
            probe_entry_cursor: 0,
            partitions_to_process,
            partition_list_idx: 0,
            load_state: GracePartitionLoadState::NeedBuildLoad,
        });

        if let Some(probe_state) = self.probe_spill_state.as_mut() {
            for partition in &mut probe_state.partitions {
                partition.current_chunk_idx = 0;
                partition.buffer_len.store(0, atomic::Ordering::Release);
                partition.read_buffer.write().clear();
                partition.partial_entry.clear();
                partition.parsed_entries = 0;
                partition.io_state.set(SpillIOState::None);
            }
        }

        self.state = HashTableState::GraceProcessing;
        Ok(true)
    }

    /// Load build partition at current index + first probe chunk. IO-blocking.
    /// Returns true if loaded, false if partition list exhausted.
    pub fn grace_load_current_partition(
        &mut self,
        mut metrics: Option<&mut HashJoinMetrics>,
    ) -> IOResultOr<bool> {
        loop {
            let grace = self.grace_state.as_ref().expect("grace state must exist");
            if grace.partition_list_idx >= grace.partitions_to_process.len() {
                return Ok(IOResult::Done(false));
            }

            let partition_idx = grace.partitions_to_process[grace.partition_list_idx];
            match grace.load_state {
                GracePartitionLoadState::NeedBuildLoad => {
                    self.evict_all_loaded_partitions();
                    return_if_io!(
                        self.load_spilled_partition(partition_idx, metrics.as_deref_mut())
                    );

                    let grace = self.grace_state.as_mut().expect("grace state");
                    grace.probe_entries.clear();
                    grace.probe_entry_cursor = 0;
                    grace.load_state = GracePartitionLoadState::NeedProbeLoad;
                }
                GracePartitionLoadState::NeedProbeLoad => {
                    return_if_io!(self.grace_load_probe_entries(partition_idx));

                    let grace = self.grace_state.as_mut().expect("grace state");
                    grace.load_state = GracePartitionLoadState::Ready;
                    if let Some(m) = metrics.as_mut() {
                        m.grace_partitions_processed =
                            m.grace_partitions_processed.saturating_add(1);
                    }
                    return Ok(IOResult::Done(true));
                }
                GracePartitionLoadState::Ready => return Ok(IOResult::Done(true)),
            }
        }
    }

    /// Advance to next probe entry. Returns keys+rowid or None when exhausted.
    pub fn grace_next_probe_entry(&mut self) -> IOResultOr<Option<GraceProbeEntry>> {
        loop {
            let grace = self.grace_state.as_ref().expect("grace state must exist");
            if grace.probe_entry_cursor < grace.probe_entries.len() {
                let entry = &grace.probe_entries[grace.probe_entry_cursor];
                let result = GraceProbeEntry {
                    key_values: entry.key_values.clone(),
                    probe_rowid: entry.rowid,
                };
                let grace = self.grace_state.as_mut().expect("grace state");
                grace.probe_entry_cursor += 1;
                return Ok(IOResult::Done(Some(result)));
            }

            // Current probe entries exhausted, try loading more
            let grace = self.grace_state.as_ref().expect("grace state");
            let partition_idx = match grace.current_partition_idx() {
                Some(idx) => idx,
                None => return Ok(IOResult::Done(None)),
            };
            match self.grace_try_load_next_probe_chunk(partition_idx)? {
                IOResult::Done(true) => continue,
                IOResult::Done(false) => return Ok(IOResult::Done(None)),
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
    }

    /// Try to load the next probe chunk for the given partition.
    /// Returns true if more probe entries were loaded, false if exhausted.
    fn grace_try_load_next_probe_chunk(&mut self, partition_idx: usize) -> IOResultOr<bool> {
        loop {
            let Some(probe_state) = self.probe_spill_state.as_ref() else {
                return Ok(IOResult::Done(false));
            };

            let Some(spilled) = probe_state.find_partition(partition_idx) else {
                return Ok(IOResult::Done(false));
            };

            if spilled.current_chunk_idx >= spilled.chunks.len() {
                return Ok(IOResult::Done(false));
            }

            match self.grace_load_next_probe_chunk(partition_idx)? {
                IOResult::Done(true) => return Ok(IOResult::Done(true)),
                IOResult::Done(false) => continue,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
    }

    /// Evict current partition, advance to next. Returns true if more partitions. No IO.
    pub fn grace_advance_partition(&mut self) -> bool {
        self.evict_all_loaded_partitions();
        let grace = self.grace_state.as_mut().expect("grace state must exist");
        grace.partition_list_idx += 1;
        grace.probe_entries.clear();
        grace.probe_entry_cursor = 0;
        grace.load_state = GracePartitionLoadState::NeedBuildLoad;
        grace.partition_list_idx < grace.partitions_to_process.len()
    }

    /// Free all in-memory build partitions (InMemory state).
    fn free_in_memory_build_partitions(&mut self) {
        if let Some(spill_state) = self.spill_state.as_mut() {
            for partition in &mut spill_state.partitions {
                if matches!(partition.state, PartitionState::InMemory) {
                    partition.buckets.clear();
                    partition.state = PartitionState::OnDisk;
                    partition.resident_mem = 0;
                }
            }
        }
        // Also free the main buckets
        self.buckets.clear();
        self.loaded_partitions_lru.borrow_mut().clear();
        self.loaded_partitions_mem = 0;
    }

    /// Evict all currently loaded build partitions.
    fn evict_all_loaded_partitions(&mut self) {
        if let Some(spill_state) = self.spill_state.as_mut() {
            for partition in &mut spill_state.partitions {
                if matches!(partition.state, PartitionState::Loaded) && !partition.chunks.is_empty()
                {
                    partition.buckets.clear();
                    partition.state = PartitionState::OnDisk;
                    partition.resident_mem = 0;
                    partition.current_chunk_idx = 0;
                    partition.buffer_len.store(0, atomic::Ordering::Release);
                    partition.read_buffer.write().clear();
                    partition.partial_entry.clear();
                    partition.parsed_entries = 0;
                    partition.io_state.set(SpillIOState::None);
                }
            }
        }
        self.loaded_partitions_lru.borrow_mut().clear();
        self.loaded_partitions_mem = 0;
    }

    /// Load probe entries for a given partition into grace_state.probe_entries.
    /// Loads from in-memory buffers or from the first spill chunk.
    fn grace_load_probe_entries(&mut self, partition_idx: usize) -> IOResultOr<()> {
        {
            let grace = self.grace_state.as_mut().expect("grace state");
            grace.probe_entries.clear();
            grace.probe_entry_cursor = 0;
        }

        let Some(probe_state) = self.probe_spill_state.as_ref() else {
            return Ok(IOResult::Done(()));
        };

        // First: check if there are in-memory entries for this partition
        let buffer = &probe_state.partition_buffers[partition_idx];
        if !buffer.is_empty() {
            let grace = self.grace_state.as_mut().expect("grace state");
            grace.probe_entries.clone_from(&buffer.entries);
            return Ok(IOResult::Done(()));
        }

        // Check if there are spill chunks
        let Some(spilled) = probe_state.find_partition(partition_idx) else {
            return Ok(IOResult::Done(()));
        };
        if spilled.chunks.is_empty() {
            return Ok(IOResult::Done(()));
        }

        self.grace_load_next_probe_chunk(partition_idx)
            .map(|result| result.map(|_| ()))
    }

    /// Load the next probe spill chunk into grace_state.probe_entries.
    fn grace_load_next_probe_chunk(&mut self, partition_idx: usize) -> IOResultOr<bool> {
        loop {
            let action = {
                let probe_state = self.probe_spill_state.as_mut().expect("probe spill state");
                let spilled = probe_state
                    .find_partition_mut(partition_idx)
                    .expect("probe partition must exist");
                let io_state = spilled.io_state.get();

                if unlikely(matches!(io_state, SpillIOState::Error)) {
                    return Err(
                        LimboError::InternalError("grace probe spill I/O failure".into()).into(),
                    );
                }

                if matches!(io_state, SpillIOState::WaitingForRead) {
                    GraceProbeChunkAction::WaitingForIO
                } else if matches!(io_state, SpillIOState::ReadComplete) {
                    GraceProbeChunkAction::ParseChunk { partition_idx }
                } else {
                    match spilled.current_chunk() {
                        Some(chunk) if chunk.size_bytes == 0 => {
                            spilled.current_chunk_idx += 1;
                            GraceProbeChunkAction::Restart
                        }
                        Some(chunk) => {
                            spilled.io_state.set(SpillIOState::WaitingForRead);
                            GraceProbeChunkAction::LoadChunk {
                                read_size: chunk.size_bytes,
                                file_offset: chunk.file_offset,
                                io_state: spilled.io_state.clone(),
                                buffer_len: spilled.buffer_len.clone(),
                                read_buffer_ref: spilled.read_buffer.clone(),
                            }
                        }
                        None => GraceProbeChunkAction::NoMoreChunks,
                    }
                }
            };

            match action {
                GraceProbeChunkAction::WaitingForIO => {
                    io_yield_one!(Completion::new_yield());
                }
                GraceProbeChunkAction::ParseChunk { partition_idx } => {
                    return Ok(IOResult::Done(self.parse_grace_probe_chunk(partition_idx)?));
                }
                GraceProbeChunkAction::LoadChunk {
                    read_size,
                    file_offset,
                    io_state,
                    buffer_len,
                    read_buffer_ref,
                } => {
                    let read_buffer = Arc::new(Buffer::new_temporary(read_size));
                    let read_complete = Box::new(
                        move |res: Result<(Arc<Buffer>, i32), CompletionError>| match res {
                            Ok((buf, bytes_read)) => {
                                let mut persistent_buf = read_buffer_ref.write();
                                persistent_buf.clear();
                                persistent_buf
                                    .extend_from_slice(&buf.as_slice()[..bytes_read as usize]);
                                buffer_len.store(bytes_read as usize, atomic::Ordering::Release);
                                io_state.set(SpillIOState::ReadComplete);
                                None
                            }
                            Err(e) => {
                                mark_unlikely();
                                tracing::error!("Error reading probe chunk: {e:?}");
                                io_state.set(SpillIOState::Error);
                                None
                            }
                        },
                    );

                    let completion = Completion::new_read(read_buffer, read_complete);
                    let probe_state = self.probe_spill_state.as_ref().expect("probe spill state");
                    let c = probe_state.temp_file.file.pread(file_offset, completion)?;
                    if !c.finished() {
                        io_yield_one!(c);
                    }
                }
                GraceProbeChunkAction::Restart => continue,
                GraceProbeChunkAction::NoMoreChunks => return Ok(IOResult::Done(false)),
            }
        }
    }

    fn parse_grace_probe_chunk(&mut self, partition_idx: usize) -> Result<bool> {
        let entries = {
            let probe_state = self.probe_spill_state.as_mut().expect("probe spill state");
            let partition = probe_state
                .find_partition_mut(partition_idx)
                .expect("probe partition must exist for parsing");
            let chunk = partition
                .current_chunk()
                .expect("probe chunk must exist while parsing");
            let expected_entries = chunk.num_entries;
            let data_len = partition.buffer_len();

            let data_guard = partition.read_buffer.read();
            let data = &data_guard[..data_len];
            let mut entries = Vec::try_with_capacity_ext(expected_entries)?;
            let mut offset = 0;
            while offset < data_len {
                let Some((entry_len, varint_size)) = read_varint_partial(&data[offset..])? else {
                    return Err(LimboError::InternalError(
                        "truncated grace probe spill chunk header".into(),
                    ));
                };
                let total_needed = varint_size + entry_len as usize;
                if offset + total_needed > data_len {
                    return Err(LimboError::InternalError(
                        "truncated grace probe spill chunk payload".into(),
                    ));
                }
                let start = offset + varint_size;
                let end = start + entry_len as usize;
                let (entry, _consumed) = HashEntry::deserialize(&data[start..end])?;
                entries.try_push(entry)?;
                offset += total_needed;
            }
            drop(data_guard);

            if unlikely(entries.len() != expected_entries) {
                return Err(LimboError::InternalError(format!(
                    "grace probe spill chunk entry count mismatch: expected {expected_entries}, got {}",
                    entries.len()
                )));
            }

            partition.buffer_len.store(0, atomic::Ordering::Release);
            partition.read_buffer.write().clear();
            partition.io_state.set(SpillIOState::None);
            partition.current_chunk_idx += 1;
            entries
        };

        let grace = self.grace_state.as_mut().expect("grace state");
        grace.probe_entries = entries;
        grace.probe_entry_cursor = 0;
        Ok(!grace.probe_entries.is_empty())
    }

    /// Returns true if grace processing has any spilled partitions to process.
    pub fn has_grace_partitions(&self) -> bool {
        self.probe_spill_state.is_some()
    }

    /// Close the hash table and free resources.
    pub fn close(&mut self) {
        self.state = HashTableState::Closed;
        self.buckets.clear();
        self.num_entries = 0;
        self.mem_used = 0;
        self.loaded_partitions_lru.borrow_mut().clear();
        self.loaded_partitions_mem = 0;
        let _ = self.spill_state.take();
        self.probe_spill_state = None;
        self.grace_state = None;
    }
}

#[cfg(test)]
#[path = "../tests/unit/vdbe/hash_table/hashtests.rs"]
mod hashtests;
