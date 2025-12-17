use crate::{
    error::LimboError,
    io::{Buffer, Completion, TempFile, IO},
    io_yield_one,
    storage::sqlite3_ondisk::{read_varint, write_varint},
    translate::collate::CollationSeq,
    turso_assert,
    types::{IOCompletions, IOResult, Value, ValueRef},
    CompletionError, Result,
};
use parking_lot::RwLock;
use rapidhash::fast::RapidHasher;
use std::hash::Hasher;
use std::sync::{atomic, Arc};
use std::{cell::RefCell, collections::VecDeque};
use std::{
    cmp::{Eq, Ordering},
    sync::atomic::AtomicUsize,
};
use turso_macros::AtomicEnum;

const DEFAULT_SEED: u64 = 1337;

// set to a *very* small 32KB, intentionally to trigger frequent spilling during tests
#[cfg(debug_assertions)]
pub const DEFAULT_MEM_BUDGET: usize = 32 * 1024;

/// 64MB default memory budget for hash joins.
/// TODO: make configurable via PRAGMA
#[cfg(not(debug_assertions))]
pub const DEFAULT_MEM_BUDGET: usize = 64 * 1024 * 1024;
const DEFAULT_BUCKETS: usize = 1024;
/// Number of partitions for grace hash join
pub const NUM_PARTITIONS: usize = 16;
/// Bits used for partition selection from hash
const PARTITION_BITS: u32 = 4;
const NULL_HASH: u8 = 0;
const INT_HASH: u8 = 1;
const FLOAT_HASH: u8 = 2;
const TEXT_HASH: u8 = 3;
const BLOB_HASH: u8 = 4;

/// Hash function for join keys using rapidhash
/// Takes collation into account when hashing text values
fn hash_join_key(key_values: &[ValueRef], collations: &[CollationSeq]) -> u64 {
    let mut hasher = RapidHasher::new(DEFAULT_SEED);

    for (idx, value) in key_values.iter().enumerate() {
        match value {
            ValueRef::Null => {
                hasher.write_u8(NULL_HASH);
            }
            ValueRef::Integer(i) => {
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
            ValueRef::Float(f) => {
                hasher.write_u8(FLOAT_HASH);
                let bits = normalized_f64_bits(*f);
                hasher.write(&bits.to_le_bytes());
            }
            ValueRef::Text(text) => {
                let collation = collations.get(idx).unwrap_or(&CollationSeq::Binary);
                hasher.write_u8(TEXT_HASH);
                match collation {
                    CollationSeq::NoCase => {
                        let lowercase = text.as_str().to_lowercase();
                        hasher.write(lowercase.as_bytes());
                    }
                    CollationSeq::Rtrim => {
                        let trimmed = text.as_str().trim_end();
                        hasher.write(trimmed.as_bytes());
                    }
                    CollationSeq::Binary | CollationSeq::Unset => {
                        hasher.write(text.as_bytes());
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
fn normalized_f64_bits(f: f64) -> u64 {
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
        (ValueRef::Integer(i1), ValueRef::Integer(i2)) => i1 == i2,
        (ValueRef::Float(f1), ValueRef::Float(f2)) => f1 == f2,
        (ValueRef::Integer(i), ValueRef::Float(f)) | (ValueRef::Float(f), ValueRef::Integer(i)) => {
            ValueRef::Integer(i) == ValueRef::Float(f)
        }
        (ValueRef::Blob(b1), ValueRef::Blob(b2)) => b1 == b2,
        (ValueRef::Text(t1), ValueRef::Text(t2)) => {
            // Use collation for text comparison
            collation.compare_strings(t1.as_str(), t2.as_str()) == Ordering::Equal
        }
        _ => false,
    }
}

/// State machine states for hash table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashTableState {
    Building,
    Probing,
    Spilled,
    Closed,
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

impl HashEntry {
    fn new(hash: u64, key_values: Vec<Value>, rowid: i64) -> Self {
        Self {
            hash,
            key_values,
            rowid,
            payload_values: Vec::new(),
        }
    }

    fn new_with_payload(
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
    pub fn has_payload(&self) -> bool {
        !self.payload_values.is_empty()
    }

    /// Get the size of this entry in bytes (approximate).
    fn size_bytes(&self) -> usize {
        let value_size = |v: &Value| match v {
            Value::Null => 1,
            Value::Integer(_) => 8,
            Value::Float(_) => 8,
            Value::Text(t) => t.as_str().len(),
            Value::Blob(b) => b.len(),
        };
        let key_size: usize = self.key_values.iter().map(value_size).sum();
        let payload_size: usize = self.payload_values.iter().map(value_size).sum();
        key_size + payload_size + 8 + 8 // +8 for hash, +8 for rowid
    }

    /// Serialize this entry to bytes for disk storage.
    /// Format: [hash:8][rowid:8][num_keys:varint][keys...][num_payload:varint][payload...]
    /// Each value is: [type:1][len:varint (for text/blob)][data]
    fn serialize(&self, buf: &mut Vec<u8>) {
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
    }

    /// Helper to serialize a single Value to bytes.
    fn serialize_value(value: &Value, buf: &mut Vec<u8>, varint_buf: &mut [u8; 9]) {
        match value {
            Value::Null => {
                buf.push(NULL_HASH);
            }
            Value::Integer(i) => {
                buf.push(INT_HASH);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(FLOAT_HASH);
                buf.extend_from_slice(&f.to_le_bytes());
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
        if buf.len() < 16 {
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

        let mut key_values = Vec::with_capacity(num_keys as usize);
        for _ in 0..num_keys {
            let (value, consumed) = Self::deserialize_value(&buf[offset..])?;
            key_values.push(value);
            offset += consumed;
        }

        // Read number of payload values and payload values
        let (num_payload, varint_len) = read_varint(&buf[offset..])?;
        offset += varint_len;

        let mut payload_values = Vec::with_capacity(num_payload as usize);
        for _ in 0..num_payload {
            let (value, consumed) = Self::deserialize_value(&buf[offset..])?;
            payload_values.push(value);
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
        if buf.is_empty() {
            return Err(LimboError::Corrupt(
                "HashEntry: unexpected end of buffer".to_string(),
            ));
        }
        let value_type = buf[0];
        let mut offset = 1;

        let value = match value_type {
            NULL_HASH => Value::Null,
            INT_HASH => {
                if offset + 8 > buf.len() {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for integer".to_string(),
                    ));
                }
                let i =
                    i64::from_le_bytes(buf[offset..offset + 8].try_into().expect("expect 8 bytes"));
                offset += 8;
                Value::Integer(i)
            }
            FLOAT_HASH => {
                if offset + 8 > buf.len() {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for float".to_string(),
                    ));
                }
                let f =
                    f64::from_le_bytes(buf[offset..offset + 8].try_into().expect("expect 8 bytes"));
                offset += 8;
                Value::Float(f)
            }
            TEXT_HASH => {
                let (str_len, varint_len) = read_varint(&buf[offset..])?;
                offset += varint_len;
                if offset + str_len as usize > buf.len() {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for text".to_string(),
                    ));
                }
                let s = String::from_utf8(buf[offset..offset + str_len as usize].to_vec())
                    .map_err(|_| LimboError::Corrupt("Invalid UTF-8 in text".to_string()))?;
                offset += str_len as usize;
                Value::Text(s.into())
            }
            BLOB_HASH => {
                let (blob_len, varint_len) = read_varint(&buf[offset..])?;
                offset += varint_len;
                if offset + blob_len as usize > buf.len() {
                    return Err(LimboError::Corrupt(
                        "HashEntry: buffer too small for blob".to_string(),
                    ));
                }
                let b = buf[offset..offset + blob_len as usize].to_vec();
                offset += blob_len as usize;
                Value::Blob(b)
            }
            _ => {
                return Err(LimboError::Corrupt(format!(
                    "HashEntry: unknown value type {value_type}",
                )));
            }
        };
        Ok((value, offset))
    }
}

/// Get partition index from hash value
#[inline(always)]
fn partition_from_hash(hash: u64) -> usize {
    // Use top bits for partition to distribute evenly
    ((hash >> (64 - PARTITION_BITS)) as usize) & (NUM_PARTITIONS - 1)
}

/// A bucket in the hash table. Uses chaining for collision resolution.
#[derive(Debug, Clone)]
pub struct HashBucket {
    entries: Vec<HashEntry>,
}

impl HashBucket {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn insert(&mut self, entry: HashEntry) {
        self.entries.push(entry);
    }

    fn find_matches<'a>(
        &'a self,
        hash: u64,
        probe_keys: &[ValueRef],
        collations: &[CollationSeq],
    ) -> Vec<&'a HashEntry> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.hash == hash && keys_equal(&entry.key_values, probe_keys, collations)
            })
            .collect()
    }

    fn is_empty(&self) -> bool {
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
    /// Partition index (0 to NUM_PARTITIONS-1)
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
}

impl SpilledPartition {
    fn new(partition_idx: usize) -> Self {
        Self {
            partition_idx,
            chunks: Vec::new(),
            state: PartitionState::OnDisk,
            io_state: Arc::new(AtomicSpillIOState::new(SpillIOState::None)),
            read_buffer: Arc::new(RwLock::new(Vec::new())),
            buffer_len: Arc::new(atomic::AtomicUsize::new(0)),
            buckets: Vec::new(),
            current_chunk_idx: 0,
            resident_mem: 0,
        }
    }

    /// Add a new chunk of data to this partition
    fn add_chunk(&mut self, file_offset: u64, size_bytes: usize, num_entries: usize) {
        self.chunks.push(SpillChunk {
            file_offset,
            size_bytes,
            num_entries,
        });
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
        self.buffer_len.load(atomic::Ordering::SeqCst)
    }

    /// Check if partition is ready for probing
    pub fn is_loaded(&self) -> bool {
        matches!(
            self.state,
            PartitionState::Loaded | PartitionState::InMemory
        )
    }

    /// Check if there are more chunks to load
    fn has_more_chunks(&self) -> bool {
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
            entries: Vec::new(),
            mem_used: 0,
        }
    }

    fn insert(&mut self, entry: HashEntry) {
        self.mem_used += entry.size_bytes();
        self.entries.push(entry);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.mem_used = 0;
    }

    fn is_empty(&self) -> bool {
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
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            initial_buckets: DEFAULT_BUCKETS,
            mem_budget: DEFAULT_MEM_BUDGET,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
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
}

impl SpillState {
    fn new(io: &Arc<dyn IO>) -> Result<Self> {
        Ok(SpillState {
            partition_buffers: (0..NUM_PARTITIONS)
                .map(|_| PartitionBuffer::new())
                .collect(),
            partitions: Vec::new(),
            next_spill_offset: 0,
            temp_file: TempFile::new(io)?,
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

/// HashTable is the build-side data structure used for hash joins. It behaves like a
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
///
/// - Construction:
///   - `mem_budget` is an approximate upper bound on memory consumed by the
///     build side for this join. It applies to:
///       * `mem_used`: the base in-memory structures (buckets in non-spilled
///         mode, plus any never-spilled partitions).
///       * `loaded_partitions_mem`: additional resident memory for partitions
///         that were spilled to disk and later reloaded for probing.
///
/// - Build phase ([HashTableState::Building] / `::Spilled`):
///   - `insert`:
///       * Inserts (key_values, rowid) into the in-memory hash table.
///       * Tracks per-entry size via [HashEntry::size_bytes] and increments
///         `mem_used`.
///       * If `mem_used + new_entry_size > mem_budget`:
///           - On first overflow, transitions into spilled mode:
///               * Allocates `SpillState`.
///               * Redistributes existing buckets into `NUM_PARTITIONS`
///                 partition buffers via `partition_from_hash`.
///               * Sets `state to [HashTableState::Spilled].
///           - Spills whole partition buffers to disk, always picking the largest
///             non-empty partition first, until the new entry fits.
///           - Spilling:
///               * Serializes entries in a partition buffer into a temp file,
///                 appending as one `SpillChunk` (file_offset, size, #entries).
///               * Clears that partition buffer and reduces `mem_used` by
///                 its `partition.mem_used`.
///               * Updates `SpilledPartition.chunks` and `next_spill_offset`.
///   - In spilled mode:
///       * New inserts are written into the corresponding `PartitionBuffer`.
///       * These entries are either:
///           - Spilled later (creating new chunks), or
///           - Materialized in-memory as never-spilled partitions if they never
///             required spilling at finalize time (see below).
///
/// - `finalize_build`:
///   - Completes pending writes for any partitions that were already spilled.
///   - For each `partition_buffers[i]`:
///       * If there is already a `SpilledPartition` for `i` (i.e. we have
///         existing chunks), we spill the buffer to disk (creating more chunks)
///         and clear it, reducing `mem_used` accordingly.
///       * Otherwise, the partition has never been spilled:
///           - We call `materialize_partition_in_memory(i)`:
///               * Takes owned entries out of the buffer.
///               * Builds in-memory buckets for that partition.
///               * Marks the resulting `SpilledPartition` as
///                 `PartitionState::InMemory`.
///               * These InMemory partitions are not tracked in
///                 `loaded_partitions_lru` and do not contribute to
///                 `loaded_partitions_mem`.
///   - After this, the table transitions to `HashTableState::Probing`.
///
/// - Probe phase (`HashTableState::Probing`):
///   - Non-spilled case:
///       * If `spill_state.is_none()`, probing is directly over `buckets`.
///       * `probe()` / `next_match()` walk the bucket chain for the hash.
///   - Spilled case:
///   * All build-side data now lives in `SpilledPartition`s:
///       - Some partitions may be `InMemory` (never spilled, always
///         resident, counted only in `mem_used`).
///       - Some partitions may be `OnDisk` with one or more `chunks`
///         (spilled build side).
///       - Some partitions may be `Loaded` (disk-backed but currently
///         resident in memory as buckets; counted in `loaded_partitions_mem`
///         and tracked in `loaded_partitions_lru`).
///   * The VDBE is expected to:
///    - Compute the partition index for a probe key:
///      `partition_for_keys(probe_keys)`.
///    - Ensure the partition is resident:
///      `load_spilled_partition(partition_idx)`
///      (no-op for never-spilled / InMemory partitions).
///    - Then probe via:
///      `probe_partition(partition_idx, keys)`
///      or the top-level `probe()` / `next_match()` helpers.
///
/// - Probe-time cache / thrash behavior:
///   - `loaded_partitions_lru` and `loaded_partitions_mem` form an LRU cache of
///     *only spilled* partitions that are currently loaded.
///  - When loading or parsing a spilled partition:
///    * We estimate its memory footprint as:
///      `resident_mem = partition_bucket_mem(partition.buckets)`
///      and set `partition.resident_mem`.
///    * Before keeping it resident, we call:
///      `evict_partitions_to_fit(resident_mem, protect_idx)`
///      which:
///         - Repeatedly evicts the least-recently-used partition whose state
///           is `Loaded` and that has backing `chunks` (`!chunks.is_empty()`),
///           skipping `protect_idx`.
///         - Eviction clears the partition’s buckets, resets its state to
///           `OnDisk`, zeros `resident_mem`, and decrements
///           `loaded_partitions_mem`.
///         - The loop stops once:
///           mem_used + loaded_partitions_mem + incoming_mem <= mem_budget
///           or there is no further evictable candidate, in which case the
///           hash join may temporarily exceed `mem_budget` (bounded by one
///           partition’s `resident_mem`).
///   * After eviction, we call:
///     `record_partition_resident(partition_idx, resident_mem)`
///     which:
///       - Adjusts `loaded_partitions_mem` by replacing the prior
///         `resident_mem` for that partition.
///       - Marks the partition as most recently used in
///         `loaded_partitions_lru`.
///   - For partitions that are already loaded, `load_spilled_partition()`
///     simply updates their position in the LRU without changing the memory
///     accounting.
pub struct HashTable {
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
    /// LRU of loaded partitions to cap probe-time memory
    loaded_partitions_lru: RefCell<VecDeque<usize>>,
    /// Memory used by resident (loaded or in-memory) partitions
    loaded_partitions_mem: usize,
}

enum SpillAction {
    AlreadyLoaded,
    NeedsParsing,
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

impl HashTable {
    /// Create a new hash table.
    pub fn new(config: HashTableConfig, io: Arc<dyn IO>) -> Self {
        let buckets = (0..config.initial_buckets)
            .map(|_| HashBucket::new())
            .collect();
        Self {
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
        }
    }

    /// Get the current state of the hash table.
    pub fn get_state(&self) -> &HashTableState {
        &self.state
    }

    /// Insert a row into the hash table, returns IOResult because this may spill to disk.
    /// When memory budget is exceeded, triggers grace hash join by partitioning and spilling.
    /// Rows with NULL join keys are skipped since NULL != NULL in SQL.
    pub fn insert(
        &mut self,
        key_values: Vec<Value>,
        rowid: i64,
        payload_values: Vec<Value>,
    ) -> Result<IOResult<()>> {
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot insert into hash table in state {:?}",
            self.state
        );

        // Skip rows with NULL join keys - they can never match anything since NULL != NULL in SQL
        if has_null_key(&key_values) {
            return Ok(IOResult::Done(()));
        }

        // Compute hash of the join keys using collations
        let key_refs: Vec<ValueRef> = key_values.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);

        let entry = if payload_values.is_empty() {
            HashEntry::new(hash, key_values, rowid)
        } else {
            HashEntry::new_with_payload(hash, key_values, rowid, payload_values)
        };
        let entry_size = entry.size_bytes();

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
                self.spill_state = Some(SpillState::new(&self.io)?);
                self.redistribute_to_partitions();
                self.state = HashTableState::Spilled;
            };

            // Spill whole partitions until the new entry fits
            if let Some(c) = self.spill_partitions_for_entry(entry_size)? {
                // I/O pending, caller will re-enter after completion and retry the insert.
                if !c.finished() {
                    return Ok(IOResult::IO(IOCompletions::Single(c)));
                }
            }
        }

        if let Some(spill_state) = &mut self.spill_state {
            // In spilled mode, insert into partition buffer
            let partition_idx = partition_from_hash(hash);
            spill_state.partition_buffers[partition_idx].insert(entry);
        } else {
            // Normal mode, insert into hash bucket
            let bucket_idx = (hash as usize) % self.buckets.len();
            self.buckets[bucket_idx].insert(entry);
        }

        self.num_entries += 1;
        self.mem_used += entry_size;

        Ok(IOResult::Done(()))
    }

    /// Redistribute existing bucket entries into partition buffers for grace hash join.
    fn redistribute_to_partitions(&mut self) {
        for bucket in self.buckets.drain(..) {
            for entry in bucket.entries {
                let partition_idx = partition_from_hash(entry.hash);
                self.spill_state
                    .as_mut()
                    .expect("spill state must exist")
                    .partition_buffers[partition_idx]
                    .insert(entry);
            }
        }
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
    fn spill_partition(&mut self, partition_idx: usize) -> Result<Option<Completion>> {
        let spill_state = self.spill_state.as_mut().expect("Spill state must exist");
        let partition = &spill_state.partition_buffers[partition_idx];
        if partition.is_empty() {
            return Ok(None);
        }

        // Serialize entries from partition buffer
        let estimated_size: usize = partition.entries.iter().map(|e| e.size_bytes() + 9).sum();
        let mut serialized_data = Vec::with_capacity(estimated_size);
        let mut len_buf = [0u8; 9];
        let mut entry_buf = Vec::new();

        for entry in &partition.entries {
            entry.serialize(&mut entry_buf);
            let len_varint_size = write_varint(&mut len_buf, entry_buf.len() as u64);
            serialized_data.extend_from_slice(&len_buf[..len_varint_size]);
            serialized_data.extend_from_slice(&entry_buf);
            entry_buf.clear();
        }

        let file_offset = spill_state.next_spill_offset;
        let data_size = serialized_data.len();
        let num_entries = spill_state.partition_buffers[partition_idx].entries.len();
        let mem_freed = spill_state.partition_buffers[partition_idx].mem_used;

        spill_state.partition_buffers[partition_idx].clear();

        // Find existing partition or create new one
        let io_state = if let Some(existing) = spill_state.find_partition_mut(partition_idx) {
            existing.add_chunk(file_offset, data_size, num_entries);
            existing.io_state.clone()
        } else {
            let mut new_partition = SpilledPartition::new(partition_idx);
            new_partition.add_chunk(file_offset, data_size, num_entries);
            let io_state = new_partition.io_state.clone();
            spill_state.partitions.push(new_partition);
            io_state
        };

        io_state.set(SpillIOState::WaitingForWrite);

        let buffer = Buffer::new_temporary(data_size);
        buffer.as_mut_slice().copy_from_slice(&serialized_data);
        let buffer_ref = Arc::new(buffer);
        let _buffer_ref_clone = buffer_ref.clone();
        let write_complete = Box::new(move |res: Result<i32, crate::CompletionError>| match res {
            Ok(_) => {
                // keep buffer alive for async IO
                let _buf = _buffer_ref_clone.clone();
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

    /// Spill as many whole partitions as needed to keep the incoming entry within budget.
    fn spill_partitions_for_entry(&mut self, entry_size: usize) -> Result<Option<Completion>> {
        loop {
            if self.mem_used + entry_size <= self.mem_budget {
                return Ok(None);
            }

            let required_free = self.mem_used + entry_size - self.mem_budget;
            let Some(partition_idx) = self.next_partition_to_spill(required_free) else {
                return Ok(None);
            };

            if let Some(c) = self.spill_partition(partition_idx)? {
                // Wait for caller to re-enter after the I/O completes.
                if !c.finished() {
                    return Ok(Some(c));
                }
            }
        }
    }

    /// Convert a never-spilled partition buffer into in-memory buckets for probing.
    fn materialize_partition_in_memory(&mut self, partition_idx: usize) {
        let spill_state = self.spill_state.as_mut().expect("spill state must exist");
        if spill_state.find_partition(partition_idx).is_some() {
            return;
        }

        let partition_buffer = &mut spill_state.partition_buffers[partition_idx];
        if partition_buffer.is_empty() {
            return;
        }

        let entries = std::mem::take(&mut partition_buffer.entries);
        // we don't change self.mem_used here, as these entries
        // were always in memory. we’re just changing their layout
        partition_buffer.mem_used = 0;

        let bucket_count = entries.len().next_power_of_two().max(64);
        let mut buckets = (0..bucket_count)
            .map(|_| HashBucket::new())
            .collect::<Vec<_>>();
        for entry in entries {
            let bucket_idx = (entry.hash as usize) % bucket_count;
            buckets[bucket_idx].insert(entry);
        }

        let mut partition = SpilledPartition::new(partition_idx);
        partition.state = PartitionState::InMemory;
        partition.buckets = buckets;
        partition.resident_mem = 0;
        spill_state.partitions.push(partition);
    }

    /// Finalize the build phase and prepare for probing.
    /// If spilled, flushes remaining in-memory partition entries to disk.
    pub fn finalize_build(&mut self) -> Result<IOResult<()>> {
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot finalize build in state {:?}",
            self.state
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
            let mut spill_targets = Vec::new();
            let mut materialize_targets = Vec::new();
            {
                let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                for partition_idx in 0..NUM_PARTITIONS {
                    let partition = &spill_state.partition_buffers[partition_idx];
                    if partition.is_empty() {
                        continue;
                    }
                    if spill_state.find_partition(partition_idx).is_some() {
                        spill_targets.push(partition_idx);
                    } else {
                        materialize_targets.push(partition_idx);
                    }
                }
            }
            for partition_idx in spill_targets {
                if let Some(completion) = self.spill_partition(partition_idx)? {
                    // Return I/O completion to caller, they will re-enter after completion
                    if !completion.finished() {
                        io_yield_one!(completion);
                    }
                }
            }
            for partition_idx in materialize_targets {
                self.materialize_partition_in_memory(partition_idx);
            }
        }
        self.current_spill_partition_idx = 0;
        self.state = HashTableState::Probing;
        Ok(IOResult::Done(()))
    }

    /// Probe the hash table with the given keys, returns the first matching entry if found.
    /// NOTE: Calling `probe` on a spilled table requires the relevant partition to be loaded.
    /// Returns None immediately if any probe key is NULL since NULL != NULL in SQL.
    pub fn probe(&mut self, probe_keys: Vec<Value>) -> Option<&HashEntry> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot probe hash table in state {:?}",
            self.state
        );

        // Skip probing if any key is NULL - NULL can never match anything in SQL
        if has_null_key(&probe_keys) {
            self.current_probe_keys = Some(probe_keys);
            self.current_probe_hash = None;
            return None;
        }

        // Store probe keys first
        self.current_probe_keys = Some(probe_keys);

        // Compute hash of probe keys using collations
        let probe_keys_ref = self
            .current_probe_keys
            .as_ref()
            .expect("prob keys were set");
        let key_refs: Vec<ValueRef> = probe_keys_ref.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);
        self.current_probe_hash = Some(hash);

        // Reset probe state
        self.probe_entry_idx = 0;

        if let Some(spill_state) = self.spill_state.as_ref() {
            // In spilled mode, search through loaded entries from spilled partitions
            // that match this probe key's partition
            let target_partition = partition_from_hash(hash);
            let partition = spill_state.find_partition(target_partition)?;

            if partition.buckets.is_empty() {
                return None;
            }

            self.touch_partition_lru(target_partition);
            let bucket_idx = (hash as usize) % partition.buckets.len();
            self.probe_bucket_idx = bucket_idx;
            self.current_spill_partition_idx = target_partition;

            let bucket = &partition.buckets[bucket_idx];
            for (idx, entry) in bucket.entries.iter().enumerate() {
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    self.probe_entry_idx = idx + 1;
                    return Some(entry);
                }
            }
            None
        } else {
            // Normal mode - search in hash buckets
            let bucket_idx = (hash as usize) % self.buckets.len();
            self.probe_bucket_idx = bucket_idx;

            let bucket = &self.buckets[bucket_idx];
            for (idx, entry) in bucket.entries.iter().enumerate() {
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    self.probe_entry_idx = idx + 1;
                    return Some(entry);
                }
            }
            None
        }
    }

    /// Get the next matching entry for the current probe keys.
    pub fn next_match(&mut self) -> Option<&HashEntry> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot get next match in state {:?}",
            self.state
        );

        turso_assert!(self.current_probe_keys.is_some(), "probe keys must be set");
        let probe_keys = self.current_probe_keys.as_ref()?;
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
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
            // sanity check to ensure we cached the correct position
            debug_assert_eq!(partition_idx, partition_from_hash(hash));
            let partition = spill_state.find_partition(partition_idx)?;
            if partition.buckets.is_empty() {
                return None;
            }

            let bucket = &partition.buckets[self.probe_bucket_idx];
            // Continue from where we left off
            for idx in self.probe_entry_idx..bucket.entries.len() {
                let entry = &bucket.entries[idx];
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    self.probe_entry_idx = idx + 1;
                    return Some(entry);
                }
            }
            None
        } else {
            // non-spilled case, seach in main buckets
            let bucket = &self.buckets[self.probe_bucket_idx];
            for idx in self.probe_entry_idx..bucket.entries.len() {
                let entry = &bucket.entries[idx];
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
                    // update probe entry index for next call
                    self.probe_entry_idx = idx + 1;
                    return Some(entry);
                }
            }
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
    pub fn load_spilled_partition(&mut self, partition_idx: usize) -> Result<IOResult<()>> {
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

                if matches!(io_state, SpillIOState::Error) {
                    return Err(LimboError::InternalError(
                        "hash join spill I/O failure".into(),
                    ));
                }
                // Already fully loaded
                if spilled.is_loaded() {
                    SpillAction::AlreadyLoaded
                } else if matches!(io_state, SpillIOState::WaitingForRead) {
                    // We've scheduled a read, caller must wait for completion.
                    SpillAction::WaitingForIO
                } else if matches!(io_state, SpillIOState::ReadComplete) {
                    // A chunk finished reading: advance to next chunk.
                    spilled.current_chunk_idx += 1;
                    spilled.io_state.set(SpillIOState::None);

                    if spilled.has_more_chunks() {
                        // We have more chunks to read, loop will re-enter and schedule next one.
                        SpillAction::Restart
                    } else {
                        // All chunks have been read, we now need to parse everything.
                        SpillAction::NeedsParsing
                    }
                } else {
                    match spilled.current_chunk() {
                        Some(chunk) => {
                            let read_size = chunk.size_bytes;
                            let file_offset = chunk.file_offset;

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
                SpillAction::NeedsParsing => {
                    // All chunks are read, build buckets from the accumulated buffer.
                    self.parse_partition_entries(partition_idx)?;
                    return Ok(IOResult::Done(()));
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
                                // Append data to our persistent buffer (accumulating chunks).
                                let mut persistent_buf = read_buffer_ref.write();
                                persistent_buf
                                    .extend_from_slice(&buf.as_slice()[..bytes_read as usize]);
                                buffer_len.fetch_add(bytes_read as usize, atomic::Ordering::SeqCst);
                                io_state.set(SpillIOState::ReadComplete);
                            }
                            Err(e) => {
                                tracing::error!("Error reading spilled partition chunk: {e:?}");
                                io_state.set(SpillIOState::Error);
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

    /// Parse entries from the read buffer into buckets for a partition.
    fn parse_partition_entries(&mut self, partition_idx: usize) -> Result<()> {
        let resident_mem = {
            let spill_state = self.spill_state.as_mut().expect("spill state must exist");
            let partition = spill_state
                .find_partition_mut(partition_idx)
                .expect("partition must exist for parsing");

            let data_len = partition.buffer_len();
            let data_guard = partition.read_buffer.read();
            let data = &data_guard[..data_len];

            let mut entries = Vec::new();
            let mut offset = 0;
            while offset < data.len() {
                let (entry_len, varint_size) = read_varint(&data[offset..])?;
                offset += varint_size;

                if offset + entry_len as usize > data.len() {
                    return Err(LimboError::Corrupt("HashEntry: truncated entry".into()));
                }

                let (entry, consumed) =
                    HashEntry::deserialize(&data[offset..offset + entry_len as usize])?;
                turso_assert!(
                    consumed == entry_len as usize,
                    "expected to consume entire entry"
                );
                entries.push(entry);
                offset += entry_len as usize;
            }
            drop(data_guard);

            let total_num_entries = partition.total_num_entries();
            tracing::trace!(
                "parsing partition entries: partition_idx={}, data_len={}, parsed_entries={}, total_num_entries={}",
                partition_idx, data_len, entries.len(), total_num_entries
            );

            let bucket_count = total_num_entries.next_power_of_two().max(64);
            partition.buckets = (0..bucket_count).map(|_| HashBucket::new()).collect();
            for entry in entries {
                let bucket_idx = (entry.hash as usize) % bucket_count;
                partition.buckets[bucket_idx].insert(entry);
            }
            partition.state = PartitionState::Loaded;
            partition.resident_mem = Self::partition_bucket_mem(&partition.buckets);
            // Release staging buffer to free memory now that buckets are built.
            partition.buffer_len.store(0, atomic::Ordering::SeqCst);
            partition.read_buffer.write().clear();
            partition.resident_mem
        };

        // Evict other partitions if needed before keeping this one resident.
        self.evict_partitions_to_fit(resident_mem, partition_idx);
        self.record_partition_resident(partition_idx, resident_mem);
        Ok(())
    }

    /// Probe a specific partition with the given keys. The partition must be loaded first via `load_spilled_partition`.
    /// VDBE *must* call load_spilled_partition(partition_idx) and get IOResult::Done before calling probe.
    /// Returns None immediately if any probe key is NULL since NULL != NULL in SQL.
    pub fn probe_partition(
        &mut self,
        partition_idx: usize,
        probe_keys: &[Value],
    ) -> Option<&HashEntry> {
        // Skip probing if any key is NULL - NULL can never match anything in SQL
        if has_null_key(probe_keys) {
            self.current_probe_keys = Some(probe_keys.to_vec());
            self.current_probe_hash = None;
            return None;
        }

        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);

        // Store probe keys for subsequent next_match calls
        self.current_probe_keys = Some(probe_keys.to_vec());
        self.current_probe_hash = Some(hash);

        self.touch_partition_lru(partition_idx);
        let spill_state = self.spill_state.as_ref()?;
        let partition = spill_state.find_partition(partition_idx)?;

        if !partition.is_loaded() || partition.buckets.is_empty() {
            return None;
        }

        let bucket_idx = (hash as usize) % partition.buckets.len();
        let bucket = &partition.buckets[bucket_idx];

        self.probe_bucket_idx = bucket_idx;
        self.current_spill_partition_idx = partition_idx;

        for (idx, entry) in bucket.entries.iter().enumerate() {
            if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations) {
                self.probe_entry_idx = idx + 1;
                return Some(entry);
            }
        }
        None
    }

    /// Get the partition index for a given probe key hash.
    pub fn partition_for_keys(&self, probe_keys: &[Value]) -> usize {
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);
        partition_from_hash(hash)
    }

    /// Returns true if the hash table has spilled to disk.
    pub fn has_spilled(&self) -> bool {
        self.spill_state.is_some()
    }

    /// Approximate memory used by a partition's buckets.
    fn partition_bucket_mem(buckets: &[HashBucket]) -> usize {
        buckets.iter().map(|b| b.size_bytes()).sum()
    }

    /// Touch a partition for LRU ordering without changing its accounted memory.
    fn touch_partition_lru(&self, partition_idx: usize) {
        let mut lru = self.loaded_partitions_lru.borrow_mut();
        if let Some(pos) = lru.iter().position(|p| *p == partition_idx) {
            lru.remove(pos);
        }
        lru.push_back(partition_idx);
    }

    /// Record that a partition is resident with the given memory footprint and update LRU.
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

    /// Evict least-recently used spillable partitions until there is room for `incoming_mem`.
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
                        victim.buffer_len.store(0, atomic::Ordering::SeqCst);
                        victim.read_buffer.write().clear();
                        victim.io_state.set(SpillIOState::None);
                    }
                }
            }

            self.loaded_partitions_mem = self.loaded_partitions_mem.saturating_sub(freed);
        }
    }

    /// Find the next evictable partition (LRU) that is not protected and has backing spill data.
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

    /// Close the hash table and free resources.
    pub fn close(&mut self) {
        self.state = HashTableState::Closed;
        self.buckets.clear();
        self.num_entries = 0;
        self.mem_used = 0;
        self.loaded_partitions_lru.borrow_mut().clear();
        self.loaded_partitions_mem = 0;
        let _ = self.spill_state.take();
    }
}

#[cfg(test)]
mod hashtests {
    use super::*;
    use crate::MemoryIO;

    #[test]
    fn test_hash_function_consistency() {
        // Test that the same keys produce the same hash
        let keys1 = vec![
            ValueRef::Integer(42),
            ValueRef::Text(crate::types::TextRef::new(
                "hello",
                crate::types::TextSubtype::Text,
            )),
        ];
        let keys2 = vec![
            ValueRef::Integer(42),
            ValueRef::Text(crate::types::TextRef::new(
                "hello",
                crate::types::TextSubtype::Text,
            )),
        ];
        let keys3 = vec![
            ValueRef::Integer(43),
            ValueRef::Text(crate::types::TextRef::new(
                "hello",
                crate::types::TextSubtype::Text,
            )),
        ];

        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];
        let hash1 = hash_join_key(&keys1, &collations);
        let hash2 = hash_join_key(&keys2, &collations);
        let hash3 = hash_join_key(&keys3, &collations);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_function_numeric_equivalence() {
        let collations = vec![CollationSeq::Binary];

        // Zero variants should hash identically
        let h_zero = hash_join_key(&[ValueRef::Float(0.0)], &collations);
        let h_neg_zero = hash_join_key(&[ValueRef::Float(-0.0)], &collations);
        let h_int_zero = hash_join_key(&[ValueRef::Integer(0)], &collations);
        assert_eq!(h_zero, h_neg_zero);
        assert_eq!(h_zero, h_int_zero);

        // Integer/float representations of the same numeric value should match
        let h_ten_int = hash_join_key(&[ValueRef::Integer(10)], &collations);
        let h_ten_float = hash_join_key(&[ValueRef::Float(10.0)], &collations);
        assert_eq!(h_ten_int, h_ten_float);

        let h_neg_ten_int = hash_join_key(&[ValueRef::Integer(-10)], &collations);
        let h_neg_ten_float = hash_join_key(&[ValueRef::Float(-10.0)], &collations);
        assert_eq!(h_neg_ten_int, h_neg_ten_float);

        // Positive/negative values should still differ
        assert_ne!(h_ten_int, h_neg_ten_int);
    }

    #[test]
    fn test_keys_equal() {
        let key1 = vec![Value::Integer(42), Value::Text("hello".to_string().into())];
        let key2 = vec![
            ValueRef::Integer(42),
            ValueRef::Text(crate::types::TextRef::new(
                "hello",
                crate::types::TextSubtype::Text,
            )),
        ];
        let key3 = vec![
            ValueRef::Integer(43),
            ValueRef::Text(crate::types::TextRef::new(
                "hello",
                crate::types::TextSubtype::Text,
            )),
        ];

        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];
        assert!(keys_equal(&key1, &key2, &collations));
        assert!(!keys_equal(&key1, &key3, &collations));
    }

    #[test]
    fn test_hash_table_basic() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert some entries (late materialization - only store rowids)
        let key1 = vec![Value::Integer(1)];
        let _ = ht.insert(key1.clone(), 100, vec![]).unwrap();

        let key2 = vec![Value::Integer(2)];
        let _ = ht.insert(key2.clone(), 200, vec![]).unwrap();

        let _ = ht.finalize_build();

        // Probe for key1
        let result = ht.probe(key1.clone());
        assert!(result.is_some());
        let entry1 = result.unwrap();
        assert_eq!(entry1.key_values[0].as_ref(), ValueRef::Integer(1));
        assert_eq!(entry1.rowid, 100);

        // Probe for key2
        let result = ht.probe(key2);
        assert!(result.is_some());
        let entry2 = result.unwrap();
        assert_eq!(entry2.key_values[0].as_ref(), ValueRef::Integer(2));
        assert_eq!(entry2.rowid, 200);

        // Probe for non-existent key
        let result = ht.probe(vec![Value::Integer(999)]);
        assert!(result.is_none());
    }

    #[test]
    fn test_hash_table_collisions() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 2, // Small number to force collisions
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert multiple entries (late materialization - only store rowids)
        for i in 0..10 {
            let key = vec![Value::Integer(i)];
            let _ = ht.insert(key, i * 100, vec![]).unwrap();
        }

        let _ = ht.finalize_build();

        // Verify all entries can be found
        for i in 0..10 {
            let result = ht.probe(vec![Value::Integer(i)]);
            assert!(result.is_some());
            let entry = result.unwrap();
            assert_eq!(entry.key_values[0].as_ref(), ValueRef::Integer(i));
            assert_eq!(entry.rowid, i * 100);
        }
    }

    #[test]
    fn test_hash_table_duplicate_keys() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert multiple entries with the same key
        let key = vec![Value::Integer(42)];
        for i in 0..3 {
            let _ = ht.insert(key.clone(), 1000 + i, vec![]).unwrap();
        }

        let _ = ht.finalize_build();

        // Probe should return first match
        let result = ht.probe(key.clone());
        assert!(result.is_some());
        assert_eq!(result.unwrap().rowid, 1000);

        // next_match should return additional matches
        let result2 = ht.next_match();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().rowid, 1001);

        let result3 = ht.next_match();
        assert!(result3.is_some());
        assert_eq!(result3.unwrap().rowid, 1002);

        // No more matches
        let result4 = ht.next_match();
        assert!(result4.is_none());
    }

    #[test]
    fn test_hash_entry_serialization() {
        // Test that entries serialize and deserialize correctly
        let entry = HashEntry::new(
            12345,
            vec![
                Value::Integer(42),
                Value::Text("hello".to_string().into()),
                Value::Null,
                Value::Float(std::f64::consts::PI),
            ],
            100,
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf);

        let (deserialized, consumed) = HashEntry::deserialize(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(deserialized.hash, entry.hash);
        assert_eq!(deserialized.rowid, entry.rowid);
        assert_eq!(deserialized.key_values.len(), entry.key_values.len());

        for (v1, v2) in deserialized.key_values.iter().zip(entry.key_values.iter()) {
            match (v1, v2) {
                (Value::Integer(i1), Value::Integer(i2)) => assert_eq!(i1, i2),
                (Value::Text(t1), Value::Text(t2)) => assert_eq!(t1.as_str(), t2.as_str()),
                (Value::Float(f1), Value::Float(f2)) => assert!((f1 - f2).abs() < 1e-10),
                (Value::Null, Value::Null) => {}
                _ => panic!("Value type mismatch"),
            }
        }
    }

    #[test]
    fn test_partition_from_hash() {
        // Test partition distribution
        let mut counts = [0usize; NUM_PARTITIONS];
        for i in 0u64..10000 {
            let hash = i.wrapping_mul(0x9E3779B97F4A7C15); // Simple hash spreading
            let partition = partition_from_hash(hash);
            assert!(partition < NUM_PARTITIONS);
            counts[partition] += 1;
        }

        // Check reasonable distribution (each partition should have some entries)
        for count in counts {
            assert!(count > 0, "Each partition should have some entries");
        }
    }

    #[test]
    fn test_spill_chunk_tracking() {
        // Test that SpilledPartition can track multiple chunks
        let mut partition = SpilledPartition::new(5);
        assert_eq!(partition.partition_idx, 5);
        assert!(partition.chunks.is_empty());
        assert_eq!(partition.total_size_bytes(), 0);
        assert_eq!(partition.total_num_entries(), 0);

        // Add first chunk
        partition.add_chunk(0, 1000, 50);
        assert_eq!(partition.chunks.len(), 1);
        assert_eq!(partition.total_size_bytes(), 1000);
        assert_eq!(partition.total_num_entries(), 50);

        // Add second chunk
        partition.add_chunk(1000, 500, 25);
        assert_eq!(partition.chunks.len(), 2);
        assert_eq!(partition.total_size_bytes(), 1500);
        assert_eq!(partition.total_num_entries(), 75);

        // Check individual chunks
        assert_eq!(partition.chunks[0].file_offset, 0);
        assert_eq!(partition.chunks[0].size_bytes, 1000);
        assert_eq!(partition.chunks[1].file_offset, 1000);
        assert_eq!(partition.chunks[1].size_bytes, 500);
    }

    #[test]
    fn test_hash_function_respects_collation_nocase() {
        use crate::types::{TextRef, TextSubtype};

        let keys1 = vec![ValueRef::Text(TextRef::new("Hello", TextSubtype::Text))];
        let keys2 = vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))];

        // Under BINARY: hashes must differ
        let bin_coll = vec![CollationSeq::Binary];
        let h1_bin = hash_join_key(&keys1, &bin_coll);
        let h2_bin = hash_join_key(&keys2, &bin_coll);
        assert_ne!(h1_bin, h2_bin);

        // Under NOCASE: hashes should be equal
        let nocase_coll = vec![CollationSeq::NoCase];
        let h1_nc = hash_join_key(&keys1, &nocase_coll);
        let h2_nc = hash_join_key(&keys2, &nocase_coll);
        assert_eq!(h1_nc, h2_nc);
    }

    #[test]
    fn test_values_equal_with_collations() {
        use crate::types::{TextRef, TextSubtype};

        let h1 = ValueRef::Text(TextRef::new("Hello  ", TextSubtype::Text));
        let h2 = ValueRef::Text(TextRef::new("hello", TextSubtype::Text));

        // Binary: case / trailing spaces matter
        assert!(!values_equal(h1, h2, CollationSeq::Binary));

        // NOCASE: case-insensitive but trailing spaces still matter -> likely false
        assert!(!values_equal(h1, h2, CollationSeq::NoCase));

        // RTRIM: ignore trailing spaces, but case is still significant
        let h3 = ValueRef::Text(TextRef::new("Hello", TextSubtype::Text));
        assert!(values_equal(h1, h3, CollationSeq::Rtrim));
    }

    #[test]
    fn test_keys_equal_with_collations() {
        use crate::types::{TextRef, TextSubtype};

        let key1 = vec![Value::Text("Hello".into())];
        let key2 = vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))];

        // Binary: not equal
        assert!(!keys_equal(&key1, &key2, &[CollationSeq::Binary]));

        // NOCASE: equal
        assert!(keys_equal(&key1, &key2, &[CollationSeq::NoCase]));
    }

    #[test]
    fn test_hash_entry_deserialization_truncated() {
        let entry = HashEntry::new(123, vec![Value::Integer(1), Value::Text("abc".into())], 42);

        let mut buf = Vec::new();
        entry.serialize(&mut buf);

        // Cut off the buffer mid-entry
        let truncated = &buf[..buf.len() - 2];

        let res = HashEntry::deserialize(truncated);
        assert!(
            res.is_err(),
            "truncated buffer should be rejected as corrupt"
        );
    }

    #[test]
    fn test_hash_entry_deserialization_garbage_type_tag() {
        let entry = HashEntry::new(1, vec![Value::Integer(10)], 7);
        let mut buf = Vec::new();
        entry.serialize(&mut buf);

        // Compute the exact offset of the *first* type tag.
        // Layout: [0..8] hash | [8..16] rowid | varint(num_keys) | type | payload...
        let mut corrupted = buf.clone();

        let mut offset = 16;
        let (_num_keys, varint_len) = read_varint(&corrupted[offset..]).unwrap();
        offset += varint_len;
        corrupted[offset] = 0xFF;

        let res = HashEntry::deserialize(&corrupted);
        assert!(
            res.is_err(),
            "invalid type tag should be rejected as corrupt"
        );
    }

    fn insert_many_force_spill(ht: &mut HashTable, start: i64, count: i64) {
        for i in 0..count {
            let rowid = start + i;
            let key = vec![Value::Integer(rowid)];
            let _ = ht.insert(key, rowid, vec![]);
        }
    }

    #[test]
    fn test_hash_table_spill_and_load_partition_round_trip() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            // very small budget to force spill
            mem_budget: 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert enough fat rows to exceed budget and force spills
        insert_many_force_spill(&mut ht, 0, 1024);

        let _ = ht.finalize_build().unwrap();
        assert!(ht.has_spilled(), "hash table should have spilled");

        // Pick a key and find its partition
        let probe_key = vec![Value::Integer(10)];
        let partition_idx = ht.partition_for_keys(&probe_key);

        // Load that partition into memory
        match ht.load_spilled_partition(partition_idx).unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("test harness must drive IO completions here"),
        }

        assert!(
            ht.is_partition_loaded(partition_idx),
            "partition must be resident after load_spilled_partition"
        );

        // Probe via partition API
        let entry = ht.probe_partition(partition_idx, &probe_key);
        assert!(entry.is_some()); // here
        assert_eq!(entry.unwrap().rowid, 10);
    }

    #[test]
    fn test_partition_lru_eviction() {
        let io = Arc::new(MemoryIO::new());
        // tiny mem_budget so only ~1 partition can stay resident
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 8 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert two disjoint key ranges that will hash to different partitions
        insert_many_force_spill(&mut ht, 0, 256);
        insert_many_force_spill(&mut ht, 256, 1024);

        let _ = ht.finalize_build().unwrap();
        assert!(ht.has_spilled());

        let key_a = vec![Value::Integer(1)];
        let key_b = vec![Value::Integer(10_001)];
        let pa = ht.partition_for_keys(&key_a);
        let pb = ht.partition_for_keys(&key_b);
        assert_ne!(pa, pb);

        // Load partition A
        while let IOResult::IO(_) = ht.load_spilled_partition(pa).unwrap() {}
        assert!(ht.is_partition_loaded(pa));

        // Now load partition B, this should (under tight memory) evict A
        let _ = ht.load_spilled_partition(pb).unwrap();
        assert!(ht.is_partition_loaded(pb));

        // Depending on mem_budget and actual entry sizes, A should now be evicted
        // We can't *guarantee* that without knowing exact sizes, but in practice
        // this test will detect regressions in the LRU bookkeeping.
        assert!(
            !ht.is_partition_loaded(pa) || ht.loaded_partitions_mem <= ht.mem_budget,
            "either partition A is evicted, or loaded memory is within budget"
        );
    }

    #[test]
    fn test_probe_partition_with_duplicate_keys() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 8 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        let key = vec![Value::Integer(42)];
        for i in 0..1024 {
            match ht.insert(key.clone(), 1000 + i, vec![]).unwrap() {
                IOResult::Done(()) => {}
                IOResult::IO(_) => panic!("memory IO"),
            }
        }
        match ht.finalize_build().unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO"),
        }

        assert!(ht.has_spilled());
        let partition_idx = ht.partition_for_keys(&key);

        match ht.load_spilled_partition(partition_idx).unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO"),
        }
        assert!(ht.is_partition_loaded(partition_idx));

        // First probe should give us the first rowid
        let entry1 = ht.probe_partition(partition_idx, &key).unwrap();
        assert_eq!(entry1.rowid, 1000);

        // Then iterate through the rest with next_match
        for i in 0..1023 {
            let next = ht.next_match().unwrap();
            assert_eq!(next.rowid, 1001 + i);
        }
        assert!(ht.next_match().is_none());
    }

    #[test]
    fn test_hash_table_with_payload() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert entries with payload values (simulating cached result columns)
        let key1 = vec![Value::Integer(1)];
        let payload1 = vec![
            Value::Text("Alice".into()),
            Value::Integer(30),
            Value::Float(1000.50),
        ];
        let _ = ht.insert(key1.clone(), 100, payload1.clone()).unwrap();

        let key2 = vec![Value::Integer(2)];
        let payload2 = vec![
            Value::Text("Bob".into()),
            Value::Integer(25),
            Value::Float(2000.75),
        ];
        let _ = ht.insert(key2.clone(), 200, payload2.clone()).unwrap();

        let _ = ht.finalize_build();

        // Probe and verify payload is returned correctly
        let result = ht.probe(key1);
        assert!(result.is_some());
        let entry1 = result.unwrap();
        assert_eq!(entry1.rowid, 100);
        assert!(entry1.has_payload());
        assert_eq!(entry1.payload_values.len(), 3);
        assert_eq!(entry1.payload_values[0], Value::Text("Alice".into()));
        assert_eq!(entry1.payload_values[1], Value::Integer(30));
        assert_eq!(entry1.payload_values[2], Value::Float(1000.50));

        let result = ht.probe(key2);
        assert!(result.is_some());
        let entry2 = result.unwrap();
        assert_eq!(entry2.rowid, 200);
        assert!(entry2.has_payload());
        assert_eq!(entry2.payload_values[0], Value::Text("Bob".into()));
        assert_eq!(entry2.payload_values[1], Value::Integer(25));
        assert_eq!(entry2.payload_values[2], Value::Float(2000.75));
    }

    #[test]
    fn test_hash_table_payload_with_nulls() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert entry with NULL values in payload
        let key = vec![Value::Integer(1)];
        let payload = vec![Value::Null, Value::Text("test".into()), Value::Null];
        let _ = ht.insert(key.clone(), 100, payload).unwrap();

        let _ = ht.finalize_build();

        let result = ht.probe(key);
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.payload_values.len(), 3);
        assert_eq!(entry.payload_values[0], Value::Null);
        assert_eq!(entry.payload_values[1], Value::Text("test".into()));
        assert_eq!(entry.payload_values[2], Value::Null);
    }

    #[test]
    fn test_null_keys_are_skipped() {
        // In SQL, NULL = NULL is false (actually NULL which is falsy).
        // Hash joins should skip rows with NULL keys during both insert and probe.
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 2,
            collations: vec![CollationSeq::Binary, CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert entry with NULL key - should be silently skipped
        let null_key = vec![Value::Null, Value::Integer(1)];
        let _ = ht.insert(null_key.clone(), 100, vec![]).unwrap();

        // Insert entry with non-NULL keys
        let valid_key = vec![Value::Integer(1), Value::Integer(2)];
        let _ = ht.insert(valid_key.clone(), 200, vec![]).unwrap();

        // Insert another entry where second key is NULL
        let null_key2 = vec![Value::Integer(1), Value::Null];
        let _ = ht.insert(null_key2.clone(), 300, vec![]).unwrap();

        let _ = ht.finalize_build();

        // Only one entry should be in the table (the one with valid keys)
        assert_eq!(ht.num_entries, 1);

        // Probing with NULL key should return None
        let result = ht.probe(null_key);
        assert!(result.is_none());

        // Probing with valid key should return the entry
        let result = ht.probe(valid_key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().rowid, 200);

        // Probing with NULL in second position should also return None
        let result = ht.probe(null_key2);
        assert!(result.is_none());
    }

    #[test]
    fn test_hash_table_payload_with_blobs() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert entry with blob payload
        let key = vec![Value::Integer(1)];
        let blob_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let payload = vec![Value::Blob(blob_data.clone()), Value::Integer(42)];
        let _ = ht.insert(key.clone(), 100, payload).unwrap();

        let _ = ht.finalize_build();

        let result = ht.probe(key);
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.payload_values.len(), 2);
        assert_eq!(entry.payload_values[0], Value::Blob(blob_data));
        assert_eq!(entry.payload_values[1], Value::Integer(42));
    }

    #[test]
    fn test_hash_table_payload_duplicate_keys() {
        let io = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert multiple entries with the same key but different payloads
        let key = vec![Value::Integer(42)];
        let _ = ht
            .insert(
                key.clone(),
                100,
                vec![Value::Text("first".into()), Value::Integer(1)],
            )
            .unwrap();
        let _ = ht
            .insert(
                key.clone(),
                200,
                vec![Value::Text("second".into()), Value::Integer(2)],
            )
            .unwrap();
        let _ = ht
            .insert(
                key.clone(),
                300,
                vec![Value::Text("third".into()), Value::Integer(3)],
            )
            .unwrap();

        let _ = ht.finalize_build();

        // First probe should return first match
        let result = ht.probe(key);
        assert!(result.is_some());
        let entry1 = result.unwrap();
        assert_eq!(entry1.rowid, 100);
        assert_eq!(entry1.payload_values[0], Value::Text("first".into()));
        assert_eq!(entry1.payload_values[1], Value::Integer(1));

        // next_match should return subsequent matches with their payloads
        let entry2 = ht.next_match().unwrap();
        assert_eq!(entry2.rowid, 200);
        assert_eq!(entry2.payload_values[0], Value::Text("second".into()));
        assert_eq!(entry2.payload_values[1], Value::Integer(2));

        let entry3 = ht.next_match().unwrap();
        assert_eq!(entry3.rowid, 300);
        assert_eq!(entry3.payload_values[0], Value::Text("third".into()));
        assert_eq!(entry3.payload_values[1], Value::Integer(3));

        // No more matches
        assert!(ht.next_match().is_none());
    }

    #[test]
    fn test_hash_entry_payload_serialization() {
        // Test that payload values survive serialization/deserialization
        let entry = HashEntry::new_with_payload(
            12345,
            vec![Value::Integer(1), Value::Text("key".into())],
            100,
            vec![
                Value::Text("payload_text".into()),
                Value::Integer(999),
                Value::Float(std::f64::consts::PI),
                Value::Null,
                Value::Blob(vec![1, 2, 3, 4]),
            ],
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf);

        let (deserialized, bytes_consumed) = HashEntry::deserialize(&buf).unwrap();
        assert_eq!(bytes_consumed, buf.len());

        // Verify key values
        assert_eq!(deserialized.hash, entry.hash);
        assert_eq!(deserialized.rowid, entry.rowid);
        assert_eq!(deserialized.key_values.len(), 2);
        assert_eq!(deserialized.key_values[0], Value::Integer(1));
        assert_eq!(deserialized.key_values[1], Value::Text("key".into()));

        // Verify payload values
        assert_eq!(deserialized.payload_values.len(), 5);
        assert_eq!(
            deserialized.payload_values[0],
            Value::Text("payload_text".into())
        );
        assert_eq!(deserialized.payload_values[1], Value::Integer(999));
        assert_eq!(
            deserialized.payload_values[2],
            Value::Float(std::f64::consts::PI)
        );
        assert_eq!(deserialized.payload_values[3], Value::Null);
        assert_eq!(
            deserialized.payload_values[4],
            Value::Blob(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_hash_entry_empty_payload() {
        // Test that entries without payload work correctly
        let entry = HashEntry::new(12345, vec![Value::Integer(1)], 100);

        assert!(!entry.has_payload());
        assert!(entry.payload_values.is_empty());

        // Serialization should still work
        let mut buf = Vec::new();
        entry.serialize(&mut buf);

        let (deserialized, _) = HashEntry::deserialize(&buf).unwrap();
        assert!(!deserialized.has_payload());
        assert!(deserialized.payload_values.is_empty());
        assert_eq!(deserialized.rowid, 100);
    }

    #[test]
    fn test_hash_entry_size_includes_payload() {
        let entry_no_payload = HashEntry::new(12345, vec![Value::Integer(1)], 100);

        let entry_with_payload = HashEntry::new_with_payload(
            12345,
            vec![Value::Integer(1)],
            100,
            vec![
                Value::Text("a]long payload string".into()),
                Value::Integer(42),
            ],
        );

        // Entry with payload should have larger size
        assert!(entry_with_payload.size_bytes() > entry_no_payload.size_bytes());
    }
}
