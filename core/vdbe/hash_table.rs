use crate::{
    error::LimboError,
    io::{Buffer, Completion, File, OpenFlags, IO},
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
use std::{
    cmp::{Eq, Ordering},
    sync::atomic::AtomicUsize,
};
use tempfile;
use turso_macros::AtomicEnum;

const DEFAULT_SEED: u64 = 1337;

// set to just 32KB to intentionally trigger more spilling when testing
#[cfg(debug_assertions)]
pub const DEFAULT_MEM_BUDGET: usize = 1024 * 32;

/// 64MB default memory budget for hash joins.
/// TODO: configurable via PRAGMA
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

#[inline(always)]
fn canonicalize_f64(f: f64) -> f64 {
    if f == 0.0 {
        0.0 // collapse -0.0 to +0.0
    } else {
        f
    }
}
/// Hash function for join keys using xxHash
/// Takes collation into account when hashing text values
fn hash_join_key(key_values: &[ValueRef], collations: &[CollationSeq]) -> u64 {
    let mut hasher = RapidHasher::new(DEFAULT_SEED);

    for (idx, value) in key_values.iter().enumerate() {
        match value {
            ValueRef::Null => {
                hasher.write_u8(NULL_HASH);
            }
            ValueRef::Integer(i) => {
                hasher.write_u8(INT_HASH);
                hasher.write_i64(*i);
            }
            ValueRef::Float(f) => {
                hasher.write_u8(FLOAT_HASH);
                hasher.write(&f.to_le_bytes());
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
                        // Binary collation: hash as-is
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
fn values_equal(v1: ValueRef, v2: ValueRef, collation: CollationSeq) -> bool {
    match (v1, v2) {
        (ValueRef::Null, ValueRef::Null) => true,
        (ValueRef::Integer(i1), ValueRef::Integer(i2)) => i1 == i2,
        (ValueRef::Float(f1), ValueRef::Float(f2)) => f1 == f2,
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
    /// During probe phase, we'll use SeekRowid to fetch the full row.
    pub rowid: i64,
}

impl HashEntry {
    fn new(hash: u64, key_values: Vec<Value>, rowid: i64) -> Self {
        Self {
            hash,
            key_values,
            rowid,
        }
    }

    /// Get the size of this entry in bytes (approximate).
    fn size_bytes(&self) -> usize {
        let key_size: usize = self
            .key_values
            .iter()
            .map(|v| match v {
                Value::Null => 1,
                Value::Integer(_) => 8,
                Value::Float(_) => 8,
                Value::Text(t) => t.as_str().len(),
                Value::Blob(b) => b.len(),
            })
            .sum();
        key_size + 8 + 8 // +8 for hash, +8 for rowid
    }

    /// Serialize this entry to bytes for disk storage.
    /// Format: [hash:8][rowid:8][num_keys:varint][key1_type:1][key1_len:varint][key1_data]...
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.hash.to_le_bytes());
        buf.extend_from_slice(&self.rowid.to_le_bytes());

        // Write number of keys
        let num_keys = self.key_values.len() as u64;
        let varint_buf = &mut [0u8; 9];
        let len = write_varint(varint_buf, num_keys);
        buf.extend_from_slice(&varint_buf[..len]);

        // Write each key value
        for value in &self.key_values {
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
    }

    /// Deserialize an entry from bytes.
    /// Returns (entry, bytes_consumed) or error.
    fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 16 {
            return Err(LimboError::Corrupt(
                "HashEntry: buffer too small for header".to_string(),
            ));
        }

        let hash = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let rowid = i64::from_le_bytes(buf[8..16].try_into().unwrap());
        let mut offset = 16;

        // Read number of keys
        let (num_keys, varint_len) = read_varint(&buf[offset..])?;
        offset += varint_len;

        let mut key_values = Vec::with_capacity(num_keys as usize);
        for _ in 0..num_keys {
            if offset >= buf.len() {
                return Err(LimboError::Corrupt(
                    "HashEntry: unexpected end of buffer".to_string(),
                ));
            }
            let value_type = buf[offset];
            offset += 1;

            let value = match value_type {
                NULL_HASH => Value::Null,
                INT_HASH => {
                    if offset + 8 > buf.len() {
                        return Err(LimboError::Corrupt(
                            "HashEntry: buffer too small for integer".to_string(),
                        ));
                    }
                    let i = i64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    Value::Integer(i)
                }
                FLOAT_HASH => {
                    if offset + 8 > buf.len() {
                        return Err(LimboError::Corrupt(
                            "HashEntry: buffer too small for float".to_string(),
                        ));
                    }
                    let f = f64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
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
            key_values.push(value);
        }

        Ok((
            Self {
                hash,
                key_values,
                rowid,
            },
            offset,
        ))
    }
}

/// Get partition index from hash value
#[inline]
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

/// Temporary file for spilled partitions.
struct TempFile {
    _temp_dir: tempfile::TempDir,
    file: Arc<dyn File>,
}

impl core::ops::Deref for TempFile {
    type Target = Arc<dyn File>;

    fn deref(&self) -> &Self::Target {
        &self.file
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
        matches!(self.state, PartitionState::Loaded)
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
            temp_file: {
                let temp_dir = tempfile::tempdir()?;
                let file: Arc<dyn File> = io.open_file(
                    temp_dir
                        .as_ref()
                        .join("hash_join_spill")
                        .to_str()
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "temp file path is not valid UTF-8".to_string(),
                            )
                        })?,
                    OpenFlags::Create,
                    false,
                )?;
                TempFile {
                    _temp_dir: temp_dir,
                    file,
                }
            },
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

/// The main hash table structure for hash joins.
/// Supports grace hash join with disk spilling when memory budget is exceeded.
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
    /// Index within loaded_entries of current spilled partition
    current_spill_entry_idx: usize,
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
            current_spill_entry_idx: 0,
        }
    }

    /// Get the current state of the hash table.
    pub fn get_state(&self) -> &HashTableState {
        &self.state
    }

    /// Insert a row into the hash table, returns IOResult because this may spill to disk.
    /// When memory budget is exceeded, triggers grace hash join by partitioning and spilling.
    pub fn insert(&mut self, key_values: Vec<Value>, rowid: i64) -> Result<IOResult<()>> {
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot insert into hash table in state {:?}",
            self.state
        );

        // Compute hash of the join keys using collations
        let key_refs: Vec<ValueRef> = key_values.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);

        let entry = HashEntry::new(hash, key_values, rowid);
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

            // Spill the largest partition to disk to make room
            if let Some(c) = self.spill_largest_partition()? {
                // I/O pending - caller will re-enter after completion and retry the insert.
                // Do NOT insert the entry here, as the caller will call insert() again.
                return Ok(IOResult::IO(IOCompletions::Single(c)));
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

    /// Spill the largest partition to disk to free up memory.
    fn spill_largest_partition(&mut self) -> Result<Option<Completion>> {
        // Find the largest non-empty partition and serialize its data
        let spill_state = self.spill_state.as_mut().expect("Spill state must exist");

        let largest_idx = spill_state
            .partition_buffers
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_empty())
            .max_by_key(|(_, p)| p.mem_used)
            .map(|(idx, _)| idx);

        let Some(partition_idx) = largest_idx else {
            // No partitions to spill - this shouldn't happen if we're over budget
            return Ok(None);
        };

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

        // Clear partition buffer now before we borrow spill_state again
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

    /// Finalize the build phase and prepare for probing.
    /// If spilled, flushes remaining in-memory partition entries to disk.
    /// Returns IOResult::IO if there's pending I/O, caller should re-enter after completion.
    pub fn finalize_build(&mut self) -> Result<IOResult<()>> {
        turso_assert!(
            self.state == HashTableState::Building || self.state == HashTableState::Spilled,
            "Cannot finalize build in state {:?}",
            self.state
        );

        if let Some(spill_state) = self.spill_state.as_mut() {
            // Check for pending writes from previous call
            for spilled in &spill_state.partitions {
                if matches!(spilled.io_state.get(), SpillIOState::WaitingForWrite) {
                    // I/O still pending, caller should wait
                    io_yield_one!(Completion::new_yield());
                }
            }

            // Flush any remaining in-memory partition entries to disk
            for partition_idx in 0..NUM_PARTITIONS {
                let partition = &spill_state.partition_buffers[partition_idx];
                if partition.is_empty() {
                    continue;
                }

                // Serialize entries
                let estimated_size: usize =
                    partition.entries.iter().map(|e| e.size_bytes() + 9).sum();
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
                if serialized_data.is_empty() {
                    continue;
                }

                let file_offset = spill_state.next_spill_offset;
                let data_size = serialized_data.len();
                let num_entries = spill_state.partition_buffers[partition_idx].entries.len();

                // Clear partition buffer before borrowing spill_state for partition lookup
                spill_state.partition_buffers[partition_idx].clear();

                // Find existing partition or create new one
                let io_state = if let Some(existing) = spill_state.find_partition_mut(partition_idx)
                {
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

                // Write to disk asynchronously
                let buffer = Buffer::new_temporary(data_size);
                buffer.as_mut_slice().copy_from_slice(&serialized_data);
                let buffer_ref = Arc::new(buffer);
                let _cbuffer_ref_clone = buffer_ref.clone();

                let write_complete =
                    Box::new(move |res: Result<i32, crate::CompletionError>| match res {
                        Ok(_) => {
                            let _buf = _cbuffer_ref_clone.clone();
                            io_state.set(SpillIOState::WriteComplete);
                        }
                        Err(e) => {
                            tracing::error!("Error writing spilled partition to disk: {e:?}");
                            io_state.set(SpillIOState::Error);
                        }
                    });
                let completion = Completion::new_write(write_complete);
                let completion =
                    spill_state
                        .temp_file
                        .file
                        .pwrite(file_offset, buffer_ref, completion)?;

                spill_state.next_spill_offset += data_size as u64;

                // Return I/O completion to caller, they will re-enter after completion
                return Ok(IOResult::IO(IOCompletions::Single(completion)));
            }
        }
        self.current_spill_partition_idx = 0;
        self.current_spill_entry_idx = 0;
        self.state = HashTableState::Probing;
        Ok(IOResult::Done(()))
    }

    /// Probe the hash table with the given keys, returns the first matching entry if found.
    /// For spilled hash tables, this searches in-memory partitions first, then spilled partitions.
    pub fn probe(&mut self, probe_keys: Vec<Value>) -> Option<&HashEntry> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot probe hash table in state {:?}",
            self.state
        );

        // Store probe keys first
        self.current_probe_keys = Some(probe_keys);

        // Compute hash of probe keys using collations
        let probe_keys_ref = self.current_probe_keys.as_ref().unwrap();
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

        let probe_keys = self.current_probe_keys.as_ref()?;
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
        let hash = self
            .current_probe_hash
            .unwrap_or_else(|| hash_join_key(&key_refs, &self.collations));

        if let Some(spill_state) = self.spill_state.as_ref() {
            let target_partition = partition_from_hash(hash);
            let partition = spill_state.find_partition(target_partition)?;

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
            let bucket = &self.buckets[self.probe_bucket_idx];
            for idx in self.probe_entry_idx..bucket.entries.len() {
                let entry = &bucket.entries[idx];
                if entry.hash == hash && keys_equal(&entry.key_values, &key_refs, &self.collations)
                {
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
                SpillAction::AlreadyLoaded | SpillAction::NoChunks | SpillAction::NotFound => {
                    // Either already loaded, or nothing to load.
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
                            }
                        },
                    );
                    let completion = Completion::new_read(read_buffer, read_complete);

                    // Re-borrow to get the file handle only
                    let spill_state = self.spill_state.as_ref().expect("spill state must exist");
                    let c = spill_state.temp_file.file.pread(file_offset, completion)?;
                    io_yield_one!(c);
                }
            }
        }
    }

    /// Parse entries from the read buffer into buckets for a partition.
    fn parse_partition_entries(&mut self, partition_idx: usize) -> Result<()> {
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

        // Release the read lock before modifying partition
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

        Ok(())
    }

    /// Probe a specific partition with the given keys.
    /// The partition must be loaded first via `load_spilled_partition`.
    pub fn probe_partition(
        &mut self,
        partition_idx: usize,
        probe_keys: &[Value],
    ) -> Option<&HashEntry> {
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs, &self.collations);

        // Store probe keys for subsequent next_match calls
        self.current_probe_keys = Some(probe_keys.to_vec());
        self.current_probe_hash = Some(hash);

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

    /// Close the hash table and free resources.
    pub fn close(&mut self) {
        self.state = HashTableState::Closed;
        self.buckets.clear();
        self.num_entries = 0;
        self.mem_used = 0;
        let _ = self.spill_state.take();
    }

    /// Check if the hash table is empty.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlatformIO;

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
        let io = Arc::new(PlatformIO::new().unwrap());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
        };
        let mut ht = HashTable::new(config, io);

        // Insert some entries (late materialization - only store rowids)
        let key1 = vec![Value::Integer(1)];
        let _ = ht.insert(key1.clone(), 100).unwrap();

        let key2 = vec![Value::Integer(2)];
        let _ = ht.insert(key2.clone(), 200).unwrap();

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
        let io = Arc::new(PlatformIO::new().unwrap());
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
            let _ = ht.insert(key, i * 100).unwrap();
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
        let io = Arc::new(PlatformIO::new().unwrap());
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
            let _ = ht.insert(key.clone(), 1000 + i).unwrap();
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
}
