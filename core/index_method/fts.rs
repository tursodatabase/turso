use crate::{
    index_method::{
        parse_patterns, IndexMethod, IndexMethodAttachment, IndexMethodConfiguration,
        IndexMethodCursor, IndexMethodDefinition,
    },
    return_if_io,
    schema::IndexColumn,
    storage::{
        btree::{BTreeCursor, BTreeKey, CursorTrait},
        pager::Pager,
    },
    translate::collate::CollationSeq,
    turso_assert,
    types::{IOResult, ImmutableRecord, IndexInfo, KeyInfo, SeekKey, SeekOp, SeekResult, Text},
    vdbe::Register,
    Connection, LimboError, Result, Value,
};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::{
    directory::{
        error::{DeleteError, OpenReadError, OpenWriteError},
        Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    },
    merge_policy::NoMergePolicy,
    schema::{Field, Schema, Value as TantivySchemaValue},
    tokenizer::{NgramTokenizer, RawTokenizer, SimpleTokenizer, WhitespaceTokenizer},
    DocAddress, HasLen, Index, IndexReader, IndexSettings, IndexWriter, Searcher, TantivyDocument,
};
use turso_parser::ast::{self, Select, SortOrder};

/// Name identifier for the FTS index method, used in `CREATE INDEX ... USING fts`.
pub const FTS_INDEX_METHOD_NAME: &str = "fts";

/// Default memory budget (64MB) for Tantivy's IndexWriter.
/// Controls how much memory Tantivy uses for in-memory indexing before flushing to disk.
pub const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Default chunk size (1MB) for splitting large files when storing in BTree.
/// Files larger than this are split into multiple chunks for efficient storage and retrieval.
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Number of documents to batch before committing to Tantivy.
/// Higher values improve throughput but increase memory usage and latency.
pub const BATCH_COMMIT_SIZE: usize = 1000;

/// Default memory budget (64MB) for hot cache (metadata + term dictionaries).
/// Hot files are frequently accessed and kept in an LRU cache.
pub const DEFAULT_HOT_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Default memory budget (128MB) for chunk LRU cache.
/// Caches segment data chunks loaded on-demand from the BTree.
pub const DEFAULT_CHUNK_CACHE_BYTES: usize = 128 * 1024 * 1024;

/// Highlight matching terms in text by wrapping them with tags.
///
/// Standalone function that can be used without an FTS index.
/// It tokenizes both the query and text using Tantivy's default tokenizer,
/// finds matching terms, and wraps them with the specified tags.
pub fn fts_highlight(text: &str, query: &str, before_tag: &str, after_tag: &str) -> String {
    use tantivy::tokenizer::{TextAnalyzer, TokenStream};
    if text.is_empty() || query.is_empty() {
        return text.to_string();
    }
    let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(tantivy::tokenizer::LowerCaser)
        .build();

    // Extract query terms (lowercased)
    let query_terms: HashSet<String> = {
        let mut terms = HashSet::new();
        let mut query_stream = tokenizer.token_stream(query);
        while let Some(token) = query_stream.next() {
            terms.insert(token.text.to_string());
        }
        terms
    };
    if query_terms.is_empty() {
        return text.to_string();
    }

    // Tokenize the text and track positions of matching tokens
    let match_ranges: Vec<(usize, usize)> = {
        let mut ranges = Vec::new();
        let mut text_stream = tokenizer.token_stream(text);
        while let Some(token) = text_stream.next() {
            if query_terms.contains(&token.text) {
                ranges.push((token.offset_from, token.offset_to));
            }
        }
        ranges
    };

    if match_ranges.is_empty() {
        return text.to_string();
    }

    // Build the highlighted text by inserting tags at match positions,
    // apply edits from the end backward when offsets are precomputed.
    let mut result = text.to_string();
    for (start, end) in match_ranges.into_iter().rev() {
        // Ensure offsets are valid UTF-8 boundaries
        if start <= result.len()
            && end <= result.len()
            && result.is_char_boundary(start)
            && result.is_char_boundary(end)
        {
            result.insert_str(end, after_tag);
            result.insert_str(start, before_tag);
        }
    }
    result
}

/// File classification for hybrid caching strategy.
/// Determines which files are kept hot in memory vs lazy-loaded on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCategory {
    /// Always in memory: meta.json, .managed.json, .lock (typically < 64KB)
    Metadata,
    /// Hot files: .term dictionaries - loaded on first access, kept in LRU
    TermDictionary,
    /// Fast fields and field norms - small, frequently accessed
    FastFields,
    /// Cold files: .idx, .pos, .store - lazy-loaded on demand
    SegmentData,
}

impl FileCategory {
    const METADATA_FILES: [&'static str; 3] = [TANTIVY_META_FILE, ".managed.json", ".lock"];
    /// Classify a file based on its path/extension.
    fn from_path(path: &Path) -> Self {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        // Check for known Tantivy metadata files first
        if Self::METADATA_FILES.contains(&name) {
            return FileCategory::Metadata;
        }

        match ext {
            // Term dictionary - hot for queries
            "term" => FileCategory::TermDictionary,
            // Fast fields and field norms - small, frequently accessed
            "fast" | "fieldnorm" => FileCategory::FastFields,
            // Segment data - large, lazy-loaded
            "idx" | "pos" | "store" => FileCategory::SegmentData,
            // Default to segment data (lazy-loaded)
            _ => FileCategory::SegmentData,
        }
    }

    /// Returns true if files in this category should be preloaded at startup.
    const fn should_preload(&self) -> bool {
        matches!(self, FileCategory::Metadata)
    }

    /// Returns true if files in this category should be kept in the hot cache.
    const fn is_hot(&self) -> bool {
        matches!(
            self,
            FileCategory::Metadata | FileCategory::TermDictionary | FileCategory::FastFields
        )
    }
}

/// Metadata about a file stored in the FTS directory.
/// Used for catalog-first loading where we build file metadata without loading content.
#[derive(Debug, Clone)]
struct FileMetadata {
    /// Total file size in bytes
    size: usize,
    /// Number of chunks this file is split into
    num_chunks: usize,
    /// File category for caching decisions
    category: FileCategory,
}

impl FileMetadata {
    fn new(path: &Path, size: usize, num_chunks: usize) -> Self {
        Self {
            size,
            num_chunks,
            category: FileCategory::from_path(path),
        }
    }
}

type ChunkKey = (PathBuf, i64);

/// Eviction samples per put
const EVICTION_SAMPLES: usize = 8;

/// Generic bounded LRU cache with sampling-based eviction.
pub struct LruCache<K> {
    capacity: usize,
    inner: RwLock<LruCacheInner<K>>,
}

#[derive(Debug)]
struct LruCacheInner<K> {
    current_size: usize,
    clock: u64,
    entries: HashMap<K, LruCacheEntry>,
}

#[derive(Debug)]
struct LruCacheEntry {
    data: Vec<u8>,
    accessed: u64,
}

impl<K: std::fmt::Debug> std::fmt::Debug for LruCache<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("LruCache")
            .field("capacity", &self.capacity)
            .field("current_size", &inner.current_size)
            .field("entries", &inner.entries.len())
            .finish()
    }
}

impl<K: Eq + std::hash::Hash + Clone> LruCache<K> {
    /// Creates a new empty cache with the specified capacity in bytes.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: RwLock::new(LruCacheInner {
                current_size: 0,
                clock: 0,
                entries: HashMap::new(),
            }),
        }
    }

    /// Lookup entry, updating access timestamp. Returns cloned data.
    fn get<Q>(&self, key: &Q) -> Option<Vec<u8>>
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        let mut inner = self.inner.write();
        inner.clock += 1;
        let ts = inner.clock;
        if let Some(entry) = inner.entries.get_mut(key) {
            entry.accessed = ts;
            Some(entry.data.clone())
        } else {
            None
        }
    }

    /// Insert entry, evicting stale entries if over capacity.
    ///
    /// Eviction uses sampling: examines K entries and evicts the one with
    /// the oldest access timestamp. Repeat until under capacity.
    fn put(&self, key: K, value: Vec<u8>) {
        let size = value.len();
        let mut inner = self.inner.write();

        // Check for existing entry - get old size if present
        let old_size = inner.entries.get(&key).map(|e| e.data.len());

        if let Some(old) = old_size {
            // Update existing entry
            inner.clock += 1;
            let ts = inner.clock;
            let entry = inner.entries.get_mut(&key).unwrap();
            entry.data = value;
            entry.accessed = ts;
            inner.current_size = inner.current_size - old + size;
            return;
        }

        // Evict until under capacity
        while inner.current_size + size > self.capacity && !inner.entries.is_empty() {
            let victim = {
                inner
                    .entries
                    .iter()
                    .take(EVICTION_SAMPLES)
                    .min_by_key(|(_, e)| e.accessed)
                    .map(|(k, _)| k.clone())
            };

            match victim {
                Some(k) => {
                    if let Some(e) = inner.entries.remove(&k) {
                        inner.current_size -= e.data.len();
                    }
                }
                None => break,
            }
        }

        inner.clock += 1;
        let ts = inner.clock;
        inner.entries.insert(
            key,
            LruCacheEntry {
                data: value,
                accessed: ts,
            },
        );
        inner.current_size += size;
    }

    /// Remove an entry from the cache.
    fn remove<Q>(&self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        let mut inner = self.inner.write();
        if let Some(e) = inner.entries.remove(key) {
            inner.current_size -= e.data.len();
        }
    }

    /// Current memory usage in bytes.
    fn size(&self) -> usize {
        self.inner.read().current_size
    }

    /// Number of entries in the cache.
    fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    /// Check if key exists in cache.
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        self.inner.read().entries.contains_key(key)
    }
}

/// Specialized methods for ChunkKey (PathBuf, i64) caches.
impl LruCache<ChunkKey> {
    /// Invalidate all chunks for a file path.
    /// Called when a file is deleted or overwritten.
    fn invalidate(&self, path: &Path) {
        let mut inner = self.inner.write();
        let mut freed = 0usize;
        inner.entries.retain(|(p, _), e| {
            if p == path {
                freed += e.data.len();
                false
            } else {
                true
            }
        });
        inner.current_size -= freed;
    }
}

/// Specialized methods for PathBuf caches (hot files).
impl LruCache<PathBuf> {
    /// Create from preloaded files (used during initialization).
    fn with_preloaded(capacity: usize, files: HashMap<PathBuf, Vec<u8>>) -> Self {
        let current_size: usize = files.values().map(|v| v.len()).sum();
        let entries: HashMap<PathBuf, LruCacheEntry> = files
            .into_iter()
            .enumerate()
            .map(|(i, (path, data))| {
                (
                    path,
                    LruCacheEntry {
                        data,
                        accessed: i as u64,
                    },
                )
            })
            .collect();

        Self {
            capacity,
            inner: RwLock::new(LruCacheInner {
                current_size,
                clock: entries.len() as u64,
                entries,
            }),
        }
    }
}

/// Type aliases to please the almighty clippy
type Catalog = HashMap<PathBuf, FileMetadata>;
type PendingWrites = Vec<(PathBuf, Vec<u8>)>;

#[derive(Clone)]
/// Hybrid Directory implementation for Tantivy with lazy loading.
/// - Metadata files (meta.json, etc.) are always kept in hot cache
/// - Large segment files (.idx, .pos, .store) are lazy-loaded on demand
/// - Uses LRU caches for both hot files and chunks to bound memory usage
struct HybridBTreeDirectory {
    /// File catalog: path -> metadata (always in memory, no content)
    catalog: Arc<RwLock<Catalog>>,

    /// Hot cache: LRU cache for frequently accessed files (metadata, term dictionaries)
    /// Bounded to DEFAULT_HOT_CACHE_BYTES (64MB) to prevent unbounded memory growth
    hot_cache: Arc<LruCache<PathBuf>>,

    /// Chunk cache: LRU cache for lazy-loaded segment chunks
    chunk_cache: Arc<LruCache<ChunkKey>>,

    /// Pending writes to be flushed to BTree
    pending_writes: Arc<RwLock<PendingWrites>>,

    /// Writes currently being flushed to BTree (still readable during flush)
    /// This preserves data for reads during async flush operations
    flushing_writes: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,

    /// Pending deletes to be flushed to BTree
    pending_deletes: Arc<RwLock<Vec<PathBuf>>>,

    /// Reference to pager for IO
    pager: Arc<Pager>,

    /// BTree root page for the FTS directory index
    btree_root_page: i64,
}

impl std::fmt::Debug for HybridBTreeDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridBTreeDirectory")
            .field("catalog_size", &self.catalog.read().len())
            .field("hot_cache_size", &self.hot_cache.len())
            .field("hot_cache_bytes", &self.hot_cache.size())
            .field("chunk_cache_size", &self.chunk_cache.size())
            .field("btree_root_page", &self.btree_root_page)
            .finish()
    }
}

impl HybridBTreeDirectory {
    /// Create a clone with fresh (empty) pending state.
    /// This is used when creating a new cursor from a cached directory to ensure
    /// each cursor has its own isolated pending_writes/pending_deletes.
    /// This prevents the bug where writes from one cursor affect the Drop behavior
    /// of another cursor.
    fn clone_with_fresh_pending(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            hot_cache: Arc::clone(&self.hot_cache),
            chunk_cache: Arc::clone(&self.chunk_cache),
            // Fresh pending state - not shared with cache
            pending_writes: Arc::new(RwLock::new(Vec::new())),
            flushing_writes: Arc::new(RwLock::new(HashMap::new())),
            pending_deletes: Arc::new(RwLock::new(Vec::new())),
            pager: Arc::clone(&self.pager),
            btree_root_page: self.btree_root_page,
        }
    }
}

fn io_not_found<M: Into<Box<dyn std::error::Error + Send + Sync>>>(msg: M) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, msg)
}

impl HybridBTreeDirectory {
    /// Create from preloaded catalog and hot cache files.
    fn with_preloaded(
        pager: Arc<Pager>,
        btree_root_page: i64,
        catalog: HashMap<PathBuf, FileMetadata>,
        hot_files: HashMap<PathBuf, Vec<u8>>,
        hot_cache_capacity: usize,
        chunk_cache_capacity: usize,
    ) -> Self {
        Self {
            catalog: Arc::new(RwLock::new(catalog)),
            hot_cache: Arc::new(LruCache::<PathBuf>::with_preloaded(
                hot_cache_capacity,
                hot_files,
            )),
            chunk_cache: Arc::new(LruCache::<ChunkKey>::new(chunk_cache_capacity)),
            pending_writes: Arc::new(RwLock::new(Vec::new())),
            flushing_writes: Arc::new(RwLock::new(HashMap::new())),
            pending_deletes: Arc::new(RwLock::new(Vec::new())),
            pager,
            btree_root_page,
        }
    }

    /// Get pending writes for flushing, deduplicated to keep only the last write for each path.
    /// The writes are also copied to flushing_writes so they remain readable during async flush.
    fn take_pending_writes(&self) -> Vec<(PathBuf, Vec<u8>)> {
        let mut pending = self.pending_writes.write();
        let all_writes = std::mem::take(&mut *pending);

        // Deduplicate: keep only the last write for each path
        let mut last_write_idx: HashMap<PathBuf, usize> = HashMap::new();
        for (idx, (path, _)) in all_writes.iter().enumerate() {
            last_write_idx.insert(path.clone(), idx);
        }

        let deduped: Vec<(PathBuf, Vec<u8>)> = all_writes
            .into_iter()
            .enumerate()
            .filter(|(idx, (path, _))| last_write_idx.get(path) == Some(idx))
            .map(|(_, entry)| entry)
            .collect();

        // Copy to flushing_writes so data remains readable during async flush
        {
            let mut flushing = self.flushing_writes.write();
            for (path, data) in &deduped {
                flushing.insert(path.clone(), data.clone());
            }
        }

        tracing::debug!(
            "FTS take_pending_writes: {} entries after deduplication",
            deduped.len()
        );
        deduped
    }

    /// Clear flushing_writes after flush completes successfully.
    /// Call this after all writes have been persisted to BTree.
    fn complete_flush(&self) {
        let mut flushing = self.flushing_writes.write();
        tracing::debug!(
            "FTS complete_flush: clearing {} entries from flushing_writes",
            flushing.len()
        );
        flushing.clear();
    }

    /// Find file data in pending writes or flushing writes.
    /// Checks pending_writes first, then flushing_writes (for in-flight flushes).
    fn find_in_pending_writes(&self, path: &Path) -> Option<Vec<u8>> {
        // Check pending_writes first (most recent)
        {
            let pending = self.pending_writes.read();
            if let Some((_, data)) = pending.iter().rev().find(|(p, _)| p == path) {
                return Some(data.clone());
            }
        }
        // Check flushing_writes (data being flushed but not yet in BTree)
        {
            let flushing = self.flushing_writes.read();
            if let Some(data) = flushing.get(path) {
                return Some(data.clone());
            }
        }

        None
    }

    const CHUNK_LEN: usize = 3;

    /// Blocking read of a single chunk from BTree.
    /// Called from LazyFileHandle::read_bytes when chunk is not in cache.
    fn get_chunk_blocking(&self, path: &Path, chunk_no: i64) -> std::io::Result<Vec<u8>> {
        // Check chunk cache first
        let cache_key = (path.to_path_buf(), chunk_no);
        if let Some(chunk) = self.chunk_cache.get(&cache_key) {
            return Ok(chunk);
        }

        // Create a temporary cursor for this read
        let mut cursor =
            BTreeCursor::new(self.pager.clone(), self.btree_root_page, Self::CHUNK_LEN);
        cursor.index_info = Some(Arc::new(IndexInfo {
            has_rowid: false,
            num_cols: Self::CHUNK_LEN,
            key_info: vec![key_info(), key_info(), key_info()],
        }));

        // Seek to the specific chunk
        let path_str = path.to_string_lossy().to_string();
        let seek_key = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(path_str.clone())),
                Value::Integer(chunk_no),
                Value::Blob(vec![]), // minimum blob for GE seek
            ],
            Self::CHUNK_LEN,
        );

        // Blocking seek loop
        loop {
            match cursor.seek(SeekKey::IndexKey(&seek_key), SeekOp::GE { eq_only: false }) {
                Ok(IOResult::Done(SeekResult::Found)) => break,
                Ok(IOResult::Done(SeekResult::TryAdvance)) => {
                    // Need to advance, try next
                    loop {
                        match cursor.next() {
                            Ok(IOResult::Done(_)) => {
                                let has_next = cursor.has_record();
                                if !has_next {
                                    return Err(io_not_found(format!(
                                        "chunk {}:{} not found",
                                        path.display(),
                                        chunk_no
                                    )));
                                }
                                break;
                            }
                            Ok(IOResult::IO(_)) => {
                                self.pager
                                    .io
                                    .step()
                                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                            }
                            Err(e) => return Err(std::io::Error::other(e.to_string())),
                        }
                    }
                    break;
                }
                Ok(IOResult::Done(SeekResult::NotFound)) => {
                    return Err(io_not_found(format!(
                        "chunk {}:{} not found",
                        path.display(),
                        chunk_no
                    )));
                }
                Ok(IOResult::IO(_)) => {
                    self.pager
                        .io
                        .step()
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
        }

        // Extract chunk data with blocking record read
        let record = loop {
            match cursor.record() {
                Ok(IOResult::Done(r)) => break r,
                Ok(IOResult::IO(_)) => {
                    self.pager
                        .io
                        .step()
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
        };

        let record = record.ok_or_else(|| io_not_found("no record at cursor"))?;

        // Validate and extract - record format: [path, chunk_no, bytes]
        let found_path = record.get_value_opt(0).and_then(|v| match v {
            crate::types::ValueRef::Text(t) => Some(t.value.to_string()),
            _ => None,
        });
        let found_chunk = record.get_value_opt(1).and_then(|v| match v {
            crate::types::ValueRef::Integer(i) => Some(i),
            _ => None,
        });
        let bytes = record.get_value_opt(2).and_then(|v| match v {
            crate::types::ValueRef::Blob(b) => Some(b.to_vec()),
            _ => None,
        });

        let (found_path, found_chunk, bytes) = match (found_path, found_chunk, bytes) {
            (Some(p), Some(c), Some(b)) => (p, c, b),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "malformed chunk record",
                ))
            }
        };

        if found_path != path_str || found_chunk != chunk_no {
            return Err(io_not_found(format!(
                "wrong chunk: expected {path_str}:{chunk_no}, got {found_path}:{found_chunk}",
            )));
        }

        self.chunk_cache.put(cache_key, bytes.clone());

        Ok(bytes)
    }

    /// Load an entire file by concatenating all its chunks (blocking).
    /// Used for loading hot files during preload phase.
    fn load_file_blocking(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let catalog = self.catalog.read();
        let metadata = catalog
            .get(path)
            .ok_or_else(|| io_not_found(format!("file not in catalog: {}", path.display())))?;

        let mut result = Vec::with_capacity(metadata.size);
        for chunk_no in 0..metadata.num_chunks as i64 {
            let chunk = self.get_chunk_blocking(path, chunk_no)?;
            result.extend_from_slice(&chunk);
        }

        Ok(result)
    }

    /// Add a file to the hot cache.
    fn add_to_hot_cache(&self, path: PathBuf, data: Vec<u8>) {
        self.hot_cache.put(path, data);
    }

    /// Update the catalog with file metadata.
    fn update_catalog(&self, path: PathBuf, metadata: FileMetadata) {
        let mut catalog = self.catalog.write();
        catalog.insert(path, metadata);
    }
}

/// Simple in-memory file handle for data already loaded (hot cache, pending writes).
struct InMemoryFileHandle {
    data: Vec<u8>,
}

impl std::fmt::Debug for InMemoryFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryFileHandle")
            .field("len", &self.data.len())
            .finish()
    }
}

impl HasLen for InMemoryFileHandle {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl FileHandle for InMemoryFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if range.end > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "range exceeds file length",
            ));
        }
        if range.start >= range.end {
            return Ok(OwnedBytes::new(Vec::new()));
        }
        Ok(OwnedBytes::new(self.data[range].to_vec()))
    }
}

/// Lazy file handle that fetches chunks on demand.
struct LazyFileHandle {
    path: PathBuf,
    size: usize,
    directory: HybridBTreeDirectory,
}

impl std::fmt::Debug for LazyFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyFileHandle")
            .field("path", &self.path)
            .field("size", &self.size)
            .finish()
    }
}

impl HasLen for LazyFileHandle {
    fn len(&self) -> usize {
        self.size
    }
}

impl FileHandle for LazyFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if range.end > self.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "range {:?} exceeds file size {} for {}",
                    range,
                    self.size,
                    self.path.display()
                ),
            ));
        }
        if range.start >= range.end {
            return Ok(OwnedBytes::new(Vec::new()));
        }

        // Check hot cache first
        if let Some(data) = self.directory.hot_cache.get(&self.path) {
            return Ok(OwnedBytes::new(data[range].to_vec()));
        }

        // Check pending/flushing writes (data not yet persisted to BTree)
        if let Some(data) = self.directory.find_in_pending_writes(&self.path) {
            return Ok(OwnedBytes::new(data[range].to_vec()));
        }

        // Calculate required chunks
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let start_chunk = range.start / chunk_size;
        let end_chunk = range.end.saturating_sub(1) / chunk_size;

        // Collect chunks
        let mut result = Vec::with_capacity(range.len());
        for chunk_no in start_chunk..=end_chunk {
            let chunk = self
                .directory
                .get_chunk_blocking(&self.path, chunk_no as i64)?;

            // Calculate slice within this chunk
            let chunk_start = chunk_no * chunk_size;
            let local_start = if chunk_no == start_chunk {
                range.start - chunk_start
            } else {
                0
            };
            let local_end = if chunk_no == end_chunk {
                range.end - chunk_start
            } else {
                chunk.len()
            };

            // Defensive bounds check to prevent panics
            let local_end = local_end.min(chunk.len());
            let local_start = local_start.min(local_end);

            result.extend_from_slice(&chunk[local_start..local_end]);
        }

        Ok(OwnedBytes::new(result))
    }
}

/// In-memory writer for HybridBTreeDirectory.
struct HybridWriter {
    path: PathBuf,
    buffer: Vec<u8>,
    directory: HybridBTreeDirectory,
}

impl Write for HybridWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for HybridWriter {
    fn drop(&mut self) {
        // Commit the write to the directory
        let data = std::mem::take(&mut self.buffer);
        if !data.is_empty() {
            // Update catalog
            let num_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE);
            let metadata = FileMetadata::new(&self.path, data.len(), num_chunks);
            self.directory
                .update_catalog(self.path.clone(), metadata.clone());

            // If it's a hot file category, add to hot cache
            if metadata.category.is_hot() {
                self.directory
                    .add_to_hot_cache(self.path.clone(), data.clone());
            }

            // Queue for BTree flush
            let mut pending = self.directory.pending_writes.write();
            pending.push((self.path.clone(), data));
        }
    }
}

impl TerminatingWrite for HybridWriter {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        let data = std::mem::take(&mut self.buffer);

        // Update catalog
        let num_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE).max(1);
        let metadata = FileMetadata::new(&self.path, data.len(), num_chunks);
        self.directory
            .update_catalog(self.path.clone(), metadata.clone());

        // If it's a hot file category, add to hot cache
        if metadata.category.is_hot() {
            self.directory
                .add_to_hot_cache(self.path.clone(), data.clone());
        }

        // Queue for BTree flush
        let mut pending = self.directory.pending_writes.write();
        pending.push((self.path.clone(), data));
        Ok(())
    }
}

impl Directory for HybridBTreeDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        // Check hot cache first
        if let Some(data) = self.hot_cache.get(path) {
            return Ok(Arc::new(InMemoryFileHandle { data }));
        }

        // Check pending writes (files written but not yet flushed to BTree)
        // This is critical for cold files that are immediately read back by Tantivy
        if let Some(data) = self.find_in_pending_writes(path) {
            return Ok(Arc::new(InMemoryFileHandle { data }));
        }

        // Check catalog for file metadata
        let catalog = self.catalog.read();
        let metadata = catalog
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(Arc::new(LazyFileHandle {
            path: path.to_path_buf(),
            size: metadata.size,
            directory: self.clone(),
        }))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        // Check hot cache
        if self.hot_cache.contains(path) {
            return Ok(true);
        }
        // Check catalog
        let catalog = self.catalog.read();
        Ok(catalog.contains_key(path))
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        // Remove from hot cache
        self.hot_cache.remove(path);
        // Remove from catalog
        {
            let mut catalog = self.catalog.write();
            catalog.remove(path);
        }
        // Invalidate chunk cache
        self.chunk_cache.invalidate(path);
        // Queue for BTree deletion
        {
            let mut pending = self.pending_deletes.write();
            pending.push(path.to_path_buf());
        }
        Ok(())
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite>>, OpenWriteError> {
        // Tantivy's Directory trait documentation states files "may not previously exist",
        // and the standard MmapDirectory implementation uses OpenOptions::create_new(true)
        // which fails with FileAlreadyExists if the file is present.
        // However, Tantivy may call open_write on existing files during operations like
        // segment merging or metadata updates. To handle this gracefully, we delete any
        // existing file first. The error is ignored because:
        // 1. If the file doesn't exist, delete() succeeds (no-op on missing files)
        // 2. Our delete() implementation always returns Ok(()) - it only removes entries
        //    from in-memory structures (hot_cache, catalog, chunk_cache) and queues the
        //    BTree deletion, none of which can fail.
        let _ = self.delete(path);
        let writer: Box<dyn TerminatingWrite> = Box::new(HybridWriter {
            path: path.to_path_buf(),
            buffer: Vec::new(),
            directory: self.clone(),
        });
        Ok(BufWriter::new(writer))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        // Check hot cache first (includes recently written files)
        if let Some(data) = self.hot_cache.get(path) {
            return Ok(data);
        }

        // Check pending writes (files written but not yet flushed to BTree)
        if let Some(data) = self.find_in_pending_writes(path) {
            return Ok(data);
        }

        // Check if file exists in catalog
        {
            let catalog = self.catalog.read();
            if !catalog.contains_key(path) {
                return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
            }
        }

        // Load file blocking from BTree
        self.load_file_blocking(path)
            .map_err(|e| OpenReadError::IoError {
                io_error: Arc::new(e),
                filepath: path.to_path_buf(),
            })
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        // Update catalog
        let num_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE).max(1);
        let metadata = FileMetadata::new(path, data.len(), num_chunks);
        self.update_catalog(path.to_path_buf(), metadata.clone());

        // If it's a hot file category, add to hot cache
        if metadata.category.is_hot() {
            self.add_to_hot_cache(path.to_path_buf(), data.to_vec());
        }

        // Queue for BTree flush
        let mut pending = self.pending_writes.write();
        pending.push((path.to_path_buf(), data.to_vec()));
        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, _cb: WatchCallback) -> std::result::Result<WatchHandle, tantivy::TantivyError> {
        Ok(WatchHandle::empty())
    }
}

/// Helper to create KeyInfo for text collation
fn key_info() -> KeyInfo {
    KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::Binary,
    }
}

fn name(name: impl ToString) -> ast::Name {
    ast::Name::exact(name.to_string())
}

/// Parse field weights from a string like "body=2.0,title=1.0"
/// Returns a HashMap mapping column names to boost factors
fn parse_field_weights(weights_str: &str, columns: &[IndexColumn]) -> Result<HashMap<String, f32>> {
    let mut weights = HashMap::new();

    if weights_str.is_empty() {
        return Ok(weights);
    }

    // Get valid column names for validation
    let valid_columns: std::collections::HashSet<&str> =
        columns.iter().map(|c| c.name.as_str()).collect();

    // Parse format: "col1=1.5,col2=2.0"
    for part in weights_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (col_name, weight_str) = part.split_once('=').ok_or_else(|| {
            LimboError::ParseError(format!(
                "invalid weight format '{part}'. Expected 'column=weight' (e.g., 'title=2.0')",
            ))
        })?;

        let col_name = col_name.trim();
        let weight_str = weight_str.trim();

        // Validate column exists in index
        if !valid_columns.contains(col_name) {
            return Err(LimboError::ParseError(format!(
                "unknown column '{}' in weights. Valid columns: {}",
                col_name,
                columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        // Parse weight as f32
        let weight: f32 = weight_str.parse().map_err(|_| {
            LimboError::ParseError(format!(
                "invalid weight value '{weight_str}' for column '{col_name}'. Expected a number (e.g., 2.0)",
            ))
        })?;

        if weight <= 0.0 {
            return Err(LimboError::ParseError(format!(
                "weight for column '{col_name}' must be positive, got {weight}",
            )));
        }

        weights.insert(col_name.to_string(), weight);
    }

    Ok(weights)
}

/// Factory for creating FTS index attachments.
///
/// Implements the `IndexMethod` trait to integrate with turso's index method system.
/// When a user creates an FTS index with `CREATE INDEX ... USING fts (...)`,
/// this factory creates an `FtsIndexAttachment` with the specified configuration.
#[derive(Debug)]
pub struct FtsIndexMethod;

impl IndexMethod for FtsIndexMethod {
    fn attach(&self, cfg: &IndexMethodConfiguration) -> Result<Arc<dyn IndexMethodAttachment>> {
        let attachment = FtsIndexAttachment::new(cfg.clone())?;
        Ok(Arc::new(attachment))
    }
}

/// Cached FTS directory shared across cursors to avoid expensive catalog reloads.
///
/// Contains a `HybridBTreeDirectory` with its catalog already loaded from the BTree.
/// Only the directory is cached, not the Tantivy Index/Reader, because each cursor
/// needs its own Index instance to handle writes correctly.
pub struct CachedFtsDirectory {
    directory: HybridBTreeDirectory,
}

impl std::fmt::Debug for CachedFtsDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFtsDirectory")
            .field("directory", &"HybridBTreeDirectory")
            .finish()
    }
}

/// FTS index attachment that holds configuration and creates cursors for queries.
///
/// Created by `FtsIndexMethod::attach()` and implements `IndexMethodAttachment`.
/// Stores the Tantivy schema, field mappings, query patterns, and a shared
/// directory cache to optimize repeated queries.
#[allow(dead_code)]
#[derive(Debug)]
pub struct FtsIndexAttachment {
    /// Internal configuration
    cfg: IndexMethodConfiguration,
    /// Tantivy schema for the FTS index
    schema: Schema,
    /// Tantivy field for the rowid column
    rowid_field: Field,
    /// Schema fields for each indexed text column
    text_fields: Vec<(IndexColumn, Field)>,
    /// Parsed query patterns for FTS queries
    patterns: Vec<Select>,
    /// Name of the tantivy tokenizer to use
    tokenizer_name: String,
    /// Weights for each field in FTS scoring
    /// Created from WITH clause parameters
    /// e.g. WITH (tokenizer='default',weights='col1=1.0,col2=2.0')
    field_weights: HashMap<String, f32>,
    /// In-memory cached tantivy directory state
    cached_directory_state: Arc<RwLock<Option<CachedFtsDirectory>>>,
}

/// Supported tokenizer names for FTS indexes
pub const SUPPORTED_TOKENIZERS: &[&str] = &[
    "default",    // Tantivy default: lowercase + punctuation split + 40 char limit
    "raw",        // No tokenization - exact match only
    "simple",     // Basic whitespace/punctuation split
    "whitespace", // Split on whitespace only
    "ngram",      // N-gram tokenizer (2-3 chars by default)
];

impl FtsIndexAttachment {
    pub fn new(cfg: IndexMethodConfiguration) -> Result<Self> {
        // Parse tokenizer from WITH clause parameters, default to "default"
        // The parser may include surrounding quotes in the value, so we strip them
        let tokenizer_name = cfg
            .parameters
            .get("tokenizer")
            .and_then(|v| match v {
                Value::Text(t) => {
                    let s = t.to_string();
                    // Strip surrounding single or double quotes if present
                    let trimmed = s.trim_matches(|c| c == '\'' || c == '"');
                    Some(trimmed.to_string())
                }
                _ => None,
            })
            .unwrap_or_else(|| "default".to_string());

        // Validate tokenizer name
        if !SUPPORTED_TOKENIZERS.contains(&tokenizer_name.as_str()) {
            return Err(LimboError::ParseError(format!(
                "unsupported FTS tokenizer '{}'. Supported tokenizers: {}",
                tokenizer_name,
                SUPPORTED_TOKENIZERS.join(", ")
            )));
        }

        // Parse field weights from WITH clause: weights='body=2.0,title=1.0'
        let field_weights = if let Some(weights_value) = cfg.parameters.get("weights") {
            let weights_str = match weights_value {
                Value::Text(t) => {
                    let s = t.to_string();
                    s.trim_matches(|c| c == '\'' || c == '"').to_string()
                }
                _ => String::new(),
            };
            parse_field_weights(&weights_str, &cfg.columns)?
        } else {
            HashMap::new()
        };

        // Build Tantivy schema (no Directory or Index creation yet)
        let mut schema_builder = Schema::builder();

        let rowid_field = schema_builder
            .add_i64_field("rowid", tantivy::schema::INDEXED | tantivy::schema::STORED);

        let mut text_fields = Vec::with_capacity(cfg.columns.len());
        for col in &cfg.columns {
            let opts = tantivy::schema::TextOptions::default()
                .set_indexing_options(
                    tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(&tokenizer_name)
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                )
                .set_stored();
            let field = schema_builder.add_text_field(&col.name, opts);
            text_fields.push((col.clone(), field));
        }

        let schema = schema_builder.build();

        // Build query patterns for FTS
        // Order matters: more specific patterns should come first
        // Pattern 0: SELECT fts_score(col1, col2, ..., 'query') as score FROM table ORDER BY score DESC LIMIT ?
        // Pattern 1: SELECT fts_score(col1, col2, ..., 'query') as score FROM table WHERE fts_match(col1, col2, ..., 'query')
        //            (combined: both score and match with same query - must come before pattern 2)
        // Pattern 2: SELECT * FROM table WHERE fts_match(col1, col2, ..., 'query')
        let cols = cfg
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        // Build all FTS patterns - more specific patterns first
        // Use explicit ?1 for shared parameters between fts_score and fts_match

        // Pattern 0: score with ORDER BY DESC LIMIT
        let score_pattern = format!(
            "SELECT fts_score({}, ?) as score FROM {} ORDER BY score DESC LIMIT ?",
            cols, cfg.table_name
        );
        // Pattern 1: combined + ORDER BY DESC + LIMIT (most specific)
        let combined_ordered_limit = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) ORDER BY score DESC LIMIT ?",
            cols, cfg.table_name, cols
        );
        // Pattern 2: combined + ORDER BY DESC (no LIMIT)
        let combined_ordered = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) ORDER BY score DESC",
            cols, cfg.table_name, cols
        );
        // Pattern 3: combined + LIMIT (no ORDER BY)
        let combined_limit = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) LIMIT ?",
            cols, cfg.table_name, cols
        );
        // Pattern 4: combined (no ORDER BY, no LIMIT)
        let combined = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1)",
            cols, cfg.table_name, cols
        );
        // Pattern 5: match + LIMIT
        let match_limit = format!(
            "SELECT * FROM {} WHERE fts_match({}, ?) LIMIT ?",
            cfg.table_name, cols
        );
        // Pattern 6: match (no LIMIT)
        let match_pattern = format!(
            "SELECT * FROM {} WHERE fts_match({}, ?)",
            cfg.table_name, cols
        );
        let patterns = parse_patterns(&[
            &score_pattern,          // 0
            &combined_ordered_limit, // 1
            &combined_ordered,       // 2
            &combined_limit,         // 3
            &combined,               // 4
            &match_limit,            // 5
            &match_pattern,          // 6
        ])?;
        Ok(Self {
            cfg,
            schema,
            rowid_field,
            text_fields,
            patterns,
            tokenizer_name,
            field_weights,
            cached_directory_state: Arc::new(RwLock::new(None)),
        })
    }
}

impl IndexMethodAttachment for FtsIndexAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: FTS_INDEX_METHOD_NAME,
            index_name: &self.cfg.index_name,
            patterns: &self.patterns,
            backing_btree: false,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(FtsCursor::new(
            &self.cfg,
            self.schema.clone(),
            self.rowid_field,
            self.text_fields.clone(),
            self.tokenizer_name.clone(),
            self.field_weights.clone(),
            self.cached_directory_state.clone(),
        )))
    }
}

const NOTNULL_CONSTRAINT: ast::NamedColumnConstraint = ast::NamedColumnConstraint {
    name: None,
    constraint: ast::ColumnConstraint::NotNull {
        nullable: false,
        conflict_clause: None,
    },
};

fn initialize_btree_storage_table(conn: &Arc<Connection>, table_name: &str) -> Result<()> {
    // inline ast to reduce parsing overhead
    // CREATE TABLE table_name (path TEXT NOT NULL, chunk_no INTEGER NOT NULL, bytes BLOB NOT NULL);
    let create_table_stmt = ast::Stmt::CreateTable {
        body: ast::CreateTableBody::ColumnsAndConstraints {
            columns: vec![
                ast::ColumnDefinition {
                    col_name: name("path"),
                    col_type: Some(ast::Type {
                        name: "TEXT".to_string(),
                        size: None,
                    }),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
                ast::ColumnDefinition {
                    col_name: name("chunk_no"),
                    col_type: Some(ast::Type {
                        name: "INTEGER".to_string(),
                        size: None,
                    }),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
                ast::ColumnDefinition {
                    col_name: name("bytes"),
                    col_type: Some(ast::Type {
                        name: "BLOB".to_string(),
                        size: None,
                    }),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
            ],
            constraints: vec![],
            options: ast::TableOptions::empty(),
        },
        temporary: false,
        if_not_exists: true,
        tbl_name: ast::QualifiedName::single(name(table_name)),
    };
    // "CREATE INDEX IF NOT EXISTS idx_name ON table_name USING backing_btree (path, chunk_no, bytes);"
    // Use backing_btree to create a BTree that stores all columns without rowid indirection
    // This allows direct cursor access with the exact key structure
    let create_index_stmt = ast::Stmt::CreateIndex {
        unique: false, // backing_btree doesn't use unique constraint
        if_not_exists: true,
        idx_name: ast::QualifiedName::single(name(format!("{table_name}_key"))),
        tbl_name: name(table_name),
        using: Some(name("backing_btree")),
        columns: vec![
            ast::SortedColumn {
                expr: Box::new(ast::Expr::Name(name("path"))),
                order: None,
                nulls: None,
            },
            ast::SortedColumn {
                expr: Box::new(ast::Expr::Name(name("chunk_no"))),
                order: None,
                nulls: None,
            },
            ast::SortedColumn {
                expr: Box::new(ast::Expr::Name(name("bytes"))),
                order: None,
                nulls: None,
            },
        ],
        where_clause: None,
        with_clause: vec![],
    };
    // Execute nested statements without subtransactions to avoid DatabaseBusy
    // (we're already inside a transaction from the parent CREATE INDEX statement)
    {
        conn.start_nested();
        let mut stmt = conn.prepare_stmt(create_table_stmt)?;
        stmt.program.needs_stmt_subtransactions = false;
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }
    {
        conn.start_nested();
        let mut stmt = conn.prepare_stmt(create_index_stmt)?;
        stmt.program.needs_stmt_subtransactions = false;
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }

    Ok(())
}

/// Pattern indices for FTS queries
const FTS_PATTERN_SCORE: i64 = 0;
const FTS_PATTERN_COMBINED_ORDERED_LIMIT: i64 = 1;
const FTS_PATTERN_COMBINED_ORDERED: i64 = 2;
const FTS_PATTERN_COMBINED_LIMIT: i64 = 3;
const FTS_PATTERN_COMBINED: i64 = 4;
const FTS_PATTERN_MATCH_LIMIT: i64 = 5;
const FTS_PATTERN_MATCH: i64 = 6;
const TANTIVY_META_FILE: &str = "meta.json";

/// State machine for FTS cursor async operations
#[derive(Debug)]
enum FtsState {
    /// Initial state
    Init,
    /// Rewinding cursor to start
    Rewinding,
    /// Loading file catalog from BTree (metadata only, not content)
    /// This is the new catalog-first approach for HybridBTreeDirectory
    LoadingCatalog {
        /// Accumulated file metadata: path -> (chunk_no -> blob_size)
        /// Using nested HashMap to properly deduplicate by (path, chunk_no) pairs
        catalog_builder: HashMap<PathBuf, HashMap<i64, usize>>,
        current_path: Option<String>,
    },
    /// Preloading essential files (meta.json and other hot files)
    PreloadingEssentials {
        /// Files that need to be preloaded
        files_to_load: Vec<PathBuf>,
        /// Files already loaded
        loaded_files: HashMap<PathBuf, Vec<u8>>,
        /// Current file being loaded
        current_loading: Option<PathBuf>,
        /// Current chunks being accumulated for the file being loaded
        current_chunks: Vec<(i64, Vec<u8>)>,
    },
    /// Creating/opening Tantivy index
    CreatingIndex,
    /// Ready for operations
    Ready,
    /// Seeking to first chunk of a path before deleting old chunks
    SeekingOldChunks {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Advancing cursor after seek returned TryAdvance
    AdvancingAfterSeek {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Checking if current record's path matches (to determine if it should be deleted)
    CheckingChunkPath {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Performing the actual delete of a chunk
    DeletingChunk {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Advancing cursor after delete to check next record
    AdvancingAfterDelete {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Flushing pending writes to BTree - seeking phase
    SeekingWrite {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        chunk_idx: usize,
    },
    /// Flushing pending writes to BTree - insert phase (after seek completed)
    InsertingWrite {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        chunk_idx: usize,
        record: ImmutableRecord,
    },
    /// Flushing pending writes to BTree - tracking state
    FlushingWrites {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        chunk_idx: usize,
    },
    /// Flushing pending deletes to BTree
    FlushingDeletes {
        deletes: Vec<PathBuf>,
        delete_idx: usize,
    },
    /// Seeking for delete operation
    SeekingDelete {
        deletes: Vec<PathBuf>,
        delete_idx: usize,
    },
    /// Deleting record at cursor position
    DeletingRecord {
        deletes: Vec<PathBuf>,
        delete_idx: usize,
    },
}

/// Cursor for executing FTS operations (queries, inserts, deletes).
///
/// Implements `IndexMethodCursor` to integrate with turso's VDBE execution.
/// Uses a state machine pattern for async IO operations. Manages:
/// - Tantivy index/reader/writer/searcher instances
/// - BTree storage via `HybridBTreeDirectory`
/// - Document batching for efficient bulk inserts
/// - Query result iteration
#[allow(dead_code)]
pub struct FtsCursor {
    schema: Schema,
    rowid_field: Field,
    text_fields: Vec<(IndexColumn, Field)>,
    dir_table_name: String,
    tokenizer_name: String,
    field_weights: HashMap<String, f32>,
    shared_directory_cache: Arc<RwLock<Option<CachedFtsDirectory>>>,
    connection: Option<Arc<Connection>>,
    fts_dir_cursor: Option<BTreeCursor>,
    btree_root_page: Option<i64>,
    hybrid_directory: Option<HybridBTreeDirectory>,
    index: Option<Index>,
    reader: Option<IndexReader>,
    writer: Option<IndexWriter>,
    searcher: Option<Searcher>,
    state: FtsState,
    pending_docs_count: usize,
    current_hits: Vec<(f32, DocAddress, i64)>,
    hit_pos: usize,
    current_pattern: i64,
}

impl FtsCursor {
    /// Maximum results when no LIMIT is specified (10 million).
    const MAX_NO_LIMIT_RESULT: usize = 10_000_000;

    /// Creates a new FTS cursor with the given configuration.
    pub fn new(
        cfg: &IndexMethodConfiguration,
        schema: Schema,
        rowid_field: Field,
        text_fields: Vec<(IndexColumn, Field)>,
        tokenizer_name: String,
        field_weights: HashMap<String, f32>,
        shared_directory_cache: Arc<RwLock<Option<CachedFtsDirectory>>>,
    ) -> Self {
        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            cfg.index_name
        );
        Self {
            schema,
            rowid_field,
            text_fields,
            dir_table_name,
            tokenizer_name,
            field_weights,
            shared_directory_cache,
            connection: None,
            fts_dir_cursor: None,
            btree_root_page: None,
            hybrid_directory: None,
            index: None,
            reader: None,
            writer: None,
            searcher: None,
            state: FtsState::Init,
            pending_docs_count: 0,
            current_hits: Vec::new(),
            hit_pos: 0,
            current_pattern: FTS_PATTERN_SCORE,
        }
    }

    /// Open the BTree cursor for FTS directory storage
    fn open_cursor(&mut self, conn: &Arc<Connection>) -> Result<()> {
        if self.fts_dir_cursor.is_some() {
            return Ok(());
        }
        // Open cursor for the FTS directory index
        // The index stores all 3 columns: (path, chunk_no, bytes) as the key
        // This is similar to how toy_vector_sparse_ivf stores all data in the index
        let index_name = format!("{}_key", self.dir_table_name);

        // Get root page for HybridBTreeDirectory
        let pager = conn.pager.load().clone();
        let schema = conn.schema.read();
        let scratch = schema
            .get_index(&self.dir_table_name, &index_name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index {} for table {} not found",
                    index_name, self.dir_table_name
                ))
            })?;
        let root_page = scratch.root_page;
        drop(schema);

        self.btree_root_page = Some(root_page);

        let mut cursor = BTreeCursor::new(pager, root_page, 3);
        cursor.index_info = Some(Arc::new(IndexInfo {
            has_rowid: false,
            num_cols: 3,
            key_info: vec![key_info(), key_info(), key_info()],
        }));
        self.fts_dir_cursor = Some(cursor);
        Ok(())
    }

    /// Register custom tokenizers with Tantivy index
    fn register_tokenizers(&self, index: &Index) {
        let tokenizers = index.tokenizers();

        // Register "raw" tokenizer - no tokenization, exact match only
        tokenizers.register("raw", RawTokenizer::default());

        // Register "simple" tokenizer - basic whitespace/punctuation split
        tokenizers.register("simple", SimpleTokenizer::default());

        // Register "whitespace" tokenizer - split on whitespace only
        tokenizers.register("whitespace", WhitespaceTokenizer::default());

        // Register "ngram" tokenizer - 2-3 character n-grams for substring matching
        // Using prefix=false for full n-gram (not just prefix)
        if let Ok(ngram) = NgramTokenizer::new(2, 3, false) {
            tokenizers.register("ngram", ngram);
        }
    }

    /// Create Tantivy index from directory (hybrid or cached)
    fn create_index_from_directory(&mut self) -> Result<()> {
        // Prefer HybridBTreeDirectory if available
        if let Some(ref hybrid_dir) = self.hybrid_directory {
            let index_exists = hybrid_dir
                .exists(Path::new(TANTIVY_META_FILE))
                .unwrap_or(false);

            let index = if index_exists {
                Index::open(hybrid_dir.clone())
                    .map_err(|e| LimboError::InternalError(e.to_string()))?
            } else {
                Index::create(
                    hybrid_dir.clone(),
                    self.schema.clone(),
                    IndexSettings::default(),
                )
                .map_err(|e| LimboError::InternalError(e.to_string()))?
            };

            // Register custom tokenizers
            self.register_tokenizers(&index);

            self.index = Some(index);
            return Ok(());
        }

        Err(LimboError::InternalError("no directory initialized".into()))
    }

    /// Internal helper to continue flush_writes state machine
    fn flush_writes_internal(&mut self) -> Result<IOResult<()>> {
        loop {
            match &mut self.state {
                FtsState::FlushingWrites {
                    writes,
                    write_idx,
                    chunk_idx,
                } => {
                    if *write_idx >= writes.len() {
                        // Done with writes - clear flushing_writes since data is now in BTree
                        if let Some(ref dir) = self.hybrid_directory {
                            dir.complete_flush();
                        }
                        self.state = FtsState::Ready;
                        return Ok(IOResult::Done(()));
                    }

                    // If starting a new file (chunk_idx == 0), first delete old chunks
                    if *chunk_idx == 0 {
                        let path_str = writes[*write_idx].0.to_string_lossy().to_string();
                        self.state = FtsState::SeekingOldChunks {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            path_str,
                        };
                        continue;
                    }

                    let (_, data) = &writes[*write_idx];
                    let chunk_size = DEFAULT_CHUNK_SIZE;
                    let total_chunks = data.len().div_ceil(chunk_size);

                    // Adjust chunk_idx if it was the special "start writing" marker
                    let actual_chunk_idx = if *chunk_idx == usize::MAX {
                        0
                    } else {
                        *chunk_idx
                    };

                    if actual_chunk_idx >= total_chunks.max(1) {
                        // Move to next file
                        *write_idx += 1;
                        *chunk_idx = 0;
                        continue;
                    }

                    // Transition to seeking state for writing this chunk
                    self.state = FtsState::SeekingWrite {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        chunk_idx: actual_chunk_idx,
                    };
                }
                FtsState::SeekingOldChunks {
                    writes,
                    write_idx,
                    path_str,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    tracing::debug!("FTS flush: deleting old chunks for path={}", path_str);

                    // Seek to first chunk of this path (with empty blob as minimum)
                    let seek_key = ImmutableRecord::from_values(
                        &[
                            Value::Text(Text::new(path_str.clone())),
                            Value::Integer(0),
                            Value::Blob(vec![]),
                        ],
                        3,
                    );

                    let seek_result =
                        return_if_io!(cursor
                            .seek(SeekKey::IndexKey(&seek_key), SeekOp::GE { eq_only: false }));

                    match seek_result {
                        SeekResult::NotFound => {
                            // No matching records at all, start writing
                            self.state = FtsState::FlushingWrites {
                                writes: std::mem::take(writes),
                                write_idx: *write_idx,
                                chunk_idx: usize::MAX,
                            };
                        }
                        SeekResult::TryAdvance => {
                            // Cursor positioned at leaf but not on matching entry, need to advance
                            self.state = FtsState::AdvancingAfterSeek {
                                writes: std::mem::take(writes),
                                write_idx: *write_idx,
                                path_str: std::mem::take(path_str),
                            };
                        }
                        SeekResult::Found => {
                            // Found a record at or after our seek key, check it
                            self.state = FtsState::CheckingChunkPath {
                                writes: std::mem::take(writes),
                                write_idx: *write_idx,
                                path_str: std::mem::take(path_str),
                            };
                        }
                    }
                }
                FtsState::AdvancingAfterSeek {
                    writes,
                    write_idx,
                    path_str,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    return_if_io!(cursor.next());
                    let has_next = cursor.has_record();

                    if has_next {
                        // Now positioned on a record, check if it matches our path
                        self.state = FtsState::CheckingChunkPath {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            path_str: std::mem::take(path_str),
                        };
                    } else {
                        // No more records, start writing
                        self.state = FtsState::FlushingWrites {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: usize::MAX,
                        };
                    }
                }
                FtsState::CheckingChunkPath {
                    writes,
                    write_idx,
                    path_str,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    if !cursor.has_record() {
                        // No more records, start writing new chunks
                        self.state = FtsState::FlushingWrites {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: usize::MAX, // Special value to trigger first write
                        };
                        continue;
                    }

                    // Check if current record matches our path
                    let record = return_if_io!(cursor.record());
                    let current_path = record.as_ref().and_then(|r| {
                        r.get_value_opt(0).and_then(|v| match v {
                            crate::types::ValueRef::Text(t) => Some(t.value.to_string()),
                            _ => None,
                        })
                    });

                    if current_path.as_deref() == Some(path_str.as_str()) {
                        // Transition to DeletingChunk to actually do the delete
                        self.state = FtsState::DeletingChunk {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            path_str: std::mem::take(path_str),
                        };
                    } else {
                        // No more chunks for this path, start writing new chunks
                        // Use usize::MAX as special marker that old chunks have been deleted
                        self.state = FtsState::FlushingWrites {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: usize::MAX,
                        };
                    }
                }
                FtsState::DeletingChunk {
                    writes,
                    write_idx,
                    path_str,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    // Perform the delete - if IO is needed, we'll come back to this state
                    return_if_io!(cursor.delete());

                    // Delete completed, advance cursor to next record before checking again
                    self.state = FtsState::AdvancingAfterDelete {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        path_str: std::mem::take(path_str),
                    };
                }
                FtsState::AdvancingAfterDelete {
                    writes,
                    write_idx,
                    path_str,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    // Advance cursor to next record after delete
                    return_if_io!(cursor.next());
                    let has_next = cursor.has_record();

                    if has_next {
                        // Check the next record in CheckingChunkPath state
                        self.state = FtsState::CheckingChunkPath {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            path_str: std::mem::take(path_str),
                        };
                    } else {
                        // No more records, start writing
                        self.state = FtsState::FlushingWrites {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: usize::MAX,
                        };
                    }
                }
                FtsState::SeekingWrite {
                    writes,
                    write_idx,
                    chunk_idx,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    let (path, data) = &writes[*write_idx];
                    let path_str = path.to_string_lossy().to_string();
                    let chunk_size = DEFAULT_CHUNK_SIZE;
                    let actual_chunk_idx = if *chunk_idx == usize::MAX {
                        0
                    } else {
                        *chunk_idx
                    };

                    let start = actual_chunk_idx * chunk_size;
                    let end = (start + chunk_size).min(data.len());
                    let chunk_data = if start < data.len() {
                        &data[start..end]
                    } else {
                        &[]
                    };

                    // Create record: [path, chunk_no, bytes]
                    let record = ImmutableRecord::from_values(
                        &[
                            Value::Text(Text::new(path_str.clone())),
                            Value::Integer(actual_chunk_idx as i64),
                            Value::Blob(chunk_data.to_vec()),
                        ],
                        3,
                    );

                    // Seek to find the correct position using GE (not eq_only)
                    // This positions the cursor at or after where the record should be inserted
                    let _result = return_if_io!(
                        cursor.seek(SeekKey::IndexKey(&record), SeekOp::GE { eq_only: false })
                    );

                    // Transition to InsertingWrite - don't do insert in same state to avoid re-seeking on IO
                    self.state = FtsState::InsertingWrite {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        chunk_idx: actual_chunk_idx,
                        record,
                    };
                }
                FtsState::InsertingWrite {
                    writes,
                    write_idx,
                    chunk_idx,
                    record,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    // Insert into BTree - the cursor should be positioned correctly after seek
                    return_if_io!(cursor.insert(&BTreeKey::IndexKey(record)));

                    // Move to next chunk
                    self.state = FtsState::FlushingWrites {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        chunk_idx: *chunk_idx + 1,
                    };
                }
                FtsState::Ready => {
                    return Ok(IOResult::Done(()));
                }
                _ => {
                    return Err(LimboError::InternalError(
                        "unexpected state in flush_writes_internal".into(),
                    ));
                }
            }
        }
    }

    /// Internal helper to continue flush_deletes state machine
    fn flush_deletes_internal(&mut self) -> Result<IOResult<()>> {
        loop {
            match &mut self.state {
                FtsState::FlushingDeletes {
                    deletes,
                    delete_idx,
                } => {
                    if *delete_idx >= deletes.len() {
                        self.state = FtsState::Ready;
                        return Ok(IOResult::Done(()));
                    }

                    self.state = FtsState::SeekingDelete {
                        deletes: std::mem::take(deletes),
                        delete_idx: *delete_idx,
                    };
                }
                FtsState::SeekingDelete {
                    deletes,
                    delete_idx,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    let path = &deletes[*delete_idx];
                    let path_str = path.to_string_lossy().to_string();

                    // Seek to first chunk of this path with empty blob (minimum value for bytes)
                    let seek_key = ImmutableRecord::from_values(
                        &[
                            Value::Text(Text::new(path_str)),
                            Value::Integer(0),
                            Value::Blob(vec![]),
                        ],
                        3,
                    );

                    let _result =
                        return_if_io!(cursor
                            .seek(SeekKey::IndexKey(&seek_key), SeekOp::GE { eq_only: false }));

                    self.state = FtsState::DeletingRecord {
                        deletes: std::mem::take(deletes),
                        delete_idx: *delete_idx,
                    };
                }
                FtsState::DeletingRecord {
                    deletes,
                    delete_idx,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    let path = &deletes[*delete_idx];
                    let path_str = path.to_string_lossy().to_string();

                    if !cursor.has_record() {
                        // No more records, move to next path
                        *delete_idx += 1;
                        if *delete_idx >= deletes.len() {
                            self.state = FtsState::Ready;
                            return Ok(IOResult::Done(()));
                        }
                        self.state = FtsState::FlushingDeletes {
                            deletes: std::mem::take(deletes),
                            delete_idx: *delete_idx,
                        };
                        continue;
                    }

                    // Check if current record matches our path
                    let record = return_if_io!(cursor.record());
                    let matches = if let Some(record) = record {
                        match record.get_value_opt(0) {
                            Some(crate::types::ValueRef::Text(t)) => t.value == path_str,
                            _ => false,
                        }
                    } else {
                        false
                    };

                    if matches {
                        // Delete this record
                        return_if_io!(cursor.delete());
                        // Cursor automatically moves to next, stay in this state
                    } else {
                        // No more chunks for this path, move to next
                        *delete_idx += 1;
                        if *delete_idx >= deletes.len() {
                            self.state = FtsState::Ready;
                            return Ok(IOResult::Done(()));
                        }
                        self.state = FtsState::FlushingDeletes {
                            deletes: std::mem::take(deletes),
                            delete_idx: *delete_idx,
                        };
                    }
                }
                FtsState::Ready => {
                    return Ok(IOResult::Done(()));
                }
                _ => {
                    return Err(LimboError::InternalError(
                        "unexpected state in flush_deletes_internal".into(),
                    ));
                }
            }
        }
    }

    /// Commit pending documents to Tantivy and flush to BTree.
    /// If `force_flush` is true, flushes directory writes even when no pending docs.
    fn commit_and_flush_inner(&mut self, force_flush: bool) -> Result<IOResult<()>> {
        // Handle flush state machine if already in progress
        match &self.state {
            FtsState::FlushingWrites { .. }
            | FtsState::SeekingOldChunks { .. }
            | FtsState::AdvancingAfterSeek { .. }
            | FtsState::CheckingChunkPath { .. }
            | FtsState::DeletingChunk { .. }
            | FtsState::AdvancingAfterDelete { .. }
            | FtsState::SeekingWrite { .. }
            | FtsState::InsertingWrite { .. } => {
                return self.flush_writes_internal();
            }
            _ => {}
        }

        if self.pending_docs_count == 0 && !force_flush {
            return Ok(IOResult::Done(()));
        }

        // Commit Tantivy to make documents visible
        if let Some(ref mut writer) = self.writer {
            tracing::debug!(
                "FTS commit_and_flush: committing {} documents",
                self.pending_docs_count
            );
            writer
                .commit()
                .map_err(|e| LimboError::InternalError(format!("FTS commit error: {e}")))?;

            // Invalidate shared directory cache since index has changed
            // Next query will reload the updated catalog
            {
                let mut cache = self.shared_directory_cache.write();
                if cache.is_some() {
                    tracing::debug!("FTS commit_and_flush: invalidating cached directory");
                    *cache = None;
                }
            }
        }
        if let Some(ref reader) = self.reader {
            reader
                .reload()
                .map_err(|e| LimboError::InternalError(format!("FTS reader reload error: {e}")))?;
            self.searcher = Some(reader.searcher());
        }

        self.pending_docs_count = 0;

        // Flush pending writes to BTree via async state machine
        let writes = self
            .hybrid_directory
            .as_ref()
            .map(|dir| dir.take_pending_writes())
            .unwrap_or_default();

        if !writes.is_empty() {
            tracing::debug!(
                "FTS commit_and_flush: flushing {} files to BTree",
                writes.len()
            );
            self.state = FtsState::FlushingWrites {
                writes,
                write_idx: 0,
                chunk_idx: 0,
            };
            return self.flush_writes_internal();
        }

        Ok(IOResult::Done(()))
    }

    /// Commit pending documents to Tantivy and flush to BTree.
    pub fn commit_and_flush(&mut self) -> Result<IOResult<()>> {
        self.commit_and_flush_inner(false)
    }
}

impl Drop for FtsCursor {
    fn drop(&mut self) {
        // Skip cleanup if we're already panicking
        if std::thread::panicking() {
            return;
        }

        // Get connection reference for transaction check and pager access
        let conn = match &self.connection {
            Some(conn) => conn.clone(),
            None => {
                if self.pending_docs_count > 0 {
                    tracing::warn!(
                        "FTS Drop: {} pending documents lost (no connection)",
                        self.pending_docs_count
                    );
                }
                return;
            }
        };

        let pager = conn.pager.load().clone();

        // Check if we're already in a flushing state (from commit_and_flush)
        // This can happen when commit_and_flush started a flush but yielded for IO
        // and the cursor is being dropped before the flush completed
        let is_flushing = matches!(
            &self.state,
            FtsState::FlushingWrites { .. }
                | FtsState::SeekingOldChunks { .. }
                | FtsState::AdvancingAfterSeek { .. }
                | FtsState::CheckingChunkPath { .. }
                | FtsState::DeletingChunk { .. }
                | FtsState::AdvancingAfterDelete { .. }
                | FtsState::SeekingWrite { .. }
                | FtsState::InsertingWrite { .. }
        );

        if is_flushing {
            turso_assert!(conn.is_in_write_tx(), "FTS Drop: in-progress flush abandoned (transaction already committed). pre_commit should have completed the flush.");

            tracing::debug!("FTS Drop: completing in-progress flush");
            loop {
                match self.flush_writes_internal() {
                    Ok(IOResult::Done(())) => break,
                    Ok(IOResult::IO(_)) => {
                        if let Err(e) = pager.io.step() {
                            tracing::error!("FTS Drop: IO error during flush: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("FTS Drop: error during flush: {}", e);
                        break;
                    }
                }
            }
            return;
        }

        // Only flush new pending documents if we have any
        if self.pending_docs_count == 0 {
            return;
        }

        // If the transaction has already committed (auto-commit), flushing to BTree
        // would create dirty pages outside of any transaction, causing the
        // "dirty pages must be empty for read txn" panic on the next read.
        turso_assert!(
            conn.is_in_write_tx(),
            "FTS Drop: {} docs remaining: transaction already committed, cannot flush",
            self.pending_docs_count
        );

        // Commit any pending writes to Tantivy
        if let Some(ref mut writer) = self.writer {
            if let Err(e) = writer.commit() {
                tracing::error!("FTS Drop: failed to commit writer: {}", e);
                return;
            }

            // Invalidate shared directory cache since index has changed
            // This MUST happen after commit but before we check for pending writes
            // to ensure the next cursor loads fresh data from BTree
            {
                let mut cache = self.shared_directory_cache.write();
                if cache.is_some() {
                    tracing::debug!("FTS Drop: invalidating cached directory");
                    *cache = None;
                }
            }
        }

        let Some(ref dir) = self.hybrid_directory else {
            return;
        };
        let writes = dir.take_pending_writes();

        if writes.is_empty() {
            return;
        }

        tracing::debug!(
            "FTS Drop: blocking flush of {} files to BTree",
            writes.len()
        );

        // Set up flush state machine
        self.state = FtsState::FlushingWrites {
            writes,
            write_idx: 0,
            chunk_idx: 0,
        };

        // Run blocking flush
        loop {
            match self.flush_writes_internal() {
                Ok(IOResult::Done(())) => break,
                Ok(IOResult::IO(_)) => {
                    // Advance IO
                    if let Err(e) = pager.io.step() {
                        tracing::error!("FTS Drop: IO error during flush: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("FTS Drop: error during flush: {}", e);
                    break;
                }
            }
        }
    }
}

impl IndexMethodCursor for FtsCursor {
    /// Creates the FTS index storage (internal BTree table for Tantivy files).
    fn create(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        initialize_btree_storage_table(conn, &self.dir_table_name)?;
        Ok(IOResult::Done(()))
    }

    /// Destroys the FTS index, dropping all storage and clearing caches.
    fn destroy(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        tracing::debug!(
            "FTS destroy: dropping internal storage {}",
            self.dir_table_name
        );

        // Drop all in-memory components first
        self.searcher = None;
        self.reader = None;
        self.writer = None;
        self.index = None;
        self.hybrid_directory = None;
        self.fts_dir_cursor = None;

        // Invalidate shared directory cache
        {
            let mut cache = self.shared_directory_cache.write();
            *cache = None;
        }

        // Drop the internal storage table and index
        // The backing_btree index will be dropped automatically when the table is dropped
        // Use start_nested() before prepare() to bypass system table protection,
        // then use prepare/run_ignore_rows pattern and disable subtransactions to avoid Busy error
        let drop_table_ast = ast::Stmt::DropTable {
            if_exists: true,
            tbl_name: ast::QualifiedName::single(ast::Name::exact(self.dir_table_name.clone())),
        };
        conn.start_nested();
        let mut stmt = conn.prepare_stmt(drop_table_ast)?;
        // Disable subtransactions since we're already inside a transaction from the parent DROP INDEX
        stmt.program.needs_stmt_subtransactions = false;
        let result = stmt.run_ignore_rows();
        conn.end_nested();
        result?;

        self.state = FtsState::Init;
        Ok(IOResult::Done(()))
    }

    /// Opens the index for reading, loading the catalog and creating a searcher.
    /// Uses async state machine for non-blocking IO during catalog/file loading.
    fn open_read(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        loop {
            match &mut self.state {
                FtsState::Init => {
                    self.connection = Some(conn.clone());
                    // Ensure storage table exists
                    initialize_btree_storage_table(conn, &self.dir_table_name)?;
                    // Open BTree cursor (needed for btree_root_page)
                    self.open_cursor(conn)?;

                    // Check for cached directory from attachment - avoids expensive catalog reload
                    // Note: We only cache the directory (with catalog), not Index/Reader
                    // Each cursor needs its own Index to handle writes correctly
                    {
                        let cache = self.shared_directory_cache.read();
                        if let Some(ref cached) = *cache {
                            tracing::debug!(
                                "FTS open_read: using cached directory (skipping catalog load)"
                            );
                            // Clone with fresh pending state to ensure this cursor's writes
                            // don't affect other cursors or cause Drop to flush after txn commits
                            self.hybrid_directory =
                                Some(cached.directory.clone_with_fresh_pending());
                            // Skip to CreatingIndex to build Index/Reader from cached directory
                            self.state = FtsState::CreatingIndex;
                            continue;
                        }
                    }

                    // No cache available, proceed with full catalog loading
                    self.state = FtsState::Rewinding;
                }
                FtsState::Rewinding => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    return_if_io!(cursor.rewind());
                    // Use catalog-first loading for HybridBTreeDirectory
                    self.state = FtsState::LoadingCatalog {
                        catalog_builder: HashMap::new(),
                        current_path: None,
                    };
                }
                FtsState::LoadingCatalog {
                    catalog_builder,
                    current_path,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    if !cursor.has_record() {
                        // Done scanning - build catalog and identify files to preload
                        let mut catalog = HashMap::new();
                        let mut files_to_load = Vec::new();

                        for (path, chunks) in catalog_builder.drain() {
                            // Calculate max_chunk and total_size from deduplicated chunks
                            let max_chunk = chunks.keys().max().copied().unwrap_or(0);
                            let total_size: usize = chunks.values().sum();
                            let num_chunks = (max_chunk + 1) as usize;
                            let metadata = FileMetadata::new(&path, total_size, num_chunks);

                            // Queue hot files for preloading
                            if metadata.category.should_preload() || metadata.category.is_hot() {
                                files_to_load.push(path.clone());
                            }

                            catalog.insert(path, metadata);
                        }

                        tracing::debug!(
                            "FTS LoadingCatalog: found {} files, {} to preload",
                            catalog.len(),
                            files_to_load.len()
                        );

                        // Create HybridBTreeDirectory with catalog
                        let pager = conn.pager.load().clone();
                        let root_page = self.btree_root_page.ok_or_else(|| {
                            LimboError::InternalError("btree_root_page not set".into())
                        })?;

                        let hybrid_dir = HybridBTreeDirectory::with_preloaded(
                            pager,
                            root_page,
                            catalog,
                            HashMap::new(), // Will be filled in PreloadingEssentials
                            DEFAULT_HOT_CACHE_BYTES,
                            DEFAULT_CHUNK_CACHE_BYTES,
                        );
                        self.hybrid_directory = Some(hybrid_dir);

                        if files_to_load.is_empty() {
                            // No files to preload, go directly to CreatingIndex
                            self.state = FtsState::CreatingIndex;
                        } else {
                            self.state = FtsState::PreloadingEssentials {
                                files_to_load,
                                loaded_files: HashMap::new(),
                                current_loading: None,
                                current_chunks: Vec::new(),
                            };
                        }
                        continue;
                    }

                    // Read record metadata (path, chunk_no, blob size estimate)
                    let record = return_if_io!(cursor.record());
                    if let Some(record) = record {
                        let path = record.get_value_opt(0).and_then(|v| match v {
                            crate::types::ValueRef::Text(t) => Some(t.value.to_string()),
                            _ => None,
                        });
                        let chunk_no = record.get_value_opt(1).and_then(|v| match v {
                            crate::types::ValueRef::Integer(i) => Some(i),
                            _ => None,
                        });
                        // Get blob size for estimation (we don't read the full blob)
                        let blob_size = record
                            .get_value_opt(2)
                            .map(|v| match v {
                                crate::types::ValueRef::Blob(b) => b.len(),
                                _ => 0,
                            })
                            .unwrap_or(0);

                        if let (Some(path_str), Some(chunk_no)) = (path, chunk_no) {
                            // Track path transition
                            if current_path.as_ref() != Some(&path_str) {
                                *current_path = Some(path_str.clone());
                            }

                            // Update catalog builder - store blob_size per chunk_no
                            // This properly deduplicates: if same chunk_no appears twice,
                            // we keep the last one (which should be the newest)
                            let path_buf = PathBuf::from(&path_str);
                            let chunks = catalog_builder.entry(path_buf).or_default();
                            chunks.insert(chunk_no, blob_size);
                        }
                    }

                    return_if_io!(cursor.next());
                }
                FtsState::PreloadingEssentials {
                    files_to_load,
                    loaded_files,
                    current_loading,
                    current_chunks,
                } => {
                    // Use blocking file load from HybridBTreeDirectory
                    let hybrid_dir = self.hybrid_directory.as_ref().ok_or_else(|| {
                        LimboError::InternalError("hybrid_directory not initialized".into())
                    })?;

                    // If we're loading a file, continue with it
                    if let Some(path) = current_loading.take() {
                        // We loaded chunks, finalize the file
                        if !current_chunks.is_empty() {
                            current_chunks.sort_by_key(|(chunk_no, _)| *chunk_no);

                            // Deduplicate
                            let mut deduped: Vec<(i64, Vec<u8>)> = Vec::new();
                            for (chunk_no, bytes) in current_chunks.drain(..) {
                                if let Some(last) = deduped.last_mut() {
                                    if last.0 == chunk_no {
                                        *last = (chunk_no, bytes);
                                    } else {
                                        deduped.push((chunk_no, bytes));
                                    }
                                } else {
                                    deduped.push((chunk_no, bytes));
                                }
                            }

                            let data: Vec<u8> =
                                deduped.iter().flat_map(|(_, b)| b.clone()).collect();
                            loaded_files.insert(path.clone(), data.clone());

                            // Add to hot cache
                            hybrid_dir.add_to_hot_cache(path, data);
                        }
                    }

                    // Check if we have more files to load
                    if let Some(next_path) = files_to_load.pop() {
                        // Load this file using blocking IO
                        match hybrid_dir.load_file_blocking(&next_path) {
                            Ok(data) => {
                                loaded_files.insert(next_path.clone(), data.clone());
                                hybrid_dir.add_to_hot_cache(next_path, data);
                            }
                            Err(e) => {
                                // File might not exist yet (new index), just log and continue
                                tracing::debug!(
                                    "FTS: could not preload {}: {}",
                                    next_path.display(),
                                    e
                                );
                            }
                        }
                        continue;
                    }

                    // All files loaded
                    tracing::debug!(
                        "FTS PreloadingEssentials: loaded {} files into hot cache",
                        loaded_files.len()
                    );
                    self.state = FtsState::CreatingIndex;
                    continue;
                }
                FtsState::CreatingIndex => {
                    // Log loaded files for debugging
                    if let Some(ref dir) = self.hybrid_directory {
                        tracing::debug!("FTS CreatingIndex: {:?}", dir);
                    }

                    // Create Tantivy index from directory
                    self.create_index_from_directory()?;

                    // Create reader and searcher
                    if let Some(ref index) = self.index {
                        self.reader = Some(
                            index
                                .reader()
                                .map_err(|e| LimboError::InternalError(e.to_string()))?,
                        );
                        if let Some(ref reader) = self.reader {
                            self.searcher = Some(reader.searcher());
                        }
                    }

                    // Cache the directory for future queries (avoids catalog reload)
                    // Note: We only cache the directory, not Index/Reader, so each cursor
                    // gets its own Index for proper write isolation
                    if let Some(ref dir) = self.hybrid_directory {
                        let mut cache = self.shared_directory_cache.write();
                        *cache = Some(CachedFtsDirectory {
                            directory: dir.clone(),
                        });
                        tracing::debug!("FTS CreatingIndex: cached directory for future queries");
                    }

                    self.state = FtsState::Ready;
                    return Ok(IOResult::Done(()));
                }
                FtsState::Ready => {
                    return Ok(IOResult::Done(()));
                }
                _ => {
                    return Err(LimboError::InternalError(
                        "unexpected state in open_read".into(),
                    ));
                }
            }
        }
    }

    /// Opens the index for writing, creating the IndexWriter.
    /// Calls `open_read` first if not already initialized.
    fn open_write(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        if self.connection.is_none() {
            self.connection = Some(conn.clone());
        }

        // First do open_read to load existing index
        match &self.state {
            FtsState::Ready => {}
            _ => {
                let result = self.open_read(conn)?;
                if let IOResult::IO(io) = result {
                    return Ok(IOResult::IO(io));
                }
            }
        }
        // Should we assert no writer here? Tantivy enforces single writer
        // it's just unsure if this can be called multiple times
        if self.writer.is_some() {
            return Ok(IOResult::Done(()));
        }

        // Now create writer
        if let Some(ref index) = self.index {
            // Use single-threaded mode to avoid concurrent access
            let writer = index
                .writer_with_num_threads(1, DEFAULT_MEMORY_BUDGET_BYTES)
                .map_err(|e| LimboError::InternalError(e.to_string()))?;
            // Disable background merges
            writer.set_merge_policy(Box::new(NoMergePolicy));
            self.writer = Some(writer);
        }
        Ok(IOResult::Done(()))
    }

    /// Inserts a document into the FTS index.
    /// Values are text columns followed by rowid. Batches commits for efficiency.
    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        // Handle flush state machine if in progress
        loop {
            match &self.state {
                FtsState::FlushingWrites { .. }
                | FtsState::SeekingOldChunks { .. }
                | FtsState::AdvancingAfterSeek { .. }
                | FtsState::CheckingChunkPath { .. }
                | FtsState::DeletingChunk { .. }
                | FtsState::AdvancingAfterDelete { .. }
                | FtsState::SeekingWrite { .. }
                | FtsState::InsertingWrite { .. } => {
                    let result = self.flush_writes_internal()?;
                    match result {
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        IOResult::Done(()) => continue, // Flush done, check state again
                    }
                }
                FtsState::FlushingDeletes { .. }
                | FtsState::SeekingDelete { .. }
                | FtsState::DeletingRecord { .. } => {
                    let result = self.flush_deletes_internal()?;
                    match result {
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        IOResult::Done(()) => continue, // Flush done, check state again
                    }
                }
                _ => break, // Not flushing, proceed with insert
            }
        }

        let Some(ref mut writer) = self.writer else {
            return Err(LimboError::InternalError(
                "FTS writer not initialized - call open_write first".into(),
            ));
        };

        // Last register is rowid
        let rowid_reg = values.last().ok_or_else(|| {
            LimboError::InternalError("FTS insert requires at least rowid".into())
        })?;
        let rowid = match rowid_reg {
            Register::Value(Value::Integer(i)) => *i,
            _ => {
                return Err(LimboError::InternalError(
                    "FTS rowid must be integer".into(),
                ))
            }
        };

        let mut doc = TantivyDocument::default();
        doc.add_i64(self.rowid_field, rowid);

        for ((_col, field), reg) in self.text_fields.iter().zip(&values[..values.len() - 1]) {
            match reg {
                Register::Value(Value::Text(t)) => {
                    doc.add_text(*field, t.as_str());
                }
                Register::Value(Value::Null) => continue,
                _ => continue,
            }
        }

        writer
            .add_document(doc)
            .map_err(|e| LimboError::InternalError(format!("FTS add_document error: {e}")))?;

        self.pending_docs_count += 1;

        // Batch commits: only commit every BATCH_COMMIT_SIZE documents
        // This dramatically improves bulk insert performance for CREATE INDEX
        if self.pending_docs_count >= BATCH_COMMIT_SIZE {
            return self.commit_and_flush();
        }

        Ok(IOResult::Done(()))
    }

    /// Deletes a document from the FTS index by rowid.
    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        let Some(ref mut writer) = self.writer else {
            return Err(LimboError::InternalError(
                "FTS writer not initialized - call open_write first".into(),
            ));
        };
        // Last register is rowid
        let rowid_reg = values.last().ok_or_else(|| {
            LimboError::InternalError("FTS delete requires at least rowid".into())
        })?;
        let rowid = match rowid_reg {
            Register::Value(Value::Integer(i)) => *i,
            _ => {
                return Err(LimboError::InternalError(
                    "FTS rowid must be integer".into(),
                ))
            }
        };

        let term = tantivy::Term::from_field_i64(self.rowid_field, rowid);
        writer.delete_term(term);

        // Track delete as a pending operation so commit_and_flush() will run
        // and invalidate the shared directory cache
        self.pending_docs_count += 1;
        if self.pending_docs_count >= BATCH_COMMIT_SIZE {
            return self.commit_and_flush();
        }

        Ok(IOResult::Done(()))
    }

    /// Starts an FTS query. Parses the query string and executes the search.
    /// Returns true if there are results, false otherwise.
    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<bool>> {
        let Some(ref searcher) = self.searcher else {
            return Err(LimboError::InternalError(
                "FTS searcher not initialized - call open_read first".into(),
            ));
        };
        if values.is_empty() {
            return Err(LimboError::InternalError(
                "FTS query_start: missing pattern id".into(),
            ));
        }

        // values[0] = pattern index
        let pattern_idx = match &values[0] {
            Register::Value(Value::Integer(i)) => *i,
            _ => FTS_PATTERN_SCORE,
        };
        self.current_pattern = pattern_idx;

        // values[1] = query string
        let query_str = match &values[1] {
            Register::Value(Value::Text(t)) => t.as_str().to_string(),
            _ => return Err(LimboError::InternalError("FTS query must be text".into())),
        };

        // Determine limit based on pattern:
        // - Patterns WITHOUT LIMIT in pattern: fetch all matches (high limit)
        // - Patterns WITH LIMIT: use the captured limit value from values[2]
        let limit = match pattern_idx {
            // Patterns without LIMIT - fetch all matches
            FTS_PATTERN_MATCH | FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_ORDERED => {
                Self::MAX_NO_LIMIT_RESULT
            }
            // Patterns with LIMIT - use captured limit value
            FTS_PATTERN_SCORE
            | FTS_PATTERN_MATCH_LIMIT
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => {
                if values.len() > 2 {
                    match &values[2] {
                        Register::Value(Value::Integer(i)) => *i as usize,
                        _ => 10,
                    }
                } else {
                    10
                }
            }
            _ => 10,
        };

        // Build query over all text fields
        let default_fields: Vec<Field> = self.text_fields.iter().map(|(_, f)| *f).collect();
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("FTS index not initialized".into()))?;

        let mut parser = tantivy::query::QueryParser::for_index(index, default_fields);

        // Apply field boosts if configured
        for (col, field) in &self.text_fields {
            if let Some(&boost) = self.field_weights.get(&col.name) {
                parser.set_field_boost(*field, boost);
            }
        }

        let query = parser
            .parse_query(&query_str)
            .map_err(|e| LimboError::InternalError(format!("FTS parse error: {e}")))?;

        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(limit))
            .map_err(|e| LimboError::InternalError(format!("FTS search error: {e}")))?;

        self.current_hits.clear();
        self.hit_pos = 0;
        for (score, doc_addr) in top_docs {
            let retrieved: TantivyDocument = searcher
                .doc(doc_addr)
                .map_err(|e| LimboError::InternalError(format!("FTS doc load error: {e}")))?;

            let rowid_vals = retrieved.get_all(self.rowid_field);
            let rowid = rowid_vals
                .filter_map(|v| TantivySchemaValue::as_i64(&v))
                .next()
                .ok_or_else(|| LimboError::InternalError("FTS doc missing rowid".into()))?;

            self.current_hits.push((score, doc_addr, rowid));
        }

        Ok(IOResult::Done(!self.current_hits.is_empty()))
    }

    /// Advances to the next query result. Returns true if more results exist.
    fn query_next(&mut self) -> Result<IOResult<bool>> {
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(false));
        }
        self.hit_pos += 1;
        Ok(IOResult::Done(self.hit_pos < self.current_hits.len()))
    }

    /// Returns the column value for the current result (score or match indicator).
    fn query_column(&mut self, idx: usize) -> Result<IOResult<Value>> {
        // Column 0 = score for fts_score, or 1 (true) for fts_match
        if idx != 0 {
            return Err(LimboError::InternalError(
                "FTS: only column 0 supported".into(),
            ));
        }

        if self.hit_pos >= self.current_hits.len() {
            return Err(LimboError::InternalError(
                "FTS: query_column out of bounds".into(),
            ));
        }

        match self.current_pattern {
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT => {
                // For fts_match patterns, return 1 (true) - indicates this row matches
                Ok(IOResult::Done(Value::Integer(1)))
            }
            FTS_PATTERN_SCORE
            | FTS_PATTERN_COMBINED
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => {
                // For fts_score and combined patterns, return the actual score
                let (score, _, _) = self.current_hits[self.hit_pos];
                Ok(IOResult::Done(Value::Float(score as f64)))
            }
            _ => {
                // Unknown pattern - return score as default
                let (score, _, _) = self.current_hits[self.hit_pos];
                Ok(IOResult::Done(Value::Float(score as f64)))
            }
        }
    }

    /// Returns the rowid for the current query result.
    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(None));
        }
        let (_, _, rowid) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Some(rowid)))
    }

    /// Flushes pending writes before transaction commit.
    /// This ensures FTS writes are persisted as part of the transaction.
    fn pre_commit(&mut self) -> Result<IOResult<()>> {
        // First, check if we're in the middle of a flush operation that needs to continue
        // This handles the case where commit_and_flush() returned IOResult::IO and we need
        // to continue the flush after IO completes
        match &self.state {
            FtsState::FlushingWrites { .. }
            | FtsState::SeekingOldChunks { .. }
            | FtsState::AdvancingAfterSeek { .. }
            | FtsState::CheckingChunkPath { .. }
            | FtsState::DeletingChunk { .. }
            | FtsState::AdvancingAfterDelete { .. }
            | FtsState::SeekingWrite { .. }
            | FtsState::InsertingWrite { .. } => {
                return self.flush_writes_internal();
            }
            _ => {}
        }

        if self.pending_docs_count > 0 {
            tracing::debug!(
                "FTS pre_commit: flushing {} pending documents",
                self.pending_docs_count
            );
            return self.commit_and_flush();
        }
        Ok(IOResult::Done(()))
    }

    /// Optimizes the FTS index by merging all segments into one.
    /// Call via `OPTIMIZE INDEX idx_name` SQL command.
    fn optimize(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>> {
        // First ensure any pending documents are flushed
        if self.pending_docs_count > 0 {
            tracing::info!(
                "FTS optimize: flushing {} pending documents first",
                self.pending_docs_count
            );
            return_if_io!(self.commit_and_flush());
        }

        // If we're not open for writing, open it
        if self.writer.is_none() {
            return_if_io!(self.open_write(connection));
        }

        let index = self
            .index
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("FTS index not initialized".to_string()))?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| LimboError::InternalError("FTS writer not initialized".to_string()))?;

        // Get all searchable segment IDs
        let segment_ids = index
            .searchable_segment_ids()
            .map_err(|e| LimboError::InternalError(format!("FTS optimize: {e}")))?;

        if segment_ids.len() <= 1 {
            tracing::debug!(
                "FTS optimize: nothing to merge ({} segments)",
                segment_ids.len()
            );
            return Ok(IOResult::Done(()));
        }

        tracing::debug!(
            "FTS optimize: merging {} segments into one",
            segment_ids.len()
        );
        // Schedule the merge operation
        let merge_future = writer.merge(&segment_ids);
        // Wait for merge to complete (blocking)
        match merge_future.wait() {
            Ok(Some(segment_meta)) => {
                tracing::debug!(
                    "FTS optimize: merge completed, new segment has {} docs",
                    segment_meta.num_docs()
                );
            }
            Ok(None) => {
                // Merge was cancelled or no merge was needed
                tracing::debug!("FTS optimize: merge was cancelled or no merge needed");
            }
            Err(e) => {
                return Err(LimboError::InternalError(format!(
                    "FTS optimize merge failed: {e}",
                )));
            }
        }

        // Commit merge and invalidate shared directory cache since we changed the structure
        writer
            .commit()
            .map_err(|e| LimboError::InternalError(format!("FTS optimize commit failed: {e}")))?;
        {
            let mut cache = self.shared_directory_cache.write();
            *cache = None;
        }

        // Reload reader to see merged segments
        if let Some(ref reader) = self.reader {
            reader.reload().map_err(|e| {
                LimboError::InternalError(format!("FTS optimize reader reload: {e}"))
            })?;
            self.searcher = Some(reader.searcher());
        }

        // Force flush directory writes to BTree (even though pending_docs_count == 0)
        self.commit_and_flush_inner(true)
    }

    /// Estimates the cost of executing a query with the given pattern.
    ///
    /// FTS queries are typically very selective (returning a small fraction of rows).
    fn estimate_cost(
        &self,
        pattern_idx: usize,
        base_table_rows: f64,
    ) -> Option<super::IndexMethodCostEstimate> {
        // FTS is typically very selective - assume ~1% of rows match
        // This is a conservative estimate; real selectivity depends on query terms
        let selectivity = 0.01;
        let estimated_rows = (base_table_rows * selectivity).max(1.0) as u64;

        // Cost model:
        // - Base cost: logarithmic in vocabulary size (approximated by table size)
        // - Result cost: linear in number of results
        let base_cost = (base_table_rows.max(1.0)).ln() * 10.0;
        let result_cost = estimated_rows as f64 * 0.1;

        // Patterns with LIMIT are significantly cheaper because Tantivy's TopDocs
        // collector can terminate early. Pattern indices:
        // 0 = SCORE (ORDER BY + LIMIT)
        // 1 = COMBINED_ORDERED_LIMIT (WHERE + ORDER BY + LIMIT)
        // 3 = COMBINED_LIMIT (WHERE + LIMIT)
        // 5 = MATCH_LIMIT (WHERE + LIMIT)
        let limit_factor = match pattern_idx {
            0 | 1 | 3 | 5 => 0.5, // Patterns with LIMIT
            _ => 1.0,
        };

        Some(super::IndexMethodCostEstimate {
            estimated_cost: (base_cost + result_cost) * limit_factor,
            estimated_rows,
        })
    }
}
