use crate::sync::{Arc, Weak};
use crate::{
    index_method::{
        open_index_cursor, parse_patterns, IndexMethod, IndexMethodAttachment,
        IndexMethodConfiguration, IndexMethodContext, IndexMethodCursor, IndexMethodDefinition,
    },
    return_if_io,
    schema::IndexColumn,
    storage::{
        btree::{BTreeKey, CursorTrait},
        pager::Pager,
    },
    translate::collate::CollationSeq,
    turso_assert,
    types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult, Text},
    util::quote_identifier,
    vdbe::Register,
    Connection, LimboError, Result, Value,
};
use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{
    cell::RefCell,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tantivy::{
    directory::{
        error::{DeleteError, OpenReadError, OpenWriteError},
        Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    },
    fastfield::{AliveBitSet, Column},
    merge_policy::{LogMergePolicy, MergePolicy, NoMergePolicy},
    query::{EnableScoring, Query, Scorer},
    schema::{Field, Schema},
    tokenizer::{
        NgramTokenizer, RawTokenizer, SimpleTokenizer, TextAnalyzer, TokenStream,
        WhitespaceTokenizer,
    },
    DocAddress, DocSet, HasLen, Index, IndexReader, IndexSettings, IndexWriter, Searcher,
    SegmentMeta, TantivyDocument, TERMINATED,
};
use turso_parser::ast::{Select, SortOrder};

/// Name identifier for the FTS index method, used in `CREATE INDEX ... USING fts`.
pub const FTS_INDEX_METHOD_NAME: &str = "fts";

/// Default memory budget (64MB) for Tantivy's IndexWriter.
/// Controls how much memory Tantivy uses for in-memory indexing before flushing to disk.
pub const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Default chunk size (512 KiB) for splitting large files when storing in BTree.
/// Files larger than this are split into multiple chunks for efficient storage and retrieval.
pub const DEFAULT_CHUNK_SIZE: usize = 512 * 1024;

/// Number of documents to batch before committing to Tantivy.
/// Higher values improve throughput but increase memory usage and latency.
pub const BATCH_COMMIT_SIZE: usize = 1000;

/// Longest accepted `fts_match` / MATCH query string, in bytes.
const FTS_MAX_QUERY_BYTES: usize = 16 * 1024;

/// Deepest accepted parenthesis nesting in a query string. Guards Tantivy's
/// recursive parser against stack overflow.
const FTS_MAX_QUERY_NESTING: usize = 64;

/// Fanout for synchronous tiered segment maintenance.
///
/// One commit merges at most one group of this size, bounding foreground
/// maintenance work while keeping the number of segments logarithmic.
const FTS_MERGE_FACTOR: usize = 8;

/// Reclaim a segment when at least this fraction of its documents are deleted.
const FTS_DELETED_DOCS_MERGE_THRESHOLD: f32 = 0.3;

/// Maximum documents read by one automatic foreground merge.
///
/// Larger maintenance remains available through `OPTIMIZE INDEX`, where the
/// caller explicitly opts into unbounded compaction latency.
const FTS_MAX_SYNC_MERGE_DOCS: u64 = 64_000;

/// Maximum source bytes read by one automatic foreground merge.
const FTS_MAX_SYNC_MERGE_BYTES: u64 = 32 * 1024 * 1024;

/// Maximum connection-local read snapshots retained per FTS attachment.
const FTS_MAX_CACHED_CONNECTIONS: usize = 4;

/// Aggregate resident file-cache budget across retained connection snapshots.
///
/// This bounds only what is *retained for reuse* after a statement finishes
/// (read snapshots in `CachedFtsStates`, the shared writer). It is not a
/// bound on live memory: a cursor always keeps its own complete file
/// snapshot resident while it runs, however large the index is, because
/// Tantivy reads through synchronous callbacks that cannot fall back to
/// storage I/O.
const FTS_MAX_RETAINED_CACHE_BYTES: usize = 192 * 1024 * 1024;

#[cfg(feature = "test_helper")]
crate::thread::thread_local! {
    static FTS_RETAINED_CACHE_BYTES_OVERRIDE: core::cell::Cell<Option<usize>> =
        const { core::cell::Cell::new(None) };
}

/// Override the retained-cache budget for tests on the current thread, so
/// budget-admission rejection is reachable without multi-hundred-MiB indexes.
/// Pass `None` to restore the default.
#[cfg(feature = "test_helper")]
pub fn set_fts_retained_cache_bytes_for_test(bytes: Option<usize>) {
    FTS_RETAINED_CACHE_BYTES_OVERRIDE.with(|cell| cell.set(bytes));
}

fn fts_max_retained_cache_bytes() -> usize {
    #[cfg(feature = "test_helper")]
    if let Some(bytes) = FTS_RETAINED_CACHE_BYTES_OVERRIDE.with(|cell| cell.get()) {
        return bytes;
    }
    FTS_MAX_RETAINED_CACHE_BYTES
}

const FTS_CONTROL_PATH: &str = "__turso_fts_control_v1";
const FTS_CONTROL_MAGIC: &[u8; 8] = b"TFTSCTL1";
const FTS_CONTROL_FORMAT_VERSION: u32 = 1;
/// An "incarnation" identifies one lifetime of an index's stored content: it
/// is minted once when the first control record is written and stays the same
/// for every later write to that index. Dropping and recreating the index
/// mints a new one. It is not a version counter — `manifest_generation`
/// advances on every publish; the incarnation only changes when the stored
/// content belongs to a different index life. Comparing it tells caches and
/// verification that "the bytes I remembered" and "the bytes on disk" come
/// from the same life of the index, even when names, root pages, or table ids
/// were reused by a recreate.
///
/// This constant is the placeholder for an index with no persisted control
/// record yet (an empty catalog). Deterministic, so every connection opening
/// a never-written index agrees on it; the real incarnation is minted when
/// the first control record is staged and is never this value.
const FTS_EMPTY_INDEX_INCARNATION: u64 = 0;
static NEXT_FTS_INDEX_INCARNATION: AtomicU64 = AtomicU64::new(1);
/// Distinguishes cursor instances within a process so a cursor can recognize
/// its own claim on the per-index writer slot across re-entrant `open_write`
/// calls.
static NEXT_FTS_CURSOR_INSTANCE: AtomicU64 = AtomicU64::new(1);

const ROWID_FIELD: &str = "rowid";

// Thread-local tokenizer cache to avoid creating a new tokenizer for each call.
// TextAnalyzer is not Send/Sync, so we use thread_local storage.
crate::thread::thread_local! {
    static FTS_TOKENIZER: RefCell<TextAnalyzer> = RefCell::new(
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build()
    );
}

/// Highlight matching terms in text by wrapping them with tags.
///
/// Standalone function that can be used without an FTS index.
/// It tokenizes both the query and text using Tantivy's default tokenizer,
/// finds matching terms, and wraps them with the specified tags.
pub fn fts_highlight(text: &str, query: &str, before_tag: &str, after_tag: &str) -> String {
    if text.is_empty() || query.is_empty() {
        return text.to_string();
    }

    FTS_TOKENIZER.with(|tokenizer| {
        let mut tokenizer = tokenizer.borrow_mut();

        // Extract query terms (lowercased)
        let query_terms: HashSet<String> = {
            let mut terms = HashSet::default();
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

        // Optimized string building: pre-calculate size and build forward
        let extra_len = match_ranges.len() * (before_tag.len() + after_tag.len());
        let mut result = String::with_capacity(text.len() + extra_len);
        let mut last_end = 0;

        for (start, end) in &match_ranges {
            // Validate UTF-8 boundaries
            if *start > text.len()
                || *end > text.len()
                || !text.is_char_boundary(*start)
                || !text.is_char_boundary(*end)
            {
                continue;
            }

            // Append text before this match
            if *start > last_end {
                result.push_str(&text[last_end..*start]);
            }

            // Append highlighted match
            result.push_str(before_tag);
            result.push_str(&text[*start..*end]);
            result.push_str(after_tag);

            last_end = *end;
        }

        // Append remaining text after last match
        if last_end < text.len() {
            result.push_str(&text[last_end..]);
        }

        result
    })
}

/// Check if text matches a query by testing for any common terms.
///
/// Standalone function that can be used without an FTS index.
/// It tokenizes both the query and text using Tantivy's default tokenizer,
/// and returns true if any query terms appear in the text.
pub fn fts_match(text: &str, query: &str) -> bool {
    if text.is_empty() || query.is_empty() {
        return false;
    }

    FTS_TOKENIZER.with(|tokenizer| {
        let mut tokenizer = tokenizer.borrow_mut();

        // Extract query terms (lowercased)
        let query_terms: HashSet<String> = {
            let mut terms = HashSet::default();
            let mut query_stream = tokenizer.token_stream(query);
            while let Some(token) = query_stream.next() {
                terms.insert(token.text.to_string());
            }
            terms
        };
        if query_terms.is_empty() {
            return false;
        }

        // Tokenize the text and check if any query terms appear
        let mut text_stream = tokenizer.token_stream(text);
        while let Some(token) = text_stream.next() {
            if query_terms.contains(&token.text) {
                return true;
            }
        }
        false
    })
}

/// In-memory catalog entry for a file stored in the FTS directory: how big
/// the file is and how many B-tree chunk rows hold its bytes. Used for
/// merge-budget accounting. [`FtsManifestFile`] is this entry's persisted
/// twin inside the control record.
#[derive(Debug, Clone)]
struct FileMetadata {
    /// Total file size in bytes
    size: usize,
    chunks: u64,
}

impl FileMetadata {
    fn new(_path: &Path, size: usize, num_chunks: usize) -> Self {
        Self {
            size,
            chunks: num_chunks as u64,
        }
    }
}

/// The persisted form of one [`FileMetadata`] entry, as serialized inside the
/// control record. `size` and `chunks` mean the same thing as there; both are
/// `u64` here because the on-disk encoding is fixed-width and must not depend
/// on the platform's `usize`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FtsManifestFile {
    size: u64,
    chunks: u64,
}

/// The one persisted record that says what the index's storage currently
/// contains: which life of the index the bytes belong to
/// (`index_incarnation`), how many times the file set has been published
/// (`manifest_generation`), and the expected size and chunk count of every
/// file. It is written in the same transaction as the file bytes, so reading
/// it back answers two questions at once: "is my cached state still current?"
/// (compare incarnation + generation) and "are the stored files intact?"
/// (compare sizes and chunk counts).
#[derive(Debug, Clone)]
struct FtsControlRecord {
    index_incarnation: u64,
    manifest_generation: u64,
    files: HashMap<PathBuf, FtsManifestFile>,
}

impl FtsControlRecord {
    fn new(index_incarnation: u64) -> Self {
        Self {
            index_incarnation,
            manifest_generation: 0,
            files: HashMap::default(),
        }
    }

    fn from_catalog(
        previous: Option<&Self>,
        index_incarnation: u64,
        catalog: &Catalog,
    ) -> Result<Self> {
        let manifest_generation = match previous {
            Some(control) => control.manifest_generation.checked_add(1).ok_or_else(|| {
                LimboError::InternalError("FTS manifest generation is exhausted".to_string())
            })?,
            None => 1,
        };
        let mut files = HashMap::default();
        for (path, metadata) in catalog {
            files.insert(
                path.clone(),
                FtsManifestFile {
                    size: metadata.size as u64,
                    chunks: metadata.chunks,
                },
            );
        }
        Ok(Self {
            index_incarnation,
            manifest_generation,
            files,
        })
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FTS_CONTROL_MAGIC);
        bytes.extend_from_slice(&FTS_CONTROL_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.index_incarnation.to_le_bytes());
        bytes.extend_from_slice(&self.manifest_generation.to_le_bytes());
        let mut files = self.files.iter().collect::<Vec<_>>();
        files.sort_by(|(left, _), (right, _)| left.cmp(right));
        let file_count = u32::try_from(files.len())
            .map_err(|_| LimboError::InternalError("FTS manifest has too many files".into()))?;
        bytes.extend_from_slice(&file_count.to_le_bytes());
        for (path, metadata) in files {
            let path = path.to_str().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "FTS manifest path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
            let path_len = u32::try_from(path.len())
                .map_err(|_| LimboError::InternalError("FTS manifest path is too long".into()))?;
            bytes.extend_from_slice(&path_len.to_le_bytes());
            bytes.extend_from_slice(path.as_bytes());
            bytes.extend_from_slice(&metadata.size.to_le_bytes());
            bytes.extend_from_slice(&metadata.chunks.to_le_bytes());
        }
        let checksum = fts_control_checksum(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        fn take<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
            let end = offset
                .checked_add(N)
                .ok_or_else(|| LimboError::Corrupt("FTS control record offset overflow".into()))?;
            let value = bytes
                .get(*offset..end)
                .ok_or_else(|| LimboError::Corrupt("truncated FTS control record".into()))?;
            *offset = end;
            Ok(value.try_into().expect("slice length checked"))
        }

        if bytes.len() < FTS_CONTROL_MAGIC.len() + 4 + 8 + 8 + 4 + 8 {
            return Err(LimboError::Corrupt("truncated FTS control record".into()));
        }
        let payload_len = bytes.len() - 8;
        let expected_checksum = u64::from_le_bytes(
            bytes[payload_len..]
                .try_into()
                .expect("checksum length checked"),
        );
        if fts_control_checksum(&bytes[..payload_len]) != expected_checksum {
            return Err(LimboError::Corrupt(
                "FTS control record checksum mismatch".into(),
            ));
        }

        let mut offset = 0;
        if take::<8>(bytes, &mut offset)? != *FTS_CONTROL_MAGIC {
            return Err(LimboError::Corrupt(
                "unrecognized FTS control record".into(),
            ));
        }
        let version = u32::from_le_bytes(take(bytes, &mut offset)?);
        if version != FTS_CONTROL_FORMAT_VERSION {
            return Err(LimboError::Corrupt(format!(
                "unsupported FTS control format version {version}"
            )));
        }
        let index_incarnation = u64::from_le_bytes(take(bytes, &mut offset)?);
        let manifest_generation = u64::from_le_bytes(take(bytes, &mut offset)?);
        let file_count = u32::from_le_bytes(take(bytes, &mut offset)?) as usize;
        let mut files = HashMap::default();
        for _ in 0..file_count {
            let path_len = u32::from_le_bytes(take(bytes, &mut offset)?) as usize;
            let path_end = offset
                .checked_add(path_len)
                .ok_or_else(|| LimboError::Corrupt("FTS manifest path offset overflow".into()))?;
            let path_bytes = bytes
                .get(offset..path_end)
                .ok_or_else(|| LimboError::Corrupt("truncated FTS manifest path".into()))?;
            offset = path_end;
            let path = std::str::from_utf8(path_bytes)
                .map_err(|_| LimboError::Corrupt("FTS manifest path is not UTF-8".into()))?;
            let size = u64::from_le_bytes(take(bytes, &mut offset)?);
            let chunks = u64::from_le_bytes(take(bytes, &mut offset)?);
            if chunks == 0 {
                return Err(LimboError::Corrupt(format!(
                    "FTS manifest file {path} has zero chunks"
                )));
            }
            if files
                .insert(PathBuf::from(path), FtsManifestFile { size, chunks })
                .is_some()
            {
                return Err(LimboError::Corrupt(format!(
                    "duplicate FTS manifest entry for {path}"
                )));
            }
        }
        if offset != payload_len {
            return Err(LimboError::Corrupt(
                "FTS control record has trailing payload bytes".into(),
            ));
        }
        Ok(Self {
            index_incarnation,
            manifest_generation,
            files,
        })
    }

    fn validate_catalog(&self, catalog: &Catalog) -> Result<()> {
        if self.files.len() != catalog.len() {
            return Err(LimboError::Corrupt(format!(
                "FTS manifest contains {} files but storage contains {}",
                self.files.len(),
                catalog.len()
            )));
        }
        for (path, metadata) in catalog {
            let manifest = self.files.get(path).ok_or_else(|| {
                LimboError::Corrupt(format!(
                    "FTS storage file {} is absent from the manifest",
                    path.display()
                ))
            })?;
            if manifest.size != metadata.size as u64 || manifest.chunks != metadata.chunks {
                return Err(LimboError::Corrupt(format!(
                    "FTS manifest metadata mismatch for {}",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

fn fts_control_checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    })
}

/// Eviction samples per put
/// Size-tracked map of a Tantivy directory's complete file contents.
///
/// Deliberately unbounded: every cataloged file must stay resident, because
/// Tantivy reads through synchronous callbacks that cannot fall back to
/// storage I/O. Nothing is ever evicted — the memory bound on FTS state is
/// the retention budget in `CachedFtsStates` / `cache_writer`, which decides
/// whether a *finished* snapshot is kept for reuse at all.
pub struct FileCache<K> {
    inner: RwLock<FileCacheInner<K>>,
}

#[derive(Debug)]
struct FileCacheInner<K> {
    current_size: usize,
    entries: HashMap<K, Arc<[u8]>>,
}

impl<K: std::fmt::Debug> std::fmt::Debug for FileCache<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("FileCache")
            .field("current_size", &inner.current_size)
            .field("entries", &inner.entries.len())
            .finish()
    }
}

impl<K: Eq + std::hash::Hash + Clone> FileCache<K> {
    /// Lookup entry. Returns Arc-cloned data.
    fn get<Q>(&self, key: &Q) -> Option<Arc<[u8]>>
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        self.inner.read().entries.get(key).map(Arc::clone)
    }

    /// Insert or replace an entry.
    fn put(&self, key: K, value: Vec<u8>) {
        let arc_value: Arc<[u8]> = Arc::from(value);
        let size = arc_value.len();
        let mut inner = self.inner.write();
        let old_size = inner
            .entries
            .insert(key, arc_value)
            .map_or(0, |old| old.len());
        inner.current_size = inner.current_size - old_size + size;
    }

    /// Remove an entry from the cache.
    fn remove<Q>(&self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        let mut inner = self.inner.write();
        if let Some(data) = inner.entries.remove(key) {
            inner.current_size -= data.len();
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

/// Specialized methods for PathBuf caches (hot files).
impl FileCache<PathBuf> {
    fn with_preloaded_arcs(files: HashMap<PathBuf, Arc<[u8]>>) -> Self {
        let current_size: usize = files.values().map(|data| data.len()).sum();
        Self {
            inner: RwLock::new(FileCacheInner {
                current_size,
                entries: files,
            }),
        }
    }

    fn arc_snapshot(&self) -> HashMap<PathBuf, Arc<[u8]>> {
        self.inner
            .read()
            .entries
            .iter()
            .map(|(path, data)| (path.clone(), Arc::clone(data)))
            .collect()
    }
}

/// Type aliases to please the almighty clippy
type Catalog = HashMap<PathBuf, FileMetadata>;

#[derive(Debug)]
enum PendingFileMutation {
    Write(Vec<u8>),
    Delete,
}

type PendingFlushes = Vec<(PathBuf, Option<Vec<u8>>)>;

#[derive(Debug, Default)]
struct PendingFileMutations {
    by_path: HashMap<PathBuf, PendingFileMutation>,
}

impl PendingFileMutations {
    fn queue_write(&mut self, path: PathBuf, data: Vec<u8>) {
        self.by_path.insert(path, PendingFileMutation::Write(data));
    }

    fn queue_delete(&mut self, path: PathBuf) {
        self.by_path.insert(path, PendingFileMutation::Delete);
    }

    fn take_flushes(&mut self) -> PendingFlushes {
        std::mem::take(&mut self.by_path)
            .into_iter()
            .map(|(path, mutation)| match mutation {
                PendingFileMutation::Write(data) => (path, Some(data)),
                PendingFileMutation::Delete => (path, None),
            })
            .collect()
    }
}

/// Tantivy Directory implementation backed by a complete in-memory snapshot.
///
/// Tantivy stores its index as a collection of files (segments, metadata, term dictionaries, etc.).
/// The `Directory` trait is synchronous, while Turso storage is asynchronous. The FTS cursor
/// therefore loads the complete directory through its resumable state machine before constructing
/// Tantivy objects. Directory callbacks only read this snapshot or cursor-local pending writes;
/// they must never open a B-tree cursor or drive the pager.
///
/// FTS index files are stored in a BTree with the schema `(path TEXT, chunk_no INTEGER, bytes BLOB)`.
/// Large files are split into chunks of `DEFAULT_CHUNK_SIZE` (512 KiB) to enable efficient
/// partial reads and bounded memory usage during loading.
///
/// File mutations are buffered in memory and flushed to the BTree when:
/// - A Tantivy commit occurs (via `commit_and_flush`)
/// - The statement is finalized (via `stage_statement_commit`)
///
/// During flush, writes are moved to `flushing_writes` so they remain readable while
/// the async BTree write completes.
#[derive(Clone)]
struct HybridBTreeDirectory {
    /// File catalog: path -> metadata (always in memory, no content)
    catalog: Arc<RwLock<Catalog>>,

    /// Complete file snapshot loaded before Tantivy is invoked. Unbounded by
    /// design (see [`FileCache`]); the retention budget bounds what outlives
    /// the cursor, not what it holds while running.
    hot_cache: Arc<FileCache<PathBuf>>,

    /// Storage view at cursor checkout, kept separate from Tantivy's mutable
    /// in-memory view so unchanged rewrites can be elided.
    base_files: Arc<RwLock<HashMap<PathBuf, Arc<[u8]>>>>,

    /// Latest pending mutation for each path.
    pending_mutations: Arc<RwLock<PendingFileMutations>>,

    /// Writes currently being flushed to BTree (still readable during flush)
    /// This preserves data for reads during async flush operations
    flushing_writes: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,

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
            .field("btree_root_page", &self.btree_root_page)
            .finish()
    }
}

impl HybridBTreeDirectory {
    /// Create a clone with fresh (empty) pending state.
    /// This is used when creating a new cursor from a cached directory to ensure
    /// each cursor has its own isolated pending file mutations.
    /// This prevents the bug where writes from one cursor affect the Drop behavior
    /// of another cursor.
    fn clone_with_fresh_pending(&self) -> Self {
        let files = self.hot_cache.arc_snapshot();
        Self {
            catalog: Arc::new(RwLock::new(self.catalog.read().clone())),
            hot_cache: Arc::new(FileCache::<PathBuf>::with_preloaded_arcs(files.clone())),
            base_files: Arc::new(RwLock::new(files)),
            // Fresh pending state - not shared with cache
            pending_mutations: Arc::new(RwLock::new(PendingFileMutations::default())),
            flushing_writes: Arc::new(RwLock::new(HashMap::default())),
            pager: Arc::clone(&self.pager),
            btree_root_page: self.btree_root_page,
        }
    }
}

impl HybridBTreeDirectory {
    /// Create from a complete, asynchronously preloaded snapshot.
    fn with_preloaded(
        pager: Arc<Pager>,
        btree_root_page: i64,
        catalog: HashMap<PathBuf, FileMetadata>,
        files: HashMap<PathBuf, Vec<u8>>,
    ) -> Self {
        let base_files = files
            .into_iter()
            .map(|(path, data)| (path, Arc::<[u8]>::from(data)))
            .collect::<HashMap<_, _>>();
        Self {
            catalog: Arc::new(RwLock::new(catalog)),
            hot_cache: Arc::new(FileCache::<PathBuf>::with_preloaded_arcs(
                base_files.clone(),
            )),
            base_files: Arc::new(RwLock::new(base_files)),
            pending_mutations: Arc::new(RwLock::new(PendingFileMutations::default())),
            flushing_writes: Arc::new(RwLock::new(HashMap::default())),
            pager,
            btree_root_page,
        }
    }

    fn queue_write(&self, path: PathBuf, data: Vec<u8>) {
        self.pending_mutations.write().queue_write(path, data);
    }

    fn queue_delete(&self, path: PathBuf) {
        self.pending_mutations.write().queue_delete(path);
    }

    /// Take the latest mutation for every path.
    ///
    /// Writes are copied to `flushing_writes` so Tantivy can still read them
    /// while the resumable BTree flush is in progress. `None` is an explicit
    /// delete; `Some(Vec::new())` is an empty file.
    fn take_pending_flushes(&self) -> PendingFlushes {
        let base_files = self.base_files.read();
        let flushes = self
            .pending_mutations
            .write()
            .take_flushes()
            .into_iter()
            .filter(|(path, data)| match data {
                Some(data) => base_files
                    .get(path)
                    .is_none_or(|base| base.as_ref() != data.as_slice()),
                None => base_files.contains_key(path),
            })
            .collect::<Vec<_>>();
        drop(base_files);

        {
            let mut flushing = self.flushing_writes.write();
            for (path, data) in &flushes {
                if let Some(data) = data {
                    flushing.insert(path.clone(), data.clone());
                }
            }
        }

        tracing::debug!("FTS take_pending_flushes: {} entries", flushes.len());
        flushes
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
        *self.base_files.write() = self.hot_cache.arc_snapshot();
    }

    /// Find file data in pending or flushing writes.
    fn find_in_pending_writes(&self, path: &Path) -> Option<Vec<u8>> {
        {
            let pending = self.pending_mutations.read();
            if let Some(mutation) = pending.by_path.get(path) {
                return match mutation {
                    PendingFileMutation::Write(data) => Some(data.clone()),
                    PendingFileMutation::Delete => None,
                };
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
/// Use `Arc<[u8]>` for zero-copy reads when backed by the hot cache.
struct InMemoryFileHandle {
    data: Arc<[u8]>,
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
            return Ok(OwnedBytes::empty());
        }
        Ok(OwnedBytes::new(Arc::clone(&self.data)).slice(range))
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
        // Only `terminate_ref` publishes: Tantivy's Directory contract says
        // callers must not rely on Drop flushing, and ManagedDirectory's
        // FooterProxy appends its CRC footer only in terminate. A file
        // published from Drop would have no footer and read back as
        // "Footer magic byte mismatch" — a recoverable serialization error
        // turned into persisted state that reports itself as corruption.
        if !self.buffer.is_empty() {
            tracing::error!(
                path = %self.path.display(),
                bytes = self.buffer.len(),
                "FTS writer dropped without terminate; discarding buffered file"
            );
        }
    }
}

impl TerminatingWrite for HybridWriter {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        let data = std::mem::take(&mut self.buffer);

        let num_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE).max(1);

        // Update catalog - even empty files should exist in the catalog
        let metadata = FileMetadata::new(&self.path, data.len(), num_chunks);
        self.directory.update_catalog(self.path.clone(), metadata);

        self.directory
            .add_to_hot_cache(self.path.clone(), data.clone());

        self.directory.queue_write(self.path.clone(), data);
        Ok(())
    }
}

impl Directory for HybridBTreeDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(data) = self.hot_cache.get(path) {
            return Ok(Arc::new(InMemoryFileHandle { data }));
        }

        // Check pending writes (files written but not yet flushed to BTree)
        // This is critical for cold files that are immediately read back by Tantivy
        if let Some(data) = self.find_in_pending_writes(path) {
            return Ok(Arc::new(InMemoryFileHandle {
                data: Arc::from(data),
            }));
        }

        if !self.catalog.read().contains_key(path) {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }

        Err(OpenReadError::IoError {
            io_error: Arc::new(std::io::Error::other(format!(
                "FTS snapshot invariant violated: {} is cataloged but was not preloaded",
                path.display()
            ))),
            filepath: path.to_path_buf(),
        })
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
        self.queue_delete(path.to_path_buf());
        Ok(())
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite + Send + Sync>>, OpenWriteError>
    {
        // Tantivy's Directory trait documentation states files "may not previously exist",
        // and the standard MmapDirectory implementation uses OpenOptions::create_new(true)
        // which fails with FileAlreadyExists if the file is present.
        // However, Tantivy may call open_write on existing files during operations like
        // segment merging or metadata updates. To handle this gracefully, we delete any
        // existing file first. The error is ignored because:
        // 1. If the file doesn't exist, delete() succeeds (no-op on missing files)
        // 2. Our delete() implementation always returns Ok(()) - it only removes entries
        //    from in-memory structures and queues the
        //    BTree deletion, none of which can fail.
        //
        // Skip delete for the meta lock file: Tantivy calls open_write on it for every
        // search query and it does not need BTree deletion.
        //
        // Because this never returns FileAlreadyExists, Tantivy's file-based
        // single-writer lock is intentionally disabled for this directory:
        // it cannot work anyway, since every cursor gets its own directory
        // clone. Writer exclusion is enforced instead by the per-connection
        // writer slot (`FtsWriterSlot`), the pager write lock in WAL mode,
        // and the per-index MVCC write lease.
        if path != Path::new(TANTIVY_META_LOCK_FILE) {
            let _ = self.delete(path);
        }
        let writer: Box<dyn TerminatingWrite + Send + Sync> = Box::new(HybridWriter {
            path: path.to_path_buf(),
            buffer: Vec::new(),
            directory: self.clone(),
        });
        Ok(BufWriter::new(writer))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        // Check hot cache first (includes recently written files)
        if let Some(data) = self.hot_cache.get(path) {
            return Ok(data.to_vec());
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

        Err(OpenReadError::IoError {
            io_error: Arc::new(std::io::Error::other(format!(
                "FTS snapshot invariant violated: {} is cataloged but was not preloaded",
                path.display()
            ))),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        // Update catalog
        let num_chunks = data.len().div_ceil(DEFAULT_CHUNK_SIZE).max(1);
        let metadata = FileMetadata::new(path, data.len(), num_chunks);
        self.update_catalog(path.to_path_buf(), metadata);

        self.add_to_hot_cache(path.to_path_buf(), data.to_vec());

        self.queue_write(path.to_path_buf(), data.to_vec());
        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    // No change notifications: every reader is built with
    // `ReloadPolicy::Manual` and reloaded explicitly after commits, so a
    // registered callback would never need to fire.
    fn watch(&self, _cb: WatchCallback) -> std::result::Result<WatchHandle, tantivy::TantivyError> {
        Ok(WatchHandle::empty())
    }
}

/// Creates default `KeyInfo` for BTree index columns.
fn key_info() -> KeyInfo {
    KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::Binary,
        nulls_order: None,
    }
}

/// Parse field weights from a string like "body=2.0,title=1.0"
/// Returns a HashMap mapping column names to tantivy 'boost factors'
fn parse_field_weights(weights_str: &str, columns: &[IndexColumn]) -> Result<HashMap<String, f32>> {
    let mut weights = HashMap::default();

    if weights_str.is_empty() {
        return Ok(weights);
    }

    // Get valid column names for validation
    let valid_columns: HashSet<&str> = columns.iter().map(|c| c.name.as_str()).collect();

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

/// Which snapshot a cached FTS entry belongs to, and so who may reuse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FtsCachedSnapshot {
    /// Private to one live MVCC transaction; only that transaction may reuse
    /// the entry, and only until the transaction resolves.
    Mvcc { transaction_id: u64 },
    /// Not private to any live transaction. `pos` is the WAL position the
    /// entry was built at; `None` means there is nothing to compare cheaply
    /// (the pager had no WAL identity, or the entry is a writer whose MVCC
    /// transaction committed), so reuse first requires the slower
    /// control-record or metadata-byte comparison.
    Wal { pos: Option<(u32, u64)> },
}

impl FtsCachedSnapshot {
    /// The live MVCC transaction that owns the entry, if any.
    fn transaction_id(self) -> Option<u64> {
        match self {
            Self::Mvcc { transaction_id } => Some(transaction_id),
            Self::Wal { .. } => None,
        }
    }

    /// The WAL position to validate reuse against. `None` both for entries
    /// that need the slower comparison and for transaction-private entries.
    fn wal_pos(self) -> Option<(u32, u64)> {
        match self {
            Self::Wal { pos } => pos,
            Self::Mvcc { .. } => None,
        }
    }
}

/// Cached FTS read state reused by cursors on the connection that populated it.
///
/// The connection owner is part of the cache identity because the directory and
/// reader reflect that connection's transaction snapshot and can contain
/// uncommitted index maintenance. Write cursors only reuse the directory's
/// immutable catalog and caches; they create a separate `Index` backed by fresh
/// pending-write state.
pub struct CachedFtsState {
    connection: Weak<Connection>,
    /// Snapshot at which the cached Tantivy metadata was loaded.
    snapshot: FtsCachedSnapshot,
    control: FtsControlRecord,
    directory: HybridBTreeDirectory,
    index: Index,
    reader: IndexReader,
    query_parser: Arc<tantivy::query::QueryParser>,
}

/// A committed writer retained between statements on one connection.
///
/// The backing B-tree remains the source of truth. Before reuse, `meta.json`
/// is compared against the B-tree so transaction or savepoint rollback, or a
/// write from another connection, discards this state instead of exposing
/// stale Tantivy segments.
struct CachedFtsWriter {
    connection: Weak<Connection>,
    /// While the owning MVCC transaction is live this is `Mvcc`, and the
    /// writer's visibility stays private to that transaction. On commit it is
    /// re-tagged `Wal { pos: None }` so the next reuse validates against the
    /// on-disk control record first.
    snapshot: FtsCachedSnapshot,
    control: FtsControlRecord,
    directory: HybridBTreeDirectory,
    index: Index,
    writer: IndexWriter,
}

impl std::fmt::Debug for CachedFtsWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFtsWriter")
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct CachedFtsStates {
    /// Least recently used entry first.
    entries: Vec<CachedFtsState>,
}

impl CachedFtsState {
    fn resident_cache_bytes(&self) -> usize {
        self.directory.hot_cache.size()
    }

    /// Is this entry positioned at the very snapshot the current operation
    /// reads from? MVCC entries compare transaction ids; WAL entries compare
    /// the WAL position the entry was built at against the pager's current
    /// one.
    fn matches_snapshot(&self, context: &IndexMethodContext) -> bool {
        match (context.transaction_id(), self.snapshot) {
            (Some(current), FtsCachedSnapshot::Mvcc { transaction_id }) => {
                current == transaction_id
            }
            (None, FtsCachedSnapshot::Wal { pos }) => pos
                .is_some_and(|snapshot_wal_pos| self.directory.pager.wal_pos() == snapshot_wal_pos),
            _ => false,
        }
    }

    /// Was this entry built from the control record that is currently visible
    /// on disk? True only when both the index life (incarnation) and the
    /// publish counter (manifest generation) match.
    fn matches_manifest(&self, visible_control: Option<&FtsControlRecord>) -> bool {
        visible_control.is_some_and(|control| {
            control.index_incarnation == self.control.index_incarnation
                && control.manifest_generation == self.control.manifest_generation
        })
    }
}

impl CachedFtsStates {
    fn resident_cache_bytes(&self) -> usize {
        self.entries.iter().fold(0usize, |total, cached| {
            total.saturating_add(cached.resident_cache_bytes())
        })
    }

    fn prune(&mut self) {
        self.entries
            .retain(|cached| cached.connection.strong_count() > 0);
        while self.entries.len() > FTS_MAX_CACHED_CONNECTIONS
            || (self.entries.len() > 1
                && self.resident_cache_bytes() > fts_max_retained_cache_bytes())
        {
            self.entries.remove(0);
        }
    }

    fn connection_position(&self, conn: &Arc<Connection>) -> Option<usize> {
        self.entries.iter().position(|cached| {
            cached
                .connection
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, conn))
        })
    }

    fn remove_connection(&mut self, conn: &Arc<Connection>) {
        if let Some(position) = self.connection_position(conn) {
            self.entries.remove(position);
        }
    }

    fn insert(&mut self, state: CachedFtsState, byte_budget: usize) -> bool {
        self.entries
            .retain(|cached| !Weak::ptr_eq(&cached.connection, &state.connection));
        // Always keep the newest snapshot and evict older ones to make room.
        // Rejecting an oversized snapshot outright would not save memory —
        // the live cursor holds the whole snapshot anyway — it would only
        // force the next statement to reload it all from storage, turning
        // the budget cliff into a full cold load per statement.
        self.entries.push(state);
        self.prune();
        while self.entries.len() > 1 && self.resident_cache_bytes() > byte_budget {
            self.entries.remove(0);
        }
        true
    }

    /// Evict oldest retained snapshots until at or under `byte_budget`.
    fn evict_to_fit(&mut self, byte_budget: usize) {
        while !self.entries.is_empty() && self.resident_cache_bytes() > byte_budget {
            self.entries.remove(0);
        }
    }
}

impl std::fmt::Debug for CachedFtsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFtsState")
            .field("directory", &"HybridBTreeDirectory")
            .finish()
    }
}

/// FTS index attachment that holds configuration and creates cursors for queries.
///
/// Created by `FtsIndexMethod::attach()` and implements `IndexMethodAttachment`.
/// Stores the Tantivy schema, field mappings, query patterns, and a shared
/// read-state cache to optimize repeated queries.
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
    /// Weights for each field in FTS scoring.
    /// Created from WITH clause parameters,
    /// e.g. `WITH (tokenizer='default',weights='col1=1.0,col2=2.0')`.
    field_weights: HashMap<String, f32>,
    /// (min_gram, max_gram) for the ngram tokenizer, from the WITH clause
    /// `min_gram`/`max_gram` keys. [DEFAULT_NGRAM_WINDOW] unless configured.
    ngram_window: (usize, usize),
    /// In-memory cached Tantivy read state.
    cached_state: Arc<RwLock<CachedFtsStates>>,
    /// Tantivy writer retained across sequential statements.
    ///
    /// `Mutex` provides exclusive access without requiring `IndexWriter` to be
    /// `Sync`; SQLite transaction rules already serialize writers.
    cached_writer: Arc<Mutex<Option<CachedFtsWriter>>>,
    /// The one cursor currently allowed to flush this index per connection.
    /// See [`FtsWriterSlot`].
    writer_slot: Arc<Mutex<Option<FtsWriterSlot>>>,
    runtime_stats: Arc<FtsRuntimeStats>,
}

/// The single cursor allowed to flush this FTS index for a given connection.
///
/// One statement can open two write cursors over the same index — a trigger
/// whose body writes the FTS-indexed table it fired on. Each cursor builds its
/// own Tantivy directory from the same starting snapshot, so letting both
/// flush would store the union of two divergent file sets under one manifest
/// and make the index unreadable on disk, permanently. The second writer is
/// refused with a `Raise(Abort)` error instead, and the statement rolls back
/// cleanly.
///
/// The slot is claimed on the cursor's first document mutation (a plan may
/// open several write-mode cursors on one index but only one ever mutates it)
/// and released once the cursor has staged its statement's writes
/// (`stage_statement_commit`), aborted, or closed — after that the cursor
/// never flushes again, so a later statement's cursor may write. Writers on
/// different connections are already serialized by the pager write lock (WAL)
/// or the per-index MVCC write lease.
#[derive(Debug)]
struct FtsWriterSlot {
    connection: Weak<Connection>,
    cursor_instance: u64,
}

/// Counters for cache and writer behavior, read back by invariant tests.
///
/// The fields are atomics because one `FtsRuntimeStats` is shared through an
/// `Arc` by the attachment and every cursor opened on it, across all
/// connections and their threads, and the counters are bumped at spots where
/// no common lock is held.
#[derive(Debug, Default)]
struct FtsRuntimeStats {
    tantivy_writer_constructions: AtomicUsize,
    writer_cache_lookups: AtomicUsize,
    writer_cache_hits: AtomicUsize,
    writer_cache_validation_failures: AtomicUsize,
    writer_cache_rollback_discards: AtomicUsize,
    writer_cache_misses: AtomicUsize,
    read_cache_lookups: AtomicUsize,
    read_cache_hits: AtomicUsize,
    read_cache_misses: AtomicUsize,
    cache_admission_rejections: AtomicUsize,
    full_snapshot_loads: AtomicUsize,
    manifest_validation_hits: AtomicUsize,
    manifest_validation_misses: AtomicUsize,
    write_lease_acquisitions: AtomicUsize,
    write_lease_rejections: AtomicUsize,
}

/// Supported tokenizer names for FTS indexes
pub const SUPPORTED_TOKENIZERS: &[&str] = &[
    "default",    // Tantivy default: lowercase + punctuation split + 40 char limit
    "raw",        // No tokenization - exact match only
    "simple",     // Basic whitespace/punctuation split
    "whitespace", // Split on whitespace only
    "ngram",      // N-gram tokenizer (2-3 chars by default)
];

/// Supported keys in the WITH clause of an FTS index
pub const SUPPORTED_WITH_KEYS: &[&str] = &["tokenizer", "weights", "min_gram", "max_gram"];

/// Ngram window used when `min_gram`/`max_gram` are not given.
pub const DEFAULT_NGRAM_WINDOW: (usize, usize) = (2, 3);

impl FtsIndexAttachment {
    #[aristo::intent(
        "Every WITH clause key this constructor accepts is also consumed by it: \
         a key outside the supported list is an error, and lookups go through \
         one case-folded map so a mis-cased key cannot slip past. Adding a key \
         to the supported list without reading it from that map recreates the \
         accepted-but-ignored failure this guards against — the DDL succeeds \
         but the index silently gets different semantics than it asked for.",
        verify = "neural",
        id = "fts_with_keys_all_validated_and_consumed"
    )]
    pub fn new(cfg: IndexMethodConfiguration) -> Result<Self> {
        // Validate WITH clause keys the same way bad values are validated:
        // a typo like `tokenzier` must be an error, not a silently different
        // index. Keys are matched case-insensitively.
        let mut parameters: HashMap<String, &Value> = HashMap::default();
        for (key, value) in &cfg.parameters {
            let normalized = key.to_ascii_lowercase();
            if !SUPPORTED_WITH_KEYS.contains(&normalized.as_str()) {
                return Err(LimboError::ParseError(format!(
                    "unsupported FTS WITH parameter '{}'. Supported parameters: {}",
                    key,
                    SUPPORTED_WITH_KEYS.join(", ")
                )));
            }
            if parameters.insert(normalized, value).is_some() {
                return Err(LimboError::ParseError(format!(
                    "duplicate FTS WITH parameter '{key}'"
                )));
            }
        }

        // Parse tokenizer from WITH clause parameters, default to "default"
        // The parser may include surrounding quotes in the value, so we strip them
        let tokenizer_name = parameters
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

        // Parse the ngram window: WITH (tokenizer = 'ngram', min_gram = 1, max_gram = 3)
        let parse_gram = |key: &str| -> Result<Option<usize>> {
            let Some(value) = parameters.get(key) else {
                return Ok(None);
            };
            match value {
                Value::Numeric(crate::numeric::Numeric::Integer(v)) if *v >= 1 => {
                    Ok(Some(usize::try_from(*v).expect("checked to be positive")))
                }
                _ => Err(LimboError::ParseError(format!(
                    "FTS WITH parameter '{key}' must be a positive integer"
                ))),
            }
        };
        let min_gram = parse_gram("min_gram")?;
        let max_gram = parse_gram("max_gram")?;
        if (min_gram.is_some() || max_gram.is_some()) && tokenizer_name != "ngram" {
            return Err(LimboError::ParseError(format!(
                "FTS WITH parameters 'min_gram' and 'max_gram' require tokenizer = 'ngram', got tokenizer = '{tokenizer_name}'"
            )));
        }
        let ngram_window = (
            min_gram.unwrap_or(DEFAULT_NGRAM_WINDOW.0),
            max_gram.unwrap_or(DEFAULT_NGRAM_WINDOW.1),
        );
        if ngram_window.0 > ngram_window.1 {
            return Err(LimboError::ParseError(format!(
                "FTS ngram window is invalid: min_gram ({}) is greater than max_gram ({})",
                ngram_window.0, ngram_window.1
            )));
        }

        // Parse field weights from WITH clause: weights='body=2.0,title=1.0'
        let field_weights = if let Some(weights_value) = parameters.get("weights") {
            let weights_str = match weights_value {
                Value::Text(t) => {
                    let s = t.to_string();
                    s.trim_matches(|c| c == '\'' || c == '"').to_string()
                }
                _ => String::new(),
            };
            parse_field_weights(&weights_str, &cfg.columns)?
        } else {
            HashMap::default()
        };

        // Build Tantivy schema (no Directory or Index creation yet)
        let mut schema_builder = Schema::builder();

        // Use FAST field for rowid to enable efficient columnar access during query result retrieval.
        // This avoids loading full documents from the .store file just to get the rowid.
        let rowid_field = schema_builder.add_i64_field(
            ROWID_FIELD,
            tantivy::schema::INDEXED | tantivy::schema::FAST,
        );

        let mut text_fields = Vec::with_capacity(cfg.columns.len());
        for col in &cfg.columns {
            let opts = tantivy::schema::TextOptions::default().set_indexing_options(
                tantivy::schema::TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name)
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            );
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
            field_weights,
            ngram_window,
            cached_state: Arc::new(RwLock::new(CachedFtsStates::default())),
            cached_writer: Arc::new(Mutex::new(None)),
            writer_slot: Arc::new(Mutex::new(None)),
            runtime_stats: Arc::new(FtsRuntimeStats::default()),
        })
    }
}

impl IndexMethodAttachment for FtsIndexAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: FTS_INDEX_METHOD_NAME,
            table_name: &self.cfg.table_name,
            index_name: &self.cfg.index_name,
            patterns: &self.patterns,
            backing_btree: false,
            // Unordered match queries stream directly from Tantivy scorers.
            // UPDATE/DELETE must therefore collect stable rowids before writes.
            results_materialized: false,
            mvcc_support: super::IndexMethodMvccSupport::TransactionalBackingStore,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(FtsCursor::new(self)))
    }
}

fn initialize_btree_storage_table(
    conn: &Arc<Connection>,
    database_id: usize,
    table_name: &str,
) -> Result<()> {
    // Fast path: both objects already exist (every open after the index was
    // created). Skips preparing and running two nested DDL statements per
    // write cursor open.
    let index_name = format!("{table_name}_key");
    let already_exists = conn.with_schema(database_id, |schema| {
        schema.get_btree_table(table_name).is_some()
            && schema.get_index(table_name, &index_name).is_some()
    });
    if already_exists {
        return Ok(());
    }
    let db_prefix = conn
        .get_database_name_by_index(database_id)
        .filter(|name| name != "main")
        .map(|name| format!("{}.", quote_identifier(&name)))
        .unwrap_or_default();
    let table_ident = quote_identifier(table_name);
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {db_prefix}{table_ident} \
         (path TEXT NOT NULL, chunk_no INTEGER NOT NULL, bytes BLOB NOT NULL)"
    );
    // Use backing_btree to create a BTree that stores all columns without rowid
    // indirection, allowing direct cursor access with the exact key structure.
    let create_index_sql = format!(
        "CREATE INDEX IF NOT EXISTS {db_prefix}{index_ident} ON {table_ident} \
         USING {method} (path, chunk_no, bytes)",
        index_ident = quote_identifier(&format!("{table_name}_key")),
        method = super::BACKING_BTREE_INDEX_METHOD_NAME,
    );
    // Execute nested statements without subtransactions to avoid DatabaseBusy
    // (we're already inside a transaction from the parent CREATE INDEX statement)
    {
        conn.start_nested();
        let mut stmt = conn.prepare(create_table_sql)?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }
    {
        conn.start_nested();
        let mut stmt = conn.prepare(create_index_sql)?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
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
const TANTIVY_META_LOCK_FILE: &str = ".tantivy-meta.lock";

fn bounded_query_limit(limit: Option<i64>, live_docs: u64) -> usize {
    let live_docs = usize::try_from(live_docs).unwrap_or(usize::MAX);
    match limit {
        Some(0) => 0,
        Some(limit) if limit > 0 => usize::try_from(limit).unwrap_or(usize::MAX).min(live_docs),
        // A negative LIMIT means unlimited in SQLite.
        Some(_) | None => live_docs,
    }
}

/// Accumulated file metadata: path -> (chunk_no -> (blob_size, Option<blob_data>))
type CatalogBuilder = HashMap<i64, (usize, Option<Vec<u8>>)>;
type CachedFtsCheckout = (
    HybridBTreeDirectory,
    Index,
    IndexReader,
    Arc<tantivy::query::QueryParser>,
    FtsControlRecord,
);

fn assemble_catalog_file(path: &Path, chunks: &CatalogBuilder) -> Result<Vec<u8>> {
    let max_chunk =
        chunks.keys().max().copied().ok_or_else(|| {
            LimboError::Corrupt(format!("FTS file {} has no chunks", path.display()))
        })?;
    if max_chunk < 0 {
        return Err(LimboError::Corrupt(format!(
            "FTS file {} has a negative chunk number",
            path.display()
        )));
    }
    let total_size = chunks.values().map(|(size, _)| size).sum();
    let mut assembled = Vec::with_capacity(total_size);
    for chunk_no in 0..=max_chunk {
        let Some((_, Some(data))) = chunks.get(&chunk_no) else {
            return Err(LimboError::Corrupt(format!(
                "FTS directory file {} is missing chunk {}",
                path.display(),
                chunk_no
            )));
        };
        assembled.extend_from_slice(data);
    }
    Ok(assembled)
}

/// State machine for FTS cursor async operations
#[derive(Debug)]
enum FtsState {
    /// Initial state
    Init,
    /// Rewinding cursor to start
    Rewinding,
    /// Seeking the small control record for cross-transaction cache validation.
    SeekingControl,
    /// Advancing after the control-record seek returned `TryAdvance`.
    AdvancingToControl,
    /// Reading the control record's chunks without scanning directory files.
    LoadingControl {
        chunks: CatalogBuilder,
        advance_pending: bool,
    },
    /// Loading file catalog from BTree (metadata only, not content)
    /// This is the new catalog-first approach for HybridBTreeDirectory
    LoadingCatalog {
        catalog_builder: HashMap<PathBuf, CatalogBuilder>,
        control_builder: CatalogBuilder,
        current_path: Option<PathBuf>,
        /// The current record was captured and `cursor.next()` must finish
        /// before it can be observed again.
        advance_pending: bool,
    },
    /// Creating/opening Tantivy index
    CreatingIndex,
    /// Ready for operations
    Ready,
    /// Seeking to first chunk of a path before deleting old chunks
    SeekingOldChunks {
        writes: PendingFlushes,
        write_idx: usize,
        path_str: String,
    },
    /// Advancing cursor after seek returned TryAdvance
    AdvancingAfterSeek {
        writes: PendingFlushes,
        write_idx: usize,
        path_str: String,
    },
    /// Checking if current record's path matches (to determine if it should be deleted)
    CheckingChunkPath {
        writes: PendingFlushes,
        write_idx: usize,
        path_str: String,
    },
    /// Performing the actual delete of a chunk
    DeletingChunk {
        writes: PendingFlushes,
        write_idx: usize,
        path_str: String,
    },
    /// Flushing pending writes to BTree - seeking phase
    SeekingWrite {
        writes: PendingFlushes,
        write_idx: usize,
        /// Current chunk index to write. None means old chunks deleted, ready to start from 0.
        chunk_idx: Option<usize>,
    },
    /// Flushing pending writes to BTree - insert phase (after seek completed)
    InsertingWrite {
        writes: PendingFlushes,
        write_idx: usize,
        chunk_idx: usize,
        record: ImmutableRecord,
    },
    /// Flushing pending writes to BTree - tracking state
    FlushingWrites {
        writes: PendingFlushes,
        write_idx: usize,
        /// Current chunk index. None means old chunks need deletion first, then start from 0.
        chunk_idx: Option<usize>,
    },
}

impl FtsState {
    fn is_flushing(&self) -> bool {
        matches!(
            self,
            Self::FlushingWrites { .. }
                | Self::SeekingOldChunks { .. }
                | Self::AdvancingAfterSeek { .. }
                | Self::CheckingChunkPath { .. }
                | Self::DeletingChunk { .. }
                | Self::SeekingWrite { .. }
                | Self::InsertingWrite { .. }
        )
    }
}

struct FtsStreamingSegment {
    scorer: Box<dyn Scorer>,
    rowids: Column<i64>,
    alive: Option<AliveBitSet>,
}

struct FtsHitStream {
    segments: Vec<FtsStreamingSegment>,
    segment_pos: usize,
    remaining: usize,
    scores_enabled: bool,
    current: Option<(f32, i64)>,
}

impl FtsHitStream {
    fn advance(&mut self) -> Result<bool> {
        self.current = None;
        if self.remaining == 0 {
            return Ok(false);
        }

        while let Some(segment) = self.segments.get_mut(self.segment_pos) {
            let doc_id = segment.scorer.doc();
            if doc_id == TERMINATED {
                self.segment_pos += 1;
                continue;
            }
            let score = self.scores_enabled.then(|| segment.scorer.score());
            segment.scorer.advance();

            if segment
                .alive
                .as_ref()
                .is_some_and(|alive| alive.is_deleted(doc_id))
            {
                continue;
            }
            let rowid = segment.rowids.first(doc_id).ok_or_else(|| {
                LimboError::InternalError("FTS: rowid fast field missing value".into())
            })?;
            self.current = Some((score.unwrap_or(0.0), rowid));
            self.remaining -= 1;
            return Ok(true);
        }

        Ok(false)
    }
}

/// Cursor for executing FTS operations (queries, inserts, deletes).
///
/// Implements `IndexMethodCursor` to integrate with turso's VDBE execution.
/// Uses a state machine pattern for async IO operations. Manages:
/// - Tantivy index/reader/writer/searcher instances
/// - BTree storage via `HybridBTreeDirectory`
/// - Document batching for efficient bulk inserts
/// - Query result iteration
pub struct FtsCursor {
    schema: Schema,
    rowid_field: Field,
    text_fields: Vec<(IndexColumn, Field)>,
    dir_table_name: String,
    /// Pre-computed default fields for QueryParser (avoids rebuilding Vec per query)
    default_fields: Vec<Field>,
    /// Pre-computed (Field, boost) pairs for QueryParser (avoids re-iterating per query)
    field_boosts: Vec<(Field, f32)>,
    /// (min_gram, max_gram) window for the ngram tokenizer
    ngram_window: (usize, usize),
    /// Query parser shared with other read cursors on the same snapshot.
    cached_parser: Option<Arc<tantivy::query::QueryParser>>,
    shared_cache: Arc<RwLock<CachedFtsStates>>,
    shared_writer: Arc<Mutex<Option<CachedFtsWriter>>>,
    /// Shared with the attachment: the one cursor allowed to flush this index
    /// per connection. See [`FtsWriterSlot`].
    writer_slot: Arc<Mutex<Option<FtsWriterSlot>>>,
    /// This cursor's identity in [`FtsWriterSlot`] claims.
    cursor_instance_id: u64,
    /// Weak so a cursor parked on its connection does not keep the connection
    /// alive (see `IndexMethodContext::connection`).
    connection: Option<Weak<Connection>>,
    database_id: Option<usize>,
    fts_dir_cursor: Option<Box<dyn CursorTrait>>,
    btree_root_page: Option<i64>,
    control: Option<FtsControlRecord>,
    hybrid_directory: Option<HybridBTreeDirectory>,
    index: Option<Index>,
    reader: Option<IndexReader>,
    writer: Option<IndexWriter>,
    searcher: Option<Searcher>,
    state: FtsState,
    pending_docs_count: usize,
    current_hits: Vec<(f32, DocAddress, i64)>,
    streaming_hits: Option<FtsHitStream>,
    hit_pos: usize,
    current_pattern: i64,
    /// True while `open_write` is driving the shared open state machine.
    /// Read state is never published or reused in this mode.
    opening_for_write: bool,
    /// True once this cursor holds the per-index writer slot (and the MVCC
    /// write lease). See [`FtsWriterSlot`].
    holds_writer_slot: bool,
    runtime_stats: Arc<FtsRuntimeStats>,
}

impl FtsCursor {
    /// Creates a new FTS cursor with the given configuration.
    fn new(attachment: &FtsIndexAttachment) -> Self {
        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            attachment.cfg.index_name
        );
        let text_fields = attachment.text_fields.clone();
        let default_fields: Vec<Field> = text_fields.iter().map(|(_, f)| *f).collect();
        let field_boosts: Vec<(Field, f32)> = text_fields
            .iter()
            .filter_map(|(col, field)| {
                attachment
                    .field_weights
                    .get(&col.name)
                    .map(|&boost| (*field, boost))
            })
            .collect();
        Self {
            schema: attachment.schema.clone(),
            rowid_field: attachment.rowid_field,
            text_fields,
            dir_table_name,
            default_fields,
            field_boosts,
            ngram_window: attachment.ngram_window,
            cached_parser: None,
            shared_cache: Arc::clone(&attachment.cached_state),
            shared_writer: Arc::clone(&attachment.cached_writer),
            writer_slot: Arc::clone(&attachment.writer_slot),
            cursor_instance_id: NEXT_FTS_CURSOR_INSTANCE.fetch_add(1, Ordering::Relaxed),
            connection: None,
            database_id: None,
            fts_dir_cursor: None,
            btree_root_page: None,
            control: None,
            hybrid_directory: None,
            index: None,
            reader: None,
            writer: None,
            searcher: None,
            state: FtsState::Init,
            pending_docs_count: 0,
            current_hits: Vec::new(),
            streaming_hits: None,
            hit_pos: 0,
            current_pattern: FTS_PATTERN_SCORE,
            opening_for_write: false,
            holds_writer_slot: false,
            runtime_stats: Arc::clone(&attachment.runtime_stats),
        }
    }

    /// Claim the per-index writer slot for this cursor, or refuse if another
    /// cursor on the same connection already holds it. Under MVCC this also
    /// acquires the per-index write lease, so a transaction only pays for the
    /// lease once it actually mutates the index. See [`FtsWriterSlot`].
    fn claim_writer_slot(&mut self) -> Result<()> {
        if self.holds_writer_slot {
            return Ok(());
        }
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                LimboError::InternalError("FTS cursor claimed writer slot before open".to_string())
            })?;
        {
            let mut slot = self.writer_slot.lock();
            if let Some(claim) = slot.as_ref() {
                let same_live_connection = claim
                    .connection
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, &conn));
                if same_live_connection && claim.cursor_instance != self.cursor_instance_id {
                    // Raise(Abort) so the whole statement rolls back: the
                    // refused write may sit mid-statement (a trigger body),
                    // and its base rows must not commit without their index
                    // entries.
                    return Err(LimboError::Raise(
                        turso_parser::ast::ResolveType::Abort,
                        "statement already has an open writer on this FTS index; \
                         a trigger cannot write the FTS-indexed table its firing \
                         statement is writing"
                            .to_string(),
                    ));
                }
                // A claim from another (or dead) connection: cross-connection
                // writers are serialized by the pager write lock or the MVCC
                // write lease, so this claim is stale. Replace it.
            }
            *slot = Some(FtsWriterSlot {
                connection: Arc::downgrade(&conn),
                cursor_instance: self.cursor_instance_id,
            });
        }
        if let Err(err) = self.acquire_mvcc_write_lease() {
            self.release_writer_slot();
            return Err(err);
        }
        self.holds_writer_slot = true;
        Ok(())
    }

    /// Under MVCC, take the per-index write lease for this cursor's
    /// transaction. Reentrant for the owning transaction; a no-op in WAL mode.
    fn acquire_mvcc_write_lease(&self) -> Result<()> {
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| LimboError::InternalError("FTS cursor has no connection".to_string()))?;
        let database_id = self.database_id.ok_or_else(|| {
            LimboError::InternalError("FTS database id is not initialized".to_string())
        })?;
        let Some(mv_store) = conn.mv_store_for_db(database_id) else {
            return Ok(());
        };
        let tx_id = conn.get_mv_tx_id_for_db(database_id).ok_or_else(|| {
            LimboError::InternalError(
                "FTS write opened without an active MVCC transaction".to_string(),
            )
        })?;
        let root_page = self.btree_root_page.ok_or_else(|| {
            LimboError::InternalError("FTS backing root is not initialized".to_string())
        })?;
        let snapshot_ts = mv_store.read_snapshot_ts(tx_id);
        // A PASSIVE checkpoint can retire this root page under a stale
        // compiled plan; that is a stale-schema read, not corruption.
        let index_id = if conn.experimental_mvcc_passive_checkpoint_enabled() {
            mv_store
                .try_get_table_id_from_root_page_at(root_page, snapshot_ts)
                .ok_or(LimboError::SchemaUpdated)?
        } else {
            mv_store.get_table_id_from_root_page_at(root_page, snapshot_ts)
        };
        match mv_store.acquire_index_method_write_lease(tx_id, index_id) {
            Ok(()) => {
                self.runtime_stats
                    .write_lease_acquisitions
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err @ (LimboError::WriteWriteConflict | LimboError::Busy)) => {
                self.runtime_stats
                    .write_lease_rejections
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    /// Body of `open_write`; split out so the wrapper can clear
    /// `opening_for_write` on every error return.
    fn open_write_inner(
        &mut self,
        context: &IndexMethodContext,
        conn: &Arc<Connection>,
        database_id: usize,
    ) -> Result<IOResult<()>> {
        if self.writer.is_some() {
            return Ok(IOResult::Done(()));
        }

        initialize_btree_storage_table(conn, database_id, &self.dir_table_name)?;
        self.open_cursor(conn, database_id)?;
        // The per-index write lease is taken lazily, on the first document
        // mutation (see `claim_writer_slot`): a write cursor that never
        // touches a document — an UPDATE matching no rows — must not lock the
        // index for the rest of its transaction.
        if self.restore_cached_writer(context)? {
            return Ok(IOResult::Done(()));
        }

        // First do open_read to load existing index
        match &self.state {
            FtsState::Ready => {}
            _ => {
                let result = self.open_read(context)?;
                if let IOResult::IO(io) = result {
                    return Ok(IOResult::IO(io));
                }
            }
        }
        // The IndexWriter itself is built lazily on the first document
        // mutation (`ensure_writer`), after the writer slot and MVCC lease
        // are claimed — so a refused writer never pays for Tantivy writer
        // construction, and a write cursor that never mutates never builds
        // one at all.
        Ok(IOResult::Done(()))
    }

    /// Build the Tantivy `IndexWriter` if this cursor does not have one yet.
    /// Callers must claim the writer slot first (`claim_writer_slot`).
    fn ensure_writer(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("FTS index not initialized".into()))?;
        // One worker and one merge thread: merges are driven synchronously by
        // commit_writer_with_maintenance, so the default four merge threads
        // would sit idle forever, at three OS threads apiece.
        self.runtime_stats
            .tantivy_writer_constructions
            .fetch_add(1, Ordering::Relaxed);
        let writer = index
            .writer_with_options(
                tantivy::indexer::IndexWriterOptions::builder()
                    .num_worker_threads(1)
                    .num_merge_threads(1)
                    .memory_budget_per_thread(DEFAULT_MEMORY_BUDGET_BYTES)
                    .build(),
            )
            .map_err(|e| LimboError::InternalError(e.to_string()))?;
        // Disable background merges.
        writer.set_merge_policy(Box::new(NoMergePolicy));
        self.writer = Some(writer);
        Ok(())
    }

    /// Release the writer slot if this cursor holds it. Idempotent. The MVCC
    /// write lease stays with the transaction until it commits or rolls back.
    fn release_writer_slot(&mut self) {
        self.holds_writer_slot = false;
        let mut slot = self.writer_slot.lock();
        if slot
            .as_ref()
            .is_some_and(|claim| claim.cursor_instance == self.cursor_instance_id)
        {
            *slot = None;
        }
    }

    /// Open the BTree cursor for FTS directory storage
    fn open_cursor(&mut self, conn: &Arc<Connection>, database_id: usize) -> Result<()> {
        if self.fts_dir_cursor.is_some() {
            return Ok(());
        }
        // Open cursor for the FTS directory index
        // The index stores all 3 columns: (path, chunk_no, bytes) as the key
        // This is similar to how toy_vector_sparse_ivf stores all data in the index
        let index_name = format!("{}_key", self.dir_table_name);

        // Get root page for HybridBTreeDirectory
        let scratch = conn
            .with_schema(database_id, |schema| {
                schema.get_index(&self.dir_table_name, &index_name).cloned()
            })
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index {} for table {} not found",
                    index_name, self.dir_table_name
                ))
            })?;
        let root_page = scratch.root_page;

        self.btree_root_page = Some(root_page);

        self.fts_dir_cursor = Some(open_index_cursor(
            conn,
            database_id,
            &self.dir_table_name,
            &index_name,
            [key_info(), key_info(), key_info()],
        )?);
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

        // Register "ngram" tokenizer for substring matching. The window comes
        // from the WITH clause `min_gram`/`max_gram` keys and was validated at
        // CREATE INDEX time, so construction cannot fail here.
        // Using prefix=false for full n-gram (not just prefix)
        let (min_gram, max_gram) = self.ngram_window;
        if let Ok(ngram) = NgramTokenizer::new(min_gram, max_gram, false) {
            // Lowercase the n-grams so matching is case-insensitive, like the
            // other tokenizers.
            let analyzer = TextAnalyzer::builder(ngram)
                .filter(tantivy::tokenizer::LowerCaser)
                .build();
            tokenizers.register("ngram", analyzer);
        }
    }

    fn build_query_parser(&self, index: &Index) -> Arc<tantivy::query::QueryParser> {
        let mut parser = tantivy::query::QueryParser::for_index(index, self.default_fields.clone());
        for &(field, boost) in &self.field_boosts {
            parser.set_field_boost(field, boost);
        }
        Arc::new(parser)
    }

    /// "Check out" this connection's cached read state: hand the cursor
    /// clones of the cache entry's immutable Tantivy objects (directory
    /// catalog, index, reader, parser) so it can serve queries without
    /// reloading everything from the backing B-tree. The entry itself stays
    /// in the cache for the next cursor; only its position moves to
    /// most-recently-used. Returns `None` when there is no entry for this
    /// connection or the entry no longer reflects what a query would see on
    /// disk.
    fn checkout_cached_state(
        &self,
        context: &IndexMethodContext,
        visible_control: Option<&FtsControlRecord>,
    ) -> Option<CachedFtsCheckout> {
        let conn = context.connection().ok()?;
        if conn.is_in_write_tx() {
            return None;
        }
        let mut cache = self.shared_cache.write();
        cache.prune();
        let position = cache.connection_position(&conn)?;
        let cached = &cache.entries[position];
        let same_snapshot = cached.matches_snapshot(context);
        let same_manifest = cached.matches_manifest(visible_control);
        if !same_snapshot && !same_manifest {
            if visible_control.is_some() {
                cache.entries.remove(position);
            }
            return None;
        }

        let cached = cache.entries.remove(position);
        let checkout = (
            cached.directory.clone_with_fresh_pending(),
            cached.index.clone(),
            cached.reader.clone(),
            Arc::clone(&cached.query_parser),
            cached.control.clone(),
        );
        cache.entries.push(cached);
        Some(checkout)
    }

    fn has_cached_state(&self, context: &IndexMethodContext) -> bool {
        let Ok(conn) = context.connection() else {
            return false;
        };
        !conn.is_in_write_tx()
            && self
                .shared_cache
                .read()
                .connection_position(&conn)
                .is_some()
    }

    /// Install a cache checkout on this cursor. Returns `true` when the cursor
    /// is ready to serve queries, `false` when the caller must keep driving the
    /// state machine (a write cursor still has to build its own Tantivy index).
    fn install_cached_checkout(&mut self, checkout: CachedFtsCheckout) -> bool {
        let (directory, index, reader, query_parser, control) = checkout;
        self.hybrid_directory = Some(directory);
        self.control = Some(control);
        if self.opening_for_write {
            // A writer needs an Index whose Directory owns cursor-local pending
            // state. The cached Index shares the cache entry's directory, so
            // installing it would send this cursor's writes into the cache
            // entry's pending map, where the flush never finds them. Rebuild
            // the Index over the fresh directory clone instead.
            tracing::debug!("FTS open_write: using cached directory snapshot, rebuilding index");
            self.state = FtsState::CreatingIndex;
            return false;
        }
        self.runtime_stats
            .read_cache_hits
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!("FTS open_read: using cached state (skipping catalog load)");
        self.index = Some(index);
        self.searcher = Some(reader.searcher());
        self.reader = Some(reader);
        self.cached_parser = Some(query_parser);
        self.state = FtsState::Ready;
        true
    }

    /// Restore a writer retained by the previous statement when its committed
    /// metadata still matches this transaction's backing B-tree view.
    fn restore_cached_writer(&mut self, context: &IndexMethodContext) -> Result<bool> {
        let conn = context.connection()?;
        self.runtime_stats
            .writer_cache_lookups
            .fetch_add(1, Ordering::Relaxed);
        let database_id = self.database_id.ok_or_else(|| {
            LimboError::InternalError("FTS database id is not initialized".to_string())
        })?;
        let mut cached_writer = self.shared_writer.lock();
        let Some(cached) = cached_writer.as_ref() else {
            self.runtime_stats
                .writer_cache_misses
                .fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        };

        let same_connection = cached
            .connection
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &conn));
        let metadata_matches = if conn.mv_store_for_db(database_id).is_some() {
            same_connection
                && context.transaction_id().is_some()
                && context.transaction_id() == cached.snapshot.transaction_id()
        } else {
            same_connection
                && cached
                    .snapshot
                    .wal_pos()
                    .is_some_and(|position| cached.directory.pager.wal_pos() == position)
        };
        if !metadata_matches {
            // A committed MVCC writer is retained until its small control
            // record can be validated asynchronously by open_read().
            if conn.mv_store_for_db(database_id).is_some()
                && same_connection
                && cached.snapshot.transaction_id().is_none()
            {
                return Ok(false);
            }
            // The slot is shared by every connection. Only remove an entry
            // this connection owns, or one whose owner is gone; a live writer
            // owned by another connection stays cached for its owner.
            let stale = if same_connection || cached.connection.strong_count() == 0 {
                cached_writer.take()
            } else {
                None
            };
            drop(cached_writer);
            self.runtime_stats
                .writer_cache_validation_failures
                .fetch_add(1, Ordering::Relaxed);
            self.runtime_stats
                .writer_cache_misses
                .fetch_add(1, Ordering::Relaxed);
            drop(stale);
            return Ok(false);
        }

        let cached = cached_writer.take().ok_or_else(|| {
            LimboError::InternalError("validated cached FTS writer disappeared".to_string())
        })?;
        drop(cached_writer);
        self.runtime_stats
            .writer_cache_hits
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!("FTS open_write: reusing committed writer state");
        self.control = Some(cached.control);
        self.hybrid_directory = Some(cached.directory);
        self.index = Some(cached.index);
        self.writer = Some(cached.writer);
        self.reader = None;
        self.searcher = None;
        self.cached_parser = None;
        self.state = FtsState::Ready;
        Ok(true)
    }

    /// Does this connection have a writer whose transaction committed but
    /// whose view has not been re-checked against the on-disk control record
    /// yet? Such a writer may only be reused after that check
    /// (`take_writer_if_control_matches`).
    fn writer_needs_control_check(&self, context: &IndexMethodContext) -> bool {
        let Ok(conn) = context.connection() else {
            return false;
        };
        self.shared_writer.lock().as_ref().is_some_and(|cached| {
            cached.snapshot.transaction_id().is_none()
                && cached
                    .connection
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, &conn))
        })
    }

    /// Reuse this connection's committed writer if the control record it was
    /// built on is still the one visible on disk; otherwise leave (or drop)
    /// it and report a miss.
    fn take_writer_if_control_matches(
        &mut self,
        context: &IndexMethodContext,
        visible_control: &FtsControlRecord,
    ) -> bool {
        let Ok(conn) = context.connection() else {
            return false;
        };
        let mut cached_writer = self.shared_writer.lock();
        let Some(cached) = cached_writer.as_ref() else {
            return false;
        };
        let owner = cached.connection.upgrade();
        let same_connection = owner
            .as_ref()
            .is_some_and(|owner| Arc::ptr_eq(owner, &conn));
        let deferred = cached.snapshot.transaction_id().is_none();
        let matches_control = cached.control.index_incarnation == visible_control.index_incarnation
            && cached.control.manifest_generation == visible_control.manifest_generation;
        if !(same_connection && deferred && matches_control) {
            // A dead owner, or a committed writer the committed control record
            // has moved past, can never be reused by anyone: remove it. A live
            // writer owned by another connection stays cached for its owner.
            let stale = if owner.is_none() || (deferred && !matches_control) {
                cached_writer.take()
            } else {
                None
            };
            drop(cached_writer);
            self.runtime_stats
                .writer_cache_validation_failures
                .fetch_add(1, Ordering::Relaxed);
            self.runtime_stats
                .writer_cache_misses
                .fetch_add(1, Ordering::Relaxed);
            drop(stale);
            return false;
        }

        // The invariant (slot lock held from the check to the take) makes
        // this infallible; treat a violation as a cache miss, not a crash.
        let Some(cached) = cached_writer.take() else {
            tracing::error!("validated cached FTS writer disappeared");
            return false;
        };
        drop(cached_writer);
        self.runtime_stats
            .writer_cache_hits
            .fetch_add(1, Ordering::Relaxed);
        self.control = Some(cached.control);
        self.hybrid_directory = Some(cached.directory);
        self.index = Some(cached.index);
        self.writer = Some(cached.writer);
        self.reader = None;
        self.searcher = None;
        self.cached_parser = None;
        self.state = FtsState::Ready;
        true
    }

    /// Drop this connection's committed-but-unchecked writer when the control
    /// record it needed to validate against turns out not to exist.
    fn drop_unchecked_writer(&self, context: &IndexMethodContext) {
        let Ok(conn) = context.connection() else {
            return;
        };
        let mut cached_writer = self.shared_writer.lock();
        // A missing control record proves this connection's deferred writer is
        // stale. The slot is shared by every connection, so a live writer
        // owned by another connection stays cached for its owner.
        let discard =
            cached_writer
                .as_ref()
                .is_some_and(|cached| match cached.connection.upgrade() {
                    None => true,
                    Some(owner) => {
                        Arc::ptr_eq(&owner, &conn) && cached.snapshot.transaction_id().is_none()
                    }
                });
        if !discard {
            return;
        }
        let stale = cached_writer.take();
        drop(cached_writer);
        self.runtime_stats
            .writer_cache_validation_failures
            .fetch_add(1, Ordering::Relaxed);
        self.runtime_stats
            .writer_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        drop(stale);
    }

    /// Retain a fully flushed writer for the next statement on this connection.
    fn cache_writer(&mut self, context: &IndexMethodContext) {
        let Ok(conn) = context.connection() else {
            return;
        };
        if self.pending_docs_count != 0 || !matches!(self.state, FtsState::Ready) {
            tracing::trace!(
                pending_documents = self.pending_docs_count,
                state = ?self.state,
                "FTS writer cache: cursor is not fully prepared"
            );
            return;
        }
        let Some(directory) = self.hybrid_directory.as_ref() else {
            tracing::trace!("FTS writer cache: directory is not initialized");
            return;
        };
        let retained_read_bytes = self.shared_cache.read().resident_cache_bytes();
        if directory
            .hot_cache
            .size()
            .saturating_add(retained_read_bytes)
            > fts_max_retained_cache_bytes()
        {
            // Keep the writer — it is the newest committed state, and the
            // cursor held these bytes while it ran anyway — and make room by
            // evicting retained read snapshots instead. Rejecting the writer
            // would leave a stale one in the slot and force the next write
            // statement into a full cold load.
            self.runtime_stats
                .cache_admission_rejections
                .fetch_add(1, Ordering::Relaxed);
            let read_budget =
                fts_max_retained_cache_bytes().saturating_sub(directory.hot_cache.size());
            self.shared_cache.write().evict_to_fit(read_budget);
            tracing::debug!(
                resident_bytes = directory.hot_cache.size(),
                budget_bytes = fts_max_retained_cache_bytes(),
                "FTS writer cache: over retention budget; evicted read snapshots"
            );
        }
        // Index creation and failed statements can close a cursor before all
        // directory mutations enter the normal document flush state machine.
        // Such a writer is not reusable.
        if !directory.pending_mutations.read().by_path.is_empty()
            || !directory.flushing_writes.read().is_empty()
        {
            tracing::trace!("FTS writer cache: directory still has pending mutations");
            return;
        }
        if self.writer.is_none() {
            tracing::trace!("FTS writer cache: cursor has no writer");
            return;
        }
        // Keep `self.control` set: `on_transaction_committed` compares it against
        // the cached writer's control record before re-stamping the WAL
        // position.
        let Some(control) = self.control.clone() else {
            tracing::error!("FTS writer cache: control record is not initialized");
            return;
        };
        let writer = self.writer.take().expect("writer checked above");
        let index = self
            .index
            .take()
            .expect("FTS writer must have an initialized index");
        let directory = self
            .hybrid_directory
            .take()
            .expect("FTS writer must have an initialized directory");
        let wal_pos = directory.pager.wal_pos();
        let snapshot = match context.transaction_id() {
            Some(transaction_id) => FtsCachedSnapshot::Mvcc { transaction_id },
            None => FtsCachedSnapshot::Wal {
                pos: (wal_pos != (u32::MAX, u64::MAX)).then_some(wal_pos),
            },
        };

        let previous = self.shared_writer.lock().replace(CachedFtsWriter {
            connection: Arc::downgrade(&conn),
            snapshot,
            control,
            directory,
            index,
            writer,
        });
        // Dropping an old writer can join Tantivy worker threads. Do it after
        // releasing the cache lock so another cursor is never blocked on it.
        drop(previous);
    }

    fn constant_integer_expression(expr: &turso_parser::ast::Expr) -> Option<i64> {
        match expr {
            turso_parser::ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                Self::constant_integer_expression(&expressions[0])
            }
            _ => match crate::util::parse_signed_number(expr).ok()? {
                Value::Numeric(crate::numeric::Numeric::Integer(value)) => Some(value),
                Value::Numeric(crate::numeric::Numeric::Float(value)) => {
                    crate::util::cast_real_to_integer(f64::from(value)).ok()
                }
                Value::Null | Value::Text(_) | Value::Blob(_) => None,
            },
        }
    }

    /// Create Tantivy index from directory (hybrid or cached)
    fn create_index_from_directory(&mut self) -> Result<()> {
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

                    // If starting a new file (chunk_idx is Some(0)), first delete old chunks
                    if *chunk_idx == Some(0) {
                        let path_str = writes[*write_idx].0.to_string_lossy().to_string();
                        self.state = FtsState::SeekingOldChunks {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            path_str,
                        };
                        continue;
                    }

                    let chunk_size = DEFAULT_CHUNK_SIZE;
                    let total_chunks = writes[*write_idx]
                        .1
                        .as_ref()
                        .map(|data| data.len().div_ceil(chunk_size).max(1))
                        .unwrap_or(0);

                    // None means old chunks deleted, ready to start from 0
                    let actual_chunk_idx = chunk_idx.unwrap_or(0);

                    // Deletes have zero chunks. Empty files have one empty chunk.
                    if total_chunks == 0 || actual_chunk_idx >= total_chunks {
                        *write_idx += 1;
                        *chunk_idx = Some(0);
                        continue;
                    }

                    // Transition to seeking state for writing this chunk
                    self.state = FtsState::SeekingWrite {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        chunk_idx: Some(actual_chunk_idx),
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
                            Value::from_i64(0),
                            Value::Blob(crate::alloc::vec![]),
                        ],
                        3,
                    )?;

                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(seek_key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));

                    match seek_result {
                        SeekResult::NotFound => {
                            // No matching records at all, start writing from chunk 0
                            self.state = FtsState::FlushingWrites {
                                writes: std::mem::take(writes),
                                write_idx: *write_idx,
                                chunk_idx: None, // None = ready to start from chunk 0
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
                            chunk_idx: None, // Ready to start from chunk 0
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
                            chunk_idx: None, // Ready to start from chunk 0 // Special value to trigger first write
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
                            chunk_idx: None, // Ready to start from chunk 0
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

                    // Re-seek from the logical prefix after every delete.
                    // B-tree deletion may retreat, preserve, or advance the
                    // physical cursor depending on balancing; calling next()
                    // can therefore skip a sibling value for the same
                    // (path, chunk_no) prefix. Re-seeking is unambiguous and
                    // enforces one logical value even though bytes remain a
                    // physical key column.
                    self.state = FtsState::SeekingOldChunks {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        path_str: std::mem::take(path_str),
                    };
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
                    let data = data.as_ref().ok_or_else(|| {
                        LimboError::InternalError("FTS delete advanced to the write phase".into())
                    })?;
                    let path_str = path.to_string_lossy().to_string();
                    let chunk_size = DEFAULT_CHUNK_SIZE;
                    // None means ready to start from chunk 0
                    let actual_chunk_idx = chunk_idx.unwrap_or(0);

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
                            Value::from_i64(actual_chunk_idx as i64),
                            Value::from_slice(chunk_data)?,
                        ],
                        3,
                    )?;

                    // Seek to find the correct position using GE (not eq_only)
                    // This positions the cursor at or after where the record should be inserted
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));

                    let exact_key_exists =
                        if matches!(seek_result, SeekResult::Found) && cursor.has_record() {
                            return_if_io!(cursor.record()).is_some_and(|existing| {
                                let existing_path =
                                    existing.get_value_opt(0).and_then(|value| match value {
                                        crate::types::ValueRef::Text(text) => Some(text.value),
                                        _ => None,
                                    });
                                let existing_chunk =
                                    existing.get_value_opt(1).and_then(|value| match value {
                                        crate::types::ValueRef::Numeric(
                                            crate::numeric::Numeric::Integer(value),
                                        ) => Some(value),
                                        _ => None,
                                    });
                                let existing_bytes =
                                    existing.get_value_opt(2).and_then(|value| match value {
                                        crate::types::ValueRef::Blob(bytes) => Some(bytes),
                                        _ => None,
                                    });
                                existing_path == Some(path_str.as_str())
                                    && existing_chunk == Some(actual_chunk_idx as i64)
                                    && existing_bytes == Some(chunk_data)
                            })
                        } else {
                            false
                        };

                    if exact_key_exists {
                        // Resumption or a previously duplicated physical row
                        // may leave the exact full key present. Treat the
                        // logical write as idempotent instead of inserting a
                        // second identical `(path, chunk_no, bytes)` key.
                        self.state = FtsState::FlushingWrites {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: Some(actual_chunk_idx + 1),
                        };
                    } else {
                        // Don't do insert in the same state to avoid re-seeking on I/O.
                        self.state = FtsState::InsertingWrite {
                            writes: std::mem::take(writes),
                            write_idx: *write_idx,
                            chunk_idx: actual_chunk_idx,
                            record,
                        };
                    }
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

                    // the cursor should be positioned correctly after seek
                    return_if_io!(cursor.insert(&BTreeKey::IndexKey(record.as_record_ref())));

                    // Move to next chunk
                    self.state = FtsState::FlushingWrites {
                        writes: std::mem::take(writes),
                        write_idx: *write_idx,
                        chunk_idx: Some(*chunk_idx + 1),
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

    /// Commit pending documents to Tantivy and flush to BTree.
    /// If `force_flush` is true, flushes directory writes even when no pending docs.
    fn commit_and_flush_inner(&mut self, force_flush: bool) -> Result<IOResult<()>> {
        // Handle flush state machine if already in progress
        if self.state.is_flushing() {
            return self.flush_writes_internal();
        }

        if self.pending_docs_count == 0 && !force_flush {
            return Ok(IOResult::Done(()));
        }

        // Commit Tantivy to make documents visible
        let mut committed_writer = false;
        if let Some(ref mut writer) = self.writer {
            tracing::debug!(
                "FTS commit_and_flush: committing {} documents",
                self.pending_docs_count
            );
            let index = self.index.as_ref().ok_or_else(|| {
                LimboError::InternalError("FTS index not initialized".to_string())
            })?;
            let directory = self.hybrid_directory.as_ref().ok_or_else(|| {
                LimboError::InternalError("FTS directory not initialized".to_string())
            })?;
            Self::commit_writer_with_maintenance(index, writer, directory)?;
            committed_writer = true;

            // The cached reader belongs to the previous index snapshot.
            if let Some(conn) = self.connection.as_ref().and_then(Weak::upgrade) {
                let mut cache = self.shared_cache.write();
                if cache.connection_position(&conn).is_some() {
                    tracing::debug!("FTS commit_and_flush: invalidating cached read state");
                    cache.remove_connection(&conn);
                }
            }
        }
        if committed_writer {
            self.stage_control_record()?;
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
            .map(|dir| dir.take_pending_flushes())
            .unwrap_or_default();

        if !writes.is_empty() {
            tracing::debug!(
                "FTS commit_and_flush: flushing {} files to BTree",
                writes.len()
            );
            self.state = FtsState::FlushingWrites {
                writes,
                write_idx: 0,
                chunk_idx: Some(0),
            };
            return self.flush_writes_internal();
        }

        Ok(IOResult::Done(()))
    }

    /// Commit pending documents to Tantivy and flush to BTree.
    pub fn commit_and_flush(&mut self) -> Result<IOResult<()>> {
        self.commit_and_flush_inner(false)
    }

    // The VDBE retries the current instruction when an operation returns
    // `IOResult::IO`, so complete such work before mutating Tantivy.
    fn flush_full_batch_before_mutation(&mut self) -> Result<IOResult<()>> {
        if self.state.is_flushing() {
            return_if_io!(self.flush_writes_internal());
        }

        turso_assert!(
            self.pending_docs_count <= BATCH_COMMIT_SIZE,
            "FTS pending operation count exceeded its batch size",
            {
                "pending_docs_count": self.pending_docs_count,
                "batch_size": BATCH_COMMIT_SIZE
            }
        );

        if self.pending_docs_count == BATCH_COMMIT_SIZE {
            return_if_io!(self.commit_and_flush());
        }

        Ok(IOResult::Done(()))
    }

    /// Build the tiered policy used to identify automatic merge candidates.
    fn automatic_merge_policy() -> LogMergePolicy {
        let mut policy = LogMergePolicy::default();
        policy.set_min_num_segments(FTS_MERGE_FACTOR);
        // Treat one-document segments as their own level. Tantivy's default
        // clips all segments below 10k docs into one level, which repeatedly
        // rewrites large merged segments under small-commit workloads.
        policy.set_min_layer_size(1);
        policy.set_del_docs_ratio_before_merge(FTS_DELETED_DOCS_MERGE_THRESHOLD);
        policy
    }

    fn bounded_merge_candidate(
        policy: &LogMergePolicy,
        segment_metas: &[SegmentMeta],
        max_source_docs: u64,
        max_source_bytes: u64,
        mut file_size: impl FnMut(&Path) -> Option<u64>,
    ) -> Option<Vec<tantivy::index::SegmentId>> {
        for candidate in policy
            .compute_merge_candidates(segment_metas)
            .into_iter()
            // The policy lists larger levels first. Maintaining the smallest
            // eligible level first prevents fresh tiny segments from piling up.
            .rev()
        {
            // Prefer the smallest segments of the candidate: taking the first
            // eight of Tantivy's largest-first list picks exactly the eight
            // most likely to blow the foreground budget, which silently
            // stalls maintenance at that level forever.
            let mut candidate_ids = candidate.0;
            candidate_ids.sort_by_key(|id| {
                segment_metas
                    .iter()
                    .find(|meta| meta.id() == *id)
                    .map_or(u64::MAX, |meta| u64::from(meta.max_doc()))
            });
            let segment_ids = candidate_ids
                .into_iter()
                .take(FTS_MERGE_FACTOR)
                .collect::<Vec<_>>();
            let mut source_docs = 0u64;
            let mut source_bytes = 0u64;

            for segment_id in &segment_ids {
                // The merge policy is third-party code; do not trust it to
                // return only segments it was given.
                let Some(segment_meta) = segment_metas.iter().find(|meta| meta.id() == *segment_id)
                else {
                    tracing::error!("merge policy returned an unknown FTS segment; skipping merge");
                    return None;
                };
                source_docs = source_docs.saturating_add(u64::from(segment_meta.max_doc()));
                for path in segment_meta.list_files() {
                    if let Some(size) = file_size(&path) {
                        source_bytes = source_bytes.saturating_add(size);
                    }
                }
            }

            if source_docs <= max_source_docs && source_bytes <= max_source_bytes {
                return Some(segment_ids);
            }

            tracing::debug!(
                source_docs,
                source_bytes,
                max_docs = max_source_docs,
                max_bytes = max_source_bytes,
                "FTS maintenance: deferring merge beyond foreground budget"
            );
        }
        None
    }

    /// Commit the writer and perform one bounded, synchronous tiered merge.
    ///
    /// Tantivy's background merge workers cannot safely drive our directory:
    /// directory mutations must be captured and persisted by the cursor's
    /// resumable BTree flush inside the current Turso transaction.
    fn commit_writer_with_maintenance(
        index: &Index,
        writer: &mut IndexWriter,
        directory: &HybridBTreeDirectory,
    ) -> Result<()> {
        writer
            .commit()
            .map_err(|e| LimboError::InternalError(format!("FTS commit error: {e}")))?;

        let policy = Self::automatic_merge_policy();

        let segment_metas = index
            .searchable_segment_metas()
            .map_err(|e| LimboError::InternalError(format!("FTS list segments: {e}")))?;
        // Directory writes update the catalog before they enter the resumable
        // BTree flush. Prefer the pending payload when present, then the
        // flushing payload, so the budget always uses the newest byte length.
        let pending_mutations = directory.pending_mutations.read();
        let flushing_writes = directory.flushing_writes.read();
        let catalog = directory.catalog.read();
        let Some(segment_ids) = Self::bounded_merge_candidate(
            &policy,
            &segment_metas,
            FTS_MAX_SYNC_MERGE_DOCS,
            FTS_MAX_SYNC_MERGE_BYTES,
            |path| match pending_mutations.by_path.get(path) {
                Some(PendingFileMutation::Write(data)) => Some(data.len() as u64),
                Some(PendingFileMutation::Delete) => None,
                None => flushing_writes
                    .get(path)
                    .map(|data| data.len() as u64)
                    .or_else(|| catalog.get(path).map(|metadata| metadata.size as u64)),
            },
        ) else {
            return Ok(());
        };
        drop(catalog);
        drop(flushing_writes);
        drop(pending_mutations);
        tracing::debug!(
            "FTS maintenance: merging {} of {} searchable segments",
            segment_ids.len(),
            segment_metas.len()
        );

        writer
            .merge(&segment_ids)
            .wait()
            .map_err(|e| LimboError::InternalError(format!("FTS maintenance merge: {e}")))?;
        writer
            .commit()
            .map_err(|e| LimboError::InternalError(format!("FTS maintenance merge commit: {e}")))?;
        Ok(())
    }

    fn stage_control_record(&mut self) -> Result<()> {
        let directory = self.hybrid_directory.as_ref().ok_or_else(|| {
            LimboError::InternalError("FTS directory not initialized".to_string())
        })?;
        let index_incarnation = self
            .control
            .as_ref()
            .map(|control| control.index_incarnation)
            // A never-written index carries the placeholder incarnation; the
            // first staged control record mints the real one.
            .filter(|&incarnation| incarnation != FTS_EMPTY_INDEX_INCARNATION)
            .or_else(|| {
                self.btree_root_page.map(|root_page| {
                    // Mix in IO-provided entropy so two processes creating
                    // the same index in different files do not mint the same
                    // on-disk incarnation (the counter restarts at 1 in every
                    // process). The IO trait's generator is deterministic
                    // under the simulator.
                    let nonce = NEXT_FTS_INDEX_INCARNATION.fetch_add(1, Ordering::Relaxed);
                    let entropy = directory.pager.io.generate_random_number() as u64;
                    // The placeholder value marks "never written": never mint it.
                    ((root_page as u64).rotate_left(32) ^ nonce ^ entropy).max(1)
                })
            })
            .ok_or_else(|| {
                LimboError::InternalError("FTS backing root is not initialized".to_string())
            })?;
        // Deriving the next generation from this cursor's copy of the control
        // record is safe only because writers are serialized: within a
        // connection by the per-index writer slot (one flushing cursor at a
        // time), across connections by the pager write lock (WAL) or the
        // per-index MVCC write lease, which also refuses any writer whose
        // snapshot predates the last published generation.
        let control = FtsControlRecord::from_catalog(
            self.control.as_ref(),
            index_incarnation,
            &directory.catalog.read(),
        )?;
        directory.queue_write(PathBuf::from(FTS_CONTROL_PATH), control.encode()?);
        self.control = Some(control);
        Ok(())
    }
}

impl Drop for FtsCursor {
    fn drop(&mut self) {
        self.release_writer_slot();
        let is_flushing = self.state.is_flushing();
        if self.pending_docs_count != 0 || is_flushing {
            tracing::error!(
                pending_documents = self.pending_docs_count,
                is_flushing,
                "FTS cursor dropped before explicit statement finalization"
            );
            debug_assert!(
                crate::thread::panicking(),
                "FTS cursor dropped with pending persistence work"
            );
        }
    }
}

impl IndexMethodCursor for FtsCursor {
    /// Creates the FTS index storage (internal BTree table for Tantivy files).
    fn create(&mut self, context: &IndexMethodContext) -> Result<IOResult<()>> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        initialize_btree_storage_table(&conn, database_id, &self.dir_table_name)?;
        return_if_io!(self.open_write(context));
        // The forced flush below stages the index's first control record,
        // which requires a committed writer even for an empty table.
        self.claim_writer_slot()?;
        self.ensure_writer()?;
        self.commit_and_flush_inner(true)
    }

    /// Destroys the FTS index, dropping all storage and clearing caches.
    fn destroy(&mut self, context: &IndexMethodContext) -> Result<IOResult<()>> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));
        tracing::debug!(
            "FTS destroy: dropping internal storage {}",
            self.dir_table_name
        );

        // Serialize with live writers through the same per-index writer slot
        // and MVCC write lease as DML, so DROP INDEX cannot tear the index
        // down under a transaction that is mid-write.
        self.open_cursor(&conn, database_id)?;
        self.claim_writer_slot()?;

        // Drop all in-memory components first
        self.searcher = None;
        self.reader = None;
        self.writer = None;
        self.index = None;
        self.hybrid_directory = None;
        self.fts_dir_cursor = None;
        self.control = None;

        // Invalidate only this connection's cached read state and writer. The
        // drop is not committed yet, so other connections' caches must stay;
        // if the drop commits, their next access misses on the schema change,
        // and a recreated index carries a fresh incarnation so no stale
        // snapshot can validate against it.
        self.shared_cache.write().remove_connection(&conn);
        {
            let mut cached_writer = self.shared_writer.lock();
            let owned_or_dead =
                cached_writer
                    .as_ref()
                    .is_some_and(|cached| match cached.connection.upgrade() {
                        None => true,
                        Some(owner) => Arc::ptr_eq(&owner, &conn),
                    });
            let stale = if owned_or_dead {
                cached_writer.take()
            } else {
                None
            };
            drop(cached_writer);
            drop(stale);
        }

        // Drop the internal storage table and index
        // The backing_btree index will be dropped automatically when the table is dropped
        // Use start_nested() before prepare() to bypass system table protection,
        // then use prepare/run_ignore_rows pattern and disable subtransactions to avoid Busy error
        let db_prefix = conn
            .get_database_name_by_index(database_id)
            .filter(|name| name != "main")
            .map(|name| format!("{}.", quote_identifier(&name)))
            .unwrap_or_default();
        let drop_table_sql = format!(
            "DROP TABLE IF EXISTS {db_prefix}{}",
            quote_identifier(&self.dir_table_name)
        );
        conn.start_nested();
        let mut stmt = conn.prepare(drop_table_sql)?;
        // Disable subtransactions since we're already inside a transaction from the parent DROP INDEX
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        let result = stmt.run_ignore_rows();
        conn.end_nested();
        result?;

        self.state = FtsState::Init;
        Ok(IOResult::Done(()))
    }

    /// Opens the index for reading, loading the catalog and creating a searcher.
    /// Uses async state machine for non-blocking IO during catalog/file loading.
    fn open_read(&mut self, context: &IndexMethodContext) -> Result<IOResult<()>> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        loop {
            match &mut self.state {
                FtsState::Init => {
                    if !self.opening_for_write {
                        self.runtime_stats
                            .read_cache_lookups
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.connection = Some(Arc::downgrade(&conn));
                    // The backing table is created by create() / open_write();
                    // a pure read must not run DDL, or FTS queries fail on
                    // read-only databases. If the table is genuinely missing,
                    // open_cursor reports it.
                    if self.opening_for_write {
                        initialize_btree_storage_table(&conn, database_id, &self.dir_table_name)?;
                    }
                    // Open BTree cursor (needed for btree_root_page)
                    self.open_cursor(&conn, database_id)?;

                    // A cache entry from the exact same snapshot can be used
                    // immediately. A cache from an older snapshot is retained
                    // until the small transactional control record has been
                    // read asynchronously and its identity validated.
                    if let Some(checkout) = self.checkout_cached_state(context, None) {
                        if self.install_cached_checkout(checkout) {
                            return Ok(IOResult::Done(()));
                        }
                        continue;
                    }

                    self.state = if self.has_cached_state(context)
                        || (self.opening_for_write && self.writer_needs_control_check(context))
                    {
                        FtsState::SeekingControl
                    } else {
                        if !self.opening_for_write {
                            self.runtime_stats
                                .read_cache_misses
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        FtsState::Rewinding
                    };
                }
                FtsState::SeekingControl => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    let seek_key = ImmutableRecord::from_values(
                        &[
                            Value::Text(Text::new(FTS_CONTROL_PATH.to_string())),
                            Value::from_i64(0),
                            Value::Blob(crate::alloc::vec![]),
                        ],
                        3,
                    )?;
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(seek_key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    self.state = match seek_result {
                        SeekResult::NotFound => {
                            if self.opening_for_write {
                                self.drop_unchecked_writer(context);
                            }
                            FtsState::Rewinding
                        }
                        SeekResult::TryAdvance => FtsState::AdvancingToControl,
                        SeekResult::Found => FtsState::LoadingControl {
                            chunks: HashMap::default(),
                            advance_pending: false,
                        },
                    };
                }
                FtsState::AdvancingToControl => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    return_if_io!(cursor.next());
                    self.state = if cursor.has_record() {
                        FtsState::LoadingControl {
                            chunks: HashMap::default(),
                            advance_pending: false,
                        }
                    } else {
                        if self.opening_for_write {
                            self.drop_unchecked_writer(context);
                        }
                        FtsState::Rewinding
                    };
                }
                FtsState::LoadingControl {
                    chunks,
                    advance_pending,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if *advance_pending {
                        return_if_io!(cursor.next());
                        *advance_pending = false;
                        continue;
                    }

                    let mut finished = !cursor.has_record();
                    if !finished {
                        let record = return_if_io!(cursor.record()).ok_or_else(|| {
                            LimboError::Corrupt("FTS cursor has no record payload".into())
                        })?;
                        let path = record
                            .get_value_opt(0)
                            .and_then(|value| match value {
                                crate::types::ValueRef::Text(text) => Some(text.value),
                                _ => None,
                            })
                            .ok_or_else(|| {
                                LimboError::Corrupt("FTS chunk path is not text".into())
                            })?;
                        finished = path != FTS_CONTROL_PATH;
                        if !finished {
                            let chunk_no = record
                                .get_value_opt(1)
                                .and_then(|value| match value {
                                    crate::types::ValueRef::Numeric(
                                        crate::numeric::Numeric::Integer(value),
                                    ) => Some(value),
                                    _ => None,
                                })
                                .ok_or_else(|| {
                                    LimboError::Corrupt(
                                        "FTS control chunk number is not an integer".into(),
                                    )
                                })?;
                            let blob = record
                                .get_value_opt(2)
                                .and_then(|value| match value {
                                    crate::types::ValueRef::Blob(blob) => Some(blob.to_vec()),
                                    _ => None,
                                })
                                .ok_or_else(|| {
                                    LimboError::Corrupt(
                                        "FTS control chunk payload is not a blob".into(),
                                    )
                                })?;
                            if chunks.insert(chunk_no, (blob.len(), Some(blob))).is_some() {
                                return Err(LimboError::Corrupt(format!(
                                    "duplicate FTS control chunk {chunk_no}"
                                )));
                            }
                            *advance_pending = true;
                        }
                    }

                    if finished {
                        if chunks.is_empty() {
                            if self.opening_for_write {
                                self.drop_unchecked_writer(context);
                            }
                            self.runtime_stats
                                .manifest_validation_misses
                                .fetch_add(1, Ordering::Relaxed);
                            if !self.opening_for_write {
                                self.runtime_stats
                                    .read_cache_misses
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            self.state = FtsState::Rewinding;
                            continue;
                        }
                        let control_chunks = std::mem::take(chunks);
                        let control_bytes =
                            assemble_catalog_file(Path::new(FTS_CONTROL_PATH), &control_chunks)?;
                        let control = FtsControlRecord::decode(&control_bytes)?;
                        if self.opening_for_write
                            && self.take_writer_if_control_matches(context, &control)
                        {
                            return Ok(IOResult::Done(()));
                        }
                        if let Some(checkout) = self.checkout_cached_state(context, Some(&control))
                        {
                            self.runtime_stats
                                .manifest_validation_hits
                                .fetch_add(1, Ordering::Relaxed);
                            if self.install_cached_checkout(checkout) {
                                return Ok(IOResult::Done(()));
                            }
                            continue;
                        }
                        self.runtime_stats
                            .manifest_validation_misses
                            .fetch_add(1, Ordering::Relaxed);
                        if !self.opening_for_write {
                            self.runtime_stats
                                .read_cache_misses
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        self.state = FtsState::Rewinding;
                    }
                }
                FtsState::Rewinding => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    return_if_io!(cursor.rewind());
                    // Use catalog-first loading for HybridBTreeDirectory
                    self.state = FtsState::LoadingCatalog {
                        catalog_builder: HashMap::default(),
                        control_builder: HashMap::default(),
                        current_path: None,
                        advance_pending: false,
                    };
                }
                FtsState::LoadingCatalog {
                    catalog_builder,
                    control_builder,
                    current_path,
                    advance_pending,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    if *advance_pending {
                        return_if_io!(cursor.next());
                        *advance_pending = false;
                        continue;
                    }

                    if !cursor.has_record() {
                        self.runtime_stats
                            .full_snapshot_loads
                            .fetch_add(1, Ordering::Relaxed);
                        // Done scanning: build a complete file snapshot before Tantivy can run.
                        let mut catalog = HashMap::default();
                        let mut files: HashMap<PathBuf, Vec<u8>> = HashMap::default();

                        for (path, chunks) in catalog_builder.drain() {
                            let max_chunk = chunks
                                .keys()
                                .max()
                                .copied()
                                .expect("catalog builder entries have chunks");
                            let total_size: usize = chunks.values().map(|(size, _)| size).sum();
                            // The chunk number comes straight out of a stored
                            // record; a corrupted value must not overflow.
                            let num_chunks =
                                usize::try_from(max_chunk.checked_add(1).ok_or_else(|| {
                                    LimboError::Corrupt(format!(
                                        "FTS chunk number out of range for {}",
                                        path.display()
                                    ))
                                })?)
                                .map_err(|_| {
                                    LimboError::Corrupt(format!(
                                        "FTS chunk number is negative for {}",
                                        path.display()
                                    ))
                                })?;
                            let metadata = FileMetadata::new(&path, total_size, num_chunks);
                            let assembled = assemble_catalog_file(&path, &chunks)?;
                            files.insert(path.clone(), assembled);
                            catalog.insert(path, metadata);
                        }

                        let control = if control_builder.is_empty() {
                            if catalog.is_empty() {
                                Some(FtsControlRecord::new(FTS_EMPTY_INDEX_INCARNATION))
                            } else {
                                // An index written by a build that predates the
                                // control record: adopt the catalog we just
                                // loaded as the manifest, so reads and writes
                                // keep working. The placeholder incarnation
                                // makes the next staged control record mint a
                                // real one and persist the manifest durably.
                                tracing::info!(
                                    files = catalog.len(),
                                    "FTS storage has no control record; adopting existing catalog"
                                );
                                Some(FtsControlRecord::from_catalog(
                                    None,
                                    FTS_EMPTY_INDEX_INCARNATION,
                                    &catalog,
                                )?)
                            }
                        } else {
                            let control_bytes = assemble_catalog_file(
                                Path::new(FTS_CONTROL_PATH),
                                control_builder,
                            )?;
                            let control = FtsControlRecord::decode(&control_bytes)?;
                            control.validate_catalog(&catalog)?;
                            Some(control)
                        };
                        self.control = control;

                        tracing::debug!(
                            "FTS LoadingCatalog: preloaded {} files ({} bytes)",
                            catalog.len(),
                            files.values().map(Vec::len).sum::<usize>()
                        );

                        let pager = conn.get_pager_from_database_index(&database_id)?;
                        let root_page = self.btree_root_page.ok_or_else(|| {
                            LimboError::InternalError("btree_root_page not set".into())
                        })?;

                        let hybrid_dir =
                            HybridBTreeDirectory::with_preloaded(pager, root_page, catalog, files);
                        self.hybrid_directory = Some(hybrid_dir);
                        self.state = FtsState::CreatingIndex;
                        continue;
                    }

                    // Capture every blob while storage I/O is under the cursor state machine.
                    let record = return_if_io!(cursor.record());
                    let record = record.ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let path_str = record
                        .get_value_opt(0)
                        .and_then(|v| match v {
                            crate::types::ValueRef::Text(t) => Some(t.value.to_string()),
                            _ => None,
                        })
                        .ok_or_else(|| LimboError::Corrupt("FTS chunk path is not text".into()))?;
                    let chunk_no = record
                        .get_value_opt(1)
                        .and_then(|v| match v {
                            crate::types::ValueRef::Numeric(crate::numeric::Numeric::Integer(
                                i,
                            )) => Some(i),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            LimboError::Corrupt("FTS chunk number is not an integer".into())
                        })?;
                    let blob = record
                        .get_value_opt(2)
                        .and_then(|v| match v {
                            crate::types::ValueRef::Blob(blob) => Some(blob.to_vec()),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            LimboError::Corrupt("FTS chunk payload is not a blob".into())
                        })?;

                    // Reuse PathBuf when path hasn't changed (records are BTree-ordered).
                    let path_buf = if current_path.as_ref().map(|p| p.as_os_str().to_str())
                        == Some(Some(&path_str))
                    {
                        current_path.clone().unwrap()
                    } else {
                        let path = PathBuf::from(&path_str);
                        *current_path = Some(path.clone());
                        path
                    };

                    let chunks = if path_buf == Path::new(FTS_CONTROL_PATH) {
                        control_builder
                    } else {
                        catalog_builder.entry(path_buf.clone()).or_default()
                    };
                    // Move the blob into the builder — copying it just to keep
                    // a reference for the error message below would duplicate
                    // the whole index once per cold load.
                    let blob_len = blob.len();
                    if let Some((existing_size, existing_blob)) =
                        chunks.insert(chunk_no, (blob_len, Some(blob)))
                    {
                        let new_blob = chunks.get(&chunk_no).and_then(|(_, blob)| blob.as_deref());
                        return Err(LimboError::Corrupt(format!(
                            "duplicate FTS chunk {}:{} (existing_size={}, new_size={}, equal={})",
                            path_buf.display(),
                            chunk_no,
                            existing_size,
                            blob_len,
                            existing_blob.as_deref() == new_blob
                        )));
                    }

                    *advance_pending = true;
                }
                FtsState::CreatingIndex => {
                    // Log loaded files for debugging
                    if let Some(ref dir) = self.hybrid_directory {
                        tracing::debug!("FTS CreatingIndex: {:?}", dir);
                    }

                    // Create Tantivy index from directory
                    self.create_index_from_directory()?;

                    // Create reader and searcher. Reload is explicit: this
                    // directory's `watch` cannot deliver callbacks, so the
                    // default on-commit policy would silently never fire.
                    let index = self.index.as_ref().ok_or_else(|| {
                        LimboError::InternalError("FTS index not initialized".into())
                    })?;
                    let reader = index
                        .reader_builder()
                        .reload_policy(tantivy::ReloadPolicy::Manual)
                        .try_into()
                        .map_err(|e: tantivy::TantivyError| {
                            LimboError::InternalError(e.to_string())
                        })?;
                    self.searcher = Some(reader.searcher());
                    self.reader = Some(reader);

                    // A write cursor must keep all pending Directory state private.
                    // Read cursors can publish their immutable Tantivy read objects.
                    if !self.opening_for_write && !conn.is_in_write_tx() {
                        let dir = self.hybrid_directory.as_ref().ok_or_else(|| {
                            LimboError::InternalError("FTS directory not initialized".into())
                        })?;
                        let index = self.index.as_ref().ok_or_else(|| {
                            LimboError::InternalError("FTS index not initialized".into())
                        })?;
                        let reader = self.reader.as_ref().ok_or_else(|| {
                            LimboError::InternalError("FTS reader not initialized".into())
                        })?;
                        let query_parser = self.build_query_parser(index);
                        self.cached_parser = Some(Arc::clone(&query_parser));
                        let wal_pos = dir.pager.wal_pos();
                        let snapshot = match context.transaction_id() {
                            Some(transaction_id) => FtsCachedSnapshot::Mvcc { transaction_id },
                            None => FtsCachedSnapshot::Wal {
                                pos: (wal_pos != (u32::MAX, u64::MAX)).then_some(wal_pos),
                            },
                        };

                        let mut cache = self.shared_cache.write();
                        let control = self.control.clone().ok_or_else(|| {
                            LimboError::InternalError("FTS control record not initialized".into())
                        })?;
                        let writer_bytes = self
                            .shared_writer
                            .lock()
                            .as_ref()
                            .map_or(0, |cached| cached.directory.hot_cache.size());
                        let cache_budget =
                            fts_max_retained_cache_bytes().saturating_sub(writer_bytes);
                        let retained = cache.insert(
                            CachedFtsState {
                                connection: Arc::downgrade(&conn),
                                snapshot,
                                control,
                                directory: dir.clone(),
                                index: index.clone(),
                                reader: reader.clone(),
                                query_parser,
                            },
                            cache_budget,
                        );
                        if !retained {
                            self.runtime_stats
                                .cache_admission_rejections
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        tracing::debug!(
                            retained,
                            cached_connections = cache.entries.len(),
                            "FTS CreatingIndex: cached read state for future queries"
                        );
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
    fn open_write(&mut self, context: &IndexMethodContext) -> Result<IOResult<()>> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.opening_for_write = true;
        if self.connection.is_none() {
            self.connection = Some(Arc::downgrade(&conn));
        }

        // `opening_for_write` must stay set across IO yields (the opcode
        // re-enters), but never survive an error: a later open_read on the
        // same cursor would then skip publishing read state forever.
        let result = self.open_write_inner(context, &conn, database_id);
        if !matches!(result, Ok(IOResult::IO(_))) {
            self.opening_for_write = false;
        }
        result
    }

    /// Inserts a document into the FTS index.
    /// Values are text columns followed by rowid. Batches commits for efficiency.
    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        self.claim_writer_slot()?;
        self.ensure_writer()?;
        return_if_io!(self.flush_full_batch_before_mutation());

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
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => {
                return Err(LimboError::InternalError(
                    "FTS rowid must be integer".into(),
                ));
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
                // Coerce every non-NULL value to text before tokenizing, the
                // way FTS5's sqlite3_value_text() does. Skipping them would
                // make the index silently miss rows a plain scan matches.
                Register::Value(value) => {
                    if let Some(text) = value.cast_text() {
                        doc.add_text(*field, &text);
                    }
                }
                _ => continue,
            }
        }

        writer
            .add_document(doc)
            .map_err(|e| LimboError::InternalError(format!("FTS add_document error: {e}")))?;

        self.pending_docs_count += 1;

        Ok(IOResult::Done(()))
    }

    /// Deletes a document from the FTS index by rowid.
    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        self.claim_writer_slot()?;
        self.ensure_writer()?;
        return_if_io!(self.flush_full_batch_before_mutation());

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
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => {
                return Err(LimboError::InternalError(
                    "FTS rowid must be integer".into(),
                ));
            }
        };

        let term = tantivy::Term::from_field_i64(self.rowid_field, rowid);
        writer.delete_term(term);

        // Track delete as a pending operation so commit_and_flush() will run
        // and invalidate cached read state.
        self.pending_docs_count += 1;

        Ok(IOResult::Done(()))
    }

    /// Starts an FTS query. Parses the query string and executes the search.
    /// Returns true if there are results, false otherwise.
    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<bool>> {
        if self.searcher.is_none() {
            let index = self.index.as_ref().ok_or_else(|| {
                LimboError::InternalError("FTS index not initialized - call open_read first".into())
            })?;
            // Manual reload policy: this directory's `watch` cannot deliver
            // callbacks, so the default on-commit policy never fires.
            let reader = index
                .reader_builder()
                .reload_policy(tantivy::ReloadPolicy::Manual)
                .try_into()
                .map_err(|e: tantivy::TantivyError| LimboError::InternalError(e.to_string()))?;
            self.searcher = Some(reader.searcher());
            self.reader = Some(reader);
        }
        let searcher = self
            .searcher
            .as_ref()
            .expect("FTS searcher initialized immediately above");
        if values.len() < 2 {
            return Err(LimboError::InternalError(
                "FTS query_start: expected pattern id and query string".into(),
            ));
        }

        // values[0] = pattern index
        let pattern_idx = match &values[0] {
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => FTS_PATTERN_SCORE,
        };
        self.current_pattern = pattern_idx;

        // values[1] = query string
        let query_str = match &values[1] {
            Register::Value(Value::Text(t)) => t.as_str().to_string(),
            _ => return Err(LimboError::InternalError("FTS query must be text".into())),
        };

        // Determine the optional SQL LIMIT captured by the selected pattern.
        let limit_raw = match pattern_idx {
            // Patterns without LIMIT fetch every live document that matches.
            FTS_PATTERN_MATCH | FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_ORDERED => None,
            // Patterns with LIMIT use the captured value.
            FTS_PATTERN_SCORE
            | FTS_PATTERN_MATCH_LIMIT
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => Some(if values.len() > 2 {
                // Coerce with the same rules as a plain LIMIT (MustBeInt):
                // numeric text and integral reals become integers; anything
                // else is a datatype mismatch, never a silent default.
                let coerced = match &values[2] {
                    Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => {
                        Some(*i)
                    }
                    Register::Value(Value::Numeric(crate::numeric::Numeric::Float(f))) => {
                        crate::util::cast_real_to_integer(f64::from(*f)).ok()
                    }
                    Register::Value(Value::Text(text)) => {
                        match crate::util::checked_cast_text_to_numeric(text.as_str(), true) {
                            Ok(Value::Numeric(crate::numeric::Numeric::Integer(i))) => Some(i),
                            Ok(Value::Numeric(crate::numeric::Numeric::Float(f))) => {
                                crate::util::cast_real_to_integer(f64::from(f)).ok()
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };
                coerced
                    .ok_or_else(|| LimboError::Constraint("datatype mismatch (19)".to_string()))?
            } else {
                return Err(LimboError::InternalError(
                    "FTS query_start: LIMIT pattern selected but no limit value captured"
                        .to_string(),
                ));
            }),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "FTS query_start: unknown pattern {pattern_idx}"
                )));
            }
        };

        // Reuse cached QueryParser or build one on first query
        if self.cached_parser.is_none() {
            let index = self
                .index
                .as_ref()
                .ok_or_else(|| LimboError::InternalError("FTS index not initialized".into()))?;
            self.cached_parser = Some(self.build_query_parser(index));
        }
        let parser = self.cached_parser.as_deref().unwrap();

        // Bound the query string before it reaches Tantivy's recursive
        // parser: a few KiB of nested parentheses would otherwise burn
        // minutes of CPU or overflow the stack (an abort no catch_unwind
        // contains). `parse_query_lenient` uses the non-backtracking parse
        // path, so nesting inside these bounds stays linear.
        if query_str.len() > FTS_MAX_QUERY_BYTES {
            return Err(LimboError::InternalError(format!(
                "FTS query is too long ({} bytes; the limit is {FTS_MAX_QUERY_BYTES})",
                query_str.len()
            )));
        }
        let mut depth = 0usize;
        for byte in query_str.bytes() {
            match byte {
                b'(' => {
                    depth += 1;
                    if depth > FTS_MAX_QUERY_NESTING {
                        return Err(LimboError::InternalError(format!(
                            "FTS query nests deeper than {FTS_MAX_QUERY_NESTING} parentheses"
                        )));
                    }
                }
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        let (query, parse_errors) = parser.parse_query_lenient(&query_str);
        if let Some(error) = parse_errors.first() {
            return Err(LimboError::InternalError(format!(
                "FTS parse error: {error:?}"
            )));
        }

        // TopDocs keeps a heap proportional to its limit. Cap that heap at the
        // number of live documents: this preserves unlimited-query semantics,
        // avoids the former one-million-hit truncation, and prevents a huge SQL
        // LIMIT from allocating beyond the largest possible result set.
        let limit = bounded_query_limit(limit_raw, searcher.num_docs());
        self.current_hits.clear();
        self.streaming_hits = None;
        self.hit_pos = 0;
        if limit == 0 {
            return Ok(IOResult::Done(false));
        }

        // Unordered patterns can walk Tantivy's per-segment scorers directly.
        // This keeps memory constant for the common MATCH path. Global score
        // ordering still uses TopDocs because it inherently needs a top-k heap.
        let streaming_scores = match pattern_idx {
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT => Some(false),
            FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_LIMIT => Some(true),
            _ => None,
        };
        if let Some(scores_enabled) = streaming_scores {
            let scoring = if scores_enabled {
                EnableScoring::enabled_from_searcher(searcher)
            } else {
                EnableScoring::disabled_from_searcher(searcher)
            };
            let weight = query
                .weight(scoring)
                .map_err(|e| LimboError::InternalError(format!("FTS query weight error: {e}")))?;
            let mut segments = Vec::with_capacity(searcher.segment_readers().len());
            for segment_reader in searcher.segment_readers() {
                let scorer = weight
                    .scorer(segment_reader, 1.0)
                    .map_err(|e| LimboError::InternalError(format!("FTS scorer error: {e}")))?;
                let rowids = segment_reader
                    .fast_fields()
                    .i64(ROWID_FIELD)
                    .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;
                segments.push(FtsStreamingSegment {
                    scorer,
                    rowids,
                    alive: segment_reader.alive_bitset().cloned(),
                });
            }
            let mut stream = FtsHitStream {
                segments,
                segment_pos: 0,
                remaining: limit,
                scores_enabled,
                current: None,
            };
            let has_result = stream.advance()?;
            self.streaming_hits = Some(stream);
            return Ok(IOResult::Done(has_result));
        }

        // A global score ordering with no effective LIMIT: TopDocs would
        // eagerly allocate per-segment heaps sized to the whole corpus before
        // scoring a single document. Walk the scorers and sort what actually
        // matched instead, so memory is proportional to the matches.
        if limit >= searcher.num_docs() as usize {
            let weight = query
                .weight(EnableScoring::enabled_from_searcher(searcher))
                .map_err(|e| LimboError::InternalError(format!("FTS query weight error: {e}")))?;
            for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let mut scorer = weight
                    .scorer(segment_reader, 1.0)
                    .map_err(|e| LimboError::InternalError(format!("FTS scorer error: {e}")))?;
                let rowids = segment_reader
                    .fast_fields()
                    .i64(ROWID_FIELD)
                    .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;
                let alive = segment_reader.alive_bitset();
                loop {
                    let doc_id = scorer.doc();
                    if doc_id == TERMINATED {
                        break;
                    }
                    let score = scorer.score();
                    scorer.advance();
                    if alive.is_some_and(|alive| alive.is_deleted(doc_id)) {
                        continue;
                    }
                    let rowid = rowids.first(doc_id).ok_or_else(|| {
                        LimboError::InternalError("FTS: rowid fast field missing value".into())
                    })?;
                    self.current_hits.push((
                        score,
                        DocAddress::new(segment_ord as u32, doc_id),
                        rowid,
                    ));
                }
            }
            self.current_hits
                .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            return Ok(IOResult::Done(!self.current_hits.is_empty()));
        }

        let top_docs = searcher
            .search(
                &query,
                &tantivy::collector::TopDocs::with_limit(limit).order_by_score(),
            )
            .map_err(|e| LimboError::InternalError(format!("FTS search error: {e}")))?;

        // Group results by segment for efficient fast field access.
        // This avoids creating a new fast field reader for each document.
        let mut by_segment: HashMap<u32, Vec<(f32, tantivy::DocAddress)>> = HashMap::default();
        for (score, doc_addr) in top_docs {
            by_segment
                .entry(doc_addr.segment_ord)
                .or_default()
                .push((score, doc_addr));
        }

        // Process each segment's results with a single fast field reader.
        // Fast fields provide columnar O(1) access to rowids without loading full documents.
        for (segment_ord, hits) in by_segment {
            let segment_reader = searcher.segment_reader(segment_ord);
            let rowid_reader = segment_reader
                .fast_fields()
                .i64(ROWID_FIELD)
                .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;

            for (score, doc_addr) in hits {
                let rowid = rowid_reader.first(doc_addr.doc_id).ok_or_else(|| {
                    LimboError::InternalError("FTS: rowid fast field missing value".into())
                })?;
                self.current_hits.push((score, doc_addr, rowid));
            }
        }

        // Re-sort by score since we grouped by segment (preserves original ranking order)
        self.current_hits
            .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(IOResult::Done(!self.current_hits.is_empty()))
    }

    /// Advances to the next query result. Returns true if more results exist.
    fn query_next(&mut self) -> Result<IOResult<bool>> {
        if let Some(stream) = &mut self.streaming_hits {
            return Ok(IOResult::Done(stream.advance()?));
        }
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

        match self.current_pattern {
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT => {
                if self
                    .streaming_hits
                    .as_ref()
                    .is_some_and(|stream| stream.current.is_none())
                    || (self.streaming_hits.is_none() && self.hit_pos >= self.current_hits.len())
                {
                    return Err(LimboError::InternalError(
                        "FTS: query_column out of bounds".into(),
                    ));
                }
                // For fts_match patterns, return 1 (true) - indicates this row matches
                Ok(IOResult::Done(Value::from_i64(1)))
            }
            FTS_PATTERN_SCORE
            | FTS_PATTERN_COMBINED
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => {
                // For fts_score and combined patterns, return the actual score
                let score = if let Some(stream) = &self.streaming_hits {
                    stream.current.map(|(score, _)| score).ok_or_else(|| {
                        LimboError::InternalError("FTS: query_column out of bounds".into())
                    })?
                } else {
                    self.current_hits
                        .get(self.hit_pos)
                        .map(|(score, _, _)| *score)
                        .ok_or_else(|| {
                            LimboError::InternalError("FTS: query_column out of bounds".into())
                        })?
                };
                Ok(IOResult::Done(Value::from_f64(score as f64)))
            }
            _ => {
                // Unknown pattern - return score as default
                let (score, _, _) = self.current_hits.get(self.hit_pos).ok_or_else(|| {
                    LimboError::InternalError("FTS: query_column out of bounds".into())
                })?;
                Ok(IOResult::Done(Value::from_f64(*score as f64)))
            }
        }
    }

    /// Returns the rowid for the current query result.
    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if let Some(stream) = &self.streaming_hits {
            return Ok(IOResult::Done(stream.current.map(|(_, rowid)| rowid)));
        }
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(None));
        }
        let (_, _, rowid) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Some(rowid)))
    }

    /// Flushes pending writes before transaction commit.
    /// This ensures FTS writes are persisted as part of the transaction.
    fn stage_statement_commit(&mut self, _context: &IndexMethodContext) -> Result<IOResult<()>> {
        // First, check if we're in the middle of a flush operation that needs to continue
        // This handles the case where commit_and_flush() returned IOResult::IO and we need
        // to continue the flush after IO completes
        if self.state.is_flushing() {
            return_if_io!(self.flush_writes_internal());
        } else if self.pending_docs_count > 0 {
            tracing::debug!(
                "FTS pre_commit: flushing {} pending documents",
                self.pending_docs_count
            );
            return_if_io!(self.commit_and_flush());
        }
        // This cursor's statement-scope writes are staged; it never flushes
        // again, so a later statement's cursor may write this index.
        self.release_writer_slot();
        Ok(IOResult::Done(()))
    }

    fn abort_statement(&mut self, context: &IndexMethodContext) {
        self.release_writer_slot();
        self.pending_docs_count = 0;
        // Dropping a Tantivy writer joins its worker threads. Take the heavy
        // pieces into locals and drop them after the shared-cache bookkeeping
        // below, mirroring cache_writer's drop(previous) pattern, so no lock
        // is held while the join runs.
        let deferred_writer = self.writer.take();
        let deferred_index = self.index.take();
        let deferred_directory = self.hybrid_directory.take();
        self.reader = None;
        self.searcher = None;
        self.fts_dir_cursor = None;
        self.control = None;
        self.cached_parser = None;
        self.current_hits.clear();
        self.streaming_hits = None;
        // During connection teardown there is no live connection left; the
        // shared caches are then cleaned up by `prune()` / dead-owner checks.
        let Ok(conn) = context.connection() else {
            self.state = FtsState::Init;
            drop(deferred_writer);
            drop(deferred_index);
            drop(deferred_directory);
            return;
        };
        self.shared_cache.write().remove_connection(&conn);
        // The slot is shared by every connection: a failed statement here
        // (e.g. the losing side of a lease conflict) must not evict another
        // connection's writer. Only remove what this connection's failed
        // statement could have produced: its own writer tagged with this
        // transaction (both WAL in WAL mode). An MVCC deferred writer
        // (already re-tagged `Wal`) predates this transaction, so it stays
        // valid across this rollback. Entries with a dead owner are removed
        // opportunistically.
        let mut cached_writer = self.shared_writer.lock();
        let (dead_owner, owned) = match cached_writer.as_ref() {
            None => (false, false),
            Some(cached) => match cached.connection.upgrade() {
                None => (true, false),
                Some(owner) => (
                    false,
                    Arc::ptr_eq(&owner, &conn)
                        && cached.snapshot.transaction_id() == context.transaction_id(),
                ),
            },
        };
        let stale = if dead_owner || owned {
            cached_writer.take()
        } else {
            None
        };
        drop(cached_writer);
        if owned {
            self.runtime_stats
                .writer_cache_rollback_discards
                .fetch_add(1, Ordering::Relaxed);
        }
        self.state = FtsState::Init;
        // Thread joins happen last, with no lock held.
        drop(stale);
        drop(deferred_writer);
        drop(deferred_index);
        drop(deferred_directory);
    }

    fn on_statement_committed(&mut self, context: &IndexMethodContext) {
        self.cache_writer(context);
    }

    fn on_transaction_committed(&mut self, context: &IndexMethodContext) {
        tracing::trace!(
            index = context.index().index_name,
            pending_documents = self.pending_docs_count,
            state = ?self.state,
            has_writer = self.writer.is_some(),
            "FTS transaction outcome: committed"
        );
        self.cache_writer(context);
        let Ok(conn) = context.connection() else {
            self.shared_cache.write().prune();
            return;
        };
        if let Some(transaction_id) = context.transaction_id() {
            // The backing transaction is now committed, so its private writer
            // may be considered by a future transaction only after validating
            // the transactional control record.
            if let Some(cached) = self.shared_writer.lock().as_mut() {
                let same_connection = cached
                    .connection
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, &conn));
                if same_connection
                    && cached.snapshot == (FtsCachedSnapshot::Mvcc { transaction_id })
                {
                    // No WAL position on purpose: the next reuse must first
                    // validate against the on-disk control record.
                    cached.snapshot = FtsCachedSnapshot::Wal { pos: None };
                }
            }
        } else if let Some(cached) = self.shared_writer.lock().as_mut() {
            // This hook also runs for read-only FTS cursors, and the shared
            // slot can still hold an older writer (e.g. one a later statement
            // failed to replace under the retention budget). Re-stamping a
            // writer this commit did not produce or validate would make its
            // stale segments pass WAL-position validation, losing documents
            // committed since. Stamp only when this connection owns the writer
            // and this cursor's control record matches it.
            let same_connection = cached
                .connection
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, &conn));
            let matches_cursor_control = self.control.as_ref().is_some_and(|control| {
                control.index_incarnation == cached.control.index_incarnation
                    && control.manifest_generation == cached.control.manifest_generation
            });
            if same_connection && matches_cursor_control {
                let wal_pos = cached.directory.pager.wal_pos();
                cached.snapshot = FtsCachedSnapshot::Wal {
                    pos: (wal_pos != (u32::MAX, u64::MAX)).then_some(wal_pos),
                };
            }
        }
        self.shared_cache.write().prune();
    }

    fn on_transaction_rolled_back(&mut self, context: &IndexMethodContext) {
        self.abort_statement(context);
    }

    fn on_savepoint_rolled_back(&mut self, context: &IndexMethodContext) {
        // Correct but coarse: the hook carries no savepoint identity, so we
        // cannot tell whether the rollback actually reverted FTS chunk rows
        // this cursor's view depends on. Discard everything; the next FTS
        // access reloads from the (correctly reverted) backing B-tree.
        // Finer granularity needs a savepoint-creation sequence passed into
        // this hook and compared against the sequence at which this cursor
        // materialized its view.
        self.abort_statement(context);
    }

    fn close(&mut self, context: &IndexMethodContext) {
        if self.pending_docs_count != 0 || self.state.is_flushing() {
            // close() is the explicit "discard whatever is left" hook, so it
            // owns this decision: log it loudly, then normalize the cursor so
            // Drop has nothing left to enforce.
            tracing::error!(
                pending_documents = self.pending_docs_count,
                is_flushing = self.state.is_flushing(),
                "closing FTS cursor with unprepared writes; discarding them"
            );
        }
        self.release_writer_slot();
        self.pending_docs_count = 0;
        self.state = FtsState::Init;
        self.writer = None;
        self.reader = None;
        self.searcher = None;
        self.index = None;
        self.hybrid_directory = None;
        self.fts_dir_cursor = None;
        self.cached_parser = None;
        self.current_hits.clear();
        self.streaming_hits = None;
        let _ = context;
        self.shared_cache.write().prune();
    }

    /// Optimizes the FTS index by merging all segments into one.
    /// Call via `OPTIMIZE INDEX idx_name` SQL command.
    fn optimize(&mut self, context: &IndexMethodContext) -> Result<IOResult<()>> {
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        // Resume a flush this opcode started before its last IO yield.
        // Re-entry arrives with `pending_docs_count` already zeroed (the
        // flush zeroes it before its first yield), so the state machine is
        // the only reliable marker; skipping this would run the merge while
        // the old flush is mid-flight and drop the merge's output.
        if self.state.is_flushing() {
            return_if_io!(self.flush_writes_internal());
        }
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
            return_if_io!(self.open_write(context));
        }
        // The merge publishes new index state, so it needs the writer slot
        // and (under MVCC) the write lease like any document mutation.
        self.claim_writer_slot()?;
        self.ensure_writer()?;

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

        // The merged segments replace the cached reader's index snapshot.
        writer
            .commit()
            .map_err(|e| LimboError::InternalError(format!("FTS optimize commit failed: {e}")))?;
        if let Some(conn) = self.connection.as_ref().and_then(Weak::upgrade) {
            let mut cache = self.shared_cache.write();
            cache.remove_connection(&conn);
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
        context: &super::IndexMethodCostContext<'_>,
    ) -> Option<super::IndexMethodCostEstimate> {
        // FTS is typically very selective - assume ~1% of rows match
        // This is a conservative estimate; real selectivity depends on query terms
        let selectivity = 0.01;
        let estimated_matches = (context.base_table_rows * selectivity).max(1.0).ceil() as u64;
        let literal_limit = if matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_SCORE
                | FTS_PATTERN_COMBINED_ORDERED_LIMIT
                | FTS_PATTERN_COMBINED_LIMIT
                | FTS_PATTERN_MATCH_LIMIT
        ) {
            context
                .arguments
                .get(1)
                .and_then(Self::constant_integer_expression)
        } else {
            None
        };
        let estimated_rows = match literal_limit {
            Some(0) => 0,
            Some(limit) if limit > 0 => estimated_matches.min(limit as u64),
            // A negative LIMIT means "unlimited" in SQLite. Runtime
            // expressions are unknown at planning time and stay conservative.
            Some(_) | None => estimated_matches,
        };

        let globally_ranked = matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_SCORE | FTS_PATTERN_COMBINED_ORDERED_LIMIT | FTS_PATTERN_COMBINED_ORDERED
        );
        let streaming_limit = matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_COMBINED_LIMIT | FTS_PATTERN_MATCH_LIMIT
        );
        let scores_matches = !matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT
        );
        let visited_matches = if streaming_limit {
            estimated_rows
        } else {
            estimated_matches
        };
        let scored_matches = if scores_matches {
            if globally_ranked {
                estimated_matches
            } else {
                visited_matches
            }
        } else {
            0
        };

        // Cost model:
        // - Load cost: the dominant real cost. A query materializes the whole
        //   index in memory, linear in index bytes, regardless of how
        //   selective it is. Size is known exactly when a snapshot or writer
        //   is retained (which also means the load is warm); otherwise it is
        //   approximated from the base table and charged as a cold load.
        // - Base cost: logarithmic in vocabulary size (approximated by table size)
        // - Posting traversal: stops at LIMIT for unordered streaming patterns
        // - Scoring: omitted for MATCH-only patterns
        // - Top-k materialization: required only for global score ordering
        let retained_bytes = {
            let read_bytes = self.shared_cache.read().resident_cache_bytes();
            let writer_bytes = self
                .shared_writer
                .lock()
                .as_ref()
                .map_or(0, |cached| cached.directory.hot_cache.size());
            read_bytes.max(writer_bytes)
        };
        const ESTIMATED_INDEX_BYTES_PER_ROW: f64 = 64.0;
        const PAGE_BYTES: f64 = 4096.0;
        let load_cost = if retained_bytes > 0 {
            // A retained snapshot makes reuse likely; charge a token amount.
            retained_bytes as f64 / PAGE_BYTES * 0.01
        } else {
            (context.base_table_rows * ESTIMATED_INDEX_BYTES_PER_ROW) / PAGE_BYTES
        };
        let base_cost = context.base_table_rows.max(1.0).ln() * 10.0;
        let traversal_cost = visited_matches as f64 * 0.05;
        let scoring_cost = scored_matches as f64 * 0.05;
        let materialization_cost = if globally_ranked {
            estimated_rows as f64 * 0.05
        } else {
            0.0
        };

        Some(super::IndexMethodCostEstimate {
            estimated_cost: load_cost
                + base_cost
                + traversal_cost
                + scoring_cost
                + materialization_cost,
            estimated_rows,
        })
    }

    #[cfg(feature = "test_helper")]
    fn test_stats(&self) -> Result<Option<super::IndexMethodTestStats>> {
        let directory = self.hybrid_directory.as_ref().ok_or_else(|| {
            LimboError::InternalError("FTS directory not initialized".to_string())
        })?;
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("FTS index not initialized".to_string()))?;
        let segment_count = index
            .searchable_segment_metas()
            .map_err(|e| LimboError::InternalError(format!("FTS list segments: {e}")))?
            .len();

        let cache = self.shared_cache.read();
        let cached_writer_bytes = self
            .shared_writer
            .lock()
            .as_ref()
            .map_or(0, |cached| cached.directory.hot_cache.size());
        let control = self.control.as_ref().ok_or_else(|| {
            LimboError::InternalError("FTS control record not initialized".to_string())
        })?;
        Ok(Some(super::IndexMethodTestStats {
            storage_format_version: Some(FTS_CONTROL_FORMAT_VERSION),
            index_incarnation: Some(control.index_incarnation),
            manifest_generation: Some(control.manifest_generation),
            manifest_file_count: Some(control.files.len()),
            storage_file_count: directory.catalog.read().len(),
            segment_count: Some(segment_count),
            cached_connection_count: Some(cache.entries.len()),
            cached_bytes: Some(
                cache
                    .resident_cache_bytes()
                    .saturating_add(cached_writer_bytes),
            ),
            cache_admission_rejections: Some(
                self.runtime_stats
                    .cache_admission_rejections
                    .load(Ordering::Relaxed),
            ),
            cached_writer: Some(self.shared_writer.lock().is_some()),
            tantivy_writer_constructions: Some(
                self.runtime_stats
                    .tantivy_writer_constructions
                    .load(Ordering::Relaxed),
            ),
            writer_cache_lookups: Some(
                self.runtime_stats
                    .writer_cache_lookups
                    .load(Ordering::Relaxed),
            ),
            writer_cache_hits: Some(self.runtime_stats.writer_cache_hits.load(Ordering::Relaxed)),
            writer_cache_validation_failures: Some(
                self.runtime_stats
                    .writer_cache_validation_failures
                    .load(Ordering::Relaxed),
            ),
            writer_cache_rollback_discards: Some(
                self.runtime_stats
                    .writer_cache_rollback_discards
                    .load(Ordering::Relaxed),
            ),
            writer_cache_misses: Some(
                self.runtime_stats
                    .writer_cache_misses
                    .load(Ordering::Relaxed),
            ),
            read_cache_lookups: Some(
                self.runtime_stats
                    .read_cache_lookups
                    .load(Ordering::Relaxed),
            ),
            read_cache_hits: Some(self.runtime_stats.read_cache_hits.load(Ordering::Relaxed)),
            read_cache_misses: Some(self.runtime_stats.read_cache_misses.load(Ordering::Relaxed)),
            full_snapshot_loads: Some(
                self.runtime_stats
                    .full_snapshot_loads
                    .load(Ordering::Relaxed),
            ),
            manifest_validation_hits: Some(
                self.runtime_stats
                    .manifest_validation_hits
                    .load(Ordering::Relaxed),
            ),
            manifest_validation_misses: Some(
                self.runtime_stats
                    .manifest_validation_misses
                    .load(Ordering::Relaxed),
            ),
            write_lease_acquisitions: Some(
                self.runtime_stats
                    .write_lease_acquisitions
                    .load(Ordering::Relaxed),
            ),
            write_lease_rejections: Some(
                self.runtime_stats
                    .write_lease_rejections
                    .load(Ordering::Relaxed),
            ),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FtsControlRecord, FtsCursor, FtsIndexAttachment, FtsManifestFile, PendingFileMutations,
        DEFAULT_MEMORY_BUDGET_BYTES, FTS_PATTERN_MATCH, FTS_PATTERN_MATCH_LIMIT, FTS_PATTERN_SCORE,
    };
    use crate::{
        index_method::{
            IndexMethodAttachment, IndexMethodConfiguration, IndexMethodCostContext,
            IndexMethodCostEstimate,
        },
        schema::IndexColumn,
        Value,
    };
    use rustc_hash::FxHashMap;
    use std::{num::NonZeroU32, path::PathBuf};
    use tantivy::{
        merge_policy::NoMergePolicy,
        schema::{Schema, TEXT},
        Index, SegmentMeta, TantivyDocument,
    };
    use turso_parser::ast::{Expr, Literal, UnaryOperator, Variable};

    fn only_flush(mutations: &mut PendingFileMutations) -> (PathBuf, Option<Vec<u8>>) {
        let flushes = mutations.take_flushes();
        assert_eq!(flushes.len(), 1);
        flushes.into_iter().next().unwrap()
    }

    #[test]
    fn control_record_round_trips_and_detects_corruption() {
        let mut files = FxHashMap::default();
        files.insert(
            PathBuf::from("segment.idx"),
            FtsManifestFile {
                size: 42,
                chunks: 2,
            },
        );
        let control = FtsControlRecord {
            index_incarnation: 17,
            manifest_generation: 9,
            files,
        };

        let encoded = control.encode().unwrap();
        let decoded = FtsControlRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.index_incarnation, 17);
        assert_eq!(decoded.manifest_generation, 9);
        assert_eq!(decoded.files, control.files);

        let mut corrupt = encoded;
        corrupt[8] ^= 1;
        assert!(matches!(
            FtsControlRecord::decode(&corrupt),
            Err(crate::LimboError::Corrupt(message))
                if message.contains("checksum mismatch")
        ));

        let exhausted = FtsControlRecord {
            index_incarnation: 17,
            manifest_generation: u64::MAX,
            files: FxHashMap::default(),
        };
        let error = FtsControlRecord::from_catalog(
            Some(&exhausted),
            exhausted.index_incarnation,
            &FxHashMap::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("generation is exhausted"));
    }

    #[test]
    fn pending_file_mutations_keep_only_the_latest_write() {
        let path = PathBuf::from("segment.idx");
        let mut mutations = PendingFileMutations::default();
        mutations.queue_write(path.clone(), b"old".to_vec());
        mutations.queue_write(path.clone(), b"new".to_vec());

        assert_eq!(only_flush(&mut mutations), (path, Some(b"new".to_vec())));
        assert!(mutations.take_flushes().is_empty());
    }

    #[test]
    fn pending_file_mutations_preserve_write_delete_order() {
        let path = PathBuf::from("segment.idx");

        let mut deleted = PendingFileMutations::default();
        deleted.queue_write(path.clone(), b"contents".to_vec());
        deleted.queue_delete(path.clone());
        assert_eq!(only_flush(&mut deleted), (path.clone(), None));

        let mut rewritten = PendingFileMutations::default();
        rewritten.queue_delete(path.clone());
        rewritten.queue_write(path.clone(), b"replacement".to_vec());
        assert_eq!(
            only_flush(&mut rewritten),
            (path, Some(b"replacement".to_vec()))
        );
    }

    #[test]
    fn pending_file_mutations_distinguish_empty_file_from_delete() {
        let path = PathBuf::from("empty.lock");
        let mut mutations = PendingFileMutations::default();
        mutations.queue_delete(path.clone());
        mutations.queue_write(path.clone(), Vec::new());

        assert_eq!(only_flush(&mut mutations), (path, Some(Vec::new())));
    }

    #[test]
    fn indexed_text_is_not_duplicated_in_tantivy_document_store() {
        let attachment = FtsIndexAttachment::new(IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "docs_fts".to_string(),
            columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
            parameters: FxHashMap::<String, Value>::default(),
        })
        .unwrap();

        for (_, field) in attachment.text_fields {
            assert!(
                !attachment.schema.get_field_entry(field).is_stored(),
                "FTS projections come from the base table, so storing indexed text duplicates data"
            );
        }
    }

    fn estimate_cost(pattern_idx: i64, limit: Option<Expr>) -> IndexMethodCostEstimate {
        let attachment = FtsIndexAttachment::new(IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "docs_fts".to_string(),
            columns: vec![IndexColumn::new("body", 1)],
            parameters: FxHashMap::<String, Value>::default(),
        })
        .unwrap();
        let cursor = attachment.init().unwrap();
        let mut arguments = vec![Expr::Literal(Literal::String("'database'".to_string()))];
        arguments.extend(limit);

        cursor
            .estimate_cost(&IndexMethodCostContext {
                pattern_idx: pattern_idx as usize,
                base_table_rows: 100_000.0,
                arguments: &arguments,
            })
            .unwrap()
    }

    #[test]
    fn fts_cost_estimate_applies_literal_limit_to_output_rows() {
        let unlimited = estimate_cost(FTS_PATTERN_MATCH, None);
        assert_eq!(unlimited.estimated_rows, 1_000);

        let limited = estimate_cost(
            FTS_PATTERN_MATCH_LIMIT,
            Some(Expr::Literal(Literal::Numeric("10".to_string()))),
        );
        assert_eq!(limited.estimated_rows, 10);
        assert!(limited.estimated_cost < unlimited.estimated_cost);
        let ranked = estimate_cost(
            FTS_PATTERN_SCORE,
            Some(Expr::Literal(Literal::Numeric("10".to_string()))),
        );
        assert_eq!(ranked.estimated_rows, 10);
        assert!(
            ranked.estimated_cost > limited.estimated_cost,
            "global score ordering must account for scoring all matches"
        );

        let zero = estimate_cost(
            FTS_PATTERN_MATCH_LIMIT,
            Some(Expr::Literal(Literal::Numeric("0".to_string()))),
        );
        assert_eq!(zero.estimated_rows, 0);

        let negative = estimate_cost(
            FTS_PATTERN_MATCH_LIMIT,
            Some(Expr::Unary(
                UnaryOperator::Negative,
                Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
            )),
        );
        assert_eq!(negative.estimated_rows, unlimited.estimated_rows);

        let dynamic = estimate_cost(
            FTS_PATTERN_MATCH_LIMIT,
            Some(Expr::Variable(Variable::indexed(NonZeroU32::MIN))),
        );
        assert_eq!(dynamic.estimated_rows, unlimited.estimated_rows);
    }

    #[test]
    fn query_limit_is_exact_and_bounded_by_live_documents() {
        assert_eq!(super::bounded_query_limit(None, 1_500_000), 1_500_000);
        assert_eq!(super::bounded_query_limit(Some(-1), 1_500_000), 1_500_000);
        assert_eq!(super::bounded_query_limit(Some(i64::MAX), 37), 37);
        assert_eq!(super::bounded_query_limit(Some(12), 37), 12);
        assert_eq!(super::bounded_query_limit(Some(0), 37), 0);
        assert_eq!(super::bounded_query_limit(None, 0), 0);
    }

    fn segment_metas(segment_sizes: &[usize]) -> Vec<SegmentMeta> {
        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index
            .writer_with_num_threads(1, DEFAULT_MEMORY_BUDGET_BYTES)
            .unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));

        for &segment_size in segment_sizes {
            for _ in 0..segment_size {
                let mut document = TantivyDocument::default();
                document.add_text(body, "database");
                writer.add_document(document).unwrap();
            }
            writer.commit().unwrap();
        }
        index.searchable_segment_metas().unwrap()
    }

    #[test]
    fn automatic_merge_candidate_obeys_document_and_byte_budgets() {
        let segment_metas = segment_metas(&[1; 8]);
        let policy = FtsCursor::automatic_merge_policy();

        let at_document_budget =
            FtsCursor::bounded_merge_candidate(&policy, &segment_metas, 8, u64::MAX, |_| None)
                .unwrap();
        assert_eq!(at_document_budget.len(), 8);
        assert!(
            FtsCursor::bounded_merge_candidate(&policy, &segment_metas, 7, u64::MAX, |_| None)
                .is_none()
        );

        // SegmentMeta lists seven possible component files per segment.
        let source_file_count = at_document_budget.len() as u64 * 7;
        assert!(FtsCursor::bounded_merge_candidate(
            &policy,
            &segment_metas,
            u64::MAX,
            source_file_count,
            |_| Some(1),
        )
        .is_some());
        assert!(FtsCursor::bounded_merge_candidate(
            &policy,
            &segment_metas,
            u64::MAX,
            source_file_count - 1,
            |_| Some(1),
        )
        .is_none());
    }

    #[test]
    fn automatic_merge_candidate_skips_over_budget_levels() {
        let segment_metas =
            segment_metas(&[10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 1, 1, 1, 1, 1, 1]);
        let policy = FtsCursor::automatic_merge_policy();
        let candidate =
            FtsCursor::bounded_merge_candidate(&policy, &segment_metas, 8, u64::MAX, |_| None)
                .unwrap();
        let candidate_docs = candidate
            .iter()
            .map(|segment_id| {
                u64::from(
                    segment_metas
                        .iter()
                        .find(|meta| meta.id() == *segment_id)
                        .unwrap()
                        .max_doc(),
                )
            })
            .sum::<u64>();
        assert_eq!(candidate_docs, 8);
    }
}
