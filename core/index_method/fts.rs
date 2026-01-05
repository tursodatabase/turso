use crate::index_method::{
    open_index_cursor, parse_patterns, IndexMethod, IndexMethodAttachment,
    IndexMethodConfiguration, IndexMethodCursor, IndexMethodDefinition,
};
use crate::return_if_io;
use crate::schema::IndexColumn;
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::translate::collate::CollationSeq;
use crate::types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult, Text};
use crate::vdbe::Register;
use crate::{Connection, LimboError, Result, Value};
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Value as TantivySchemaValue;
use tantivy::{
    schema::{Field, Schema},
    Index, IndexSettings,
};
use tantivy::{DocAddress, HasLen, IndexReader, IndexWriter, Searcher, TantivyDocument};
use turso_parser::ast::SortOrder;
use turso_parser::ast::{self, Select};

pub const FTS_INDEX_METHOD_NAME: &str = "fts";
pub const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 512 * 1024;
/// Number of documents to batch before committing to Tantivy
pub const BATCH_COMMIT_SIZE: usize = 1000;

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

type PendingWrites = Arc<RwLock<Vec<(PathBuf, Vec<u8>)>>>;

/// CachedBTreeDirectory: In-memory Directory implementation for Tantivy.
/// All operations are synchronous and work on in-memory data only.
/// Actual BTree IO happens in the FtsCursor state machines.
#[derive(Debug, Clone)]
pub struct CachedBTreeDirectory {
    /// Files loaded into memory: path -> content
    files: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
    /// Pending writes to be flushed to BTree
    pending_writes: PendingWrites,
    /// Pending deletes to be flushed to BTree
    pending_deletes: Arc<RwLock<Vec<PathBuf>>>,
}
impl Default for CachedBTreeDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl CachedBTreeDirectory {
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            pending_writes: Arc::new(RwLock::new(Vec::new())),
            pending_deletes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create from preloaded files
    pub fn with_files(files: HashMap<PathBuf, Vec<u8>>) -> Self {
        Self {
            files: Arc::new(RwLock::new(files)),
            pending_writes: Arc::new(RwLock::new(Vec::new())),
            pending_deletes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get pending writes for flushing, deduplicated to keep only the last write for each path.
    /// This is critical because Tantivy may write the same file multiple times during indexing,
    /// and we only want to persist the final version of each file.
    pub fn take_pending_writes(&self) -> Vec<(PathBuf, Vec<u8>)> {
        let mut pending = self.pending_writes.write().unwrap();
        let all_writes = std::mem::take(&mut *pending);

        // Deduplicate: keep only the last write for each path
        // Use a HashMap to track the last write index for each path
        let mut last_write_idx: HashMap<PathBuf, usize> = HashMap::new();
        for (idx, (path, _)) in all_writes.iter().enumerate() {
            last_write_idx.insert(path.clone(), idx);
        }

        // Collect only the entries that are the last write for their path
        let deduped: Vec<(PathBuf, Vec<u8>)> = all_writes
            .into_iter()
            .enumerate()
            .filter(|(idx, (path, _))| last_write_idx.get(path) == Some(idx))
            .map(|(_, entry)| entry)
            .collect();

        tracing::debug!(
            "FTS take_pending_writes: {} entries after deduplication",
            deduped.len()
        );
        deduped
    }

    /// Get pending deletes for flushing
    pub fn take_pending_deletes(&self) -> Vec<PathBuf> {
        let mut pending = self.pending_deletes.write().unwrap();
        std::mem::take(&mut *pending)
    }

    /// Check if there are pending changes
    pub fn has_pending_changes(&self) -> bool {
        let writes = self.pending_writes.read().unwrap();
        let deletes = self.pending_deletes.read().unwrap();
        !writes.is_empty() || !deletes.is_empty()
    }
}

/// In-memory file handle for CachedBTreeDirectory
struct CachedFileHandle {
    data: Vec<u8>,
}

impl std::fmt::Debug for CachedFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFileHandle")
            .field("len", &self.data.len())
            .finish()
    }
}

impl HasLen for CachedFileHandle {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl FileHandle for CachedFileHandle {
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

/// In-memory writer for CachedBTreeDirectory
struct CachedWriter {
    path: PathBuf,
    buffer: Vec<u8>,
    directory: CachedBTreeDirectory,
}

impl Write for CachedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for CachedWriter {
    fn drop(&mut self) {
        // Commit the write to the directory
        let data = std::mem::take(&mut self.buffer);
        if !data.is_empty() {
            // Update in-memory files
            let mut files = self.directory.files.write().unwrap();
            files.insert(self.path.clone(), data.clone());
            drop(files);
            // Queue for BTree flush
            let mut pending = self.directory.pending_writes.write().unwrap();
            pending.push((self.path.clone(), data));
        }
    }
}

impl TerminatingWrite for CachedWriter {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        // Commit the write to the directory
        let data = std::mem::take(&mut self.buffer);
        // Update in-memory files
        let mut files = self.directory.files.write().unwrap();
        files.insert(self.path.clone(), data.clone());
        drop(files);
        // Queue for BTree flush
        let mut pending = self.directory.pending_writes.write().unwrap();
        pending.push((self.path.clone(), data));
        Ok(())
    }
}

impl Directory for CachedBTreeDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let files = self.files.read().unwrap();
        let data = files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?
            .clone();
        Ok(Arc::new(CachedFileHandle { data }))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        let files = self.files.read().unwrap();
        Ok(files.contains_key(path))
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        // Remove from in-memory files
        let mut files = self.files.write().unwrap();
        files.remove(path);
        drop(files);
        // Queue for BTree deletion
        let mut pending = self.pending_deletes.write().unwrap();
        pending.push(path.to_path_buf());
        Ok(())
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite>>, OpenWriteError> {
        // First delete existing file
        let _ = self.delete(path);
        let writer: Box<dyn TerminatingWrite> = Box::new(CachedWriter {
            path: path.to_path_buf(),
            buffer: Vec::new(),
            directory: self.clone(),
        });
        Ok(BufWriter::new(writer))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let files = self.files.read().unwrap();
        files
            .get(path)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        // Update in-memory files
        let mut files = self.files.write().unwrap();
        files.insert(path.to_path_buf(), data.to_vec());
        drop(files);
        // Queue for BTree flush
        let mut pending = self.pending_writes.write().unwrap();
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

#[derive(Debug)]
/// FtsIndexMethod: Factory for creating FTS attachments
pub struct FtsIndexMethod;

impl IndexMethod for FtsIndexMethod {
    fn attach(&self, cfg: &IndexMethodConfiguration) -> Result<Arc<dyn IndexMethodAttachment>> {
        let attachment = FtsIndexAttachment::new(cfg.clone())?;
        Ok(Arc::new(attachment))
    }
}

#[derive(Debug)]
/// IndexAttachment holds configuration, creates cursors
pub struct FtsIndexAttachment {
    /// Configuration for this index
    cfg: IndexMethodConfiguration,
    /// Underlying Tantivy schema
    schema: Schema,
    /// Field for rowid
    rowid_field: Field,
    /// Text fields mapping
    text_fields: Vec<(IndexColumn, Field)>,
    /// Parsed query patterns
    patterns: Vec<Select>,
}

impl FtsIndexAttachment {
    pub fn new(cfg: IndexMethodConfiguration) -> Result<Self> {
        // Build Tantivy schema (no Directory or Index creation yet)
        let mut schema_builder = Schema::builder();

        let rowid_field = schema_builder
            .add_i64_field("rowid", tantivy::schema::INDEXED | tantivy::schema::STORED);

        let mut text_fields = Vec::with_capacity(cfg.columns.len());
        for col in &cfg.columns {
            let opts = tantivy::schema::TextOptions::default()
                .set_indexing_options(
                    tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer("default")
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
    /// Loading files from BTree into memory
    LoadingFiles {
        files: HashMap<PathBuf, Vec<u8>>,
        current_path: Option<String>,
        current_chunks: Vec<(i64, Vec<u8>)>,
    },
    /// Creating/opening Tantivy index
    CreatingIndex,
    /// Ready for operations
    Ready,
    /// Deleting old chunks for a path before writing new ones
    DeletingOldChunks {
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
    /// Deleting a single chunk during old chunk cleanup - waiting for delete to complete
    DeletingOldChunk {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Performing the actual delete operation (cursor positioned, just waiting for IO)
    PerformingDelete {
        writes: Vec<(PathBuf, Vec<u8>)>,
        write_idx: usize,
        path_str: String,
    },
    /// Advancing cursor after delete to get the next record
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

/// Cursor for executing FTS queries
pub struct FtsCursor {
    schema: Schema,
    rowid_field: Field,
    text_fields: Vec<(IndexColumn, Field)>,

    // Storage table name for BTree directory
    dir_table_name: String,

    // Connection for blocking IO in Drop
    connection: Option<Arc<Connection>>,

    // BTree cursor for direct access (no SQL execution)
    fts_dir_cursor: Option<BTreeCursor>,

    // Cached directory for Tantivy (in-memory)
    cached_directory: Option<CachedBTreeDirectory>,

    // Tantivy components
    index: Option<Index>,
    reader: Option<IndexReader>,
    writer: Option<IndexWriter>,
    searcher: Option<Searcher>,

    // State machine for async operations
    state: FtsState,

    // Batching: count of uncommitted documents in Tantivy
    pending_docs_count: usize,

    // Query state
    current_hits: Vec<(f32, DocAddress, i64)>,
    hit_pos: usize,
    /// Which pattern is being used (0=fts_score, 1=fts_match)
    current_pattern: i64,
}

impl FtsCursor {
    /// Maximum number of results when no LIMIT clause is specified.
    /// Used to prevent excessive memory usage, users needing more results should always specify
    /// LIMIT.
    const MAX_NO_LIMIT_RESULT: usize = 10_000_000;

    pub fn new(
        cfg: &IndexMethodConfiguration,
        schema: Schema,
        rowid_field: Field,
        text_fields: Vec<(IndexColumn, Field)>,
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
            connection: None,
            fts_dir_cursor: None,
            cached_directory: None,
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
        let cursor = open_index_cursor(
            conn,
            &self.dir_table_name,
            &index_name,
            // path (TEXT), chunk_no (INTEGER), bytes (BLOB) - all 3 columns in the key
            vec![key_info(), key_info(), key_info()],
        )?;
        self.fts_dir_cursor = Some(cursor);
        Ok(())
    }

    /// Finalize file loading: combine chunks into complete file
    fn finalize_current_file(
        files: &mut HashMap<PathBuf, Vec<u8>>,
        current_path: &Option<String>,
        current_chunks: &mut Vec<(i64, Vec<u8>)>,
    ) {
        if let Some(path) = current_path {
            if !current_chunks.is_empty() {
                // Sort by chunk_no
                current_chunks.sort_by_key(|(chunk_no, _)| *chunk_no);

                // CRITICAL FIX: Deduplicate chunks with the same chunk_no.
                // Due to the async state machine, the same chunk may be inserted
                // multiple times into the BTree. Keep only the LAST chunk for each chunk_no.
                let mut deduped_chunks: Vec<(i64, Vec<u8>)> = Vec::new();
                for (chunk_no, bytes) in current_chunks.drain(..) {
                    if let Some(last) = deduped_chunks.last_mut() {
                        if last.0 == chunk_no {
                            // Replace with the newer one (last one wins)
                            *last = (chunk_no, bytes);
                        } else {
                            deduped_chunks.push((chunk_no, bytes));
                        }
                    } else {
                        deduped_chunks.push((chunk_no, bytes));
                    }
                }

                // Verify chunk sequence integrity
                for (i, (chunk_no, _)) in deduped_chunks.iter().enumerate() {
                    if *chunk_no != i as i64 {
                        tracing::error!(
                            "FTS CHUNK GAP DETECTED: path={}, expected chunk_no={}, got={}",
                            path,
                            i,
                            chunk_no
                        );
                    }
                }

                let data: Vec<u8> = deduped_chunks
                    .iter()
                    .flat_map(|(_, bytes)| bytes.clone())
                    .collect();
                tracing::debug!(
                    "FTS finalize_current_file: path={}, num_chunks={}, total_bytes={}",
                    path,
                    deduped_chunks.len(),
                    data.len()
                );
                files.insert(PathBuf::from(path), data);
            }
        }
    }

    /// Create Tantivy index from cached directory
    fn create_index_from_cache(&mut self) -> Result<()> {
        let cached_dir = self
            .cached_directory
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("cached directory not initialized".into()))?
            .clone();

        let index_exists = cached_dir
            .exists(Path::new(TANTIVY_META_FILE))
            .unwrap_or(false);

        let index = if index_exists {
            Index::open(cached_dir).map_err(|e| LimboError::InternalError(e.to_string()))?
        } else {
            Index::create(cached_dir, self.schema.clone(), IndexSettings::default())
                .map_err(|e| LimboError::InternalError(e.to_string()))?
        };

        self.index = Some(index);
        Ok(())
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
                        // Done with writes
                        self.state = FtsState::Ready;
                        return Ok(IOResult::Done(()));
                    }

                    // If starting a new file (chunk_idx == 0), first delete old chunks
                    if *chunk_idx == 0 {
                        let path_str = writes[*write_idx].0.to_string_lossy().to_string();
                        self.state = FtsState::DeletingOldChunks {
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
                FtsState::DeletingOldChunks {
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
                            self.state = FtsState::DeletingOldChunk {
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

                    let has_next = return_if_io!(cursor.next());

                    if has_next {
                        // Now positioned on a record, check if it matches our path
                        self.state = FtsState::DeletingOldChunk {
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
                FtsState::DeletingOldChunk {
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
                        // Transition to PerformingDelete to actually do the delete
                        self.state = FtsState::PerformingDelete {
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
                FtsState::PerformingDelete {
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
                    let has_next = return_if_io!(cursor.next());

                    if has_next {
                        // Check the next record in DeletingOldChunk state
                        self.state = FtsState::DeletingOldChunk {
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

    /// Commit pending documents to Tantivy and flush to BTree
    pub fn commit_and_flush(&mut self) -> Result<IOResult<()>> {
        // Handle flush state machine if already in progress
        match &self.state {
            FtsState::FlushingWrites { .. }
            | FtsState::DeletingOldChunks { .. }
            | FtsState::AdvancingAfterSeek { .. }
            | FtsState::DeletingOldChunk { .. }
            | FtsState::PerformingDelete { .. }
            | FtsState::AdvancingAfterDelete { .. }
            | FtsState::SeekingWrite { .. }
            | FtsState::InsertingWrite { .. } => {
                return self.flush_writes_internal();
            }
            _ => {}
        }

        if self.pending_docs_count == 0 {
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
        }
        if let Some(ref reader) = self.reader {
            reader
                .reload()
                .map_err(|e| LimboError::InternalError(format!("FTS reader reload error: {e}")))?;
            self.searcher = Some(reader.searcher());
        }

        self.pending_docs_count = 0;

        // Flush pending writes to BTree via async state machine
        if let Some(ref dir) = self.cached_directory {
            let writes = dir.take_pending_writes();
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
        }

        Ok(IOResult::Done(()))
    }
}

impl Drop for FtsCursor {
    fn drop(&mut self) {
        // Skip cleanup if we're already panicking
        if std::thread::panicking() {
            return;
        }

        // Only flush if we have pending documents
        if self.pending_docs_count == 0 {
            return;
        }

        // Commit any pending writes to Tantivy
        if let Some(ref mut writer) = self.writer {
            if let Err(e) = writer.commit() {
                tracing::error!("FTS Drop: failed to commit writer: {}", e);
                return;
            }
        }

        // Flush pending writes to BTree (blocking)
        // Clone pager Arc before we start to avoid borrow conflicts
        let pager = match &self.connection {
            Some(conn) => conn.pager.load().clone(),
            None => {
                tracing::warn!("FTS Drop: no connection for flush");
                return;
            }
        };

        let writes = match &self.cached_directory {
            Some(dir) => dir.take_pending_writes(),
            None => return,
        };

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
    fn create(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        // Ensure storage table exists (still uses SQL for DDL)
        initialize_btree_storage_table(conn, &self.dir_table_name)?;
        Ok(IOResult::Done(()))
    }

    fn destroy(&mut self, _conn: &Arc<Connection>) -> Result<IOResult<()>> {
        // Commit any pending Tantivy writes first
        if let Some(ref mut writer) = self.writer {
            let _ = writer.commit();
        }

        // Flush pending changes to BTree
        if let Some(ref dir) = self.cached_directory {
            if dir.has_pending_changes() {
                // Start flush state machine
                let writes = dir.take_pending_writes();
                let deletes = dir.take_pending_deletes();

                if !writes.is_empty() {
                    self.state = FtsState::FlushingWrites {
                        writes,
                        write_idx: 0,
                        chunk_idx: 0,
                    };
                    return self.flush_writes_internal();
                }
                if !deletes.is_empty() {
                    self.state = FtsState::FlushingDeletes {
                        deletes,
                        delete_idx: 0,
                    };
                    return self.flush_deletes_internal();
                }
            }
        }

        // Drop all components
        self.searcher = None;
        self.reader = None;
        self.writer = None;
        self.index = None;
        self.cached_directory = None;
        self.fts_dir_cursor = None;
        self.state = FtsState::Init;
        Ok(IOResult::Done(()))
    }

    fn open_read(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        loop {
            match &mut self.state {
                FtsState::Init => {
                    // Store connection for blocking flush in Drop
                    self.connection = Some(conn.clone());
                    // Ensure storage table exists
                    initialize_btree_storage_table(conn, &self.dir_table_name)?;
                    // Open BTree cursor
                    self.open_cursor(conn)?;
                    self.state = FtsState::Rewinding;
                }
                FtsState::Rewinding => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    return_if_io!(cursor.rewind());
                    self.state = FtsState::LoadingFiles {
                        files: HashMap::new(),
                        current_path: None,
                        current_chunks: Vec::new(),
                    };
                }
                FtsState::LoadingFiles {
                    files,
                    current_path,
                    current_chunks,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;

                    if !cursor.has_record() {
                        // Done loading - finalize last file
                        Self::finalize_current_file(files, current_path, current_chunks);
                        // Create cached directory with loaded files
                        let loaded_files = std::mem::take(files);
                        self.cached_directory =
                            Some(CachedBTreeDirectory::with_files(loaded_files));
                        self.state = FtsState::CreatingIndex;
                        continue;
                    }

                    // Read current record
                    let record = return_if_io!(cursor.record());
                    if let Some(record) = record {
                        // Record format: [path, chunk_no, bytes]
                        let path = record.get_value_opt(0).and_then(|v| match v {
                            crate::types::ValueRef::Text(t) => Some(t.value.to_string()),
                            _ => None,
                        });
                        let chunk_no = record.get_value_opt(1).and_then(|v| match v {
                            crate::types::ValueRef::Integer(i) => Some(i),
                            _ => None,
                        });
                        let bytes = record.get_value_opt(2).and_then(|v| match v {
                            crate::types::ValueRef::Blob(b) => Some(b.to_vec()),
                            _ => None,
                        });

                        if let (Some(path_str), Some(chunk_no), Some(bytes)) =
                            (path, chunk_no, bytes)
                        {
                            tracing::debug!(
                                "FTS load: record path={}, chunk_no={}, bytes_len={}",
                                path_str,
                                chunk_no,
                                bytes.len()
                            );
                            // Check if we've moved to a new file
                            if current_path.as_ref() != Some(&path_str) {
                                // Finalize previous file
                                Self::finalize_current_file(files, current_path, current_chunks);
                                *current_path = Some(path_str.clone());
                            }
                            current_chunks.push((chunk_no, bytes));
                        } else {
                            tracing::warn!("FTS load: skipping malformed record");
                        }
                    }

                    // Move to next record
                    return_if_io!(cursor.next());
                }
                FtsState::CreatingIndex => {
                    // Log loaded files for debugging
                    if let Some(ref dir) = self.cached_directory {
                        let files = dir.files.read().unwrap();
                        tracing::debug!(
                            "FTS CreatingIndex: loaded {} files from BTree",
                            files.len()
                        );
                    }

                    // Create Tantivy index from cached directory
                    self.create_index_from_cache()?;

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

    fn open_write(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        // Ensure connection is stored for blocking flush in Drop
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

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
        // Handle flush state machine if in progress - loop until flush completes
        // This is critical: we must NOT return Done when flush completes,
        // otherwise the VDBE thinks the insert is done but we never added the document!
        loop {
            match &self.state {
                FtsState::FlushingWrites { .. }
                | FtsState::DeletingOldChunks { .. }
                | FtsState::AdvancingAfterSeek { .. }
                | FtsState::DeletingOldChunk { .. }
                | FtsState::PerformingDelete { .. }
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
        // This dramatically improves bulk insert performance
        if self.pending_docs_count >= BATCH_COMMIT_SIZE {
            return self.commit_and_flush();
        }

        Ok(IOResult::Done(()))
    }

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

        Ok(IOResult::Done(()))
    }

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

        let parser = tantivy::query::QueryParser::for_index(index, default_fields);
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

    fn query_next(&mut self) -> Result<IOResult<bool>> {
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(false));
        }
        self.hit_pos += 1;
        Ok(IOResult::Done(self.hit_pos < self.current_hits.len()))
    }

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

    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(None));
        }
        let (_, _, rowid) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Some(rowid)))
    }
}
