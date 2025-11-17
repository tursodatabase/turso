use crate::index_method::{
    parse_patterns, IndexMethod, IndexMethodAttachment, IndexMethodConfiguration,
    IndexMethodCursor, IndexMethodDefinition,
};
use crate::schema::IndexColumn;
use crate::types::IOResult;
use crate::vdbe::Register;
use crate::{Connection, LimboError, Result, StepResult, Value};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
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
use turso_parser::ast::Select;

pub const FTS_INDEX_METHOD_NAME: &str = "fts";
pub const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 512 * 1024;

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

        let mut text_fields = Vec::new();
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

        // Build query pattern for FTS
        // Pattern: SELECT fts_score(col1, col2, ..., 'search query') as score FROM table ORDER BY score DESC LIMIT ?
        let cols = cfg
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let query_pattern = format!(
            "SELECT fts_score({}, ?) as score FROM {} ORDER BY score DESC LIMIT ?",
            cols, cfg.table_name
        );
        let patterns = parse_patterns(&[&query_pattern])?;
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
            self.cfg.clone(),
            self.schema.clone(),
            self.rowid_field,
            self.text_fields.clone(),
        )))
    }
}

#[derive(Clone)]
/// BTreeDirectory : Tantivy Directory impl backed by btree table storage
pub struct BTreeDirectory {
    /// Database table name for storage
    table_name: String,
    /// Chunk size for storage
    chunk_size: usize,
    /// Database connection
    connection: Arc<Connection>,
}

impl std::fmt::Debug for BTreeDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreeDirectory")
            .field("table_name", &self.table_name)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl BTreeDirectory {
    pub fn new(connection: Arc<Connection>, table_name: String, chunk_size: usize) -> Result<Self> {
        initialize_btree_storage_table(&connection, &table_name)?;
        Ok(Self {
            table_name,
            chunk_size,
            connection,
        })
    }

    /// Get total length of file by summing all chunk sizes
    fn fetch_file_len(&self, path: &str) -> std::result::Result<usize, OpenReadError> {
        let sql = format!(
            "SELECT SUM(LENGTH(bytes)) FROM {} WHERE path = '{}'",
            self.table_name,
            path.replace('\'', "''")
        );
        let mut stmt = self
            .connection
            .prepare(&sql)
            .map_err(|e| OpenReadError::IoError {
                io_error: std::io::Error::other(e.to_string()).into(),
                filepath: path.into(),
            })?;

        stmt.program.needs_stmt_subtransactions = false;
        self.connection.start_nested();

        let result = loop {
            match stmt.step() {
                Ok(StepResult::Row) => {
                    let val = if let Some(row) = stmt.row() {
                        let values: Vec<_> = row.get_values().cloned().collect();
                        if let Some(Value::Integer(n)) = values.first() {
                            *n as usize
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    break Ok(val);
                }
                Ok(StepResult::Done) => break Ok(0),
                Ok(StepResult::IO) => {
                    let _ = self.connection.pager.load().io.step();
                    continue;
                }
                Ok(StepResult::Busy) => continue,
                Ok(StepResult::Interrupt) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other("interrupted").into(),
                        filepath: path.into(),
                    });
                }
                Err(e) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other(e.to_string()).into(),
                        filepath: path.into(),
                    });
                }
            }
        };

        self.connection.end_nested();
        result
    }

    /// Check if any row exists for path
    fn exists_row_for_path(&self, path: &str) -> std::result::Result<bool, OpenReadError> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE path = '{}' LIMIT 1",
            self.table_name,
            path.replace('\'', "''")
        );
        let mut stmt = self
            .connection
            .prepare(&sql)
            .map_err(|e| OpenReadError::IoError {
                io_error: std::io::Error::other(e.to_string()).into(),
                filepath: path.into(),
            })?;

        stmt.program.needs_stmt_subtransactions = false;
        self.connection.start_nested();

        let result = loop {
            match stmt.step() {
                Ok(StepResult::Row) => break Ok(true),
                Ok(StepResult::Done) => break Ok(false),
                Ok(StepResult::IO) => {
                    let _ = self.connection.pager.load().io.step();
                    continue;
                }
                Ok(StepResult::Busy) => continue,
                Ok(StepResult::Interrupt) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other("interrupted").into(),
                        filepath: path.into(),
                    });
                }
                Err(e) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other(e.to_string()).into(),
                        filepath: path.into(),
                    });
                }
            }
        };

        self.connection.end_nested();
        result
    }

    /// Delete all chunks for a path
    fn delete_all_chunks_for_path(&self, path: &str) -> std::result::Result<(), DeleteError> {
        let sql = format!(
            "DELETE FROM {} WHERE path = '{}'",
            self.table_name,
            path.replace('\'', "''")
        );
        let mut stmt = self
            .connection
            .prepare(&sql)
            .map_err(|e| DeleteError::IoError {
                io_error: std::io::Error::other(e.to_string()).into(),
                filepath: path.into(),
            })?;

        stmt.program.needs_stmt_subtransactions = false;
        self.connection.start_nested();
        let res = stmt.run_ignore_rows();
        self.connection.end_nested();

        res.map_err(|e| DeleteError::IoError {
            io_error: std::io::Error::other(e.to_string()).into(),
            filepath: path.into(),
        })
    }

    /// Read all bytes for a path (concatenating chunks in order)
    fn read_all_bytes_for_path(&self, path: &str) -> std::result::Result<Vec<u8>, OpenReadError> {
        let sql = format!(
            "SELECT bytes FROM {} WHERE path = '{}' ORDER BY chunk_no ASC",
            self.table_name,
            path.replace('\'', "''")
        );
        let mut stmt = self
            .connection
            .prepare(&sql)
            .map_err(|e| OpenReadError::IoError {
                io_error: std::io::Error::other(e.to_string()).into(),
                filepath: path.into(),
            })?;

        stmt.program.needs_stmt_subtransactions = false;
        self.connection.start_nested();

        let mut data = Vec::new();
        let result = loop {
            match stmt.step() {
                Ok(StepResult::Row) => {
                    if let Some(row) = stmt.row() {
                        let values: Vec<_> = row.get_values().cloned().collect();
                        if let Some(Value::Blob(bytes)) = values.first() {
                            data.extend_from_slice(bytes);
                        }
                    }
                }
                Ok(StepResult::Done) => break Ok(data),
                Ok(StepResult::IO) => {
                    let _ = self.connection.pager.load().io.step();
                    continue;
                }
                Ok(StepResult::Busy) => continue,
                Ok(StepResult::Interrupt) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other("interrupted").into(),
                        filepath: path.into(),
                    });
                }
                Err(e) => {
                    break Err(OpenReadError::IoError {
                        io_error: std::io::Error::other(e.to_string()).into(),
                        filepath: path.into(),
                    });
                }
            }
        };

        self.connection.end_nested();
        result
    }

    /// Atomically overwrite file with single chunk (delete + insert)
    fn atomic_overwrite_single_chunk(&self, path: &str, data: &[u8]) -> std::io::Result<()> {
        // Delete existing
        let _ = self.delete_all_chunks_for_path(path);

        // Insert new - use hex encoding for blob data
        let hex_data: String = data.iter().map(|b| format!("{:02x}", b)).collect();
        let insert_sql = format!(
            "INSERT INTO {} (path, chunk_no, bytes) VALUES ('{}', 0, X'{}')",
            self.table_name,
            path.replace('\'', "''"),
            hex_data
        );
        let mut insert_stmt = self
            .connection
            .prepare(&insert_sql)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        insert_stmt.program.needs_stmt_subtransactions = false;
        self.connection.start_nested();
        let res = insert_stmt.run_ignore_rows();
        self.connection.end_nested();

        res.map_err(|e| std::io::Error::other(e.to_string()))
    }
}

fn initialize_btree_storage_table(conn: &Arc<Connection>, table_name: &str) -> Result<()> {
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (
            path TEXT NOT NULL,
            chunk_no INTEGER NOT NULL,
            bytes BLOB NOT NULL
        )",
    );
    let create_index_sql = format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_key
         ON {table_name}(path, chunk_no ASC)",
    );

    // Execute nested statements without subtransactions to avoid DatabaseBusy
    // (we're already inside a transaction from the parent CREATE INDEX statement)
    {
        let mut stmt = conn.prepare(&create_table_sql)?;
        stmt.program.needs_stmt_subtransactions = false;
        conn.start_nested();
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }
    {
        let mut stmt = conn.prepare(&create_index_sql)?;
        stmt.program.needs_stmt_subtransactions = false;
        conn.start_nested();
        let res = stmt.run_ignore_rows();
        conn.end_nested();
        res?;
    }

    Ok(())
}

/// FileHandle impl for reading from btree storage
struct BTreeFileHandle {
    /// Underlying database connection
    conn: Arc<Connection>,
    /// Name of table for storage
    table_name: String,
    /// Tantivy path being read
    path: String,
    /// Total length of file in bytes
    len: usize,
    /// Size of chunks in bytes
    chunk_size: usize,
}

impl std::fmt::Debug for BTreeFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreeFileHandle")
            .field("table_name", &self.table_name)
            .field("path", &self.path)
            .field("len", &self.len)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl HasLen for BTreeFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl FileHandle for BTreeFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if range.end > self.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "range exceeds file length",
            ));
        }
        if range.start >= range.end {
            return Ok(OwnedBytes::new(Vec::new()));
        }

        // Calculate which chunks we need based on byte positions
        // chunk_no = byte_offset / chunk_size
        let first_chunk = (range.start / self.chunk_size) as i64;
        let last_chunk = ((range.end - 1) / self.chunk_size) as i64;

        let sql = format!(
            "SELECT chunk_no, bytes FROM {} WHERE path = '{}' AND chunk_no >= {} AND chunk_no <= {} ORDER BY chunk_no ASC",
            self.table_name,
            self.path.replace('\'', "''"),
            first_chunk,
            last_chunk
        );
        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        stmt.program.needs_stmt_subtransactions = false;
        self.conn.start_nested();

        let mut chunks: Vec<(i64, Vec<u8>)> = Vec::new();
        let loop_result: std::io::Result<()> = loop {
            match stmt.step() {
                Ok(StepResult::Row) => {
                    if let Some(row) = stmt.row() {
                        let values: Vec<_> = row.get_values().cloned().collect();
                        if values.len() >= 2 {
                            if let (Some(Value::Integer(chunk_no)), Some(Value::Blob(bytes))) =
                                (values.first(), values.get(1))
                            {
                                chunks.push((*chunk_no, bytes.clone()));
                            }
                        }
                    }
                }
                Ok(StepResult::Done) => break Ok(()),
                Ok(StepResult::IO) => {
                    let _ = self.conn.pager.load().io.step();
                    continue;
                }
                Ok(StepResult::Busy) => continue,
                Ok(StepResult::Interrupt) => {
                    break Err(std::io::Error::other("interrupted"));
                }
                Err(e) => break Err(std::io::Error::other(e.to_string())),
            }
        };

        self.conn.end_nested();
        loop_result?;

        // Concatenate chunks
        let mut buf = Vec::new();
        for (_, bytes) in &chunks {
            buf.extend_from_slice(bytes);
        }

        // Calculate offset within the concatenated chunks
        let offset_into_first = range.start - (first_chunk as usize * self.chunk_size);
        let needed = range.end - range.start;

        if offset_into_first + needed > buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "insufficient data: needed {} bytes at offset {}, but only {} bytes available (from {} chunks)",
                    needed, offset_into_first, buf.len(), chunks.len()
                ),
            ));
        }

        let slice = &buf[offset_into_first..offset_into_first + needed];
        Ok(OwnedBytes::new(slice.to_vec()))
    }
}

/// BTreeWrite - Write impl for writing chunks to btree storage
/// Uses position-based chunk numbers: chunk_no = byte_offset / chunk_size
struct BTreeWrite {
    /// Underlying database connection
    conn: Arc<Connection>,
    /// Table name for storage
    table_name: String,
    /// Path being written
    path: String,
    /// Chunk size for storage (DEFAULT 512KB)
    chunk_size: usize,
    /// Buffer for current chunk
    buffer: Vec<u8>,
    /// Current chunk number (based on byte position)
    current_chunk: i64,
    /// Byte offset within current chunk
    offset_in_chunk: usize,
}

impl BTreeWrite {
    fn new(conn: Arc<Connection>, table_name: String, path: String, chunk_size: usize) -> Self {
        Self {
            conn,
            table_name,
            path,
            chunk_size,
            buffer: Vec::with_capacity(chunk_size),
            current_chunk: 0,
            offset_in_chunk: 0,
        }
    }

    fn write_chunk(&mut self, chunk_no: i64, data: &[u8]) -> std::io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Use hex encoding for blob data in SQL
        let hex_data = bytes_to_hex(data);
        let sql = format!(
            "INSERT OR REPLACE INTO {} (path, chunk_no, bytes) VALUES ('{}', {}, X'{}')",
            self.table_name,
            self.path.replace('\'', "''"),
            chunk_no,
            hex_data
        );
        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Execute within nested context to avoid transaction conflicts
        stmt.program.needs_stmt_subtransactions = false;
        self.conn.start_nested();
        let res = stmt.run_ignore_rows();
        self.conn.end_nested();
        res.map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(())
    }

    fn flush_current_chunk(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk_data = std::mem::take(&mut self.buffer);
        self.write_chunk(self.current_chunk, &chunk_data)?;
        Ok(())
    }
}

/// Convert bytes to hex string for SQL X'' literals
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Write for BTreeWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = buf;
        while !remaining.is_empty() {
            let space_in_chunk = self.chunk_size - self.buffer.len();
            let to_write = remaining.len().min(space_in_chunk);
            self.buffer.extend_from_slice(&remaining[..to_write]);
            remaining = &remaining[to_write..];

            // If chunk is full, write it and move to next chunk
            if self.buffer.len() >= self.chunk_size {
                // Clone the buffer to avoid borrow conflict
                let chunk_data = std::mem::take(&mut self.buffer);
                self.write_chunk(self.current_chunk, &chunk_data)?;
                self.current_chunk += 1;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Don't flush partial chunks on regular flush - only on terminate
        // This ensures chunks are written with consistent sizes except for the last one
        Ok(())
    }
}

impl Drop for BTreeWrite {
    fn drop(&mut self) {
        // Flush any remaining data in buffer
        let _ = self.flush_current_chunk();
    }
}

impl TerminatingWrite for BTreeWrite {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        // Flush any remaining data (the final partial chunk)
        self.flush_current_chunk()
    }
}

/// Directory trait implementation for BTreeDirectory
impl Directory for BTreeDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let path_str = path.to_string_lossy().to_string();
        // Check if file exists first
        if !self.exists_row_for_path(&path_str)? {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        let len = self.fetch_file_len(&path_str)?;
        Ok(Arc::new(BTreeFileHandle {
            conn: self.connection.clone(),
            table_name: self.table_name.clone(),
            path: path_str,
            len,
            chunk_size: self.chunk_size,
        }))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        let path_str = path.to_string_lossy().to_string();
        self.exists_row_for_path(&path_str)
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        let path_str = path.to_string_lossy().to_string();
        self.delete_all_chunks_for_path(&path_str)
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite>>, OpenWriteError> {
        let path_str = path.to_string_lossy().to_string();
        // Delete existing file first
        let _ = self.delete_all_chunks_for_path(&path_str);
        let writer: Box<dyn TerminatingWrite> = Box::new(BTreeWrite::new(
            self.connection.clone(),
            self.table_name.clone(),
            path_str,
            self.chunk_size,
        ));
        Ok(BufWriter::new(writer))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let path_str = path.to_string_lossy().to_string();
        // Check if file exists first - atomic_read must return FileDoesNotExist
        // error when file is absent (not empty bytes)
        if !self.exists_row_for_path(&path_str)? {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        self.read_all_bytes_for_path(&path_str)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let path_str = path.to_string_lossy().to_string();
        self.atomic_overwrite_single_chunk(&path_str, data)
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, _cb: WatchCallback) -> std::result::Result<WatchHandle, tantivy::TantivyError> {
        // Watch not needed for our use case
        Ok(WatchHandle::empty())
    }
}

/// Cursor for executing FTS queries
pub struct FtsCursor {
    cfg: IndexMethodConfiguration,
    schema: Schema,
    rowid_field: Field,
    text_fields: Vec<(IndexColumn, Field)>,

    // Lazy-initialized components
    directory: Option<Arc<BTreeDirectory>>,
    index: Option<Index>,
    reader: Option<IndexReader>,
    writer: Option<IndexWriter>,
    searcher: Option<Searcher>,

    // Query state
    current_hits: Vec<(f32, DocAddress, i64)>,
    hit_pos: usize,
}

impl FtsCursor {
    pub fn new(
        cfg: IndexMethodConfiguration,
        schema: Schema,
        rowid_field: Field,
        text_fields: Vec<(IndexColumn, Field)>,
    ) -> Self {
        Self {
            cfg,
            schema,
            rowid_field,
            text_fields,
            directory: None,
            index: None,
            reader: None,
            writer: None,
            searcher: None,
            current_hits: Vec::new(),
            hit_pos: 0,
        }
    }

    fn ensure_index_initialized(&mut self, conn: &Arc<Connection>) -> Result<()> {
        if self.index.is_some() {
            return Ok(());
        }

        let dir_table_name = format!("fts_dir_{}", self.cfg.index_name);
        let dir = BTreeDirectory::new(conn.clone(), dir_table_name, DEFAULT_CHUNK_SIZE)?;

        let index_exists = dir.exists(Path::new("meta.json")).unwrap_or(false);

        // Clone for storage, original for Index
        let dir_for_storage = Arc::new(dir.clone());

        let index = if index_exists {
            Index::open(dir).map_err(|e| LimboError::InternalError(e.to_string()))?
        } else {
            Index::create(dir, self.schema.clone(), IndexSettings::default())
                .map_err(|e| LimboError::InternalError(e.to_string()))?
        };

        self.directory = Some(dir_for_storage);
        self.index = Some(index);
        Ok(())
    }
}

impl Drop for FtsCursor {
    fn drop(&mut self) {
        // Commit any pending writes before dropping
        if let Some(ref mut writer) = self.writer {
            let _ = writer.commit();
        }
    }
}

impl IndexMethodCursor for FtsCursor {
    fn create(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        self.ensure_index_initialized(conn)?;
        Ok(IOResult::Done(()))
    }

    fn destroy(&mut self, _conn: &Arc<Connection>) -> Result<IOResult<()>> {
        // Drop all Tantivy components
        self.searcher = None;
        self.reader = None;
        self.writer = None;
        self.index = None;
        self.directory = None;
        Ok(IOResult::Done(()))
    }

    fn open_read(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        self.ensure_index_initialized(conn)?;

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
        Ok(IOResult::Done(()))
    }

    fn open_write(&mut self, conn: &Arc<Connection>) -> Result<IOResult<()>> {
        self.ensure_index_initialized(conn)?;

        if let Some(ref index) = self.index {
            // Use single-threaded mode to avoid concurrent access to BTreeDirectory
            // Our Connection is not thread-safe for concurrent queries
            let writer = index
                .writer_with_num_threads(1, DEFAULT_MEMORY_BUDGET_BYTES)
                .map_err(|e| LimboError::InternalError(e.to_string()))?;
            // Disable background merges: our BTreeDirectory can't handle concurrent access
            // from the merge thread. Merges will happen synchronously during commit instead.
            writer.set_merge_policy(Box::new(NoMergePolicy));

            self.writer = Some(writer);
        }
        Ok(IOResult::Done(()))
    }

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>> {
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

        // Don't commit on every insert: Tantivy will auto-commit when memory budget is reached
        // Final commit happens when writer is dropped or explicitly called
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
        // values[1] = query string
        // values[2] = limit (optional)
        let query_str = match &values[1] {
            Register::Value(Value::Text(t)) => t.as_str().to_string(),
            _ => return Err(LimboError::InternalError("FTS query must be text".into())),
        };

        let limit = if values.len() > 2 {
            match &values[2] {
                Register::Value(Value::Integer(i)) => *i as usize,
                _ => 10,
            }
        } else {
            10
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
        // Column 0 = score
        if idx != 0 {
            return Err(LimboError::InternalError(
                "FTS: only column 0 (score) supported".into(),
            ));
        }

        if self.hit_pos >= self.current_hits.len() {
            return Err(LimboError::InternalError(
                "FTS: query_column out of bounds".into(),
            ));
        }

        let (score, _, _) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Value::Float(score as f64)))
    }

    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(None));
        }
        let (_, _, rowid) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Some(rowid)))
    }
}
