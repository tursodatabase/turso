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
use turso_parser::ast::{self, Select};

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
        let stmt_ast = ast_builder::select_total_length_by_path(&self.table_name, path);
        self.connection.start_nested();
        let mut stmt =
            self.connection
                .prepare_stmt(stmt_ast)
                .map_err(|e| OpenReadError::IoError {
                    io_error: std::io::Error::other(e.to_string()).into(),
                    filepath: path.into(),
                })?;

        stmt.program.needs_stmt_subtransactions = false;

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
        let stmt_ast = ast_builder::select_exists_by_path(&self.table_name, path);
        self.connection.start_nested();
        let mut stmt =
            self.connection
                .prepare_stmt(stmt_ast)
                .map_err(|e| OpenReadError::IoError {
                    io_error: std::io::Error::other(e.to_string()).into(),
                    filepath: path.into(),
                })?;

        stmt.program.needs_stmt_subtransactions = false;

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
        let stmt_ast = ast_builder::delete_by_path(&self.table_name, path);
        self.connection.start_nested();
        let mut stmt =
            self.connection
                .prepare_stmt(stmt_ast)
                .map_err(|e| DeleteError::IoError {
                    io_error: std::io::Error::other(e.to_string()).into(),
                    filepath: path.into(),
                })?;

        stmt.program.needs_stmt_subtransactions = false;
        let res = stmt.run_ignore_rows();
        self.connection.end_nested();

        res.map_err(|e| DeleteError::IoError {
            io_error: std::io::Error::other(e.to_string()).into(),
            filepath: path.into(),
        })
    }

    /// Read all bytes for a path (concatenating chunks in order)
    fn read_all_bytes_for_path(&self, path: &str) -> std::result::Result<Vec<u8>, OpenReadError> {
        let stmt_ast = ast_builder::select_bytes_by_path(&self.table_name, path);
        self.connection.start_nested();
        let mut stmt =
            self.connection
                .prepare_stmt(stmt_ast)
                .map_err(|e| OpenReadError::IoError {
                    io_error: std::io::Error::other(e.to_string()).into(),
                    filepath: path.into(),
                })?;

        stmt.program.needs_stmt_subtransactions = false;

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
        let hex_data: String = data.iter().map(|b| format!("{b:02x}")).collect();
        let stmt_ast = ast_builder::insert_chunk(&self.table_name, path, 0, &hex_data);
        self.connection.start_nested();
        let mut insert_stmt = self
            .connection
            .prepare_stmt(stmt_ast)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        insert_stmt.program.needs_stmt_subtransactions = false;
        let res = insert_stmt.run_ignore_rows();
        self.connection.end_nested();

        res.map_err(|e| std::io::Error::other(e.to_string()))
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
                    col_name: ast_builder::name("path"),
                    col_type: Some(ast_builder::text_type()),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
                ast::ColumnDefinition {
                    col_name: ast_builder::name("chunk_no"),
                    col_type: Some(ast_builder::integer_type()),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
                ast::ColumnDefinition {
                    col_name: ast_builder::name("bytes"),
                    col_type: Some(ast_builder::blob_type()),
                    constraints: vec![NOTNULL_CONSTRAINT],
                },
            ],
            constraints: vec![],
            options: ast::TableOptions::empty(),
        },
        temporary: false,
        if_not_exists: false,
        tbl_name: ast_builder::table_name(table_name),
    };
    // "CREATE UNIQUE INDEX idx_name ON table_name (path, chunk_no);"
    let create_index_stmt = ast::Stmt::CreateIndex {
        unique: true,
        if_not_exists: false,
        idx_name: ast_builder::table_name(&format!("{table_name}_key")),
        tbl_name: ast_builder::name(table_name),
        using: None,
        columns: vec![
            ast::SortedColumn {
                expr: Box::new(ast::Expr::Name(ast_builder::name("path"))),
                order: None,
                nulls: None,
            },
            ast::SortedColumn {
                expr: Box::new(ast::Expr::Name(ast_builder::name("chunk_no"))),
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

        let stmt = ast_builder::select_chunk_by_path_and_no(
            self.table_name.as_str(),
            self.path.as_str(),
            first_chunk,
            last_chunk,
        );
        self.conn.start_nested();
        let mut stmt = self
            .conn
            .prepare_stmt(stmt)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        stmt.program.needs_stmt_subtransactions = false;

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
        }
    }

    fn write_chunk(&mut self, chunk_no: i64, data: &[u8]) -> std::io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Use hex encoding for blob data
        let hex_data = bytes_to_hex(data);
        let stmt_ast =
            ast_builder::insert_or_replace_chunk(&self.table_name, &self.path, chunk_no, &hex_data);
        self.conn.start_nested();
        let mut stmt = self
            .conn
            .prepare_stmt(stmt_ast)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Execute within nested context to avoid transaction conflicts
        stmt.program.needs_stmt_subtransactions = false;
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

/// Pattern indices for FTS queries
const FTS_PATTERN_SCORE: i64 = 0;
const FTS_PATTERN_COMBINED_ORDERED_LIMIT: i64 = 1;
const FTS_PATTERN_COMBINED_ORDERED: i64 = 2;
const FTS_PATTERN_COMBINED_LIMIT: i64 = 3;
const FTS_PATTERN_COMBINED: i64 = 4;
const FTS_PATTERN_MATCH_LIMIT: i64 = 5;
const FTS_PATTERN_MATCH: i64 = 6;
const TANTIVY_META_FILE: &str = "meta.json";

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
    /// Which pattern is being used (0=fts_score, 1=fts_match)
    current_pattern: i64,
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
            current_pattern: FTS_PATTERN_SCORE,
        }
    }

    fn ensure_index_initialized(&mut self, conn: &Arc<Connection>) -> Result<()> {
        if self.index.is_some() {
            return Ok(());
        }

        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            self.cfg.index_name
        );
        let dir = BTreeDirectory::new(conn.clone(), dir_table_name, DEFAULT_CHUNK_SIZE)?;

        let index_exists = dir.exists(Path::new(TANTIVY_META_FILE)).unwrap_or(false);

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
            FTS_PATTERN_MATCH | FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_ORDERED => 10_000_000,
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

/// AST builder module for FTS storage operations.
/// These helpers construct AST nodes directly so we an avoid the SQL string parsing overhead.
mod ast_builder {
    use turso_parser::ast::{
        self, Expr, FromClause, FunctionTail, InsertBody, Limit, Literal, Name, OneSelect,
        Operator, QualifiedName, ResultColumn, Select, SelectBody, SelectTable, SortOrder,
        SortedColumn, Stmt,
    };

    /// Build a Name from a string
    pub fn name(s: &str) -> Name {
        Name::exact(s.to_string())
    }

    /// Build a QualifiedName from a table name
    pub fn table_name(table: &str) -> QualifiedName {
        QualifiedName::single(name(table))
    }

    /// Build a column identifier expression
    fn col(column: &str) -> Box<Expr> {
        Box::new(Expr::Id(name(column)))
    }

    /// Build a string literal expression
    fn str_lit(s: &str) -> Box<Expr> {
        // The translator's sanitize_string() expects quotes around the string,
        // so we need to include them when building AST programmatically
        Box::new(Expr::Literal(Literal::String(format!(
            "'{}'",
            s.replace('\'', "''")
        ))))
    }

    pub fn integer_type() -> ast::Type {
        ast::Type {
            name: "INTEGER".to_string(),
            size: None,
        }
    }

    pub fn text_type() -> ast::Type {
        ast::Type {
            name: "TEXT".to_string(),
            size: None,
        }
    }
    pub fn blob_type() -> ast::Type {
        ast::Type {
            name: "BLOB".to_string(),
            size: None,
        }
    }

    /// Build an integer literal expression
    fn int_lit(n: i64) -> Box<Expr> {
        Box::new(Expr::Literal(Literal::Numeric(n.to_string())))
    }

    /// Build a blob literal from hex string (already encoded)
    fn blob_lit(hex: &str) -> Box<Expr> {
        Box::new(Expr::Literal(Literal::Blob(hex.to_string())))
    }

    /// Build a binary equals expression: lhs = rhs
    fn eq(lhs: Box<Expr>, rhs: Box<Expr>) -> Box<Expr> {
        Box::new(Expr::Binary(lhs, Operator::Equals, rhs))
    }

    /// Build FROM clause for a single table
    fn from_table(table: &str) -> Option<FromClause> {
        Some(FromClause {
            select: Box::new(SelectTable::Table(table_name(table), None, None)),
            joins: vec![],
        })
    }

    /// Build ORDER BY clause for a column
    fn order_by_asc(column: &str) -> Vec<SortedColumn> {
        vec![SortedColumn {
            expr: col(column),
            order: Some(SortOrder::Asc),
            nulls: None,
        }]
    }

    /// Build: SELECT {columns} FROM {table} WHERE path = '{path}' ORDER BY chunk_no ASC
    pub fn select_bytes_by_path(table: &str, path: &str) -> Stmt {
        Stmt::Select(Select {
            with: None,
            body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(col("bytes"), None)],
                    from: from_table(table),
                    where_clause: Some(eq(col("path"), str_lit(path))),
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: order_by_asc("chunk_no"),
            limit: None,
        })
    }

    /// Build: SELECT 1 FROM {table} WHERE path = '{path}' LIMIT 1
    pub fn select_exists_by_path(table: &str, path: &str) -> Stmt {
        Stmt::Select(Select {
            with: None,
            body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(int_lit(1), None)],
                    from: from_table(table),
                    where_clause: Some(eq(col("path"), str_lit(path))),
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: Some(Limit {
                expr: int_lit(1),
                offset: None,
            }),
        })
    }

    /// Build: SELECT SUM(LENGTH(bytes)) FROM {table} WHERE path = '{path}'
    pub fn select_total_length_by_path(table: &str, path: &str) -> Stmt {
        // SUM(LENGTH(bytes))
        let length_call = Box::new(Expr::FunctionCall {
            name: name("LENGTH"),
            distinctness: None,
            args: vec![col("bytes")],
            order_by: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        });
        let sum_call = Box::new(Expr::FunctionCall {
            name: name("SUM"),
            distinctness: None,
            args: vec![length_call],
            order_by: vec![],
            filter_over: FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        });

        Stmt::Select(Select {
            with: None,
            body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(sum_call, None)],
                    from: from_table(table),
                    where_clause: Some(eq(col("path"), str_lit(path))),
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        })
    }

    /// Build: DELETE FROM {table} WHERE path = '{path}'
    pub fn delete_by_path(table: &str, path: &str) -> Stmt {
        Stmt::Delete {
            with: None,
            tbl_name: table_name(table),
            indexed: None,
            where_clause: Some(eq(col("path"), str_lit(path))),
            returning: vec![],
            order_by: vec![],
            limit: None,
        }
    }

    /// Build: INSERT INTO {table} (path, chunk_no, bytes) VALUES ('{path}', {chunk_no}, X'{hex_data}')
    pub fn insert_chunk(table: &str, path: &str, chunk_no: i64, hex_data: &str) -> Stmt {
        insert_chunk_internal(table, path, chunk_no, hex_data, None)
    }

    /// Build: INSERT OR REPLACE INTO {table} (path, chunk_no, bytes) VALUES ('{path}', {chunk_no}, X'{hex_data}')
    pub fn insert_or_replace_chunk(table: &str, path: &str, chunk_no: i64, hex_data: &str) -> Stmt {
        use turso_parser::ast::ResolveType;
        insert_chunk_internal(table, path, chunk_no, hex_data, Some(ResolveType::Replace))
    }

    /// Build: SELECT chunk_no, bytes FROM {} WHERE path = '{}' AND chunk_no >= {} AND chunk_no <= {} ORDER BY chunk_no ASC
    pub fn select_chunk_by_path_and_no(
        table: &str,
        path: &str,
        chunk_min: i64,
        chunk_max: i64,
    ) -> Stmt {
        Stmt::Select(Select {
            with: None,
            body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![
                        ResultColumn::Expr(col("chunk_no"), None),
                        ResultColumn::Expr(col("bytes"), None),
                    ],
                    from: from_table(table),
                    where_clause: Some(Box::new(Expr::Binary(
                        eq(col("path"), str_lit(path)),
                        Operator::And,
                        Box::new(Expr::Binary(
                            Box::new(Expr::Binary(
                                col("chunk_no"),
                                Operator::GreaterEquals,
                                int_lit(chunk_min),
                            )),
                            Operator::And,
                            Box::new(Expr::Binary(
                                col("chunk_no"),
                                Operator::LessEquals,
                                int_lit(chunk_max),
                            )),
                        )),
                    ))),
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: order_by_asc("chunk_no"),
            limit: None,
        })
    }

    fn insert_chunk_internal(
        table: &str,
        path: &str,
        chunk_no: i64,
        hex_data: &str,
        or_conflict: Option<turso_parser::ast::ResolveType>,
    ) -> Stmt {
        // VALUES clause as a Select with OneSelect::Values
        let values_select = Select {
            with: None,
            body: SelectBody {
                select: OneSelect::Values(vec![vec![
                    str_lit(path),
                    int_lit(chunk_no),
                    blob_lit(hex_data),
                ]]),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };

        Stmt::Insert {
            with: None,
            or_conflict,
            tbl_name: table_name(table),
            columns: vec![name("path"), name("chunk_no"), name("bytes")],
            body: InsertBody::Select(values_select, None),
            returning: vec![],
        }
    }
}
