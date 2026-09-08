//! Transaction-private component output. Only the final segment descriptor
//! publishes these chunk rows; scratch rows are removed before publication.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AsyncWritePtr, Directory, DirectoryFuture, FileHandle, ReadQueue, WatchCallback, WatchHandle,
    WriteOperation, WritePtr, WriteQueue, WriteRequest,
};

use super::format::{
    segment_chunk_path, segment_chunk_prefix, SegmentDescriptor, SegmentFileEntry,
};
use super::rows::{PathTarget, PendingRow, RowDeleter, RowInserter};
use super::{Arc, HashMap, Mutex, SegmentId, DEFAULT_CHUNK_SIZE};
use crate::storage::btree::CursorTrait;
use crate::types::{IOCompletions, IOResult, IOResultOr};
use crate::{return_if_io, Completion, LimboError, Result};

const COMPONENTS: [&str; 6] = ["fast", "fieldnorm", "idx", "pos", "store", "term"];

#[derive(Clone, Debug)]
struct FileMeta {
    path: String,
    component: Option<usize>,
    len: u64,
    closed: bool,
}

#[derive(Debug, Default)]
struct OutputFiles {
    files: HashMap<PathBuf, FileMeta>,
    metadata: HashMap<PathBuf, Vec<u8>>,
    scratch_counter: u64,
}

#[derive(Clone, Debug)]
pub(super) struct OutputDirectory {
    segment_id: SegmentId,
    files: Arc<Mutex<OutputFiles>>,
    reads: ReadQueue,
    writes: WriteQueue,
}

impl OutputDirectory {
    pub fn new(segment_id: SegmentId, reads: ReadQueue) -> Self {
        Self {
            segment_id,
            files: Arc::default(),
            reads,
            writes: WriteQueue::new(NonZeroUsize::new(DEFAULT_CHUNK_SIZE).unwrap()),
        }
    }

    pub fn descriptor(&self, max_doc: u32) -> Result<SegmentDescriptor> {
        let inner = self.files.lock();
        let mut files = Vec::with_capacity(COMPONENTS.len());
        for (ordinal, extension) in COMPONENTS.iter().enumerate() {
            let entry = inner
                .files
                .values()
                .find(|file| file.component == Some(ordinal))
                .ok_or_else(|| {
                    LimboError::InternalError(format!("FTS output is missing {extension}"))
                })?;
            if !entry.closed {
                return Err(LimboError::InternalError(format!(
                    "FTS output {extension} is not finalized"
                )));
            }
            files.push(SegmentFileEntry {
                name: format!("{}.{extension}", self.segment_id.uuid_string()),
                size: entry.len,
                num_chunks: u32::try_from(entry.len.div_ceil(DEFAULT_CHUNK_SIZE as u64).max(1))
                    .map_err(|_| {
                        LimboError::InternalError("FTS output has too many chunks".into())
                    })?,
            });
        }
        Ok(SegmentDescriptor {
            segment_id: self.segment_id,
            max_doc,
            files,
        })
    }

    fn scratch_prefix(&self) -> String {
        format!("{}tmp/", segment_chunk_prefix(&self.segment_id))
    }
}

impl Directory for OutputDirectory {
    fn open_write_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> DirectoryFuture<'a, std::result::Result<AsyncWritePtr, OpenWriteError>> {
        Box::pin(async move {
            let physical = {
                let mut inner = self.files.lock();
                if inner.files.contains_key(path) {
                    return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
                }
                let name = path.to_str().ok_or_else(|| {
                    OpenWriteError::wrap_io_error(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "FTS output name is not UTF-8",
                        ),
                        path.to_path_buf(),
                    )
                })?;
                let component = COMPONENTS
                    .iter()
                    .position(|extension| name.ends_with(&format!(".{extension}")));
                let physical = if name.starts_with('.') {
                    let counter = inner.scratch_counter;
                    inner.scratch_counter = counter.checked_add(1).ok_or_else(|| {
                        OpenWriteError::wrap_io_error(
                            std::io::Error::other("FTS scratch counter overflow"),
                            path.to_path_buf(),
                        )
                    })?;
                    format!("{}{counter:016}", self.scratch_prefix())
                } else if let Some(ordinal) = component {
                    segment_chunk_path(&self.segment_id, ordinal as u32)
                } else {
                    return Err(OpenWriteError::wrap_io_error(
                        std::io::Error::other("Unknown FTS output component"),
                        path.to_path_buf(),
                    ));
                };
                if inner.files.values().any(|file| file.path == physical) {
                    return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
                }
                inner.files.insert(
                    path.to_path_buf(),
                    FileMeta {
                        path: physical.clone(),
                        component: if name.starts_with('.') {
                            None
                        } else {
                            component
                        },
                        len: 0,
                        closed: false,
                    },
                );
                physical
            };
            self.writes
                .open(physical)
                .await
                .map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))
        })
    }

    fn get_file_handle_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> DirectoryFuture<'a, std::result::Result<Arc<dyn FileHandle>, OpenReadError>> {
        Box::pin(async move {
            let inner = self.files.lock();
            let file = inner
                .files
                .get(path)
                .filter(|file| file.closed)
                .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
            let len = usize::try_from(file.len).map_err(|_| {
                OpenReadError::wrap_io_error(
                    std::io::Error::other("File exceeds address space"),
                    path.to_path_buf(),
                )
            })?;
            Ok(self.reads.file(file.path.clone(), len))
        })
    }

    fn atomic_read_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> DirectoryFuture<'a, std::result::Result<Vec<u8>, OpenReadError>> {
        Box::pin(async move {
            self.files
                .lock()
                .metadata
                .get(path)
                .cloned()
                .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
        })
    }

    fn atomic_write_async<'a>(
        &'a self,
        path: &'a Path,
        data: &'a [u8],
    ) -> DirectoryFuture<'a, std::io::Result<()>> {
        Box::pin(async move {
            if path != Path::new("meta.json") && path != Path::new(".managed.json") {
                return Err(std::io::Error::other(
                    "Only private index metadata may use atomic output",
                ));
            }
            self.files
                .lock()
                .metadata
                .insert(path.to_path_buf(), data.to_vec());
            Ok(())
        })
    }

    fn sync_directory_async(&self) -> DirectoryFuture<'_, std::io::Result<()>> {
        // These metadata files are private Tantivy bookkeeping. Turso owns
        // WAL durability and publishes a separate transactional descriptor.
        Box::pin(async { Ok(()) })
    }

    fn delete_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> DirectoryFuture<'a, std::result::Result<(), DeleteError>> {
        Box::pin(async move {
            let mut inner = self.files.lock();
            let Some(file) = inner.files.get(path) else {
                return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
            };
            if file.component.is_some() || !file.closed {
                return Err(DeleteError::IoError {
                    io_error: Arc::new(std::io::Error::other(
                        "Only finalized scratch files can be retired",
                    )),
                    filepath: path.to_path_buf(),
                });
            }
            // Keep physical rows until the build finishes: an already-open
            // scratch slice must remain readable after its name is retired.
            inner.files.remove(path);
            Ok(())
        })
    }

    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            async_required(),
            path.to_path_buf(),
        ))
    }
    fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            async_required(),
            path.to_path_buf(),
        ))
    }
    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            async_required(),
            path.to_path_buf(),
        ))
    }
    fn atomic_write(&self, _: &Path, _: &[u8]) -> std::io::Result<()> {
        Err(async_required())
    }
    fn sync_directory(&self) -> std::io::Result<()> {
        Err(async_required())
    }
    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(async_required()),
            filepath: path.to_path_buf(),
        })
    }
    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        let inner = self.files.lock();
        Ok(inner.files.contains_key(path) || inner.metadata.contains_key(path))
    }
    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

fn async_required() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FTS output requires asynchronous storage",
    )
}

struct OpenFile {
    offset: u64,
    chunk: i64,
    tail: Vec<u8>,
    persisted: bool,
    dirty: bool,
    closed: bool,
}

struct PendingOutput {
    request: WriteRequest,
    consumed: usize,
    replace: Option<(Option<RowDeleter>, RowInserter)>,
}

pub(super) struct OutputIo {
    directory: OutputDirectory,
    files: HashMap<String, OpenFile>,
    pending: Option<PendingOutput>,
    wait: Option<Completion>,
    cleanup: RowDeleter,
}

impl OutputIo {
    pub fn new(directory: OutputDirectory) -> Self {
        let cleanup = RowDeleter::new(vec![PathTarget::Prefix(directory.scratch_prefix())]);
        Self {
            directory,
            files: HashMap::default(),
            pending: None,
            wait: None,
            cleanup,
        }
    }

    pub fn start_request(&mut self) -> bool {
        assert!(self.pending.is_none());
        self.pending = self.directory.writes.pop().map(|request| PendingOutput {
            request,
            consumed: 0,
            replace: None,
        });
        self.pending.is_some()
    }

    pub fn resume(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        if let Some(wait) = &self.wait {
            if !wait.finished() {
                return Ok(IOResult::IO(IOCompletions(wait.clone())));
            }
            if let Some(error) = wait.get_error() {
                return Err(LimboError::CompletionError(error).into());
            }
            self.wait = None;
        }
        let result = self.drive(cursor);
        if let Ok(IOResult::IO(wait)) = &result {
            self.wait = Some(wait.0.clone());
        }
        result
    }

    fn drive(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        let Some(pending) = &mut self.pending else {
            return Ok(IOResult::Done(()));
        };
        if matches!(pending.request.operation(), WriteOperation::Open) {
            if self.files.contains_key(pending.request.name()) {
                return Err(
                    LimboError::InternalError("Duplicate FTS output creation".into()).into(),
                );
            }
            self.files.insert(
                pending.request.name().to_owned(),
                OpenFile {
                    offset: 0,
                    chunk: 0,
                    tail: Vec::new(),
                    persisted: false,
                    dirty: true,
                    closed: false,
                },
            );
            self.pending.take().unwrap().request.complete(Ok(0));
            return Ok(IOResult::Done(()));
        }
        let file = self
            .files
            .get_mut(pending.request.name())
            .ok_or_else(|| LimboError::InternalError("FTS output was not opened".into()))?;
        if file.closed {
            return Err(LimboError::InternalError("FTS output is already finalized".into()).into());
        }
        loop {
            if let Some((deleter, inserter)) = &mut pending.replace {
                if let Some(delete) = deleter {
                    return_if_io!(delete.step(cursor));
                    *deleter = None;
                }
                return_if_io!(inserter.step(cursor));
                pending.replace = None;
                file.persisted = true;
                file.dirty = false;
                if file.tail.len() == DEFAULT_CHUNK_SIZE {
                    file.tail.clear();
                    file.chunk += 1;
                    file.persisted = false;
                }
            }
            match pending.request.operation() {
                WriteOperation::Write { offset, bytes } => {
                    if file.offset != offset + pending.consumed as u64 {
                        return Err(LimboError::InternalError(
                            "FTS output offset changed across suspension".into(),
                        )
                        .into());
                    }
                    if pending.consumed == bytes.len() {
                        break;
                    }
                    let count =
                        (DEFAULT_CHUNK_SIZE - file.tail.len()).min(bytes.len() - pending.consumed);
                    file.tail
                        .extend_from_slice(&bytes[pending.consumed..pending.consumed + count]);
                    file.offset += count as u64;
                    file.dirty = true;
                    pending.consumed += count;
                    if file.tail.len() < DEFAULT_CHUNK_SIZE {
                        break;
                    }
                }
                WriteOperation::Flush => {
                    if !file.dirty {
                        break;
                    }
                }
                WriteOperation::Finish { len } => {
                    if file.offset != *len {
                        return Err(LimboError::InternalError(
                            "FTS final output length mismatch".into(),
                        )
                        .into());
                    }
                    if !file.dirty {
                        file.closed = true;
                        let mut inner = self.directory.files.lock();
                        let entry = inner
                            .files
                            .values_mut()
                            .find(|entry| entry.path == pending.request.name())
                            .expect("output directory owns every opened file");
                        entry.len = file.offset;
                        entry.closed = true;
                        break;
                    }
                }
                WriteOperation::Open => unreachable!(),
            }
            let path = pending.request.name().to_owned();
            let deleter = file
                .persisted
                .then(|| RowDeleter::new(vec![PathTarget::Chunk(path.clone(), file.chunk)]));
            let inserter = RowInserter::new(vec![PendingRow {
                path,
                chunk_no: file.chunk,
                bytes: file.tail.clone(),
            }]);
            pending.replace = Some((deleter, inserter));
        }
        let pending = self.pending.take().unwrap();
        if matches!(pending.request.operation(), WriteOperation::Finish { .. }) {
            self.files.remove(pending.request.name());
        }
        pending.request.complete(Ok(pending.consumed));
        Ok(IOResult::Done(()))
    }

    pub fn cleanup(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        if !self.files.is_empty() {
            return Err(
                LimboError::InternalError("FTS build left output unfinished".into()).into(),
            );
        }
        if let Some(wait) = &self.wait {
            if !wait.finished() {
                return Ok(IOResult::IO(IOCompletions(wait.clone())));
            }
            if let Some(error) = wait.get_error() {
                return Err(LimboError::CompletionError(error).into());
            }
            self.wait = None;
        }
        let result = self.cleanup.step(cursor);
        if let Ok(IOResult::IO(wait)) = &result {
            self.wait = Some(wait.0.clone());
        }
        result
    }
}

#[cfg(all(test, feature = "io_memory_yield"))]
mod tests {
    use super::*;
    use crate::{Database, DatabaseOpts, OpenFlags, SqliteDialect, IO};

    #[test]
    fn chunk_output_yields_replaces_partial_rows_and_rolls_back() {
        let io = Arc::new(crate::io::MemoryYieldIO::new());
        let open = || {
            Database::open_file_with_flags(
                io.clone(),
                "fts-output.db",
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap()
        };
        {
            let db = open();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE docs(body TEXT)").unwrap();
            conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
                .unwrap();
        }
        let db = open();
        let conn = db.connect().unwrap();
        conn.execute("BEGIN IMMEDIATE").unwrap();
        let mut cursor = backing_cursor(&conn);
        let reads = ReadQueue::default();
        let id = SegmentId::from_uuid_string("00000000000000000000000000000042").unwrap();
        let directory = OutputDirectory::new(id, reads.clone());
        let output = directory.clone();
        let future = async move {
            let managed =
                tantivy::directory::ManagedDirectory::wrap_async(Box::new(output)).await?;
            for extension in COMPONENTS {
                let name = PathBuf::from(format!("{}.{extension}", id.uuid_string()));
                let mut writer = managed.open_write_async(&name).await?;
                if extension == "fieldnorm" {
                    let mut schema = tantivy::schema::Schema::builder();
                    let field = schema.add_text_field("body", tantivy::schema::TEXT);
                    let mut norms =
                        tantivy::fieldnorm::FieldNormsWriter::for_schema(&schema.build());
                    norms.record(0, field, 5);
                    norms.fill_up_to_max_doc((DEFAULT_CHUNK_SIZE + 31) as u32);
                    norms.serialize_async(writer).await?;
                    let file = managed.open_read_async(&name).await?;
                    let composite = tantivy::directory::CompositeFile::open_async(&file).await?;
                    let bytes = composite
                        .open_read(field)
                        .unwrap()
                        .read_bytes_async()
                        .await?;
                    assert_eq!(bytes.len(), DEFAULT_CHUNK_SIZE + 31);
                    assert_eq!(bytes[0], 5);
                    assert!(bytes[1..].iter().all(|byte| *byte == 0));
                    continue;
                }
                if extension == "pos" {
                    let mut expected = Vec::new();
                    {
                        let mut reference =
                            tantivy::positions::PositionSerializer::new(&mut expected);
                        for count in [0, 1, 127, 128, 129, DEFAULT_CHUNK_SIZE / 4 + 129] {
                            let deltas = vec![u32::MAX; count];
                            reference.write_positions_delta(&deltas);
                            reference.close_term()?;
                            let mut positions =
                                tantivy::positions::AsyncPositionSerializer::new(&managed).await?;
                            for part in deltas.chunks(37) {
                                positions.write_positions_delta(part).await?;
                            }
                            positions.close_term(writer.as_mut()).await?;
                        }
                        reference.close()?;
                    }
                    writer.finish().await?;
                    let bytes = managed
                        .open_read_async(&name)
                        .await?
                        .read_bytes_async()
                        .await?;
                    assert_eq!(bytes.as_ref(), expected.as_slice());
                    continue;
                }
                writer.write_all(&[3; 17]).await?;
                writer.flush().await?;
                let data = vec![9; DEFAULT_CHUNK_SIZE + 31];
                writer.write_all(&data).await?;
                writer.flush().await?;
                writer.finish().await?;
                let file = managed.open_read_async(&name).await?;
                let bytes = file.read_bytes_async().await?;
                assert_eq!(bytes.len(), data.len() + 17);
                assert_eq!(&bytes[..17], &[3; 17]);
                assert_eq!(&bytes[17..], &data);
            }
            compare_postings(&managed).await?;
            compare_dictionaries(&managed).await?;
            let name = Path::new(".scratch");
            let mut writer = managed.open_write_async(name).await?;
            writer.write_all(&[11; 37]).await?;
            writer.finish().await?;
            let slice = managed.open_read_async(name).await?;
            managed
                .delete_async(name)
                .await
                .map_err(std::io::Error::other)?;
            assert_eq!(slice.read_bytes_async().await?.as_ref(), &[11; 37]);
            Ok(())
        };
        let mut operation =
            super::super::read::SnapshotIo::new(reads, future).with_output(directory.clone());
        let mut yields = 0;
        loop {
            match operation.resume(cursor.as_mut()).unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(wait) => {
                    if wait.0.finished() {
                        continue;
                    }
                    for _ in 0..3 {
                        assert!(matches!(
                            operation.resume(cursor.as_mut()).unwrap(),
                            IOResult::IO(_)
                        ));
                    }
                    yields += 1;
                    io.step().unwrap();
                }
            }
        }
        assert!(yields > 0);
        let descriptor = directory.descriptor(1).unwrap();
        assert!(descriptor.files.iter().all(|file| file.num_chunks == 2));
        let rows = scan(io.as_ref(), cursor.as_mut());
        let output_rows = rows
            .iter()
            .filter(|(path, _, _)| path.starts_with(&segment_chunk_prefix(&id)))
            .collect::<Vec<_>>();
        assert_eq!(
            output_rows.len(),
            12,
            "no duplicate partial chunks or scratch rows"
        );
        assert!(!rows
            .iter()
            .any(|(path, _, _)| path == &super::super::format::segment_registry_path(&id)));
        drop(cursor);
        conn.execute("ROLLBACK").unwrap();
        conn.execute("BEGIN").unwrap();
        let mut cursor = backing_cursor(&conn);
        assert!(!scan(io.as_ref(), cursor.as_mut())
            .iter()
            .any(|(path, _, _)| path.starts_with(&segment_chunk_prefix(&id))));
        drop(cursor);
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn chunk_output_errors_and_cancellation_leave_no_published_rows() {
        for cancel in [false, true] {
            for stop_at in 0..4 {
                let io = Arc::new(crate::io::MemoryYieldIO::new());
                let open = || {
                    Database::open_file_with_flags(
                        io.clone(),
                        "fts-output-failure.db",
                        OpenFlags::default(),
                        DatabaseOpts::new().with_index_method(true),
                        None,
                        Arc::new(SqliteDialect),
                    )
                    .unwrap()
                };
                {
                    let db = open();
                    let conn = db.connect().unwrap();
                    conn.execute("CREATE TABLE docs(body TEXT)").unwrap();
                    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
                        .unwrap();
                }
                let db = open();
                let conn = db.connect().unwrap();
                conn.execute("PRAGMA cache_size=10").unwrap();
                conn.execute("BEGIN IMMEDIATE").unwrap();
                let mut cursor = backing_cursor(&conn);
                let reads = ReadQueue::default();
                let id = SegmentId::generate_random();
                let directory = OutputDirectory::new(id, reads.clone());
                let output = directory.clone();
                let mut operation = super::super::read::SnapshotIo::new(reads, async move {
                    let mut writer = output.open_write_async(Path::new("output.idx")).await?;
                    writer.write_all(&vec![7; DEFAULT_CHUNK_SIZE * 12]).await?;
                    writer.finish().await?;
                    Ok(())
                })
                .with_output(directory.clone());
                let mut yields = 0;
                loop {
                    let IOResult::IO(wait) = operation.resume(cursor.as_mut()).unwrap() else {
                        panic!("output completed before boundary {stop_at}");
                    };
                    if wait.0.finished() {
                        continue;
                    }
                    if yields == stop_at {
                        if !cancel {
                            wait.0.error(crate::error::CompletionError::IOError(
                                std::io::ErrorKind::Other,
                                "injected output failure",
                            ));
                            assert!(operation.resume(cursor.as_mut()).is_err());
                        }
                        break;
                    }
                    yields += 1;
                    io.step().unwrap();
                }
                drop(operation);
                io.step().unwrap();
                assert!(directory.descriptor(1).is_err());
                drop(cursor);
                conn.execute("ROLLBACK").unwrap();
                conn.execute("BEGIN").unwrap();
                let mut cursor = backing_cursor(&conn);
                assert!(!scan(io.as_ref(), cursor.as_mut())
                    .iter()
                    .any(|(path, _, _)| path.starts_with(&segment_chunk_prefix(&id))
                        || path == &super::super::format::segment_registry_path(&id)));
                drop(cursor);
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }

    #[test]
    fn native_inverted_components_match_reference() {
        let io = Arc::new(crate::io::MemoryYieldIO::new());
        let open = || {
            Database::open_file_with_flags(
                io.clone(),
                "fts-inverted-output.db",
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap()
        };
        {
            let db = open();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE docs(body TEXT)").unwrap();
            conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
                .unwrap();
        }
        let db = open();
        let conn = db.connect().unwrap();
        conn.execute("BEGIN IMMEDIATE").unwrap();
        let mut cursor = backing_cursor(&conn);
        let reads = ReadQueue::default();
        let directory = OutputDirectory::new(SegmentId::generate_random(), reads.clone());
        let output = directory.clone();
        let future = async move {
            use tantivy::directory::TerminatingWrite;
            use tantivy::fastfield::FastFieldsWriter;
            use tantivy::index::SegmentComponent;
            use tantivy::postings::{AsyncInvertedIndexSerializer, InvertedIndexSerializer};
            let mut schema = tantivy::schema::Schema::builder();
            let text = schema.add_text_field("text", tantivy::schema::TEXT);
            let number =
                schema.add_i64_field("number", tantivy::schema::INDEXED | tantivy::schema::FAST);
            let schema = schema.build();
            let reference = tantivy::Index::create_in_ram(schema.clone());
            let mut reference_segment = reference.new_segment();
            let index = tantivy::Index::create_async(output, schema, Default::default()).await?;
            let segment = index.new_segment();
            let mut expected = InvertedIndexSerializer::open(&mut reference_segment)?;
            let mut encoder = AsyncInvertedIndexSerializer::open(&segment).await?;
            for field in [text, number] {
                let norms =
                    (field == text).then(|| tantivy::fieldnorm::FieldNormReader::constant(257, 2));
                let mut reference_field = expected.new_field(field, 514, norms.clone())?;
                let mut output_field = encoder.new_field(field, 514, norms).await?;
                for (key, count) in [
                    (b"a".as_slice(), 1),
                    (b"aa".as_slice(), 128),
                    (b"b".as_slice(), 257),
                ] {
                    reference_field.new_term(key, count, field == text)?;
                    output_field.new_term(key, count, field == text).await?;
                    for doc in 0..count {
                        let freq = doc % 3 + 1;
                        let deltas = &[1, 2, 3][..freq as usize];
                        reference_field.write_doc(doc, freq, deltas);
                        output_field.write_doc(doc, freq, deltas).await?;
                    }
                    reference_field.close_term()?;
                    output_field.close_term().await?;
                }
                reference_field.close()?;
                output_field.close().await?;
            }
            expected.close()?;
            encoder.close().await?;
            let mut expected_store = tantivy::store::StoreWriter::new(
                reference_segment.open_write(SegmentComponent::Store)?,
                tantivy::store::Compressor::None,
                16,
                false,
            )?;
            let mut store = tantivy::store::AsyncStoreWriter::new(
                segment.open_write_async(SegmentComponent::Store).await?,
                Box::new(index.directory().clone()),
                tantivy::store::Compressor::None,
                16,
            );
            let mut expected_fast = FastFieldsWriter::from_schema_and_tokenizer_manager(
                &segment.schema(),
                Default::default(),
            )?;
            let mut fast = FastFieldsWriter::from_schema_and_tokenizer_manager(
                &segment.schema(),
                Default::default(),
            )?;
            for id in 0..8193 {
                let mut document = tantivy::TantivyDocument::default();
                document.add_i64(number, id - 4096);
                expected_fast.add_document(&document)?;
                fast.add_document(&document)?;
                expected_store.store(&document, &segment.schema())?;
                store.store(&document, &segment.schema()).await?;
            }
            expected_store.close()?;
            store.close().await?;
            let mut fast_write = reference_segment.open_write(SegmentComponent::FastFields)?;
            expected_fast.serialize(&mut fast_write)?;
            fast_write.terminate()?;
            fast.serialize_async(
                segment
                    .open_write_async(SegmentComponent::FastFields)
                    .await?,
            )
            .await?;
            for component in [
                SegmentComponent::Terms,
                SegmentComponent::Postings,
                SegmentComponent::Positions,
                SegmentComponent::Store,
                SegmentComponent::FastFields,
            ] {
                let expected = reference_segment.open_read(component)?.read_bytes()?;
                let actual = segment
                    .open_read_async(component)
                    .await?
                    .read_bytes_async()
                    .await?;
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
            Ok(())
        };
        let mut operation =
            super::super::read::SnapshotIo::new(reads, future).with_output(directory);
        let mut yields = 0;
        loop {
            match operation.resume(cursor.as_mut()).unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(wait) if wait.0.finished() => continue,
                IOResult::IO(_) => {
                    yields += 1;
                    io.step().unwrap();
                }
            }
        }
        assert!(yields > 0);
        drop(cursor);
        conn.execute("ROLLBACK").unwrap();
    }

    async fn compare_dictionaries(
        directory: &tantivy::directory::ManagedDirectory,
    ) -> tantivy::Result<()> {
        use tantivy::termdict::{AsyncTermDictionaryBuilder, TermDictionaryBuilder};
        for count in [0usize, 1, 255, 256, 257, 1025, 65537] {
            let path = Path::new(".dictionary-case");
            let mut writer = directory.open_write_async(path).await?;
            let mut reference = TermDictionaryBuilder::create(Vec::new())?;
            let mut builder = AsyncTermDictionaryBuilder::new(directory, writer.as_mut()).await?;
            for i in 0..count {
                let key = format!("{i:08}");
                let offset = i * (1usize << (usize::BITS - 24));
                let info = tantivy::postings::TermInfo {
                    doc_freq: i as u32 % 32 + 1,
                    postings_range: offset..offset + 17,
                    positions_range: offset..offset + 11,
                };
                reference.insert(key.as_bytes(), &info)?;
                builder.insert(key.as_bytes(), &info).await?;
            }
            builder.finish().await?;
            writer.finish().await?;
            let bytes = directory
                .open_read_async(path)
                .await?
                .read_bytes_async()
                .await?;
            assert_eq!(
                bytes.as_ref(),
                reference.finish()?.as_slice(),
                "terms={count}"
            );
            directory
                .delete_async(path)
                .await
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }

    async fn compare_postings(
        directory: &tantivy::directory::ManagedDirectory,
    ) -> tantivy::Result<()> {
        use tantivy::directory::{CompositeFile, CompositeWrite, RamDirectory};
        use tantivy::postings::{AsyncPostingsSerializer, FieldSerializer};
        use tantivy::schema::{Field, IndexRecordOption};
        for mode in [
            IndexRecordOption::Basic,
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqsAndPositions,
        ] {
            for count in [0, 1, 127, 128, 129, 1025, 65537] {
                let ram = RamDirectory::default();
                let mut terms = CompositeWrite::wrap(ram.open_write(Path::new("term"))?);
                let mut postings = CompositeWrite::wrap(ram.open_write(Path::new("idx"))?);
                let mut positions = CompositeWrite::wrap(ram.open_write(Path::new("pos"))?);
                let field = Field::from_field_id(0);
                let mut reference = FieldSerializer::create(
                    mode,
                    count as u64,
                    terms.for_field(field),
                    postings.for_field(field),
                    positions.for_field(field),
                    None,
                )?;
                reference.new_term(b"word", count, true)?;
                let path = Path::new(".postings-case");
                let mut output = directory.open_write_async(path).await?;
                let mut encoder =
                    AsyncPostingsSerializer::new(directory, 1.0, mode, None, count, true).await?;
                for doc in 0..count {
                    let freq = doc % 3 + 1;
                    reference.write_doc(doc * 17, freq, &[1, 2, 3][..freq as usize]);
                    encoder.write_doc(doc * 17, freq).await?;
                }
                reference.close()?;
                postings.close()?;
                let file = ram.open_read(Path::new("idx"))?;
                let expected = CompositeFile::open(&file)?
                    .open_read(field)
                    .unwrap()
                    .slice_from(8)
                    .read_bytes()?;
                assert_eq!(
                    encoder.close_term(output.as_mut()).await?,
                    expected.len() as u64
                );
                output.finish().await?;
                let bytes = directory
                    .open_read_async(path)
                    .await?
                    .read_bytes_async()
                    .await?;
                assert_eq!(
                    bytes.as_ref(),
                    expected.as_ref(),
                    "mode={mode:?}, count={count}"
                );
                directory
                    .delete_async(path)
                    .await
                    .map_err(std::io::Error::other)?;
                terms.close()?;
                positions.close()?;
            }
        }
        Ok(())
    }

    fn backing_cursor(conn: &Arc<crate::Connection>) -> Box<dyn CursorTrait> {
        let table = format!("{}fts_dir_docs_fts", crate::schema::TURSO_INTERNAL_PREFIX);
        super::super::open_index_cursor(
            conn,
            0,
            &table,
            &format!("{table}_key"),
            [
                super::super::key_info(),
                super::super::key_info(),
                super::super::key_info(),
            ],
        )
        .unwrap()
    }

    fn scan(io: &dyn IO, cursor: &mut dyn CursorTrait) -> Vec<(String, i64, Vec<u8>)> {
        loop {
            match cursor.rewind().unwrap() {
                IOResult::Done(_) => break,
                IOResult::IO(_) => io.step().unwrap(),
            }
        }
        let mut rows = Vec::new();
        while cursor.has_record() {
            loop {
                match cursor.record().unwrap() {
                    IOResult::Done(record) => {
                        rows.push(super::super::rows::row_fields(record.unwrap()).unwrap());
                        break;
                    }
                    IOResult::IO(_) => io.step().unwrap(),
                }
            }
            loop {
                match cursor.next().unwrap() {
                    IOResult::Done(_) => break,
                    IOResult::IO(_) => io.step().unwrap(),
                }
            }
        }
        rows
    }
}
