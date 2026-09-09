use std::collections::HashSet;
use std::fmt;
#[cfg(feature = "mmap")]
use std::path::Path;
use std::path::PathBuf;
use std::thread::available_parallelism;

use super::segment::Segment;
use super::segment_reader::merge_field_meta_data;
use super::{FieldMetadata, IndexSettings};
use crate::core::{Executor, META_FILEPATH};
use crate::directory::error::OpenReadError;
#[cfg(feature = "mmap")]
use crate::directory::MmapDirectory;
use crate::directory::{Directory, ManagedDirectory, RamDirectory, INDEX_WRITER_LOCK};
use crate::error::{DataCorruption, TantivyError};
use crate::index::{IndexMeta, SegmentId, SegmentMeta, SegmentMetaInventory};
use crate::indexer::index_writer::{
    IndexWriterOptions, MAX_NUM_THREAD, MEMORY_BUDGET_NUM_BYTES_MIN,
};
use crate::indexer::segment_updater::save_metas;
use crate::indexer::{IndexWriter, SingleSegmentIndexWriter};
use crate::reader::{IndexReader, IndexReaderBuilder};
use crate::schema::document::Document;
use crate::schema::{Field, FieldType, Schema};
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::SegmentReader;

fn load_metas(
    directory: &dyn Directory,
    inventory: &SegmentMetaInventory,
) -> crate::Result<IndexMeta> {
    let meta_data = directory.atomic_read(&META_FILEPATH)?;
    let meta_string = String::from_utf8(meta_data).map_err(|_utf8_err| {
        error!("Meta data is not valid utf8.");
        DataCorruption::new(
            META_FILEPATH.to_path_buf(),
            "Meta file does not contain valid utf8 file.".to_string(),
        )
    })?;
    IndexMeta::deserialize(&meta_string, inventory)
        .map_err(|e| {
            DataCorruption::new(
                META_FILEPATH.to_path_buf(),
                format!("Meta file cannot be deserialized. {e:?}. Content: {meta_string:?}"),
            )
        })
        .map_err(From::from)
}

/// Save the index meta file.
/// This operation is atomic :
/// Either
/// - it fails, in which case an error is returned, and the `meta.json` remains untouched,
/// - it succeeds, and `meta.json` is written and flushed.
///
/// This method is not part of tantivy's public API
fn save_new_metas(
    schema: Schema,
    index_settings: IndexSettings,
    directory: &dyn Directory,
) -> crate::Result<()> {
    save_metas(
        &IndexMeta {
            index_settings,
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        },
        directory,
    )?;
    directory.sync_directory()?;
    Ok(())
}

/// IndexBuilder can be used to create an index.
///
/// Use in conjunction with [`SchemaBuilder`][crate::schema::SchemaBuilder].
/// Global index settings can be configured with [`IndexSettings`].
///
/// # Examples
///
/// ```
/// use tantivy::schema::*;
/// use tantivy::{Index, IndexSettings};
///
/// let mut schema_builder = Schema::builder();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let number_field = schema_builder.add_u64_field(
///     "number",
///     NumericOptions::default().set_fast(),
/// );
///
/// let schema = schema_builder.build();
/// let settings = IndexSettings{
///     docstore_blocksize: 100_000,
///     ..Default::default()
/// };
/// let index = Index::builder().schema(schema).settings(settings).create_in_ram();
/// ```
pub struct IndexBuilder {
    schema: Option<Schema>,
    index_settings: IndexSettings,
    tokenizer_manager: TokenizerManager,
    fast_field_tokenizer_manager: TokenizerManager,
}
impl Default for IndexBuilder {
    fn default() -> Self {
        IndexBuilder::new()
    }
}
impl IndexBuilder {
    /// Creates a new `IndexBuilder`
    pub fn new() -> Self {
        Self {
            schema: None,
            index_settings: IndexSettings::default(),
            tokenizer_manager: TokenizerManager::default(),
            fast_field_tokenizer_manager: TokenizerManager::default(),
        }
    }

    /// Set the settings
    #[must_use]
    pub fn settings(mut self, settings: IndexSettings) -> Self {
        self.index_settings = settings;
        self
    }

    /// Set the schema
    #[must_use]
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the tokenizers.
    pub fn tokenizers(mut self, tokenizers: TokenizerManager) -> Self {
        self.tokenizer_manager = tokenizers;
        self
    }

    /// Set the fast field tokenizers.
    pub fn fast_field_tokenizers(mut self, tokenizers: TokenizerManager) -> Self {
        self.fast_field_tokenizer_manager = tokenizers;
        self
    }

    /// Creates a new index using the [`RamDirectory`].
    ///
    /// The index will be allocated in anonymous memory.
    /// This is useful for indexing small set of documents
    /// for instances like unit test or temporary in memory index.
    pub fn create_in_ram(self) -> Result<Index, TantivyError> {
        let ram_directory = RamDirectory::create();
        self.create(ram_directory)
    }

    /// Creates a new index in a given filepath.
    /// The index will use the [`MmapDirectory`].
    ///
    /// If a previous index was in this directory, it returns an
    /// [`TantivyError::IndexAlreadyExists`] error.
    #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(self, directory_path: P) -> crate::Result<Index> {
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::open(directory_path)?);
        if Index::exists(&*mmap_directory)? {
            return Err(TantivyError::IndexAlreadyExists);
        }
        self.create(mmap_directory)
    }

    /// Dragons ahead!!!
    ///
    /// The point of this API is to let users create a simple index with a single segment
    /// and without starting any thread.
    ///
    /// Do not use this method if you are not sure what you are doing.
    ///
    /// It expects an originally empty directory, and will not run any GC operation.
    #[doc(hidden)]
    pub fn single_segment_index_writer<D: Document>(
        self,
        dir: impl Into<Box<dyn Directory>>,
        mem_budget: usize,
    ) -> crate::Result<SingleSegmentIndexWriter<D>> {
        let index = self.create(dir)?;
        let index_simple_writer = SingleSegmentIndexWriter::new(index, mem_budget)?;
        Ok(index_simple_writer)
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the [`MmapDirectory`] in a newly created directory.
    /// The temp directory will be destroyed automatically when the [`Index`] object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the [`MmapDirectory`].
    /// For other unit tests, prefer the [`RamDirectory`], see:
    /// [`IndexBuilder::create_in_ram()`].
    #[cfg(feature = "mmap")]
    pub fn create_from_tempdir(self) -> crate::Result<Index> {
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::create_from_tempdir()?);
        self.create(mmap_directory)
    }

    fn get_expect_schema(&self) -> crate::Result<Schema> {
        self.schema
            .as_ref()
            .cloned()
            .ok_or(TantivyError::IndexBuilderMissingArgument("schema"))
    }

    /// Opens or creates a new index in the provided directory
    pub fn open_or_create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        let dir: Box<dyn Directory> = dir.into();
        if !Index::exists(&*dir)? {
            return self.create(dir);
        }
        let mut index = Index::open(dir)?;
        index.set_tokenizers(self.tokenizer_manager.clone());
        if index.schema() == self.get_expect_schema()? {
            Ok(index)
        } else {
            Err(TantivyError::SchemaError(
                "An index exists but the schema does not match.".to_string(),
            ))
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(_schema) = self.schema.as_ref() {
            Ok(())
        } else {
            Err(TantivyError::InvalidArgument(
                "no schema passed".to_string(),
            ))
        }
    }

    /// Creates a new index given an implementation of the trait `Directory`.
    ///
    /// If a directory previously existed, it will be erased.
    fn create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        self.validate()?;
        let dir = dir.into();
        let directory = ManagedDirectory::wrap(dir)?;
        save_new_metas(
            self.get_expect_schema()?,
            self.index_settings.clone(),
            &directory,
        )?;
        let mut metas = IndexMeta::with_schema(self.get_expect_schema()?);
        metas.index_settings = self.index_settings;
        let mut index = Index::open_from_metas(directory, &metas, SegmentMetaInventory::default());
        index.set_tokenizers(self.tokenizer_manager);
        index.set_fast_field_tokenizers(self.fast_field_tokenizer_manager);
        Ok(index)
    }
}

/// Search Index
#[derive(Clone)]
pub struct Index {
    directory: ManagedDirectory,
    schema: Schema,
    settings: IndexSettings,
    executor: Executor,
    tokenizers: TokenizerManager,
    fast_field_tokenizers: TokenizerManager,
    inventory: SegmentMetaInventory,
}

impl Index {
    /// Creates a new builder.
    pub fn builder() -> IndexBuilder {
        IndexBuilder::new()
    }
    /// Examines the directory to see if it contains an index.
    ///
    /// Effectively, it only checks for the presence of the `meta.json` file.
    pub fn exists(dir: &dyn Directory) -> Result<bool, OpenReadError> {
        dir.exists(&META_FILEPATH)
    }

    /// Accessor to the search executor.
    ///
    /// This pool is used by default when calling `searcher.search(...)`
    /// to perform search on the individual segments.
    ///
    /// By default the executor is single thread, and simply runs in the calling thread.
    pub fn search_executor(&self) -> &Executor {
        &self.executor
    }

    /// Replace the default single thread search executor pool
    /// by a thread pool with a given number of threads.
    pub fn set_multithread_executor(&mut self, num_threads: usize) -> crate::Result<()> {
        self.executor = Executor::multi_thread(num_threads, "tantivy-search-")?;
        Ok(())
    }

    /// Custom thread pool by a outer thread pool.
    pub fn set_executor(&mut self, executor: Executor) {
        self.executor = executor;
    }

    /// Replace the default single thread search executor pool
    /// by a thread pool with as many threads as there are CPUs on the system.
    pub fn set_default_multithread_executor(&mut self) -> crate::Result<()> {
        let default_num_threads = available_parallelism()?.get();
        self.set_multithread_executor(default_num_threads)
    }

    /// Creates a new index using the [`RamDirectory`].
    ///
    /// The index will be allocated in anonymous memory.
    /// This is useful for indexing small set of documents
    /// for instances like unit test or temporary in memory index.
    pub fn create_in_ram(schema: Schema) -> Index {
        IndexBuilder::new().schema(schema).create_in_ram().unwrap()
    }

    /// Creates a new index in a given filepath.
    /// The index will use the [`MmapDirectory`].
    ///
    /// If a previous index was in this directory, then it returns
    /// a [`TantivyError::IndexAlreadyExists`] error.
    #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(
        directory_path: P,
        schema: Schema,
    ) -> crate::Result<Index> {
        IndexBuilder::new()
            .schema(schema)
            .create_in_dir(directory_path)
    }

    /// Opens or creates a new index in the provided directory
    pub fn open_or_create<T: Into<Box<dyn Directory>>>(
        dir: T,
        schema: Schema,
    ) -> crate::Result<Index> {
        let dir = dir.into();
        IndexBuilder::new().schema(schema).open_or_create(dir)
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the [`MmapDirectory`] in a newly created directory.
    /// The temp directory will be destroyed automatically when the [`Index`] object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the [`MmapDirectory`].
    /// For other unit tests, prefer the [`RamDirectory`],
    /// see: [`IndexBuilder::create_in_ram()`].
    #[cfg(feature = "mmap")]
    pub fn create_from_tempdir(schema: Schema) -> crate::Result<Index> {
        IndexBuilder::new().schema(schema).create_from_tempdir()
    }

    /// Creates a new index given an implementation of the trait `Directory`.
    ///
    /// If a directory previously existed, it will be erased.
    pub fn create<T: Into<Box<dyn Directory>>>(
        dir: T,
        schema: Schema,
        settings: IndexSettings,
    ) -> crate::Result<Index> {
        let dir: Box<dyn Directory> = dir.into();
        let mut builder = IndexBuilder::new().schema(schema);
        builder = builder.settings(settings);
        builder.create(dir)
    }

    /// Creates a new index given a directory and an [`IndexMeta`].
    fn open_from_metas(
        directory: ManagedDirectory,
        metas: &IndexMeta,
        inventory: SegmentMetaInventory,
    ) -> Index {
        let schema = metas.schema.clone();
        Index {
            settings: metas.index_settings.clone(),
            directory,
            schema,
            tokenizers: TokenizerManager::default(),
            fast_field_tokenizers: TokenizerManager::default(),
            executor: Executor::single_thread(),
            inventory,
        }
    }

    /// Setter for the tokenizer manager.
    pub fn set_tokenizers(&mut self, tokenizers: TokenizerManager) {
        self.tokenizers = tokenizers;
    }

    /// Accessor for the tokenizer manager.
    pub fn tokenizers(&self) -> &TokenizerManager {
        &self.tokenizers
    }

    /// Setter for the fast field tokenizer manager.
    pub fn set_fast_field_tokenizers(&mut self, tokenizers: TokenizerManager) {
        self.fast_field_tokenizers = tokenizers;
    }

    /// Accessor for the fast field tokenizer manager.
    pub fn fast_field_tokenizer(&self) -> &TokenizerManager {
        &self.fast_field_tokenizers
    }

    /// Get the tokenizer associated with a specific field.
    pub fn tokenizer_for_field(&self, field: Field) -> crate::Result<TextAnalyzer> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let tokenizer_manager: &TokenizerManager = self.tokenizers();
        let indexing_options_opt = match field_type {
            FieldType::JsonObject(options) => options.get_text_indexing_options(),
            FieldType::Str(options) => options.get_indexing_options(),
            _ => {
                return Err(TantivyError::SchemaError(format!(
                    "{:?} is not a text field.",
                    field_entry.name()
                )))
            }
        };
        let indexing_options = indexing_options_opt.ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "No indexing options set for field {field_entry:?}"
            ))
        })?;

        tokenizer_manager
            .get(indexing_options.tokenizer())
            .ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "No Tokenizer found for field {field_entry:?}"
                ))
            })
    }

    /// Create a default [`IndexReader`] for the given index.
    ///
    /// See [`Index.reader_builder()`].
    pub fn reader(&self) -> crate::Result<IndexReader> {
        self.reader_builder().try_into()
    }

    /// Create a [`IndexReader`] for the given index.
    ///
    /// Most project should create at most one reader for a given index.
    /// This method is typically called only once per `Index` instance.
    pub fn reader_builder(&self) -> IndexReaderBuilder {
        IndexReaderBuilder::new(self.clone())
    }

    /// Opens a new directory from an index path.
    #[cfg(feature = "mmap")]
    pub fn open_in_dir<P: AsRef<Path>>(directory_path: P) -> crate::Result<Index> {
        let mmap_directory = MmapDirectory::open(directory_path)?;
        Index::open(mmap_directory)
    }

    /// Returns the list of the segment metas tracked by the index.
    ///
    /// Such segments can of course be part of the index,
    /// but also they could be segments being currently built or in the middle of a merge
    /// operation.
    pub(crate) fn list_all_segment_metas(&self) -> Vec<SegmentMeta> {
        self.inventory.all()
    }

    /// Returns the list of fields that have been indexed in the Index.
    /// The field list includes the field defined in the schema as well as the fields
    /// that have been indexed as a part of a JSON field.
    /// The returned field name is the full field name, including the name of the JSON field.
    ///
    /// The returned field names can be used in queries.
    ///
    /// Notice: If your data contains JSON fields this is **very expensive**, as it requires
    /// browsing through the inverted index term dictionary and the columnar field dictionary.
    ///
    /// Disclaimer: Some fields may not be listed here. For instance, if the schema contains a json
    /// field that is not indexed nor a fast field but is stored, it is possible for the field
    /// to not be listed.
    pub fn fields_metadata(&self) -> crate::Result<Vec<FieldMetadata>> {
        let segments = self.searchable_segments()?;
        let fields_metadata: Vec<Vec<FieldMetadata>> = segments
            .into_iter()
            .map(|segment| SegmentReader::open(&segment)?.fields_metadata())
            .collect::<Result<_, _>>()?;
        Ok(merge_field_meta_data(fields_metadata))
    }

    /// Creates a new segment_meta (Advanced user only).
    ///
    /// As long as the `SegmentMeta` lives, the files associated with the
    /// `SegmentMeta` are guaranteed to not be garbage collected, regardless of
    /// whether the segment is recorded as part of the index or not.
    pub fn new_segment_meta(&self, segment_id: SegmentId, max_doc: u32) -> SegmentMeta {
        self.inventory.new_segment_meta(segment_id, max_doc)
    }

    /// Open the index using the provided directory
    pub fn open<T: Into<Box<dyn Directory>>>(directory: T) -> crate::Result<Index> {
        let directory = directory.into();
        let directory = ManagedDirectory::wrap(directory)?;
        let inventory = SegmentMetaInventory::default();
        let metas = load_metas(&directory, &inventory)?;
        let index = Index::open_from_metas(directory, &metas, inventory);
        Ok(index)
    }

    /// Reads the index meta file from the directory.
    pub fn load_metas(&self) -> crate::Result<IndexMeta> {
        load_metas(self.directory(), &self.inventory)
    }

    /// Open a new index writer with the given options. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// - `options` defines the writer configuration which includes things like buffer sizes,
    ///   indexer threads, etc...
    ///
    /// # Errors
    /// If the lockfile already exists, returns `TantivyError::LockFailure`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub fn writer_with_options<D: Document>(
        &self,
        options: IndexWriterOptions,
    ) -> crate::Result<IndexWriter<D>> {
        let directory_lock = self
            .directory
            .acquire_lock(&INDEX_WRITER_LOCK)
            .map_err(|err| {
                TantivyError::LockFailure(
                    err,
                    Some(
                        "Failed to acquire index lock. If you are using a regular directory, this \
                         means there is already an `IndexWriter` working on this `Directory`, in \
                         this process or in a different process."
                            .to_string(),
                    ),
                )
            })?;

        IndexWriter::new(self, options, directory_lock)
    }

    /// Open a new index writer. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// - `num_threads` defines the number of indexing workers that should work at the same time.
    ///
    /// - `overall_memory_budget_in_bytes` sets the amount of memory allocated for all indexing
    ///   thread.
    ///
    /// Each thread will receive a budget of `overall_memory_budget_in_bytes / num_threads`.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::DirectoryLockBusy` or an `Error::IoError`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub fn writer_with_num_threads<D: Document>(
        &self,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> crate::Result<IndexWriter<D>> {
        let memory_arena_in_bytes_per_thread = overall_memory_budget_in_bytes / num_threads;
        let options = IndexWriterOptions::builder()
            .num_worker_threads(num_threads)
            .memory_budget_per_thread(memory_arena_in_bytes_per_thread)
            .build();
        self.writer_with_options(options)
    }

    /// Helper to create an index writer for tests.
    ///
    /// That index writer only simply has a single thread and a memory budget of 15 MB.
    /// Using a single thread gives us a deterministic allocation of DocId.
    #[cfg(test)]
    pub fn writer_for_tests<D: Document>(&self) -> crate::Result<IndexWriter<D>> {
        self.writer_with_num_threads(1, MEMORY_BUDGET_NUM_BYTES_MIN)
    }

    /// Creates a multithreaded writer
    ///
    /// Tantivy will automatically define the number of threads to use, but
    /// no more than 8 threads.
    /// `overall_memory_arena_in_bytes` is the total target memory usage that will be split
    /// between a given number of threads.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub fn writer<D: Document>(
        &self,
        memory_budget_in_bytes: usize,
    ) -> crate::Result<IndexWriter<D>> {
        let mut num_threads = std::cmp::min(available_parallelism()?.get(), MAX_NUM_THREAD);
        let memory_budget_num_bytes_per_thread = memory_budget_in_bytes / num_threads;
        if memory_budget_num_bytes_per_thread < MEMORY_BUDGET_NUM_BYTES_MIN {
            num_threads = (memory_budget_in_bytes / MEMORY_BUDGET_NUM_BYTES_MIN).max(1);
        }
        self.writer_with_num_threads(num_threads, memory_budget_in_bytes)
    }

    /// Accessor to the index settings
    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    /// Accessor to the index settings
    pub fn settings_mut(&mut self) -> &mut IndexSettings {
        &mut self.settings
    }

    /// Accessor to the index schema
    ///
    /// The schema is actually cloned.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Returns the list of segments that are searchable
    pub fn searchable_segments(&self) -> crate::Result<Vec<Segment>> {
        Ok(self
            .searchable_segment_metas()?
            .into_iter()
            .map(|segment_meta| self.segment(segment_meta))
            .collect())
    }

    #[doc(hidden)]
    pub fn segment(&self, segment_meta: SegmentMeta) -> Segment {
        Segment::for_index(self.clone(), segment_meta)
    }

    /// Creates a new segment.
    pub fn new_segment(&self) -> Segment {
        let segment_meta = self
            .inventory
            .new_segment_meta(SegmentId::generate_random(), 0);
        self.segment(segment_meta)
    }

    /// Return a reference to the index directory.
    pub fn directory(&self) -> &ManagedDirectory {
        &self.directory
    }

    /// Return a mutable reference to the index directory.
    pub fn directory_mut(&mut self) -> &mut ManagedDirectory {
        &mut self.directory
    }

    /// Reads the meta.json and returns the list of
    /// `SegmentMeta` from the last commit.
    pub fn searchable_segment_metas(&self) -> crate::Result<Vec<SegmentMeta>> {
        Ok(self.load_metas()?.segments)
    }

    /// Returns the list of segment ids that are searchable.
    pub fn searchable_segment_ids(&self) -> crate::Result<Vec<SegmentId>> {
        Ok(self
            .searchable_segment_metas()?
            .iter()
            .map(SegmentMeta::id)
            .collect())
    }

    /// Returns the set of corrupted files
    pub fn validate_checksum(&self) -> crate::Result<HashSet<PathBuf>> {
        let managed_files = self.directory.list_managed_files();
        let active_segments_files: HashSet<PathBuf> = self
            .searchable_segment_metas()?
            .iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();
        let active_existing_files: HashSet<&PathBuf> =
            active_segments_files.intersection(&managed_files).collect();

        let mut damaged_files = HashSet::new();
        for path in active_existing_files {
            if !self.directory.validate_checksum(path)? {
                damaged_files.insert((*path).clone());
            }
        }
        Ok(damaged_files)
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({:?})", self.directory)
    }
}
