mod warming;

use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc, Weak};

use arc_swap::ArcSwap;
pub use warming::Warmer;

use self::warming::WarmingState;
use crate::core::searcher::{SearcherGeneration, SearcherInner};
use crate::directory::{Directory, WatchCallback, WatchHandle, META_LOCK};
use crate::store::DOCSTORE_CACHE_CAPACITY;
use crate::{Index, Inventory, Searcher, SegmentReader, TrackedObject};

/// Defines when a new version of the index should be reloaded.
///
/// Regardless of whether you search and index in the same process, tantivy does not necessarily
/// reflects the change that are committed to your index. `ReloadPolicy` precisely helps you define
/// when you want your index to be reloaded.
#[derive(Clone, Copy)]
pub enum ReloadPolicy {
    /// The index is entirely reloaded manually.
    /// All updates of the index should be manual.
    ///
    /// No change is reflected automatically. You are required to call [`IndexReader::reload()`]
    /// manually.
    Manual,
    /// The index is reloaded within milliseconds after a new commit is available.
    /// This is made possible by watching changes in the `meta.json` file.
    OnCommitWithDelay, // TODO add NEAR_REAL_TIME(target_ms)
}

/// [`IndexReader`] builder
///
/// It makes it possible to configure:
/// - [`ReloadPolicy`] defining when new index versions are detected
/// - [`Warmer`] implementations
/// - number of warming threads, for parallelizing warming work
/// - The cache size of the underlying doc store readers.
#[derive(Clone)]
pub struct IndexReaderBuilder {
    reload_policy: ReloadPolicy,
    index: Index,
    warmers: Vec<Weak<dyn Warmer>>,
    num_warming_threads: usize,
    doc_store_cache_num_blocks: usize,
}

impl IndexReaderBuilder {
    #[must_use]
    pub(crate) fn new(index: Index) -> IndexReaderBuilder {
        IndexReaderBuilder {
            reload_policy: ReloadPolicy::OnCommitWithDelay,
            index,
            warmers: Vec::new(),
            num_warming_threads: 1,
            doc_store_cache_num_blocks: DOCSTORE_CACHE_CAPACITY,
        }
    }

    /// Builds the reader.
    ///
    /// Building the reader is a non-trivial operation that requires
    /// to open different segment readers. It may take hundreds of milliseconds
    /// of time and it may return an error.
    pub fn try_into(self) -> crate::Result<IndexReader> {
        let searcher_generation_inventory = Inventory::default();
        let warming_state = WarmingState::new(
            self.num_warming_threads,
            self.warmers,
            searcher_generation_inventory.clone(),
        )?;
        let inner_reader = InnerIndexReader::new(
            self.doc_store_cache_num_blocks,
            self.index,
            warming_state,
            searcher_generation_inventory,
        )?;
        let inner_reader_arc = Arc::new(inner_reader);
        let watch_handle_opt: Option<WatchHandle> = match self.reload_policy {
            ReloadPolicy::Manual => {
                // No need to set anything...
                None
            }
            ReloadPolicy::OnCommitWithDelay => {
                let inner_reader_arc_clone = inner_reader_arc.clone();
                let callback = move || {
                    if let Err(err) = inner_reader_arc_clone.reload() {
                        error!("Error while loading searcher after commit was detected. {err:?}");
                    }
                };
                let watch_handle = inner_reader_arc
                    .index
                    .directory()
                    .watch(WatchCallback::new(callback))?;
                Some(watch_handle)
            }
        };
        Ok(IndexReader {
            inner: inner_reader_arc,
            _watch_handle_opt: watch_handle_opt,
        })
    }

    /// Sets the reload_policy.
    ///
    /// See [`ReloadPolicy`] for more details.
    #[must_use]
    pub fn reload_policy(mut self, reload_policy: ReloadPolicy) -> IndexReaderBuilder {
        self.reload_policy = reload_policy;
        self
    }

    /// Sets the cache size of the doc store readers.
    ///
    /// The doc store readers cache by default DOCSTORE_CACHE_CAPACITY(100) decompressed blocks.
    #[must_use]
    pub fn doc_store_cache_num_blocks(
        mut self,
        doc_store_cache_num_blocks: usize,
    ) -> IndexReaderBuilder {
        self.doc_store_cache_num_blocks = doc_store_cache_num_blocks;
        self
    }

    /// Set the [`Warmer`]s that are invoked when reloading searchable segments.
    #[must_use]
    pub fn warmers(mut self, warmers: Vec<Weak<dyn Warmer>>) -> IndexReaderBuilder {
        self.warmers = warmers;
        self
    }

    /// Sets the number of warming threads.
    ///
    /// This allows parallelizing warming work when there are multiple [`Warmer`] registered with
    /// the [`IndexReader`].
    #[must_use]
    pub fn num_warming_threads(mut self, num_warming_threads: usize) -> IndexReaderBuilder {
        self.num_warming_threads = num_warming_threads;
        self
    }
}

impl TryInto<IndexReader> for IndexReaderBuilder {
    type Error = crate::TantivyError;

    fn try_into(self) -> crate::Result<IndexReader> {
        IndexReaderBuilder::try_into(self)
    }
}

struct InnerIndexReader {
    doc_store_cache_num_blocks: usize,
    index: Index,
    warming_state: WarmingState,
    searcher: arc_swap::ArcSwap<SearcherInner>,
    searcher_generation_counter: Arc<AtomicU64>,
    searcher_generation_inventory: Inventory<SearcherGeneration>,
}

impl InnerIndexReader {
    fn new(
        doc_store_cache_num_blocks: usize,
        index: Index,
        warming_state: WarmingState,
        // The searcher_generation_inventory is not used as source, but as target to track the
        // loaded segments.
        searcher_generation_inventory: Inventory<SearcherGeneration>,
    ) -> crate::Result<Self> {
        let searcher_generation_counter: Arc<AtomicU64> = Default::default();

        let searcher = Self::create_searcher(
            &index,
            doc_store_cache_num_blocks,
            &warming_state,
            &searcher_generation_counter,
            &searcher_generation_inventory,
        )?;
        Ok(InnerIndexReader {
            doc_store_cache_num_blocks,
            index,
            warming_state,
            searcher: ArcSwap::from(searcher),
            searcher_generation_counter,
            searcher_generation_inventory,
        })
    }
    /// Opens the freshest segments [`SegmentReader`].
    ///
    /// This function acquires a lock to prevent GC from removing files
    /// as we are opening our index.
    fn open_segment_readers(index: &Index) -> crate::Result<Vec<SegmentReader>> {
        // Prevents segment files from getting deleted while we are in the process of opening them
        let _meta_lock = index.directory().acquire_lock(&META_LOCK)?;
        let searchable_segments = index.searchable_segments()?;
        let segment_readers = searchable_segments
            .iter()
            .map(SegmentReader::open)
            .collect::<crate::Result<_>>()?;
        Ok(segment_readers)
    }

    fn track_segment_readers_in_inventory(
        segment_readers: &[SegmentReader],
        searcher_generation_counter: &Arc<AtomicU64>,
        searcher_generation_inventory: &Inventory<SearcherGeneration>,
    ) -> TrackedObject<SearcherGeneration> {
        let generation_id = searcher_generation_counter.fetch_add(1, atomic::Ordering::AcqRel);
        let searcher_generation =
            SearcherGeneration::from_segment_readers(segment_readers, generation_id);
        searcher_generation_inventory.track(searcher_generation)
    }

    fn create_searcher(
        index: &Index,
        doc_store_cache_num_blocks: usize,
        warming_state: &WarmingState,
        searcher_generation_counter: &Arc<AtomicU64>,
        searcher_generation_inventory: &Inventory<SearcherGeneration>,
    ) -> crate::Result<Arc<SearcherInner>> {
        let segment_readers = Self::open_segment_readers(index)?;
        let searcher_generation = Self::track_segment_readers_in_inventory(
            &segment_readers,
            searcher_generation_counter,
            searcher_generation_inventory,
        );

        let schema = index.schema();
        let searcher = Arc::new(SearcherInner::new(
            schema,
            index.clone(),
            segment_readers,
            searcher_generation,
            doc_store_cache_num_blocks,
        )?);

        warming_state.warm_new_searcher_generation(&searcher.clone().into())?;
        Ok(searcher)
    }

    fn reload(&self) -> crate::Result<()> {
        let searcher = Self::create_searcher(
            &self.index,
            self.doc_store_cache_num_blocks,
            &self.warming_state,
            &self.searcher_generation_counter,
            &self.searcher_generation_inventory,
        )?;

        self.searcher.store(searcher);

        Ok(())
    }

    fn searcher(&self) -> Searcher {
        self.searcher.load().clone().into()
    }
}

/// `IndexReader` is your entry point to read and search the index.
///
/// It controls when a new version of the index should be loaded and lends
/// you instances of `Searcher` for the last loaded version.
///
/// `IndexReader` just wraps an `Arc`.
#[derive(Clone)]
pub struct IndexReader {
    inner: Arc<InnerIndexReader>,
    _watch_handle_opt: Option<WatchHandle>,
}

impl IndexReader {
    #[cfg(test)]
    pub(crate) fn index(&self) -> Index {
        self.inner.index.clone()
    }

    /// Update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// If you set up the [`ReloadPolicy::OnCommitWithDelay`] (which is the default)
    /// every commit should be rapidly reflected on your `IndexReader` and you should
    /// not need to call `reload()` at all.
    ///
    /// This automatic reload can take 10s of milliseconds to kick in however, and in unit tests
    /// it can be nice to deterministically force the reload of searchers.
    pub fn reload(&self) -> crate::Result<()> {
        self.inner.reload()
    }

    /// Returns a searcher
    ///
    /// This method should be called every single time a search
    /// query is performed.
    ///
    /// The same searcher must be used for a given query, as it ensures
    /// the use of a consistent segment set.
    pub fn searcher(&self) -> Searcher {
        self.inner.searcher()
    }
}
