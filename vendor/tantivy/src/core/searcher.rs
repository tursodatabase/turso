use std::collections::BTreeMap;
use std::sync::Arc;
use std::{fmt, io};

use crate::collector::Collector;
use crate::core::Executor;
use crate::index::{SegmentId, SegmentReader};
use crate::query::{Bm25StatisticsProvider, EnableScoring, Query, Scorer, Weight};
use crate::schema::document::DocumentDeserialize;
use crate::schema::{Schema, Term};
use crate::space_usage::SearcherSpaceUsage;
use crate::store::{CacheStats, StoreReader};
use crate::{DocAddress, Index, Opstamp, TrackedObject};

/// Identifies the searcher generation accessed by a [`Searcher`].
///
/// While this might seem redundant, a [`SearcherGeneration`] contains
/// both a `generation_id` AND a list of `(SegmentId, DeleteOpstamp)`.
///
/// This is on purpose. This object is used by the [`Warmer`](crate::reader::Warmer) API.
/// Having both information makes it possible to identify which
/// artifact should be refreshed or garbage collected.
///
/// Depending on the use case, `Warmer`'s implementers can decide to
/// produce artifacts per:
/// - `generation_id` (e.g. some searcher level aggregates)
/// - `(segment_id, delete_opstamp)` (e.g. segment level aggregates)
/// - `segment_id` (e.g. for immutable document level information)
/// - `(generation_id, segment_id)` (e.g. for consistent dynamic column)
/// - ...
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearcherGeneration {
    segments: BTreeMap<SegmentId, Option<Opstamp>>,
    generation_id: u64,
}

impl SearcherGeneration {
    pub(crate) fn from_segment_readers(
        segment_readers: &[SegmentReader],
        generation_id: u64,
    ) -> Self {
        let mut segment_id_to_del_opstamp = BTreeMap::new();
        for segment_reader in segment_readers {
            segment_id_to_del_opstamp
                .insert(segment_reader.segment_id(), segment_reader.delete_opstamp());
        }
        Self {
            segments: segment_id_to_del_opstamp,
            generation_id,
        }
    }

    /// Returns the searcher generation id.
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }

    /// Return a `(SegmentId -> DeleteOpstamp)` mapping.
    pub fn segments(&self) -> &BTreeMap<SegmentId, Option<Opstamp>> {
        &self.segments
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
#[derive(Clone)]
pub struct Searcher {
    inner: Arc<SearcherInner>,
}

impl Searcher {
    /// Opens an immutable snapshot through asynchronous file handles.
    /// Metadata must already be resident; this does not reload or watch an index.
    /// Dictionaries are loaded for global BM25 statistics, but posting lists,
    /// positions, columns and stored document blocks remain lazy.
    pub async fn open_async(
        index: Index,
        segments: Vec<crate::SegmentMeta>,
        doc_store_cache_num_blocks: usize,
    ) -> crate::Result<Self> {
        let schema = index.schema();
        let mut segment_readers = Vec::with_capacity(segments.len());
        let mut store_readers = Vec::with_capacity(segments.len());
        for meta in segments {
            let reader = SegmentReader::open_async(&index.segment(meta)).await?;
            for (field, entry) in schema.fields() {
                if entry.is_indexed() {
                    reader.inverted_index_async(field).await?;
                }
            }
            store_readers.push(
                reader
                    .get_store_reader_async(doc_store_cache_num_blocks)
                    .await?,
            );
            segment_readers.push(reader);
        }
        let inventory = crate::Inventory::default();
        let generation = inventory.track(SearcherGeneration::from_segment_readers(
            &segment_readers,
            0,
        ));
        Ok(Self {
            inner: Arc::new(SearcherInner {
                schema,
                index,
                segment_readers,
                store_readers,
                generation,
            }),
        })
    }

    /// Searches with suspended scorer construction and CPU-only collection.
    /// The collector must not perform synchronous storage reads of its own.
    pub async fn search_async<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
    ) -> crate::Result<C::Fruit> {
        use crate::collector::SegmentCollector;
        collector.check_schema(self.schema())?;
        let scoring = if collector.requires_scoring() {
            EnableScoring::enabled_from_searcher(self)
        } else {
            EnableScoring::disabled_from_searcher(self)
        };
        let weight = query.weight(scoring)?;
        let mut fruits = Vec::with_capacity(self.segment_readers().len());
        for (ord, reader) in self.segment_readers().iter().enumerate() {
            let mut scorer = weight.scorer_async(reader, 1.0).await?;
            let mut child = collector.for_segment(ord as u32, reader)?;
            while scorer.doc() != crate::TERMINATED {
                let doc = scorer.doc();
                if reader
                    .alive_bitset()
                    .map_or(true, |alive| alive.is_alive(doc))
                {
                    let score = if collector.requires_scoring() {
                        scorer.score()
                    } else {
                        0.0
                    };
                    child.collect(doc, score);
                }
                scorer.advance();
            }
            fruits.push(child.harvest());
        }
        collector.merge_fruits(fruits)
    }

    /// Returns an unordered async hit stream. Only the current segment's
    /// scorer is retained; statistics still cover this entire snapshot.
    pub fn stream(&self, query: &dyn Query, scoring: bool) -> crate::Result<SearchStream> {
        let enabled = if scoring {
            EnableScoring::enabled_from_searcher(self)
        } else {
            EnableScoring::disabled_from_searcher(self)
        };
        Ok(SearchStream {
            searcher: self.clone(),
            weight: query.weight(enabled)?,
            scorer: None,
            segment_ord: 0,
            scoring,
        })
    }

    /// Returns the `Index` associated with the `Searcher`
    pub fn index(&self) -> &Index {
        &self.inner.index
    }

    /// [`SearcherGeneration`] which identifies the version of the snapshot held by this `Searcher`.
    pub fn generation(&self) -> &SearcherGeneration {
        self.inner.generation.as_ref()
    }

    /// Fetches a document from tantivy's store given a [`DocAddress`].
    ///
    /// The searcher uses the segment ordinal to route the
    /// request to the right `Segment`.
    pub fn doc<D: DocumentDeserialize>(&self, doc_address: DocAddress) -> crate::Result<D> {
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get(doc_address.doc_id)
    }

    /// The cache stats for the underlying store reader.
    ///
    /// Aggregates the sum for each segment store reader.
    pub fn doc_store_cache_stats(&self) -> CacheStats {
        let cache_stats: CacheStats = self
            .inner
            .store_readers
            .iter()
            .map(|reader| reader.cache_stats())
            .sum();
        cache_stats
    }

    /// Fetches a document in an asynchronous manner.
    #[cfg(feature = "quickwit")]
    pub async fn doc_async<D: DocumentDeserialize>(
        &self,
        doc_address: DocAddress,
    ) -> crate::Result<D> {
        let executor = self.inner.index.search_executor();
        let store_reader = &self.inner.store_readers[doc_address.segment_ord as usize];
        store_reader.get_async(doc_address.doc_id, executor).await
    }

    /// Access the schema associated with the index of this searcher.
    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.inner
            .segment_readers
            .iter()
            .map(|segment_reader| u64::from(segment_reader.num_docs()))
            .sum::<u64>()
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> crate::Result<u64> {
        let mut total_doc_freq = 0;
        for segment_reader in &self.inner.segment_readers {
            let inverted_index = segment_reader.inverted_index(term.field())?;
            let doc_freq = inverted_index.doc_freq(term)?;
            total_doc_freq += u64::from(doc_freq);
        }
        Ok(total_doc_freq)
    }

    /// Return the overall number of documents containing
    /// the given term in an asynchronous manner.
    #[cfg(feature = "quickwit")]
    pub async fn doc_freq_async(&self, term: &Term) -> crate::Result<u64> {
        let mut total_doc_freq = 0;
        for segment_reader in &self.inner.segment_readers {
            let inverted_index = segment_reader.inverted_index(term.field())?;
            let doc_freq = inverted_index.doc_freq_async(term).await?;
            total_doc_freq += u64::from(doc_freq);
        }
        Ok(total_doc_freq)
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.inner.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ord
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        &self.inner.segment_readers[segment_ord as usize]
    }

    /// Runs a query on the segment readers wrapped by the searcher.
    ///
    /// Search works as follows :
    ///
    ///  First the weight object associated with the query is created.
    ///
    ///  Then, the query loops over the segments and for each segment :
    ///  - setup the collector and informs it that the segment being processed has changed.
    ///  - creates a SegmentCollector for collecting documents associated with the segment
    ///  - creates a `Scorer` object associated for this segment
    ///  - iterate through the matched documents and push them to the segment collector.
    ///
    ///  Finally, the Collector merges each of the child collectors into itself for result usability
    ///  by the caller.
    pub fn search<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
    ) -> crate::Result<C::Fruit> {
        self.search_with_statistics_provider(query, collector, self)
    }

    /// Same as [`search(...)`](Searcher::search) but allows specifying
    /// a [Bm25StatisticsProvider].
    ///
    /// This can be used to adjust the statistics used in computing BM25
    /// scores.
    pub fn search_with_statistics_provider<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
        statistics_provider: &dyn Bm25StatisticsProvider,
    ) -> crate::Result<C::Fruit> {
        let enabled_scoring = if collector.requires_scoring() {
            EnableScoring::enabled_from_statistics_provider(statistics_provider, self)
        } else {
            EnableScoring::disabled_from_searcher(self)
        };
        let executor = self.inner.index.search_executor();
        self.search_with_executor(query, collector, executor, enabled_scoring)
    }

    /// Same as [`search(...)`](Searcher::search) but multithreaded.
    ///
    /// The current implementation is rather naive :
    /// multithreading is by splitting search into as many task
    /// as there are segments.
    ///
    /// It is powerless at making search faster if your index consists in
    /// one large segment.
    ///
    /// Also, keep in mind multithreading a single query on several
    /// threads will not improve your throughput. It can actually
    /// hurt it. It will however, decrease the average response time.
    pub fn search_with_executor<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
        executor: &Executor,
        enabled_scoring: EnableScoring,
    ) -> crate::Result<C::Fruit> {
        let weight = query.weight(enabled_scoring)?;
        collector.check_schema(self.schema())?;
        let segment_readers = self.segment_readers();
        let fruits = executor.map(
            |(segment_ord, segment_reader)| {
                collector.collect_segment(weight.as_ref(), segment_ord as u32, segment_reader)
            },
            segment_readers.iter().enumerate(),
        )?;
        collector.merge_fruits(fruits)
    }

    /// Summarize total space usage of this searcher.
    pub fn space_usage(&self) -> io::Result<SearcherSpaceUsage> {
        let mut space_usage = SearcherSpaceUsage::new();
        for segment_reader in self.segment_readers() {
            space_usage.add_segment(segment_reader.space_usage()?);
        }
        Ok(space_usage)
    }
}

/// Async iteration in segment/document order, without collecting all hits.
/// This bounds scorer residency to one segment, not the scorer's byte size.
/// Await each `next` through completion; a storage error terminates the stream.
pub struct SearchStream {
    searcher: Searcher,
    weight: Box<dyn Weight>,
    scorer: Option<Box<dyn Scorer>>,
    segment_ord: usize,
    scoring: bool,
}

impl SearchStream {
    /// Gets the next live hit, loading the next scorer only when needed.
    pub async fn next(&mut self) -> crate::Result<Option<(crate::Score, DocAddress)>> {
        while self.segment_ord < self.searcher.segment_readers().len() {
            let reader = &self.searcher.segment_readers()[self.segment_ord];
            if self.scorer.is_none() {
                match self.weight.scorer_async(reader, 1.0).await {
                    Ok(scorer) => self.scorer = Some(scorer),
                    Err(error) => {
                        self.segment_ord = self.searcher.segment_readers().len();
                        return Err(error);
                    }
                }
            }
            let scorer = self.scorer.as_mut().unwrap();
            let doc = scorer.doc();
            if doc == crate::TERMINATED {
                self.scorer = None;
                self.segment_ord += 1;
                continue;
            }
            let alive = reader
                .alive_bitset()
                .map_or(true, |alive| alive.is_alive(doc));
            let score = if alive && self.scoring {
                scorer.score()
            } else {
                0.0
            };
            scorer.advance();
            if alive {
                return Ok(Some((score, DocAddress::new(self.segment_ord as u32, doc))));
            }
        }
        Ok(None)
    }
}

impl From<Arc<SearcherInner>> for Searcher {
    fn from(inner: Arc<SearcherInner>) -> Self {
        Searcher { inner }
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
pub(crate) struct SearcherInner {
    schema: Schema,
    index: Index,
    segment_readers: Vec<SegmentReader>,
    store_readers: Vec<StoreReader>,
    generation: TrackedObject<SearcherGeneration>,
}

impl SearcherInner {
    /// Creates a new `Searcher`
    pub(crate) fn new(
        schema: Schema,
        index: Index,
        segment_readers: Vec<SegmentReader>,
        generation: TrackedObject<SearcherGeneration>,
        doc_store_cache_num_blocks: usize,
    ) -> io::Result<SearcherInner> {
        assert_eq!(
            &segment_readers
                .iter()
                .map(|reader| (reader.segment_id(), reader.delete_opstamp()))
                .collect::<BTreeMap<_, _>>(),
            generation.segments(),
            "Set of segments referenced by this Searcher and its SearcherGeneration must match"
        );
        let store_readers: Vec<StoreReader> = segment_readers
            .iter()
            .map(|segment_reader| segment_reader.get_store_reader(doc_store_cache_num_blocks))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(SearcherInner {
            schema,
            index,
            segment_readers,
            store_readers,
            generation,
        })
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_ids = self
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect::<Vec<_>>();
        write!(f, "Searcher({segment_ids:?})")
    }
}
