use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use super::Collector;
use crate::collector::sort_key::{
    Comparator, ComparatorEnum, NaturalComparator, ReverseComparator, SortBySimilarityScore,
    SortByStaticFastValue, SortByString,
};
use crate::collector::sort_key_top_collector::TopBySortKeyCollector;
use crate::collector::top_collector::ComparableDoc;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::fastfield::FastValue;
use crate::{DocAddress, DocId, Order, Score, SegmentReader};

/// The `TopDocs` collector keeps track of the top `K` documents
/// sorted by their score.
///
/// The implementation is based on a repeatedly truncating on the median after K * 2 documents
/// with pattern defeating QuickSort.
/// The theoretical complexity for collecting the top `K` out of `N` documents
/// is `O(N + K)`.
///
/// This collector guarantees a stable sorting in case of a tie on the
/// document score/sort key: The document address (`DocAddress`) is used as a tie breaker.
/// In case of a tie on the sort key, documents are always sorted by ascending `DocAddress`.
///
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, DocAddress, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
/// index_writer.add_document(doc!(title => "The Name of the Wind"))?;
/// index_writer.add_document(doc!(title => "The Diary of Muadib"))?;
/// index_writer.add_document(doc!(title => "A Dairy Cow"))?;
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl"))?;
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary")?;
/// let top_docs = searcher.search(&query, &TopDocs::with_limit(2).order_by_score())?;
///
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 1));
/// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
/// # Ok(())
/// # }
/// ```
pub struct TopDocs {
    limit: usize,
    offset: usize,
}

impl fmt::Debug for TopDocs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopDocs(limit={}, offset={})", self.limit, self.offset)
    }
}

impl TopDocs {
    /// Builds a `TopDocs` capturing a given document range.
    ///
    /// The range start..end translates in a limit of `end - start`
    /// and an offset of start.
    pub fn for_doc_range(doc_range: Range<usize>) -> Self {
        TopDocs {
            limit: doc_range.end.saturating_sub(doc_range.start),
            offset: doc_range.start,
        }
    }

    /// Returns the doc range we are trying to capture.
    pub fn doc_range(&self) -> Range<usize> {
        self.offset..self.offset + self.limit
    }

    /// Creates a top score collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopDocs {
        assert_ne!(limit, 0, "Limit must be greater than 0");
        TopDocs { limit, offset: 0 }
    }

    /// Skip the first "offset" documents when collecting.
    ///
    /// This is equivalent to `OFFSET` in MySQL or PostgreSQL and `start` in
    /// Lucene's TopDocsCollector.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tantivy::collector::TopDocs;
    /// use tantivy::query::QueryParser;
    /// use tantivy::schema::{Schema, TEXT};
    /// use tantivy::{doc, DocAddress, Index};
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// let mut schema_builder = Schema::builder();
    /// let title = schema_builder.add_text_field("title", TEXT);
    /// let schema = schema_builder.build();
    /// let index = Index::create_in_ram(schema);
    ///
    /// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// index_writer.add_document(doc!(title => "The Name of the Wind"))?;
    /// index_writer.add_document(doc!(title => "The Diary of Muadib"))?;
    /// index_writer.add_document(doc!(title => "A Dairy Cow"))?;
    /// index_writer.add_document(doc!(title => "The Diary of a Young Girl"))?;
    /// index_writer.add_document(doc!(title => "The Diary of Lena Mukhina"))?;
    /// index_writer.commit()?;
    ///
    /// let reader = index.reader()?;
    /// let searcher = reader.searcher();
    ///
    /// let query_parser = QueryParser::for_index(&index, vec![title]);
    /// let query = query_parser.parse_query("diary")?;
    /// let top_docs = searcher.search(&query, &TopDocs::with_limit(2).and_offset(1).order_by_score())?;
    ///
    /// assert_eq!(top_docs.len(), 2);
    /// assert_eq!(top_docs[0].1, DocAddress::new(0, 4));
    /// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
    /// Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn and_offset(self, offset: usize) -> TopDocs {
        TopDocs {
            limit: self.limit,
            offset,
        }
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast or does not exist, this method returns successfully (it is not
    /// aware of any schema). An error will be returned at the moment of search.
    ///
    /// If the field is a FAST field but not a u64 field, search will return successfully but it
    /// will return returns a monotonic u64-representation (ie. the order is still correct) of
    /// the requested field type.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress, Order};
    /// # use tantivy::query::{Query, QueryParser};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("title", TEXT);
    /// #   let rating = schema_builder.add_u64_field("rating", FAST);
    /// #   let schema = schema_builder.build();
    /// #
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// #   index_writer.add_document(doc!(title => "The Name of the Wind", rating => 92u64))?;
    /// #   index_writer.add_document(doc!(title => "The Diary of Muadib", rating => 97u64))?;
    /// #   index_writer.add_document(doc!(title => "A Dairy Cow", rating => 63u64))?;
    /// #   index_writer.add_document(doc!(title => "The Diary of a Young Girl", rating => 80u64))?;
    /// #   index_writer.commit()?;
    /// #   let reader = index.reader()?;
    /// #   let query = QueryParser::for_index(&index, vec![title]).parse_query("diary")?;
    /// #   let top_docs = docs_sorted_by_rating(&reader.searcher(), &query)?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(Some(97u64), DocAddress::new(0u32, 1)),
    /// #                 (Some(80u64), DocAddress::new(0u32, 3))]);
    /// #   Ok(())
    /// # }
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the u64-`field`
    /// /// given in argument.
    /// fn docs_sorted_by_rating(searcher: &Searcher,
    ///                          query: &dyn Query)
    ///     -> tantivy::Result<Vec<(Option<u64>, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the `rating_field` needs to be a FAST field here.
    ///     let top_books_by_rating = TopDocs
    ///                 ::with_limit(10)
    ///                  .order_by_fast_field("rating", Order::Desc);
    ///
    ///     // ... and here are our documents. Note this is a simple vec.
    ///     // The `u64` in the pair is the value of our fast field for
    ///     // each documents.
    ///     //
    ///     // The vec is sorted decreasingly by `sort_by_field`, and has a
    ///     // length of 10, or less if not enough documents matched the
    ///     // query.
    ///     let resulting_docs: Vec<(Option<u64>, DocAddress)> =
    ///          searcher.search(query, &top_books_by_rating)?;
    ///
    ///     Ok(resulting_docs)
    /// }
    /// ```
    ///
    /// # See also
    ///
    /// To comfortably work with `u64`s, `i64`s, `f64`s, or `date`s, please refer to
    /// the [.order_by_fast_field(...)](TopDocs::order_by_fast_field) method.
    pub fn order_by_u64_field(
        self,
        field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(Option<u64>, DocAddress)>> {
        self.order_by((SortByStaticFastValue::for_field(field), order))
    }

    /// Order docs by decreasing BM25 similarity score.
    pub fn order_by_score(self) -> impl Collector<Fruit = Vec<(Score, DocAddress)>> {
        TopBySortKeyCollector::new(SortBySimilarityScore, self.doc_range())
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast field, or its field type does not match the generic type, this
    /// method does not panic, but an explicit error will be returned at the moment of
    /// collection.
    ///
    /// Note that this method is a generic. The requested fast field type will be often
    /// inferred in your code by the rust compiler.
    ///
    /// Implementation-wise, for performance reason, tantivy will manipulate the u64 representation
    /// of your fast field until the last moment.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress,Order};
    /// # use tantivy::query::{Query, AllQuery};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("company", TEXT);
    /// #   let revenue = schema_builder.add_i64_field("revenue", FAST);
    /// #   let schema = schema_builder.build();
    /// #
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// #   index_writer.add_document(doc!(title => "MadCow Inc.", revenue => 92_000_000i64))?;
    /// #   index_writer.add_document(doc!(title => "Zozo Cow KKK", revenue => 119_000_000i64))?;
    /// #   index_writer.add_document(doc!(title => "Declining Cow", revenue => -63_000_000i64))?;
    /// #   assert!(index_writer.commit().is_ok());
    /// #   let reader = index.reader()?;
    /// #   let top_docs = docs_sorted_by_revenue(&reader.searcher(), &AllQuery, "revenue")?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(Some(119_000_000i64), DocAddress::new(0, 1)),
    /// #                 (Some(92_000_000i64), DocAddress::new(0, 0))]);
    /// #   Ok(())
    /// # }
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the u64-`field`
    /// /// given in argument.
    /// fn docs_sorted_by_revenue(searcher: &Searcher,
    ///                          query: &dyn Query,
    ///                          revenue_field: &str)
    ///     -> tantivy::Result<Vec<(Option<i64>, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the generics parameter that needs to match the
    ///     // type `sort_by_field`. revenue_field here is a FAST i64 field.
    ///     let top_company_by_revenue = TopDocs
    ///                 ::with_limit(2)
    ///                  .order_by_fast_field("revenue", Order::Desc);
    ///
    ///     // ... and here are our documents. Note this is a simple vec.
    ///     // The `i64` in the pair is the value of our fast field for
    ///     // each documents.
    ///     //
    ///     // The vec is sorted decreasingly by `sort_by_field`, and has a
    ///     // length of 10, or less if not enough documents matched the
    ///     // query.
    ///     let resulting_docs: Vec<(Option<i64>, DocAddress)> =
    ///          searcher.search(query, &top_company_by_revenue)?;
    ///
    ///     Ok(resulting_docs)
    /// }
    /// ```
    pub fn order_by_fast_field<TFastValue>(
        self,
        fast_field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(Option<TFastValue>, DocAddress)>>
    where
        TFastValue: FastValue,
        ComparatorEnum: Comparator<Option<TFastValue>>,
    {
        self.order_by((SortByStaticFastValue::for_field(fast_field), order))
    }

    /// Like `order_by_fast_field`, but for a `String` fast field.
    pub fn order_by_string_fast_field(
        self,
        fast_field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(Option<String>, DocAddress)>> {
        let by_string_sort_key_computer = SortByString::for_field(fast_field.to_string());
        self.order_by((by_string_sort_key_computer, order))
    }

    /// Ranks the documents using a sort key.
    pub fn order_by<TSortKey>(
        self,
        sort_key_computer: impl SortKeyComputer<SortKey = TSortKey> + Send + 'static,
    ) -> impl Collector<Fruit = Vec<(TSortKey, DocAddress)>>
    where
        TSortKey: 'static + Clone + Send + Sync + std::fmt::Debug,
    {
        TopBySortKeyCollector::new(sort_key_computer, self.doc_range())
    }

    /// Helper function to tweak the similarity score of documents using a function.
    /// (usually a closure).
    ///
    /// This method offers a convenient way to tweak or replace
    /// the documents score. As suggested by the prototype you can
    /// manually define your own [`SortKeyComputer`]
    /// and pass it as an argument, but there is a much simpler way to
    /// tweak your score: you can use a closure as in the following
    /// example.
    ///
    /// # Example
    ///
    /// Typically, you will want to rely on one or more fast fields,
    /// to alter the original relevance `Score`.
    ///
    /// For instance, in the following, we assume that we are implementing
    /// an e-commerce website that has a fast field called `popularity`
    /// that rates whether a product is typically often bought by users.
    ///
    /// In the following example will will tweak our ranking a bit by
    /// boosting popular products a notch.
    ///
    /// In more serious application, this tweaking could involve running a
    /// learning-to-rank model over various features
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress, DocId, Score};
    /// # use tantivy::query::QueryParser;
    /// use tantivy::SegmentReader;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// fn create_schema() -> Schema {
    ///    let mut schema_builder = Schema::builder();
    ///    schema_builder.add_text_field("product_name", TEXT);
    ///    schema_builder.add_u64_field("popularity", FAST);
    ///    schema_builder.build()
    /// }
    ///
    /// fn create_index() -> tantivy::Result<Index> {
    ///   let schema = create_schema();
    ///   let index = Index::create_in_ram(schema);
    ///   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    ///   let product_name = index.schema().get_field("product_name").unwrap();
    ///   let popularity: Field = index.schema().get_field("popularity").unwrap();
    ///   index_writer.add_document(doc!(product_name => "The Diary of Muadib", popularity => 1u64))?;
    ///   index_writer.add_document(doc!(product_name => "A Dairy Cow", popularity => 10u64))?;
    ///   index_writer.add_document(doc!(product_name => "The Diary of a Young Girl", popularity => 15u64))?;
    ///   index_writer.commit()?;
    ///   Ok(index)
    /// }
    ///
    /// let index = create_index().unwrap();
    /// let product_name = index.schema().get_field("product_name").unwrap();
    /// let popularity: Field = index.schema().get_field("popularity").unwrap();
    ///
    /// let user_query_str = "diary";
    /// let query_parser = QueryParser::for_index(&index, vec![product_name]);
    /// let query = query_parser.parse_query(user_query_str).unwrap();
    ///
    /// // This is where we build our collector with our custom score.
    /// let top_docs_by_custom_score = TopDocs
    ///         ::with_limit(10)
    ///          .tweak_score(move |segment_reader: &SegmentReader| {
    ///             // The argument is a function that returns our scoring
    ///             // function.
    ///             //
    ///             // The point of this "mother" function is to gather all
    ///             // of the segment level information we need for scoring.
    ///             // Typically, fast_fields.
    ///             //
    ///             // In our case, we will get a reader for the popularity
    ///             // fast field. For simplicity we read the first or default value in the fast
    ///             // field.
    ///             let popularity_reader =
    ///                 segment_reader.fast_fields().u64("popularity").unwrap().first_or_default_col(0);
    ///
    ///             // We can now define our actual scoring function
    ///             move |doc: DocId, original_score: Score| {
    ///                 let popularity: u64 = popularity_reader.get_val(doc);
    ///                 // Well.. For the sake of the example we use a simple logarithm
    ///                 // function.
    ///                 let popularity_boost_score = ((2u64 + popularity) as Score).log2();
    ///                 popularity_boost_score * original_score
    ///             }
    ///           });
    /// let reader = index.reader().unwrap();
    /// let searcher = reader.searcher();
    /// // ... and here are our documents. Note this is a simple vec.
    /// // The `Score` in the pair is our tweaked score.
    /// let resulting_docs: Vec<(Score, DocAddress)> =
    ///      searcher.search(&query, &top_docs_by_custom_score).unwrap();
    /// ``
    pub fn tweak_score<F, TSortKey>(
        self,
        sort_key_fn: F,
    ) -> impl Collector<Fruit = Vec<(TSortKey, DocAddress)>>
    where
        F: 'static + Send + Sync,
        TSortKey: 'static + PartialOrd + Clone + Send + Sync + std::fmt::Debug,
        TweakScoreFn<F>: SortKeyComputer<SortKey = TSortKey>,
    {
        self.order_by(TweakScoreFn(sort_key_fn))
    }
}

/// Helper struct to make it possible to define a sort key computer that does not use
/// the similary score from a simple function.
pub struct TweakScoreFn<F>(F);

impl<F, TTweakScoreSortKeyFn, TSortKey> SortKeyComputer for TweakScoreFn<F>
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TTweakScoreSortKeyFn,
    TTweakScoreSortKeyFn: 'static + Fn(DocId, Score) -> TSortKey,
    TweakScoreSegmentSortKeyComputer<TTweakScoreSortKeyFn>:
        SegmentSortKeyComputer<SortKey = TSortKey, SegmentSortKey = TSortKey>,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync + std::fmt::Debug,
{
    type SortKey = TSortKey;
    type Child = TweakScoreSegmentSortKeyComputer<TTweakScoreSortKeyFn>;
    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok({
            TweakScoreSegmentSortKeyComputer {
                sort_key_fn: (self.0)(segment_reader),
            }
        })
    }
}

pub struct TweakScoreSegmentSortKeyComputer<TTweakScoreSortKeyFn> {
    sort_key_fn: TTweakScoreSortKeyFn,
}

impl<TTweakScoreSortKeyFn, TSortKey> SegmentSortKeyComputer
    for TweakScoreSegmentSortKeyComputer<TTweakScoreSortKeyFn>
where
    TTweakScoreSortKeyFn: 'static + Fn(DocId, Score) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync,
{
    type SortKey = TSortKey;
    type SegmentSortKey = TSortKey;
    type SegmentComparator = NaturalComparator;

    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> TSortKey {
        (self.sort_key_fn)(doc, score)
    }

    /// Convert a segment level score into the global level score.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key
    }
}

/// Fast TopN Computation
///
/// Capacity of the vec is 2 * top_n.
/// The buffer is truncated to the top_n elements when it reaches the capacity of the Vec.
/// That means capacity has special meaning and should be carried over when cloning or serializing.
///
/// For TopN == 0, it will be relative expensive.
///
/// The TopNComputer will tiebreak by using ascending `D` (DocId or DocAddress):
/// i.e., in case of a tie on the sort key, the `DocId|DocAddress` are always sorted in
/// ascending order, regardless of the `Comparator` used for the `Score` type.
///
/// NOTE: Items must be `push`ed to the TopNComputer in ascending `DocId|DocAddress` order, as the
/// threshold used to eliminate docs does not include the `DocId` or `DocAddress`: this provides
/// the ascending `DocId|DocAddress` tie-breaking behavior without additional comparisons.
#[derive(Serialize, Deserialize)]
#[serde(from = "TopNComputerDeser<Score, D, C>")]
pub struct TopNComputer<Score, D, C> {
    /// The buffer reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<ComparableDoc<Score, D>>,
    top_n: usize,
    pub(crate) threshold: Option<Score>,
    comparator: C,
}

// Intermediate struct for TopNComputer for deserialization, to keep vec capacity
#[derive(Deserialize)]
struct TopNComputerDeser<Score, D, C> {
    buffer: Vec<ComparableDoc<Score, D>>,
    top_n: usize,
    threshold: Option<Score>,
    comparator: C,
}

impl<Score, D, C> From<TopNComputerDeser<Score, D, C>> for TopNComputer<Score, D, C> {
    fn from(mut value: TopNComputerDeser<Score, D, C>) -> Self {
        let expected_cap = value.top_n.max(1) * 2;
        let current_cap = value.buffer.capacity();
        if current_cap < expected_cap {
            value.buffer.reserve_exact(expected_cap - current_cap);
        } else {
            value.buffer.shrink_to(expected_cap);
        }

        TopNComputer {
            buffer: value.buffer,
            top_n: value.top_n,
            threshold: value.threshold,
            comparator: value.comparator,
        }
    }
}

impl<Score: std::fmt::Debug, D, C> std::fmt::Debug for TopNComputer<Score, D, C>
where C: Comparator<Score>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TopNComputer")
            .field("buffer_len", &self.buffer.len())
            .field("top_n", &self.top_n)
            .field("current_threshold", &self.threshold)
            .field("comparator", &self.comparator)
            .finish()
    }
}

// Custom clone to keep capacity
impl<Score: Clone, D: Clone, C: Clone> Clone for TopNComputer<Score, D, C> {
    fn clone(&self) -> Self {
        let mut buffer_clone = Vec::with_capacity(self.buffer.capacity());
        buffer_clone.extend(self.buffer.iter().cloned());
        TopNComputer {
            buffer: buffer_clone,
            top_n: self.top_n,
            threshold: self.threshold.clone(),
            comparator: self.comparator.clone(),
        }
    }
}

impl<TSortKey, D> TopNComputer<TSortKey, D, ReverseComparator>
where
    D: Ord,
    TSortKey: Clone,
    NaturalComparator: Comparator<TSortKey>,
{
    /// Create a new `TopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new(top_n: usize) -> Self {
        TopNComputer::new_with_comparator(top_n, ReverseComparator)
    }
}

#[inline(always)]
fn compare_for_top_k<TSortKey, D: Ord, C: Comparator<TSortKey>>(
    c: &C,
    lhs: &ComparableDoc<TSortKey, D>,
    rhs: &ComparableDoc<TSortKey, D>,
) -> std::cmp::Ordering {
    c.compare(&lhs.sort_key, &rhs.sort_key)
        .reverse() // Reverse here because we want top K.
        .then_with(|| lhs.doc.cmp(&rhs.doc)) // Regardless of asc/desc, in presence of a tie, we
                                             // sort by doc id
}

impl<TSortKey, D, C> TopNComputer<TSortKey, D, C>
where
    D: Ord,
    TSortKey: Clone,
    C: Comparator<TSortKey>,
{
    /// Create a new `TopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new_with_comparator(top_n: usize, comparator: C) -> Self {
        let vec_cap = top_n.max(1) * 2;
        TopNComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: None,
            comparator,
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    ///
    /// NOTE: `push` must be called in ascending `DocId`/`DocAddress` order.
    #[inline]
    pub fn push(&mut self, sort_key: TSortKey, doc: D) {
        if let Some(last_median) = &self.threshold {
            // See the struct docs for an explanation of why this comparison is strict.
            if self.comparator.compare(&sort_key, last_median) != Ordering::Greater {
                return;
            }
        }
        self.append_doc(doc, sort_key);
    }

    // Append a document to the top n.
    //
    // At this point, we need to have established that the doc is above the threshold.
    #[inline(always)]
    pub(crate) fn append_doc(&mut self, doc: D, sort_key: TSortKey) {
        if self.buffer.len() == self.buffer.capacity() {
            let median = self.truncate_top_n();
            self.threshold = Some(median);
        }
        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is 2.
        let comparable_doc = ComparableDoc { doc, sort_key };
        push_assuming_capacity(comparable_doc, &mut self.buffer);
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) -> TSortKey {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable_by(self.top_n, |lhs, rhs| {
            compare_for_top_k(&self.comparator, lhs, rhs)
        });

        let median_score = median_el.sort_key.clone();
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        median_score
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<ComparableDoc<TSortKey, D>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer
            .sort_unstable_by(|lhs, rhs| compare_for_top_k(&self.comparator, lhs, rhs));
        self.buffer
    }

    /// Returns the top n elements in stored order.
    /// Useful if you do not need the elements in sorted order,
    /// for example when merging the results of multiple segments.
    pub fn into_vec(mut self) -> Vec<ComparableDoc<TSortKey, D>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer
    }
}

// Push an element provided there is enough capacity to do so.
//
// Panics if there is not enough capacity to add an element.
#[inline(always)]
fn push_assuming_capacity<T>(el: T, buf: &mut Vec<T>) {
    let prev_len = buf.len();
    assert!(prev_len < buf.capacity());
    // This is mimicking the current (non-stabilized) implementation in std.
    // SAFETY: we just checked we have enough capacity.
    unsafe {
        let end = buf.as_mut_ptr().add(prev_len);
        std::ptr::write(end, el);
        buf.set_len(prev_len + 1);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{TopDocs, TopNComputer};
    use crate::collector::sort_key::{ComparatorEnum, NaturalComparator, ReverseComparator};
    use crate::collector::top_collector::ComparableDoc;
    use crate::collector::{Collector, DocSetCollector};
    use crate::query::{AllQuery, Query, QueryParser};
    use crate::schema::{Field, Schema, FAST, STORED, TEXT};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::{
        assert_nearly_equals, DateTime, DocAddress, DocId, Index, IndexWriter, Order, Score,
        SegmentReader,
    };

    fn make_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
        index_writer.add_document(doc!(text_field=>"Hello happy tax payer."))?;
        index_writer.add_document(doc!(text_field=>"Droopy says hello happy tax payer"))?;
        index_writer.add_document(doc!(text_field=>"I like Droopy"))?;
        index_writer.commit()?;
        Ok(index)
    }

    fn assert_results_equals(results: &[(Score, DocAddress)], expected: &[(Score, DocAddress)]) {
        for (result, expected) in results.iter().zip(expected.iter()) {
            assert_eq!(result.1, expected.1);
            crate::assert_nearly_equals!(result.0, expected.0);
        }
    }

    #[test]
    fn test_empty_topn_computer() {
        let mut computer: TopNComputer<u32, u32, NaturalComparator> =
            TopNComputer::new_with_comparator(0, NaturalComparator);

        computer.push(1u32, 1u32);
        computer.push(1u32, 2u32);
        computer.push(1u32, 3u32);
        assert!(computer.into_vec().is_empty());
    }

    #[test]
    fn test_topn_computer() {
        let mut computer: TopNComputer<u32, u32, NaturalComparator> =
            TopNComputer::new_with_comparator(2, NaturalComparator);

        computer.push(1u32, 1u32);
        computer.push(2u32, 2u32);
        computer.push(3u32, 3u32);
        computer.push(2u32, 4u32);
        computer.push(1u32, 5u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 3u32,
                    doc: 3u32,
                },
                ComparableDoc {
                    sort_key: 2u32,
                    doc: 2u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_duplicates() {
        let mut computer: TopNComputer<u32, u32, NaturalComparator> =
            TopNComputer::new_with_comparator(2, NaturalComparator);

        computer.push(1u32, 1u32);
        computer.push(1u32, 2u32);
        computer.push(1u32, 3u32);
        computer.push(1u32, 4u32);
        computer.push(1u32, 5u32);

        // In the presence of duplicates, DocIds are always ascending order.
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 1u32,
                    doc: 1u32,
                },
                ComparableDoc {
                    sort_key: 1u32,
                    doc: 2u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_no_panic() {
        for top_n in 0..10 {
            let mut computer: TopNComputer<u32, u32, NaturalComparator> =
                TopNComputer::new_with_comparator(top_n, NaturalComparator);

            for _ in 0..1 + top_n * 2 {
                computer.push(1u32, 1u32);
            }
            let _vals = computer.into_sorted_vec();
        }
    }

    proptest! {
        #[test]
        fn test_topn_computer_asc_prop(
          limit in 0..10_usize,
          mut docs in proptest::collection::vec((0..100_u64, 0..100_u64), 0..100_usize),
        ) {
            // NB: TopNComputer must receive inputs in ascending DocId order.
            docs.sort_by_key(|(_, doc_id)| *doc_id);
            let mut computer: TopNComputer<_, _, ReverseComparator> = TopNComputer::new_with_comparator(limit, ReverseComparator);
            for (feature, doc) in &docs {
                computer.push(*feature, *doc);
            }
            let mut comparable_docs: Vec<ComparableDoc<u64, u64>> =
                docs.into_iter().map(|(sort_key, doc)| ComparableDoc { sort_key, doc }).collect();
            crate::collector::sort_key::tests::sort_hits(&mut comparable_docs, Order::Asc);
            comparable_docs.truncate(limit);
            prop_assert_eq!(
                computer.into_sorted_vec(),
                comparable_docs,
            );
        }
    }

    #[test]
    fn test_top_collector_not_at_capacity_without_offset() -> crate::Result<()> {
        let index = make_index()?;
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax")?;
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()?
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4).order_by_score())?;
        assert_results_equals(
            &score_docs,
            &[
                (0.81221175, DocAddress::new(0u32, 1)),
                (0.5376842, DocAddress::new(0u32, 2)),
                (0.48527452, DocAddress::new(0, 0)),
            ],
        );
        Ok(())
    }

    #[test]
    fn test_top_collector_not_at_capacity_with_offset() {
        let index = make_index().unwrap();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(
                &text_query,
                &TopDocs::with_limit(4).and_offset(2).order_by_score(),
            )
            .unwrap();
        assert_results_equals(&score_docs[..], &[(0.48527452, DocAddress::new(0, 0))]);
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let index = make_index().unwrap();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(2).order_by_score())
            .unwrap();
        assert_results_equals(
            &score_docs,
            &[
                (0.81221175, DocAddress::new(0u32, 1)),
                (0.5376842, DocAddress::new(0u32, 2)),
            ],
        );
    }

    #[test]
    fn test_top_collector_at_capacity_with_offset() {
        let index = make_index().unwrap();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(
                &text_query,
                &TopDocs::with_limit(2).and_offset(1).order_by_score(),
            )
            .unwrap();
        assert_results_equals(
            &score_docs[..],
            &[
                (0.5376842, DocAddress::new(0u32, 2)),
                (0.48527452, DocAddress::new(0, 0)),
            ],
        );
    }

    #[test]
    fn test_top_collector_stable_sorting() {
        let index = make_index().unwrap();

        // using AllQuery to get a constant score
        let searcher = index.reader().unwrap().searcher();

        let page_0 = searcher
            .search(&AllQuery, &TopDocs::with_limit(1).order_by_score())
            .unwrap();

        let page_1 = searcher
            .search(&AllQuery, &TopDocs::with_limit(2).order_by_score())
            .unwrap();

        let page_2 = searcher
            .search(&AllQuery, &TopDocs::with_limit(3).order_by_score())
            .unwrap();

        // precondition for the test to be meaningful: we did get documents
        // with the same score
        assert!(page_0.iter().all(|result| result.0 == page_1[0].0));
        assert!(page_1.iter().all(|result| result.0 == page_1[0].0));
        assert!(page_2.iter().all(|result| result.0 == page_2[0].0));

        // sanity check since we're relying on make_index()
        assert_eq!(page_0.len(), 1);
        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 3);

        assert_eq!(page_1, &page_2[..page_1.len()]);
        assert_eq!(page_0, &page_2[..page_0.len()]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        /// Build multiple segments with equal-scoring docs and verify stable ordering
        /// across pages when increasing limit or offset.
        #[test]
        fn proptest_stable_ordering_across_segments_with_pagination(
            docs_per_segment in proptest::collection::vec(1usize..50, 2..5)
        ) {
            use crate::indexer::NoMergePolicy;

            // Build an index with multiple segments; all docs will have the same score using AllQuery.
            let mut schema_builder = Schema::builder();
            let text = schema_builder.add_text_field("text", TEXT);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut writer = index.writer_for_tests().unwrap();
            writer.set_merge_policy(Box::new(NoMergePolicy));

            for num_docs in &docs_per_segment {
                for _ in 0..*num_docs {
                    writer.add_document(doc!(text => "x")).unwrap();
                }
                writer.commit().unwrap();
            }

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();

            let total_docs: usize = docs_per_segment.iter().sum();
            // Full result set, first assert all scores are identical.
            let full_with_scores: Vec<(Score, DocAddress)> = searcher
                .search(&AllQuery, &TopDocs::with_limit(total_docs).order_by_score())
                .unwrap();
            // Sanity: at least one document was returned.
            prop_assert!(!full_with_scores.is_empty());
            let first_score = full_with_scores[0].0;
            prop_assert!(full_with_scores.iter().all(|(score, _)| *score == first_score));

            // Keep only the addresses for the remaining checks.
            let full: Vec<DocAddress> = full_with_scores
                .into_iter()
                .map(|(_score, addr)| addr)
                .collect();

            // Sanity: we actually created multiple segments and have documents.
            prop_assert!(docs_per_segment.len() >= 2);
            prop_assert!(total_docs >= 2);

            // 1) Increasing limit should preserve prefix ordering.
            for k in 1..=total_docs {
                let page: Vec<DocAddress> = searcher
                    .search(&AllQuery, &TopDocs::with_limit(k).order_by_score())
                    .unwrap()
                    .into_iter()
                    .map(|(_score, addr)| addr)
                    .collect();
                prop_assert_eq!(page, full[..k].to_vec());
            }

            // 2) Offset + limit pages should always match the corresponding slice.
            //    For each offset, check three representative page sizes:
            //    - first page (size 1)
            //    - a middle page (roughly half of remaining)
            //    - the last page (size = remaining)
            for offset in 0..total_docs {
                let remaining = total_docs - offset;

                let assert_page_eq = |limit: usize| -> proptest::test_runner::TestCaseResult {
                    let page: Vec<DocAddress> = searcher
                        .search(&AllQuery, &TopDocs::with_limit(limit).and_offset(offset).order_by_score())
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    prop_assert_eq!(page, full[offset..offset + limit].to_vec());
                    Ok(())
                };

                // Smallest page.
                assert_page_eq(1)?;
                // A middle-sized page (dedupes to 1 if remaining == 1).
                assert_page_eq((remaining / 2).max(1))?;
                // Largest page for this offset.
                assert_page_eq(remaining)?;
            }

            // 3) Concatenating fixed-size pages by offset reproduces the full order.
            for page_size in 1..=total_docs.min(5) {
                let mut concat: Vec<DocAddress> = Vec::new();
                let mut offset = 0;
                while offset < total_docs {
                    let size = page_size.min(total_docs - offset);
                    let page: Vec<DocAddress> = searcher
                        .search(&AllQuery, &TopDocs::with_limit(size).and_offset(offset).order_by_score())
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    concat.extend(page);
                    offset += size;
                }
                // Avoid moving `full` across loop iterations.
                prop_assert_eq!(concat, full.clone());
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        /// Build multiple segments with same-scoring term matches and verify stable ordering
        /// across pages for a real scoring query (TermQuery with identical TF and fieldnorm).
        #[test]
        fn proptest_stable_ordering_across_segments_with_term_query_and_pagination(
            docs_per_segment in proptest::collection::vec(1usize..50, 2..5)
        ) {
            use crate::indexer::NoMergePolicy;
            use crate::schema::IndexRecordOption;
            use crate::query::TermQuery;
            use crate::Term;

            // Build an index with multiple segments; each doc has exactly one token "x",
            // ensuring equal BM25 scores across all matching docs (same TF=1 and fieldnorm=1).
            let mut schema_builder = Schema::builder();
            let text = schema_builder.add_text_field("text", TEXT);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut writer = index.writer_for_tests().unwrap();
            writer.set_merge_policy(Box::new(NoMergePolicy));

            for num_docs in &docs_per_segment {
                for _ in 0..*num_docs {
                    writer.add_document(doc!(text => "x")).unwrap();
                }
                writer.commit().unwrap();
            }

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();

            let total_docs: usize = docs_per_segment.iter().sum();
            let term = Term::from_field_text(text, "x");
            let tq = TermQuery::new(term, IndexRecordOption::WithFreqs);

            // Full result set, first assert all scores are identical across docs.
            let full_with_scores: Vec<(Score, DocAddress)> = searcher
                .search(&tq, &TopDocs::with_limit(total_docs).order_by_score())
                .unwrap();
            // Sanity: at least one document was returned.
            prop_assert!(!full_with_scores.is_empty());
            let first_score = full_with_scores[0].0;
            prop_assert!(full_with_scores.iter().all(|(score, _)| *score == first_score));

            // Keep only the addresses for the remaining checks.
            let full: Vec<DocAddress> = full_with_scores
                .into_iter()
                .map(|(_score, addr)| addr)
                .collect();

            // Sanity: we actually created multiple segments and have documents.
            prop_assert!(docs_per_segment.len() >= 2);
            prop_assert!(total_docs >= 2);

            // 1) Increasing limit should preserve prefix ordering.
            for k in 1..=total_docs {
                let page: Vec<DocAddress> = searcher
                    .search(&tq, &TopDocs::with_limit(k).order_by_score())
                    .unwrap()
                    .into_iter()
                    .map(|(_score, addr)| addr)
                    .collect();
                prop_assert_eq!(page, full[..k].to_vec());
            }

            // 2) Offset + limit pages should always match the corresponding slice.
            //    Check three representative page sizes for each offset: 1, ~half, and remaining.
            for offset in 0..total_docs {
                let remaining = total_docs - offset;

                let assert_page_eq = |limit: usize| -> proptest::test_runner::TestCaseResult {
                    let page: Vec<DocAddress> = searcher
                        .search(&tq, &TopDocs::with_limit(limit).and_offset(offset).order_by_score())
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    prop_assert_eq!(page, full[offset..offset + limit].to_vec());
                    Ok(())
                };

                assert_page_eq(1)?;
                assert_page_eq((remaining / 2).max(1))?;
                assert_page_eq(remaining)?;
            }

            // 3) Concatenating fixed-size pages by offset reproduces the full order.
            for page_size in 1..=total_docs.min(5) {
                let mut concat: Vec<DocAddress> = Vec::new();
                let mut offset = 0;
                while offset < total_docs {
                    let size = page_size.min(total_docs - offset);
                    let page: Vec<DocAddress> = searcher
                        .search(&tq, &TopDocs::with_limit(size).and_offset(offset).order_by_score())
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    concat.extend(page);
                    offset += size;
                }
                prop_assert_eq!(concat, full.clone());
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopDocs::with_limit(0);
    }

    const TITLE: &str = "title";
    const SIZE: &str = "size";

    #[test]
    fn test_top_field_collector_not_at_capacity() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "growler of beer",
                    size => 64u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "pint of beer",
                    size => 16u64,
                ))
                .unwrap();
        });
        let searcher = index.reader()?.searcher();

        let top_collector = TopDocs::with_limit(4).order_by_u64_field(SIZE, Order::Desc);
        let top_docs: Vec<(Option<u64>, DocAddress)> = searcher.search(&query, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (Some(64), DocAddress::new(0, 1)),
                (Some(16), DocAddress::new(0, 2)),
                (Some(12), DocAddress::new(0, 0))
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_datetime() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", TEXT);
        let birthday = schema_builder.add_date_field("birthday", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let pr_birthday = DateTime::from_utc(OffsetDateTime::parse(
            "1898-04-09T00:00:00+00:00",
            &Rfc3339,
        )?);
        index_writer.add_document(doc!(
            name => "Paul Robeson",
            birthday => pr_birthday,
        ))?;
        let mr_birthday = DateTime::from_utc(OffsetDateTime::parse(
            "1947-11-08T00:00:00+00:00",
            &Rfc3339,
        )?);
        index_writer.add_document(doc!(
            name => "Minnie Riperton",
            birthday => mr_birthday,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("birthday", Order::Desc);
        let top_docs: Vec<(Option<DateTime>, DocAddress)> =
            searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (Some(mr_birthday), DocAddress::new(0, 1)),
                (Some(pr_birthday), DocAddress::new(0, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_i64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT);
        let altitude = schema_builder.add_i64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "georgetown",
                altitude =>  -1i64,
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40i64,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("altitude", Order::Desc);
        let top_docs: Vec<(Option<i64>, DocAddress)> =
            searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (Some(40i64), DocAddress::new(0, 1)),
                (Some(-1i64), DocAddress::new(0, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_f64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT);
        let altitude = schema_builder.add_f64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "georgetown",
                altitude =>  -1.0f64,
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40f64,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("altitude", Order::Desc);
        let top_docs: Vec<(Option<f64>, DocAddress)> =
            searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (Some(40f64), DocAddress::new(0, 1)),
                (Some(-1.0f64), DocAddress::new(0, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_string() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "austin",
        ))?;
        index_writer.add_document(doc!(
                city => "greenville",
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
        ))?;
        index_writer.commit()?;

        fn query(
            index: &Index,
            order: Order,
            limit: usize,
            offset: usize,
        ) -> crate::Result<Vec<(Option<String>, DocAddress)>> {
            let searcher = index.reader()?.searcher();
            let top_collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_string_fast_field("city", order);
            searcher.search(&AllQuery, &top_collector)
        }

        assert_eq!(
            &query(&index, Order::Desc, 3, 0)?,
            &[
                (Some("tokyo".to_owned()), DocAddress::new(0, 2)),
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
                (Some("austin".to_owned()), DocAddress::new(0, 0)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Desc, 2, 0)?,
            &[
                (Some("tokyo".to_owned()), DocAddress::new(0, 2)),
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
            ]
        );

        assert_eq!(&query(&index, Order::Desc, 3, 3)?, &[]);

        assert_eq!(
            &query(&index, Order::Desc, 2, 1)?,
            &[
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
                (Some("austin".to_owned()), DocAddress::new(0, 0)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 3, 0)?,
            &[
                (Some("austin".to_owned()), DocAddress::new(0, 0)),
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
                (Some("tokyo".to_owned()), DocAddress::new(0, 2)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 2, 1)?,
            &[
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
                (Some("tokyo".to_owned()), DocAddress::new(0, 2)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 2, 0)?,
            &[
                (Some("austin".to_owned()), DocAddress::new(0, 0)),
                (Some("greenville".to_owned()), DocAddress::new(0, 1)),
            ]
        );

        assert_eq!(&query(&index, Order::Asc, 3, 3)?, &[]);

        Ok(())
    }

    proptest! {
        #[test]
        fn test_top_field_collect_string_prop(
          order in prop_oneof!(Just(Order::Desc), Just(Order::Asc)),
          limit in 1..256_usize,
          offset in 0..256_usize,
          segments_terms in
            proptest::collection::vec(
                proptest::collection::vec(0..32_u8, 1..32_usize),
                0..8_usize,
            )
        ) {
            let mut schema_builder = Schema::builder();
            let city = schema_builder.add_text_field("city", TEXT | FAST);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_for_tests()?;

            // A Vec<Vec<u8>>, where the outer Vec represents segments, and the inner Vec
            // represents terms.
            for segment_terms in segments_terms.into_iter() {
                for term in segment_terms.into_iter() {
                    let term = format!("{term:0>3}");
                    index_writer.add_document(doc!(
                        city => term,
                    ))?;
                }
                index_writer.commit()?;
            }

            let searcher = index.reader()?.searcher();
            let top_n_results = searcher.search(&AllQuery, &TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_string_fast_field("city", order))?;
            let all_results = searcher.search(&AllQuery, &DocSetCollector)?.into_iter().map(|doc_address| {
                // Get the term for this address.
                // NOTE: We can't determine the SegmentIds that will be generated for Segments
                // ahead of time, so we can't pre-compute the expected `DocAddress`es.
                let column = searcher.segment_readers()[doc_address.segment_ord as usize].fast_fields().str("city").unwrap().unwrap();
                let term_ord = column.term_ords(doc_address.doc_id).next().unwrap();
                let mut city = Vec::new();
                column.dictionary().ord_to_term(term_ord, &mut city).unwrap();
                (Some(String::try_from(city).unwrap()), doc_address)
            });

            // Using the TopDocs collector should always be equivalent to sorting, skipping the
            // offset, and then taking the limit.
            let sorted_docs: Vec<_> = {
                let mut comparable_docs: Vec<ComparableDoc<_, _>> =
                    all_results.into_iter().map(|(sort_key, doc)| ComparableDoc { sort_key, doc}).collect();
                crate::collector::sort_key::tests::sort_hits(&mut comparable_docs, order);
                comparable_docs.into_iter().map(|cd| (cd.sort_key, cd.doc)).collect()
            };
            let expected_docs = sorted_docs.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
            prop_assert_eq!(
                expected_docs,
                top_n_results
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_field_does_not_exist() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
        });
        let searcher = index.reader().unwrap().searcher();
        let top_collector = TopDocs::with_limit(4).order_by_u64_field("missing_field", Order::Desc);
        let segment_reader = searcher.segment_reader(0u32);
        top_collector
            .for_segment(0, segment_reader)
            .expect("should panic");
    }

    #[test]
    fn test_field_not_fast_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(size=>1u64))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_u64_field(SIZE, Order::Desc);
        let err = top_collector.for_segment(0, segment).err().unwrap();
        assert!(matches!(err, crate::TantivyError::InvalidArgument(_)));
        Ok(())
    }

    #[test]
    fn test_field_wrong_type() {
        let mut schema_builder = Schema::builder();
        let _size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let top_collector = TopDocs::with_limit(4).order_by_fast_field::<i64>(SIZE, Order::Desc);
        let err = top_collector.check_schema(&schema).err().unwrap();
        assert!(
            matches!(err, crate::TantivyError::SchemaError(msg) if msg == "Field `size` is not a fast field.")
        );
    }

    #[test]
    fn test_sort_key_top_collector_with_offset() -> crate::Result<()> {
        let index = make_index()?;
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax")?;
        let collector = TopDocs::with_limit(2)
            .and_offset(1)
            .order_by(move |_segment_reader: &SegmentReader| move |doc: DocId| doc);
        let score_docs: Vec<(u32, DocAddress)> =
            index.reader()?.searcher().search(&text_query, &collector)?;
        assert_eq!(
            score_docs,
            vec![(1, DocAddress::new(0, 1)), (0, DocAddress::new(0, 0)),]
        );
        Ok(())
    }

    #[test]
    fn test_custom_score_top_collector_with_offset() {
        let index = make_index().unwrap();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let collector = TopDocs::with_limit(2)
            .and_offset(1)
            .order_by(move |_segment_reader: &SegmentReader| move |doc: DocId| doc);
        let score_docs: Vec<(u32, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &collector)
            .unwrap();

        assert_eq!(
            score_docs,
            vec![(1, DocAddress::new(0, 1)), (0, DocAddress::new(0, 0)),]
        );
    }

    fn index(
        query: &str,
        query_field: Field,
        schema: Schema,
        mut doc_adder: impl FnMut(&mut IndexWriter),
    ) -> (Index, Box<dyn Query>) {
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        doc_adder(&mut index_writer);
        index_writer.commit().unwrap();
        let query_parser = QueryParser::for_index(&index, vec![query_field]);
        let query = query_parser.parse_query(query).unwrap();
        (index, query)
    }
    #[test]
    fn test_fast_field_ascending_order() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "growler of beer",
                    size => 64u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "pint of beer",
                    size => 16u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "empty beer",
                ))
                .unwrap();
        });
        let searcher = index.reader()?.searcher();

        let top_collector = TopDocs::with_limit(4).order_by_fast_field(SIZE, Order::Asc);
        let top_docs: Vec<(Option<u64>, DocAddress)> = searcher.search(&query, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (Some(12), DocAddress::new(0, 0)),
                (Some(16), DocAddress::new(0, 2)),
                (Some(64), DocAddress::new(0, 1)),
                (None, DocAddress::new(0, 3)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_topn_computer_desc() {
        let mut computer: TopNComputer<u32, u32, _> =
            TopNComputer::new_with_comparator(2, ComparatorEnum::from(Order::Desc));

        computer.push(1u32, 1u32);
        computer.push(2u32, 2u32);
        computer.push(3u32, 3u32);
        computer.push(2u32, 4u32);
        computer.push(4u32, 5u32);
        computer.push(1u32, 6u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 4u32,
                    doc: 5u32,
                },
                ComparableDoc {
                    sort_key: 3u32,
                    doc: 3u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_asc() {
        let mut computer: TopNComputer<u32, u32, _> =
            TopNComputer::new_with_comparator(2, ComparatorEnum::from(Order::Asc));
        computer.push(1u32, 1u32);
        computer.push(2u32, 2u32);
        computer.push(3u32, 3u32);
        computer.push(2u32, 4u32);
        computer.push(4u32, 5u32);
        computer.push(1u32, 6u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 1u32,
                    doc: 1u32,
                },
                ComparableDoc {
                    sort_key: 1u32,
                    doc: 6u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_option_asc_null_at_the_end() {
        let mut computer: TopNComputer<Option<u32>, u32, _> =
            TopNComputer::new_with_comparator(2, ComparatorEnum::ReverseNoneLower);
        computer.push(Some(1u32), 1u32);
        computer.push(Some(2u32), 2u32);
        computer.push(None, 3u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: Some(1u32),
                    doc: 1u32,
                },
                ComparableDoc {
                    sort_key: Some(2u32),
                    doc: 2u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_option_asc_null_at_the_begining() {
        let mut computer: TopNComputer<Option<u32>, u32, _> =
            TopNComputer::new_with_comparator(2, ComparatorEnum::Reverse);
        computer.push(Some(1u32), 1u32);
        computer.push(Some(2u32), 2u32);
        computer.push(None, 3u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: None,
                    doc: 3u32,
                },
                ComparableDoc {
                    sort_key: Some(1u32),
                    doc: 1u32,
                },
            ]
        );
    }

    #[test]
    fn test_push_assuming_capacity() {
        let mut vec = Vec::with_capacity(2);
        super::push_assuming_capacity(1, &mut vec);
        assert_eq!(&vec, &[1]);
        super::push_assuming_capacity(2, &mut vec);
        assert_eq!(&vec, &[1, 2]);
    }

    #[test]
    #[should_panic]
    fn test_push_assuming_capacity_panics_when_no_cap() {
        let mut vec = Vec::with_capacity(1);
        super::push_assuming_capacity(1, &mut vec);
        assert_eq!(&vec, &[1]);
        super::push_assuming_capacity(2, &mut vec);
    }

    #[test]
    fn test_top_n_computer_not_at_capacity() {
        let mut top_n_computer = TopNComputer::new_with_comparator(4, NaturalComparator);
        top_n_computer.append_doc(1, 0.8);
        top_n_computer.append_doc(3, 0.2);
        top_n_computer.append_doc(5, 0.3);
        assert_eq!(
            &top_n_computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 0.8,
                    doc: 1
                },
                ComparableDoc {
                    sort_key: 0.3,
                    doc: 5
                },
                ComparableDoc {
                    sort_key: 0.2,
                    doc: 3
                },
            ]
        );
    }

    #[test]
    fn test_top_n_computer_at_capacity() {
        let mut top_collector = TopNComputer::new_with_comparator(4, NaturalComparator);
        top_collector.append_doc(1, 0.8);
        top_collector.append_doc(3, 0.2);
        top_collector.append_doc(5, 0.3);
        top_collector.append_doc(7, 0.9);
        top_collector.append_doc(9, -0.2);
        assert_eq!(
            &top_collector.into_sorted_vec(),
            &[
                ComparableDoc {
                    sort_key: 0.9,
                    doc: 7
                },
                ComparableDoc {
                    sort_key: 0.8,
                    doc: 1
                },
                ComparableDoc {
                    sort_key: 0.3,
                    doc: 5
                },
                ComparableDoc {
                    sort_key: 0.2,
                    doc: 3
                },
            ]
        );
    }

    #[test]
    fn test_top_segment_collector_stable_ordering_for_equal_feature() {
        // given that the documents are collected in ascending doc id order,
        // when harvesting we have to guarantee stable sorting in case of a tie
        // on the score
        let doc_ids_collection = [4, 5, 6];
        let score = 3.3f32;

        let mut top_collector_limit_2 = TopNComputer::new_with_comparator(2, NaturalComparator);
        for id in &doc_ids_collection {
            top_collector_limit_2.append_doc(*id, score);
        }

        let mut top_collector_limit_3 = TopNComputer::new_with_comparator(3, NaturalComparator);
        for id in &doc_ids_collection {
            top_collector_limit_3.append_doc(*id, score);
        }

        let docs_limit_2 = top_collector_limit_2.into_sorted_vec();
        let docs_limit_3 = top_collector_limit_3.into_sorted_vec();

        assert_eq!(&docs_limit_2, &docs_limit_3[..2],);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use test::Bencher;

    use super::TopNComputer;
    use crate::collector::sort_key::NaturalComparator;

    #[bench]
    fn bench_top_segment_collector_collect_at_capacity(b: &mut Bencher) {
        let mut top_collector = TopNComputer::new_with_comparator(100, NaturalComparator);

        for i in 0..100 {
            top_collector.append_doc(i, 0.8);
        }

        b.iter(|| {
            for i in 0..100 {
                top_collector.append_doc(i, 0.8);
            }
        });
    }
}
