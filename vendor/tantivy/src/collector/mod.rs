//! # Collectors
//!
//! Collectors define the information you want to extract from the documents matching the queries.
//! In tantivy jargon, we call this information your search "fruit".
//!
//! Your fruit could for instance be :
//! - [the count of matching documents](crate::collector::Count)
//! - [the top 10 documents, by relevancy or by a fast field](crate::collector::TopDocs)
//! - [facet counts](FacetCollector)
//!
//! At some point in your code, you will trigger the actual search operation by calling
//! [`Searcher::search()`](crate::Searcher::search).
//! This call will look like this:
//!
//! ```verbatim
//! let fruit = searcher.search(&query, &collector)?;
//! ```
//!
//! Here the type of fruit is actually determined as an associated type of the collector
//! (`Collector::Fruit`).
//!
//!
//! # Combining several collectors
//!
//! A rich search experience often requires to run several collectors on your search query.
//! For instance,
//! - selecting the top-K products matching your query
//! - counting the matching documents
//! - computing several facets
//! - computing statistics about the matching product prices
//!
//! A simple and efficient way to do that is to pass your collectors as one tuple.
//! The resulting `Fruit` will then be a typed tuple with each collector's original fruits
//! in their respective position.
//!
//! ```rust
//! # use tantivy::schema::*;
//! # use tantivy::*;
//! # use tantivy::query::*;
//! use tantivy::collector::{Count, TopDocs};
//! #
//! # fn main() -> tantivy::Result<()> {
//! # let mut schema_builder = Schema::builder();
//! #     let title = schema_builder.add_text_field("title", TEXT);
//! #     let schema = schema_builder.build();
//! #     let index = Index::create_in_ram(schema);
//! #     let mut index_writer = index.writer(15_000_000)?;
//! #       index_writer.add_document(doc!(
//! #       title => "The Name of the Wind",
//! #      ))?;
//! #     index_writer.add_document(doc!(
//! #        title => "The Diary of Muadib",
//! #     ))?;
//! #     index_writer.commit()?;
//! #     let reader = index.reader()?;
//! #     let searcher = reader.searcher();
//! #     let query_parser = QueryParser::for_index(&index, vec![title]);
//! #     let query = query_parser.parse_query("diary")?;
//! let (doc_count, top_docs): (usize, Vec<(Score, DocAddress)>) =
//! searcher.search(&query, &(Count, TopDocs::with_limit(2).order_by_score()))?;
//! #     Ok(())
//! # }
//! ```
//!
//! The `Collector` trait is implemented for up to 4 collectors.
//! If you have more than 4 collectors, you can either group them into
//! tuples of tuples `(a,(b,(c,d)))`, or rely on [`MultiCollector`].
//!
//! # Combining several collectors dynamically
//!
//! Combining collectors into a tuple is a zero-cost abstraction: everything
//! happens as if you had manually implemented a single collector
//! combining all of our features.
//!
//! Unfortunately it requires you to know at compile time your collector types.
//! If on the other hand, the collectors depend on some query parameter,
//! you can rely on [`MultiCollector`]'s.
//!
//!
//! # Implementing your own collectors.
//!
//! See the `custom_collector` example.

use downcast_rs::impl_downcast;

use crate::schema::Schema;
use crate::{DocId, Score, SegmentOrdinal, SegmentReader};

mod count_collector;
pub use self::count_collector::Count;

/// Sort keys
pub mod sort_key;

mod histogram_collector;
pub use histogram_collector::HistogramCollector;

mod multi_collector;
pub use self::multi_collector::{FruitHandle, MultiCollector, MultiFruit};

mod top_collector;
pub use self::top_collector::ComparableDoc;

mod top_score_collector;
pub use self::top_score_collector::{TopDocs, TopNComputer};

mod sort_key_top_collector;
pub use self::sort_key::{SegmentSortKeyComputer, SortKeyComputer};
mod facet_collector;
pub use self::facet_collector::{FacetCollector, FacetCounts};
use crate::query::Weight;

mod docset_collector;
pub use self::docset_collector::DocSetCollector;

mod filter_collector_wrapper;
pub use self::filter_collector_wrapper::{BytesFilterCollector, FilterCollector};

/// `Fruit` is the type for the result of our collection.
/// e.g. `usize` for the `Count` collector.
pub trait Fruit: Send + downcast_rs::Downcast {}

impl<T> Fruit for T where T: Send + downcast_rs::Downcast {}

/// Collectors are in charge of collecting and retaining relevant
/// information from the document found and scored by the query.
///
/// For instance,
///
/// - keeping track of the top 10 best documents
/// - computing a breakdown over a fast field
/// - computing the number of documents matching the query
///
/// Our search index is in fact a collection of segments, so
/// a `Collector` trait is actually more of a factory to instance
/// `SegmentCollector`s for each segments.
///
/// The collection logic itself is in the `SegmentCollector`.
///
/// Segments are not guaranteed to be visited in any specific order.
pub trait Collector: Sync + Send {
    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// Type of the `SegmentCollector` associated with this collector.
    type Child: SegmentCollector;

    /// Returns an error if the schema is not compatible with the collector.
    fn check_schema(&self, _schema: &Schema) -> crate::Result<()> {
        Ok(())
    }

    /// `set_segment` is called before beginning to enumerate
    /// on this segment.
    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child>;

    /// Returns true iff the collector requires to compute scores for documents.
    fn requires_scoring(&self) -> bool;

    /// Combines the fruit associated with the collection of each segments
    /// into one fruit.
    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit>;

    /// Created a segment collector and
    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> crate::Result<<Self::Child as SegmentCollector>::Fruit> {
        let with_scoring = self.requires_scoring();
        let mut segment_collector = self.for_segment(segment_ord, reader)?;
        default_collect_segment_impl(&mut segment_collector, weight, reader, with_scoring)?;
        Ok(segment_collector.harvest())
    }
}

pub(crate) fn default_collect_segment_impl<TSegmentCollector: SegmentCollector>(
    segment_collector: &mut TSegmentCollector,
    weight: &dyn Weight,
    reader: &SegmentReader,
    with_scoring: bool,
) -> crate::Result<()> {
    match (reader.alive_bitset(), with_scoring) {
        (Some(alive_bitset), true) => {
            weight.for_each(reader, &mut |doc, score| {
                if alive_bitset.is_alive(doc) {
                    segment_collector.collect(doc, score);
                }
            })?;
        }
        (Some(alive_bitset), false) => {
            weight.for_each_no_score(reader, &mut |docs| {
                for doc in docs.iter().cloned() {
                    if alive_bitset.is_alive(doc) {
                        segment_collector.collect(doc, 0.0);
                    }
                }
            })?;
        }
        (None, true) => {
            weight.for_each(reader, &mut |doc, score| {
                segment_collector.collect(doc, score);
            })?;
        }
        (None, false) => {
            weight.for_each_no_score(reader, &mut |docs| {
                segment_collector.collect_block(docs);
            })?;
        }
    }
    Ok(())
}

impl<TSegmentCollector: SegmentCollector> SegmentCollector for Option<TSegmentCollector> {
    type Fruit = Option<TSegmentCollector::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score) {
        if let Some(segment_collector) = self {
            segment_collector.collect(doc, score);
        }
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        if let Some(segment_collector) = self {
            segment_collector.collect_block(docs);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.map(|segment_collector| segment_collector.harvest())
    }
}

impl<TCollector: Collector> Collector for Option<TCollector> {
    type Fruit = Option<TCollector::Fruit>;

    type Child = Option<<TCollector as Collector>::Child>;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        if let Some(underlying_collector) = self {
            underlying_collector.check_schema(schema)?;
        }
        Ok(())
    }

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(if let Some(inner) = self {
            let inner_segment_collector = inner.for_segment(segment_local_id, segment)?;
            Some(inner_segment_collector)
        } else {
            None
        })
    }

    fn requires_scoring(&self) -> bool {
        self.as_ref()
            .map(|inner| inner.requires_scoring())
            .unwrap_or(false)
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        if let Some(inner) = self.as_ref() {
            let inner_segment_fruits: Vec<_> = segment_fruits
                .into_iter()
                .flat_map(|fruit_opt| fruit_opt.into_iter())
                .collect();
            let fruit = inner.merge_fruits(inner_segment_fruits)?;
            Ok(Some(fruit))
        } else {
            Ok(None)
        }
    }
}

/// The `SegmentCollector` is the trait in charge of defining the
/// collect operation at the scale of the segment.
///
/// `.collect(doc, score)` will be called for every documents
/// matching the query.
pub trait SegmentCollector: 'static {
    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);

    /// The query pushes the scored document to the collector via this method.
    /// This method is used when the collector does not require scoring.
    ///
    /// See [`COLLECT_BLOCK_BUFFER_LEN`](crate::COLLECT_BLOCK_BUFFER_LEN) for the
    /// buffer size passed to the collector.
    fn collect_block(&mut self, docs: &[DocId]) {
        for doc in docs {
            self.collect(*doc, 0.0);
        }
    }

    /// Extract the fruit of the collection from the `SegmentCollector`.
    fn harvest(self) -> Self::Fruit;
}

// -----------------------------------------------
// Tuple implementations.

impl<Left, Right> Collector for (Left, Right)
where
    Left: Collector,
    Right: Collector,
{
    type Fruit = (Left::Fruit, Right::Fruit);
    type Child = (Left::Child, Right::Child);

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        Ok(())
    }

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let left = self.0.for_segment(segment_local_id, segment)?;
        let right = self.1.for_segment(segment_local_id, segment)?;
        Ok((left, right))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<(Left::Fruit, Right::Fruit)> {
        let mut left_fruits = vec![];
        let mut right_fruits = vec![];
        for (left_fruit, right_fruit) in segment_fruits {
            left_fruits.push(left_fruit);
            right_fruits.push(right_fruit);
        }
        Ok((
            self.0.merge_fruits(left_fruits)?,
            self.1.merge_fruits(right_fruits)?,
        ))
    }
}

impl<Left, Right> SegmentCollector for (Left, Right)
where
    Left: SegmentCollector,
    Right: SegmentCollector,
{
    type Fruit = (Left::Fruit, Right::Fruit);

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        self.0.collect_block(docs);
        self.1.collect_block(docs);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (self.0.harvest(), self.1.harvest())
    }
}

// 3-Tuple

impl<One, Two, Three> Collector for (One, Two, Three)
where
    One: Collector,
    Two: Collector,
    Three: Collector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit);
    type Child = (One::Child, Two::Child, Three::Child);

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        Ok(())
    }

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let one = self.0.for_segment(segment_local_id, segment)?;
        let two = self.1.for_segment(segment_local_id, segment)?;
        let three = self.2.for_segment(segment_local_id, segment)?;
        Ok((one, two, three))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring() || self.2.requires_scoring()
    }

    fn merge_fruits(
        &self,
        children: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut one_fruits = vec![];
        let mut two_fruits = vec![];
        let mut three_fruits = vec![];
        for (one_fruit, two_fruit, three_fruit) in children {
            one_fruits.push(one_fruit);
            two_fruits.push(two_fruit);
            three_fruits.push(three_fruit);
        }
        Ok((
            self.0.merge_fruits(one_fruits)?,
            self.1.merge_fruits(two_fruits)?,
            self.2.merge_fruits(three_fruits)?,
        ))
    }
}

impl<One, Two, Three> SegmentCollector for (One, Two, Three)
where
    One: SegmentCollector,
    Two: SegmentCollector,
    Three: SegmentCollector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit);

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
        self.2.collect(doc, score);
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        self.0.collect_block(docs);
        self.1.collect_block(docs);
        self.2.collect_block(docs);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (self.0.harvest(), self.1.harvest(), self.2.harvest())
    }
}

// 4-Tuple

impl<One, Two, Three, Four> Collector for (One, Two, Three, Four)
where
    One: Collector,
    Two: Collector,
    Three: Collector,
    Four: Collector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit, Four::Fruit);
    type Child = (One::Child, Two::Child, Three::Child, Four::Child);

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        self.3.check_schema(schema)?;
        Ok(())
    }

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let one = self.0.for_segment(segment_local_id, segment)?;
        let two = self.1.for_segment(segment_local_id, segment)?;
        let three = self.2.for_segment(segment_local_id, segment)?;
        let four = self.3.for_segment(segment_local_id, segment)?;
        Ok((one, two, three, four))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
            || self.1.requires_scoring()
            || self.2.requires_scoring()
            || self.3.requires_scoring()
    }

    fn merge_fruits(
        &self,
        children: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut one_fruits = vec![];
        let mut two_fruits = vec![];
        let mut three_fruits = vec![];
        let mut four_fruits = vec![];
        for (one_fruit, two_fruit, three_fruit, four_fruit) in children {
            one_fruits.push(one_fruit);
            two_fruits.push(two_fruit);
            three_fruits.push(three_fruit);
            four_fruits.push(four_fruit);
        }
        Ok((
            self.0.merge_fruits(one_fruits)?,
            self.1.merge_fruits(two_fruits)?,
            self.2.merge_fruits(three_fruits)?,
            self.3.merge_fruits(four_fruits)?,
        ))
    }
}

impl<One, Two, Three, Four> SegmentCollector for (One, Two, Three, Four)
where
    One: SegmentCollector,
    Two: SegmentCollector,
    Three: SegmentCollector,
    Four: SegmentCollector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit, Four::Fruit);

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
        self.2.collect(doc, score);
        self.3.collect(doc, score);
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        self.0.collect_block(docs);
        self.1.collect_block(docs);
        self.2.collect_block(docs);
        self.3.collect_block(docs);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (
            self.0.harvest(),
            self.1.harvest(),
            self.2.harvest(),
            self.3.harvest(),
        )
    }
}

impl_downcast!(Fruit);

#[cfg(test)]
pub(crate) mod tests;
