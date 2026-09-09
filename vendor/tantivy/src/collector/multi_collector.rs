use std::marker::PhantomData;
use std::ops::Deref;

use super::{Collector, SegmentCollector};
use crate::collector::Fruit;
use crate::schema::Schema;
use crate::{DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

/// MultiFruit keeps Fruits from every nested Collector
pub struct MultiFruit {
    sub_fruits: Vec<Option<Box<dyn Fruit>>>,
}

pub struct CollectorWrapper<TCollector: Collector>(TCollector);

impl<TCollector: Collector> Collector for CollectorWrapper<TCollector> {
    type Fruit = Box<dyn Fruit>;
    type Child = Box<dyn BoxableSegmentCollector>;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)
    }

    fn for_segment(
        &self,
        segment_local_id: u32,
        reader: &SegmentReader,
    ) -> crate::Result<Box<dyn BoxableSegmentCollector>> {
        let child = self.0.for_segment(segment_local_id, reader)?;
        Ok(Box::new(SegmentCollectorWrapper(child)))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn merge_fruits(
        &self,
        children: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Box<dyn Fruit>> {
        let typed_fruit: Vec<<TCollector::Child as SegmentCollector>::Fruit> = children
            .into_iter()
            .map(|untyped_fruit| {
                untyped_fruit
                    .downcast::<<TCollector::Child as SegmentCollector>::Fruit>()
                    .map(|boxed_but_typed| *boxed_but_typed)
                    .map_err(|_| {
                        TantivyError::InvalidArgument("Failed to cast child fruit.".to_string())
                    })
            })
            .collect::<crate::Result<_>>()?;
        let merged_fruit = self.0.merge_fruits(typed_fruit)?;
        Ok(Box::new(merged_fruit))
    }
}

impl SegmentCollector for Box<dyn BoxableSegmentCollector> {
    type Fruit = Box<dyn Fruit>;

    #[inline]
    fn collect(&mut self, doc: u32, score: Score) {
        self.as_mut().collect(doc, score);
    }

    #[inline]
    fn collect_block(&mut self, docs: &[DocId]) {
        self.as_mut().collect_block(docs);
    }

    fn harvest(self) -> Box<dyn Fruit> {
        BoxableSegmentCollector::harvest_from_box(self)
    }
}

pub trait BoxableSegmentCollector {
    fn collect(&mut self, doc: u32, score: Score);
    fn collect_block(&mut self, docs: &[DocId]) {
        for &doc in docs {
            self.collect(doc, 0.0);
        }
    }
    fn harvest_from_box(self: Box<Self>) -> Box<dyn Fruit>;
}

pub struct SegmentCollectorWrapper<TSegmentCollector: SegmentCollector>(TSegmentCollector);

impl<TSegmentCollector: SegmentCollector> BoxableSegmentCollector
    for SegmentCollectorWrapper<TSegmentCollector>
{
    #[inline]
    fn collect(&mut self, doc: u32, score: Score) {
        self.0.collect(doc, score);
    }
    #[inline]
    fn collect_block(&mut self, docs: &[DocId]) {
        self.0.collect_block(docs);
    }

    fn harvest_from_box(self: Box<Self>) -> Box<dyn Fruit> {
        Box::new(self.0.harvest())
    }
}

/// FruitHandle stores reference to the corresponding collector inside MultiCollector
pub struct FruitHandle<TFruit: Fruit> {
    pos: usize,
    _phantom: PhantomData<TFruit>,
}

impl<TFruit: Fruit> FruitHandle<TFruit> {
    /// Extract a typed fruit off a multifruit.
    ///
    /// This function involves downcasting and can panic if the multifruit was
    /// created using faulty code.
    pub fn extract(self, fruits: &mut MultiFruit) -> TFruit {
        let boxed_fruit = fruits.sub_fruits[self.pos].take().expect("");
        *boxed_fruit
            .downcast::<TFruit>()
            .map_err(|_| ())
            .expect("Failed to downcast collector fruit.")
    }
}

/// Multicollector makes it possible to collect on more than one collector.
/// It should only be used for use cases where the Collector types is unknown
/// at compile time.
///
/// If the type of the collectors is known, you can just group yours collectors
/// in a tuple. See the
/// [Combining several collectors section of the collector
/// documentation](./index.html#combining-several-collectors).
///
/// ```rust
/// use tantivy::collector::{Count, TopDocs, MultiCollector};
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
/// let mut index_writer = index.writer(15_000_000)?;
/// index_writer.add_document(doc!(title => "The Name of the Wind"))?;
/// index_writer.add_document(doc!(title => "The Diary of Muadib"))?;
/// index_writer.add_document(doc!(title => "A Dairy Cow"))?;
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl"))?;
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let mut collectors = MultiCollector::new();
/// let top_docs_handle = collectors.add_collector(TopDocs::with_limit(2).order_by_score());
/// let count_handle = collectors.add_collector(Count);
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary").unwrap();
/// let mut multi_fruit = searcher.search(&query, &collectors).unwrap();
///
/// let count = count_handle.extract(&mut multi_fruit);
/// let top_docs = top_docs_handle.extract(&mut multi_fruit);
///
/// assert_eq!(count, 2);
/// assert_eq!(top_docs.len(), 2);
/// # Ok(())
/// # }
/// ```
#[expect(clippy::type_complexity)]
#[derive(Default)]
pub struct MultiCollector<'a> {
    collector_wrappers: Vec<
        Box<dyn Collector<Child = Box<dyn BoxableSegmentCollector>, Fruit = Box<dyn Fruit>> + 'a>,
    >,
}

impl<'a> MultiCollector<'a> {
    /// Create a new `MultiCollector`
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a new collector to our `MultiCollector`.
    pub fn add_collector<'b: 'a, TCollector: Collector + 'b>(
        &mut self,
        collector: TCollector,
    ) -> FruitHandle<TCollector::Fruit> {
        let pos = self.collector_wrappers.len();
        self.collector_wrappers
            .push(Box::new(CollectorWrapper(collector)));
        FruitHandle {
            pos,
            _phantom: PhantomData,
        }
    }
}

impl Collector for MultiCollector<'_> {
    type Fruit = MultiFruit;
    type Child = MultiCollectorChild;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        for collector in &self.collector_wrappers {
            collector.check_schema(schema)?;
        }
        Ok(())
    }

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> crate::Result<MultiCollectorChild> {
        let children = self
            .collector_wrappers
            .iter()
            .map(|collector_wrapper| collector_wrapper.for_segment(segment_local_id, segment))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(MultiCollectorChild { children })
    }

    fn requires_scoring(&self) -> bool {
        self.collector_wrappers
            .iter()
            .map(Deref::deref)
            .any(Collector::requires_scoring)
    }

    fn merge_fruits(&self, segments_multifruits: Vec<MultiFruit>) -> crate::Result<MultiFruit> {
        let mut segment_fruits_list: Vec<Vec<Box<dyn Fruit>>> = (0..self.collector_wrappers.len())
            .map(|_| Vec::with_capacity(segments_multifruits.len()))
            .collect::<Vec<_>>();
        for segment_multifruit in segments_multifruits {
            for (idx, segment_fruit_opt) in segment_multifruit.sub_fruits.into_iter().enumerate() {
                if let Some(segment_fruit) = segment_fruit_opt {
                    segment_fruits_list[idx].push(segment_fruit);
                }
            }
        }
        let sub_fruits = self
            .collector_wrappers
            .iter()
            .zip(segment_fruits_list)
            .map(|(child_collector, segment_fruits)| {
                Ok(Some(child_collector.merge_fruits(segment_fruits)?))
            })
            .collect::<crate::Result<_>>()?;
        Ok(MultiFruit { sub_fruits })
    }
}

pub struct MultiCollectorChild {
    children: Vec<Box<dyn BoxableSegmentCollector>>,
}

impl SegmentCollector for MultiCollectorChild {
    type Fruit = MultiFruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        for child in &mut self.children {
            child.collect(doc, score);
        }
    }

    fn collect_block(&mut self, docs: &[DocId]) {
        for child in &mut self.children {
            child.collect_block(docs);
        }
    }

    fn harvest(self) -> MultiFruit {
        MultiFruit {
            sub_fruits: self
                .children
                .into_iter()
                .map(|child| Some(child.harvest()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::collector::{Count, TopDocs};
    use crate::query::TermQuery;
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{Index, Term};

    #[test]
    fn test_multi_collector() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text=>"abc"))?;
            index_writer.add_document(doc!(text=>"abc abc abc"))?;
            index_writer.add_document(doc!(text=>"abc abc"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text=>""))?;
            index_writer.add_document(doc!(text=>"abc abc abc abc"))?;
            index_writer.add_document(doc!(text=>"abc"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let term = Term::from_field_text(text, "abc");
        let query = TermQuery::new(term, IndexRecordOption::Basic);

        let mut collectors = MultiCollector::new();
        let topdocs_handler = collectors.add_collector(TopDocs::with_limit(2).order_by_score());
        let count_handler = collectors.add_collector(Count);
        let mut multifruits = searcher.search(&query, &collectors).unwrap();

        assert_eq!(count_handler.extract(&mut multifruits), 5);
        assert_eq!(topdocs_handler.extract(&mut multifruits).len(), 2);
        Ok(())
    }
}
