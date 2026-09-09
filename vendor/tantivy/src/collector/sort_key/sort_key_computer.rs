use std::cmp::Ordering;

use crate::collector::sort_key::{Comparator, NaturalComparator};
use crate::collector::sort_key_top_collector::TopBySortKeySegmentCollector;
use crate::collector::{default_collect_segment_impl, SegmentCollector as _, TopNComputer};
use crate::schema::Schema;
use crate::{DocAddress, DocId, Result, Score, SegmentReader};

/// A `SegmentSortKeyComputer` makes it possible to modify the default score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`SortKeyComputer`].
pub trait SegmentSortKeyComputer: 'static {
    /// The final score being emitted.
    type SortKey: 'static + Send + Sync + Clone;

    /// Sort key used by at the segment level by the `SegmentSortKeyComputer`.
    ///
    /// It is typically small like a `u64`, and is meant to be converted
    /// to the final score at the end of the collection of the segment.
    type SegmentSortKey: 'static + Clone + Send + Sync + Clone;

    /// Comparator type.
    type SegmentComparator: Comparator<Self::SegmentSortKey> + 'static;

    /// Returns the segment sort key comparator.
    fn segment_comparator(&self) -> Self::SegmentComparator {
        Self::SegmentComparator::default()
    }

    /// Computes the sort key for the given document and score.
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey;

    /// Computes the sort key and pushes the document in a TopN Computer.
    ///
    /// When using a tuple as the sorting key, the sort key is evaluated in a lazy manner.
    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        let sort_key = self.segment_sort_key(doc, score);
        top_n_computer.push(sort_key, doc);
    }

    /// A SegmentSortKeyComputer maps to a SegmentSortKey, but it can also decide on
    /// its ordering.
    ///
    /// This method must be consistent with the `SortKey` ordering.
    #[inline(always)]
    fn compare_segment_sort_key(
        &self,
        left: &Self::SegmentSortKey,
        right: &Self::SegmentSortKey,
    ) -> Ordering {
        self.segment_comparator().compare(left, right)
    }

    /// Implementing this method makes it possible to avoid computing
    /// a sort_key entirely if we can assess that it won't pass a threshold
    /// with a partial computation.
    ///
    /// This is currently used for lexicographic sorting.
    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        let sort_key = self.segment_sort_key(doc_id, score);
        let cmp = self.compare_segment_sort_key(&sort_key, threshold);
        if cmp == Ordering::Less {
            None
        } else {
            Some((cmp, sort_key))
        }
    }

    /// Convert a segment level sort key into the global sort key.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey;
}

/// `SortKeyComputer` defines the sort key to be used by a TopK Collector.
///
/// The `SortKeyComputer` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the sort key at a segment scale.
pub trait SortKeyComputer: Sync {
    /// The sort key type.
    type SortKey: 'static + Send + Sync + Clone + std::fmt::Debug;
    /// Type of the associated [`SegmentSortKeyComputer`].
    type Child: SegmentSortKeyComputer<SortKey = Self::SortKey>;
    /// Comparator type.
    type Comparator: Comparator<Self::SortKey>
        + Comparator<<Self::Child as SegmentSortKeyComputer>::SegmentSortKey>
        + 'static;

    /// Checks whether the schema is compatible with the sort key computer.
    fn check_schema(&self, _schema: &Schema) -> crate::Result<()> {
        Ok(())
    }

    /// Returns the sort key comparator.
    fn comparator(&self) -> Self::Comparator {
        Self::Comparator::default()
    }

    /// Indicates whether the sort key actually uses the similarity score (by default BM25).
    /// If set to false, the similary score might not be computed (as an optimization),
    /// and the score fed in the segment sort key computer could take any value.
    fn requires_scoring(&self) -> bool {
        false
    }

    /// Sorting by score has a overriding implementation for BM25 scores, using Block-WAND.
    fn collect_segment_top_k(
        &self,
        k: usize,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_ord: u32,
    ) -> crate::Result<Vec<(Self::SortKey, DocAddress)>> {
        let with_scoring = self.requires_scoring();
        let segment_sort_key_computer = self.segment_sort_key_computer(reader)?;
        let topn_computer = TopNComputer::new_with_comparator(k, self.comparator());
        let mut segment_top_key_collector = TopBySortKeySegmentCollector {
            topn_computer,
            segment_ord,
            segment_sort_key_computer,
        };
        default_collect_segment_impl(&mut segment_top_key_collector, weight, reader, with_scoring)?;
        Ok(segment_top_key_collector.harvest())
    }

    /// Builds a child sort key computer for a specific segment.
    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<HeadSortKeyComputer, TailSortKeyComputer> SortKeyComputer
    for (HeadSortKeyComputer, TailSortKeyComputer)
where
    HeadSortKeyComputer: SortKeyComputer,
    TailSortKeyComputer: SortKeyComputer,
{
    type SortKey = (HeadSortKeyComputer::SortKey, TailSortKeyComputer::SortKey);
    type Child = (HeadSortKeyComputer::Child, TailSortKeyComputer::Child);

    type Comparator = (
        HeadSortKeyComputer::Comparator,
        TailSortKeyComputer::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (self.0.comparator(), self.1.comparator())
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((
            self.0.segment_sort_key_computer(segment_reader)?,
            self.1.segment_sort_key_computer(segment_reader)?,
        ))
    }

    /// Checks whether the schema is compatible with the sort key computer.
    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        Ok(())
    }

    /// Indicates whether the sort key actually uses the similarity score (by default BM25).
    /// If set to false, the similary score might not be computed (as an optimization),
    /// and the score fed in the segment sort key computer could take any value.
    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring()
    }
}

impl<HeadSegmentSortKeyComputer, TailSegmentSortKeyComputer> SegmentSortKeyComputer
    for (HeadSegmentSortKeyComputer, TailSegmentSortKeyComputer)
where
    HeadSegmentSortKeyComputer: SegmentSortKeyComputer,
    TailSegmentSortKeyComputer: SegmentSortKeyComputer,
{
    type SortKey = (
        HeadSegmentSortKeyComputer::SortKey,
        TailSegmentSortKeyComputer::SortKey,
    );
    type SegmentSortKey = (
        HeadSegmentSortKeyComputer::SegmentSortKey,
        TailSegmentSortKeyComputer::SegmentSortKey,
    );

    type SegmentComparator = (
        HeadSegmentSortKeyComputer::SegmentComparator,
        TailSegmentSortKeyComputer::SegmentComparator,
    );

    /// A SegmentSortKeyComputer maps to a SegmentSortKey, but it can also decide on
    /// its ordering.
    ///
    /// By default, it uses the natural ordering.
    #[inline]
    fn compare_segment_sort_key(
        &self,
        left: &Self::SegmentSortKey,
        right: &Self::SegmentSortKey,
    ) -> Ordering {
        self.0
            .compare_segment_sort_key(&left.0, &right.0)
            .then_with(|| self.1.compare_segment_sort_key(&left.1, &right.1))
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        let sort_key: Self::SegmentSortKey;
        if let Some(threshold) = &top_n_computer.threshold {
            if let Some((_cmp, lazy_sort_key)) = self.accept_sort_key_lazy(doc, score, threshold) {
                sort_key = lazy_sort_key;
            } else {
                return;
            }
        } else {
            sort_key = self.segment_sort_key(doc, score);
        };
        top_n_computer.append_doc(doc, sort_key);
    }

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let head_sort_key = self.0.segment_sort_key(doc, score);
        let tail_sort_key = self.1.segment_sort_key(doc, score);
        (head_sort_key, tail_sort_key)
    }

    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        let (head_threshold, tail_threshold) = threshold;
        let (head_cmp, head_sort_key) =
            self.0.accept_sort_key_lazy(doc_id, score, head_threshold)?;
        if head_cmp == Ordering::Equal {
            let (tail_cmp, tail_sort_key) =
                self.1.accept_sort_key_lazy(doc_id, score, tail_threshold)?;
            Some((tail_cmp, (head_sort_key, tail_sort_key)))
        } else {
            let tail_sort_key = self.1.segment_sort_key(doc_id, score);
            Some((head_cmp, (head_sort_key, tail_sort_key)))
        }
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let (head_sort_key, tail_sort_key) = sort_key;
        (
            self.0.convert_segment_sort_key(head_sort_key),
            self.1.convert_segment_sort_key(tail_sort_key),
        )
    }
}

/// This struct is used as an adapter to take a sort key computer and map its score to another
/// new sort key.
pub struct MappedSegmentSortKeyComputer<T, PreviousSortKey, NewSortKey> {
    sort_key_computer: T,
    map: fn(PreviousSortKey) -> NewSortKey,
}

impl<T, PreviousScore, NewScore> SegmentSortKeyComputer
    for MappedSegmentSortKeyComputer<T, PreviousScore, NewScore>
where
    T: SegmentSortKeyComputer<SortKey = PreviousScore>,
    PreviousScore: 'static + Clone + Send + Sync,
    NewScore: 'static + Clone + Send + Sync,
{
    type SortKey = NewScore;
    type SegmentSortKey = T::SegmentSortKey;
    type SegmentComparator = T::SegmentComparator;

    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.sort_key_computer.segment_sort_key(doc, score)
    }

    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        self.sort_key_computer
            .accept_sort_key_lazy(doc_id, score, threshold)
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        self.sort_key_computer
            .compute_sort_key_and_collect(doc, score, top_n_computer);
    }

    fn convert_segment_sort_key(&self, segment_sort_key: Self::SegmentSortKey) -> Self::SortKey {
        (self.map)(
            self.sort_key_computer
                .convert_segment_sort_key(segment_sort_key),
        )
    }
}

// We then re-use our (head, tail) implement and our mapper by seeing mapping any tuple (a, b, c,
// ...) as the chain (a, (b, (c, ...)))

impl<SortKeyComputer1, SortKeyComputer2, SortKeyComputer3> SortKeyComputer
    for (SortKeyComputer1, SortKeyComputer2, SortKeyComputer3)
where
    SortKeyComputer1: SortKeyComputer,
    SortKeyComputer2: SortKeyComputer,
    SortKeyComputer3: SortKeyComputer,
{
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
    );
    type Child = MappedSegmentSortKeyComputer<
        <(SortKeyComputer1, (SortKeyComputer2, SortKeyComputer3)) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (SortKeyComputer2::SortKey, SortKeyComputer3::SortKey),
        ),
        Self::SortKey,
    >;

    type Comparator = (
        SortKeyComputer1::Comparator,
        SortKeyComputer2::Comparator,
        SortKeyComputer3::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (
            self.0.comparator(),
            self.1.comparator(),
            self.2.comparator(),
        )
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let map = |(sort_key1, (sort_key2, sort_key3))| (sort_key1, sort_key2, sort_key3);
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (sort_key_computer1, (sort_key_computer2, sort_key_computer3)),
            map,
        })
    }

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring() || self.2.requires_scoring()
    }
}

impl<SortKeyComputer1, SortKeyComputer2, SortKeyComputer3, SortKeyComputer4> SortKeyComputer
    for (
        SortKeyComputer1,
        SortKeyComputer2,
        SortKeyComputer3,
        SortKeyComputer4,
    )
where
    SortKeyComputer1: SortKeyComputer,
    SortKeyComputer2: SortKeyComputer,
    SortKeyComputer3: SortKeyComputer,
    SortKeyComputer4: SortKeyComputer,
{
    type Child = MappedSegmentSortKeyComputer<
        <(
            SortKeyComputer1,
            (SortKeyComputer2, (SortKeyComputer3, SortKeyComputer4)),
        ) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (
                SortKeyComputer2::SortKey,
                (SortKeyComputer3::SortKey, SortKeyComputer4::SortKey),
            ),
        ),
        Self::SortKey,
    >;
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
        SortKeyComputer4::SortKey,
    );
    type Comparator = (
        SortKeyComputer1::Comparator,
        SortKeyComputer2::Comparator,
        SortKeyComputer3::Comparator,
        SortKeyComputer4::Comparator,
    );

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer4 = self.3.segment_sort_key_computer(segment_reader)?;
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (
                sort_key_computer1,
                (sort_key_computer2, (sort_key_computer3, sort_key_computer4)),
            ),
            map: |(sort_key1, (sort_key2, (sort_key3, sort_key4)))| {
                (sort_key1, sort_key2, sort_key3, sort_key4)
            },
        })
    }

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        self.3.check_schema(schema)?;
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
            || self.1.requires_scoring()
            || self.2.requires_scoring()
            || self.3.requires_scoring()
    }
}

impl<F, SegmentF, TSortKey> SortKeyComputer for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> SegmentF,
    SegmentF: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync + std::fmt::Debug,
{
    type SortKey = TSortKey;
    type Child = SegmentF;
    type Comparator = NaturalComparator;

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TSortKey> SegmentSortKeyComputer for F
where
    F: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync,
{
    type SortKey = TSortKey;
    type SegmentSortKey = TSortKey;
    type SegmentComparator = NaturalComparator;

    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> TSortKey {
        (self)(doc)
    }

    /// Convert a segment level score into the global level score.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
    use crate::schema::Schema;
    use crate::{DocId, Index, Order, SegmentReader};

    fn build_test_index() -> Index {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(crate::TantivyDocument::default())
            .unwrap();
        index_writer.commit().unwrap();
        index
    }

    #[test]
    fn test_lazy_score_computer() {
        let score_computer_primary = |_segment_reader: &SegmentReader| |_doc: DocId| 200u32;
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let score_computer_secondary = move |_segment_reader: &SegmentReader| {
            let call_count_new_clone = call_count_clone.clone();
            move |_doc: DocId| {
                call_count_new_clone.fetch_add(1, AtomicOrdering::SeqCst);
                "b"
            }
        };
        let lazy_score_computer = (score_computer_primary, score_computer_secondary);
        let index = build_test_index();
        let searcher = index.reader().unwrap().searcher();
        let mut segment_sort_key_computer = lazy_score_computer
            .segment_sort_key_computer(searcher.segment_reader(0))
            .unwrap();
        let expected_sort_key = (200, "b");
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, "a"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, "c"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 2);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, "a"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 3);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, "c"));
            assert!(sort_key_opt.is_none());
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, "a"));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, "c"));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &expected_sort_key);
            assert_eq!(sort_key_opt, Some((Ordering::Equal, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 5);
        }
    }

    #[test]
    fn test_lazy_score_computer_dynamic_ordering() {
        let score_computer_primary = |_segment_reader: &SegmentReader| |_doc: DocId| 200u32;
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let score_computer_secondary = move |_segment_reader: &SegmentReader| {
            let call_count_new_clone = call_count_clone.clone();
            move |_doc: DocId| {
                call_count_new_clone.fetch_add(1, AtomicOrdering::SeqCst);
                2u32
            }
        };
        let lazy_score_computer = (
            (score_computer_primary, Order::Desc),
            (score_computer_secondary, Order::Asc),
        );
        let index = build_test_index();
        let searcher = index.reader().unwrap().searcher();
        let mut segment_sort_key_computer = lazy_score_computer
            .segment_sort_key_computer(searcher.segment_reader(0))
            .unwrap();
        let expected_sort_key = (200, 2u32);

        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, 1u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, 3u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 2);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, 1u32));
            assert!(sort_key_opt.is_none());
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 3);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, 3u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, 1u32));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, 3u32));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &expected_sort_key);
            assert_eq!(sort_key_opt, Some((Ordering::Equal, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 5);
        }
        assert_eq!(
            segment_sort_key_computer.convert_segment_sort_key(expected_sort_key),
            (200u32, 2u32)
        );
    }
}
