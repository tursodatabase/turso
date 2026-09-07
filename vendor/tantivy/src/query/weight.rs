use super::Scorer;
use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::index::SegmentReader;
use crate::query::Explanation;
use crate::{DocId, DocSet, Score, TERMINATED};

/// Iterates through all of the documents and scores matched by the DocSet
/// `DocSet`.
pub(crate) fn for_each_scorer<TScorer: Scorer + ?Sized>(
    scorer: &mut TScorer,
    callback: &mut dyn FnMut(DocId, Score),
) {
    let mut doc = scorer.doc();
    while doc != TERMINATED {
        callback(doc, scorer.score());
        doc = scorer.advance();
    }
}

/// Iterates through all of the documents matched by the DocSet
/// `DocSet`.
#[inline]
pub(crate) fn for_each_docset_buffered<T: DocSet + ?Sized>(
    docset: &mut T,
    buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN],
    mut callback: impl FnMut(&[DocId]),
) {
    loop {
        let num_items = docset.fill_buffer(buffer);
        callback(&buffer[..num_items]);
        if num_items != buffer.len() {
            break;
        }
    }
}

/// Calls `callback` with all of the `(doc, score)` for which score
/// is exceeding a given threshold.
///
/// This method is useful for the [`TopDocs`](crate::collector::TopDocs) collector.
/// For all docsets, the blanket implementation has the benefit
/// of prefiltering (doc, score) pairs, avoiding the
/// virtual dispatch cost.
///
/// More importantly, it makes it possible for scorers to implement
/// important optimization (e.g. BlockWAND for union).
pub(crate) fn for_each_pruning_scorer<TScorer: Scorer + ?Sized>(
    scorer: &mut TScorer,
    mut threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    let mut doc = scorer.doc();
    while doc != TERMINATED {
        let score = scorer.score();
        if score > threshold {
            threshold = callback(doc, score);
        }
        doc = scorer.advance();
    }
}

/// A Weight is the specialization of a `Query`
/// for a given set of segments.
///
/// See [`Query`](crate::query::Query).
pub trait Weight: Send + Sync + 'static {
    /// Returns the scorer for the given segment.
    ///
    /// `boost` is a multiplier to apply to the score.
    ///
    /// See [`Query`](crate::query::Query).
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>>;

    /// Returns an [`Explanation`] for the given document.
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation>;

    /// Returns the number documents within the given [`SegmentReader`].
    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if let Some(alive_bitset) = reader.alive_bitset() {
            Ok(scorer.count(alive_bitset))
        } else {
            Ok(scorer.count_including_deleted())
        }
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> crate::Result<()> {
        let mut scorer = self.scorer(reader, 1.0)?;
        for_each_scorer(scorer.as_mut(), callback);
        Ok(())
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> crate::Result<()> {
        let mut docset = self.scorer(reader, 1.0)?;

        let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];
        for_each_docset_buffered(&mut docset, &mut buffer, callback);
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the [`TopDocs`](crate::collector::TopDocs) collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let mut scorer = self.scorer(reader, 1.0)?;
        for_each_pruning_scorer(scorer.as_mut(), threshold, callback);
        Ok(())
    }
}
