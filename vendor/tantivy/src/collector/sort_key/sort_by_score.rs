use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer, TopNComputer};
use crate::{DocAddress, DocId, Score};

/// Sort by similarity score.
#[derive(Clone, Debug, Copy)]
pub struct SortBySimilarityScore;

impl SortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;

    type Child = SortBySimilarityScore;

    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(SortBySimilarityScore)
    }

    // Sorting by score is special in that it allows for the Block-Wand optimization.
    fn collect_segment_top_k(
        &self,
        k: usize,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_ord: u32,
    ) -> crate::Result<Vec<(Self::SortKey, DocAddress)>> {
        let mut top_n: TopNComputer<Score, DocId, Self::Comparator> =
            TopNComputer::new_with_comparator(k, self.comparator());

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            top_n.threshold = Some(threshold);
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                top_n.push(score, doc);
                top_n.threshold.unwrap_or(Score::MIN)
            })?;
        }

        Ok(top_n
            .into_vec()
            .into_iter()
            .map(|cid| (cid.sort_key, DocAddress::new(segment_ord, cid.doc)))
            .collect())
    }
}

impl SegmentSortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;
    type SegmentSortKey = Score;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }
}
