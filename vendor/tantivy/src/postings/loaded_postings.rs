use crate::docset::{DocSet, TERMINATED};
use crate::postings::{Postings, SegmentPostings};
use crate::DocId;

/// `LoadedPostings` is a `DocSet` and `Postings` implementation.
/// It is used to represent the postings of a term in memory.
/// It is suitable if there are few documents for a term.
///
/// It exists mainly to reduce memory usage.
/// `SegmentPostings` uses 1840 bytes per instance due to its caches.
/// If you need to keep many terms around with few docs, it's cheaper to load all the
/// postings in memory.
///
/// This is relevant for `RegexPhraseQuery`, which may have a lot of
/// terms.
/// E.g. 100_000 terms would need 184MB due to SegmentPostings.
pub struct LoadedPostings {
    doc_ids: Box<[DocId]>,
    position_offsets: Box<[u32]>,
    positions: Box<[u32]>,
    cursor: usize,
}

impl LoadedPostings {
    /// Creates a new `LoadedPostings` from a `SegmentPostings`.
    ///
    /// It will also preload positions, if positions are available in the SegmentPostings.
    pub fn load(segment_postings: &mut SegmentPostings) -> LoadedPostings {
        let num_docs = segment_postings.doc_freq() as usize;
        let mut doc_ids = Vec::with_capacity(num_docs);
        let mut positions = Vec::with_capacity(num_docs);
        let mut position_offsets = Vec::with_capacity(num_docs);
        while segment_postings.doc() != TERMINATED {
            position_offsets.push(positions.len() as u32);
            doc_ids.push(segment_postings.doc());
            segment_postings.append_positions_with_offset(0, &mut positions);
            segment_postings.advance();
        }
        position_offsets.push(positions.len() as u32);
        LoadedPostings {
            doc_ids: doc_ids.into_boxed_slice(),
            positions: positions.into_boxed_slice(),
            position_offsets: position_offsets.into_boxed_slice(),
            cursor: 0,
        }
    }
}

#[cfg(test)]
impl From<(Vec<DocId>, Vec<Vec<u32>>)> for LoadedPostings {
    fn from(doc_ids_and_positions: (Vec<DocId>, Vec<Vec<u32>>)) -> LoadedPostings {
        let mut position_offsets = Vec::new();
        let mut all_positions = Vec::new();
        let (doc_ids, docid_positions) = doc_ids_and_positions;
        for positions in docid_positions {
            position_offsets.push(all_positions.len() as u32);
            all_positions.extend_from_slice(&positions);
        }
        position_offsets.push(all_positions.len() as u32);
        LoadedPostings {
            doc_ids: doc_ids.into_boxed_slice(),
            positions: all_positions.into_boxed_slice(),
            position_offsets: position_offsets.into_boxed_slice(),
            cursor: 0,
        }
    }
}

impl DocSet for LoadedPostings {
    fn advance(&mut self) -> DocId {
        self.cursor += 1;
        if self.cursor >= self.doc_ids.len() {
            self.cursor = self.doc_ids.len();
            return TERMINATED;
        }
        self.doc()
    }

    fn doc(&self) -> DocId {
        if self.cursor >= self.doc_ids.len() {
            return TERMINATED;
        }
        self.doc_ids[self.cursor]
    }

    fn size_hint(&self) -> u32 {
        self.doc_ids.len() as u32
    }
}
impl Postings for LoadedPostings {
    fn term_freq(&self) -> u32 {
        let start = self.position_offsets[self.cursor] as usize;
        let end = self.position_offsets[self.cursor + 1] as usize;
        (end - start) as u32
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let start = self.position_offsets[self.cursor] as usize;
        let end = self.position_offsets[self.cursor + 1] as usize;
        for pos in &self.positions[start..end] {
            output.push(*pos + offset);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use super::*;

    #[test]
    pub fn test_vec_postings() {
        let doc_ids: Vec<DocId> = (0u32..1024u32).map(|e| e * 3).collect();
        let mut postings = LoadedPostings::from((doc_ids, vec![]));
        assert_eq!(postings.doc(), 0u32);
        assert_eq!(postings.advance(), 3u32);
        assert_eq!(postings.doc(), 3u32);
        assert_eq!(postings.seek(14u32), 15u32);
        assert_eq!(postings.doc(), 15u32);
        assert_eq!(postings.seek(300u32), 300u32);
        assert_eq!(postings.doc(), 300u32);
        assert_eq!(postings.seek(6000u32), TERMINATED);
    }

    #[test]
    pub fn test_vec_postings2() {
        let doc_ids: Vec<DocId> = (0u32..1024u32).map(|e| e * 3).collect();
        let mut positions = Vec::new();
        positions.resize(1024, Vec::new());
        positions[0] = vec![1u32, 2u32, 3u32];
        positions[1] = vec![30u32];
        positions[2] = vec![10u32];
        positions[4] = vec![50u32];
        let mut postings = LoadedPostings::from((doc_ids, positions));

        let load = |postings: &mut LoadedPostings| {
            let mut loaded_positions = Vec::new();
            postings.positions(loaded_positions.as_mut());
            loaded_positions
        };
        assert_eq!(postings.doc(), 0u32);
        assert_eq!(load(&mut postings), vec![1u32, 2u32, 3u32]);

        assert_eq!(postings.advance(), 3u32);
        assert_eq!(postings.doc(), 3u32);

        assert_eq!(load(&mut postings), vec![30u32]);

        assert_eq!(postings.seek(14u32), 15u32);
        assert_eq!(postings.doc(), 15u32);
        assert_eq!(postings.seek(300u32), 300u32);
        assert_eq!(postings.doc(), 300u32);
        assert_eq!(postings.seek(6000u32), TERMINATED);
    }
}
