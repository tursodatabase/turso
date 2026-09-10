use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::bm25::Bm25Weight;
use crate::query::phrase_query::{intersection_count, PhraseScorer};
use crate::query::Scorer;
use crate::{DocId, Score};

// MultiPrefix is the larger variant, and also the one we expect most often. PhraseScorer is > 1kB
// though, it would be interesting to slim it down if possible.
#[expect(clippy::large_enum_variant)]
enum PhraseKind<TPostings: Postings> {
    SinglePrefix {
        position_offset: u32,
        postings: TPostings,
        positions: Vec<u32>,
    },
    MultiPrefix(PhraseScorer<TPostings>),
}

impl<TPostings: Postings> PhraseKind<TPostings> {
    fn get_intersection(&mut self) -> &[u32] {
        match self {
            PhraseKind::SinglePrefix {
                position_offset,
                postings,
                positions,
            } => {
                if positions.is_empty() {
                    postings.positions_with_offset(*position_offset, positions);
                }
                positions
            }
            PhraseKind::MultiPrefix(postings) => postings.get_intersection(),
        }
    }
}

impl<TPostings: Postings> DocSet for PhraseKind<TPostings> {
    fn advance(&mut self) -> DocId {
        match self {
            PhraseKind::SinglePrefix {
                postings,
                positions,
                ..
            } => {
                positions.clear();
                postings.advance()
            }
            PhraseKind::MultiPrefix(postings) => postings.advance(),
        }
    }

    fn doc(&self) -> DocId {
        match self {
            PhraseKind::SinglePrefix { postings, .. } => postings.doc(),
            PhraseKind::MultiPrefix(postings) => postings.doc(),
        }
    }

    fn size_hint(&self) -> u32 {
        match self {
            PhraseKind::SinglePrefix { postings, .. } => postings.size_hint(),
            PhraseKind::MultiPrefix(postings) => postings.size_hint(),
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        match self {
            PhraseKind::SinglePrefix {
                postings,
                positions,
                ..
            } => {
                positions.clear();
                postings.seek(target)
            }
            PhraseKind::MultiPrefix(postings) => postings.seek(target),
        }
    }
}

impl<TPostings: Postings> Scorer for PhraseKind<TPostings> {
    #[inline]
    fn score(&mut self) -> Score {
        match self {
            PhraseKind::SinglePrefix { positions, .. } => {
                if positions.is_empty() {
                    0.0
                } else {
                    1.0
                }
            }
            PhraseKind::MultiPrefix(postings) => postings.score(),
        }
    }
}

pub struct PhrasePrefixScorer<TPostings: Postings> {
    phrase_scorer: PhraseKind<TPostings>,
    suffixes: Vec<TPostings>,
    suffix_offset: u32,
    phrase_count: u32,
    suffix_position_buffer: Vec<u32>,
}

impl<TPostings: Postings> PhrasePrefixScorer<TPostings> {
    // If similarity_weight is None, then scoring is disabled.
    pub fn new(
        mut term_postings: Vec<(usize, TPostings)>,
        similarity_weight_opt: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        suffixes: Vec<TPostings>,
        suffix_pos: usize,
    ) -> PhrasePrefixScorer<TPostings> {
        // correct indices so we can merge with our suffix term the PhraseScorer doesn't know about
        let max_offset = term_postings
            .iter()
            .map(|(pos, _)| *pos)
            .chain(std::iter::once(suffix_pos))
            .max()
            .unwrap();

        let phrase_scorer = if term_postings.len() > 1 {
            PhraseKind::MultiPrefix(PhraseScorer::new_with_offset(
                term_postings,
                similarity_weight_opt,
                fieldnorm_reader,
                0,
                1,
            ))
        } else {
            let (pos, postings) = term_postings
                .pop()
                .expect("PhrasePrefixScorer must have at least two terms");
            let offset = suffix_pos - pos;
            PhraseKind::SinglePrefix {
                position_offset: offset as u32,
                postings,
                positions: Vec::with_capacity(100),
            }
        };
        let mut phrase_prefix_scorer = PhrasePrefixScorer {
            phrase_scorer,
            suffixes,
            suffix_offset: (max_offset - suffix_pos) as u32,
            phrase_count: 0,
            suffix_position_buffer: Vec::with_capacity(100),
        };
        if phrase_prefix_scorer.doc() != TERMINATED && !phrase_prefix_scorer.matches_prefix() {
            phrase_prefix_scorer.advance();
        }
        phrase_prefix_scorer
    }

    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    fn matches_prefix(&mut self) -> bool {
        let mut count = 0;
        let current_doc = self.doc();
        let pos_matching = self.phrase_scorer.get_intersection();
        for suffix in &mut self.suffixes {
            if suffix.doc() > current_doc {
                continue;
            }
            let doc = suffix.seek(current_doc);
            if doc == current_doc {
                suffix.positions_with_offset(self.suffix_offset, &mut self.suffix_position_buffer);
                count += intersection_count(pos_matching, &self.suffix_position_buffer);
            }
        }
        self.phrase_count = count as u32;
        count != 0
    }
}

impl<TPostings: Postings> DocSet for PhrasePrefixScorer<TPostings> {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.phrase_scorer.advance();
            if doc == TERMINATED || self.matches_prefix() {
                return doc;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let doc = self.phrase_scorer.seek(target);
        if doc == TERMINATED || self.matches_prefix() {
            return doc;
        }
        self.advance()
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        let seek_res = self.phrase_scorer.seek_danger(target);
        if seek_res != SeekDangerResult::Found {
            return seek_res;
        }
        // The intersection matched. Now let's see if we match the prefix.
        if self.matches_prefix() {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.phrase_scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.phrase_scorer.size_hint()
    }

    fn cost(&self) -> u64 {
        self.phrase_scorer.cost()
    }
}

impl<TPostings: Postings> Scorer for PhrasePrefixScorer<TPostings> {
    #[inline]
    fn score(&mut self) -> Score {
        // TODO modify score??
        self.phrase_scorer.score()
    }
}
