use crate::docset::{DocSet, TERMINATED};
use crate::postings::Postings;
use crate::DocId;

/// A `SimpleUnion` is a `DocSet` that is the union of multiple `DocSet`.
/// Unlike `BufferedUnion`, it doesn't do any horizon precomputation.
///
/// For that reason SimpleUnion is a good choice for queries that skip a lot.
pub struct SimpleUnion<TDocSet> {
    docsets: Vec<TDocSet>,
    doc: DocId,
}

impl<TDocSet: DocSet> SimpleUnion<TDocSet> {
    pub(crate) fn build(mut docsets: Vec<TDocSet>) -> SimpleUnion<TDocSet> {
        docsets.retain(|docset| docset.doc() != TERMINATED);
        let mut docset = SimpleUnion { docsets, doc: 0 };

        docset.initialize_first_doc_id();

        docset
    }

    fn initialize_first_doc_id(&mut self) {
        let mut next_doc = TERMINATED;

        for docset in &self.docsets {
            next_doc = next_doc.min(docset.doc());
        }
        self.doc = next_doc;
    }

    fn advance_to_next(&mut self) -> DocId {
        let mut next_doc = TERMINATED;

        for docset in &mut self.docsets {
            if docset.doc() <= self.doc {
                docset.advance();
            }
            next_doc = next_doc.min(docset.doc());
        }
        self.doc = next_doc;
        self.doc
    }
}

impl<TDocSet: Postings> Postings for SimpleUnion<TDocSet> {
    fn term_freq(&self) -> u32 {
        let mut term_freq = 0;
        for docset in &self.docsets {
            let doc = docset.doc();
            if doc == self.doc {
                term_freq += docset.term_freq();
            }
        }
        term_freq
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        for docset in &mut self.docsets {
            let doc = docset.doc();
            if doc == self.doc {
                docset.append_positions_with_offset(offset, output);
            }
        }
        output.sort_unstable();
        output.dedup();
    }
}

impl<TDocSet: DocSet> DocSet for SimpleUnion<TDocSet> {
    fn advance(&mut self) -> DocId {
        self.advance_to_next();
        self.doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = TERMINATED;
        for docset in &mut self.docsets {
            if docset.doc() < target {
                docset.seek(target);
            }
            if docset.doc() < self.doc {
                self.doc = docset.doc();
            }
        }
        self.doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        // TODO: use estimate_union
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }

    fn cost(&self) -> u64 {
        self.docsets.iter().map(|docset| docset.cost()).sum()
    }

    fn count_including_deleted(&mut self) -> u32 {
        if self.doc == TERMINATED {
            return 0u32;
        }
        let mut count = 1u32;
        while self.advance_to_next() != TERMINATED {
            count += 1;
        }
        count
    }
}
