use std::cell::RefCell;

use crate::docset::DocSet;
use crate::postings::Postings;
use crate::query::BitSetDocSet;
use crate::DocId;

/// Creates a `Posting` that uses the bitset for hits and the docsets for PostingLists.
///
/// It is used for the regex phrase query, where we need the union of a large amount of
/// terms, but need to keep the docsets for the postings.
pub struct BitSetPostingUnion<TDocSet> {
    /// The docsets are required to load positions
    ///
    /// RefCell because we mutate in term_freq
    docsets: RefCell<Vec<TDocSet>>,
    /// The already unionized BitSet of the docsets
    bitset: BitSetDocSet,
}

impl<TDocSet: DocSet> BitSetPostingUnion<TDocSet> {
    pub(crate) fn build(
        docsets: Vec<TDocSet>,
        bitset: BitSetDocSet,
    ) -> BitSetPostingUnion<TDocSet> {
        BitSetPostingUnion {
            docsets: RefCell::new(docsets),
            bitset,
        }
    }
}

impl<TDocSet: Postings> Postings for BitSetPostingUnion<TDocSet> {
    fn term_freq(&self) -> u32 {
        let curr_doc = self.bitset.doc();
        let mut term_freq = 0;
        let mut docsets = self.docsets.borrow_mut();
        for docset in docsets.iter_mut() {
            if docset.doc() < curr_doc {
                docset.seek(curr_doc);
            }
            if docset.doc() == curr_doc {
                term_freq += docset.term_freq();
            }
        }
        term_freq
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let curr_doc = self.bitset.doc();
        let mut docsets = self.docsets.borrow_mut();
        for docset in docsets.iter_mut() {
            if docset.doc() < curr_doc {
                docset.seek(curr_doc);
            }
            if docset.doc() == curr_doc {
                docset.append_positions_with_offset(offset, output);
            }
        }
        debug_assert!(
            !output.is_empty(),
            "this method should only be called if positions are available"
        );
        output.sort_unstable();
        output.dedup();
    }
}

impl<TDocSet: DocSet> DocSet for BitSetPostingUnion<TDocSet> {
    fn advance(&mut self) -> DocId {
        self.bitset.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.bitset.seek(target)
    }

    fn doc(&self) -> DocId {
        self.bitset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.bitset.size_hint()
    }

    fn count_including_deleted(&mut self) -> u32 {
        self.bitset.count_including_deleted()
    }
}
