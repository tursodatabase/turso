use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sstable::TermOrdinal;

use crate::Streamer;

/// The terms of a column with the ordinal of the segment.
pub struct TermsWithSegmentOrd<'a> {
    pub terms: Streamer<'a>,
    pub segment_ord: usize,
}

impl PartialEq for TermsWithSegmentOrd<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.segment_ord == other.segment_ord
    }
}

impl Eq for TermsWithSegmentOrd<'_> {}

impl<'a> PartialOrd for TermsWithSegmentOrd<'a> {
    fn partial_cmp(&self, other: &TermsWithSegmentOrd<'a>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for TermsWithSegmentOrd<'a> {
    fn cmp(&self, other: &TermsWithSegmentOrd<'a>) -> Ordering {
        (&other.terms.key(), &other.segment_ord).cmp(&(&self.terms.key(), &self.segment_ord))
    }
}

/// Given a list of sorted term streams,
/// returns an iterator over sorted unique terms.
///
/// The item yield is actually a pair with
/// - the term
/// - a slice with the ordinal of the segments containing the terms.
pub struct TermMerger<'a> {
    heap: BinaryHeap<TermsWithSegmentOrd<'a>>,
    term_streams_with_segment: Vec<TermsWithSegmentOrd<'a>>,
}

impl<'a> TermMerger<'a> {
    /// Stream of merged term dictionary
    pub fn new(term_streams_with_segment: Vec<TermsWithSegmentOrd<'a>>) -> TermMerger<'a> {
        TermMerger {
            heap: BinaryHeap::new(),
            term_streams_with_segment,
        }
    }

    pub(crate) fn matching_segments<'b: 'a>(
        &'b self,
    ) -> impl 'b + Iterator<Item = (usize, TermOrdinal)> {
        self.term_streams_with_segment
            .iter()
            .map(|heap_item| (heap_item.segment_ord, heap_item.terms.term_ord()))
    }

    fn advance_segments(&mut self) {
        let streamers = &mut self.term_streams_with_segment;
        let heap = &mut self.heap;
        for mut heap_item in streamers.drain(..) {
            if heap_item.terms.advance() {
                heap.push(heap_item);
            }
        }
    }

    /// Advance the term iterator to the next term.
    /// Returns true if there is indeed another term
    /// False if there is none.
    pub fn advance(&mut self) -> bool {
        self.advance_segments();
        match self.heap.pop() {
            Some(head) => {
                self.term_streams_with_segment.push(head);
                while let Some(next_streamer) = self.heap.peek() {
                    if self.term_streams_with_segment[0].terms.key() != next_streamer.terms.key() {
                        break;
                    }
                    let next_heap_it = self.heap.pop().unwrap(); // safe : we peeked beforehand
                    self.term_streams_with_segment.push(next_heap_it);
                }
                true
            }
            _ => false,
        }
    }

    /// Returns the current term.
    ///
    /// This method may be called
    /// if and only if advance() has been called before
    /// and "true" was returned.
    pub fn key(&self) -> &[u8] {
        self.term_streams_with_segment[0].terms.key()
    }
}
