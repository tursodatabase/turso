use std::borrow::{Borrow, BorrowMut};

use crate::fastfield::AliveBitSet;
use crate::DocId;

/// Sentinel value returned when a [`DocSet`] has been entirely consumed.
///
/// This is not `u32::MAX` as one would have expected, due to the lack of SSE2 instructions
/// to compare `[u32; 4]`.
pub const TERMINATED: DocId = i32::MAX as u32;

/// The collect_block method on `SegmentCollector` uses a buffer of this size.
/// Passed results to `collect_block` will not exceed this size and will be
/// exactly this size as long as we can fill the buffer.
pub const COLLECT_BLOCK_BUFFER_LEN: usize = 64;

/// Represents an iterable set of sorted doc ids.
pub trait DocSet: Send {
    /// Goes to the next element.
    ///
    /// The DocId of the next element is returned.
    /// In other words we should always have :
    /// ```compile_fail
    /// let doc = docset.advance();
    /// assert_eq!(doc, docset.doc());
    /// ```
    ///
    /// If we reached the end of the `DocSet`, [`TERMINATED`] should be returned.
    ///
    /// Calling `.advance()` on a terminated `DocSet` should be supported, and [`TERMINATED`] should
    /// be returned.
    fn advance(&mut self) -> DocId;

    /// Advances the `DocSet` forward until reaching the target, or going to the
    /// lowest [`DocId`] greater than the target.
    ///
    /// If the end of the `DocSet` is reached, [`TERMINATED`] is returned.
    ///
    /// Calling `.seek(target)` on a terminated `DocSet` is legal. Implementation
    /// of `DocSet` should support it.
    ///
    /// Calling `seek(TERMINATED)` is also legal and is the normal way to consume a `DocSet`.
    ///
    /// `target` has to be larger or equal to `.doc()` when calling `seek`.
    fn seek(&mut self, target: DocId) -> DocId {
        let mut doc = self.doc();
        debug_assert!(doc <= target);
        while doc < target {
            doc = self.advance();
        }
        doc
    }

    /// !!!Dragons ahead!!!
    /// In spirit, this is an approximate and dangerous version of `seek`.
    ///
    /// It can leave the DocSet in an `invalid` state and might return a
    /// lower bound of what the result of Seek would have been.
    ///
    ///
    /// More accurately it returns either:
    /// - Found if the target is in the docset. In that case, the DocSet is left in a valid state.
    /// - SeekLowerBound(seek_lower_bound) if the target is not in the docset. In that case, The
    ///   DocSet can be the left in a invalid state. The DocSet should then only receives call to
    ///   `seek_danger(..)` until it returns `Found`, and get back to a valid state.
    ///
    /// `seek_lower_bound` can be any `DocId` (in the docset or not) as long as it is in
    /// `(target .. seek_result] U {TERMINATED}` where `seek_result` is the first document in the
    /// docset greater than to `target`.
    ///
    /// `seek_danger` may return `SeekLowerBound(TERMINATED)`.
    ///
    /// Calling `seek_danger` with TERMINATED as a target is allowed,
    /// and should always return NewTarget(TERMINATED) or anything larger as TERMINATED is NOT in
    /// the DocSet.
    ///
    /// DocSets that already have an efficient `seek` method don't need to implement
    /// `seek_danger`.
    ///
    /// Consecutive calls to seek_danger are guaranteed to have strictly increasing `target`
    /// values.
    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target >= TERMINATED {
            debug_assert!(target == TERMINATED);
            // No need to advance.
            return SeekDangerResult::SeekLowerBound(target);
        }

        // The default implementation does not include any
        // `danger zone` behavior.
        //
        // It does not leave the scorer in an invalid state.
        // For this reason, we can safely call `self.doc()`.
        let mut doc = self.doc();
        if doc < target {
            doc = self.seek(target);
        }
        if doc == target {
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(doc)
        }
    }

    /// Fills a given mutable buffer with the next doc ids from the
    /// `DocSet`
    ///
    /// If that many `DocId`s are available, the method should
    /// fill the entire buffer and return the length of the buffer.
    ///
    /// If we reach the end of the `DocSet` before filling
    /// it entirely, then the buffer is filled up to this point, and
    /// return value is the number of elements that were filled.
    ///
    /// # Warning
    ///
    /// This method is only here for specific high-performance
    /// use case where batching. The normal way to
    /// go through the `DocId`'s is to call `.advance()`.
    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        if self.doc() == TERMINATED {
            return 0;
        }
        for (i, buffer_val) in buffer.iter_mut().enumerate() {
            *buffer_val = self.doc();
            if self.advance() == TERMINATED {
                return i + 1;
            }
        }
        buffer.len()
    }

    /// Returns the current document
    /// Right after creating a new `DocSet`, the docset points to the first document.
    ///
    /// If the `DocSet` is empty, `.doc()` should return [`TERMINATED`].
    fn doc(&self) -> DocId;

    /// Returns a best-effort hint of the
    /// length of the docset.
    fn size_hint(&self) -> u32;

    /// Returns a best-effort hint of the cost to consume the entire docset.
    ///
    /// Consuming means calling advance until [`TERMINATED`] is returned.
    /// The cost should be relative to the cost of driving a Term query,
    /// which would be the number of documents in the DocSet.
    ///
    /// By default this returns `size_hint()`.
    ///
    /// DocSets may have vastly different cost depending on their type,
    /// e.g. an intersection with 10 hits is much cheaper than
    /// a phrase search with 10 hits, since it needs to load positions.
    ///
    /// ### Future Work
    /// We may want to differentiate `DocSet` costs more more granular, e.g.
    /// creation_cost, advance_cost, seek_cost on to get a good estimation
    /// what query types to choose.
    fn cost(&self) -> u64 {
        self.size_hint() as u64
    }

    /// Returns the number documents matching.
    /// Calling this method consumes the `DocSet`.
    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        let mut count = 0u32;
        let mut doc = self.doc();
        while doc != TERMINATED {
            if alive_bitset.is_alive(doc) {
                count += 1u32;
            }
            doc = self.advance();
        }
        count
    }

    /// Returns the count of documents, deleted or not.
    /// Calling this method consumes the `DocSet`.
    ///
    /// Of course, the result is an upper bound of the result
    /// given by `count()`.
    fn count_including_deleted(&mut self) -> u32 {
        let mut count = 0u32;
        let mut doc = self.doc();
        while doc != TERMINATED {
            count += 1u32;
            doc = self.advance();
        }
        count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeekDangerResult {
    /// The target was found in the DocSet.
    Found,
    /// The target was not found in the DocSet.
    /// We return a range in which the value could be.
    /// The given target can be any DocId, that is <= than the first document
    /// in the docset after the target.
    SeekLowerBound(DocId),
}

impl DocSet for &mut dyn DocSet {
    fn advance(&mut self) -> u32 {
        (**self).advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        (**self).seek(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        (**self).seek_danger(target)
    }

    fn doc(&self) -> u32 {
        (**self).doc()
    }

    fn size_hint(&self) -> u32 {
        (**self).size_hint()
    }

    fn cost(&self) -> u64 {
        (**self).cost()
    }

    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        (**self).count(alive_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        (**self).count_including_deleted()
    }
}

impl<TDocSet: DocSet + ?Sized> DocSet for Box<TDocSet> {
    fn advance(&mut self) -> DocId {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.seek(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.seek_danger(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        let unboxed: &TDocSet = self.borrow();
        unboxed.doc()
    }

    fn size_hint(&self) -> u32 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.size_hint()
    }

    fn cost(&self) -> u64 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.cost()
    }

    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count(alive_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count_including_deleted()
    }
}
