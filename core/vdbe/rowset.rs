//! RowSet data structure for efficient set operations on integer rowids.
//!
//! RowSet is optimized for batch-oriented insertions where sets of integers are inserted
//! in distinct phases, with each phase containing no duplicates. Operations are optimized
//! for two distinct use cases:
//!
//! 1. **TEST mode**: Check membership and insert values with batch-based consolidation.
//!    Values are inserted into a fresh list and consolidated into a BTreeSet when the batch
//!    number changes, enabling efficient membership tests.
//!
//! 2. **SMALLEST mode**: Extract values in sorted order. Once extraction begins, a sorted
//!    buffer is built and values are extracted one at a time.
//!
//! **Critical constraint**: TEST and SMALLEST operations are mutually exclusive. Once `test()`
//! has been called, `smallest()` cannot be used. Once `smallest()` has been called, `test()`
//! cannot be used. This matches SQLite's RowSet semantics.
//!
//! ## Batch Semantics
//!
//! Batches identify distinct phases of insertion:
//! - `batch == 0`: First set (guaranteed not to contain values, so no test needed)
//! - `batch > 0`: Intermediate sets (consolidation happens when batch changes)
//! - `batch == -1`: Final set (no insertion needed, only testing)
//!
//! When `test()` is called with a different batch number than the current `i_batch`, all
//! values in the fresh list are consolidated into the consolidated set.

use branches::mark_unlikely;

use crate::alloc::vec;
use crate::alloc::*;
use crate::turso_assert;
use crate::Result;
use std::collections::BTreeSet;

/// The mode of usage for a RowSet.
/// Test: the rowset will be used for set membership tests.
/// Smallest: the rowset will be used to extract the smallest value in sorted order.
#[derive(Debug)]
pub enum RowSetMode {
    Test {
        /// Set of distinct rowids.
        set: BTreeSet<i64>,
        /// Batch number of the last test.
        batch_number: i32,
    },
    Smallest {
        sorted_vec: Vec<i64>,
    },
    Unset,
}

/// A set of integer rowids optimized for batch-oriented operations.
#[derive(Debug)]
pub struct RowSet {
    /// Fresh inserts since last consolidation
    fresh: Vec<i64>,
    /// The mode of usage for the RowSet.
    mode: RowSetMode,
}

impl Default for RowSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RowSet {
    /// Creates a new empty RowSet.
    pub fn new() -> Self {
        Self {
            fresh: vec![],
            mode: RowSetMode::Unset,
        }
    }

    /// Inserts a rowid into the set.
    ///
    /// Values are added to the fresh list and will be consolidated when `test()` is called
    /// with a different batch number.
    ///
    /// # Panics
    ///
    /// Panics if `smallest()` extraction has already started.
    pub fn insert(&mut self, rowid: i64) -> Result<()> {
        turso_assert!(
            !matches!(self.mode, RowSetMode::Smallest { .. }),
            "cannot insert after smallest() has been used"
        );
        self.fresh.try_push(rowid)?;
        Ok(())
    }

    /// Tests if the rowid exists in the set, with batch-based consolidation.
    ///
    /// If `batch` differs from the current batch, consolidates fresh values into the
    /// consolidated set. Returns `true` if the rowid is found.
    ///
    /// # Panics
    ///
    /// Panics if `smallest()` extraction has already started, because rowsets have two
    /// mutually exclusive uses: set membership tests (test()) and in-order iteration (smallest()).
    pub fn test(&mut self, rowid: i64, batch: i32) -> bool {
        turso_assert!(
            !matches!(self.mode, RowSetMode::Smallest { .. }),
            "cannot call test() after smallest() has started"
        );
        if matches!(self.mode, RowSetMode::Unset) {
            self.mode = RowSetMode::Test {
                set: BTreeSet::new(),
                batch_number: 0,
            };
        }
        let RowSetMode::Test { set, batch_number } = &mut self.mode else {
            mark_unlikely();
            unreachable!()
        };

        // If a new batch has started, fold the fresh vector into the set.
        if batch != *batch_number {
            for v in self.fresh.drain(..) {
                set.insert(v);
            }
            *batch_number = batch;
        }
        // Note: If the batch number has not changed, we only check whether any previous batch inserted this value,
        // since the rowset implementation expects that any single batch does not insert any duplicates nor
        // test for duplicates wrt the current batch.
        set.contains(&rowid)
    }

    /// Extracts and returns the smallest rowid from the set.
    ///
    /// On the first call, builds a sorted buffer from all values (O(N log N)).
    /// Subsequent calls are O(1). Returns `None` if the set is empty.
    ///
    /// # Panics
    ///
    /// Panics if `test()` has been called on this RowSet, because rowsets have two
    /// mutually exclusive uses: set membership tests (test()) and in-order iteration (smallest()).
    pub fn smallest(&mut self) -> Option<i64> {
        turso_assert!(
            !matches!(self.mode, RowSetMode::Test { .. }),
            "cannot call smallest() after test() has been used"
        );
        if matches!(self.mode, RowSetMode::Unset) {
            let mut v = std::mem::replace(&mut self.fresh, vec![]);
            v.sort_unstable();
            v.dedup();
            v.reverse();
            self.mode = RowSetMode::Smallest { sorted_vec: v };
        }
        let RowSetMode::Smallest { sorted_vec } = &mut self.mode else {
            mark_unlikely();
            unreachable!()
        };

        sorted_vec.pop()
    }

    /// Returns `true` if the RowSet contains no values.
    pub fn is_empty(&self) -> bool {
        if !self.fresh.is_empty() {
            return false;
        }
        match &self.mode {
            RowSetMode::Test { set, .. } => set.is_empty(),
            RowSetMode::Smallest { sorted_vec, .. } => sorted_vec.is_empty(),
            RowSetMode::Unset => true,
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/vdbe/rowset/tests.rs"]
mod tests;
