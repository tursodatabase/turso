use std::fmt;

use common::BitSet;

use crate::index::{SegmentId, SegmentMeta};
use crate::indexer::delete_queue::DeleteCursor;

/// A segment entry describes the state of
/// a given segment, at a given instant.
///
/// In addition to segment `meta`,
/// it contains a few transient states
/// - `alive_bitset` is a bitset describing documents that were alive during the commit itself.
/// - `delete_cursor` is the position in the delete queue. Deletes happening before the cursor are
///   reflected either in the .del file or in the `alive_bitset`.
#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    alive_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor,
}

impl SegmentEntry {
    /// Create a new `SegmentEntry`
    pub fn new(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor,
        alive_bitset: Option<BitSet>,
    ) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            alive_bitset,
            delete_cursor,
        }
    }

    /// Return a reference to the segment entry deleted bitset.
    ///
    /// `DocId` in this bitset are flagged as deleted.
    pub fn alive_bitset(&self) -> Option<&BitSet> {
        self.alive_bitset.as_ref()
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Return a reference to the segment_entry's delete cursor
    pub fn delete_cursor(&mut self) -> &mut DeleteCursor {
        &mut self.delete_cursor
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?})", self.meta)
    }
}
