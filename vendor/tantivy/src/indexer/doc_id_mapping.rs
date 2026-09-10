//! This module is used when sorting the index by a property, e.g.
//! to get mappings from old doc_id to new doc_id and vice versa, after sorting

use common::ReadOnlyBitSet;

use crate::DocAddress;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MappingType {
    Stacked,
    StackedWithDeletes,
}

/// Struct to provide mapping from new doc_id to old doc_id and segment.
#[derive(Clone)]
pub(crate) struct SegmentDocIdMapping {
    pub(crate) new_doc_id_to_old_doc_addr: Vec<DocAddress>,
    pub(crate) alive_bitsets: Vec<Option<ReadOnlyBitSet>>,
    mapping_type: MappingType,
}

impl SegmentDocIdMapping {
    pub(crate) fn new(
        new_doc_id_to_old_doc_addr: Vec<DocAddress>,
        mapping_type: MappingType,
        alive_bitsets: Vec<Option<ReadOnlyBitSet>>,
    ) -> Self {
        Self {
            new_doc_id_to_old_doc_addr,
            mapping_type,
            alive_bitsets,
        }
    }

    pub fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    /// Returns an iterator over the old document addresses, ordered by the new document ids.
    ///
    /// In the returned `DocAddress`, the `segment_ord` is the ordinal of targeted segment
    /// in the list of merged segments.
    pub(crate) fn iter_old_doc_addrs(&self) -> impl Iterator<Item = DocAddress> + '_ {
        self.new_doc_id_to_old_doc_addr.iter().copied()
    }
}
