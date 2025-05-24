use limbo_sqlite3_parser::ast::SortOrder;

use crate::{
    translate::collate::CollationSeq,
    types::{compare_immutable, ImmutableRecord, IndexKeySortOrder},
};

pub struct Sorter {
    records: Vec<ImmutableRecord>,
    current: Option<ImmutableRecord>,
    order: IndexKeySortOrder,
    key_len: usize,
    collations: Vec<CollationSeq>,
}

impl Sorter {
    pub fn new(order: &[SortOrder], collations: Vec<CollationSeq>) -> Self {
        Self {
            records: Vec::new(),
            current: None,
            key_len: order.len(),
            order: IndexKeySortOrder::from_list(order),
            collations,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) {
        self.records.sort_by(|a, b| {
            compare_immutable(
                &a.values[..self.key_len],
                &b.values[..self.key_len],
                self.order,
                &self.collations,
            )
        });
        self.records.reverse();
        self.next()
    }
    pub fn next(&mut self) {
        self.current = self.records.pop();
    }
    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &ImmutableRecord) {
        self.records.push(record.clone());
    }
}
