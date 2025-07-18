use turso_sqlite3_parser::ast::SortOrder;

use crate::{
    translate::collate::CollationSeq,
    types::{compare_immutable, ImmutableRecord, KeyInfo},
};

pub struct Sorter {
    records: Vec<ImmutableRecord>,
    current: Option<ImmutableRecord>,
    key_len: usize,
    index_key_info: Vec<KeyInfo>,
}

impl Sorter {
    pub fn new(order: &[SortOrder], collations: Vec<CollationSeq>) -> Self {
        assert_eq!(order.len(), collations.len());
        Self {
            records: Vec::new(),
            current: None,
            key_len: order.len(),
            index_key_info: order
                .iter()
                .zip(collations)
                .map(|(order, collation)| KeyInfo {
                    sort_order: *order,
                    collation,
                })
                .collect(),
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
            let a_values = a.get_values();
            let b_values = b.get_values();

            let a_key = if a_values.len() >= self.key_len {
                &a_values[..self.key_len]
            } else {
                &a_values[..]
            };

            let b_key = if b_values.len() >= self.key_len {
                &b_values[..self.key_len]
            } else {
                &b_values[..]
            };

            compare_immutable(a_key, b_key, &self.index_key_info)
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
