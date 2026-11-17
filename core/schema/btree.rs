use super::{Schema, Table};
use crate::storage::btree_descriptor::BtreeDescriptor;
use crate::sync::Arc;
use crate::types::{IndexInfo, KeyInfo};
use crate::Result;

impl Schema {
    pub(crate) fn btree_descriptors(&self) -> Result<Vec<(i64, BtreeDescriptor)>> {
        let mut objects = Vec::new();
        for table in self.all_tables() {
            let Table::BTree(table) = table.as_ref() else {
                continue;
            };
            let descriptor = if table.has_rowid {
                BtreeDescriptor::Table
            } else {
                let keys = table.primary_key_columns.iter().map(|(name, order)| {
                    let (_, column) = table.get_column(name).expect("primary key column exists");
                    KeyInfo {
                        sort_order: *order,
                        collation: column.collation(),
                        nulls_order: None,
                    }
                });
                BtreeDescriptor::Index(Arc::new(IndexInfo::new(
                    keys,
                    false,
                    table.primary_key_columns.len(),
                    true,
                )?))
            };
            objects.push((table.root_page, descriptor));
        }
        for index in self.all_indexes().filter(|index| index.is_btree_backed()) {
            objects.push((
                index.root_page,
                BtreeDescriptor::Index(Arc::new(IndexInfo::new_from_index(index)?)),
            ));
        }
        Ok(objects)
    }
}
