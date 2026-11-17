use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::types::IndexInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtreeDescriptor {
    Table,
    Index(Arc<IndexInfo>),
}

impl BtreeDescriptor {
    pub fn flags(&self) -> CreateBTreeFlags {
        match self {
            Self::Table => CreateBTreeFlags::new_table(),
            Self::Index(_) => CreateBTreeFlags::new_index(),
        }
    }
}
