use std::sync::Arc;

use crate::{
    index::{
        IndexConfiguration, IndexCursor, IndexDefinition, IndexDescriptor, IndexModule,
        HIDDEN_BTREE_MODULE_NAME,
    },
    Result,
};

#[derive(Debug)]
pub struct HiddenBtreeIndex;

#[derive(Debug)]
pub struct HiddenBTreeIndexDescriptor(IndexConfiguration);

impl IndexModule for HiddenBtreeIndex {
    fn descriptor(&self, configuration: &IndexConfiguration) -> Result<Arc<dyn IndexDescriptor>> {
        Ok(Arc::new(HiddenBTreeIndexDescriptor(configuration.clone())))
    }
}

impl IndexDescriptor for HiddenBTreeIndexDescriptor {
    fn definition<'a>(&'a self) -> IndexDefinition<'a> {
        IndexDefinition {
            module_name: HIDDEN_BTREE_MODULE_NAME,
            index_name: &self.0.index_name,
            patterns: &[],
            hidden: true,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexCursor>> {
        panic!("hidden 'fake' module which acts as a regular btree")
    }
}
