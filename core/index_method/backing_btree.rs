use std::sync::Arc;

use crate::{
    index_method::{
        IndexMethod, IndexMethodAttachment, IndexMethodConfiguration, IndexMethodCursor,
        IndexMethodDefinition, BACKING_BTREE_INDEX_METHOD_NAME,
    },
    Result,
};

/// Special 'backing_btree' index method which can be used by other custom index methods
///
/// Under the hood, it's marked as 'treat_as_btree' which recognized by the tursodb core as a special index method
/// which should be translated to ordinary btree but also do not explicitly managed by the core
#[derive(Debug)]
pub struct BackingBtreeIndexMethod;

#[derive(Debug)]
pub struct BackingBTreeIndexMethodAttachment(String);

impl IndexMethod for BackingBtreeIndexMethod {
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>> {
        Ok(Arc::new(BackingBTreeIndexMethodAttachment(
            configuration.index_name.clone(),
        )))
    }
}

impl IndexMethodAttachment for BackingBTreeIndexMethodAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: BACKING_BTREE_INDEX_METHOD_NAME,
            index_name: &self.0,
            patterns: &[],
            backing_btree: true,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        panic!("init is not supported for backing_btree index method")
    }
}
