use super::TursoAllocExt;
use crate::alloc::BTreeMap;

fn btree_map<K, V>() -> BTreeMap<K, V> {
    std::collections::BTreeMap::new()
}

impl<K, V> TursoAllocExt for BTreeMap<K, V> {
    fn new() -> Self {
        btree_map()
    }
}
