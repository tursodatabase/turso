use super::TursoAllocExt;
use crate::BTreeMap;

#[cfg(not(nightly))]
fn btree_map<K, V>() -> BTreeMap<K, V> {
    std::collections::BTreeMap::new()
}

#[cfg(nightly)]
fn btree_map<K, V>() -> BTreeMap<K, V> {
    std::collections::BTreeMap::new_in(crate::TursoAllocator)
}

#[cfg(not(nightly))]
impl<K, V> TursoAllocExt for BTreeMap<K, V> {
    fn new() -> Self {
        btree_map()
    }
}

#[cfg(nightly)]
impl<K, V> TursoAllocExt for BTreeMap<K, V> {
    fn new() -> Self {
        btree_map()
    }
}
