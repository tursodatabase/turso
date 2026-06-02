use super::TursoAllocExt;
use crate::alloc::BTreeSet;

#[cfg(not(nightly))]
fn btree_set<T>() -> BTreeSet<T> {
    BTreeSet::new()
}

#[cfg(nightly)]
fn btree_set<T>() -> BTreeSet<T> {
    BTreeSet::new_in(crate::alloc::TursoAllocator)
}

impl<T> TursoAllocExt for BTreeSet<T> {
    fn new() -> Self {
        btree_set()
    }
}
