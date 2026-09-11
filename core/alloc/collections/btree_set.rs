use super::TursoAllocExt;
use crate::alloc::BTreeSet;

fn btree_set<T>() -> BTreeSet<T> {
    BTreeSet::new()
}

impl<T> TursoAllocExt for BTreeSet<T> {
    fn new() -> Self {
        btree_set()
    }
}
