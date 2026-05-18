use super::TursoAllocExt;
use crate::alloc::LinkedList;

#[cfg(not(nightly))]
fn linked_list<T>() -> LinkedList<T> {
    std::collections::LinkedList::new()
}

#[cfg(nightly)]
fn linked_list<T>() -> LinkedList<T> {
    std::collections::LinkedList::new_in(crate::alloc::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for LinkedList<T> {
    fn new() -> Self {
        linked_list()
    }
}

#[cfg(nightly)]
impl<T> TursoAllocExt for LinkedList<T> {
    fn new() -> Self {
        linked_list()
    }
}
