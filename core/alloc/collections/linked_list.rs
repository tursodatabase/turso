use super::TursoAllocExt;
use crate::alloc::LinkedList;

fn linked_list<T>() -> LinkedList<T> {
    std::collections::LinkedList::new()
}

impl<T> TursoAllocExt for LinkedList<T> {
    fn new() -> Self {
        linked_list()
    }
}
