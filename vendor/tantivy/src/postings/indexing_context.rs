use stacker::{ArenaHashMap, MemoryArena};

use crate::indexer::path_to_unordered_id::PathToUnorderedId;

/// IndexingContext contains all of the transient memory arenas
/// required for building the inverted index.
pub(crate) struct IndexingContext {
    /// The term index is an adhoc hashmap,
    /// itself backed by a dedicated memory arena.
    pub term_index: ArenaHashMap,
    /// Arena is a memory arena that stores posting lists / term frequencies / positions.
    pub arena: MemoryArena,
    pub path_to_unordered_id: PathToUnorderedId,
}

impl IndexingContext {
    /// Create a new IndexingContext given the size of the term hash map.
    pub(crate) fn new(table_size: usize) -> IndexingContext {
        let term_index = ArenaHashMap::with_capacity(table_size);
        IndexingContext {
            arena: MemoryArena::default(),
            term_index,
            path_to_unordered_id: PathToUnorderedId::default(),
        }
    }

    /// Returns the memory usage for the inverted index memory arenas, in bytes.
    pub(crate) fn mem_usage(&self) -> usize {
        self.term_index.mem_usage() + self.arena.mem_usage()
    }
}
