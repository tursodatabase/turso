#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(all(test, feature = "unstable"))]
extern crate test;

mod arena_hashmap;
mod expull;
mod fastcmp;
mod fastcpy;
mod memory_arena;
mod shared_arena_hashmap;

pub use self::arena_hashmap::ArenaHashMap;
pub use self::expull::ExpUnrolledLinkedList;
pub use self::memory_arena::{Addr, MemoryArena};
pub use self::shared_arena_hashmap::{SharedArenaHashMap, compute_table_memory_size};

/// When adding an element in a `ArenaHashMap`, we get a unique id associated to the given key.
pub type UnorderedId = u32;
