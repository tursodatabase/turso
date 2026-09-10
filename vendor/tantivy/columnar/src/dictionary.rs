use std::io;

use sstable::SSTable;
use stacker::{MemoryArena, SharedArenaHashMap};

pub(crate) struct TermIdMapping {
    unordered_to_ord: Vec<OrderedId>,
}

impl TermIdMapping {
    pub fn to_ord(&self, unordered: UnorderedId) -> OrderedId {
        self.unordered_to_ord[unordered.0 as usize]
    }
}

/// When we add values, we cannot know their ordered id yet.
/// For this reason, we temporarily assign them a `UnorderedId`
/// that will be mapped to an `OrderedId` upon serialization.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct UnorderedId(pub u32);

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct OrderedId(pub u32);

/// `DictionaryBuilder` for dictionary encoding.
///
/// It stores the different terms encounterred and assigns them a temporary value
/// we call unordered id.
///
/// Upon serialization, we will sort the ids and hence build a `UnorderedId -> Term ordinal`
/// mapping.
#[derive(Default)]
pub(crate) struct DictionaryBuilder {
    dict: SharedArenaHashMap,
}

impl DictionaryBuilder {
    /// Get or allocate an unordered id.
    /// (This ID is simply an auto-incremented id.)
    pub fn get_or_allocate_id(&mut self, term: &[u8], arena: &mut MemoryArena) -> UnorderedId {
        let next_id = self.dict.len() as u32;
        let unordered_id = self
            .dict
            .mutate_or_create(term, arena, |unordered_id: Option<u32>| {
                if let Some(unordered_id) = unordered_id {
                    unordered_id
                } else {
                    next_id
                }
            });
        UnorderedId(unordered_id)
    }

    /// Serialize the dictionary into an fst, and returns the
    /// `UnorderedId -> TermOrdinal` map.
    pub fn serialize<'a, W: io::Write + 'a>(
        &self,
        arena: &MemoryArena,
        wrt: &mut W,
    ) -> io::Result<TermIdMapping> {
        let mut terms: Vec<(&[u8], UnorderedId)> = self
            .dict
            .iter(arena)
            .map(|(k, v)| (k, arena.read(v)))
            .collect();
        terms.sort_unstable_by_key(|(key, _)| *key);
        // TODO Remove the allocation.
        let mut unordered_to_ord: Vec<OrderedId> = vec![OrderedId(0u32); terms.len()];
        let mut sstable_builder = sstable::VoidSSTable::writer(wrt);
        for (ord, (key, unordered_id)) in terms.into_iter().enumerate() {
            let ordered_id = OrderedId(ord as u32);
            sstable_builder.insert(key, &())?;
            unordered_to_ord[unordered_id.0 as usize] = ordered_id;
        }
        sstable_builder.finish()?;
        Ok(TermIdMapping { unordered_to_ord })
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.dict.mem_usage()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_builder() {
        let mut arena = MemoryArena::default();
        let mut dictionary_builder = DictionaryBuilder::default();
        let hello_uid = dictionary_builder.get_or_allocate_id(b"hello", &mut arena);
        let happy_uid = dictionary_builder.get_or_allocate_id(b"happy", &mut arena);
        let tax_uid = dictionary_builder.get_or_allocate_id(b"tax", &mut arena);
        let mut buffer = Vec::new();
        let id_mapping = dictionary_builder.serialize(&arena, &mut buffer).unwrap();
        assert_eq!(id_mapping.to_ord(hello_uid), OrderedId(1));
        assert_eq!(id_mapping.to_ord(happy_uid), OrderedId(0));
        assert_eq!(id_mapping.to_ord(tax_uid), OrderedId(2));
    }
}
