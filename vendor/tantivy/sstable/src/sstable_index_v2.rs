use common::OwnedBytes;
use tantivy_fst::Automaton;

use crate::block_match_automaton::can_block_match_automaton;
use crate::{BlockAddr, SSTable, SSTableDataCorruption, TermOrdinal};

#[derive(Default, Debug, Clone)]
pub struct SSTableIndex {
    pub(crate) blocks: Vec<BlockMeta>,
}

impl SSTableIndex {
    /// Load an index from its binary representation
    pub fn load(data: OwnedBytes) -> Result<SSTableIndex, SSTableDataCorruption> {
        let mut reader = IndexSSTable::reader(data);
        let mut blocks = Vec::new();

        while reader.advance().map_err(|_| SSTableDataCorruption)? {
            blocks.push(BlockMeta {
                last_key_or_greater: reader.key().to_vec(),
                block_addr: reader.value().clone(),
            });
        }

        Ok(SSTableIndex { blocks })
    }

    /// Get the [`BlockAddr`] of the requested block.
    pub(crate) fn get_block(&self, block_id: usize) -> Option<BlockAddr> {
        self.blocks
            .get(block_id)
            .map(|block_meta| block_meta.block_addr.clone())
    }

    /// Get the block id of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub(crate) fn locate_with_key(&self, key: &[u8]) -> Option<usize> {
        let pos = self
            .blocks
            .binary_search_by_key(&key, |block| &block.last_key_or_greater);
        match pos {
            Ok(pos) => Some(pos),
            Err(pos) => {
                if pos < self.blocks.len() {
                    Some(pos)
                } else {
                    // after end of last block: no block matches
                    None
                }
            }
        }
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, key: &[u8]) -> Option<BlockAddr> {
        self.locate_with_key(key).and_then(|id| self.get_block(id))
    }

    pub(crate) fn locate_with_ord(&self, ord: TermOrdinal) -> usize {
        let pos = self
            .blocks
            .binary_search_by_key(&ord, |block| block.block_addr.first_ordinal);

        match pos {
            Ok(pos) => pos,
            // Err(0) can't happen as the sstable starts with ordinal zero
            Err(pos) => pos - 1,
        }
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub(crate) fn get_block_with_ord(&self, ord: TermOrdinal) -> BlockAddr {
        // locate_with_ord always returns an index within range
        self.get_block(self.locate_with_ord(ord)).unwrap()
    }

    pub(crate) fn get_block_for_automaton<'a>(
        &'a self,
        automaton: &'a impl Automaton,
    ) -> impl Iterator<Item = (u64, BlockAddr)> + 'a {
        std::iter::once((None, &self.blocks[0]))
            .chain(self.blocks.windows(2).map(|window| {
                let [prev, curr] = window else {
                    unreachable!();
                };
                (Some(&*prev.last_key_or_greater), curr)
            }))
            .enumerate()
            .filter_map(move |(pos, (prev_key, current_block))| {
                if can_block_match_automaton(
                    prev_key,
                    &current_block.last_key_or_greater,
                    automaton,
                ) {
                    Some((pos as u64, current_block.block_addr.clone()))
                } else {
                    None
                }
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockMeta {
    /// Any byte string that is lexicographically greater or equal to
    /// the last key in the block,
    /// and yet strictly smaller than the first key in the next block.
    pub last_key_or_greater: Vec<u8>,
    pub block_addr: BlockAddr,
}

/// SSTable representing an index
///
/// `last_key_or_greater` is used as the key, the value contains the
/// length and first ordinal of each block. The start offset is implicitly
/// obtained from lengths.
struct IndexSSTable;

impl SSTable for IndexSSTable {
    type Value = BlockAddr;

    type ValueReader = crate::value::index::IndexValueReader;

    type ValueWriter = crate::value::index::IndexValueWriter;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_match_automaton::tests::EqBuffer;

    #[test]
    fn test_get_block_for_automaton() {
        let sstable = SSTableIndex {
            blocks: vec![
                BlockMeta {
                    last_key_or_greater: vec![0, 1, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 0,
                        byte_range: 0..10,
                    },
                },
                BlockMeta {
                    last_key_or_greater: vec![0, 2, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 5,
                        byte_range: 10..20,
                    },
                },
                BlockMeta {
                    last_key_or_greater: vec![0, 3, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 10,
                        byte_range: 20..30,
                    },
                },
            ],
        };

        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 1, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                0,
                BlockAddr {
                    first_ordinal: 0,
                    byte_range: 0..10
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 2, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                1,
                BlockAddr {
                    first_ordinal: 5,
                    byte_range: 10..20
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 3, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                2,
                BlockAddr {
                    first_ordinal: 10,
                    byte_range: 20..30
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 4, 1]))
            .collect::<Vec<_>>();
        assert!(res.is_empty());

        let complex_automaton = EqBuffer(vec![0, 1, 1]).union(EqBuffer(vec![0, 3, 1]));
        let res = sstable
            .get_block_for_automaton(&complex_automaton)
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![
                (
                    0,
                    BlockAddr {
                        first_ordinal: 0,
                        byte_range: 0..10
                    }
                ),
                (
                    2,
                    BlockAddr {
                        first_ordinal: 10,
                        byte_range: 20..30
                    }
                )
            ]
        );
    }
}
