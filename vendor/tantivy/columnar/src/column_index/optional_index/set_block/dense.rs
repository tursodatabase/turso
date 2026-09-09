use std::io::{self, Write};

use common::BinarySerializable;

use crate::column_index::optional_index::{ELEMENTS_PER_BLOCK, SelectCursor, Set, SetCodec};

#[inline(always)]
fn get_bit_at(input: u64, n: u16) -> bool {
    input & (1 << n) != 0
}

#[inline]
fn set_bit_at(input: &mut u64, n: u16) {
    *input |= 1 << n;
}

/// For the `DenseCodec`, `data` which contains the encoded blocks.
/// Each block consists of [u8; 12]. The first 8 bytes is a bitvec for 64 elements.
/// The last 4 bytes are the offset, the number of set bits so far.
///
/// When translating the original index to a dense index, the correct block can be computed
/// directly `orig_idx/64`. Inside the block the position is `orig_idx%64`.
///
/// When translating a dense index to the original index, we can use the offset to find the correct
/// block. Direct computation is not possible, but we can employ a linear or binary search.
const ELEMENTS_PER_MINI_BLOCK: u16 = 64;
const MINI_BLOCK_BITVEC_NUM_BYTES: usize = 8;
const MINI_BLOCK_OFFSET_NUM_BYTES: usize = 2;
pub const MINI_BLOCK_NUM_BYTES: usize = MINI_BLOCK_BITVEC_NUM_BYTES + MINI_BLOCK_OFFSET_NUM_BYTES;

/// Number of bytes in a dense block.
pub const DENSE_BLOCK_NUM_BYTES: u32 =
    (ELEMENTS_PER_BLOCK / ELEMENTS_PER_MINI_BLOCK as u32) * MINI_BLOCK_NUM_BYTES as u32;

pub struct DenseBlockCodec;

impl SetCodec for DenseBlockCodec {
    type Item = u16;
    type Reader<'a> = DenseBlock<'a>;

    fn serialize(els: impl Iterator<Item = u16>, wrt: impl io::Write) -> io::Result<()> {
        serialize_dense_codec(els, wrt)
    }

    #[inline]
    fn open(data: &[u8]) -> Self::Reader<'_> {
        assert_eq!(data.len(), DENSE_BLOCK_NUM_BYTES as usize);
        DenseBlock(data)
    }
}

/// Interpreting the bitvec as a set of integer within 0..=63
/// and given an element, returns the number of elements in the
/// set lesser than the element.
///
/// # Panics
///
/// May panic or return a wrong result if el <= 64.
#[inline(always)]
fn rank_u64(bitvec: u64, el: u16) -> u16 {
    debug_assert!(el < 64);
    let mask = (1u64 << el) - 1;
    let masked_bitvec = bitvec & mask;
    masked_bitvec.count_ones() as u16
}

#[inline(always)]
fn select_u64(mut bitvec: u64, rank: u16) -> u16 {
    for _ in 0..rank {
        bitvec &= bitvec - 1;
    }
    bitvec.trailing_zeros() as u16
}

// TODO test the following solution on Intel... on Ryzen Zen <3 it is a catastrophy.
// #[target_feature(enable = "bmi2")]
// unsafe fn select_bitvec_unsafe(bitvec: u64, rank: u16) -> u16 {
//     let pdep = _pdep_u64(1u64 << rank, bitvec);
//     pdep.trailing_zeros() as u16
// }

#[derive(Clone, Copy, Debug)]
struct DenseMiniBlock {
    bitvec: u64,
    rank: u16,
}

impl DenseMiniBlock {
    fn from_bytes(data: [u8; MINI_BLOCK_NUM_BYTES]) -> Self {
        let bitvec = u64::from_le_bytes(data[..MINI_BLOCK_BITVEC_NUM_BYTES].try_into().unwrap());
        let rank = u16::from_le_bytes(data[MINI_BLOCK_BITVEC_NUM_BYTES..].try_into().unwrap());
        Self { bitvec, rank }
    }

    fn to_bytes(self) -> [u8; MINI_BLOCK_NUM_BYTES] {
        let mut bytes = [0u8; MINI_BLOCK_NUM_BYTES];
        bytes[..MINI_BLOCK_BITVEC_NUM_BYTES].copy_from_slice(&self.bitvec.to_le_bytes());
        bytes[MINI_BLOCK_BITVEC_NUM_BYTES..].copy_from_slice(&self.rank.to_le_bytes());
        bytes
    }
}

#[derive(Copy, Clone)]
pub struct DenseBlock<'a>(&'a [u8]);

pub struct DenseBlockSelectCursor<'a> {
    block_id: u16,
    dense_block: DenseBlock<'a>,
}

impl SelectCursor<u16> for DenseBlockSelectCursor<'_> {
    #[inline]
    fn select(&mut self, rank: u16) -> u16 {
        self.block_id = self
            .dense_block
            .find_miniblock_containing_rank(rank, self.block_id)
            .unwrap();
        let index_block = self.dense_block.mini_block(self.block_id);
        let in_block_rank = rank - index_block.rank;
        self.block_id * ELEMENTS_PER_MINI_BLOCK + select_u64(index_block.bitvec, in_block_rank)
    }
}

impl<'a> Set<u16> for DenseBlock<'a> {
    type SelectCursor<'b>
        = DenseBlockSelectCursor<'a>
    where Self: 'b;

    #[inline(always)]
    fn contains(&self, el: u16) -> bool {
        let mini_block_id = el / ELEMENTS_PER_MINI_BLOCK;
        let bitvec = self.mini_block(mini_block_id).bitvec;
        let pos_in_bitvec = el % ELEMENTS_PER_MINI_BLOCK;
        get_bit_at(bitvec, pos_in_bitvec)
    }

    #[inline(always)]
    fn rank_if_exists(&self, el: u16) -> Option<u16> {
        let block_pos = el / ELEMENTS_PER_MINI_BLOCK;
        let index_block = self.mini_block(block_pos);
        let pos_in_block_bit_vec = el % ELEMENTS_PER_MINI_BLOCK;
        let ones_in_block = rank_u64(index_block.bitvec, pos_in_block_bit_vec);
        let rank = index_block.rank + ones_in_block;
        if get_bit_at(index_block.bitvec, pos_in_block_bit_vec) {
            Some(rank)
        } else {
            None
        }
    }

    #[inline(always)]
    fn rank(&self, el: u16) -> u16 {
        let block_pos = el / ELEMENTS_PER_MINI_BLOCK;
        let index_block = self.mini_block(block_pos);
        let pos_in_block_bit_vec = el % ELEMENTS_PER_MINI_BLOCK;
        let ones_in_block = rank_u64(index_block.bitvec, pos_in_block_bit_vec);
        index_block.rank + ones_in_block
    }

    #[inline(always)]
    fn select(&self, rank: u16) -> u16 {
        let block_id = self.find_miniblock_containing_rank(rank, 0).unwrap();
        let index_block = self.mini_block(block_id);
        let in_block_rank = rank - index_block.rank;
        block_id * ELEMENTS_PER_MINI_BLOCK + select_u64(index_block.bitvec, in_block_rank)
    }

    #[inline(always)]
    fn select_cursor(&self) -> Self::SelectCursor<'_> {
        DenseBlockSelectCursor {
            block_id: 0,
            dense_block: *self,
        }
    }
}

impl DenseBlock<'_> {
    #[inline]
    fn mini_block(&self, mini_block_id: u16) -> DenseMiniBlock {
        let data_start_pos = mini_block_id as usize * MINI_BLOCK_NUM_BYTES;
        DenseMiniBlock::from_bytes(
            self.0[data_start_pos..data_start_pos + MINI_BLOCK_NUM_BYTES]
                .try_into()
                .unwrap(),
        )
    }

    #[inline]
    fn iter_miniblocks(
        &self,
        from_block_id: u16,
    ) -> impl Iterator<Item = (u16, DenseMiniBlock)> + '_ {
        self.0
            .chunks_exact(MINI_BLOCK_NUM_BYTES)
            .enumerate()
            .skip(from_block_id as usize)
            .map(|(block_id, bytes)| {
                let mini_block = DenseMiniBlock::from_bytes(bytes.try_into().unwrap());
                (block_id as u16, mini_block)
            })
    }

    /// Finds the block position containing the dense_idx.
    ///
    /// # Correctness
    /// dense_idx needs to be smaller than the number of values in the index
    ///
    /// The last offset number is equal to the number of values in the index.
    #[inline]
    fn find_miniblock_containing_rank(&self, rank: u16, from_block_id: u16) -> Option<u16> {
        self.iter_miniblocks(from_block_id)
            .take_while(|(_, block)| block.rank <= rank)
            .map(|(block_id, _)| block_id)
            .last()
    }
}

/// Iterator over all values, true if set, otherwise false
pub fn serialize_dense_codec(
    els: impl Iterator<Item = u16>,
    mut output: impl Write,
) -> io::Result<()> {
    let mut non_null_rows_before: u16 = 0u16;
    let mut block = 0u64;
    let mut current_block_id = 0u16;
    for el in els {
        let block_id = el / ELEMENTS_PER_MINI_BLOCK;
        let in_offset = el % ELEMENTS_PER_MINI_BLOCK;
        while block_id > current_block_id {
            let dense_mini_block = DenseMiniBlock {
                bitvec: block,
                rank: non_null_rows_before,
            };
            output.write_all(&dense_mini_block.to_bytes())?;
            non_null_rows_before += block.count_ones() as u16;
            block = 0u64;
            current_block_id += 1u16;
        }
        set_bit_at(&mut block, in_offset);
    }
    while current_block_id <= u16::MAX / ELEMENTS_PER_MINI_BLOCK {
        block.serialize(&mut output)?;
        non_null_rows_before.serialize(&mut output)?;
        // This will overflow to 0 exactly if all bits are set.
        // This is however not problem as we won't use this last value.
        non_null_rows_before = non_null_rows_before.wrapping_add(block.count_ones() as u16);
        block = 0u64;
        current_block_id += 1u16;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_bitvec() {
        assert_eq!(select_u64(1u64, 0), 0);
        assert_eq!(select_u64(2u64, 0), 1);
        assert_eq!(select_u64(4u64, 0), 2);
        assert_eq!(select_u64(8u64, 0), 3);
        assert_eq!(select_u64(1 | 8u64, 0), 0);
        assert_eq!(select_u64(1 | 8u64, 1), 3);
    }

    #[test]
    fn test_count_ones() {
        for i in 0..=63 {
            assert_eq!(rank_u64(u64::MAX, i), i);
        }
    }

    #[test]
    fn test_dense() {
        assert_eq!(DENSE_BLOCK_NUM_BYTES, 10_240);
    }
}
