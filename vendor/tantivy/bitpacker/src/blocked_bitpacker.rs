use super::bitpacker::BitPacker;
use super::compute_num_bits;
use crate::{BitUnpacker, minmax};

const BLOCK_SIZE: usize = 128;

/// `BlockedBitpacker` compresses data in blocks of
/// 128 elements, while keeping an index on it
#[derive(Debug, Clone)]
pub struct BlockedBitpacker {
    // bitpacked blocks
    compressed_blocks: Vec<u8>,
    // uncompressed data, collected until BLOCK_SIZE
    buffer: Vec<u64>,
    offset_and_bits: Vec<BlockedBitpackerEntryMetaData>,
}
impl Default for BlockedBitpacker {
    fn default() -> Self {
        BlockedBitpacker::new()
    }
}

/// `BlockedBitpackerEntryMetaData` encodes the
/// offset and bit_width into a u64 bit field
///
/// This saves some space, since 7byte is more
/// than enough and also keeps the access fast
/// because of alignment
#[derive(Debug, Clone, Default)]
struct BlockedBitpackerEntryMetaData {
    encoded: u64,
    base_value: u64,
}

impl BlockedBitpackerEntryMetaData {
    fn new(offset: u64, num_bits: u8, base_value: u64) -> Self {
        let encoded = offset | (u64::from(num_bits) << (64 - 8));
        Self {
            encoded,
            base_value,
        }
    }
    fn offset(&self) -> u64 {
        (self.encoded << 8) >> 8
    }
    fn num_bits(&self) -> u8 {
        (self.encoded >> 56) as u8
    }
    fn base_value(&self) -> u64 {
        self.base_value
    }
}

#[test]
fn metadata_test() {
    let meta = BlockedBitpackerEntryMetaData::new(50000, 6, 40000);
    assert_eq!(meta.offset(), 50000);
    assert_eq!(meta.num_bits(), 6);
}

fn mem_usage<T>(items: &Vec<T>) -> usize {
    items.capacity() * std::mem::size_of::<T>()
}

impl BlockedBitpacker {
    pub fn new() -> Self {
        Self {
            compressed_blocks: vec![0; 8],
            buffer: vec![],
            offset_and_bits: vec![],
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<BlockedBitpacker>()
            + self.compressed_blocks.capacity()
            + mem_usage(&self.offset_and_bits)
            + mem_usage(&self.buffer)
    }

    #[inline]
    pub fn add(&mut self, val: u64) {
        self.buffer.push(val);
        if self.buffer.len() == BLOCK_SIZE {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        if let Some((min_value, max_value)) = minmax(self.buffer.iter()) {
            let mut bit_packer = BitPacker::new();
            let num_bits_block = compute_num_bits(*max_value - min_value);
            // todo performance: the padding handling could be done better, e.g. use a slice and
            // return num_bytes written from bitpacker
            self.compressed_blocks
                .resize(self.compressed_blocks.len() - 8, 0); // remove padding for bitpacker
            let offset = self.compressed_blocks.len() as u64;
            // todo performance: for some bit_width we
            // can encode multiple vals into the
            // mini_buffer before checking to flush
            // (to be done in BitPacker)
            for val in self.buffer.iter() {
                bit_packer
                    .write(
                        *val - min_value,
                        num_bits_block,
                        &mut self.compressed_blocks,
                    )
                    .expect("cannot write bitpacking to output"); // write to in memory can't fail
            }
            bit_packer.flush(&mut self.compressed_blocks).unwrap();
            self.offset_and_bits
                .push(BlockedBitpackerEntryMetaData::new(
                    offset,
                    num_bits_block,
                    *min_value,
                ));

            self.buffer.clear();
            self.compressed_blocks
                .resize(self.compressed_blocks.len() + 8, 0); // add padding for bitpacker
        }
    }
    #[inline]
    pub fn get(&self, idx: usize) -> u64 {
        let metadata_pos = idx / BLOCK_SIZE;
        let pos_in_block = idx % BLOCK_SIZE;
        if let Some(metadata) = self.offset_and_bits.get(metadata_pos) {
            let unpacked = BitUnpacker::new(metadata.num_bits()).get(
                pos_in_block as u32,
                &self.compressed_blocks[metadata.offset() as usize..],
            );
            unpacked + metadata.base_value()
        } else {
            self.buffer[pos_in_block]
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        // todo performance: we could decompress a whole block and cache it instead
        let bitpacked_elems = self.offset_and_bits.len() * BLOCK_SIZE;

        (0..bitpacked_elems)
            .map(move |idx| self.get(idx))
            .chain(self.buffer.iter().cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocked_bitpacker_empty() {
        let blocked_bitpacker = BlockedBitpacker::new();
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![]);
    }
    #[test]
    fn blocked_bitpacker_one() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        blocked_bitpacker.add(50000);
        assert_eq!(blocked_bitpacker.get(0), 50000);
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![50000]);
    }
    #[test]
    fn blocked_bitpacker_test() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..21500 {
            blocked_bitpacker.add(val);
        }
        for val in 0..21500 {
            assert_eq!(blocked_bitpacker.get(val as usize), val);
        }
        assert_eq!(blocked_bitpacker.iter().count(), 21500);
        assert_eq!(blocked_bitpacker.iter().last().unwrap(), 21499);
    }
}
