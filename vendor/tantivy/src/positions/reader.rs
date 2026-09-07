use std::io;

use common::{BinarySerializable, VInt};

use crate::directory::OwnedBytes;
use crate::positions::COMPRESSION_BLOCK_SIZE;
use crate::postings::compression::{BlockDecoder, VIntDecoder};

/// When accessing the positions of a term, we get a positions_idx from the `Terminfo`.
/// This means we need to skip to the `nth` position efficiently.
///
/// Blocks are compressed using bitpacking, so `skip_read` contains the number of bits
/// (values can go from 0 to 32 bits) required to decompress every block.
///
/// A given block obviously takes `(128 x  num_bit_for_the_block / num_bits_in_a_byte)`,
/// so skipping a block without decompressing it is just a matter of advancing that many
/// bytes.

#[derive(Clone)]
pub struct PositionReader {
    bit_widths: OwnedBytes,
    positions: OwnedBytes,

    block_decoder: BlockDecoder,

    // offset, expressed in positions, for the first position of the block currently loaded
    // block_offset is a multiple of COMPRESSION_BLOCK_SIZE.
    block_offset: u64,
    // offset, expressed in positions, for the position of the first block encoded
    // in the `self.positions` bytes, and if bitpacked, compressed using the bitwidth in
    // `self.bit_widths`.
    //
    // As we advance, anchor increases simultaneously with bit_widths and positions get consumed.
    anchor_offset: u64,

    // These are just copies used for .reset().
    original_bit_widths: OwnedBytes,
    original_positions: OwnedBytes,
}

impl PositionReader {
    /// Open and reads the term positions encoded into the positions_data owned bytes.
    pub fn open(mut positions_data: OwnedBytes) -> io::Result<PositionReader> {
        let num_positions_bitpacked_blocks = VInt::deserialize(&mut positions_data)?.0 as usize;
        let (bit_widths, positions) = positions_data.split(num_positions_bitpacked_blocks);
        Ok(PositionReader {
            bit_widths: bit_widths.clone(),
            positions: positions.clone(),
            block_decoder: BlockDecoder::default(),
            block_offset: i64::MAX as u64,
            anchor_offset: 0u64,
            original_bit_widths: bit_widths,
            original_positions: positions,
        })
    }

    fn reset(&mut self) {
        self.positions = self.original_positions.clone();
        self.bit_widths = self.original_bit_widths.clone();
        self.block_offset = i64::MAX as u64;
        self.anchor_offset = 0u64;
    }

    /// Advance from num_blocks bitpacked blocks.
    ///
    /// Panics if there are not that many remaining blocks.
    fn advance_num_blocks(&mut self, num_blocks: usize) {
        let num_bits: usize = self.bit_widths.as_ref()[..num_blocks]
            .iter()
            .cloned()
            .map(|num_bits| num_bits as usize)
            .sum();
        let num_bytes_to_skip = num_bits * COMPRESSION_BLOCK_SIZE / 8;
        self.bit_widths.advance(num_blocks);
        self.positions.advance(num_bytes_to_skip);
        self.anchor_offset += (num_blocks * COMPRESSION_BLOCK_SIZE) as u64;
    }

    /// block_rel_id is counted relatively to the anchor.
    /// block_rel_id = 0 means the anchor block.
    /// block_rel_id = i means the ith block after the anchor block.
    fn load_block(&mut self, block_rel_id: usize) {
        let bit_widths = self.bit_widths.as_slice();
        let byte_offset: usize = bit_widths[0..block_rel_id]
            .iter()
            .map(|&b| b as usize)
            .sum::<usize>()
            * COMPRESSION_BLOCK_SIZE
            / 8;
        let compressed_data = &self.positions.as_slice()[byte_offset..];
        if bit_widths.len() > block_rel_id {
            // that block is bitpacked.
            let bit_width = bit_widths[block_rel_id];
            self.block_decoder
                .uncompress_block_unsorted(compressed_data, bit_width, false);
        } else {
            // that block is vint encoded.
            self.block_decoder
                .uncompress_vint_unsorted_until_end(compressed_data);
        }
        self.block_offset = self.anchor_offset + (block_rel_id * COMPRESSION_BLOCK_SIZE) as u64;
    }

    /// Fills a buffer with the positions `[offset..offset+output.len())` integers.
    ///
    /// This function is optimized to be called with increasing values of `offset`.
    pub fn read(&mut self, mut offset: u64, mut output: &mut [u32]) {
        if offset < self.anchor_offset {
            self.reset();
        }
        let delta_to_block_offset = offset as i64 - self.block_offset as i64;
        if !(0..128).contains(&delta_to_block_offset) {
            // The first position is not within the first block.
            // (Note that it could be before or after)
            // We need to possibly skip a few blocks, and decompress the first relevant  block.
            let delta_to_anchor_offset = offset - self.anchor_offset;
            let num_blocks_to_skip =
                (delta_to_anchor_offset / (COMPRESSION_BLOCK_SIZE as u64)) as usize;
            self.advance_num_blocks(num_blocks_to_skip);
            self.load_block(0);
        } else {
            // The request offset is within the loaded block.
            // We still need to advance anchor_offset to our current block.
            let num_blocks_to_skip =
                ((self.block_offset - self.anchor_offset) / COMPRESSION_BLOCK_SIZE as u64) as usize;
            self.advance_num_blocks(num_blocks_to_skip);
        }

        // At this point, the block containing offset is loaded, and anchor has
        // been updated to point to it as well.
        for i in 1.. {
            // we copy the part from block i - 1 that is relevant.
            let offset_in_block = (offset as usize) % COMPRESSION_BLOCK_SIZE;
            let remaining_in_block = COMPRESSION_BLOCK_SIZE - offset_in_block;
            if remaining_in_block >= output.len() {
                output.copy_from_slice(
                    &self.block_decoder.output_array()[offset_in_block..][..output.len()],
                );
                break;
            }
            output[..remaining_in_block]
                .copy_from_slice(&self.block_decoder.output_array()[offset_in_block..]);
            output = &mut output[remaining_in_block..];
            // we load block #i if necessary.
            offset += remaining_in_block as u64;
            self.load_block(i);
        }
    }
}
