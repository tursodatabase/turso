use bitpacking::{BitPacker, BitPacker4x};

pub const COMPRESSION_BLOCK_SIZE: usize = BitPacker4x::BLOCK_LEN;
// in vint encoding, each byte stores 7 bits of data, so we need at most 32 / 7 = 4.57 bytes to
// store a u32 in the worst case, rounding up to 5 bytes total
const MAX_VINT_SIZE: usize = 5;
const COMPRESSED_BLOCK_MAX_SIZE: usize = COMPRESSION_BLOCK_SIZE * MAX_VINT_SIZE;

mod vint;

/// Returns the size in bytes of a compressed block, given `num_bits`.
#[inline]
pub fn compressed_block_size(num_bits: u8) -> usize {
    (num_bits as usize) * COMPRESSION_BLOCK_SIZE / 8
}

pub struct BlockEncoder {
    bitpacker: BitPacker4x,
    pub output: [u8; COMPRESSED_BLOCK_MAX_SIZE],
}

impl Default for BlockEncoder {
    fn default() -> Self {
        BlockEncoder::new()
    }
}

impl BlockEncoder {
    pub fn new() -> BlockEncoder {
        BlockEncoder {
            bitpacker: BitPacker4x::new(),
            output: [0u8; COMPRESSED_BLOCK_MAX_SIZE],
        }
    }

    pub fn compress_block_sorted(&mut self, block: &[u32], offset: u32) -> (u8, &[u8]) {
        // if offset is zero, convert it to None. This is correct as long as we do the same when
        // decompressing. It's required in case the block starts with an actual zero.
        let offset = if offset == 0u32 { None } else { Some(offset) };

        let num_bits = self.bitpacker.num_bits_strictly_sorted(offset, block);
        let written_size =
            self.bitpacker
                .compress_strictly_sorted(offset, block, &mut self.output[..], num_bits);
        (num_bits, &self.output[..written_size])
    }

    /// Compress a single block of unsorted numbers.
    ///
    /// If `minus_one_encoded` is set, each value must be >= 1, and will be encoded in a sligly
    /// more compact format. This is useful for some values where 0 isn't a correct value, such
    /// as term frequency, but isn't correct for some usages like position lists, where 0 can
    /// appear.
    pub fn compress_block_unsorted(
        &mut self,
        block: &[u32],
        minus_one_encoded: bool,
    ) -> (u8, &[u8]) {
        debug_assert!(!minus_one_encoded || !block.contains(&0));

        let mut block_minus_one = [0; COMPRESSION_BLOCK_SIZE];
        let block = if minus_one_encoded {
            for (elem_min_one, elem) in block_minus_one.iter_mut().zip(block) {
                *elem_min_one = elem - 1;
            }
            &block_minus_one
        } else {
            block
        };

        let num_bits = self.bitpacker.num_bits(block);
        let written_size = self
            .bitpacker
            .compress(block, &mut self.output[..], num_bits);
        (num_bits, &self.output[..written_size])
    }
}

#[derive(Clone)]
pub struct BlockDecoder {
    bitpacker: BitPacker4x,
    output: [u32; COMPRESSION_BLOCK_SIZE],
    pub output_len: usize,
}

impl Default for BlockDecoder {
    fn default() -> Self {
        BlockDecoder::with_val(0u32)
    }
}

impl BlockDecoder {
    pub fn with_val(val: u32) -> BlockDecoder {
        BlockDecoder {
            bitpacker: BitPacker4x::new(),
            output: [val; COMPRESSION_BLOCK_SIZE],
            output_len: 0,
        }
    }

    /// Decompress block of sorted integers.
    ///
    /// `strict_delta` depends on what encoding was used. Older version of tantivy never use strict
    /// deltas, newer versions always use them.
    pub fn uncompress_block_sorted(
        &mut self,
        compressed_data: &[u8],
        offset: u32,
        num_bits: u8,
        strict_delta: bool,
    ) -> usize {
        if strict_delta {
            let offset = std::num::NonZeroU32::new(offset).map(std::num::NonZeroU32::get);

            self.output_len = COMPRESSION_BLOCK_SIZE;
            self.bitpacker.decompress_strictly_sorted(
                offset,
                compressed_data,
                &mut self.output,
                num_bits,
            )
        } else {
            self.output_len = COMPRESSION_BLOCK_SIZE;
            self.bitpacker
                .decompress_sorted(offset, compressed_data, &mut self.output, num_bits)
        }
    }

    /// Decompress block of unsorted integers.
    ///
    /// `minus_one_encoded` depends on what encoding was used. Older version of tantivy never use
    /// that encoding. Newer version use it for some structures, but not all. See the corresponding
    /// call to `BlockEncoder::compress_block_unsorted`.
    pub fn uncompress_block_unsorted(
        &mut self,
        compressed_data: &[u8],
        num_bits: u8,
        minus_one_encoded: bool,
    ) -> usize {
        self.output_len = COMPRESSION_BLOCK_SIZE;
        let res = self
            .bitpacker
            .decompress(compressed_data, &mut self.output, num_bits);
        if minus_one_encoded {
            for val in &mut self.output {
                *val += 1;
            }
        }
        res
    }

    #[inline]
    pub fn output_array(&self) -> &[u32] {
        &self.output[..self.output_len]
    }

    /// Return in-block index of first value >= `target`.
    /// Uses the padded buffer to enable branchless search.
    #[inline]
    pub(crate) fn seek_within_block(&self, target: u32) -> usize {
        crate::postings::branchless_binary_search(&self.output, target)
    }

    #[inline]
    pub fn output(&self, idx: usize) -> u32 {
        self.output[idx]
    }
}

pub trait VIntEncoder {
    /// Compresses an array of `u32` integers,
    /// using [delta-encoding](https://en.wikipedia.org/wiki/Delta_encoding)
    /// and variable bytes encoding.
    ///
    /// The method takes an array of ints to compress, and returns
    /// a `&[u8]` representing the compressed data.
    ///
    /// The method also takes an offset to give the value of the
    /// hypothetical previous element in the delta-encoding.
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8];

    /// Compresses an array of `u32` integers,
    /// using variable bytes encoding.
    ///
    /// The method takes an array of ints to compress, and returns
    /// a `&[u8]` representing the compressed data.
    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8];
}

pub trait VIntDecoder {
    /// Uncompress an array of `u32` integers,
    /// that were compressed using [delta-encoding](https://en.wikipedia.org/wiki/Delta_encoding)
    /// and variable bytes encoding.
    ///
    /// The method takes a number of int to decompress, and returns
    /// the amount of bytes that were read to decompress them.
    ///
    /// The method also takes an offset to give the value of the
    /// hypothetical previous element in the delta-encoding.
    ///
    /// For instance, if delta encoded are `1, 3, 9`, and the
    /// `offset` is 5, then the output will be:
    /// `5 + 1 = 6, 6 + 3= 9, 9 + 9 = 18`
    ///
    /// The value given in `padding` will be used to fill the remaining `128 - num_els` values.
    fn uncompress_vint_sorted(
        &mut self,
        compressed_data: &[u8],
        offset: u32,
        num_els: usize,
        padding: u32,
    ) -> usize;

    /// Uncompress an array of `u32s`, compressed using variable
    /// byte encoding.
    ///
    /// The method takes a number of int to decompress, and returns
    /// the amount of bytes that were read to decompress them.
    ///
    /// The value given in `padding` will be used to fill the remaining `128 - num_els` values.
    fn uncompress_vint_unsorted(
        &mut self,
        compressed_data: &[u8],
        num_els: usize,
        padding: u32,
    ) -> usize;

    fn uncompress_vint_unsorted_until_end(&mut self, compressed_data: &[u8]);
}

impl VIntEncoder for BlockEncoder {
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8] {
        vint::compress_sorted(input, &mut self.output, offset)
    }

    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8] {
        vint::compress_unsorted(input, &mut self.output)
    }
}

impl VIntDecoder for BlockDecoder {
    fn uncompress_vint_sorted(
        &mut self,
        compressed_data: &[u8],
        offset: u32,
        num_els: usize,
        padding: u32,
    ) -> usize {
        self.output_len = num_els;
        self.output.iter_mut().for_each(|el| *el = padding);
        vint::uncompress_sorted(compressed_data, &mut self.output[..num_els], offset)
    }

    fn uncompress_vint_unsorted(
        &mut self,
        compressed_data: &[u8],
        num_els: usize,
        padding: u32,
    ) -> usize {
        self.output_len = num_els;
        self.output.iter_mut().for_each(|el| *el = padding);
        vint::uncompress_unsorted(compressed_data, &mut self.output[..num_els])
    }

    fn uncompress_vint_unsorted_until_end(&mut self, compressed_data: &[u8]) {
        let num_els = vint::uncompress_unsorted_until_end(compressed_data, &mut self.output);
        self.output_len = num_els;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::TERMINATED;

    #[test]
    fn test_encode_sorted_block() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| i * 7).collect();
        let mut encoder = BlockEncoder::new();
        let (num_bits, compressed_data) = encoder.compress_block_sorted(&vals, 0);
        let mut decoder = BlockDecoder::default();
        {
            let consumed_num_bytes =
                decoder.uncompress_block_sorted(compressed_data, 0, num_bits, true);
            assert_eq!(consumed_num_bytes, compressed_data.len());
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output(i));
        }
    }

    #[test]
    fn test_encode_sorted_block_with_offset() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| 11 + i * 7).collect();
        let mut encoder = BlockEncoder::default();
        let (num_bits, compressed_data) = encoder.compress_block_sorted(&vals, 10);
        let mut decoder = BlockDecoder::default();
        {
            let consumed_num_bytes =
                decoder.uncompress_block_sorted(compressed_data, 10, num_bits, true);
            assert_eq!(consumed_num_bytes, compressed_data.len());
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output(i));
        }
    }

    #[test]
    fn test_encode_sorted_block_with_junk() {
        let mut compressed: Vec<u8> = Vec::new();
        let n = 128;
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32) * 7u32).collect();
        let mut encoder = BlockEncoder::default();
        let (num_bits, compressed_data) = encoder.compress_block_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = BlockDecoder::default();
        {
            let consumed_num_bytes =
                decoder.uncompress_block_sorted(&compressed, 10, num_bits, true);
            assert_eq!(consumed_num_bytes, compressed.len() - 1);
            assert_eq!(compressed[consumed_num_bytes], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output(i));
        }
    }

    #[test]
    fn test_encode_unsorted_block_with_junk() {
        for minus_one_encode in [false, true] {
            let mut compressed: Vec<u8> = Vec::new();
            let n = 128;
            let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32) * 7u32 % 12).collect();
            let mut encoder = BlockEncoder::default();
            let (num_bits, compressed_data) =
                encoder.compress_block_unsorted(&vals, minus_one_encode);
            compressed.extend_from_slice(compressed_data);
            compressed.push(173u8);
            let mut decoder = BlockDecoder::default();
            {
                let consumed_num_bytes =
                    decoder.uncompress_block_unsorted(&compressed, num_bits, minus_one_encode);
                assert_eq!(consumed_num_bytes + 1, compressed.len());
                assert_eq!(compressed[consumed_num_bytes], 173u8);
            }
            for i in 0..n {
                assert_eq!(vals[i], decoder.output(i));
            }
        }
    }

    #[test]
    fn test_block_decoder_initialization() {
        let block = BlockDecoder::with_val(TERMINATED);
        assert_eq!(block.output(0), TERMINATED);
    }
    #[test]
    fn test_encode_vint() {
        const PADDING_VALUE: u32 = 234_234_345u32;
        let expected_length = 154;
        let mut encoder = BlockEncoder::new();
        let input: Vec<u32> = (0u32..123u32).map(|i| 4 + i * 7 / 2).collect();
        for offset in &[0u32, 1u32, 2u32] {
            let encoded_data = encoder.compress_vint_sorted(&input, *offset);
            assert!(encoded_data.len() <= expected_length);
            let mut decoder = BlockDecoder::default();
            let consumed_num_bytes =
                decoder.uncompress_vint_sorted(encoded_data, *offset, input.len(), PADDING_VALUE);
            assert_eq!(consumed_num_bytes, encoded_data.len());
            assert_eq!(input, decoder.output_array());
            for i in input.len()..COMPRESSION_BLOCK_SIZE {
                assert_eq!(decoder.output(i), PADDING_VALUE);
            }
        }
    }

    #[test]
    fn test_compress_vint_unsorted_does_not_overflow() {
        let mut encoder = BlockEncoder::new();
        let input: Vec<u32> = vec![u32::MAX; COMPRESSION_BLOCK_SIZE];
        encoder.compress_vint_unsorted(&input);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;

    use super::*;
    use crate::TERMINATED;

    fn generate_array_with_seed(n: usize, ratio: f64, seed_val: u8) -> Vec<u32> {
        let mut seed: [u8; 32] = [0; 32];
        seed[31] = seed_val;
        let mut rng = StdRng::from_seed(seed);
        (0u32..)
            .filter(|_| rng.random_bool(ratio))
            .take(n)
            .collect()
    }

    pub fn generate_array(n: usize, ratio: f64) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }

    #[bench]
    fn bench_compress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = generate_array(COMPRESSION_BLOCK_SIZE, 0.1);
        b.iter(|| {
            encoder.compress_block_sorted(&data, 0u32);
        });
    }

    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = generate_array(COMPRESSION_BLOCK_SIZE, 0.1);
        let (num_bits, compressed) = encoder.compress_block_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::default();
        b.iter(|| {
            decoder.uncompress_block_sorted(compressed, 0u32, num_bits, true);
        });
    }

    //#[test]
    // fn test_all_docs_compression_numbits() {
    // for expected_num_bits in 0u8.. {
    // let mut data = [0u32; 128];
    // if expected_num_bits > 0 {
    // data[0] = (1u64 << (expected_num_bits as usize) - 1) as u32;
    //}
    // let mut encoder = BlockEncoder::new();
    // let (num_bits, compressed) = encoder.compress_block_unsorted(&data);
    // assert_eq!(compressed.len(), compressed_block_size(num_bits));
    //}

    const NUM_INTS_BENCH_VINT: usize = 10;

    #[bench]
    fn bench_compress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = generate_array(NUM_INTS_BENCH_VINT, 0.001);
        b.iter(|| {
            encoder.compress_vint_sorted(&data, 0u32);
        });
    }

    #[bench]
    fn bench_uncompress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = generate_array(NUM_INTS_BENCH_VINT, 0.001);
        let compressed = encoder.compress_vint_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::default();
        b.iter(|| {
            decoder.uncompress_vint_sorted(compressed, 0u32, NUM_INTS_BENCH_VINT, TERMINATED);
        });
    }
}
