use std::io;
use std::ops::{Range, RangeInclusive};

use bitpacking::{BitPacker as ExternalBitPackerTrait, BitPacker1x};

pub struct BitPacker {
    mini_buffer: u64,
    mini_buffer_written: usize,
}

impl Default for BitPacker {
    fn default() -> Self {
        BitPacker::new()
    }
}
impl BitPacker {
    pub fn new() -> BitPacker {
        BitPacker {
            mini_buffer: 0u64,
            mini_buffer_written: 0,
        }
    }

    #[inline]
    pub fn write<TWrite: io::Write + ?Sized>(
        &mut self,
        val: u64,
        num_bits: u8,
        output: &mut TWrite,
    ) -> io::Result<()> {
        let num_bits = num_bits as usize;
        if self.mini_buffer_written + num_bits > 64 {
            self.mini_buffer |= val.wrapping_shl(self.mini_buffer_written as u32);
            output.write_all(self.mini_buffer.to_le_bytes().as_ref())?;
            self.mini_buffer = val.wrapping_shr((64 - self.mini_buffer_written) as u32);
            self.mini_buffer_written = self.mini_buffer_written + num_bits - 64;
        } else {
            self.mini_buffer |= val << self.mini_buffer_written;
            self.mini_buffer_written += num_bits;
            if self.mini_buffer_written == 64 {
                output.write_all(self.mini_buffer.to_le_bytes().as_ref())?;
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u64;
            }
        }
        Ok(())
    }

    pub fn flush<TWrite: io::Write + ?Sized>(&mut self, output: &mut TWrite) -> io::Result<()> {
        if self.mini_buffer_written > 0 {
            let num_bytes = self.mini_buffer_written.div_ceil(8);
            let bytes = self.mini_buffer.to_le_bytes();
            output.write_all(&bytes[..num_bytes])?;
            self.mini_buffer_written = 0;
            self.mini_buffer = 0;
        }
        Ok(())
    }

    pub fn close<TWrite: io::Write + ?Sized>(&mut self, output: &mut TWrite) -> io::Result<()> {
        self.flush(output)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Copy)]
pub struct BitUnpacker {
    num_bits: usize,
    mask: u64,
}

impl BitUnpacker {
    /// Creates a bit unpacker, that assumes the same bitwidth for all values.
    ///
    /// The bitunpacker works by doing an unaligned read of 8 bytes.
    /// For this reason, values of `num_bits` between
    /// [57..63] are forbidden.
    pub fn new(num_bits: u8) -> BitUnpacker {
        assert!(num_bits <= 7 * 8 || num_bits == 64);
        let mask: u64 = if num_bits == 64 {
            !0u64
        } else {
            (1u64 << num_bits) - 1u64
        };
        BitUnpacker {
            num_bits: usize::from(num_bits),
            mask,
        }
    }

    pub fn bit_width(&self) -> u8 {
        self.num_bits as u8
    }

    #[inline]
    pub fn get(&self, idx: u32, data: &[u8]) -> u64 {
        let addr_in_bits = idx as usize * self.num_bits;
        let addr = addr_in_bits >> 3;
        if addr + 8 > data.len() {
            if self.num_bits == 0 {
                return 0;
            }
            let bit_shift = addr_in_bits & 7;
            return self.get_slow_path(addr, bit_shift as u32, data);
        }
        let bit_shift = addr_in_bits & 7;
        let bytes: [u8; 8] = (&data[addr..addr + 8]).try_into().unwrap();
        let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
        let val_shifted = val_unshifted_unmasked >> bit_shift;
        val_shifted & self.mask
    }

    #[inline(never)]
    fn get_slow_path(&self, addr: usize, bit_shift: u32, data: &[u8]) -> u64 {
        let mut bytes: [u8; 8] = [0u8; 8];
        let available_bytes = data.len() - addr;
        // This function is meant to only be called if we did not have 8 bytes to load.
        debug_assert!(available_bytes < 8);
        bytes[..available_bytes].copy_from_slice(&data[addr..]);
        let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
        let val_shifted = val_unshifted_unmasked >> bit_shift;
        val_shifted & self.mask
    }

    // Decodes the range of bitpacked `u32` values with idx
    // in [start_idx, start_idx + output.len()).
    //
    // #Panics
    //
    // This methods panics if `num_bits` is > 32.
    fn get_batch_u32s(&self, start_idx: u32, data: &[u8], output: &mut [u32]) {
        assert!(
            self.bit_width() <= 32,
            "Bitwidth must be <= 32 to use this method."
        );

        let end_idx: u32 = start_idx + output.len() as u32;

        // We use `usize` here to avoid overflow issues.
        let end_bit_read = (end_idx as usize) * self.num_bits;
        let end_byte_read = end_bit_read.div_ceil(8);
        assert!(
            end_byte_read <= data.len(),
            "Requested index is out of bounds."
        );

        // Simple slow implementation of get_batch_u32s, to deal with our ramps.
        let get_batch_ramp = |start_idx: u32, output: &mut [u32]| {
            for (out, idx) in output.iter_mut().zip(start_idx..) {
                *out = self.get(idx, data) as u32;
            }
        };

        // We use an unrolled routine to decode 32 values at once.
        // We therefore decompose our range of values to decode into three ranges:
        // - Entrance ramp: [start_idx, fast_track_start) (up to 31 values)
        // - Highway: [fast_track_start, fast_track_end) (a length multiple of 32s)
        // - Exit ramp: [fast_track_end, start_idx + output.len()) (up to 31 values)

        // We want the start of the fast track to start align with bytes.
        // A sufficient condition is to start with an idx that is a multiple of 8,
        // so highway start is the closest multiple of 8 that is >= start_idx.
        let entrance_ramp_len: u32 = 8 - (start_idx % 8) % 8;

        let highway_start: u32 = start_idx + entrance_ramp_len;

        if highway_start + (BitPacker1x::BLOCK_LEN as u32) > end_idx {
            // We don't have enough values to have even a single block of highway.
            // Let's just supply the values the simple way.
            get_batch_ramp(start_idx, output);
            return;
        }

        let num_blocks: usize = (end_idx - highway_start) as usize / BitPacker1x::BLOCK_LEN;

        // Entrance ramp
        get_batch_ramp(start_idx, &mut output[..entrance_ramp_len as usize]);

        // Highway
        let mut offset = (highway_start as usize * self.num_bits) / 8;
        let mut output_cursor = (highway_start - start_idx) as usize;
        for _ in 0..num_blocks {
            offset += BitPacker1x.decompress(
                &data[offset..],
                &mut output[output_cursor..],
                self.num_bits as u8,
            );
            output_cursor += 32;
        }

        // Exit ramp
        let highway_end: u32 = highway_start + (num_blocks * BitPacker1x::BLOCK_LEN) as u32;
        get_batch_ramp(highway_end, &mut output[output_cursor..]);
    }

    pub fn get_ids_for_value_range(
        &self,
        range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        if self.bit_width() > 32 {
            self.get_ids_for_value_range_slow(range, id_range, data, positions)
        } else {
            if *range.start() > u32::MAX as u64 {
                positions.clear();
                return;
            }
            let range_u32 = (*range.start() as u32)..=(*range.end()).min(u32::MAX as u64) as u32;
            self.get_ids_for_value_range_fast(range_u32, id_range, data, positions)
        }
    }

    fn get_ids_for_value_range_slow(
        &self,
        range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        positions.clear();
        for i in id_range {
            // If we cared we could make this branchless, but the slow implementation should rarely
            // kick in.
            let val = self.get(i, data);
            if range.contains(&val) {
                positions.push(i);
            }
        }
    }

    fn get_ids_for_value_range_fast(
        &self,
        value_range: RangeInclusive<u32>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        positions.resize(id_range.len(), 0u32);
        self.get_batch_u32s(id_range.start, data, positions);
        crate::filter_vec::filter_vec_in_place(value_range, id_range.start, positions)
    }
}

#[cfg(test)]
mod test {
    use super::{BitPacker, BitUnpacker};

    fn create_bitpacker(len: usize, num_bits: u8) -> (BitUnpacker, Vec<u64>, Vec<u8>) {
        let mut data = Vec::new();
        let mut bitpacker = BitPacker::new();
        let max_val: u64 = (1u64 << num_bits as u64) - 1u64;
        let vals: Vec<u64> = (0u64..len as u64)
            .map(|i| if max_val == 0 { 0 } else { i % max_val })
            .collect();
        for &val in &vals {
            bitpacker.write(val, num_bits, &mut data).unwrap();
        }
        bitpacker.close(&mut data).unwrap();
        assert_eq!(data.len(), ((num_bits as usize) * len).div_ceil(8));
        let bitunpacker = BitUnpacker::new(num_bits);
        (bitunpacker, vals, data)
    }

    fn test_bitpacker_util(len: usize, num_bits: u8) {
        let (bitunpacker, vals, data) = create_bitpacker(len, num_bits);
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(bitunpacker.get(i as u32, &data), *val);
        }
    }

    #[test]
    fn test_bitpacker() {
        test_bitpacker_util(10, 3);
        test_bitpacker_util(10, 0);
        test_bitpacker_util(10, 1);
        test_bitpacker_util(6, 14);
        test_bitpacker_util(1000, 14);
    }

    use proptest::prelude::*;

    fn num_bits_strategy() -> impl Strategy<Value = u8> {
        prop_oneof!(Just(0), Just(1), 2u8..56u8, Just(56), Just(64),)
    }

    fn vals_strategy() -> impl Strategy<Value = (u8, Vec<u64>)> {
        (num_bits_strategy(), 0usize..100usize).prop_flat_map(|(num_bits, len)| {
            let max_val = if num_bits == 64 {
                u64::MAX
            } else {
                (1u64 << num_bits as u32) - 1
            };
            let vals = proptest::collection::vec(0..=max_val, len);
            vals.prop_map(move |vals| (num_bits, vals))
        })
    }

    fn test_bitpacker_aux(num_bits: u8, vals: &[u64]) {
        let mut buffer: Vec<u8> = Vec::new();
        let mut bitpacker = BitPacker::new();
        for &val in vals {
            bitpacker.write(val, num_bits, &mut buffer).unwrap();
        }
        bitpacker.flush(&mut buffer).unwrap();
        assert_eq!(buffer.len(), (vals.len() * num_bits as usize).div_ceil(8));
        let bitunpacker = BitUnpacker::new(num_bits);
        let max_val = if num_bits == 64 {
            u64::MAX
        } else {
            (1u64 << num_bits) - 1
        };
        for (i, val) in vals.iter().copied().enumerate() {
            assert!(val <= max_val);
            assert_eq!(bitunpacker.get(i as u32, &buffer), val);
        }
    }

    proptest::proptest! {
        #[test]
        fn test_bitpacker_proptest((num_bits, vals) in vals_strategy()) {
            test_bitpacker_aux(num_bits, &vals);
        }
    }

    #[test]
    #[should_panic]
    fn test_get_batch_panics_over_32_bits() {
        let bitunpacker = BitUnpacker::new(33);
        let mut output: [u32; 1] = [0u32];
        bitunpacker.get_batch_u32s(0, &[0, 0, 0, 0, 0, 0, 0, 0], &mut output[..]);
    }

    #[test]
    fn test_get_batch_limit() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u32; 3] = [0u32, 0u32, 0u32];
        bitunpacker.get_batch_u32s(8 * 4 - 3, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    #[test]
    #[should_panic]
    fn test_get_batch_panics_when_off_scope() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u32; 3] = [0u32, 0u32, 0u32];
        // We are missing exactly one bit.
        bitunpacker.get_batch_u32s(8 * 4 - 2, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    proptest::proptest! {
        #[test]
        fn test_get_batch_u32s_proptest(num_bits in 0u8..=32u8) {
            let mask =
                if num_bits == 32u8 {
                    u32::MAX
                } else {
                    (1u32 << num_bits) - 1
                };
            let mut buffer: Vec<u8> = Vec::new();
            let mut bitpacker = BitPacker::new();
            for val in 0..100 {
                bitpacker.write(val & mask as u64, num_bits, &mut buffer).unwrap();
            }
            bitpacker.flush(&mut buffer).unwrap();
            let bitunpacker = BitUnpacker::new(num_bits);
            let mut output: Vec<u32> = Vec::new();
            for len in [0, 1, 2, 32, 33, 34, 64] {
                for start_idx in 0u32..32u32 {
                    output.resize(len, 0);
                    bitunpacker.get_batch_u32s(start_idx, &buffer, &mut output);
                    for (i, output_byte) in output.iter().enumerate() {
                        let expected = (start_idx + i as u32) & mask;
                        assert_eq!(*output_byte, expected);
                    }
                }
            }
        }
    }
}
