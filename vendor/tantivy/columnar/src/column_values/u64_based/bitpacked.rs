use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::{Range, RangeInclusive};

use common::{BinarySerializable, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::{ColumnValues, RowId};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    stats: ColumnStats,
}

#[inline(always)]
const fn div_ceil(n: u64, q: NonZeroU64) -> u64 {
    // copied from unstable rust standard library.
    let d = n / q.get();
    let r = n % q.get();
    if r > 0 { d + 1 } else { d }
}

// The bitpacked codec applies a linear transformation `f` over data that are bitpacked.
// f is defined by:
// f: bitpacked -> stats.min_value + stats.gcd * bitpacked
//
// In order to run range queries, we invert the transformation.
// `transform_range_before_linear_transformation` returns the range of values
// [min_bipacked_value..max_bitpacked_value] such that
// f(bitpacked) ∈ [min_value, max_value] <=> bitpacked ∈ [min_bitpacked_value, max_bitpacked_value]
fn transform_range_before_linear_transformation(
    stats: &ColumnStats,
    range: RangeInclusive<u64>,
) -> Option<RangeInclusive<u64>> {
    if range.is_empty() {
        return None;
    }
    let shifted_range =
        range.start().saturating_sub(stats.min_value)..=range.end().saturating_sub(stats.min_value);
    let start_before_gcd_multiplication: u64 = div_ceil(*shifted_range.start(), stats.gcd);
    let end_before_gcd_multiplication: u64 = *shifted_range.end() / stats.gcd;
    Some(start_before_gcd_multiplication..=end_before_gcd_multiplication)
}

impl ColumnValues for BitpackedReader {
    #[inline(always)]
    fn get_val(&self, doc: u32) -> u64 {
        self.stats.min_value + self.stats.gcd.get() * self.bit_unpacker.get(doc, &self.data)
    }
    #[inline]
    fn min_value(&self) -> u64 {
        self.stats.min_value
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.stats.max_value
    }
    #[inline]
    fn num_vals(&self) -> RowId {
        self.stats.num_rows
    }

    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<u64>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let Some(transformed_range) =
            transform_range_before_linear_transformation(&self.stats, range)
        else {
            positions.clear();
            return;
        };
        self.bit_unpacker.get_ids_for_value_range(
            transformed_range,
            doc_id_range,
            &self.data,
            positions,
        );
    }
}

fn num_bits(stats: &ColumnStats) -> u8 {
    compute_num_bits(stats.amplitude() / stats.gcd)
}

#[derive(Default)]
pub struct BitpackedCodecEstimator;

impl ColumnCodecEstimator for BitpackedCodecEstimator {
    fn collect(&mut self, _value: u64) {}

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let num_bits_per_value = num_bits(stats);
        Some(stats.num_bytes() + (stats.num_rows as u64 * (num_bits_per_value as u64)).div_ceil(8))
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let num_bits = num_bits(stats);
        let mut bit_packer = BitPacker::new();
        let divider = DividerU64::divide_by(stats.gcd.get());
        for val in vals {
            bit_packer.write(divider.divide(val - stats.min_value), num_bits, wrt)?;
        }
        bit_packer.close(wrt)?;
        Ok(())
    }
}

pub struct BitpackedCodec;

impl ColumnCodec for BitpackedCodec {
    type ColumnValues = BitpackedReader;
    type Estimator = BitpackedCodecEstimator;

    /// Opens a fast field given a file.
    fn load(mut data: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut data)?;
        let num_bits = num_bits(&stats);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedReader {
            data,
            bit_unpacker,
            stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_with_codec_data_sets_simple() {
        create_and_validate::<BitpackedCodec>(&[4, 3, 12], "name");
    }

    #[test]
    fn test_with_codec_data_sets_simple_gcd() {
        create_and_validate::<BitpackedCodec>(&[1000, 2000, 3000], "name");
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = crate::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BitpackedCodec>(&data, name);
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, name);
        }
    }

    #[test]
    fn bitpacked_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BitpackedCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, "rand");
        }
    }
}
