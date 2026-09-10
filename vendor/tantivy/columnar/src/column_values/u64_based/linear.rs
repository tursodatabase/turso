use std::io;

use common::{BinarySerializable, OwnedBytes};
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use super::ColumnValues;
use super::line::Line;
use crate::RowId;
use crate::column_values::VecColumn;
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};

const HALF_SPACE: u64 = u64::MAX / 2;
const LINE_ESTIMATION_BLOCK_LEN: usize = 512;

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearReader {
    data: OwnedBytes,
    linear_params: LinearParams,
    stats: ColumnStats,
}

impl ColumnValues for LinearReader {
    #[inline]
    fn get_val(&self, doc: u32) -> u64 {
        let interpoled_val: u64 = self.linear_params.line.eval(doc);
        let bitpacked_diff = self.linear_params.bit_unpacker.get(doc, &self.data);
        interpoled_val.wrapping_add(bitpacked_diff)
    }

    #[inline(always)]
    fn min_value(&self) -> u64 {
        self.stats.min_value
    }

    #[inline(always)]
    fn max_value(&self) -> u64 {
        self.stats.max_value
    }

    #[inline]
    fn num_vals(&self) -> u32 {
        self.stats.num_rows
    }
}

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference bitpacked.
pub struct LinearCodec;

#[derive(Debug, Clone)]
struct LinearParams {
    line: Line,
    bit_unpacker: BitUnpacker,
}

impl BinarySerializable for LinearParams {
    fn serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.line.serialize(writer)?;
        self.bit_unpacker.bit_width().serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let line = Line::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        Ok(Self {
            line,
            bit_unpacker: BitUnpacker::new(bit_width),
        })
    }
}

pub struct LinearCodecEstimator {
    block: Vec<u64>,
    line: Option<Line>,
    row_id: RowId,
    min_deviation: u64,
    max_deviation: u64,
    first_val: u64,
    last_val: u64,
}

impl Default for LinearCodecEstimator {
    fn default() -> LinearCodecEstimator {
        LinearCodecEstimator {
            block: Vec::with_capacity(LINE_ESTIMATION_BLOCK_LEN),
            line: None,
            row_id: 0,
            min_deviation: u64::MAX,
            max_deviation: u64::MIN,
            first_val: 0u64,
            last_val: 0u64,
        }
    }
}

impl ColumnCodecEstimator for LinearCodecEstimator {
    fn finalize(&mut self) {
        if let Some(line) = self.line.as_mut() {
            line.intercept = line
                .intercept
                .wrapping_add(self.min_deviation)
                .wrapping_sub(HALF_SPACE);
        }
    }

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let line = self.line?;
        let amplitude = self.max_deviation - self.min_deviation;
        let num_bits = compute_num_bits(amplitude);
        let linear_params = LinearParams {
            line,
            bit_unpacker: BitUnpacker::new(num_bits),
        };
        Some(
            stats.num_bytes()
                + linear_params.num_bytes()
                + (num_bits as u64 * stats.num_rows as u64).div_ceil(8),
        )
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn io::Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let line = self.line.unwrap();
        let amplitude = self.max_deviation - self.min_deviation;
        let num_bits = compute_num_bits(amplitude);
        let linear_params = LinearParams {
            line,
            bit_unpacker: BitUnpacker::new(num_bits),
        };
        linear_params.serialize(wrt)?;
        let mut bit_packer = BitPacker::new();
        for (pos, value) in vals.enumerate() {
            let calculated_value = line.eval(pos as u32);
            let offset = value.wrapping_sub(calculated_value);
            bit_packer.write(offset, num_bits, wrt)?;
        }
        bit_packer.close(wrt)?;
        Ok(())
    }

    fn collect(&mut self, value: u64) {
        if let Some(line) = self.line {
            self.collect_after_line_estimation(&line, value);
        } else {
            self.collect_before_line_estimation(value);
        }
    }
}

impl LinearCodecEstimator {
    #[inline]
    fn collect_after_line_estimation(&mut self, line: &Line, value: u64) {
        let interpoled_val: u64 = line.eval(self.row_id);
        let deviation = value.wrapping_add(HALF_SPACE).wrapping_sub(interpoled_val);
        self.min_deviation = self.min_deviation.min(deviation);
        self.max_deviation = self.max_deviation.max(deviation);
        if self.row_id == 0 {
            self.first_val = value;
        }
        self.last_val = value;
        self.row_id += 1u32;
    }

    #[inline]
    fn collect_before_line_estimation(&mut self, value: u64) {
        self.block.push(value);
        if self.block.len() == LINE_ESTIMATION_BLOCK_LEN {
            let column = VecColumn::from(std::mem::take(&mut self.block));
            let line = Line::train(&column);
            self.block = column.into();
            let block = std::mem::take(&mut self.block);
            for val in block {
                self.collect_after_line_estimation(&line, val);
            }
            self.line = Some(line);
        }
    }
}

impl ColumnCodec for LinearCodec {
    type ColumnValues = LinearReader;

    type Estimator = LinearCodecEstimator;

    fn load(mut data: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut data)?;
        let linear_params = LinearParams::deserialize(&mut data)?;
        Ok(LinearReader {
            stats,
            linear_params,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use super::*;
    use crate::column_values::u64_based::tests::{create_and_validate, get_codec_test_datasets};

    #[test]
    fn test_compression_simple() {
        let vals = (100u64..)
            .take(super::LINE_ESTIMATION_BLOCK_LEN)
            .collect::<Vec<_>>();
        create_and_validate::<LinearCodec>(&vals, "simple monotonically large").unwrap();
    }

    #[test]
    fn test_compression() {
        let data = (10..=6_000_u64).collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate::<LinearCodec>(&data, "simple monotonically large").unwrap();
        assert_le!(actual_compression, 0.001);
        assert_le!(estimate, 0.02);
    }

    #[test]
    fn test_with_codec_datasets() {
        let data_sets = get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<LinearCodec>(&data, name);
            data.reverse();
            create_and_validate::<LinearCodec>(&data, name);
        }
    }
    #[test]
    fn linear_interpol_fast_field_test_large_amplitude() {
        let data = vec![
            i64::MAX as u64 / 2,
            i64::MAX as u64 / 3,
            i64::MAX as u64 / 2,
        ];
        create_and_validate::<LinearCodec>(&data, "large amplitude");
    }

    #[test]
    fn overflow_error_test() {
        let data = vec![1572656989877777, 1170935903116329, 720575940379279, 0];
        create_and_validate::<LinearCodec>(&data, "overflow test");
    }

    #[test]
    fn linear_interpol_fast_concave_data() {
        let data = vec![0, 1, 2, 5, 8, 10, 20, 50];
        create_and_validate::<LinearCodec>(&data, "concave data");
    }
    #[test]
    fn linear_interpol_fast_convex_data() {
        let data = vec![0, 40, 60, 70, 75, 77];
        create_and_validate::<LinearCodec>(&data, "convex data");
    }
    #[test]
    fn linear_interpol_fast_field_test_simple() {
        let data = (10..=20_u64).collect::<Vec<_>>();
        create_and_validate::<LinearCodec>(&data, "simple monotonically");
    }

    #[test]
    fn linear_interpol_fast_field_rand() {
        let mut rng = rand::rng();
        for _ in 0..50 {
            let mut data = (0..10_000).map(|_| rng.next_u64()).collect::<Vec<_>>();
            create_and_validate::<LinearCodec>(&data, "random");
            data.reverse();
            create_and_validate::<LinearCodec>(&data, "random");
        }
    }
}
