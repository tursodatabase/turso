use std::io;
use std::ops::Range;

use crate::value::{ValueReader, ValueWriter, deserialize_vint_u64};

/// See module comment.
#[derive(Default)]
pub struct RangeValueReader {
    vals: Vec<Range<u64>>,
}

impl ValueReader for RangeValueReader {
    type Value = Range<u64>;

    #[inline(always)]
    fn value(&self, idx: usize) -> &Range<u64> {
        &self.vals[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        self.vals.clear();
        let original_num_bytes = data.len();
        let len = deserialize_vint_u64(&mut data) as usize;
        if len != 0 {
            let mut prev_val = deserialize_vint_u64(&mut data);
            for _ in 1..len {
                let next_val = prev_val + deserialize_vint_u64(&mut data);
                self.vals.push(prev_val..next_val);
                prev_val = next_val;
            }
        }
        Ok(original_num_bytes - data.len())
    }
}

/// Range writer. The range are required to partition the
/// space.
///
/// In other words, two consecutive keys `k1` and `k2`
/// are required to observe
/// `range_sstable[k1].end == range_sstable[k2].start`.
///
/// The writer will panic if the inserted value do not follow
/// this property.
///
/// The first range is not required to start at `0`.
#[derive(Default)]
pub struct RangeValueWriter {
    vals: Vec<u64>,
}

impl ValueWriter for RangeValueWriter {
    type Value = Range<u64>;

    fn write(&mut self, val: &Range<u64>) {
        if let Some(previous_offset) = self.vals.last().copied() {
            assert_eq!(previous_offset, val.start);
            self.vals.push(val.end);
        } else {
            self.vals.push(val.start);
            self.vals.push(val.end)
        }
    }

    fn serialize_block(&self, writer: &mut Vec<u8>) {
        let mut prev_val = 0u64;
        crate::vint::serialize_into_vec(self.vals.len() as u64, writer);
        for &val in &self.vals {
            let delta = val - prev_val;
            crate::vint::serialize_into_vec(delta, writer);
            prev_val = val;
        }
    }

    fn clear(&mut self) {
        self.vals.clear();
    }
}

#[cfg(test)]
#[expect(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;

    #[test]
    fn test_range_reader_writer() {
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[]);
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[
            0..3,
        ]);
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[
            0..3,
            3..10,
        ]);
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[
            0..0,
            0..10,
        ]);
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[
            100..110,
            110..121,
            121..1250,
        ]);
    }

    #[test]
    #[should_panic]
    fn test_range_reader_writer_panics() {
        crate::value::tests::test_value_reader_writer::<_, RangeValueReader, RangeValueWriter>(&[
            1..3,
            4..10,
        ]);
    }
}
