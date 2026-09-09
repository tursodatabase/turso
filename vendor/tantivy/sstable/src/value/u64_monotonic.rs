use std::io;

use crate::value::{ValueReader, ValueWriter, deserialize_vint_u64};
use crate::vint;

#[derive(Default)]
pub struct U64MonotonicValueReader {
    vals: Vec<u64>,
}

impl ValueReader for U64MonotonicValueReader {
    type Value = u64;

    #[inline(always)]
    fn value(&self, idx: usize) -> &Self::Value {
        &self.vals[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let original_num_bytes = data.len();
        let num_vals = deserialize_vint_u64(&mut data) as usize;
        self.vals.clear();
        let mut prev_val = 0u64;
        for _ in 0..num_vals {
            let delta = deserialize_vint_u64(&mut data);
            let val = prev_val + delta;
            self.vals.push(val);
            prev_val = val;
        }
        Ok(original_num_bytes - data.len())
    }
}

#[derive(Default)]
pub struct U64MonotonicValueWriter {
    vals: Vec<u64>,
}

impl ValueWriter for U64MonotonicValueWriter {
    type Value = u64;

    fn write(&mut self, val: &Self::Value) {
        self.vals.push(*val);
    }

    fn serialize_block(&self, output: &mut Vec<u8>) {
        let mut prev_val = 0u64;
        vint::serialize_into_vec(self.vals.len() as u64, output);
        for &val in &self.vals {
            let delta = val - prev_val;
            vint::serialize_into_vec(delta, output);
            prev_val = val;
        }
    }

    fn clear(&mut self) {
        self.vals.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_monotonic_reader_writer() {
        crate::value::tests::test_value_reader_writer::<
            _,
            U64MonotonicValueReader,
            U64MonotonicValueWriter,
        >(&[]);
        crate::value::tests::test_value_reader_writer::<
            _,
            U64MonotonicValueReader,
            U64MonotonicValueWriter,
        >(&[5]);
        crate::value::tests::test_value_reader_writer::<
            _,
            U64MonotonicValueReader,
            U64MonotonicValueWriter,
        >(&[1u64, 30u64]);
    }
}
