use std::io;

use crate::value::{ValueReader, ValueWriter, deserialize_vint_u64};
use crate::{BlockAddr, vint};

#[derive(Default)]
pub(crate) struct IndexValueReader {
    vals: Vec<BlockAddr>,
}

impl ValueReader for IndexValueReader {
    type Value = BlockAddr;

    #[inline(always)]
    fn value(&self, idx: usize) -> &Self::Value {
        &self.vals[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let original_num_bytes = data.len();
        let num_vals = deserialize_vint_u64(&mut data) as usize;
        self.vals.clear();
        let mut first_ordinal = 0u64;
        let mut prev_start = deserialize_vint_u64(&mut data) as usize;
        for _ in 0..num_vals {
            let len = deserialize_vint_u64(&mut data);
            let delta_ordinal = deserialize_vint_u64(&mut data);

            first_ordinal += delta_ordinal;
            let end = prev_start + len as usize;
            self.vals.push(BlockAddr {
                byte_range: prev_start..end,
                first_ordinal,
            });
            prev_start = end;
        }
        Ok(original_num_bytes - data.len())
    }
}

#[derive(Default)]
pub(crate) struct IndexValueWriter {
    vals: Vec<BlockAddr>,
}

impl ValueWriter for IndexValueWriter {
    type Value = BlockAddr;

    fn write(&mut self, val: &Self::Value) {
        self.vals.push(val.clone());
    }

    fn serialize_block(&self, output: &mut Vec<u8>) {
        let mut prev_ord = 0u64;
        vint::serialize_into_vec(self.vals.len() as u64, output);

        let start_pos = if let Some(block_addr) = self.vals.first() {
            block_addr.byte_range.start as u64
        } else {
            0
        };
        vint::serialize_into_vec(start_pos, output);

        // TODO use array_windows when it gets stabilized
        for elem in self.vals.windows(2) {
            let [current, next] = elem else {
                unreachable!("windows should always return exactly 2 elements");
            };
            let len = next.byte_range.start - current.byte_range.start;
            vint::serialize_into_vec(len as u64, output);
            let delta = current.first_ordinal - prev_ord;
            vint::serialize_into_vec(delta, output);
            prev_ord = current.first_ordinal;
        }
        if let Some(last) = self.vals.last() {
            let len = last.byte_range.end - last.byte_range.start;
            vint::serialize_into_vec(len as u64, output);
            let delta = last.first_ordinal - prev_ord;
            vint::serialize_into_vec(delta, output);
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
    fn test_index_reader_writer() {
        crate::value::tests::test_value_reader_writer::<_, IndexValueReader, IndexValueWriter>(&[]);
        crate::value::tests::test_value_reader_writer::<_, IndexValueReader, IndexValueWriter>(&[
            BlockAddr {
                byte_range: 0..10,
                first_ordinal: 0,
            },
        ]);
        crate::value::tests::test_value_reader_writer::<_, IndexValueReader, IndexValueWriter>(&[
            BlockAddr {
                byte_range: 0..10,
                first_ordinal: 0,
            },
            BlockAddr {
                byte_range: 10..20,
                first_ordinal: 5,
            },
        ]);
        crate::value::tests::test_value_reader_writer::<_, IndexValueReader, IndexValueWriter>(&[
            BlockAddr {
                byte_range: 0..10,
                first_ordinal: 0,
            },
            BlockAddr {
                byte_range: 10..20,
                first_ordinal: 5,
            },
            BlockAddr {
                byte_range: 20..30,
                first_ordinal: 10,
            },
        ]);
        crate::value::tests::test_value_reader_writer::<_, IndexValueReader, IndexValueWriter>(&[
            BlockAddr {
                byte_range: 5..10,
                first_ordinal: 2,
            },
        ]);
    }
}
