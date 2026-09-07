use std::io;

use super::{ValueReader, ValueWriter};

#[derive(Default)]
pub struct VecU32ValueReader {
    vals: Vec<Vec<u32>>,
}

impl ValueReader for VecU32ValueReader {
    type Value = Vec<u32>;

    #[inline(always)]
    fn value(&self, idx: usize) -> &Self::Value {
        &self.vals[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let original_num_bytes = data.len();
        self.vals.clear();

        // The first 4 bytes are the number of blocks
        let num_blocks = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        data = &data[4..];

        for _ in 0..num_blocks {
            // Each block starts with a 4-byte length
            let segment_len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            data = &data[4..];

            // Read the segment IDs for this block
            let mut segment_ids = Vec::with_capacity(segment_len);
            for _ in 0..segment_len {
                let segment_id = u32::from_le_bytes(data[..4].try_into().unwrap());
                segment_ids.push(segment_id);
                data = &data[4..];
            }
            self.vals.push(segment_ids);
        }

        // Return the number of bytes consumed
        Ok(original_num_bytes - data.len())
    }
}

#[derive(Default)]
pub struct VecU32ValueWriter {
    vals: Vec<Vec<u32>>,
}

impl ValueWriter for VecU32ValueWriter {
    type Value = Vec<u32>;

    fn write(&mut self, val: &Self::Value) {
        self.vals.push(val.to_vec());
    }

    fn serialize_block(&self, output: &mut Vec<u8>) {
        let num_blocks = self.vals.len() as u32;
        output.extend_from_slice(&num_blocks.to_le_bytes());
        for vals in &self.vals {
            let len = vals.len() as u32;
            output.extend_from_slice(&len.to_le_bytes());
            for &segment_id in vals.iter() {
                output.extend_from_slice(&segment_id.to_le_bytes());
            }
        }
    }

    fn clear(&mut self) {
        self.vals.clear();
    }
}
