use std::io;

use crate::value::{ValueReader, ValueWriter};

#[derive(Default)]
pub struct VoidValueReader;

impl ValueReader for VoidValueReader {
    type Value = ();

    #[inline(always)]
    fn value(&self, _idx: usize) -> &() {
        &()
    }

    fn load(&mut self, _data: &[u8]) -> io::Result<usize> {
        Ok(0)
    }
}

#[derive(Default)]
pub struct VoidValueWriter;

impl ValueWriter for VoidValueWriter {
    type Value = ();

    fn write(&mut self, _val: &()) {}

    fn serialize_block(&self, _output: &mut Vec<u8>) {}

    fn clear(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_reader_writer() {
        crate::value::tests::test_value_reader_writer::<_, VoidValueReader, VoidValueWriter>(&[]);
        crate::value::tests::test_value_reader_writer::<_, VoidValueReader, VoidValueWriter>(&[()]);
        crate::value::tests::test_value_reader_writer::<_, VoidValueReader, VoidValueWriter>(&[
            (),
            (),
            (),
        ]);
    }
}
