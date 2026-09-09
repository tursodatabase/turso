use std::io;
use std::io::Write;

use common::json_path_writer::JSON_END_OF_PATH;
use common::{BinarySerializable, CountingWriter};
use sstable::RangeSSTable;
use sstable::value::RangeValueWriter;

use crate::RowId;
use crate::columnar::ColumnType;

pub struct ColumnarSerializer<W: io::Write> {
    wrt: CountingWriter<W>,
    sstable_range: sstable::Writer<Vec<u8>, RangeValueWriter>,
    prepare_key_buffer: Vec<u8>,
}

/// Returns a key consisting of the concatenation of the key and the column_type_and_cardinality
/// code.
fn prepare_key(key: &[u8], column_type: ColumnType, buffer: &mut Vec<u8>) {
    buffer.clear();
    buffer.extend_from_slice(key);
    buffer.push(JSON_END_OF_PATH);
    buffer.push(column_type.to_code());
}

impl<W: io::Write> ColumnarSerializer<W> {
    pub(crate) fn new(wrt: W) -> ColumnarSerializer<W> {
        let sstable_range: sstable::Writer<Vec<u8>, RangeValueWriter> =
            sstable::Dictionary::<RangeSSTable>::builder(Vec::with_capacity(100_000)).unwrap();
        ColumnarSerializer {
            wrt: CountingWriter::wrap(wrt),
            sstable_range,
            prepare_key_buffer: Vec::new(),
        }
    }

    /// Creates a ColumnSerializer.
    pub fn start_serialize_column<'a>(
        &'a mut self,
        column_name: &[u8],
        column_type: ColumnType,
    ) -> ColumnSerializer<'a, W> {
        let start_offset = self.wrt.written_bytes();
        prepare_key(column_name, column_type, &mut self.prepare_key_buffer);
        ColumnSerializer {
            columnar_serializer: self,
            start_offset,
        }
    }

    pub(crate) fn finalize(mut self, num_rows: RowId) -> io::Result<()> {
        let sstable_bytes: Vec<u8> = self.sstable_range.finish()?;
        let sstable_num_bytes: u64 = sstable_bytes.len() as u64;
        self.wrt.write_all(&sstable_bytes)?;
        self.wrt.write_all(&sstable_num_bytes.to_le_bytes()[..])?;
        num_rows.serialize(&mut self.wrt)?;
        self.wrt
            .write_all(&super::super::format_version::footer())?;
        self.wrt.flush()?;
        Ok(())
    }
}

pub struct ColumnSerializer<'a, W: io::Write> {
    columnar_serializer: &'a mut ColumnarSerializer<W>,
    start_offset: u64,
}

impl<W: io::Write> ColumnSerializer<'_, W> {
    pub fn finalize(self) -> io::Result<()> {
        let end_offset: u64 = self.columnar_serializer.wrt.written_bytes();
        let byte_range = self.start_offset..end_offset;
        self.columnar_serializer.sstable_range.insert(
            &self.columnar_serializer.prepare_key_buffer[..],
            &byte_range,
        )?;
        self.columnar_serializer.prepare_key_buffer.clear();
        Ok(())
    }
}

impl<W: io::Write> io::Write for ColumnSerializer<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.columnar_serializer.wrt.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.columnar_serializer.wrt.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.columnar_serializer.wrt.write_all(buf)
    }
}
