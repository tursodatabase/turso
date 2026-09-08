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

pub(crate) struct AsyncColumnarSerializer {
    output: common::async_write::AsyncWritePtr,
    written: u64,
    ranges: sstable::Writer<Vec<u8>, RangeValueWriter>,
    num_rows: RowId,
    usable: bool,
}

impl AsyncColumnarSerializer {
    pub fn new(output: common::async_write::AsyncWritePtr, num_rows: RowId) -> io::Result<Self> {
        Ok(Self {
            output,
            written: 0,
            num_rows,
            usable: true,
            ranges: sstable::Dictionary::<RangeSSTable>::builder(Vec::new())?,
        })
    }

    pub async fn full_column<I: Iterator<Item = u64> + Send>(
        &mut self,
        name: &[u8],
        column_type: ColumnType,
        values: impl Fn() -> I + Send + Sync,
    ) -> io::Result<()> {
        use common::async_write::AsyncWrite;
        self.begin()?;
        let start = self.written;
        self.write_all(&[crate::Cardinality::Full.to_code()])
            .await?;
        crate::column_values::serialize_u64_based_column_values_async(
            values,
            &[
                crate::column_values::CodecType::Bitpacked,
                crate::column_values::CodecType::BlockwiseLinear,
            ],
            self,
        )
        .await?;
        self.write_all(&1u32.to_le_bytes()).await?;
        let mut key = Vec::new();
        prepare_key(name, column_type, &mut key);
        self.ranges.insert(&key, &(start..self.written))?;
        self.usable = true;
        Ok(())
    }

    pub async fn close(mut self) -> io::Result<()> {
        self.begin()?;
        // The table describes columns, not rows. FTS has one rowid column.
        let ranges = self.ranges.finish()?;
        self.output.write_all(&ranges).await?;
        self.output
            .write_all(&(ranges.len() as u64).to_le_bytes())
            .await?;
        self.output.write_all(&self.num_rows.to_le_bytes()).await?;
        self.output
            .write_all(&super::super::format_version::footer())
            .await?;
        self.output.finish().await
    }

    fn begin(&mut self) -> io::Result<()> {
        if !std::mem::replace(&mut self.usable, false) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Column output was cancelled or failed",
            ));
        }
        Ok(())
    }
}

impl common::async_write::AsyncWrite for AsyncColumnarSerializer {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> common::async_write::WriteFuture<'a, usize> {
        Box::pin(async move {
            let count = self.output.write(bytes).await?;
            self.written += count as u64;
            Ok(count)
        })
    }

    fn flush(&mut self) -> common::async_write::WriteFuture<'_, ()> {
        self.output.flush()
    }

    fn finish(self: Box<Self>) -> common::async_write::WriteFuture<'static, ()> {
        Box::pin(async move { self.close().await })
    }
}
