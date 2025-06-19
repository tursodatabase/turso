use limbo_sqlite3_parser::ast::SortOrder;

use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::rc::Rc;
use tempfile;

use crate::{
    storage::sqlite3_ondisk::read_record,
    translate::collate::CollationSeq,
    types::{compare_immutable, ImmutableRecord, IndexKeySortOrder},
    LimboError, Result,
};

pub struct Sorter {
    /// The records in the in-memory buffer.
    records: Vec<SortableImmutableRecord>,
    /// The current record.
    current: Option<ImmutableRecord>,
    /// The sort order.
    order: IndexKeySortOrder,
    /// The number of values in the key.
    key_len: usize,
    /// The collations.
    collations: Rc<Vec<CollationSeq>>,
    /// Readers for the sorted chunks stored on disk.
    chunk_readers: Vec<ChunkReader>,
    /// The heap of records and their chunk index.
    chunk_heap: BinaryHeap<(Reverse<SortableImmutableRecord>, usize)>,
    /// The maximum size of the in-memory buffer in bytes.
    max_buffer_size: usize,
    /// The current size of the in-memory buffer in bytes.
    current_buffer_size: usize,
    /// The maximum record payload size in the in-memory buffer in bytes.
    max_payload_size_in_buffer: usize,
    /// The maximum number of values in a record in the in-memory buffer.
    max_values_len_in_buffer: usize,
}

impl Sorter {
    pub fn new(
        order: &[SortOrder],
        collations: Vec<CollationSeq>,
        max_buffer_size_bytes: usize,
    ) -> Self {
        Self {
            records: Vec::new(),
            current: None,
            key_len: order.len(),
            order: IndexKeySortOrder::from_list(order),
            collations: Rc::new(collations),
            chunk_readers: Vec::new(),
            chunk_heap: BinaryHeap::new(),
            max_buffer_size: max_buffer_size_bytes,
            current_buffer_size: 0,
            max_payload_size_in_buffer: 0,
            max_values_len_in_buffer: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) -> Result<()> {
        if self.chunk_readers.is_empty() {
            self.records.sort();
            self.records.reverse();
        } else {
            self.flush()?;
            self.chunk_heap.reserve(self.chunk_readers.len());
            for chunk_idx in 0..self.chunk_readers.len() {
                self.push_to_heap_from_chunk(chunk_idx, true)?;
            }
        }
        self.next()
    }

    pub fn next(&mut self) -> Result<()> {
        if self.chunk_readers.is_empty() {
            self.current = self.records.pop().map(|r| r.record);
        } else {
            if let Some((next_record, next_chunk_idx)) = self.chunk_heap.pop() {
                self.current = Some(next_record.0.record);
                self.push_to_heap_from_chunk(next_chunk_idx, false)?;
            } else {
                self.current = None;
            }
        }
        Ok(())
    }

    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &ImmutableRecord) -> Result<()> {
        let payload_size = record.get_payload().len();
        if self.current_buffer_size + payload_size > self.max_buffer_size {
            self.flush()?;
        }
        self.records.push(SortableImmutableRecord::new(
            record.clone(),
            self.key_len,
            self.order,
            self.collations.clone(),
        ));
        self.current_buffer_size += payload_size;
        self.max_payload_size_in_buffer = self.max_payload_size_in_buffer.max(payload_size);
        self.max_values_len_in_buffer = self.max_values_len_in_buffer.max(record.len());
        Ok(())
    }

    fn push_to_heap_from_chunk(&mut self, chunk_idx: usize, init: bool) -> Result<()> {
        let chunk_reader = &mut self.chunk_readers[chunk_idx];

        if init {
            chunk_reader.next()?;
        }

        if chunk_reader.has_more() {
            let record = chunk_reader.consume_record()?.unwrap();
            self.chunk_heap.push((
                Reverse(SortableImmutableRecord::new(
                    record,
                    self.key_len,
                    self.order,
                    self.collations.clone(),
                )),
                chunk_idx,
            ));
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.records.sort();

        let mut fd = tempfile::tempfile()?;
        let mut writer = io::BufWriter::new(fd.try_clone()?);
        for record in self.records.drain(..) {
            writer
                .write(record.record.get_payload())
                .map_err(LimboError::IOError)?;
        }
        writer.flush().map_err(LimboError::IOError)?;

        fd.seek(io::SeekFrom::Start(0))
            .map_err(LimboError::IOError)?;
        self.chunk_readers.push(ChunkReader::new(
            fd,
            self.max_payload_size_in_buffer,
            self.max_values_len_in_buffer,
        ));

        self.current_buffer_size = 0;
        self.max_payload_size_in_buffer = 0;
        self.max_values_len_in_buffer = 0;

        Ok(())
    }
}

struct ChunkReader {
    /// The reader for the chunk file.
    reader: io::BufReader<File>,
    /// The read buffer.
    buffer: Vec<u8>,
    /// The current length of the buffer.
    buffer_len: usize,
    /// The maximum number of values in a record in this chunk.
    max_values_len: usize,
    /// The current record.
    current: Option<ImmutableRecord>,
}

impl ChunkReader {
    fn new(fd: File, max_payload_size: usize, max_values_len: usize) -> Self {
        Self {
            reader: io::BufReader::new(fd),
            buffer: vec![0; max_payload_size],
            buffer_len: 0, // Start with empty buffer
            max_values_len,
            current: None,
        }
    }

    fn has_more(&self) -> bool {
        self.current.is_some()
    }

    fn consume_record(&mut self) -> Result<Option<ImmutableRecord>> {
        let result = self.current.take();
        self.next()?;
        Ok(result)
    }

    fn next(&mut self) -> Result<()> {
        let mut record = ImmutableRecord::new(self.buffer.len(), self.max_values_len);

        let bytes_read = self
            .reader
            .read(&mut self.buffer[self.buffer_len..])
            .map_err(LimboError::IOError)?;

        if bytes_read == 0 && self.buffer_len == 0 {
            // There are no more records in the chunk.
            self.current = None;
            return Ok(());
        }
        self.buffer_len += bytes_read;

        let bytes_consumed = read_record(&self.buffer[..self.buffer_len], &mut record)?;

        if bytes_consumed < self.buffer_len {
            self.buffer.copy_within(bytes_consumed..self.buffer_len, 0);
            self.buffer_len -= bytes_consumed;
        } else {
            self.buffer_len = 0;
        }

        self.current = Some(record);
        Ok(())
    }
}

struct SortableImmutableRecord {
    record: ImmutableRecord,
    key_len: usize,
    order: IndexKeySortOrder,
    collations: Rc<Vec<CollationSeq>>,
}

impl SortableImmutableRecord {
    fn new(
        record: ImmutableRecord,
        key_len: usize,
        order: IndexKeySortOrder,
        collations: Rc<Vec<CollationSeq>>,
    ) -> Self {
        Self {
            record,
            key_len,
            order,
            collations,
        }
    }
}

impl Ord for SortableImmutableRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_immutable(
            &self.record.values[..self.key_len],
            &other.record.values[..self.key_len],
            self.order,
            &self.collations[..],
        )
    }
}

impl PartialOrd for SortableImmutableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortableImmutableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SortableImmutableRecord {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ImmutableRecord, RefValue, Value};
    use crate::vdbe::Register;

    #[test]
    fn test_chunk_reader() {
        let record_a = ImmutableRecord::from_registers(&[
            Register::Value(Value::Null),
            Register::Value(Value::Integer(1)),
            Register::Value(Value::Integer(1234)),
        ]);
        let record_b = ImmutableRecord::from_registers(&[
            Register::Value(Value::Null),
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Integer(1)),
        ]);

        let mut fd = tempfile::tempfile().expect("Failed to create temp file");
        fd.write_all(record_a.get_payload())
            .expect("Failed to write the first record");
        fd.write_all(record_b.get_payload())
            .expect("Failed to write the second record");
        fd.flush().expect("Failed to flush the file");
        fd.seek(io::SeekFrom::Start(0))
            .expect("Failed to seek to the start of the file");

        let mut reader = ChunkReader::new(fd, 64, 4);

        reader.next().expect("Failed to read the first record");
        assert!(reader.has_more());

        let record_a_from_chunk = reader
            .consume_record()
            .expect("Failed to read the second record")
            .unwrap();
        assert_eq!(record_a_from_chunk.values, record_a.values);

        assert!(reader.has_more());
        let record_b_from_chunk = reader
            .consume_record()
            .expect("Failed to consume the record")
            .unwrap();
        assert_eq!(record_b_from_chunk.values, record_b.values);

        assert!(!reader.has_more());
        reader.next().expect("Failed to perform the next read");
        assert!(reader
            .consume_record()
            .expect("Failed to consume the record")
            .is_none());
    }

    #[test]
    fn test_external_sort() {
        let mut sorter = Sorter::new(&[SortOrder::Asc], vec![], 64);
        for i in (0..1024).rev() {
            sorter
                .insert(&ImmutableRecord::from_registers(&[Register::Value(
                    Value::Integer(i),
                )]))
                .expect("Failed to insert the record");
        }

        sorter.sort().expect("Failed to sort the records");
        assert_eq!(sorter.chunk_readers.len(), 63);

        for i in 0..1024 {
            assert!(sorter.has_more());
            let record = sorter.record().unwrap();
            assert_eq!(record.values[0], RefValue::Integer(i));
            sorter.next().expect("Failed to get the next record");
        }
        assert!(!sorter.has_more());
    }
}
