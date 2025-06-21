use limbo_sqlite3_parser::ast::SortOrder;

use std::cell::{Cell, RefCell};
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd, Reverse};
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::sync::Arc;
use tempfile;

use crate::{
    error::LimboError,
    io::{Buffer, BufferData, Completion, File, OpenFlags, ReadCompletion, WriteCompletion, IO},
    storage::sqlite3_ondisk::read_record,
    translate::collate::CollationSeq,
    types::{compare_immutable, CursorResult, ImmutableRecord, IndexKeySortOrder},
    Result,
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
    /// Sorted chunks stored on disk.
    chunks: Vec<SortedChunk>,
    /// The heap of records consumed from the chunks and their corresponding chunk index.
    chunk_heap: BinaryHeap<(Reverse<SortableImmutableRecord>, usize)>,
    /// The maximum size of the in-memory buffer in bytes before the records are flushed to a chunk file.
    max_buffer_size: usize,
    /// The current size of the in-memory buffer in bytes.
    current_buffer_size: usize,
    /// The minimum size of a chunk buffer in bytes. The actual buffer size can be larger if the largest
    /// record in the buffer is larger than this value.
    min_chunk_buffer_size: usize,
    /// The maximum record payload size in the in-memory buffer.
    max_payload_size_in_buffer: usize,
    /// The maximum number of values in a record in the in-memory buffer.
    max_values_len_in_buffer: usize,
    /// The IO object.
    io: Arc<dyn IO>,
    /// The indices of the chunks that are waiting for the read to complete.
    wait_for_read_complete: Vec<usize>,
    /// The temporary directory for chunk files.
    temp_dir: Option<tempfile::TempDir>,
}

impl Sorter {
    pub fn new(
        order: &[SortOrder],
        collations: Vec<CollationSeq>,
        max_buffer_size_bytes: usize,
        min_chunk_buffer_size_bytes: usize,
        io: Arc<dyn IO>,
    ) -> Self {
        Self {
            records: Vec::new(),
            current: None,
            key_len: order.len(),
            order: IndexKeySortOrder::from_list(order),
            collations: Rc::new(collations),
            chunks: Vec::new(),
            chunk_heap: BinaryHeap::new(),
            max_buffer_size: max_buffer_size_bytes,
            current_buffer_size: 0,
            min_chunk_buffer_size: min_chunk_buffer_size_bytes,
            max_payload_size_in_buffer: 0,
            max_values_len_in_buffer: 0,
            io,
            wait_for_read_complete: Vec::new(),
            temp_dir: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.chunks.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) -> Result<CursorResult<()>> {
        if self.chunks.is_empty() {
            self.records.sort();
            self.records.reverse();
        } else {
            self.flush()?;
            if let CursorResult::IO = self.init_chunk_heap()? {
                return Ok(CursorResult::IO);
            }
        }
        self.next()
    }

    pub fn next(&mut self) -> Result<CursorResult<()>> {
        if self.chunks.is_empty() {
            // Serve from the in-memory buffer.
            self.current = self.records.pop().map(|r| r.record);
        } else {
            // Serve from sorted chunk files.
            match self.next_from_chunk_heap()? {
                CursorResult::IO => return Ok(CursorResult::IO),
                CursorResult::Ok(record) => self.current = record,
            }
        }
        Ok(CursorResult::Ok(()))
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

    fn init_chunk_heap(&mut self) -> Result<CursorResult<()>> {
        let mut all_read_complete = true;
        // Make sure all chunks read at least one record into their buffer.
        for chunk in self.chunks.iter_mut() {
            match chunk.io_state.get() {
                SortedChunkIOState::WriteComplete => {
                    all_read_complete = false;
                    // Write complete, we can now read from the chunk.
                    chunk.read()?;
                }
                SortedChunkIOState::WaitingForWrite => {
                    all_read_complete = false;
                }
                SortedChunkIOState::ReadEOF | SortedChunkIOState::ReadComplete => {}
                _ => {
                    unreachable!("Unexpected chunk IO state: {:?}", chunk.io_state.get())
                }
            }
        }
        if !all_read_complete {
            return Ok(CursorResult::IO);
        }
        self.chunk_heap.reserve(self.chunks.len());
        for chunk_idx in 0..self.chunks.len() {
            self.push_to_chunk_heap(chunk_idx)?;
        }
        Ok(CursorResult::Ok(()))
    }

    fn next_from_chunk_heap(&mut self) -> Result<CursorResult<Option<ImmutableRecord>>> {
        let mut all_read_complete = true;
        for chunk_idx in self.wait_for_read_complete.iter() {
            let chunk_io_state = self.chunks[*chunk_idx].io_state.get();
            match chunk_io_state {
                SortedChunkIOState::ReadComplete | SortedChunkIOState::ReadEOF => {}
                SortedChunkIOState::WaitingForRead => {
                    all_read_complete = false;
                }
                _ => {
                    unreachable!("Unexpected chunk IO state: {:?}", chunk_io_state)
                }
            }
        }
        if !all_read_complete {
            // We need to wait for the read to complete.
            return Ok(CursorResult::IO);
        }
        self.wait_for_read_complete.clear();

        if let Some((next_record, next_chunk_idx)) = self.chunk_heap.pop() {
            self.push_to_chunk_heap(next_chunk_idx)?;
            Ok(CursorResult::Ok(Some(next_record.0.record)))
        } else {
            Ok(CursorResult::Ok(None))
        }
    }

    fn push_to_chunk_heap(&mut self, chunk_idx: usize) -> Result<()> {
        let chunk_reader = &mut self.chunks[chunk_idx];

        if chunk_reader.has_more() {
            let record = chunk_reader.next()?.unwrap();
            self.chunk_heap.push((
                Reverse(SortableImmutableRecord::new(
                    record,
                    self.key_len,
                    self.order,
                    self.collations.clone(),
                )),
                chunk_idx,
            ));
            if let SortedChunkIOState::WaitingForRead = chunk_reader.io_state.get() {
                self.wait_for_read_complete.push(chunk_idx);
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        self.records.sort();

        if self.temp_dir.is_none() {
            self.temp_dir = Some(tempfile::tempdir().map_err(LimboError::IOError)?);
        }

        let chunk_file_path = self
            .temp_dir
            .as_ref()
            .unwrap()
            .path()
            .join(format!("chunk_{}", self.chunks.len()));
        let chunk_file =
            self.io
                .open_file(chunk_file_path.to_str().unwrap(), OpenFlags::Create, false)?;

        // Make sure the chunk buffer size can fit the largest record.
        let chunk_buffer_size = self
            .min_chunk_buffer_size
            .max(self.max_payload_size_in_buffer);
        let mut chunk = SortedChunk::new(
            chunk_file.clone(),
            chunk_buffer_size,
            self.max_values_len_in_buffer,
        );
        chunk.write(&mut self.records, self.current_buffer_size)?;
        self.chunks.push(chunk);

        self.current_buffer_size = 0;
        self.max_payload_size_in_buffer = 0;
        self.max_values_len_in_buffer = 0;

        Ok(())
    }
}

struct SortedChunk {
    /// The chunk file.
    file: Arc<dyn File>,
    /// The read buffer.
    buffer: Rc<RefCell<Vec<u8>>>,
    /// The current length of the buffer.
    buffer_len: Rc<Cell<usize>>,
    /// The maximum number of values in a record in this chunk.
    max_values_len: usize,
    /// The records decoded from the chunk file.
    records: Vec<ImmutableRecord>,
    /// The current IO state of the chunk.
    io_state: Rc<Cell<SortedChunkIOState>>,
    /// The total number of bytes read from the chunk file.
    total_bytes_read: Rc<Cell<usize>>,
}

impl SortedChunk {
    fn new(file: Arc<dyn File>, buffer_size: usize, max_values_len: usize) -> Self {
        Self {
            file,
            buffer: Rc::new(RefCell::new(vec![0; buffer_size])),
            buffer_len: Rc::new(Cell::new(0)),
            max_values_len,
            records: Vec::new(),
            io_state: Rc::new(Cell::new(SortedChunkIOState::None)),
            total_bytes_read: Rc::new(Cell::new(0)),
        }
    }

    fn has_more(&self) -> bool {
        !self.records.is_empty() || self.io_state.get() != SortedChunkIOState::ReadEOF
    }

    fn next(&mut self) -> Result<Option<ImmutableRecord>> {
        let mut buffer_len = self.buffer_len.get();
        if self.records.is_empty() && buffer_len == 0 {
            return Ok(None);
        }

        if self.records.is_empty() {
            let mut buffer_ref = self.buffer.borrow_mut();
            let buffer = buffer_ref.as_mut_slice();
            let mut buffer_offset = 0;
            while buffer_offset < buffer_len {
                // Decode records from the buffer until we run out of the buffer or we hit an incomplete record.
                let mut record = ImmutableRecord::new(buffer_len, self.max_values_len);

                buffer_offset += match read_record(&buffer[buffer_offset..buffer_len], &mut record)
                {
                    Ok(bytes_consumed) => bytes_consumed,
                    Err(LimboError::Corrupt(_))
                        if self.io_state.get() != SortedChunkIOState::ReadEOF =>
                    {
                        // Failed to decode a partial record.
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };

                self.records.push(record);
            }
            if buffer_offset < buffer_len {
                buffer.copy_within(buffer_offset..buffer_len, 0);
                buffer_len -= buffer_offset;
            } else {
                buffer_len = 0;
            }
            self.buffer_len.set(buffer_len);

            self.records.reverse();
        }

        let record = self.records.pop();
        if self.records.is_empty() && self.io_state.get() != SortedChunkIOState::ReadEOF {
            // We've consumed the last record. Read more payload into the buffer.
            self.read()?;
        }
        Ok(record)
    }

    fn read(&mut self) -> Result<()> {
        if self.io_state.get() == SortedChunkIOState::ReadEOF {
            return Ok(());
        }
        self.io_state.set(SortedChunkIOState::WaitingForRead);

        let drop_fn = Rc::new(|_buffer: BufferData| {});
        let read_buffer =
            Buffer::allocate(self.buffer.borrow().len() - self.buffer_len.get(), drop_fn);
        #[allow(clippy::arc_with_non_send_sync)]
        let read_buffer_ref = Arc::new(RefCell::new(read_buffer));

        let chunk_io_state_copy = self.io_state.clone();
        let stored_buffer_copy = self.buffer.clone();
        let stored_buffer_len_copy = self.buffer_len.clone();
        let total_bytes_read_copy = self.total_bytes_read.clone();
        let read_complete = Box::new(move |buf: Arc<RefCell<Buffer>>, bytes_read: i32| {
            let read_buf_ref = buf.borrow();
            let read_buf = read_buf_ref.as_slice();

            let bytes_read = bytes_read as usize;
            if bytes_read == 0 {
                chunk_io_state_copy.set(SortedChunkIOState::ReadEOF);
                return;
            }
            chunk_io_state_copy.set(SortedChunkIOState::ReadComplete);

            let mut stored_buf_ref = stored_buffer_copy.borrow_mut();
            let stored_buf = stored_buf_ref.as_mut_slice();
            let mut stored_buf_len = stored_buffer_len_copy.get();

            stored_buf[stored_buf_len..stored_buf_len + bytes_read]
                .copy_from_slice(&read_buf[..bytes_read]);
            stored_buf_len += bytes_read;

            stored_buffer_len_copy.set(stored_buf_len);
            total_bytes_read_copy.set(total_bytes_read_copy.get() + bytes_read);
        });

        #[allow(clippy::arc_with_non_send_sync)]
        let c = Arc::new(Completion::Read(ReadCompletion::new(
            read_buffer_ref,
            read_complete,
        )));
        self.file.pread(self.total_bytes_read.get(), c)
    }

    fn write(
        &mut self,
        records: &mut Vec<SortableImmutableRecord>,
        total_size: usize,
    ) -> Result<()> {
        assert!(self.io_state.get() == SortedChunkIOState::None);
        self.io_state.set(SortedChunkIOState::WaitingForWrite);

        let drop_fn = Rc::new(|_buffer: BufferData| {});
        let mut buffer = Buffer::allocate(total_size, drop_fn);

        let mut buf_pos = 0;
        let buf = buffer.as_mut_slice();
        for record in records.drain(..) {
            let payload = record.record.get_payload();
            buf[buf_pos..buf_pos + payload.len()].copy_from_slice(payload);
            buf_pos += payload.len();
        }

        #[allow(clippy::arc_with_non_send_sync)]
        let buffer_ref = Arc::new(RefCell::new(buffer));

        let buffer_ref_copy = buffer_ref.clone();
        let chunk_io_state_copy = self.io_state.clone();
        let write_complete = Box::new(move |bytes_written: i32| {
            let buf_len = buffer_ref_copy.borrow().len();
            chunk_io_state_copy.set(SortedChunkIOState::WriteComplete);

            if bytes_written < buf_len as i32 {
                tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        });

        #[allow(clippy::arc_with_non_send_sync)]
        let c = Arc::new(Completion::Write(WriteCompletion::new(write_complete)));
        self.file.pwrite(0, buffer_ref, c)
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SortedChunkIOState {
    WaitingForRead,
    ReadComplete,
    ReadEOF,
    WaitingForWrite,
    WriteComplete,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ImmutableRecord, RefValue, Value};
    use crate::vdbe::Register;
    use crate::PlatformIO;

    #[test]
    fn test_external_sort() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let mut sorter = Sorter::new(&[SortOrder::Asc], vec![], 64, 13, io.clone());
        for i in (0..1024).rev() {
            sorter
                .insert(&ImmutableRecord::from_registers(
                    &[Register::Value(Value::Integer(i))],
                    1,
                ))
                .expect("Failed to insert the record");
        }

        loop {
            if let CursorResult::IO = sorter.sort().expect("Failed to sort the records") {
                io.run_once().expect("Failed to run the IO");
                continue;
            }
            break;
        }

        assert!(!sorter.is_empty());
        assert_eq!(sorter.chunks.len(), 63);

        for i in 0..1024 {
            assert!(sorter.has_more());
            let record = sorter.record().unwrap();
            assert_eq!(record.values[0], RefValue::Integer(i));
            loop {
                if let CursorResult::IO = sorter.next().expect("Failed to get the next record") {
                    io.run_once().expect("Failed to run the IO");
                    continue;
                }
                break;
            }
        }
        assert!(!sorter.has_more());
    }
}
