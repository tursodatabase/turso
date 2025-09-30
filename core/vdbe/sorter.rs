use turso_parser::ast::SortOrder;

use std::cell::RefCell;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd, Reverse};
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::sync::{atomic, Arc, RwLock};
use tempfile;

use crate::types::IOCompletions;
use crate::util::IOExt;
use crate::{
    error::LimboError,
    io::{Buffer, Completion, File, OpenFlags, IO},
    storage::sqlite3_ondisk::{read_varint, varint_len, write_varint},
    translate::collate::CollationSeq,
    turso_assert,
    types::{IOResult, ImmutableRecord, KeyInfo, RecordCursor, RefValue},
    Result,
};
use crate::{io_yield_many, io_yield_one, return_if_io, CompletionError};

#[derive(Debug, Clone, Copy)]
enum SortState {
    Start,
    Flush,
    InitHeap,
    Next,
}

#[derive(Debug, Clone, Copy)]
enum InsertState {
    Start,
    Insert,
}

#[derive(Debug, Clone, Copy)]
enum InitChunkHeapState {
    Start,
    PushChunk,
}

struct TempFile {
    // When temp_dir is dropped the folder is deleted
    _temp_dir: tempfile::TempDir,
    file: Arc<dyn File>,
}

impl core::ops::Deref for TempFile {
    type Target = Arc<dyn File>;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

pub struct Sorter {
    /// The records in the in-memory buffer.
    records: Vec<SortableImmutableRecord>,
    /// The current record.
    current: Option<ImmutableRecord>,
    /// The number of values in the key.
    key_len: usize,
    /// The key info.
    index_key_info: Rc<Vec<KeyInfo>>,
    /// Sorted chunks stored on disk.
    chunks: Vec<SortedChunk>,
    /// The heap of records consumed from the chunks and their corresponding chunk index.
    chunk_heap: BinaryHeap<(Reverse<SortableImmutableRecord>, usize)>,
    /// The maximum size of the in-memory buffer in bytes before the records are flushed to a chunk file.
    max_buffer_size: usize,
    /// The current size of the in-memory buffer in bytes.
    current_buffer_size: usize,
    /// The minimum size of a chunk read buffer in bytes. The actual buffer size can be larger if the largest
    /// record in the buffer is larger than this value.
    min_chunk_read_buffer_size: usize,
    /// The maximum record payload size in the in-memory buffer.
    max_payload_size_in_buffer: usize,
    /// The IO object.
    io: Arc<dyn IO>,
    /// The temporary file for chunks.
    temp_file: Option<TempFile>,
    /// Offset where the next chunk will be placed in the `temp_file`
    next_chunk_offset: usize,
    /// State machine for [Sorter::sort]
    sort_state: SortState,
    /// State machine for [Sorter::insert]
    insert_state: InsertState,
    /// State machine for [Sorter::init_chunk_heap]
    init_chunk_heap_state: InitChunkHeapState,
    seq_count: i64,
}

impl Sorter {
    pub fn new(
        order: &[SortOrder],
        collations: Vec<CollationSeq>,
        max_buffer_size_bytes: usize,
        min_chunk_read_buffer_size_bytes: usize,
        io: Arc<dyn IO>,
    ) -> Self {
        assert_eq!(order.len(), collations.len());
        Self {
            records: Vec::new(),
            current: None,
            key_len: order.len(),
            index_key_info: Rc::new(
                order
                    .iter()
                    .zip(collations)
                    .map(|(order, collation)| KeyInfo {
                        sort_order: *order,
                        collation,
                    })
                    .collect(),
            ),
            chunks: Vec::new(),
            chunk_heap: BinaryHeap::new(),
            max_buffer_size: max_buffer_size_bytes,
            current_buffer_size: 0,
            min_chunk_read_buffer_size: min_chunk_read_buffer_size_bytes,
            max_payload_size_in_buffer: 0,
            io,
            temp_file: None,
            next_chunk_offset: 0,
            sort_state: SortState::Start,
            insert_state: InsertState::Start,
            init_chunk_heap_state: InitChunkHeapState::Start,
            seq_count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.chunks.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    /// Get current sequence count and increment it
    pub fn next_sequence(&mut self) -> i64 {
        let current = self.seq_count;
        self.seq_count += 1;
        current
    }

    /// Test if at beginning of sequence (count == 0) and increment
    /// Returns true if this was the first call (seq_count was 0)
    pub fn seq_beginning(&mut self) -> bool {
        let was_zero = self.seq_count == 0;
        self.seq_count += 1;
        was_zero
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) -> Result<IOResult<()>> {
        loop {
            match self.sort_state {
                SortState::Start => {
                    if self.chunks.is_empty() {
                        self.records.sort();
                        self.records.reverse();
                        self.sort_state = SortState::Next;
                    } else {
                        self.sort_state = SortState::Flush;
                    }
                }
                SortState::Flush => {
                    self.sort_state = SortState::InitHeap;
                    if let Some(c) = self.flush()? {
                        io_yield_one!(c);
                    }
                }
                SortState::InitHeap => {
                    turso_assert!(
                        !self.chunks.iter().any(|chunk| {
                            matches!(
                                *chunk.io_state.read().unwrap(),
                                SortedChunkIOState::WaitingForWrite
                            )
                        }),
                        "chunks should been written"
                    );
                    return_if_io!(self.init_chunk_heap());
                    self.sort_state = SortState::Next;
                }
                SortState::Next => {
                    return_if_io!(self.next());
                    self.sort_state = SortState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn next(&mut self) -> Result<IOResult<()>> {
        let record = if self.chunks.is_empty() {
            // Serve from the in-memory buffer.
            self.records.pop()
        } else {
            // Serve from sorted chunk files.
            return_if_io!(self.next_from_chunk_heap())
        };
        match record {
            Some(record) => {
                if let Some(error) = record.deserialization_error.replace(None) {
                    // If there was a key deserialization error during the comparison, return the error.
                    return Err(error);
                }
                self.current = Some(record.record);
            }
            None => self.current = None,
        }
        Ok(IOResult::Done(()))
    }

    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &ImmutableRecord) -> Result<IOResult<()>> {
        let payload_size = record.get_payload().len();
        loop {
            match self.insert_state {
                InsertState::Start => {
                    self.insert_state = InsertState::Insert;
                    if self.current_buffer_size + payload_size > self.max_buffer_size {
                        if let Some(c) = self.flush()? {
                            io_yield_one!(c);
                        }
                    }
                }
                InsertState::Insert => {
                    turso_assert!(
                        !self.chunks.iter().any(|chunk| {
                            matches!(
                                *chunk.io_state.read().unwrap(),
                                SortedChunkIOState::WaitingForWrite
                            )
                        }),
                        "chunks should have written"
                    );
                    self.records.push(SortableImmutableRecord::new(
                        record.clone(),
                        self.key_len,
                        self.index_key_info.clone(),
                    )?);
                    self.current_buffer_size += payload_size;
                    self.max_payload_size_in_buffer =
                        self.max_payload_size_in_buffer.max(payload_size);
                    self.insert_state = InsertState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    fn init_chunk_heap(&mut self) -> Result<IOResult<()>> {
        match self.init_chunk_heap_state {
            InitChunkHeapState::Start => {
                let mut completions: Vec<Completion> = Vec::with_capacity(self.chunks.len());
                for chunk in self.chunks.iter_mut() {
                    match chunk.read() {
                        Err(e) => {
                            tracing::error!("Failed to read chunk: {e}");
                            self.io.cancel(&completions)?;
                            self.io.drain()?;
                            return Err(e);
                        }
                        Ok(c) => completions.push(c),
                    };
                }
                self.init_chunk_heap_state = InitChunkHeapState::PushChunk;
                io_yield_many!(completions);
            }
            InitChunkHeapState::PushChunk => {
                // Make sure all chunks read at least one record into their buffer.
                turso_assert!(
                    !self.chunks.iter().any(|chunk| matches!(
                        *chunk.io_state.read().unwrap(),
                        SortedChunkIOState::WaitingForRead
                    )),
                    "chunks should have been read"
                );
                self.chunk_heap.reserve(self.chunks.len());
                // TODO: blocking will be unnecessary here with IO completions
                let io = self.io.clone();
                for chunk_idx in 0..self.chunks.len() {
                    io.block(|| self.push_to_chunk_heap(chunk_idx))?;
                }
                self.init_chunk_heap_state = InitChunkHeapState::Start;
                Ok(IOResult::Done(()))
            }
        }
    }

    fn next_from_chunk_heap(&mut self) -> Result<IOResult<Option<SortableImmutableRecord>>> {
        // Make sure all chunks read at least one record into their buffer.
        turso_assert!(
            !self.chunks.iter().any(|chunk| matches!(
                *chunk.io_state.read().unwrap(),
                SortedChunkIOState::WaitingForRead
            )),
            "chunks should have been read"
        );

        if let Some((next_record, next_chunk_idx)) = self.chunk_heap.pop() {
            // TODO: blocking will be unnecessary here with IO completions
            let io = self.io.clone();
            io.block(|| self.push_to_chunk_heap(next_chunk_idx))?;
            Ok(IOResult::Done(Some(next_record.0)))
        } else {
            Ok(IOResult::Done(None))
        }
    }

    fn push_to_chunk_heap(&mut self, chunk_idx: usize) -> Result<IOResult<()>> {
        let chunk = &mut self.chunks[chunk_idx];

        if let Some(record) = return_if_io!(chunk.next()) {
            self.chunk_heap.push((
                Reverse(SortableImmutableRecord::new(
                    record,
                    self.key_len,
                    self.index_key_info.clone(),
                )?),
                chunk_idx,
            ));
        }

        Ok(IOResult::Done(()))
    }

    fn flush(&mut self) -> Result<Option<Completion>> {
        if self.records.is_empty() {
            // Dummy completion to not complicate logic handling
            return Ok(None);
        }

        self.records.sort();

        let chunk_file = match &self.temp_file {
            Some(temp_file) => temp_file.file.clone(),
            None => {
                let temp_dir = tempfile::tempdir()?;
                let chunk_file_path = temp_dir.as_ref().join("chunk_file");
                let chunk_file = self.io.open_file(
                    chunk_file_path.to_str().unwrap(),
                    OpenFlags::Create,
                    false,
                )?;
                self.temp_file = Some(TempFile {
                    _temp_dir: temp_dir,
                    file: chunk_file.clone(),
                });
                chunk_file
            }
        };

        // Make sure the chunk buffer size can fit the largest record and its size varint.
        let chunk_buffer_size = self
            .min_chunk_read_buffer_size
            .max(self.max_payload_size_in_buffer + 9);

        let mut chunk_size = 0;
        // Pre-compute varint lengths for record sizes to determine the total buffer size.
        let mut record_size_lengths = Vec::with_capacity(self.records.len());
        for record in self.records.iter() {
            let record_size = record.record.get_payload().len();
            let size_len = varint_len(record_size as u64);
            record_size_lengths.push(size_len);
            chunk_size += size_len + record_size;
        }

        let mut chunk = SortedChunk::new(chunk_file, self.next_chunk_offset, chunk_buffer_size);
        let c = chunk.write(&mut self.records, record_size_lengths, chunk_size)?;
        self.chunks.push(chunk);

        self.current_buffer_size = 0;
        self.max_payload_size_in_buffer = 0;
        // increase offset start for next chunk
        self.next_chunk_offset += chunk_size;

        Ok(Some(c))
    }
}

#[derive(Debug, Clone, Copy)]
enum NextState {
    Start,
    Finish,
}

struct SortedChunk {
    /// The chunk file.
    file: Arc<dyn File>,
    /// Offset of the start of chunk in file
    start_offset: u64,
    /// The size of this chunk file in bytes.
    chunk_size: usize,
    /// The read buffer.
    buffer: Arc<RwLock<Vec<u8>>>,
    /// The current length of the buffer.
    buffer_len: Arc<atomic::AtomicUsize>,
    /// The records decoded from the chunk file.
    records: Vec<ImmutableRecord>,
    /// The current IO state of the chunk.
    io_state: Arc<RwLock<SortedChunkIOState>>,
    /// The total number of bytes read from the chunk file.
    total_bytes_read: Arc<atomic::AtomicUsize>,
    /// State machine for [SortedChunk::next]
    next_state: NextState,
}

impl SortedChunk {
    fn new(file: Arc<dyn File>, start_offset: usize, buffer_size: usize) -> Self {
        Self {
            file,
            start_offset: start_offset as u64,
            chunk_size: 0,
            buffer: Arc::new(RwLock::new(vec![0; buffer_size])),
            buffer_len: Arc::new(atomic::AtomicUsize::new(0)),
            records: Vec::new(),
            io_state: Arc::new(RwLock::new(SortedChunkIOState::None)),
            total_bytes_read: Arc::new(atomic::AtomicUsize::new(0)),
            next_state: NextState::Start,
        }
    }

    fn buffer_len(&self) -> usize {
        self.buffer_len.load(atomic::Ordering::SeqCst)
    }

    fn set_buffer_len(&self, len: usize) {
        self.buffer_len.store(len, atomic::Ordering::SeqCst);
    }

    fn next(&mut self) -> Result<IOResult<Option<ImmutableRecord>>> {
        loop {
            match self.next_state {
                NextState::Start => {
                    let mut buffer_len = self.buffer_len();
                    if self.records.is_empty() && buffer_len == 0 {
                        return Ok(IOResult::Done(None));
                    }

                    if self.records.is_empty() {
                        let mut buffer_ref = self.buffer.write().unwrap();
                        let buffer = buffer_ref.as_mut_slice();
                        let mut buffer_offset = 0;
                        while buffer_offset < buffer_len {
                            // Extract records from the buffer until we run out of the buffer or we hit an incomplete record.
                            let (record_size, bytes_read) =
                                match read_varint(&buffer[buffer_offset..buffer_len]) {
                                    Ok((record_size, bytes_read)) => {
                                        (record_size as usize, bytes_read)
                                    }
                                    Err(LimboError::Corrupt(_))
                                        if *self.io_state.read().unwrap()
                                            != SortedChunkIOState::ReadEOF =>
                                    {
                                        // Failed to decode a partial varint.
                                        break;
                                    }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                };
                            if record_size > buffer_len - (buffer_offset + bytes_read) {
                                if *self.io_state.read().unwrap() == SortedChunkIOState::ReadEOF {
                                    crate::bail_corrupt_error!("Incomplete record");
                                }
                                break;
                            }
                            buffer_offset += bytes_read;

                            let mut record = ImmutableRecord::new(record_size);
                            record.start_serialization(
                                &buffer[buffer_offset..buffer_offset + record_size],
                            );
                            buffer_offset += record_size;

                            self.records.push(record);
                        }
                        if buffer_offset < buffer_len {
                            buffer.copy_within(buffer_offset..buffer_len, 0);
                            buffer_len -= buffer_offset;
                        } else {
                            buffer_len = 0;
                        }
                        self.set_buffer_len(buffer_len);

                        self.records.reverse();
                    }

                    self.next_state = NextState::Finish;
                    // This check is done to see if we need to read more from the chunk before popping the record
                    if self.records.len() == 1
                        && *self.io_state.read().unwrap() != SortedChunkIOState::ReadEOF
                    {
                        // We've consumed the last record. Read more payload into the buffer.
                        if self.chunk_size - self.total_bytes_read.load(atomic::Ordering::SeqCst)
                            == 0
                        {
                            *self.io_state.write().unwrap() = SortedChunkIOState::ReadEOF;
                        } else {
                            let c = self.read()?;
                            io_yield_one!(c);
                        }
                    }
                }
                NextState::Finish => {
                    self.next_state = NextState::Start;
                    return Ok(IOResult::Done(self.records.pop()));
                }
            }
        }
    }

    fn read(&mut self) -> Result<Completion> {
        *self.io_state.write().unwrap() = SortedChunkIOState::WaitingForRead;

        let read_buffer_size = self.buffer.read().unwrap().len() - self.buffer_len();
        let read_buffer_size = read_buffer_size
            .min(self.chunk_size - self.total_bytes_read.load(atomic::Ordering::SeqCst));

        let read_buffer = Buffer::new_temporary(read_buffer_size);
        let read_buffer_ref = Arc::new(read_buffer);

        let chunk_io_state_copy = self.io_state.clone();
        let stored_buffer_copy = self.buffer.clone();
        let stored_buffer_len_copy = self.buffer_len.clone();
        let total_bytes_read_copy = self.total_bytes_read.clone();
        let read_complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                return;
            };
            let read_buf_ref = buf.clone();
            let read_buf = read_buf_ref.as_slice();

            let bytes_read = bytes_read as usize;
            if bytes_read == 0 {
                *chunk_io_state_copy.write().unwrap() = SortedChunkIOState::ReadEOF;
                return;
            }
            *chunk_io_state_copy.write().unwrap() = SortedChunkIOState::ReadComplete;

            let mut stored_buf_ref = stored_buffer_copy.write().unwrap();
            let stored_buf = stored_buf_ref.as_mut_slice();
            let mut stored_buf_len = stored_buffer_len_copy.load(atomic::Ordering::SeqCst);

            stored_buf[stored_buf_len..stored_buf_len + bytes_read]
                .copy_from_slice(&read_buf[..bytes_read]);
            stored_buf_len += bytes_read;

            stored_buffer_len_copy.store(stored_buf_len, atomic::Ordering::SeqCst);
            total_bytes_read_copy.fetch_add(bytes_read, atomic::Ordering::SeqCst);
        });

        let c = Completion::new_read(read_buffer_ref, read_complete);
        let c = self.file.pread(
            self.start_offset + self.total_bytes_read.load(atomic::Ordering::SeqCst) as u64,
            c,
        )?;
        Ok(c)
    }

    fn write(
        &mut self,
        records: &mut Vec<SortableImmutableRecord>,
        record_size_lengths: Vec<usize>,
        chunk_size: usize,
    ) -> Result<Completion> {
        assert!(*self.io_state.read().unwrap() == SortedChunkIOState::None);
        *self.io_state.write().unwrap() = SortedChunkIOState::WaitingForWrite;
        self.chunk_size = chunk_size;

        let buffer = Buffer::new_temporary(self.chunk_size);

        let mut buf_pos = 0;
        let buf = buffer.as_mut_slice();
        for (record, size_len) in records.drain(..).zip(record_size_lengths) {
            let payload = record.record.get_payload();
            // Write the record size varint.
            write_varint(&mut buf[buf_pos..buf_pos + size_len], payload.len() as u64);
            buf_pos += size_len;
            // Write the record payload.
            buf[buf_pos..buf_pos + payload.len()].copy_from_slice(payload);
            buf_pos += payload.len();
        }

        let buffer_ref = Arc::new(buffer);

        let buffer_ref_copy = buffer_ref.clone();
        let chunk_io_state_copy = self.io_state.clone();
        let write_complete = Box::new(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            *chunk_io_state_copy.write().unwrap() = SortedChunkIOState::WriteComplete;
            let buf_len = buffer_ref_copy.len();
            if bytes_written < buf_len as i32 {
                tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        });

        let c = Completion::new_write(write_complete);
        let c = self.file.pwrite(self.start_offset, buffer_ref, c)?;
        Ok(c)
    }
}

struct SortableImmutableRecord {
    record: ImmutableRecord,
    cursor: RecordCursor,
    key_values: RefCell<Vec<RefValue>>,
    key_len: usize,
    index_key_info: Rc<Vec<KeyInfo>>,
    /// The key deserialization error, if any.
    deserialization_error: RefCell<Option<LimboError>>,
}

impl SortableImmutableRecord {
    fn new(
        record: ImmutableRecord,
        key_len: usize,
        index_key_info: Rc<Vec<KeyInfo>>,
    ) -> Result<Self> {
        let mut cursor = RecordCursor::with_capacity(key_len);
        cursor.ensure_parsed_upto(&record, key_len - 1)?;
        turso_assert!(
            index_key_info.len() >= cursor.serial_types.len(),
            "index_key_info.len() < cursor.serial_types.len()"
        );
        Ok(Self {
            record,
            cursor,
            key_values: RefCell::new(Vec::with_capacity(key_len)),
            index_key_info,
            deserialization_error: RefCell::new(None),
            key_len,
        })
    }

    /// Attempts to deserialize the key value at the given index.
    /// If the key value has already been deserialized, this does nothing.
    /// The deserialized key value is stored in the `key_values` field.
    /// In case of an error, the error is stored in the `deserialization_error` field.
    fn try_deserialize_key(&self, idx: usize) {
        let mut key_values = self.key_values.borrow_mut();
        if idx < key_values.len() {
            // The key value with this index has already been deserialized.
            return;
        }
        match self.cursor.deserialize_column(&self.record, idx) {
            Ok(value) => key_values.push(value),
            Err(error) => {
                self.deserialization_error.replace(Some(error));
            }
        }
    }
}

impl Ord for SortableImmutableRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.deserialization_error.borrow().is_some()
            || other.deserialization_error.borrow().is_some()
        {
            // If one of the records has a deserialization error, circumvent the comparison and return early.
            return Ordering::Equal;
        }
        assert_eq!(
            self.cursor.serial_types.len(),
            other.cursor.serial_types.len()
        );
        let this_key_values_len = self.key_values.borrow().len();
        let other_key_values_len = other.key_values.borrow().len();

        for i in 0..self.key_len {
            // Lazily deserialize the key values if they haven't been deserialized already.
            if i >= this_key_values_len {
                self.try_deserialize_key(i);
                if self.deserialization_error.borrow().is_some() {
                    return Ordering::Equal;
                }
            }
            if i >= other_key_values_len {
                other.try_deserialize_key(i);
                if other.deserialization_error.borrow().is_some() {
                    return Ordering::Equal;
                }
            }

            let this_key_value = &self.key_values.borrow()[i];
            let other_key_value = &other.key_values.borrow()[i];
            let column_order = self.index_key_info[i].sort_order;
            let collation = self.index_key_info[i].collation;

            let cmp = match (this_key_value, other_key_value) {
                (RefValue::Text(left), RefValue::Text(right)) => {
                    collation.compare_strings(left.as_str(), right.as_str())
                }
                _ => this_key_value.partial_cmp(other_key_value).unwrap(),
            };
            if !cmp.is_eq() {
                return match column_order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
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
    WaitingForWrite,
    WriteComplete,
    ReadEOF,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::types::{ImmutableRecord, RefValue, Value, ValueType};
    use crate::util::IOExt;
    use crate::PlatformIO;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    fn get_seed() -> u64 {
        std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            |v| {
                v.parse()
                    .expect("Failed to parse SEED environment variable as u64")
            },
        ) as u64
    }

    #[test]
    fn fuzz_external_sort() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let io = Arc::new(PlatformIO::new().unwrap());

        let attempts = 8;
        for _ in 0..attempts {
            let mut sorter = Sorter::new(
                &[SortOrder::Asc],
                vec![CollationSeq::Binary],
                256,
                64,
                io.clone(),
            );

            let num_records = 1000 + rng.next_u64() % 2000;
            let num_records = num_records as i64;

            let num_values = 1 + rng.next_u64() % 4;
            let value_types = generate_value_types(&mut rng, num_values as usize);

            let mut initial_records = Vec::with_capacity(num_records as usize);
            for i in (0..num_records).rev() {
                let mut values = vec![Value::Integer(i)];
                values.append(&mut generate_values(&mut rng, &value_types));
                let record = ImmutableRecord::from_values(&values, values.len());

                io.block(|| sorter.insert(&record))
                    .expect("Failed to insert the record");
                initial_records.push(record);
            }

            io.block(|| sorter.sort())
                .expect("Failed to sort the records");

            assert!(!sorter.is_empty());
            assert!(!sorter.chunks.is_empty());

            for i in 0..num_records {
                assert!(sorter.has_more());
                let record = sorter.record().unwrap();
                assert_eq!(record.get_values()[0], RefValue::Integer(i));
                // Check that the record remained unchanged after sorting.
                assert_eq!(record, &initial_records[(num_records - i - 1) as usize]);

                io.block(|| sorter.next())
                    .expect("Failed to get the next record");
            }
            assert!(!sorter.has_more());
        }
    }

    fn generate_value_types<R: RngCore>(rng: &mut R, num_values: usize) -> Vec<ValueType> {
        let mut value_types = Vec::with_capacity(num_values);

        for _ in 0..num_values {
            let value_type: ValueType = match rng.next_u64() % 4 {
                0 => ValueType::Integer,
                1 => ValueType::Float,
                2 => ValueType::Blob,
                3 => ValueType::Null,
                _ => unreachable!(),
            };
            value_types.push(value_type);
        }

        value_types
    }

    fn generate_values<R: RngCore>(rng: &mut R, value_types: &[ValueType]) -> Vec<Value> {
        let mut values = Vec::with_capacity(value_types.len());
        for value_type in value_types {
            let value = match value_type {
                ValueType::Integer => Value::Integer(rng.next_u64() as i64),
                ValueType::Float => {
                    let numerator = rng.next_u64() as f64;
                    let denominator = rng.next_u64() as f64;
                    Value::Float(numerator / denominator)
                }
                ValueType::Blob => {
                    let mut blob = Vec::with_capacity((rng.next_u64() % 2047 + 1) as usize);
                    rng.fill_bytes(&mut blob);
                    Value::Blob(blob)
                }
                ValueType::Null => Value::Null,
                _ => unreachable!(),
            };
            values.push(value);
        }
        values
    }
}
