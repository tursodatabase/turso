//! Snapshot-bound range reads. The backing cursor, not a callback on a
//! shared Tantivy Directory, owns the transaction and outstanding page I/O.

use std::ops::Range;

use rustc_hash::FxHashMap as HashMap;
use tantivy::directory::cooperative_io::{RangeReader, ReadBytes, ReadError, ReadStatus};

use super::format::{segment_chunk_path, segment_chunk_prefix, SegmentData, SegmentDescriptor};
use super::rows::{row_fields, seek_key_for_path};
use super::DEFAULT_CHUNK_SIZE;
use crate::storage::btree::CursorTrait;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult, IOResultOr, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, Completion, LimboError};

/// The future owns Tantivy's partial progress; this driver alone owns the
/// transaction cursor. No snapshot handle can access or outlive that cursor.
pub(super) struct SnapshotIo<T> {
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = tantivy::Result<T>> + Send>>>,
    queue: tantivy::directory::ReadQueue,
    pending: Option<RangeRead>,
    chunks: ChunkCache,
}

impl<T> SnapshotIo<T> {
    pub fn new(
        queue: tantivy::directory::ReadQueue,
        future: impl std::future::Future<Output = tantivy::Result<T>> + Send + 'static,
    ) -> Self {
        Self {
            future: Some(Box::pin(future)),
            queue,
            pending: None,
            chunks: ChunkCache::default(),
        }
    }

    pub fn resume(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<T> {
        let result = self.drive(cursor);
        if result.is_err() {
            self.future = None;
            self.pending = None;
        }
        result
    }

    fn drive(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<T> {
        loop {
            if let Some(read) = &mut self.pending {
                let bytes = return_if_io!(read.resume(cursor, &mut self.chunks));
                let read = self.pending.take().unwrap();
                read.request
                    .complete(Ok(tantivy::directory::OwnedBytes::new(bytes)));
            }
            let future = self.future.as_mut().ok_or_else(|| {
                LimboError::InternalError("FTS operation resumed after completion".into())
            })?;
            // All suspension comes from this queue, which the owner drains
            // before polling again. Storage readiness is a Turso Completion.
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            if let std::task::Poll::Ready(result) = future.as_mut().poll(&mut context) {
                self.future = None;
                return Ok(IOResult::Done(result.map_err(|error| {
                    LimboError::InternalError(format!("FTS asynchronous operation: {error}"))
                })?));
            }
            let request = self.queue.pop().ok_or_else(|| {
                LimboError::InternalError("FTS suspended without a storage request".into())
            })?;
            self.pending = Some(RangeRead::new(request));
        }
    }
}

struct RangeRead {
    request: tantivy::directory::ReadRequest,
    next: usize,
    bytes: Vec<u8>,
    key: Option<crate::types::ImmutableRecord>,
    scan: ChunkScan,
}

impl RangeRead {
    fn new(request: tantivy::directory::ReadRequest) -> Self {
        Self {
            next: request.range().start,
            request,
            bytes: Vec::new(),
            key: None,
            scan: ChunkScan::default(),
        }
    }

    fn resume(
        &mut self,
        cursor: &mut dyn CursorTrait,
        chunks: &mut ChunkCache,
    ) -> IOResultOr<Vec<u8>> {
        if let Some(wait) = &self.scan.wait {
            if !wait.finished() {
                return Ok(IOResult::IO(IOCompletions(wait.clone())));
            }
            if let Some(error) = wait.get_error() {
                return Err(LimboError::CompletionError(error).into());
            }
            self.scan.wait = None;
        }
        let result = self.drive(cursor, chunks);
        if let Ok(IOResult::IO(wait)) = &result {
            self.scan.wait = Some(wait.0.clone());
        }
        result
    }

    fn drive(
        &mut self,
        cursor: &mut dyn CursorTrait,
        chunks: &mut ChunkCache,
    ) -> IOResultOr<Vec<u8>> {
        use crate::types::{ImmutableRecord, TextRef, TextSubtype, ValueRef};
        while self.next < self.request.range().end {
            let chunk = self.next / DEFAULT_CHUNK_SIZE;
            if let Some(bytes) = chunks.get(self.request.name(), chunk) {
                self.append_chunk(bytes);
                continue;
            }
            if self.key.is_none() {
                self.key = Some(ImmutableRecord::from_values(
                    [
                        ValueRef::Text(TextRef::new(self.request.name(), TextSubtype::Text)),
                        ValueRef::Numeric(crate::Numeric::Integer(chunk as i64)),
                        ValueRef::Blob(&[]),
                    ],
                    3,
                )?);
            }
            if !self.scan.seeked {
                let found = return_if_io!(cursor.seek(
                    SeekKey::IndexKey(self.key.as_ref().unwrap().as_record_ref()),
                    SeekOp::GE { eq_only: false },
                ));
                self.scan.seeked = true;
                self.scan.advance = matches!(found, SeekResult::TryAdvance);
            }
            return_if_io!(self.scan.advance(cursor));
            if !cursor.has_record() {
                return Err(LimboError::Corrupt("FTS missing range chunk".into()).into());
            }
            let record = return_if_io!(cursor.record())
                .ok_or_else(|| LimboError::Corrupt("FTS missing record".into()))?;
            let bytes = chunk_bytes(record, self.request.name(), chunk as i64)?;
            let chunk_start = chunk * DEFAULT_CHUNK_SIZE;
            let expected = (self.request.file_len() - chunk_start).min(DEFAULT_CHUNK_SIZE);
            if bytes.len() != expected {
                return Err(LimboError::Corrupt("FTS range chunk has wrong size".into()).into());
            }
            self.append_chunk(&bytes);
            chunks.put(self.request.name().into(), chunk, bytes);
            self.key = None;
            self.scan.seeked = false;
        }
        Ok(IOResult::Done(std::mem::take(&mut self.bytes)))
    }

    fn append_chunk(&mut self, bytes: &[u8]) {
        let chunk_start = self.next / DEFAULT_CHUNK_SIZE * DEFAULT_CHUNK_SIZE;
        let start = self.next - chunk_start;
        let end = (self.request.range().end - chunk_start).min(bytes.len());
        self.bytes.extend_from_slice(&bytes[start..end]);
        self.next = chunk_start + end;
    }
}

/// Avoid rereading a 512 KiB row for every small term payload in a merge.
/// This bounds only this operation's chunk cache, not Tantivy's live memory.
#[derive(Default)]
struct ChunkCache(std::collections::VecDeque<(String, usize, Vec<u8>)>);

impl ChunkCache {
    fn get(&mut self, path: &str, chunk: usize) -> Option<&[u8]> {
        let pos = self
            .0
            .iter()
            .position(|(name, index, _)| name == path && *index == chunk)?;
        let entry = self.0.remove(pos).unwrap();
        self.0.push_back(entry);
        Some(&self.0.back().unwrap().2)
    }

    fn put(&mut self, path: String, chunk: usize, bytes: Vec<u8>) {
        assert!(bytes.len() <= DEFAULT_CHUNK_SIZE);
        if self.0.len() == 8 {
            self.0.pop_front();
        }
        self.0.push_back((path, chunk, bytes));
    }
}

#[derive(Debug, Default)]
pub(super) struct SegmentRead {
    file: usize,
    read: Option<ReadBytes>,
    files: HashMap<String, Arc<[u8]>>,
    scan: ChunkScan,
}

impl SegmentRead {
    pub fn resume(
        &mut self,
        cursor: &mut dyn CursorTrait,
        descriptor: &SegmentDescriptor,
    ) -> IOResultOr<SegmentData> {
        if let Some(wait) = &self.scan.wait {
            if !wait.finished() {
                return Ok(IOResult::IO(IOCompletions(wait.clone())));
            }
            if let Some(error) = wait.get_error() {
                return Err(LimboError::CompletionError(error).into());
            }
            self.scan.wait = None;
        }
        let result = self.drive(cursor, descriptor);
        if let Ok(IOResult::IO(wait)) = &result {
            self.scan.wait = Some(wait.0.clone());
        }
        result
    }

    fn drive(
        &mut self,
        cursor: &mut dyn CursorTrait,
        descriptor: &SegmentDescriptor,
    ) -> IOResultOr<SegmentData> {
        if !self.scan.seeked {
            let key = seek_key_for_path(&segment_chunk_prefix(&descriptor.segment_id))?;
            let found = return_if_io!(cursor.seek(
                SeekKey::IndexKey(key.as_record_ref()),
                SeekOp::GE { eq_only: false },
            ));
            self.scan.seeked = true;
            self.scan.advance = matches!(found, SeekResult::TryAdvance);
        }
        while let Some(entry) = descriptor.files.get(self.file) {
            let size = usize::try_from(entry.size)
                .map_err(|_| LimboError::Corrupt("FTS file length overflows usize".into()))?;
            if size.div_ceil(DEFAULT_CHUNK_SIZE).max(1) != entry.num_chunks as usize {
                return Err(
                    LimboError::Corrupt("FTS file chunk count disagrees with size".into()).into(),
                );
            }
            if self.read.is_none() {
                self.read = Some(
                    ReadBytes::new(size, 0..size, DEFAULT_CHUNK_SIZE)
                        .map_err(|e| LimboError::Corrupt(format!("FTS read range: {e:?}")))?,
                );
            }
            let mut reader = ChunkReader {
                cursor,
                scan: &mut self.scan,
                path: segment_chunk_path(&descriptor.segment_id, self.file as u32),
            };
            // The on-disk format represents an empty file with one empty row.
            if size == 0 {
                match reader.read_range(0..0)? {
                    ReadStatus::Pending(wait) => return Ok(IOResult::IO(wait)),
                    ReadStatus::Done(bytes) if bytes.is_empty() => {}
                    ReadStatus::Done(_) => {
                        return Err(LimboError::Corrupt("FTS empty file has data".into()).into())
                    }
                }
            }
            let bytes = match self.read.as_mut().unwrap().resume(&mut reader) {
                Ok(ReadStatus::Pending(wait)) => return Ok(IOResult::IO(wait)),
                Ok(ReadStatus::Done(bytes)) => bytes,
                Err(ReadError::Storage(error)) => return Err(error),
                Err(error) => {
                    return Err(LimboError::Corrupt(format!("FTS file read: {error:?}")).into())
                }
            };
            self.files.insert(entry.name.clone(), bytes);
            self.file += 1;
            self.read = None;
        }
        return_if_io!(self.scan.advance(cursor));
        if cursor.has_record() {
            let record = return_if_io!(cursor.record())
                .ok_or_else(|| LimboError::Corrupt("FTS missing record".into()))?;
            let (path, _, _) = row_fields(record)?;
            if path.starts_with(&segment_chunk_prefix(&descriptor.segment_id)) {
                return Err(LimboError::Corrupt("FTS segment has extra chunks".into()).into());
            }
        }
        Ok(IOResult::Done(SegmentData::new(std::mem::take(
            &mut self.files,
        ))))
    }
}

#[derive(Debug, Default)]
struct ChunkScan {
    seeked: bool,
    advance: bool,
    wait: Option<Completion>,
}

impl ChunkScan {
    fn advance(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        if self.advance {
            return_if_io!(cursor.next());
            self.advance = false;
        }
        Ok(IOResult::Done(()))
    }
}

struct ChunkReader<'a> {
    cursor: &'a mut dyn CursorTrait,
    scan: &'a mut ChunkScan,
    path: String,
}

impl RangeReader for ChunkReader<'_> {
    type Completion = IOCompletions;
    type Error = Box<LimboError>;

    fn read_range(
        &mut self,
        range: Range<usize>,
    ) -> std::result::Result<ReadStatus<Vec<u8>, IOCompletions>, Self::Error> {
        let result = self.read_chunk(range)?;
        Ok(match result {
            IOResult::Done(bytes) => ReadStatus::Done(bytes),
            IOResult::IO(wait) => ReadStatus::Pending(wait),
        })
    }
}

impl ChunkReader<'_> {
    fn read_chunk(&mut self, range: Range<usize>) -> IOResultOr<Vec<u8>> {
        return_if_io!(self.scan.advance(self.cursor));
        if !self.cursor.has_record() {
            return Err(LimboError::Corrupt("FTS segment is missing chunks".into()).into());
        }
        let record = return_if_io!(self.cursor.record())
            .ok_or_else(|| LimboError::Corrupt("FTS missing record".into()))?;
        let bytes = chunk_bytes(
            record,
            &self.path,
            (range.start / DEFAULT_CHUNK_SIZE) as i64,
        )?;
        self.scan.advance = true;
        Ok(IOResult::Done(bytes))
    }
}

fn chunk_bytes(
    record: &crate::types::ImmutableRecord,
    expected_path: &str,
    expected_chunk: i64,
) -> crate::Result<Vec<u8>> {
    let (path, chunk, bytes) = row_fields(record)?;
    if path != expected_path || chunk != expected_chunk {
        return Err(LimboError::Corrupt(format!(
            "FTS unexpected chunk {path}:{chunk}"
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_cache_evicts_least_recently_used_row() {
        let mut cache = ChunkCache::default();
        for chunk in 0..8 {
            cache.put("file".into(), chunk, vec![chunk as u8; DEFAULT_CHUNK_SIZE]);
        }
        assert_eq!(cache.get("file", 0).unwrap()[0], 0);
        cache.put("file".into(), 8, vec![8; DEFAULT_CHUNK_SIZE]);
        assert!(cache.get("file", 1).is_none());
        assert!(cache.get("file", 0).is_some());
        assert!(cache.get("other-file", 8).is_none());
        assert_eq!(cache.0.len(), 8);
        assert_eq!(
            cache
                .0
                .iter()
                .map(|(_, _, bytes)| bytes.len())
                .sum::<usize>(),
            8 * DEFAULT_CHUNK_SIZE
        );
    }

    #[test]
    fn chunk_read_rejects_missing_duplicate_and_stray_chunk_numbers() {
        use crate::types::{ImmutableRecord, TextRef, TextSubtype, ValueRef};
        for chunk in [-1, 0, 1, 2] {
            let record = ImmutableRecord::from_values(
                [
                    ValueRef::Text(TextRef::new("file", TextSubtype::Text)),
                    ValueRef::Numeric(crate::Numeric::Integer(chunk)),
                    ValueRef::Blob(&[1, 2, 3]),
                ],
                3,
            )
            .unwrap();
            if chunk == 1 {
                assert_eq!(chunk_bytes(&record, "file", 1).unwrap(), vec![1, 2, 3]);
            } else {
                assert!(matches!(
                    chunk_bytes(&record, "file", 1),
                    Err(LimboError::Corrupt(_))
                ));
            }
            assert!(matches!(
                chunk_bytes(&record, "other-file", chunk),
                Err(LimboError::Corrupt(_))
            ));
        }
    }

    #[test]
    fn cooperative_read_resumes_without_resubmitting_ranges() {
        let mut reader = DelayedReader::default();
        let mut read = ReadBytes::new(20, 3..14, 4).unwrap();
        for expected in [3..7, 7..11, 11..14] {
            let ReadStatus::Pending(wait) = read.resume(&mut reader).unwrap() else {
                panic!()
            };
            assert!(!wait.finished());
            assert_eq!(reader.requests.last(), Some(&expected));
            for _ in 0..3 {
                assert!(matches!(
                    read.resume(&mut reader).unwrap(),
                    ReadStatus::Pending(_)
                ));
            }
            wait.complete(0);
        }
        let ReadStatus::Done(bytes) = read.resume(&mut reader).unwrap() else {
            panic!()
        };
        assert_eq!(&*bytes, &(3..14).collect::<Vec<u8>>());
        assert_eq!(reader.requests, vec![3..7, 7..11, 11..14]);
        assert!(matches!(read.resume(&mut reader), Err(ReadError::Finished)));
    }

    #[test]
    fn cooperative_read_propagates_completion_error_and_stops() {
        let mut reader = DelayedReader::default();
        let mut read = ReadBytes::new(8, 0..8, 4).unwrap();
        let ReadStatus::Pending(wait) = read.resume(&mut reader).unwrap() else {
            panic!()
        };
        wait.abort();
        assert!(matches!(
            read.resume(&mut reader),
            Err(ReadError::Storage(_))
        ));
        assert!(matches!(read.resume(&mut reader), Err(ReadError::Finished)));
        assert_eq!(reader.requests, vec![0..4]);
    }

    #[test]
    fn cooperative_read_accepts_inline_completion_and_rejects_short_read() {
        let mut reader = DelayedReader {
            inline: true,
            ..Default::default()
        };
        let mut read = ReadBytes::new(8, 1..8, 4).unwrap();
        let ReadStatus::Done(bytes) = read.resume(&mut reader).unwrap() else {
            panic!()
        };
        assert_eq!(&*bytes, &[1, 2, 3, 4, 5, 6, 7]);
        reader.short = true;
        let mut read = ReadBytes::new(8, 0..8, 4).unwrap();
        assert!(matches!(
            read.resume(&mut reader),
            Err(ReadError::WrongLength {
                expected: 4,
                actual: 3
            })
        ));
        assert!(matches!(read.resume(&mut reader), Err(ReadError::Finished)));
    }

    #[test]
    fn cooperative_read_drop_does_not_destroy_pending_storage() {
        let mut reader = DelayedReader::default();
        let mut read = ReadBytes::new(8, 0..8, 4).unwrap();
        let ReadStatus::Pending(wait) = read.resume(&mut reader).unwrap() else {
            panic!()
        };
        drop(read);
        assert!(!wait.finished());
        wait.complete(0);
        assert!(wait.succeeded());
        assert_eq!(reader.requests, vec![0..4]);
    }

    #[test]
    fn cooperative_read_validates_ranges_and_empty_reads_do_no_io() {
        assert!(ReadBytes::new(4, 3..5, 1).is_err());
        assert!(ReadBytes::new(4, Range { start: 3, end: 2 }, 1).is_err());
        assert!(ReadBytes::new(4, 0..4, 0).is_err());
        let mut reader = DelayedReader::default();
        let mut read = ReadBytes::new(4, 2..2, 1).unwrap();
        let ReadStatus::Done(bytes) = read.resume(&mut reader).unwrap() else {
            panic!()
        };
        assert!(bytes.is_empty());
        assert!(reader.requests.is_empty());
    }

    #[derive(Default)]
    struct DelayedReader {
        requests: Vec<Range<usize>>,
        pending: Option<(Range<usize>, Completion)>,
        inline: bool,
        short: bool,
    }

    impl RangeReader for DelayedReader {
        type Completion = Completion;
        type Error = crate::CompletionError;

        fn read_range(
            &mut self,
            range: Range<usize>,
        ) -> std::result::Result<ReadStatus<Vec<u8>, Completion>, Self::Error> {
            if self.pending.is_none() {
                self.requests.push(range.clone());
                let wait = Completion::new_sync(|_| {});
                if self.inline {
                    wait.complete(0);
                }
                self.pending = Some((range.clone(), wait));
            }
            let (submitted, wait) = self.pending.as_ref().unwrap();
            assert_eq!(&range, submitted);
            if !wait.finished() {
                return Ok(ReadStatus::Pending(wait.clone()));
            }
            if let Some(error) = wait.get_error() {
                return Err(error);
            }
            self.pending = None;
            let mut bytes = range.map(|n| n as u8).collect::<Vec<_>>();
            if self.short {
                bytes.pop();
            }
            Ok(ReadStatus::Done(bytes))
        }
    }
}
