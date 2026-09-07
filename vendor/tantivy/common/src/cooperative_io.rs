//! Runtime-free reads. The caller owns the operation and injects storage on
//! each resume; neither a scheduler nor a database dependency lives here.
//!
//! This is separate from `FileHandle`: synchronous Tantivy readers must not
//! silently block or turn a pending read into an I/O error and retry a search.

use std::ops::Range;
use std::sync::Arc;

/// The wait token belongs to the storage implementation (for example, a
/// Turso Completion). The caller waits outside Tantivy before resuming.
#[derive(Debug)]
pub enum ReadStatus<T, C> {
    Done(T),
    Pending(C),
}

/// Injected read access to one immutable file in a pinned snapshot.
pub trait RangeReader {
    type Completion;
    type Error;

    /// Resume the same range until Done. Implementations retain submitted I/O
    /// and its buffers across Pending, including if the operation is dropped.
    /// They must propagate completion errors before consuming the read result.
    fn read_range(
        &mut self,
        range: Range<usize>,
    ) -> Result<ReadStatus<Vec<u8>, Self::Completion>, Self::Error>;
}

#[derive(Debug)]
pub enum ReadError<E> {
    Storage(E),
    InvalidRange,
    WrongLength { expected: usize, actual: usize },
    Finished,
}

/// Assemble only the requested range, in bounded requests. Progress advances
/// only after a successful read. The output remains contiguous, not paged.
#[derive(Debug)]
pub struct ReadBytes {
    range: Range<usize>,
    next: usize,
    request_size: usize,
    bytes: Option<Vec<u8>>,
}

impl ReadBytes {
    pub fn new(
        file_len: usize,
        range: Range<usize>,
        request_size: usize,
    ) -> Result<Self, ReadError<std::convert::Infallible>> {
        if range.start > range.end || range.end > file_len || request_size == 0 {
            return Err(ReadError::InvalidRange);
        }
        Ok(Self {
            next: range.start,
            // File lengths can come from untrusted metadata. Allocate only
            // as storage returns data, not from the declared length.
            bytes: Some(Vec::new()),
            range,
            request_size,
        })
    }

    pub fn resume<R: RangeReader>(
        &mut self,
        reader: &mut R,
    ) -> Result<ReadStatus<Arc<[u8]>, R::Completion>, ReadError<R::Error>> {
        let Some(bytes) = self.bytes.as_mut() else {
            return Err(ReadError::Finished);
        };
        while self.next < self.range.end {
            let end = self
                .next
                .saturating_add(self.request_size)
                .min(self.range.end);
            let data = match reader.read_range(self.next..end) {
                Ok(ReadStatus::Pending(wait)) => return Ok(ReadStatus::Pending(wait)),
                Ok(ReadStatus::Done(data)) => data,
                Err(error) => {
                    self.bytes = None;
                    return Err(ReadError::Storage(error));
                }
            };
            if data.len() != end - self.next {
                let expected = end - self.next;
                self.bytes = None;
                return Err(ReadError::WrongLength {
                    expected,
                    actual: data.len(),
                });
            }
            bytes.extend_from_slice(&data);
            self.next = end;
        }
        Ok(ReadStatus::Done(Arc::from(self.bytes.take().unwrap())))
    }
}
