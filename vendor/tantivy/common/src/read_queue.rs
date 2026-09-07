//! An injected range-request queue, not an executor. Polling a Tantivy future
//! submits requests; its owner completes them using its own storage driver.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use async_trait::async_trait;
use ownedbytes::OwnedBytes;

use crate::HasLen;
use crate::file_slice::FileHandle;

/// One snapshot's pending range requests. Never share it across snapshots.
#[derive(Clone, Debug, Default)]
pub struct ReadQueue(Arc<Mutex<VecDeque<ReadRequest>>>);

impl ReadQueue {
    /// Creates a logical file. The caller must pin its contents until all
    /// handles and requests for this snapshot have been dropped.
    pub fn file(&self, name: String, len: usize) -> Arc<dyn FileHandle> {
        Arc::new(QueuedFile {
            queue: self.clone(),
            name,
            len,
        })
    }

    /// Takes the next request, skipping futures that have been dropped.
    pub fn pop(&self) -> Option<ReadRequest> {
        let mut queue = self.0.lock().expect("Read queue poisoned");
        while let Some(request) = queue.pop_front() {
            if !request.is_cancelled() {
                return Some(request);
            }
        }
        None
    }
}

#[derive(Debug)]
struct QueuedFile {
    queue: ReadQueue,
    name: String,
    len: usize,
}

impl HasLen for QueuedFile {
    fn len(&self) -> usize {
        self.len
    }
}

#[async_trait]
impl FileHandle for QueuedFile {
    fn read_bytes(&self, _range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Snapshot file requires asynchronous reads",
        ))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.start > range.end || range.end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Read range exceeds file",
            ));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let response = Arc::new(Mutex::new(Response::default()));
        self.queue
            .0
            .lock()
            .expect("Read queue poisoned")
            .push_back(ReadRequest {
                name: self.name.clone(),
                len: self.len,
                range,
                response: Arc::downgrade(&response),
            });
        poll_fn(|cx| {
            let mut response = response.lock().expect("Read response poisoned");
            if let Some(result) = response.result.take() {
                Poll::Ready(result)
            } else {
                response.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

#[derive(Debug, Default)]
struct Response {
    result: Option<io::Result<OwnedBytes>>,
    waker: Option<Waker>,
}

/// A request owned by the storage driver. Dropping it reports cancellation;
/// dropping the waiting future never frees buffers submitted to storage.
#[derive(Debug)]
pub struct ReadRequest {
    name: String,
    len: usize,
    range: Range<usize>,
    response: Weak<Mutex<Response>>,
}

impl ReadRequest {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn file_len(&self) -> usize {
        self.len
    }
    pub fn range(&self) -> Range<usize> {
        self.range.clone()
    }
    pub fn is_cancelled(&self) -> bool {
        self.response.strong_count() == 0
    }

    /// Completes exactly once, including an inline completion before the
    /// waiting future has registered its waker.
    pub fn complete(mut self, result: io::Result<OwnedBytes>) {
        let result = result.and_then(|bytes| {
            if bytes.len() != self.range.len() {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Short range read",
                ))
            } else {
                Ok(bytes)
            }
        });
        self.respond(result);
    }

    fn respond(&mut self, result: io::Result<OwnedBytes>) {
        if let Some(response) = std::mem::take(&mut self.response).upgrade() {
            let waker = {
                let mut response = response.lock().expect("Read response poisoned");
                response.result = Some(result);
                response.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl Drop for ReadRequest {
    fn drop(&mut self) {
        self.respond(Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "Range request was cancelled",
        )));
    }
}
