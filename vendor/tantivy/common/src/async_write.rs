//! Runtime-free output. Requests own submitted bytes until the storage driver
//! acknowledges them; a cancelled producer cannot free an in-flight buffer.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

pub type WriteFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;
pub type AsyncWritePtr = Box<dyn AsyncWrite>;

/// An append-only output. `finish` must complete before the file is published.
/// A failed or cancelled mutation invalidates the writer. Dropping it does not
/// flush, finalize, or roll back requests already submitted to storage.
pub trait AsyncWrite: Send {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> WriteFuture<'a, usize>;
    fn flush(&mut self) -> WriteFuture<'_, ()>;
    fn finish(self: Box<Self>) -> WriteFuture<'static, ()>;

    fn write_all<'a>(&'a mut self, mut bytes: &'a [u8]) -> WriteFuture<'a, ()> {
        Box::pin(async move {
            while !bytes.is_empty() {
                let written = self.write(bytes).await?;
                if written == 0 || written > bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "Invalid write progress",
                    ));
                }
                bytes = &bytes[written..];
            }
            Ok(())
        })
    }
}

/// One private build's output queue. The bound applies to each submitted write,
/// not to the encoder's input arrays or the total memory of an index build.
#[derive(Clone, Debug)]
pub struct WriteQueue {
    pending: Arc<Mutex<VecDeque<WriteRequest>>>,
    request_size: NonZeroUsize,
}

impl WriteQueue {
    pub fn new(request_size: NonZeroUsize) -> Self {
        Self {
            pending: Arc::default(),
            request_size,
        }
    }

    pub async fn open(&self, name: String) -> io::Result<AsyncWritePtr> {
        self.submit(name.clone(), WriteOperation::Open).await?;
        Ok(Box::new(QueuedWriter {
            queue: self.clone(),
            name,
            offset: 0,
            usable: true,
        }))
    }

    pub fn pop(&self) -> Option<WriteRequest> {
        let mut pending = self.pending.lock().expect("Write queue poisoned");
        while let Some(request) = pending.pop_front() {
            if !request.is_cancelled() {
                return Some(request);
            }
        }
        None
    }

    async fn submit(&self, name: String, operation: WriteOperation) -> io::Result<usize> {
        let response = Arc::new(Mutex::new(Response::default()));
        self.pending
            .lock()
            .expect("Write queue poisoned")
            .push_back(WriteRequest {
                name,
                operation,
                response: Arc::downgrade(&response),
            });
        poll_fn(|cx| {
            let mut response = response.lock().expect("Write response poisoned");
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

struct QueuedWriter {
    queue: WriteQueue,
    name: String,
    offset: u64,
    usable: bool,
}

impl AsyncWrite for QueuedWriter {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> WriteFuture<'a, usize> {
        Box::pin(async move {
            self.begin()?;
            if bytes.is_empty() {
                self.usable = true;
                return Ok(0);
            }
            let len = bytes.len().min(self.queue.request_size.get());
            self.offset
                .checked_add(len as u64)
                .ok_or_else(|| io::Error::other("File offset overflow"))?;
            let written = self
                .queue
                .submit(
                    self.name.clone(),
                    WriteOperation::Write {
                        offset: self.offset,
                        bytes: Arc::from(&bytes[..len]),
                    },
                )
                .await?;
            self.offset += written as u64;
            self.usable = true;
            Ok(written)
        })
    }

    fn flush(&mut self) -> WriteFuture<'_, ()> {
        Box::pin(async move {
            self.begin()?;
            self.queue
                .submit(self.name.clone(), WriteOperation::Flush)
                .await?;
            self.usable = true;
            Ok(())
        })
    }

    fn finish(mut self: Box<Self>) -> WriteFuture<'static, ()> {
        Box::pin(async move {
            self.begin()?;
            self.queue
                .submit(
                    self.name.clone(),
                    WriteOperation::Finish { len: self.offset },
                )
                .await?;
            Ok(())
        })
    }
}

impl QueuedWriter {
    fn begin(&mut self) -> io::Result<()> {
        if !std::mem::replace(&mut self.usable, false) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Writer failed or was cancelled",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum WriteOperation {
    Open,
    Write { offset: u64, bytes: Arc<[u8]> },
    Flush,
    Finish { len: u64 },
}

#[derive(Debug, Default)]
struct Response {
    result: Option<io::Result<usize>>,
    waker: Option<Waker>,
}

#[derive(Debug)]
pub struct WriteRequest {
    name: String,
    operation: WriteOperation,
    response: Weak<Mutex<Response>>,
}

impl WriteRequest {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn operation(&self) -> &WriteOperation {
        &self.operation
    }
    pub fn is_cancelled(&self) -> bool {
        self.response.strong_count() == 0
    }

    /// Writes report bytes accepted; control operations report zero. A zero
    /// byte write is an error, not a signal to resubmit the same operation.
    pub fn complete(mut self, result: io::Result<usize>) {
        let result = result.and_then(|count| match &self.operation {
            WriteOperation::Write { bytes, .. } if count == 0 || count > bytes.len() => Err(
                io::Error::new(io::ErrorKind::WriteZero, "Invalid write progress"),
            ),
            WriteOperation::Write { .. } => Ok(count),
            _ if count == 0 => Ok(0),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Control operation returned a byte count",
            )),
        });
        self.respond(result);
    }

    fn respond(&mut self, result: io::Result<usize>) {
        if let Some(response) = std::mem::take(&mut self.response).upgrade() {
            let waker = {
                let mut response = response.lock().expect("Write response poisoned");
                response.result = Some(result);
                response.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl Drop for WriteRequest {
    fn drop(&mut self) {
        self.respond(Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "Write request was cancelled",
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Context;

    #[test]
    fn partial_writes_preserve_offsets_and_apply_backpressure() {
        let queue = WriteQueue::new(NonZeroUsize::new(4).unwrap());
        let mut future = Box::pin(async {
            let mut writer = queue.open("file".into()).await?;
            writer.write_all(b"abcdefghij").await?;
            writer.flush().await?;
            writer.finish().await
        });
        let mut output = Vec::new();
        let mut finished = false;
        loop {
            if let Poll::Ready(result) = poll(future.as_mut()) {
                result.unwrap();
                break;
            }
            let request = queue.pop().unwrap();
            for _ in 0..3 {
                assert!(poll(future.as_mut()).is_pending());
                assert!(queue.pop().is_none());
            }
            let written = match request.operation() {
                WriteOperation::Open => {
                    assert!(output.is_empty());
                    0
                }
                WriteOperation::Write { offset, bytes } => {
                    assert_eq!(*offset as usize, output.len());
                    assert!(bytes.len() <= 4);
                    let count = bytes.len().min(2);
                    output.extend_from_slice(&bytes[..count]);
                    count
                }
                WriteOperation::Flush => {
                    assert_eq!(output, b"abcdefghij");
                    0
                }
                WriteOperation::Finish { len } => {
                    assert_eq!(*len, 10);
                    finished = true;
                    0
                }
            };
            request.complete(Ok(written));
        }
        assert!(finished);
        assert_eq!(output, b"abcdefghij");
    }

    #[test]
    fn cancelled_and_failed_writes_poison_the_writer() {
        for failure in 0..5 {
            let queue = WriteQueue::new(NonZeroUsize::new(4).unwrap());
            let mut opening = Box::pin(queue.open("file".into()));
            assert!(poll(opening.as_mut()).is_pending());
            queue.pop().unwrap().complete(Ok(0));
            let Poll::Ready(Ok(mut writer)) = poll(opening.as_mut()) else {
                panic!("open did not finish")
            };
            let mut write = writer.write_all(b"abcd");
            assert!(poll(write.as_mut()).is_pending());
            let request = queue.pop().unwrap();
            if failure == 0 {
                drop(write);
                assert!(request.is_cancelled());
                assert!(
                    matches!(request.operation(), WriteOperation::Write { bytes, .. } if bytes.as_ref() == b"abcd")
                );
                request.complete(Ok(4));
            } else {
                match failure {
                    1 => request.complete(Ok(0)),
                    2 => request.complete(Ok(5)),
                    3 => request.complete(Err(io::Error::other("injected"))),
                    _ => drop(request),
                }
                assert!(matches!(poll(write.as_mut()), Poll::Ready(Err(_))));
                drop(write);
            }
            let mut retry = writer.write(b"x");
            assert!(
                matches!(poll(retry.as_mut()), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::BrokenPipe)
            );
            assert!(queue.pop().is_none());
        }
    }

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(std::task::Waker::noop()))
    }
}
