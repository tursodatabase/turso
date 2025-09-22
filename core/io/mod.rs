use crate::storage::buffer_pool::ArenaBuffer;
use crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;
use crate::{BufferPool, CompletionError, Result};
use bitflags::bitflags;
use cfg_block::cfg_block;
use parking_lot::Once;
use std::cell::RefCell;
use std::fmt;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use std::{fmt::Debug, pin::Pin};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion>;
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion>;
    fn sync(&self, c: Completion) -> Result<Completion>;
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        if buffers.is_empty() {
            c.complete(0);
            return Ok(c);
        }
        if buffers.len() == 1 {
            return self.pwrite(pos, buffers[0].clone(), c);
        }
        // naive default implementation can be overridden on backends where it makes sense to
        let mut pos = pos;
        let outstanding = Arc::new(AtomicUsize::new(buffers.len()));
        let total_written = Arc::new(AtomicUsize::new(0));

        for buf in buffers {
            let len = buf.len();
            let child_c = {
                let c_main = c.clone();
                let outstanding = outstanding.clone();
                let total_written = total_written.clone();
                let _cloned = buf.clone();
                Completion::new_write(move |n| {
                    if let Ok(n) = n {
                        // reference buffer in callback to ensure alive for async io
                        let _buf = _cloned.clone();
                        // accumulate bytes actually reported by the backend
                        total_written.fetch_add(n as usize, Ordering::SeqCst);
                        if outstanding.fetch_sub(1, Ordering::AcqRel) == 1 {
                            // last one finished
                            c_main.complete(total_written.load(Ordering::Acquire) as i32);
                        }
                    }
                })
            };
            if let Err(e) = self.pwrite(pos, buf.clone(), child_c) {
                c.abort();
                return Err(e);
            }
            pos += len as u64;
        }
        Ok(c)
    }
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

bitflags! {
    impl OpenFlags: i32 {
        const None = 0b00000000;
        const Create = 0b0000001;
        const ReadOnly = 0b0000010;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    // remove_file is used in the sync-engine
    fn remove_file(&self, path: &str) -> Result<()>;

    fn step(&self) -> Result<()> {
        Ok(())
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        c.iter().for_each(|c| c.abort());
        Ok(())
    }

    fn drain(&self) -> Result<()> {
        Ok(())
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        while !c.finished() {
            self.step()?
        }
        if let Some(Some(err)) = c.inner.result.get().copied() {
            return Err(err.into());
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }

    fn register_fixed_buffer(&self, _ptr: NonNull<u8>, _len: usize) -> Result<u32> {
        Err(crate::LimboError::InternalError(
            "unsupported operation".to_string(),
        ))
    }
}

pub type ReadComplete = dyn Fn(Result<(Arc<Buffer>, i32), CompletionError>);
pub type WriteComplete = dyn Fn(Result<i32, CompletionError>);
pub type SyncComplete = dyn Fn(Result<i32, CompletionError>);
pub type TruncateComplete = dyn Fn(Result<i32, CompletionError>);

#[must_use]
#[derive(Debug, Clone)]
pub struct Completion {
    inner: Arc<CompletionInner>,
}

#[derive(Debug)]
struct CompletionInner {
    completion_type: CompletionType,
    /// None means we completed successfully
    // Thread safe with OnceLock
    result: std::sync::OnceLock<Option<CompletionError>>,
    needs_link: bool,
    /// before calling callback we check if done is true
    done: Once,
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(..) => f.debug_tuple("Read").finish(),
            Self::Write(..) => f.debug_tuple("Write").finish(),
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
            Self::Truncate(..) => f.debug_tuple("Truncate").finish(),
        }
    }
}

pub enum CompletionType {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
    Truncate(TruncateCompletion),
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Self {
            inner: Arc::new(CompletionInner {
                completion_type,
                result: OnceLock::new(),
                needs_link: false,
                done: Once::new(),
            }),
        }
    }

    pub fn new_linked(completion_type: CompletionType) -> Self {
        Self {
            inner: Arc::new(CompletionInner {
                completion_type,
                result: OnceLock::new(),
                needs_link: true,
                done: Once::new(),
            }),
        }
    }

    pub fn needs_link(&self) -> bool {
        self.inner.needs_link
    }

    pub fn new_write_linked<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new_linked(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Result<(Arc<Buffer>, i32), CompletionError>) + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }
    pub fn new_sync<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Sync(SyncCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_trunc<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Truncate(TruncateCompletion::new(Box::new(
            complete,
        ))))
    }

    /// Create a dummy completed completion
    pub fn new_dummy() -> Self {
        let c = Self::new_write(|_| {});
        c.complete(0);
        c
    }

    pub fn is_completed(&self) -> bool {
        self.inner.result.get().is_some_and(|val| val.is_none())
    }

    pub fn has_error(&self) -> bool {
        self.inner.result.get().is_some_and(|val| val.is_some())
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        self.inner.result.get().and_then(|res| *res)
    }

    /// Checks if the Completion completed or errored
    pub fn finished(&self) -> bool {
        self.inner.result.get().is_some()
    }

    pub fn complete(&self, result: i32) {
        let result = Ok(result);
        self.callback(result);
    }

    pub fn error(&self, err: CompletionError) {
        let result = Err(err);
        self.callback(result);
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted);
    }

    fn callback(&self, result: Result<i32, CompletionError>) {
        self.inner.done.call_once(|| {
            match &self.inner.completion_type {
                CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => w.callback(result),
                CompletionType::Sync(s) => s.callback(result), // fix
                CompletionType::Truncate(t) => t.callback(result),
            };
            self.inner
                .result
                .set(result.err())
                .expect("result must be set only once");
        });
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        match self.inner.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        }
    }

    /// only call this method if you are sure that the completion is
    /// a WriteCompletion, panics otherwise
    pub fn as_write(&self) -> &WriteCompletion {
        match self.inner.completion_type {
            CompletionType::Write(ref w) => w,
            _ => unreachable!(),
        }
    }
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<ReadComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<ReadComplete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Buffer {
        &self.buf
    }

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) {
        (self.complete)(bytes_read.map(|b| (self.buf.clone(), b)));
    }

    pub fn buf_arc(&self) -> Arc<Buffer> {
        self.buf.clone()
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        (self.complete)(bytes_written);
    }
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

pub struct TruncateCompletion {
    pub complete: Box<TruncateComplete>,
}

impl TruncateCompletion {
    pub fn new(complete: Box<TruncateComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

pub type BufferData = Pin<Box<[u8]>>;

pub enum Buffer {
    Heap(BufferData),
    Pooled(ArenaBuffer),
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "Pooled(len={})", p.logical_len()),
            Self::Heap(buf) => write!(f, "{buf:?}: {}", buf.len()),
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let len = self.len();
        if let Self::Heap(buf) = self {
            TEMP_BUFFER_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                // take ownership of the buffer by swapping it with a dummy
                let buffer = std::mem::replace(buf, Pin::new(vec![].into_boxed_slice()));
                cache.return_buffer(buffer, len);
            });
        }
    }
}

impl Buffer {
    pub fn new(data: Vec<u8>) -> Self {
        tracing::trace!("buffer::new({:?})", data);
        Self::Heap(Pin::new(data.into_boxed_slice()))
    }

    /// Returns the index of the underlying `Arena` if it was registered with
    /// io_uring. Only for use with `UringIO` backend.
    pub fn fixed_id(&self) -> Option<u32> {
        match self {
            Self::Heap { .. } => None,
            Self::Pooled(buf) => buf.fixed_id(),
        }
    }

    pub fn new_pooled(buf: ArenaBuffer) -> Self {
        Self::Pooled(buf)
    }

    pub fn new_temporary(size: usize) -> Self {
        TEMP_BUFFER_CACHE.with(|cache| {
            if let Some(buffer) = cache.borrow_mut().get_buffer(size) {
                Self::Heap(buffer)
            } else {
                Self::Heap(Pin::new(vec![0; size].into_boxed_slice()))
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(buf) => buf.len(),
            Self::Pooled(buf) => buf.logical_len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap(buf) => {
                // SAFETY: The buffer is guaranteed to be valid for the lifetime of the slice
                unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.len()) }
            }
            Self::Pooled(buf) => buf,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr(),
            Self::Pooled(buf) => buf.as_ptr(),
        }
    }
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr() as *mut u8,
            Self::Pooled(buf) => buf.as_ptr() as *mut u8,
        }
    }
}

thread_local! {
    /// thread local cache to re-use temporary buffers to prevent churn when pool overflows
    pub static TEMP_BUFFER_CACHE: RefCell<TempBufferCache> = RefCell::new(TempBufferCache::new());
}

/// A cache for temporary or any additional `Buffer` allocations beyond
/// what the `BufferPool` has room for, or for use before the pool is
/// fully initialized.
pub(crate) struct TempBufferCache {
    /// The `[Database::page_size]` at the time the cache is initiated.
    page_size: usize,
    /// Cache of buffers of size `self.page_size`.
    page_buffers: Vec<BufferData>,
    /// Cache of buffers of size `self.page_size` + WAL_FRAME_HEADER_SIZE.
    wal_frame_buffers: Vec<BufferData>,
    /// Maximum number of buffers that will live in each cache.
    max_cached: usize,
}

impl TempBufferCache {
    const DEFAULT_MAX_CACHE_SIZE: usize = 256;

    fn new() -> Self {
        Self {
            page_size: BufferPool::DEFAULT_PAGE_SIZE,
            page_buffers: Vec::with_capacity(8),
            wal_frame_buffers: Vec::with_capacity(8),
            max_cached: Self::DEFAULT_MAX_CACHE_SIZE,
        }
    }

    /// If the `[Database::page_size]` is set, any temporary buffers that might
    /// exist prior need to be cleared and new `page_size` needs to be saved.
    pub fn reinit_cache(&mut self, page_size: usize) {
        self.page_buffers.clear();
        self.wal_frame_buffers.clear();
        self.page_size = page_size;
    }

    fn get_buffer(&mut self, size: usize) -> Option<BufferData> {
        match size {
            sz if sz == self.page_size => self.page_buffers.pop(),
            sz if sz == (self.page_size + WAL_FRAME_HEADER_SIZE) => self.wal_frame_buffers.pop(),
            _ => None,
        }
    }

    fn return_buffer(&mut self, buff: BufferData, len: usize) {
        let sz = self.page_size;
        let cache = match len {
            n if n.eq(&sz) => &mut self.page_buffers,
            n if n.eq(&(sz + WAL_FRAME_HEADER_SIZE)) => &mut self.wal_frame_buffers,
            _ => return,
        };
        if self.max_cached > cache.len() {
            cache.push(buff);
        }
    }
}

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring"))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
    }

    #[cfg(target_family = "unix")] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(not(any(target_family = "unix", target_os = "android", target_os = "ios")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

mod memory;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
pub mod clock;
mod common;
pub use clock::Clock;
