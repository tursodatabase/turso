use crate::storage::buffer_pool::ArenaBuffer;
use crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;
use crate::sync::Arc;
use crate::turso_assert;
use crate::{BufferPool, Result};
use bitflags::bitflags;
use cfg_block::cfg_block;
use rand::{Rng, RngCore};
use std::cell::RefCell;
use std::fmt;
use std::ptr::NonNull;
use std::{fmt::Debug, pin::Pin};
use turso_macros::AtomicEnum;

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
    }

    #[cfg(all(target_family = "unix", not(miri)))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))] {
        mod win_iocp;
        #[cfg(feature = "fs")]
        pub use win_iocp::WindowsIOCP;
    }

    #[cfg(any(not(any(target_family = "unix", target_os = "android", target_os = "ios")), miri))] {
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
mod completions;
pub use clock::Clock;
pub use completions::*;

/// Controls which sync mechanism to use for durability.
/// `FullFsync` only has effect on Apple platforms (uses F_FULLFSYNC fcntl).
/// On other platforms, both variants behave the same (regular fsync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, AtomicEnum)]
pub enum FileSyncType {
    /// Regular fsync - flushes to disk but may not flush disk write cache on macOS.
    Fsync,
    /// Full fsync - on macOS uses F_FULLFSYNC to flush disk write cache.
    /// On other platforms, behaves the same as Fsync.
    FullFsync,
}

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion>;
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion>;
    /// Sync file data&metadata to disk.
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion>;
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        use crate::sync::atomic::{AtomicUsize, Ordering};
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

    /// Optional method implemented by the IO which supports "partial" files (e.g. file with "holes")
    /// This method is used in sync engine only for now (in partial sync mode) and never used in the core database code
    ///
    /// The hole is the contiguous file region which is not allocated by the file-system
    /// If there is a single byte which is allocated within a given range - method must return false in this case
    // todo: need to add custom completion type?
    fn has_hole(&self, _pos: usize, _len: usize) -> Result<bool> {
        panic!("has_hole is not supported for the given IO implementation")
    }
    /// Optional method implemented by the IO which supports "partial" files (e.g. file with "holes")
    /// This method is used in sync engine only for now (in partial sync mode) and never used in the core database code
    // todo: need to add custom completion type?
    fn punch_hole(&self, _pos: usize, _len: usize) -> Result<()> {
        panic!("punch_hole is not supported for the given IO implementation")
    }
}

pub struct TempFile {
    /// When temp_dir is dropped the folder is deleted
    /// set to None if tempfile allocated in memory (for example, in case of WASM target)
    _temp_dir: Option<tempfile::TempDir>,
    pub(crate) file: Arc<dyn File>,
}

impl TempFile {
    pub fn new(io: &Arc<dyn IO>) -> Result<Self> {
        #[cfg(not(target_family = "wasm"))]
        {
            let temp_dir = tempfile::tempdir().map_err(|e| crate::error::io_error(e, "tempdir"))?;
            let chunk_file_path = temp_dir.as_ref().join("tursodb_temp_file");
            let chunk_file_path_str = chunk_file_path.to_str().ok_or_else(|| {
                crate::LimboError::InternalError("temp file path is not valid UTF-8".to_string())
            })?;
            let chunk_file = io.open_file(chunk_file_path_str, OpenFlags::Create, false)?;
            Ok(TempFile {
                _temp_dir: Some(temp_dir),
                file: chunk_file.clone(),
            })
        }
        // on WASM in browser we do not support temp files (as we pre-register db files in advance and can't easily create a new one)
        // so, for now, we use in-memory IO for tempfiles in WASM
        #[cfg(target_family = "wasm")]
        {
            use crate::MemoryIO;

            let memory_io = Arc::new(MemoryIO::new());
            let memory_file = memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
            Ok(TempFile {
                _temp_dir: None,
                file: memory_file,
            })
        }
    }

    /// Creates a TempFile respecting the temp_store setting.
    /// When temp_store is Memory, uses in-memory storage.
    /// When temp_store is Default or File, uses file-based storage.
    pub fn with_temp_store(io: &Arc<dyn IO>, temp_store: crate::TempStore) -> Result<Self> {
        #[cfg(not(target_family = "wasm"))]
        {
            if matches!(temp_store, crate::TempStore::Memory) {
                let memory_io = Arc::new(MemoryIO::new());
                let memory_file =
                    memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
                return Ok(TempFile {
                    _temp_dir: None,
                    file: memory_file,
                });
            }
            // Fall through to file-based for Default and File modes
            Self::new(io)
        }
        #[cfg(target_family = "wasm")]
        {
            // WASM always uses memory, ignore temp_store setting
            let _ = temp_store;
            Self::new(io)
        }
    }
}

impl core::ops::Deref for TempFile {
    type Target = Arc<dyn File>;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

// OpenFlags is a newtype over i32, which is inherently Send+Sync.
// The assertion below verifies this at compile time.
crate::assert::assert_send_sync!(OpenFlags);

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
        if let Some(inner) = &c.inner {
            if let Some(Some(err)) = inner.result.get().copied() {
                return Err(err.into());
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        rand::rng().random()
    }

    /// Fill `dest` with random data.
    fn fill_bytes(&self, dest: &mut [u8]) {
        rand::rng().fill_bytes(dest);
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }

    fn register_fixed_buffer(&self, _ptr: NonNull<u8>, _len: usize) -> Result<u32> {
        Err(crate::LimboError::InternalError(
            "unsupported operation".to_string(),
        ))
    }

    /// Yield the current thread to the scheduler.
    /// Used for backoff in contended lock acquisition.
    fn yield_now(&self) {
        crate::thread::yield_now();
    }

    /// Sleep for the specified duration.
    /// Used for progressive backoff in contended lock acquisition.
    fn sleep(&self, duration: std::time::Duration) {
        crate::thread::sleep(duration);
    }
}

/// Batches multiple vectored writes for submission.
pub struct WriteBatch<'a> {
    file: Arc<dyn File>,
    ops: Vec<WriteOp<'a>>,
}

struct WriteOp<'a> {
    pos: u64,
    bufs: &'a [Arc<Buffer>],
}

impl<'a> WriteBatch<'a> {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {
            file,
            ops: Vec::new(),
        }
    }

    #[inline]
    pub fn writev(&mut self, pos: u64, bufs: &'a [Arc<Buffer>]) {
        if !bufs.is_empty() {
            self.ops.push(WriteOp { pos, bufs });
        }
    }

    /// Total bytes across all operations.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.ops
            .iter()
            .map(|op| op.bufs.iter().map(|b| b.len()).sum::<usize>())
            .sum()
    }

    /// Submit all writes. Returns completions caller must wait on.
    #[inline]
    pub fn submit(self) -> Result<Vec<Completion>> {
        let mut completions = Vec::with_capacity(self.ops.len());
        for WriteOp { pos, bufs } in self.ops {
            let total_len = bufs.iter().map(|b| b.len()).sum::<usize>() as i32;
            let c = Completion::new_write(move |res| {
                let Ok(bytes_written) = res else {
                    return;
                };
                turso_assert!(
                    bytes_written == total_len,
                    "pwritev wrote {bytes_written} bytes, expected {total_len}"
                );
            });
            completions.push(self.file.pwritev(pos, bufs.to_vec(), c)?);
        }
        Ok(completions)
    }

    /// Returns the file for fsync after writes complete.
    #[inline]
    pub const fn file(&self) -> &Arc<dyn File> {
        &self.file
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

    #[inline]
    pub fn is_pooled(&self) -> bool {
        matches!(self, Self::Pooled(..))
    }

    #[inline]
    pub fn is_heap(&self) -> bool {
        matches!(self, Self::Heap(..))
    }
}

crate::thread::thread_local! {
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

#[cfg(all(shuttle, test))]
mod shuttle_tests {
    use std::path::PathBuf;

    use super::*;
    use crate::io::{Buffer, Completion, OpenFlags, IO};
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::Arc;
    use crate::thread;

    /// Factory trait for creating IO implementations in tests.
    /// Allows the same test logic to run against different IO backends.
    trait IOFactory: Send + Sync + 'static {
        fn create(&self) -> Arc<dyn IO>;
        /// Returns a unique temp directory path for this factory instance.
        fn temp_dir(&self) -> PathBuf;
    }

    struct MemoryIOFactory {
        id: u64,
    }

    impl MemoryIOFactory {
        fn new() -> Self {
            use crate::sync::atomic::AtomicU64;
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            Self {
                id: COUNTER.fetch_add(1, Ordering::SeqCst),
            }
        }
    }

    impl IOFactory for MemoryIOFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(MemoryIO::new())
        }
        fn temp_dir(&self) -> PathBuf {
            format!("mem_{}", self.id).into()
        }
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    struct PlatformIOFactory {
        temp_dir: tempfile::TempDir,
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    impl PlatformIOFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().unwrap(),
            }
        }
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    impl IOFactory for PlatformIOFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(PlatformIO::new().unwrap())
        }
        fn temp_dir(&self) -> PathBuf {
            self.temp_dir.path().to_path_buf()
        }
    }

    #[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
    struct UringIOFactory {
        temp_dir: tempfile::TempDir,
    }

    #[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
    impl UringIOFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().unwrap(),
            }
        }
    }

    #[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
    impl IOFactory for UringIOFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(UringIO::new().unwrap())
        }
        fn temp_dir(&self) -> PathBuf {
            self.temp_dir.path().to_path_buf()
        }
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    struct WinIOCPFactory {
        temp_dir: tempfile::TempDir,
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    impl WinIOCPFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().unwrap(),
            }
        }
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    impl IOFactory for WinIOCPFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(WindowsIOCP::new().unwrap())
        }
        fn temp_dir(&self) -> PathBuf {
            self.temp_dir.path().to_path_buf()
        }
    }

    /// Macro to generate shuttle tests for all IO implementations.
    /// Creates a test for MemoryIO, and conditionally for PlatformIO and UringIO.
    macro_rules! shuttle_io_test {
        ($test_name:ident, $test_impl:ident) => {
            paste::paste! {
                #[test]
                fn [<shuttle_ $test_name _memory>]() {
                    shuttle::check_random(|| $test_impl(MemoryIOFactory::new()), 1000);
                }

                #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
                #[test]
                fn [<shuttle_ $test_name _platform>]() {
                    shuttle::check_random(|| $test_impl(PlatformIOFactory::new()), 1000);
                }

                #[cfg(all(target_os = "linux", feature = "io_uring", feature = "fs", not(miri)))]
                #[test]
                fn [<shuttle_ $test_name _uring>]() {
                    shuttle::check_random(|| $test_impl(UringIOFactory::new()), 1000);
                }

                #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", feature = "fs", not(miri)))]
                #[test]
                fn [<shuttle_ $test_name _win_iocp>]() {
                    shuttle::check_random(|| $test_impl(WinIOCPFactory::new()), 1000);
                }

            }
        };
    }

    /// Helper to wait for a completion synchronously and assert it succeeded.
    fn wait_completion_ok(io: &dyn IO, c: &Completion) {
        io.wait_for_completion(c.clone()).unwrap();
        assert!(c.succeeded(), "completion failed: {:?}", c.get_error());
        assert!(!c.failed());
        assert!(c.finished());
        assert!(c.get_error().is_none());
    }

    /// Helper to wait for a completion synchronously without asserting success.
    #[allow(dead_code)]
    fn wait_completion(io: &dyn IO, c: &Completion) {
        io.wait_for_completion(c.clone()).unwrap();
        assert!(c.finished());
    }

    /// Test concurrent file creation from multiple threads.
    fn test_concurrent_file_creation_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();
        let mut handles = vec![];
        const NUM_THREADS: usize = 3;

        for i in 0..NUM_THREADS {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("test_file_{}.db", i));
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                    .unwrap();
                assert!(file.size().unwrap() == 0);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(concurrent_file_creation, test_concurrent_file_creation_impl);

    /// Test concurrent writes to different offsets in the same file.
    fn test_concurrent_writes_different_offsets_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];
        const NUM_THREADS: usize = 3;

        for i in 0..NUM_THREADS {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let data = vec![i as u8; 100];
                let buf = Arc::new(Buffer::new(data));
                let pos = (i * 100) as u64;

                let c = Completion::new_write(|_| {});
                let c = file.pwrite(pos, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify file size accounts for all writes
        let expected_size = (NUM_THREADS * 100) as u64;
        assert_eq!(file.size().unwrap(), expected_size);

        // Read back and verify each segment contains correct data
        for i in 0..NUM_THREADS {
            let read_buf = Arc::new(Buffer::new_temporary(100));
            let pos = (i * 100) as u64;
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(pos, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            let expected = vec![i as u8; 100];
            assert_eq!(
                read_buf.as_slice(),
                expected.as_slice(),
                "data mismatch at offset {}",
                pos
            );
        }
    }

    shuttle_io_test!(
        concurrent_writes_different_offsets,
        test_concurrent_writes_different_offsets_impl
    );

    /// Test concurrent reads and writes to the same file.
    fn test_concurrent_read_write_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // First write some initial data
        let initial_data = vec![0xAA; 1000];
        let buf = Arc::new(Buffer::new(initial_data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn readers
        for _ in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let c = Completion::new_read(read_buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // All bytes read should be 0xAA (initial data at offset 0)
                assert!(
                    read_buf.as_slice().iter().all(|&b| b == 0xAA),
                    "read buffer should contain initial data 0xAA"
                );
            }));
        }

        // Spawn a writer
        {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let data = vec![0xBB; 100];
                let buf = Arc::new(Buffer::new(data));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(500, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify the write at offset 500 succeeded
        let read_buf = Arc::new(Buffer::new_temporary(100));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(500, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);
        assert!(
            read_buf.as_slice().iter().all(|&b| b == 0xBB),
            "data at offset 500 should be 0xBB"
        );
    }

    shuttle_io_test!(concurrent_read_write, test_concurrent_read_write_impl);

    /// Test that completion callbacks are invoked correctly under concurrency.
    fn test_completion_callbacks_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        const NUM_WRITES: usize = 3;

        for i in 0..NUM_WRITES {
            let file = file.clone();
            let io = io.clone();
            let count = callback_count.clone();
            handles.push(thread::spawn(move || {
                let data = vec![i as u8; 50];
                let buf = Arc::new(Buffer::new(data));
                let count_clone = count.clone();
                let c = Completion::new_write(move |res| {
                    assert!(res.is_ok());
                    count_clone.fetch_add(1, Ordering::SeqCst);
                });
                let c = file.pwrite((i * 50) as u64, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(callback_count.load(Ordering::SeqCst), NUM_WRITES);
    }

    shuttle_io_test!(
        completion_callbacks_concurrent,
        test_completion_callbacks_concurrent_impl
    );

    /// Test concurrent truncate operations.
    fn test_concurrent_truncate_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write initial data
        let initial = vec![0xFF; 5000];
        let buf = Arc::new(Buffer::new(initial));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn threads that truncate to different sizes
        for i in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let truncate_size = ((i + 1) * 1000) as u64;
                let c = Completion::new_trunc(|_| {});
                let c = file.truncate(truncate_size, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Size should be one of the truncate values
        let final_size = file.size().unwrap();
        assert!(final_size == 1000 || final_size == 2000 || final_size == 3000);
    }

    shuttle_io_test!(concurrent_truncate, test_concurrent_truncate_impl);

    /// Test pwritev with concurrent reads.
    fn test_pwritev_with_concurrent_reads_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write initial data so reads have something to return
        let initial = vec![0x11; 2000];
        let buf = Arc::new(Buffer::new(initial));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn a pwritev thread
        {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let bufs = vec![
                    Arc::new(Buffer::new(vec![0x22; 100])),
                    Arc::new(Buffer::new(vec![0x33; 100])),
                    Arc::new(Buffer::new(vec![0x44; 100])),
                ];
                let c = Completion::new_write(|_| {});
                let c = file.pwritev(0, bufs, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        // Spawn reader threads
        for _ in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let buf = Arc::new(Buffer::new_temporary(100));
                let c = Completion::new_read(buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Data should be either initial (0x11) or from pwritev (0x22)
                // depending on race ordering
                let first_byte = buf.as_slice()[0];
                assert!(
                    first_byte == 0x11 || first_byte == 0x22,
                    "first byte should be 0x11 or 0x22, got {:#x}",
                    first_byte
                );
                // All 100 bytes should be consistent
                assert!(
                    buf.as_slice().iter().all(|&b| b == first_byte),
                    "all bytes should be the same value"
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all threads complete, verify pwritev data is present
        let read_buf = Arc::new(Buffer::new_temporary(300));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(0, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        // Should have 0x22 for first 100, 0x33 for next 100, 0x44 for last 100
        assert!(
            read_buf.as_slice()[..100].iter().all(|&b| b == 0x22),
            "bytes 0-99 should be 0x22"
        );
        assert!(
            read_buf.as_slice()[100..200].iter().all(|&b| b == 0x33),
            "bytes 100-199 should be 0x33"
        );
        assert!(
            read_buf.as_slice()[200..300].iter().all(|&b| b == 0x44),
            "bytes 200-299 should be 0x44"
        );
    }

    shuttle_io_test!(
        pwritev_with_concurrent_reads,
        test_pwritev_with_concurrent_reads_impl
    );

    /// Test concurrent access to multiple files.
    fn test_concurrent_multifile_access_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        let mut handles = vec![];
        const NUM_FILES: usize = 3;

        for i in 0..NUM_FILES {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("file_{}.db", i));
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                    .unwrap();

                // Write to file
                let data = vec![i as u8; 200];
                let buf = Arc::new(Buffer::new(data.clone()));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Read back and verify
                let read_buf = Arc::new(Buffer::new_temporary(200));
                let c = Completion::new_read(read_buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                assert_eq!(read_buf.as_slice(), data.as_slice());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        concurrent_multifile_access,
        test_concurrent_multifile_access_impl
    );

    /// Test file locking under concurrent access.
    fn test_file_locking_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        // Multiple threads try to lock/unlock
        for _ in 0..3 {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Exclusive lock
                file.lock_file(true).unwrap();
                thread::yield_now();
                file.unlock_file().unwrap();

                // Shared lock
                file.lock_file(false).unwrap();
                thread::yield_now();
                file.unlock_file().unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(file_locking_concurrent, test_file_locking_concurrent_impl);

    /// Test reading past end of file returns zero bytes.
    fn test_read_past_eof_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write 100 bytes
        let data = vec![0xAA; 100];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple threads try to read past EOF
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let bytes_read = Arc::new(AtomicUsize::new(999));
                let bytes_read_clone = bytes_read.clone();
                let c = Completion::new_read(read_buf, move |res| {
                    if let Ok((_, n)) = res {
                        bytes_read_clone.store(n as usize, Ordering::SeqCst);
                    }
                    None
                });
                let c = file.pread(200, c).unwrap(); // Past EOF
                                                     // Reading past EOF succeeds with 0 bytes read
                wait_completion_ok(io.as_ref(), &c);
                assert_eq!(bytes_read.load(Ordering::SeqCst), 0);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(read_past_eof, test_read_past_eof_impl);

    /// Test empty write operations.
    fn test_empty_write_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                // Empty buffer write
                let buf = Arc::new(Buffer::new(vec![]));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(file.size().unwrap(), 0);
    }

    shuttle_io_test!(empty_write, test_empty_write_impl);

    /// Test sync operations under concurrency.
    fn test_concurrent_sync_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write some data first
        let data = vec![0xFF; 1000];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple sync calls concurrently
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let c = Completion::new_sync(|_| {});
                let c = file.sync(c, FileSyncType::Fsync).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(concurrent_sync, test_concurrent_sync_impl);

    /// Test concurrent open of the same file returns same file instance.
    fn test_concurrent_open_same_file_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("shared.db");

        // Create file first
        let _ = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let io = io.clone();
            let path = path.clone();
            handles.push(thread::spawn(move || {
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::None, false)
                    .unwrap();
                thread::yield_now();
                // Write a byte to prove we got a valid file
                let buf = Arc::new(Buffer::new(vec![0xAA]));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        concurrent_open_same_file,
        test_concurrent_open_same_file_impl
    );

    /// Test file removal while concurrent access.
    fn test_file_remove_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        // Create multiple files
        for i in 0..3 {
            let path = base.join(format!("remove_{}.db", i));
            let file = io
                .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                .unwrap();
            let buf = Arc::new(Buffer::new(vec![0xFF; 100]));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }

        let mut handles = vec![];

        // Remove files concurrently
        for i in 0..3 {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("remove_{}.db", i));
                io.remove_file(path.to_str().unwrap()).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(file_remove_concurrent, test_file_remove_concurrent_impl);

    /// Test write spanning multiple internal pages.
    fn test_large_write_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        // Multiple threads write large buffers that span multiple pages
        for i in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                // Write 10000 bytes (spans multiple 4096-byte pages)
                let data = vec![(i + 1) as u8; 10000];
                let buf = Arc::new(Buffer::new(data));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite((i * 10000) as u64, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(file.size().unwrap(), 20000);

        // Read back and verify each segment contains correct data
        for i in 0..2 {
            let read_buf = Arc::new(Buffer::new_temporary(10000));
            let pos = (i * 10000) as u64;
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(pos, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            let expected_byte = (i + 1) as u8;
            assert!(
                read_buf.as_slice().iter().all(|&b| b == expected_byte),
                "all bytes at offset {} should be {:#x}",
                pos,
                expected_byte
            );
        }
    }

    shuttle_io_test!(large_write_concurrent, test_large_write_concurrent_impl);

    /// Test has_hole and punch_hole under concurrency.
    /// Note: Only runs on MemoryIO as hole operations are not supported on all backends.
    fn test_hole_operations_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write data spanning multiple pages (at least 3 pages = 12288 bytes)
        let data = vec![0xFF; 16384];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Thread 1: punch holes
        {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Punch hole in middle page (page-aligned)
                file.punch_hole(4096, 4096).unwrap();
            }));
        }

        // Thread 2: check for holes
        {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Check various regions
                let has_hole = file.has_hole(0, 4096).unwrap();
                assert!(!has_hole);
                let _ = file.has_hole(4096, 4096).unwrap();
                let has_hole = file.has_hole(8192, 4096).unwrap();
                assert!(!has_hole);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    // hole_operations only runs on MemoryIO since not all backends support holes
    #[test]
    fn shuttle_hole_operations_concurrent_memory() {
        shuttle::check_random(
            || test_hole_operations_concurrent_impl(MemoryIOFactory::new()),
            1000,
        );
    }

    /// Test that partial reads work correctly at EOF boundary.
    fn test_partial_read_at_eof_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write exactly 150 bytes
        let data = vec![0xAB; 150];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple threads try to read 100 bytes starting at offset 100
        // Should only get 50 bytes back
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let bytes_read = Arc::new(AtomicUsize::new(999));
                let bytes_read_clone = bytes_read.clone();
                let c = Completion::new_read(read_buf.clone(), move |res| {
                    if let Ok((_, n)) = res {
                        bytes_read_clone.store(n as usize, Ordering::SeqCst);
                    }
                    None
                });
                let c = file.pread(100, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Should read exactly 50 bytes (150 - 100)
                assert_eq!(bytes_read.load(Ordering::SeqCst), 50);
                // Verify the bytes read are correct
                assert_eq!(&read_buf.as_slice()[..50], &[0xAB; 50]);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(partial_read_at_eof, test_partial_read_at_eof_impl);

    /// Test empty pwritev.
    fn test_empty_pwritev_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let bufs: Vec<Arc<Buffer>> = vec![];
                let c = Completion::new_write(|_| {});
                let c = file.pwritev(0, bufs, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(empty_pwritev, test_empty_pwritev_impl);

    /// Test error case: opening non-existent file without Create flag.
    fn test_open_nonexistent_without_create_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        let mut handles = vec![];

        for i in 0..3 {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("nonexistent_{}.db", i));
                let result = io.open_file(path.to_str().unwrap(), OpenFlags::None, false);
                assert!(result.is_err());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        open_nonexistent_without_create,
        test_open_nonexistent_without_create_impl
    );

    /// Test concurrent writes to overlapping regions.
    /// This tests that the final state is consistent (one of the writes wins).
    fn test_concurrent_overlapping_writes_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let write_complete = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads write to the same offset
        for i in 0..3 {
            let file = file.clone();
            let io = io.clone();
            let write_complete = write_complete.clone();
            handles.push(thread::spawn(move || {
                let data = vec![(i + 1) as u8; 100];
                let buf = Arc::new(Buffer::new(data));
                let write_complete_clone = write_complete.clone();
                let c = Completion::new_write(move |_| {
                    write_complete_clone.fetch_add(1, Ordering::SeqCst);
                });
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All writes should have completed
        assert_eq!(write_complete.load(Ordering::SeqCst), 3);

        // Read back and verify we got one of the written values
        let read_buf = Arc::new(Buffer::new_temporary(100));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(0, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let first_byte = read_buf.as_slice()[0];
        assert!(first_byte == 1 || first_byte == 2 || first_byte == 3);

        // All 100 bytes should be the same value
        assert!(read_buf.as_slice().iter().all(|&b| b == first_byte));
    }

    shuttle_io_test!(
        concurrent_overlapping_writes,
        test_concurrent_overlapping_writes_impl
    );
}
