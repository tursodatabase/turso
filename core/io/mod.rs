use crate::alloc::DynBoxedSlice;
use crate::storage::buffer_pool::ArenaBuffer;
use crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;
use crate::sync::Arc;
use crate::turso_assert;
use crate::{BufferPool, Result};
use bitflags::bitflags;
use cfg_block::cfg_block;
use rand::{Rng, RngCore};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::ptr::NonNull;
use std::sync::LazyLock;
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

    #[cfg(all(target_os = "windows", not(miri)))] {
        mod windows_lock;
        mod windows;
        #[cfg(feature = "fs")]
        pub use windows::WindowsIO;
        pub use windows::WindowsIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))] {
        mod win_iocp;
        #[cfg(feature = "fs")]
        pub use win_iocp::WindowsIOCP;
    }

    #[cfg(any(not(any(target_family = "unix", target_os = "windows")), miri))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

mod memory;
#[cfg(feature = "io_memory_yield")]
mod memory_yield;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
#[cfg(feature = "io_memory_yield")]
pub use memory_yield::MemoryYieldIO;
pub mod clock;
mod common;
mod completions;
pub use clock::Clock;
pub use completions::*;

/// Platform-independent file identity, analogous to SQLite's `struct unixFileId`.
/// On Unix: (st_dev, st_ino). On Windows: (dwVolumeSerialNumber, nFileIndex).
/// On non-filesystem backends: synthetic hash-based identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId {
    pub dev: u64,
    pub ino: u64,
}

impl FileId {
    /// Synthetic identity from a path hash, for backends without real inodes
    /// (MemoryIO, OPFS, simulators).
    pub fn from_path_hash(path: &str) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        FileId {
            dev: 0,
            ino: hasher.finish(),
        }
    }
}

/// Return the OS-level file identity for a path.
#[cfg(unix)]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path)?;
    Ok(FileId {
        dev: m.dev(),
        ino: m.ino(),
    })
}

#[cfg(windows)]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };
    let file = std::fs::File::open(path)?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ret = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    if ret == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(FileId {
        dev: info.dwVolumeSerialNumber as u64,
        ino: (info.nFileIndexHigh as u64) << 32 | info.nFileIndexLow as u64,
    })
}

#[cfg(not(any(unix, windows)))]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    Ok(FileId::from_path_hash(path))
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWalLockKind {
    LinuxOfd,
    ProcessScopedFcntl,
}

pub trait SharedWalMappedRegion: Send + Sync {
    fn ptr(&self) -> NonNull<u8>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
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
                Completion::new_write(move |n| {
                    if let Ok(n) = n {
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

    fn shared_wal_lock_byte(
        &self,
        _offset: u64,
        _exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte locking is not supported for this file".into(),
        ))
    }

    fn shared_wal_try_lock_byte(
        &self,
        _offset: u64,
        _exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<bool> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte locking is not supported for this file".into(),
        ))
    }

    /// Probe whether the caller could hold an exclusive lock without leaving
    /// any lock state changed on return.
    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        let locked = self.shared_wal_try_lock_byte(offset, true, kind)?;
        if locked {
            self.shared_wal_unlock_byte(offset, kind)?;
        }
        Ok(locked)
    }

    /// Probe whether the caller's existing shared lock can become exclusive,
    /// restoring that shared lock before returning.
    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.shared_wal_unlock_byte(offset, kind)?;
        let probe = match self.shared_wal_probe_exclusive_byte(offset, kind) {
            Ok(probe) => probe,
            Err(err) => {
                self.shared_wal_lock_byte(offset, false, kind)?;
                return Err(err);
            }
        };
        self.shared_wal_lock_byte(offset, false, kind)?;
        Ok(probe)
    }

    fn shared_wal_unlock_byte(&self, _offset: u64, _kind: SharedWalLockKind) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte unlocking is not supported for this file".into(),
        ))
    }

    fn shared_wal_set_len(&self, _len: u64) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination resizing is not supported for this file".into(),
        ))
    }

    fn shared_wal_map(&self, _offset: u64, _len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination memory mapping is not supported for this file".into(),
        ))
    }
}

pub struct TempFile {
    pub(crate) file: Arc<dyn File>,
    /// When temp_dir is dropped the folder is deleted
    /// set to None if tempfile allocated in memory (for example, in case of WASM target)
    /// Declared after `file` so the file closes before Windows removes its directory.
    #[allow(dead_code, reason = "held for its Drop side effect")]
    temp_dir: Option<tempfile::TempDir>,
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
                temp_dir: Some(temp_dir),
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
                temp_dir: None,
                file: memory_file,
            })
        }
    }

    /// Creates a TempFile respecting the temp_store setting.
    /// When temp_store is Memory, uses in-memory storage.
    /// When temp_store is Default or File, uses file-based storage when
    /// available. In `no-fs` builds, temp storage always falls back to memory.
    pub fn with_temp_store(io: &Arc<dyn IO>, temp_store: crate::TempStore) -> Result<Self> {
        #[cfg(not(target_family = "wasm"))]
        {
            #[cfg(not(feature = "fs"))]
            {
                let _ = (io, temp_store);
                let memory_io = Arc::new(MemoryIO::new());
                let memory_file =
                    memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
                Ok(TempFile {
                    temp_dir: None,
                    file: memory_file,
                })
            }
            #[cfg(feature = "fs")]
            {
                if matches!(temp_store, crate::TempStore::Memory) {
                    let memory_io = Arc::new(MemoryIO::new());
                    let memory_file =
                        memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
                    return Ok(TempFile {
                        temp_dir: None,
                        file: memory_file,
                    });
                }
                // Fall through to file-based for Default and File modes
                Self::new(io)
            }
        }
        #[cfg(target_family = "wasm")]
        {
            // WASM always uses memory, ignore temp_store setting
            let _ = temp_store;
            Self::new(io)
        }
    }
}

#[cfg(all(test, target_os = "windows", feature = "fs"))]
#[path = "../tests/unit/io/temp_file_tests.rs"]
mod temp_file_tests;

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
        const NoLock = 0b0000100;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)
    }

    // remove_file is used in the sync-engine
    fn remove_file(&self, path: &str) -> Result<()>;

    /// Whether this IO backend can back host-filesystem shared WAL coordination.
    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    fn step(&self) -> Result<()> {
        Ok(())
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        c.iter().for_each(|c| c.abort());
        Ok(())
    }

    /// Drive the IO backend until each completion in `completions` is
    /// `finished()`. Used after `cancel()` (so cancelled ops actually
    /// release their buffers before the caller returns) and after a
    /// single `pwrite`/`pwritev`/`sync` that the caller wants to await
    /// synchronously.
    ///
    /// Unlike a global "drain the ring" barrier, this only waits on the
    /// completions the caller passes in. Other threads can keep
    /// submitting concurrently — their work doesn't extend or interfere
    /// with this call. `Completion::finished()` is monotonic
    /// (`OnceLock`-backed), so the loop will terminate as soon as every
    /// caller-owned completion has had its CQE processed.
    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        while completions.iter().any(|c| !c.finished()) {
            self.step()?;
        }
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

    /// Return the file identity for the given path.
    /// Default uses OS-level metadata; non-filesystem backends override
    /// with synthetic hash-based identity.
    fn file_id(&self, path: &str) -> Result<FileId> {
        get_file_id(path).map_err(|e| {
            crate::LimboError::InternalError(format!(
                "failed to get file identity for '{path}': {e}"
            ))
        })
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

    /// Submit all writes. Returns completions caller must wait on. Each
    /// write is added to `group`, when given, before it is submitted.
    #[inline]
    pub fn submit(self, mut group: Option<&mut CompletionGroup>) -> Result<Vec<Completion>> {
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
            if let Some(group) = group.as_deref_mut() {
                group.add(&c);
            }
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

#[derive(Clone)]
pub enum SharedBufferData {
    Full(Arc<DynBoxedSlice<u8>>),
    View(SharedBufferView),
}

#[derive(Clone)]
pub struct SharedBufferView {
    data: Arc<DynBoxedSlice<u8>>,
    start: usize,
}

impl SharedBufferView {
    fn new(data: Arc<DynBoxedSlice<u8>>, start: usize) -> Self {
        assert!(
            start <= data.len(),
            "SharedBufferData::new_view: start ({start}) > data.len() ({})",
            data.len()
        );
        Self { data, start }
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data.as_ref().as_ref()[self.start..]
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ref().as_ptr().add(self.start) }
    }
}

impl SharedBufferData {
    pub fn new(data: Arc<DynBoxedSlice<u8>>) -> Self {
        Self::Full(data)
    }

    pub fn new_view(data: Arc<DynBoxedSlice<u8>>, start: usize) -> Self {
        Self::View(SharedBufferView::new(data, start))
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Full(data) => data.len(),
            Self::View(view) => view.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Full(data) => data.as_ref().as_ref(),
            Self::View(view) => view.as_slice(),
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Full(data) => data.as_ref().as_ptr(),
            Self::View(view) => view.as_ptr(),
        }
    }
}

pub enum Buffer {
    Heap(BufferData),
    Shared(SharedBufferData),
    /// A heap buffer with a logical start offset: only `data[start..]` is
    /// exposed via [`Buffer::as_slice`] / [`Buffer::len`]. Used to skip a
    /// pre-allocated prefix without shifting bytes in memory before I/O.
    HeapView {
        data: BufferData,
        start: usize,
    },
    Pooled(ArenaBuffer),
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "Pooled(len={})", p.logical_len()),
            Self::Heap(buf) => write!(f, "{buf:?}: {}", buf.len()),
            Self::Shared(buf) => write!(f, "Shared(len={})", buf.len()),
            Self::HeapView { data, start } => {
                write!(
                    f,
                    "HeapView({start}..{}, view_len={})",
                    data.len(),
                    data.len() - start
                )
            }
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        match self {
            Self::Heap(buf) | Self::HeapView { data: buf, .. } => {
                let underlying_len = buf.len();
                TEMP_BUFFER_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    // take ownership of the buffer by swapping it with a dummy
                    let buffer = std::mem::replace(buf, Pin::new(vec![].into_boxed_slice()));
                    cache.return_buffer(buffer, underlying_len);
                });
            }
            Self::Pooled(_) | Self::Shared(_) => {}
        }
    }
}

impl Buffer {
    pub fn new(data: Vec<u8>) -> Self {
        tracing::trace!("buffer::new({:?})", data);
        Self::Heap(Pin::new(data.into_boxed_slice()))
    }

    pub fn new_shared(data: Arc<DynBoxedSlice<u8>>) -> Self {
        Self::Shared(SharedBufferData::new(data))
    }

    pub fn new_shared_data(data: SharedBufferData) -> Self {
        Self::Shared(data)
    }

    /// Wraps `data` so that only bytes `[start..]` are visible via
    /// [`Buffer::as_slice`] / [`Buffer::len`]. The skipped prefix lives in
    /// memory but is never read by the I/O layer — useful when a caller
    /// has pre-allocated optional framing room at the front of a buffer
    /// and wants to elide it on a particular write without a memmove.
    pub fn new_with_start(data: Vec<u8>, start: usize) -> Self {
        assert!(
            start <= data.len(),
            "Buffer::new_with_start: start ({start}) > data.len() ({})",
            data.len()
        );
        Self::HeapView {
            data: Pin::new(data.into_boxed_slice()),
            start,
        }
    }

    /// Returns the index of the underlying `Arena` if it was registered with
    /// io_uring. Only for use with `UringIO` backend.
    pub fn fixed_id(&self) -> Option<u32> {
        match self {
            Self::Heap(..) | Self::HeapView { .. } | Self::Shared(..) => None,
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
            Self::Shared(buf) => buf.len(),
            Self::HeapView { data, start } => data.len() - *start,
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
            Self::Shared(buf) => buf.as_slice(),
            Self::HeapView { data, start } => {
                // SAFETY: `start` was bounds-checked at construction; the buffer
                // is valid for the lifetime of the returned slice.
                unsafe {
                    std::slice::from_raw_parts(data.as_ptr().add(*start), data.len() - *start)
                }
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
            Self::Shared(buf) => buf.as_ptr(),
            Self::HeapView { data, start } => unsafe { data.as_ptr().add(*start) },
            Self::Pooled(buf) => buf.as_ptr(),
        }
    }
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr() as *mut u8,
            Self::Shared(_) => panic!("Buffer::Shared is immutable"),
            Self::HeapView { data, start } => unsafe { (data.as_ptr() as *mut u8).add(*start) },
            Self::Pooled(buf) => buf.as_ptr() as *mut u8,
        }
    }

    #[inline]
    pub fn is_pooled(&self) -> bool {
        matches!(self, Self::Pooled(..))
    }

    #[inline]
    pub fn is_heap(&self) -> bool {
        matches!(self, Self::Heap(..) | Self::HeapView { .. })
    }
}

crate::thread::thread_local! {
    /// thread local cache to re-use temporary buffers to prevent churn when pool overflows
    pub static TEMP_BUFFER_CACHE: RefCell<TempBufferCache> = RefCell::new(TempBufferCache::new());
}

#[cfg(test)]
#[path = "../tests/unit/io/buffer_tests.rs"]
mod buffer_tests;

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

// Runtime-registrable Rust IO backends, resolved by `Database::io_for_vfs`.
#[allow(clippy::type_complexity)]
static IO_REGISTRY: LazyLock<parking_lot::Mutex<HashMap<String, Arc<dyn IO>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

const BUILTIN_VFS_NAMES: &[&str] = &["memory", "syscall", "io_uring", "experimental_win_iocp"];

/// Register a named Rust IO backend.
///
/// Once registered, it can be used via [`Database::io_for_vfs`] or through
/// any language binding's `vfs=` parameter (Go DSN, Python kwarg, etc.).
///
/// Re-registering the same name replaces the previous backend. Registered
/// names take precedence over C VFS extensions and built-in backends
/// (`"memory"`, `"syscall"`, `"io_uring"`), so registering a built-in name
/// will shadow the default implementation.
///
/// # Errors
///
/// Returns [`LimboError::InvalidArgument`] if `name` is empty.
pub fn register_io(name: &str, io: Arc<dyn IO>) -> crate::Result<()> {
    if name.is_empty() {
        return Err(crate::LimboError::InvalidArgument(
            "IO backend name must not be empty".into(),
        ));
    }
    if BUILTIN_VFS_NAMES.contains(&name) {
        tracing::warn!("registered IO backend \"{name}\" shadows a built-in VFS");
    }
    IO_REGISTRY.lock().insert(name.to_string(), io);
    Ok(())
}

/// Remove a registered Rust IO backend by name.
///
/// Returns `true` if an entry was removed, `false` if the name was not found.
pub fn unregister_io(name: &str) -> bool {
    IO_REGISTRY.lock().remove(name).is_some()
}

/// Look up a registered Rust IO backend by name.
pub fn get_registered_io(name: &str) -> Option<Arc<dyn IO>> {
    IO_REGISTRY.lock().get(name).cloned()
}

/// List all registered Rust IO backend names.
pub fn list_registered_io() -> Vec<String> {
    IO_REGISTRY.lock().keys().cloned().collect()
}

#[cfg(test)]
#[path = "../tests/unit/io/io_registry_tests.rs"]
mod io_registry_tests;

#[cfg(all(shuttle, test))]
#[path = "../tests/unit/io/shuttle_tests.rs"]
mod shuttle_tests;
