use crate::storage::buffer_pool::ArenaBuffer;
use crate::Result;
use cfg_block::cfg_block;
use std::{cell::UnsafeCell, fmt::Debug, pin::Pin, sync::Arc};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Completion) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: Arc<Buffer>, c: Completion) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn size(&self) -> Result<u64>;
}

#[derive(Copy, Clone)]
pub enum OpenFlags {
    None,
    Create,
}

impl OpenFlags {
    pub fn to_flags(&self) -> i32 {
        match self {
            Self::None => 0,
            Self::Create => 1,
        }
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn run_once(&self) -> Result<()>;

    fn generate_random_number(&self) -> i64;

    fn get_memory_io(&self) -> Arc<MemoryIO>;

    /// IO_URING only. noop for other implementations.
    fn register_buffer(&self, _arena_id: u32, _iovec: (*const u8, usize)) -> Result<()> {
        Ok(())
    }
}

pub type Complete = dyn Fn(Arc<Buffer>, i32);
pub type WriteComplete = dyn Fn(i32);
pub type SyncComplete = dyn Fn(i32);

pub enum Completion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<Complete>,
}

impl Completion {
    pub fn complete(&self, result: i32) {
        match self {
            Self::Read(r) => r.complete(result),
            Self::Write(w) => w.complete(result),
            Self::Sync(s) => s.complete(result), // fix
        }
    }
    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(i32) + 'static,
    {
        Self::Write(WriteCompletion::new(Box::new(complete)))
    }
    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Arc<Buffer>, i32) + 'static,
    {
        Self::Read(ReadCompletion::new(buf, Box::new(complete)))
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        match self {
            Self::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<Complete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> Arc<Buffer> {
        self.buf.clone()
    }

    pub fn buf_mut(&self) -> &mut [u8] {
        self.buf.slice_mut()
    }

    pub fn complete(&self, res: i32) {
        (self.complete)(self.buf.clone(), res);
    }
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn complete(&self, bytes_written: i32) {
        (self.complete)(bytes_written);
    }
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self { complete }
    }

    pub fn complete(&self, res: i32) {
        (self.complete)(res);
    }
}
type BuffData = Pin<Box<[u8]>>;

#[derive(Debug)]
pub enum Buffer {
    Heap(UnsafeCell<BuffData>),
    Pooled(UnsafeCell<ArenaBuffer>),
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Self::Pooled(slice) = self {
            unsafe { &*slice.get() }.mark_free();
        }
    }
}

impl Buffer {
    pub fn new_heap(len: usize) -> Arc<Self> {
        let out = Pin::new(vec![0u8; len].into_boxed_slice());
        tracing::trace!("new_heap buffer: len: {}", len);
        Buffer::Heap(UnsafeCell::new(out)).into()
    }

    pub fn new_pooled(page: ArenaBuffer) -> Arc<Self> {
        tracing::trace!("new_pooled: page: {:?}, len: {}", page, page.len());
        Buffer::Pooled(UnsafeCell::new(page)).into()
    }

    pub fn arena_id(&self) -> Option<u32> {
        match self {
            Self::Heap(_) => None,
            Self::Pooled(slice) => Some(unsafe { &*slice.get() }.io_id()),
        }
    }
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Heap(data) => unsafe { &*data.get() }.as_ptr(),
            Self::Pooled(slice) => unsafe { &*slice.get() }.as_ptr(),
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Self::Heap(data) => unsafe { &mut *data.get() }.as_mut_ptr(),
            Self::Pooled(slice) => unsafe { &mut *slice.get() }.as_mut_ptr(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(data) => unsafe { &*data.get() }.len(),
            Self::Pooled(slice) => unsafe { &*slice.get() }.len(),
        }
    }

    #[inline]
    pub fn slice(&self) -> &[u8] {
        unsafe {
            match self {
                Self::Heap(b) => &(*b.get()),
                Self::Pooled(s) => &(*s.get())[..(*s.get()).len()],
            }
        }
    }

    /// Mutable view – caller must guarantee exclusivity!
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn slice_mut(&self) -> &mut [u8] {
        match self {
            Self::Heap(b) => unsafe { &mut (*b.get()) },
            Self::Pooled(s) => unsafe { &mut (*s.get())[..(*s.get()).len()] },
        }
    }

    /// Deep copy of underyling data. avoid using this if possible.
    pub fn deep_clone(&self) -> Self {
        let mut out = Pin::new(vec![0u8; self.len()].into_boxed_slice());
        out.as_mut().copy_from_slice(self.slice());
        Buffer::Heap(UnsafeCell::new(out))
    }
}

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring"))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as SyscallIO;
        pub use unix::UnixIO as PlatformIO;
    }

    #[cfg(any(all(target_os = "linux",not(feature = "io_uring")), target_os = "macos"))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(target_os = "windows")] {
        mod windows;
        pub use windows::WindowsIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))] {
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
