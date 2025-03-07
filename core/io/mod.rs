use crate::Result;
use cfg_block::cfg_block;
use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;
use std::{fmt::Debug, mem::ManuallyDrop, pin::Pin, rc::Rc};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Completion) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: IOBuff, c: Completion) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn size(&self) -> Result<u64>;
}

#[derive(Debug, Clone, Copy)]
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

pub trait IO: Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn run_once(&self) -> Result<()>;

    fn generate_random_number(&self) -> i64;

    fn get_current_time(&self) -> String;
}

pub type Complete = dyn Fn(IOBuff);
pub type WriteComplete = dyn Fn(i32);
pub type SyncComplete = dyn Fn(i32);

pub enum Completion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
}

pub struct ReadCompletion {
    pub buf: IOBuff,
    pub complete: Box<Complete>,
}

impl Completion {
    pub fn complete(&self, result: i32) {
        match self {
            Self::Read(r) => r.complete(),
            Self::Write(w) => w.complete(result),
            Self::Sync(s) => s.complete(result), // fix
        }
    }

    // for io_uring, to ensure the buffer lives
    pub fn async_write(c: Completion, buff: IOBuff) -> Self {
        let cloned = buff.clone();
        Self::Write(WriteCompletion {
            complete: Box::new(move |res| {
                let _ = cloned.clone();
                c.complete(res);
            }),
            buf: Some(buff),
        })
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
    pub buf: Option<IOBuff>,
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl ReadCompletion {
    pub fn new(buf: IOBuff, complete: Box<Complete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Buffer {
        unsafe { &*self.buf.0.get() }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn buf_mut(&self) -> &mut Buffer {
        unsafe { &mut *self.buf.0.get() }
    }

    pub fn complete(&self) {
        (self.complete)(self.buf.clone());
    }
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self {
            complete,
            buf: None,
        }
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

#[derive(Debug, Clone)]
pub struct IOBuff(Arc<UnsafeCell<Buffer>>);

#[allow(clippy::arc_with_non_send_sync)]
impl IOBuff {
    pub fn new(buf: Buffer) -> Self {
        Self(Arc::new(UnsafeCell::new(buf)))
    }

    pub fn clone(&self) -> Self {
        Self(self.0.clone())
    }

    // Clones the inner buffer and not just a refcount increase
    pub fn clone_inner(&self) -> Self {
        Self(Arc::new(UnsafeCell::new(unsafe {
            (*self.0.get()).clone()
        })))
    }

    pub fn buf(&self) -> &Buffer {
        unsafe { &*self.0.get() }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn buf_mut(&self) -> &mut Buffer {
        unsafe { &mut *self.0.get() }
    }

    pub fn len(&self) -> usize {
        self.buf().len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf().is_empty()
    }
}

pub type BufferData = Pin<Vec<u8>>;

pub type BufferDropFn = Rc<dyn Fn(BufferData)>;

#[derive(Clone)]
pub struct Buffer {
    data: ManuallyDrop<BufferData>,
    drop: BufferDropFn,
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        (self.drop)(data);
    }
}

impl Buffer {
    pub fn allocate(size: usize, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(Pin::new(vec![0; size]));
        Self { data, drop }
    }

    pub fn new(data: BufferData, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(data);
        Self { data, drop }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
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
        pub use io_uring::UringIO as PlatformIO;
    }

    #[cfg(any(all(target_os = "linux",not(feature = "io_uring")), target_os = "macos"))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
    }

    #[cfg(target_os = "windows")] {
        mod windows;
        pub use windows::WindowsIO as PlatformIO;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
    }
}

mod memory;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
mod common;
