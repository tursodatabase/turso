use crate::{ExtResult, ExtensionApi, ResultCode};
use std::{
    ffi::{c_char, c_void},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

/// Field for ExtensionApi to interface with VFS extensions,
/// separated to more easily feature flag out for WASM builds.
#[repr(C)]
pub struct VfsInterface {
    pub register_vfs: RegisterVfsFn,
    pub builtin_vfs: *mut *const VfsImpl,
    pub builtin_vfs_count: i32,
}
unsafe impl Send for VfsInterface {}

pub trait VfsExtension: Default + Send + Sync {
    const NAME: &'static str;
    type File: VfsFile;
    fn open_file(&self, path: &str, flags: i32, direct: bool) -> ExtResult<Self::File>;
    fn remove_file(&self, path: &str) -> ExtResult<()>;
    fn run_once(&self) -> ExtResult<()> {
        Ok(())
    }
    fn close(&self, _file: Self::File) -> ExtResult<()> {
        Ok(())
    }
    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("failed to generate random bytes");
        i64::from_ne_bytes(buf)
    }
    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

pub trait VfsFile: Send + Sync {
    fn lock(&mut self, _exclusive: bool) -> ExtResult<()> {
        Ok(())
    }
    fn unlock(&self) -> ExtResult<()> {
        Ok(())
    }
    fn read(&mut self, buf: BufferRef, offset: i64, cb: Callback) -> ExtResult<()>;
    fn write(&mut self, buf: BufferRef, offset: i64, cb: Callback) -> ExtResult<()>;
    fn sync(&self, cb: Callback) -> ExtResult<()>;
    fn truncate(&self, len: i64, cb: Callback) -> ExtResult<()>;
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct VfsImpl {
    pub name: *const c_char,
    pub vfs: *const c_void,
    pub open: VfsOpen,
    pub remove: VfsRemove,
    pub close: VfsClose,
    pub read: VfsRead,
    pub write: VfsWrite,
    pub sync: VfsSync,
    pub lock: VfsLock,
    pub unlock: VfsUnlock,
    pub size: VfsSize,
    pub run_once: VfsRunOnce,
    pub current_time: VfsGetCurrentTime,
    pub gen_random_number: VfsGenerateRandomNumber,
    pub truncate: VfsTruncate,
}

/// a wrapper around the raw `*mut u8` buffer for extensions.
#[derive(Debug)]
#[repr(C)]
pub struct BufferRef {
    _ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for BufferRef {}
impl BufferRef {
    /// create a new `BufferRef` from a raw pointer
    ///
    /// # Safety
    /// The caller must ensure that the pointer is valid and the buffer is not deallocated.
    /// This function should only be called with a pointer to a buffer allocated by the `Buffer` type defined in the `core` module.
    pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
        Self {
            _ptr: NonNull::new(ptr).expect("Received null buffer pointer"),
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get a safe slice reference to the buffer
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self._ptr.as_ptr(), self.len) }
    }

    /// Get a safe mutable slice reference to the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self._ptr.as_ptr(), self.len) }
    }
}

impl Deref for BufferRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for BufferRef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}
pub type Callback = Box<dyn FnOnce(i32) + Send>;

#[repr(C)]
pub struct IOCallback {
    pub callback: CallbackFn,
    pub ctx: SendPtr,
}

unsafe impl Send for IOCallback {}

#[repr(transparent)]
/// Wrapper type to support creating Box<dyn FnOnce()+Send> obj
/// that needs to call a C function with an opaque pointer.
pub struct SendPtr(NonNull<c_void>);
unsafe impl Send for SendPtr {}

impl SendPtr {
    pub fn inner(&self) -> NonNull<c_void> {
        self.0
    }
}

impl IOCallback {
    pub fn new(cb: CallbackFn, ctx: NonNull<c_void>) -> Self {
        Self {
            callback: cb,
            ctx: SendPtr(ctx),
        }
    }
}

pub type CallbackFn = unsafe extern "C" fn(res: i32, user_data: SendPtr);

pub type RegisterVfsFn =
    unsafe extern "C" fn(name: *const c_char, vfs: *const VfsImpl) -> ResultCode;

pub type VfsOpen = unsafe extern "C" fn(
    ctx: *const c_void,
    path: *const c_char,
    flags: i32,
    direct: bool,
) -> *const c_void;

pub type VfsClose = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsRead = unsafe extern "C" fn(
    file: *const c_void,
    buf: BufferRef,
    offset: i64,
    cb: IOCallback,
) -> ResultCode;

pub type VfsWrite = unsafe extern "C" fn(
    file: *const c_void,
    buf: BufferRef,
    offset: i64,
    cb: IOCallback,
) -> ResultCode;

pub type VfsSync = unsafe extern "C" fn(file: *const c_void, cb: IOCallback) -> ResultCode;

pub type VfsTruncate =
    unsafe extern "C" fn(file: *const c_void, len: i64, cb: IOCallback) -> ResultCode;

pub type VfsLock = unsafe extern "C" fn(file: *const c_void, exclusive: bool) -> ResultCode;

pub type VfsUnlock = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsSize = unsafe extern "C" fn(file: *const c_void) -> i64;

pub type VfsRunOnce = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsGetCurrentTime = unsafe extern "C" fn() -> *const c_char;

pub type VfsGenerateRandomNumber = unsafe extern "C" fn() -> i64;

pub type VfsRemove = unsafe extern "C" fn(ctx: *const c_void, path: *const c_char) -> ResultCode;

#[repr(C)]
pub struct VfsFileImpl {
    pub file: *const c_void,
    pub vfs: *const VfsImpl,
}
unsafe impl Send for VfsFileImpl {}
unsafe impl Sync for VfsFileImpl {}

impl VfsFileImpl {
    pub fn new(file: *const c_void, vfs: *const VfsImpl) -> ExtResult<Self> {
        if file.is_null() || vfs.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Self { file, vfs })
    }
}

impl Drop for VfsFileImpl {
    fn drop(&mut self) {
        if self.vfs.is_null() || self.file.is_null() {
            return;
        }
        let vfs = unsafe { &*self.vfs };
        unsafe {
            (vfs.close)(self.file);
        }
    }
}

impl ExtensionApi {
    /// Since we want the option to build in extensions at compile time as well,
    /// we add a slice of VfsImpls to the extension API, and this is called with any
    /// libraries that we load staticly that will add their VFS implementations to the list.
    pub fn add_builtin_vfs(&mut self, vfs: *const VfsImpl) -> ResultCode {
        if vfs.is_null() || self.vfs_interface.builtin_vfs.is_null() {
            return ResultCode::Error;
        }
        let mut new = unsafe {
            let slice = std::slice::from_raw_parts_mut(
                self.vfs_interface.builtin_vfs,
                self.vfs_interface.builtin_vfs_count as usize,
            );
            Vec::from(slice)
        };
        new.push(vfs);
        self.vfs_interface.builtin_vfs =
            Box::into_raw(new.into_boxed_slice()) as *mut *const VfsImpl;
        self.vfs_interface.builtin_vfs_count += 1;
        ResultCode::OK
    }
}
