use crate::{ExtResult, ExtensionApi, ResultCode};
use std::ffi::{c_char, c_void};

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
    fn run_once(&self) -> ExtResult<()> {
        Ok(())
    }
    fn close(&self, _file: Self::File) -> ExtResult<()> {
        Ok(())
    }
    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).unwrap();
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
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> ExtResult<i32>;
    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> ExtResult<i32>;
    fn sync(&self) -> ExtResult<()>;
    fn truncate(&self, len: i64) -> ExtResult<()>;
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct VfsImpl {
    pub name: *const c_char,
    pub vfs: *const c_void,
    pub open: VfsOpen,
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

pub type RegisterVfsFn =
    unsafe extern "C" fn(name: *const c_char, vfs: *const VfsImpl) -> ResultCode;

pub type VfsOpen = unsafe extern "C" fn(
    ctx: *const c_void,
    path: *const c_char,
    flags: i32,
    direct: bool,
) -> *const c_void;

pub type VfsClose = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsRead =
    unsafe extern "C" fn(file: *const c_void, buf: *mut u8, count: usize, offset: i64) -> i32;

pub type VfsWrite =
    unsafe extern "C" fn(file: *const c_void, buf: *const u8, count: usize, offset: i64) -> i32;

pub type VfsSync = unsafe extern "C" fn(file: *const c_void) -> i32;

pub type VfsTruncate = unsafe extern "C" fn(file: *const c_void, len: i64) -> ResultCode;

pub type VfsLock = unsafe extern "C" fn(file: *const c_void, exclusive: bool) -> ResultCode;

pub type VfsUnlock = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsSize = unsafe extern "C" fn(file: *const c_void) -> i64;

pub type VfsRunOnce = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsGetCurrentTime = unsafe extern "C" fn() -> *const c_char;

pub type VfsGenerateRandomNumber = unsafe extern "C" fn() -> i64;

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
