use super::{Buffer, Completion, File, OpenFlags, IO};
use crate::ext::VfsMod;
use crate::io::clock::{Clock, Instant};
use crate::io::CompletionInner;
use crate::{LimboError, Result};
use std::ffi::{c_void, CString};
use std::ptr::NonNull;
use std::sync::Arc;
use turso_ext::{BufferRef, IOCallback, SendPtr, VfsFileImpl, VfsImpl};

impl Clock for VfsMod {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

impl IO for VfsMod {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = self.ctx as *mut c_void;
        let vfs = unsafe { &*self.ctx };
        let file = unsafe { (vfs.open)(ctx, c_path.as_ptr(), flags.0, direct) };
        if file.is_null() {
            return Err(LimboError::ExtensionError("File not found".to_string()));
        }
        Ok(Arc::new(turso_ext::VfsFileImpl::new(file, self.ctx)?))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = self.ctx as *mut c_void;
        let vfs = unsafe { &*self.ctx };
        let result = unsafe { (vfs.remove)(ctx, c_path.as_ptr()) };
        if !result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        if self.ctx.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.ctx };
        let result = unsafe { (vfs.run_once)(vfs.vfs) };
        if !result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        if self.ctx.is_null() {
            return -1;
        }
        let vfs = unsafe { &*self.ctx };
        unsafe { (vfs.gen_random_number)() }
    }
}

impl VfsMod {
    #[allow(dead_code)] // used in FFI call
    fn get_current_time(&self) -> String {
        if self.ctx.is_null() {
            return "".to_string();
        }
        unsafe {
            let vfs = &*self.ctx;
            let chars = (vfs.current_time)();
            let cstr = CString::from_raw(chars as *mut _);
            cstr.to_string_lossy().into_owned()
        }
    }
}

/// # Safety
/// the callback wrapper in the extension library is FnOnce, so we know
/// that the into_raw/from_raw contract will hold
unsafe extern "C" fn callback_fn(result: i32, ctx: SendPtr) {
    let completion = Completion {
        inner: (Arc::from_raw(ctx.inner().as_ptr() as *mut CompletionInner)),
    };
    completion.complete(result);
}

fn to_callback(c: Completion) -> IOCallback {
    IOCallback::new(callback_fn, unsafe {
        NonNull::new_unchecked(Arc::into_raw(c.inner) as *mut c_void)
    })
}

impl File for VfsFileImpl {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.lock)(self.file, exclusive) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.unlock)(self.file) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            c.complete(-1);
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let r = c.as_read();
        let buf = r.buf();
        let len = buf.len();
        let cb = to_callback(c.clone());
        let vfs = unsafe { &*self.vfs };
        let res = unsafe {
            (vfs.read)(
                self.file,
                BufferRef::new(buf.as_mut_ptr(), len),
                pos as i64,
                cb,
            )
        };
        if res.is_error() {
            return Err(LimboError::ExtensionError("pread failed".to_string()));
        }
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            c.complete(-1);
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let res = unsafe {
            let buf = buffer.clone();
            let len = buf.len();
            let cb = to_callback(c.clone());
            (vfs.write)(
                self.file,
                BufferRef::new(buf.as_ptr() as *mut u8, len),
                pos as i64,
                cb,
            )
        };
        if res.is_error() {
            return Err(LimboError::ExtensionError("pwrite failed".to_string()));
        }
        Ok(c)
    }

    fn sync(&self, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            c.complete(-1);
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let cb = to_callback(c.clone());
        let res = unsafe { (vfs.sync)(self.file, cb) };
        if res.is_error() {
            return Err(LimboError::ExtensionError("sync failed".to_string()));
        }
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.size)(self.file) };
        if result < 0 {
            Err(LimboError::ExtensionError("size failed".to_string()))
        } else {
            Ok(result as u64)
        }
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            c.complete(-1);
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let cb = to_callback(c.clone());
        let res = unsafe { (vfs.truncate)(self.file, len as i64, cb) };
        if res.is_error() {
            return Err(LimboError::ExtensionError("truncate failed".to_string()));
        }
        Ok(c)
    }
}

impl Drop for VfsMod {
    fn drop(&mut self) {
        if self.ctx.is_null() {
            return;
        }
        unsafe {
            let _ = Box::from_raw(self.ctx as *mut VfsImpl);
        }
    }
}
