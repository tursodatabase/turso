use crate::ext::VfsMod;
use crate::{LimboError, Result};
use limbo_ext::{VfsFileImpl, VfsImpl};
use std::cell::RefCell;
use std::ffi::{c_void, CString};
use std::sync::Arc;

use super::{Buffer, Completion, File, OpenFlags, IO};

impl IO for VfsMod {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = self.ctx as *mut c_void;
        let vfs = unsafe { &*self.ctx };
        let file = unsafe { (vfs.open)(ctx, c_path.as_ptr(), flags.to_flags(), direct) };
        if file.is_null() {
            return Err(LimboError::ExtensionError("File not found".to_string()));
        }
        Ok(Arc::new(limbo_ext::VfsFileImpl::new(file, self.ctx)?))
    }

    fn run_once(&self) -> Result<()> {
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

    fn get_current_time(&self) -> String {
        if self.ctx.is_null() {
            return "".to_string();
        }
        unsafe {
            let vfs = &*self.ctx;
            let chars = (vfs.current_time)();
            let cstr = CString::from_raw(chars as *mut i8);
            cstr.to_string_lossy().into_owned()
        }
    }
}

unsafe extern "C" fn callback(result: i32, user_data: *mut c_void) {
    let completion = Box::from_raw(user_data as *mut Completion);
    completion.complete(result);
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

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let len = r.buf().len();
        let buf = r.buf_mut().as_mut_slice().as_mut_ptr();
        let vfs = unsafe { &*self.vfs };
        let ctx = Box::into_raw(Box::new(c)) as *mut c_void;
        let cb = limbo_ext::IOCallback::new(callback, ctx);
        unsafe { (vfs.read)(self.file, buf, len, pos as i64, cb) };
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<Buffer>>, c: Completion) -> Result<()> {
        let buf = buffer.borrow();
        let count = buf.as_slice().len();
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let ctx = Box::into_raw(Box::new(c)) as *mut c_void;
        // buffer is ManuallyDrop so it's safe to pass a pointer to an extension
        // without incrementing the strong count of our Arc
        let cb = limbo_ext::IOCallback::new(callback, ctx);
        unsafe {
            (vfs.write)(
                self.file,
                buf.as_slice().as_ptr() as *mut u8,
                count,
                pos as i64,
                cb,
            )
        };
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let vfs = unsafe { &*self.vfs };
        let ctx = Box::into_raw(Box::new(c)) as *mut c_void;
        let cb = limbo_ext::IOCallback::new(callback, ctx);
        unsafe { (vfs.sync)(self.file, cb) };
        Ok(())
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
