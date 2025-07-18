use super::{Buffer, Completion, File, MemoryIO, OpenFlags, IO};
use crate::ext::VfsMod;
use crate::io::clock::{Clock, Instant};
use crate::io::CompletionType;
use crate::{LimboError, Result};
use std::cell::RefCell;
use std::ffi::{c_void, CString};
use std::sync::Arc;
use turso_ext::{VfsFileImpl, VfsImpl};

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

    fn wait_for_completion(&self, _c: Arc<Completion>) -> Result<()> {
        todo!();
    }

    fn generate_random_number(&self) -> i64 {
        if self.ctx.is_null() {
            return -1;
        }
        let vfs = unsafe { &*self.ctx };
        unsafe { (vfs.gen_random_number)() }
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
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

    fn pread(&self, pos: usize, c: Arc<Completion>) -> Result<Arc<Completion>> {
        let r = match c.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let result = {
            let mut buf = r.buf_mut();
            let count = buf.len();
            let vfs = unsafe { &*self.vfs };
            unsafe { (vfs.read)(self.file, buf.as_mut_ptr(), count, pos as i64) }
        };
        if result < 0 {
            Err(LimboError::ExtensionError("pread failed".to_string()))
        } else {
            c.complete(result);
            Ok(c)
        }
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<Buffer>>,
        c: Arc<Completion>,
    ) -> Result<Arc<Completion>> {
        let buf = buffer.borrow();
        let count = buf.as_slice().len();
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe {
            (vfs.write)(
                self.file,
                buf.as_slice().as_ptr() as *mut u8,
                count,
                pos as i64,
            )
        };

        if result < 0 {
            Err(LimboError::ExtensionError("pwrite failed".to_string()))
        } else {
            c.complete(result);
            Ok(c)
        }
    }

    fn sync(&self, c: Arc<Completion>) -> Result<Arc<Completion>> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.sync)(self.file) };
        if result < 0 {
            Err(LimboError::ExtensionError("sync failed".to_string()))
        } else {
            c.complete(0);
            Ok(c)
        }
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

    fn truncate(&self, len: usize, c: Arc<Completion>) -> Result<Arc<Completion>> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.truncate)(self.file, len as i64) };
        if result.is_error() {
            Err(LimboError::ExtensionError("truncate failed".to_string()))
        } else {
            c.complete(0);
            Ok(c)
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
