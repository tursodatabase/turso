//! In-memory file implementation for simulation.

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use turso_core::{Completion, File, Result};

/// An in-memory file for simulation.
///
/// Operations execute synchronously and the file contents are stored in memory.
pub struct MemorySimFile {
    /// File path used as identifier.
    pub path: String,
    /// In-memory file buffer.
    pub buffer: RefCell<Vec<u8>>,
    /// Whether the file has been closed.
    pub closed: Cell<bool>,
}

unsafe impl Send for MemorySimFile {}
unsafe impl Sync for MemorySimFile {}

impl MemorySimFile {
    /// Create a new in-memory file.
    pub fn new(path: String) -> Self {
        Self {
            path,
            buffer: RefCell::new(Vec::new()),
            closed: Cell::new(false),
        }
    }

    /// Write a buffer to the file at the given offset.
    pub fn write_buf(&self, buf: &[u8], offset: usize) -> usize {
        let mut file_buf = self.buffer.borrow_mut();

        // Extend file if needed
        let required_len = offset + buf.len();
        if file_buf.len() < required_len {
            file_buf.resize(required_len, 0);
        }

        file_buf[offset..][0..buf.len()].copy_from_slice(buf);
        buf.len()
    }
}

impl File for MemorySimFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let file_buf = self.buffer.borrow();
        let offset = pos as usize;

        let buffer = c.as_read().buf.clone();
        let buf_size = {
            let buf = buffer.as_mut_slice();
            let available = file_buf.len().saturating_sub(offset);
            let read_len = buf.len().min(available);
            if read_len > 0 {
                buf[..read_len].copy_from_slice(&file_buf[offset..][..read_len]);
            }
            read_len as i32
        };
        c.complete(buf_size);
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        let written = self.write_buf(buffer.as_slice(), pos as usize);
        c.complete(written as i32);
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        if buffers.is_empty() {
            c.complete(0);
            return Ok(c);
        }

        let mut offset = pos as usize;
        let mut total_written = 0;

        for buffer in buffers {
            let written = self.write_buf(buffer.as_slice(), offset);
            offset += written;
            total_written += written;
        }

        c.complete(total_written as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: turso_core::io::FileSyncType) -> Result<Completion> {
        // No-op for in-memory files
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.buffer.borrow().len() as u64)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.buffer.borrow_mut().truncate(len as usize);
        c.complete(0);
        Ok(c)
    }
}
