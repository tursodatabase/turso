use super::MemoryIO;
use crate::{
    Clock, Completion, CompletionType, File, FsyncKind, Instant, LimboError, OpenFlags, Result, IO,
};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, trace};

pub struct GenericIO {}

impl GenericIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'generic'");
        Ok(Self {})
    }
}

unsafe impl Send for GenericIO {}
unsafe impl Sync for GenericIO {}

impl IO for GenericIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        Ok(Arc::new(GenericFile {
            path: std::path::PathBuf::from(path),
            file: RefCell::new(file),
            memory_io: Arc::new(MemoryIO::new()),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

impl Clock for GenericIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct GenericFile {
    path: std::path::PathBuf,
    file: RefCell<std::fs::File>,
    memory_io: Arc<MemoryIO>,
}

unsafe impl Send for GenericFile {}
unsafe impl Sync for GenericFile {}

impl File for GenericFile {
    // Since we let the OS handle the locking, file locking is not supported on the generic IO implementation
    // No-op implementation allows compilation but provides no actual file locking.
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<Completion> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        {
            let r = c.as_read();
            let mut buf = r.buf();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
        }
        c.complete(0);
        Ok(c)
    }

    fn pwrite(&self, pos: usize, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.as_slice();
        file.write_all(buf)?;
        c.complete(buf.len() as i32);
        Ok(c)
    }

    fn sync(&self, kind: FsyncKind, c: Completion) -> Result<Completion> {
        let mut file = self.file.borrow_mut();
        match kind {
            FsyncKind::Full => {
                file.sync_all()?;
            }
            FsyncKind::Data => {
                file.sync_data()?;
            }
        }
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let mut file = self.file.borrow_mut();
        file.set_len(len as u64)?;
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata().unwrap().len())
    }
}

impl Drop for GenericFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}
