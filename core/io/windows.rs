use super::MemoryIO;
use crate::{Clock, Completion, File, Instant, LimboError, OpenFlags, Result, IO};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, trace};
pub struct WindowsIO {}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

unsafe impl Send for WindowsIO {}
unsafe impl Sync for WindowsIO {}

impl IO for WindowsIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        Ok(Arc::new(WindowsFile {
            file: RefCell::new(file),
        }))
    }

    fn wait_for_completion(&self, c: Arc<Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }
}

impl Clock for WindowsIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct WindowsFile {
    file: RefCell<std::fs::File>,
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

impl File for WindowsFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        unimplemented!()
    }

    fn unlock_file(&self) -> Result<()> {
        unimplemented!()
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<Arc<Completion>> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let nr = {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
            buf.len() as i32
        };
        c.complete(nr);
        Ok(Arc::new(c))
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<crate::Buffer>>,
        c: Completion,
    ) -> Result<Arc<Completion>> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.borrow();
        let buf = buf.as_slice();
        file.write_all(buf)?;
        c.complete(buffer.borrow().len() as i32);
        Ok(Arc::new(c))
    }

    fn sync(&self, c: Completion) -> Result<Arc<Completion>> {
        let file = self.file.borrow_mut();
        file.sync_all().map_err(LimboError::IOError)?;
        c.complete(0);
        Ok(Arc::new(c))
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata().unwrap().len())
    }
}
