use super::MemoryIO;
use crate::{Clock, Completion, File, Instant, LimboError, OpenFlags, Result, IO};
use parking_lot::RwLock;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, instrument, trace, Level};
pub struct WindowsIO {}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

impl IO for WindowsIO {
    #[instrument(err, skip_all, level = Level::TRACE)]
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
            file: RwLock::new(file),
        }))
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        while !c.is_completed() {
            self.step()?;
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    #[instrument(skip_all, level = Level::TRACE)]
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
    file: RwLock<std::fs::File>,
}

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        unimplemented!()
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        unimplemented!()
    }

    #[instrument(skip(self, c), level = Level::TRACE)]
    fn pread(&self, pos: usize, c: Completion) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let nr = {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
            buf.len() as i32
        };
        c.complete(nr);
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.borrow();
        let buf = buf.as_slice();
        file.write_all(buf)?;
        c.complete(buffer.borrow().len() as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        let file = self.file.write();
        file.sync_all().map_err(LimboError::IOError)?;
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let file = self.file.write();
        file.set_len(len as u64).map_err(LimboError::IOError)?;
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.read();
        Ok(file.metadata().unwrap().len())
    }
}
