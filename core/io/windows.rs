use crate::{Clock, Completion, File, Instant, LimboError, OpenFlags, Result, IO};
use parking_lot::RwLock;
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
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        Ok(std::fs::remove_file(path)?)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        Ok(())
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
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos))?;
        let nr = {
            let r = c.as_read();
            let buf = r.buf();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
            buf.len() as i32
        };
        c.complete(nr);
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos))?;
        let buf = buffer.as_slice();
        file.write_all(buf)?;
        c.complete(buffer.len() as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        let file = self.file.write();
        file.sync_all()?;
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let file = self.file.write();
        file.set_len(len)?;
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.read();
        Ok(file.metadata().unwrap().len())
    }
}
