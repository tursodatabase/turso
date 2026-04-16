use crate::error::{io_error, LimboError};
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::file_lock;
use crate::{Clock, Completion, File, OpenFlags, Result, IO};
use crate::sync::RwLock;
use std::io::{Read, Seek, Write};
use crate::sync::Arc;
use tracing::{debug, instrument, trace, Level};
pub struct GenericIO {}

impl GenericIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

impl IO for GenericIO {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path).map_err(|e| io_error(e, "open"))?;
        let generic_file = Arc::new(GenericFile {
            file: RwLock::new(file),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.contains(OpenFlags::ReadOnly)
        {
            generic_file.lock_file(true)?;
        }
        Ok(generic_file)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        Ok(())
    }
}

impl Clock for GenericIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

pub struct GenericFile {
    file: RwLock<std::fs::File>,
}

impl Drop for GenericFile {
    fn drop(&mut self) {
        let _ = self.unlock_file();
    }
}

impl File for GenericFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        file_lock::try_lock(&self.file.read(), exclusive).map_err(|e| {
            let message = if file_lock::is_contention_error(&e) {
                "Failed locking file. File is locked by another process".to_string()
            } else {
                format!("Failed locking file, {e}")
            };
            LimboError::LockingError(message)
        })
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        file_lock::unlock(&self.file.read()).map_err(|e| {
            LimboError::LockingError(format!("Failed to release file lock: {e}"))
        })
    }

    #[instrument(skip(self, c), level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos)).map_err(|e| io_error(e, "pread"))?;
        let nr = {
            let r = c.as_read();
            let buf = r.buf();
            let buf = buf.as_mut_slice();
            file.read(buf).map_err(|e| io_error(e, "pread"))? as i32
        };
        c.complete(nr);
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let mut file = self.file.write();
        file.seek(std::io::SeekFrom::Start(pos)).map_err(|e| io_error(e, "pwrite"))?;
        let buf = buffer.as_slice();
        file.write_all(buf).map_err(|e| io_error(e, "pwrite"))?;
        c.complete(buffer.len() as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion, _sync_type: crate::io::FileSyncType) -> Result<Completion> {
        let file = self.file.write();
        file.sync_all().map_err(|e| io_error(e, "sync"))?;
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let file = self.file.write();
        file.set_len(len).map_err(|e| io_error(e, "truncate"))?;
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.read();
        Ok(file.metadata().map_err(|e| io_error(e, "metadata"))?.len())
    }
}
