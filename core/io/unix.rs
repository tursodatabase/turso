use crate::error::LimboError;
use crate::io::common;
use crate::Result;

use super::{Completion, File, MemoryIO, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use parking_lot::Mutex;
use rustix::{
    fd::AsFd,
    fs::{self, FlockOperation, OFlags, OpenOptionsExt},
};
use std::cell::RefCell;
use std::{io::ErrorKind, sync::Arc};
use tracing::{debug, instrument, trace, Level};

// TODO: Arc + Mutex here for Send + Sync functionality
// can maybe see a way for only submitting IO through
type CallbackQueue = Arc<Mutex<Vec<CompletionCallback>>>;

/// UnixIO lives longer than any of the files it creates, so it is
/// safe to store references to it's internals in the UnixFiles
pub struct UnixIO {
    callbacks: CallbackQueue,
}

unsafe impl Send for UnixIO {}
unsafe impl Sync for UnixIO {}

impl UnixIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl Clock for UnixIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

impl IO for UnixIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true).custom_flags(OFlags::NONBLOCK.bits() as i32);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let unix_file = UnixFile::new(file, self.callbacks.clone());
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            unix_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(Arc::new(unix_file))
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn run_once(&self) -> Result<()> {
        let mut callbacks = self.callbacks.lock();
        if callbacks.is_empty() {
            return Ok(());
        }

        while let Some(cf) = callbacks.pop() {
            let n = match cf {
                CompletionCallback::Read {
                    ref file,
                    ref completion,
                    pos,
                } => {
                    let r = completion.as_read();
                    let mut buf = r.buf_mut();
                    rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
                }
                CompletionCallback::Write {
                    ref file,
                    ref buf,
                    pos,
                    ..
                } => {
                    let buf = buf.borrow();
                    rustix::io::pwrite(file.as_fd(), buf.as_slice(), pos as u64)
                }
                CompletionCallback::Sync { ref file, .. } => {
                    fs::fsync(file.as_fd())?;
                    Ok(0)
                }
            }?;
            match &cf {
                CompletionCallback::Read { ref completion, .. }
                | CompletionCallback::Write { ref completion, .. }
                | CompletionCallback::Sync { ref completion, .. } => completion.complete(n as i32),
            }
        }
        Ok(())
    }

    fn wait_for_completion(&self, c: Arc<Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
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

enum CompletionCallback {
    Read {
        file: Arc<std::fs::File>,
        completion: Arc<Completion>,
        pos: usize,
    },
    Write {
        file: Arc<std::fs::File>,
        completion: Arc<Completion>,
        buf: Arc<RefCell<crate::Buffer>>,
        pos: usize,
    },
    Sync {
        file: Arc<std::fs::File>,
        completion: Arc<Completion>,
    },
}

pub struct UnixFile {
    #[allow(clippy::arc_with_non_send_sync)]
    file: Arc<std::fs::File>,
    callbacks: CallbackQueue,
}

impl UnixFile {
    fn new(file: std::fs::File, callbacks: CallbackQueue) -> Self {
        Self {
            file: Arc::new(file),
            callbacks,
        }
    }
}

unsafe impl Send for UnixFile {}
unsafe impl Sync for UnixFile {}

impl File for UnixFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {io_error}"),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn pread(&self, pos: usize, c: Completion) -> Arc<Completion> {
        tracing::trace!("");
        let c = Arc::new(c);
        self.callbacks.lock().push(CompletionCallback::Read {
            file: self.file.clone(),
            completion: c.clone(),
            pos,
        });
        c
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<super::Buffer>>,
        c: Completion,
    ) -> Arc<Completion> {
        tracing::trace!("");
        let c = Arc::new(c);
        self.callbacks.lock().push(CompletionCallback::Write {
            file: self.file.clone(),
            completion: c.clone(),
            buf: buffer.clone(),
            pos,
        });
        c
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Arc<Completion> {
        tracing::trace!("");
        let c = Arc::new(c);
        self.callbacks.lock().push(CompletionCallback::Sync {
            file: self.file.clone(),
            completion: c.clone(),
        });
        c
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for UnixFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
    }
}
