#![allow(clippy::arc_with_non_send_sync)]

use super::{common, Completion, File, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use crate::{turso_assert, DatabaseMode, LimboError, MemoryIO, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::io::ErrorKind;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, trace};

const ENTRIES: u32 = 512;
const SQPOLL_IDLE: u32 = 1000;
const FILES: u32 = 8;

#[derive(Debug, Error)]
enum UringIOError {
    IOUringCQError(i32),
}

impl fmt::Display for UringIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UringIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {code}",
            ),
        }
    }
}

pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    free_files: VecDeque<u32>,
}

impl UringIO {
    pub fn new() -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_single_issuer()
            .setup_coop_taskrun()
            .setup_sqpoll(SQPOLL_IDLE)
            .build(ENTRIES)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(ENTRIES)?,
        };
        // we only ever have 2 files open at a time for the moment
        ring.submitter().register_files_sparse(FILES)?;
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
            },
            free_files: (0..FILES).collect(),
        };
        debug!("Using IO backend 'io-uring'");
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

impl InnerUringIO {
    fn register_file(&mut self, fd: i32) -> Result<u32> {
        if let Some(slot) = self.free_files.pop_front() {
            self.ring
                .ring
                .submitter()
                .register_files_update(slot, &[fd.as_raw_fd()])?;
            return Ok(slot);
        }
        Err(LimboError::UringIOError(
            "unable to register file, no free slots available".to_string(),
        ))
    }
    fn unregister_file(&mut self, id: u32) -> Result<()> {
        self.ring
            .ring
            .submitter()
            .register_files_update(id, &[-1])?;
        self.free_files.push_back(id);
        Ok(())
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry) {
        trace!("submit_entry({:?})", entry);
        unsafe {
            self.ring
                .submission()
                .push(entry)
                .expect("submission queue is full");
        }
        self.pending_ops += 1;
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        self.ring.submit_and_wait(1)?;
        Ok(())
    }

    fn get_completion(&mut self) -> Option<io_uring::cqueue::Entry> {
        // NOTE: This works because CompletionQueue's next function pops the head of the queue. This is not normal behaviour of iterators
        let entry = self.ring.completion().next();
        if entry.is_some() {
            trace!("get_completion({:?})", entry);
            // consumed an entry from completion queue, update pending_ops
            self.pending_ops -= 1;
        }
        entry
    }

    fn empty(&self) -> bool {
        self.pending_ops == 0
    }
}

impl IO for UringIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_fd();
        if direct {
            match fs::fcntl_setfl(fd, OFlags::DIRECT) {
                Ok(_) => {}
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            }
        }
        let id = self.inner.borrow_mut().register_file(file.as_raw_fd()).ok();
        let uring_file = Arc::new(UringFile {
            io: self.inner.clone(),
            file,
            id,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(uring_file)
    }

    fn wait_for_completion(&self, c: Arc<Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut inner = self.inner.borrow_mut();
        let ring = &mut inner.ring;

        if ring.empty() {
            return Ok(());
        }

        ring.wait_for_completion()?;
        while let Some(cqe) = ring.get_completion() {
            let result = cqe.result();
            if result < 0 {
                return Err(LimboError::UringIOError(format!(
                    "{} cqe: {:?}",
                    UringIOError::IOUringCQError(result),
                    cqe
                )));
            }
            let ud = cqe.user_data();
            turso_assert!(ud > 0, "therea are no linked timeouts or cancelations, all cqe user_data should be valid arc pointers");
            completion_from_key(ud).complete(result);
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

    fn database_mode(&self) -> crate::DatabaseMode {
        DatabaseMode::File
    }
}

impl Clock for UringIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

#[inline(always)]
/// use the callback pointer as the user_data for the operation as is
/// common practice for io_uring to prevent more indirection
fn get_key(c: Arc<Completion>) -> u64 {
    Arc::into_raw(c) as u64
}

#[inline(always)]
/// convert the user_data back to an Arc<Completion> pointer
fn completion_from_key(key: u64) -> Arc<Completion> {
    unsafe { Arc::from_raw(key as *const Completion) }
}

pub struct UringFile {
    io: Rc<RefCell<InnerUringIO>>,
    file: std::fs::File,
    id: Option<u32>,
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}

macro_rules! with_fd {
    ($file:expr, |$fd:ident| $body:expr) => {
        match $file.id {
            Some(id) => {
                let $fd = io_uring::types::Fixed(id);
                $body
            }
            None => {
                let $fd = io_uring::types::Fd($file.file.as_raw_fd());
                $body
            }
        }
    };
}

impl File for UringFile {
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

    fn pread(&self, pos: usize, c: Arc<Completion>) -> Result<Arc<Completion>> {
        let r = c.as_read();
        trace!("pread(pos = {}, length = {})", pos, r.buf().len());
        let mut io = self.io.borrow_mut();
        let read_e = {
            let mut buf = r.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            with_fd!(self, |fd| {
                io_uring::opcode::Read::new(fd, buf, len as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(get_key(c.clone()))
            })
        };
        io.ring.submit_entry(&read_e);
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<crate::Buffer>>,
        c: Arc<Completion>,
    ) -> Result<Arc<Completion>> {
        let mut io = self.io.borrow_mut();
        let write = {
            let buf = buffer.borrow();
            trace!("pwrite(pos = {}, length = {})", pos, buf.len());
            with_fd!(self, |fd| {
                io_uring::opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(get_key(c.clone()))
            })
        };
        io.ring.submit_entry(&write);
        Ok(c)
    }

    fn sync(&self, c: Arc<Completion>) -> Result<Arc<Completion>> {
        let mut io = self.io.borrow_mut();
        trace!("sync()");
        let sync = with_fd!(self, |fd| {
            io_uring::opcode::Fsync::new(fd)
                .build()
                .user_data(get_key(c.clone()))
        });
        io.ring.submit_entry(&sync);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
        if let Some(id) = self.id {
            self.io
                .borrow_mut()
                .unregister_file(id)
                .inspect_err(|e| {
                    debug!("Failed to unregister file: {e}");
                })
                .ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::common;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
    }
}
