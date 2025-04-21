use super::{common, Completion, File, OpenFlags, WriteCompletion, IO};
use crate::io::clock::{Clock, Instant};
use crate::storage::buffer_pool::MAX_ARENA_PAGES;
use crate::{LimboError, MemoryIO, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::cell::UnsafeCell;
use std::fmt;
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, trace};

const ENTRIES: u32 = 512;
const SQPOLL_IDLE: u32 = 500;
const ACTIVE_FILES: u32 = 2;

#[derive(Debug, Error)]
enum UringIOError {
    IOUringCQError(i32),
}

impl fmt::Display for UringIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UringIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {}",
                code
            ),
        }
    }
}

pub struct UringIO {
    inner: Arc<UnsafeCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pub pending: [MaybeUninit<Completion>; ENTRIES as usize + 1],
    key: u64,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    files: AtomicU32,
    buffers: AtomicU32,
}

impl UringIO {
    pub fn new() -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_sqpoll(SQPOLL_IDLE)
            .setup_single_issuer()
            .build(ENTRIES)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(ENTRIES)?,
        };
        let sub = ring.submitter();
        sub.register_buffers_sparse(MAX_ARENA_PAGES)?;
        sub.register_files_sparse(ACTIVE_FILES)?; // db and wal files
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: [const { MaybeUninit::uninit() }; ENTRIES as usize + 1],
                key: 0,
            },
            files: AtomicU32::new(0),
            buffers: AtomicU32::new(0),
        };
        debug!("Using IO backend 'io-uring'");
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Self {
            inner: Arc::new(UnsafeCell::new(inner)),
        })
    }

    /// Register file descriptor with io_uring to allow for Fixed opcodes.
    fn register_file(&self, fd: i32) -> Result<u32> {
        let inner = unsafe { &mut *self.inner.get() };
        let mut fd_idx = inner
            .files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if inner.files.load(Ordering::Relaxed) > ACTIVE_FILES {
            // TODO: when we support multiple databases, this will likely need to change.
            // each Database will register at most 2 files (db + WAL)
            // so this means we need to replace the oldest file and start over
            // io_uring will allow overwriting the fd index so we can just set back to 0.
            // If < 2 files are being used, we are always just using the next available slot.
            fd_idx = 0;
        }
        inner
            .ring
            .ring
            .submitter()
            .register_files_update(fd_idx, &[fd])?;
        trace!("io_uring(registered file: {fd})");
        Ok(fd_idx)
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Completion) {
        trace!("submit_entry({:?})", entry);
        self.pending[entry.get_user_data() as usize].write(c);
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

    fn get_key(&mut self) -> u64 {
        self.key += 1;
        if self.key == ENTRIES as u64 {
            let key = self.key;
            self.key = 0;
            return key;
        }
        self.key
    }
}

impl IO for UringIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_fd();
        if direct {
            match fs::fcntl_setfl(fd, OFlags::DIRECT) {
                Ok(_) => {}
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            }
        }
        let id = self.register_file(file.as_raw_fd())?;
        let uring_file = Arc::new(UringFile {
            io: self.inner.clone(),
            file,
            id,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(true)?;
        }
        Ok(uring_file)
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let inner = unsafe { &mut *self.inner.get() };
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
            let c = unsafe { ring.pending[cqe.user_data() as usize].assume_init_read() };
            c.complete(cqe.result());
            ring.pending[cqe.user_data() as usize] = const { MaybeUninit::uninit() };
        }
        Ok(())
    }

    fn register_buffer(&self, arena_id: u32, iovec: (*const u8, usize)) -> Result<()> {
        let inner = unsafe { &*self.inner.get() };
        let mut offset = inner.buffers.fetch_add(1, Ordering::SeqCst);
        tracing::trace!("register_buffer: arena_id: {arena_id}, offset: {offset}");
        if arena_id != offset {
            // this means that a new connection + buffer pool is reusing the IO, so we can overwrite
            // the earlier buffers by calling update on previously registered indexes.
            inner.buffers.store(1, Ordering::Relaxed);
            offset = 0;
        }
        assert_eq!(arena_id, offset, "arena_id is not the expected offset");
        unsafe {
            inner.ring.ring.submitter().register_buffers_update(
                offset,
                &[libc::iovec {
                    iov_base: iovec.0 as _,
                    iov_len: iovec.1,
                }],
                None,
            )?
        };
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

impl Clock for UringIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct UringFile {
    io: Arc<UnsafeCell<InnerUringIO>>,
    file: std::fs::File,
    id: u32,
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}

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
                _ => format!("Failed locking file, {}", io_error),
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

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let io = unsafe { &mut *self.io.get() };
        let read_e = {
            let len = r.buf.len();
            let ptr = r.buf.as_ptr() as *mut u8;
            if let Some(id) = r.buf.arena_id() {
                trace!("pread_fixed(pos = {}, length = {})", pos, r.buf.len());
                io_uring::opcode::ReadFixed::new(
                    io_uring::types::Fixed(self.id),
                    ptr,
                    len as u32,
                    id as u16,
                )
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
            } else {
                trace!("pread(pos = {}, length = {})", pos, r.buf.len());
                io_uring::opcode::Read::new(fd, ptr, len as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(io.ring.get_key())
            }
        };
        io.ring.submit_entry(&read_e, c);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<crate::Buffer>, c: Completion) -> Result<()> {
        let io = unsafe { &mut *self.io.get() };
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            if let Some(id) = buffer.arena_id() {
                trace!("pwrite_fixed(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::WriteFixed::new(
                    io_uring::types::Fixed(self.id),
                    buffer.as_ptr(),
                    buffer.len() as u32,
                    id as u16,
                )
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
            } else {
                trace!("pwrite(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::Write::new(fd, buffer.as_ptr(), buffer.len() as u32)
                    .offset(pos as u64)
                    .build()
                    .user_data(io.ring.get_key())
            }
        };
        io.ring.submit_entry(
            &write,
            Completion::Write(WriteCompletion::new(Box::new(move |result| {
                c.complete(result);
                // NOTE: Explicitly reference buffer to ensure it lives until here
                let _ = buffer.clone();
            }))),
        );
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let io = unsafe { &mut *self.io.get() };
        trace!("sync()");
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(io.ring.get_key());
        io.ring.submit_entry(&sync, c);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
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
