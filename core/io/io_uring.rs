#![allow(clippy::arc_with_non_send_sync)]

use super::{common, Completion, CompletionInner, File, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use crate::storage::wal::CKPT_BATCH_PAGES;
use crate::{turso_assert, LimboError, MemoryIO, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    ops::Deref,
    os::{fd::AsFd, unix::io::AsRawFd},
    rc::Rc,
    sync::Arc,
};
use tracing::{debug, trace};

/// Size of the io_uring submission and completion queues
const ENTRIES: u32 = 512;

/// Idle timeout for the sqpoll kernel thread before it needs
/// to be woken back up by a call IORING_ENTER_SQ_WAKEUP flag.
/// (handled by the io_uring crate in `submit_and_wait`)
const SQPOLL_IDLE: u32 = 1000;

/// Number of file descriptors we preallocate for io_uring.
/// NOTE: we may need to increase this when `attach` is fully implemented.
const FILES: u32 = 8;

/// Number of Vec<Box<[iovec]>> we preallocate on initialization
const IOVEC_POOL_SIZE: usize = 64;

/// Maximum number of iovec entries per writev operation.
/// IOV_MAX is typically 1024, but we limit it to a smaller number
const MAX_IOVEC_ENTRIES: usize = CKPT_BATCH_PAGES;

/// Maximum number of I/O operations to wait for in a single run,
/// waiting for > 1 can reduce the amount of `io_uring_enter` syscalls we
/// make, but can increase single operation latency.
const MAX_WAIT: usize = 4;

pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    writev_states: HashMap<u64, WritevState>,
    iov_pool: IovecPool,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    free_files: VecDeque<u32>,
}

/// preallocated vec of iovec arrays to avoid allocations during writev operations
struct IovecPool {
    pool: Vec<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl IovecPool {
    fn new() -> Self {
        let pool = (0..IOVEC_POOL_SIZE)
            .map(|_| {
                Box::new(
                    [libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    }; MAX_IOVEC_ENTRIES],
                )
            })
            .collect();
        Self { pool }
    }

    #[inline(always)]
    fn acquire(&mut self) -> Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>> {
        self.pool.pop()
    }

    #[inline(always)]
    fn release(&mut self, iovec: Box<[libc::iovec; MAX_IOVEC_ENTRIES]>) {
        if self.pool.len() < IOVEC_POOL_SIZE {
            self.pool.push(iovec);
        }
    }
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
                writev_states: HashMap::new(),
                iov_pool: IovecPool::new(),
            },
            free_files: (0..FILES).collect(),
        };
        debug!("Using IO backend 'io-uring'");
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

/// io_uring crate decides not to export their `UseFixed` trait, so we
/// are forced to use a macro here to handle either fixed or raw file descriptors.
macro_rules! with_fd {
    ($file:expr, |$fd:ident| $body:expr) => {
        match $file.id() {
            Some(id) => {
                let $fd = io_uring::types::Fixed(id);
                $body
            }
            None => {
                let $fd = io_uring::types::Fd($file.as_raw_fd());
                $body
            }
        }
    };
}

/// wrapper type to represent a possibly registered file descriptor,
/// only used in WritevState, and piggy-backs on the available methods from
/// `UringFile`, so we don't have to store the file on `WritevState`.
enum Fd {
    Fixed(u32),
    RawFd(i32),
}

impl Fd {
    /// to match the behavior of the File, we need to implement the same methods
    fn id(&self) -> Option<u32> {
        match self {
            Fd::Fixed(id) => Some(*id),
            Fd::RawFd(_) => None,
        }
    }
    /// ONLY to be called by the macro, in the case where id() is None
    fn as_raw_fd(&self) -> i32 {
        match self {
            Fd::RawFd(fd) => *fd,
            _ => panic!("Cannot call as_raw_fd on a Fixed Fd"),
        }
    }
}

/// State to track an ongoing writev operation in
/// the case of a partial write.
struct WritevState {
    /// File descriptor/id of the file we are writing to
    file_id: Fd,
    /// absolute file offset for next submit
    file_pos: usize,
    /// current buffer index in `bufs`
    current_buffer_idx: usize,
    /// intra-buffer offset
    current_buffer_offset: usize,
    /// total bytes written so far
    total_written: usize,
    /// cache the sum of all buffer lengths for the total expected write
    total_len: usize,
    /// buffers to write
    bufs: Vec<Arc<RefCell<crate::Buffer>>>,
    /// we keep the last iovec allocation alive until final CQE
    last_iov_allocation: Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl WritevState {
    fn new(file: &UringFile, pos: usize, bufs: Vec<Arc<RefCell<crate::Buffer>>>) -> Self {
        let file_id = file
            .id()
            .map(Fd::Fixed)
            .unwrap_or_else(|| Fd::RawFd(file.as_raw_fd()));
        let total_len = bufs.iter().map(|b| b.borrow().len()).sum();
        Self {
            file_id,
            file_pos: pos,
            current_buffer_idx: 0,
            current_buffer_offset: 0,
            total_written: 0,
            bufs,
            last_iov_allocation: None,
            total_len,
        }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.total_len - self.total_written
    }

    /// Advance (idx, off, pos) after written bytes
    #[inline(always)]
    fn advance(&mut self, written: usize) {
        let mut remaining = written;
        while remaining > 0 {
            let current_buf_len = {
                let r = self.bufs[self.current_buffer_idx].borrow();
                r.len()
            };
            let left = current_buf_len - self.current_buffer_offset;
            if remaining < left {
                self.current_buffer_offset += remaining;
                self.file_pos += remaining;
                remaining = 0;
            } else {
                remaining -= left;
                self.file_pos += left;
                self.current_buffer_idx += 1;
                self.current_buffer_offset = 0;
            }
        }
        self.total_written += written;
    }

    #[inline(always)]
    /// Free the allocation that keeps the iovec array alive while writev is ongoing
    fn free_last_iov(&mut self, pool: &mut IovecPool) {
        if let Some(allocation) = self.last_iov_allocation.take() {
            pool.release(allocation);
        }
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
            let mut sub = self.ring.submission_shared();
            match sub.push(entry) {
                Ok(_) => self.pending_ops += 1,
                Err(e) => {
                    tracing::error!("Failed to submit entry: {e}");
                    self.ring.submit().expect("failed to submit entry");
                    sub.push(entry).expect("failed to push entry after submit");
                    self.pending_ops += 1;
                }
            }
        }
    }

    fn submit_and_wait(&mut self) -> Result<()> {
        if self.empty() {
            return Ok(());
        }
        let wants = std::cmp::min(self.pending_ops, MAX_WAIT);
        tracing::trace!("submit_and_wait for {wants} pending operations to complete");
        self.ring.submit_and_wait(wants)?;
        Ok(())
    }

    fn empty(&self) -> bool {
        self.pending_ops == 0
    }

    /// Submit or resubmit a writev operation
    fn submit_writev(&mut self, key: u64, mut st: WritevState) {
        st.free_last_iov(&mut self.iov_pool);
        let mut iov_allocation = self.iov_pool.acquire().unwrap_or_else(|| {
            // Fallback: allocate a new one if pool is exhausted
            Box::new(
                [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOVEC_ENTRIES],
            )
        });
        let mut iov_count = 0;
        for (idx, buffer) in st
            .bufs
            .iter()
            .enumerate()
            .skip(st.current_buffer_idx)
            .take(MAX_IOVEC_ENTRIES)
        {
            let buf = buffer.borrow();
            let buf_slice = buf.as_slice();
            // ensure we are providing a pointer to the proper offset in the buffer
            let slice = if idx == st.current_buffer_idx {
                &buf_slice[st.current_buffer_offset..]
            } else {
                buf_slice
            };
            if slice.is_empty() {
                continue;
            }
            iov_allocation[iov_count] = libc::iovec {
                iov_base: slice.as_ptr() as *mut _,
                iov_len: slice.len(),
            };
            iov_count += 1;
        }
        // Store the pointers and get the pointer to the iovec array that we pass
        // to the writev operation, and keep the array itself alive
        let ptr = iov_allocation.as_ptr() as *mut libc::iovec;
        st.last_iov_allocation = Some(iov_allocation);

        let entry = with_fd!(st.file_id, |fd| {
            io_uring::opcode::Writev::new(fd, ptr, iov_count as u32)
                .offset(st.file_pos as u64)
                .build()
                .user_data(key)
        });
        // track the current state in case we get a partial write
        self.writev_states.insert(key, st);
        self.submit_entry(&entry);
    }

    fn handle_writev_completion(&mut self, mut state: WritevState, user_data: u64, result: i32) {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(result);
            tracing::error!("writev failed (user_data: {}): {}", user_data, err);
            state.free_last_iov(&mut self.iov_pool);
            completion_from_key(user_data).complete(result);
            return;
        }

        let written = result as usize;
        state.advance(written);
        match state.remaining() {
            0 => {
                tracing::info!(
                    "writev operation completed: wrote {} bytes",
                    state.total_written
                );
                // write complete, return iovec to pool
                state.free_last_iov(&mut self.iov_pool);
                completion_from_key(user_data).complete(state.total_written as i32);
            }
            remaining => {
                tracing::trace!(
                    "resubmitting writev operation for user_data {}: wrote {} bytes, remaining {}",
                    user_data,
                    written,
                    remaining
                );
                // partial write, submit next
                self.submit_writev(user_data, state);
            }
        }
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

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        while !c.is_completed() {
            self.step()?;
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        trace!("step()");
        let mut inner = self.inner.borrow_mut();
        let ring = &mut inner.ring;
        if ring.empty() {
            return Ok(());
        }
        ring.submit_and_wait()?;
        loop {
            let Some(cqe) = ring.ring.completion().next() else {
                return Ok(());
            };
            ring.pending_ops -= 1;
            let user_data = cqe.user_data();
            let result = cqe.result();
            turso_assert!(
                    user_data != 0,
                    "user_data must not be zero, we dont submit linked timeouts or cancelations that would cause this"
                );
            if let Some(state) = ring.writev_states.remove(&user_data) {
                // if we have ongoing writev state, handle it separately and don't call completion
                ring.handle_writev_completion(state, user_data, result);
                continue;
            }
            completion_from_key(user_data).complete(result)
        }
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

#[inline(always)]
/// use the callback pointer as the user_data for the operation as is
/// common practice for io_uring to prevent more indirection
fn get_key(c: Completion) -> u64 {
    Arc::into_raw(c.inner.clone()) as u64
}

#[inline(always)]
/// convert the user_data back to an Completion pointer
fn completion_from_key(key: u64) -> Completion {
    let c_inner = unsafe { Arc::from_raw(key as *const CompletionInner) };
    Completion { inner: c_inner }
}

pub struct UringFile {
    io: Rc<RefCell<InnerUringIO>>,
    file: std::fs::File,
    id: Option<u32>,
}

impl Deref for UringFile {
    type Target = std::fs::File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl UringFile {
    fn id(&self) -> Option<u32> {
        self.id
    }
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

    fn pread(&self, pos: usize, c: Completion) -> Result<Completion> {
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
        c: Completion,
    ) -> Result<Completion> {
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

    fn sync(&self, c: Completion) -> Result<Completion> {
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

    fn pwritev(
        &self,
        pos: usize,
        bufs: Vec<Arc<RefCell<crate::Buffer>>>,
        c: Completion,
    ) -> Result<Completion> {
        // for a single buffer use pwrite directly
        if bufs.len().eq(&1) {
            return self.pwrite(pos, bufs[0].clone(), c.clone());
        }
        tracing::trace!("pwritev(pos = {}, bufs.len() = {})", pos, bufs.len());
        let mut io = self.io.borrow_mut();
        // create state to track ongoing writev operation
        let state = WritevState::new(self, pos, bufs);
        io.ring.submit_writev(get_key(c.clone()), state);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let mut io = self.io.borrow_mut();
        let truncate = with_fd!(self, |fd| {
            io_uring::opcode::Ftruncate::new(fd, len as u64)
                .build()
                .user_data(get_key(c.clone()))
        });
        io.ring.submit_entry(&truncate);
        Ok(c)
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
