#![allow(clippy::arc_with_non_send_sync)]

use super::{common, Completion, CompletionInner, File, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use crate::io::FsyncKind;
use crate::storage::wal::CKPT_BATCH_PAGES;
use crate::{turso_assert, LimboError, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::ptr::NonNull;
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
/// IOV_MAX is typically 1024
const MAX_IOVEC_ENTRIES: usize = CKPT_BATCH_PAGES;

/// Maximum number of I/O operations to wait for in a single run,
/// waiting for > 1 can reduce the amount of `io_uring_enter` syscalls we
/// make, but can increase single operation latency.
const MAX_WAIT: usize = 4;

/// One memory arena for DB pages and another for WAL frames
const ARENA_COUNT: usize = 2;

pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    writev_states: HashMap<u64, WritevState>,
    overflow: VecDeque<io_uring::squeue::Entry>,
    iov_pool: IovecPool,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    free_files: VecDeque<u32>,
    free_arenas: [Option<(NonNull<u8>, usize)>; ARENA_COUNT],
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
        // RL_MEMLOCK cap is typically 8MB, the current design is to have one large arena
        // registered at startup and therefore we can simply use the zero index, falling back
        // to similar logic as the existing buffer pool for cases where it is over capacity.
        ring.submitter()
            .register_buffers_sparse(ARENA_COUNT as u32)?;
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                overflow: VecDeque::new(),
                pending_ops: 0,
                writev_states: HashMap::new(),
                iov_pool: IovecPool::new(),
            },
            free_files: (0..FILES).collect(),
            free_arenas: [const { None }; ARENA_COUNT],
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
    bufs: Vec<Arc<crate::Buffer>>,
    /// we keep the last iovec allocation alive until final CQE
    last_iov_allocation: Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl WritevState {
    fn new(file: &UringFile, pos: usize, bufs: Vec<Arc<crate::Buffer>>) -> Self {
        let file_id = file
            .id()
            .map(Fd::Fixed)
            .unwrap_or_else(|| Fd::RawFd(file.as_raw_fd()));
        let total_len = bufs.iter().map(|b| b.len()).sum();
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
            let current_buf_len = self.bufs[self.current_buffer_idx].len();
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
        Err(crate::error::CompletionError::UringIOError(
            "unable to register file, no free slots available",
        )
        .into())
    }
    fn unregister_file(&mut self, id: u32) -> Result<()> {
        self.ring
            .ring
            .submitter()
            .register_files_update(id, &[-1])?;
        self.free_files.push_back(id);
        Ok(())
    }

    #[cfg(debug_assertions)]
    fn debug_check_fixed(&self, idx: u32, ptr: *const u8, len: usize) {
        let (base, blen) = self.free_arenas[idx as usize].expect("slot not registered");
        let start = base.as_ptr() as usize;
        let end = start + blen;
        let p = ptr as usize;
        turso_assert!(
            p >= start && p + len <= end,
            "Fixed operation, pointer out of registered range"
        );
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry) {
        trace!("submit_entry({:?})", entry);
        // we cannot push current entries before any overflow
        if self.flush_overflow().is_ok() {
            let pushed = unsafe {
                let mut sub = self.ring.submission();
                sub.push(entry).is_ok()
            };
            if pushed {
                self.pending_ops += 1;
                return;
            }
        }
        // if we were unable to push, add to overflow
        self.overflow.push_back(entry.clone());
        self.ring.submit().expect("submiting when full");
    }

    /// Flush overflow entries to submission queue when possible
    fn flush_overflow(&mut self) -> Result<()> {
        while !self.overflow.is_empty() {
            let sub_len = self.ring.submission().len();
            // safe subtraction as submission len will always be < ENTRIES
            let available_space = ENTRIES as usize - sub_len;
            if available_space == 0 {
                // No space available, always return error if we dont flush all overflow entries
                // to prevent out of order I/O operations
                return Err(crate::error::CompletionError::UringIOError("squeue full").into());
            }
            // Push as many as we can
            let to_push = std::cmp::min(available_space, self.overflow.len());
            unsafe {
                let mut sq = self.ring.submission();
                for _ in 0..to_push {
                    let entry = self.overflow.pop_front().unwrap();
                    if sq.push(&entry).is_err() {
                        // Unexpected failure, put it back
                        self.overflow.push_front(entry);
                        // No space available, always return error if we dont flush all overflow entries
                        // to prevent out of order I/O operations
                        return Err(
                            crate::error::CompletionError::UringIOError("squeue full").into()
                        );
                    }
                    self.pending_ops += 1;
                }
            }
        }
        Ok(())
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
        let mut last_end: Option<(*const u8, usize)> = None;
        for buffer in st.bufs.iter().skip(st.current_buffer_idx) {
            let ptr = buffer.as_ptr();
            let len = buffer.len();
            if let Some((last_ptr, last_len)) = last_end {
                // Check if this buffer is adjacent to the last
                if unsafe { last_ptr.add(last_len) } == ptr {
                    // Extend the last iovec instead of adding new
                    iov_allocation[iov_count - 1].iov_len += len;
                    last_end = Some((last_ptr, last_len + len));
                    continue;
                }
            }
            // Add new iovec
            iov_allocation[iov_count] = libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: len,
            };
            last_end = Some((ptr, len));
            iov_count += 1;
            if iov_count >= MAX_IOVEC_ENTRIES {
                break;
            }
        }
        // If we have coalesced everything into a single iovec, submit as a single`pwrite`
        if iov_count == 1 {
            let entry = with_fd!(st.file_id, |fd| {
                if let Some(id) = st.bufs[st.current_buffer_idx].fixed_id() {
                    io_uring::opcode::WriteFixed::new(
                        fd,
                        iov_allocation[0].iov_base as *const u8,
                        iov_allocation[0].iov_len as u32,
                        id as u16,
                    )
                    .offset(st.file_pos as u64)
                    .build()
                    .user_data(key)
                } else {
                    io_uring::opcode::Write::new(
                        fd,
                        iov_allocation[0].iov_base as *const u8,
                        iov_allocation[0].iov_len as u32,
                    )
                    .offset(st.file_pos as u64)
                    .build()
                    .user_data(key)
                }
            });
            self.submit_entry(&entry);
            return;
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
            path: std::path::PathBuf::from(path),
            io: self.inner.clone(),
            file,
            id,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(uring_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut inner = self.inner.borrow_mut();
        let ring = &mut inner.ring;
        ring.flush_overflow()?;
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

    fn register_fixed_buffer(&self, ptr: std::ptr::NonNull<u8>, len: usize) -> Result<u32> {
        turso_assert!(
            len % 512 == 0,
            "fixed buffer length must be logical block aligned"
        );
        let mut inner = self.inner.borrow_mut();
        let slot = inner.free_arenas.iter().position(|e| e.is_none()).ok_or(
            crate::error::CompletionError::UringIOError("no free fixed buffer slots"),
        )?;
        unsafe {
            inner.ring.ring.submitter().register_buffers_update(
                slot as u32,
                &[libc::iovec {
                    iov_base: ptr.as_ptr() as *mut libc::c_void,
                    iov_len: len,
                }],
                None,
            )?
        };
        inner.free_arenas[slot] = Some((ptr, len));
        Ok(slot as u32)
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
    path: std::path::PathBuf,
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
    fn path(&self) -> &std::path::Path {
        self.path.as_path()
    }
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
        let mut io = self.io.borrow_mut();
        let read_e = {
            let buf = r.buf();
            let ptr = buf.as_mut_ptr();
            let len = buf.len();
            with_fd!(self, |fd| {
                if let Some(idx) = buf.fixed_id() {
                    trace!(
                        "pread_fixed(pos = {}, length = {}, idx = {})",
                        pos,
                        len,
                        idx
                    );
                    #[cfg(debug_assertions)]
                    {
                        io.debug_check_fixed(idx, ptr, len);
                    }
                    io_uring::opcode::ReadFixed::new(fd, ptr, len as u32, idx as u16)
                        .offset(pos as u64)
                        .build()
                        .user_data(get_key(c.clone()))
                } else {
                    trace!("pread(pos = {}, length = {})", pos, len);
                    // Use Read opcode if fixed buffer is not available
                    io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), len as u32)
                        .offset(pos as u64)
                        .build()
                        .user_data(get_key(c.clone()))
                }
            })
        };
        io.ring.submit_entry(&read_e);
        Ok(c)
    }

    fn pwrite(&self, pos: usize, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let mut io = self.io.borrow_mut();
        let write = {
            let ptr = buffer.as_ptr();
            let len = buffer.len();
            with_fd!(self, |fd| {
                if let Some(idx) = buffer.fixed_id() {
                    trace!(
                        "pwrite_fixed(pos = {}, length = {}, idx= {})",
                        pos,
                        len,
                        idx
                    );
                    #[cfg(debug_assertions)]
                    {
                        io.debug_check_fixed(idx, ptr, len);
                    }
                    io_uring::opcode::WriteFixed::new(fd, ptr, len as u32, idx as u16)
                        .offset(pos as u64)
                        .build()
                        .user_data(get_key(c.clone()))
                } else {
                    trace!("pwrite(pos = {}, length = {})", pos, buffer.len());
                    io_uring::opcode::Write::new(fd, ptr, len as u32)
                        .offset(pos as u64)
                        .build()
                        .user_data(get_key(c.clone()))
                }
            })
        };
        io.ring.submit_entry(&write);
        Ok(c)
    }

    fn sync(&self, kind: FsyncKind, c: Completion) -> Result<Completion> {
        let mut io = self.io.borrow_mut();
        trace!("sync()");
        let sync = with_fd!(self, |fd| {
            io_uring::opcode::Fsync::new(fd)
                .flags(if matches!(kind, FsyncKind::Full) {
                    io_uring::types::FsyncFlags::empty()
                } else {
                    io_uring::types::FsyncFlags::DATASYNC
                })
                .build()
                .user_data(get_key(c.clone()))
        });
        io.ring.submit_entry(&sync);
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: usize,
        bufs: Vec<Arc<crate::Buffer>>,
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
