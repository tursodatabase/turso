#![allow(clippy::arc_with_non_send_sync)]

use super::{common, Completion, File, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use crate::{LimboError, MemoryIO, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, trace};

const ENTRIES: u32 = 512;
const SQPOLL_IDLE: u32 = 1000;

pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    writev_states: HashMap<u64, WritevState>,
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
        ring.submitter().register_files_sparse(8)?;
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                writev_states: HashMap::new(),
            },
            free_files: (0..8).collect(),
        };
        debug!("Using IO backend 'io-uring'");
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

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

struct WritevState {
    // fixed fd slot or 0xFFFF_FFFF if none
    file_id: u32,
    // absolute file offset for next submit
    pos: usize,
    // current buffer index in `bufs`
    idx: usize,
    // intra-buffer offset
    off: usize,
    bufs: Vec<Arc<RefCell<crate::Buffer>>>,
    // completion returned to caller
    user_c: Arc<Completion>,
    // we keep the last iovec allocation alive until CQE:
    // raw ptr to Box<[iovec]>
    last_iov: *mut libc::iovec,
    last_iov_len: usize,
}

impl WritevState {
    fn remaining(&self) -> usize {
        let mut total = 0;
        for (i, b) in self.bufs.iter().enumerate().skip(self.idx) {
            let r = b.borrow();
            let len = r.len();
            total += if i == self.idx { len - self.off } else { len };
        }
        total
    }

    /// Advance (idx, off, pos) after written bytes
    fn advance(&mut self, written: usize) {
        let mut rem = written;
        while rem > 0 {
            let len = {
                let r = self.bufs[self.idx].borrow();
                r.len()
            };
            let left = len - self.off;
            if rem < left {
                self.off += rem;
                self.pos += rem;
                rem = 0;
            } else {
                rem -= left;
                self.pos += left;
                self.idx += 1;
                self.off = 0;
            }
        }
    }

    fn free_last_iov(&mut self) {
        if !self.last_iov.is_null() {
            unsafe {
                drop(Box::from_raw(core::slice::from_raw_parts_mut(
                    self.last_iov,
                    self.last_iov_len,
                )))
            };
            self.last_iov = core::ptr::null_mut();
            self.last_iov_len = 0;
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

    fn wait_for_completion(&mut self) -> Result<()> {
        if self.pending_ops == 0 {
            return Ok(());
        }
        let wants = std::cmp::min(self.pending_ops, 8);
        tracing::info!("Waiting for {wants} pending operations to complete");
        self.ring.submit_and_wait(wants)?;
        Ok(())
    }

    fn empty(&self) -> bool {
        self.pending_ops == 0
    }

    fn submit_writev(&mut self, key: u64) {
        let st = self.writev_states.get_mut(&key).expect("state must exist");
        // build iovecs for the remaining slice (respect UIO_MAXIOV)
        const MAX_IOV: usize = libc::UIO_MAXIOV as usize;
        let mut iov = Vec::with_capacity(MAX_IOV);
        for (i, b) in st.bufs.iter().enumerate().skip(st.idx).take(MAX_IOV) {
            let r = b.borrow();
            let s = r.as_slice();
            let slice = if i == st.idx { &s[st.off..] } else { s };
            if slice.is_empty() {
                continue;
            }
            iov.push(libc::iovec {
                iov_base: slice.as_ptr() as *mut _,
                iov_len: slice.len(),
            });
        }

        // keep iov alive until CQE
        let boxed = iov.into_boxed_slice();
        let ptr = boxed.as_ptr() as *mut libc::iovec;
        let len = boxed.len();
        st.free_last_iov();
        st.last_iov = ptr;
        st.last_iov_len = len;
        // leak; freed when CQE processed
        let _ = Box::into_raw(boxed);
        let entry =
            io_uring::opcode::Writev::new(io_uring::types::Fixed(st.file_id), ptr, len as u32)
                .offset(st.pos as u64)
                .build()
                .user_data(key);
        self.submit_entry(&entry);
    }
}

#[inline(always)]
fn get_key(c: Arc<Completion>) -> u64 {
    Arc::into_raw(c) as u64
}

#[inline(always)]
fn completion_from_key(key: u64) -> Arc<Completion> {
    unsafe { Arc::from_raw(key as *const Completion) }
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
        {
            let mut inner = self.inner.borrow_mut();
            let ring = &mut inner.ring;

            if ring.empty() {
                return Ok(());
            }
            ring.wait_for_completion()?;
        }
        loop {
            let (had, user_data, result) = {
                let mut inner = self.inner.borrow_mut();
                let mut cq = inner.ring.ring.completion();
                if let Some(cqe) = cq.next() {
                    (true, cqe.user_data(), cqe.result())
                } else {
                    (false, 0, 0)
                }
            }; // cq dropped here

            if !had {
                break;
            }
            self.inner.borrow_mut().ring.pending_ops -= 1;
            if user_data == 0 {
                tracing::error!("CQE with user_data = 0");
                continue;
            }

            let mut inner = self.inner.borrow_mut();
            if let Some(mut st) = inner.ring.writev_states.remove(&user_data) {
                if result < 0 {
                    st.free_last_iov();
                    st.user_c.complete(result);
                } else {
                    let written = result as usize;
                    st.free_last_iov();
                    st.advance(written);
                    if st.remaining() == 0 {
                        st.user_c.complete(st.pos as i32);
                    } else {
                        inner.ring.writev_states.insert(user_data, st);
                        inner.ring.submit_writev(user_data); // safe: no CQ borrow alive
                    }
                }
            } else {
                completion_from_key(user_data).complete(result);
            }
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
    io: Rc<RefCell<InnerUringIO>>,
    file: std::fs::File,
    id: Option<u32>,
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

    fn pwritev(
        &self,
        pos: usize,
        bufs: Vec<Arc<RefCell<crate::Buffer>>>,
        user_c: Arc<Completion>,
    ) -> Result<Arc<Completion>> {
        // create state
        let key = get_key(user_c.clone());
        let mut io = self.io.borrow_mut();

        let state = WritevState {
            file_id: self.id.unwrap_or(u32::MAX),
            pos,
            idx: 0,
            off: 0,
            bufs,
            user_c: user_c.clone(),
            last_iov: core::ptr::null_mut(),
            last_iov_len: 0,
        };
        io.ring.writev_states.insert(key, state);
        io.ring.submit_writev(key);

        Ok(user_c.clone())
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
