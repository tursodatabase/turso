use super::{Completion, File, MemoryIO, OpenFlags, IO};
use crate::error::LimboError;
use crate::io::clock::{Clock, Instant};
use crate::io::common;
use crate::Result;
use polling::{Event, Events, Poller};
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation, OFlags, OpenOptionsExt},
    io::Errno,
};
use std::os::fd::RawFd;
use std::{cell::UnsafeCell, mem::MaybeUninit, sync::Mutex};

use std::{io::ErrorKind, sync::Arc};
#[cfg(feature = "fs")]
use tracing::debug;
use tracing::{instrument, trace, Level};

struct OwnedCallbacks(UnsafeCell<Callbacks>);
// We assume we locking on IO level is done by user.
unsafe impl Send for OwnedCallbacks {}
unsafe impl Sync for OwnedCallbacks {}
struct BorrowedCallbacks<'io>(UnsafeCell<&'io mut Callbacks>);

impl OwnedCallbacks {
    #[allow(dead_code)]
    fn new() -> Self {
        Self(UnsafeCell::new(Callbacks::new()))
    }
    fn as_mut<'io>(&self) -> &'io mut Callbacks {
        unsafe { &mut *self.0.get() }
    }

    fn is_empty(&self) -> bool {
        self.as_mut().inline_count == 0
    }

    fn remove(&self, fd: usize) -> Option<CompletionCallback> {
        let callbacks = unsafe { &mut *self.0.get() };
        callbacks.remove(fd)
    }
}

impl BorrowedCallbacks<'_> {
    fn insert(&self, fd: usize, callback: CompletionCallback) {
        let callbacks = unsafe { &mut *self.0.get() };
        callbacks.insert(fd, callback);
    }
}

struct EventsHandler(UnsafeCell<Events>);

impl EventsHandler {
    #[allow(dead_code)]
    fn new() -> Self {
        Self(UnsafeCell::new(Events::new()))
    }

    fn clear(&self) {
        let events = unsafe { &mut *self.0.get() };
        events.clear();
    }

    fn iter(&self) -> impl Iterator<Item = Event> {
        let events = unsafe { &*self.0.get() };
        events.iter()
    }

    fn as_mut<'io>(&self) -> &'io mut Events {
        unsafe { &mut *self.0.get() }
    }
}
struct PollHandler(UnsafeCell<Poller>);
struct BorrowedPollHandler<'io>(UnsafeCell<&'io mut Poller>);

impl BorrowedPollHandler<'_> {
    fn add(&self, fd: &rustix::fd::BorrowedFd, event: Event) -> Result<()> {
        let poller = unsafe { &mut *self.0.get() };
        unsafe { poller.add(fd, event)? }
        Ok(())
    }
}

impl PollHandler {
    #[allow(dead_code)]
    fn new() -> Self {
        Self(UnsafeCell::new(Poller::new().unwrap()))
    }
    fn wait(&self, events: &mut Events, timeout: Option<std::time::Duration>) -> Result<()> {
        let poller = unsafe { &mut *self.0.get() };
        poller.wait(events, timeout)?;
        Ok(())
    }

    fn as_mut<'io>(&self) -> &'io mut Poller {
        unsafe { &mut *self.0.get() }
    }
}

type CallbackEntry = (usize, CompletionCallback);

const FD_INLINE_SIZE: usize = 32;

struct Callbacks {
    inline_entries: [MaybeUninit<(usize, CompletionCallback)>; FD_INLINE_SIZE],
    heap_entries: Vec<CallbackEntry>,
    inline_count: usize,
}

impl Callbacks {
    fn new() -> Self {
        Self {
            inline_entries: [const { MaybeUninit::uninit() }; FD_INLINE_SIZE],
            heap_entries: Vec::new(),
            inline_count: 0,
        }
    }

    fn insert(&mut self, fd: usize, callback: CompletionCallback) {
        if self.inline_count < FD_INLINE_SIZE {
            self.inline_entries[self.inline_count].write((fd, callback));
            self.inline_count += 1;
        } else {
            self.heap_entries.push((fd, callback));
        }
    }

    fn remove(&mut self, fd: usize) -> Option<CompletionCallback> {
        if let Some(pos) = self.find_inline(fd) {
            let (_, callback) = unsafe { self.inline_entries[pos].assume_init_read() };

            // if not the last element, move the last valid entry into this position
            if pos < self.inline_count - 1 {
                let last_valid =
                    unsafe { self.inline_entries[self.inline_count - 1].assume_init_read() };
                self.inline_entries[pos].write(last_valid);
            }

            self.inline_count -= 1;
            return Some(callback);
        }

        if let Some(pos) = self.heap_entries.iter().position(|&(k, _)| k == fd) {
            return Some(self.heap_entries.swap_remove(pos).1);
        }
        None
    }

    fn find_inline(&self, fd: usize) -> Option<usize> {
        (0..self.inline_count)
            .find(|&i| unsafe { self.inline_entries[i].assume_init_ref().0 == fd })
    }
}

impl Drop for Callbacks {
    fn drop(&mut self) {
        for i in 0..self.inline_count {
            unsafe { self.inline_entries[i].assume_init_drop() };
        }
    }
}

/// UnixIO lives longer than any of the files it creates, so it is
/// safe to store references to it's internals in the UnixFiles
pub struct UnixIO {
    poller: PollHandler,
    events: EventsHandler,
    callbacks: OwnedCallbacks,
}

unsafe impl Send for UnixIO {}
unsafe impl Sync for UnixIO {}

impl UnixIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {
            poller: PollHandler::new(),
            events: EventsHandler::new(),
            callbacks: OwnedCallbacks::new(),
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

fn try_pwritev_raw(
    fd: RawFd,
    off: u64,
    bufs: &[Arc<crate::Buffer>],
    start_idx: usize,
    start_off: usize,
) -> std::io::Result<usize> {
    const MAX_IOV: usize = 1024;
    let iov_len = std::cmp::min(bufs.len() - start_idx, MAX_IOV);
    let mut iov: Vec<libc::iovec> = Vec::with_capacity(iov_len);

    let mut last_end: Option<(*const u8, usize)> = None;
    let mut iov_count = 0;
    for (i, b) in bufs.iter().enumerate().skip(start_idx).take(iov_len) {
        let s = b.as_slice();
        let ptr = if i == start_idx { &s[start_off..] } else { s }.as_ptr();
        let len = b.len();

        if let Some((last_ptr, last_len)) = last_end {
            // Check if this buffer is adjacent to the last
            if unsafe { last_ptr.add(last_len) } == ptr {
                // Extend the last iovec instead of adding new
                iov[iov_count - 1].iov_len += len;
                last_end = Some((last_ptr, last_len + len));
                continue;
            }
        }
        last_end = Some((ptr, len));
        iov_count += 1;
        iov.push(libc::iovec {
            iov_base: ptr as *mut libc::c_void,
            iov_len: len,
        });
    }
    let n = if iov.len().eq(&1) {
        unsafe {
            libc::pwrite(
                fd,
                iov[0].iov_base as *const libc::c_void,
                iov[0].iov_len,
                off as i64,
            )
        }
    } else {
        unsafe { libc::pwritev(fd, iov.as_ptr(), iov.len() as i32, off as i64) }
    };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
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
        let unix_file = Arc::new(UnixFile {
            file: Arc::new(Mutex::new(file)),
            poller: BorrowedPollHandler(self.poller.as_mut().into()),
            callbacks: BorrowedCallbacks(self.callbacks.as_mut().into()),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            unix_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(unix_file)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn run_once(&self) -> Result<()> {
        if self.callbacks.is_empty() {
            return Ok(());
        }

        self.events.clear();
        trace!("run_once() waits for events");
        self.poller.wait(self.events.as_mut(), None)?;

        for event in self.events.iter() {
            let key = event.key;
            let cb = match self.callbacks.remove(key) {
                Some(cb) => cb,
                None => continue, // could have been completed/removed already
            };

            match cb {
                CompletionCallback::Read(ref file, c, pos) => {
                    let f = file
                        .lock()
                        .map_err(|e| LimboError::LockingError(e.to_string()))?;
                    let r = c.as_read();
                    let buf = r.buf();
                    match rustix::io::pread(f.as_fd(), buf.as_mut_slice(), pos as u64) {
                        Ok(n) => c.complete(n as i32),
                        Err(Errno::AGAIN) => {
                            // re-arm
                            unsafe { self.poller.as_mut().add(&f.as_fd(), Event::readable(key))? };
                            self.callbacks.as_mut().insert(
                                key,
                                CompletionCallback::Read(file.clone(), c.clone(), pos),
                            );
                        }
                        Err(e) => return Err(e.into()),
                    }
                }

                CompletionCallback::Write(ref file, c, buf, pos) => {
                    let f = file
                        .lock()
                        .map_err(|e| LimboError::LockingError(e.to_string()))?;
                    match rustix::io::pwrite(f.as_fd(), buf.as_slice(), pos as u64) {
                        Ok(n) => c.complete(n as i32),
                        Err(Errno::AGAIN) => {
                            unsafe { self.poller.as_mut().add(&f.as_fd(), Event::writable(key))? };
                            self.callbacks.as_mut().insert(
                                key,
                                CompletionCallback::Write(file.clone(), c, buf.clone(), pos),
                            );
                        }
                        Err(e) => return Err(e.into()),
                    }
                }

                CompletionCallback::Writev(file, c, bufs, mut pos, mut idx, mut off) => {
                    let f = file
                        .lock()
                        .map_err(|e| LimboError::LockingError(e.to_string()))?;
                    // keep trying until WouldBlock or we're done with this event
                    match try_pwritev_raw(f.as_raw_fd(), pos as u64, &bufs, idx, off) {
                        Ok(written) => {
                            // advance through buffers
                            let mut rem = written;
                            while rem > 0 {
                                let len = bufs[idx].len();
                                let left = len - off;
                                if rem < left {
                                    off += rem;
                                    rem = 0;
                                } else {
                                    rem -= left;
                                    idx += 1;
                                    off = 0;
                                    if idx == bufs.len() {
                                        break;
                                    }
                                }
                            }
                            pos += written;

                            if idx == bufs.len() {
                                c.complete(pos as i32);
                            } else {
                                // Not finished; re-arm and store updated state
                                unsafe {
                                    self.poller.as_mut().add(&f.as_fd(), Event::writable(key))?
                                };
                                self.callbacks.as_mut().insert(
                                    key,
                                    CompletionCallback::Writev(
                                        file.clone(),
                                        c.clone(),
                                        bufs,
                                        pos,
                                        idx,
                                        off,
                                    ),
                                );
                            }
                            break;
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            // re-arm with same state
                            unsafe { self.poller.as_mut().add(&f.as_fd(), Event::writable(key))? };
                            self.callbacks.as_mut().insert(
                                key,
                                CompletionCallback::Writev(
                                    file.clone(),
                                    c.clone(),
                                    bufs,
                                    pos,
                                    idx,
                                    off,
                                ),
                            );
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }

        Ok(())
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
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
    Read(Arc<Mutex<std::fs::File>>, Completion, usize),
    Write(
        Arc<Mutex<std::fs::File>>,
        Completion,
        Arc<crate::Buffer>,
        usize,
    ),
    Writev(
        Arc<Mutex<std::fs::File>>,
        Completion,
        Vec<Arc<crate::Buffer>>,
        usize, // absolute file offset
        usize, // buf index
        usize, // intra-buf offset
    ),
}

pub struct UnixFile<'io> {
    #[allow(clippy::arc_with_non_send_sync)]
    file: Arc<Mutex<std::fs::File>>,
    poller: BorrowedPollHandler<'io>,
    callbacks: BorrowedCallbacks<'io>,
}
unsafe impl Send for UnixFile<'_> {}
unsafe impl Sync for UnixFile<'_> {}

impl File for UnixFile<'_> {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.lock().unwrap();
        let fd = fd.as_fd();
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
        let fd = self.file.lock().unwrap();
        let fd = fd.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pread(&self, pos: usize, c: Completion) -> Result<Completion> {
        let file = self.file.lock().unwrap();
        let result = {
            let r = c.as_read();
            let buf = r.buf();
            rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
        };
        match result {
            Ok(n) => {
                trace!("pread n: {}", n);
                // Read succeeded immediately
                c.complete(n as i32);
                Ok(c)
            }
            Err(Errno::AGAIN) => {
                trace!("pread blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                self.poller
                    .add(&file.as_fd(), Event::readable(fd as usize))?;
                {
                    self.callbacks.insert(
                        fd as usize,
                        CompletionCallback::Read(self.file.clone(), c.clone(), pos),
                    );
                }
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwrite(&self, pos: usize, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let file = self.file.lock().unwrap();
        let result = { rustix::io::pwrite(file.as_fd(), buffer.as_slice(), pos as u64) };
        match result {
            Ok(n) => {
                trace!("pwrite n: {}", n);
                // Read succeeded immediately
                c.complete(n as i32);
                Ok(c)
            }
            Err(Errno::AGAIN) => {
                trace!("pwrite blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                self.poller
                    .add(&file.as_fd(), Event::writable(fd as usize))?;
                self.callbacks.insert(
                    fd as usize,
                    CompletionCallback::Write(self.file.clone(), c.clone(), buffer.clone(), pos),
                );
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwritev(
        &self,
        pos: usize,
        buffers: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        if buffers.len().eq(&1) {
            // use `pwrite` for single buffer
            return self.pwrite(pos, buffers[0].clone(), c);
        }
        let file = self
            .file
            .lock()
            .map_err(|e| LimboError::LockingError(e.to_string()))?;

        match try_pwritev_raw(file.as_raw_fd(), pos as u64, &buffers, 0, 0) {
            Ok(written) => {
                trace!("pwritev wrote {written}");
                c.complete(written as i32);
                Ok(c)
            }
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    trace!("pwritev blocks");
                } else {
                    return Err(e.into());
                }
                // Set up state so we can resume later
                let fd = file.as_raw_fd();
                self.poller
                    .add(&file.as_fd(), Event::writable(fd as usize))?;
                let buf_idx = 0;
                let buf_offset = 0;
                self.callbacks.insert(
                    fd as usize,
                    CompletionCallback::Writev(
                        self.file.clone(),
                        c.clone(),
                        buffers,
                        pos,
                        buf_idx,
                        buf_offset,
                    ),
                );
                Ok(c)
            }
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        let file = self.file.lock().unwrap();
        let result = fs::fsync(file.as_fd());
        match result {
            Ok(()) => {
                trace!("fsync");
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn size(&self) -> Result<u64> {
        let file = self.file.lock().unwrap();
        Ok(file.metadata()?.len())
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let file = self.file.lock().map_err(|e| {
            LimboError::LockingError(format!("Failed to lock file for truncation: {e}"))
        })?;
        let result = file.set_len(len as u64);
        match result {
            Ok(()) => {
                trace!("file truncated to len=({})", len);
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl Drop for UnixFile<'_> {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use turso_macros::turso_test;

    use super::*;

    #[turso_test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
    }
}
