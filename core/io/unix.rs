use crate::error::LimboError;
use crate::io::common;
use crate::Result;

use super::{Completion, File, MemoryIO, OpenFlags, IO};
use crate::io::clock::{Clock, Instant};
use polling::{Event, Events, Poller};
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation, OFlags, OpenOptionsExt},
    io::Errno,
};
use std::{
    cell::{RefCell, UnsafeCell},
    mem::MaybeUninit,
};
use std::{
    io::{ErrorKind, Read, Seek, Write},
    sync::Arc,
};
use tracing::{debug, instrument, trace, Level};

struct OwnedCallbacks(UnsafeCell<Callbacks>);
// We assume we locking on IO level is done by user.
unsafe impl Send for OwnedCallbacks {}
unsafe impl Sync for OwnedCallbacks {}
struct BorrowedCallbacks<'io>(UnsafeCell<&'io mut Callbacks>);

impl OwnedCallbacks {
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
            file: Arc::new(RefCell::new(file)),
            poller: BorrowedPollHandler(self.poller.as_mut().into()),
            callbacks: BorrowedCallbacks(self.callbacks.as_mut().into()),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            unix_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(unix_file)
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn run_once(&self) -> Result<()> {
        if self.callbacks.is_empty() {
            return Ok(());
        }
        self.events.clear();
        trace!("run_once() waits for events");
        self.poller.wait(self.events.as_mut(), None)?;

        for event in self.events.iter() {
            if let Some(cf) = self.callbacks.remove(event.key) {
                let result = match cf {
                    CompletionCallback::Read(ref file, ref c, pos) => {
                        let mut file = file.borrow_mut();
                        let r = c.as_read();
                        let mut buf = r.buf_mut();
                        file.seek(std::io::SeekFrom::Start(pos as u64))?;
                        file.read(buf.as_mut_slice())
                    }
                    CompletionCallback::Write(ref file, _, ref buf, pos) => {
                        let mut file = file.borrow_mut();
                        let buf = buf.borrow();
                        file.seek(std::io::SeekFrom::Start(pos as u64))?;
                        file.write(buf.as_slice())
                    }
                };
                match result {
                    Ok(n) => match &cf {
                        CompletionCallback::Read(_, ref c, _) => c.complete(0),
                        CompletionCallback::Write(_, ref c, _, _) => c.complete(n as i32),
                    },
                    Err(e) => return Err(e.into()),
                }
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
    Read(Arc<RefCell<std::fs::File>>, Arc<Completion>, usize),
    Write(
        Arc<RefCell<std::fs::File>>,
        Arc<Completion>,
        Arc<RefCell<crate::Buffer>>,
        usize,
    ),
}

pub struct UnixFile<'io> {
    #[allow(clippy::arc_with_non_send_sync)]
    file: Arc<RefCell<std::fs::File>>,
    poller: BorrowedPollHandler<'io>,
    callbacks: BorrowedCallbacks<'io>,
}
unsafe impl Send for UnixFile<'_> {}
unsafe impl Sync for UnixFile<'_> {}

impl File for UnixFile<'_> {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.borrow();
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
        let fd = self.file.borrow();
        let fd = fd.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn pread(&self, pos: usize, c: Completion) -> Result<Arc<Completion>> {
        let file = self.file.borrow();
        let result = {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
        };
        let c = Arc::new(c);
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

    #[instrument(err, skip_all, level = Level::INFO)]
    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<crate::Buffer>>,
        c: Completion,
    ) -> Result<Arc<Completion>> {
        let file = self.file.borrow();
        let result = {
            let buf = buffer.borrow();
            rustix::io::pwrite(file.as_fd(), buf.as_slice(), pos as u64)
        };
        let c = Arc::new(c);
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
                    .add(&file.as_fd(), Event::readable(fd as usize))?;
                self.callbacks.insert(
                    fd as usize,
                    CompletionCallback::Write(self.file.clone(), c.clone(), buffer.clone(), pos),
                );
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn sync(&self, c: Completion) -> Result<Arc<Completion>> {
        let file = self.file.borrow();
        let result = fs::fsync(file.as_fd());
        let c = Arc::new(c);
        match result {
            Ok(()) => {
                trace!("fsync");
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata()?.len())
    }
}

impl Drop for UnixFile<'_> {
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
