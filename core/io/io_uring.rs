use super::{common, Completion, File, OpenFlags, WriteCompletion, IO};
use crate::io::clock::{Clock, Instant};
use crate::{LimboError, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use rustix::io_uring::iovec;
use std::cell::RefCell;
use std::fmt;
use std::io::ErrorKind;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, trace};

/// Configuration options for the io_uring IO backend.
///
/// This struct allows tuning of the io_uring backend parameters for optimal performance
/// in different use cases.
///
/// # Examples
///
/// ```rust,ignore
/// use limbo_core::io::UringOpts;
///
/// // Create custom options with higher concurrency
/// let opts = UringOpts {
///     max_iovecs: 2048,  // Allow more concurrent operations
///     sqpoll_idle: 500,  // More aggressive polling
/// };
///
/// // Use default options
/// let default_opts = UringOpts::default();
/// ```
#[derive(Debug)]
pub struct UringOpts {
    /// Maximum number of concurrent IO operations that can be in flight.
    /// This determines the size of the submission and completion queues.
    /// Higher values allow more concurrent operations but use more memory.
    /// Default is 1024.
    pub max_iovecs: u32,
    /// Time in milliseconds that the kernel will wait before entering idle state
    /// in the submission queue polling thread. Lower values mean more aggressive
    /// polling but higher CPU usage. Default is 1000ms.
    pub sqpoll_idle: u32,
}

impl Default for UringOpts {
    fn default() -> Self {
        Self {
            max_iovecs: 1024,
            sqpoll_idle: 1000,
        }
    }
}

const MAX_IOVECS: u32 = 1024;

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

/// An IO backend implementation using Linux's io_uring interface.
///
/// This implementation provides high-performance asynchronous IO operations using
/// the io_uring interface available in recent Linux kernels. It supports:
/// - Asynchronous reads and writes
/// - File locking
/// - Direct IO (when supported by the filesystem)
/// - Configurable concurrency limits
///
/// # Performance Characteristics
///
/// - Uses a single submission queue and completion queue for all operations
/// - Supports high concurrency through the io_uring interface
/// - Minimizes system calls by batching operations
/// - Uses kernel polling for reduced latency
///
/// # Examples
///
/// ```rust,ignore
/// use limbo_core::io::UringIO;
///
/// // Create with default options
/// let io = UringIO::new()?;
///
/// // Create with custom options for high concurrency
/// let opts = UringOpts {
///     max_iovecs: 2048,
///     sqpoll_idle: 500,
/// };
/// let io = UringIO::with_opts(opts)?;
///
/// // Open a file for reading and writing
/// let file = io.open_file("test.db", OpenFlags::Create, true)?;
///
/// // Perform async read
/// let buffer = Buffer::allocate(4096, Arc::new(|_| {}));
/// file.pread(0, Completion::Read(ReadCompletion::new(
///     buffer.clone(),
///     Box::new(move |buf| {
///         // Handle read completion
///         println!("Read {} bytes", buf.borrow().len());
///     }),
/// )))?;
///
/// // Run the IO loop to process completions
/// io.run_once()?;
/// ```
///
/// # Notes
///
/// - This implementation is only available on Linux systems with io_uring support
/// - The io_uring interface requires Linux kernel 5.1 or later
/// - Performance may vary depending on kernel version and system configuration
/// - Consider using `max_iovecs` to tune for your specific workload
pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pending: [Option<Completion>; MAX_IOVECS as usize + 1],
    key: u64,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    iovecs: [iovec; MAX_IOVECS as usize],
    next_iovec: usize,
}

impl UringIO {
    /// Creates a new UringIO instance with default options.
    ///
    /// The default options are:
    /// - max_iovecs: 1024 (allows 1024 concurrent operations)
    /// - sqpoll_idle: 1000 (1 second idle timeout)
    ///
    /// # Returns
    ///
    /// A Result containing the new UringIO instance or an error if initialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The io_uring interface is not available
    /// - The kernel version is too old
    /// - System resources are insufficient
    pub fn new() -> Result<Self> {
        Self::with_opts(UringOpts::default())
    }

    /// Creates a new UringIO instance with custom options.
    ///
    /// This allows tuning the io_uring backend for specific use cases.
    ///
    /// # Arguments
    ///
    /// * `opts` - Configuration options for the io_uring backend
    ///
    /// # Returns
    ///
    /// A Result containing the new UringIO instance or an error if initialization fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::{UringIO, UringOpts};
    ///
    /// // Create with custom options for high concurrency
    /// let opts = UringOpts {
    ///     max_iovecs: 2048,  // Allow more concurrent operations
    ///     sqpoll_idle: 500,  // More aggressive polling
    /// };
    /// let io = UringIO::with_opts(opts)?;
    /// ```
    pub fn with_opts(opts: UringOpts) -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_sqpoll(opts.sqpoll_idle)
            .build(opts.max_iovecs)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(opts.max_iovecs)?,
        };
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: [const { None }; MAX_IOVECS as usize + 1],
                key: 0,
            },
            iovecs: [iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS as usize],
            next_iovec: 0,
        };
        debug!(
            "Using IO backend 'io-uring' with max_iovecs={}",
            opts.max_iovecs
        );
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

impl InnerUringIO {
    pub fn get_iovec(&mut self, buf: *const u8, len: usize) -> &iovec {
        let iovec = &mut self.iovecs[self.next_iovec];
        iovec.iov_base = buf as *mut std::ffi::c_void;
        iovec.iov_len = len;
        self.next_iovec = (self.next_iovec + 1) % MAX_IOVECS as usize;
        iovec
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Completion) {
        trace!("submit_entry({:?})", entry);
        self.pending[entry.get_user_data() as usize] = Some(c);
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
        if self.key == MAX_IOVECS as u64 {
            let key = self.key;
            self.key = 0;
            return key;
        }
        self.key
    }
}

impl IO for UringIO {
    /// Opens a file for reading and writing using io_uring.
    ///
    /// This method supports:
    /// - Creating new files (with `OpenFlags::Create`)
    /// - Direct IO (when supported by the filesystem)
    /// - File locking (unless disabled via environment variable)
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    /// * `flags` - Open flags (Create or None)
    /// * `direct` - Whether to use direct IO
    ///
    /// # Returns
    ///
    /// A Result containing an Arc<dyn File> or an error if the file cannot be opened.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::{UringIO, OpenFlags};
    ///
    /// let io = UringIO::new()?;
    ///
    /// // Open existing file
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// // Create new file with direct IO
    /// let file = io.open_file("new.db", OpenFlags::Create, true)?;
    /// ```
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
        let uring_file = Arc::new(UringFile {
            io: self.inner.clone(),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(true)?;
        }
        Ok(uring_file)
    }

    /// Processes any pending IO completions.
    ///
    /// This method should be called periodically to handle completed IO operations.
    /// It will:
    /// - Wait for at least one completion
    /// - Process all available completions
    /// - Call the appropriate completion callbacks
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::UringIO;
    ///
    /// let io = UringIO::new()?;
    ///
    /// // Process any pending completions
    /// io.run_once()?;
    /// ```
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
            {
                if let Some(c) = ring.pending[cqe.user_data() as usize].as_ref() {
                    c.complete(cqe.result());
                }
            }
            ring.pending[cqe.user_data() as usize] = None;
        }
        Ok(())
    }

    /// Generates a random number using the system's random number generator.
    ///
    /// This is used internally for generating unique keys for IO operations.
    ///
    /// # Returns
    ///
    /// A random 64-bit integer.
    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
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

/// A file handle for the io_uring IO backend.
///
/// This struct provides the actual file operations using io_uring.
/// It supports:
/// - Asynchronous reads and writes
/// - File locking
/// - Direct IO
///
/// # Examples
///
/// ```rust,ignore
/// use limbo_core::io::{UringIO, OpenFlags, Completion, ReadCompletion, Buffer};
///
/// let io = UringIO::new()?;
/// let file = io.open_file("test.db", OpenFlags::None, true)?;
///
/// // Perform async read
/// let buffer = Buffer::allocate(4096, Arc::new(|_| {}));
/// file.pread(0, Completion::Read(ReadCompletion::new(
///     buffer.clone(),
///     Box::new(move |buf| {
///         println!("Read {} bytes", buf.borrow().len());
///     }),
/// )))?;
/// ```
pub struct UringFile {
    io: Rc<RefCell<InnerUringIO>>,
    file: std::fs::File,
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}

impl File for UringFile {
    /// Locks the file for exclusive or shared access.
    ///
    /// This implements file locking using fcntl F_SETLK.
    /// The lock is non-blocking and will be released when the file is closed.
    ///
    /// # Arguments
    ///
    /// * `exclusive` - Whether to acquire an exclusive lock
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the lock operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::UringIO;
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// // Acquire exclusive lock
    /// file.lock_file(true)?;
    /// ```
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

    /// Unlocks the file.
    ///
    /// This releases any lock previously acquired with `lock_file`.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the unlock operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::UringIO;
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// // Release lock
    /// file.unlock_file()?;
    /// ```
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

    /// Performs an asynchronous read operation.
    ///
    /// This method submits a read operation to the io_uring submission queue.
    /// The completion callback will be called when the read completes.
    ///
    /// # Arguments
    ///
    /// * `pos` - Position in the file to read from
    /// * `c` - Completion callback to handle the read result
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of submitting the read operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::{UringIO, Completion, ReadCompletion, Buffer};
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// let buffer = Buffer::allocate(4096, Arc::new(|_| {}));
    /// file.pread(0, Completion::Read(ReadCompletion::new(
    ///     buffer.clone(),
    ///     Box::new(move |buf| {
    ///         println!("Read {} bytes", buf.borrow().len());
    ///     }),
    /// )))?;
    /// ```
    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        trace!("pread(pos = {}, length = {})", pos, r.buf().len());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        let read_e = {
            let mut buf = r.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            let iovec = io.get_iovec(buf, len);
            io_uring::opcode::Readv::new(fd, iovec as *const iovec as *const libc::iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(&read_e, c);
        Ok(())
    }

    /// Performs an asynchronous write operation.
    ///
    /// This method submits a write operation to the io_uring submission queue.
    /// The completion callback will be called when the write completes.
    ///
    /// # Arguments
    ///
    /// * `pos` - Position in the file to write to
    /// * `buffer` - Buffer containing the data to write
    /// * `c` - Completion callback to handle the write result
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of submitting the write operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::{UringIO, Completion, WriteCompletion, Buffer};
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// let data = vec![1, 2, 3, 4];
    /// let buffer = Buffer::new(Pin::new(data), Arc::new(|_| {}));
    /// file.pwrite(0, Arc::new(RefCell::new(buffer)), Completion::Write(
    ///     WriteCompletion::new(Box::new(move |result| {
    ///         println!("Wrote {} bytes", result);
    ///     }))
    /// ))?;
    /// ```
    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<crate::Buffer>>, c: Completion) -> Result<()> {
        let mut io = self.io.borrow_mut();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            let buf = buffer.borrow();
            trace!("pwrite(pos = {}, length = {})", pos, buf.len());
            let iovec = io.get_iovec(buf.as_ptr(), buf.len());
            io_uring::opcode::Writev::new(fd, iovec as *const iovec as *const libc::iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(
            &write,
            Completion::Write(WriteCompletion::new(Box::new(move |result| {
                c.complete(result);
                // NOTE: Explicitly reference buffer to ensure it lives until here
                let _ = buffer.borrow();
            }))),
        );
        Ok(())
    }

    /// Performs an asynchronous sync operation.
    ///
    /// This method submits a sync operation to the io_uring submission queue.
    /// The completion callback will be called when the sync completes.
    ///
    /// # Arguments
    ///
    /// * `c` - Completion callback to handle the sync result
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of submitting the sync operation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::{UringIO, Completion, SyncCompletion};
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// file.sync(Completion::Sync(SyncCompletion::new(
    ///     Box::new(move |result| {
    ///         println!("Sync completed with result {}", result);
    ///     })
    /// )))?;
    /// ```
    fn sync(&self, c: Completion) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        trace!("sync()");
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(io.ring.get_key());
        io.ring.submit_entry(&sync, c);
        Ok(())
    }

    /// Gets the current size of the file.
    ///
    /// # Returns
    ///
    /// A Result containing the file size in bytes or an error if the size cannot be determined.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use limbo_core::io::UringIO;
    ///
    /// let io = UringIO::new()?;
    /// let file = io.open_file("test.db", OpenFlags::None, true)?;
    ///
    /// let size = file.size()?;
    /// println!("File size: {} bytes", size);
    /// ```
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
