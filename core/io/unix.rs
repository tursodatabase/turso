use super::{Completion, File, OpenFlags, IO};
use crate::error::{io_error, CompletionError, LimboError};
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::Result;
use crate::sync::Mutex;
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation},
};
use std::os::fd::RawFd;

use std::{io::ErrorKind, sync::Arc};
#[cfg(feature = "fs")]
use tracing::debug;
use tracing::{instrument, trace, Level};

pub struct UnixIO {}

impl UnixIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

impl Clock for UnixIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
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
        let slice = if i == start_idx { &s[start_off..] } else { s };
        let ptr = slice.as_ptr();
        let len = slice.len();

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
    // On Android, off_t is i32. Cast to libc::off_t instead of hardcoding i64 for portability.
    let n = if iov.len().eq(&1) {
        unsafe {
            libc::pwrite(
                fd,
                iov[0].iov_base as *const libc::c_void,
                iov[0].iov_len,
                off as libc::off_t,
            )
        }
    } else {
        unsafe { libc::pwritev(fd, iov.as_ptr(), iov.len() as i32, off as libc::off_t) }
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
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path).map_err(|e| io_error(e, "open"))?;

        #[allow(clippy::arc_with_non_send_sync)]
        let unix_file = Arc::new(UnixFile {
            file: Arc::new(Mutex::new(file)),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.contains(OpenFlags::ReadOnly)
        {
            unix_file.lock_file(true)?;
        }
        Ok(unix_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        Ok(())
    }
}

pub struct UnixFile {
    file: Arc<Mutex<std::fs::File>>,
}

impl File for UnixFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.lock();
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
        let fd = self.file.lock();
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
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let file = self.file.lock();
        let result = unsafe {
            let r = c.as_read();
            let buf = r.buf();
            let slice = buf.as_mut_slice();
            libc::pread(
                file.as_raw_fd(),
                slice.as_mut_ptr() as *mut libc::c_void,
                slice.len(),
                pos as libc::off_t,
            )
        };
        if result == -1 {
            let e = std::io::Error::last_os_error();
            Err(io_error(e, "pread"))
        } else {
            trace!("pread n: {}", result);
            // Read succeeded immediately
            c.complete(result as i32);
            Ok(c)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let file = self.file.lock();
        let buf_slice = buffer.as_slice();
        let total_size = buf_slice.len();

        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining_slice = &buf_slice[total_written..];
            let result = unsafe {
                libc::pwrite(
                    file.as_raw_fd(),
                    remaining_slice.as_ptr() as *const libc::c_void,
                    remaining_slice.len(),
                    current_pos as libc::off_t,
                )
            };
            if result == -1 {
                let e = std::io::Error::last_os_error();
                if e.kind() == ErrorKind::Interrupted {
                    // EINTR, retry without advancing
                    continue;
                }
                return Err(io_error(e, "pwrite"));
            }
            let written = result as usize;
            if written == 0 {
                // Unexpected EOF for regular files
                return Err(LimboError::CompletionError(CompletionError::IOError(
                    ErrorKind::UnexpectedEof,
                    "pwrite",
                )));
            }

            total_written += written;
            current_pos += written as u64;
            trace!("pwrite iteration: wrote {written}, total {total_written}/{total_size}");
        }
        trace!("pwrite complete: wrote {total_written} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        if buffers.len().eq(&1) {
            // use `pwrite` for single buffer
            return self.pwrite(pos, buffers[0].clone(), c);
        }

        let file = self.file.lock();
        let mut total_written = 0usize;
        let mut current_pos = pos;
        let mut buf_idx = 0;
        let mut buf_offset = 0;

        let total_size: usize = buffers.iter().map(|b| b.len()).sum();
        while total_written < total_size {
            match try_pwritev_raw(file.as_raw_fd(), current_pos, &buffers, buf_idx, buf_offset) {
                Ok(written) => {
                    if written == 0 {
                        // Unexpected EOF
                        return Err(LimboError::CompletionError(CompletionError::IOError(
                            ErrorKind::UnexpectedEof,
                            "pwritev",
                        )));
                    }
                    total_written += written;
                    current_pos += written as u64;

                    let mut remaining = written;
                    while remaining > 0 && buf_idx < buffers.len() {
                        let buf_remaining = buffers[buf_idx].len() - buf_offset;

                        if remaining >= buf_remaining {
                            // Consumed rest of current buffer
                            remaining -= buf_remaining;
                            buf_idx += 1;
                            buf_offset = 0;
                        } else {
                            // Partial write within current buffer
                            buf_offset += remaining;
                            remaining = 0;
                        }
                    }

                    trace!(
                        "pwritev iteration: wrote {written}, total {total_written}/{total_size}"
                    );
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    // EINTR - retry without advancing
                    continue;
                }
                Err(e) => {
                    return Err(io_error(e, "pwritev"));
                }
            }
        }
        trace!("pwritev complete: wrote {total_written} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        let file = self.file.lock();

        let result = unsafe {
            #[cfg(target_vendor = "apple")]
            {
                match sync_type {
                    FileSyncType::Fsync => libc::fsync(file.as_raw_fd()),
                    FileSyncType::FullFsync => libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC),
                }
            }
            #[cfg(not(target_vendor = "apple"))]
            {
                // FullFsync has no effect on non-Apple platforms
                let _ = sync_type;
                libc::fsync(file.as_raw_fd())
            }
        };

        if result == -1 {
            let e = std::io::Error::last_os_error();
            Err(io_error(e, "sync"))
        } else {
            #[cfg(target_vendor = "apple")]
            match sync_type {
                FileSyncType::FullFsync => trace!("fcntl(F_FULLFSYNC)"),
                FileSyncType::Fsync => trace!("fsync"),
            }
            #[cfg(not(target_vendor = "apple"))]
            trace!("fsync");

            c.complete(0);
            Ok(c)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn size(&self) -> Result<u64> {
        let file = self.file.lock();
        Ok(file.metadata().map_err(|e| io_error(e, "metadata"))?.len())
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let file = self.file.lock();
        let result = file.set_len(len);
        match result {
            Ok(()) => {
                trace!("file truncated to len=({})", len);
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(io_error(e, "truncate")),
        }
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
