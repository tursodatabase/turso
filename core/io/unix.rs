use super::{Completion, File, OpenFlags, IO};
use crate::error::LimboError;
use crate::io::clock::{Clock, Instant};
use crate::io::common;
use crate::Result;
use parking_lot::Mutex;
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
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let unix_file = Arc::new(UnixFile {
            file: Arc::new(Mutex::new(file)),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            unix_file.lock_file(!flags.contains(OpenFlags::ReadOnly))?;
        }
        Ok(unix_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        std::fs::remove_file(path)?;
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
            Err(e.into())
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
                return Err(e.into());
            }
            let written = result as usize;
            if written == 0 {
                // Unexpected EOF for regular files
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "pwrite returned 0 bytes written",
                )
                .into());
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
                        return Err(std::io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "pwritev returned 0 bytes written",
                        )
                        .into());
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
                    return Err(e.into());
                }
            }
        }
        trace!("pwritev complete: wrote {total_written} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        let file = self.file.lock();

        let result = unsafe {
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            {
                libc::fsync(file.as_raw_fd())
            }

            #[cfg(any(target_os = "macos", target_os = "ios"))]
            {
                libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC)
            }
        };

        if result == -1 {
            let e = std::io::Error::last_os_error();
            Err(e.into())
        } else {
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            trace!("fsync");

            #[cfg(any(target_os = "macos", target_os = "ios"))]
            trace!("fcntl(F_FULLSYNC)");

            c.complete(0);
            Ok(c)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn size(&self) -> Result<u64> {
        let file = self.file.lock();
        Ok(file.metadata()?.len())
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
            Err(e) => Err(e.into()),
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
