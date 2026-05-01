use super::{Completion, File, OpenFlags, SharedWalLockKind, SharedWalMappedRegion, IO};
use crate::error::{io_error, CompletionError, LimboError};
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::Result;
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation},
};
use std::os::fd::RawFd;
use std::ptr::NonNull;

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
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

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
            file,
            path: path.to_string(),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
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
    file: std::fs::File,
    path: String,
}

pub(crate) struct UnixSharedWalMapping {
    mapping_ptr: NonNull<u8>,
    mapping_len: usize,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for UnixSharedWalMapping {}
unsafe impl Sync for UnixSharedWalMapping {}

impl SharedWalMappedRegion for UnixSharedWalMapping {
    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for UnixSharedWalMapping {
    fn drop(&mut self) {
        let rc = unsafe { libc::munmap(self.mapping_ptr.as_ptr().cast(), self.mapping_len) };
        if rc != 0 {
            // Log rather than panic — panicking in Drop aborts if we're already
            // unwinding (double panic).
            tracing::error!(
                "munmap failed for shared WAL coordination region: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

pub(crate) fn unix_shared_wal_lock_byte(
    fd: RawFd,
    offset: u64,
    exclusive: bool,
    blocking: bool,
    kind: SharedWalLockKind,
) -> Result<bool> {
    let mut flock = libc::flock {
        l_type: if exclusive {
            libc::F_WRLCK as libc::c_short
        } else {
            libc::F_RDLCK as libc::c_short
        },
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset as libc::off_t,
        l_len: 1,
        l_pid: 0,
        #[cfg(target_os = "freebsd")]
        l_sysid: 0,
    };
    let cmd = match (kind, blocking) {
        #[cfg(target_os = "linux")]
        (SharedWalLockKind::LinuxOfd, true) => libc::F_OFD_SETLKW,
        #[cfg(target_os = "linux")]
        (SharedWalLockKind::LinuxOfd, false) => libc::F_OFD_SETLK,
        (SharedWalLockKind::ProcessScopedFcntl, true) => libc::F_SETLKW,
        (SharedWalLockKind::ProcessScopedFcntl, false) => libc::F_SETLK,
        #[cfg(not(target_os = "linux"))]
        (SharedWalLockKind::LinuxOfd, _) => {
            return Err(LimboError::InternalError(
                "linux OFD locks are not supported on this platform".into(),
            ))
        }
    };
    loop {
        let rc = unsafe { libc::fcntl(fd, cmd, &mut flock) };
        if rc == -1 {
            let error = std::io::Error::last_os_error();
            if blocking && error.kind() == ErrorKind::Interrupted {
                continue;
            }
            if !blocking && error.kind() == ErrorKind::WouldBlock {
                return Ok(false);
            }
            let message = match error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking shared WAL coordination file. File is locked by another process"
                        .to_string()
                }
                _ => format!("Failed locking shared WAL coordination file, {error}"),
            };
            return Err(LimboError::LockingError(message));
        }
        return Ok(true);
    }
}

pub(crate) fn unix_shared_wal_unlock_byte(
    fd: RawFd,
    offset: u64,
    kind: SharedWalLockKind,
) -> Result<()> {
    let mut flock = libc::flock {
        l_type: libc::F_UNLCK as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset as libc::off_t,
        l_len: 1,
        l_pid: 0,
        #[cfg(target_os = "freebsd")]
        l_sysid: 0,
    };
    let cmd = match kind {
        #[cfg(target_os = "linux")]
        SharedWalLockKind::LinuxOfd => libc::F_OFD_SETLK,
        SharedWalLockKind::ProcessScopedFcntl => libc::F_SETLK,
        #[cfg(not(target_os = "linux"))]
        SharedWalLockKind::LinuxOfd => {
            return Err(LimboError::InternalError(
                "linux OFD locks are not supported on this platform".into(),
            ))
        }
    };
    let rc = unsafe { libc::fcntl(fd, cmd, &mut flock) };
    if rc == -1 {
        Err(LimboError::LockingError(format!(
            "Failed to release shared WAL coordination lock: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn unix_shared_wal_map(
    offset: u64,
    len: usize,
    fd: RawFd,
) -> Result<Box<dyn SharedWalMappedRegion>> {
    if len == 0 {
        return Err(LimboError::InternalError(
            "cannot mmap shared WAL coordination region with zero length".into(),
        ));
    }
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(LimboError::LockingError(format!(
            "failed to determine shared WAL mmap page size: {}",
            std::io::Error::last_os_error()
        )));
    }
    let page_size = page_size as u64;
    let aligned_offset = offset / page_size * page_size;
    let prefix_len = (offset - aligned_offset) as usize;
    let mapping_len = prefix_len
        .checked_add(len)
        .ok_or_else(|| LimboError::InternalError("shared WAL mmap length overflow".into()))?;
    let mapping_ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            mapping_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            aligned_offset as libc::off_t,
        )
    };
    if mapping_ptr == libc::MAP_FAILED {
        let error = std::io::Error::last_os_error();
        let file_size = unsafe {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if libc::fstat(fd, stat.as_mut_ptr()) == 0 {
                stat.assume_init().st_size
            } else {
                -1
            }
        };
        return Err(LimboError::LockingError(format!(
            "mmap shared WAL coordination file failed: {error} (offset={offset}, aligned_offset={aligned_offset}, len={len}, mapping_len={mapping_len}, fd={fd}, file_size={file_size})"
        )));
    }
    let mapping_ptr =
        NonNull::new(mapping_ptr.cast::<u8>()).expect("mmap returned null for shared WAL map");
    let ptr = NonNull::new(unsafe { mapping_ptr.as_ptr().add(prefix_len) })
        .expect("aligned mmap base plus prefix_len returned null");
    Ok(Box::new(UnixSharedWalMapping {
        mapping_ptr,
        mapping_len,
        ptr,
        len,
    }))
}

impl File for UnixFile {
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
                ErrorKind::WouldBlock => format!(
                    "Failed locking file '{}'. File is locked by another process",
                    self.path
                ),
                _ => format!("Failed locking file '{}', {io_error}", self.path),
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

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let result = unsafe {
            let r = c.as_read();
            let buf = r.buf();
            let slice = buf.as_mut_slice();
            libc::pread(
                self.file.as_raw_fd(),
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
        let buf_slice = buffer.as_slice();
        let total_size = buf_slice.len();

        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining_slice = &buf_slice[total_written..];
            let result = unsafe {
                libc::pwrite(
                    self.file.as_raw_fd(),
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

        let mut total_written = 0usize;
        let mut current_pos = pos;
        let mut buf_idx = 0;
        let mut buf_offset = 0;

        let total_size: usize = buffers.iter().map(|b| b.len()).sum();
        while total_written < total_size {
            match try_pwritev_raw(
                self.file.as_raw_fd(),
                current_pos,
                &buffers,
                buf_idx,
                buf_offset,
            ) {
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
        let result = unsafe {
            #[cfg(target_vendor = "apple")]
            {
                match sync_type {
                    FileSyncType::Fsync => libc::fsync(self.file.as_raw_fd()),
                    FileSyncType::FullFsync => {
                        libc::fcntl(self.file.as_raw_fd(), libc::F_FULLFSYNC)
                    }
                }
            }
            #[cfg(not(target_vendor = "apple"))]
            {
                // FullFsync has no effect on non-Apple platforms
                let _ = sync_type;
                libc::fsync(self.file.as_raw_fd())
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
        Ok(self
            .file
            .metadata()
            .map_err(|e| io_error(e, "metadata"))?
            .len())
    }

    #[instrument(err, skip_all, level = Level::INFO)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let result = self.file.set_len(len);
        match result {
            Ok(()) => {
                trace!("file truncated to len=({})", len);
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(io_error(e, "truncate")),
        }
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, true, kind).map(|_| ())
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, false, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        unix_shared_wal_unlock_byte(self.file.as_raw_fd(), offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|err| io_error(err, "resize shared WAL coordination file"))
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        unix_shared_wal_map(offset, len, self.file.as_raw_fd())
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
    use std::io::Write;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
    }

    #[test]
    fn test_shared_wal_map_supports_unaligned_logical_offset() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let backing_len = 128 * 1024;
        let bytes: Vec<u8> = (0..backing_len).map(|i| (i % 251) as u8).collect();
        file.as_file().write_all(&bytes).unwrap();
        file.as_file().sync_all().unwrap();

        let mapped = unix_shared_wal_map(4096, 81920, file.as_file().as_raw_fd()).unwrap();
        assert_eq!(mapped.len(), 81920);
        let slice = unsafe { std::slice::from_raw_parts(mapped.ptr().as_ptr(), mapped.len()) };
        assert_eq!(&slice[..128], &bytes[4096..4096 + 128]);
        assert_eq!(&slice[mapped.len() - 128..], &bytes[4096 + 81920 - 128..4096 + 81920]);
    }
}
