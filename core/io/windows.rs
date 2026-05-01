use crate::error::io_error;
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::sync::{Arc, Mutex};
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::ptr;
use std::sync::OnceLock;
#[cfg(feature = "fs")]
use tracing::debug;
use tracing::{instrument, trace, Level};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_HANDLE_EOF, ERROR_IO_PENDING, FALSE, GENERIC_READ,
    GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, TRUE, WAIT_OBJECT_0,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileEndOfFileInfo, FlushFileBuffers, GetFileSizeEx, LockFileEx, ReadFile,
    SetFileInformationByHandle, UnlockFileEx, WriteFile, FILE_ATTRIBUTE_NORMAL,
    FILE_END_OF_FILE_INFO, FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, OPEN_ALWAYS,
    OPEN_EXISTING,
};
use windows_sys::Win32::System::Threading::{CreateEventW, ResetEvent, WaitForSingleObject, INFINITE};
use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

// Per-thread event handle used to synchronously wait for completion of an
// overlapped ReadFile/WriteFile. Created lazily on first use and reused for
// the lifetime of the thread (closed by the OS on thread exit). One event
// per thread (rather than per file or per call) so concurrent I/O on the
// same WindowsFile from different threads doesn't race on a shared event,
// and we don't pay CreateEventW cost per page read.
thread_local! {
    static IO_EVENT: Cell<HANDLE> = const { Cell::new(ptr::null_mut()) };
}

/// Run `f` with a thread-local manual-reset Event, freshly reset.
fn with_io_event<F, R>(f: F) -> Result<R>
where
    F: FnOnce(HANDLE) -> Result<R>,
{
    IO_EVENT.with(|cell| {
        let mut event = cell.get();
        if event.is_null() {
            // SAFETY: passing null SECURITY_ATTRIBUTES, manual-reset, initial-non-signaled,
            // null name. CreateEventW returns null on failure.
            event = unsafe { CreateEventW(ptr::null(), TRUE, FALSE, ptr::null()) };
            if event.is_null() {
                return Err(last_os_error("CreateEventW"));
            }
            cell.set(event);
        }
        // SAFETY: event is a valid manual-reset event we own for this thread.
        unsafe {
            ResetEvent(event);
        }
        f(event)
    })
}

/// Issue a single overlapped `WriteFile` and wait for it to complete on the
/// thread-local event. Returns the byte count, mirroring the three completion
/// paths described on `pread`.
fn write_chunk(handle: HANDLE, event: HANDLE, pos: u64, data: &[u8]) -> Result<usize> {
    let mut overlapped = overlapped_at(pos);
    overlapped.hEvent = event;
    let mut bytes_written: u32 = 0;
    let issued = unsafe {
        WriteFile(
            handle,
            data.as_ptr(),
            data.len() as u32,
            &mut bytes_written,
            &mut overlapped,
        )
    };
    if issued == FALSE {
        let error = unsafe { GetLastError() };
        if error != ERROR_IO_PENDING {
            return Err(last_os_error("pwrite"));
        }
        let wait = unsafe { WaitForSingleObject(event, INFINITE) };
        if wait != WAIT_OBJECT_0 {
            return Err(last_os_error("pwrite WaitForSingleObject"));
        }
        let ok =
            unsafe { GetOverlappedResult(handle, &overlapped, &mut bytes_written, FALSE) };
        if ok == FALSE {
            return Err(last_os_error("pwrite GetOverlappedResult"));
        }
    }
    Ok(bytes_written as usize)
}

/// Creates an OVERLAPPED structure with the given file offset.
/// Used with ReadFile/WriteFile on synchronous (non-OVERLAPPED) handles
/// to achieve pread/pwrite semantics in a single syscall.
#[inline]
fn overlapped_at(pos: u64) -> OVERLAPPED {
    OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Anonymous: OVERLAPPED_0 {
            Anonymous: OVERLAPPED_0_0 {
                Offset: pos as u32,
                OffsetHigh: (pos >> 32) as u32,
            },
        },
        hEvent: std::ptr::null_mut(),
    }
}

#[inline]
fn last_os_error(context: &'static str) -> LimboError {
    let err = std::io::Error::last_os_error();
    io_error(err, context)
}

/// Process-wide cross-process advisory lock for a single path.
///
/// `LockFileEx` is per-handle on Windows: two handles in the same process — even
/// to the same file — race against each other instead of sharing a lock the way
/// POSIX `fcntl` locks do per-process. Acquiring an exclusive byte-range lock on
/// every `open_file` therefore breaks legitimate same-process patterns (multiple
/// connections, busy-timeout retries, parallel readers).
///
/// To match Unix semantics — one OS lock per (process, path) — we hold a single
/// dedicated lock handle in a process-global registry, ref-counted across all
/// `WindowsFile`s for the same canonical path. The first opener acquires the
/// `LockFileEx`; subsequent in-process openers just bump the refcount. Once the
/// last opener drops its guard, the dedicated handle is closed and the OS lock
/// is released, freeing the path for other processes.
///
/// The lock targets a single sentinel byte at an offset far beyond any
/// realistic database size: `LockFileEx` blocks `ReadFile`/`WriteFile` on
/// locked regions across *all* handles (including ones in our own process), so
/// locking the data region itself would deadlock our own writes. The sentinel
/// byte is never read or written by the engine, only locked.
const PROCESS_LOCK_OFFSET: u64 = 0x4000_0000_0000_0000;

struct ProcessFileLockEntry {
    handle: HANDLE,
    refcount: usize,
}

// HANDLE is a raw pointer; the registry mutex serializes all access to it.
unsafe impl Send for ProcessFileLockEntry {}

type ProcessFileLockRegistry = Mutex<HashMap<PathBuf, ProcessFileLockEntry>>;

fn process_file_lock_registry() -> &'static ProcessFileLockRegistry {
    static REGISTRY: OnceLock<ProcessFileLockRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

struct ProcessFileLockGuard {
    key: PathBuf,
}

impl Drop for ProcessFileLockGuard {
    fn drop(&mut self) {
        let mut registry = process_file_lock_registry().lock();
        let Some(entry) = registry.get_mut(&self.key) else {
            return;
        };
        entry.refcount -= 1;
        if entry.refcount > 0 {
            return;
        }
        // Release the OS lock and close the handle while still holding the
        // registry mutex. A concurrent acquirer for the same path is blocked
        // on the mutex; once we drop it the entry is gone and they'll open a
        // fresh dedicated handle. Releasing inside the critical section
        // ensures the previous `LockFileEx` is fully unwound before any new
        // `LockFileEx` is attempted, otherwise the new attempt would race
        // and fail with `ERROR_LOCK_VIOLATION`.
        let handle = registry.remove(&self.key).expect("entry just observed").handle;
        unsafe {
            let mut overlapped = overlapped_at(PROCESS_LOCK_OFFSET);
            UnlockFileEx(handle, 0, 1, 0, &mut overlapped);
            CloseHandle(handle);
        }
    }
}

fn acquire_process_file_lock(path: &str) -> Result<ProcessFileLockGuard> {
    // Canonicalize so that different path strings resolving to the same file
    // (e.g. relative vs. absolute) share a single registry entry. Fall back
    // to the raw path if canonicalize fails for any reason; collisions across
    // distinct paths only matter if the same caller mixes spellings, and the
    // OS lock still provides correctness in that case.
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
    let mut registry = process_file_lock_registry().lock();
    if let Some(entry) = registry.get_mut(&key) {
        entry.refcount += 1;
        return Ok(ProcessFileLockGuard { key });
    }

    // First opener for this path in this process. Open a dedicated handle whose
    // sole purpose is to hold the byte-range lock. Its lifetime is tied to the
    // registry entry, decoupled from any individual `WindowsFile`.
    let wide_path: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
    let handle = unsafe {
        CreateFileW(
            wide_path.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(last_os_error("open lock handle"));
    }

    let mut overlapped = overlapped_at(PROCESS_LOCK_OFFSET);
    let ok = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if ok == FALSE {
        let err = std::io::Error::last_os_error();
        unsafe {
            CloseHandle(handle);
        }
        let message = match err.kind() {
            ErrorKind::WouldBlock => {
                "Failed locking file. File is locked by another process".to_string()
            }
            _ => format!("Failed locking file, {err}"),
        };
        return Err(LimboError::LockingError(message));
    }

    registry.insert(key.clone(), ProcessFileLockEntry { handle, refcount: 1 });
    Ok(ProcessFileLockGuard { key })
}

pub struct WindowsIO {}

impl WindowsIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

impl IO for WindowsIO {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);

        let wide_path: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();

        let desired_access = if flags.contains(OpenFlags::ReadOnly) {
            GENERIC_READ
        } else {
            GENERIC_READ | GENERIC_WRITE
        };

        let creation_disposition = if flags.contains(OpenFlags::Create) {
            OPEN_ALWAYS
        } else {
            OPEN_EXISTING
        };

        let shared_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

        // Open with FILE_FLAG_OVERLAPPED so concurrent ReadFile/WriteFile calls
        // from different threads aren't serialized by the kernel's
        // FO_SYNCHRONOUS_IO file-object lock. We still wait for each call to
        // complete before returning (via a per-thread Event), so semantics
        // remain synchronous from the caller's point of view, but multiple
        // threads can have I/O in flight at the same time.
        let file_handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                desired_access,
                shared_mode,
                ptr::null(),
                creation_disposition,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
                ptr::null_mut(),
            )
        };

        if file_handle == INVALID_HANDLE_VALUE {
            return Err(last_os_error("open"));
        }

        // Cross-process advisory lock: rejects opens from other processes while
        // permitting multiple in-process handles. See `ProcessFileLockEntry`.
        let process_lock = if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
        {
            match acquire_process_file_lock(path) {
                Ok(guard) => Some(guard),
                Err(e) => {
                    unsafe {
                        CloseHandle(file_handle);
                    }
                    return Err(e);
                }
            }
        } else {
            None
        };

        Ok(Arc::new(WindowsFile {
            handle: file_handle,
            _process_lock: process_lock,
        }))
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        Ok(())
    }
}

impl Clock for WindowsIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

pub struct WindowsFile {
    handle: HANDLE,
    /// Process-wide advisory lock held while this file is open. The lock lives
    /// in a dedicated handle managed by `ProcessFileLockEntry`; this field
    /// just keeps the registry refcount up. Dropped automatically with `self`.
    _process_lock: Option<ProcessFileLockGuard>,
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        trace!("lock_file(exclusive = {})", exclusive);

        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };

        let mut overlapped = overlapped_at(0);
        let result =
            unsafe { LockFileEx(self.handle, flags, 0, u32::MAX, u32::MAX, &mut overlapped) };

        if result == FALSE {
            let err = std::io::Error::last_os_error();
            let message = match err.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {err}"),
            };
            return Err(LimboError::LockingError(message));
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        trace!("unlock_file");

        let mut overlapped = overlapped_at(0);
        let result = unsafe { UnlockFileEx(self.handle, 0, u32::MAX, u32::MAX, &mut overlapped) };

        if result == FALSE {
            let err = std::io::Error::last_os_error();
            return Err(LimboError::LockingError(format!(
                "Failed to release file lock: {err}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self, c), level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        with_io_event(|event| {
            let mut overlapped = overlapped_at(pos);
            overlapped.hEvent = event;
            let mut bytes_read: u32 = 0;

            let issued = unsafe {
                let r = c.as_read();
                let buf = r.buf();
                let slice = buf.as_mut_slice();
                ReadFile(
                    self.handle,
                    slice.as_mut_ptr(),
                    slice.len() as u32,
                    &mut bytes_read,
                    &mut overlapped,
                )
            };

            // Three completion paths on an overlapped handle:
            //  1. ReadFile returns TRUE: synchronous completion. bytes_read
            //     already holds the count.
            //  2. ReadFile returns FALSE / ERROR_IO_PENDING: I/O is in flight.
            //     Wait on the event, then GetOverlappedResult to read the
            //     final byte count.
            //  3. ReadFile returns FALSE / ERROR_HANDLE_EOF: synchronous EOF.
            if issued == FALSE {
                let error = unsafe { GetLastError() };
                if error == ERROR_HANDLE_EOF {
                    c.complete(0);
                    return Ok(c);
                }
                if error != ERROR_IO_PENDING {
                    return Err(last_os_error("pread"));
                }
                let wait = unsafe { WaitForSingleObject(event, INFINITE) };
                if wait != WAIT_OBJECT_0 {
                    return Err(last_os_error("pread WaitForSingleObject"));
                }
                let ok = unsafe {
                    GetOverlappedResult(self.handle, &overlapped, &mut bytes_read, FALSE)
                };
                if ok == FALSE {
                    let err = unsafe { GetLastError() };
                    if err == ERROR_HANDLE_EOF {
                        c.complete(0);
                        return Ok(c);
                    }
                    return Err(last_os_error("pread GetOverlappedResult"));
                }
            }

            trace!("pread n: {}", bytes_read);
            c.complete(bytes_read as i32);
            Ok(c)
        })
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let buf_slice = buffer.as_slice();
        let total_size = buf_slice.len();
        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining = &buf_slice[total_written..];
            let written = with_io_event(|event| write_chunk(self.handle, event, current_pos, remaining))?;
            if written == 0 {
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

    #[instrument(skip_all, level = Level::TRACE)]
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        if buffers.is_empty() {
            c.complete(0);
            return Ok(c);
        }
        if buffers.len() == 1 {
            return self.pwrite(pos, buffers[0].clone(), c);
        }

        let total_size: usize = buffers.iter().map(|b| b.len()).sum();
        let mut total_written = 0usize;
        let mut current_pos = pos;

        for buf in &buffers {
            let slice = buf.as_slice();
            let mut buf_written = 0usize;

            while buf_written < slice.len() {
                let remaining = &slice[buf_written..];
                let written = with_io_event(|event| {
                    write_chunk(self.handle, event, current_pos, remaining)
                })?;
                if written == 0 {
                    return Err(LimboError::CompletionError(CompletionError::IOError(
                        ErrorKind::UnexpectedEof,
                        "pwritev",
                    )));
                }
                buf_written += written;
                current_pos += written as u64;
                total_written += written;
            }
        }

        trace!("pwritev complete: wrote {total_written}/{total_size} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        let result = unsafe { FlushFileBuffers(self.handle) };
        if result == FALSE {
            return Err(last_os_error("sync"));
        }
        trace!("FlushFileBuffers");
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let file_info = FILE_END_OF_FILE_INFO {
            EndOfFile: len as i64,
        };
        let result = unsafe {
            SetFileInformationByHandle(
                self.handle,
                FileEndOfFileInfo,
                (&raw const file_info).cast(),
                std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
            )
        };
        if result == FALSE {
            return Err(last_os_error("truncate"));
        }
        trace!("file truncated to len=({})", len);
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let mut file_size: i64 = 0;
        let result = unsafe { GetFileSizeEx(self.handle, &mut file_size) };
        if result == FALSE {
            return Err(last_os_error("size"));
        }
        Ok(file_size as u64)
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.handle);
        }
        // The process-wide advisory lock is released by `ProcessFileLockGuard::drop`
        // after the `_process_lock` field is dropped here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(WindowsIO::new);
    }
}
