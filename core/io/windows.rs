use crate::error::io_error;
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::sync::Arc;
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};
use std::io::ErrorKind;
use std::os::windows::io::AsRawHandle;
use std::ptr;
use tracing::{debug, instrument, trace, Level};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, FALSE, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FlushFileBuffers, GetFileSizeEx, LockFileEx, ReadFile, SetFileInformationByHandle,
    UnlockFileEx, WriteFile, FileEndOfFileInfo, FILE_END_OF_FILE_INFO, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

pub struct WindowsIO {}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {})
    }
}

impl IO for WindowsIO {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);

        let unicode_path: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();

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

        let handle = unsafe {
            CreateFileW(
                unicode_path.as_ptr(),
                desired_access,
                shared_mode,
                ptr::null(),
                creation_disposition,
                0, // no special flags — synchronous, buffered IO
                ptr::null_mut(),
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(LimboError::InternalError(format!(
                "CreateFileW failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        let file = Arc::new(WindowsFile { handle });

        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.contains(OpenFlags::ReadOnly)
        {
            file.lock_file(true)?;
        }

        Ok(file)
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
}

// HANDLE is Send+Sync safe — it's an opaque kernel object pointer.
unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

/// Build an OVERLAPPED struct that encodes a file offset (no event handle).
#[inline]
fn overlapped_at(pos: u64) -> OVERLAPPED {
    // Safety: zero-init is valid for OVERLAPPED.
    let mut ovl: OVERLAPPED = unsafe { std::mem::zeroed() };
    ovl.Anonymous.Anonymous.Offset = pos as u32;
    ovl.Anonymous.Anonymous.OffsetHigh = (pos >> 32) as u32;
    ovl
}

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };
        let mut ovl = overlapped_at(0);
        let ok = unsafe { LockFileEx(self.handle, flags, 0, u32::MAX, u32::MAX, &mut ovl) };
        if ok == FALSE {
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
        let mut ovl = overlapped_at(0);
        let ok = unsafe { UnlockFileEx(self.handle, 0, u32::MAX, u32::MAX, &mut ovl) };
        if ok == FALSE {
            let err = std::io::Error::last_os_error();
            return Err(LimboError::LockingError(format!(
                "Failed to release file lock: {err}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self, c), level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let buf = r.buf();
        let slice = buf.as_mut_slice();
        let mut ovl = overlapped_at(pos);
        let mut bytes_read: u32 = 0;

        let ok = unsafe {
            ReadFile(
                self.handle,
                slice.as_mut_ptr(),
                slice.len() as u32,
                &mut bytes_read,
                &mut ovl,
            )
        };
        if ok == FALSE {
            let err = std::io::Error::last_os_error();
            // ERROR_HANDLE_EOF is not a real error for reads
            if err.kind() == ErrorKind::UnexpectedEof || err.raw_os_error() == Some(38) {
                c.complete(0);
                return Ok(c);
            }
            return Err(io_error(err, "pread"));
        }
        c.complete(bytes_read as i32);
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let buf = buffer.as_slice();
        let total_size = buf.len();
        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining = &buf[total_written..];
            let mut ovl = overlapped_at(current_pos);
            let mut bytes_written: u32 = 0;

            let ok = unsafe {
                WriteFile(
                    self.handle,
                    remaining.as_ptr(),
                    remaining.len() as u32,
                    &mut bytes_written,
                    &mut ovl,
                )
            };
            if ok == FALSE {
                return Err(io_error(std::io::Error::last_os_error(), "pwrite"));
            }
            if bytes_written == 0 {
                return Err(LimboError::CompletionError(CompletionError::IOError(
                    ErrorKind::UnexpectedEof,
                    "pwrite",
                )));
            }
            total_written += bytes_written as usize;
            current_pos += bytes_written as u64;
        }
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        if unsafe { FlushFileBuffers(self.handle) } == FALSE {
            return Err(io_error(std::io::Error::last_os_error(), "sync"));
        }
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let file_info = FILE_END_OF_FILE_INFO {
            EndOfFile: len as i64,
        };
        let ok = unsafe {
            SetFileInformationByHandle(
                self.handle,
                FileEndOfFileInfo,
                (&raw const file_info).cast(),
                std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
            )
        };
        if ok == FALSE {
            return Err(io_error(std::io::Error::last_os_error(), "truncate"));
        }
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let mut file_size: i64 = 0;
        if unsafe { GetFileSizeEx(self.handle, &mut file_size) } == FALSE {
            return Err(io_error(std::io::Error::last_os_error(), "size"));
        }
        Ok(file_size as u64)
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        // Unlock before closing — ignore errors during drop.
        let _ = self.unlock_file();
        unsafe {
            CloseHandle(self.handle);
        }
    }
}
