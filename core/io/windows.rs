use crate::error::io_error;
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::sync::Arc;
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};
use std::io::ErrorKind;
use std::ptr;
#[cfg(feature = "fs")]
use tracing::debug;
use tracing::{instrument, trace, Level};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_HANDLE_EOF, FALSE, GENERIC_READ, GENERIC_WRITE, HANDLE,
    INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileEndOfFileInfo, FlushFileBuffers, GetFileSizeEx, LockFileEx, ReadFile,
    SetFileInformationByHandle, UnlockFileEx, WriteFile, FILE_ATTRIBUTE_NORMAL,
    FILE_END_OF_FILE_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

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

        let file_handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                desired_access,
                shared_mode,
                ptr::null(),
                creation_disposition,
                FILE_ATTRIBUTE_NORMAL,
                ptr::null_mut(),
            )
        };

        if file_handle == INVALID_HANDLE_VALUE {
            return Err(last_os_error("open"));
        }

        let windows_file = Arc::new(WindowsFile {
            handle: file_handle,
        });

        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.contains(OpenFlags::ReadOnly)
        {
            if let Err(e) = windows_file.lock_file(true) {
                unsafe { CloseHandle(windows_file.handle) };
                return Err(e);
            }
        }

        Ok(windows_file)
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
        let mut overlapped = overlapped_at(pos);
        let mut bytes_read: u32 = 0;

        let result = unsafe {
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

        if result == FALSE {
            let error = unsafe { GetLastError() };
            if error == ERROR_HANDLE_EOF {
                // EOF - report 0 bytes read, same as Unix pread at EOF
                c.complete(0);
                return Ok(c);
            }
            return Err(last_os_error("pread"));
        }

        trace!("pread n: {}", bytes_read);
        c.complete(bytes_read as i32);
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let buf_slice = buffer.as_slice();
        let total_size = buf_slice.len();
        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining = &buf_slice[total_written..];
            let mut bytes_written: u32 = 0;
            let mut overlapped = overlapped_at(current_pos);

            let result = unsafe {
                WriteFile(
                    self.handle,
                    remaining.as_ptr(),
                    remaining.len() as u32,
                    &mut bytes_written,
                    &mut overlapped,
                )
            };

            if result == FALSE {
                return Err(last_os_error("pwrite"));
            }

            let written = bytes_written as usize;
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
                let mut bytes_written: u32 = 0;
                let mut overlapped = overlapped_at(current_pos);

                let result = unsafe {
                    WriteFile(
                        self.handle,
                        remaining.as_ptr(),
                        remaining.len() as u32,
                        &mut bytes_written,
                        &mut overlapped,
                    )
                };

                if result == FALSE {
                    return Err(last_os_error("pwritev"));
                }

                let written = bytes_written as usize;
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
        let _ = self.unlock_file();
        unsafe {
            CloseHandle(self.handle);
        }
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
