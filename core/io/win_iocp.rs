// TODO: Add more logs and tracing
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};
use smallvec::Array;
use std::ffi::OsString;
use std::io::{Read, Seek, Write};
use std::ops::Deref;
use std::os::windows::ffi::{self, OsStringExt};
use std::ptr::{addr_of, addr_of_mut, NonNull};
use std::{mem, ptr, u32};
use tracing::{debug, instrument, trace, warn, Level};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_IO_PENDING, ERROR_OPERATION_ABORTED, ERROR_TIMEOUT, FALSE,
    GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, MAX_PATH, TRUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileEndOfFileInfo, FlushFileBuffers, GetFileSizeEx, LockFile, ReadFile,
    ReadFileEx, SetEndOfFile, SetFileInformationByHandle, SetFilePointerEx, UnlockFile, WriteFile,
    FILE_BEGIN, FILE_END_OF_FILE_INFO, FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED,
    FILE_FLAG_WRITE_THROUGH, FILE_INFO_BY_HANDLE_CLASS, FILE_SHARE_DELETE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{
    CancelIo, CreateIoCompletionPort, GetQueuedCompletionStatus, GetQueuedCompletionStatusEx,
    OVERLAPPED,
};

use windows_sys::Win32::System::Threading::INFINITE;

pub struct WindowsIOCP {
    handle: HANDLE,
}

impl WindowsIOCP {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'win_iocp'");

        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 0) };
        if handle == INVALID_HANDLE_VALUE {
            return Err(LimboError::NullValue);
        }
        Ok(Self { handle })
    }
}

#[repr(C)]
struct IoOverlapped {
    ov: OVERLAPPED,
    completion: Completion,
}

unsafe impl Send for WindowsIOCP {}
unsafe impl Sync for WindowsIOCP {}
crate::assert::assert_send_sync!(WindowsIOCP);

impl IO for WindowsIOCP {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        debug!("open_file(path = {})", path);

        let mut encoded_path = path.encode_utf16().fold(vec![], |mut acc, v| {
            acc.push(v);
            acc
        });
        encoded_path.push(0);

        let mut desired_access = 0;
        let mut creation_disposition = 0;
        desired_access |= if flags.contains(OpenFlags::ReadOnly) {
            GENERIC_READ
        } else {
            GENERIC_WRITE | GENERIC_READ
        };

        creation_disposition |= if flags.contains(OpenFlags::Create) {
            OPEN_ALWAYS
        } else {
            OPEN_EXISTING
        };

        let flags = if direct {
            FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH
        } else {
            FILE_FLAG_OVERLAPPED
        };

        unsafe {
            let file = CreateFileW(
                encoded_path.as_ptr(),
                desired_access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                ptr::null(),
                creation_disposition,
                flags,
                ptr::null_mut(),
            );

            if file == INVALID_HANDLE_VALUE {
                return Err(LimboError::InternalError(format!(
                    "CreatFile failed: {}",
                    GetLastError()
                )));
            };

            // Bind file to IOCP
            let ret = CreateIoCompletionPort(file, self.handle, 0, 0);

            if ret.is_null() {
                return Err(LimboError::InternalError(format!(
                    "CreatFile failed: {}",
                    GetLastError()
                )));
            };

            Ok(Arc::new(WindowsFile { handle: file }))
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        Ok(std::fs::remove_file(path)?)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn drain(&self) -> Result<()> {
        let mut overlapped_ptr = ptr::null_mut();
        let mut bytes = 0;
        let mut key = 0;
        loop {
            unsafe {
                let result = GetQueuedCompletionStatus(
                    self.handle,
                    &raw mut bytes,
                    &raw mut key,
                    &raw mut overlapped_ptr,
                    0,
                );

                if overlapped_ptr.is_null() {
                    if result != TRUE {
                        if GetLastError() == WAIT_TIMEOUT {
                            break;
                        }

                        let error = GetLastError() as i32;
                        let error = std::io::Error::from_raw_os_error(error);
                        return Err(LimboError::InternalError(format!("Error {error}")));
                    }
                    return Ok(());
                }

                let overlapped_ptr: *mut IoOverlapped = overlapped_ptr.cast();
                let overlapped_ptr = NonNull::new(overlapped_ptr).ok_or(LimboError::NullValue)?;

                let overlapped = Box::from_raw(overlapped_ptr.as_ptr());

                if result == TRUE {
                    if !overlapped.completion.finished() {
                        let bytes = bytes.try_into().map_err(|_| LimboError::NullValue)?;
                        overlapped.completion.complete(bytes);
                    }
                } else {
                    let error = GetLastError();
                    let error = error as i32;
                    let err = std::io::Error::from_raw_os_error(error);
                    overlapped.completion.error(err.into());
                }
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        unsafe {
            let mut overlapped_ptr = ptr::null_mut();
            let mut bytes = 0;
            let mut key = 0;

            let result = GetQueuedCompletionStatus(
                self.handle,
                &raw mut bytes,
                &raw mut key,
                &raw mut overlapped_ptr,
                0,
            );

            if overlapped_ptr.is_null() {
                // GetQueuedCompletionStatus failed
                if result != TRUE {
                    if GetLastError() == WAIT_TIMEOUT {
                        return Ok(());
                    }

                    let error = GetLastError() as i32;
                    let error = std::io::Error::from_raw_os_error(error);
                    return Err(LimboError::InternalError(format!("Error {error}")));
                }

                //IOCP Canceled somehow
                return Ok(());
            }

            let overlapped = Box::<IoOverlapped>::from_raw(overlapped_ptr.cast());

            if overlapped.completion.finished() {
                return Ok(());
            }

            if result == TRUE {
                let bytes = bytes as i32;
                overlapped.completion.complete(bytes);
            } else {
                // I/O Operation failed
                let error = GetLastError();
                let error = error as i32;
                let err = std::io::Error::from_raw_os_error(error);
                overlapped.completion.error(err.into());
            }
        }
        Ok(())
    }
}

impl Drop for WindowsIOCP {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.handle);
        }
    }
}

impl Clock for WindowsIOCP {
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
crate::assert::assert_send_sync!(WindowsFile);

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        };

        unsafe {
            if FALSE == LockFile(self.handle, flags, 0, u32::MAX, u32::MAX) {
                return Err(LimboError::InternalError(format!(
                    "UnlockFile failed:{:x}",
                    GetLastError()
                )));
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        unsafe {
            if FALSE == UnlockFile(self.handle, 0, 0, u32::MAX, u32::MAX) {
                return Err(LimboError::InternalError(format!(
                    "UnlockFile failed:{:x}",
                    GetLastError()
                )));
            }
        }

        Ok(())
    }

    #[instrument(skip(self, c), level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        if c.finished() {
            return Err(LimboError::CompletionError(CompletionError::Aborted));
        }

        let read = c.as_read();
        let buf = read.buf();
        let ptr = buf.as_mut_ptr();
        let len = buf.len().try_into().map_err(|_| LimboError::TooBig)?;

        let overlapped = Box::leak(Box::new(IoOverlapped {
            ov: OVERLAPPED::default(),
            completion: c.clone(),
        }));

        let low = pos as u32;
        let high = (pos >> 32) as u32;

        unsafe {
            overlapped.ov.Anonymous.Anonymous.Offset = low;
            overlapped.ov.Anonymous.Anonymous.OffsetHigh = high;

            if FALSE
                == ReadFile(
                    self.handle,
                    ptr,
                    len,
                    ptr::null_mut(),
                    ptr::from_mut(overlapped).cast(),
                )
                && GetLastError() != ERROR_IO_PENDING
            {
                return Err(LimboError::InternalError(format!(
                    "ReadFile failed:{:x}",
                    GetLastError()
                )));
            }
        }
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        if c.finished() {
            return Err(LimboError::CompletionError(CompletionError::Aborted));
        }

        let ptr = buffer.as_mut_ptr();
        let len = buffer.len().try_into().map_err(|_| LimboError::TooBig)?;

        let overlapped = Box::leak(Box::new(IoOverlapped {
            ov: OVERLAPPED::default(),
            completion: c.clone(),
        }));

        let low = pos as u32;
        let high = (pos >> 32) as u32;

        unsafe {
            overlapped.ov.Anonymous.Anonymous.Offset = low;
            overlapped.ov.Anonymous.Anonymous.OffsetHigh = high;

            if FALSE
                == WriteFile(
                    self.handle,
                    ptr,
                    len,
                    ptr::null_mut(),
                    ptr::from_mut(overlapped).cast(),
                )
                && GetLastError() != ERROR_IO_PENDING
            {
                return Err(LimboError::InternalError(format!(
                    "WriteFile failed:{:x}",
                    GetLastError()
                )));
            }
        }
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        if c.finished() {
            return Err(LimboError::CompletionError(CompletionError::Aborted));
        }

        unsafe {
            if FALSE == FlushFileBuffers(self.handle) {
                println!("FlushFileBuffers failed:{:x}", GetLastError());

                return Err(LimboError::InternalError(format!(
                    "FlushFileBuffers failed:{:x}",
                    GetLastError()
                )));
            }
        };
        c.complete(0);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if c.finished() {
            return Err(LimboError::CompletionError(CompletionError::Aborted));
        }

        unsafe {
            let len = len.try_into().map_err(|_| LimboError::TooBig)?;

            let data = FILE_END_OF_FILE_INFO { EndOfFile: len };
            if FALSE
                == SetFileInformationByHandle(
                    self.handle,
                    FileEndOfFileInfo,
                    (&raw const data).cast(),
                    size_of_val(&data) as u32,
                )
            {
                println!("SetFileInformationByHandle failed:{:x}", GetLastError());
                return Err(LimboError::InternalError(format!(
                    "SetFileInformationByHandle failed:{:x}",
                    GetLastError()
                )));
            }
        }
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let mut filesize = 0i64;
        unsafe {
            if FALSE == GetFileSizeEx(self.handle, &raw mut filesize) {
                println!("GetFileSizeEx failed:{:x}", GetLastError());

                return Err(LimboError::InternalError(format!(
                    "GetFileSizeEx failed:{:x}",
                    GetLastError()
                )));
            }
        }

        let filesize = filesize.try_into().map_err(|_| LimboError::TooBig)?;
        Ok(filesize)
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        unsafe {
            CancelIo(self.handle);
            CloseHandle(self.handle);
        }
        self.handle = INVALID_HANDLE_VALUE;
    }
}
