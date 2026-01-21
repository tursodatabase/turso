// TODO: Add more logs and tracing
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::{Clock, Completion, File, LimboError, OpenFlags, Result, IO};
use std::ffi::OsString;
use std::io::{Read, Seek, Write};
use std::ops::Deref;
use std::os::windows::ffi::{self, OsStringExt};
use std::ptr::{addr_of, addr_of_mut, NonNull};
use std::{mem, ptr, u32};
use tracing::{debug, instrument, trace, warn, Level};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_IO_PENDING, FALSE, GENERIC_READ, GENERIC_WRITE, HANDLE,
    INVALID_HANDLE_VALUE, MAX_PATH, TRUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FlushFileBuffers, GetFileSizeEx, LockFile, ReadFile, ReadFileEx, SetEndOfFile,
    UnlockFile, WriteFile, CREATE_NEW, FILE_FLAG_OVERLAPPED, FILE_SHARE_READ,
    LOCKFILE_EXCLUSIVE_LOCK, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, GetQueuedCompletionStatusEx, OVERLAPPED,
};

use windows_sys::Win32::System::Threading::INFINITE;
struct ClosableHandle {
    value: HANDLE,
}

impl ClosableHandle {
    fn from(value: HANDLE) -> Self {
        Self { value }
    }
}

impl Deref for ClosableHandle {
    type Target = HANDLE;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl Drop for ClosableHandle {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.value) };
    }
}

pub struct WindowsIOCP {
    handle: ClosableHandle,
}

impl WindowsIOCP {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'win_iocp'");

        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 0) };
        if handle == INVALID_HANDLE_VALUE {
            return Err(LimboError::NullValue);
        }
        let handle = ClosableHandle::from(handle);
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

impl IO for WindowsIOCP {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);

        let path = path.encode_utf16().fold(
            smallvec::SmallVec::<[u16; 1024 as usize]>::new(),
            |mut acc, v| {
                acc.push(v);
                acc
            },
        );

        let mut desired_access = 0;
        let mut creation_disposition = 0;
        desired_access |= if flags.contains(OpenFlags::ReadOnly) {
            GENERIC_READ
        } else {
            GENERIC_WRITE | GENERIC_READ
        };

        creation_disposition |= if flags.contains(OpenFlags::Create) {
            CREATE_NEW
        } else {
            OPEN_EXISTING
        };

        let file = unsafe {
            let handle = CreateFileW(
                path.as_ptr(),
                desired_access,
                FILE_SHARE_READ,
                ptr::null(),
                creation_disposition,
                FILE_FLAG_OVERLAPPED,
                ptr::null_mut(),
            );

            if handle == INVALID_HANDLE_VALUE {
                return Err(LimboError::NullValue);
            };

            let file = ClosableHandle::from(handle);

            let iocp = CreateIoCompletionPort(handle, *self.handle, 0, 0);
            if handle == INVALID_HANDLE_VALUE {
                return Err(LimboError::NullValue);
            };

            file
        };

        Ok(Arc::new(WindowsFile {
            file: RwLock::new(file),
        }))
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        Ok(std::fs::remove_file(path)?)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        let mut overlapped_ptr = ptr::null_mut();
        let mut bytes = 0;
        let mut key = 0;
        unsafe {
            if TRUE
                == GetQueuedCompletionStatus(
                    *self.handle,
                    addr_of_mut!(bytes),
                    addr_of_mut!(key),
                    addr_of_mut!(overlapped_ptr),
                    INFINITE,
                )
            {
                let overlapped_ptr: *mut IoOverlapped = overlapped_ptr.cast();
                let overlapped_ptr = NonNull::new(overlapped_ptr).ok_or(LimboError::NullValue)?;

                let overlapped = Box::from_raw(overlapped_ptr.as_ptr());

                overlapped.completion.complete(
                    overlapped
                        .ov
                        .InternalHigh
                        .try_into()
                        .map_err(|_| LimboError::NullValue)?,
                );
            } else {
                println!("step failed: {}", GetLastError());
                return Err(LimboError::InternalError(format!(
                    "step failed: {}",
                    GetLastError()
                )));
            }
        }
        Ok(())
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
    file: RwLock<ClosableHandle>,
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let lock = self.file.write();

        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        };

        unsafe {
            if FALSE == LockFile(**lock, flags, 0, u32::MAX, u32::MAX) {
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
        let lock = self.file.write();

        unsafe {
            if FALSE == UnlockFile(**lock, 0, 0, u32::MAX, u32::MAX) {
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
        let lock = self.file.read();
        let read = c.as_read();
        let buf = read.buf();
        let ptr = buf.as_mut_ptr();
        let len = buf.len().try_into().map_err(|_| LimboError::TooBig)?;

        println!("Len = {len}");
        let overlapped = Box::leak(Box::new(IoOverlapped {
            ov: OVERLAPPED::default(),
            completion: c.clone(),
        }));

        let low = pos as u32;
        let high = (pos >> 32) as u32;
        overlapped.ov.Anonymous.Anonymous.Offset = low;
        overlapped.ov.Anonymous.Anonymous.OffsetHigh = high;

        unsafe {
            if FALSE
                == ReadFile(
                    **lock,
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
        let lock = self.file.write();

        let ptr = buffer.as_mut_ptr();
        let len = buffer.len().try_into().map_err(|_| LimboError::TooBig)?;

        let overlapped = Box::leak(Box::new(IoOverlapped {
            ov: OVERLAPPED::default(),
            completion: c.clone(),
        }));

        let low = pos as u32;
        let high = (pos >> 32) as u32;
        overlapped.ov.Anonymous.Anonymous.Offset = low;
        overlapped.ov.Anonymous.Anonymous.OffsetHigh = high;

        unsafe {
            if FALSE
                == WriteFile(
                    **lock,
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
        let lock = self.file.write();

        unsafe {
            if FALSE == FlushFileBuffers(**lock) {
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
        let lock = self.file.write();

        unsafe {
            if FALSE == SetEndOfFile(**lock) {
                return Err(LimboError::InternalError(format!(
                    "SetEndOfFile failed:{:x}",
                    GetLastError()
                )));
            }
        }
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let lock = self.file.read();

        let mut filesize = 0i64;
        unsafe {
            if FALSE == GetFileSizeEx(**lock, addr_of_mut!(filesize)) {
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

#[cfg(test)]
mod tests {
    use ryu::Buffer;

    use super::*;
    use crate::io::common;

    #[test]
    fn test_windows_iocp() {
        let iocp = WindowsIOCP::new();
        assert!(iocp.is_ok());
        let iocp = iocp.unwrap();
        let file = iocp.open_file("osama.txt", OpenFlags::ReadOnly, false);
        assert!(file.is_ok());
        let buffer = Arc::new(crate::Buffer::new_temporary(128));

        let c = Completion::new_read(buffer, |x| {
            let x = x.unwrap();
            let y = x.0;
            let y = y.as_slice();
            println!(">>> {:?} {}", y, x.1);
            None
        });
        let read = file.unwrap().pread(35, c.clone());
        match read {
            Ok(_) => {}
            Err(ref err) => println!("{err}"),
        };

        assert!(read.is_ok());
        let res = iocp.step();
        assert!(res.is_ok());
    }

    #[test]
    fn test_windows_iocp_write() {
        let iocp = WindowsIOCP::new();
        assert!(iocp.is_ok());
        let iocp = iocp.unwrap();
        let file = iocp.open_file("osama1.txt", OpenFlags::Create, false);
        assert!(file.is_ok());
        let buffer = Arc::new(crate::Buffer::new_temporary(5));
        buffer.as_mut_slice().copy_from_slice(b"Osama");

        let c = Completion::new_write(|x| {
            let x = x.unwrap();

            println!(">>>  {}", x);
        });
        let write = file.unwrap().pwrite(0, buffer, c.clone());
        match write {
            Ok(_) => {}
            Err(ref err) => println!("{err}"),
        };

        assert!(write.is_ok());
        let res = iocp.step();
        assert!(res.is_ok());
    }

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
    }
}
