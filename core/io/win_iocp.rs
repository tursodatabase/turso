// TODO: Add more logs and tracing
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Arc;

use crate::sync::{Mutex, RwLock};
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};

use smallvec::{Array, SmallVec};
use std::collections::{HashMap, VecDeque};

use std::ptr::NonNull;
use std::{ptr, u32};
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
    CancelIo, CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatus,
    GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0,
};

use windows_sys::Win32::System::Threading::INFINITE;

#[derive(Clone)]
struct IORequest {
    handle: HANDLE,
    ovl: Arc<IoOverlapped>,
}

fn get_key(c: &Completion) -> *const () {
    Arc::as_ptr(c.get_inner()).cast()
}

fn consume_and_get_ptr(c: Completion) -> *const () {
    Arc::into_raw(c.get_inner().clone()).cast()
}

pub struct WindowsIOCP {
    inner: Arc<InnerWindowsIOCP>,
}

pub struct InnerWindowsIOCP {
    handle: HANDLE,
    tracked_ovs: RwLock<HashMap<*const (), IORequest>>,
}

impl InnerWindowsIOCP {
    fn new(handle: HANDLE) -> Arc<Self> {
        Arc::new(Self {
            handle,
            tracked_ovs: RwLock::new(HashMap::new()),
        })
    }

    fn track_overlapped(&self, handle: HANDLE, ovl: Arc<IoOverlapped>) {
        let key = get_key(&ovl.completion);
        self.tracked_ovs
            .write()
            .insert(key, IORequest { handle, ovl });
    }

    fn forget_overlapped(&self, ovl: Arc<IoOverlapped>) {
        let key = get_key(&ovl.completion);

        self.tracked_ovs.write().remove(&key);
    }

    fn get_io_request_from_completion(&self, c: &Completion) -> Option<IORequest> {
        let key = get_key(c);
        self.tracked_ovs.read().get(&key).cloned()
    }
}

impl WindowsIOCP {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'win_iocp'");

        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 0) };
        if handle == INVALID_HANDLE_VALUE {
            return Err(LimboError::NullValue);
        }
        Ok(Self {
            inner: InnerWindowsIOCP::new(handle),
        })
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

        let mut encoded_path =
            path.encode_utf16()
                .fold(SmallVec::<[u16; 1024]>::new(), |mut acc, v| {
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
            let ret = CreateIoCompletionPort(file, self.inner.handle, 0, 0);

            if ret.is_null() {
                return Err(LimboError::InternalError(format!(
                    "CreatFile failed: {}",
                    GetLastError()
                )));
            };

            Ok(Arc::new(WindowsFile {
                handle: file,
                iocp: self.inner.clone(),
            }))
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, path: &str) -> Result<()> {
        trace!("remove_file(path = {})", path);
        Ok(std::fs::remove_file(path)?)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        for c in completions {
            c.abort();
            let IORequest { handle, ovl } = self
                .inner
                .get_io_request_from_completion(c)
                .expect("Completion not found");
            unsafe {
                if FALSE == CancelIoEx(handle, &ovl.ov as *const OVERLAPPED) {
                    return Err(LimboError::InternalError(format!(
                        "CancelIoEx failed:{:x}",
                        GetLastError()
                    )));
                };
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn drain(&self) -> Result<()> {
        let mut overlapped_ptr = ptr::null_mut();
        let mut bytes = 0;
        let mut key = 0;
        loop {
            unsafe {
                let result = GetQueuedCompletionStatus(
                    self.inner.handle,
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

                let overlapped = Arc::from_raw(overlapped_ptr.as_ptr());

                self.inner.forget_overlapped(overlapped.clone());

                if result == TRUE {
                    if !overlapped.completion.finished() {
                        let bytes = bytes.try_into().map_err(|_| LimboError::NullValue)?;
                        overlapped.completion.complete(bytes);
                    }
                } else {
                    let error = GetLastError();
                    if error == ERROR_OPERATION_ABORTED {
                        // handle aborted
                    } else {
                        let error = error as i32;
                        let err = std::io::Error::from_raw_os_error(error);
                        overlapped.completion.error(err.into());
                    }
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
                self.inner.handle,
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

            let overlapped = Arc::<IoOverlapped>::from_raw(overlapped_ptr.cast());

            self.inner.forget_overlapped(overlapped.clone());

            if overlapped.completion.finished() {
                return Ok(());
            }

            if result == TRUE {
                let bytes = bytes as i32;
                overlapped.completion.complete(bytes);
            } else {
                // I/O Operation failed
                let error = GetLastError();
                if error == ERROR_OPERATION_ABORTED {
                    return Ok(());
                }
                let error = error as i32;
                let err = std::io::Error::from_raw_os_error(error);
                overlapped.completion.error(err.into());
            }
        }
        Ok(())
    }
}

impl Drop for InnerWindowsIOCP {
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
    iocp: Arc<InnerWindowsIOCP>,
}

impl WindowsFile {
    fn generate_overlapped(&self, pos: u64, c: Completion) -> *mut OVERLAPPED {
        let low = pos as u32;
        let high = (pos >> 32) as u32;

        let overlapped = Arc::new(IoOverlapped {
            ov: OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: low,
                        OffsetHigh: high,
                    },
                },
                ..Default::default()
            },
            completion: c,
        });

        self.iocp.track_overlapped(self.handle, overlapped.clone());

        let overlapped = Arc::into_raw(overlapped);
        overlapped.cast_mut() as *mut OVERLAPPED
    }
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

        let overlapped = self.generate_overlapped(pos, c.clone());

        unsafe {
            if FALSE == ReadFile(self.handle, ptr, len, ptr::null_mut(), overlapped)
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

        let overlapped = self.generate_overlapped(pos, c.clone());

        unsafe {
            if FALSE == WriteFile(self.handle, ptr, len, ptr::null_mut(), overlapped)
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

#[cfg(test)]
mod tests {

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
    fn test_windows_iocp_cancel() {
        let iocp = WindowsIOCP::new();
        assert!(iocp.is_ok());
        let iocp = iocp.unwrap();
        let file = iocp.open_file("osama.txt", OpenFlags::ReadOnly, false);
        assert!(file.is_ok());
        let buffer = Arc::new(crate::Buffer::new_temporary(128));

        let c = Completion::new_read(buffer, |x| {
            println!(">>> {:?} ", x);
            None
        });
        let read = file.unwrap().pread(35, c.clone());
        iocp.cancel(&[c]);

        // match read {
        //     Ok(_) => {}
        //     Err(ref err) => println!("{err}"),
        // };

        assert!(read.is_ok());
        let res = iocp.step();
        assert!(res.is_ok());
    }
}
