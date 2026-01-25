// TODO: Add more logs and tracing
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Arc;

use crate::sync::{Mutex, RwLock};
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};

use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::ptr::NonNull;
use windows_sys::core::BOOL;

use std::{ptr, u32};
use tracing::{debug, instrument, trace, warn, Level};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_IO_PENDING, ERROR_OPERATION_ABORTED, FALSE, GENERIC_READ,
    GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, MAX_PATH, TRUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileEndOfFileInfo, FlushFileBuffers, GetFileSizeEx, LockFile, ReadFile,
    SetFileInformationByHandle, UnlockFile, WriteFile, FILE_END_OF_FILE_INFO,
    FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_FLAG_WRITE_THROUGH, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{
    CancelIo, CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
    OVERLAPPED_0, OVERLAPPED_0_0,
};

#[derive(Clone)]
struct IORequest {
    handle: HANDLE,
    ovl: Arc<IoOverlapped>,
}

fn get_key(c: &Completion) -> usize {
    Arc::as_ptr(c.get_inner()) as usize
}

fn consume_and_get_ptr(c: Completion) -> usize {
    Arc::into_raw(c.get_inner().clone()) as usize
}

fn restore_and_forget(key: usize) {
    unsafe { Arc::from_raw(key as *const Completion) };
}

pub struct WindowsIOCP {
    inner: Arc<InnerWindowsIOCP>,
}

#[repr(C)]
struct IoOverlapped {
    ov: OVERLAPPED,
    completion: Option<Completion>,
}

pub struct InnerWindowsIOCP {
    handle: HANDLE,
    free_ovs: Mutex<VecDeque<Arc<IoOverlapped>>>,
    tracked_ovs: RwLock<HashMap<usize, IORequest>>,
}

impl InnerWindowsIOCP {
    fn new(handle: HANDLE) -> Arc<Self> {
        let mut vecq = VecDeque::with_capacity(128);

        for _ in 0..128 {
            vecq.push_back(Arc::new(IoOverlapped {
                ov: OVERLAPPED::default(),
                completion: None,
            }));
        }

        Arc::new(Self {
            handle,
            free_ovs: Mutex::new(vecq),
            tracked_ovs: RwLock::new(HashMap::with_capacity(128)),
        })
    }

    fn salvage_or_create_overlapped(&self, c: Completion, pos: u64) -> Arc<IoOverlapped> {
        let low = pos as u32;
        let high = (pos >> 32) as u32;

        let mut ovl = self.free_ovs.lock().pop_front().unwrap_or_else(|| {
            Arc::new(IoOverlapped {
                ov: OVERLAPPED::default(),
                completion: None,
            })
        });

        let content =
            Arc::get_mut(&mut ovl).expect("This object should have no references elsewhere");

        *content = IoOverlapped {
            completion: Some(c),
            ov: OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: low,
                        OffsetHigh: high,
                    },
                },
                ..Default::default()
            },
        };
        ovl
    }

    fn track_overlapped(&self, handle: HANDLE, ovl: Arc<IoOverlapped>) -> Option<()> {
        let completion = ovl.completion.as_ref()?.clone();
        let key = consume_and_get_ptr(completion);

        self.tracked_ovs
            .write()
            .insert(key, IORequest { handle, ovl });

        Some(())
    }

    fn forget_overlapped(&self, mut ovl: Arc<IoOverlapped>) -> Option<Completion> {
        let key = {
            let ov = ovl.completion.as_ref()?;
            get_key(ov)
        };

        if let Some((key, _val)) = self.tracked_ovs.write().remove_entry(&key) {
            restore_and_forget(key);
        }

        let cmpl = Arc::get_mut(&mut ovl)?.completion.take()?;

        self.free_ovs.lock().push_back(ovl);
        Some(cmpl)
    }

    fn pop_io_request_from_completion(&self, c: &Completion) -> Option<IORequest> {
        let key = get_key(c);
        self.tracked_ovs.write().remove(&key)
    }
}

enum GetIOCPPacketError {
    Empty,
    SystemError(u32),
    Aborted,
    InvalidIO,
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

    fn process_packet_from_iocp(&self) -> Result<(), GetIOCPPacketError> {
        let mut overlapped_ptr = ptr::null_mut();
        let mut bytes = 0;
        let mut key = 0;

        let result = unsafe {
            GetQueuedCompletionStatus(
                self.inner.handle,
                &raw mut bytes,
                &raw mut key,
                &raw mut overlapped_ptr,
                0,
            )
        };

        let error = unsafe { GetLastError() };

        let Some(overlapped_ptr) = NonNull::new(overlapped_ptr) else {
            return Err(match (result, error) {
                (FALSE, WAIT_TIMEOUT) => GetIOCPPacketError::Empty,
                (FALSE, e) => GetIOCPPacketError::SystemError(e),
                (TRUE, _) => GetIOCPPacketError::Aborted,
                _ => unreachable!(),
            });
        };

        let overlapped = unsafe { Arc::<IoOverlapped>::from_raw(overlapped_ptr.as_ptr().cast()) };
        let completion = self
            .inner
            .forget_overlapped(overlapped)
            .ok_or(GetIOCPPacketError::InvalidIO)?;

        match (result, error) {
            (TRUE, _) => {
                let bytes = bytes as i32;
                completion.complete(bytes);
            }
            (FALSE, ERROR_OPERATION_ABORTED) => {
                completion.abort();
            }
            (FALSE, error) => {
                let error = error as i32;
                let err = std::io::Error::from_raw_os_error(error);
                completion.error(err.into());
            }
            (_, _) => unreachable!(),
        }
        Ok(())
    }
}

unsafe impl Send for WindowsIOCP {}
unsafe impl Sync for WindowsIOCP {}
crate::assert::assert_send_sync!(WindowsIOCP);

impl IO for WindowsIOCP {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        debug!("open_file(path = {})", path);

        let vec: SmallVec<[u16; 1024]> = SmallVec::new();

        let encoded_path = path
            .encode_utf16()
            .chain(std::iter::once(0))
            .fold(vec, |mut acc, v| {
                acc.push(v);
                acc
            });

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
            let result = CreateIoCompletionPort(file, self.inner.handle, 0, 0);

            if result.is_null() {
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
            if let Some(IORequest { handle, ovl }) = self.inner.pop_io_request_from_completion(c) {
                unsafe {
                    if FALSE == CancelIoEx(handle, &ovl.ov as *const OVERLAPPED) {
                        trace!("CancelIoEx failed:{:x}", GetLastError());
                    };
                }
            } else {
                c.abort();
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn drain(&self) -> Result<()> {
        loop {
            if let Err(GetIOCPPacketError::Empty) = self.process_packet_from_iocp() {
                break;
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        let _ = self.process_packet_from_iocp();
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
    fn generate_overlapped(&self, pos: u64, c: Completion) -> Arc<IoOverlapped> {
        self.iocp.salvage_or_create_overlapped(c, pos)
    }

    fn perform_iocp_operation(
        &self,
        pos: u64,
        c: Completion,
        io_func: impl Fn(*mut OVERLAPPED) -> BOOL,
    ) -> () {
        if c.finished() {
            return;
        }

        let overlapped = self.generate_overlapped(pos, c.clone());
        let overlapped_ptr = Arc::into_raw(overlapped.clone()).cast_mut() as *mut OVERLAPPED;
        self.iocp.track_overlapped(self.handle, overlapped);

        unsafe {
            if io_func(overlapped_ptr) != FALSE || GetLastError() != ERROR_IO_PENDING {
                let arc = Arc::from_raw(overlapped_ptr as *mut IoOverlapped);
                let error = GetLastError() as i32;
                let err = std::io::Error::from_raw_os_error(error);
                let completion = self
                    .iocp
                    .forget_overlapped(arc)
                    .expect("Completion should be single-referenced and tracked");
                completion.error(err.into());
                return;
            }
        }
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
        let read = c.as_read();
        let buf = read.buf();
        let ptr = buf.as_mut_ptr();
        let len = buf.len().try_into().map_err(|_| LimboError::TooBig)?;

        self.perform_iocp_operation(pos, c.clone(), |ov| unsafe {
            ReadFile(self.handle, ptr, len, ptr::null_mut(), ov)
        });
        Ok(c)
    }

    #[instrument(skip(self, c, buffer), level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let ptr = buffer.as_mut_ptr();
        let len = buffer.len().try_into().map_err(|_| LimboError::TooBig)?;

        self.perform_iocp_operation(pos, c.clone(), |ov| unsafe {
            WriteFile(self.handle, ptr, len, ptr::null_mut(), ov)
        });
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
            //let x = x.unwrap();

            println!(">>>  {:?}", x);
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
