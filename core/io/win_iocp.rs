// Windows IOCP Cycle
// ===================
//
//                                   pread/pwrite
//                                        |
//                                        |      Get Packet
//                  Completion -----> IO Packet <-----------|
//                                     |  |                 |
//                         |<-- Track -|  |                 |
//                         |              |                 |
//                    ==========      Issuing IO        ==========
//                    [||||||||]        queue           ||||||||||
//                    ==========          |             ==========
//                     Tracked        ( Windows )     Free IO Packets
//                     Packets            |                 |
//                         |              |                 |
//              Cancel     |   Untrack    |    -->(abort)   |
//            ------------>|===========> Step ..............|
//                         |              |                 |
//                         |              |                 |
//                         |           Io Completed         |
//                         |              |                 |
//                         |   Untrack    |      Reuse      |
//                         |-----------> Step ------------->|
//                                        |      Packet
//                                        |
//                                   To Completion
//                                        -->(complete/error)
//
//
// Assumption
// ==========
// - The IOPacket should have one reference just after withdrawing and before deposit
//   back to object pools.
// - The only place that should forget IO Packet should be in process queue step
//   OR failure cases just after issueing IO.
// - in Sync, IO Pakcet should not be touched, it should be handled in -and only in-
//  `process_packet_from_iocp`

use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::sync::Arc;

use crate::sync::Mutex;
use crate::{Clock, Completion, File, LimboError, OpenFlags, Result, IO};

use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::ffi::OsString;
use std::os::windows::ffi::OsStringExt;
use std::ptr::NonNull;
use windows_sys::core::BOOL;
use windows_sys::Win32::System::Diagnostics::Debug::{
    FormatMessageW, FORMAT_MESSAGE_ALLOCATE_BUFFER, FORMAT_MESSAGE_FROM_SYSTEM,
    FORMAT_MESSAGE_IGNORE_INSERTS,
};

use std::{io, mem, ptr};
use tracing::{debug, instrument, trace, warn, Level};

use super::FileSyncType;
use crate::io::completions::CompletionInner;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_IO_PENDING, ERROR_OPERATION_ABORTED, FALSE,
    GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, TRUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileEndOfFileInfo, FlushFileBuffers, GetFileSizeEx, LockFileEx, ReadFile,
    SetFileInformationByHandle, UnlockFileEx, WriteFile, FILE_END_OF_FILE_INFO,
    FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_FLAG_WRITE_THROUGH, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatus, OVERLAPPED,
    OVERLAPPED_0, OVERLAPPED_0_0,
};

// Constants

const CACHING_CAPACITY: usize = 128;

// Types

#[derive(Clone)]
struct IoContext {
    file_handle: HANDLE,
    io_packet: IoPacket,
}

enum GetIOCPPacketError {
    Empty,
    SystemError(u32),
    Aborted,
    InvalidIO,
}

#[repr(C)]
#[derive(Debug)]
enum IoKind {
    Write(Arc<crate::Buffer>),
    Read,
    Lock,
    Unlock,
    Unknown,
}

#[repr(C)]
struct IoOverlappedPacket {
    overlapped: OVERLAPPED,
    completion: Option<Completion>,
    kind: IoKind,
}

impl std::fmt::Debug for IoOverlappedPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IoOverlappedPacket {{")?;
        writeln!(f, "-- completion: {:?} ", self.completion)?;
        writeln!(f, "-- kind: {:?} ", self.kind)?;
        writeln!(f, "}}")?;

        Ok(())
    }
}

type IoPacket = Arc<IoOverlappedPacket>;
type CompletionKey = *const CompletionInner;

// Functions
#[inline]
fn get_unique_key_from_completion(c: &Completion) -> CompletionKey {
    Arc::as_ptr(c.get_inner())
}

#[inline]
fn get_limboerror_from_os_err(err: u32) -> LimboError {
    let Ok(error_code) = err.try_into() else {
        return LimboError::InternalError(format!("Unknown error [{err}]"));
    };

    let error = std::io::Error::from_raw_os_error(error_code);
    error.into()
}

#[inline]
fn get_generic_limboerror_from_last_os_err() -> LimboError {
    get_generic_limboerror_from_os_err(unsafe { GetLastError() })
}

#[inline]
fn get_generic_limboerror_from_os_err(err: u32) -> LimboError {
    let mut buffer: *mut u16 = ptr::null_mut();
    unsafe {
        let size = FormatMessageW(
            FORMAT_MESSAGE_ALLOCATE_BUFFER
                | FORMAT_MESSAGE_FROM_SYSTEM
                | FORMAT_MESSAGE_IGNORE_INSERTS,
            ptr::null(),
            err,
            0,
            (&raw mut buffer).cast(),
            0,
            ptr::null(),
        );

        if buffer.is_null() || size == 0 {
            return LimboError::InternalError(format!("Windows Error: [{}]", err));
        }

        let Ok(size) = size.try_into() else {
            LocalFree(buffer.cast());
            return LimboError::InternalError(format!("Windows Error: [{}]", err));
        };

        let buffer_slice = std::slice::from_raw_parts(buffer, size);
        let string = OsString::from_wide(buffer_slice);

        LocalFree(buffer.cast());

        let Ok(string) = string.into_string() else {
            return LimboError::InternalError(format!("Windows Error: [{}]", err));
        };

        LimboError::InternalError(format!("Windows Error: [{err}]{string}"))
    }
}

#[inline]
fn get_limboerror_from_last_os_err() -> LimboError {
    get_limboerror_from_os_err(unsafe { GetLastError() })
}

#[inline]
fn get_limboerror_from_std_error(err: impl Error) -> LimboError {
    LimboError::InternalError(err.to_string())
}

// Windows IOCP

pub struct WindowsIOCP {
    instance: Arc<InnerWindowsIOCP>,
}

impl WindowsIOCP {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'win_iocp'");

        let iocp_queue_handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 0) };
        if iocp_queue_handle == INVALID_HANDLE_VALUE {
            return Err(LimboError::NullValue);
        }
        Ok(Self {
            instance: InnerWindowsIOCP::new(iocp_queue_handle),
        })
    }
}

unsafe impl Send for WindowsIOCP {}
unsafe impl Sync for WindowsIOCP {}
crate::assert::assert_send_sync!(WindowsIOCP);

impl IO for WindowsIOCP {
    #[instrument(skip_all, level = Level::TRACE)]
    fn open_file(
        &self,
        file_path: &str,
        open_flags: OpenFlags,
        direct_access: bool,
    ) -> Result<Arc<dyn File>> {
        debug!("open_file(path = {})", file_path);

        let path_unicode: SmallVec<[u16; 1024]> = SmallVec::new();

        let unicode_path =
            file_path
                .encode_utf16()
                .chain(std::iter::once(0))
                .fold(path_unicode, |mut acc, v| {
                    acc.push(v);
                    acc
                });

        let mut desired_access = 0;
        let mut creation_disposition = 0;

        desired_access |= if open_flags.contains(OpenFlags::ReadOnly) {
            GENERIC_READ
        } else {
            GENERIC_WRITE | GENERIC_READ
        };

        creation_disposition |= if open_flags.contains(OpenFlags::Create) {
            OPEN_ALWAYS
        } else {
            OPEN_EXISTING
        };

        let flags_and_attributes = if direct_access {
            FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH
        } else {
            FILE_FLAG_OVERLAPPED
        };

        let shared_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

        unsafe {
            let file_handle = CreateFileW(
                unicode_path.as_ptr(),
                desired_access,
                shared_mode,
                ptr::null(),
                creation_disposition,
                flags_and_attributes,
                ptr::null_mut(),
            );

            if file_handle == INVALID_HANDLE_VALUE {
                return Err(get_generic_limboerror_from_last_os_err());
            };

            let windows_file = Arc::new(WindowsFile {
                file_handle,
                parent_io: self.instance.clone(),
            });

            // Bind file to IOCP
            let result = CreateIoCompletionPort(file_handle, self.instance.iocp_queue_handle, 0, 0);

            if result.is_null() {
                return Err(get_generic_limboerror_from_last_os_err());
            };

            if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
                || !open_flags.contains(OpenFlags::ReadOnly)
            {
                windows_file.lock_file(true)?;
            }

            Ok(windows_file)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, file_path: &str) -> Result<()> {
        trace!("remove_file(path = {})", file_path);
        Ok(std::fs::remove_file(file_path)?)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        for cmpl in completions {
            trace!("cancelling {}", get_unique_key_from_completion(cmpl).addr());
            let mut succeeded = false;
            if let Some(IoContext {
                file_handle,
                io_packet,
            }) = self.instance.pop_io_context_from_completion(cmpl)
            {
                unsafe {
                    if CancelIoEx(file_handle, &raw const io_packet.overlapped) == TRUE {
                        // if succeeded the abort will be performed once cancel completed
                        succeeded = true;
                    } else {
                        trace!("CancelIoEx failed:{}.. Ignored", GetLastError());
                    };
                }
            }

            if !succeeded {
                cmpl.abort();
            }
        }
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn drain(&self) -> Result<()> {
        trace!("I/O drainning..");

        self.instance.drain()
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        trace!("I/O Step..");

        match self.instance.process_packet_from_iocp() {
            Err(GetIOCPPacketError::SystemError(code)) => {
                Err(get_generic_limboerror_from_os_err(code))
            }
            Err(GetIOCPPacketError::Aborted)
            | Err(GetIOCPPacketError::Empty)
            | Err(GetIOCPPacketError::InvalidIO)
            | Ok(()) => Ok(()),
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

// Inner IOCP
//
pub struct InnerWindowsIOCP {
    iocp_queue_handle: HANDLE,
    free_io_packets: Mutex<VecDeque<IoPacket>>,
    tracked_io_packets: Mutex<HashMap<CompletionKey, IoContext>>,
}

unsafe impl Send for InnerWindowsIOCP {}
unsafe impl Sync for InnerWindowsIOCP {}
crate::assert::assert_send_sync!(WindowsFile);

impl InnerWindowsIOCP {
    fn new(iocp_handle: HANDLE) -> Arc<Self> {
        let mut free_packets = VecDeque::with_capacity(CACHING_CAPACITY);

        for _ in 0..CACHING_CAPACITY {
            free_packets.push_back(Arc::new(IoOverlappedPacket {
                overlapped: OVERLAPPED::default(),
                completion: None,
                kind: IoKind::Unknown,
            }));
        }

        Arc::new(Self {
            iocp_queue_handle: iocp_handle,
            free_io_packets: Mutex::new(free_packets),
            tracked_io_packets: Mutex::new(HashMap::with_capacity(CACHING_CAPACITY)),
        })
    }

    fn recycle_or_create_io_packet(&self, kind: IoKind) -> IoPacket {
        self.free_io_packets.lock().pop_front().unwrap_or_else(|| {
            Arc::new(IoOverlappedPacket {
                overlapped: OVERLAPPED::default(),
                completion: None,
                kind,
            })
        })
    }

    fn recycle_or_create_io_packet_from_completion(
        &self,
        completion: Completion,
        position: u64,
        kind: IoKind,
    ) -> IoPacket {
        trace!("new salvaged overlapped packet. ");

        let mut packet = self.recycle_or_create_io_packet(IoKind::Unknown);

        assert!(
            packet.completion.is_none(),
            "New packet should has no completion"
        );

        let content =
            Arc::get_mut(&mut packet).expect("This IO Packet should not have references elsewhere");

        let low_part = position as u32;
        let high_part = (position >> 32) as u32;

        *content = IoOverlappedPacket {
            completion: Some(completion),
            kind,
            overlapped: OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: low_part,
                        OffsetHigh: high_part,
                    },
                },
                ..Default::default()
            },
        };
        packet
    }

    fn map_completion_to_io_packet(&self, file_handle: HANDLE, io_packet: IoPacket) -> bool {
        let Some(completion) = io_packet.completion.as_ref().cloned() else {
            return false;
        };

        let mut lock = self.tracked_io_packets.lock();

        let completion_key = get_unique_key_from_completion(&completion);

        if lock.contains_key(&completion_key) {
            panic!("Completion should have one and only one io packet, this should not happen");
        }

        let completion_key = get_unique_key_from_completion(&completion);
        trace!("tracked completion for {}", completion_key.addr());
        lock.insert(
            completion_key,
            IoContext {
                file_handle,
                io_packet,
            },
        );
        true
    }

    fn forget_io_packet(&self, mut io_packet: IoPacket) -> Option<(Option<Completion>, IoKind)> {
        trace!("forget packet and completion");

        if let Some(completion) = io_packet.completion.as_ref() {
            // this may be removed earlier in cancel
            // so this operation is optional if the record exists
            let _ = self.pop_io_context_from_completion(completion);
        };

        let internals = Arc::get_mut(&mut io_packet).unwrap();
        let completion = internals.completion.take();
        let kind = mem::replace(&mut internals.kind, IoKind::Unknown);

        self.free_io_packets.lock().push_back(io_packet);
        Some((completion, kind))
    }

    fn pop_io_context_from_completion(&self, completion: &Completion) -> Option<IoContext> {
        let key = get_unique_key_from_completion(completion);
        if let Some((key, context)) = self.tracked_io_packets.lock().remove_entry(&key) {
            trace!("remove completion {} from mapped IO table", key.addr());
            return Some(context);
        }
        None
    }

    fn process_packet_from_iocp(&self) -> Result<(), GetIOCPPacketError> {
        let mut overlapped_ptr = ptr::null_mut();
        let mut bytes_received = 0;
        let mut iocp_key = 0;

        let result = unsafe {
            GetQueuedCompletionStatus(
                self.iocp_queue_handle,
                &raw mut bytes_received,
                &raw mut iocp_key,
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

        let io_packet = unsafe { IoPacket::from_raw(overlapped_ptr.as_ptr().cast()) };

        let data = self
            .forget_io_packet(io_packet)
            .ok_or(GetIOCPPacketError::InvalidIO)?;

        if let IoKind::Write(buffer) = data.1 {
            drop(buffer);
        }

        let completion = data.0.ok_or(GetIOCPPacketError::InvalidIO)?;
        match (result, error) {
            (TRUE, _) => {
                trace!(
                    "completion {} completed",
                    get_unique_key_from_completion(&completion).addr()
                );
                completion.complete(
                    bytes_received
                        .try_into()
                        .map_err(|_| GetIOCPPacketError::InvalidIO)?,
                );
            }
            (FALSE, ERROR_OPERATION_ABORTED) => {
                trace!(
                    "completion {} cancelled",
                    get_unique_key_from_completion(&completion).addr()
                );
                completion.abort();
            }
            (FALSE, error_code) => {
                let error = io::Error::from_raw_os_error(
                    error_code
                        .try_into()
                        .map_err(|_| GetIOCPPacketError::InvalidIO)?,
                );
                trace!(
                    "completion {} errored {error}",
                    get_unique_key_from_completion(&completion).addr()
                );

                completion.error(error.into());
            }
            (_, _) => unreachable!(),
        }
        Ok(())
    }

    fn drain(&self) -> Result<()> {
        loop {
            match self.process_packet_from_iocp() {
                Err(GetIOCPPacketError::Empty | GetIOCPPacketError::Aborted) => {
                    break;
                }
                Err(GetIOCPPacketError::SystemError(e)) => {
                    let error = e.try_into().map_err(get_limboerror_from_std_error)?;
                    let err = std::io::Error::from_raw_os_error(error);
                    return Err(err.into());
                }
                Err(GetIOCPPacketError::InvalidIO) | Ok(()) => {}
            }
        }
        Ok(())
    }
}

impl Drop for InnerWindowsIOCP {
    fn drop(&mut self) {
        trace!("Dropping Windows IOCP Queue..");

        self.tracked_io_packets
            .lock()
            .drain()
            .for_each(|(_key, ctx)| {
                unsafe { CancelIoEx(ctx.file_handle, &raw const ctx.io_packet.overlapped) };
            });

        let _ = self.drain();

        unsafe {
            CloseHandle(self.iocp_queue_handle);
        }
    }
}

// Windows File

pub struct WindowsFile {
    file_handle: HANDLE,
    parent_io: Arc<InnerWindowsIOCP>,
}

impl WindowsFile {
    fn sync_iocp_operation(
        &self,
        kind: IoKind,
        io_function: impl Fn(*mut OVERLAPPED) -> BOOL,
    ) -> Result<(), u32> {
        let mut bytes = 0;
        let packet_io = self.parent_io.recycle_or_create_io_packet(kind);
        let overlapped_ptr = Arc::into_raw(packet_io) as *mut OVERLAPPED;
        unsafe {
            let result = io_function(overlapped_ptr);
            let error = GetLastError();
            // the io function fails
            if result == FALSE && error != ERROR_IO_PENDING {
                let restored_io_packet = Arc::from_raw(overlapped_ptr as *mut IoOverlappedPacket);
                let _ = self.parent_io.forget_io_packet(restored_io_packet);
                return Err(GetLastError());
            }

            // if it is async wait for it
            if result == FALSE
                // && error == ERROR_IO_PENDING (just to remember)
                && GetOverlappedResult(self.file_handle, overlapped_ptr, &raw mut bytes, TRUE)
                    == FALSE
            {
                return Err(GetLastError());
            }
        }

        Ok(())
    }

    fn async_iocp_operation(
        &self,
        position: u64,
        completion: Completion,
        kind: IoKind,

        io_function: impl Fn(*mut OVERLAPPED) -> BOOL,
    ) -> Result<Completion> {
        let packet_io = self.parent_io.recycle_or_create_io_packet_from_completion(
            completion.clone(),
            position,
            kind,
        );

        let overlapped_ptr = Arc::into_raw(packet_io.clone()) as *mut OVERLAPPED;

        if !self
            .parent_io
            .map_completion_to_io_packet(self.file_handle, packet_io)
        {
            return Err(LimboError::InternalError(
                "Cannot map the completion to I/O Packet".into(),
            ));
        }

        unsafe {
            if io_function(overlapped_ptr) == FALSE && GetLastError() != ERROR_IO_PENDING {
                let io_packet = Arc::from_raw(overlapped_ptr as *mut IoOverlappedPacket);
                let _ = self.parent_io.forget_io_packet(io_packet);
                return Err(get_generic_limboerror_from_last_os_err());
            }
        }
        Ok(completion)
    }
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}
crate::assert::assert_send_sync!(WindowsFile);

impl File for WindowsFile {
    #[instrument(err, skip_all, level = Level::TRACE)]
    fn lock_file(&self, exclusive_access: bool) -> Result<()> {
        trace!(
            "locking file {:08X} [ exclusive: {exclusive_access} ]..",
            self.file_handle.addr()
        );

        let locking_flags = if exclusive_access {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };

        self.sync_iocp_operation(IoKind::Lock, |overlapped| unsafe {
            LockFileEx(
                self.file_handle,
                locking_flags,
                0,
                u32::MAX,
                u32::MAX,
                overlapped,
            )
        })
        .map_err(|err| {
            let error = io::Error::from_raw_os_error(err as i32);
            LimboError::LockingError(error.to_string())
        })
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        trace!("Unlocking file {:08X}", self.file_handle.addr());
        self.sync_iocp_operation(IoKind::Unlock, |overlapped| unsafe {
            UnlockFileEx(self.file_handle, 0, u32::MAX, u32::MAX, overlapped)
        })
        .map_err(|err| {
            let error = io::Error::from_raw_os_error(err as i32);
            LimboError::LockingError(error.to_string())
        })
    }

    #[instrument(skip(self, completion), level = Level::TRACE)]
    fn pread(&self, position: u64, completion: Completion) -> Result<Completion> {
        trace!(
            "pread for handle {:08X} with completion {}",
            self.file_handle.addr(),
            get_unique_key_from_completion(&completion).addr()
        );

        let read_completion = completion.as_read();
        let read_buffer = read_completion.buf();
        let read_buffer_ptr = read_buffer.as_mut_ptr();
        let read_buffer_len = read_buffer
            .len()
            .try_into()
            .map_err(get_limboerror_from_std_error)?;

        self.async_iocp_operation(position, completion, IoKind::Read, |overlapped| unsafe {
            ReadFile(
                self.file_handle,
                read_buffer_ptr,
                read_buffer_len,
                ptr::null_mut(),
                overlapped,
            )
        })
    }

    #[instrument(skip(self, completion, buffer), level = Level::TRACE)]
    fn pwrite(
        &self,
        position: u64,
        buffer: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        trace!(
            "pwrite for handle {:08X} with completion {}",
            self.file_handle.addr(),
            get_unique_key_from_completion(&completion).addr()
        );

        let buffer_ptr = buffer.as_mut_ptr();
        let buffer_len = buffer
            .len()
            .try_into()
            .map_err(get_limboerror_from_std_error)?;

        self.async_iocp_operation(
            position,
            completion,
            IoKind::Write(buffer),
            |overlapped| unsafe {
                WriteFile(
                    self.file_handle,
                    buffer_ptr,
                    buffer_len,
                    ptr::null_mut(),
                    overlapped,
                )
            },
        )
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, completion: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        trace!(
            "sync for handle {:08X} with completion {}",
            self.file_handle.addr(),
            get_unique_key_from_completion(&completion).addr()
        );

        unsafe {
            if FlushFileBuffers(self.file_handle) == FALSE {
                return Err(get_generic_limboerror_from_last_os_err());
            }
        };
        completion.complete(0);
        Ok(completion)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn truncate(&self, length: u64, completion: Completion) -> Result<Completion> {
        trace!(
            "truncate for handle {:08X} with completion {}",
            self.file_handle.addr(),
            get_unique_key_from_completion(&completion).addr()
        );

        unsafe {
            let file_info = FILE_END_OF_FILE_INFO {
                EndOfFile: length.try_into().map_err(get_limboerror_from_std_error)?,
            };

            if SetFileInformationByHandle(
                self.file_handle,
                FileEndOfFileInfo,
                (&raw const file_info).cast(),
                size_of_val(&file_info)
                    .try_into()
                    .map_err(get_limboerror_from_std_error)?, // CONVERSION SAFETY:
                                                              // the struct size will not exceed u32
            ) == FALSE
            {
                return Err(get_generic_limboerror_from_last_os_err());
            }
        }
        completion.complete(0);
        Ok(completion)
    }

    fn size(&self) -> Result<u64> {
        let mut filesize = 0;

        unsafe {
            if GetFileSizeEx(self.file_handle, &raw mut filesize) == FALSE {
                return Err(get_generic_limboerror_from_last_os_err());
            }
        }

        trace!("size for handle {:08X} {filesize}", self.file_handle.addr());

        filesize.try_into().map_err(get_limboerror_from_std_error)
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        trace!("dropping handle {:08X}", self.file_handle.addr());

        let _ = self.unlock_file();

        unsafe {
            CancelIoEx(self.file_handle, ptr::null());
            CloseHandle(self.file_handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        io::{
            common,
            win_iocp::{get_generic_limboerror_from_os_err, get_limboerror_from_os_err},
            TempFile,
        },
        Buffer, Completion, IO,
    };

    use super::WindowsIOCP;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(WindowsIOCP::new);
    }

    #[test]
    fn test_file_read_write() {
        let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
        let file = TempFile::new(&iocp).unwrap();

        const WRITE: &[u8] = b"ABCD";

        let mut vec = vec![];
        for n in 0..150 {
            let comp = Completion::new_write(|res| {
                assert_eq!(res, Ok(4));
            });
            let buffer = Arc::new(Buffer::new_temporary(WRITE.len()));

            buffer.as_mut_slice().copy_from_slice(WRITE);

            let ret = file.pwrite(n * WRITE.len() as u64, buffer, comp).unwrap();
            vec.push(ret);
        }
        vec.into_iter().for_each(|c| {
            iocp.wait_for_completion(c.clone()).unwrap();
            if c.failed() {
                panic!();
            }
        });
        let mut vec = vec![];

        for n in 0..150 {
            let buffer = Arc::new(Buffer::new_temporary(WRITE.len()));

            let comp = Completion::new_read(buffer, |res| {
                assert_eq!(res.clone().unwrap().1, 4);
                res.err()
            });

            let ret = file.pread(n * WRITE.len() as u64, comp).unwrap();
            vec.push(ret);
        }
        vec.iter().for_each(|c| {
            iocp.wait_for_completion(c.clone()).unwrap();
        });
        vec.iter().any(|c| c.failed()).then(|| panic!());

        assert_eq!(file.size().unwrap(), 150 * WRITE.iter().len() as u64);
    }

    #[test]
    fn test_error_functions() {
        assert_eq!(
            get_limboerror_from_os_err(5).to_string(),
            String::from("I/O error: permission denied")
        );
        assert_eq!(
            get_limboerror_from_os_err(u32::MAX).to_string(),
            String::from("Internal error: Unknown error [4294967295]")
        );

        assert_eq!(
            get_limboerror_from_os_err(15818).to_string(),
            String::from("I/O error: uncategorized error")
        );

        assert_eq!(
            get_generic_limboerror_from_os_err(5).to_string(),
            String::from("Internal error: Windows Error: [5]Access is denied.\r\n")
        );
    }

    #[test]
    fn test_proper_drop() {
        let write = b"Abcd";
        let iocp: Arc<dyn IO> = Arc::new(WindowsIOCP::new().unwrap());
        let file = TempFile::new(&iocp).unwrap();
        let comp = Completion::new_write(|_| {});
        let buffer = Arc::new(Buffer::new_temporary(write.len()));

        buffer.as_mut_slice().copy_from_slice(write);

        let _ = file.pwrite(0, buffer, comp).unwrap();
        drop(iocp);
        drop(file);
    }
}
