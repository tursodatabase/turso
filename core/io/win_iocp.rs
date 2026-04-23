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

use crate::error::io_error;
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::{Clock, Completion, CompletionError, File, LimboError, OpenFlags, Result, IO};

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
use crate::io::{SharedWalLockKind, SharedWalMappedRegion};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_HANDLE_EOF, ERROR_IO_PENDING,
    ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED,
    ERROR_OPERATION_ABORTED, FALSE, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
    TRUE, WAIT_TIMEOUT,
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
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, FILE_MAP_READ, FILE_MAP_WRITE,
    PAGE_READWRITE,
};
use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};
use windows_sys::Win32::System::Threading::CreateEventW;

// Constants

const CACHING_CAPACITY: usize = 128;
//TODO: enable this or remove when direct IO stabilized
const ENABLE_DIRECT_IO: bool = false;
const ENABLE_LOCK_ON_OPEN: bool = true;

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
    Unknown,
}

#[repr(C)]
struct IoOverlappedPacket {
    overlapped: OVERLAPPED,
    completion: Option<Completion>,
    kind: IoKind,
}

unsafe impl Send for IoOverlappedPacket {}
unsafe impl Sync for IoOverlappedPacket {}

impl std::fmt::Debug for IoOverlappedPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IoOverlappedPacket {{")?;
        writeln!(f, "-- completion: {:?} ", self.completion)?;
        writeln!(f, "-- kind: {:?} ", self.kind)?;
        unsafe {
            writeln!(
                f,
                "-- offset: {:?} ",
                self.overlapped.Anonymous.Anonymous.Offset
            )?;
            writeln!(
                f,
                "-- offsetHigh: {:?} ",
                self.overlapped.Anonymous.Anonymous.OffsetHigh
            )?;
        }
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
            return LimboError::InternalError(format!("Windows Error: [{err}]"));
        }

        let Ok(size) = size.try_into() else {
            LocalFree(buffer.cast());
            return LimboError::InternalError(format!("Windows Error: [{err}]"));
        };

        let buffer_slice = std::slice::from_raw_parts(buffer, size);
        let string = OsString::from_wide(buffer_slice);

        LocalFree(buffer.cast());

        let Ok(string) = string.into_string() else {
            return LimboError::InternalError(format!("Windows Error: [{err}]"));
        };

        LimboError::InternalError(format!("Windows Error: [{err}]{string}"))
    }
}

#[inline]
fn get_limboerror_from_std_error(err: impl Error) -> LimboError {
    LimboError::InternalError(err.to_string())
}

// Windows IOCP

pub struct WindowsIOCP {
    instance: Arc<InnerWindowsIOCP>,
}

struct WindowsSharedWalMapping {
    mapping_handle: HANDLE,
    view_ptr: NonNull<u8>,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for WindowsSharedWalMapping {}
unsafe impl Sync for WindowsSharedWalMapping {}

impl SharedWalMappedRegion for WindowsSharedWalMapping {
    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for WindowsSharedWalMapping {
    fn drop(&mut self) {
        unsafe {
            if UnmapViewOfFile(windows_sys::Win32::System::Memory::MEMORY_MAPPED_VIEW_ADDRESS {
                Value: self.view_ptr.as_ptr().cast(),
            }) == FALSE
            {
                tracing::error!(
                    "UnmapViewOfFile failed for shared WAL coordination region: {}",
                    io::Error::last_os_error()
                );
            }
            if CloseHandle(self.mapping_handle) == FALSE {
                tracing::error!(
                    "CloseHandle failed for shared WAL mapping: {}",
                    io::Error::last_os_error()
                );
            }
        }
    }
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
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

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

        let flags_and_attributes = if ENABLE_DIRECT_IO && direct_access {
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
                return Err(io_error(io::Error::last_os_error(), "open"));
            };

            let windows_file = Arc::new(WindowsFile {
                file_handle,
                parent_io: self.instance.clone(),
            });

            // Bind file to IOCP
            let result = CreateIoCompletionPort(file_handle, self.instance.iocp_queue_handle, 0, 0);

            if result.is_null() {
                return Err(io_error(
                    io::Error::last_os_error(),
                    "associate file with iocp",
                ));
            };

            if ENABLE_LOCK_ON_OPEN
                && !open_flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
                && std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            {
                windows_file.lock_file(true)?;
            }

            Ok(windows_file)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn remove_file(&self, file_path: &str) -> Result<()> {
        trace!("remove_file(path = {})", file_path);
        std::fs::remove_file(file_path).map_err(|e| io_error(e, "remove-file"))
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

    fn recycle_or_create_io_packet(&self) -> IoPacket {
        self.free_io_packets.lock().pop_front().unwrap_or_else(|| {
            Arc::new(IoOverlappedPacket {
                overlapped: OVERLAPPED::default(),
                completion: None,
                kind: IoKind::Unknown,
            })
        })
    }

    fn build_io_packet(
        &self,
        completion: Option<Completion>,
        position: u64,
        kind: IoKind,
    ) -> IoPacket {
        trace!("new salvaged overlapped packet. ");

        let mut packet = self.recycle_or_create_io_packet();

        assert!(
            packet.completion.is_none(),
            "New packet should has no completion"
        );

        let content =
            Arc::get_mut(&mut packet).expect("This IO Packet should not have references elsewhere");

        let low_part = position as u32;
        let high_part = (position >> 32) as u32;

        *content = IoOverlappedPacket {
            completion,
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

        if let Some(completion) = io_packet.completion.as_ref().cloned() {
            // Prefer the tracked packet when it still exists: the raw IOCP alias is
            // an extra Arc clone and cannot be recycled in place until we drop it.
            if let Some(context) = self.pop_io_context_from_completion(&completion) {
                drop(io_packet);
                io_packet = context.io_packet;
            }
        }

        let internals = Arc::get_mut(&mut io_packet)?;
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
                let error = match error_code {
                    ERROR_HANDLE_EOF => {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "Reading past the EOF point")
                    }
                    code => io::Error::from_raw_os_error(
                        code.try_into().map_err(|_| GetIOCPPacketError::InvalidIO)?,
                    ),
                };

                trace!(
                    "completion {} errored {error}",
                    get_unique_key_from_completion(&completion).addr()
                );

                completion.error(CompletionError::IOError(
                    error.kind(),
                    "io-error-completion",
                ));
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
                    return Err(io_error(err, "process-io-packet-sys-error"));
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
    fn overlapped_for_position(position: u64) -> OVERLAPPED {
        unsafe {
            let mut overlapped: OVERLAPPED = mem::zeroed();
            overlapped.Anonymous = OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: position as u32,
                    OffsetHigh: (position >> 32) as u32,
                },
            };
            overlapped
        }
    }

    fn suppressed_iocp_overlapped_for_position(position: u64) -> Result<(OVERLAPPED, HANDLE)> {
        let event = unsafe { CreateEventW(ptr::null(), TRUE, FALSE, ptr::null()) };
        if event.is_null() {
            return Err(get_generic_limboerror_from_last_os_err());
        }

        let mut overlapped = Self::overlapped_for_position(position);
        overlapped.hEvent = ((event as usize) | 1) as HANDLE;
        Ok((overlapped, event))
    }

    fn lock_range(
        &self,
        offset: u64,
        len: u64,
        exclusive: bool,
        fail_immediately: bool,
    ) -> Result<bool> {
        let (mut overlapped, event) = Self::suppressed_iocp_overlapped_for_position(offset)?;
        let flags = (if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        }) | if fail_immediately {
            LOCKFILE_FAIL_IMMEDIATELY
        } else {
            0
        };
        let low = len as u32;
        let high = (len >> 32) as u32;
        let result = (|| {
            unsafe {
                if LockFileEx(self.file_handle, flags, 0, low, high, &raw mut overlapped) == TRUE {
                    return Ok(true);
                }
            }

            let initial_error = unsafe { GetLastError() };
            if initial_error == ERROR_LOCK_VIOLATION {
                return Ok(false);
            }
            if initial_error != ERROR_IO_PENDING {
                return Err(LimboError::LockingError(
                    io::Error::from_raw_os_error(initial_error as i32).to_string(),
                ));
            }

            let mut bytes = 0;
            unsafe {
                if GetOverlappedResult(
                    self.file_handle,
                    &raw mut overlapped,
                    &raw mut bytes,
                    TRUE,
                ) == TRUE
                {
                    return Ok(true);
                }
            }

            let completion_error = unsafe { GetLastError() };
            if completion_error == ERROR_LOCK_VIOLATION {
                return Ok(false);
            }
            Err(LimboError::LockingError(
                io::Error::from_raw_os_error(completion_error as i32).to_string(),
            ))
        })();

        unsafe {
            CloseHandle(event);
        }

        result
    }

    fn unlock_range(&self, offset: u64, len: u64) -> Result<()> {
        let (mut overlapped, event) = Self::suppressed_iocp_overlapped_for_position(offset)?;
        let low = len as u32;
        let high = (len >> 32) as u32;
        let result = (|| {
            unsafe {
                if UnlockFileEx(self.file_handle, 0, low, high, &raw mut overlapped) == TRUE {
                    return Ok(());
                }
            }

            let initial_error = unsafe { GetLastError() };
            if initial_error == ERROR_NOT_LOCKED {
                return Ok(());
            }
            if initial_error != ERROR_IO_PENDING {
                return Err(LimboError::LockingError(
                    io::Error::from_raw_os_error(initial_error as i32).to_string(),
                ));
            }

            let mut bytes = 0;
            unsafe {
                if GetOverlappedResult(
                    self.file_handle,
                    &raw mut overlapped,
                    &raw mut bytes,
                    TRUE,
                ) == TRUE
                {
                    return Ok(());
                }
            }

            let completion_error = unsafe { GetLastError() };
            if completion_error == ERROR_NOT_LOCKED {
                return Ok(());
            }
            Err(LimboError::LockingError(
                io::Error::from_raw_os_error(completion_error as i32).to_string(),
            ))
        })();

        unsafe {
            CloseHandle(event);
        }

        result
    }

    fn async_iocp_operation(
        &self,
        position: u64,
        completion: Completion,
        kind: IoKind,

        io_function: impl Fn(*mut OVERLAPPED) -> BOOL,
    ) -> Result<Completion> {
        let packet_io = self
            .parent_io
            .build_io_packet(Some(completion.clone()), position, kind);

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
            let result = io_function(overlapped_ptr);
            let error = GetLastError();
            if result == FALSE && error != ERROR_IO_PENDING {
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

        match self.lock_range(0, u64::MAX, exclusive_access, true) {
            Ok(true) => Ok(()),
            Ok(false) => Err(LimboError::LockingError(
                "The process cannot access the file because another process has locked a portion of the file."
                    .into(),
            )),
            Err(err) => Err(err),
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn unlock_file(&self) -> Result<()> {
        trace!("Unlocking file {:08X}", self.file_handle.addr());
        self.unlock_range(0, u64::MAX)
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

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<()> {
        match self.lock_range(offset, 1, exclusive, false) {
            Ok(true) => Ok(()),
            Ok(false) => Err(LimboError::LockingError(
                "Failed locking shared WAL coordination file. File is locked by another process"
                    .into(),
            )),
            Err(err) => Err(err),
        }
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.lock_range(offset, 1, exclusive, true)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, _kind: SharedWalLockKind) -> Result<()> {
        self.unlock_range(offset, 1)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        unsafe {
            let file_info = FILE_END_OF_FILE_INFO {
                EndOfFile: len.try_into().map_err(get_limboerror_from_std_error)?,
            };

            if SetFileInformationByHandle(
                self.file_handle,
                FileEndOfFileInfo,
                (&raw const file_info).cast(),
                size_of_val(&file_info)
                    .try_into()
                    .map_err(get_limboerror_from_std_error)?,
            ) == FALSE
            {
                return Err(io_error(
                    io::Error::last_os_error(),
                    "resize shared WAL coordination file",
                ));
            }
        }

        Ok(())
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        if len == 0 {
            return Err(LimboError::InternalError(
                "cannot map shared WAL coordination region with zero length".into(),
            ));
        }

        let mut system_info = unsafe { mem::zeroed::<SYSTEM_INFO>() };
        unsafe { GetSystemInfo(&raw mut system_info) };
        let granularity = u64::from(system_info.dwAllocationGranularity);
        if granularity == 0 {
            return Err(LimboError::LockingError(
                "failed to determine shared WAL mapping allocation granularity".into(),
            ));
        }

        let aligned_offset = offset / granularity * granularity;
        let prefix_len = (offset - aligned_offset) as usize;
        let view_len = prefix_len
            .checked_add(len)
            .ok_or_else(|| LimboError::InternalError("shared WAL map length overflow".into()))?;

        let mapping_handle =
            unsafe { CreateFileMappingW(self.file_handle, ptr::null(), PAGE_READWRITE, 0, 0, ptr::null()) };
        if mapping_handle.is_null() {
            return Err(io_error(
                io::Error::last_os_error(),
                "create shared WAL file mapping",
            ));
        }

        let offset_high = (aligned_offset >> 32) as u32;
        let offset_low = aligned_offset as u32;
        let mapped_ptr = unsafe {
            MapViewOfFile(
                mapping_handle,
                FILE_MAP_READ | FILE_MAP_WRITE,
                offset_high,
                offset_low,
                view_len,
            )
        };
        if mapped_ptr.Value.is_null() {
            unsafe {
                CloseHandle(mapping_handle);
            }
            return Err(io_error(io::Error::last_os_error(), "map shared WAL coordination file"));
        }

        let view_ptr = NonNull::new(mapped_ptr.Value.cast::<u8>())
            .expect("MapViewOfFile returned null for shared WAL map");
        let ptr = NonNull::new(unsafe { view_ptr.as_ptr().add(prefix_len) })
            .expect("mapped base plus prefix_len returned null");

        Ok(Box::new(WindowsSharedWalMapping {
            mapping_handle,
            view_ptr,
            ptr,
            len,
        }))
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        trace!("dropping handle {:08X}", self.file_handle.addr());

        if ENABLE_LOCK_ON_OPEN {
            let _ = self.unlock_file();
        }

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
        io::{win_iocp::get_generic_limboerror_from_os_err, TempFile},
        Buffer, Completion, IO,
    };

    use super::WindowsIOCP;

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

        drop(file.pwrite(0, buffer, comp).unwrap());
        drop(iocp);
        drop(file);
    }
}
