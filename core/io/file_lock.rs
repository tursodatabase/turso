/// Cross-platform file locking.
///
/// - Unix: fcntl(F_SETLK) via rustix
/// - Windows: LockFileEx / UnlockFileEx via windows-sys
/// - Other (WASM, miri): no-op with warning

#[cfg(target_family = "unix")]
mod imp {
    use rustix::fd::AsFd;
    use rustix::fs::{self, FlockOperation};

    pub fn try_lock(file: &std::fs::File, exclusive: bool) -> std::io::Result<()> {
        fs::fcntl_lock(
            file.as_fd(),
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(std::io::Error::from)
    }

    pub fn unlock(file: &std::fs::File) -> std::io::Result<()> {
        fs::fcntl_lock(file.as_fd(), FlockOperation::NonBlockingUnlock)
            .map_err(std::io::Error::from)
    }

    pub fn is_contention_error(e: &std::io::Error) -> bool {
        e.kind() == std::io::ErrorKind::WouldBlock
    }
}

#[cfg(target_os = "windows")]
mod imp {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, UnlockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

    /// SQLite lock page offset. Locking here instead of the whole file ensures
    /// sqlite3 reports "database is locked" rather than "disk I/O error",
    /// because Windows byte-range locks are mandatory and a whole-file lock
    /// would block sqlite3 from reading data bytes.
    const PENDING_BYTE: u32 = 0x40000000;
    /// Size of the SQLite lock page (PENDING + RESERVED + SHARED_SIZE).
    const LOCK_PAGE_SIZE: u32 = 512;

    fn lock_page_overlapped() -> OVERLAPPED {
        OVERLAPPED {
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: PENDING_BYTE,
                    OffsetHigh: 0,
                },
            },
            ..unsafe { std::mem::zeroed() }
        }
    }

    pub fn try_lock(file: &std::fs::File, exclusive: bool) -> std::io::Result<()> {
        let handle = file.as_raw_handle();
        let flags = LOCKFILE_FAIL_IMMEDIATELY
            | if exclusive {
                LOCKFILE_EXCLUSIVE_LOCK
            } else {
                0
            };
        let mut overlapped = lock_page_overlapped();
        let result =
            unsafe { LockFileEx(handle, flags, 0, LOCK_PAGE_SIZE, 0, &mut overlapped) };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn unlock(file: &std::fs::File) -> std::io::Result<()> {
        let handle = file.as_raw_handle();
        let mut overlapped = lock_page_overlapped();
        let result =
            unsafe { UnlockFileEx(handle, 0, LOCK_PAGE_SIZE, 0, &mut overlapped) };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn is_contention_error(e: &std::io::Error) -> bool {
        use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
        e.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32)
    }
}

#[cfg(not(any(target_family = "unix", target_os = "windows")))]
mod imp {
    pub fn try_lock(_file: &std::fs::File, _exclusive: bool) -> std::io::Result<()> {
        tracing::warn!("file locking is not supported on this platform");
        Ok(())
    }

    pub fn unlock(_file: &std::fs::File) -> std::io::Result<()> {
        tracing::warn!("file unlocking is not supported on this platform");
        Ok(())
    }

    pub fn is_contention_error(_e: &std::io::Error) -> bool {
        false
    }
}

pub use imp::{is_contention_error, try_lock, unlock};
