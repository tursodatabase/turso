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
    use windows_sys::Win32::System::IO::OVERLAPPED;

    pub fn try_lock(file: &std::fs::File, exclusive: bool) -> std::io::Result<()> {
        let handle = file.as_raw_handle() as isize;
        let flags = LOCKFILE_FAIL_IMMEDIATELY | if exclusive { LOCKFILE_EXCLUSIVE_LOCK } else { 0 };
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let result =
            unsafe { LockFileEx(handle, flags, 0, u32::MAX, u32::MAX, &mut overlapped) };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn unlock(file: &std::fs::File) -> std::io::Result<()> {
        let handle = file.as_raw_handle() as isize;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let result = unsafe { UnlockFileEx(handle, 0, u32::MAX, u32::MAX, &mut overlapped) };
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
