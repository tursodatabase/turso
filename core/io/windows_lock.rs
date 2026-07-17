use crate::sync::Mutex;
use crate::{LimboError, Result};
use std::collections::HashMap;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED, FALSE, GENERIC_READ,
    GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, TRUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, LockFileEx, UnlockFileEx, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

/// Keep the open-time database lock outside the useful range of every Turso
/// file. Locking the data range itself makes Windows reject I/O through sibling
/// handles in this process.
const PROCESS_LOCK_OFFSET: u64 = 0x4000_0000_0000_0000;

fn canonical_path(path: &str) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path))
}

fn overlapped_at(offset: u64) -> OVERLAPPED {
    OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Anonymous: OVERLAPPED_0 {
            Anonymous: OVERLAPPED_0_0 {
                Offset: offset as u32,
                OffsetHigh: (offset >> 32) as u32,
            },
        },
        hEvent: std::ptr::null_mut(),
    }
}

fn open_lock_handle(path: &Path) -> Result<HANDLE> {
    let path: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(LimboError::LockingError(format!(
            "Failed opening lock handle: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(handle)
}

fn lock_range(handle: HANDLE, offset: u64, exclusive: bool, fail_immediately: bool) -> Result<bool> {
    let flags = (if exclusive {
        LOCKFILE_EXCLUSIVE_LOCK
    } else {
        0
    }) | if fail_immediately {
        LOCKFILE_FAIL_IMMEDIATELY
    } else {
        0
    };
    let mut overlapped = overlapped_at(offset);
    let result = unsafe { LockFileEx(handle, flags, 0, 1, 0, &mut overlapped) };
    if result == TRUE {
        return Ok(true);
    }
    if unsafe { GetLastError() } == ERROR_LOCK_VIOLATION {
        return Ok(false);
    }
    Err(LimboError::LockingError(format!(
        "Failed acquiring Windows file lock: {}",
        std::io::Error::last_os_error()
    )))
}

fn unlock_range(handle: HANDLE, offset: u64) -> Result<()> {
    let mut overlapped = overlapped_at(offset);
    let result = unsafe { UnlockFileEx(handle, 0, 1, 0, &mut overlapped) };
    if result == TRUE || unsafe { GetLastError() } == ERROR_NOT_LOCKED {
        return Ok(());
    }
    Err(LimboError::LockingError(format!(
        "Failed releasing Windows file lock: {}",
        std::io::Error::last_os_error()
    )))
}

struct ProcessFileLockEntry {
    handle: HANDLE,
    refcount: usize,
}

unsafe impl Send for ProcessFileLockEntry {}

type ProcessFileLockRegistry = Mutex<HashMap<PathBuf, ProcessFileLockEntry>>;

fn process_file_lock_registry() -> &'static ProcessFileLockRegistry {
    static REGISTRY: OnceLock<ProcessFileLockRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A reference to this process's single open-time lock for one database file.
pub(crate) struct ProcessFileLockGuard {
    key: PathBuf,
}

impl Drop for ProcessFileLockGuard {
    fn drop(&mut self) {
        let mut registry = process_file_lock_registry().lock();
        let Some(entry) = registry.get_mut(&self.key) else {
            tracing::error!(
                path = %self.key.display(),
                "Windows process lock registry entry missing during release"
            );
            return;
        };
        if entry.refcount == 0 {
            tracing::error!(
                path = %self.key.display(),
                "Windows process lock registry reference count underflow"
            );
            return;
        }
        entry.refcount -= 1;
        if entry.refcount != 0 {
            return;
        }

        let handle = registry
            .remove(&self.key)
            .expect("process lock entry disappeared while releasing")
            .handle;
        if let Err(err) = unlock_range(handle, PROCESS_LOCK_OFFSET) {
            tracing::error!(path = %self.key.display(), ?err, "failed releasing Windows process lock");
        }
        if unsafe { CloseHandle(handle) } == FALSE {
            tracing::error!(
                path = %self.key.display(),
                error = %std::io::Error::last_os_error(),
                "failed closing Windows process lock handle"
            );
        }
    }
}

/// Acquire the per-process open-time lock for `path`.
///
/// Windows byte-range locks are per-handle, so all same-process opens share a
/// single dedicated handle. The lock lives beyond the data range and therefore
/// does not interfere with normal database I/O.
pub(crate) fn acquire_process_file_lock(path: &str) -> Result<ProcessFileLockGuard> {
    let key = canonical_path(path);
    let mut registry = process_file_lock_registry().lock();
    if let Some(entry) = registry.get_mut(&key) {
        entry.refcount += 1;
        return Ok(ProcessFileLockGuard { key });
    }

    let handle = open_lock_handle(&key)?;
    match lock_range(handle, PROCESS_LOCK_OFFSET, true, true) {
        Err(err) => {
            unsafe {
                CloseHandle(handle);
            }
            Err(err)
        }
        Ok(true) => {
            registry.insert(key.clone(), ProcessFileLockEntry { handle, refcount: 1 });
            Ok(ProcessFileLockGuard { key })
        }
        Ok(false) => {
            unsafe {
                CloseHandle(handle);
            }
            Err(LimboError::LockingError(
                "Failed locking file. File is locked by another process".into(),
            ))
        }
    }
}

#[derive(Default)]
pub(crate) struct SharedWalLockState {
    held: HashMap<u64, HeldSharedWalLock>,
}

#[derive(Default)]
struct HeldSharedWalLock {
    shared: usize,
    exclusive: usize,
}

enum SharedWalGlobalLock {
    Shared { holders: usize },
    Exclusive,
}

struct SharedWalFileLockEntry {
    handle: HANDLE,
    locks: HashMap<u64, SharedWalGlobalLock>,
}

unsafe impl Send for SharedWalFileLockEntry {}

type SharedWalLockRegistry = Mutex<HashMap<PathBuf, SharedWalFileLockEntry>>;

fn shared_wal_lock_registry() -> &'static SharedWalLockRegistry {
    static REGISTRY: OnceLock<SharedWalLockRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn close_shared_wal_entry(
    registry: &mut HashMap<PathBuf, SharedWalFileLockEntry>,
    key: &Path,
) {
    let Some(entry) = registry.remove(key) else {
        return;
    };
    debug_assert!(entry.locks.is_empty());
    if unsafe { CloseHandle(entry.handle) } == FALSE {
        tracing::error!(
            path = %key.display(),
            error = %std::io::Error::last_os_error(),
            "failed closing shared WAL lock handle"
        );
    }
}

fn lock_shared_wal_byte(
    path: &Path,
    state: &Mutex<SharedWalLockState>,
    offset: u64,
    exclusive: bool,
    fail_immediately: bool,
) -> Result<bool> {
    let key = canonical_path(path.to_str().ok_or_else(|| {
        LimboError::InternalError("shared WAL coordination path is not valid UTF-8".into())
    })?);
    let mut local = state.lock();
    let mut registry = shared_wal_lock_registry().lock();
    let entry = match registry.entry(key.clone()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let handle = open_lock_handle(&key)?;
            entry.insert(SharedWalFileLockEntry {
                handle,
                locks: HashMap::new(),
            })
        }
    };

    match entry.locks.get_mut(&offset) {
        Some(SharedWalGlobalLock::Shared { holders }) if !exclusive => {
            *holders += 1;
            local.held.entry(offset).or_default().shared += 1;
            Ok(true)
        }
        Some(_) => Ok(false),
        None => match lock_range(entry.handle, offset, exclusive, fail_immediately) {
            Err(err) => {
                if entry.locks.is_empty() {
                    close_shared_wal_entry(&mut registry, &key);
                }
                Err(err)
            }
            Ok(true) => {
                entry.locks.insert(
                    offset,
                    if exclusive {
                        SharedWalGlobalLock::Exclusive
                    } else {
                        SharedWalGlobalLock::Shared { holders: 1 }
                    },
                );
                let local_lock = local.held.entry(offset).or_default();
                if exclusive {
                    local_lock.exclusive += 1;
                } else {
                    local_lock.shared += 1;
                }
                Ok(true)
            }
            Ok(false) => {
                if entry.locks.is_empty() {
                    close_shared_wal_entry(&mut registry, &key);
                }
                Ok(false)
            }
        },
    }
}

fn release_shared_wal_lock(
    path: &Path,
    offset: u64,
    shared: usize,
    exclusive: usize,
) -> Result<()> {
    debug_assert!(shared == 0 || exclusive == 0);
    let key = canonical_path(path.to_str().ok_or_else(|| {
        LimboError::InternalError("shared WAL coordination path is not valid UTF-8".into())
    })?);
    let mut registry = shared_wal_lock_registry().lock();
    let entry = registry.get_mut(&key).ok_or_else(|| {
        LimboError::LockingError("shared WAL lock registry entry is missing".into())
    })?;

    let remove_lock = match entry.locks.get_mut(&offset) {
        Some(SharedWalGlobalLock::Shared { holders }) if shared != 0 => {
            if *holders < shared {
                return Err(LimboError::LockingError(
                    "shared WAL shared lock reference count underflow".into(),
                ));
            }
            *holders -= shared;
            *holders == 0
        }
        Some(SharedWalGlobalLock::Exclusive) if exclusive != 0 => true,
        _ => {
            return Err(LimboError::LockingError(
                "shared WAL lock released with a mismatched mode".into(),
            ))
        }
    };

    if remove_lock {
        if let Err(err) = unlock_range(entry.handle, offset) {
            if let Some(SharedWalGlobalLock::Shared { holders }) = entry.locks.get_mut(&offset) {
                *holders += shared;
            }
            return Err(err);
        }
        entry.locks.remove(&offset);
    }
    if entry.locks.is_empty() {
        close_shared_wal_entry(&mut registry, &key);
    }
    Ok(())
}

pub(crate) fn shared_wal_lock_byte(
    path: &Path,
    state: &Mutex<SharedWalLockState>,
    offset: u64,
    exclusive: bool,
) -> Result<()> {
    if lock_shared_wal_byte(path, state, offset, exclusive, false)? {
        Ok(())
    } else {
        Err(LimboError::LockingError(
            "Failed locking shared WAL coordination file. File is locked by another process".into(),
        ))
    }
}

pub(crate) fn shared_wal_try_lock_byte(
    path: &Path,
    state: &Mutex<SharedWalLockState>,
    offset: u64,
    exclusive: bool,
) -> Result<bool> {
    lock_shared_wal_byte(path, state, offset, exclusive, true)
}

pub(crate) fn shared_wal_unlock_byte(
    path: &Path,
    state: &Mutex<SharedWalLockState>,
    offset: u64,
) -> Result<()> {
    let (shared, exclusive) = {
        let local = state.lock();
        let held = local.held.get(&offset).ok_or_else(|| {
            LimboError::LockingError("shared WAL lock released without an acquisition".into())
        })?;
        if held.exclusive != 0 {
            (0, 1)
        } else if held.shared != 0 {
            (1, 0)
        } else {
            unreachable!("empty shared WAL lock state was retained");
        }
    };

    release_shared_wal_lock(path, offset, shared, exclusive)?;

    let mut local = state.lock();
    let held = local
        .held
        .get_mut(&offset)
        .expect("shared WAL local lock disappeared while releasing");
    if exclusive != 0 {
        held.exclusive -= 1;
    } else {
        held.shared -= 1;
    }
    if held.shared == 0 && held.exclusive == 0 {
        local.held.remove(&offset);
    }
    Ok(())
}

/// Probe whether no other process holds `offset` without changing this
/// process's aggregate shared-lock state.
pub(crate) fn shared_wal_probe_exclusive_byte(path: &Path, offset: u64) -> Result<bool> {
    let key = canonical_path(path.to_str().ok_or_else(|| {
        LimboError::InternalError("shared WAL coordination path is not valid UTF-8".into())
    })?);
    let mut registry = shared_wal_lock_registry().lock();
    let entry = match registry.entry(key.clone()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let handle = open_lock_handle(&key)?;
            entry.insert(SharedWalFileLockEntry {
                handle,
                locks: HashMap::new(),
            })
        }
    };

    let had_shared_lock = matches!(
        entry.locks.get(&offset),
        Some(SharedWalGlobalLock::Shared { .. })
    );
    if matches!(
        entry.locks.get(&offset),
        Some(SharedWalGlobalLock::Exclusive)
    ) {
        return Ok(false);
    }

    // Use a disposable handle for the exclusive probe. If releasing that
    // exclusive lock fails, closing the handle still guarantees it cannot
    // survive past the probe and block restoration of the shared lock.
    let probe_handle = open_lock_handle(&key)?;
    if had_shared_lock {
        if let Err(err) = unlock_range(entry.handle, offset) {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            return Err(err);
        }
    }

    let probe = match lock_range(probe_handle, offset, true, true) {
        Ok(true) => {
            let result = unlock_range(probe_handle, offset).map(|()| true);
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            result
        }
        Ok(false) => {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            Ok(false)
        }
        Err(err) => {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            Err(err)
        }
    };

    if had_shared_lock {
        match lock_range(entry.handle, offset, false, false) {
            Ok(true) => {}
            Ok(false) => {
                panic!("shared WAL lifetime lock restoration was blocked after exclusive probe")
            }
            Err(err) => {
                panic!("shared WAL lifetime lock restoration failed after exclusive probe: {err}")
            }
        }
    }

    if entry.locks.is_empty() {
        close_shared_wal_entry(&mut registry, &key);
    }
    probe
}

pub(crate) fn release_shared_wal_locks_on_drop(path: &Path, state: &Mutex<SharedWalLockState>) {
    let held = std::mem::take(&mut state.lock().held);
    for (offset, held) in held {
        if let Err(err) = release_shared_wal_lock(path, offset, held.shared, held.exclusive) {
            tracing::error!(
                path = %path.display(),
                offset,
                ?err,
                "failed releasing shared WAL lock while dropping Windows file"
            );
        }
    }
}
