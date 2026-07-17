use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};
use std::collections::HashMap;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
#[cfg(test)]
use std::{cell::RefCell, collections::VecDeque};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED, FALSE, GENERIC_READ,
    GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, TRUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, GetFinalPathNameByHandleW, LockFileEx, UnlockFileEx, FILE_ATTRIBUTE_NORMAL,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, LOCKFILE_EXCLUSIVE_LOCK,
    LOCKFILE_FAIL_IMMEDIATELY, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

/// Keep the open-time database lock outside the useful range of every Turso
/// file. Locking the data range itself makes Windows reject I/O through sibling
/// handles in this process.
const PROCESS_LOCK_OFFSET: u64 = 0x4000_0000_0000_0000;

/// Resolve a path once so registry identity is stable after a caller changes
/// its working directory or the file is later removed.
pub(crate) fn stable_lock_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(path))
                .unwrap_or_else(|_| path.to_path_buf())
        }
    })
}

/// Resolve an identity from the file already opened by IOCP so a concurrent
/// process-wide current-directory change cannot redirect later lock operations.
pub(crate) fn stable_lock_path_for_handle(handle: HANDLE, fallback: &Path) -> PathBuf {
    let mut buffer = vec![0_u16; 260];
    let mut length =
        unsafe { GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, 0) }
            as usize;
    if length == 0 {
        return stable_lock_path(fallback);
    }
    if length >= buffer.len() {
        buffer.resize(length + 1, 0);
        length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, 0)
        } as usize;
        if length == 0 || length >= buffer.len() {
            return stable_lock_path(fallback);
        }
    }
    PathBuf::from(std::ffi::OsString::from_wide(&buffer[..length]))
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
    #[cfg(test)]
    if take_injected_lock_failure(offset, exclusive, fail_immediately) {
        return Err(LimboError::LockingError(
            "injected Windows file lock acquisition failure".into(),
        ));
    }

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
    #[cfg(test)]
    if take_injected_unlock_failure(offset) {
        return Err(LimboError::LockingError(
            "injected Windows file lock release failure".into(),
        ));
    }

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

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum InjectedLockFailure {
    Lock {
        offset: u64,
        exclusive: bool,
        fail_immediately: bool,
    },
    Unlock {
        offset: u64,
    },
}

#[cfg(test)]
thread_local! {
    static LOCK_FAILURES: RefCell<VecDeque<InjectedLockFailure>> = const { RefCell::new(VecDeque::new()) };
}

#[cfg(test)]
fn inject_lock_failures(failures: impl IntoIterator<Item = InjectedLockFailure>) {
    LOCK_FAILURES.with(|injected| {
        let mut injected = injected.borrow_mut();
        injected.clear();
        injected.extend(failures);
    });
}

#[cfg(test)]
fn take_injected_lock_failure(offset: u64, exclusive: bool, fail_immediately: bool) -> bool {
    LOCK_FAILURES.with(|injected| {
        let mut injected = injected.borrow_mut();
        if matches!(
            injected.front(),
            Some(InjectedLockFailure::Lock {
                offset: expected_offset,
                exclusive: expected_exclusive,
                fail_immediately: expected_fail_immediately,
            }) if *expected_offset == offset
                && *expected_exclusive == exclusive
                && *expected_fail_immediately == fail_immediately
        ) {
            injected.pop_front();
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
fn take_injected_unlock_failure(offset: u64) -> bool {
    LOCK_FAILURES.with(|injected| {
        let mut injected = injected.borrow_mut();
        if matches!(
            injected.front(),
            Some(InjectedLockFailure::Unlock {
                offset: expected_offset,
            }) if *expected_offset == offset
        ) {
            injected.pop_front();
            true
        } else {
            false
        }
    })
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
    let key = stable_lock_path(Path::new(path));
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
    entry: Option<SharedWalRegistryEntry>,
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
    poisoned: bool,
}

unsafe impl Send for SharedWalFileLockEntry {}

#[derive(Clone)]
struct SharedWalRegistryEntry {
    key: PathBuf,
    entry: Arc<Mutex<SharedWalFileLockEntry>>,
}

type SharedWalLockRegistry = Mutex<HashMap<PathBuf, Arc<Mutex<SharedWalFileLockEntry>>>>;

fn shared_wal_lock_registry() -> &'static SharedWalLockRegistry {
    static REGISTRY: OnceLock<SharedWalLockRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn acquire_shared_wal_entry(
    path: &Path,
    state: &mut SharedWalLockState,
) -> Result<SharedWalRegistryEntry> {
    if let Some(entry) = &state.entry {
        return Ok(entry.clone());
    }

    let key = path.to_path_buf();
    let entry = {
        let mut registry = shared_wal_lock_registry().lock();
        match registry.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let handle = open_lock_handle(&key)?;
                let shared_entry = Arc::new(Mutex::new(SharedWalFileLockEntry {
                    handle,
                    locks: HashMap::new(),
                    poisoned: false,
                }));
                entry.insert(shared_entry.clone());
                shared_entry
            }
        }
    };
    let entry = SharedWalRegistryEntry { key, entry };
    state.entry = Some(entry.clone());
    Ok(entry)
}

fn release_shared_wal_entry(entry: SharedWalRegistryEntry) {
    let removed = {
        let mut registry = shared_wal_lock_registry().lock();
        let Some(current) = registry.get(&entry.key) else {
            tracing::error!(
                path = %entry.key.display(),
                "shared WAL lock registry entry missing during release"
            );
            return;
        };
        if !Arc::ptr_eq(current, &entry.entry) {
            tracing::error!(
                path = %entry.key.display(),
                "shared WAL lock registry entry changed during release"
            );
            return;
        }
        if Arc::strong_count(&entry.entry) != 2 {
            return;
        }
        registry.remove(&entry.key)
    };

    let Some(removed) = removed else {
        return;
    };
    let entry_state = removed.lock();
    if !entry_state.locks.is_empty() {
        tracing::error!(
            path = %entry.key.display(),
            "closing shared WAL lock handle with unreleased byte locks"
        );
    }
    if unsafe { CloseHandle(entry_state.handle) } == FALSE {
        tracing::error!(
            path = %entry.key.display(),
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
    let mut local = state.lock();
    let entry = acquire_shared_wal_entry(path, &mut local)?;
    let mut entry_state = entry.entry.lock();
    if entry_state.poisoned {
        return Err(LimboError::LockingError(
            "shared WAL lock state is unavailable after a failed lifetime lock restoration".into(),
        ));
    }

    match entry_state.locks.get_mut(&offset) {
        Some(SharedWalGlobalLock::Shared { holders }) if !exclusive => {
            *holders += 1;
            local.held.entry(offset).or_default().shared += 1;
            Ok(true)
        }
        Some(_) => Ok(false),
        None => match lock_range(entry_state.handle, offset, exclusive, fail_immediately) {
            Err(err) => Err(err),
            Ok(true) => {
                entry_state.locks.insert(
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
            Ok(false) => Ok(false),
        },
    }
}

fn release_shared_wal_lock(
    entry: &SharedWalRegistryEntry,
    offset: u64,
    shared: usize,
    exclusive: usize,
) -> Result<()> {
    debug_assert!(shared == 0 || exclusive == 0);
    let mut entry_state = entry.entry.lock();
    if entry_state.poisoned && !entry_state.locks.contains_key(&offset) {
        return Ok(());
    }
    let remove_lock = match entry_state.locks.get_mut(&offset) {
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
        if let Err(err) = unlock_range(entry_state.handle, offset) {
            if let Some(SharedWalGlobalLock::Shared { holders }) = entry_state.locks.get_mut(&offset)
            {
                *holders += shared;
            }
            return Err(err);
        }
        entry_state.locks.remove(&offset);
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
    let mut local = state.lock();
    let (shared, exclusive) = {
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

    let entry = local.entry.as_ref().ok_or_else(|| {
        LimboError::LockingError("shared WAL lock released without a registry entry".into())
    })?;
    if entry.key != path {
        return Err(LimboError::InternalError(
            "shared WAL lock state was used with a different coordination path".into(),
        ));
    }
    release_shared_wal_lock(entry, offset, shared, exclusive)?;

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
pub(crate) fn shared_wal_probe_exclusive_byte(
    path: &Path,
    state: &Mutex<SharedWalLockState>,
    offset: u64,
) -> Result<bool> {
    let mut local = state.lock();
    let entry = acquire_shared_wal_entry(path, &mut local)?;
    let mut entry_state = entry.entry.lock();
    if entry_state.poisoned {
        return Err(LimboError::LockingError(
            "shared WAL lock state is unavailable after a failed lifetime lock restoration".into(),
        ));
    }

    let had_shared_lock = matches!(
        entry_state.locks.get(&offset),
        Some(SharedWalGlobalLock::Shared { .. })
    );
    if matches!(
        entry_state.locks.get(&offset),
        Some(SharedWalGlobalLock::Exclusive)
    ) {
        return Ok(false);
    }

    // Use a disposable handle for the exclusive probe. If releasing that
    // exclusive lock fails, closing the handle still guarantees it cannot
    // survive past the probe and block restoration of the shared lock.
    let probe_handle = open_lock_handle(&entry.key)?;
    if had_shared_lock {
        if let Err(err) = unlock_range(entry_state.handle, offset) {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %entry.key.display(),
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
                    path = %entry.key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            result
        }
        Ok(false) => {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %entry.key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            Ok(false)
        }
        Err(err) => {
            if unsafe { CloseHandle(probe_handle) } == FALSE {
                tracing::error!(
                    path = %entry.key.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed closing shared WAL exclusive probe handle"
                );
            }
            Err(err)
        }
    };

    if had_shared_lock {
        match lock_range(entry_state.handle, offset, false, false) {
            Ok(true) => {}
            Ok(false) => {
                entry_state.poisoned = true;
                return Err(LimboError::LockingError(
                    "shared WAL lifetime lock restoration was blocked after exclusive probe".into(),
                ));
            }
            Err(err) => {
                entry_state.poisoned = true;
                return Err(LimboError::LockingError(format!(
                    "shared WAL lifetime lock restoration failed after exclusive probe: {err}"
                )));
            }
        }
    }

    probe
}

pub(crate) fn release_shared_wal_locks_on_drop(path: &Path, state: &Mutex<SharedWalLockState>) {
    let (held, entry) = {
        let mut state = state.lock();
        (std::mem::take(&mut state.held), state.entry.take())
    };
    let Some(entry) = entry else {
        debug_assert!(held.is_empty());
        return;
    };
    if entry.key != path {
        tracing::error!(
            path = %path.display(),
            registry_path = %entry.key.display(),
            "shared WAL lock state dropped with a different coordination path"
        );
        return;
    }
    for (offset, held) in held {
        if let Err(err) = release_shared_wal_lock(&entry, offset, held.shared, held.exclusive) {
            tracing::error!(
                path = %path.display(),
                offset,
                ?err,
                "failed releasing shared WAL lock while dropping Windows file"
            );
        }
    }
    release_shared_wal_entry(entry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::io::AsRawHandle;

    struct CurrentDirGuard(PathBuf);

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.0).expect("restore current directory");
        }
    }

    fn shared_wal_test_state() -> (tempfile::NamedTempFile, PathBuf, Mutex<SharedWalLockState>) {
        let file = tempfile::NamedTempFile::new().expect("create coordination file");
        let path = stable_lock_path(file.path());
        (file, path, Mutex::new(SharedWalLockState::default()))
    }

    #[test]
    fn shared_wal_lock_uses_cached_path_after_current_dir_change() {
        let current_dir = std::env::current_dir().expect("read current directory");
        let _restore = CurrentDirGuard(current_dir);
        let root = tempfile::tempdir().expect("create root directory");
        let first_dir = root.path().join("first");
        let second_dir = root.path().join("second");
        std::fs::create_dir_all(&first_dir).expect("create first directory");
        std::fs::create_dir_all(&second_dir).expect("create second directory");
        std::fs::write(first_dir.join("coordination.tshm"), []).expect("create coordination file");

        std::env::set_current_dir(&first_dir).expect("enter first directory");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("coordination.tshm")
            .expect("open coordination file");
        let path = stable_lock_path_for_handle(file.as_raw_handle(), Path::new("coordination.tshm"));
        let state = Mutex::new(SharedWalLockState::default());
        shared_wal_lock_byte(&path, &state, 0, false).expect("acquire shared lifetime lock");

        std::env::set_current_dir(&second_dir).expect("enter second directory");
        shared_wal_unlock_byte(&path, &state, 0).expect("release shared lifetime lock");
        release_shared_wal_locks_on_drop(&path, &state);
    }

    #[test]
    fn probe_unlock_failure_preserves_shared_lifetime_lock() {
        let (_file, path, state) = shared_wal_test_state();
        shared_wal_lock_byte(&path, &state, 0, false).expect("acquire shared lifetime lock");

        inject_lock_failures([InjectedLockFailure::Unlock { offset: 0 }]);
        assert!(shared_wal_probe_exclusive_byte(&path, &state, 0).is_err());

        shared_wal_unlock_byte(&path, &state, 0).expect("release preserved shared lifetime lock");
        release_shared_wal_locks_on_drop(&path, &state);
    }

    #[test]
    fn probe_failure_restores_shared_lifetime_lock() {
        let (_file, path, state) = shared_wal_test_state();
        shared_wal_lock_byte(&path, &state, 0, false).expect("acquire shared lifetime lock");

        inject_lock_failures([InjectedLockFailure::Lock {
            offset: 0,
            exclusive: true,
            fail_immediately: true,
        }]);
        assert!(shared_wal_probe_exclusive_byte(&path, &state, 0).is_err());

        shared_wal_unlock_byte(&path, &state, 0).expect("release restored shared lifetime lock");
        release_shared_wal_locks_on_drop(&path, &state);
    }

    #[test]
    fn shared_wal_locks_for_different_files_use_independent_entries() {
        let (_first_file, first_path, first_state) = shared_wal_test_state();
        let (_second_file, second_path, second_state) = shared_wal_test_state();
        shared_wal_lock_byte(&first_path, &first_state, 0, false)
            .expect("acquire first shared lifetime lock");
        shared_wal_lock_byte(&second_path, &second_state, 0, false)
            .expect("acquire second shared lifetime lock");

        let first_entry = first_state
            .lock()
            .entry
            .as_ref()
            .expect("first lock entry")
            .entry
            .clone();
        let second_entry = second_state
            .lock()
            .entry
            .as_ref()
            .expect("second lock entry")
            .entry
            .clone();
        assert!(!Arc::ptr_eq(&first_entry, &second_entry));
        drop(first_entry);
        drop(second_entry);

        shared_wal_unlock_byte(&first_path, &first_state, 0)
            .expect("release first shared lifetime lock");
        release_shared_wal_locks_on_drop(&first_path, &first_state);
        shared_wal_unlock_byte(&second_path, &second_state, 0)
            .expect("release second shared lifetime lock");
        release_shared_wal_locks_on_drop(&second_path, &second_state);
    }

    #[test]
    fn failed_lifetime_lock_restoration_poisoned_entry_cleans_up_on_drop() {
        let (_file, path, state) = shared_wal_test_state();
        shared_wal_lock_byte(&path, &state, 0, false).expect("acquire shared lifetime lock");

        inject_lock_failures([InjectedLockFailure::Lock {
            offset: 0,
            exclusive: false,
            fail_immediately: false,
        }]);
        assert!(shared_wal_probe_exclusive_byte(&path, &state, 0).is_err());
        assert!(shared_wal_try_lock_byte(&path, &state, 1, true).is_err());

        shared_wal_unlock_byte(&path, &state, 0).expect("release poisoned lifetime lock state");
        release_shared_wal_locks_on_drop(&path, &state);

        let reopened_state = Mutex::new(SharedWalLockState::default());
        shared_wal_lock_byte(&path, &reopened_state, 0, false)
            .expect("reopen clean shared lifetime lock state");
        shared_wal_unlock_byte(&path, &reopened_state, 0)
            .expect("release reopened shared lifetime lock state");
        release_shared_wal_locks_on_drop(&path, &reopened_state);
    }
}
