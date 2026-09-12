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
