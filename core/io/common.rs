pub const ENV_DISABLE_FILE_LOCK: &str = "LIMBO_DISABLE_FILE_LOCK";

/// Acquire an fcntl (POSIX) lock on the given file descriptor.
/// Non-blocking: returns `LimboError::LockingError` if the lock is held.
#[cfg(unix)]
pub fn fcntl_lock(fd: rustix::fd::BorrowedFd<'_>, exclusive: bool) -> crate::Result<()> {
    use rustix::fs::{self, FlockOperation};
    fs::fcntl_lock(
        fd,
        if exclusive {
            FlockOperation::NonBlockingLockExclusive
        } else {
            FlockOperation::NonBlockingLockShared
        },
    )
    .map_err(|e| {
        let io_error = std::io::Error::from(e);
        let message = match io_error.kind() {
            std::io::ErrorKind::WouldBlock => {
                "Failed to open database: file is locked by another process. \
                 To enable multi-process access, all processes must open with: \
                 file:database.db?locking=shared_reads \
                 or PRAGMA locking_mode = shared_reads. \
                 For concurrent writers, use shared_writes instead."
                    .to_string()
            }
            _ => format!("Failed locking file, {io_error}"),
        };
        crate::LimboError::LockingError(message)
    })
}

/// Release an fcntl (POSIX) lock on the given file descriptor.
#[cfg(unix)]
pub fn fcntl_unlock(fd: rustix::fd::BorrowedFd<'_>) -> crate::Result<()> {
    use rustix::fs::{self, FlockOperation};
    fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
        crate::LimboError::LockingError(format!(
            "Failed to release file lock: {}",
            std::io::Error::from(e)
        ))
    })
}

#[cfg(test)]
pub mod tests {
    use crate::io::OpenFlags;
    use crate::{Result, IO};
    use std::process::{Command, Stdio};
    use tempfile::NamedTempFile;

    /// Spawn a child process that tries to open `path` with the given flags.
    /// Returns true if the child opened successfully, false if it failed.
    fn child_can_open(path: &str, child_flags: OpenFlags) -> bool {
        let current_exe = std::env::current_exe().expect("Failed to get current executable path");
        let flags_str = if child_flags.contains(OpenFlags::SharedLock) {
            "shared"
        } else {
            "exclusive"
        };
        let child = Command::new(current_exe)
            .env("RUST_TEST_CHILD_PROCESS", "1")
            .env("RUST_TEST_FILE_PATH", path)
            .env("RUST_TEST_LOCK_MODE", flags_str)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn child process");
        let output = child.wait_with_output().expect("Failed to wait on child");
        output.status.success()
    }

    /// Must be called at the start of every test that uses subprocess spawning.
    /// If we ARE the child process, open the file with the requested flags and exit.
    pub fn handle_child_process<T: IO>(create_io: fn() -> Result<T>) {
        if std::env::var("RUST_TEST_CHILD_PROCESS").is_ok() {
            let path = std::env::var("RUST_TEST_FILE_PATH").unwrap();
            let lock_mode = std::env::var("RUST_TEST_LOCK_MODE").unwrap_or_default();
            let flags = if lock_mode == "shared" {
                OpenFlags::SharedLock
            } else {
                OpenFlags::None
            };
            let io = create_io().unwrap();
            match io.open_file(&path, flags, false) {
                Ok(_) => std::process::exit(0),
                Err(_) => std::process::exit(1),
            }
        }
    }

    pub fn test_multiple_processes_cannot_open_file<T: IO>(create_io: fn() -> Result<T>) {
        handle_child_process(create_io);

        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap().to_string();

        // Parent process opens the file (exclusive lock by default)
        let io = create_io().expect("Failed to create IO");
        let _file = io
            .open_file(&path, OpenFlags::None, false)
            .expect("Failed to open file in parent process");

        assert!(
            !child_can_open(&path, OpenFlags::None),
            "Child with exclusive should have failed when parent holds exclusive"
        );
    }

    /// Parent holds exclusive lock → child with shared lock should fail.
    pub fn test_exclusive_blocks_shared<T: IO>(create_io: fn() -> Result<T>) {
        handle_child_process(create_io);

        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap().to_string();

        let io = create_io().expect("Failed to create IO");
        let _file = io
            .open_file(&path, OpenFlags::None, false)
            .expect("Parent: exclusive open");

        assert!(
            !child_can_open(&path, OpenFlags::SharedLock),
            "Child with shared lock should fail when parent holds exclusive"
        );
    }

    /// Parent holds shared lock → child with exclusive lock should fail.
    pub fn test_shared_blocks_exclusive<T: IO>(create_io: fn() -> Result<T>) {
        handle_child_process(create_io);

        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap().to_string();

        let io = create_io().expect("Failed to create IO");
        let _file = io
            .open_file(&path, OpenFlags::SharedLock, false)
            .expect("Parent: shared open");

        assert!(
            !child_can_open(&path, OpenFlags::None),
            "Child with exclusive lock should fail when parent holds shared"
        );
    }

    /// Parent holds shared lock → child with shared lock should succeed.
    pub fn test_shared_allows_shared<T: IO>(create_io: fn() -> Result<T>) {
        handle_child_process(create_io);

        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap().to_string();

        let io = create_io().expect("Failed to create IO");
        let _file = io
            .open_file(&path, OpenFlags::SharedLock, false)
            .expect("Parent: shared open");

        assert!(
            child_can_open(&path, OpenFlags::SharedLock),
            "Child with shared lock should succeed when parent holds shared"
        );
    }
}
