//! SQLite `-shm` sentinel file.
//!
//! Prevents SQLite from operating on the same database by exploiting its
//! WAL-index locking protocol. SQLite uses `fcntl(F_SETLK)` byte-range
//! locks on bytes 120-127 of the `-shm` file:
//!
//! - Byte 120: WAL_WRITE_LOCK   (exclusive for writers)
//! - Byte 121: WAL_CKPT_LOCK    (exclusive for checkpointers)
//! - Byte 122: WAL_RECOVER_LOCK (exclusive for recovery)
//! - Bytes 123-127: WAL_READ_LOCK(0-4) (shared for readers)
//!
//! We hold **shared** fcntl locks on bytes 120-122, which blocks SQLite
//! from acquiring the exclusive locks it needs for writing, checkpointing,
//! and recovery. Multiple Turso processes can coexist because shared locks
//! don't conflict with each other.
//!
//! We also hold a shared lock on byte 200 (outside SQLite's range) as a
//! "Turso marker" — used by `F_GETLK` probing to distinguish Turso from
//! SQLite when detecting existing lock holders.
//!
//! Additionally, we invalidate the WAL-index header (bytes 0-95) so that
//! even an idle SQLite process is forced into recovery mode on its next
//! operation — which our shared lock on byte 122 blocks.

use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;

use crate::LimboError;

/// Start of SQLite's WAL-index lock region.
const WALINDEX_LOCK_OFFSET: i64 = 120;

/// Number of bytes we lock in the SQLite lock region (120, 121, 122).
const WALINDEX_LOCK_COUNT: i64 = 3;

/// Byte outside SQLite's range, used as a Turso process marker.
const TURSO_MARKER_BYTE: i64 = 200;

/// Size of the WAL-index header (two 48-byte copies).
const WAL_INDEX_HEADER_SIZE: usize = 96;

/// Holds an open `-shm` file with fcntl byte-range locks that prevent
/// SQLite from using the database. Dropping this struct closes the fd
/// and releases all locks.
pub struct ShmSentinel {
    _file: File,
}

impl ShmSentinel {
    /// Open (or create) the `-shm` sentinel file, detect any active SQLite
    /// process, acquire shared locks, and invalidate the WAL-index header.
    pub fn open(shm_path: &str) -> crate::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(shm_path)
            .map_err(|e| LimboError::LockingError(format!("Failed to open {shm_path}: {e}")))?;

        let fd = file.as_raw_fd();

        // Probe for active SQLite: check if any process holds locks on
        // SQLite's WAL-index bytes (120-127) WITHOUT also holding the
        // Turso marker (byte 200).
        if is_byte_range_locked(fd, WALINDEX_LOCK_OFFSET, 8)
            && !is_byte_range_locked(fd, TURSO_MARKER_BYTE, 1)
        {
            return Err(LimboError::LockingError(
                "Database -shm file is locked by another process (possibly SQLite). \
                 Cannot open database while SQLite holds WAL-index locks."
                    .to_string(),
            ));
        }

        // Acquire Turso marker FIRST, before WAL-index locks. This prevents
        // a race where a concurrent ShmSentinel::open() sees our WAL-index
        // locks (120-122) but not yet the Turso marker (200), falsely
        // concluding SQLite holds the locks.
        fcntl_shared_lock(fd, TURSO_MARKER_BYTE, 1).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to lock {shm_path} Turso marker byte {TURSO_MARKER_BYTE}: {e}"
            ))
        })?;

        // Acquire shared locks on SQLite's critical exclusive-only bytes.
        fcntl_shared_lock(fd, WALINDEX_LOCK_OFFSET, WALINDEX_LOCK_COUNT).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to lock {shm_path} bytes {WALINDEX_LOCK_OFFSET}-{}: {e}",
                WALINDEX_LOCK_OFFSET + WALINDEX_LOCK_COUNT - 1
            ))
        })?;

        // Invalidate the WAL-index header so that any SQLite process
        // (even one that was idle with a valid mmap) is forced into
        // recovery mode on its next operation.
        invalidate_wal_index_header(&file, shm_path)?;

        Ok(Self { _file: file })
    }
}

/// Check if any process holds a lock (shared or exclusive) on the given
/// byte range using `fcntl(F_GETLK)`.
fn is_byte_range_locked(fd: i32, start: i64, len: i64) -> bool {
    let mut fl = new_flock(libc::F_WRLCK as libc::c_short, start, len);
    let ret = unsafe { libc::fcntl(fd, libc::F_GETLK, &mut fl) };
    if ret == -1 {
        return false; // If F_GETLK fails, assume unlocked.
    }
    // F_GETLK sets l_type to F_UNLCK if no conflicting lock exists.
    fl.l_type != libc::F_UNLCK as libc::c_short
}

/// Acquire a shared (F_RDLCK) byte-range lock using `fcntl(F_SETLK)`.
/// Non-blocking: returns an error if the lock cannot be acquired.
fn fcntl_shared_lock(fd: i32, start: i64, len: i64) -> std::io::Result<()> {
    let fl = new_flock(libc::F_RDLCK as libc::c_short, start, len);
    let ret = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
    if ret == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Zero the WAL-index header region (bytes 0-95) to invalidate it for SQLite.
fn invalidate_wal_index_header(file: &File, shm_path: &str) -> crate::Result<()> {
    use std::os::unix::fs::FileExt;

    let len = file
        .metadata()
        .map_err(|e| LimboError::LockingError(format!("Failed to stat {shm_path}: {e}")))?
        .len();

    if len == 0 {
        // File is empty — SQLite already needs recovery if it tries to use this.
        return Ok(());
    }

    let write_len = std::cmp::min(WAL_INDEX_HEADER_SIZE, len as usize);
    let zeros = [0u8; WAL_INDEX_HEADER_SIZE];
    file.write_all_at(&zeros[..write_len], 0).map_err(|e| {
        LimboError::LockingError(format!(
            "Failed to invalidate WAL-index header in {shm_path}: {e}"
        ))
    })?;

    Ok(())
}

/// Construct a `libc::flock` struct for byte-range locking.
fn new_flock(l_type: libc::c_short, start: i64, len: i64) -> libc::flock {
    libc::flock {
        l_type,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: start as libc::off_t,
        l_len: len as libc::off_t,
        l_pid: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_shm_path() -> (TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db-shm");
        (dir, path.to_string_lossy().to_string())
    }

    #[test]
    fn test_sentinel_creates_file() {
        let (_dir, path) = temp_shm_path();
        let _sentinel = ShmSentinel::open(&path).unwrap();
        assert!(std::path::Path::new(&path).exists());
    }

    #[test]
    fn test_multiple_turso_sentinels_same_process() {
        let (_dir, path) = temp_shm_path();
        let _s1 = ShmSentinel::open(&path).unwrap();
        // Second sentinel on same file should succeed (shared locks).
        let _s2 = ShmSentinel::open(&path).unwrap();
    }

    // All lock-conflict tests use fork() because fcntl locks are per-process:
    // F_GETLK doesn't report own-process locks, and F_SETLK upgrades instead
    // of conflicting within the same process.

    #[test]
    fn test_turso_coexistence_cross_process() {
        // Parent opens ShmSentinel, then child opens another — should succeed
        // because both hold SHARED locks (shared+shared = no conflict).
        let (_dir, path) = temp_shm_path();
        let _sentinel = ShmSentinel::open(&path).unwrap();

        let child = unsafe { libc::fork() };
        if child == 0 {
            let code = match ShmSentinel::open(&path) {
                Ok(_) => 0,
                Err(_) => 1,
            };
            unsafe { libc::_exit(code) };
        }
        assert!(child > 0, "fork failed");
        let mut status: libc::c_int = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "Second Turso process should coexist with first via shared locks"
        );
    }

    #[test]
    fn test_sqlite_exclusive_locks_blocked_cross_process() {
        // Parent holds ShmSentinel (shared locks on 120-122).
        // Child (simulating SQLite) tries exclusive locks on each byte — all
        // should be blocked.
        let (_dir, path) = temp_shm_path();
        let _sentinel = ShmSentinel::open(&path).unwrap();

        let child = unsafe { libc::fork() };
        if child == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();

            for byte in [120i64, 121, 122] {
                let fl = new_flock(libc::F_WRLCK as libc::c_short, byte, 1);
                let ret = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
                if ret != -1 {
                    unsafe { libc::_exit(1) }; // Lock succeeded — unexpected
                }
            }
            unsafe { libc::_exit(0) }; // All blocked — expected
        }
        assert!(child > 0, "fork failed");
        let mut status: libc::c_int = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "SQLite exclusive locks on bytes 120-122 should be blocked by Turso sentinel"
        );
    }

    #[test]
    fn test_detect_active_sqlite_process() {
        // Child (simulating SQLite) holds an exclusive lock on byte 122.
        // Parent tries ShmSentinel::open — should detect the active SQLite
        // process and refuse to open.
        let (_dir, path) = temp_shm_path();

        // Create the file so both processes can open it.
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // Pipe for child→parent "lock acquired" synchronization.
        let mut pipefd = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipefd.as_mut_ptr()) }, 0);

        let child = unsafe { libc::fork() };
        if child == 0 {
            unsafe { libc::close(pipefd[0]) }; // close read end
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            let fl = new_flock(libc::F_WRLCK as libc::c_short, 122, 1);
            let ret = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
            if ret == -1 {
                unsafe { libc::_exit(2) }; // couldn't acquire lock
            }
            // Signal parent that lock is held.
            let sig = [1u8];
            unsafe { libc::write(pipefd[1], sig.as_ptr() as *const libc::c_void, 1) };
            // Hold lock until killed.
            std::thread::sleep(std::time::Duration::from_secs(10));
            unsafe { libc::_exit(0) };
        }

        assert!(child > 0, "fork failed");
        unsafe { libc::close(pipefd[1]) }; // close write end in parent

        // Wait for child to signal that it holds the lock.
        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(pipefd[0], buf.as_mut_ptr() as *mut libc::c_void, 1) };
        assert_eq!(n, 1, "Should receive signal from child");

        // ShmSentinel::open should detect active SQLite and fail.
        let result = ShmSentinel::open(&path);
        assert!(
            result.is_err(),
            "Should detect active SQLite process holding exclusive lock"
        );

        // Clean up child.
        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            libc::waitpid(child, &mut status, 0);
        }
    }

    #[test]
    fn test_concurrent_sentinel_open_cross_process() {
        // N children all race to ShmSentinel::open() on the same file.
        // Without the marker-first acquisition order, some children see
        // WAL-index locks (120-122) before the Turso marker (200) and
        // falsely detect SQLite.
        let (_dir, path) = temp_shm_path();
        let n = 20;

        let mut children = Vec::new();
        for _ in 0..n {
            let child = unsafe { libc::fork() };
            if child == 0 {
                let code = match ShmSentinel::open(&path) {
                    Ok(_sentinel) => {
                        // Hold locks briefly so other processes can observe them.
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        0
                    }
                    Err(_) => 1,
                };
                unsafe { libc::_exit(code) };
            }
            assert!(child > 0, "fork failed");
            children.push(child);
        }

        for child in children {
            let mut status: libc::c_int = 0;
            unsafe { libc::waitpid(child, &mut status, 0) };
            assert!(
                libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                "All Turso processes should coexist — child {child} failed"
            );
        }
    }

    #[test]
    fn test_sentinel_cleanup_on_drop() {
        // After dropping a ShmSentinel, a new process should be able to open
        // the same file without seeing any stale locks.
        let (_dir, path) = temp_shm_path();

        // Open and immediately drop.
        let sentinel = ShmSentinel::open(&path).unwrap();
        drop(sentinel);

        // Child should see no locks at all.
        let child = unsafe { libc::fork() };
        if child == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            let wal_locked = is_byte_range_locked(fd, WALINDEX_LOCK_OFFSET, 8);
            let marker_locked = is_byte_range_locked(fd, TURSO_MARKER_BYTE, 1);
            // Neither should be locked after parent dropped the sentinel.
            let code = if wal_locked || marker_locked { 1 } else { 0 };
            unsafe { libc::_exit(code) };
        }
        assert!(child > 0, "fork failed");
        let mut status: libc::c_int = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "Locks should be fully released after ShmSentinel is dropped"
        );
    }

    #[test]
    fn test_sentinel_survives_child_exit() {
        // A child opens a ShmSentinel and exits. The parent (which also holds
        // a ShmSentinel) should still have its locks intact — child exit must
        // not affect parent's locks.
        let (_dir, path) = temp_shm_path();
        let _parent_sentinel = ShmSentinel::open(&path).unwrap();

        // Child opens, holds briefly, exits.
        let child = unsafe { libc::fork() };
        if child == 0 {
            let _child_sentinel = ShmSentinel::open(&path).unwrap();
            unsafe { libc::_exit(0) };
        }
        assert!(child > 0, "fork failed");
        let mut status: libc::c_int = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);

        // Verify parent's locks are still held: a new child should see them.
        let child2 = unsafe { libc::fork() };
        if child2 == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            let wal_locked = is_byte_range_locked(fd, WALINDEX_LOCK_OFFSET, WALINDEX_LOCK_COUNT);
            let marker_locked = is_byte_range_locked(fd, TURSO_MARKER_BYTE, 1);
            let code = if wal_locked && marker_locked { 0 } else { 1 };
            unsafe { libc::_exit(code) };
        }
        assert!(child2 > 0, "fork failed");
        unsafe { libc::waitpid(child2, &mut status, 0) };
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "Parent's ShmSentinel locks should survive child process exit"
        );
    }

    #[test]
    fn test_sqlite_shared_read_locks_not_false_positive() {
        // SQLite readers hold SHARED locks on bytes 123-127 (WAL_READ_LOCK).
        // Our probe checks bytes 120-127 with F_WRLCK, which would detect
        // shared locks too. Verify that reader-only locks (without 120-122
        // exclusive locks) do NOT trigger a false positive, since our check
        // requires bytes 120-127 to be locked AND byte 200 to be unlocked.
        //
        // In practice, SQLite readers hold shared locks on 123-127 and the
        // writer holds exclusive on 120. But if ONLY shared read locks exist
        // (no writer), our probe should still detect them because F_WRLCK
        // conflicts with F_RDLCK. The key is that byte 200 distinguishes
        // Turso from SQLite.
        let (_dir, path) = temp_shm_path();

        // Create the file.
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // Pipe for synchronization.
        let mut pipefd = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipefd.as_mut_ptr()) }, 0);

        // Child simulates a SQLite reader: shared lock on byte 123 only.
        let child = unsafe { libc::fork() };
        if child == 0 {
            unsafe { libc::close(pipefd[0]) };
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            let fl = new_flock(libc::F_RDLCK as libc::c_short, 123, 1);
            let ret = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
            if ret == -1 {
                unsafe { libc::_exit(2) };
            }
            let sig = [1u8];
            unsafe { libc::write(pipefd[1], sig.as_ptr() as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(10));
            unsafe { libc::_exit(0) };
        }
        assert!(child > 0, "fork failed");
        unsafe { libc::close(pipefd[1]) };

        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(pipefd[0], buf.as_mut_ptr() as *mut libc::c_void, 1) };
        assert_eq!(n, 1);

        // Parent opens ShmSentinel — should detect the SQLite reader lock
        // on byte 123 (within 120-127 range) without a Turso marker.
        let result = ShmSentinel::open(&path);
        assert!(
            result.is_err(),
            "Should detect SQLite reader holding shared lock on byte 123"
        );

        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            libc::waitpid(child, &mut status, 0);
        }
    }

    #[test]
    fn test_walindex_without_marker_detected_as_sqlite() {
        // A process holding WAL-index locks (120-122) WITHOUT the Turso marker
        // (byte 200) looks exactly like SQLite. ShmSentinel::open() must reject.
        // This is the intermediate state that the old (buggy) lock acquisition
        // order could expose to concurrent observers.
        let (_dir, path) = temp_shm_path();

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        let mut pipefd = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipefd.as_mut_ptr()) }, 0);

        let child = unsafe { libc::fork() };
        if child == 0 {
            unsafe { libc::close(pipefd[0]) };
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            // Acquire ONLY WAL-index locks — no Turso marker.
            fcntl_shared_lock(fd, WALINDEX_LOCK_OFFSET, WALINDEX_LOCK_COUNT).unwrap();
            let sig = [1u8];
            unsafe { libc::write(pipefd[1], sig.as_ptr() as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(10));
            unsafe { libc::_exit(0) };
        }
        assert!(child > 0, "fork failed");
        unsafe { libc::close(pipefd[1]) };

        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(pipefd[0], buf.as_mut_ptr() as *mut libc::c_void, 1) };
        assert_eq!(n, 1);

        let result = ShmSentinel::open(&path);
        assert!(
            result.is_err(),
            "WAL-index locks without Turso marker must be detected as SQLite"
        );

        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            libc::waitpid(child, &mut status, 0);
        }
    }

    #[test]
    fn test_marker_without_walindex_not_detected_as_sqlite() {
        // A process holding ONLY the Turso marker (byte 200) without WAL-index
        // locks (120-122) must NOT trigger SQLite detection. This is the
        // intermediate state during the correct (marker-first) acquisition order.
        let (_dir, path) = temp_shm_path();

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        let mut pipefd = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipefd.as_mut_ptr()) }, 0);

        let child = unsafe { libc::fork() };
        if child == 0 {
            unsafe { libc::close(pipefd[0]) };
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            // Acquire ONLY Turso marker — no WAL-index locks.
            fcntl_shared_lock(fd, TURSO_MARKER_BYTE, 1).unwrap();
            let sig = [1u8];
            unsafe { libc::write(pipefd[1], sig.as_ptr() as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(10));
            unsafe { libc::_exit(0) };
        }
        assert!(child > 0, "fork failed");
        unsafe { libc::close(pipefd[1]) };

        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(pipefd[0], buf.as_mut_ptr() as *mut libc::c_void, 1) };
        assert_eq!(n, 1);

        // Should succeed — marker-only means Turso, not SQLite.
        let result = ShmSentinel::open(&path);
        assert!(
            result.is_ok(),
            "Turso marker without WAL-index locks must NOT be detected as SQLite"
        );

        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            libc::waitpid(child, &mut status, 0);
        }
    }

    #[test]
    fn test_both_walindex_and_marker_not_detected_as_sqlite() {
        // A process holding BOTH WAL-index locks AND the Turso marker must NOT
        // trigger SQLite detection. This is the steady state for a Turso process.
        let (_dir, path) = temp_shm_path();

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        let mut pipefd = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipefd.as_mut_ptr()) }, 0);

        let child = unsafe { libc::fork() };
        if child == 0 {
            unsafe { libc::close(pipefd[0]) };
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let fd = file.as_raw_fd();
            // Acquire both — same as a fully-initialized ShmSentinel.
            fcntl_shared_lock(fd, TURSO_MARKER_BYTE, 1).unwrap();
            fcntl_shared_lock(fd, WALINDEX_LOCK_OFFSET, WALINDEX_LOCK_COUNT).unwrap();
            let sig = [1u8];
            unsafe { libc::write(pipefd[1], sig.as_ptr() as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(10));
            unsafe { libc::_exit(0) };
        }
        assert!(child > 0, "fork failed");
        unsafe { libc::close(pipefd[1]) };

        let mut buf = [0u8; 1];
        let n = unsafe { libc::read(pipefd[0], buf.as_mut_ptr() as *mut libc::c_void, 1) };
        assert_eq!(n, 1);

        let result = ShmSentinel::open(&path);
        assert!(
            result.is_ok(),
            "Both WAL-index and Turso marker must NOT be detected as SQLite"
        );

        unsafe {
            libc::kill(child, libc::SIGTERM);
            let mut status: libc::c_int = 0;
            libc::waitpid(child, &mut status, 0);
        }
    }

    #[test]
    fn test_rapid_open_close_cycles() {
        // Stress test: rapidly open and close sentinels across processes.
        // Tests that lock cleanup is reliable and no stale state accumulates.
        let (_dir, path) = temp_shm_path();

        for _ in 0..50 {
            let child = unsafe { libc::fork() };
            if child == 0 {
                let code = match ShmSentinel::open(&path) {
                    Ok(_) => 0,
                    Err(_) => 1,
                };
                unsafe { libc::_exit(code) };
            }
            assert!(child > 0, "fork failed");
            let mut status: libc::c_int = 0;
            unsafe { libc::waitpid(child, &mut status, 0) };
            assert!(
                libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                "Rapid open/close cycle failed"
            );
        }
    }
}
