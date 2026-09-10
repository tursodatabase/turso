use std::path::PathBuf;

use once_cell::sync::Lazy;

/// A directory lock.
///
/// A lock is associated with a specific path.
///
/// The lock will be passed to [`Directory::acquire_lock`](crate::Directory::acquire_lock).
///
/// Tantivy itself uses only two locks but client application
/// can use the directory facility to define their own locks.
/// - [`INDEX_WRITER_LOCK`]
/// - [`META_LOCK`]
///
/// Check out these locks documentation for more information.
#[derive(Debug)]
pub struct Lock {
    /// The lock needs to be associated with its own file `path`.
    /// Depending on the platform, the lock might rely on the creation
    /// and deletion of this filepath.
    pub filepath: PathBuf,
    /// `is_blocking` describes whether acquiring the lock is meant
    /// to be a blocking operation or a non-blocking.
    ///
    /// Acquiring a blocking lock blocks until the lock is
    /// available.
    ///
    /// Acquiring a non-blocking lock returns rapidly, either successfully
    /// or with an error signifying that someone is already holding
    /// the lock.
    pub is_blocking: bool,
}

/// Only one process should be able to write tantivy's index at a time.
/// This lock file, when present, is in charge of preventing other processes to open an
/// `IndexWriter`.
///
/// If the process is killed and this file remains, it is safe to remove it manually.
///
/// Failing to acquire this lock usually means a misuse of tantivy's API,
/// (creating more than one instance of the `IndexWriter`), are a spurious
/// lock file remaining after a crash. In the latter case, removing the file after
/// checking no process running tantivy is running is safe.
pub static INDEX_WRITER_LOCK: Lazy<Lock> = Lazy::new(|| Lock {
    filepath: PathBuf::from(".tantivy-writer.lock"),
    is_blocking: false,
});
/// The meta lock file is here to protect the segment files being opened by
/// `IndexReader::reload()` from being garbage collected.
///
/// It makes it possible for another process to safely consume
/// our index in-writing. Ideally, we may have preferred `RWLock` semantics
/// here, but it is difficult to achieve on Windows.
///
/// Opening segment readers is a very fast process.
pub static META_LOCK: Lazy<Lock> = Lazy::new(|| Lock {
    filepath: PathBuf::from(".tantivy-meta.lock"),
    is_blocking: true,
});
