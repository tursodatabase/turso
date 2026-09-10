//! WORM (Write Once Read Many) directory abstraction.

#[cfg(feature = "mmap")]
mod mmap_directory;

mod directory;
mod directory_lock;
pub mod footer;
mod managed_directory;
mod ram_directory;
mod watch_event_router;

/// Errors specific to the directory module.
pub mod error;

mod composite_file;

use std::io::BufWriter;
use std::path::PathBuf;

pub use common::file_slice::{FileHandle, FileSlice};
pub use common::{AntiCallToken, OwnedBytes, TerminatingWrite};

pub use self::composite_file::{CompositeFile, CompositeWrite};
pub use self::directory::{Directory, DirectoryClone, DirectoryLock};
pub use self::directory_lock::{Lock, INDEX_WRITER_LOCK, META_LOCK};
pub use self::ram_directory::RamDirectory;
pub use self::watch_event_router::{WatchCallback, WatchCallbackList, WatchHandle};

/// Outcome of the Garbage collection
pub struct GarbageCollectionResult {
    /// List of files that were deleted in this cycle
    pub deleted_files: Vec<PathBuf>,
    /// List of files that were schedule to be deleted in this cycle,
    /// but deletion did not work. This typically happens on windows,
    /// as deleting a memory mapped file is forbidden.
    ///
    /// If a searcher is still held, a file cannot be deleted.
    /// This is not considered a bug, the file will simply be deleted
    /// in the next GC.
    pub failed_to_delete_files: Vec<PathBuf>,
}

#[cfg(all(feature = "mmap", unix))]
pub use memmap2::Advice;

pub use self::managed_directory::ManagedDirectory;
#[cfg(feature = "mmap")]
pub use self::mmap_directory::MmapDirectory;

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = BufWriter<Box<dyn TerminatingWrite + Send + Sync>>;

#[cfg(test)]
mod tests;
