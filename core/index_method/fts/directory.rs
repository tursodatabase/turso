//! Tantivy `Directory` implementations for segment-registry storage.
//!
//! Tantivy's `Directory` trait is synchronous while Turso storage is
//! asynchronous, so every byte a directory serves must already be resident:
//! the cursor loads segment contents through its resumable state machine
//! before any Tantivy object is constructed, and captures every byte Tantivy
//! writes so the cursor can persist it afterwards. Directory callbacks never
//! open a B-tree cursor or drive the pager.
//!
//! Two directories cover the two directions:
//!
//! * [`SnapshotDirectory`] — an immutable per-snapshot read view: resident
//!   segment files, synthesized `meta.json` and `.del` files. Nothing can be
//!   written through it.
//! * [`BuildDirectory`] — a private write buffer for building one immutable
//!   segment (or one merged segment). Files are captured on terminate;
//!   `meta.json` / `.managed.json` writes land in an in-memory slot and are
//!   never persisted.
//!
//! Both make Tantivy's file locks no-ops: with per-transaction private
//! segment builds there is no shared file state left for a lock to protect,
//! and the default `acquire_lock` would otherwise create and delete a shared
//! lock-file path on every searcher creation.

use rustc_hash::FxHashMap as HashMap;
use std::io::{BufWriter, Write};
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite, WatchCallback,
    WatchHandle,
};

#[cfg(not(nightly))]
use crate::alloc::TursoVecInExt;
use crate::alloc::{
    try_arc_slice_from_slice_in, ArcSlice, DynAllocator, DynVec, TursoFromIterator,
};
use crate::sync::Arc;

const TANTIVY_META_FILE: &str = "meta.json";
const TANTIVY_MANAGED_FILE: &str = ".managed.json";

/// In-memory file handle over resident bytes.
#[derive(Clone)]
pub(super) struct InMemoryFileHandle {
    data: ArcSlice<u8>,
}

impl std::fmt::Debug for InMemoryFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryFileHandle")
            .field("len", &self.data.len())
            .finish()
    }
}

impl FileHandle for InMemoryFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if range.end > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "range exceeds file length",
            ));
        }
        if range.start >= range.end {
            return Ok(OwnedBytes::empty());
        }
        Ok(OwnedBytes::new(self.clone()).slice(range))
    }
}

impl Deref for InMemoryFileHandle {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

unsafe impl stable_deref_trait::StableDeref for InMemoryFileHandle {}

/// A no-op directory lock: immediately satisfied, releases nothing.
struct NoopLockGuard;

fn noop_lock() -> DirectoryLock {
    DirectoryLock::from(Box::new(NoopLockGuard))
}

/// Immutable read view of one snapshot's visible segment set.
///
/// `files` holds every byte Tantivy may ask for: each visible segment's
/// files under their real names, plus one synthesized `.del` file per
/// segment with tombstones. `meta_json` is the `meta.json` synthesized
/// from the visible registry rows; no stored file ever carries that name.
#[derive(Clone)]
pub(super) struct SnapshotDirectory {
    files: Arc<HashMap<PathBuf, ArcSlice<u8>>>,
    meta_json: ArcSlice<u8>,
}

impl SnapshotDirectory {
    #[turso_macros::allocation_site(crate::alloc::FtsAllocationSite::SnapshotMetadata)]
    pub fn new(
        files: HashMap<PathBuf, ArcSlice<u8>>,
        meta_json: &[u8],
        allocator: DynAllocator,
    ) -> crate::Result<Self> {
        Ok(Self {
            files: Arc::new(files),
            meta_json: try_arc_slice_from_slice_in(meta_json, allocator)?,
        })
    }

    fn lookup(&self, path: &Path) -> Option<ArcSlice<u8>> {
        if path == Path::new(TANTIVY_META_FILE) {
            return Some(Arc::clone(&self.meta_json));
        }
        self.files.get(path).map(Arc::clone)
    }
}

impl std::fmt::Debug for SnapshotDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotDirectory")
            .field("files", &self.files.len())
            .field("meta_json_bytes", &self.meta_json.len())
            .finish()
    }
}

impl Directory for SnapshotDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        match self.lookup(path) {
            Some(data) => Ok(Arc::new(InMemoryFileHandle { data })),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        Ok(self.lookup(path).is_some())
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        // `.managed.json` intentionally reads as absent: it makes
        // `ManagedDirectory` inert, and its only purpose — garbage
        // collection — never runs against snapshot state.
        if path == Path::new(TANTIVY_MANAGED_FILE) {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        match self.lookup(path) {
            Some(data) => Ok(data.to_vec()),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> std::io::Result<()> {
        if path == Path::new(TANTIVY_MANAGED_FILE) {
            // ManagedDirectory bookkeeping; nothing to manage.
            return Ok(());
        }
        Err(std::io::Error::other(format!(
            "FTS snapshot is read-only: refused atomic write to {}",
            path.display()
        )))
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite + Send + Sync>>, OpenWriteError>
    {
        Err(OpenWriteError::wrap_io_error(
            std::io::Error::other("FTS snapshot is read-only"),
            path.to_path_buf(),
        ))
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(std::io::Error::other("FTS snapshot is read-only")),
            filepath: path.to_path_buf(),
        })
    }

    fn acquire_lock(&self, _lock: &Lock) -> std::result::Result<DirectoryLock, LockError> {
        Ok(noop_lock())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, _cb: WatchCallback) -> std::result::Result<WatchHandle, tantivy::TantivyError> {
        // Readers use `ReloadPolicy::Manual`; a callback would never fire.
        Ok(WatchHandle::empty())
    }
}

#[derive(Debug, Default)]
struct BuildDirectoryInner {
    /// Segment files captured on terminate, footer included.
    files: HashMap<PathBuf, ArcSlice<u8>>,
    /// Atomic writes (`meta.json`, `.managed.json`): absorbed here so
    /// whole-index manifests never reach the B-tree.
    atomic: HashMap<PathBuf, ArcSlice<u8>>,
    // Tantivy can convert typed I/O errors into strings. Remember allocation
    // failures so write_error can still return OutOfMemory for this build.
    allocation_failed: bool,
}

/// Private in-memory write buffer for building one immutable segment.
#[derive(Clone, Default)]
pub(super) struct BuildDirectory {
    inner: Arc<RwLock<BuildDirectoryInner>>,
    allocator: DynAllocator,
}

impl BuildDirectory {
    pub fn new(allocator: DynAllocator) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BuildDirectoryInner::default())),
            allocator,
        }
    }

    /// The captured segment files (everything written through `open_write`).
    /// Atomic slots (`meta.json`, `.managed.json`) are excluded by
    /// construction.
    pub fn captured_files(&self) -> HashMap<PathBuf, ArcSlice<u8>> {
        self.inner.read().files.clone()
    }

    pub fn write_error(&self, error: tantivy::TantivyError, context: &str) -> crate::LimboError {
        if self.inner.read().allocation_failed {
            crate::LimboError::OutOfMemory
        } else {
            crate::LimboError::InternalError(format!("{context}: {error}"))
        }
    }
}

impl std::fmt::Debug for BuildDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("BuildDirectory")
            .field("files", &inner.files.len())
            .field("atomic", &inner.atomic.len())
            .finish()
    }
}

/// Captures one file written through [`BuildDirectory::open_write`].
struct CaptureWriter {
    path: PathBuf,
    buffer: DynVec<u8>,
    allocator: DynAllocator,
    inner: Arc<RwLock<BuildDirectoryInner>>,
}

impl Write for CaptureWriter {
    #[turso_macros::allocation_site(crate::alloc::FtsAllocationSite::CaptureBuffer)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .try_extend(buf.iter().copied())
            .map_err(|error| {
                self.inner.write().allocation_failed = true;
                std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)
            })?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for CaptureWriter {
    fn drop(&mut self) {
        // Only terminate publishes: a file published from Drop would lack
        // its CRC footer and read back as corruption. Tantivy's Directory
        // contract says callers must not rely on Drop flushing.
        if !self.buffer.is_empty() {
            tracing::error!(
                path = %self.path.display(),
                bytes = self.buffer.len(),
                "FTS segment writer dropped without terminate; discarding buffered file"
            );
        }
    }
}

impl TerminatingWrite for CaptureWriter {
    #[turso_macros::allocation_site(crate::alloc::FtsAllocationSite::CapturedFile)]
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        let data = try_arc_slice_from_slice_in(self.buffer.as_slice(), self.allocator.clone())
            .map_err(|error| {
                self.inner.write().allocation_failed = true;
                std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)
            })?;
        self.inner.write().files.insert(self.path.clone(), data);
        self.buffer.clear();
        Ok(())
    }
}

impl Directory for BuildDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        match self.inner.read().files.get(path) {
            Some(data) => Ok(Arc::new(InMemoryFileHandle {
                data: Arc::clone(data),
            })),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        let inner = self.inner.read();
        Ok(inner.files.contains_key(path) || inner.atomic.contains_key(path))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        match self.inner.read().atomic.get(path) {
            Some(data) => Ok(data.to_vec()),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    #[turso_macros::allocation_site(crate::alloc::FtsAllocationSite::AtomicMetadata)]
    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let data = try_arc_slice_from_slice_in(data, self.allocator.clone()).map_err(|error| {
            self.inner.write().allocation_failed = true;
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)
        })?;
        self.inner.write().atomic.insert(path.to_path_buf(), data);
        Ok(())
    }

    fn open_write(
        &self,
        path: &Path,
    ) -> std::result::Result<BufWriter<Box<dyn TerminatingWrite + Send + Sync>>, OpenWriteError>
    {
        // Strict trait contract, and an invariant check: segment files are
        // write-once per uuid, so nothing ever legitimately rewrites a path.
        if self.inner.read().files.contains_key(path) {
            return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
        }
        let writer: Box<dyn TerminatingWrite + Send + Sync> = Box::new(CaptureWriter {
            path: path.to_path_buf(),
            buffer: DynVec::new_in(self.allocator.clone()),
            allocator: self.allocator.clone(),
            inner: Arc::clone(&self.inner),
        });
        Ok(BufWriter::new(writer))
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        self.inner.write().files.remove(path);
        Ok(())
    }

    fn acquire_lock(&self, _lock: &Lock) -> std::result::Result<DirectoryLock, LockError> {
        Ok(noop_lock())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, _cb: WatchCallback) -> std::result::Result<WatchHandle, tantivy::TantivyError> {
        Ok(WatchHandle::empty())
    }
}
