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
use std::ops::Range;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite, WatchCallback,
    WatchHandle,
};
use tantivy::HasLen;

use crate::sync::Arc;

const TANTIVY_META_FILE: &str = "meta.json";
const TANTIVY_MANAGED_FILE: &str = ".managed.json";

/// In-memory file handle over resident bytes.
pub(super) struct InMemoryFileHandle {
    data: Arc<[u8]>,
}

impl std::fmt::Debug for InMemoryFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryFileHandle")
            .field("len", &self.data.len())
            .finish()
    }
}

impl HasLen for InMemoryFileHandle {
    fn len(&self) -> usize {
        self.data.len()
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
        Ok(OwnedBytes::new(Arc::clone(&self.data)).slice(range))
    }
}

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
    files: Arc<HashMap<PathBuf, Arc<[u8]>>>,
    meta_json: Arc<[u8]>,
}

impl SnapshotDirectory {
    pub fn new(files: HashMap<PathBuf, Arc<[u8]>>, meta_json: Vec<u8>) -> Self {
        Self {
            files: Arc::new(files),
            meta_json: Arc::from(meta_json),
        }
    }

    fn lookup(&self, path: &Path) -> Option<Arc<[u8]>> {
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
    files: HashMap<PathBuf, Arc<[u8]>>,
    /// Atomic writes (`meta.json`, `.managed.json`): absorbed here so
    /// whole-index manifests never reach the B-tree.
    atomic: HashMap<PathBuf, Vec<u8>>,
}

/// Private in-memory write buffer for building one immutable segment.
#[derive(Clone, Default)]
pub(super) struct BuildDirectory {
    inner: Arc<RwLock<BuildDirectoryInner>>,
}

impl BuildDirectory {
    /// The captured segment files (everything written through `open_write`).
    /// Atomic slots (`meta.json`, `.managed.json`) are excluded by
    /// construction.
    pub fn captured_files(&self) -> HashMap<PathBuf, Arc<[u8]>> {
        self.inner.read().files.clone()
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
    buffer: Vec<u8>,
    inner: Arc<RwLock<BuildDirectoryInner>>,
}

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
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
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        let data = std::mem::take(&mut self.buffer);
        self.inner
            .write()
            .files
            .insert(self.path.clone(), Arc::from(data));
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
            Some(data) => Ok(data.clone()),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.inner
            .write()
            .atomic
            .insert(path.to_path_buf(), data.to_vec());
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
            buffer: Vec::new(),
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
