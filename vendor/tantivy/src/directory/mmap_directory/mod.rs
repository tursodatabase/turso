mod file_watcher;

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};

use common::StableDeref;
use file_watcher::FileWatcher;
use fs4::fs_std::FileExt;
#[cfg(all(feature = "mmap", unix))]
pub use memmap2::Advice;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::core::META_FILEPATH;
use crate::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use crate::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
};

pub type ArcBytes = Arc<dyn Deref<Target = [u8]> + Send + Sync + 'static>;
pub type WeakArcBytes = Weak<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

/// Create a default io error given a string.
pub(crate) fn make_io_err(msg: String) -> io::Error {
    io::Error::other(msg)
}

/// Returns `None` iff the file exists, can be read, but is empty (and hence
/// cannot be mmapped)
fn open_mmap(full_path: &Path) -> Result<Option<Mmap>, OpenReadError> {
    let file = File::open(full_path).map_err(|io_err| {
        if io_err.kind() == io::ErrorKind::NotFound {
            OpenReadError::FileDoesNotExist(full_path.to_path_buf())
        } else {
            OpenReadError::wrap_io_error(io_err, full_path.to_path_buf())
        }
    })?;

    let meta_data = file
        .metadata()
        .map_err(|io_err| OpenReadError::wrap_io_error(io_err, full_path.to_owned()))?;
    if meta_data.len() == 0 {
        // if the file size is 0, it will not be possible
        // to mmap the file, so we return None
        // instead.
        return Ok(None);
    }
    let mmap_opt: Option<memmap2::Mmap> = unsafe {
        memmap2::Mmap::map(&file)
            .map(Some)
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, full_path.to_path_buf()))
    }?;

    Ok(mmap_opt)
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct CacheCounters {
    /// Number of time the cache prevents to call `mmap`
    pub hit: usize,
    /// Number of time tantivy had to call `mmap`
    /// as no entry was in the cache.
    pub miss: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheInfo {
    pub counters: CacheCounters,
    pub mmapped: Vec<PathBuf>,
}

struct MmapCache {
    counters: CacheCounters,
    cache: HashMap<PathBuf, WeakArcBytes>,
    #[cfg(unix)]
    madvice_opt: Option<Advice>,
}

impl MmapCache {
    fn new() -> MmapCache {
        MmapCache {
            counters: CacheCounters::default(),
            cache: HashMap::default(),
            #[cfg(unix)]
            madvice_opt: None,
        }
    }

    #[cfg(unix)]
    fn set_advice(&mut self, madvice: Advice) {
        self.madvice_opt = Some(madvice);
    }

    fn get_info(&self) -> CacheInfo {
        let paths: Vec<PathBuf> = self.cache.keys().cloned().collect();
        CacheInfo {
            counters: self.counters.clone(),
            mmapped: paths,
        }
    }

    fn remove_weak_ref(&mut self) {
        let keys_to_remove: Vec<PathBuf> = self
            .cache
            .iter()
            .filter(|(_, mmap_weakref)| mmap_weakref.upgrade().is_none())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys_to_remove {
            self.cache.remove(&key);
        }
    }

    fn open_mmap_impl(&self, full_path: &Path) -> Result<Option<Mmap>, OpenReadError> {
        let mmap_opt = open_mmap(full_path)?;
        #[cfg(unix)]
        if let (Some(mmap), Some(madvice)) = (mmap_opt.as_ref(), self.madvice_opt) {
            // We ignore madvise errors.
            let _ = mmap.advise(madvice);
        }
        Ok(mmap_opt)
    }

    // Returns None if the file exists but as a len of 0 (and hence is not mmappable).
    fn get_mmap(&mut self, full_path: &Path) -> Result<Option<ArcBytes>, OpenReadError> {
        if let Some(mmap_weak) = self.cache.get(full_path) {
            if let Some(mmap_arc) = mmap_weak.upgrade() {
                self.counters.hit += 1;
                return Ok(Some(mmap_arc));
            }
        }
        self.cache.remove(full_path);
        self.counters.miss += 1;
        let mmap_opt = self.open_mmap_impl(full_path)?;
        Ok(mmap_opt.map(|mmap| {
            let mmap_arc: ArcBytes = Arc::new(mmap);
            let mmap_weak = Arc::downgrade(&mmap_arc);
            self.cache.insert(full_path.to_owned(), mmap_weak);
            mmap_arc
        }))
    }
}

/// Directory storing data in files, read via mmap.
///
/// The Mmap object are cached to limit the
/// system calls.
///
/// In the `MmapDirectory`, locks are implemented using the `fs2` crate definition of locks.
///
/// On MacOS & linux, it relies on `flock` (aka `BSD Lock`). These locks solve most of the
/// problems related to POSIX Locks, but may their contract may not be respected on `NFS`
/// depending on the implementation.
///
/// On Windows the semantics are again different.
#[derive(Clone)]
pub struct MmapDirectory {
    inner: Arc<MmapDirectoryInner>,
}

struct MmapDirectoryInner {
    root_path: PathBuf,
    mmap_cache: RwLock<MmapCache>,
    _temp_directory: Option<TempDir>,
    watcher: FileWatcher,
}

impl MmapDirectoryInner {
    fn new(root_path: PathBuf, temp_directory: Option<TempDir>) -> MmapDirectoryInner {
        MmapDirectoryInner {
            mmap_cache: RwLock::new(MmapCache::new()),
            _temp_directory: temp_directory,
            watcher: FileWatcher::new(&root_path.join(*META_FILEPATH)),
            root_path,
        }
    }

    fn watch(&self, callback: WatchCallback) -> WatchHandle {
        self.watcher.watch(callback)
    }
}

impl fmt::Debug for MmapDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MmapDirectory({:?})", self.inner.root_path)
    }
}

impl MmapDirectory {
    fn new(root_path: PathBuf, temp_directory: Option<TempDir>) -> MmapDirectory {
        let inner = MmapDirectoryInner::new(root_path, temp_directory);
        MmapDirectory {
            inner: Arc::new(inner),
        }
    }

    /// Creates a new MmapDirectory in a temporary directory.
    ///
    /// This is mostly useful to test the MmapDirectory itself.
    /// For your unit tests, prefer the RamDirectory.
    pub fn create_from_tempdir() -> Result<MmapDirectory, OpenDirectoryError> {
        let tempdir = TempDir::new()
            .map_err(|io_err| OpenDirectoryError::FailedToCreateTempDir(Arc::new(io_err)))?;
        Ok(MmapDirectory::new(
            tempdir.path().to_path_buf(),
            Some(tempdir),
        ))
    }

    /// Opens a MmapDirectory in a directory, with a given access pattern.
    ///
    /// This is only supported on unix platforms.
    #[cfg(unix)]
    pub fn open_with_madvice(
        directory_path: impl AsRef<Path>,
        madvice: Advice,
    ) -> Result<MmapDirectory, OpenDirectoryError> {
        let dir = Self::open_impl_to_avoid_monomorphization(directory_path.as_ref())?;
        dir.inner.mmap_cache.write().unwrap().set_advice(madvice);
        Ok(dir)
    }

    /// Opens a MmapDirectory in a directory.
    ///
    /// Returns an error if the `directory_path` does not
    /// exist or if it is not a directory.
    pub fn open(directory_path: impl AsRef<Path>) -> Result<MmapDirectory, OpenDirectoryError> {
        Self::open_impl_to_avoid_monomorphization(directory_path.as_ref())
    }

    #[inline(never)]
    fn open_impl_to_avoid_monomorphization(
        directory_path: &Path,
    ) -> Result<MmapDirectory, OpenDirectoryError> {
        if !directory_path.exists() {
            return Err(OpenDirectoryError::DoesNotExist(PathBuf::from(
                directory_path,
            )));
        }
        #[expect(clippy::bind_instead_of_map)]
        let canonical_path: PathBuf = directory_path.canonicalize().or_else(|io_err| {
            let directory_path = directory_path.to_owned();

            #[cfg(windows)]
            {
                // `canonicalize` returns "Incorrect function" (error code 1)
                // for virtual drives (network drives, ramdisk, etc.).
                if io_err.raw_os_error() == Some(1) && directory_path.exists() {
                    // Should call `std::path::absolute` when it is stabilised.
                    return Ok(directory_path);
                }
            }

            Err(OpenDirectoryError::wrap_io_error(io_err, directory_path))
        })?;
        if !canonical_path.is_dir() {
            return Err(OpenDirectoryError::NotADirectory(PathBuf::from(
                directory_path,
            )));
        }
        Ok(MmapDirectory::new(canonical_path, None))
    }

    /// Joins a relative_path to the directory `root_path`
    /// to create a proper complete `filepath`.
    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.inner.root_path.join(relative_path)
    }

    /// Returns some statistical information
    /// about the Mmap cache.
    ///
    /// The `MmapDirectory` embeds a `MmapDirectory`
    /// to avoid multiplying the `mmap` system calls.
    pub fn get_cache_info(&self) -> CacheInfo {
        self.inner
            .mmap_cache
            .write()
            .expect("mmap cache lock is poisoned")
            .remove_weak_ref();
        self.inner
            .mmap_cache
            .read()
            .expect("Mmap cache lock is poisoned.")
            .get_info()
    }
}

/// We rely on fs2 for file locking. On Windows & MacOS this
/// uses BSD locks (`flock`). The lock is actually released when
/// the `File` object is dropped and its associated file descriptor
/// is closed.
struct ReleaseLockFile {
    _file: File,
    path: PathBuf,
}

impl Drop for ReleaseLockFile {
    fn drop(&mut self) {
        debug!("Releasing lock {:?}", self.path);
    }
}

/// This Write wraps a File, but has the specificity of
/// call `sync_all` on flush.
struct SafeFileWriter(File);

impl SafeFileWriter {
    fn new(file: File) -> SafeFileWriter {
        SafeFileWriter(file)
    }
}

impl Write for SafeFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for SafeFileWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.0.flush()?;
        self.0.sync_data()?;
        Ok(())
    }
}

#[derive(Clone)]
struct MmapArc(Arc<dyn Deref<Target = [u8]> + Send + Sync>);

impl Deref for MmapArc {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for MmapArc {}

/// Writes a file in an atomic manner.
pub(crate) fn atomic_write(path: &Path, content: &[u8]) -> io::Result<()> {
    // We create the temporary file in the same directory as the target file.
    // Indeed the canonical temp directory and the target file might sit in different
    // filesystem, in which case the atomic write may actually not work.
    let parent_path = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path {:?} does not have parent directory.",
        )
    })?;
    let mut tempfile = tempfile::Builder::new().tempfile_in(parent_path)?;
    tempfile.write_all(content)?;
    tempfile.flush()?;
    tempfile.as_file_mut().sync_data()?;
    tempfile.into_temp_path().persist(path)?;
    Ok(())
}

impl Directory for MmapDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        debug!("Open Read {path:?}");
        let full_path = self.resolve_path(path);

        let mut mmap_cache = self.inner.mmap_cache.write().map_err(|_| {
            let msg = format!("Failed to acquired write lock on mmap cache while reading {path:?}");
            let io_err = make_io_err(msg);
            OpenReadError::wrap_io_error(io_err, path.to_path_buf())
        })?;

        let owned_bytes = mmap_cache
            .get_mmap(&full_path)?
            .map(|mmap_arc| {
                let mmap_arc_obj = MmapArc(mmap_arc);
                OwnedBytes::new(mmap_arc_obj)
            })
            .unwrap_or_else(OwnedBytes::empty);

        Ok(Arc::new(owned_bytes))
    }

    /// Any entry associated with the path in the mmap will be
    /// removed before the file is deleted.
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let full_path = self.resolve_path(path);
        fs::remove_file(full_path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                DeleteError::FileDoesNotExist(path.to_owned())
            } else {
                DeleteError::IoError {
                    io_error: Arc::new(e),
                    filepath: path.to_path_buf(),
                }
            }
        })?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let full_path = self.resolve_path(path);
        full_path
            .try_exists()
            .map_err(|io_err| OpenReadError::wrap_io_error(io_err, path.to_path_buf()))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug!("Open Write {path:?}");
        let full_path = self.resolve_path(path);

        let open_res = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(full_path);

        let mut file = open_res.map_err(|io_err| {
            if io_err.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_path_buf())
            } else {
                OpenWriteError::wrap_io_error(io_err, path.to_path_buf())
            }
        })?;

        // making sure the file is created.
        file.flush()
            .map_err(|io_error| OpenWriteError::wrap_io_error(io_error, path.to_path_buf()))?;

        // Note we actually do not sync the parent directory here.
        //
        // A newly created file, may, in some case, be created and even flushed to disk.
        // and then lost...
        //
        // The file will only be durably written after we terminate AND
        // sync_directory() is called.

        let writer = SafeFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let full_path = self.resolve_path(path);
        let mut buffer = Vec::new();
        match File::open(full_path) {
            Ok(mut file) => {
                file.read_to_end(&mut buffer).map_err(|io_error| {
                    OpenReadError::wrap_io_error(io_error, path.to_path_buf())
                })?;
                Ok(buffer)
            }
            Err(io_error) => {
                if io_error.kind() == io::ErrorKind::NotFound {
                    Err(OpenReadError::FileDoesNotExist(path.to_owned()))
                } else {
                    Err(OpenReadError::wrap_io_error(io_error, path.to_path_buf()))
                }
            }
        }
    }

    fn atomic_write(&self, path: &Path, content: &[u8]) -> io::Result<()> {
        debug!("Atomic Write {path:?}");
        let full_path = self.resolve_path(path);
        atomic_write(&full_path, content)?;
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let full_path = self.resolve_path(&lock.filepath);
        // We make sure that the file exists.
        let file: File = OpenOptions::new()
            .write(true)
            .create(true) //< if the file does not exist yet, create it.
            .truncate(false)
            .open(full_path)
            .map_err(LockError::wrap_io_error)?;
        if lock.is_blocking {
            file.lock_exclusive().map_err(LockError::wrap_io_error)?;
        } else if !file.try_lock_exclusive().map_err(|_| LockError::LockBusy)? {
            return Err(LockError::LockBusy);
        }
        // dropping the file handle will release the lock.
        Ok(DirectoryLock::from(Box::new(ReleaseLockFile {
            path: lock.filepath.clone(),
            _file: file,
        })))
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        Ok(self.inner.watch(watch_callback))
    }

    #[cfg(windows)]
    fn sync_directory(&self) -> Result<(), io::Error> {
        // On Windows, it is not necessary to fsync the parent directory to
        // ensure that the directory entry containing the file has also reached
        // disk, and calling sync_data on a handle to directory is a no-op on
        // local disks, but will return an error on virtual drives.
        Ok(())
    }

    #[cfg(not(windows))]
    fn sync_directory(&self) -> Result<(), io::Error> {
        let mut open_opts = OpenOptions::new();

        // Linux needs read to be set, otherwise returns EINVAL
        // write must not be set, or it fails with EISDIR
        open_opts.read(true);

        let fd = open_opts.open(&self.inner.root_path)?;
        fd.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    // There are more tests in directory/mod.rs
    // The following tests are specific to the MmapDirectory

    use std::time::Duration;

    use common::HasLen;

    use super::*;
    use crate::indexer::LogMergePolicy;
    use crate::schema::{Schema, SchemaBuilder, TEXT};
    use crate::{Index, IndexSettings, IndexWriter, ReloadPolicy};

    #[test]
    fn test_open_non_existent_path() {
        assert!(MmapDirectory::open(PathBuf::from("./nowhere")).is_err());
    }

    #[test]
    fn test_open_empty() {
        // empty file is actually an edge case because those
        // cannot be mmapped.
        //
        // In that case the directory returns a SharedVecSlice.
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = PathBuf::from("test");
        {
            let mut w = mmap_directory.open_write(&path).unwrap();
            w.flush().unwrap();
        }
        let readonlymap = mmap_directory.open_read(&path).unwrap();
        assert_eq!(readonlymap.len(), 0);
    }

    #[test]
    fn test_cache() {
        let content = b"abc";

        // here we test if the cache releases
        // mmaps correctly.
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let num_paths = 10;
        let paths: Vec<PathBuf> = (0..num_paths)
            .map(|i| PathBuf::from(&*format!("file_{i}")))
            .collect();
        {
            for path in &paths {
                let mut w = mmap_directory.open_write(path).unwrap();
                w.write_all(content).unwrap();
                w.flush().unwrap();
            }
        }

        let mut keep = vec![];
        for (i, path) in paths.iter().enumerate() {
            keep.push(mmap_directory.open_read(path).unwrap());
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), i + 1);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 0);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), num_paths);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 10);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);

        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        }

        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 10);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 10);
        drop(keep);
        for path in paths.iter() {
            let _r = mmap_directory.open_read(path).unwrap();
            assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 1);
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 20);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);

        for path in &paths {
            mmap_directory.delete(path).unwrap();
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 20);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);
        for path in paths.iter() {
            assert!(mmap_directory.open_read(path).is_err());
        }
        assert_eq!(mmap_directory.get_cache_info().counters.hit, 20);
        assert_eq!(mmap_directory.get_cache_info().counters.miss, 30);
        assert_eq!(mmap_directory.get_cache_info().mmapped.len(), 0);
    }

    fn assert_eventually<P: Fn() -> Option<String>>(predicate: P) {
        for _ in 0..30 {
            if predicate().is_none() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        if let Some(error_msg) = predicate() {
            panic!("{}", error_msg);
        }
    }

    #[test]
    fn test_mmap_released() {
        let mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        let mut schema_builder: SchemaBuilder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        {
            let index =
                Index::create(mmap_directory.clone(), schema, IndexSettings::default()).unwrap();

            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_num_segments(3);
            index_writer.set_merge_policy(Box::new(log_merge_policy));
            for _num_commits in 0..10 {
                for _ in 0..10 {
                    index_writer.add_document(doc!(text_field=>"abc")).unwrap();
                }
                index_writer.commit().unwrap();
            }

            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .unwrap();

            for _ in 0..4 {
                index_writer.add_document(doc!(text_field=>"abc")).unwrap();
                index_writer.commit().unwrap();
                reader.reload().unwrap();
            }
            index_writer.wait_merging_threads().unwrap();

            reader.reload().unwrap();
            let num_segments = reader.searcher().segment_readers().len();
            assert!(num_segments <= 4);
            let num_components_except_deletes_and_tempstore =
                crate::index::SegmentComponent::iterator().len() - 1;
            let max_num_mmapped = num_components_except_deletes_and_tempstore * num_segments;
            assert_eventually(|| {
                let num_mmapped = mmap_directory.get_cache_info().mmapped.len();
                if num_mmapped > max_num_mmapped {
                    Some(format!(
                        "Expected at most {max_num_mmapped} mmapped files, got {num_mmapped}"
                    ))
                } else {
                    None
                }
            });
        }
        // This test failed on CI. The last Mmap is dropped from the merging thread so there might
        // be a race condition indeed.
        assert_eventually(|| {
            let num_mmapped = mmap_directory.get_cache_info().mmapped.len();
            if num_mmapped > 0 {
                Some(format!("Expected no mmapped files, got {num_mmapped}"))
            } else {
                None
            }
        });
    }
}
