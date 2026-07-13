//! Sparse-file IO backends for partial sync.
//!
//! The lazy page storage tracks which pages are locally present via
//! [`File::has_hole`] and evicts them via [`File::punch_hole`]. Two backends
//! implement that contract for persistent files:
//!
//! * [`SparseLinuxIo`] (Linux only) uses the filesystem as the source of
//!   truth: never-written ranges of a sparse file are holes, `lseek(SEEK_DATA)`
//!   reports them exactly, and `fallocate(PUNCH_HOLE)` restores them.
//! * [`SparseBitmapIo`] (portable across Unix) keeps an explicit presence
//!   bitmap for the lazily-populated database file, mirroring the presence-map
//!   semantics of `MemoryIO`, and persists it to a sidecar file. Filesystem
//!   hole punching is only best-effort space reclamation. Files other than the
//!   tracked database file (WAL, metadata) are delegated to `PlatformIO`
//!   untouched.
//!
//! The bitmap backend exists because APFS on macOS cannot carry the Linux
//! contract. Delayed allocation rounds writes up to multi-block extents and
//! materializes neighbouring never-written (and even previously punched)
//! ranges as allocated zeros when dirty data is flushed, so `lseek(SEEK_DATA)`
//! reports absent pages as data. For lazy page storage that error direction is
//! fatal: an absent page reported present is served as zeros instead of being
//! fetched from the server. An explicit bitmap makes presence exact on any
//! filesystem; it is also fully exercisable on Linux, where the simulator,
//! stress, and Antithesis workloads run.
//!
//! # Crash consistency of the presence bitmap
//!
//! Presence bits protect two different things: fetched server pages (where a
//! lost bit only costs a re-fetch) and locally modified pages checkpointed
//! into the database file (where a lost bit would let the lazy storage
//! overwrite local data with an older server page). The backend therefore
//! maintains two properties:
//!
//! 1. The on-disk bitmap never *over*-claims: the data file is synced before
//!    every bitmap persist, and punched bits are made durable (temp file +
//!    fsync + rename + parent-directory fsync) before the bytes are released.
//! 2. The on-disk bitmap can lag the data file only within a single
//!    [`File::sync`] call. Because the pager only treats a checkpoint as
//!    complete once `sync` returns — and the WAL is truncated after that —
//!    a crash inside the window is repaired by WAL replay. Outside normal
//!    operation (sidecar deleted, corrupted, or belonging to a different
//!    file generation) the backend refuses to open rather than guess.

use std::{
    os::{fd::AsRawFd, unix::fs::FileExt},
    sync::{Arc, RwLock},
};

use tracing::{instrument, Level};
use turso_core::{
    io::{clock::DefaultClock, FileSyncType},
    io_error, Buffer, Clock, Completion, File, MonotonicInstant, OpenFlags, Result,
    WallClockInstant, IO,
};

fn completion_error(e: std::io::Error, op: &'static str) -> turso_core::LimboError {
    turso_core::LimboError::CompletionError(turso_core::CompletionError::IOError(e.kind(), op))
}

#[cfg(target_os = "linux")]
pub use linux::{SparseLinuxFile, SparseLinuxIo};

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    pub struct SparseLinuxIo {}

    impl SparseLinuxIo {
        pub fn new() -> Result<Self> {
            Ok(Self {})
        }
    }

    impl IO for SparseLinuxIo {
        #[instrument(skip_all, level = Level::TRACE)]
        fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
            let mut file = std::fs::File::options();
            file.read(true);

            if !flags.contains(OpenFlags::ReadOnly) {
                file.write(true);
                file.create(flags.contains(OpenFlags::Create));
            }

            let file = file.open(path).map_err(|e| io_error(e, "open"))?;
            Ok(Arc::new(SparseLinuxFile {
                file: RwLock::new(file),
            }))
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn remove_file(&self, path: &str) -> Result<()> {
            std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
            Ok(())
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn step(&self) -> Result<()> {
            Ok(())
        }
    }

    impl Clock for SparseLinuxIo {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            DefaultClock.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> WallClockInstant {
            DefaultClock.current_time_wall_clock()
        }
    }

    pub struct SparseLinuxFile {
        file: RwLock<std::fs::File>,
    }

    #[allow(clippy::readonly_write_lock)]
    impl File for SparseLinuxFile {
        #[instrument(err, skip_all, level = Level::TRACE)]
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            Ok(())
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }

        #[instrument(skip(self, c), level = Level::TRACE)]
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            let file = self.file.read().unwrap();
            let nr = {
                let r = c.as_read();
                let buf = r.buf();
                let buf = buf.as_mut_slice();
                file.read_exact_at(buf, pos)
                    .map_err(|e| io_error(e, "pread"))?;
                buf.len() as i32
            };
            c.complete(nr);
            Ok(c)
        }

        #[instrument(skip(self, c, buffer), level = Level::TRACE)]
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            let file = self.file.write().unwrap();
            let buf = buffer.as_slice();
            file.write_all_at(buf, pos)
                .map_err(|e| io_error(e, "pwrite"))?;
            c.complete(buffer.len() as i32);
            Ok(c)
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
            let file = self.file.write().unwrap();
            file.sync_all().map_err(|e| io_error(e, "sync"))?;
            c.complete(0);
            Ok(c)
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            let file = self.file.write().unwrap();
            file.set_len(len).map_err(|e| io_error(e, "truncate"))?;
            c.complete(0);
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            let file = self.file.read().unwrap();
            Ok(file.metadata().map_err(|e| io_error(e, "metadata"))?.len())
        }

        fn has_hole(&self, pos: usize, len: usize) -> turso_core::Result<bool> {
            let file = self.file.read().unwrap();
            // SEEK_DATA: Adjust the file offset to the next location in the file
            // greater than or equal to offset containing data.  If offset
            // points to data, then the file offset is set to offset
            // (see https://man7.org/linux/man-pages/man2/lseek.2.html#DESCRIPTION)
            let res = unsafe { libc::lseek(file.as_raw_fd(), pos as i64, libc::SEEK_DATA) };
            if res == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENXIO) {
                    // ENXIO: whence is SEEK_DATA or SEEK_HOLE, and offset is beyond the
                    // end of the file, or whence is SEEK_DATA and offset is
                    // within a hole at the end of the file.
                    // (see https://man7.org/linux/man-pages/man2/lseek.2.html#ERRORS)
                    return Ok(true);
                } else {
                    return Err(completion_error(err, "lseek"));
                }
            }
            // lseek succeeded - the hole is here if next data is strictly before pos + len - 1 (the last byte of the checked region
            Ok(res as usize >= pos + len)
        }

        fn punch_hole(&self, pos: usize, len: usize) -> turso_core::Result<()> {
            let file = self.file.write().unwrap();
            let res = unsafe {
                libc::fallocate(
                    file.as_raw_fd(),
                    libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                    pos as i64,
                    len as i64,
                )
            };
            if res == -1 {
                Err(completion_error(
                    std::io::Error::last_os_error(),
                    "fallocate",
                ))
            } else {
                Ok(())
            }
        }
    }
}

pub use bitmap::{SparseBitmapFile, SparseBitmapIo};

mod bitmap {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use turso_core::{turso_assert, turso_assert_reachable, turso_assert_sometimes};

    /// Presence is tracked per 4 KiB granule, matching `MemoryIO`'s
    /// `PAGE_SIZE` presence map and the minimum database page size. Like
    /// `MemoryIO` and the Linux backend (where any write allocates its whole
    /// block), a write marks every granule it touches; the untouched
    /// remainder of an edge granule reads as zeros — enforced by explicit
    /// zero-fill when the granule was absent, so a punched granule whose
    /// physical reclamation did not survive a crash can never leak stale
    /// bytes back through a later partial write.
    const GRANULE: u64 = 4096;

    const SIDECAR_SUFFIX: &str = ".present";
    const SIDECAR_MAGIC: &[u8; 4] = b"TPRB";
    const SIDECAR_VERSION: u8 = 3;
    /// magic + version + granule (u32) + dev (u64) + ino (u64) + crc32c (u32)
    const SIDECAR_HEADER_LEN: usize = 4 + 1 + 4 + 8 + 8 + 4;

    /// File identity: `(dev, ino)`. Keying the registry by identity rather
    /// than path string means alias spellings of one file (case-insensitive
    /// volumes, symlinks) share one presence state; a spelling that differs
    /// from the one the sidecar was persisted under fails closed at the next
    /// open (missing-sidecar anomaly) instead of corrupting.
    type FileId = (u64, u64);

    /// All handles to a tracked file share one [`FileEntry`], process-wide —
    /// across `SparseBitmapIo` instances too — mirroring how `MemoryIO`
    /// shares one store per path and how the Linux backend shares filesystem
    /// state between descriptors. Handle counting (instead of `Weak`) makes
    /// the last-close flush deterministic: it runs under the registry lock,
    /// so a concurrent reopen serializes after it.
    struct RegistryEntry {
        entry: Arc<FileEntry>,
        handles: usize,
    }

    fn registry() -> MutexGuard<'static, HashMap<FileId, RegistryEntry>> {
        static REGISTRY: OnceLock<Mutex<HashMap<FileId, RegistryEntry>>> = OnceLock::new();
        match REGISTRY.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Canonicalize the parent directory (the file itself may not exist yet)
    /// so relative/absolute spellings and symlinked directories compare
    /// equal for the tracked-path decision.
    fn canonical_key(path: &str) -> std::io::Result<std::path::PathBuf> {
        let p = std::path::Path::new(path);
        let name = p.file_name().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no file name")
        })?;
        let dir = p
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        Ok(std::fs::canonicalize(dir)?.join(name))
    }

    /// Sparse IO for one lazily-populated database file.
    ///
    /// `open_file` returns presence-tracked handles for `tracked_path` only;
    /// every other path (WAL, metadata, …) is delegated to [`PlatformIO`]
    /// so it keeps stock durability behavior and pays no sidecar overhead.
    ///
    /// [`PlatformIO`]: turso_core::PlatformIO
    pub struct SparseBitmapIo {
        tracked_key: std::path::PathBuf,
        /// The canonical spelling used for the sidecar name and entry path,
        /// so every alias spelling of the tracked file maps to one sidecar.
        tracked_path: String,
        inner: Arc<dyn IO>,
    }

    impl SparseBitmapIo {
        /// The tracked file's parent directory must already exist (the
        /// engine creates its metadata files alongside before opening the
        /// database), so the tracked-path comparison is stable for the
        /// lifetime of this IO.
        pub fn new(tracked_path: &str) -> Result<Self> {
            let tracked_key = canonical_key(tracked_path).map_err(|e| {
                turso_core::LimboError::InvalidArgument(format!(
                    "cannot resolve database directory for {tracked_path}: {e}"
                ))
            })?;
            let tracked_path = tracked_key.to_string_lossy().into_owned();
            Ok(Self {
                tracked_key,
                tracked_path,
                inner: Arc::new(turso_core::PlatformIO::new()?),
            })
        }

        fn is_tracked(&self, path: &str) -> bool {
            canonical_key(path).is_ok_and(|key| key == self.tracked_key)
        }

        /// Alias spellings of the tracked file (final-component symlinks,
        /// hard links, case aliases on case-insensitive volumes) are
        /// rejected rather than silently bypassing presence tracking.
        /// Detection is best-effort — it cannot be raced-proof against
        /// external renames — but the engine only ever uses the configured
        /// spelling, so any alias access is caller error, and an undetected
        /// alias is unsupported external interference.
        fn reject_alias(&self, path: &str, op: &'static str) -> Result<()> {
            let tracked = std::fs::metadata(&self.tracked_key).ok();
            let this = std::fs::metadata(path).ok();
            if let (Some(tracked), Some(this)) = (tracked, this) {
                if (tracked.dev(), tracked.ino()) == (this.dev(), this.ino()) {
                    return Err(turso_core::LimboError::InvalidArgument(format!(
                        "{op}: {path} is an alias of the presence-tracked database file;                          use the configured spelling"
                    )));
                }
            }
            Ok(())
        }
    }

    impl IO for SparseBitmapIo {
        #[instrument(skip_all, level = Level::TRACE)]
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            if !self.is_tracked(path) {
                self.reject_alias(path, "open")?;
                return self.inner.open_file(path, flags, direct);
            }
            let read_only = flags.contains(OpenFlags::ReadOnly);
            let mut options = std::fs::File::options();
            options.read(true);
            if !read_only {
                options.write(true);
                options.create(flags.contains(OpenFlags::Create));
            }
            let file = options.open(path).map_err(|e| io_error(e, "open"))?;
            let meta = file.metadata().map_err(|e| io_error(e, "metadata"))?;
            let id: FileId = (meta.dev(), meta.ino());

            let mut registry = registry();
            // Re-verify under the registry lock that the path still names the
            // inode we opened: `remove_file` holds this lock across its
            // unlinks, so an open racing a removal must not register (and
            // later persist presence for) an already-unlinked inode.
            let still_named = std::fs::metadata(path)
                .map(|current| (current.dev(), current.ino()) == id)
                .unwrap_or(false);
            if !still_named {
                return Err(io_error(
                    std::io::Error::from(std::io::ErrorKind::NotFound),
                    "open",
                ));
            }
            if let Some(reg_entry) = registry.get_mut(&id) {
                // The freshly opened descriptor names the same inode by
                // construction; if it is writable and the shared one is not,
                // upgrade in place.
                if !read_only && !reg_entry.entry.fd_writable.load(Ordering::Acquire) {
                    *reg_entry.entry.file.write().unwrap() = file;
                    reg_entry.entry.fd_writable.store(true, Ordering::Release);
                }
                reg_entry.handles += 1;
                return Ok(Arc::new(SparseBitmapFile {
                    entry: reg_entry.entry.clone(),
                    id,
                    read_only,
                }));
            }
            let entry = Arc::new(FileEntry::new(&self.tracked_path, file, &meta, read_only)?);
            registry.insert(
                id,
                RegistryEntry {
                    entry: entry.clone(),
                    handles: 1,
                },
            );
            Ok(Arc::new(SparseBitmapFile {
                entry,
                id,
                read_only,
            }))
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn remove_file(&self, path: &str) -> Result<()> {
            if !self.is_tracked(path) {
                self.reject_alias(path, "remove_file")?;
                return self.inner.remove_file(path);
            }
            // Hold the registry lock across the whole removal so a concurrent
            // open cannot observe a half-removed file, and the entry's
            // persist lock so an in-flight sidecar persist either completes
            // before the unlinks or observes `defunct` after them.
            //
            // Failure ordering keeps every exit internally consistent: the
            // data unlink goes first (failure leaves all state untouched);
            // a sidecar-unlink failure afterwards leaves residue that the
            // identity binding fails closed on; registry state is only
            // mutated once the files are gone.
            let mut registry = registry();
            let entry = std::fs::metadata(path)
                .ok()
                .map(|meta| (meta.dev(), meta.ino()))
                .and_then(|id| {
                    registry
                        .get(&id)
                        .map(|reg_entry| (id, reg_entry.entry.clone()))
                });
            let _persist_guard = entry
                .as_ref()
                .map(|(_, entry)| match entry.persist_lock.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                });
            std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
            if let Some((id, entry)) = entry.as_ref() {
                entry.defunct.store(true, Ordering::Release);
                registry.remove(id);
            }
            match std::fs::remove_file(sidecar_path(&self.tracked_path)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(e, "remove_file")),
            }
            // Make both unlinks durable together.
            sync_parent_dir(&self.tracked_path, FileSyncType::Fsync)
                .map_err(|e| completion_error(e, "sync dir after remove"))?;
            Ok(())
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn step(&self) -> Result<()> {
            self.inner.step()
        }
    }

    impl Clock for SparseBitmapIo {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            DefaultClock.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> WallClockInstant {
            DefaultClock.current_time_wall_clock()
        }
    }

    fn sidecar_path(path: &str) -> String {
        format!("{path}{SIDECAR_SUFFIX}")
    }

    fn sync_parent_dir(path: &str, sync_type: FileSyncType) -> std::io::Result<()> {
        let dir = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let dir = std::fs::File::open(dir)?;
        barrier_sync(&dir, sync_type)
    }

    /// Fsync honoring [`FileSyncType`]: on Apple platforms `FullFsync` maps
    /// to `fcntl(F_FULLFSYNC)` per the `File::sync` contract; elsewhere it
    /// is a plain fsync (mirrors `UnixFile`).
    fn barrier_sync(file: &std::fs::File, sync_type: FileSyncType) -> std::io::Result<()> {
        #[cfg(target_vendor = "apple")]
        {
            let res = match sync_type {
                FileSyncType::Fsync => unsafe { libc::fsync(file.as_raw_fd()) },
                FileSyncType::FullFsync => unsafe {
                    libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC)
                },
            };
            if res == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            let _ = sync_type;
            file.sync_all()
        }
    }

    struct PresenceState {
        present: roaring::RoaringTreemap,
        dirty: bool,
    }

    struct FileEntry {
        path: String,
        sidecar_path: String,
        file: RwLock<std::fs::File>,
        state: RwLock<PresenceState>,
        /// Serializes sidecar persistence against `remove_file`, closing the
        /// window where an in-flight persist could re-create the sidecar of
        /// a just-removed file. No cycle: `sync`/`punch_hole`/`Drop` acquire
        /// it while holding file/state locks, but `remove_file` (registry →
        /// persist_lock) never takes file/state locks, and the final `Drop`
        /// cannot overlap another operation on the same handle.
        persist_lock: Mutex<()>,
        /// Whether the shared descriptor was opened with write access.
        fd_writable: AtomicBool,
        /// Set by `remove_file`: presence persistence stops, data operations
        /// keep POSIX unlinked-file semantics.
        defunct: AtomicBool,
    }

    /// A presence-tracked handle to the lazily-populated database file. All
    /// handles to one file share a [`FileEntry`]; the access mode is tracked
    /// per handle.
    pub struct SparseBitmapFile {
        entry: Arc<FileEntry>,
        id: FileId,
        read_only: bool,
    }

    impl FileEntry {
        fn new(
            path: &str,
            file: std::fs::File,
            meta: &std::fs::Metadata,
            read_only: bool,
        ) -> Result<Self> {
            let sidecar = sidecar_path(path);
            if !read_only {
                sweep_orphan_tmp_files(&sidecar);
            }
            let present = load_sidecar(&sidecar, meta.len(), meta.dev(), meta.ino(), read_only)?;
            Ok(Self {
                path: path.to_string(),
                sidecar_path: sidecar,
                file: RwLock::new(file),
                state: RwLock::new(PresenceState {
                    present,
                    dirty: false,
                }),
                persist_lock: Mutex::new(()),
                fd_writable: AtomicBool::new(!read_only),
                defunct: AtomicBool::new(false),
            })
        }
    }

    impl SparseBitmapFile {
        fn reject_if_read_only(&self, op: &'static str) -> Result<()> {
            if self.read_only {
                return Err(completion_error(
                    std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                    op,
                ));
            }
            Ok(())
        }
    }

    /// Deterministic last-close flush: the handle count lives in the global
    /// registry, and the final flush runs under the registry lock, so a
    /// concurrent reopen serializes after it. Pure read hydration never
    /// triggers [`File::sync`]; without this flush a clean close would leave
    /// the fetched pages on disk (OS writeback) but not the bitmap — the
    /// Linux backend gets that coupling from the filesystem for free. A hard
    /// crash between syncs still loses hydration state and re-fetches, which
    /// is the safe direction.
    impl Drop for SparseBitmapFile {
        fn drop(&mut self) {
            let mut registry = registry();
            let Some(reg_entry) = registry.get_mut(&self.id) else {
                return; // removed via remove_file
            };
            if !Arc::ptr_eq(&reg_entry.entry, &self.entry) {
                return; // the file was removed and re-registered
            }
            reg_entry.handles -= 1;
            if reg_entry.handles > 0 {
                return;
            }
            let Some(reg_entry) = registry.remove(&self.id) else {
                return;
            };
            let entry = reg_entry.entry;
            if entry.defunct.load(Ordering::Acquire) {
                return;
            }
            let mut state = match entry.state.write() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if !state.dirty {
                return;
            }
            let file = match entry.file.read() {
                Ok(file) => file,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Err(e) = barrier_sync(&file, FileSyncType::Fsync) {
                tracing::warn!(
                    "failed to sync {} on close; leaving presence sidecar stale: {e}",
                    entry.path
                );
                return;
            }
            if let Err(e) = persist_presence(&entry, &file, &mut state, FileSyncType::Fsync) {
                tracing::warn!("failed to persist presence sidecar on close: {e}");
            }
        }
    }

    /// Load and validate the sidecar against the opened data file.
    ///
    /// An empty data file is fresh by definition: any sidecar found next to
    /// it is stale and discarded. For a non-empty data file the sidecar is
    /// required and must match this file's identity and checksum — the bits
    /// may guard locally modified pages, so guessing "all absent" could let
    /// the lazy storage overwrite local data with older server pages. Refuse
    /// instead.
    fn load_sidecar(
        path: &str,
        data_len: u64,
        dev: u64,
        ino: u64,
        read_only: bool,
    ) -> Result<roaring::RoaringTreemap> {
        let anomaly = |reason: &str| {
            turso_core::LimboError::Corrupt(format!(
                "presence sidecar {path} {reason}; refusing to guess page presence for a \
                 non-empty database file (delete the database file and its sidecar to \
                 re-bootstrap)"
            ))
        };
        let discard_stale = |bytes_existed: bool| {
            if bytes_existed && !read_only {
                turso_assert_reachable!("stale presence sidecar discarded for fresh file");
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!("failed to remove stale presence sidecar {path}: {e}");
                }
            }
            roaring::RoaringTreemap::new()
        };
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if data_len == 0 {
                    return Ok(discard_stale(false));
                }
                return Err(anomaly("is missing"));
            }
            Err(e) => return Err(io_error(e, "read presence sidecar")),
        };
        if data_len == 0 {
            return Ok(discard_stale(true));
        }
        match parse_sidecar(&bytes) {
            Some((sidecar_dev, sidecar_ino, bitmap)) => {
                if sidecar_dev != dev || sidecar_ino != ino {
                    turso_assert_reachable!("presence sidecar belongs to another file generation");
                    return Err(anomaly("belongs to a different file generation"));
                }
                Ok(bitmap)
            }
            None => {
                turso_assert_reachable!("presence sidecar is corrupt");
                Err(anomaly("is corrupt or has an unknown format"))
            }
        }
    }

    fn parse_sidecar(bytes: &[u8]) -> Option<(u64, u64, roaring::RoaringTreemap)> {
        if bytes.len() < SIDECAR_HEADER_LEN {
            return None;
        }
        let (header, payload) = bytes.split_at(SIDECAR_HEADER_LEN);
        if &header[0..4] != SIDECAR_MAGIC || header[4] != SIDECAR_VERSION {
            return None;
        }
        let granule = u32::from_le_bytes(header[5..9].try_into().ok()?);
        if u64::from(granule) != GRANULE {
            return None;
        }
        let dev = u64::from_le_bytes(header[9..17].try_into().ok()?);
        let ino = u64::from_le_bytes(header[17..25].try_into().ok()?);
        let crc = u32::from_le_bytes(header[25..29].try_into().ok()?);
        if crc32c::crc32c(payload) != crc {
            return None;
        }
        let bitmap = roaring::RoaringTreemap::deserialize_from(payload).ok()?;
        Some((dev, ino, bitmap))
    }

    /// Remove leftovers of persists interrupted by a crash or failure. The
    /// temp names are namespaced by the sidecar path, so this can only touch
    /// this backend's own files.
    fn sweep_orphan_tmp_files(sidecar: &str) {
        let path = std::path::Path::new(sidecar);
        let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
            return;
        };
        let dir = if dir.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            dir
        };
        let Ok(dir_entries) = std::fs::read_dir(dir) else {
            return;
        };
        let prefix = format!("{}.", name.to_string_lossy());
        for dir_entry in dir_entries.flatten() {
            let file_name = dir_entry.file_name();
            let file_name = file_name.to_string_lossy();
            let Some(middle) = file_name
                .strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix(".tmp"))
            else {
                continue;
            };
            // Names are `<sidecar>.<pid>.<seq>.tmp`; only sweep our own
            // leftovers or those of processes that no longer exist, so an
            // in-flight persist elsewhere is never deleted.
            let Some(pid) = middle
                .split('.')
                .next()
                .and_then(|pid| pid.parse::<u32>().ok())
            else {
                continue;
            };
            let orphaned = pid == std::process::id()
                || unsafe { libc::kill(pid as libc::pid_t, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            if orphaned {
                let _ = std::fs::remove_file(dir_entry.path());
            }
        }
    }

    /// Persist the bitmap via write-to-temp + fsync + rename + parent
    /// directory fsync, so a crash leaves either the old or the new sidecar
    /// — durably — and never a torn one. Fsyncing the renamed file alone
    /// would not make the new *directory entry* durable. The temp name is
    /// unique per process and per persist, so concurrent writers can never
    /// interleave inside one temp file.
    ///
    /// Ordering invariant: the caller must sync the data file BEFORE the
    /// bitmap is persisted. The on-disk bitmap may then lag the data file
    /// (safe: within a `File::sync` call the pager has not yet completed
    /// its checkpoint, so WAL replay repairs a crash) but never lead it
    /// (which could serve unfetched bytes as zeros).
    fn persist_presence(
        entry: &FileEntry,
        file: &std::fs::File,
        state: &mut PresenceState,
        sync_type: FileSyncType,
    ) -> Result<()> {
        if !state.dirty {
            return Ok(());
        }
        // Serialize against `remove_file`: it holds this lock across its
        // unlinks, so we either finish the rename before removal starts or
        // observe `defunct` after it finished.
        let _persist_guard = match entry.persist_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if entry.defunct.load(Ordering::Acquire) {
            return Ok(());
        }
        // If the path no longer names this entry's inode (removed or replaced
        // externally), renaming the sidecar into place would attach this
        // entry's bits to some other file's name. Skip; the identity binding
        // makes any stale on-disk sidecar fail closed at the next open.
        let fd_meta = file.metadata().map_err(|e| io_error(e, "metadata"))?;
        let still_named = std::fs::metadata(&entry.path)
            .map(|current| (current.dev(), current.ino()) == (fd_meta.dev(), fd_meta.ino()))
            .unwrap_or(false);
        if !still_named {
            return Err(turso_core::LimboError::Corrupt(format!(
                "presence sidecar for {} not persisted: the path no longer names this file",
                entry.path
            )));
        }
        let mut payload = Vec::with_capacity(state.present.serialized_size());
        state
            .present
            .serialize_into(&mut payload)
            .map_err(|e| completion_error(e, "serialize presence sidecar"))?;
        let meta = file.metadata().map_err(|e| io_error(e, "metadata"))?;
        let mut buf = Vec::with_capacity(SIDECAR_HEADER_LEN + payload.len());
        buf.extend_from_slice(SIDECAR_MAGIC);
        buf.push(SIDECAR_VERSION);
        buf.extend_from_slice(&(GRANULE as u32).to_le_bytes());
        buf.extend_from_slice(&meta.dev().to_le_bytes());
        buf.extend_from_slice(&meta.ino().to_le_bytes());
        buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        buf.extend_from_slice(&payload);

        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let tmp_path = format!(
            "{}.{}.{}.tmp",
            entry.sidecar_path,
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut tmp = std::fs::File::create(&tmp_path)
            .map_err(|e| completion_error(e, "create presence sidecar"))?;
        tmp.write_all(&buf)
            .map_err(|e| completion_error(e, "write presence sidecar"))?;
        barrier_sync(&tmp, sync_type).map_err(|e| completion_error(e, "sync presence sidecar"))?;
        std::fs::rename(&tmp_path, &entry.sidecar_path)
            .map_err(|e| completion_error(e, "rename presence sidecar"))?;
        sync_parent_dir(&entry.sidecar_path, sync_type)
            .map_err(|e| completion_error(e, "sync sidecar dir"))?;
        state.dirty = false;
        Ok(())
    }

    /// Release the punched byte range back to the filesystem. Best-effort:
    /// the bitmap is the source of truth, so failure (or a filesystem that
    /// later re-materializes the range, as APFS does next to flushed writes)
    /// costs space, never correctness.
    #[cfg(target_os = "linux")]
    fn reclaim_range(file: &std::fs::File, pos: u64, len: u64) -> std::io::Result<()> {
        let res = unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                pos as i64,
                len as i64,
            )
        };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Release the punched byte range back to the filesystem. Best-effort:
    /// the bitmap is the source of truth, so failure (or a filesystem that
    /// later re-materializes the range, as APFS does next to flushed writes)
    /// costs space, never correctness.
    ///
    /// `F_PUNCHHOLE` requires block-size alignment (4096 on APFS); callers
    /// punch at granule alignment, which satisfies it.
    #[cfg(target_vendor = "apple")]
    fn reclaim_range(file: &std::fs::File, pos: u64, len: u64) -> std::io::Result<()> {
        let args = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: pos as libc::off_t,
            fp_length: len as libc::off_t,
        };
        let res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &args) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    fn reclaim_range(_file: &std::fs::File, _pos: u64, _len: u64) -> std::io::Result<()> {
        Ok(())
    }

    /// Granule indices overlapping `[pos, pos + len)`: a write marks all of
    /// them (any touched block is allocated, like `MemoryIO` and Linux).
    fn overlapping(pos: u64, len: u64) -> Result<std::ops::Range<u64>> {
        let end = pos.checked_add(len).ok_or_else(|| {
            turso_core::LimboError::InvalidArgument(format!(
                "byte range overflows: pos={pos} len={len}"
            ))
        })?;
        Ok(pos / GRANULE..end.div_ceil(GRANULE))
    }

    #[allow(clippy::readonly_write_lock)]
    impl File for SparseBitmapFile {
        #[instrument(err, skip_all, level = Level::TRACE)]
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            Ok(())
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }

        #[instrument(skip(self, c), level = Level::TRACE)]
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            let nr = {
                let file = self.entry.file.read().unwrap();
                let r = c.as_read();
                let buf = r.buf();
                let buf = buf.as_mut_slice();
                file.read_exact_at(buf, pos)
                    .map_err(|e| io_error(e, "pread"))?;
                buf.len() as i32
                // Guards drop here: completion callbacks may reopen or drop
                // other handles, which takes the registry lock.
            };
            c.complete(nr);
            Ok(c)
        }

        #[instrument(skip(self, c, buffer), level = Level::TRACE)]
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.reject_if_read_only("pwrite")?;
            let buf = buffer.as_slice();
            if buf.is_empty() {
                // An empty write allocates nothing (matches `MemoryIO`).
                c.complete(0);
                return Ok(c);
            }
            {
                let file = self.entry.file.write().unwrap();
                let range = overlapping(pos, buf.len() as u64)?;
                let mut state = self.entry.state.write().unwrap();
                // A previously absent edge granule must not leak whatever
                // bytes physically precede/follow the written span (e.g. a
                // punched range whose physical reclamation did not happen or
                // did not survive a crash): zero the uncovered remainder so
                // "present" always means fully defined bytes, exactly like a
                // fresh `MemoryIO` page.
                let file_len = file.metadata().map_err(|e| io_error(e, "metadata"))?.len();
                if let Some(first) = range.clone().next() {
                    let granule_start = first * GRANULE;
                    if pos > granule_start && !state.present.contains(first) {
                        let zeros = vec![0u8; (pos - granule_start) as usize];
                        file.write_all_at(&zeros, granule_start)
                            .map_err(|e| io_error(e, "pwrite"))?;
                    }
                }
                if let Some(last) = range.clone().next_back() {
                    let span_end = pos + buf.len() as u64;
                    let granule_end = (last + 1)
                        .checked_mul(GRANULE)
                        .unwrap_or(u64::MAX)
                        .min(file_len.max(span_end));
                    if granule_end > span_end && !state.present.contains(last) {
                        let zeros = vec![0u8; (granule_end - span_end) as usize];
                        file.write_all_at(&zeros, span_end)
                            .map_err(|e| io_error(e, "pwrite"))?;
                    }
                }
                file.write_all_at(buf, pos)
                    .map_err(|e| io_error(e, "pwrite"))?;
                if !range.is_empty() && state.present.insert_range(range) > 0 {
                    state.dirty = true;
                }
                // Guards drop here, before the completion callback runs.
            }
            c.complete(buffer.len() as i32);
            Ok(c)
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
            {
                let file = self.entry.file.write().unwrap();
                barrier_sync(&file, sync_type).map_err(|e| io_error(e, "sync"))?;
                let mut state = self.entry.state.write().unwrap();
                persist_presence(&self.entry, &file, &mut state, sync_type)?;
                // Guards drop here, before the completion callback runs.
            }
            c.complete(0);
            Ok(c)
        }

        #[instrument(err, skip_all, level = Level::TRACE)]
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.reject_if_read_only("truncate")?;
            {
                let file = self.entry.file.write().unwrap();
                file.set_len(len).map_err(|e| io_error(e, "truncate"))?;
                let mut state = self.entry.state.write().unwrap();
                // Granules wholly beyond the new end are gone; the boundary
                // granule keeps its bit (its in-range prefix still holds
                // data, and the extension reads back as zeros).
                let first_gone = len.div_ceil(GRANULE);
                if state.present.remove_range(first_gone..=u64::MAX) > 0 {
                    state.dirty = true;
                }
                // Guards drop here, before the completion callback runs.
            }
            c.complete(0);
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            let file = self.entry.file.read().unwrap();
            Ok(file.metadata().map_err(|e| io_error(e, "metadata"))?.len())
        }

        /// Whether `[pos, pos + len)` is entirely absent, per the trait
        /// contract ("if there is a single byte which is allocated within a
        /// given range - method must return false") and matching `MemoryIO`:
        /// any overlapping present granule means data. Takes the file lock so
        /// the answer is atomic with a concurrent `pwrite`'s data-plus-bits
        /// installation, as it is for the Linux and memory backends.
        fn has_hole(&self, pos: usize, len: usize) -> turso_core::Result<bool> {
            if len == 0 {
                return Ok(true);
            }
            let range = overlapping(pos as u64, len as u64)?;
            let _file = self.entry.file.read().unwrap();
            let state = self.entry.state.read().unwrap();
            // Present-count within [start, end) via rank difference.
            let below_end = state.present.rank(range.end - 1);
            let below_start = match range.start.checked_sub(1) {
                Some(prev) => state.present.rank(prev),
                None => 0,
            };
            let hole = below_end == below_start;
            turso_assert_sometimes!(hole, "sparse bitmap: probed range is absent");
            turso_assert_sometimes!(!hole, "sparse bitmap: probed range is present");
            Ok(hole)
        }

        fn punch_hole(&self, pos: usize, len: usize) -> turso_core::Result<()> {
            self.reject_if_read_only("punch_hole")?;
            turso_assert!(
                pos as u64 % GRANULE == 0 && len as u64 % GRANULE == 0,
                "hole must be granule aligned"
            );
            let range = overlapping(pos as u64, len as u64)?;
            let file = self.entry.file.write().unwrap();
            let mut state = self.entry.state.write().unwrap();
            if !range.is_empty() && state.present.remove_range(range) > 0 {
                state.dirty = true;
            }
            // The cleared bits must be durable before the bytes are released:
            // sync the data file (persist ordering invariant), then the
            // sidecar. A crash in between leaves extra "absent" bits — safe,
            // because evicted pages are re-fetchable cache by definition, and
            // stale physical bytes cannot resurface: a later write into an
            // absent granule zero-fills its uncovered remainder.
            barrier_sync(&file, FileSyncType::Fsync).map_err(|e| io_error(e, "sync"))?;
            persist_presence(&self.entry, &file, &mut state, FileSyncType::Fsync)?;
            drop(state);

            if let Err(e) = reclaim_range(&file, pos as u64, len as u64) {
                tracing::warn!("failed to reclaim punched range ({pos}, {len}): {e}");
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_core::{Buffer, Completion, OpenFlags, IO};

    use crate::sparse_io::SparseBitmapIo;

    const G: u64 = 4096;

    fn truncate(file: &Arc<dyn turso_core::File>, len: u64) {
        #[expect(clippy::let_underscore_future)]
        let _ = file.truncate(len, Completion::new_trunc(|_| {})).unwrap();
    }

    fn write(file: &Arc<dyn turso_core::File>, pos: u64, len: usize, fill: u8) {
        let buffer = Arc::new(Buffer::new_temporary(len));
        buffer.as_mut_slice().fill(fill);
        #[expect(clippy::let_underscore_future)]
        let _ = file
            .pwrite(pos, buffer, Completion::new_write(|_| {}))
            .unwrap();
    }

    fn sync(file: &Arc<dyn turso_core::File>, sync_type: turso_core::io::FileSyncType) {
        #[expect(clippy::let_underscore_future)]
        let _ = file.sync(Completion::new_sync(|_| {}), sync_type).unwrap();
    }

    fn fsync(file: &Arc<dyn turso_core::File>) {
        sync(file, turso_core::io::FileSyncType::Fsync);
    }

    fn tracked_io_and_path() -> (SparseBitmapIo, tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.db").to_str().unwrap().to_string();
        let io = SparseBitmapIo::new(&path).unwrap();
        (io, dir, path)
    }

    /// The `has_hole`/`punch_hole` contract every sparse backend must satisfy.
    fn check_sparse_semantics(io: &dyn IO, path: &str) {
        let file = io.open_file(path, OpenFlags::default(), false).unwrap();
        truncate(&file, 1024 * 1024);
        assert!(file.has_hole(0, 4096).unwrap());

        write(&file, 0, 4096, 1);
        assert!(!file.has_hole(0, 4096).unwrap());

        assert!(file.has_hole(4096, 4096).unwrap());
        assert!(file.has_hole(4096 * 2, 4096).unwrap());

        write(&file, 4096 * 2, 4096, 1);
        assert!(file.has_hole(4096, 4096).unwrap());
        assert!(!file.has_hole(4096 * 2, 4096).unwrap());

        assert!(!file.has_hole(4096, 4097).unwrap());

        file.punch_hole(2 * 4096, 4096).unwrap();
        assert!(file.has_hole(4096 * 2, 4096).unwrap());
        assert!(file.has_hole(4096, 4097).unwrap());
    }

    #[cfg(target_os = "linux")]
    #[test]
    pub fn sparse_io_test() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.into_temp_path();
        let io = crate::sparse_io::SparseLinuxIo::new().unwrap();
        check_sparse_semantics(&io, tmp_path.as_os_str().to_str().unwrap());
    }

    #[test]
    pub fn sparse_bitmap_io_test() {
        let (io, _dir, path) = tracked_io_and_path();
        check_sparse_semantics(&io, &path);
    }

    /// Sub-granule writes must mark their granule present, per the trait
    /// contract ("a single allocated byte within a given range" ⇒ `false`)
    /// and matching Linux/MemoryIO allocation semantics.
    #[test]
    pub fn sparse_bitmap_partial_write_marks_presence() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 1, 1, 7);
        assert!(!file.has_hole(0, G as usize).unwrap());
        // A write straddling a granule boundary marks both granules.
        write(&file, 2 * G - 1, 2, 7);
        assert!(!file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
        assert!(file.has_hole(3 * G as usize, G as usize).unwrap());
    }

    /// A write into an absent granule must not expose stale physical bytes
    /// (e.g. a punched range whose reclamation did not survive): the
    /// uncovered remainder of the granule reads as zeros, like a fresh
    /// `MemoryIO` page.
    #[test]
    pub fn sparse_bitmap_partial_write_zero_fills_absent_granule() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 0, G as usize, 0xAA);
        fsync(&file);
        file.punch_hole(0, G as usize).unwrap();
        // Simulate reclamation not surviving: put stale bytes back on disk
        // behind the backend's back.
        {
            use std::os::unix::fs::FileExt;
            let raw = std::fs::File::options().write(true).open(&path).unwrap();
            raw.write_all_at(&[0xAA; 4096], 0).unwrap();
        }
        // A one-byte write marks the granule present; the other 4095 bytes
        // must read as zeros, not the stale 0xAA.
        write(&file, 7, 1, 0xBB);
        assert!(!file.has_hole(0, G as usize).unwrap());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes[7], 0xBB);
        assert!(bytes[..7].iter().all(|b| *b == 0));
        assert!(bytes[8..G as usize].iter().all(|b| *b == 0));
    }

    /// A zero-length write allocates nothing, matching `MemoryIO`: it must
    /// not zero-fill or mark any granule present.
    #[test]
    pub fn sparse_bitmap_empty_write_is_noop() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 7, 0, 0);
        assert!(file.has_hole(0, G as usize).unwrap());
    }

    /// Alias spellings of the tracked file are rejected: presence tracking
    /// is keyed to the configured spelling, so a symlink or hard link that
    /// bypasses it must error rather than silently splitting state.
    #[test]
    pub fn sparse_bitmap_alias_spellings_are_rejected() {
        let (io, dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            truncate(&file, 16 * G);
            write(&file, 0, G as usize, 1);
            fsync(&file);
        }
        let symlink = dir.path().join("symlink.db").to_str().unwrap().to_string();
        std::os::unix::fs::symlink(&path, &symlink).unwrap();
        assert!(io.open_file(&symlink, OpenFlags::default(), false).is_err());
        assert!(io.remove_file(&symlink).is_err());
        let hardlink = dir.path().join("hardlink.db").to_str().unwrap().to_string();
        std::fs::hard_link(&path, &hardlink).unwrap();
        assert!(io
            .open_file(&hardlink, OpenFlags::default(), false)
            .is_err());
        assert!(io.remove_file(&hardlink).is_err());
        // The configured spelling keeps working.
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!file.has_hole(0, G as usize).unwrap());
    }

    /// Untracked paths must be plain `PlatformIO` files: full durability
    /// semantics, no sidecar, no presence overhead.
    #[test]
    pub fn sparse_bitmap_untracked_paths_have_no_sidecar() {
        let (io, dir, _path) = tracked_io_and_path();
        let other = dir.path().join("other-wal").to_str().unwrap().to_string();
        let file = io.open_file(&other, OpenFlags::default(), false).unwrap();
        write(&file, 0, 4096, 1);
        fsync(&file);
        drop(file);
        assert!(!std::path::Path::new(&format!("{other}.present")).exists());
    }

    /// All handles to the tracked path share presence state — across
    /// separate `SparseBitmapIo` instances too: a punch through one handle
    /// must not be resurrected by a sync through another.
    #[test]
    pub fn sparse_bitmap_handles_share_state() {
        let (io, _dir, path) = tracked_io_and_path();
        let io2 = SparseBitmapIo::new(&path).unwrap();
        let a = io.open_file(&path, OpenFlags::default(), false).unwrap();
        let b = io2.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&a, 16 * G);
        write(&a, 0, G as usize, 1);
        write(&a, G, G as usize, 1);
        fsync(&a);
        a.punch_hole(0, G as usize).unwrap();
        // The second instance's handle sees the punch immediately…
        assert!(b.has_hole(0, G as usize).unwrap());
        write(&b, 2 * G, G as usize, 2);
        fsync(&b);
        drop(a);
        drop(b);

        // …and the persisted state is the merged view, not a stale snapshot.
        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(file.has_hole(0, G as usize).unwrap());
        assert!(!file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
    }

    /// Presence must survive reopen exactly as persisted: pages written and
    /// synced stay present, punched pages stay absent.
    #[test]
    pub fn sparse_bitmap_presence_survives_reopen() {
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            truncate(&file, 1024 * 1024);
            for pos in [0, G, 2 * G] {
                write(&file, pos, G as usize, 1);
            }
            file.punch_hole(G as usize, G as usize).unwrap();
            sync(&file, turso_core::io::FileSyncType::FullFsync);
        }

        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!file.has_hole(0, G as usize).unwrap());
        assert!(file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
        assert!(file.has_hole(3 * G as usize, G as usize).unwrap());
    }

    /// Hydration that never triggers an explicit sync must still be present
    /// after a clean close: the last handle's drop flushes the bitmap.
    #[test]
    pub fn sparse_bitmap_drop_flushes_presence() {
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            truncate(&file, 16 * G);
            write(&file, 0, G as usize, 1);
        }
        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!file.has_hole(0, G as usize).unwrap());
        assert!(file.has_hole(G as usize, G as usize).unwrap());
    }

    /// A live handle must not resurrect the sidecar of a removed file, and
    /// removal deletes the sidecar together with the data file.
    #[test]
    pub fn sparse_bitmap_remove_fences_live_handles() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 0, G as usize, 1);
        fsync(&file);
        io.remove_file(&path).unwrap();
        assert!(!std::path::Path::new(&path).exists());
        assert!(!std::path::Path::new(&format!("{path}.present")).exists());
        // Writes keep POSIX unlinked-file semantics; presence must not be
        // persisted again.
        write(&file, G, G as usize, 2);
        fsync(&file);
        drop(file);
        assert!(!std::path::Path::new(&format!("{path}.present")).exists());
    }

    /// A non-empty data file whose sidecar is missing, corrupt, or belongs
    /// to a different file must refuse to open: its bits may guard locally
    /// modified pages, so "all absent" could overwrite local data with older
    /// server pages.
    #[test]
    pub fn sparse_bitmap_anomalies_refuse_to_open() {
        // Missing sidecar.
        let (io, _dir, path) = tracked_io_and_path();
        std::fs::write(&path, vec![1u8; 4096]).unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());

        // Corrupt sidecar (fails header parse).
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, 4096, 1);
            fsync(&file);
        }
        std::fs::write(format!("{path}.present"), b"garbage").unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());

        // Bit rot in a structurally valid payload (fails the checksum).
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, 4096, 1);
            fsync(&file);
        }
        let sidecar = format!("{path}.present");
        let mut bytes = std::fs::read(&sidecar).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&sidecar, bytes).unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());

        // Sidecar from a different file generation: replace the data file.
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, 4096, 1);
            fsync(&file);
        }
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, vec![2u8; 4096]).unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());
    }

    /// A stale sidecar next to a fresh (empty) data file is discarded.
    #[test]
    pub fn sparse_bitmap_fresh_file_discards_stale_sidecar() {
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::Create, false).unwrap();
            truncate(&file, 16 * G);
            write(&file, 0, G as usize, 1);
            fsync(&file);
        }
        // Replace the data file with a new empty one; the old sidecar is stale.
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"").unwrap();
        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(file.has_hole(0, G as usize).unwrap());
    }

    /// Access mode is per handle: a read-only handle may probe presence but
    /// must not mutate it, even while a writable handle to the same file
    /// exists.
    #[test]
    pub fn sparse_bitmap_read_only_is_per_handle() {
        let (io, _dir, path) = tracked_io_and_path();
        let rw = io.open_file(&path, OpenFlags::Create, false).unwrap();
        truncate(&rw, 16 * G);
        write(&rw, 0, G as usize, 1);
        fsync(&rw);

        let ro = io.open_file(&path, OpenFlags::ReadOnly, false).unwrap();
        assert!(!ro.has_hole(0, G as usize).unwrap());
        assert!(ro.has_hole(G as usize, G as usize).unwrap());
        let buffer = Arc::new(Buffer::new_temporary(G as usize));
        assert!(ro.pwrite(0, buffer, Completion::new_write(|_| {})).is_err());
        assert!(ro.truncate(G, Completion::new_trunc(|_| {})).is_err());
        assert!(ro.punch_hole(0, G as usize).is_err());

        // The writable handle is unaffected.
        write(&rw, G, G as usize, 2);
        assert!(!rw.has_hole(G as usize, G as usize).unwrap());
    }

    /// Differential check against `MemoryIO`, whose in-memory presence map is
    /// the reference model for the `has_hole`/`punch_hole` contract: run
    /// random (including unaligned) write and aligned punch sequences against
    /// both backends and require identical `has_hole` answers throughout —
    /// including after the bitmap file is synced, dropped, and reopened.
    #[test]
    pub fn sparse_bitmap_matches_memory_io() {
        use rand::{Rng, SeedableRng};

        const GRANULES: u64 = 64;

        for seed in 0..16u64 {
            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

            let (io, _dir, path) = tracked_io_and_path();
            let mut bitmap_file = io.open_file(&path, OpenFlags::default(), false).unwrap();

            let memory_io = turso_core::MemoryIO::new();
            let oracle_file = memory_io
                .open_file("oracle.db", OpenFlags::Create, false)
                .unwrap();

            truncate(&bitmap_file, GRANULES * G);
            truncate(&oracle_file, GRANULES * G);

            let check_all = |bitmap: &Arc<dyn turso_core::File>,
                             oracle: &Arc<dyn turso_core::File>,
                             step: usize| {
                for granule in 0..GRANULES {
                    for (pos, len) in [
                        (granule * G, G),
                        (granule * G, 2 * G),
                        (granule * G + 1, G / 2),
                        (granule * G + G - 1, 2),
                    ] {
                        let (pos, len) = (pos as usize, len as usize);
                        assert_eq!(
                            bitmap.has_hole(pos, len).unwrap(),
                            oracle.has_hole(pos, len).unwrap(),
                            "seed={seed} step={step} pos={pos} len={len}"
                        );
                    }
                }
            };

            for step in 0..96 {
                let granule = rng.random_range(0..GRANULES);
                let count = rng.random_range(1..=4).min(GRANULES - granule);
                if rng.random_bool(0.7) {
                    // Writes may be unaligned and sub-granule.
                    let jitter = rng.random_range(0..G);
                    let pos = (granule * G + jitter).min(GRANULES * G - 1);
                    let max_len = (count * G - jitter).max(1);
                    let len = rng.random_range(1..=max_len).min(GRANULES * G - pos);
                    write(&bitmap_file, pos, len as usize, step as u8);
                    write(&oracle_file, pos, len as usize, step as u8);
                } else {
                    // Punches stay granule-aligned per the punch contract.
                    let (pos, len) = ((granule * G) as usize, (count * G) as usize);
                    bitmap_file.punch_hole(pos, len).unwrap();
                    oracle_file.punch_hole(pos, len).unwrap();
                }
                check_all(&bitmap_file, &oracle_file, step);
            }

            // The persisted bitmap must reproduce the oracle after reopen.
            fsync(&bitmap_file);
            drop(bitmap_file);
            let io = SparseBitmapIo::new(&path).unwrap();
            bitmap_file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            check_all(&bitmap_file, &oracle_file, usize::MAX);
        }
    }
}
