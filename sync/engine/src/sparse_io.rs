//! Persistent sparse-file IO for partial sync.
//!
//! Linux derives page presence from `SEEK_DATA` and reclaims space with
//! `PUNCH_HOLE`. [`SparseBitmapIo`] uses an explicit sidecar on filesystems
//! where allocation cannot represent page presence reliably. It tracks only
//! the configured database path, rejects aliases, and delegates other files.
//!
//! Data is durable before new presence bits; cleared bits are durable before
//! space reclamation. A bitmap may under-claim until sync or clean close:
//! fetched pages are re-fetched, while local checkpoints remain WAL-protected.
//! Cold non-empty opens reject missing, corrupt, or mismatched metadata.

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

    // Partial-sync transport and `MemoryIO` both track `PAGE_SIZE` chunks.
    const GRANULE: u64 = crate::database_sync_operations::PAGE_SIZE as u64;

    const SIDECAR_SUFFIX: &str = ".present";
    const ANCHOR_SUFFIX: &str = ".anchor";
    const SIDECAR_MAGIC: &[u8; 4] = b"TPRB";
    const SIDECAR_VERSION: u8 = 4;
    const SIDECAR_HEADER_LEN: usize = 4 + 1 + 4 + 8 + 8 + 4;
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    #[cfg(test)]
    struct OpenMetadataPause {
        path: String,
        reached: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    }

    #[cfg(test)]
    fn open_metadata_pause() -> &'static Mutex<Option<OpenMetadataPause>> {
        static PAUSE: OnceLock<Mutex<Option<OpenMetadataPause>>> = OnceLock::new();
        PAUSE.get_or_init(|| Mutex::new(None))
    }

    #[cfg(test)]
    pub(super) fn pause_next_open_after_metadata(
        path: String,
        reached: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    ) {
        let previous = open_metadata_pause()
            .lock()
            .unwrap()
            .replace(OpenMetadataPause {
                path,
                reached,
                resume,
            });
        assert!(
            previous.is_none(),
            "an open metadata pause is already installed"
        );
    }

    #[cfg(test)]
    fn maybe_pause_after_open_metadata(path: &str) {
        let pause = {
            let mut slot = open_metadata_pause().lock().unwrap();
            if slot.as_ref().is_some_and(|pause| pause.path == path) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.wait();
            pause.resume.wait();
        }
    }

    // The anchor pins the database inode while its sidecar exists, preventing
    // `(dev, ino)` reuse from attaching stale presence state to a new file.
    type FileId = (u64, u64);

    // Live handles share state process-wide. Counting them makes final-close
    // persistence serialize with a concurrent reopen under the registry lock.
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

    // The file may not exist yet, so canonicalize only its parent.
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

    /// Presence-tracked IO for one lazily populated database file.
    pub struct SparseBitmapIo {
        tracked_key: std::path::PathBuf,
        // Canonical configured spelling; final-component aliases are rejected.
        tracked_path: String,
        inner: Arc<dyn IO>,
    }

    impl SparseBitmapIo {
        /// The tracked file's parent directory must exist.
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

        // Reject final-component aliases instead of bypassing presence state.
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
            #[cfg(test)]
            maybe_pause_after_open_metadata(path);
            let id = file_id(&meta);

            let mut registry = registry();
            if !path_names_file(&self.tracked_path, id) {
                return Err(io_error(
                    std::io::Error::from(std::io::ErrorKind::NotFound),
                    "open",
                ));
            }
            let sidecar = sidecar_path(&self.tracked_path);
            if let Some(entry) = registry.get(&id).map(|entry| entry.entry.clone()) {
                // Re-read length under the file lock so empty-file setup cannot
                // overwrite a sidecar persisted by a concurrent write.
                let existing_file = entry.file.read().unwrap();
                let current_meta = existing_file
                    .metadata()
                    .map_err(|e| io_error(e, "metadata"))?;
                prepare_presence_metadata(
                    &self.tracked_path,
                    &sidecar,
                    &existing_file,
                    id,
                    current_meta.len(),
                    read_only,
                )?;
            } else {
                if !read_only {
                    sweep_orphan_tmp_files(&sidecar);
                }
                prepare_presence_metadata(
                    &self.tracked_path,
                    &sidecar,
                    &file,
                    id,
                    meta.len(),
                    read_only,
                )?;
            }
            // Re-check under the registry lock so an open racing `remove_file`
            // cannot register an already-unlinked inode.
            let still_named = path_names_file(&self.tracked_path, id);
            if !still_named {
                return Err(io_error(
                    std::io::Error::from(std::io::ErrorKind::NotFound),
                    "open",
                ));
            }
            if let Some(reg_entry) = registry.get_mut(&id) {
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
            // Serialize open, persistence, and removal. Do not remove metadata
            // unless the data unlink succeeds; after that, clean up both paths.
            let mut registry = registry();
            let entry = std::fs::symlink_metadata(path)
                .ok()
                .map(|meta| file_id(&meta))
                .and_then(|id| {
                    registry
                        .get(&id)
                        .map(|reg_entry| (id, reg_entry.entry.clone()))
                })
                .or_else(|| {
                    registry.iter().find_map(|(id, reg_entry)| {
                        (reg_entry.entry.path == self.tracked_path)
                            .then(|| (*id, reg_entry.entry.clone()))
                    })
                });
            let _persist_guard = entry
                .as_ref()
                .map(|(_, entry)| match entry.persist_lock.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                });
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(e, "remove_file")),
            }
            if let Some((id, entry)) = entry.as_ref() {
                entry.defunct.store(true, Ordering::Release);
                registry.remove(id);
            }
            let sidecar = sidecar_path(&self.tracked_path);
            let anchor = anchor_path(&sidecar);
            let mut first_error = None;
            for metadata_path in [&sidecar, &anchor] {
                match std::fs::remove_file(metadata_path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) if first_error.is_none() => {
                        first_error = Some(io_error(e, "remove_file"));
                    }
                    Err(_) => {}
                }
            }
            if let Err(e) = sync_parent_dir(&self.tracked_path, FileSyncType::Fsync) {
                if first_error.is_none() {
                    first_error = Some(completion_error(e, "sync dir after remove"));
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
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

    fn anchor_path(sidecar: &str) -> String {
        format!("{sidecar}{ANCHOR_SUFFIX}")
    }

    fn file_id(meta: &std::fs::Metadata) -> FileId {
        (meta.dev(), meta.ino())
    }

    fn path_names_file(path: &str, id: FileId) -> bool {
        std::fs::symlink_metadata(path)
            .is_ok_and(|meta| meta.file_type().is_file() && file_id(&meta) == id)
    }

    // Writable empty initialization syncs the data inode before publishing its
    // anchor and empty sidecar. A cold non-empty open requires that anchor.
    fn prepare_presence_metadata(
        data_path: &str,
        sidecar: &str,
        data_file: &std::fs::File,
        id: FileId,
        data_len: u64,
        read_only: bool,
    ) -> Result<()> {
        if data_len != 0 {
            return require_matching_anchor(sidecar, id);
        }
        if read_only {
            return Ok(());
        }

        barrier_sync(data_file, FileSyncType::Fsync)
            .map_err(|e| completion_error(e, "sync empty database file"))?;

        let anchor = anchor_path(sidecar);
        if !path_names_file(&anchor, id) {
            let tmp_anchor = format!(
                "{sidecar}.{}.{}.anchor.tmp",
                std::process::id(),
                TMP_SEQ.fetch_add(1, Ordering::Relaxed)
            );
            std::fs::hard_link(data_path, &tmp_anchor)
                .map_err(|e| completion_error(e, "create presence anchor"))?;
            if let Err(e) = std::fs::rename(&tmp_anchor, &anchor) {
                let _ = std::fs::remove_file(&tmp_anchor);
                return Err(completion_error(e, "install presence anchor"));
            }
        }
        require_matching_anchor(sidecar, id)?;
        write_sidecar(
            sidecar,
            id,
            &roaring::RoaringTreemap::new(),
            FileSyncType::Fsync,
        )
    }

    fn require_matching_anchor(sidecar: &str, id: FileId) -> Result<()> {
        let anchor = anchor_path(sidecar);
        match std::fs::symlink_metadata(&anchor) {
            Ok(meta) if meta.file_type().is_file() && file_id(&meta) == id => Ok(()),
            Ok(_) => Err(presence_anomaly(
                sidecar,
                "has an anchor for a different database file",
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(presence_anomaly(
                sidecar,
                "is missing its database-file anchor",
            )),
            Err(e) => Err(io_error(e, "read presence anchor metadata")),
        }
    }

    fn presence_anomaly(path: &str, reason: &str) -> turso_core::LimboError {
        turso_core::LimboError::Corrupt(format!(
            "presence metadata {path} {reason}; refusing to guess page presence (delete the \
             database file and its presence metadata to re-bootstrap)"
        ))
    }

    fn sync_parent_dir(path: &str, sync_type: FileSyncType) -> std::io::Result<()> {
        let dir = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let dir = std::fs::File::open(dir)?;
        barrier_sync(&dir, sync_type)
    }

    // Honor `FullFsync` with `F_FULLFSYNC` on Apple platforms.
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
        anchor_path: String,
        file: RwLock<std::fs::File>,
        state: RwLock<PresenceState>,
        // Serialize persistence with removal. No lock cycle: `remove_file` takes
        // registry → persist_lock but never file/state; sync and punch take
        // file/state → persist_lock but never registry. Final Drop owns the last
        // live handle.
        persist_lock: Mutex<()>,
        fd_writable: AtomicBool,
        // Data handles retain POSIX unlink semantics, but metadata must stay gone.
        defunct: AtomicBool,
    }

    /// A presence-tracked database handle with per-handle access mode.
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
            let present = load_sidecar(&sidecar, meta.len(), meta.dev(), meta.ino(), read_only)?;
            Ok(Self {
                path: path.to_string(),
                anchor_path: anchor_path(&sidecar),
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

    // Persist hydration on final close; a crash may instead cause a safe re-fetch.
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

    // Empty read-only opens do not mutate metadata. Other cold opens require a
    // sidecar bound to this data inode.
    fn load_sidecar(
        path: &str,
        data_len: u64,
        dev: u64,
        ino: u64,
        read_only: bool,
    ) -> Result<roaring::RoaringTreemap> {
        if data_len == 0 && read_only {
            return Ok(roaring::RoaringTreemap::new());
        }
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(presence_anomaly(path, "is missing"));
            }
            Err(e) => return Err(io_error(e, "read presence sidecar")),
        };
        match parse_sidecar(&bytes) {
            Some((sidecar_dev, sidecar_ino, bitmap)) => {
                if sidecar_dev != dev || sidecar_ino != ino {
                    turso_assert_reachable!("presence sidecar belongs to another file generation");
                    return Err(presence_anomaly(
                        path,
                        "belongs to a different file generation",
                    ));
                }
                Ok(bitmap)
            }
            None => {
                turso_assert_reachable!("presence sidecar is corrupt");
                Err(presence_anomaly(
                    path,
                    "is corrupt or has an unknown format",
                ))
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

    // Sweep this sidecar's temp files from this process or dead processes.
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

    // Callers sync data before publishing new bits. Cleared bits are published
    // before physical reclamation, so the durable bitmap never over-claims.
    fn persist_presence(
        entry: &FileEntry,
        file: &std::fs::File,
        state: &mut PresenceState,
        sync_type: FileSyncType,
    ) -> Result<()> {
        if !state.dirty {
            return Ok(());
        }
        let _persist_guard = match entry.persist_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if entry.defunct.load(Ordering::Acquire) {
            return Ok(());
        }
        // Never attach this entry's bitmap to an externally replaced path.
        let fd_meta = file.metadata().map_err(|e| io_error(e, "metadata"))?;
        let id = file_id(&fd_meta);
        if !path_names_file(&entry.path, id) || !path_names_file(&entry.anchor_path, id) {
            return Err(turso_core::LimboError::Corrupt(format!(
                "presence sidecar for {} not persisted: its path or anchor no longer names this file",
                entry.path
            )));
        }
        write_sidecar(&entry.sidecar_path, id, &state.present, sync_type)?;
        state.dirty = false;
        Ok(())
    }

    fn write_sidecar(
        sidecar: &str,
        id: FileId,
        present: &roaring::RoaringTreemap,
        sync_type: FileSyncType,
    ) -> Result<()> {
        // Rename prevents torn replacement; the two barriers make it durable.
        let mut payload = Vec::with_capacity(present.serialized_size());
        present
            .serialize_into(&mut payload)
            .map_err(|e| completion_error(e, "serialize presence sidecar"))?;
        let mut buf = Vec::with_capacity(SIDECAR_HEADER_LEN + payload.len());
        buf.extend_from_slice(SIDECAR_MAGIC);
        buf.push(SIDECAR_VERSION);
        buf.extend_from_slice(&(GRANULE as u32).to_le_bytes());
        buf.extend_from_slice(&id.0.to_le_bytes());
        buf.extend_from_slice(&id.1.to_le_bytes());
        buf.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        buf.extend_from_slice(&payload);

        let tmp_path = format!(
            "{}.{}.{}.tmp",
            sidecar,
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let result = (|| {
            let mut tmp = std::fs::File::create(&tmp_path)
                .map_err(|e| completion_error(e, "create presence sidecar"))?;
            tmp.write_all(&buf)
                .map_err(|e| completion_error(e, "write presence sidecar"))?;
            barrier_sync(&tmp, sync_type)
                .map_err(|e| completion_error(e, "sync presence sidecar"))?;
            std::fs::rename(&tmp_path, sidecar)
                .map_err(|e| completion_error(e, "rename presence sidecar"))?;
            sync_parent_dir(sidecar, sync_type)
                .map_err(|e| completion_error(e, "sync sidecar dir"))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        result
    }

    // Best-effort: the bitmap, not allocation state, is authoritative.
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

    // Best-effort: the bitmap, not allocation state, is authoritative.
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
            };
            c.complete(nr);
            Ok(c)
        }

        #[instrument(skip(self, c, buffer), level = Level::TRACE)]
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.reject_if_read_only("pwrite")?;
            let buf = buffer.as_slice();
            if buf.is_empty() {
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
            }
            c.complete(0);
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            let file = self.entry.file.read().unwrap();
            Ok(file.metadata().map_err(|e| io_error(e, "metadata"))?.len())
        }

        fn has_hole(&self, pos: usize, len: usize) -> turso_core::Result<bool> {
            if len == 0 {
                return Ok(true);
            }
            let range = overlapping(pos as u64, len as u64)?;
            let _file = self.entry.file.read().unwrap();
            let state = self.entry.state.read().unwrap();
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
            // Publish absent bits before reclamation; stale bytes remain hidden.
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
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;

    use turso_core::{Buffer, Completion, OpenFlags, IO};

    use crate::sparse_io::SparseBitmapIo;

    const G: u64 = crate::database_sync_operations::PAGE_SIZE as u64;

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

    #[test]
    pub fn sparse_bitmap_partial_write_marks_presence() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 1, 1, 7);
        assert!(!file.has_hole(0, G as usize).unwrap());
        write(&file, 2 * G - 1, 2, 7);
        assert!(!file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
        assert!(file.has_hole(3 * G as usize, G as usize).unwrap());
    }

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
        write(&file, 7, 1, 0xBB);
        assert!(!file.has_hole(0, G as usize).unwrap());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes[7], 0xBB);
        assert!(bytes[..7].iter().all(|b| *b == 0));
        assert!(bytes[8..G as usize].iter().all(|b| *b == 0));
    }

    #[test]
    pub fn sparse_bitmap_empty_write_is_noop() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 7, 0, 0);
        assert!(file.has_hole(0, G as usize).unwrap());
    }

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
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!file.has_hole(0, G as usize).unwrap());
    }

    #[test]
    pub fn sparse_bitmap_configured_symlink_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.db");
        std::fs::write(&target, b"").unwrap();
        let path = dir.path().join("sparse.db");
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let path = path.to_str().unwrap();
        let io = SparseBitmapIo::new(path).unwrap();

        assert!(io.open_file(path, OpenFlags::default(), false).is_err());
        assert!(!std::path::Path::new(&format!("{path}.present.anchor")).exists());
    }

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
        assert!(b.has_hole(0, G as usize).unwrap());
        write(&b, 2 * G, G as usize, 2);
        fsync(&b);
        drop(a);
        drop(b);

        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(file.has_hole(0, G as usize).unwrap());
        assert!(!file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
    }

    #[test]
    pub fn sparse_bitmap_open_preserves_concurrently_persisted_sidecar() {
        let (io, _dir, path) = tracked_io_and_path();
        let io = Arc::new(io);
        let writer = io.open_file(&path, OpenFlags::default(), false).unwrap();
        let reached = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        crate::sparse_io::bitmap::pause_next_open_after_metadata(
            path.clone(),
            reached.clone(),
            resume.clone(),
        );

        let opener = {
            let io = io.clone();
            let path = path.clone();
            std::thread::spawn(move || io.open_file(&path, OpenFlags::default(), false).unwrap())
        };
        reached.wait();
        write(&writer, 0, G as usize, 1);
        fsync(&writer);
        let sidecar = format!("{path}.present");
        assert!(std::path::Path::new(&sidecar).exists());
        resume.wait();

        let second = opener.join().unwrap();
        assert!(std::path::Path::new(&sidecar).exists());
        drop(second);
        drop(writer);
        let reopened = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!reopened.has_hole(0, G as usize).unwrap());
    }

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

        let data = std::fs::metadata(&path).unwrap();
        let anchor = std::fs::metadata(format!("{path}.present.anchor")).unwrap();
        assert_eq!((anchor.dev(), anchor.ino()), (data.dev(), data.ino()));
        let io = SparseBitmapIo::new(&path).unwrap();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(!file.has_hole(0, G as usize).unwrap());
        assert!(file.has_hole(G as usize, G as usize).unwrap());
        assert!(!file.has_hole(2 * G as usize, G as usize).unwrap());
        assert!(file.has_hole(3 * G as usize, G as usize).unwrap());
    }

    #[test]
    pub fn sparse_bitmap_incomplete_bootstrap_reopens_as_absent() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::Create, false).unwrap();
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");

        let data_meta = std::fs::metadata(&path).unwrap();
        let anchor_meta = std::fs::metadata(&anchor).unwrap();
        assert_eq!(
            (anchor_meta.dev(), anchor_meta.ino()),
            (data_meta.dev(), data_meta.ino())
        );
        let sidecar_bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(&sidecar_bytes[..4], b"TPRB");
        assert_eq!(sidecar_bytes[4], 4);
        assert_eq!(
            u32::from_le_bytes(sidecar_bytes[5..9].try_into().unwrap()),
            G as u32
        );
        assert_eq!(
            u64::from_le_bytes(sidecar_bytes[9..17].try_into().unwrap()),
            data_meta.dev()
        );
        assert_eq!(
            u64::from_le_bytes(sidecar_bytes[17..25].try_into().unwrap()),
            data_meta.ino()
        );
        assert_eq!(
            u32::from_le_bytes(sidecar_bytes[25..29].try_into().unwrap()),
            crc32c::crc32c(&sidecar_bytes[29..])
        );
        let present = roaring::RoaringTreemap::deserialize_from(&sidecar_bytes[29..]).unwrap();
        assert!(present.is_empty());
        drop(file);

        {
            use std::os::unix::fs::FileExt;
            let raw = std::fs::File::options().write(true).open(&path).unwrap();
            raw.write_all_at(&[0xAA; 4096], 0).unwrap();
            raw.sync_all().unwrap();
        }
        assert_eq!(std::fs::read(&sidecar).unwrap(), sidecar_bytes);

        let io = SparseBitmapIo::new(&path).unwrap();
        let reopened = io.open_file(&path, OpenFlags::default(), false).unwrap();
        assert!(reopened.has_hole(0, G as usize).unwrap());
    }

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

    #[test]
    pub fn sparse_bitmap_remove_fences_live_handles() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        truncate(&file, 16 * G);
        write(&file, 0, G as usize, 1);
        fsync(&file);
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");
        io.remove_file(&path).unwrap();
        assert!(!std::path::Path::new(&path).exists());
        assert!(!std::path::Path::new(&sidecar).exists());
        assert!(!std::path::Path::new(&anchor).exists());
        // Writes keep POSIX unlinked-file semantics; presence must not be
        // persisted again.
        write(&file, G, G as usize, 2);
        fsync(&file);
        drop(file);
        assert!(!std::path::Path::new(&sidecar).exists());
        assert!(!std::path::Path::new(&anchor).exists());
    }

    #[test]
    pub fn sparse_bitmap_external_replacement_cannot_receive_old_handle_sidecar() {
        let (io, _dir, path) = tracked_io_and_path();
        let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
        write(&file, 0, G as usize, 1);
        fsync(&file);
        let sidecar = format!("{path}.present");
        let persisted = std::fs::read(&sidecar).unwrap();

        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, vec![2u8; G as usize]).unwrap();
        write(&file, G, G as usize, 3);
        assert!(file
            .sync(
                Completion::new_sync(|_| {}),
                turso_core::io::FileSyncType::Fsync,
            )
            .is_err());
        drop(file);

        assert_eq!(std::fs::read(&sidecar).unwrap(), persisted);
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());
    }

    #[test]
    pub fn sparse_bitmap_remove_finishes_partial_cleanup() {
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, G as usize, 1);
            fsync(&file);
        }
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");
        std::fs::remove_file(&path).unwrap();

        io.remove_file(&path).unwrap();
        assert!(!std::path::Path::new(&sidecar).exists());
        assert!(!std::path::Path::new(&anchor).exists());
        io.remove_file(&path).unwrap();
    }

    #[test]
    pub fn sparse_bitmap_remove_failure_order_is_retryable() {
        // A hard data-path unlink error must leave required metadata intact.
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, G as usize, 1);
            fsync(&file);
        }
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(io.remove_file(&path).is_err());
        assert!(std::path::Path::new(&sidecar).is_file());
        assert!(std::path::Path::new(&anchor).is_file());

        // Failure on the first metadata path must not skip the anchor, and a
        // later call must be able to finish cleanup after the obstacle clears.
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, G as usize, 1);
            fsync(&file);
        }
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");
        std::fs::remove_file(&sidecar).unwrap();
        std::fs::create_dir(&sidecar).unwrap();
        assert!(io.remove_file(&path).is_err());
        assert!(!std::path::Path::new(&path).exists());
        assert!(std::path::Path::new(&sidecar).is_dir());
        assert!(!std::path::Path::new(&anchor).exists());

        std::fs::remove_dir(&sidecar).unwrap();
        std::fs::write(&sidecar, b"residue").unwrap();
        io.remove_file(&path).unwrap();
        assert!(!std::path::Path::new(&sidecar).exists());
    }

    #[test]
    pub fn sparse_bitmap_anomalies_refuse_to_open() {
        // Missing sidecar.
        let (io, _dir, path) = tracked_io_and_path();
        std::fs::write(&path, vec![1u8; 4096]).unwrap();
        std::fs::hard_link(&path, format!("{path}.present.anchor")).unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());

        // Missing anchor.
        let (io, _dir, path) = tracked_io_and_path();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, 4096, 1);
            fsync(&file);
        }
        std::fs::remove_file(format!("{path}.present.anchor")).unwrap();
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

    #[test]
    pub fn sparse_bitmap_anchor_refuses_stale_sidecar_with_matching_stat_identity() {
        let (io, _dir, path) = tracked_io_and_path();
        std::fs::write(&path, b"").unwrap();
        let anchor = format!("{path}.present.anchor");
        std::fs::hard_link(&path, &anchor).unwrap();
        {
            let file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            write(&file, 0, 4096, 1);
            fsync(&file);
        }
        let original = std::fs::metadata(&path).unwrap();
        let anchor_meta = std::fs::symlink_metadata(&anchor).unwrap();
        assert_eq!(
            (anchor_meta.dev(), anchor_meta.ino()),
            (original.dev(), original.ino())
        );

        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, vec![2u8; 4096]).unwrap();
        let replacement = std::fs::metadata(&path).unwrap();
        assert_ne!(
            (replacement.dev(), replacement.ino()),
            (original.dev(), original.ino())
        );

        // Simulate complete reuse of the stat identity serialized by the
        // sidecar. The hard-link anchor remains an independent witness for
        // the original inode.
        let sidecar = format!("{path}.present");
        let mut bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(
            u32::from_le_bytes(bytes[25..29].try_into().unwrap()),
            crc32c::crc32c(&bytes[29..])
        );
        bytes[9..17].copy_from_slice(&replacement.dev().to_le_bytes());
        bytes[17..25].copy_from_slice(&replacement.ino().to_le_bytes());
        std::fs::write(&sidecar, &bytes).unwrap();

        assert_eq!(
            (
                u64::from_le_bytes(bytes[9..17].try_into().unwrap()),
                u64::from_le_bytes(bytes[17..25].try_into().unwrap()),
            ),
            (replacement.dev(), replacement.ino())
        );

        assert!(io.open_file(&path, OpenFlags::default(), false).is_err());

        // The same sidecar is otherwise valid for the replacement: retargeting
        // the anchor is the only change needed for it to open.
        std::fs::remove_file(&anchor).unwrap();
        std::fs::hard_link(&path, &anchor).unwrap();
        assert!(io.open_file(&path, OpenFlags::default(), false).is_ok());
    }

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
        let data = std::fs::metadata(&path).unwrap();
        let anchor = std::fs::metadata(format!("{path}.present.anchor")).unwrap();
        assert_eq!((anchor.dev(), anchor.ino()), (data.dev(), data.ino()));
    }

    #[test]
    pub fn sparse_bitmap_writable_upgrade_initializes_empty_read_only_file() {
        let (io, _dir, path) = tracked_io_and_path();
        std::fs::write(&path, b"").unwrap();
        let read_only = io.open_file(&path, OpenFlags::ReadOnly, false).unwrap();
        assert!(!std::path::Path::new(&format!("{path}.present.anchor")).exists());

        let writable = io.open_file(&path, OpenFlags::default(), false).unwrap();
        let data = std::fs::metadata(&path).unwrap();
        let anchor = std::fs::metadata(format!("{path}.present.anchor")).unwrap();
        assert_eq!((anchor.dev(), anchor.ino()), (data.dev(), data.ino()));

        write(&writable, 0, G as usize, 1);
        fsync(&writable);
        drop(writable);
        drop(read_only);
        assert!(io.open_file(&path, OpenFlags::default(), false).is_ok());
    }

    #[test]
    pub fn sparse_bitmap_empty_read_only_open_preserves_metadata() {
        let (io, _dir, path) = tracked_io_and_path();
        std::fs::write(&path, b"").unwrap();
        let sidecar = format!("{path}.present");
        let anchor = format!("{sidecar}.anchor");
        std::fs::write(&sidecar, b"stale sidecar").unwrap();
        std::fs::write(&anchor, b"stale anchor").unwrap();
        let sidecar_before = std::fs::read(&sidecar).unwrap();
        let anchor_before = std::fs::read(&anchor).unwrap();
        let anchor_id_before = {
            let meta = std::fs::metadata(&anchor).unwrap();
            (meta.dev(), meta.ino())
        };

        let file = io.open_file(&path, OpenFlags::ReadOnly, false).unwrap();
        assert!(file.has_hole(0, G as usize).unwrap());
        drop(file);

        assert_eq!(std::fs::read(&sidecar).unwrap(), sidecar_before);
        assert_eq!(std::fs::read(&anchor).unwrap(), anchor_before);
        let anchor_meta = std::fs::metadata(&anchor).unwrap();
        assert_eq!((anchor_meta.dev(), anchor_meta.ino()), anchor_id_before);
    }

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

        write(&rw, G, G as usize, 2);
        assert!(!rw.has_hole(G as usize, G as usize).unwrap());
    }

    // Compare random operations with `MemoryIO`, including persisted reopen.
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
                    let jitter = rng.random_range(0..G);
                    let pos = (granule * G + jitter).min(GRANULES * G - 1);
                    let max_len = (count * G - jitter).max(1);
                    let len = rng.random_range(1..=max_len).min(GRANULES * G - pos);
                    write(&bitmap_file, pos, len as usize, step as u8);
                    write(&oracle_file, pos, len as usize, step as u8);
                } else {
                    let (pos, len) = ((granule * G) as usize, (count * G) as usize);
                    bitmap_file.punch_hole(pos, len).unwrap();
                    oracle_file.punch_hole(pos, len).unwrap();
                }
                check_all(&bitmap_file, &oracle_file, step);
            }

            fsync(&bitmap_file);
            drop(bitmap_file);
            let io = SparseBitmapIo::new(&path).unwrap();
            bitmap_file = io.open_file(&path, OpenFlags::default(), false).unwrap();
            check_all(&bitmap_file, &oracle_file, usize::MAX);
        }
    }
}
