//! Multi-process WAL wrapper.
//!
//! Wraps a [`WalFile`] and adds inter-process coordination via:
//! - **TSHM** reader slots (readers publish their snapshot frame for checkpoint safety)
//! - **TSHM** writer state (writer publishes max_frame+checkpoint_seq so readers
//!   can skip expensive WAL rescans when nothing changed)
//! - **TSHM** write lock (CAS + fcntl byte-range lock for exclusive write access)
//!
//! The WAL file format is unchanged — this is purely a coordination layer.

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(dead_code)]
mod imp {
    use std::fmt;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;

    use crate::io::{File, FileSyncType};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::IOContext;
    use crate::storage::pager::{PageRef, Pager};
    use crate::storage::sqlite3_ondisk::PageSize;
    use crate::storage::tshm::{SlotHandle, Tshm};
    use crate::storage::wal::{
        CheckpointMode, CheckpointResult, PreparedFrames, RollbackTo, Wal, WalFile,
    };
    use crate::storage::wal_index::WalIndex;
    use crate::sync::Mutex;
    use crate::types::IOResult;
    use crate::{Completion, LimboError};

    /// Multi-process-aware WAL that wraps a [`WalFile`] with inter-process
    /// coordination via TSHM reader slots and CAS + fcntl write locking.
    pub struct MultiProcessWal {
        inner: WalFile,
        tshm: Arc<Tshm>,
        locking_mode: crate::LockingMode,
        /// Slot handle for the current read transaction (if any).
        reader_slot: Mutex<Option<SlotHandle>>,
        /// Cached TSHM writer state. Compared against the live TSHM value
        /// in `begin_read_tx` to decide whether a full WAL rescan is needed.
        /// Packed as [checkpoint_seq:32][max_frame:32].
        cached_writer_state: AtomicU64,
        /// Shared WAL index in mmap'd memory for zero-I/O frame lookups.
        wal_index: Option<Arc<WalIndex>>,
        /// WalIndex generation at the time of our read transaction.
        /// If the WalIndex generation changes (WAL restart/truncate), we fall
        /// back to inner.find_frame to avoid reading stale entries.
        cached_wal_index_generation: AtomicU32,
        /// WalIndex pgno_checksum snapshot from begin_read_tx.
        /// Used to detect hash table corruption between transactions:
        /// if pgno_checksum changed without a TSHM bump, corruption is likely.
        cached_pgno_checksum: AtomicU32,
        /// Max frame from successful spill (append_frames_vectored) that
        /// hasn't been committed to WalIndex yet. Set by append_frames_vectored
        /// on success (no failure), consumed by finish_append_frames_commit.
        /// 0 means no pending spill. This prevents uncommitted spill frames
        /// from being visible to cross-process readers via WalIndex.
        pending_spill_max_frame: AtomicU32,
        /// Cached per-connection min_frame from begin_read_tx. Avoids
        /// a cross-struct atomic load in find_frame (called per page read).
        /// min_frame is stable within a transaction (only changes after
        /// checkpoint), so caching it in begin_read_tx is safe.
        /// Note: max_frame is NOT cached because it changes during writes
        /// (commit_prepared_frames, append_frames_vectored, rollback).
        cached_reader_min_frame: AtomicU32,
        /// Cached process ID (avoid syscall on macOS per begin_read_tx).
        pid: u32,
    }

    impl fmt::Debug for MultiProcessWal {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MultiProcessWal")
                .field("inner", &self.inner)
                .finish()
        }
    }

    impl MultiProcessWal {
        pub fn new(
            inner: WalFile,
            tshm: Arc<Tshm>,
            locking_mode: crate::LockingMode,
            wal_index: Option<Arc<WalIndex>>,
        ) -> Self {
            Self {
                inner,
                tshm,
                locking_mode,
                reader_slot: Mutex::new(None),
                cached_writer_state: AtomicU64::new(0),
                wal_index,
                // Sentinel: no generation matches until begin_read_tx validates
                // the WalIndex state. Prevents stale WalIndex data from being
                // used during early reads (e.g. page 1 during database open).
                cached_wal_index_generation: AtomicU32::new(u32::MAX),
                cached_pgno_checksum: AtomicU32::new(0),
                pending_spill_max_frame: AtomicU32::new(0),
                cached_reader_min_frame: AtomicU32::new(0),
                pid: std::process::id(),
            }
        }

        /// Access the inner WalFile (for tests that need to inspect WAL internals).
        pub fn inner(&self) -> &WalFile {
            &self.inner
        }

        /// Publish the current WAL state (checkpoint_seq + max_frame) to the
        /// TSHM header so other processes can detect changes cheaply.
        /// Also updates our local cache so we don't trigger a spurious rescan
        /// on our own next `begin_read_tx`.
        /// Bump the TSHM writer-state counter so readers in other processes
        /// detect that the WAL has changed and trigger a rescan.
        fn publish_writer_state(&self) {
            let new_state = self.tshm.increment_writer_state();
            self.cached_writer_state.store(new_state, Ordering::Release);
        }

        /// Read all header fields from WalIndex and sync the inner WalFile.
        /// Returns true if WalIndex was populated and sync succeeded,
        /// false if WalIndex is empty (max_frame == 0).
        ///
        /// Does NOT call `verify_hash_integrity` — callers that need integrity
        /// verification (write path) should call it explicitly. The read path
        /// relies on the O(1) pgno_checksum comparison in `begin_read_tx`.
        fn sync_inner_from_wal_index(&self, wal_index: &WalIndex) -> crate::Result<bool> {
            wal_index.ensure_segments_mapped()?;
            // Seqlock: snapshot generation before reading header fields.
            let gen_before = wal_index.generation();
            let wi_max_frame = wal_index.max_frame();
            if wi_max_frame == 0 {
                return Ok(false);
            }
            let (wi_s1, wi_s2) = wal_index.salt();
            let wi_page_size = wal_index.page_size();
            let wi_last_checksum = wal_index.last_checksum();
            let wi_checkpoint_seq = wal_index.checkpoint_seq();
            // If generation changed during our reads, a concurrent
            // clear/restart occurred and our values may be torn.
            // Return false to trigger disk fallback.
            if wal_index.generation() != gen_before {
                return Ok(false);
            }
            self.inner.sync_from_wal_index_header(
                wi_max_frame as u64,
                wi_s1,
                wi_s2,
                wi_page_size,
                wi_last_checksum,
                wi_checkpoint_seq,
            );
            Ok(true)
        }

        /// Fall back to WAL file rescan and invalidate the cached WalIndex
        /// generation so find_frame uses inner.find_frame.
        fn rescan_and_invalidate_generation(&self) -> crate::Result<()> {
            self.inner.rescan_wal_from_disk()?;
            self.cached_wal_index_generation
                .store(u32::MAX, Ordering::Release);
            Ok(())
        }

        /// Clear the WalIndex, passing the current WAL salt so readers
        /// don't see a transient zero-salt window.
        fn clear_wal_index(&self, wal_index: &WalIndex) {
            let (s1, s2) = self.inner.get_wal_salt();
            wal_index.clear(s1, s2);
        }

        /// Clear WalIndex and rescan WAL from disk. Used when WalIndex
        /// becomes inconsistent (e.g. append_frame I/O failure).
        /// After a successful rescan, repopulates the WalIndex so readers
        /// can use it immediately instead of falling back to inner.find_frame.
        fn recover_wal_index(&self, wal_index: &WalIndex, context: &str) {
            tracing::warn!("WalIndex {context}, clearing index and rescanning from disk");
            self.clear_wal_index(wal_index);
            match self.inner.rescan_wal_from_disk() {
                Ok(()) => {
                    if let Err(e) = self.inner.populate_wal_index(wal_index) {
                        tracing::warn!("WalIndex repopulation after recovery failed: {e}");
                        // Not fatal — next begin_write_tx will retry.
                        wal_index.cleanup_uncommitted();
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "rescan_wal_from_disk failed during WalIndex recovery: {e}. \
                         WalIndex cleared; readers fall back to disk until next rescan."
                    );
                    // Force next begin_read_tx to attempt a fresh rescan
                    // by invalidating cached state.
                    self.cached_writer_state.store(0, Ordering::Release);
                    self.cached_wal_index_generation
                        .store(u32::MAX, Ordering::Release);
                }
            }
        }

        /// After appending frames to WalIndex, handle failure recovery
        /// or commit. Called by commit_prepared_frames (with commit=true)
        /// and append_frames_vectored (with commit=false).
        ///
        /// When commit=false (spill path), frames are inserted into the hash
        /// table but max_frame is NOT advanced. This prevents cross-process
        /// readers from seeing uncommitted spilled data. The max_frame is
        /// advanced later by finish_append_frames_commit on successful commit,
        /// or the entries are cleaned up by rollback(None) via
        /// cleanup_uncommitted.
        fn finalize_wal_index_append(
            &self,
            wal_index: &WalIndex,
            any_failed: bool,
            new_max_frame: u32,
            commit: bool,
            context: &str,
        ) {
            if any_failed {
                self.recover_wal_index(wal_index, context);
            } else if commit && new_max_frame > 0 {
                wal_index.commit_max_frame(new_max_frame);
            }
            self.sync_wal_index_metadata(wal_index);
        }

        /// Compare the current pgno_checksum against our cached value.
        /// On mismatch, call verify_hash_integrity to detect corruption.
        /// Updates the cached checksum on success.
        ///
        /// The O(max_frame) verify only triggers when pgno_checksum changed,
        /// which requires a cross-process writer commit. Within a process,
        /// TSHM state never diverges so checksum stays constant.
        fn check_pgno_checksum(&self, wal_index: &WalIndex) -> crate::Result<()> {
            let current_cksum = wal_index.pgno_checksum();
            let cached_cksum = self.cached_pgno_checksum.load(Ordering::Acquire);
            if current_cksum != cached_cksum {
                wal_index.verify_hash_integrity()?;
                self.cached_pgno_checksum
                    .store(current_cksum, Ordering::Release);
            }
            Ok(())
        }

        /// Check the TSHM writer state and sync WAL metadata if another
        /// process has modified it since our last check. Returns whether the
        /// TSHM state changed (useful for forcing page cache invalidation even
        /// when the WAL rescan reports no changes, e.g. after Truncate).
        ///
        /// Used by the WRITE path (begin_write_tx). When WalIndex is available
        /// and populated, syncs from mmap instead of reading the WAL file.
        /// Falls back to disk rescan only when WalIndex is empty/unavailable.
        fn maybe_rescan_from_tshm(&self) -> crate::Result<bool> {
            let current_state = self.tshm.writer_state();
            let cached_state = self.cached_writer_state.load(Ordering::Acquire);
            if current_state == cached_state {
                return Ok(false);
            }

            if let Some(wal_index) = &self.wal_index {
                // Write path needs integrity verification before trusting
                // WalIndex header values (max_frame, salt, last_checksum)
                // that will be used for checksum chaining in prepare_frames.
                wal_index.verify_hash_integrity()?;
                if self.sync_inner_from_wal_index(wal_index)? {
                    // Don't invalidate WalIndex generation — it's still valid.
                    self.cached_writer_state
                        .store(current_state, Ordering::Release);
                    return Ok(true);
                }
            }

            // WalIndex empty or unavailable — fall back to disk.
            self.rescan_and_invalidate_generation()?;
            self.cached_writer_state
                .store(current_state, Ordering::Release);
            Ok(true)
        }

        /// Sync WAL state for the READ path using the WalIndex (zero I/O).
        ///
        /// When the WalIndex is populated (max_frame > 0), updates the inner
        /// WalFile's shared state from the mmap'd header — no file I/O at all.
        /// The read path then uses WalIndex for `find_frame` lookups.
        ///
        /// When the WalIndex is empty (max_frame == 0), falls back to a WAL
        /// file rescan. This handles two cases:
        /// - WAL genuinely empty after a Truncate checkpoint (rescan is cheap)
        /// - WalIndex cleared due to an error (rescan discovers actual frames)
        fn sync_from_wal_index_for_read(&self) -> crate::Result<bool> {
            let current_state = self.tshm.writer_state();
            let cached_state = self.cached_writer_state.load(Ordering::Acquire);
            if current_state == cached_state {
                return Ok(false);
            }

            if let Some(wal_index) = &self.wal_index {
                if !self.sync_inner_from_wal_index(wal_index)? {
                    // WalIndex empty — fall back to WAL rescan.
                    self.rescan_and_invalidate_generation()?;
                }
            } else {
                self.inner.rescan_wal_from_disk()?;
            }

            self.cached_writer_state
                .store(current_state, Ordering::Release);
            Ok(true)
        }

        /// Publish external reader constraint from TSHM so that WalFile's
        /// checkpoint and try_restart_log respect cross-process readers.
        /// Excludes our own PID (managed by process-local read_locks).
        fn publish_external_reader_constraint(&self) {
            let min_frame = self
                .tshm
                .min_reader_frame_excluding(self.pid)
                .map(|f| f as u64)
                .unwrap_or(0);
            self.inner.set_external_reader_max_frame(min_frame);
        }

        /// Sync WAL metadata (salt, page_size, last_checksum) from inner
        /// WalFile to WalIndex header. Called by the writer after commits
        /// so that readers can sync from WalIndex without reading the WAL file.
        fn sync_wal_index_metadata(&self, wal_index: &WalIndex) {
            let ps = self.inner.get_wal_page_size();
            if ps > 0 && wal_index.page_size() != ps {
                wal_index.set_page_size(ps);
            }
            let (s1, s2) = self.inner.get_wal_salt();
            let (wi_s1, wi_s2) = wal_index.salt();
            if (s1, s2) != (wi_s1, wi_s2) {
                wal_index.set_salt(s1, s2);
            }
            let (c1, c2) = self.inner.get_last_checksum();
            wal_index.set_last_checksum(c1, c2);
            let ckpt_seq = self.inner.get_checkpoint_seq();
            wal_index.set_checkpoint_seq(ckpt_seq);
        }
    }

    impl Wal for MultiProcessWal {
        fn begin_read_tx(&self) -> crate::Result<bool> {
            let tshm_changed = self.sync_from_wal_index_for_read()?;

            // Claim a TSHM slot BEFORE inner.begin_read_tx() so that
            // concurrent checkpointers in other processes see our presence.
            // Without this, a Truncate checkpoint could scan TSHM between
            // our inner.begin_read_tx() and slot claim, not see us, and
            // truncate the WAL while we still need its frames.
            //
            // Use the shared max_frame as a conservative preliminary value.
            // The actual per-connection max_frame (set by inner.begin_read_tx)
            // may differ in either direction, but both cases are safe for
            // checkpoint bounding (see update comment below).
            let pid = self.pid;
            let preliminary_max = self.inner.get_max_frame_in_wal() as u32;
            let handle = match self.tshm.claim_reader_slot(pid, preliminary_max) {
                Some(h) => h,
                None => return Err(LimboError::Busy),
            };

            let changed = match self.inner.begin_read_tx() {
                Ok(c) => c,
                Err(e) => {
                    self.tshm.release_reader_slot(handle);
                    return Err(e);
                }
            };

            // Update the slot with the actual per-connection max_frame.
            // May differ from preliminary_max in either direction:
            // - After WAL restart/truncate: actual < preliminary (slot was
            //   conservative, blocks checkpoint longer — safe)
            // - After concurrent writer commit: actual > preliminary (slot
            //   briefly allows more checkpoint, but inner read_lock provides
            //   per-process protection — safe)
            let actual_max = self.inner.get_max_frame() as u32;
            if actual_max != preliminary_max {
                self.tshm.update_reader_slot(&handle, pid, actual_max);
            }

            // Cache min_frame for find_frame so it doesn't need to reach
            // into inner (separate struct / cache line) on every page read.
            self.cached_reader_min_frame
                .store(self.inner.get_min_frame() as u32, Ordering::Relaxed);

            // Snapshot the WalIndex generation and pgno_checksum so find_frame
            // can detect stale entries and mid-transaction corruption.
            if let Some(wal_index) = &self.wal_index {
                let wi_max = wal_index.max_frame();
                if wi_max > 0 {
                    // Cache generation so find_frame uses WalIndex.
                    self.cached_wal_index_generation
                        .store(wal_index.generation(), Ordering::Release);
                    // Detect hash table corruption via O(1) pgno_checksum
                    // comparison. Checked every begin_read_tx because
                    // corruption can co-occur with legitimate TSHM bumps.
                    if let Err(e) = self.check_pgno_checksum(wal_index) {
                        // Reset generation so a retry doesn't accidentally
                        // match a stale value from this failed attempt.
                        self.cached_wal_index_generation
                            .store(u32::MAX, Ordering::Release);
                        self.tshm.release_reader_slot(handle);
                        self.inner.end_read_tx();
                        return Err(e);
                    }
                } else {
                    // WalIndex empty (cleared or fresh) — sentinel forces
                    // find_frame to fall through to inner.find_frame.
                    self.cached_wal_index_generation
                        .store(u32::MAX, Ordering::Release);
                }
            }

            // Single mutex acquisition: release any leftover slot (defense-in-depth)
            // and store the new handle.
            {
                let mut slot = self.reader_slot.lock();
                if let Some(old_handle) = slot.take() {
                    debug_assert!(false, "begin_read_tx called without end_read_tx");
                    self.tshm.release_reader_slot(old_handle);
                }
                *slot = Some(handle);
            }

            // If the TSHM writer state changed, always force page cache
            // invalidation. After a Truncate checkpoint the WAL goes back
            // to empty — rescan_wal_from_disk may see identical WAL state
            // (0,0) and skip, but the DB file has been updated by the
            // checkpoint. The TSHM counter change tells us *something*
            // happened, so it's always safe (and necessary) to invalidate.
            Ok(changed || tshm_changed)
        }

        fn mvcc_refresh_if_db_changed(&self) -> crate::Result<bool> {
            // MVCC is incompatible with shared locking modes (enforced at
            // Database::header_validation). This path is unreachable but
            // required by the Wal trait.
            debug_assert!(
                false,
                "mvcc_refresh_if_db_changed called on MultiProcessWal"
            );
            Ok(self.inner.mvcc_refresh_if_db_changed())
        }

        fn begin_write_tx(&self) -> crate::Result<()> {
            // Clear any stale pending spill max_frame from a previous
            // transaction (e.g. if end_write_tx was called without commit
            // or rollback due to an error path).
            self.pending_spill_max_frame.store(0, Ordering::Release);

            // Acquire inter-process write lock (may return Busy).
            // In SharedReads mode, the CAS lock in TSHM provides exclusion:
            // only one process can hold it at a time. If the previous holder
            // crashed, dead-owner detection allows another process to take over.
            self.tshm.write_lock()?;

            // Check if another process modified the WAL since our last rescan.
            // Critical case: another process did a Restart checkpoint (changing
            // the salt) and wrote frames with the new salt. Without this rescan,
            // our WalFileShared still has the old salt, and prepare_frames would
            // embed the old salt in frame headers — making those frames
            // invisible on recovery (salt mismatch with the on-disk WAL header).
            //
            // begin_read_tx also rescans (TSHM-gated), but that may not have
            // run if the caller held an explicit transaction open (BEGIN +
            // SELECT, then INSERT upgrades directly to write without a new
            // begin_read_tx). This check catches that case.
            //
            // TSHM-based detection is safe here because in our implementation
            // the TSHM writer_state is always bumped (via publish_writer_state
            // in begin_write_tx) before the WAL header is flushed to disk (via
            // prepare_wal_start). So any on-disk salt change is always preceded
            // by a TSHM bump that we can observe.
            if let Err(e) = self.maybe_rescan_from_tshm() {
                self.tshm.write_unlock();
                return Err(e);
            }

            // Publish external reader info so that try_restart_log_before_write
            // won't destroy WAL frames that other-process readers still need.
            self.publish_external_reader_constraint();

            // Save checkpoint_seq before begin_write_tx (which may restart the WAL).
            let checkpoint_seq_before = self.inner.get_checkpoint_seq();

            match self.inner.begin_write_tx() {
                Ok(()) => {
                    // Detect WAL restart/truncate: checkpoint_seq changed or WAL
                    // is empty but WalIndex still has stale frames.
                    if let Some(wal_index) = &self.wal_index {
                        let inner_max = self.inner.get_max_frame();
                        if self.inner.get_checkpoint_seq() != checkpoint_seq_before
                            || (inner_max == 0 && wal_index.max_frame() > 0)
                        {
                            self.clear_wal_index(wal_index);
                        }
                        // Bootstrap: WalIndex is empty but WAL has frames.
                        // This happens on first write after open, after a
                        // clear(), or when another process wrote frames that
                        // the WalIndex doesn't know about yet.
                        if wal_index.max_frame() == 0 && inner_max > 0 {
                            if let Err(e) = self.inner.populate_wal_index(wal_index) {
                                // Clean up partially-inserted hash entries to
                                // avoid duplicate insertions on retry.
                                wal_index.cleanup_uncommitted();
                                self.inner.end_write_tx();
                                self.inner.set_external_reader_max_frame(0);
                                self.tshm.write_unlock();
                                return Err(e);
                            }
                        }
                        self.sync_wal_index_metadata(wal_index);
                    }
                    // begin_write_tx may trigger WAL restart (checkpoint_seq
                    // changes, max_frame → 0). Publish so readers see it.
                    self.publish_writer_state();
                    Ok(())
                }
                Err(e) => {
                    // Inner lock failed — release inter-process lock.
                    self.inner.set_external_reader_max_frame(0);
                    self.tshm.write_unlock();
                    Err(e)
                }
            }
        }

        fn end_read_tx(&self) {
            // Release TSHM slot before inner read lock.
            if let Some(handle) = self.reader_slot.lock().take() {
                self.tshm.release_reader_slot(handle);
            }
            self.inner.end_read_tx();
        }

        fn end_write_tx(&self) {
            // No publish_writer_state() here — commit_prepared_frames,
            // finish_append_frames_commit, and begin_write_tx already
            // published the final state. Publishing again causes readers
            // to spuriously enter the sync slow path for no new data.
            self.inner.end_write_tx();
            self.inner.set_external_reader_max_frame(0);
            // In SharedReads mode, keep the write lock permanently held
            // (sticky) so no other process can write. Only release if the
            // refcount is > 1 (header_validation already holds one ref).
            // When a process takes over after the original holder crashed,
            // active_writers() == 1, so we skip the unlock — making the
            // new holder permanent too.
            if self.locking_mode == crate::LockingMode::SharedReads
                && self.tshm.active_writers() <= 1
            {
                return;
            }
            self.tshm.write_unlock();
        }

        fn holds_read_lock(&self) -> bool {
            self.inner.holds_read_lock()
        }

        fn holds_write_lock(&self) -> bool {
            self.inner.holds_write_lock()
        }

        fn find_frame(
            &self,
            page_id: u64,
            frame_watermark: Option<u64>,
        ) -> crate::Result<Option<u64>> {
            // Use the shared WAL index for zero-I/O lookups when populated
            // and from the same generation as our read snapshot. The live
            // generation comparison acts as a safety net: if anything changes
            // the WalIndex during this read transaction (e.g. WAL restart
            // after a TRUNCATE checkpoint where our reader_max equals the
            // WAL max_frame), the mismatch forces fallback to inner.find_frame.
            if let Some(wal_index) = &self.wal_index {
                let wi_gen = wal_index.generation();
                // Relaxed: cached_gen is set by begin_read_tx in the same
                // thread; no cross-thread synchronization needed.
                let cached_gen = self.cached_wal_index_generation.load(Ordering::Relaxed);
                let reader_max = self.inner.get_max_frame() as u32;
                if wi_gen == cached_gen && reader_max > 0 {
                    // Use cached min_frame (set in begin_read_tx, stable
                    // within a transaction) to avoid reaching into inner.
                    let min_frame = self.cached_reader_min_frame.load(Ordering::Relaxed);
                    let max_frame = frame_watermark.unwrap_or(u32::MAX as u64) as u32;
                    let effective_max = max_frame.min(reader_max);
                    return Ok(wal_index
                        .find_frame(page_id as u32, min_frame, effective_max)
                        .map(|f| f as u64));
                }
            }
            // Stale generation or WalIndex unavailable — use inner frame_cache.
            self.inner.find_frame(page_id, frame_watermark)
        }

        fn read_frame(
            &self,
            frame_id: u64,
            page: PageRef,
            buffer_pool: Arc<BufferPool>,
        ) -> crate::Result<Completion> {
            self.inner.read_frame(frame_id, page, buffer_pool)
        }

        fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> crate::Result<Completion> {
            self.inner.read_frame_raw(frame_id, frame)
        }

        fn write_frame_raw(
            &self,
            buffer_pool: Arc<BufferPool>,
            frame_id: u64,
            page_id: u64,
            db_size: u64,
            page: &[u8],
            sync_type: FileSyncType,
        ) -> crate::Result<()> {
            self.inner
                .write_frame_raw(buffer_pool, frame_id, page_id, db_size, page, sync_type)
        }

        fn prepare_wal_start(&self, page_sz: PageSize) -> crate::Result<Option<Completion>> {
            self.inner.prepare_wal_start(page_sz)
        }

        fn prepare_wal_finish(&self, sync_type: FileSyncType) -> crate::Result<Completion> {
            self.inner.prepare_wal_finish(sync_type)
        }

        fn prepare_frames(
            &self,
            pages: &[PageRef],
            page_sz: PageSize,
            db_size_on_commit: Option<u32>,
            prev: Option<&PreparedFrames>,
        ) -> crate::Result<PreparedFrames> {
            self.inner
                .prepare_frames(pages, page_sz, db_size_on_commit, prev)
        }

        fn commit_prepared_frames(&self, prepared: &[PreparedFrames]) {
            self.inner.commit_prepared_frames(prepared);

            // Append committed frames to the shared WalIndex.
            if let Some(wal_index) = &self.wal_index {
                let mut any_failed = false;
                for batch in prepared {
                    for (page, frame_id, _checksum) in &batch.metadata {
                        if wal_index
                            .append_frame(page.get().id as u32, *frame_id as u32)
                            .is_err()
                        {
                            any_failed = true;
                            break;
                        }
                    }
                    if any_failed {
                        break;
                    }
                }
                let new_max = prepared
                    .last()
                    .map(|b| b.final_max_frame as u32)
                    .unwrap_or(0);
                self.finalize_wal_index_append(
                    wal_index,
                    any_failed,
                    new_max,
                    true, // commit: frames are committed
                    "append_frame failed in commit",
                );
            }

            // Publish new max_frame so readers in other processes can see
            // the committed frames without waiting for end_write_tx.
            self.publish_writer_state();
        }

        fn finalize_committed_pages(&self, prepared: &[PreparedFrames]) {
            self.inner.finalize_committed_pages(prepared)
        }

        fn wal_file(&self) -> crate::Result<Arc<dyn File>> {
            self.inner.wal_file()
        }

        fn append_frames_vectored(
            &self,
            pages: Vec<PageRef>,
            page_sz: PageSize,
        ) -> crate::Result<Completion> {
            if let Some(wal_index) = &self.wal_index {
                // Capture page IDs and pre-append max_frame before consuming pages.
                let pre_max = self.inner.get_max_frame() as u32;
                let page_ids: Vec<u32> = pages.iter().map(|p| p.get().id as u32).collect();
                let result = self.inner.append_frames_vectored(pages, page_sz)?;
                let mut any_failed = false;
                for (i, &page_id) in page_ids.iter().enumerate() {
                    let frame_id = match pre_max.checked_add(1 + i as u32) {
                        Some(id) => id,
                        None => {
                            any_failed = true;
                            break;
                        }
                    };
                    if wal_index.append_frame(page_id, frame_id).is_err() {
                        any_failed = true;
                        break;
                    }
                }
                // Do NOT commit max_frame here — spilled frames are
                // uncommitted. commit_max_frame is deferred to
                // finish_append_frames_commit so cross-process readers
                // cannot see uncommitted data. On rollback,
                // cleanup_uncommitted trims entries above the committed
                // max_frame.
                let new_max = self.inner.get_max_frame() as u32;
                self.finalize_wal_index_append(
                    wal_index,
                    any_failed,
                    new_max,
                    false, // spill: do not commit max_frame yet
                    "append_frame failed in vectored spill",
                );
                // Record the pending max_frame for finish_append_frames_commit
                // to commit on successful transaction completion. Reset to 0
                // on failure (recover_wal_index cleared the WalIndex).
                if !any_failed {
                    self.pending_spill_max_frame
                        .store(new_max, Ordering::Release);
                } else {
                    self.pending_spill_max_frame.store(0, Ordering::Release);
                }
                Ok(result)
            } else {
                self.inner.append_frames_vectored(pages, page_sz)
            }
        }

        fn finish_append_frames_commit(&self) -> crate::Result<()> {
            let result = self.inner.finish_append_frames_commit();
            if result.is_ok() {
                // Commit the pending spill max_frame to WalIndex now that the
                // transaction is committed. Only commits if append_frames_vectored
                // recorded a pending value (non-zero) without failure.
                //
                // Guard: commit_prepared_frames may have already advanced
                // WalIndex max_frame beyond the spill value (spill frames 1-N,
                // then commit frames N+1..M calls commit_max_frame(M)).
                // Only advance when pending exceeds the current committed max.
                let pending = self.pending_spill_max_frame.swap(0, Ordering::AcqRel);
                if pending > 0 {
                    if let Some(wal_index) = &self.wal_index {
                        if pending > wal_index.max_frame() {
                            wal_index.commit_max_frame(pending);
                        }
                    }
                }
                self.publish_writer_state();
            }
            result
        }

        fn should_checkpoint(&self) -> bool {
            self.inner.should_checkpoint()
        }

        fn checkpoint(
            &self,
            pager: &Pager,
            mode: CheckpointMode,
        ) -> crate::Result<IOResult<CheckpointResult>> {
            // Rescan the WAL from disk so our frame_cache includes frames
            // written by other processes. Without this, we'd only checkpoint
            // our own frames, and a subsequent WAL restart would lose the
            // uncopied frames. Also clear the page cache so checkpoint doesn't
            // use stale cached pages that don't match the on-disk WAL frames.
            self.inner.rescan_wal_from_disk()?;
            pager.clear_page_cache(false);

            // Publish TSHM min_reader_frame so that WalFile's
            // determine_max_safe_checkpoint_frame() respects cross-process readers.
            self.publish_external_reader_constraint();

            let result = self.inner.checkpoint(pager, mode);

            // Clear after checkpoint completes.
            self.inner.set_external_reader_max_frame(0);

            // Only clear WalIndex on successful checkpoint. On failure after
            // a partial Restart (restart_log succeeded but a later step failed),
            // max_frame_in_wal may be 0 but the WAL frames are still valid.
            // Clearing would create a WalIndex/TSHM inconsistency since
            // publish_writer_state is gated on success below.
            if let Some(wal_index) = &self.wal_index {
                if result.is_ok()
                    && self.inner.get_max_frame_in_wal() == 0
                    && wal_index.max_frame() > 0
                {
                    self.clear_wal_index(wal_index);
                }
                // Sync metadata (salt may have changed after Restart checkpoint).
                self.sync_wal_index_metadata(wal_index);
            }

            // After a Restart checkpoint the WAL header is updated in memory
            // (new checkpoint_seq, new salt) but NOT on disk. The new header
            // is flushed lazily by prepare_wal_start() on the next write,
            // which happens only after the pager has fully synced the DB file.
            // Flushing the header eagerly here would risk data loss: a crash
            // before the DB sync would leave the WAL with a new salt (making
            // old frames invisible) while the DB pages are not durable.
            //
            // Other processes detect the restart via the
            // TSHM writer_state change (publish_writer_state below) and rescan
            // from disk: they see the old header + old frames, which contain
            // data already backfilled to the DB, so reads are correct.

            // Publish so readers in other processes detect the change and
            // trigger a rescan.
            if result.is_ok() {
                self.publish_writer_state();
            }

            result
        }

        fn sync(&self, sync_type: FileSyncType) -> crate::Result<Completion> {
            self.inner.sync(sync_type)
        }

        fn is_syncing(&self) -> bool {
            self.inner.is_syncing()
        }

        fn get_max_frame_in_wal(&self) -> u64 {
            self.inner.get_max_frame_in_wal()
        }

        fn get_checkpoint_seq(&self) -> u32 {
            self.inner.get_checkpoint_seq()
        }

        fn get_max_frame(&self) -> u64 {
            self.inner.get_max_frame()
        }

        fn get_min_frame(&self) -> u64 {
            self.inner.get_min_frame()
        }

        fn rollback(&self, rollback_to: Option<RollbackTo>) {
            if let Some(ref rt) = rollback_to {
                // Savepoint rollback: trim pending spill max_frame to the
                // savepoint frame. Spilled frames before the savepoint are
                // still valid; only post-savepoint spills are discarded.
                // Use fetch_update to atomically clamp (only decrease).
                let savepoint_frame = rt.frame as u32;
                let _ = self.pending_spill_max_frame.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |current| {
                        if current > savepoint_frame {
                            Some(savepoint_frame)
                        } else {
                            None // already at or below savepoint
                        }
                    },
                );
                if let Some(wal_index) = &self.wal_index {
                    // Remove any uncommitted spill entries above WalIndex
                    // committed max_frame. Without this, stale entries from
                    // spills that were rolled back accumulate in the hash
                    // table, wasting slots and lengthening probe chains.
                    wal_index.cleanup_uncommitted();
                    wal_index.rollback_to(rt.frame as u32);
                }
                // Savepoint rollback changes visible WAL state — notify
                // cross-process readers so they re-sync.
                self.publish_writer_state();
            } else {
                // Full rollback: discard all pending spill state.
                self.pending_spill_max_frame.store(0, Ordering::Release);
                // Clean up any uncommitted WalIndex entries (appended via
                // append_frame but not committed via commit_max_frame).
                // Since append_frames_vectored no longer calls commit_max_frame,
                // spilled entries are above the WalIndex max_frame and
                // cleanup_uncommitted correctly removes them.
                if let Some(wal_index) = &self.wal_index {
                    wal_index.cleanup_uncommitted();
                }
                // begin_write_tx published to TSHM. Readers that synced during
                // this write tx may have stale state. Publish so they re-sync
                // on their next begin_read_tx.
                self.publish_writer_state();
            }
            self.inner.rollback(rollback_to)
        }

        fn abort_checkpoint(&self) {
            self.inner.abort_checkpoint()
        }

        fn get_last_checksum(&self) -> (u32, u32) {
            self.inner.get_last_checksum()
        }

        fn changed_pages_after(&self, frame_watermark: u64) -> crate::Result<Vec<u32>> {
            self.inner.changed_pages_after(frame_watermark)
        }

        fn set_io_context(&self, ctx: IOContext) {
            self.inner.set_io_context(ctx)
        }

        fn update_max_frame(&self) {
            self.inner.update_max_frame()
        }

        fn truncate_wal(
            &self,
            result: &mut CheckpointResult,
            sync_type: FileSyncType,
        ) -> crate::Result<IOResult<()>> {
            let r = self.inner.truncate_wal(result, sync_type)?;
            // WAL file was truncated to 0 — clear the WalIndex.
            if let Some(wal_index) = &self.wal_index {
                if wal_index.max_frame() > 0 {
                    self.clear_wal_index(wal_index);
                }
            }
            // Notify other processes that the WAL was truncated so they
            // rescan and don't try to read frames from the empty file.
            self.publish_writer_state();
            Ok(r)
        }

        #[cfg(debug_assertions)]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl Drop for MultiProcessWal {
        fn drop(&mut self) {
            // Release the TSHM reader slot if one is held, so other processes
            // can checkpoint past our snapshot frame.
            if let Some(handle) = self.reader_slot.get_mut().take() {
                self.tshm.release_reader_slot(handle);
            }
            // Clear external reader constraint to prevent a stale value from
            // blocking checkpoint progress if dropped mid-write-transaction.
            self.inner.set_external_reader_max_frame(0);
        }
    }
}

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(unused_imports)]
pub use imp::*;
