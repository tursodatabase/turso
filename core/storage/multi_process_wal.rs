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
    use crate::sync::RwLock;
    use crate::types::IOResult;
    use crate::{Completion, LimboError};

    /// Multi-process-aware WAL that wraps a [`WalFile`] with inter-process
    /// coordination via TSHM reader slots and CAS + fcntl write locking.
    pub struct MultiProcessWal {
        inner: WalFile,
        tshm: Arc<Tshm>,
        locking_mode: crate::LockingMode,
        /// Slot handle for the current read transaction (if any).
        reader_slot: RwLock<Option<SlotHandle>>,
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
                reader_slot: RwLock::new(None),
                cached_writer_state: AtomicU64::new(0),
                wal_index,
                // Sentinel: no generation matches until begin_read_tx validates
                // the WalIndex state. Prevents stale WalIndex data from being
                // used during early reads (e.g. page 1 during database open).
                cached_wal_index_generation: AtomicU32::new(u32::MAX),
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

        /// Check the TSHM writer state and rescan the WAL from disk if another
        /// process has modified it since our last check. Returns whether the
        /// TSHM state changed (useful for forcing page cache invalidation even
        /// when the WAL rescan reports no changes, e.g. after Truncate).
        ///
        /// Three error-handling modes (caller picks the right one):
        /// - `begin_read_tx`: propagates errors via `?`
        /// - `begin_write_tx`: propagates errors (caller releases write lock)
        /// - `refresh_if_db_changed`: swallows errors (bool return type)
        fn maybe_rescan_from_tshm(&self) -> crate::Result<bool> {
            let current_state = self.tshm.writer_state();
            let cached_state = self.cached_writer_state.load(Ordering::Acquire);
            if current_state == cached_state {
                return Ok(false);
            }
            self.inner.rescan_wal_from_disk()?;

            // Invalidate the cached WalIndex generation so find_frame falls
            // through to inner.find_frame until begin_read_tx re-validates.
            // We must NOT mutate the shared WalIndex here (no clear/populate):
            // only the writer process may modify the WalIndex. A reader calling
            // clear() could race with the writer's append, destroying entries.
            self.cached_wal_index_generation
                .store(u32::MAX, Ordering::Release);

            self.cached_writer_state
                .store(current_state, Ordering::Release);
            Ok(true)
        }

        /// Publish external reader constraint from TSHM so that WalFile's
        /// checkpoint and try_restart_log respect cross-process readers.
        /// Excludes our own PID (managed by process-local read_locks).
        fn publish_external_reader_constraint(&self) {
            let my_pid = std::process::id();
            let min_frame = self
                .tshm
                .min_reader_frame_excluding(my_pid)
                .map(|f| f as u64)
                .unwrap_or(0);
            self.inner.set_external_reader_max_frame(min_frame);
        }
    }

    impl Wal for MultiProcessWal {
        fn begin_read_tx(&self) -> crate::Result<bool> {
            let tshm_changed = self.maybe_rescan_from_tshm()?;

            let changed = self.inner.begin_read_tx()?;

            // Snapshot the WalIndex generation so find_frame can detect stale entries.
            if let Some(wal_index) = &self.wal_index {
                self.cached_wal_index_generation
                    .store(wal_index.generation(), Ordering::Release);
            }
            // Claim a TSHM slot so checkpointers in other processes know our
            // snapshot boundary.
            let pid = std::process::id();
            let max_frame_u64 = self.inner.get_max_frame();
            debug_assert!(
                max_frame_u64 <= u32::MAX as u64,
                "WAL max_frame {max_frame_u64} exceeds u32 capacity"
            );
            let max_frame = max_frame_u64 as u32;
            match self.tshm.claim_reader_slot(pid, max_frame) {
                Some(handle) => {
                    *self.reader_slot.write() = Some(handle);
                }
                None => {
                    // All slots full — release the inner read lock and fail.
                    self.inner.end_read_tx();
                    return Err(LimboError::Busy);
                }
            }
            // If the TSHM writer state changed, always force page cache
            // invalidation. After a Truncate checkpoint the WAL goes back
            // to empty — rescan_wal_from_disk may see identical WAL state
            // (0,0) and skip, but the DB file has been updated by the
            // checkpoint. The TSHM counter change tells us *something*
            // happened, so it's always safe (and necessary) to invalidate.
            Ok(changed || tshm_changed)
        }

        fn mvcc_refresh_if_db_changed(&self) -> bool {
            // Check TSHM writer state first — another process may have
            // checkpointed (writing WAL frames then truncating), which
            // would not be visible to the inner WalFile without a rescan.
            let tshm_changed = self.maybe_rescan_from_tshm().unwrap_or_else(|e| {
                tracing::warn!("rescan_wal_from_disk failed in refresh_if_db_changed: {e}");
                // TSHM changed but rescan failed — report changed so the
                // caller invalidates caches. cached_writer_state is NOT
                // updated (maybe_rescan_from_tshm only updates on success),
                // so the next call will retry the rescan.
                true
            });
            // After a Truncate checkpoint the WAL goes back to empty —
            // rescan may see identical WAL state (0,0) and skip, but the
            // DB file has been updated. The TSHM counter change tells us
            // something happened, so always report changed in that case.
            self.inner.mvcc_refresh_if_db_changed() || tshm_changed
        }

        fn begin_write_tx(&self) -> crate::Result<()> {
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
                            wal_index.clear(0, 0);
                        }
                        // Bootstrap: WalIndex is empty but WAL has frames.
                        // This happens on first write after open, after a
                        // clear(), or when another process wrote frames that
                        // the WalIndex doesn't know about yet. Non-fatal if
                        // it fails — find_frame falls through to inner.
                        if wal_index.max_frame() == 0 && inner_max > 0 {
                            if let Err(e) = self.inner.populate_wal_index(wal_index) {
                                tracing::warn!("populate_wal_index failed: {e}");
                            }
                        }
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
            if let Some(handle) = self.reader_slot.write().take() {
                self.tshm.release_reader_slot(handle);
            }
            self.inner.end_read_tx();
        }

        fn end_write_tx(&self) {
            // Publish final state before releasing the write lock so that
            // readers in other processes see the update immediately.
            self.publish_writer_state();
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
            // and from the same generation as our read snapshot.
            if let Some(wal_index) = &self.wal_index {
                let gen = wal_index.generation();
                let cached_gen = self.cached_wal_index_generation.load(Ordering::Acquire);
                if gen == cached_gen && wal_index.max_frame() > 0 {
                    let max_frame = frame_watermark.unwrap_or(u32::MAX as u64) as u32;
                    let min_frame = self.inner.get_min_frame() as u32;
                    // Cap max_frame at the WalIndex's committed max to avoid
                    // probing stale entries beyond what was committed.
                    let effective_max = max_frame.min(wal_index.max_frame());
                    if let Some(frame_id) =
                        wal_index.find_frame(page_id as u32, min_frame, effective_max)
                    {
                        return Ok(Some(frame_id as u64));
                    }
                    // WalIndex miss — fall through to inner.find_frame as a
                    // safety net. If the WalIndex is correct, inner will also
                    // return None (page not in WAL). If the WalIndex has a
                    // false negative (corruption, partial populate), inner
                    // catches it and returns the correct frame, preventing
                    // silent stale reads from the DB file.
                }
            }
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
                if any_failed {
                    // WalIndex is inconsistent — clear it so find_frame falls
                    // through to inner.find_frame for all lookups. The next
                    // begin_write_tx will re-populate it from the inner WAL.
                    tracing::warn!("WalIndex append_frame failed, clearing index");
                    wal_index.clear(0, 0);
                } else if let Some(last_batch) = prepared.last() {
                    wal_index.commit_max_frame(last_batch.final_max_frame as u32);
                }
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
                if any_failed {
                    tracing::warn!("WalIndex append_frame failed in vectored path, clearing index");
                    wal_index.clear(0, 0);
                }
                Ok(result)
            } else {
                self.inner.append_frames_vectored(pages, page_sz)
            }
        }

        fn finish_append_frames_commit(&self) -> crate::Result<()> {
            let result = self.inner.finish_append_frames_commit();
            if result.is_ok() {
                if let Some(wal_index) = &self.wal_index {
                    // Only update max_frame if WalIndex is populated. If
                    // max_frame is 0, the index was cleared by
                    // commit_prepared_frames due to an append failure — don't
                    // restore it or the cleared state would be undone.
                    if wal_index.max_frame() > 0 {
                        wal_index.commit_max_frame(self.inner.get_max_frame() as u32);
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

            // If the checkpoint emptied the WAL (Restart/Truncate), clear the
            // WalIndex so readers don't find stale frame entries.
            if let Some(wal_index) = &self.wal_index {
                if self.inner.get_max_frame_in_wal() == 0 && wal_index.max_frame() > 0 {
                    wal_index.clear(0, 0);
                }
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
                if let Some(wal_index) = &self.wal_index {
                    wal_index.rollback_to(rt.frame as u32);
                }
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
                    wal_index.clear(0, 0);
                }
            }
            Ok(r)
        }

        #[cfg(debug_assertions)]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
}

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(unused_imports)]
pub use imp::*;
