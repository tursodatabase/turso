//! Multi-process WAL wrapper.
//!
//! Wraps a [`WalFile`] and adds inter-process coordination via:
//! - **TSHM** reader slots (readers publish their snapshot frame for checkpoint safety)
//! - **TSHM** writer state (writer publishes max_frame+checkpoint_seq so readers
//!   can skip expensive WAL rescans when nothing changed)
//! - **WriteLockFile** (flock-based exclusive write lock with MVCC refcount)
//!
//! The WAL file format is unchanged — this is purely a coordination layer.

#[cfg(all(unix, target_pointer_width = "64"))]
#[allow(dead_code)]
mod imp {
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};
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
    use crate::sync::RwLock;
    use crate::types::IOResult;
    use crate::{Completion, LimboError};

    /// Multi-process-aware WAL that wraps a [`WalFile`] with inter-process
    /// coordination via TSHM reader slots and flock-based write locking.
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
    }

    impl fmt::Debug for MultiProcessWal {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MultiProcessWal")
                .field("inner", &self.inner)
                .finish()
        }
    }

    impl MultiProcessWal {
        pub fn new(inner: WalFile, tshm: Arc<Tshm>, locking_mode: crate::LockingMode) -> Self {
            Self {
                inner,
                tshm,
                locking_mode,
                reader_slot: RwLock::new(None),
                cached_writer_state: AtomicU64::new(0),
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
        /// - `mvcc_refresh_if_db_changed`: swallows errors (bool return type)
        fn maybe_rescan_from_tshm(&self) -> crate::Result<bool> {
            let current_state = self.tshm.writer_state();
            let cached_state = self.cached_writer_state.load(Ordering::Acquire);
            if current_state == cached_state {
                return Ok(false);
            }
            self.inner.rescan_wal_from_disk()?;
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
                tracing::warn!("rescan_wal_from_disk failed in mvcc_refresh: {e}");
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

            match self.inner.begin_write_tx() {
                Ok(()) => {
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
            self.inner.append_frames_vectored(pages, page_sz)
        }

        fn finish_append_frames_commit(&self) -> crate::Result<()> {
            let result = self.inner.finish_append_frames_commit();
            if result.is_ok() {
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
            self.inner.truncate_wal(result, sync_type)
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
