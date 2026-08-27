use crate::io::FileSyncType;
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::{Mutex, RwLock};
use crate::turso_assert;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub mod logical_log;
use crate::mvcc::database::{LogRecord, RowVersion};
use crate::mvcc::persistent_storage::logical_log::{
    LogSerializer, LogicalLog, OnSerializationComplete, DEFAULT_LOG_CHECKPOINT_THRESHOLD,
};
use crate::{CheckpointResult, Completion, File, LimboError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalLogTruncateOutcome {
    Truncated,
    Retained,
}

pub trait DurableStorage: Send + Sync + Debug {
    /// Append one row-version op to `log_record`'s payload buffer, in the
    /// on-disk wire format used by the logical log. Updates `op_count`.
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()>;

    /// Append a `DatabaseHeader` op to `log_record`'s payload buffer.
    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()>;

    /// Append a transaction to the logical log. The writer offset and CRC
    /// chain advance before this returns, so the next committer can append
    /// behind this record while its pwrite and fsync are still in flight.
    ///
    /// If `on_serialization_complete` is provided, it is called with shared
    /// ownership of the framed bytes and the frame's
    /// [`logical_log::LogTxFrameInfo`] chain state (start offset, pre-frame
    /// committed CRC, post-frame CRC) after framing but before the disk write.
    /// The callback runs while the internal write lock is held, so it should
    /// be fast.
    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<LogAppend>;

    /// Make the log durable through `target_offset`, sharing one fsync among
    /// every committer waiting for it (group commit).
    ///
    /// Returns `None` once everything up to `target_offset` is on disk.
    /// Returns `Some(completion)` when the caller has to wait: either a group
    /// fsync is already in flight, an earlier append's pwrite has not finished
    /// yet, or this call just submitted the group fsync itself. The caller
    /// parks on the completion and calls this again when it finishes.
    fn log_group_sync(
        &self,
        target_offset: u64,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>>;

    /// True after a log pwrite or group fsync failed. The log's tail can no
    /// longer be trusted, so commits must not report durability from it.
    fn log_is_poisoned(&self) -> bool {
        false
    }

    /// If `m` needs a logical-log header upgrade before it can be appended,
    /// start that write and return its completion. Callers must wait for this
    /// completion and then call `log_tx`.
    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>>;

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion>;

    /// Called after a logical-log write completed successfully, before the
    /// transaction is made visible by advancing the logical-log offset.
    ///
    /// Implementations may return a completion for any additional durability
    /// work that must finish before commit publication.
    fn on_log_write_complete(&self) -> Result<Completion> {
        Ok(Completion::new_yield())
    }

    /// Persist the current logical-log header to durable storage.
    ///
    /// This is used by MVCC recovery/checkpoint flows. Keeping this in the trait avoids
    /// reaching into concrete storage internals.
    fn update_header(&self) -> Result<Completion>;

    /// Truncate the logical log, discarding frames at or below
    /// `checkpointed_through_ts` (the checkpoint's published boundary). Frames
    /// above the boundary (uncheckpointed concurrent commits) are preserved.
    ///
    /// Returns whether the log was actually truncated ([`LogicalLogTruncateOutcome::Truncated`])
    /// or left intact ([`LogicalLogTruncateOutcome::Retained`]).
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)>;

    /// Reset the logical log to a fresh header-only file.
    ///
    /// Used after an external database restore so future MVCC recovery starts
    /// from the restored image instead of replaying stale local log frames.
    fn reset_to_fresh_header(&self) -> Result<Completion>;
    fn get_logical_log_file(&self) -> Arc<dyn File>;
    fn logical_log_offset(&self) -> u64;
    fn should_checkpoint(&self) -> bool;
    /// Set the checkpoint threshold in bytes of logical-log data written.
    /// A negative value disables automatic checkpointing.
    fn set_checkpoint_threshold(&self, threshold: i64);
    fn checkpoint_threshold(&self) -> i64;
    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32);

    /// Set the in-memory log header from a previously-read on-disk header.
    ///
    /// Called during recovery to seed the CRC state from the header's salt.
    fn set_header(&self, header: logical_log::LogHeader);

    /// Called when a checkpoint begins, before any rows are written to the B-tree.
    fn on_checkpoint_start(&self) -> Result<()> {
        Ok(())
    }

    /// Called after the checkpoint has fully completed: rows are flushed, WAL is
    /// truncated, and the logical log is reset.
    ///
    /// Runs while checkpoint locks are still held.
    fn on_checkpoint_end(&self, _result: Result<&CheckpointResult>) -> Result<()> {
        Ok(())
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        None
    }
}

/// One appended log record: its pwrite completion, its size, and the log
/// offset one past its last byte, which is the durability target the
/// committer hands to [`DurableStorage::log_group_sync`].
pub struct LogAppend {
    pub completion: Completion,
    pub bytes: u64,
    pub end_offset: u64,
}

/// Durability bookkeeping shared by every committer appending to one logical
/// log. Appends are serialized (the log's CRC chain requires it), but their
/// pwrites and the fsync are not: committers park here until one fsync covers
/// them all, instead of each holding the commit lock across its own fsync.
#[derive(Debug, Default)]
pub struct GroupCommitState {
    /// Bytes of the log known to be on disk, contiguous from offset 0.
    durable: AtomicU64,
    /// True after a log pwrite or group fsync failed; see
    /// [`DurableStorage::log_is_poisoned`].
    poisoned: AtomicBool,
    /// Bumped when the log is truncated or reset so completions from before
    /// the reset cannot corrupt the new offsets.
    epoch: AtomicU64,
    inner: Mutex<GroupCommitInner>,
}

#[derive(Debug, Default)]
struct GroupCommitInner {
    /// End offset of the last append whose pwrite finished, with no
    /// unfinished append before it. Only bytes below this line are safe to
    /// claim durable after an fsync: an fsync cannot vouch for a write that
    /// had not finished when it was submitted, and claiming past an
    /// unfinished write would let a crash tear a hole below an acked commit.
    written: u64,
    /// Finished pwrite ranges that still have an unfinished append below
    /// them, keyed by start offset.
    finished_above: BTreeMap<u64, u64>,
    /// True while a group fsync is in flight; committers park instead of
    /// piling more fsyncs onto the device.
    sync_in_flight: bool,
    /// Committers parked with the durability target each one waits for.
    /// Waking is selective: waking everyone on every append completion made
    /// 32 parked committers re-check and re-park thousands of times a
    /// second, and the wakes plus the re-checks burned more CPU than the
    /// commits themselves.
    waiters: Vec<(u64, Completion)>,
}

impl GroupCommitState {
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn durable_offset(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Called from an append's pwrite completion. Advances the contiguous
    /// written line and, when no fsync is in flight, wakes the committers
    /// the line now covers so one of them starts the next group fsync.
    /// While an fsync IS in flight nobody is woken: its completion is the
    /// next event, and it does the waking.
    pub fn append_finished(&self, epoch: u64, start: u64, end: u64) {
        let mut inner = self.inner.lock();
        if self.epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        if inner.written == start {
            inner.written = end;
            while let Some((&next_start, &next_end)) = inner.finished_above.first_key_value() {
                if next_start != inner.written {
                    break;
                }
                inner.finished_above.remove(&next_start);
                inner.written = next_end;
            }
        } else {
            inner.finished_above.insert(start, end);
        }
        if !inner.sync_in_flight {
            Self::wake_leader_candidates(&mut inner);
        }
    }

    /// Called from the group fsync's completion: everything written before
    /// the fsync was submitted is now on disk. `epoch` is from submission
    /// time, so a completion straddling a log reset cannot claim durability
    /// for the new log. Wakes the committers the fsync covered, plus the
    /// ones that can lead the next fsync.
    fn sync_finished(&self, epoch: u64, covers: u64) {
        let mut inner = self.inner.lock();
        if self.epoch.load(Ordering::Acquire) == epoch {
            self.durable.fetch_max(covers, Ordering::AcqRel);
        }
        inner.sync_in_flight = false;
        let durable = self.durable.load(Ordering::Acquire);
        inner.waiters.retain(|(target, waiter)| {
            if *target <= durable {
                waiter.complete(0);
                false
            } else {
                true
            }
        });
        Self::wake_leader_candidates(&mut inner);
    }

    /// Wake every parked committer whose bytes the written line covers: each
    /// can submit the next group fsync, one wins, the rest park again. A
    /// waiter above the written line stays parked; the append completion
    /// that advances the line past it wakes it. Callers hold the lock and
    /// have already checked that no fsync is in flight.
    fn wake_leader_candidates(inner: &mut GroupCommitInner) {
        let written = inner.written;
        inner.waiters.retain(|(target, waiter)| {
            if *target <= written {
                waiter.complete(0);
                false
            } else {
                true
            }
        });
    }

    /// Mark the log tail untrustworthy and wake everyone so they observe it.
    pub fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        let mut inner = self.inner.lock();
        inner.sync_in_flight = false;
        Self::wake_all_waiters(&mut inner);
    }

    /// Forget all progress after a truncate or reset. Stale completions from
    /// before the reset are ignored via the epoch.
    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.durable.store(0, Ordering::Release);
        inner.written = 0;
        inner.finished_above.clear();
        inner.sync_in_flight = false;
        Self::wake_all_waiters(&mut inner);
    }

    fn wake_all_waiters(inner: &mut GroupCommitInner) {
        for (_, waiter) in inner.waiters.drain(..) {
            waiter.complete(0);
        }
    }
}

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
    /// Shared with `logical_log` (whose pwrite completions feed it); see
    /// [`GroupCommitState`].
    group_commit: Arc<GroupCommitState>,
    /// Shadowed from LogicalLog::offset for lock-free should_checkpoint() reads.
    log_offset: AtomicU64,
    checkpoint_threshold: AtomicI64,
}

impl Storage {
    pub fn new(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
    ) -> Self {
        let logical_log = LogicalLog::new(file, io, encryption_ctx);
        let group_commit = logical_log.group_commit();
        Self {
            logical_log: RwLock::new(logical_log),
            group_commit,
            log_offset: AtomicU64::new(0),
            checkpoint_threshold: AtomicI64::new(DEFAULT_LOG_CHECKPOINT_THRESHOLD),
        }
    }

    /// Update the shadow offset to stay in sync with LogicalLog::offset.
    /// Called after any operation that mutates the canonical offset under the write lock.
    #[inline(always)]
    fn shadow_offset_store(&self, value: u64) {
        self.log_offset.store(value, Ordering::Relaxed);
    }

    #[inline(always)]
    fn shadow_offset_advance(&self, bytes: u64) {
        self.log_offset.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl DurableStorage for Storage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        LogSerializer::new(&mut log_record.buf)
            .serialize_op_entry(row_version, portable_extension)?;
        log_record.op_count = log_record.op_count.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("logical log op_count exceeds u32".to_string())
        })?;
        Ok(())
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()> {
        turso_assert!(
            !log_record.has_header,
            "DatabaseHeader op appended more than once to a single LogRecord"
        );
        LogSerializer::new(&mut log_record.buf).serialize_header_entry(header)?;
        log_record.has_header = true;
        log_record.op_count = log_record.op_count.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("logical log op_count exceeds u32".to_string())
        })?;
        Ok(())
    }

    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<LogAppend> {
        if self.group_commit.is_poisoned() {
            return Err(LimboError::InternalError(
                "logical log poisoned by an earlier write or fsync failure".to_string(),
            ));
        }
        let append = self
            .logical_log
            .write()
            .log_tx_append(m, on_serialization_complete)?;
        self.shadow_offset_advance(append.bytes);
        Ok(append)
    }

    fn log_group_sync(
        &self,
        target_offset: u64,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>> {
        let group = &self.group_commit;
        if group.is_poisoned() {
            return Err(LimboError::InternalError(
                "logical log poisoned by an earlier write or fsync failure".to_string(),
            ));
        }
        if group.durable_offset() >= target_offset {
            return Ok(None);
        }
        // Everything below happens under the group lock so a wake between the
        // re-check and the park cannot be missed.
        let (submit_epoch, submit_covers) = {
            let mut inner = group.inner.lock();
            if group.durable_offset() >= target_offset {
                return Ok(None);
            }
            if inner.sync_in_flight || inner.written < target_offset {
                // Either the in-flight fsync's completion or an earlier
                // append's pwrite completion will wake this waiter.
                let waiter = Completion::new_wait();
                inner.waiters.push((target_offset, waiter.clone()));
                return Ok(Some(waiter));
            }
            inner.sync_in_flight = true;
            (group.epoch(), inner.written)
        };
        let group_for_callback = Arc::clone(group);
        let completion = Completion::new_sync(move |result| match result {
            Ok(_) => group_for_callback.sync_finished(submit_epoch, submit_covers),
            Err(err) => {
                tracing::error!("group fsync of logical log failed: {err}");
                group_for_callback.poison();
            }
        });
        match self
            .logical_log
            .write()
            .sync_with_completion(completion, sync_type)
        {
            Ok(c) => Ok(Some(c)),
            Err(err) => {
                // Parked committers would otherwise wait forever on a fsync
                // that never started.
                group.poison();
                Err(err)
            }
        }
    }

    fn log_is_poisoned(&self) -> bool {
        self.group_commit.is_poisoned()
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
        self.logical_log.write().upgrade_header_for_log_tx(m)
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.logical_log.write().sync(sync_type)
    }

    fn update_header(&self) -> Result<Completion> {
        self.logical_log.write().update_header()
    }

    #[aristo::intent("after a truncate, once no write is in flight, the in-memory shadow_offset equals the on-disk durable_offset (the tracked end-of-log matches what's been fsync'd)", id = "aristos:logical_log_shadow_offset_matches_durable", verify = "full")]
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)> {
        let mut log = self.logical_log.write();
        let (c, outcome) = log.truncate(checkpointed_through_ts)?;
        // Shadow the log's actual offset: 0 if it truncated, unchanged if it
        // skipped (uncheckpointed frames remain), so should_checkpoint() stays
        // accurate.
        let new_offset = log.offset;
        drop(log);
        self.shadow_offset_store(new_offset);
        Ok((c, outcome))
    }

    fn reset_to_fresh_header(&self) -> Result<Completion> {
        let c = self.logical_log.write().reset_to_fresh_header()?;
        self.shadow_offset_store(0);
        Ok(c)
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.read().file.clone()
    }

    fn logical_log_offset(&self) -> u64 {
        self.log_offset.load(Ordering::Relaxed)
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.logical_log.read().encryption_ctx().cloned()
    }

    /// Lock-free: reads shadowed atomics only.
    fn should_checkpoint(&self) -> bool {
        let threshold = self.checkpoint_threshold.load(Ordering::Relaxed);
        if threshold < 0 {
            return false;
        }
        self.log_offset.load(Ordering::Relaxed) >= threshold as u64
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.checkpoint_threshold
            .store(threshold, Ordering::Relaxed);
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.checkpoint_threshold.load(Ordering::Relaxed)
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        let mut log = self.logical_log.write();
        log.offset = offset;
        log.running_crc = running_crc;
        // Recovery read these bytes back from disk, so they are written and
        // durable by definition.
        self.group_commit.reset();
        self.group_commit.durable.store(offset, Ordering::Release);
        self.group_commit.inner.lock().written = offset;
        self.shadow_offset_store(offset);
    }

    fn set_header(&self, header: logical_log::LogHeader) {
        self.logical_log.write().set_header(header);
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;

    fn completion_finished(c: &Completion) -> bool {
        c.succeeded()
    }

    #[test]
    fn written_line_advances_only_over_contiguous_finished_appends() {
        let group = GroupCommitState::default();
        let epoch = group.epoch();
        // Appends at [0,10), [10,25), [25,30); the middle one finishes last.
        group.append_finished(epoch, 0, 10);
        assert_eq!(group.inner.lock().written, 10);
        group.append_finished(epoch, 25, 30);
        assert_eq!(
            group.inner.lock().written,
            10,
            "a finished append above an unfinished one must not advance the line"
        );
        group.append_finished(epoch, 10, 25);
        assert_eq!(
            group.inner.lock().written,
            30,
            "filling the gap must advance over everything finished above it"
        );
    }

    #[test]
    fn stale_epoch_completions_are_ignored_after_reset() {
        let group = GroupCommitState::default();
        let old_epoch = group.epoch();
        group.append_finished(old_epoch, 0, 10);
        group.reset();
        group.append_finished(old_epoch, 10, 25);
        assert_eq!(
            group.inner.lock().written,
            0,
            "a completion from before the reset must not move the new log's line"
        );
    }

    #[test]
    fn parked_committers_wake_when_the_group_fsync_finishes() {
        let group = GroupCommitState::default();
        let epoch = group.epoch();
        group.append_finished(epoch, 0, 10);
        group.append_finished(epoch, 10, 20);
        // Someone is fsyncing; a committer for [10,20) has to park.
        {
            let mut inner = group.inner.lock();
            inner.sync_in_flight = true;
        }
        let waiter = Completion::new_wait();
        group.inner.lock().waiters.push((20, waiter.clone()));
        assert!(!completion_finished(&waiter));
        // The fsync was submitted when only [0,10) was written; the waiter's
        // bytes are not durable yet, but it is a leader candidate for the
        // next fsync, so it must wake.
        group.sync_finished(epoch, 10);
        assert!(
            completion_finished(&waiter),
            "sync completion must wake parked committers"
        );
        assert_eq!(group.durable_offset(), 10);
        assert!(!group.inner.lock().sync_in_flight);
    }

    #[test]
    fn poison_wakes_parked_committers() {
        let group = GroupCommitState::default();
        let waiter = Completion::new_wait();
        group.inner.lock().waiters.push((20, waiter.clone()));
        group.poison();
        assert!(completion_finished(&waiter));
        assert!(group.is_poisoned());
    }
}
