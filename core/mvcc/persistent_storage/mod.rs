use crate::io::FileSyncType;
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::turso_assert;
use std::fmt::Debug;

#[cfg(test)]
mod discard_pending_tests;
pub mod logical_log;
use crate::mvcc::database::{LogRecord, RowID, RowVersion};
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
        row_id: &RowID,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()>;

    /// Append a `DatabaseHeader` op to `log_record`'s payload buffer.
    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()>;

    /// Write a transaction to the logical log without advancing the writer offset.
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
    ) -> Result<(Completion, u64)>;

    /// If `m` needs a logical-log header upgrade before it can be appended,
    /// start that write and return its completion. While a header write or a
    /// truncate of the log is in flight, return its completion instead.
    /// Callers must wait for this completion and call this again; once it
    /// returns `None`, they call `log_tx`.
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
    /// or left intact ([`LogicalLogTruncateOutcome::Retained`]). Callers wait for
    /// the completion and then call `finish_truncate`.
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)>;

    /// Applies a finished truncate started by `truncate`: if it succeeded, the
    /// log is empty and `logical_log_offset` returns 0; if it failed, the log
    /// keeps its frames and offset.
    fn finish_truncate(&self);

    /// Reset the logical log to a fresh header-only file.
    ///
    /// Used after an external database restore so future MVCC recovery starts
    /// from the restored image instead of replaying stale local log frames.
    fn reset_to_fresh_header(&self) -> Result<Completion>;
    fn get_logical_log_file(&self) -> Arc<dyn File>;
    fn logical_log_offset(&self) -> u64;
    /// The salt of the logical log header. While the log is empty, this is
    /// the salt of the header that the next frame writes.
    fn logical_log_salt(&self) -> Result<u64>;
    fn should_checkpoint(&self) -> bool;
    /// Set the checkpoint threshold in bytes of logical-log data written.
    /// A negative value disables automatic checkpointing.
    fn set_checkpoint_threshold(&self, threshold: i64);
    fn checkpoint_threshold(&self) -> i64;
    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()>;
    #[aristo::intent(
        "the pending running-CRC slot is cleared by the storage abort path after an abandoned deferred-offset write",
        id = "logical_log_pending_crc_cleared_on_abort",
        verify = "full"
    )]
    fn discard_pending_log_write(&self) -> Result<()>;
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

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
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
        Self {
            logical_log: RwLock::new(LogicalLog::new(file, io, encryption_ctx)),
            log_offset: AtomicU64::new(0),
            checkpoint_threshold: AtomicI64::new(DEFAULT_LOG_CHECKPOINT_THRESHOLD),
        }
    }

    /// Runs `f` with the log locked for writing, then copies the log's offset
    /// to the shadow offset. Every write access goes through here because the
    /// offset can change in any of them: a truncate that finished after its
    /// call returned resets the offset to 0 the next time the log is used.
    fn with_log<R>(&self, f: impl FnOnce(&mut LogicalLog) -> R) -> R {
        let mut log = self.logical_log.write();
        let result = f(&mut log);
        self.log_offset.store(log.offset, Ordering::Relaxed);
        result
    }
}

impl DurableStorage for Storage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_id: &RowID,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        LogSerializer::new(&mut log_record.buf).serialize_op_entry(
            row_id,
            row_version,
            portable_extension,
        )?;
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
    ) -> Result<(Completion, u64)> {
        self.with_log(|log| log.log_tx_deferred_offset(m, on_serialization_complete))
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
        self.with_log(|log| log.upgrade_header_for_log_tx(m))
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.with_log(|log| log.sync(sync_type))
    }

    fn update_header(&self) -> Result<Completion> {
        self.with_log(|log| log.update_header())
    }

    #[aristo::intent("after a truncate, once no write is in flight, the in-memory shadow_offset equals the on-disk durable_offset (the tracked end-of-log matches what's been fsync'd)", id = "aristos:logical_log_shadow_offset_matches_durable", verify = "full")]
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)> {
        self.with_log(|log| log.truncate(checkpointed_through_ts))
    }

    fn finish_truncate(&self) {
        self.with_log(|log| log.publish_finished_header_change());
    }

    fn reset_to_fresh_header(&self) -> Result<Completion> {
        self.with_log(|log| log.reset_to_fresh_header())
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.read().file.clone()
    }

    fn logical_log_offset(&self) -> u64 {
        self.log_offset.load(Ordering::Relaxed)
    }

    fn logical_log_salt(&self) -> Result<u64> {
        self.with_log(|log| log.header_salt())
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

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()> {
        self.with_log(|log| log.advance_offset_after_success(bytes));
        Ok(())
    }

    fn discard_pending_log_write(&self) -> Result<()> {
        self.with_log(|log| log.discard_pending_write());
        Ok(())
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.with_log(|log| {
            log.offset = offset;
            log.running_crc = running_crc;
        });
    }

    fn set_header(&self, header: logical_log::LogHeader) {
        self.with_log(|log| log.set_header(header));
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
