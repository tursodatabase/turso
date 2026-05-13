use crate::io::FileSyncType;
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::turso_assert;
use std::fmt::Debug;

pub mod logical_log;
use crate::mvcc::database::{LogRecord, RowVersion};
use crate::mvcc::persistent_storage::logical_log::{
    serialize_header_entry, serialize_op_entry, LogicalLog, OnSerializationComplete,
    DEFAULT_LOG_CHECKPOINT_THRESHOLD,
};
use crate::{CheckpointResult, Completion, File, LimboError, Result};

pub trait DurableStorage: Send + Sync + Debug {
    /// Append one row-version op to `log_record`'s payload buffer, in the
    /// on-disk wire format used by the logical log. Updates `op_count`.
    ///
    /// Called incrementally during `BuildLogRecord` so the committing
    /// transaction can stream bytes into the log record without first
    /// collecting a `Vec<RowVersion>` intermediate.
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
    ) -> Result<()>;

    /// Append a `DatabaseHeader` op to `log_record`'s payload buffer.
    /// Asserts that no header op has already been appended. Updates
    /// `op_count` and sets `has_header`.
    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()>;

    /// Write a transaction to the logical log without advancing the writer offset.
    ///
    /// `m.buf` is the plaintext payload built incrementally via
    /// [`serialize_row_version`] and [`serialize_database_header`]. The
    /// implementation reuses `m.buf` as the on-disk frame buffer (wrapping it
    /// with the TX header, optional chunked encryption, and the trailer), then
    /// hands the same allocation to the I/O layer — no separate framing buffer
    /// is kept alive across calls. On return, `m.buf` is emptied.
    ///
    /// If `on_serialization_complete` is provided, it is called with a zero-copy
    /// reference to the framed bytes and the running CRC after framing but
    /// before the disk write. The callback runs while the internal write lock
    /// is held, so it should be fast (e.g. memcpy to a side buffer).
    fn log_tx(
        &self,
        m: &mut LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)>;

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion>;

    /// Persist the current logical-log header to durable storage.
    ///
    /// This is used by MVCC recovery/checkpoint flows. Keeping this in the trait avoids
    /// reaching into concrete storage internals.
    fn update_header(&self) -> Result<Completion>;

    fn truncate(&self) -> Result<Completion>;
    fn get_logical_log_file(&self) -> Arc<dyn File>;
    fn should_checkpoint(&self) -> bool;
    /// Set the checkpoint threshold in bytes of logical-log data written.
    /// A negative value disables automatic checkpointing.
    fn set_checkpoint_threshold(&self, threshold: i64);
    fn checkpoint_threshold(&self) -> i64;
    fn advance_logical_log_offset_after_success(&self, bytes: u64);
    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32);

    /// Set the in-memory log header from a previously-read on-disk header.
    ///
    /// Called during recovery to seed the CRC state from the header's salt.
    fn set_header(&self, header: logical_log::LogHeader);

    /// Called when a checkpoint begins, before any rows are written to the B-tree.
    /// `durable_txid_max` is the transaction watermark that will be durably persisted
    /// once the checkpoint completes.
    fn on_checkpoint_start(&self, _durable_txid_max: u64) -> Result<()> {
        Ok(())
    }

    /// Called after the checkpoint has fully completed: rows are flushed, WAL is
    /// truncated, and the logical log is reset.
    fn on_checkpoint_end(
        &self,
        _durable_txid_max: u64,
        _result: Result<&CheckpointResult>,
    ) -> Result<()> {
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
    ) -> Result<()> {
        serialize_op_entry(&mut log_record.buf, row_version)?;
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
        serialize_header_entry(&mut log_record.buf, header);
        log_record.has_header = true;
        log_record.op_count = log_record.op_count.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("logical log op_count exceeds u32".to_string())
        })?;
        Ok(())
    }

    fn log_tx(
        &self,
        m: &mut LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.logical_log
            .write()
            .log_tx_deferred_offset(m, on_serialization_complete)
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.logical_log.write().sync(sync_type)
    }

    fn update_header(&self) -> Result<Completion> {
        self.logical_log.write().update_header()
    }

    fn truncate(&self) -> Result<Completion> {
        let c = self.logical_log.write().truncate()?;
        self.shadow_offset_store(0);
        Ok(c)
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.read().file.clone()
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

    fn advance_logical_log_offset_after_success(&self, bytes: u64) {
        self.logical_log.write().advance_offset_after_success(bytes);
        self.shadow_offset_advance(bytes);
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        let mut log = self.logical_log.write();
        log.offset = offset;
        log.running_crc = running_crc;
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
