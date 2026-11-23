//! `DurableStorage` double that fails `log_tx` with `Busy` once.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use turso_core::io::FileSyncType;
use turso_core::mvcc::database::{LogRecord, RowVersion};
use turso_core::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
use turso_core::mvcc::persistent_storage::DurableStorage;
use turso_core::storage::sqlite3_ondisk::DatabaseHeader;
use turso_core::EncryptionContext;
use turso_core::{CheckpointResult, Completion, File, LimboError, Result};

/// Fails the next `log_tx` with [`LimboError::Busy`].
///
/// `arm_before_busy` runs its hook after the call has started and before
/// the error returns, so a test can drop another statement while this
/// write is in progress.
pub(super) struct BusyOnLogTxStorage {
    inner: Arc<dyn DurableStorage>,
    arm_log_tx_busy: AtomicBool,
    before_busy: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl std::fmt::Debug for BusyOnLogTxStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusyOnLogTxStorage").finish_non_exhaustive()
    }
}

impl BusyOnLogTxStorage {
    pub(super) fn new(inner: Arc<dyn DurableStorage>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            arm_log_tx_busy: AtomicBool::new(false),
            before_busy: Mutex::new(None),
        })
    }

    pub(super) fn arm(&self) {
        self.arm_log_tx_busy.store(true, Ordering::Release);
    }

    pub(super) fn arm_before_busy(&self, hook: impl FnOnce() + Send + 'static) {
        *self.before_busy.lock().unwrap() = Some(Box::new(hook));
        self.arm_log_tx_busy.store(true, Ordering::Release);
    }
}

impl DurableStorage for BusyOnLogTxStorage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_id: &turso_core::mvcc::database::RowID,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        self.inner
            .serialize_row_version(log_record, row_id, row_version, portable_extension)
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()> {
        self.inner.serialize_database_header(log_record, header)
    }

    fn log_tx(
        &self,
        record: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        if self.arm_log_tx_busy.swap(false, Ordering::AcqRel) {
            let hook = {
                let mut slot = self.before_busy.lock().unwrap();
                slot.take()
            };
            if let Some(hook) = hook {
                hook();
            }
            return Err(LimboError::Busy);
        }
        self.inner.log_tx(record, on_serialization_complete)
    }

    fn upgrade_header_for_log_tx(&self, record: &LogRecord) -> Result<Option<Completion>> {
        self.inner.upgrade_header_for_log_tx(record)
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.inner.sync(sync_type)
    }

    fn update_header(&self) -> Result<Completion> {
        self.inner.update_header()
    }

    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(
        Completion,
        turso_core::mvcc::persistent_storage::LogicalLogTruncateOutcome,
    )> {
        self.inner.truncate(checkpointed_through_ts)
    }

    fn finish_truncate(&self) {
        self.inner.finish_truncate()
    }

    fn reset_to_fresh_header(&self) -> Result<Completion> {
        self.inner.reset_to_fresh_header()
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.inner.get_logical_log_file()
    }

    fn logical_log_offset(&self) -> u64 {
        self.inner.logical_log_offset()
    }

    fn should_checkpoint(&self) -> bool {
        self.inner.should_checkpoint()
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.inner.set_checkpoint_threshold(threshold)
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.inner.checkpoint_threshold()
    }

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()> {
        self.inner.advance_logical_log_offset_after_success(bytes)
    }

    fn discard_pending_log_write(&self) -> Result<()> {
        self.inner.discard_pending_log_write()
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.inner
            .restore_logical_log_state_after_recovery(offset, running_crc)
    }

    fn set_header(&self, header: LogHeader) {
        self.inner.set_header(header)
    }

    fn on_checkpoint_start(&self) -> Result<()> {
        self.inner.on_checkpoint_start()
    }

    fn on_checkpoint_end(&self, result: Result<&CheckpointResult>) -> Result<()> {
        self.inner.on_checkpoint_end(result)
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.inner.encryption_ctx()
    }
}
