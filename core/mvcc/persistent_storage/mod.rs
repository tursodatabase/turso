use crate::io::FileSyncType;
use crate::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::RwLock;
use std::fmt::Debug;

pub mod logical_log;
use crate::mvcc::database::LogRecord;
use crate::mvcc::persistent_storage::logical_log::{LogicalLog, DEFAULT_LOG_CHECKPOINT_THRESHOLD};
use crate::{Completion, File, Result};

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
    /// Shadowed from LogicalLog::offset for lock-free should_checkpoint() reads.
    log_offset: AtomicU64,
    checkpoint_threshold: AtomicI64,
}

impl Storage {
    pub fn new(file: Arc<dyn File>, io: Arc<dyn crate::IO>) -> Self {
        Self {
            logical_log: RwLock::new(LogicalLog::new(file, io)),
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

impl Storage {
    pub fn log_tx(&self, m: &LogRecord) -> Result<(Completion, u64)> {
        self.logical_log.write().log_tx_deferred_offset(m)
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        todo!()
    }

    pub fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.logical_log.write().sync(sync_type)
    }

    pub fn update_header(&self) -> Result<Completion> {
        self.logical_log.write().update_header()
    }

    pub fn truncate(&self) -> Result<Completion> {
        let c = self.logical_log.write().truncate()?;
        self.shadow_offset_store(0);
        Ok(c)
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.write().file.clone()
    }

    /// Lock-free: reads shadowed atomics only.
    pub fn should_checkpoint(&self) -> bool {
        let threshold = self.checkpoint_threshold.load(Ordering::Relaxed);
        if threshold < 0 {
            return false;
        }
        self.log_offset.load(Ordering::Relaxed) >= threshold as u64
    }

    pub fn set_checkpoint_threshold(&self, threshold: i64) {
        self.checkpoint_threshold
            .store(threshold, Ordering::Relaxed);
    }

    pub fn checkpoint_threshold(&self) -> i64 {
        self.checkpoint_threshold.load(Ordering::Relaxed)
    }

    pub fn advance_logical_log_offset_after_success(&self, bytes: u64) {
        self.logical_log.write().advance_offset_after_success(bytes);
        self.shadow_offset_advance(bytes);
    }

    pub fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        let mut log = self.logical_log.write();
        log.offset = offset;
        log.running_crc = running_crc;
        self.shadow_offset_store(offset);
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
