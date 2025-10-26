use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;

pub mod logical_log;
use crate::mvcc::database::LogRecord;
use crate::mvcc::persistent_storage::logical_log::LogicalLog;
use crate::{Completion, File, Result};

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
}

impl Storage {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {
            logical_log: RwLock::new(LogicalLog::new(file)),
        }
    }
}

impl Storage {
    pub fn log_tx(&self, m: &LogRecord) -> Result<Completion> {
        self.logical_log.write().log_tx(m)
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        todo!()
    }

    pub fn sync(&self) -> Result<Completion> {
        self.logical_log.write().sync()
    }

    pub fn truncate(&self) -> Result<Completion> {
        self.logical_log.write().truncate()
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.write().file.clone()
    }

    pub fn should_checkpoint(&self) -> bool {
        self.logical_log.read().should_checkpoint()
    }

    pub fn set_checkpoint_threshold(&self, threshold: i64) {
        self.logical_log.write().set_checkpoint_threshold(threshold)
    }

    pub fn checkpoint_threshold(&self) -> i64 {
        self.logical_log.read().checkpoint_threshold()
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
