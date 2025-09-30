use std::fmt::Debug;
use std::sync::{Arc, RwLock};

pub mod logical_log;
use crate::mvcc::database::LogRecord;
use crate::mvcc::persistent_storage::logical_log::LogicalLog;
use crate::types::IOResult;
use crate::{File, Result};

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
    pub fn log_tx(&self, m: &LogRecord) -> Result<IOResult<()>> {
        self.logical_log.write().unwrap().log_tx(m)
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        todo!()
    }

    pub fn sync(&self) -> Result<IOResult<()>> {
        self.logical_log.write().unwrap().sync()
    }

    pub fn truncate(&self) -> Result<IOResult<()>> {
        self.logical_log.write().unwrap().truncate()
    }

    pub fn needs_recover(&self) -> bool {
        self.logical_log.read().unwrap().needs_recover()
    }

    pub fn mark_recovered(&self) {
        self.logical_log.write().unwrap().mark_recovered();
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.write().unwrap().file.clone()
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
