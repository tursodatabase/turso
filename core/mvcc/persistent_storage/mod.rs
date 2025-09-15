use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;

mod logical_log;
use crate::mvcc::database::LogRecord;
use crate::mvcc::persistent_storage::logical_log::LogicalLog;
use crate::types::IOResult;
use crate::{File, LimboError, Result};

pub enum Storage {
    Noop,
    LogicalLog { logical_log: RefCell<LogicalLog> },
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }

    pub fn new_logical_log(file: Arc<dyn File>) -> Self {
        Self::LogicalLog {
            logical_log: RefCell::new(LogicalLog::new(file)),
        }
    }
}

impl Storage {
    pub fn log_tx(&self, m: &LogRecord) -> Result<IOResult<()>> {
        match self {
            Self::Noop => Ok(IOResult::Done(())),
            Self::LogicalLog { logical_log } => logical_log.borrow_mut().log_tx(m),
        }
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        match self {
            Self::Noop => Err(LimboError::InternalError(
                "cannot read from Noop storage".to_string(),
            )),
            Self::LogicalLog { logical_log } => todo!(),
        }
    }

    pub fn is_logical_log(&self) -> bool {
        matches!(self, Self::LogicalLog { .. })
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "Noop"),
            Self::LogicalLog { logical_log: _ } => write!(f, "LogicalLog {{ logical_log }}"),
        }
    }
}
