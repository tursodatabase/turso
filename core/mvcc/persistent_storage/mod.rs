use std::fmt::Debug;
use std::sync::Arc;

use crate::mvcc::database::LogRecord;
use crate::{File, LimboError, Result};

pub enum Storage {
    Noop,
    LogicalLog { file: Arc<dyn File> },
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }

    pub fn new_logical_log(file: Arc<dyn File>) -> Self {
        Self::LogicalLog { file }
    }
}

impl Storage {
    pub fn log_tx(&self, _m: LogRecord) -> Result<()> {
        match self {
            Self::Noop => (),
            Self::LogicalLog { file } => {
                todo!()
            }
        }
        Ok(())
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        match self {
            Self::Noop => Err(LimboError::InternalError(
                "cannot read from Noop storage".to_string(),
            )),
            Self::LogicalLog { file } => todo!(),
        }
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "Noop"),
            Self::LogicalLog { file: _ } => write!(f, "LogicalLog {{ file }}"),
        }
    }
}
