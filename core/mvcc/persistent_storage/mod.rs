use std::fmt::Debug;

use crate::mvcc::database::LogRecord;
use crate::{Result, TursoError};

#[derive(Debug)]
pub enum Storage {
    Noop,
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }
}

impl Storage {
    pub fn log_tx(&self, _m: LogRecord) -> Result<()> {
        match self {
            Self::Noop => (),
        }
        Ok(())
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        match self {
            Self::Noop => Err(TursoError::InternalError(
                "cannot read from Noop storage".to_string(),
            )),
        }
    }
}
