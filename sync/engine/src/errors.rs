#[derive(Debug, thiserror::Error, Clone)]
pub enum Error {
    #[error("database error: {0}")]
    TursoError(#[from] turso_core::LimboError),
    #[error("database tape error: {0}")]
    DatabaseTapeError(String),
    #[error("deserialization error: {0}")]
    JsonDecode(String),
    #[error("database sync engine error: {0}")]
    DatabaseSyncEngineError(String),
    #[error("database sync engine conflict: {0}")]
    DatabaseSyncEngineConflict(String),
    #[error("database sync engine IO error: {0}")]
    IoError(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::JsonDecode(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IoError(e.to_string())
    }
}

#[cfg(test)]
impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::TursoError(turso_core::LimboError::InternalError(value.to_string()))
    }
}
