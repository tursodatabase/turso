#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    TursoError(#[from] turso_core::LimboError),
    #[error("database tape error: {0}")]
    DatabaseTapeError(String),
    #[error("deserialization error: {0}")]
    JsonDecode(#[from] serde_json::Error),
    #[error("database sync engine error: {0}")]
    DatabaseSyncEngineError(String),
    #[error("database sync engine conflict: {0}")]
    DatabaseSyncEngineConflict(String),
    #[error("database sync engine IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[cfg(test)]
impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::TursoError(turso_core::LimboError::InternalError(value.to_string()))
    }
}
