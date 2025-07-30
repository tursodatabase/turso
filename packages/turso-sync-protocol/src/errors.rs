use crate::types::DbSyncStatus;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    TursoError(turso::Error),
    #[error("database tape error: {0}")]
    DatabaseTapeError(String),
    #[error("invalid URI: {0}")]
    Uri(http::uri::InvalidUri),
    #[error("invalid HTTP request: {0}")]
    Http(http::Error),
    #[error("HTTP request error: {0}")]
    HyperRequest(hyper::Error),
    #[error("HTTP response error: {0}")]
    HyperResponse(hyper::Error),
    #[error("unexpected sync server error: code={0}, info={1}")]
    SyncServerError(http::StatusCode, String),
    #[error("deserialization error: {0}")]
    JsonDecode(serde_json::Error),
    #[error("unexpected sync server status: {0:?}")]
    SyncServerUnexpectedStatus(DbSyncStatus),
    #[error("unexpected filesystem error: {0}")]
    FilesystemError(std::io::Error),
    #[error("local metadata error: {0}")]
    MetadataError(String),
    #[error("database sync error: {0}")]
    DatabaseSyncError(String),
    #[error("sync server pull error: checkpoint required: `{0:?}`")]
    PullNeedCheckpoint(DbSyncStatus),
    #[error("sync server push error: wal conflict detected")]
    PushConflict,
    #[error("sync server push error: inconsitent state on remote: `{0:?}`")]
    PushInconsistent(DbSyncStatus),
}

impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::TursoError(value)
    }
}

impl From<turso_core::LimboError> for Error {
    fn from(value: turso_core::LimboError) -> Self {
        Self::TursoError(value.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::FilesystemError(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonDecode(value)
    }
}
