use crate::sync_server::DbSyncStatus;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("turso-db error: {0}")]
    TursoError(turso::Error),
    #[error("invalid URI: {0}")]
    Uri(http::uri::InvalidUri),
    #[error("invalid HTTP request: {0}")]
    Http(http::Error),
    #[error("HTTP request error: {0}")]
    HyperRequest(hyper_util::client::legacy::Error),
    #[error("HTTP response error: {0}")]
    HyperResponse(hyper::Error),
    #[error("deserialization error: {0}")]
    JsonDecode(serde_json::Error),
    #[error("unexpected sync server error: code={0}, info={1}")]
    SyncServerError(http::StatusCode, String),
    #[error("unexpected sync server status: {0:?}")]
    SyncServerUnexpectedStatus(DbSyncStatus),
    #[error("unexpected filesystem error: {0}")]
    FilesystemError(std::io::Error),
    #[error("local metadata error: {0}")]
    MetadataError(String),
    #[error("database tape error: {0}")]
    DatabaseTapeError(String),
    #[error("database sync error: {0}")]
    DatabaseSyncError(String),
    #[error("sync server pull error: checkpoint required: `{0:?}`")]
    PullNeedCheckpoint(DbSyncStatus),
    #[error("sync server push error: wal conflict detected: `{0:?}`")]
    PushConflict(DbSyncStatus),
    #[error("sync server push error: inconsitent state on remote: `{0:?}`")]
    PushInconsistent(DbSyncStatus),
}

impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::TursoError(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::FilesystemError(value)
    }
}
