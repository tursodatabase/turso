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
    #[error("local metadata error: {0}")]
    MetadataError(String),
    #[error("unexpected client error: {0}")]
    ClientError(String),
    #[error("sync server pull error: checkpoint required: `{0:?}`")]
    PullCheckpointNeeded(DbSyncStatus),
    #[error("sync server push error: wal conflict detected: `{0:?}`")]
    PushConflict(DbSyncStatus),
    #[error("sync server push error: inconsitent state on remote: `{0:?}`")]
    PushInconsistent(DbSyncStatus),
}
