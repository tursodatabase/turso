use std::{path::PathBuf, sync::Arc};

use tokio::sync::RwLock;
use turso::Connection;

use crate::{
    database_tape::DatabaseTape,
    metadata::{DatabaseTandemSelection, SyncedDatabaseMetadata},
    sync_server::SyncServer,
};

struct DatabaseSyncInner<S: SyncServer> {
    sync_server: S,
    local_path: PathBuf,
    upstream_path: PathBuf,
    meta_path: PathBuf,
    meta: Option<SyncedDatabaseMetadata>,
    database: Arc<RwLock<DatabaseSyncTandem>>,
}

struct DatabaseSyncTandem {
    local: Option<DatabaseTape>,
    upstream: Option<DatabaseTape>,
    primary: DatabaseTandemSelection,
    connections: Vec<Connection>,
}
