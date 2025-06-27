use crate::helpers::wrapper::Wrapper;

pub use super::connection::LibsqlConnection;
use std::sync::Arc;

pub enum LibsqlOpenFlags {
    ReadOnly,
    ReadWrite,
    Create,
}

pub struct ConnectArgs {
    pub url: String,
    pub auth_token: Option<String>,
    pub sync_url: Option<String>,
    pub sync_interval_seconds: Option<u64>,
    pub encryption_key: Option<String>,
    pub read_your_writes: Option<bool>,
    pub open_flags: Option<LibsqlOpenFlags>,
    pub offline: Option<bool>,
}

pub async fn connect(args: ConnectArgs) -> LibsqlConnection {
    let database = if args.url == ":memory:" {
        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new());
        limbo_core::Database::open_file(io, args.url.as_str(), false, false)
    } else {
        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        limbo_core::Database::open_file(io, args.url.as_str(), false, false)
    }
    .unwrap();
    let connection = database.connect().unwrap();
    LibsqlConnection::new(Wrapper { inner: connection }, Wrapper { inner: database })
}
