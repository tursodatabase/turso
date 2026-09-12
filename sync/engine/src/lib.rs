pub mod alloc;
pub mod client_proto;
pub mod database_replay_generator;
pub mod database_sync_engine;
pub mod database_sync_engine_io;
pub mod database_sync_lazy_storage;
pub mod database_sync_operations;
pub mod database_tape;
pub mod errors;
pub mod io_operations;
pub mod server_proto;
pub mod types;
pub mod wal_session;

#[cfg(target_os = "linux")]
pub mod sparse_io;

pub type Result<T> = std::result::Result<T, errors::Error>;

#[cfg(test)]
#[path = "../tests/unit/tests.rs"]
mod tests;
