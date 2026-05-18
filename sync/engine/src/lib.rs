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
mod tests {
    use tracing_subscriber::EnvFilter;

    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_ansi(false)
            .init();
    }
}
