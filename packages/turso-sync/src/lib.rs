pub mod database_local_sync;
pub mod errors;
pub mod metadata;
pub mod sync_server;
pub mod types;

pub type Result<T> = std::result::Result<T, errors::Error>;

#[cfg(test)]
mod tests {
    use tracing_subscriber::EnvFilter;

    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    }
}
