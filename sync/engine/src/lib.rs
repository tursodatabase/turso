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

    #[allow(dead_code)]
    pub fn seed_u64() -> u64 {
        seed().parse().unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn seed() -> String {
        std::env::var("SEED").unwrap_or("0".to_string())
    }

    #[allow(dead_code)]
    pub fn deterministic_runtime_from_seed<F: std::future::Future<Output = ()>>(
        seed: &[u8],
        f: impl Fn() -> F,
    ) {
        let seed = tokio::runtime::RngSeed::from_bytes(seed);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .rng_seed(seed)
            .build_local(Default::default())
            .unwrap();
        runtime.block_on(f());
    }

    #[allow(dead_code)]
    pub fn deterministic_runtime<F: std::future::Future<Output = ()>>(f: impl Fn() -> F) {
        let seed = seed();
        deterministic_runtime_from_seed(seed.as_bytes(), f);
    }
}
