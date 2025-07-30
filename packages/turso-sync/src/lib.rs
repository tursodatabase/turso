pub mod database;

pub mod filesystem;
pub mod sync_server;

mod metadata;
#[cfg(test)]
mod test_context;
mod wal_session;

#[cfg(test)]
mod tests {
    use tracing_subscriber::EnvFilter;

    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            // .with_ansi(false)
            .init();
    }

    pub fn seed_u64() -> u64 {
        seed().parse().unwrap_or(0)
    }

    pub fn seed() -> String {
        std::env::var("SEED").unwrap_or("0".to_string())
    }

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

    pub fn deterministic_runtime<F: std::future::Future<Output = ()>>(f: impl Fn() -> F) {
        let seed = seed();
        deterministic_runtime_from_seed(seed.as_bytes(), f);
    }
}
