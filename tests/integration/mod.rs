mod common;
mod functions;
mod fuzz;
mod fuzz_transaction;
mod pragma;
mod query_processing;
mod storage;
mod wal;

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
