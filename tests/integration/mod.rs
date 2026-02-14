mod assert_details;
mod common;
mod conflict_resolution;
mod functions;
mod fuzz_transaction;
mod index_method;
mod integrity_check;
mod mvcc;
mod pragma;
mod query_processing;
mod storage;
mod trigger;
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
