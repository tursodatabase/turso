mod assert_details;
mod attach;
mod common;
mod conflict_resolution;
mod custom_types;
mod database;
mod functions;
mod fuzz_transaction;
mod index_method;
mod integrity_check;
mod mvcc;
mod pragma;
mod query_processing;
mod query_timeout;
mod statement_reset;
mod stmt_journal;
mod storage;
mod trigger;
mod wal;
#[cfg(feature = "wasm-udf")]
mod wasm_extension;
#[cfg(feature = "wasm-udf")]
mod wasm_udf;

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
