mod test_cdc;
mod test_function_rowid;
mod test_wal_api;

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
