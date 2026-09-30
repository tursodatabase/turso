//! Shared helpers for the serverless conformance tests.
//!
//! The tests run against a live Turso Cloud database configured through
//! the `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` environment variables.
//! When the variables are not set, every test skips itself, so the suite
//! is safe to run in environments without credentials.

use std::sync::atomic::{AtomicU64, Ordering};

/// Connection details for the database under test.
pub struct TestConfig {
    /// Database URL, normalized to `https://` / `http://`.
    pub url: String,
    pub auth_token: String,
}

/// Read the test configuration from the environment. Returns `None` when
/// `TURSO_DATABASE_URL` or `TURSO_AUTH_TOKEN` is not set, in which case
/// the caller should skip the test (see [`config_or_skip!`]).
pub fn config() -> Option<TestConfig> {
    let url = std::env::var("TURSO_DATABASE_URL").ok()?;
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").ok()?;
    let url = if let Some(rest) = url.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("turso://") {
        format!("https://{rest}")
    } else {
        url.trim_end_matches('/').to_string()
    };
    Some(TestConfig { url, auth_token })
}

/// Fetch the test configuration, or skip the current test with a message
/// when the environment is not configured.
#[macro_export]
macro_rules! config_or_skip {
    () => {
        match $crate::config() {
            Some(config) => config,
            None => {
                eprintln!(
                    "skipping: set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run conformance tests"
                );
                return;
            }
        }
    };
}

/// Generate a unique SQL identifier with the given prefix. The tests share
/// one live database, so every test works on its own uniquely named tables.
pub fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{nanos}_{count}", std::process::id())
}
