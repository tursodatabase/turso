use std::sync::Arc;

use turso_core::DatabaseOpts;
use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

const LOG_WITH_GENERATED_COLUMN: &[&str] = &[
    "CREATE TABLE items (x)",
    "INSERT INTO items VALUES (1)",
    "CREATE TABLE g (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
];

#[test]
fn failed_catch_up_after_bootstrap_is_retried_on_next_open() -> Result<()> {
    let empty_base = Remote::new(&[])?;
    let remote = Remote::new(LOG_WITH_GENERATED_COLUMN)?;
    let dir = Arc::new(tempfile::tempdir()?);
    let server = || InProcessServer::with_bootstrap_base(remote.clone(), empty_base.clone());

    for _ in 0..2 {
        assert_fails_on_generated_column(Replica::open(&dir, server(), DatabaseOpts::new()));
    }

    let replica = Replica::open(&dir, server(), database_opts())?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn failed_catch_up_after_bootstrap_is_retried_when_opening_existing_replica() -> Result<()> {
    let empty_base = Remote::new(&[])?;
    let remote = Remote::new(LOG_WITH_GENERATED_COLUMN)?;
    let dir = Arc::new(tempfile::tempdir()?);
    let server = || InProcessServer::with_bootstrap_base(remote.clone(), empty_base.clone());

    assert_fails_on_generated_column(Replica::open(&dir, server(), DatabaseOpts::new()));
    assert_fails_on_generated_column(Replica::open_existing(&dir, server(), DatabaseOpts::new()));

    let replica = Replica::open_existing(&dir, server(), database_opts())?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

fn assert_fails_on_generated_column(result: Result<Replica>) {
    let Err(error) = result else {
        panic!("opening without generated columns must fail to replay CREATE TABLE g");
    };
    assert!(
        error
            .to_string()
            .contains("Generated columns require --experimental-generated-columns flag"),
        "unexpected error: {error}"
    );
}
