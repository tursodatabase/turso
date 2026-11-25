use turso_sync_engine::Result;

use crate::harness::Remote;

/// The remote drops its last generated column and adds it again with another
/// expression. The server writes the table text again, without the quotes that
/// the table name does not need. The replica must store the same text.
#[test]
fn redefine_last_generated_column_of_table_with_quoted_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE \"muggles\" (id TEXT PRIMARY KEY, surname VARCHAR(40), tag TEXT AS (LOWER (surname)))",
        "INSERT INTO \"muggles\" (id, surname) VALUES ('a', 'Smith')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE muggles DROP COLUMN tag",
        "ALTER TABLE muggles ADD COLUMN tag TEXT AS (UPPER (surname))",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}
