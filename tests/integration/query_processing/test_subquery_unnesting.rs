use std::sync::Arc;

use crate::common::TempDatabase;

/// Return the text rows from `EXPLAIN QUERY PLAN`.
fn explain(connection: &Arc<turso_core::Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut statement = connection.prepare(format!("EXPLAIN QUERY PLAN {sql}"))?;
    let mut details = Vec::new();
    statement.run_with_row_callback(|row| {
        details.push(row.get::<String>(3)?);
        Ok(())
    })?;
    Ok(details)
}

/// A direct positive `IN` can use the inner table as a semi-join.
#[test]
fn correlated_in_uses_a_semi_join() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT, amount INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE amount IN (
             SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1
         )",
    )?;

    assert!(
        details
            .iter()
            .any(|detail| detail.contains("inner_rows") && detail.contains("key1=?")),
        "expected the inner table to be searched by the outer key, got {details:?}"
    );
    assert!(
        details.iter().all(|detail| !detail.contains("CORRELATED")),
        "expected no per-row IN subquery, got {details:?}"
    );
    Ok(())
}

/// An indexed `EXISTS` keeps the index search after it becomes a semi-join.
#[test]
fn indexed_correlated_exists_uses_a_semi_join() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INTEGER PRIMARY KEY)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(outer_id INT, value INT)")?;
    connection.execute("CREATE INDEX inner_rows_outer_id ON inner_rows(outer_id)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE EXISTS (
             SELECT 1 FROM inner_rows i
             WHERE i.outer_id = o.id AND i.value > 10
         )",
    )?;

    assert!(
        details.iter().all(|detail| !detail.contains("CORRELATED")),
        "expected EXISTS to use a semi-join, got {details:?}"
    );
    assert!(
        details.iter().any(|detail| {
            detail.contains("inner_rows_outer_id") && detail.contains("outer_id=?")
        }),
        "expected the semi-join to use the index on the linked column, got {details:?}"
    );
    Ok(())
}
