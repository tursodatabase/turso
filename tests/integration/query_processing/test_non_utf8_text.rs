//! Regression test for issue #5164: reading a TEXT value containing invalid
//! UTF-8 (as can be created by SQLite with `CAST(X'FF' AS TEXT)`) must not
//! trigger undefined behavior. In debug builds the record decoder returned
//! `Internal error: Invalid UTF-8 in TEXT serial type`; in release builds the
//! unchecked UTF-8 conversion caused a SIGSEGV.

use crate::common::try_limbo_exec_rows;

#[turso_macros::test(init_sql = "CREATE TABLE t(val TEXT);")]
fn test_like_on_non_utf8_text(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    // Use SQLite to store a TEXT value that is not valid UTF-8. SQLite
    // happily stores the raw byte 0xFF with a TEXT serial type.
    {
        let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
        sqlite_conn.execute("INSERT INTO t VALUES(CAST(X'FF' AS TEXT))", [])?;
    }

    let limbo_conn = tmp_db.connect_limbo();

    // This query must complete without error (and without UB in release
    // builds). SQLite returns zero rows: the 0xFF byte does not match '%a%'.
    let rows = try_limbo_exec_rows(
        &tmp_db,
        &limbo_conn,
        "SELECT val FROM t WHERE val LIKE '%a%'",
    )?;
    assert_eq!(rows.len(), 0, "no rows should match LIKE '%a%'");

    Ok(())
}
