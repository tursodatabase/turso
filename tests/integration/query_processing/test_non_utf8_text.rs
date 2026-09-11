//! Regression test for issue #5164: TEXT values containing invalid UTF-8 must
//! be rejected instead of entering the engine as text or being demoted to blobs.

use crate::common::try_limbo_exec_rows;
use turso_core::LimboError;

#[turso_macros::test(init_sql = "CREATE TABLE t(val TEXT);")]
fn test_non_utf8_text_is_rejected(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    {
        let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
        sqlite_conn.execute("INSERT INTO t VALUES(CAST(X'FF' AS TEXT)), ('valid')", [])?;
    }

    let limbo_conn = tmp_db.connect_limbo();

    for query in [
        "SELECT val FROM t",
        "SELECT 1 FROM t ORDER BY val COLLATE NOCASE",
    ] {
        let error = try_limbo_exec_rows(&tmp_db, &limbo_conn, query)
            .expect_err("invalid UTF-8 stored as TEXT must be rejected");
        assert!(
            matches!(
                error,
                LimboError::Corrupt(ref message)
                    if message == "TEXT value contains invalid UTF-8"
            ),
            "unexpected error for {query}: {error}"
        );
    }

    let rows = try_limbo_exec_rows(&tmp_db, &limbo_conn, "SELECT CAST(X'FF' AS BLOB)")?;
    assert_eq!(
        rows,
        vec![vec![rusqlite::types::Value::Blob(vec![0xFF])]],
        "the same bytes remain valid when explicitly typed as a blob"
    );

    Ok(())
}
