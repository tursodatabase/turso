//! Regression test for issue #8272: UNION/INTERSECT/EXCEPT dedupe and
//! collection indexes must resolve each arm's collation like SQLite's
//! multiSelectCollSeq — a bare column carries its declared collation or the
//! default BINARY and only a no-opinion arm (literal, function, ...) defers
//! rightward. Before the fix the left arm's plain column silently adopted
//! the right arm's NOCASE, returning wrong rows.

use crate::common::{try_limbo_exec_rows, TempDatabase};

const SEED_Q: &str = "INSERT INTO q VALUES(1,'a'),(2,'B'),(NULL,'c'),('3','z')";
const SEED_P: &str = "INSERT INTO p VALUES(1,'A'),(2,'b'),(NULL,'C'),(3,NULL),(2,'B'),(1.0,'a')";

const QUERIES: &[&str] = &[
    "SELECT y FROM q INTERSECT SELECT y FROM p",
    "SELECT y FROM q EXCEPT SELECT y FROM p",
    "SELECT y FROM q UNION SELECT y FROM p ORDER BY 1",
    // literal left arm: both engines defer to the right arm's NOCASE
    "SELECT 'a' FROM q INTERSECT SELECT y FROM p",
];

#[turso_macros::test]
fn compound_select_collation_matches_sqlite(
    tmp_db: crate::common::TempDatabase,
) -> anyhow::Result<()> {
    let limbo_conn = tmp_db.connect_limbo();
    limbo_conn.execute("CREATE TABLE q(x, y TEXT)")?;
    limbo_conn.execute("CREATE TABLE p(x, y TEXT COLLATE NOCASE)")?;
    limbo_conn.execute(SEED_Q)?;
    limbo_conn.execute(SEED_P)?;
    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    // The limbo connection above created the schema; seed the same file.
    sqlite_conn.execute_batch(
        "INSERT INTO q VALUES(1,'a'),(2,'B'),(NULL,'c'),('3','z');
         INSERT INTO p VALUES(1,'A'),(2,'b'),(NULL,'C'),(3,NULL),(2,'B'),(1.0,'a');",
    )?;

    for query in QUERIES {
        let mut limbo_rows = try_limbo_exec_rows(&tmp_db, &limbo_conn, query)?;
        limbo_rows.sort_by_key(|r| format!("{r:?}"));
        let mut sqlite_rows: Vec<Vec<rusqlite::types::Value>> = Vec::new();
        let mut stmt = sqlite_conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            sqlite_rows.push(vec![row.get(0)?]);
        }
        sqlite_rows.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(limbo_rows, sqlite_rows, "wrong rows for {query}");
    }

    // Pin the oracle explicitly so a regression cannot hide behind an
    // equally-wrong pair of engines.
    let intersect = try_limbo_exec_rows(&tmp_db, &limbo_conn, QUERIES[0])?;
    assert_eq!(
        intersect,
        vec![
            vec![rusqlite::types::Value::Text("B".into())],
            vec![rusqlite::types::Value::Text("a".into())],
        ],
        "'c' has no BINARY match in p and must not survive INTERSECT"
    );

    Ok(())
}
