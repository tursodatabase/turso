use crate::common::TempDatabase;
use std::sync::Arc;

fn is_stmt_readonly(conn: &Arc<turso_core::Connection>, sql: &str) -> bool {
    let stmt = conn.prepare(sql).unwrap();
    stmt.get_program().prepared().is_readonly()
}

#[turso_macros::test]
fn select_is_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(is_stmt_readonly(&conn, "SELECT 1"));
    Ok(())
}

#[turso_macros::test]
fn begin_deferred_is_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(is_stmt_readonly(&conn, "BEGIN"));
    Ok(())
}

#[turso_macros::test]
fn begin_immediate_is_not_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!is_stmt_readonly(&conn, "BEGIN IMMEDIATE"));
    Ok(())
}

#[turso_macros::test]
fn pragma_journal_mode_is_not_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!is_stmt_readonly(&conn, "PRAGMA journal_mode"));
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(x)")]
fn create_table_if_not_exists_existing_is_not_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!is_stmt_readonly(&conn, "CREATE TABLE IF NOT EXISTS t(x)"));
    Ok(())
}

#[turso_macros::test]
fn drop_if_exists_of_a_missing_object_matches_sqlite(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let cases = [
        ("DROP TRIGGER IF EXISTS tr", true),
        ("DROP TABLE IF EXISTS t", false),
        ("DROP INDEX IF EXISTS i", false),
        ("DROP VIEW IF EXISTS v", false),
    ];
    let wrong: Vec<_> = cases
        .iter()
        .filter(|(sql, readonly)| is_stmt_readonly(&conn, sql) != *readonly)
        .collect();
    assert!(
        wrong.is_empty(),
        "read-only flag differs from sqlite3_stmt_readonly: {wrong:?}"
    );
    Ok(())
}

#[turso_macros::test]
fn drop_of_an_existing_object_is_not_readonly(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    for sql in [
        "CREATE TABLE t(x)",
        "CREATE INDEX i ON t(x)",
        "CREATE VIEW v AS SELECT x FROM t",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
    ] {
        conn.execute(sql)?;
    }
    let statements = [
        "DROP TRIGGER IF EXISTS tr",
        "DROP TABLE IF EXISTS t",
        "DROP INDEX IF EXISTS i",
        "DROP VIEW IF EXISTS v",
        "DROP TRIGGER tr",
        "DROP TABLE t",
        "DROP INDEX i",
        "DROP VIEW v",
    ];
    let wrong: Vec<_> = statements
        .iter()
        .filter(|sql| is_stmt_readonly(&conn, sql))
        .collect();
    assert!(wrong.is_empty(), "classified as read-only: {wrong:?}");
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(x)")]
fn writes_to_temporary_query_tables_do_not_make_a_statement_read_write(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let cases = [
        ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c", true),
        ("WITH m AS MATERIALIZED (SELECT x FROM t) SELECT a.x FROM m AS a, m AS b", true),
        ("SELECT a.x FROM (SELECT x FROM t ORDER BY x LIMIT 3) AS a JOIN t USING (x)", true),
        ("SELECT DISTINCT x FROM t ORDER BY x", true),
        ("SELECT x FROM t EXCEPT SELECT x FROM t", true),
        ("SELECT x FROM t UNION SELECT x FROM t", true),
        ("SELECT x FROM t WHERE x IN (SELECT x FROM t)", true),
        ("SELECT sum(x) OVER (ORDER BY x) FROM t", true),
        ("INSERT INTO t VALUES (1)", false),
        ("UPDATE t SET x = x + 1", false),
        ("DELETE FROM t", false),
        ("CREATE TABLE u(y)", false),
        ("INSERT INTO t WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c", false),
        ("UPDATE t SET x = x + 1 WHERE x IN (SELECT x FROM t)", false),
        ("DELETE FROM t WHERE x IN (SELECT x FROM t ORDER BY x LIMIT 1)", false),
    ];
    let wrong: Vec<_> = cases
        .iter()
        .filter(|(sql, readonly)| is_stmt_readonly(&conn, sql) != *readonly)
        .collect();
    assert!(
        wrong.is_empty(),
        "read-only flag differs from sqlite3_stmt_readonly: {wrong:?}"
    );
    Ok(())
}
