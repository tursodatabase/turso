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
