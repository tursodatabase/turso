use crate::common::{limbo_exec_rows, TempDatabase};

const SCHEMA: [&str; 2] = [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    "CREATE INDEX idx_users_age ON users(age)",
];

fn connect_with_schema(tmp_db: &TempDatabase) -> std::sync::Arc<turso_core::Connection> {
    let conn = tmp_db.connect_limbo();
    for ddl in SCHEMA {
        limbo_exec_rows(&conn, ddl);
    }
    conn
}

#[turso_macros::test]
fn format_json_statement_reports_one_text_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let stmt = conn.prepare("EXPLAIN QUERY PLAN FORMAT=JSON SELECT 1")?;
    assert_eq!(stmt.num_columns(), 1);
    assert_eq!(stmt.get_column_name(0), "plan_json");
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("TEXT"));
    Ok(())
}
