use crate::common::TempDatabase;
use turso_pg::PgConnection;

#[test]
fn enabled_row_security_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE TABLE t (x int)").unwrap();
    assert!(tables_with_row_security(&conn2).is_empty());

    conn1
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();

    assert_eq!(tables_with_row_security(&conn2), vec!["t"]);
}

#[test]
fn disabled_row_security_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE TABLE t (x int)").unwrap();
    conn1
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();
    assert_eq!(tables_with_row_security(&conn2), vec!["t"]);

    conn1
        .execute("ALTER TABLE t DISABLE ROW LEVEL SECURITY")
        .unwrap();

    assert!(tables_with_row_security(&conn2).is_empty());
}

#[test]
fn row_security_is_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE enabled (x int)").unwrap();
    conn.execute("CREATE TABLE disabled (x int)").unwrap();
    conn.execute("CREATE TABLE renamed (x int)").unwrap();
    conn.execute("CREATE TABLE dropped (x int)").unwrap();
    for table in ["enabled", "disabled", "renamed", "dropped"] {
        conn.execute(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
            .unwrap();
    }
    conn.execute("ALTER TABLE disabled DISABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("ALTER TABLE renamed RENAME TO new_name")
        .unwrap();
    conn.execute("DROP TABLE dropped").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE renamed (x int)").unwrap();
    conn.execute("CREATE TABLE dropped (x int)").unwrap();
    assert_eq!(tables_with_row_security(&conn), vec!["enabled", "new_name"]);
}

#[test]
fn schema_is_read_again_after_enabling_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    // Read the connection's own schema: a query would first adopt the
    // database's shared schema, which still has the table.
    let schema = conn.inner().current_schema();
    let public = schema.namespace_named("public").unwrap();
    assert!(schema.policies.is_enabled(public, "t"));
}

#[test]
fn role_that_does_not_own_the_table_cannot_enable_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap_err();

    assert_eq!(error.to_string(), "must be owner of table t");
    conn.execute("RESET ROLE").unwrap();
    assert!(tables_with_row_security(&conn).is_empty());
}

#[test]
fn force_row_security_is_not_supported() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();

    let error = conn
        .execute("ALTER TABLE t FORCE ROW LEVEL SECURITY")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "ALTER TABLE ... FORCE ROW LEVEL SECURITY is not supported"
    );
}

fn tables_with_row_security(conn: &PgConnection) -> Vec<String> {
    let mut stmt = conn
        .query("SELECT relname FROM pg_class WHERE relrowsecurity ORDER BY relname")
        .unwrap()
        .unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}
