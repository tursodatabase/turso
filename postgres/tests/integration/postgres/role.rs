use crate::common::TempDatabase;
use turso_pg::PgConnection;

#[test]
fn created_role_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    assert_eq!(role_names(&conn2), vec!["postgres"]);

    conn1.execute("CREATE ROLE alice").unwrap();

    assert_eq!(role_names(&conn2), vec!["postgres", "alice"]);
}

#[test]
fn role_created_in_rolled_back_transaction_is_not_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();

    conn1.execute("BEGIN").unwrap();
    conn1.execute("CREATE ROLE alice").unwrap();
    conn1.execute("ROLLBACK").unwrap();

    assert_eq!(role_names(&conn1), vec!["postgres"]);
    assert_eq!(role_names(&conn2), vec!["postgres"]);
}

#[test]
fn preparing_create_role_without_running_it_does_not_create_the_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();

    drop(conn.prepare("CREATE ROLE alice").unwrap());

    assert_eq!(role_names(&conn), vec!["postgres"]);
    conn.execute("CREATE ROLE alice").unwrap();
    assert_eq!(role_names(&conn), vec!["postgres", "alice"]);
}

#[test]
fn roles_are_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE ROLE bob").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    assert_eq!(role_names(&conn), vec!["postgres", "alice", "bob"]);
    conn.execute("CREATE ROLE carol").unwrap();
    assert_eq!(role_names(&conn), vec!["postgres", "alice", "bob", "carol"]);
}

#[test]
fn roles_are_kept_after_reopening_an_mvcc_database() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "alice"]
    );
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "alice"]
    );
}

#[test]
fn roles_are_kept_after_checkpointing_and_reopening_an_mvcc_database() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "alice"]
    );
}

#[test]
fn roles_are_kept_after_vacuum() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();

    db.connect_limbo().execute("VACUUM").unwrap();

    assert_eq!(role_names(&conn), vec!["postgres", "alice"]);
    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "alice"]
    );
}

#[test]
fn statement_prepared_before_set_role_is_checked_against_the_new_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    let mut select = conn.prepare("SELECT x FROM t").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    let error = select.run_collect_rows().unwrap_err();
    assert_eq!(error.to_string(), "permission denied for table t");
}

#[test]
fn statement_prepared_as_role_without_privileges_runs_after_reset_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert!(select.run_collect_rows().is_err());
    select.reset().unwrap();

    conn.execute("RESET ROLE").unwrap();

    assert_eq!(select.run_collect_rows().unwrap().len(), 1);
}

#[test]
fn set_role_changes_only_its_own_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE TABLE t (x int)").unwrap();
    conn1.execute("CREATE ROLE alice").unwrap();

    conn1.execute("SET ROLE alice").unwrap();

    assert!(conn1.execute("SELECT x FROM t").is_err());
    conn2.execute("SELECT x FROM t").unwrap();
}

#[test]
fn set_role_inside_transaction_block_fails() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("BEGIN").unwrap();

    let error = conn.execute("SET ROLE alice").unwrap_err();

    assert_eq!(
        error.to_string(),
        "SET ROLE inside a transaction block is not supported"
    );
    assert_eq!(current_user(&conn), "postgres");
}

#[test]
fn role_without_privileges_cannot_create_schema() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("CREATE SCHEMA s").unwrap_err();

    assert!(
        error
            .to_string()
            .starts_with("permission denied for database "),
        "{error}"
    );
}

#[turso_macros::test]
fn role_without_privileges_cannot_drop_schema_or_copy_from_a_file(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA s").unwrap();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let drop_schema = conn.execute("DROP SCHEMA s").unwrap_err();
    let copy = conn.execute("COPY t FROM '/nonexistent'").unwrap_err();

    assert_eq!(drop_schema.to_string(), "must be owner of schema s");
    assert_eq!(copy.to_string(), "permission denied to COPY from a file");
}

#[test]
fn set_role_finds_role_created_by_another_connection_after_prepare() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    let mut set_role = conn1.prepare("SET ROLE alice").unwrap();

    conn2.execute("CREATE ROLE alice").unwrap();

    set_role.run_ignore_rows().unwrap();
    assert_eq!(current_user(&conn1), "alice");
}

#[test]
fn current_user_and_session_user_name_the_superuser() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();

    assert_eq!(current_user(&conn), "postgres");
    let mut stmt = conn.query("SELECT session_user").unwrap().unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap()[0][0].to_string(),
        "postgres"
    );
}

fn current_user(conn: &PgConnection) -> String {
    let mut stmt = conn.query("SELECT current_user").unwrap().unwrap();
    stmt.run_collect_rows().unwrap()[0][0].to_string()
}

fn role_names(conn: &PgConnection) -> Vec<String> {
    let mut stmt = conn
        .query("SELECT rolname FROM pg_roles ORDER BY oid")
        .unwrap()
        .unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}
