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
