use crate::common::TempDatabase;
use turso_pg::PgConnection;

#[test]
fn created_role_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    assert_eq!(role_names(&conn2), vec!["postgres", "pg_database_owner"]);

    conn1.execute("CREATE ROLE alice").unwrap();

    assert_eq!(
        role_names(&conn2),
        vec!["postgres", "pg_database_owner", "alice"]
    );
}

#[test]
fn role_created_in_rolled_back_transaction_is_not_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();

    conn1.execute("BEGIN").unwrap();
    conn1.execute("CREATE ROLE alice").unwrap();
    conn1.execute("ROLLBACK").unwrap();

    assert_eq!(role_names(&conn1), vec!["postgres", "pg_database_owner"]);
    assert_eq!(role_names(&conn2), vec!["postgres", "pg_database_owner"]);
}

#[test]
fn preparing_create_role_without_running_it_does_not_create_the_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();

    drop(conn.prepare("CREATE ROLE alice").unwrap());

    assert_eq!(role_names(&conn), vec!["postgres", "pg_database_owner"]);
    conn.execute("CREATE ROLE alice").unwrap();
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice"]
    );
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
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice", "bob"]
    );
    conn.execute("CREATE ROLE carol").unwrap();
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice", "bob", "carol"]
    );
}

/// The in-memory catalog rejects a name it knows, but only the stored table
/// can keep a role name unique against every writer of the file.
#[test]
fn stored_roles_table_keeps_role_names_unique() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_limbo();
    let has_unique_name_index = conn.with_schema(turso_core::MAIN_DB_ID, |schema| {
        schema
            .get_indices(turso_core::NamespaceId::ROOT, "__turso_internal_roles")
            .any(|index| {
                index.unique
                    && index
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .eq(["name"])
            })
    });
    assert!(has_unique_name_index);
    db.connect_postgres().execute("CREATE ROLE bob").unwrap();
}

#[test]
fn roles_are_kept_after_reopening_an_mvcc_database() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "pg_database_owner", "alice"]
    );
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "pg_database_owner", "alice"]
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
        vec!["postgres", "pg_database_owner", "alice"]
    );
}

#[test]
fn roles_are_kept_after_vacuum() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();

    // VACUUM of an MVCC database needs its changes checkpointed first.
    let core_conn = db.connect_limbo();
    core_conn
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    core_conn.execute("VACUUM").unwrap();

    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice"]
    );
    assert_eq!(
        role_names(&db.connect_postgres()),
        vec!["postgres", "pg_database_owner", "alice"]
    );
}

#[test]
fn statement_prepared_before_set_role_is_checked_against_the_new_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    let mut create = conn.prepare("CREATE TABLE t (x int)").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    let error = create.run_collect_rows().unwrap_err();
    assert_eq!(error.to_string(), "permission denied for schema public");
}

#[test]
fn statement_prepared_as_role_without_privileges_runs_after_reset_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    let mut create = conn.prepare("CREATE TABLE t (x int)").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert!(create.run_collect_rows().is_err());
    create.reset().unwrap();

    conn.execute("RESET ROLE").unwrap();

    create.run_collect_rows().unwrap();
    conn.execute("SELECT x FROM t").unwrap();
}

#[test]
fn set_role_changes_only_its_own_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE ROLE alice").unwrap();

    conn1.execute("SET ROLE alice").unwrap();

    assert!(conn1.execute("CREATE TABLE t (x int)").is_err());
    conn2.execute("CREATE TABLE t (x int)").unwrap();
}

#[test]
fn schema_is_read_again_after_creating_a_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice"]
    );
}

/// The table-valued PRAGMA functions run their PRAGMA as a statement of the
/// engine's own, so the PRAGMA is checked where the function is called.
#[test]
fn role_without_privileges_cannot_call_pragma_functions() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SELECT file FROM pragma_database_list")
        .unwrap();
    conn.execute("CREATE VIEW columns_of_t AS SELECT name FROM pragma_table_info('t')")
        .unwrap();
    conn.execute("CREATE VIEW databases AS SELECT file FROM pragma_database_list")
        .unwrap();
    conn.execute("CREATE VIEW view_of_databases AS SELECT file FROM databases")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    for (sql, pragma) in [
        ("SELECT file FROM pragma_database_list", "database_list"),
        ("SELECT name FROM pragma_table_info('t')", "table_info"),
        (
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM pragma_database_list)",
            "database_list",
        ),
        ("SELECT name FROM columns_of_t", "table_info"),
        ("SELECT file FROM view_of_databases", "database_list"),
    ] {
        let error = conn.execute(sql).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("permission denied to examine \"{pragma}\""),
            "{sql}"
        );
    }
}

/// Reading a view is allowed until GRANT exists, so the sequences the view
/// changes are what is denied. PostgreSQL denies reading the view itself
/// unless SELECT on it was granted.
#[test]
fn role_without_privileges_cannot_change_a_sequence_through_a_view() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE SEQUENCE s").unwrap();
    conn.execute("CREATE VIEW next_s AS SELECT nextval('s') AS n")
        .unwrap();
    conn.execute("CREATE VIEW set_s AS SELECT setval('s', 5) AS n")
        .unwrap();
    conn.execute("CREATE VIEW view_of_set_s AS SELECT n FROM set_s")
        .unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    for sql in ["SELECT n FROM next_s", "SELECT n FROM view_of_set_s"] {
        let error = conn.execute(sql).unwrap_err();
        assert_eq!(
            error.to_string(),
            "permission denied for sequence s",
            "{sql}"
        );
    }

    conn.execute("RESET ROLE").unwrap();
    let mut stmt = conn.query("SELECT nextval('s')").unwrap().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0].to_string(), "1");
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

/// The PostgreSQL frontend runs in MVCC mode, which has no materialized
/// views, so REFRESH does nothing for the superuser even without the view.
#[test]
fn role_without_privileges_cannot_refresh_materialized_view() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap_err();

    assert_eq!(
        error.to_string(),
        "permission denied for materialized view mv"
    );
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

#[test]
fn create_role_keeps_the_last_inserted_rowid() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(conn.last_insert_rowid(), 3);

    conn.execute("CREATE ROLE alice").unwrap();

    assert_eq!(conn.last_insert_rowid(), 3);
}

#[test]
fn interrupted_set_role_keeps_the_current_role() {
    for interrupt_at_step in 1.. {
        let db = TempDatabase::builder().build();
        let conn = db.connect_postgres();
        conn.execute("CREATE ROLE alice").unwrap();
        let result = run_interrupted_at(&conn, "SET ROLE alice", interrupt_at_step);
        if result.is_ok() {
            assert!(interrupt_at_step > 1, "the statement was never interrupted");
            return;
        }

        assert_eq!(
            current_user(&conn),
            "postgres",
            "interrupted at step {interrupt_at_step}, the role changed"
        );
    }
}

#[test]
fn interrupted_create_role_leaves_no_role_behind() {
    for interrupt_at_step in 1.. {
        let db = TempDatabase::builder().build();
        let conn = db.connect_postgres();
        let result = run_interrupted_at(&conn, "CREATE ROLE alice", interrupt_at_step);
        if result.is_ok() {
            assert!(interrupt_at_step > 1, "the statement was never interrupted");
            return;
        }

        assert!(
            conn.inner().role_catalog().get_by_name("alice").is_none(),
            "interrupted at step {interrupt_at_step}, the role is still in the catalog"
        );
        conn.execute("CREATE ROLE alice").unwrap();
    }
}

/// Runs `sql` with a progress handler that interrupts it at the
/// `interrupt_at_step`th step.
fn run_interrupted_at(
    conn: &PgConnection,
    sql: &str,
    interrupt_at_step: u64,
) -> turso_pg::Result<()> {
    let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    conn.inner().set_progress_handler(
        1,
        Some(Box::new(move || {
            steps.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == interrupt_at_step
        })),
    );
    let result = conn.execute(sql);
    conn.inner().set_progress_handler(0, None);
    result
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
