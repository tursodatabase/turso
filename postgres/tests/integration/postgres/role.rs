use crate::common::TempDatabase;
use turso_core::{ConnectOpts, NamespaceId, SessionRole, MAIN_DB_ID};
use turso_pg::{ErrorReport, PgConnection};

#[test]
fn session_runs_as_the_role_it_was_opened_as() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    superuser.execute("CREATE ROLE alice").unwrap();
    superuser.execute("CREATE TABLE t (x INT)").unwrap();

    let alice = PgConnection::connect_with(
        &db.db,
        ConnectOpts::new(SessionRole::Named("alice".to_string())),
    )
    .unwrap();

    assert_eq!(current_user(&alice), "alice");
    assert_eq!(session_user(&alice), "alice");
    assert!(!alice.inner().current_role_is_superuser());
    let Err(error) = alice.query("SELECT x FROM t") else {
        panic!("a role without privileges read table t");
    };
    assert_eq!(error.to_string(), "permission denied for table t");
    alice.execute("RESET ROLE").unwrap();
    assert_eq!(current_user(&alice), "alice");
    assert_eq!(current_user(&superuser), "postgres");
}

#[test]
fn connecting_as_a_role_that_does_not_exist_fails() {
    let db = TempDatabase::builder().build();

    for name in ["alice", "public"] {
        let Err(error) = db
            .db
            .connect(ConnectOpts::new(SessionRole::Named(name.to_string())))
        else {
            panic!("connecting as {name}, which does not exist, succeeded");
        };

        assert_eq!(error.to_string(), format!("role \"{name}\" does not exist"));
    }
}

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
    let has_unique_name_index = conn.with_schema(MAIN_DB_ID, |schema| {
        schema
            .get_indices(NamespaceId::ROOT, "__turso_internal_authid")
            .any(|index| {
                index.unique
                    && index
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .eq(["rolname"])
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

/// Foreign-key checks and actions need no privileges on the other table, as
/// in PostgreSQL. The PostgreSQL frontend does not turn foreign keys on, so
/// the test does.
#[test]
fn foreign_key_checks_and_actions_need_no_privileges_on_the_other_table() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.inner().set_foreign_keys_enabled(true);
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE parent (id int PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id int, parent int REFERENCES parent (id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
    conn.execute("GRANT INSERT ON child TO alice").unwrap();
    conn.execute("GRANT DELETE ON parent TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    conn.execute("INSERT INTO child VALUES (10, 1), (20, 2)")
        .unwrap();
    conn.execute("DELETE FROM parent").unwrap();

    conn.execute("RESET ROLE").unwrap();
    let mut count = conn.query("SELECT count(*) FROM child").unwrap().unwrap();
    assert_eq!(
        count.run_collect_rows().unwrap(),
        vec![vec![turso_core::Value::from_i64(0)]]
    );
}

/// As in PostgreSQL, a constraint error shows the values of a row only to a
/// role that may read the table. Otherwise a role with UPDATE but not SELECT
/// could read the rows from the errors of updates that fail.
#[test]
fn constraint_errors_show_values_only_to_a_role_with_select() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute(
        "CREATE TABLE docs (id int PRIMARY KEY, secret text, n int CHECK (n < 1000), \
         a int, b int, UNIQUE (a, b))",
    )
    .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'alpha', 0, 1, 10), (2, 'bravo', 0, 2, 10)")
        .unwrap();
    conn.execute("GRANT INSERT, UPDATE ON docs TO alice")
        .unwrap();
    let detail = |sql: &str| ErrorReport::from(conn.execute(sql).unwrap_err().core_error()).detail;

    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(detail("UPDATE docs SET n = 5000"), None);
    assert_eq!(detail("UPDATE docs SET a = 1"), None);
    assert_eq!(detail("INSERT INTO docs VALUES (1, 'x', 0, 3, 3)"), None);
    assert_eq!(detail("INSERT INTO docs VALUES (3, 'x', 5000, 3, 3)"), None);

    conn.execute("RESET ROLE").unwrap();
    conn.execute("GRANT SELECT ON docs TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    assert_eq!(
        detail("UPDATE docs SET n = 5000").as_deref(),
        Some("Failing row contains (1, alpha, 5000, 1, 10).")
    );
    assert_eq!(
        detail("UPDATE docs SET a = 1").as_deref(),
        Some("Key (a, b)=(1, 10) already exists.")
    );
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
    conn.execute("GRANT SELECT ON columns_of_t, databases, view_of_databases TO alice")
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

/// An embedder, such as a server measuring the database after each write,
/// reads the storage counts and turns off the automatic checkpoint on the
/// connection of a user who switched to a role that may not run the
/// PRAGMAs that do the same.
#[test]
fn storage_stats_and_checkpoint_setting_work_after_set_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x text)").unwrap();
    conn.execute("GRANT ALL ON t TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    let superuser = db.connect_postgres();
    let log_path = format!("{}-log", db.path.display());

    conn.inner().disable_mvcc_auto_checkpoint().unwrap();
    assert_eq!(pragma(&superuser, "mvcc_checkpoint_threshold"), -1);

    let before = storage_stats(&db, &conn);
    conn.execute("INSERT INTO t VALUES (repeat('a', 10000))")
        .unwrap();
    let after_insert = storage_stats(&db, &conn);
    assert_eq!(after_insert.logical_log_bytes, file_size(&log_path));
    let mut stmt = superuser.query("SELECT length(x) FROM t").unwrap().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0].to_string(), "10000");
    // The row is only in the logical log until it is checkpointed.
    assert!(
        after_insert.logical_log_bytes >= before.logical_log_bytes + 10000,
        "{before:?} {after_insert:?}"
    );
    superuser
        .inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let after_checkpoint = storage_stats(&db, &conn);

    assert_eq!(after_checkpoint.logical_log_bytes, file_size(&log_path));
    assert_eq!(
        after_checkpoint.page_size as i64,
        pragma(&superuser, "page_size")
    );
    assert_eq!(
        after_checkpoint.page_count as i64,
        pragma(&superuser, "page_count")
    );
    assert_eq!(
        after_checkpoint.freelist_count as i64,
        pragma(&superuser, "freelist_count")
    );
    assert!(
        after_checkpoint.page_count > after_insert.page_count,
        "{after_insert:?} {after_checkpoint:?}"
    );
    for sql in [
        "PRAGMA page_size",
        "PRAGMA page_count",
        "PRAGMA freelist_count",
        "PRAGMA mvcc_checkpoint_threshold = -1",
    ] {
        let error = conn.inner().prepare(sql).unwrap_err();
        assert!(
            matches!(error, turso_core::LimboError::PermissionDenied(_)),
            "{sql}: {error}"
        );
    }
}

fn storage_stats(db: &TempDatabase, conn: &PgConnection) -> turso_core::StorageStats {
    let mut read = conn.inner().storage_stats();
    loop {
        match read.read().unwrap() {
            turso_core::IOResult::Done(stats) => return stats,
            turso_core::IOResult::IO(io) => io.wait(&*db.io).unwrap(),
        }
    }
}

fn file_size(path: &str) -> u64 {
    std::fs::metadata(path).map_or(0, |file| file.len())
}

fn pragma(conn: &PgConnection, name: &str) -> i64 {
    let rows = conn.inner().pragma_query(name).unwrap();
    match rows[0][0] {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => value,
        ref value => panic!("PRAGMA {name} returned {value:?}"),
    }
}

/// With SELECT on the views granted, the sequences the views change are
/// what is denied, as in PostgreSQL, where nextval runs as the reader and
/// not as the view's owner.
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
    conn.execute("GRANT SELECT ON next_s, set_s, view_of_set_s TO alice")
        .unwrap();
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
            !role_names(&conn).iter().any(|name| name == "alice"),
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

fn session_user(conn: &PgConnection) -> String {
    let mut stmt = conn.query("SELECT session_user").unwrap().unwrap();
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

/// An owner may give its relation only to a role it may SET ROLE to, as in
/// PostgreSQL, and alice is not a member of bob.
#[test]
fn owner_cannot_give_its_table_to_a_role_it_is_not_a_member_of() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE ROLE bob").unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("ALTER TABLE t OWNER TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("ALTER TABLE t OWNER TO bob").unwrap_err();
    assert_eq!(error.to_string(), "permission denied to set role \"bob\"");
    conn.execute("ALTER TABLE t OWNER TO CURRENT_USER").unwrap();
}

/// The memberships of `pg_auth_members` as `(role, member)` names.
fn memberships(conn: &PgConnection) -> Vec<(String, String)> {
    let mut stmt = conn
        .query(
            "SELECT r.rolname, m.rolname FROM pg_auth_members a \
             JOIN pg_roles r ON r.oid = a.roleid \
             JOIN pg_roles m ON m.oid = a.member \
             ORDER BY r.rolname, m.rolname",
        )
        .unwrap()
        .unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| (row[0].to_string(), row[1].to_string()))
        .collect()
}

fn membership(role: &str, member: &str) -> (String, String) {
    (role.to_string(), member.to_string())
}

#[test]
fn memberships_are_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE writers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("GRANT readers, writers TO alice").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    assert_eq!(
        memberships(&conn),
        vec![
            membership("readers", "alice"),
            membership("writers", "alice")
        ]
    );
    conn.execute("REVOKE readers FROM alice").unwrap();
    assert_eq!(memberships(&conn), vec![membership("writers", "alice")]);
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        memberships(&db.connect_postgres()),
        vec![membership("writers", "alice")]
    );
}

#[test]
fn memberships_are_kept_after_reopening_an_mvcc_database() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("GRANT readers TO alice").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        memberships(&db.connect_postgres()),
        vec![membership("readers", "alice")]
    );
}

#[test]
fn membership_granted_by_one_connection_is_visible_to_another() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE ROLE readers").unwrap();
    conn1.execute("CREATE ROLE alice").unwrap();
    assert_eq!(memberships(&conn2), vec![]);

    conn1.execute("GRANT readers TO alice").unwrap();

    assert_eq!(memberships(&conn2), vec![membership("readers", "alice")]);
    conn2.execute("REVOKE readers FROM alice").unwrap();
    assert_eq!(memberships(&conn1), vec![]);
}

/// A statement that fails on its last pair stores none of its memberships.
#[test]
fn grant_that_fails_grants_none_of_its_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();

    let error = conn.execute("GRANT readers, alice TO alice").unwrap_err();

    assert_eq!(
        error.to_string(),
        "role \"alice\" is a member of role \"alice\""
    );
    assert_eq!(memberships(&conn), vec![]);
    assert_eq!(memberships(&db.connect_postgres()), vec![]);
}

/// Removes the membership table, as a database made before tursopg stored
/// memberships has none.
fn drop_membership_table(db: &TempDatabase) {
    let conn = db.connect_postgres();
    conn.inner()
        .prepare_engine_schema_statement("DROP TABLE __turso_internal_auth_members")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    conn.close().unwrap();
}

#[test]
fn database_without_the_membership_table_opens_read_only_without_memberships() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x INT)").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop_membership_table(&db);
    let TempDatabase { path, io, db } = db;
    drop(db);

    let db = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default() | turso_core::OpenFlags::ReadOnly,
        turso_core::DatabaseOpts::new().with_encryption(true),
    )
    .unwrap();
    let conn = PgConnection::connect(&db).unwrap();

    assert_eq!(memberships(&conn), vec![]);
    conn.execute("SET ROLE alice").unwrap();
    let error = conn.query("SELECT x FROM t").err().unwrap();
    assert_eq!(error.to_string(), "permission denied for table t");
    conn.execute("RESET ROLE").unwrap();
    assert!(conn.execute("GRANT readers TO alice").is_err());
}

#[test]
fn database_without_the_membership_table_gets_it_when_opened() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop_membership_table(&db);

    let db = db.reopen();

    let conn = db.connect_postgres();
    assert_eq!(memberships(&conn), vec![]);
    conn.execute("GRANT readers TO alice").unwrap();
    assert_eq!(memberships(&conn), vec![membership("readers", "alice")]);
}

#[test]
fn database_without_the_membership_table_has_no_memberships_after_its_schema_is_read_again() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop_membership_table(&db);
    let conn = PgConnection::connect(&reopen_read_only(db)).unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    assert_eq!(memberships(&conn), vec![]);
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "readers", "alice"]
    );
}

/// A database can have the relation catalog and no roles table. Its roles
/// and memberships are then not read, and its relations still are.
#[test]
fn database_without_the_roles_table_opens_read_only_without_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x INT)").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop_membership_table(&db);
    drop_roles_table(&db);
    let conn = PgConnection::connect(&reopen_read_only(db)).unwrap();

    for read in ["after open", "after the schema is read again"] {
        assert_eq!(
            role_names(&conn),
            vec!["postgres", "pg_database_owner"],
            "{read}"
        );
        assert_eq!(memberships(&conn), vec![], "{read}");
        let mut stmt = conn
            .query("SELECT relname FROM pg_class WHERE relname = 't'")
            .unwrap()
            .unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::build_text("t")]],
            "{read}"
        );
        conn.inner().force_reparse_schema_without_publish().unwrap();
    }
}

/// Removes the roles table, which a database may lack while it has the
/// relation catalog.
fn drop_roles_table(db: &TempDatabase) {
    let conn = db.connect_postgres();
    conn.inner()
        .prepare_engine_schema_statement("DROP TABLE __turso_internal_authid")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    conn.close().unwrap();
}

/// Closes `db` and opens its file again read-only. Such an open creates
/// none of the tables that the file lacks.
fn reopen_read_only(db: TempDatabase) -> std::sync::Arc<turso_core::Database> {
    let TempDatabase { path, io, db } = db;
    drop(db);
    turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default() | turso_core::OpenFlags::ReadOnly,
        turso_core::DatabaseOpts::new().with_encryption(true),
    )
    .unwrap()
}

/// The in-memory catalog skips a membership it knows, but only the stored
/// table can keep a membership unique against every writer of the file. A
/// duplicate would leave the database unable to open.
#[test]
fn stored_membership_table_keeps_memberships_unique() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE readers").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("GRANT readers TO alice").unwrap();

    let error = conn
        .inner()
        .prepare_engine_schema_statement(
            "INSERT INTO __turso_internal_auth_members \
             SELECT NULL, roleid, member, grantor, admin_option, inherit_option, set_option \
             FROM __turso_internal_auth_members",
        )
        .unwrap()
        .run_ignore_rows()
        .unwrap_err();
    assert!(error.to_string().contains("UNIQUE"), "{error}");
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    assert_eq!(
        memberships(&db.connect_postgres()),
        vec![membership("readers", "alice")]
    );
}

/// A session that logs in as `name`.
fn connect_as(db: &TempDatabase, name: &str) -> PgConnection {
    PgConnection::connect_with(
        &db.db,
        ConnectOpts::new(SessionRole::Named(name.to_string())),
    )
    .unwrap()
}

/// A session that logs in as a role that is not a superuser may switch to
/// the roles it is a member of, and to no other.
#[test]
fn session_role_may_set_a_role_it_is_a_member_of() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    for role in ["app_login NOINHERIT", "guest", "reader", "other"] {
        superuser.execute(format!("CREATE ROLE {role}")).unwrap();
    }
    superuser
        .execute("GRANT guest, reader TO app_login")
        .unwrap();

    let app_login = connect_as(&db, "app_login");

    app_login.execute("SET ROLE reader").unwrap();
    assert_eq!(current_user(&app_login), "reader");
    app_login.execute("SET ROLE guest").unwrap();
    assert_eq!(current_user(&app_login), "guest");
    let error = app_login.execute("SET ROLE other").unwrap_err();
    assert_eq!(error.to_string(), "permission denied to set role \"other\"");
    let error = app_login.execute("SET ROLE postgres").unwrap_err();
    assert_eq!(
        error.to_string(),
        "permission denied to set role \"postgres\""
    );
    app_login.execute("RESET ROLE").unwrap();
    assert_eq!(current_user(&app_login), "app_login");
}

#[test]
fn session_role_may_set_a_role_it_reaches_through_other_roles() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    for role in ["alice", "middle NOINHERIT", "top"] {
        superuser.execute(format!("CREATE ROLE {role}")).unwrap();
    }
    superuser.execute("GRANT top TO middle").unwrap();
    superuser.execute("GRANT middle TO alice").unwrap();

    let alice = connect_as(&db, "alice");

    alice.execute("SET ROLE top").unwrap();
    assert_eq!(current_user(&alice), "top");
}

#[test]
fn session_role_may_not_set_a_role_after_its_membership_is_revoked() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    superuser.execute("CREATE ROLE alice").unwrap();
    superuser.execute("CREATE ROLE readers").unwrap();
    superuser.execute("GRANT readers TO alice").unwrap();
    let alice = connect_as(&db, "alice");
    alice.execute("SET ROLE readers").unwrap();
    alice.execute("RESET ROLE").unwrap();

    superuser.execute("REVOKE readers FROM alice").unwrap();

    let error = alice.execute("SET ROLE readers").unwrap_err();
    assert_eq!(
        error.to_string(),
        "permission denied to set role \"readers\""
    );
}

/// A session that logs in as a role without privileges of its own switches
/// to a role it is a member of, and a policy for that role decides which
/// rows it reads.
#[test]
fn logged_in_role_reads_through_a_policy_after_set_role() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    for sql in [
        "CREATE ROLE guest NOINHERIT",
        "CREATE ROLE reader NOINHERIT",
        "CREATE ROLE app_login NOINHERIT",
        "GRANT guest, reader TO app_login",
        "CREATE TABLE todos (id INT PRIMARY KEY, owner TEXT)",
        "INSERT INTO todos VALUES (1, 'alice'), (2, 'bob')",
        "ALTER TABLE todos ENABLE ROW LEVEL SECURITY",
        "GRANT SELECT ON todos TO guest, reader",
        "CREATE POLICY alices ON todos FOR SELECT TO reader USING (owner = 'alice')",
    ] {
        superuser.execute(sql).unwrap();
    }

    let app_login = connect_as(&db, "app_login");

    let error = app_login.query("SELECT id FROM todos").err().unwrap();
    assert_eq!(error.to_string(), "permission denied for table todos");
    app_login.execute("SET ROLE guest").unwrap();
    assert_eq!(todo_ids(&app_login), Vec::<String>::new());
    app_login.execute("SET ROLE reader").unwrap();
    assert_eq!(todo_ids(&app_login), vec!["1"]);
}

fn todo_ids(conn: &PgConnection) -> Vec<String> {
    let mut stmt = conn
        .query("SELECT id FROM todos ORDER BY id")
        .unwrap()
        .unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}

#[test]
fn dropped_role_is_not_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE ROLE alice").unwrap();
    assert_eq!(
        role_names(&conn2),
        vec!["postgres", "pg_database_owner", "alice"]
    );

    conn1.execute("DROP ROLE alice").unwrap();

    assert_eq!(role_names(&conn2), vec!["postgres", "pg_database_owner"]);
}

#[test]
fn drop_role_that_fails_drops_none_of_its_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();

    let error = conn.execute("DROP ROLE alice, bob").unwrap_err();

    assert_eq!(error.to_string(), "role \"bob\" does not exist");
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "alice"]
    );
}

#[test]
fn dropped_roles_and_their_memberships_stay_dropped_after_reopening() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for role in ["readers", "writers", "alice", "bob"] {
        conn.execute(format!("CREATE ROLE {role}")).unwrap();
    }
    conn.execute("GRANT readers, writers TO alice, bob")
        .unwrap();
    conn.execute("DROP ROLE writers, alice").unwrap();
    assert_eq!(memberships(&conn), vec![membership("readers", "bob")]);
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    assert_eq!(
        role_names(&conn),
        vec!["postgres", "pg_database_owner", "readers", "bob"]
    );
    assert_eq!(memberships(&conn), vec![membership("readers", "bob")]);
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("GRANT readers TO alice").unwrap();
    assert_eq!(
        memberships(&conn),
        vec![membership("readers", "alice"), membership("readers", "bob")]
    );
}

/// The DETAIL lists each object that depends on the role, as PostgreSQL
/// describes it, and names a relation that the search path does not find
/// by its bare name with its schema.
#[test]
fn drop_role_lists_the_objects_that_depend_on_it() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE ROLE alice",
        "CREATE SCHEMA app",
        "CREATE TABLE owned (id SERIAL)",
        "ALTER TABLE owned OWNER TO alice",
        "CREATE INDEX owned_id ON owned (id)",
        "CREATE TABLE app.owned (x INT)",
        "ALTER TABLE app.owned OWNER TO alice",
        "CREATE TABLE shared (x INT)",
        "GRANT SELECT ON shared TO alice",
        "CREATE POLICY readers ON shared TO alice USING (true)",
    ] {
        conn.execute(sql).unwrap();
    }

    let error = conn.execute("DROP ROLE alice").unwrap_err();

    let report = ErrorReport::from(error.core_error());
    assert_eq!(report.sqlstate, "2BP01");
    assert_eq!(
        report.message,
        "role \"alice\" cannot be dropped because some objects depend on it"
    );
    assert_eq!(
        report.detail.as_deref(),
        Some(
            "owner of sequence owned_id_seq\n\
             owner of table owned\n\
             owner of table app.owned\n\
             privileges for table shared\n\
             target of policy readers on table shared"
        )
    );
}

/// The DETAIL quotes names the way PostgreSQL does and lists the objects in
/// the order they were created, policies among relations.
#[test]
fn drop_role_detail_names_objects_as_postgresql_does() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE ROLE alice",
        "CREATE SCHEMA \"my s\"",
        "CREATE TABLE \"my t\" (x INT)",
        "CREATE TABLE \"my s\".\"q\" (x INT)",
        "CREATE TABLE later (x INT)",
        "GRANT SELECT ON \"my t\" TO alice",
        "GRANT SELECT ON \"my s\".\"q\" TO alice",
        "GRANT SELECT ON later TO alice",
        "CREATE POLICY p ON \"my t\" TO alice USING (true)",
    ] {
        conn.execute(sql).unwrap();
    }

    let error = conn.execute("DROP ROLE alice").unwrap_err();

    let report = ErrorReport::from(error.core_error());
    assert_eq!(
        report.detail.as_deref(),
        Some(
            "privileges for table \"my t\"\n\
             privileges for table \"my s\".q\n\
             privileges for table later\n\
             target of policy p on table \"my t\""
        )
    );
}

/// The DETAIL gives a table the name it was created with, not the
/// lowercase name it is stored under.
#[test]
fn drop_role_detail_keeps_the_case_of_table_names() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE ROLE alice",
        "CREATE TABLE \"MixT\" (x INT)",
        "ALTER TABLE \"MixT\" OWNER TO alice",
        "CREATE SCHEMA app",
        "CREATE TABLE app.\"Other\" (x INT)",
        "ALTER TABLE app.\"Other\" OWNER TO alice",
    ] {
        conn.execute(sql).unwrap();
    }

    let error = conn.execute("DROP ROLE alice").unwrap_err();

    let report = ErrorReport::from(error.core_error());
    assert_eq!(
        report.detail.as_deref(),
        Some(
            "owner of table \"MixT\"\n\
             owner of table app.\"Other\""
        )
    );
}

#[test]
fn role_without_privileges_is_told_who_may_create_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("CREATE ROLE bob").unwrap_err();

    let report = ErrorReport::from(error.core_error());
    assert_eq!(report.sqlstate, "42501");
    assert_eq!(report.message, "permission denied to create role");
    assert_eq!(
        report.detail.as_deref(),
        Some("Only roles with the CREATEROLE attribute may create roles.")
    );
}

#[test]
fn role_without_privileges_is_told_who_may_drop_roles() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("DROP ROLE alice").unwrap_err();

    let report = ErrorReport::from(error.core_error());
    assert_eq!(report.sqlstate, "42501");
    assert_eq!(report.message, "permission denied to drop role");
    assert_eq!(
        report.detail.as_deref(),
        Some(
            "Only roles with the CREATEROLE attribute and the ADMIN option on the target \
             roles may drop roles."
        )
    );
}

/// A session that logs in as a stored superuser may drop roles, but not
/// its own role or the built-in superuser.
#[test]
fn superuser_session_cannot_drop_its_session_role_or_the_built_in_superuser() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE admin").unwrap();
    conn.execute("CREATE ROLE other").unwrap();
    conn.inner()
        .prepare_engine_schema_statement(
            "UPDATE __turso_internal_authid SET rolsuper = 1 WHERE rolname = 'admin'",
        )
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    conn.close().unwrap();
    drop(conn);
    let db = db.reopen();
    let admin = connect_as(&db, "admin");

    let error = admin.execute("DROP ROLE admin").unwrap_err();
    assert_eq!(error.to_string(), "current user cannot be dropped");
    admin.execute("SET ROLE other").unwrap();
    admin.execute("RESET ROLE").unwrap();
    let error = admin.execute("DROP ROLE postgres").unwrap_err();
    let report = ErrorReport::from(error.core_error());
    assert_eq!(report.sqlstate, "2BP01");
    assert_eq!(
        report.message,
        "cannot drop role postgres because it is required by the database system"
    );

    // A superuser that switched to another superuser role still cannot drop
    // the role it logged in as.
    let conn = db.connect_postgres();
    conn.execute("SET ROLE admin").unwrap();
    let error = conn.execute("DROP ROLE postgres").unwrap_err();
    assert_eq!(error.to_string(), "session user cannot be dropped");

    admin.execute("DROP ROLE other").unwrap();
    assert_eq!(
        role_names(&admin),
        vec!["postgres", "pg_database_owner", "admin"]
    );
}

/// PostgreSQL lets a superuser drop the role another session runs as. That
/// session keeps running, as a role without privileges.
#[test]
fn session_of_a_dropped_role_keeps_running_without_privileges() {
    let db = TempDatabase::builder().build();
    let superuser = db.connect_postgres();
    superuser.execute("CREATE ROLE alice").unwrap();
    superuser.execute("CREATE TABLE t (x INT)").unwrap();
    let alice = connect_as(&db, "alice");

    superuser.execute("DROP ROLE alice").unwrap();

    let mut stmt = alice.query("SELECT 1").unwrap().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0].to_string(), "1");
    let Err(error) = alice.query("SELECT x FROM t") else {
        panic!("the session of a dropped role read table t");
    };
    assert_eq!(error.to_string(), "permission denied for table t");
}
