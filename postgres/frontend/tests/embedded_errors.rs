use std::sync::Arc;
use turso_core::Value;
use turso_pg::{Connection, DatabaseOpts, Error, ErrorReport, LimboError, OpenFlags};

fn pg_connection() -> (Arc<dyn turso_core::IO>, Connection) {
    let (io, db) =
        turso_pg::open_database(":memory:", None, OpenFlags::default(), DatabaseOpts::new())
            .unwrap();
    let conn = Connection::connect(&db).unwrap();
    (io, conn)
}

fn exec_err(conn: &Connection, sql: &str) -> Error {
    conn.execute(sql)
        .expect_err(&format!("statement must fail: {sql}"))
}

fn constraint_violation(error: &Error) -> &ErrorReport {
    assert!(
        matches!(error.core_error(), LimboError::ConstraintViolation(_)),
        "{error:?}"
    );
    error
}

#[test]
fn embedded_execute_formats_postgres_errors() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let error = conn.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert_eq!(
        error.to_string(),
        "duplicate key value violates unique constraint \"t_pkey\""
    );
}

#[test]
fn embedded_statement_methods_format_postgres_errors() {
    let (io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    for method in 0..8 {
        let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        let mut error = None;
        for _ in 0..100 {
            let result = match method {
                0 => stmt.step().map(|_| ()).map_err(|e| e.to_string()),
                1 => stmt
                    .step_with_waker(std::task::Waker::noop())
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                2 => stmt.run_ignore_rows().map_err(|e| e.to_string()),
                3 => stmt
                    .run_collect_rows()
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                4 => stmt
                    .run_with_row_callback(|_| Ok(()))
                    .map_err(|e| e.to_string()),
                5 => stmt
                    .run_ignore_rows_nonblock()
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                6 => stmt
                    .run_with_row_callback_nonblock(|_| Ok(()))
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                7 => stmt
                    .run_one_step_blocking(|| Ok(()), || Ok(()))
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                _ => unreachable!(),
            };
            if let Err(message) = result {
                error = Some(message);
                break;
            }
            io.step().unwrap();
        }
        assert_eq!(
            error.as_deref(),
            Some("duplicate key value violates unique constraint \"t_pkey\""),
            "method {method}"
        );
    }
}

#[test]
fn embedded_prepare_errors_keep_the_core_source() {
    let (_io, conn) = pg_connection();
    let error = conn.prepare("SELECT * FROM missing_table").err().unwrap();
    assert!(std::error::Error::source(&error).is_some(), "{error:?}");
    assert_eq!(error.sqlstate, "42P01");
}

#[test]
fn embedded_query_runner_returns_frontend_errors() {
    let (_io, conn) = pg_connection();
    let error = conn
        .query_runner(b"SELECT * FROM missing_table")
        .next()
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.sqlstate, "42P01");
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn embedded_callbacks_preserve_frontend_errors() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let expected = conn.execute("INSERT INTO t VALUES (1)").unwrap_err();
    for nonblocking in [false, true] {
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        let error = if nonblocking {
            stmt.run_with_row_callback_nonblock(|_| Err(expected.clone()))
                .err()
                .unwrap()
        } else {
            stmt.run_with_row_callback(|_| Err(expected.clone()))
                .unwrap_err()
        };
        assert_eq!(error.to_string(), expected.to_string());
        assert_eq!(error.sqlstate, expected.sqlstate);
        assert!(matches!(
            error.core_error(),
            LimboError::ConstraintViolation(_)
        ));
    }
}

#[test]
fn primary_key_violation_reports_structured_fields() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_pk (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t_pk VALUES (1, 'alice')")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO t_pk VALUES (1, 'bob')");
    assert_eq!(e.sqlstate, "23505");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_pk_pkey"));
    assert_eq!(cv.table_name.as_deref(), Some("t_pk"));
    assert_eq!(cv.detail.as_deref(), Some("Key (id)=(1) already exists."));
    assert_eq!(
        e.message,
        "duplicate key value violates unique constraint \"t_pk_pkey\""
    );
}

#[test]
fn named_unique_violation_reports_declared_constraint_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_u (id INT PRIMARY KEY, code TEXT CONSTRAINT t_u_code_uq UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t_u VALUES (1, 'a')").unwrap();
    let e = exec_err(&conn, "INSERT INTO t_u VALUES (2, 'a')");
    assert_eq!(e.sqlstate, "23505");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_u_code_uq"));
    assert_eq!(cv.detail.as_deref(), Some("Key (code)=(a) already exists."));
}

#[test]
fn unnamed_unique_violation_reports_generated_constraint_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_uu (id INT PRIMARY KEY, code TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t_uu VALUES (1, 'a')").unwrap();
    let e = exec_err(&conn, "INSERT INTO t_uu VALUES (2, 'a')");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_uu_code_key"));
}

#[test]
fn foreign_key_violation_reports_structured_fields_with_fk_on_by_default() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_parent (id INT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE t_child (id INT PRIMARY KEY, pid INT REFERENCES t_parent(id))")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO t_child VALUES (1, 99)");
    assert_eq!(e.sqlstate, "23503");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_child_pid_fkey"));
    assert_eq!(cv.table_name.as_deref(), Some("t_child"));
    assert_eq!(
        cv.detail.as_deref(),
        Some("Key (pid)=(99) is not present in table \"t_parent\".")
    );
    assert_eq!(
        e.message,
        "insert or update on table \"t_child\" violates foreign key constraint \"t_child_pid_fkey\""
    );
}

#[test]
fn not_null_violation_reports_column_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_nn (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO t_nn VALUES (1, NULL)");
    assert_eq!(e.sqlstate, "23502");
    let cv = constraint_violation(&e);
    assert_eq!(cv.column_name.as_deref(), Some("name"));
    assert_eq!(cv.table_name.as_deref(), Some("t_nn"));
    assert_eq!(
        e.message,
        "null value in column \"name\" of relation \"t_nn\" violates not-null constraint"
    );
}

#[test]
fn check_violation_reports_constraint_and_table() {
    let (_io, conn) = pg_connection();
    conn.execute(
        "CREATE TABLE t_ck (id INT PRIMARY KEY, age INT CONSTRAINT age_positive CHECK (age > 0))",
    )
    .unwrap();
    let e = exec_err(&conn, "INSERT INTO t_ck VALUES (1, -5)");
    assert_eq!(e.sqlstate, "23514");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("age_positive"));
    assert_eq!(cv.table_name.as_deref(), Some("t_ck"));
    assert_eq!(
        e.message,
        "new row for relation \"t_ck\" violates check constraint \"age_positive\""
    );
}

#[test]
fn errors_report_postgres_sqlstates() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t_present (id INT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE SEQUENCE s_exhaust START 2 INCREMENT 1 MINVALUE 1 MAXVALUE 2")
        .unwrap();
    conn.execute("SELECT nextval('s_exhaust')").unwrap();
    for (sql, code) in [
        ("SELECTT 1", "42601"),
        ("SELECT * FROM no_such_table_xyz", "42P01"),
        ("SELECT bad_col FROM t_present", "42703"),
        ("SELECT nonexistent_fn_xyz()", "42883"),
        ("CREATE TABLE t_present (id INT)", "42P07"),
        ("SELECT abs(-9223372036854775808)", "22003"),
        ("SELECT zeroblob(1000000001)", "22001"),
        ("SELECT nextval('s_exhaust')", "2200H"),
    ] {
        let e = exec_err(&conn, sql);
        assert_eq!(e.sqlstate, code, "wrong SQLSTATE for: {sql}, got {e:?}");
    }
}

#[test]
fn nested_begin_reports_active_sql_transaction() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let e = exec_err(&conn, "BEGIN");
    assert_eq!(e.sqlstate, "25001");
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn primary_key_update_reports_structured_fields() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let e = exec_err(&conn, "UPDATE t SET id = 1 WHERE id = 2");
    assert_eq!(e.sqlstate, "23505");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_pkey"));
    assert_eq!(cv.table_name.as_deref(), Some("t"));
    assert_eq!(cv.detail.as_deref(), Some("Key (id)=(1) already exists."));
    let mut stmt = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}

#[test]
fn named_primary_key_reports_declared_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT CONSTRAINT custom_pk PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let e = exec_err(&conn, "INSERT INTO t VALUES (1)");
    assert_eq!(
        constraint_violation(&e).constraint_name.as_deref(),
        Some("custom_pk")
    );
}

#[test]
fn text_primary_key_reports_pkey_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('a'), ('b')").unwrap();
    for sql in [
        "INSERT INTO t VALUES ('a')",
        "UPDATE t SET id = 'a' WHERE id = 'b'",
    ] {
        let e = exec_err(&conn, sql);
        assert_eq!(
            constraint_violation(&e).constraint_name.as_deref(),
            Some("t_pkey")
        );
    }
}

#[test]
fn named_foreign_key_reports_declared_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (pid INT CONSTRAINT custom_fk REFERENCES parent(id))")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO child VALUES (99)");
    assert_eq!(
        constraint_violation(&e).constraint_name.as_deref(),
        Some("custom_fk")
    );
}

#[test]
fn unnamed_check_reports_generated_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (x INT CHECK (x > 0))")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO t VALUES (-1)");
    assert_eq!(
        constraint_violation(&e).constraint_name.as_deref(),
        Some("t_x_check")
    );
}

#[test]
fn table_constraints_report_declared_names() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE t (id INT, pid INT, x INT, CONSTRAINT named_pk PRIMARY KEY (id), CONSTRAINT named_fk FOREIGN KEY (pid) REFERENCES parent(id), CONSTRAINT named_check CHECK (x > 0))").unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 1)").unwrap();
    for (sql, name) in [
        ("INSERT INTO t VALUES (1, 1, 1)", "named_pk"),
        ("INSERT INTO t VALUES (2, 99, 1)", "named_fk"),
        ("INSERT INTO t VALUES (2, 1, -1)", "named_check"),
    ] {
        let e = exec_err(&conn, sql);
        assert_eq!(
            constraint_violation(&e).constraint_name.as_deref(),
            Some(name),
            "{sql}"
        );
    }
}

#[test]
fn unnamed_checks_have_distinct_names() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (x INT CHECK (x > 0) CHECK (x < 10))")
        .unwrap();
    for (value, name) in [(-1, "t_x_check"), (11, "t_x_check1")] {
        let e = exec_err(&conn, &format!("INSERT INTO t VALUES ({value})"));
        assert_eq!(
            constraint_violation(&e).constraint_name.as_deref(),
            Some(name)
        );
    }
}

#[test]
fn unnamed_check_with_multiple_columns_reports_table_name() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (a INT, b INT, CHECK (b > 0 AND a > 0))")
        .unwrap();
    let e = exec_err(&conn, "INSERT INTO t VALUES (-1, -1)");
    assert_eq!(
        constraint_violation(&e).constraint_name.as_deref(),
        Some("t_check")
    );
}

#[test]
fn foreign_key_update_keeps_key_values_when_later_rows_reuse_registers() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE parent (a INT, b TEXT, PRIMARY KEY (a, b))")
        .unwrap();
    conn.execute("CREATE TABLE child (id INT PRIMARY KEY, a INT, b TEXT, CONSTRAINT child_fk FOREIGN KEY (a, b) REFERENCES parent(a, b))").unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 1, 'a'), (2, 1, 'a')")
        .unwrap();
    let mut stmt = conn
        .prepare("UPDATE child SET a = CASE WHEN id = 1 THEN 99 ELSE 1 END")
        .unwrap();
    for _ in 0..2 {
        let e = stmt.run_ignore_rows().unwrap_err();
        let cv = constraint_violation(&e);
        assert_eq!(cv.constraint_name.as_deref(), Some("child_fk"));
        assert_eq!(
            cv.detail.as_deref(),
            Some("Key (a, b)=(99, a) is not present in table \"parent\".")
        );
        stmt.reset().unwrap();
    }
}

#[test]
fn foreign_key_update_reports_structured_fields() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 1)").unwrap();
    let e = exec_err(&conn, "UPDATE child SET pid = 99 WHERE id = 1");
    assert_eq!(e.sqlstate, "23503");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("child_pid_fkey"));
    assert_eq!(cv.table_name.as_deref(), Some("child"));
    assert_eq!(
        cv.detail.as_deref(),
        Some("Key (pid)=(99) is not present in table \"parent\".")
    );
    assert_eq!(
        e.message,
        "insert or update on table \"child\" violates foreign key constraint \"child_pid_fkey\""
    );
    let mut stmt = conn.prepare("SELECT pid FROM child").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(1)]]
    );
}

#[test]
fn structured_foreign_key_errors_preserve_sqlite_transaction_behavior() {
    for sql in [
        "UPDATE OR FAIL child SET pid = 99",
        "INSERT OR ROLLBACK INTO child VALUES (2, 99)",
        "INSERT OR FAIL INTO child VALUES (2, 1), (3, 99)",
    ] {
        let (_pg_io, pg) = pg_connection();
        let (_sqlite_io, sqlite) = sqlite_connection();
        let mut outcomes = Vec::new();
        for (dialect, conn) in [("sqlite", &sqlite), ("postgres", pg.inner())] {
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            )
            .unwrap();
            conn.execute("INSERT INTO parent VALUES (1)").unwrap();
            conn.execute("INSERT INTO child VALUES (1, 1)").unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO parent VALUES (7)").unwrap();
            let error = conn.execute(sql).unwrap_err();
            assert_eq!(ErrorReport::from(&error).sqlstate, "23503", "{sql}");
            if dialect == "sqlite" {
                assert!(
                    matches!(error, LimboError::ForeignKeyConstraint(_)),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(error, LimboError::ConstraintViolation(_)),
                    "{error:?}"
                );
            }
            let mut stmt = conn
                .prepare("SELECT id, pid FROM child ORDER BY id")
                .unwrap();
            let children = stmt.run_collect_rows().unwrap();
            let mut stmt = conn.prepare("SELECT id FROM parent ORDER BY id").unwrap();
            let parents = stmt.run_collect_rows().unwrap();
            outcomes.push((children, parents, conn.get_auto_commit()));
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "reporting changed transaction behavior for {sql}"
        );
    }
}

fn sqlite_connection() -> (Arc<dyn turso_core::IO>, Arc<turso_core::Connection>) {
    let io = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file_with_flags(
        io.clone(),
        ":memory:",
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(turso_core::ConnectOpts::new()).unwrap();
    conn.set_foreign_keys_enabled(true);
    (io, conn)
}
