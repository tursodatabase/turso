use std::sync::Arc;
use turso_core::Value;
use turso_pg::{
    Connection, DatabaseOpts, Error, ErrorReport, LimboError, OpenFlags, StepResult,
    TransactionState,
};

fn pg_connection() -> (Arc<dyn turso_core::IO>, Connection) {
    let (io, db) =
        turso_pg::open_database(":memory:", None, OpenFlags::default(), DatabaseOpts::new())
            .unwrap();
    let conn = Connection::connect(&db).unwrap();
    (io, conn)
}

fn pg_connection_with_fk() -> (Arc<dyn turso_core::IO>, Connection) {
    let (io, conn) = pg_connection();
    conn.inner().set_foreign_keys_enabled(true);
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
fn foreign_key_violation_reports_structured_fields() {
    let (_io, conn) = pg_connection_with_fk();
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
fn nested_begin_keeps_the_open_transaction() {
    // PostgreSQL warns "there is already a transaction in progress" for BEGIN
    // inside a transaction block and keeps the transaction.
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    conn.execute("BEGIN").unwrap();
    assert!(!conn.inner().get_auto_commit());
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn division_by_zero_raises_22012() {
    let (_io, conn) = pg_connection();
    for sql in [
        "SELECT 1/0",
        "SELECT 1 % 0",
        "SELECT 1/0.0",
        "SELECT 1.5/0",
        "SELECT mod(1, 0)",
        "SELECT mod(1.5, 0.0)",
    ] {
        let e = exec_err(&conn, sql);
        assert!(
            matches!(e.core_error(), LimboError::DivisionByZero),
            "expected DivisionByZero for {sql}, got: {e:?}"
        );
        assert_eq!(e.sqlstate, "22012");
        assert_eq!(e.message, "division by zero");
    }
}

#[test]
fn null_divided_by_zero_returns_null() {
    let (_io, conn) = pg_connection();
    let mut stmt = conn.prepare("SELECT NULL/0").unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Null), "got: {:?}", rows[0][0]);
}

#[test]
fn quoted_zero_divisors_raise_22012() {
    let (_io, conn) = pg_connection();
    for sql in ["SELECT 1 / '0'", "SELECT 1 % '0'", "SELECT 1.5 / '0.0'"] {
        let e = exec_err(&conn, sql);
        assert_eq!(e.sqlstate, "22012", "{sql}: {e:?}");
    }
}

#[test]
fn non_numeric_string_divisor_raises_22p02() {
    let (_io, conn) = pg_connection();
    for sql in [
        "SELECT 1 / 'abc'",
        "SELECT 1 % 'abc'",
        "SELECT 1 / ''",
        "SELECT mod(1, 'abc')",
    ] {
        let e = exec_err(&conn, sql);
        assert!(
            matches!(e.core_error(), LimboError::InvalidTextRepresentation(_)),
            "expected InvalidTextRepresentation for {sql}, got: {e:?}"
        );
        assert_eq!(e.sqlstate, "22P02", "{sql}: {e:?}");
    }
}

#[test]
fn lookup_and_input_errors_report_postgres_sqlstates() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (j jsonb)").unwrap();
    for (sql, code) in [
        ("SELECT current_setting('nosuch.x')", "42704"),
        ("SELECT set_config('nodot', 'x', false)", "42704"),
        ("SELECT set_config('a..b', 'x', false)", "42602"),
        ("SELECT current_setting('a..b')", "42704"),
        ("INSERT INTO t VALUES ('not json')", "22P02"),
        ("SELECT * FROM nosuchschema.t", "42P01"),
        ("INSERT INTO nosuchschema.t VALUES ('{}')", "42P01"),
        ("UPDATE nosuchschema.t SET j = '{}'", "42P01"),
        ("DELETE FROM nosuchschema.t", "42P01"),
        ("SELECT nextval('nosuchschema.q')", "42P01"),
        ("SELECT nextval('public.q')", "42P01"),
        ("SELECT currval('nosuchschema.q')", "42P01"),
        ("SELECT setval('nosuchschema.q', 1)", "42P01"),
        ("DROP VIEW nosuchschema.v", "3F000"),
        ("CREATE VIEW nosuchschema.v AS SELECT 1", "3F000"),
        ("CREATE SEQUENCE nosuchschema.q", "3F000"),
        ("CREATE TABLE nosuchschema.t (a INT)", "3F000"),
        ("DROP TABLE nosuchschema.t", "3F000"),
        ("ALTER TABLE nosuchschema.t ADD COLUMN b INT", "3F000"),
        ("ALTER TABLE nosuchschema.t RENAME TO u", "3F000"),
        ("CREATE INDEX i ON nosuchschema.t (a)", "3F000"),
        ("DROP INDEX nosuchschema.i", "3F000"),
        ("DROP SEQUENCE nosuchschema.q", "3F000"),
        ("DROP SCHEMA nosuchschema", "3F000"),
        ("SELECT 1 x 'malformed JSON'", "42601"),
    ] {
        let e = exec_err(&conn, sql);
        assert_eq!(e.sqlstate, code, "{sql}: {e:?}");
    }
    conn.execute("SET search_path = nosuchschema").unwrap();
    for sql in ["CREATE TABLE z (a INT)", "CREATE SEQUENCE zq"] {
        let e = exec_err(&conn, sql);
        assert_eq!(e.sqlstate, "3F000", "{sql}: {e:?}");
    }
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
    let (_io, conn) = pg_connection_with_fk();
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
    let (_io, conn) = pg_connection_with_fk();
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
fn upsert_do_update_unique_violation_reports_structured_fields() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (a INT PRIMARY KEY, c TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'q')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    let e = exec_err(
        &conn,
        "INSERT INTO t VALUES (1, 'q') ON CONFLICT(a) DO UPDATE SET c = 'x'",
    );
    assert_eq!(e.sqlstate, "23505");
    let cv = constraint_violation(&e);
    assert_eq!(cv.constraint_name.as_deref(), Some("t_c_key"));
    assert_eq!(cv.table_name.as_deref(), Some("t"));
}

#[test]
fn foreign_key_update_keeps_key_values_when_later_rows_reuse_registers() {
    let (_io, conn) = pg_connection_with_fk();
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
    let (_io, conn) = pg_connection_with_fk();
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
fn zero_divisors_raise_postgres_errors() {
    let (_pg_io, pg) = pg_connection();
    for sql in [
        "SELECT 1 / 0",
        "SELECT 1 % 0",
        "SELECT 1 / 0.0",
        "SELECT 1.5 / 0",
        "SELECT 1 / '0'",
    ] {
        let error = exec_err(&pg, sql);
        assert_eq!(error.sqlstate, "22012", "{sql}");
    }
    for sql in ["SELECT NULL / 0", "SELECT NULL % 0"] {
        let mut stmt = pg.prepare(sql).unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![Value::Null]],
            "{sql}"
        );
    }
}

#[test]
fn structured_foreign_key_errors_preserve_sqlite_transaction_behavior() {
    for sql in [
        "UPDATE OR FAIL child SET pid = 99",
        "INSERT OR ROLLBACK INTO child VALUES (2, 99)",
        "INSERT OR FAIL INTO child VALUES (2, 1), (3, 99)",
    ] {
        let (_pg_io, pg) = pg_connection_with_fk();
        let (_sqlite_io, sqlite) = sqlite_connection();
        let mut outcomes = Vec::new();
        for (dialect, conn) in [("sqlite", &sqlite), ("postgres", pg.inner())] {
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .unwrap();
            // The frontend binds the foreign key's table to its schema; a
            // table created through core alone references the root namespace.
            let create_child =
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))";
            if dialect == "postgres" {
                pg.execute(create_child).unwrap();
            } else {
                conn.execute(create_child).unwrap();
            }
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

#[test]
fn runtime_error_in_explicit_transaction_aborts_it() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    let e = exec_err(&conn, "SELECT 1");
    assert_eq!(
        e.sqlstate, "25P02",
        "statements after a runtime error in an explicit tx must return 25P02"
    );
    assert_eq!(
        e.message,
        "current transaction is aborted, commands ignored until end of transaction block"
    );
    conn.execute("ROLLBACK").unwrap();
    conn.execute("SELECT 1").unwrap();
}

#[test]
fn commit_of_aborted_transaction_rolls_back() {
    use std::sync::atomic::Ordering::Relaxed;
    use turso_core::IO;

    let io = Arc::new(CountedYieldIO::default());
    let db = turso_pg::open_database_with_io(
        io.clone(),
        "aborted-block.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
    )
    .unwrap();
    let conn = Connection::connect(&db).unwrap();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let waker = std::task::Waker::from(Arc::new(WakeCounter::default()));
    for sql in ["COMMIT", "ROLLBACK"] {
        let mut end = conn.prepare(sql).unwrap();
        conn.execute("BEGIN; INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(exec_err(&conn, "SELECT 1/0").sqlstate, "22012");
        loop {
            let before = io.steps.load(Relaxed);
            let result = end.step_with_waker(&waker).unwrap();
            assert_eq!(
                io.steps.load(Relaxed),
                before,
                "aborted block cleanup pumped I/O"
            );
            match result {
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    assert_eq!(conn.transaction_state(), TransactionState::Aborted);
                    assert_eq!(end.command_tag_override(), None);
                    io.step().unwrap();
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert_eq!(
            end.command_tag_override(),
            (sql == "COMMIT").then_some("ROLLBACK")
        );
        assert_eq!(conn.transaction_state(), TransactionState::Idle);
        assert_eq!(
            conn.prepare("SELECT COUNT(*) FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap()[0][0],
            Value::from_i64(0)
        );
        end.reset().unwrap();
        conn.execute("BEGIN; INSERT INTO t VALUES (2)").unwrap();
        drive_prerequisites(&io, &mut end).unwrap();
        assert_eq!(end.command_tag_override(), None);
        let count = if sql == "COMMIT" { 1 } else { 0 };
        assert_eq!(
            conn.prepare("SELECT COUNT(*) FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap()[0][0],
            Value::from_i64(count)
        );
        conn.execute("DELETE FROM t").unwrap();
    }
}

#[test]
fn syntax_error_aborts_transaction() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT)").unwrap();
    conn.execute("BEGIN").unwrap();
    assert!(conn.execute("SELCT 1").is_err());
    let e = exec_err(&conn, "INSERT INTO t VALUES (1)");
    assert_eq!(e.sqlstate, "25P02", "a syntax error must abort the block");
    conn.execute("ROLLBACK").unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0], Value::from_i64(0));
}

#[test]
fn unknown_table_error_at_prepare_aborts_transaction() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    assert!(conn.prepare("SELECT * FROM nonexistent").is_err());
    let e = exec_err(&conn, "SELECT 1");
    assert_eq!(e.sqlstate, "25P02", "a prepare error must abort the block");
    conn.execute("ROLLBACK").unwrap();
    conn.execute("SELECT 1").unwrap();
}

#[test]
fn statement_prepared_before_the_error_is_rejected_after_it() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let mut stmt = conn.prepare("SELECT 1").unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    let e = stmt.run_collect_rows().unwrap_err();
    assert_eq!(
        e.sqlstate, "25P02",
        "executing a statement prepared before the error must be rejected too"
    );
    conn.execute("ROLLBACK").unwrap();
    stmt.reset().unwrap();
    stmt.run_collect_rows().unwrap();
}

#[test]
fn failing_insert_aborts_the_block_and_rollback_recovers() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (x INT)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(
        exec_err(&conn, "INSERT INTO t VALUES (1/0)").sqlstate,
        "22012"
    );
    assert_eq!(
        exec_err(&conn, "SELECT 1").sqlstate,
        "25P02",
        "a failed write must abort the block like any other error"
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(conn.transaction_state(), TransactionState::Idle);
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0], Value::from_i64(0));
}

#[test]
fn rollback_to_savepoint_recovers_aborted_transaction() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    let _err = exec_err(&conn, "INSERT INTO t VALUES (1)");
    assert_eq!(conn.transaction_state(), TransactionState::Aborted);
    conn.execute("ROLLBACK TO SAVEPOINT a").unwrap();
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Active,
        "ROLLBACK TO SAVEPOINT must return the block to normal"
    );
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0], Value::from_i64(2));
}

#[test]
fn interrupted_statement_aborts_the_block() {
    // A query timeout interrupts the statement; the core reports that as
    // StepResult::Interrupt rather than an error, but PostgreSQL treats
    // cancellation as an ordinary error that aborts the block.
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (x INT)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let mut stmt = conn.prepare("SELECT x FROM t").unwrap();
    stmt.set_query_timeout_override(Some(Some(std::time::Duration::from_nanos(1))));
    assert!(matches!(stmt.step(), Ok(StepResult::Interrupt)));
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Aborted,
        "query cancellation is an ordinary error in PostgreSQL and aborts the block"
    );
    assert_eq!(exec_err(&conn, "SELECT 1").sqlstate, "25P02");
    conn.execute("ROLLBACK").unwrap();
    conn.execute("SELECT 1").unwrap();
}

#[test]
fn error_in_autocommit_does_not_abort_the_session() {
    let (_io, conn) = pg_connection();
    let _err = exec_err(&conn, "SELECT 1/0");
    assert_eq!(conn.transaction_state(), TransactionState::Idle);
    conn.execute("SELECT 1").unwrap();
}

#[test]
fn second_error_keeps_the_transaction_aborted() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    assert_eq!(exec_err(&conn, "SELECT 1").sqlstate, "25P02");
    assert_eq!(exec_err(&conn, "SELECT 2").sqlstate, "25P02");
    assert_eq!(conn.transaction_state(), TransactionState::Aborted);
    conn.execute("ROLLBACK").unwrap();
    conn.execute("SELECT 1").unwrap();
}

#[test]
fn commit_of_aborted_transaction_reports_the_rollback_tag() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    let mut commit = conn.prepare("COMMIT").unwrap();
    assert_eq!(commit.command_tag_override(), None);
    commit.run_ignore_rows().unwrap();
    assert_eq!(
        commit.command_tag_override(),
        Some("ROLLBACK"),
        "COMMIT that ended an aborted block must report ROLLBACK"
    );
    assert_eq!(conn.transaction_state(), TransactionState::Idle);

    conn.execute("BEGIN").unwrap();
    let mut commit = conn.prepare("COMMIT").unwrap();
    commit.run_ignore_rows().unwrap();
    assert_eq!(
        commit.command_tag_override(),
        None,
        "a normal COMMIT keeps its own tag"
    );
}

#[test]
fn aborted_commit_never_reports_success_with_transaction_still_open() {
    // An unfinished statement makes the core reject ROLLBACK with
    // "SQL statements in progress". COMMIT on the aborted block must then
    // fail and leave the block aborted — reporting success while the
    // transaction is still open would let a later COMMIT persist it.
    let (io, conn) = pg_connection();
    conn.execute("CREATE TABLE t(x INT)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    let mut writer = conn.prepare("INSERT INTO t VALUES(2) RETURNING x").unwrap();
    loop {
        match writer.step().unwrap() {
            StepResult::Row => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step().unwrap(),
            other => panic!("expected a returned row, got {other:?}"),
        }
    }

    assert!(conn.execute("SELECT 1/0").is_err());

    let result = conn.execute("COMMIT");
    assert!(
        result.is_err() || conn.inner().get_auto_commit(),
        "COMMIT reported success, but the aborted transaction is still open"
    );
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Aborted,
        "the block must stay aborted while it cannot be rolled back"
    );

    // Finishing the statement unblocks the rollback; COMMIT then ends the
    // block and nothing from it survives.
    drop(writer);
    conn.execute("COMMIT").unwrap();
    assert_eq!(conn.transaction_state(), TransactionState::Idle);
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0], Value::from_i64(0));
}

#[test]
fn copy_to_stdout_is_rejected_while_aborted() {
    let (_io, conn) = pg_connection();
    conn.execute("CREATE TABLE t (x INT)").unwrap();
    conn.execute("BEGIN").unwrap();
    // A COPY prepared before the error is rejected at execution.
    let spec = turso_pg_parser::copy::parse_copy_to_stdout("COPY t TO STDOUT")
        .unwrap()
        .unwrap();
    let mut prepared_before = conn.prepare_copy_to_stdout(&spec).unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    let e = prepared_before.run_collect_rows().unwrap_err();
    assert_eq!(e.sqlstate, "25P02", "COPY must respect the aborted block");
    // Preparing a COPY while aborted is rejected outright.
    let e = conn.prepare_copy_to_stdout(&spec).err().unwrap();
    assert_eq!(e.sqlstate, "25P02");
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn and_chain_is_rejected_in_every_transaction_state() {
    // PostgreSQL's COMMIT/ROLLBACK AND CHAIN ends the transaction and starts
    // a new one with the same characteristics — on an aborted block too
    // (xact.c, the chain handling in the abort paths). The frontend does not
    // support chaining, so both forms must be rejected in every state rather
    // than silently dropping the chain; in particular, a rejected chained
    // COMMIT must leave an aborted block aborted.
    //
    // Implementing AND CHAIN makes this test fail. When that happens, the
    // aborted-block redirect (TxRole::Commit/Rollback in statement.rs
    // tx_gate) must also start the chained transaction, and this test must
    // assert that behavior instead.
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let e = exec_err(&conn, "COMMIT AND CHAIN");
    assert_eq!(e.sqlstate, "0A000", "{e:?}");
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Aborted,
        "the rejected statement is an error inside the block, aborting it"
    );
    let e = exec_err(&conn, "ROLLBACK AND CHAIN");
    assert_eq!(e.sqlstate, "0A000", "{e:?}");
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Aborted,
        "a rejected chained ROLLBACK must not end the aborted block"
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(conn.transaction_state(), TransactionState::Idle);
}

#[test]
fn preparing_commit_while_aborted_has_no_side_effects() {
    let (_io, conn) = pg_connection();
    conn.execute("BEGIN").unwrap();
    let _err = exec_err(&conn, "SELECT 1/0");
    let stmt = conn.prepare("COMMIT").unwrap();
    drop(stmt);
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Aborted,
        "preparing COMMIT without executing it must not end the block"
    );
    assert_eq!(exec_err(&conn, "SELECT 1").sqlstate, "25P02");
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn failed_commit_returns_the_session_to_idle() {
    // A schema change committed by another connection makes this
    // connection's COMMIT fail with a schema conflict.
    let io = Arc::new(CountedYieldIO::default());
    let db = turso_pg::open_database_with_io(
        io.clone(),
        "failed-commit.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
    )
    .unwrap();
    let conn = Connection::connect(&db).unwrap();
    let ddl_conn = Connection::connect(&db).unwrap();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 0)").unwrap();
    ddl_conn.execute("ALTER TABLE t ADD COLUMN w INT").unwrap();
    let mut commit = conn.prepare("COMMIT").unwrap();
    let e = drive_prerequisites(&io, &mut commit).unwrap_err();
    assert_eq!(e.sqlstate, "40001");
    assert_eq!(
        conn.transaction_state(),
        TransactionState::Idle,
        "a failed COMMIT rolls back and goes idle, it does not abort the session"
    );
    conn.execute("SELECT 1").unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap()[0][0],
        Value::from_i64(0),
        "the failed transaction's insert must be rolled back"
    );
}

#[test]
fn transaction_rolled_back_by_the_core_still_reports_aborted() {
    // A write-write conflict makes the core roll the whole transaction back
    // on its own. The session must still be in the aborted state afterwards:
    // without it, later statements would silently run and commit in
    // autocommit while the client believes it is mid-transaction.
    let (_io, db) =
        turso_pg::open_database(":memory:", None, OpenFlags::default(), DatabaseOpts::new())
            .unwrap();
    let conn1 = Connection::connect(&db).unwrap();
    let conn2 = Connection::connect(&db).unwrap();
    conn1
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    conn1.execute("BEGIN").unwrap();
    conn2.execute("BEGIN").unwrap();
    conn1.execute("UPDATE t SET v = 1 WHERE id = 1").unwrap();
    let _conflict = exec_err(&conn2, "UPDATE t SET v = 2 WHERE id = 1");
    assert!(
        conn2.inner().get_auto_commit(),
        "the conflict must have rolled the core transaction back already"
    );
    assert_eq!(conn2.transaction_state(), TransactionState::Aborted);
    assert_eq!(
        exec_err(&conn2, "INSERT INTO t VALUES (3, 3)").sqlstate,
        "25P02"
    );
    conn2.execute("ROLLBACK").unwrap();
    assert_eq!(conn2.transaction_state(), TransactionState::Idle);
    conn1.execute("COMMIT").unwrap();
}

#[test]
fn serial_prerequisites_yield_to_the_caller() {
    use turso_core::{IOResult, IO};

    for method in 0..4 {
        let io = Arc::new(CountedYieldIO::default());
        let db = turso_pg::open_database_with_io(
            io.clone(),
            "serial-yield.db",
            OpenFlags::default(),
            DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        let mut stmt = conn
            .prepare("CREATE TABLE t (a SERIAL, b SERIAL, payload INT)")
            .unwrap();
        conn.inner()
            .set_yield_injector(Some(Arc::new(NthYield::new(4))));
        let wakes = Arc::new(WakeCounter::default());
        let waker = std::task::Waker::from(wakes.clone());
        let mut yields = 0;
        let mut explicit_yields = 0;
        loop {
            let before = io.steps.load(std::sync::atomic::Ordering::Relaxed);
            let result = match method {
                0 => stmt.step(),
                1 => stmt.step_with_waker(&waker),
                2 | 3 => {
                    let result = if method == 2 {
                        stmt.run_ignore_rows_nonblock()
                    } else {
                        stmt.run_with_row_callback_nonblock(|_| panic!("DDL returned a row"))
                    };
                    result.map(|result| match result {
                        IOResult::Done(()) => StepResult::Done,
                        IOResult::IO(completion) => {
                            assert!(completion.is_explicit_yield() || !completion.finished());
                            if completion.is_explicit_yield() {
                                StepResult::Yield
                            } else {
                                StepResult::IO
                            }
                        }
                    })
                }
                _ => unreachable!(),
            };
            assert_eq!(
                io.steps.load(std::sync::atomic::Ordering::Relaxed),
                before,
                "method {method} pumped I/O inside the nonblocking call"
            );
            let result = result.unwrap();
            match result {
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    yields += 1;
                    if method < 2 {
                        let completion = stmt.io_completions().unwrap();
                        assert!(completion.is_explicit_yield() || !completion.finished());
                    }
                    if matches!(result, StepResult::Yield) {
                        explicit_yields += 1;
                    }
                    let before_wake = wakes.0.load(std::sync::atomic::Ordering::Relaxed);
                    io.step().unwrap();
                    if method == 1 && matches!(result, StepResult::IO) {
                        assert!(wakes.0.load(std::sync::atomic::Ordering::Relaxed) > before_wake);
                    }
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert!(yields > 0);
        assert!(explicit_yields > 0);
        conn.inner().set_yield_injector(None);
        assert_eq!(conn.transaction_state(), TransactionState::Idle);
        conn.execute("INSERT INTO t (payload) VALUES (42)").unwrap();
        assert_eq!(
            conn.prepare("SELECT a, b FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(1), Value::from_i64(1)]]
        );
        assert_eq!(
            conn.prepare("SELECT sequencename FROM pg_sequences ORDER BY sequencename")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![
                vec![Value::build_text("t_a_seq")],
                vec![Value::build_text("t_b_seq")]
            ]
        );
    }
}

#[test]
fn serial_prerequisites_clean_up_when_abandoned() {
    use turso_core::IO;

    for action in 0..3 {
        for pause_at in 1..200 {
            let io = Arc::new(CountedYieldIO::default());
            let db = turso_pg::open_database_with_io(
                io.clone(),
                "abandoned.db",
                OpenFlags::default(),
                DatabaseOpts::new(),
            )
            .unwrap();
            let conn = Connection::connect(&db).unwrap();
            conn.execute(
                "CREATE TABLE kept (v INT); BEGIN; INSERT INTO kept VALUES (42); SAVEPOINT user_sp",
            )
            .unwrap();
            let mut stmt = conn.prepare("CREATE TABLE t (a SERIAL, b SERIAL)").unwrap();
            let injector = Arc::new(NthYield::new(pause_at));
            conn.inner().set_yield_injector(Some(injector.clone()));
            let mut paused = false;
            loop {
                let before = io.steps.load(std::sync::atomic::Ordering::Relaxed);
                let result = stmt.step().unwrap();
                assert_eq!(io.steps.load(std::sync::atomic::Ordering::Relaxed), before);
                match result {
                    StepResult::Done => break,
                    StepResult::Yield
                        if injector.0.load(std::sync::atomic::Ordering::Relaxed) == 0 =>
                    {
                        paused = true;
                        break;
                    }
                    StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                        io.step().unwrap()
                    }
                    other => panic!("unexpected result: {other:?}"),
                }
            }
            conn.inner().set_yield_injector(None);
            if !paused {
                assert!(pause_at > 1);
                break;
            }
            match action {
                0 => drop(stmt),
                1 => {
                    stmt.reset().unwrap();
                    drop(stmt);
                }
                2 => {
                    stmt.interrupt();
                    let error = drive_prerequisites(&io, &mut stmt)
                        .expect_err(&format!("pause {pause_at}"));
                    assert_eq!(error.sqlstate, "57014", "pause {pause_at}: {error}");
                    assert_eq!(conn.transaction_state(), TransactionState::Aborted);
                    conn.execute("ROLLBACK TO SAVEPOINT user_sp").unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                conn.transaction_state(),
                TransactionState::Active,
                "action {action}, pause {pause_at}"
            );
            assert!(
                conn.prepare("SELECT sequencename FROM pg_sequences")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap()
                    .is_empty(),
                "action {action}, pause {pause_at}"
            );
            assert!(conn
                .prepare("SELECT tablename FROM pg_tables WHERE tablename = 't'")
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .is_empty());
            conn.execute("COMMIT").unwrap();
            assert_eq!(
                conn.prepare("SELECT v FROM kept")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![vec![Value::from_i64(42)]]
            );
            assert!(pause_at < 199, "did not exhaust yield points");
        }
    }

    for read_completion in [false, true] {
        for reset in [false, true] {
            let io = Arc::new(CountedYieldIO::default());
            let db = turso_pg::open_database_with_io(
                io.clone(),
                "pending.db",
                OpenFlags::default(),
                DatabaseOpts::new(),
            )
            .unwrap();
            let conn = Connection::connect(&db).unwrap();
            let mut stmt = conn.prepare("CREATE TABLE t (a SERIAL, b SERIAL)").unwrap();
            assert!(matches!(stmt.step().unwrap(), StepResult::IO));
            if read_completion {
                assert!(!stmt.io_completions().unwrap().finished());
                assert!(!stmt.io_completions().unwrap().finished());
            }
            if reset {
                stmt.reset().unwrap();
            }
            drop(stmt);
            assert_eq!(conn.transaction_state(), TransactionState::Idle);
            let tables = conn
                .prepare("SELECT tablename FROM pg_tables WHERE tablename = 't'")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            let sequences = conn
                .prepare("SELECT sequencename FROM pg_sequences")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(sequences.len(), 2 * tables.len());
            conn.execute("CREATE TABLE usable (v INT)").unwrap();
        }
    }
}

#[test]
fn serial_prerequisites_rollback_conflicts_and_reset() {
    for explicit_tx in [false, true] {
        let io = Arc::new(CountedYieldIO::default());
        let db = turso_pg::open_database_with_io(
            io.clone(),
            "conflict.db",
            OpenFlags::default(),
            DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        if explicit_tx {
            conn.execute("BEGIN; SAVEPOINT user_sp").unwrap();
        }
        let mut stmt = conn.prepare("CREATE TABLE t (a SERIAL, b SERIAL)").unwrap();
        conn.execute("CREATE TABLE t (v INT)").unwrap();
        conn.inner()
            .set_yield_injector(Some(Arc::new(NthYield::new(4))));
        let error = drive_prerequisites(&io, &mut stmt).unwrap_err();
        conn.inner().set_yield_injector(None);
        assert_eq!(error.sqlstate, "42P07");
        if explicit_tx {
            assert_eq!(conn.transaction_state(), TransactionState::Aborted);
            conn.execute("ROLLBACK TO SAVEPOINT user_sp").unwrap();
        } else {
            assert_eq!(conn.transaction_state(), TransactionState::Idle);
            conn.execute("DROP TABLE t").unwrap();
        }
        assert!(conn
            .prepare("SELECT sequencename FROM pg_sequences")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
        stmt.reset().unwrap();
        drive_prerequisites(&io, &mut stmt).unwrap();
        let mut noop = conn
            .prepare("CREATE TABLE IF NOT EXISTS t (extra SERIAL)")
            .unwrap();
        drive_prerequisites(&io, &mut noop).unwrap();
        noop.reset().unwrap();
        drive_prerequisites(&io, &mut noop).unwrap();
        assert_eq!(
            conn.prepare("SELECT sequencename FROM pg_sequences ORDER BY sequencename")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![
                vec![Value::build_text("t_a_seq")],
                vec![Value::build_text("t_b_seq")]
            ]
        );
        if explicit_tx {
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn session_statements_yield_to_the_caller() {
    let io = Arc::new(CountedYieldIO::default());
    let db = turso_pg::open_database_with_io(
        io.clone(),
        "session-yield.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
    )
    .unwrap();
    let conn = Connection::connect(&db).unwrap();
    let mut create = conn
        .prepare("CREATE SCHEMA app CREATE TABLE t (id SERIAL, payload INT UNIQUE)")
        .unwrap();
    assert!(conn.inner().qualifier("app").is_none());
    drive_prerequisites(&io, &mut create).unwrap();
    let data = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(data.path(), "13\n27\n").unwrap();
    let mut copy = conn
        .prepare(format!(
            "COPY app.t (payload) FROM '{}'",
            data.path().display()
        ))
        .unwrap();
    drive_prerequisites(&io, &mut copy).unwrap();
    assert_eq!(copy.n_change(), 2);
    assert_eq!(
        conn.prepare("SELECT id, payload FROM app.t ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![
            vec![Value::from_i64(1), Value::from_i64(13)],
            vec![Value::from_i64(2), Value::from_i64(27)],
        ]
    );
    copy.reset().unwrap();
    std::fs::write(data.path(), "41\n27\n").unwrap();
    assert_eq!(
        drive_prerequisites(&io, &mut copy).unwrap_err().sqlstate,
        "23505"
    );
    assert_eq!(
        conn.prepare("SELECT count(*) FROM app.t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
    copy.reset().unwrap();
    std::fs::write(data.path(), "").unwrap();
    drive_prerequisites(&io, &mut copy).unwrap();
    assert_eq!(copy.n_change(), 0);
    let mut invalid_copy = conn
        .prepare(format!(
            "COPY app.t (missing) FROM '{}'",
            data.path().display()
        ))
        .unwrap();
    assert!(drive_prerequisites(&io, &mut invalid_copy).is_err());
    let mut drop_schema = conn.prepare("DROP SCHEMA app CASCADE").unwrap();
    drive_prerequisites(&io, &mut drop_schema).unwrap();
    assert!(conn.inner().qualifier("app").is_none());
    create.reset().unwrap();
    drive_prerequisites(&io, &mut create).unwrap();
    let mut failure = conn
        .prepare("CREATE SCHEMA failed CREATE TABLE t (v INT) CREATE TABLE t (v INT)")
        .unwrap();
    assert!(drive_prerequisites(&io, &mut failure).is_err());
    assert!(conn.inner().qualifier("failed").is_none());
    assert_eq!(conn.transaction_state(), TransactionState::Idle);

    for sql in ["RESET ALL", "DISCARD ALL"] {
        conn.execute("SET search_path TO app").unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        assert_eq!(conn.inner().search_path(), Some(vec!["app".to_string()]));
        drive_prerequisites(&io, &mut stmt).unwrap();
        assert_eq!(conn.inner().search_path(), Some(vec!["public".to_string()]));
        conn.execute("SET search_path TO app").unwrap();
        drive_prerequisites(&io, &mut stmt).unwrap();
        assert_eq!(conn.inner().search_path(), Some(vec!["app".to_string()]));
        stmt.reset().unwrap();
        drive_prerequisites(&io, &mut stmt).unwrap();
        assert_eq!(conn.inner().search_path(), Some(vec!["public".to_string()]));
    }
}

#[test]
fn session_statements_clean_up_when_abandoned() {
    use std::sync::atomic::Ordering::Relaxed;
    use turso_core::IO;

    let data = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(data.path(), "13\n27\n").unwrap();
    for operation in 0..3 {
        for action in 0..6 {
            let mut exhausted = false;
            for pause_at in 0..200 {
                let io = Arc::new(CountedYieldIO::default());
                let db = turso_pg::open_database_with_io(
                    io.clone(),
                    "session-abandoned.db",
                    OpenFlags::default(),
                    DatabaseOpts::new(),
                )
                .unwrap();
                let conn = Connection::connect(&db).unwrap();
                conn.inner().set_foreign_keys_enabled(true);
                conn.execute("SET search_path TO public; CREATE TABLE kept (v INT)")
                    .unwrap();
                let create = "CREATE SCHEMA work CREATE TABLE t (id SERIAL PRIMARY KEY, payload INT) CREATE TABLE u (id INT REFERENCES work.t(id))";
                if operation != 0 {
                    conn.execute(create).unwrap();
                }
                if operation == 1 {
                    conn.execute("BEGIN; INSERT INTO kept VALUES (42); SAVEPOINT user_sp")
                        .unwrap();
                }
                let sql = match operation {
                    0 => create.to_string(),
                    1 => "DROP SCHEMA work CASCADE".to_string(),
                    2 => format!("COPY work.t (payload) FROM '{}'", data.path().display()),
                    _ => unreachable!(),
                };
                let mut stmt = conn.prepare(sql).unwrap();
                let injector = Arc::new(NthYield::new(pause_at));
                conn.inner().set_yield_injector(Some(injector.clone()));
                let wakes = Arc::new(WakeCounter::default());
                let waker = std::task::Waker::from(wakes.clone());
                let mut paused = false;
                loop {
                    let before = io.steps.load(Relaxed);
                    let result = stmt.step_with_waker(&waker).unwrap();
                    assert_eq!(io.steps.load(Relaxed), before);
                    assert_eq!(conn.inner().search_path(), Some(vec!["public".to_string()]));
                    assert!(conn.inner().foreign_keys_enabled());
                    match result {
                        StepResult::Done => break,
                        StepResult::Yield if injector.0.load(Relaxed) == 0 => {
                            paused = true;
                            break;
                        }
                        StepResult::IO if pause_at == 0 => {
                            assert!(!stmt.io_completions().unwrap().finished());
                            assert!(!stmt.io_completions().unwrap().finished());
                            paused = true;
                            break;
                        }
                        StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                            let before_wake = wakes.0.load(Relaxed);
                            io.step().unwrap();
                            if matches!(result, StepResult::IO) {
                                assert!(wakes.0.load(Relaxed) > before_wake);
                            }
                        }
                        other => panic!("unexpected result: {other:?}"),
                    }
                }
                conn.inner().set_yield_injector(None);
                if !paused {
                    if pause_at == 0 {
                        continue;
                    }
                    assert!(pause_at > 1, "operation {operation}, action {action}");
                    exhausted = true;
                    break;
                }
                match action {
                    0 => drop(stmt),
                    1 => {
                        stmt.reset().unwrap();
                        drop(stmt);
                    }
                    2 => {
                        stmt.interrupt();
                        if let Err(error) = drive_prerequisites(&io, &mut stmt) {
                            assert_eq!(error.sqlstate, "57014");
                            if operation == 1 {
                                assert_eq!(conn.transaction_state(), TransactionState::Aborted);
                                conn.execute("ROLLBACK TO SAVEPOINT user_sp").unwrap();
                            }
                        }
                    }
                    3 | 4 => {
                        loop {
                            let before = io.steps.load(Relaxed);
                            let result = stmt.cleanup_nonblock().unwrap();
                            assert_eq!(io.steps.load(Relaxed), before, "cleanup pumped I/O");
                            assert_eq!(
                                conn.inner().search_path(),
                                Some(vec!["public".to_string()])
                            );
                            assert!(conn.inner().foreign_keys_enabled());
                            match result {
                                turso_core::IOResult::Done(()) => break,
                                turso_core::IOResult::IO(completion) => {
                                    completion.set_waker(Some(&waker));
                                    let pending = !completion.finished();
                                    let before_wake = wakes.0.load(Relaxed);
                                    io.step().unwrap();
                                    if pending {
                                        assert!(wakes.0.load(Relaxed) > before_wake);
                                    }
                                }
                            }
                        }
                        let before = io.steps.load(Relaxed);
                        if action == 3 {
                            stmt.reset().unwrap();
                        }
                        drop(stmt);
                        assert_eq!(
                            io.steps.load(Relaxed),
                            before,
                            "reset/drop repeated cleanup I/O"
                        );
                    }
                    5 => {
                        let before = io.steps.load(Relaxed);
                        let _ = stmt.cleanup_nonblock().unwrap();
                        assert_eq!(io.steps.load(Relaxed), before);
                        drop(stmt);
                    }
                    _ => unreachable!(),
                }
                if operation == 1 {
                    assert_eq!(conn.transaction_state(), TransactionState::Active);
                    conn.execute("COMMIT").unwrap();
                    assert_eq!(
                        conn.prepare("SELECT v FROM kept")
                            .unwrap()
                            .run_collect_rows()
                            .unwrap(),
                        vec![vec![Value::from_i64(42)]]
                    );
                }
                assert_eq!(conn.transaction_state(), TransactionState::Idle);
                let tables = conn
                    .prepare("SELECT tablename FROM pg_tables WHERE schemaname = 'work'")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap();
                let sequences = conn
                    .prepare("SELECT sequencename FROM pg_sequences WHERE schemaname = 'work'")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap();
                assert!(
                    tables.is_empty() || tables.len() == 2,
                    "operation {operation}, action {action}, pause {pause_at}: {tables:?}"
                );
                assert_eq!(sequences.len() * 2, tables.len());
                assert_eq!(conn.inner().qualifier("work").is_some(), !tables.is_empty());
                if operation == 2 {
                    let rows = conn
                        .prepare("SELECT payload FROM work.t ORDER BY payload")
                        .unwrap()
                        .run_collect_rows()
                        .unwrap();
                    assert!(
                        rows.is_empty()
                            || rows == vec![vec![Value::from_i64(13)], vec![Value::from_i64(27)]]
                    );
                }
                conn.execute("INSERT INTO kept VALUES (99)").unwrap();
            }
            assert!(exhausted, "did not exhaust yield points");
        }
    }
}

#[test]
fn catalog_checks_do_not_execute_during_prepare() {
    use std::sync::atomic::Ordering::Relaxed;

    let io = Arc::new(CountedYieldIO::default());
    let db = turso_pg::open_database_with_io(
        io.clone(),
        "catalog-prepare.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
    )
    .unwrap();
    let conn = Connection::connect(&db).unwrap();
    let other = Connection::connect(&db).unwrap();
    conn.execute("CREATE TABLE t (v INT); CREATE VIEW v AS SELECT v FROM t")
        .unwrap();
    other.execute("CREATE SEQUENCE t_v_idx; ALTER TABLE t ADD COLUMN w INT DEFAULT 9; INSERT INTO t (v) VALUES (7)")
        .unwrap();
    let spec = turso_pg_parser::copy::parse_copy_to_stdout("COPY t TO STDOUT")
        .unwrap()
        .unwrap();
    conn.inner()
        .set_yield_injector(Some(Arc::new(NthYield::new(1))));
    let before = io.steps.load(Relaxed);
    let mut copy = conn.prepare_copy_to_stdout(&spec).unwrap();
    assert_eq!(
        io.steps.load(Relaxed),
        before,
        "COPY preparation pumped I/O"
    );
    conn.inner().set_yield_injector(None);
    assert_eq!(
        copy.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(7), Value::from_i64(9)]]
    );
    drop(copy);
    for sql in ["CREATE INDEX ON t (v)", "DROP VIEW v", "DROP VIEW t"] {
        conn.inner()
            .set_yield_injector(Some(Arc::new(NthYield::new(1))));
        let before = io.steps.load(Relaxed);
        let result = conn.prepare(sql);
        assert_eq!(io.steps.load(Relaxed), before, "{sql} pumped I/O");
        conn.inner().set_yield_injector(None);
        if sql == "DROP VIEW t" {
            assert_eq!(result.err().unwrap().sqlstate, "42809");
        } else {
            drive_prerequisites(&io, &mut result.unwrap()).unwrap();
        }
    }
    assert_eq!(
        conn.prepare("SELECT indexname FROM pg_indexes WHERE tablename = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text("t_v_idx1")]]
    );
}

fn drive_prerequisites(
    io: &CountedYieldIO,
    stmt: &mut turso_pg::Statement,
) -> turso_pg::Result<()> {
    use turso_core::{IOResult, IO};
    loop {
        let before = io.steps.load(std::sync::atomic::Ordering::Relaxed);
        let result = stmt.run_ignore_rows_nonblock();
        assert_eq!(
            io.steps.load(std::sync::atomic::Ordering::Relaxed),
            before,
            "nonblocking execution pumped I/O"
        );
        match result? {
            IOResult::Done(()) => return Ok(()),
            IOResult::IO(_) => io.step().unwrap(),
        }
    }
}

#[derive(Debug)]
struct NthYield(std::sync::atomic::AtomicUsize);

impl NthYield {
    fn new(n: usize) -> Self {
        Self(std::sync::atomic::AtomicUsize::new(n))
    }
}

impl turso_core::mvcc::yield_points::YieldInjector for NthYield {
    fn should_yield(
        &self,
        _instance: u64,
        _key: u64,
        _point: turso_core::mvcc::yield_points::YieldPoint,
    ) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        self.0.fetch_update(Relaxed, Relaxed, |n| n.checked_sub(1)) == Ok(1)
    }
}

#[derive(Default)]
struct WakeCounter(std::sync::atomic::AtomicUsize);

impl std::task::Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Default)]
struct CountedYieldIO {
    inner: turso_core::io::MemoryYieldIO,
    steps: std::sync::atomic::AtomicUsize,
}

impl turso_core::Clock for CountedYieldIO {
    fn current_time_monotonic(&self) -> turso_core::MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> turso_core::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl turso_core::IO for CountedYieldIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn turso_core::File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    fn step(&self) -> turso_core::Result<()> {
        self.steps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.step()
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
