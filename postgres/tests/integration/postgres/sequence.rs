use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

/// Helper: execute a statement
fn pg_execute(conn: &PgConnection, sql: &str) {
    conn.execute(sql).unwrap();
}

/// Helper: query a single integer value from a SELECT.
///
/// **Must drive the Statement all the way to `Done`** after reading the
/// row, not just call `step()` once. With Turso's disk-only sequence
/// design, `nextval`/`setval` execute their backing-table write inline
/// with the result-row emission; the autocommit transaction only commits
/// when the program reaches `Halt`. Dropping the Statement after a single
/// `step()` triggers `reset_internal`'s rollback path (the change counter
/// isn't set for SELECT-shaped reads), undoing the write — every
/// subsequent `nextval` then re-emits the same value.
fn pg_query_int(conn: &PgConnection, sql: &str) -> i64 {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row for: {sql}");
    };
    let value = match rows.row().unwrap().get_value(0) {
        Value::Numeric(Numeric::Integer(v)) => *v,
        other => panic!("expected integer, got {other:?} for: {sql}"),
    };
    drain_to_done(&mut rows, sql);
    value
}

/// Helper: query a single text value. Same draining contract as
/// [`pg_query_int`] — see its docstring for why we must run to `Done`.
fn pg_query_text(conn: &PgConnection, sql: &str) -> String {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row for: {sql}");
    };
    let value = match rows.row().unwrap().get_value(0) {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?} for: {sql}"),
    };
    drain_to_done(&mut rows, sql);
    value
}

/// Step a Statement until it returns `Done`, panicking on extra rows or
/// non-Done terminal states. Used after reading a single expected row to
/// make sure the program reaches `Halt` and any sequence side-effects
/// commit.
fn drain_to_done(rows: &mut turso_core::Statement, sql: &str) {
    loop {
        match rows.step().unwrap() {
            StepResult::Done => return,
            // The driver may yield IO mid-program (e.g. backing-table cursor
            // page reads). Loop back and keep stepping.
            StepResult::IO => continue,
            StepResult::Row => panic!("expected single-row result for: {sql}"),
            other => panic!("unexpected step result while draining: {other:?} for: {sql}"),
        }
    }
}

/// Helper: check that a query returns no rows
fn pg_assert_no_rows(conn: &PgConnection, sql: &str) {
    let mut rows = conn.query(sql).unwrap().unwrap();
    match rows.step().unwrap() {
        StepResult::Done => {}
        StepResult::Row => panic!("expected no rows for: {sql}"),
        other => panic!("unexpected step result: {other:?} for: {sql}"),
    }
}

/// Helper: execute a statement that is expected to fail, return the error message
fn pg_execute_err(conn: &PgConnection, sql: &str) -> String {
    match conn.execute(sql) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error for: {sql}"),
    }
}

/// Helper: query that is expected to fail, return the error message
fn pg_query_err(conn: &PgConnection, sql: &str) -> String {
    match conn.query(sql) {
        Err(e) => e.to_string(),
        Ok(Some(mut rows)) => match rows.step() {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for: {sql}"),
        },
        Ok(None) => panic!("expected error for: {sql}"),
    }
}

// ============================================================
// CREATE SEQUENCE — Error Cases
// ============================================================

#[turso_macros::test(mvcc)]
fn test_create_sequence_increment_zero(db: TempDatabase) {
    let conn = db.connect_postgres();

    let err = pg_execute_err(&conn, "CREATE SEQUENCE bad_seq INCREMENT BY 0");
    assert!(
        err.contains("INCREMENT must not be zero"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_cache_zero(db: TempDatabase) {
    let conn = db.connect_postgres();

    let err = pg_execute_err(&conn, "CREATE SEQUENCE bad_seq CACHE 0");
    assert!(
        err.contains("must be greater than zero"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_minvalue_exceeds_maxvalue(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Ascending with MAXVALUE below default MINVALUE
    let err = pg_execute_err(&conn, "CREATE SEQUENCE bad_seq MAXVALUE -20");
    assert!(
        err.contains("MINVALUE") && err.contains("must be less than MAXVALUE"),
        "unexpected error: {err}"
    );

    // Descending with MINVALUE above default MAXVALUE
    let err = pg_execute_err(
        &conn,
        "CREATE SEQUENCE bad_seq2 INCREMENT BY -1 MINVALUE 20",
    );
    assert!(
        err.contains("MINVALUE") && err.contains("must be less than MAXVALUE"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_start_out_of_bounds(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Ascending: START below MINVALUE
    let err = pg_execute_err(
        &conn,
        "CREATE SEQUENCE bad_seq START WITH -10 MINVALUE 1 MAXVALUE 100",
    );
    assert!(
        err.contains("START value") && err.contains("cannot be less than MINVALUE"),
        "unexpected error: {err}"
    );

    // Descending: START above MAXVALUE
    let err = pg_execute_err(
        &conn,
        "CREATE SEQUENCE bad_seq2 INCREMENT BY -1 START WITH 10 MINVALUE -100 MAXVALUE 5",
    );
    assert!(
        err.contains("START value") && err.contains("cannot be greater than MAXVALUE"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_duplicate(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE dup_seq");

    // Duplicate without IF NOT EXISTS -> error
    let err = pg_execute_err(&conn, "CREATE SEQUENCE dup_seq");
    assert!(err.contains("already exists"), "unexpected error: {err}");

    // With IF NOT EXISTS -> ok
    pg_execute(&conn, "CREATE SEQUENCE IF NOT EXISTS dup_seq");

    pg_execute(&conn, "DROP SEQUENCE dup_seq");
}

// ============================================================
// CREATE SEQUENCE — Defaults & Options
// ============================================================

#[turso_macros::test(mvcc)]
fn test_create_sequence_defaults(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE def_seq");

    let start = pg_query_int(
        &conn,
        "SELECT start_value FROM pg_sequences WHERE sequencename = 'def_seq'",
    );
    assert_eq!(start, 1);

    let inc = pg_query_int(
        &conn,
        "SELECT increment_by FROM pg_sequences WHERE sequencename = 'def_seq'",
    );
    assert_eq!(inc, 1);

    // First nextval should return 1
    let v = pg_query_int(&conn, "SELECT nextval('def_seq')");
    assert_eq!(v, 1);

    pg_execute(&conn, "DROP SEQUENCE def_seq");
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_descending_defaults(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE desc_seq INCREMENT BY -1");

    let start = pg_query_int(
        &conn,
        "SELECT start_value FROM pg_sequences WHERE sequencename = 'desc_seq'",
    );
    assert_eq!(start, -1);

    let max = pg_query_int(
        &conn,
        "SELECT max_value FROM pg_sequences WHERE sequencename = 'desc_seq'",
    );
    assert_eq!(max, -1);

    // First nextval should return -1
    let v = pg_query_int(&conn, "SELECT nextval('desc_seq')");
    assert_eq!(v, -1);

    pg_execute(&conn, "DROP SEQUENCE desc_seq");
}

#[turso_macros::test(mvcc)]
fn test_create_sequence_custom_options(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE custom_seq START WITH 10 INCREMENT BY 5 MINVALUE 0 MAXVALUE 100",
    );

    let start = pg_query_int(
        &conn,
        "SELECT start_value FROM pg_sequences WHERE sequencename = 'custom_seq'",
    );
    assert_eq!(start, 10);

    let inc = pg_query_int(
        &conn,
        "SELECT increment_by FROM pg_sequences WHERE sequencename = 'custom_seq'",
    );
    assert_eq!(inc, 5);

    let min = pg_query_int(
        &conn,
        "SELECT min_value FROM pg_sequences WHERE sequencename = 'custom_seq'",
    );
    assert_eq!(min, 0);

    let max = pg_query_int(
        &conn,
        "SELECT max_value FROM pg_sequences WHERE sequencename = 'custom_seq'",
    );
    assert_eq!(max, 100);

    pg_execute(&conn, "DROP SEQUENCE custom_seq");
}

// ============================================================
// DROP SEQUENCE
// ============================================================

#[turso_macros::test(mvcc)]
fn test_drop_sequence_basic(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE drop_seq");

    // Verify it exists
    let name = pg_query_text(
        &conn,
        "SELECT sequencename FROM pg_sequences WHERE sequencename = 'drop_seq'",
    );
    assert_eq!(name, "drop_seq");

    pg_execute(&conn, "DROP SEQUENCE drop_seq");

    // Verify it's gone
    pg_assert_no_rows(
        &conn,
        "SELECT sequencename FROM pg_sequences WHERE sequencename = 'drop_seq'",
    );
}

#[turso_macros::test(mvcc)]
fn test_drop_sequence_if_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    // IF EXISTS on nonexistent -> ok
    pg_execute(&conn, "DROP SEQUENCE IF EXISTS nonexistent_seq");

    // Drop nonexistent without IF EXISTS -> error
    let err = pg_execute_err(&conn, "DROP SEQUENCE nonexistent_seq");
    assert!(err.contains("does not exist"), "unexpected error: {err}");
}

#[turso_macros::test(mvcc)]
fn test_drop_sequence_reuse_name(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE reuse_seq");
    pg_execute(&conn, "DROP SEQUENCE reuse_seq");

    // Create again with same name -> ok
    pg_execute(&conn, "CREATE SEQUENCE reuse_seq");

    let v = pg_query_int(&conn, "SELECT nextval('reuse_seq')");
    assert_eq!(v, 1);

    pg_execute(&conn, "DROP SEQUENCE reuse_seq");
}

// ============================================================
// nextval — Basic
// ============================================================

#[turso_macros::test(mvcc)]
fn test_nextval_sequential(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE seq_seq");

    assert_eq!(pg_query_int(&conn, "SELECT nextval('seq_seq')"), 1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('seq_seq')"), 2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('seq_seq')"), 3);

    pg_execute(&conn, "DROP SEQUENCE seq_seq");
}

#[turso_macros::test(mvcc)]
fn test_nextval_custom_increment(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE inc_seq START WITH 10 INCREMENT BY 5",
    );

    assert_eq!(pg_query_int(&conn, "SELECT nextval('inc_seq')"), 10);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('inc_seq')"), 15);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('inc_seq')"), 20);

    pg_execute(&conn, "DROP SEQUENCE inc_seq");
}

#[turso_macros::test(mvcc)]
fn test_nextval_descending(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE desc_nv INCREMENT BY -1");

    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nv')"), -1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nv')"), -2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nv')"), -3);

    pg_execute(&conn, "DROP SEQUENCE desc_nv");
}

#[turso_macros::test(mvcc)]
fn test_nextval_descending_custom(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE desc_custom INCREMENT BY -5 START WITH -10 MINVALUE -100 MAXVALUE -1",
    );

    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_custom')"), -10);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_custom')"), -15);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_custom')"), -20);

    pg_execute(&conn, "DROP SEQUENCE desc_custom");
}

// ============================================================
// nextval — CYCLE / NO CYCLE
// ============================================================

#[turso_macros::test(mvcc)]
fn test_nextval_ascending_cycle(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE cyc_seq MINVALUE 1 MAXVALUE 3 CYCLE");

    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 3);
    // Wraps around
    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('cyc_seq')"), 3);

    pg_execute(&conn, "DROP SEQUENCE cyc_seq");
}

#[turso_macros::test(mvcc)]
fn test_nextval_ascending_no_cycle(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE nocyc_seq MINVALUE 1 MAXVALUE 3");

    assert_eq!(pg_query_int(&conn, "SELECT nextval('nocyc_seq')"), 1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('nocyc_seq')"), 2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('nocyc_seq')"), 3);

    // Should error — reached maximum
    let err = pg_query_err(&conn, "SELECT nextval('nocyc_seq')");
    assert!(
        err.contains("reached maximum value"),
        "unexpected error: {err}"
    );

    pg_execute(&conn, "DROP SEQUENCE nocyc_seq");
}

#[turso_macros::test(mvcc)]
fn test_nextval_descending_cycle(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE desc_cyc INCREMENT BY -1 MINVALUE -3 MAXVALUE -1 CYCLE",
    );

    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_cyc')"), -1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_cyc')"), -2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_cyc')"), -3);
    // Wraps around
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_cyc')"), -1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_cyc')"), -2);

    pg_execute(&conn, "DROP SEQUENCE desc_cyc");
}

#[turso_macros::test(mvcc)]
fn test_nextval_descending_no_cycle(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE desc_nocyc INCREMENT BY -1 MINVALUE -3 MAXVALUE -1",
    );

    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nocyc')"), -1);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nocyc')"), -2);
    assert_eq!(pg_query_int(&conn, "SELECT nextval('desc_nocyc')"), -3);

    // Should error — reached minimum
    let err = pg_query_err(&conn, "SELECT nextval('desc_nocyc')");
    assert!(
        err.contains("reached minimum value"),
        "unexpected error: {err}"
    );

    pg_execute(&conn, "DROP SEQUENCE desc_nocyc");
}

// ============================================================
// currval
// ============================================================

#[turso_macros::test(mvcc)]
fn test_currval_tracks_nextval(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE cv_seq");

    let v1 = pg_query_int(&conn, "SELECT nextval('cv_seq')");
    assert_eq!(v1, 1);
    let cv1 = pg_query_int(&conn, "SELECT currval('cv_seq')");
    assert_eq!(cv1, 1);

    let v2 = pg_query_int(&conn, "SELECT nextval('cv_seq')");
    assert_eq!(v2, 2);
    let cv2 = pg_query_int(&conn, "SELECT currval('cv_seq')");
    assert_eq!(cv2, 2);

    pg_execute(&conn, "DROP SEQUENCE cv_seq");
}

#[turso_macros::test(mvcc)]
fn test_currval_not_yet_defined(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE fresh_seq");

    // currval before any nextval -> error
    let err = pg_query_err(&conn, "SELECT currval('fresh_seq')");
    assert!(
        err.contains("not yet defined") || err.contains("currval"),
        "unexpected error: {err}"
    );

    pg_execute(&conn, "DROP SEQUENCE fresh_seq");
}

// ============================================================
// setval
// ============================================================

#[turso_macros::test(mvcc)]
fn test_setval_basic(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE sv_seq");

    let sv = pg_query_int(&conn, "SELECT setval('sv_seq', 100)");
    assert_eq!(sv, 100);

    // Next call should return 101 (is_called defaults to true)
    let v = pg_query_int(&conn, "SELECT nextval('sv_seq')");
    assert_eq!(v, 101);

    pg_execute(&conn, "DROP SEQUENCE sv_seq");
}

#[turso_macros::test(mvcc)]
fn test_setval_is_called_false(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE sv_f_seq");

    let sv = pg_query_int(&conn, "SELECT setval('sv_f_seq', 99, false)");
    assert_eq!(sv, 99);

    // With is_called=false, nextval returns the set value itself
    let v = pg_query_int(&conn, "SELECT nextval('sv_f_seq')");
    assert_eq!(v, 99);

    pg_execute(&conn, "DROP SEQUENCE sv_f_seq");
}

#[turso_macros::test(mvcc)]
fn test_setval_is_called_true(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE sv_t_seq");

    let sv = pg_query_int(&conn, "SELECT setval('sv_t_seq', 99, true)");
    assert_eq!(sv, 99);

    // With is_called=true, nextval returns set value + increment
    let v = pg_query_int(&conn, "SELECT nextval('sv_t_seq')");
    assert_eq!(v, 100);

    pg_execute(&conn, "DROP SEQUENCE sv_t_seq");
}

#[turso_macros::test(mvcc)]
fn test_setval_bounds_checking(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE bnd_seq MINVALUE 10 MAXVALUE 20 START WITH 10",
    );

    // Below min -> error
    let err = pg_query_err(&conn, "SELECT setval('bnd_seq', 5)");
    assert!(err.contains("out of bounds"), "unexpected error: {err}");

    // Above max -> error
    let err = pg_query_err(&conn, "SELECT setval('bnd_seq', 25)");
    assert!(err.contains("out of bounds"), "unexpected error: {err}");

    // At min -> ok
    let v = pg_query_int(&conn, "SELECT setval('bnd_seq', 10)");
    assert_eq!(v, 10);

    // At max -> ok
    let v = pg_query_int(&conn, "SELECT setval('bnd_seq', 20)");
    assert_eq!(v, 20);

    pg_execute(&conn, "DROP SEQUENCE bnd_seq");
}

// ============================================================
// Serial Columns
// ============================================================

#[turso_macros::test(mvcc)]
fn test_serial_auto_increment(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE TABLE ser_items (id serial, name text NOT NULL)",
    );

    pg_execute(&conn, "INSERT INTO ser_items (name) VALUES ('alice')");
    pg_execute(&conn, "INSERT INTO ser_items (name) VALUES ('bob')");
    pg_execute(&conn, "INSERT INTO ser_items (name) VALUES ('charlie')");

    assert_eq!(
        pg_query_int(&conn, "SELECT id FROM ser_items WHERE name = 'alice'"),
        1
    );
    assert_eq!(
        pg_query_int(&conn, "SELECT id FROM ser_items WHERE name = 'bob'"),
        2
    );
    assert_eq!(
        pg_query_int(&conn, "SELECT id FROM ser_items WHERE name = 'charlie'"),
        3
    );

    pg_execute(&conn, "DROP TABLE ser_items");
}

#[turso_macros::test(mvcc)]
fn test_serial_explicit_value(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE TABLE ser_exp (id serial, name text NOT NULL)",
    );

    // Insert with explicit id
    pg_execute(
        &conn,
        "INSERT INTO ser_exp (id, name) VALUES (42, 'explicit')",
    );
    assert_eq!(
        pg_query_int(&conn, "SELECT id FROM ser_exp WHERE name = 'explicit'"),
        42
    );

    // Serial does NOT imply PRIMARY KEY — duplicate ids should be allowed
    pg_execute(
        &conn,
        "INSERT INTO ser_exp (id, name) VALUES (42, 'duplicate')",
    );

    pg_execute(&conn, "DROP TABLE ser_exp");
}

#[turso_macros::test(mvcc)]
fn test_serial_null_rejected(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE TABLE ser_nn (id serial, name text)");

    // SERIAL implies NOT NULL
    let err = pg_execute_err(&conn, "INSERT INTO ser_nn (id, name) VALUES (NULL, 'bad')");
    assert!(
        err.to_lowercase().contains("not null")
            || err.to_lowercase().contains("null")
            || err.to_lowercase().contains("constraint"),
        "unexpected error: {err}"
    );

    pg_execute(&conn, "DROP TABLE ser_nn");
}

#[turso_macros::test(mvcc)]
fn test_serial_sequence_in_pg_sequences(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE TABLE ser_cat (id serial, val text)");

    // The implicit sequence should be visible in pg_sequences
    // Convention: <table>_<column>_seq
    let name = pg_query_text(
        &conn,
        "SELECT sequencename FROM pg_sequences WHERE sequencename = 'ser_cat_id_seq'",
    );
    assert_eq!(name, "ser_cat_id_seq");

    pg_execute(&conn, "DROP TABLE ser_cat");
}

// ============================================================
// pg_sequences Catalog
// ============================================================

#[turso_macros::test(mvcc)]
fn test_pg_sequences_columns(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE SEQUENCE cat_full START WITH 5 INCREMENT BY 2 MINVALUE 1 MAXVALUE 1000 CACHE 10",
    );

    let schema = pg_query_text(
        &conn,
        "SELECT schemaname FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(schema, "public");

    let start = pg_query_int(
        &conn,
        "SELECT start_value FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(start, 5);

    let inc = pg_query_int(
        &conn,
        "SELECT increment_by FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(inc, 2);

    let min = pg_query_int(
        &conn,
        "SELECT min_value FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(min, 1);

    let max = pg_query_int(
        &conn,
        "SELECT max_value FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(max, 1000);

    // pg_sequences.cache_size reports the value the engine actually uses for
    // allocation, not the value requested in CREATE SEQUENCE. Turso's
    // disk-only sequence design never caches values (every nextval reads and
    // writes the backing-table watermark inline with the executing
    // transaction), so cache_size is always 1 regardless of the requested
    // `CACHE n`. PG's behavior on a server that physically can't cache is
    // the same — it reports the actual cache size, not the requested one.
    let cache = pg_query_int(
        &conn,
        "SELECT cache_size FROM pg_sequences WHERE sequencename = 'cat_full'",
    );
    assert_eq!(cache, 1);

    pg_execute(&conn, "DROP SEQUENCE cat_full");
}

#[turso_macros::test(mvcc)]
fn test_pg_sequences_multiple(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE SEQUENCE multi_a");
    pg_execute(&conn, "CREATE SEQUENCE multi_b");
    pg_execute(&conn, "CREATE SEQUENCE multi_c");

    // Count sequences with our prefix
    let count = pg_query_int(
        &conn,
        "SELECT COUNT(*) FROM pg_sequences WHERE sequencename LIKE 'multi_%'",
    );
    assert_eq!(count, 3);

    // Drop one
    pg_execute(&conn, "DROP SEQUENCE multi_b");

    let count = pg_query_int(
        &conn,
        "SELECT COUNT(*) FROM pg_sequences WHERE sequencename LIKE 'multi_%'",
    );
    assert_eq!(count, 2);

    // Verify removed
    pg_assert_no_rows(
        &conn,
        "SELECT 1 FROM pg_sequences WHERE sequencename = 'multi_b'",
    );

    pg_execute(&conn, "DROP SEQUENCE multi_a");
    pg_execute(&conn, "DROP SEQUENCE multi_c");
}

// ============================================================
// SQLite Compatibility — integrity_check
// ============================================================

#[turso_macros::test]
fn test_sequence_passes_sqlite_integrity_check(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a sequence and advance it
    pg_execute(
        &conn,
        "CREATE SEQUENCE integ_seq START WITH 1 INCREMENT BY 1",
    );
    pg_query_int(&conn, "SELECT nextval('integ_seq')");
    pg_query_int(&conn, "SELECT nextval('integ_seq')");

    // Drop connection to flush everything
    let path = db.path.clone();
    drop(conn);
    drop(db);

    // Open with rusqlite (real SQLite) and run integrity check
    let sqlite_conn = rusqlite::Connection::open(&path).unwrap();
    let result: String = sqlite_conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(result, "ok", "SQLite integrity check failed: {result}");
}
