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
fn drain_to_done(rows: &mut turso_pg::Statement, sql: &str) {
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
// Integrity check
// ============================================================

#[turso_macros::test]
fn test_sequence_passes_integrity_check(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a sequence and advance it
    pg_execute(
        &conn,
        "CREATE SEQUENCE integ_seq START WITH 1 INCREMENT BY 1",
    );
    pg_query_int(&conn, "SELECT nextval('integ_seq')");
    pg_query_int(&conn, "SELECT nextval('integ_seq')");

    // Drop connection to flush everything, then reopen and verify integrity.
    let path = db.path.clone();
    drop(conn);
    drop(db);

    let db2 = TempDatabase::builder()
        .with_db_name(path.to_str().unwrap())
        .build();
    let conn2 = db2.connect_limbo();
    let rows = conn2
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let result = match rows.first().and_then(|r| r.first()) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        other => panic!("unexpected integrity_check result: {other:?}"),
    };
    assert_eq!(result, "ok", "integrity check failed: {result}");
}

// ============================================================
// Prepare-time side-effect isolation (#8110)
// ============================================================

fn pg_sequence_names(conn: &PgConnection) -> Vec<String> {
    let rows = conn
        .prepare("SELECT sequencename FROM pg_sequences ORDER BY sequencename")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.into_iter()
        .map(|row| match row.first() {
            Some(Value::Text(name)) => name.as_str().to_string(),
            other => panic!("expected text sequencename, got {other:?}"),
        })
        .collect()
}

#[turso_macros::test(mvcc)]
fn test_failed_create_table_serial_does_not_leak_sequence(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE TABLE c (id SERIAL)");
    let err = pg_execute_err(&conn, "CREATE TABLE c (should_fail SERIAL)");
    assert!(err.contains("already exists"), "unexpected error: {err}");
    assert_eq!(pg_sequence_names(&conn), vec!["c_id_seq".to_string()]);

    let path = db.path.clone();
    drop(conn);
    drop(db);

    let db2 = TempDatabase::builder()
        .with_db_name(path.to_str().unwrap())
        .build();
    let conn2 = db2.connect_postgres();
    assert_eq!(pg_sequence_names(&conn2), vec!["c_id_seq".to_string()]);
}

#[turso_macros::test(mvcc)]
fn test_prepare_without_execute_performs_no_writes(db: TempDatabase) {
    let conn = db.connect_postgres();
    pg_execute(&conn, "CREATE TABLE copy_dst (v INTEGER)");

    let dir = tempfile::tempdir().unwrap();
    let tsv = dir.path().join("rows.tsv");
    std::fs::write(&tsv, "42\n99\n").unwrap();
    let copy_sql = format!("COPY copy_dst FROM '{}'", tsv.display());

    // Prepare statements with write / prerequisite side effects and drop them unexecuted.
    let unexecuted_serial = conn.prepare("CREATE TABLE p_tbl (id SERIAL)").unwrap();
    drop(unexecuted_serial);
    assert!(pg_sequence_names(&conn).is_empty());
    assert!(conn.prepare("SELECT * FROM p_tbl").is_err());

    let unexecuted_schema = conn.prepare("CREATE SCHEMA p_schema").unwrap();
    drop(unexecuted_schema);
    pg_assert_no_rows(
        &conn,
        "SELECT 1 FROM pg_namespace WHERE nspname = 'p_schema'",
    );

    let unexecuted_copy = conn.prepare(&copy_sql).unwrap();
    drop(unexecuted_copy);
    assert_eq!(pg_query_int(&conn, "SELECT count(*) FROM copy_dst"), 0);

    // Preparing and executing them afterwards applies the expected mutations.
    conn.prepare("CREATE SCHEMA p_schema")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    let unexecuted_drop = conn.prepare("DROP SCHEMA p_schema").unwrap();
    drop(unexecuted_drop);
    assert_eq!(
        pg_query_int(
            &conn,
            "SELECT count(*) FROM pg_namespace WHERE nspname = 'p_schema'"
        ),
        1
    );

    let mut copy_stmt = conn.prepare(&copy_sql).unwrap();
    copy_stmt.run_ignore_rows().unwrap();
    assert_eq!(copy_stmt.n_change(), 2);
    assert_eq!(pg_query_int(&conn, "SELECT count(*) FROM copy_dst"), 2);

    conn.prepare("DROP SCHEMA p_schema")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    pg_assert_no_rows(
        &conn,
        "SELECT 1 FROM pg_namespace WHERE nspname = 'p_schema'",
    );
}

#[turso_macros::test(mvcc)]
fn test_create_table_if_not_exists_serial_on_existing_table_leaves_no_sequence(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(&conn, "CREATE TABLE c (id SERIAL)");
    pg_execute(&conn, "CREATE TABLE IF NOT EXISTS c (extra SERIAL)");
    assert_eq!(pg_sequence_names(&conn), vec!["c_id_seq".to_string()]);
}

#[turso_macros::test(mvcc)]
fn test_prepared_serial_table_rolls_back_sequence_on_execution_conflict(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Autocommit: prepare while table does not exist, create conflicting table, then execute.
    let mut prepared = conn.prepare("CREATE TABLE race_tbl (a SERIAL)").unwrap();
    assert!(pg_sequence_names(&conn).is_empty());
    pg_execute(&conn, "CREATE TABLE race_tbl (x INTEGER)");
    let err = prepared.run_ignore_rows().unwrap_err().to_string();
    assert!(err.contains("already exists"), "unexpected error: {err}");
    assert!(pg_sequence_names(&conn).is_empty());

    // Explicit transaction: a constraint violation aborts the entire transaction
    // (PostgreSQL 25P02 semantics). Issuing COMMIT on an aborted transaction
    // triggers a ROLLBACK, undoing all changes including kept_tx_k_seq.
    pg_execute(&conn, "BEGIN");
    pg_execute(&conn, "CREATE TABLE kept_tx (k SERIAL)");
    let mut prepared_tx = conn.prepare("CREATE TABLE conflict_tx (b SERIAL)").unwrap();
    pg_execute(&conn, "CREATE TABLE conflict_tx (y INTEGER)");
    assert!(prepared_tx.run_ignore_rows().is_err()); // tx aborted (25P02)
    pg_execute(&conn, "COMMIT"); // triggers ROLLBACK, losing all changes in the tx
    assert!(pg_sequence_names(&conn).is_empty());
}

#[test]
fn test_pg_prepare_and_serial_ddl_dst_fuzz() {
    use rand::Rng;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use turso_core::{DatabaseOpts, MemoryIO, OpenFlags};

    for seed in 0..16u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(0x8110_0000 + seed);
        let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
        let db = turso_pg::open_database_with_io(
            io.clone(),
            "pg_prepare_dst.db",
            OpenFlags::default(),
            DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();

        let mut expected_seqs: BTreeSet<String> = BTreeSet::new();
        let mut live_tables: BTreeSet<String> = BTreeSet::new();

        for step in 0..40usize {
            let slot = rng.random_range(0..4usize);
            let tbl = format!("t{slot}");
            let col = format!("c{step}");
            let seq = format!("{tbl}_{col}_seq");

            match rng.random_range(0..5u8) {
                0 => {
                    // Prepare only (unexecuted): must never mutate catalog state.
                    let sql = format!("CREATE TABLE {tbl} ({col} SERIAL)");
                    let _ = conn.prepare(&sql);
                }
                1 => {
                    // Execute CREATE TABLE without IF NOT EXISTS.
                    let sql = format!("CREATE TABLE {tbl} ({col} SERIAL)");
                    let res = conn.execute(&sql);
                    if live_tables.contains(&tbl) {
                        assert!(res.is_err(), "seed={seed} step={step}: expected conflict");
                    } else {
                        res.unwrap();
                        live_tables.insert(tbl);
                        expected_seqs.insert(seq);
                    }
                }
                2 => {
                    // Execute CREATE TABLE IF NOT EXISTS: only creates sequence when table is new.
                    let sql = format!("CREATE TABLE IF NOT EXISTS {tbl} ({col} SERIAL)");
                    conn.execute(&sql).unwrap();
                    if live_tables.insert(tbl) {
                        expected_seqs.insert(seq);
                    }
                }
                3 => {
                    // Prepare while missing, race with plain table create, then execute.
                    if !live_tables.contains(&tbl) {
                        let sql = format!("CREATE TABLE {tbl} ({col} SERIAL)");
                        let mut stmt = conn.prepare(&sql).unwrap();
                        conn.execute(format!("CREATE TABLE {tbl} (plain_id INT)"))
                            .unwrap();
                        live_tables.insert(tbl);
                        assert!(stmt.run_ignore_rows().is_err());
                    }
                }
                _ => {
                    // Explicit transaction that rolls back after creating a new table + sequence.
                    let tmp_tbl = format!("tx_roll_{step}");
                    conn.execute("BEGIN").unwrap();
                    conn.execute(format!("CREATE TABLE {tmp_tbl} (id SERIAL)"))
                        .unwrap();
                    conn.execute("ROLLBACK").unwrap();
                }
            }

            let actual: BTreeSet<String> = pg_sequence_names(&conn).into_iter().collect();
            assert_eq!(
                actual, expected_seqs,
                "seed={seed} step={step}: sequence catalog diverged"
            );
        }
    }
}
