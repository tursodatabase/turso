use std::sync::Arc;

use crate::common::{limbo_exec_rows, TempDatabase};
use turso_core::{Connection, LimboError, Statement};

const ROW_COUNT: i64 = 2000;

#[test]
fn test_forward_scan_returns_all_rows_after_auto_checkpoint_between_steps() {
    assert_eq!(count_rows_while_inserting_between_steps("ASC"), ROW_COUNT);
}

#[test]
fn test_backward_scan_returns_all_rows_after_auto_checkpoint_between_steps() {
    assert_eq!(count_rows_while_inserting_between_steps("DESC"), ROW_COUNT);
}

#[test]
fn test_forward_scan_returns_all_rows_when_another_connection_commits_between_steps() {
    assert_eq!(
        count_rows_while_two_connections_insert_between_steps("ASC"),
        ROW_COUNT
    );
}

#[test]
fn test_backward_scan_returns_all_rows_when_another_connection_commits_between_steps() {
    assert_eq!(
        count_rows_while_two_connections_insert_between_steps("DESC"),
        ROW_COUNT
    );
}

#[test]
fn test_forward_scan_returns_all_rows_after_sibling_select_finishes() {
    assert_eq!(count_rows_after_sibling_select_finishes("ASC"), ROW_COUNT);
}

#[test]
fn test_backward_scan_returns_all_rows_after_sibling_select_finishes() {
    assert_eq!(count_rows_after_sibling_select_finishes("DESC"), ROW_COUNT);
}

#[test]
fn test_commit_during_select_ends_read_transaction_when_select_finishes() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_tables(&conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO other(v) VALUES (1)").unwrap();
    let mut select = conn.prepare("SELECT id FROM t").unwrap();
    assert!(select
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_some());
    conn.execute("COMMIT").unwrap();
    assert_eq!(1 + count_rows(&mut select, |_| {}), ROW_COUNT);

    other_conn
        .execute("INSERT INTO other(v) VALUES (2)")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM other"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

fn count_rows_while_inserting_between_steps(order: &str) -> i64 {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    create_tables(&conn);

    let mut select = conn
        .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
        .unwrap();
    count_rows(&mut select, |row| {
        conn.execute(format!("INSERT INTO other(v) VALUES ({row})"))
            .unwrap();
    })
}

fn count_rows_while_two_connections_insert_between_steps(order: &str) -> i64 {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_tables(&conn);

    let mut select = conn
        .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
        .unwrap();
    count_rows(&mut select, |row| {
        match conn.execute(format!("INSERT INTO other(v) VALUES ({row})")) {
            Ok(()) | Err(LimboError::BusySnapshot) => {}
            Err(err) => panic!("unexpected insert error: {err}"),
        }
        other_conn
            .execute(format!("INSERT INTO other(v) VALUES ({row})"))
            .unwrap();
    })
}

fn count_rows_after_sibling_select_finishes(order: &str) -> i64 {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_tables(&conn);

    let mut first_select = conn.prepare("SELECT id FROM t LIMIT 1").unwrap();
    let mut select = conn
        .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
        .unwrap();
    assert!(first_select
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_some());
    assert!(select
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_some());
    assert!(first_select
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_none());

    1 + count_rows(&mut select, |row| {
        other_conn
            .execute(format!("INSERT INTO other(v) VALUES ({row})"))
            .unwrap();
        limbo_exec_rows(&conn, "SELECT count(*) FROM other");
    })
}

fn create_tables(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROW_COUNT}) \
         INSERT INTO t SELECT x FROM c"
    ))
    .unwrap();
}

fn count_rows(select: &mut Statement, mut after_each_row: impl FnMut(i64)) -> i64 {
    let mut rows = 0;
    select
        .run_with_row_callback(|_| {
            rows += 1;
            after_each_row(rows);
            Ok(())
        })
        .unwrap();
    rows
}
