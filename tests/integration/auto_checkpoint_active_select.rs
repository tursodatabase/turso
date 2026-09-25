use std::num::NonZero;
use std::sync::Arc;

use crate::common::{limbo_exec_rows, TempDatabase};
use turso_core::{Connection, LimboError, Statement, StepResult, Value};

const ROWS: i64 = 2000;

fn create_table_with_rows(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROWS}) \
         INSERT INTO t SELECT x FROM c"
    ))
    .unwrap();
}

fn next_id(stmt: &mut Statement) -> Option<i64> {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Numeric(turso_core::Numeric::Integer(id)) = row.get_value(0) else {
                    panic!("expected integer id, got {:?}", row.get_value(0));
                };
                return Some(*id);
            }
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Done => return None,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn count_rows_while_inserting_between_steps(order: &str) -> i64 {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut select = conn
        .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
        .unwrap();
    let mut insert = conn.prepare("INSERT INTO other(v) VALUES (?)").unwrap();
    let mut rows = 0;
    while next_id(&mut select).is_some() {
        rows += 1;
        insert.reset().unwrap();
        insert
            .bind_at(NonZero::new(1).unwrap(), Value::from_i64(rows))
            .unwrap();
        insert.run_ignore_rows().unwrap();
    }
    rows
}

#[test]
fn test_forward_scan_returns_all_rows_after_auto_checkpoint_between_steps() {
    assert_eq!(count_rows_while_inserting_between_steps("ASC"), ROWS);
}

#[test]
fn test_backward_scan_returns_all_rows_after_auto_checkpoint_between_steps() {
    assert_eq!(count_rows_while_inserting_between_steps("DESC"), ROWS);
}

fn scan_while_other_connection_commits(order: &str) -> Vec<i64> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut select = conn
        .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
        .unwrap();
    let mut ids = vec![next_id(&mut select).unwrap()];
    conn.execute("INSERT INTO other(v) VALUES (1)").unwrap();
    let mut new_id = ROWS;
    while let Some(id) = next_id(&mut select) {
        ids.push(id);
        new_id += 1;
        other_conn
            .execute(format!("INSERT INTO t VALUES ({new_id})"))
            .unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
            vec![vec![rusqlite::types::Value::Integer(ROWS)]],
            "a statement on the same connection must read the SELECT's snapshot"
        );
    }
    ids
}

#[test]
fn test_forward_scan_returns_snapshot_rows_while_other_connection_commits() {
    let expected: Vec<i64> = (1..=ROWS).collect();
    assert_eq!(scan_while_other_connection_commits("ASC"), expected);
}

#[test]
fn test_backward_scan_returns_snapshot_rows_while_other_connection_commits() {
    let expected: Vec<i64> = (1..=ROWS).rev().collect();
    assert_eq!(scan_while_other_connection_commits("DESC"), expected);
}

#[test]
fn test_scan_continues_after_sibling_select_finishes_and_other_connection_commits() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut long_select = conn.prepare("SELECT id FROM t ORDER BY id DESC").unwrap();
    let mut short_select = conn.prepare("SELECT id FROM t WHERE id <= 2").unwrap();
    let mut ids = vec![next_id(&mut long_select).unwrap()];
    assert_eq!(next_id(&mut short_select), Some(1));
    ids.push(next_id(&mut long_select).unwrap());
    assert_eq!(next_id(&mut short_select), Some(2));
    assert_eq!(next_id(&mut short_select), None);

    let mut new_id = ROWS;
    while let Some(id) = next_id(&mut long_select) {
        ids.push(id);
        new_id += 1;
        other_conn
            .execute(format!("INSERT INTO t VALUES ({new_id})"))
            .unwrap();
        conn.execute("SELECT count(*) FROM other").unwrap();
    }
    let expected: Vec<i64> = (1..=ROWS).rev().collect();
    assert_eq!(ids, expected);
}

#[test]
fn test_wal_checkpoint_on_same_connection_is_refused_during_scan() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut select = conn.prepare("SELECT id FROM t ORDER BY id DESC").unwrap();
    let mut ids = Vec::new();
    while let Some(id) = next_id(&mut select) {
        ids.push(id);
        conn.execute(format!("INSERT INTO other(v) VALUES ({id})"))
            .unwrap();
        let err = conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect_err("checkpoint must not run while a statement is reading");
        assert!(
            matches!(err, LimboError::StatementsInProgress(_)),
            "unexpected error: {err:?}"
        );
    }
    let expected: Vec<i64> = (1..=ROWS).rev().collect();
    assert_eq!(ids, expected);
}

#[test]
fn test_resetting_unfinished_scan_ends_read_transaction_kept_by_commit() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut select = conn.prepare("SELECT id FROM t").unwrap();
    assert_eq!(next_id(&mut select), Some(1));
    conn.execute("INSERT INTO other(v) VALUES (1)").unwrap();
    other_conn.execute("INSERT INTO t VALUES (2001)").unwrap();
    let err = conn
        .execute("INSERT INTO other(v) VALUES (2)")
        .expect_err("the write must fail because the read snapshot is stale");
    assert!(
        matches!(err, LimboError::BusySnapshot),
        "unexpected error: {err:?}"
    );
    select.reset().unwrap();

    conn.execute("INSERT INTO other(v) VALUES (2)").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![rusqlite::types::Value::Integer(ROWS + 1)]]
    );
}

#[test]
fn test_commit_during_scan_keeps_snapshot_until_scan_finishes() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO other(v) VALUES (1)").unwrap();
    let mut select = conn.prepare("SELECT id FROM t ORDER BY id DESC").unwrap();
    let mut ids = vec![next_id(&mut select).unwrap()];
    conn.execute("COMMIT").unwrap();

    let mut new_id = ROWS;
    while let Some(id) = next_id(&mut select) {
        ids.push(id);
        new_id += 1;
        other_conn
            .execute(format!("INSERT INTO t VALUES ({new_id})"))
            .unwrap();
        conn.execute("SELECT count(*) FROM other").unwrap();
    }
    let expected: Vec<i64> = (1..=ROWS).rev().collect();
    assert_eq!(ids, expected);

    assert_eq!(
        limbo_exec_rows(&other_conn, "PRAGMA wal_checkpoint(TRUNCATE)")[0][0],
        rusqlite::types::Value::Integer(0),
        "the finished scan must not keep the read transaction open"
    );
}

#[test]
fn test_statement_waiting_for_write_lock_keeps_read_transaction_open() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);

    let mut select = conn.prepare("SELECT id FROM t").unwrap();
    assert_eq!(next_id(&mut select), Some(1));
    other_conn.execute("BEGIN IMMEDIATE").unwrap();
    let mut insert = conn.prepare("INSERT INTO other(v) VALUES (1)").unwrap();
    assert!(matches!(insert.step().unwrap(), StepResult::Busy));
    while next_id(&mut select).is_some() {}
    other_conn.execute("INSERT INTO t VALUES (2001)").unwrap();
    other_conn.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![rusqlite::types::Value::Integer(ROWS)]],
        "the waiting INSERT must keep the read snapshot open"
    );
    let err = insert
        .step()
        .expect_err("the write must fail because the read snapshot is stale");
    assert!(
        matches!(err, LimboError::BusySnapshot),
        "unexpected error: {err:?}"
    );
    insert.reset().unwrap();

    conn.execute("INSERT INTO other(v) VALUES (2)").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![rusqlite::types::Value::Integer(ROWS + 1)]]
    );
}

#[test]
fn test_open_blob_handle_keeps_read_transaction_open_after_sibling_finishes() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let other_conn = tmp_db.connect_limbo();
    create_table_with_rows(&conn);
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, data BLOB)")
        .unwrap();
    conn.execute("INSERT INTO b VALUES (1, x'01020304')")
        .unwrap();

    let mut select = conn.prepare("SELECT id FROM t").unwrap();
    assert_eq!(next_id(&mut select), Some(1));
    let mut blob = conn.blob_open("b", "data", 1, false).unwrap();
    while next_id(&mut select).is_some() {}
    other_conn
        .execute("UPDATE b SET data = x'0a0b0c0d' WHERE id = 1")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&other_conn, "PRAGMA wal_checkpoint(TRUNCATE)")[0][0],
        rusqlite::types::Value::Integer(1),
        "the open blob handle must keep its read lock"
    );
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    blob.close().unwrap();

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT hex(data) FROM b"),
        vec![vec![rusqlite::types::Value::Text("0A0B0C0D".to_string())]]
    );
    assert_eq!(
        limbo_exec_rows(&other_conn, "PRAGMA wal_checkpoint(TRUNCATE)")[0][0],
        rusqlite::types::Value::Integer(0),
        "closing the blob handle must end the read transaction"
    );
}
