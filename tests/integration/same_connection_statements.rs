//! A connection runs one statement of the application at a time. While a
//! statement is in progress (it returned a row, or it waits for I/O, and it is
//! not done or reset), another statement on the same connection fails at its
//! first step with `StatementsInProgress`. SQLite permits such statements, but
//! the engine cannot keep the state of the open statement correct while
//! another statement changes it. The issue tests below show bugs that came
//! from this.

use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, LimboError, Statement, StepResult, Value};
use turso_sqlite::SqliteDialect;

use crate::common::{ExecRows, TempDatabase};
use crate::queued_io::QueuedIo;

#[turso_macros::test(mvcc)]
fn statement_is_refused_while_a_select_has_unread_rows(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    assert_eq!(next_row(&mut select), Some(vec![Value::from_i64(1)]));

    for sql in [
        "SELECT 1",
        "INSERT INTO t VALUES (4)",
        "BEGIN",
        "PRAGMA user_version = 1",
    ] {
        let err = conn.execute(sql).unwrap_err();
        assert!(
            matches!(err, LimboError::StatementsInProgress(_)),
            "{sql}: {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "cannot start a statement - SQL statements in progress"
        );
    }

    assert_eq!(ints(rest_rows(&mut select)), vec![2, 3]);
    // The SELECT is done, so it is not in progress, also without a reset.
    conn.execute("INSERT INTO t VALUES (4)").unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(4,)]);
}

#[turso_macros::test(mvcc)]
fn statement_can_start_after_the_open_statement_is_reset_or_dropped(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    assert!(next_row(&mut select).is_some());
    select.reset().unwrap();
    conn.execute("INSERT INTO t VALUES (4)").unwrap();

    assert!(next_row(&mut select).is_some());
    drop(select);
    conn.execute("INSERT INTO t VALUES (5)").unwrap();

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(5,)]);
}

#[turso_macros::test(mvcc)]
fn refused_statement_runs_when_stepped_again_after_the_other_is_done(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();

    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    assert!(next_row(&mut select).is_some());
    let mut insert = conn.prepare("INSERT INTO t VALUES (3)").unwrap();
    assert_refused(insert.step());

    assert_eq!(ints(rest_rows(&mut select)), vec![2]);
    insert.run_ignore_rows().unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t ORDER BY x");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);
}

#[turso_macros::test(mvcc)]
fn statements_of_another_connection_are_not_refused(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let other = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();

    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    assert!(next_row(&mut select).is_some());
    other.execute("INSERT INTO t VALUES (3)").unwrap();
    let rows: Vec<(i64,)> = other.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);
    assert_eq!(ints(rest_rows(&mut select)), vec![2]);
}

#[test]
fn statement_is_refused_while_a_select_waits_for_io() {
    let io = Arc::new(QueuedIo::new());
    let path = "statement-refused-during-io.db";
    {
        let conn = open_queued_io_db(&io, path);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        conn.execute("INSERT INTO t SELECT value, zeroblob(100) FROM generate_series(1, 200)")
            .unwrap();
        conn.close().unwrap();
    }

    // A new Database starts with a cold page cache, so the scan waits for I/O.
    let conn = open_queued_io_db(&io, path);
    let mut select = conn.prepare("SELECT id FROM t").unwrap();
    assert!(matches!(select.step().unwrap(), StepResult::IO));
    assert_refused(conn.execute("INSERT INTO t VALUES (1000, zeroblob(100))"));

    let ids = ints(rest_rows_with_queued_io(&mut select, &io));
    assert_eq!(ids, (1..=200).collect::<Vec<_>>());
}

#[test]
fn open_blob_handle_does_not_refuse_statements() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, x'0102030405')")
        .unwrap();

    let mut blob = conn.blob_open("t", "b", 1, false).unwrap();
    conn.execute("INSERT INTO t VALUES (2, x'06')").unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);

    // A blob handle can also open while a SELECT is in progress.
    let mut select = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
    assert!(next_row(&mut select).is_some());
    let second = conn.blob_open("t", "b", 2, false).unwrap();
    assert_eq!(ints(rest_rows(&mut select)), vec![2]);

    let mut buf = [0u8; 5];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [1, 2, 3, 4, 5]);
    blob.close().unwrap();
    second.close().unwrap();
}

/// Issue #9296: INSERTs on the connection of an open SELECT started an
/// auto-checkpoint that ended the SELECT early, and a backward scan panicked.
#[turso_macros::test]
fn insert_cannot_end_an_open_select_through_auto_checkpoint(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t SELECT value FROM generate_series(1, 2000)")
        .unwrap();
    conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    for order in ["ASC", "DESC"] {
        let mut select = conn
            .prepare(format!("SELECT id FROM t ORDER BY id {order}"))
            .unwrap();
        let mut ids = Vec::new();
        while let Some(row) = next_row(&mut select) {
            ids.push(row[0].as_int().unwrap());
            assert_refused(conn.execute("INSERT INTO other(v) VALUES (1)"));
        }
        assert_eq!(ids.len(), 2000, "ORDER BY id {order}");
    }
}

/// Issue #9322: DROP TABLE ran while a SELECT on the table was open, and the
/// SELECT then returned rows of another table. DROP INDEX and ALTER TABLE
/// ended an open scan early.
#[turso_macros::test]
fn schema_change_is_refused_while_a_select_reads_the_table(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, pad TEXT, b TEXT)")
        .unwrap();
    conn.execute(
        "INSERT INTO t SELECT value * 10, printf('%.100c', 'x'), printf('b%05d', value) \
         FROM generate_series(1, 300)",
    )
    .unwrap();
    conn.execute("CREATE INDEX tb ON t(b)").unwrap();

    for query in [
        "SELECT a FROM t",
        "SELECT a FROM t INDEXED BY tb ORDER BY b",
    ] {
        let mut select = conn.prepare(query).unwrap();
        let mut ids = Vec::new();
        for _ in 0..10 {
            ids.push(next_row(&mut select).unwrap()[0].as_int().unwrap());
        }
        for sql in [
            "DROP TABLE t",
            "DROP INDEX tb",
            "ALTER TABLE t ALTER COLUMN b TO c BLOB",
            "CREATE TABLE u(b INTEGER PRIMARY KEY, pad TEXT)",
        ] {
            assert_refused(conn.execute(sql));
        }
        ids.extend(ints(rest_rows(&mut select)));
        assert_eq!(
            ids,
            (1..=300).map(|n| n * 10).collect::<Vec<_>>(),
            "{query}"
        );
    }
}

/// Issue #8637: a SELECT that was open while the connection ran INSERT,
/// DROP TABLE and CREATE TABLE returned the rows of the new table.
#[turso_macros::test]
fn select_cannot_read_a_table_created_while_it_is_open(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t SELECT value FROM generate_series(1, 20)")
        .unwrap();

    let mut select = conn.prepare("SELECT k FROM t").unwrap();
    assert_eq!(next_row(&mut select), Some(vec![Value::from_i64(1)]));
    assert_refused(conn.execute("INSERT INTO t VALUES (500)"));
    assert_refused(conn.execute("DROP TABLE t"));
    assert_refused(conn.execute("CREATE TABLE u(k INTEGER PRIMARY KEY)"));
    assert_eq!(ints(rest_rows(&mut select)), (2..=20).collect::<Vec<_>>());
}

/// Issue #9323: a failed statement, ROLLBACK TO or ROLLBACK on the connection
/// of an open SELECT ended the SELECT with no error.
#[turso_macros::test]
fn rollback_cannot_end_an_open_select(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t SELECT value FROM generate_series(1, 300)")
        .unwrap();
    conn.execute("CREATE TABLE seen(k INTEGER UNIQUE)").unwrap();

    let mut select = conn.prepare("SELECT a FROM t").unwrap();
    let mut visited = 0;
    while let Some(row) = next_row(&mut select) {
        visited += 1;
        let a = row[0].as_int().unwrap();
        assert_refused(conn.execute(format!("INSERT INTO seen VALUES ({})", a % 7)));
    }
    assert_eq!(visited, 300);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let mut select = conn.prepare("SELECT a FROM t").unwrap();
    for _ in 0..10 {
        assert!(next_row(&mut select).is_some());
    }
    assert_refused(conn.execute("ROLLBACK TO sp"));
    assert_refused(conn.execute("ROLLBACK"));
    assert_eq!(rest_rows(&mut select).len(), 290);
    conn.execute("ROLLBACK").unwrap();
}

/// Issue #9364: a statement that failed inside an explicit transaction ended
/// the other open scans of the connection.
#[turso_macros::test]
fn failed_statement_in_a_transaction_cannot_end_an_open_scan(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    conn.execute("CREATE TABLE u(b UNIQUE)").unwrap();
    conn.execute("INSERT INTO u VALUES (10)").unwrap();

    conn.execute("BEGIN").unwrap();
    let mut scan = conn.prepare("SELECT a FROM t").unwrap();
    assert_eq!(next_row(&mut scan), Some(vec![Value::from_i64(1)]));
    assert_refused(conn.execute("INSERT INTO u VALUES (20), (10)"));
    assert_eq!(ints(rest_rows(&mut scan)), vec![2, 3]);
    conn.execute("COMMIT").unwrap();
}

/// Issue #11542: BEGIN ran while a SELECT was open, and the reset of the
/// SELECT then rolled back the explicit transaction.
#[turso_macros::test]
fn begin_is_refused_while_a_select_is_open(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();

    let mut select = conn.prepare("SELECT x FROM t").unwrap();
    assert!(next_row(&mut select).is_some());
    assert_refused(conn.execute("BEGIN"));
    assert_refused(conn.execute("SAVEPOINT sp"));
    assert!(conn.get_auto_commit());
    select.reset().unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    conn.execute("COMMIT").unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);
}

/// Issue #11540: a statement that was prepared before a schema change did its
/// first step while an INSERT ... RETURNING was in progress. Its new
/// preparation rolled back the write of the INSERT.
#[turso_macros::test]
fn stale_statement_cannot_roll_back_an_insert_in_progress(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE u(y)").unwrap();
    let mut stale = conn.prepare("SELECT y FROM u").unwrap();
    conn.execute("CREATE TABLE v(z)").unwrap();

    let mut insert = conn
        .prepare("INSERT INTO t(x) VALUES (1) RETURNING id")
        .unwrap();
    assert_eq!(next_row(&mut insert), Some(vec![Value::from_i64(1)]));
    assert_refused(stale.step());
    assert!(rest_rows(&mut insert).is_empty());

    for conn in [conn.clone(), tmp_db.connect_limbo()] {
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(1,)]);
    }
    assert!(stale.run_collect_rows().unwrap().is_empty());
}

/// Issue #11543: an INSERT ... RETURNING started while a SELECT with a window
/// function was in progress, and the reset of the SELECT then panicked with
/// "shared autocommit transaction needed full rollback outside MVCC".
#[turso_macros::test]
fn insert_is_refused_while_a_window_select_is_open(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)")
        .unwrap();
    conn.execute("INSERT INTO t(a) VALUES (1), (2)").unwrap();

    let mut window = conn
        .prepare("SELECT id, count(a) OVER (ORDER BY id) FROM t")
        .unwrap();
    assert!(next_row(&mut window).is_some());
    let mut insert = conn
        .prepare("INSERT INTO t(a) VALUES (3) RETURNING id")
        .unwrap();
    assert_refused(insert.step());
    window.reset().unwrap();

    assert_eq!(next_row(&mut insert), Some(vec![Value::from_i64(3)]));
    assert!(rest_rows(&mut insert).is_empty());
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);
}

/// Issue #7728: DROP TABLE, CREATE TABLE and INSERT ran while a SELECT on the
/// table waited for I/O. The SELECT then read the recycled root page and
/// panicked.
#[test]
fn drop_table_is_refused_while_a_select_waits_for_io() {
    let io = Arc::new(QueuedIo::new());
    let path = "drop-table-refused-during-io.db";
    {
        let conn = open_queued_io_db(&io, path);
        conn.execute("PRAGMA page_size=512").unwrap();
        conn.execute("PRAGMA cache_size=9").unwrap();
        conn.execute("PRAGMA cache_spill=ON").unwrap();
        conn.execute("PRAGMA journal_mode='wal'").unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        for id in 1..=32 {
            conn.execute(format!("INSERT INTO u VALUES({id}, zeroblob(60))"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    let conn = open_queued_io_db(&io, path);
    let mut select = conn
        .prepare("SELECT id, length(b) FROM u WHERE id = 16")
        .unwrap();
    assert!(matches!(select.step().unwrap(), StepResult::IO));
    io.step_one().unwrap();
    assert_refused(conn.execute("DROP TABLE u"));
    assert_refused(conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)"));
    // CREATE TABLE was refused, so the INSERT of the issue has no table.
    let err = conn
        .execute("INSERT INTO reuse VALUES(16, zeroblob(5000))")
        .unwrap_err();
    assert!(err.to_string().contains("no such table: reuse"), "{err}");
    assert_eq!(
        rest_rows_with_queued_io(&mut select, &io),
        vec![vec![Value::from_i64(16), Value::from_i64(60)]]
    );
}

/// Issue #9329: a write while a table scan waited for I/O in a descent to the
/// next leaf failed with "save_position: table cursor with has_record=true
/// must be on a leaf".
#[test]
fn write_is_refused_while_a_scan_waits_for_io_in_a_descent() {
    let io = Arc::new(QueuedIo::new());
    let path = "write-refused-during-descent.db";
    {
        let conn = open_queued_io_db(&io, path);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        conn.execute("INSERT INTO t SELECT value, randomblob(100) FROM generate_series(1, 200)")
            .unwrap();
        conn.close().unwrap();
    }

    let conn = open_queued_io_db(&io, path);
    let mut scan = conn.prepare("SELECT id FROM t").unwrap();
    let mut ids = Vec::new();
    let mut refused_writes = 0;
    loop {
        match scan.step().unwrap() {
            StepResult::Row => ids.push(scan.row().unwrap().get::<i64>(0).unwrap()),
            StepResult::IO => {
                // After the first row, I/O is a load of the next leaf page.
                if !ids.is_empty() {
                    assert_refused(conn.execute("INSERT INTO t VALUES (100000, 1)"));
                    refused_writes += 1;
                }
                io.step_one().unwrap();
            }
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
    assert!(refused_writes > 0, "the scan never waited for a leaf page");
    assert_eq!(ids, (1..=200).collect::<Vec<_>>());
    conn.execute("INSERT INTO t VALUES (100000, 1)").unwrap();
}

/// Issue #9328: with io_uring, an INSERT from another task on the same
/// connection ran while a SELECT waited for I/O. The SELECT ended early, or
/// the commit of the INSERT panicked.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[test]
fn insert_is_refused_while_a_select_waits_for_io_uring() {
    let tmp_db = TempDatabase::builder().with_io_uring(true).build();
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA cache_size = 4").unwrap();
    conn.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn.execute("INSERT INTO big SELECT value, zeroblob(2000) FROM generate_series(1, 800)")
        .unwrap();

    for explicit_transaction in [false, true] {
        if explicit_transaction {
            conn.execute("BEGIN").unwrap();
        }
        let mut select = conn.prepare("SELECT id FROM big").unwrap();
        let mut seen = 0;
        let mut refused_writes = 0;
        loop {
            match select.step().unwrap() {
                StepResult::Row => seen += 1,
                StepResult::IO => {
                    assert_refused(conn.execute("INSERT INTO big VALUES (NULL, zeroblob(2000))"));
                    refused_writes += 1;
                    select._io().step().unwrap();
                }
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        assert!(refused_writes > 0, "the SELECT never waited for I/O");
        assert_eq!(seen, 800);
        if explicit_transaction {
            conn.execute("COMMIT").unwrap();
        }
    }
}

/// Issue #10622: in MVCC mode, an INSERT that ran beside an open reader joined
/// the transaction of the reader. A later write-write conflict rolled back
/// that INSERT after it had returned Ok.
#[test]
fn mvcc_insert_is_refused_while_a_reader_is_open() {
    let tmp_db = TempDatabase::new_with_mvcc("mvcc-insert-beside-reader.db");
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();
    let observer = tmp_db.connect_limbo();
    conn1
        .execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        .unwrap();
    conn1
        .execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    conn1
        .execute("INSERT INTO accounts VALUES (1, 100), (2, 200)")
        .unwrap();

    let mut reader = conn2
        .prepare("SELECT id FROM accounts ORDER BY id")
        .unwrap();
    assert!(next_row(&mut reader).is_some());
    let insert = conn2.execute("INSERT INTO audit VALUES (1, 'payment recorded')");
    let insert_returned_ok = insert.is_ok();
    assert_refused(insert);
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("UPDATE accounts SET balance = 150 WHERE id = 1")
        .unwrap();
    let _ = conn2.execute("UPDATE accounts SET balance = 90 WHERE id = 1");
    conn1.execute("COMMIT").unwrap();
    drop(reader);

    // An INSERT that returned Ok must stay committed.
    let rows: Vec<(i64,)> = observer.exec_rows("SELECT count(*) FROM audit");
    assert_eq!(rows, vec![(i64::from(insert_returned_ok),)]);
}

/// Issues #9327 and #9619: in MVCC mode, COMMIT or ROLLBACK ran while a SELECT
/// of the transaction was open. When the SELECT continued, it panicked with
/// "transaction should exist in txs map" or "transaction should exist while
/// its cursor is active".
#[test]
fn mvcc_commit_and_rollback_are_refused_while_a_select_of_the_transaction_is_open() {
    let tmp_db = TempDatabase::new_with_mvcc("mvcc-commit-beside-select.db");
    let conn = tmp_db.connect_limbo();
    let other = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    // A row of another transaction that stays uncommitted.
    other.execute("BEGIN").unwrap();
    other.execute("INSERT INTO t VALUES (4, 'd')").unwrap();

    for checkpoint_first in [false, true] {
        if checkpoint_first {
            other.execute("ROLLBACK").unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        for query in [
            "SELECT id FROM t",
            "SELECT id FROM t INDEXED BY idx_v WHERE v > ''",
        ] {
            conn.execute("BEGIN").unwrap();
            let mut select = conn.prepare(query).unwrap();
            assert_eq!(next_row(&mut select), Some(vec![Value::from_i64(1)]));
            assert_refused(conn.execute("COMMIT"));
            assert_refused(conn.execute("ROLLBACK"));
            assert_eq!(ints(rest_rows(&mut select)), vec![2, 3], "{query}");
            conn.execute("COMMIT").unwrap();
        }
    }
}

fn open_queued_io_db(io: &Arc<QueuedIo>, path: &str) -> Arc<Connection> {
    Database::open_file(io.clone(), path, Arc::new(SqliteDialect))
        .unwrap()
        .connect(ConnectOpts::new())
        .unwrap()
}

fn assert_refused<T: std::fmt::Debug>(result: turso_core::Result<T>) {
    match result {
        Err(LimboError::StatementsInProgress(_)) => {}
        other => panic!("expected StatementsInProgress, got {other:?}"),
    }
}

/// Steps `stmt` to its next row. Returns `None` when the statement is done.
fn next_row(stmt: &mut Statement) -> Option<Vec<Value>> {
    stmt.run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .map(|row| row.get_values().cloned().collect())
}

fn rest_rows(stmt: &mut Statement) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(row) = next_row(stmt) {
        rows.push(row);
    }
    rows
}

/// Like [`rest_rows`], for a statement on a database that uses `io`.
fn rest_rows_with_queued_io(stmt: &mut Statement, io: &QueuedIo) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::IO | StepResult::Yield => {
                io.step_one().unwrap();
            }
            StepResult::Done => return rows,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn ints(rows: Vec<Vec<Value>>) -> Vec<i64> {
    rows.into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect()
}
