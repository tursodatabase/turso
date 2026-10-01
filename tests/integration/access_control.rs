use std::sync::Arc;

use turso_core::{Database, DatabaseOpts, IOResult, OpenFlags, SqliteDialect, IO};

use crate::common::{ExecRows, TempDatabase};
use crate::queued_io::QueuedIo;

fn assert_error_contains(result: turso_core::Result<()>, expected: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(expected),
        "expected \"{expected}\", got: {err}"
    );
}

#[test]
fn test_create_and_drop_role() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    conn.execute("CREATE ROLE alice").unwrap();
    assert_error_contains(conn.execute("CREATE ROLE alice"), "already exists");
    conn.execute("DROP ROLE alice").unwrap();
    assert_error_contains(conn.execute("DROP ROLE alice"), "does not exist");
    conn.execute("DROP ROLE IF EXISTS alice").unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
}

#[test]
fn test_public_role_name_is_reserved() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    assert_error_contains(conn.execute("CREATE ROLE public"), "reserved");
}

#[test]
fn test_roles_survive_reopen() {
    for mvcc in [false, true] {
        let db = TempDatabase::builder().with_mvcc(mvcc).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE ROLE alice").unwrap();
        conn.execute("CREATE ROLE bob").unwrap();
        conn.execute("DROP ROLE bob").unwrap();
        conn.close().unwrap();
        let path = db.path.clone();
        drop(db);

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        assert_error_contains(conn.execute("CREATE ROLE alice"), "already exists");
        conn.execute("CREATE ROLE bob").unwrap();
    }
}

#[test]
fn test_role_created_on_one_connection_is_seen_by_another() {
    for mvcc in [false, true] {
        let db = TempDatabase::builder().with_mvcc(mvcc).build();
        let first = db.connect_limbo();
        let second = db.connect_limbo();
        second.execute("CREATE TABLE t(x)").unwrap();
        first.execute("CREATE ROLE alice").unwrap();
        assert_error_contains(second.execute("CREATE ROLE alice"), "already exists");
    }
}

/// Opening a database through `open_async` must not wait for I/O while it
/// reads the roles: the same database opens with the same number of blocking
/// waits with and without roles.
#[test]
fn test_roles_load_without_blocking_on_io() {
    let without_roles = blocking_waits_while_opening(&["CREATE TABLE t(x)"]);
    let with_roles = blocking_waits_while_opening(&["CREATE TABLE t(x)", "CREATE ROLE alice"]);
    assert_eq!(with_roles, without_roles);
}

fn blocking_waits_while_opening(statements: &[&str]) -> usize {
    let io = Arc::new(QueuedIo::new());
    let path = "roles-async-open.db";
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        for sql in statements {
            conn.execute(sql).unwrap();
        }
        conn.close().unwrap();
    }

    let file = io.open_file(path, OpenFlags::default(), false).unwrap();
    let options = turso_core::OpenOptions::new(Arc::new(SqliteDialect)).storage(Arc::new(
        turso_core::storage::database::DatabaseFile::new(file),
    ));
    let mut state = turso_core::OpenDbAsyncState::new();
    let waits_before = io.blocking_waits();
    loop {
        match Database::open_async(&mut state, io.clone(), path, &options).unwrap() {
            IOResult::Done(_) => break,
            IOResult::IO(_) => {
                io.step_one().unwrap();
            }
        }
    }
    io.blocking_waits() - waits_before
}

/// With MVCC, a role can still be only in the MVCC log when the database is
/// opened again. It must be loaded after the log is replayed.
#[test]
fn test_role_in_mvcc_log_survives_reopen() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let path = db.path.clone();
    let io = db.io.clone();
    {
        let conn = db.connect_limbo();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.execute("CREATE ROLE alice").unwrap();
    }
    drop(db);

    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    assert_error_contains(conn.execute("CREATE ROLE alice"), "already exists");
}

/// Loading the roles while opening must not leave a transaction open, or a
/// checkpoint would wait for it for as long as the caller keeps the open
/// state, which the bindings do for the lifetime of the database.
#[test]
fn test_loading_roles_does_not_block_checkpoint() {
    for mvcc in [false, true] {
        let io = Arc::new(QueuedIo::new());
        let path = "roles-checkpoint.db";
        {
            let db = Database::open_file_with_flags(
                io.clone(),
                path,
                OpenFlags::default(),
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            if mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            }
            conn.execute("CREATE ROLE alice").unwrap();
            conn.close().unwrap();
        }

        let file = io.open_file(path, OpenFlags::default(), false).unwrap();
        let options = turso_core::OpenOptions::new(Arc::new(SqliteDialect)).storage(Arc::new(
            turso_core::storage::database::DatabaseFile::new(file),
        ));
        let mut state = turso_core::OpenDbAsyncState::new();
        let db = loop {
            match Database::open_async(&mut state, io.clone(), path, &options).unwrap() {
                IOResult::Done(db) => break db,
                IOResult::IO(_) => {
                    io.step_one().unwrap();
                }
            }
        };
        let conn = db.connect().unwrap();
        let rows: Vec<(i64, i64, i64)> = conn.exec_rows("PRAGMA wal_checkpoint(TRUNCATE)");
        assert_eq!(rows[0].0, 0, "checkpoint was busy, mvcc={mvcc}");
        drop(state);
    }
}

/// A role statement that fails, here because it cannot run in a concurrent
/// transaction, must leave the roles as they were.
#[test]
fn test_failed_role_statement_leaves_roles_unchanged() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_limbo();
    conn.execute("CREATE ROLE bob").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    for _ in 0..2 {
        assert_error_contains(conn.execute("CREATE ROLE alice"), "exclusive transaction");
        assert_error_contains(conn.execute("DROP ROLE bob"), "exclusive transaction");
    }
    conn.execute("ROLLBACK").unwrap();
}
