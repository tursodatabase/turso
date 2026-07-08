#![cfg(feature = "io_memory_yield")]

use std::sync::Arc;

use turso_core::{Connection, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, StepResult, IO};

fn open_conn(io: Arc<MemoryYieldIO>, path: &str) -> Arc<Connection> {
    Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None)
        .unwrap()
        .connect()
        .unwrap()
}

fn integrity_check(conn: &Arc<Connection>) -> Vec<String> {
    conn.prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}

fn ids(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    conn.prepare(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect()
}

/// Steps `sql` until the `target_io`-th `StepResult::IO`, completes that I/O,
/// then drops the statement without stepping it again. Returns false if the
/// statement finished before reaching `target_io` I/Os.
fn abandon_statement_after_io_completion(
    conn: &Arc<Connection>,
    io: &Arc<MemoryYieldIO>,
    sql: &str,
    target_io: usize,
) -> bool {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut io_count = 0;

    loop {
        match stmt.step().unwrap() {
            StepResult::IO => {
                io_count += 1;
                io.step().unwrap();
                if io_count == target_io {
                    drop(stmt);
                    return true;
                }
            }
            StepResult::Done => return false,
            StepResult::Yield => {}
            other => panic!("unexpected step result while abandoning statement: {other:?}"),
        }
    }
}

/// Steps `sql` until the `target_io`-th `StepResult::IO` and drops the
/// statement while that I/O is still pending. Returns false if the statement
/// finished before reaching `target_io` I/Os.
fn abandon_statement_before_io_completion(
    conn: &Arc<Connection>,
    io: &Arc<MemoryYieldIO>,
    sql: &str,
    target_io: usize,
) -> bool {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut io_count = 0;

    loop {
        match stmt.step().unwrap() {
            StepResult::IO => {
                io_count += 1;
                if io_count == target_io {
                    drop(stmt);
                    return true;
                }
                io.step().unwrap();
            }
            StepResult::Done => return false,
            StepResult::Yield => {}
            other => panic!("unexpected step result while abandoning statement: {other:?}"),
        }
    }
}

fn seed_indexed_overflow_rows(conn: &Arc<Connection>, payload: &str) {
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x, b TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_x ON t(x)").unwrap();
    for id in 1..=8 {
        conn.execute(format!("INSERT INTO t VALUES({id}, {id}, '{payload}')"))
            .unwrap();
    }
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn test_abandoned_insert_savepoint_rollback_preserves_freelist() {
    for target_io in 1..=512 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("abandoned-overflow-insert-{target_io}.db"));
        let path = path.to_str().unwrap();

        {
            let conn = open_conn(io.clone(), path);
            conn.execute("PRAGMA page_size=512").unwrap();
            conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
                .unwrap();
            conn.execute(
                "INSERT INTO t VALUES
                 (1, zeroblob(50000)),
                 (2, zeroblob(50000)),
                 (3, zeroblob(50000)),
                 (4, zeroblob(50000)),
                 (5, zeroblob(50000))",
            )
            .unwrap();
            conn.execute("DELETE FROM t WHERE id IN (2,3,4)").unwrap();
        }

        let conn = open_conn(io.clone(), path);
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT s").unwrap();

        if !abandon_statement_before_io_completion(
            &conn,
            &io,
            "INSERT INTO t VALUES (100, zeroblob(50000))",
            target_io,
        ) {
            conn.execute("ROLLBACK").unwrap();
            break;
        }

        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("RELEASE s").unwrap();

        let rows = conn
            .prepare("SELECT id, length(b) FROM t ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| (row[0].as_int().unwrap(), row[1].as_int().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![(1, 50000), (5, 50000)],
            "target_io={target_io}: abandoned INSERT changed visible table rows"
        );

        conn.execute("INSERT INTO t VALUES (200, zeroblob(50000))")
            .unwrap();
        conn.execute("COMMIT").unwrap();

        assert_eq!(
            integrity_check(&conn),
            vec!["ok"],
            "target_io={target_io}: abandoned INSERT poisoned freelist state"
        );
    }
}

#[test]
fn test_abandoned_delete_does_not_poison_next_delete() {
    let payload = "x".repeat(20_000);

    for target_io in 1..=128 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("abandoned-delete-{target_io}.db"));
        let path = path.to_str().unwrap();

        {
            let conn = open_conn(io.clone(), path);
            seed_indexed_overflow_rows(&conn, &payload);
        }

        let conn = open_conn(io.clone(), path);
        conn.execute("BEGIN").unwrap();

        if !abandon_statement_after_io_completion(
            &conn,
            &io,
            "DELETE FROM t WHERE id IN (2,3)",
            target_io,
        ) {
            conn.execute("ROLLBACK").unwrap();
            break;
        }

        assert_eq!(
            ids(&conn, "SELECT id FROM t ORDER BY id"),
            vec![2, 3, 4, 5, 6, 7, 8],
            "target_io={target_io}: abandoned DELETE was not rolled back"
        );

        conn.execute("DELETE FROM t WHERE id = 4").unwrap();
        conn.execute("COMMIT").unwrap();

        assert_eq!(
            integrity_check(&conn),
            vec!["ok"],
            "target_io={target_io}: abandoned DELETE poisoned the next DELETE"
        );
        assert_eq!(
            ids(&conn, "SELECT id FROM t INDEXED BY t_x ORDER BY x"),
            vec![2, 3, 5, 6, 7, 8],
            "target_io={target_io}: indexed scan disagrees after next DELETE"
        );
    }
}

#[test]
fn test_abandoned_insert_does_not_poison_next_insert() {
    let payload = "x".repeat(20_000);

    for target_io in 1..=128 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("abandoned-insert-{target_io}.db"));
        let path = path.to_str().unwrap();

        {
            let conn = open_conn(io.clone(), path);
            seed_indexed_overflow_rows(&conn, &payload);
        }

        let conn = open_conn(io.clone(), path);
        conn.execute("BEGIN").unwrap();

        if !abandon_statement_after_io_completion(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES(9, 9, '{payload}')"),
            target_io,
        ) {
            conn.execute("ROLLBACK").unwrap();
            break;
        }

        assert_eq!(
            ids(&conn, "SELECT id FROM t ORDER BY id"),
            vec![2, 3, 4, 5, 6, 7, 8],
            "target_io={target_io}: abandoned INSERT was not rolled back"
        );

        conn.execute(format!("INSERT INTO t VALUES(10, 10, '{payload}')"))
            .unwrap();
        conn.execute("COMMIT").unwrap();

        assert_eq!(
            integrity_check(&conn),
            vec!["ok"],
            "target_io={target_io}: abandoned INSERT poisoned the next INSERT"
        );
        assert_eq!(
            ids(&conn, "SELECT id FROM t INDEXED BY t_x ORDER BY x"),
            vec![2, 3, 4, 5, 6, 7, 8, 10],
            "target_io={target_io}: indexed scan disagrees after next INSERT"
        );
    }
}
