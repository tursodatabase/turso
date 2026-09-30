use crate::assertions::{AssertColumn, Cell};
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;
use rusqlite::types::Value as RValue;
use turso_core::StepResult;

#[turso_macros::test(mvcc)]
fn test_pragma_module_list_returns_list(db: TempDatabase) {
    let conn = db.connect_limbo();

    let mut module_list = conn.query("PRAGMA module_list;").unwrap();

    let mut counter = 0;

    if let Some(ref mut rows) = module_list {
        while let StepResult::Row = rows.step().unwrap() {
            counter += 1;
        }
    }

    assert!(counter > 0)
}

#[turso_macros::test(mvcc)]
fn test_pragma_module_list_generate_series(db: TempDatabase) {
    let conn = db.connect_limbo();

    assert_that!(limbo_exec_rows(
        &conn,
        "SELECT * FROM generate_series(1, 3);"
    ))
    .is_equal_to(vec![row![1], row![2], row![3]]);

    assert_that!(limbo_exec_rows(&conn, "PRAGMA module_list;"))
        .named("module_list")
        .column(0)
        .contains(Cell::from("generate_series"));
}

#[turso_macros::test(mvcc)]
fn test_pragma_page_sizes_without_writes_persists(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    for test_page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let db = builder.clone().build();
        {
            let conn = db.connect_limbo();
            let pragma_query = format!("PRAGMA page_size={test_page_size}");
            conn.execute(&pragma_query).unwrap();
            conn.execute("PRAGMA user_version=1").unwrap(); // even sqlite behavior is that just changing page_size pragma doesn't persist it, so we do this to make a minimal persistent change
        }

        let conn = db.connect_limbo();
        assert_that!(limbo_exec_rows(&conn, "PRAGMA page_size"))
            .is_equal_to(vec![row![test_page_size]]);

        // Reopen database and verify page size
        let db = builder.clone().with_db_path(&db.path).build();
        let conn = db.connect_limbo();
        assert_that!(limbo_exec_rows(&conn, "PRAGMA page_size"))
            .named("page_size after reopen")
            .is_equal_to(vec![row![test_page_size]]);
    }
}

#[turso_macros::test(mvcc)]
fn test_pragma_page_sizes_with_writes_persists(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    for test_page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let db = builder.clone().build();
        {
            {
                let conn = db.connect_limbo();
                let pragma_query = format!("PRAGMA page_size={test_page_size}");
                conn.execute(&pragma_query).unwrap();

                // Create table and insert data
                conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
                    .unwrap();
                conn.execute("INSERT INTO test (id, value) VALUES (1, 'test data')")
                    .unwrap();
                // Insert a big blob just as a small smoke test that our btree handles this well with different page sizes.
                conn.execute("INSERT INTO test (id, value) VALUES (2, randomblob(1024*1024))")
                    .unwrap();
                assert_that!(conn.pragma_query("page_size").unwrap())
                    .is_equal_to(vec![row![test_page_size]]);
            } // Connection is dropped here

            // Reopen database and verify page size and data
            let conn = db.connect_limbo();

            // Check page size is still test_page_size
            assert_that!(conn.pragma_query("page_size").unwrap())
                .named("page_size after reconnect")
                .is_equal_to(vec![row![test_page_size]]);

            // Verify data can still be read
            assert_that!(limbo_exec_rows(
                &conn,
                "SELECT value FROM test WHERE id = 1"
            ))
            .is_equal_to(vec![row!["test data"]]);
        }

        // Drop the db and reopen it, and verify the same
        let db = builder.clone().with_db_path(&db.path).build();
        let conn = db.connect_limbo();
        assert_that!(conn.pragma_query("page_size").unwrap())
            .named("page_size after reopen")
            .is_equal_to(vec![row![test_page_size]]);
    }
}

#[cfg(target_vendor = "apple")]
#[turso_macros::test(mvcc)]
fn test_pragma_fullfsync(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a test table
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // Query default value (should be 0/off)
    assert_that!(limbo_exec_rows(&conn, "PRAGMA fullfsync"))
        .named("fullfsync default")
        .is_equal_to(vec![row![0]]);

    // Enable fullfsync
    conn.execute("PRAGMA fullfsync=1").unwrap();
    assert_that!(limbo_exec_rows(&conn, "PRAGMA fullfsync"))
        .named("enabled fullfsync")
        .is_equal_to(vec![row![1]]);

    // Do an insert with fullfsync enabled
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'with fullfsync')")
        .unwrap();
    assert_that!(limbo_exec_rows(&conn, "PRAGMA fullfsync"))
        .named("fullfsync after an insert")
        .is_equal_to(vec![row![1]]);

    // Disable fullfsync
    conn.execute("PRAGMA fullfsync=0").unwrap();
    assert_that!(limbo_exec_rows(&conn, "PRAGMA fullfsync"))
        .named("disabled fullfsync")
        .is_equal_to(vec![row![0]]);

    // Do an insert with fullfsync disabled
    conn.execute("INSERT INTO test (id, value) VALUES (2, 'without fullfsync')")
        .unwrap();
    assert_that!(limbo_exec_rows(&conn, "PRAGMA fullfsync"))
        .named("fullfsync after an insert")
        .is_equal_to(vec![row![0]]);

    // Verify both rows exist
    assert_that!(limbo_exec_rows(&conn, "SELECT COUNT(*) FROM test"))
        .named("row count")
        .is_equal_to(vec![row![2]]);
}

#[cfg(not(target_vendor = "apple"))]
#[turso_macros::test(mvcc)]
fn test_pragma_fullfsync(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a test table
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // On non-Apple platforms, fullfsync is unknown and silently ignored (SQLite behavior).
    conn.execute("PRAGMA fullfsync=1").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_pragma_synchronous_normal(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a test table
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // Set synchronous=NORMAL (1)
    conn.execute("PRAGMA synchronous=NORMAL").unwrap();

    // Do inserts with synchronous=NORMAL (should skip WAL commit fsync)
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();

    // Verify data is there
    assert_that!(limbo_exec_rows(&conn, "SELECT COUNT(*) FROM test"))
        .named("row count with synchronous=NORMAL")
        .is_equal_to(vec![row![2]]);

    // Set synchronous=FULL (2) and do another insert
    conn.execute("PRAGMA synchronous=FULL").unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (3, 'third')")
        .unwrap();
    assert_that!(limbo_exec_rows(&conn, "SELECT COUNT(*) FROM test"))
        .named("row count with synchronous=FULL")
        .is_equal_to(vec![row![3]]);

    // Set synchronous=OFF (0) and do another insert
    conn.execute("PRAGMA synchronous=OFF").unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (4, 'fourth')")
        .unwrap();
    assert_that!(limbo_exec_rows(&conn, "SELECT COUNT(*) FROM test"))
        .named("row count with synchronous=OFF")
        .is_equal_to(vec![row![4]]);

    // Also test numeric values: 0, 1, 2
    conn.execute("PRAGMA synchronous=0").unwrap(); // OFF
    conn.execute("INSERT INTO test (id, value) VALUES (5, 'fifth')")
        .unwrap();
    conn.execute("PRAGMA synchronous=1").unwrap(); // NORMAL
    conn.execute("INSERT INTO test (id, value) VALUES (6, 'sixth')")
        .unwrap();
    conn.execute("PRAGMA synchronous=2").unwrap(); // FULL
    conn.execute("INSERT INTO test (id, value) VALUES (7, 'seventh')")
        .unwrap();

    assert_that!(limbo_exec_rows(&conn, "SELECT COUNT(*) FROM test"))
        .named("row count with numeric synchronous values")
        .is_equal_to(vec![row![7]]);
}

#[turso_macros::test(mvcc)]
fn test_pragma_cache_size_min_value(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Verify it doesn't panic on i64::MIN
    let min_val = i64::MIN;
    let query = format!("PRAGMA cache_size = {min_val}");
    conn.execute(&query).unwrap();

    // Check the value was reset to default (0 in this implementation's logic for overflow)
    assert_that!(limbo_exec_rows(&conn, "PRAGMA cache_size")).is_equal_to(vec![row![200]]);
}

#[turso_macros::test(mvcc)]
fn test_pragma_cache_size_i32_min_order_by(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Regression test for issue #7150: setting cache_size to i32::MIN and then
    // running an ORDER BY query used to panic in op_sorter_open due to i32::abs()
    // overflowing on i32::MIN.
    let min_val = i32::MIN;
    conn.execute(format!("PRAGMA cache_size = {min_val}"))
        .unwrap();

    // Sanity check: the connection must actually hold i32::MIN, otherwise the
    // ORDER BY below no longer exercises the overflow path in op_sorter_open.
    assert_that!(limbo_exec_rows(&conn, "PRAGMA cache_size"))
        .is_equal_to(vec![row![min_val as i64]]);

    conn.execute("CREATE TABLE items (id TEXT)").unwrap();
    conn.execute("INSERT INTO items VALUES ('b'), ('a')")
        .unwrap();

    assert_that!(limbo_exec_rows(&conn, "SELECT id FROM items ORDER BY id"))
        .is_equal_to(vec![row!["a"], row!["b"]]);
}

#[turso_macros::test]
fn test_pragma_wal_checkpoint_targets_attached_database(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Attach an in-memory database
    conn.execute("ATTACH ':memory:' AS aux").unwrap();

    // Write to the attached database to generate WAL frames on its pager
    conn.execute("CREATE TABLE aux.t1(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO aux.t1 VALUES(1, 'hello')")
        .unwrap();

    // Checkpoint the attached database — before the fix, this would target the
    // main pager instead. The aux pager should have WAL frames, so the returned
    // frame counts must be non-zero.
    assert_that!(limbo_exec_rows(&conn, "PRAGMA aux.wal_checkpoint"))
        .named("aux.wal_checkpoint")
        .single_element()
        .satisfies_with_message(
            "report [busy, log, checkpointed] with busy 0 and more than zero frames",
            |row| {
                matches!(
                    row[..],
                    [
                        RValue::Integer(0),
                        RValue::Integer(log),
                        RValue::Integer(checkpointed)
                    ] if log > 0 && checkpointed > 0
                )
            },
        );
}

// Regression tests for https://github.com/tursodatabase/turso/issues/7466:
// querying a pragma virtual table (pragma_table_info, pragma_function_list, ...)
// left the connection's implicit read transaction open, so every subsequent
// write on that connection reported success but was never committed.

#[turso_macros::test(mvcc)]
fn test_pragma_vtab_query_does_not_break_subsequent_writes(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    // Full scan of a pragma virtual table.
    let mut stmt = conn
        .query("SELECT * FROM pragma_table_info('t')")
        .unwrap()
        .unwrap();
    while let StepResult::Row = stmt.step().unwrap() {}
    drop(stmt);

    assert!(
        conn.get_auto_commit(),
        "autocommit must survive a pragma vtab query"
    );

    // This write must auto-commit at Halt like any other statement.
    conn.execute("INSERT INTO t VALUES (1, 'one')").unwrap();

    // A second connection only sees committed data.
    let conn2 = db.connect_limbo();
    assert_that!(limbo_exec_rows(&conn2, "SELECT a, b FROM t"))
        .named("rows committed after a pragma vtab query")
        .is_equal_to(vec![row![1, "one"]]);
}

#[turso_macros::test(mvcc)]
fn test_pragma_function_list_then_create_and_insert(db: TempDatabase) {
    let conn = db.connect_limbo();

    let mut stmt = conn
        .query("SELECT name FROM pragma_function_list()")
        .unwrap()
        .unwrap();
    let mut n = 0;
    while let StepResult::Row = stmt.step().unwrap() {
        n += 1;
    }
    assert!(n > 0, "pragma_function_list should return rows");
    drop(stmt);

    conn.execute("CREATE TABLE t (x)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();

    let conn2 = db.connect_limbo();
    assert_that!(limbo_exec_rows(&conn2, "SELECT x FROM t"))
        .named("rows committed after a pragma_function_list query")
        .is_equal_to(vec![row![42]]);
}

#[turso_macros::test(mvcc)]
fn test_pragma_vtab_partial_scan_does_not_break_subsequent_writes(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
        .unwrap();

    // Partial scan: step once, then abandon the statement mid-scan so cleanup
    // goes through the abort path instead of Halt.
    let mut stmt = conn
        .query("SELECT * FROM pragma_table_info('t')")
        .unwrap()
        .unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected at least one row from pragma_table_info");
    };
    drop(stmt);

    conn.execute("INSERT INTO t VALUES (1, 'one', 1.5)")
        .unwrap();

    let conn2 = db.connect_limbo();
    assert_that!(limbo_exec_rows(&conn2, "SELECT a FROM t"))
        .named("rows committed after an abandoned pragma vtab scan")
        .is_equal_to(vec![row![1]]);
}

#[turso_macros::test(mvcc)]
fn test_pragma_vtab_query_with_limit_then_write(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
        .unwrap();

    // LIMIT terminates the scan before the virtual table cursor is exhausted,
    // so the program halts while the cursor still holds its helper statement.
    let mut stmt = conn
        .query("SELECT * FROM pragma_table_info('t') LIMIT 1")
        .unwrap()
        .unwrap();
    while let StepResult::Row = stmt.step().unwrap() {}
    drop(stmt);

    conn.execute("INSERT INTO t VALUES (2, 'two', 2.5)")
        .unwrap();

    let conn2 = db.connect_limbo();
    assert_that!(limbo_exec_rows(&conn2, "SELECT a FROM t"))
        .named("rows committed after a pragma vtab query with LIMIT")
        .is_equal_to(vec![row![2]]);
}
