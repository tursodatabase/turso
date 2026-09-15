use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as RValue;
use turso_core::{Numeric, StepResult, Value};

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

    let mut rows = conn
        .query("SELECT * FROM generate_series(1, 3);")
        .expect("generate_series module not available")
        .expect("query did not return rows");

    let mut values = vec![];
    while let StepResult::Row = rows.step().unwrap() {
        let row = rows.row().unwrap();
        values.push(row.get_value(0).clone());
    }

    assert_eq!(
        values,
        vec![Value::from_i64(1), Value::from_i64(2), Value::from_i64(3),]
    );

    let mut module_list = conn.query("PRAGMA module_list;").unwrap();
    let mut found = false;

    if let Some(ref mut rows) = module_list {
        while let StepResult::Row = rows.step().unwrap() {
            let row = rows.row().unwrap();
            if let Value::Text(name) = row.get_value(0) {
                if name.as_str() == "generate_series" {
                    found = true;
                    break;
                }
            }
        }
    }

    assert!(found, "generate_series should appear in module_list");
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
        let mut rows = conn.query("PRAGMA page_size").unwrap().unwrap();
        let StepResult::Row = rows.step().unwrap() else {
            panic!("expected row");
        };
        let row = rows.row().unwrap();
        let Value::Numeric(Numeric::Integer(page_size)) = row.get_value(0) else {
            panic!("expected integer value");
        };
        assert_eq!(*page_size, test_page_size);

        // Reopen database and verify page size
        let db = builder.clone().with_db_path(&db.path).build();
        let conn = db.connect_limbo();
        let mut rows = conn.query("PRAGMA page_size").unwrap().unwrap();
        let StepResult::Row = rows.step().unwrap() else {
            panic!("expected row");
        };
        let row = rows.row().unwrap();
        let Value::Numeric(Numeric::Integer(page_size)) = row.get_value(0) else {
            panic!("expected integer value");
        };
        assert_eq!(*page_size, test_page_size);
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
                let mut page_size = conn.pragma_query("page_size").unwrap();
                let mut page_size = page_size.pop().unwrap();
                let page_size = page_size.pop().unwrap();
                let Value::Numeric(Numeric::Integer(page_size)) = page_size else {
                    panic!("expected integer value");
                };
                assert_eq!(page_size, test_page_size);
            } // Connection is dropped here

            // Reopen database and verify page size and data
            let conn = db.connect_limbo();

            // Check page size is still test_page_size
            let mut page_size = conn.pragma_query("page_size").unwrap();
            let mut page_size = page_size.pop().unwrap();
            let page_size = page_size.pop().unwrap();
            let Value::Numeric(Numeric::Integer(page_size)) = page_size else {
                panic!("expected integer value");
            };
            assert_eq!(page_size, test_page_size);

            // Verify data can still be read
            let mut rows = conn
                .query("SELECT value FROM test WHERE id = 1")
                .unwrap()
                .unwrap();
            rows.run_with_row_callback(|row| {
                let Value::Text(value) = row.get_value(0) else {
                    panic!("expected text value");
                };
                assert_eq!(value.as_str(), "test data");
                Ok(())
            })
            .unwrap();
        }

        // Drop the db and reopen it, and verify the same
        let db = builder.clone().with_db_path(&db.path).build();
        let conn = db.connect_limbo();
        let mut page_size = conn.pragma_query("page_size").unwrap();
        let mut page_size = page_size.pop().unwrap();
        let page_size = page_size.pop().unwrap();
        let Value::Numeric(Numeric::Integer(page_size)) = page_size else {
            panic!("expected integer value");
        };
        assert_eq!(page_size, test_page_size);
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
    let mut rows = conn.query("PRAGMA fullfsync").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 0, "fullfsync should default to 0");
    drop(rows);

    // Enable fullfsync
    conn.execute("PRAGMA fullfsync=1").unwrap();

    // Verify it's enabled
    let mut rows = conn.query("PRAGMA fullfsync").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 1, "fullfsync should be enabled");
    drop(rows);

    // Do an insert with fullfsync enabled
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'with fullfsync')")
        .unwrap();

    // Verify fullfsync is still enabled after insert
    let mut rows = conn.query("PRAGMA fullfsync").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 1, "fullfsync should still be enabled after insert");
    drop(rows);

    // Disable fullfsync
    conn.execute("PRAGMA fullfsync=0").unwrap();

    // Verify it's disabled
    let mut rows = conn.query("PRAGMA fullfsync").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 0, "fullfsync should be disabled");
    drop(rows);

    // Do an insert with fullfsync disabled
    conn.execute("INSERT INTO test (id, value) VALUES (2, 'without fullfsync')")
        .unwrap();

    // Verify fullfsync is still disabled after insert
    let mut rows = conn.query("PRAGMA fullfsync").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 0, "fullfsync should still be disabled after insert");
    drop(rows);

    // Verify both rows exist
    let mut rows = conn.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*count, 2, "both inserts should have succeeded");
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
    let mut rows = conn.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*count, 2, "both inserts should have succeeded");
    drop(rows);

    // Set synchronous=FULL (2) and do another insert
    conn.execute("PRAGMA synchronous=FULL").unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (3, 'third')")
        .unwrap();

    // Verify all data is there
    let mut rows = conn.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*count, 3, "all inserts should have succeeded");
    drop(rows);

    // Set synchronous=OFF (0) and do another insert
    conn.execute("PRAGMA synchronous=OFF").unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (4, 'fourth')")
        .unwrap();

    // Verify all data is there
    let mut rows = conn.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*count, 4, "all inserts should have succeeded");

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

    let mut rows = conn.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*count, 7, "all inserts should have succeeded");
}

#[turso_macros::test(mvcc)]
fn test_pragma_cache_size_min_value(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Verify it doesn't panic on i64::MIN
    let min_val = i64::MIN;
    let query = format!("PRAGMA cache_size = {min_val}");
    conn.execute(&query).unwrap();

    // Check the value was reset to default (0 in this implementation's logic for overflow)
    let mut rows = conn.query("PRAGMA cache_size").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 200);
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
    let rows = limbo_exec_rows(&conn, "PRAGMA cache_size");
    assert_eq!(rows, vec![vec![RValue::Integer(min_val as i64)]]);

    conn.execute("CREATE TABLE items (id TEXT)").unwrap();
    conn.execute("INSERT INTO items VALUES ('b'), ('a')")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT id FROM items ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![RValue::Text("a".to_string())],
            vec![RValue::Text("b".to_string())],
        ]
    );
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
    let rows = limbo_exec_rows(&conn, "PRAGMA aux.wal_checkpoint");
    assert_eq!(
        rows.len(),
        1,
        "wal_checkpoint should return exactly one row"
    );

    let row = &rows[0];
    // row is [busy, log, checkpointed]
    let RValue::Integer(busy) = &row[0] else {
        panic!("expected integer for busy flag, got {:?}", row[0]);
    };
    let RValue::Integer(log) = &row[1] else {
        panic!("expected integer for log frames, got {:?}", row[1]);
    };
    let RValue::Integer(checkpointed) = &row[2] else {
        panic!("expected integer for checkpointed frames, got {:?}", row[2]);
    };

    assert_eq!(*busy, 0, "checkpoint should not be busy");
    assert!(*log > 0, "aux pager should have WAL frames (got {log})");
    assert!(
        *checkpointed > 0,
        "aux pager should have checkpointed frames (got {checkpointed})"
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
    let rows = limbo_exec_rows(&conn2, "SELECT a, b FROM t");
    assert_eq!(
        rows,
        vec![vec![RValue::Integer(1), RValue::Text("one".to_string())]],
        "insert after pragma vtab query must be committed"
    );
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
    let rows = limbo_exec_rows(&conn2, "SELECT x FROM t");
    assert_eq!(
        rows,
        vec![vec![RValue::Integer(42)]],
        "writes after pragma_function_list query must be committed"
    );
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
    let rows = limbo_exec_rows(&conn2, "SELECT a FROM t");
    assert_eq!(
        rows,
        vec![vec![RValue::Integer(1)]],
        "insert after abandoned pragma vtab scan must be committed"
    );
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
    let rows = limbo_exec_rows(&conn2, "SELECT a FROM t");
    assert_eq!(
        rows,
        vec![vec![RValue::Integer(2)]],
        "insert after LIMITed pragma vtab query must be committed"
    );
}

fn writable_schema_flag(conn: &std::sync::Arc<turso_core::Connection>) -> i64 {
    let rows = limbo_exec_rows(conn, "PRAGMA writable_schema");
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [RValue::Integer(value)] => *value,
            other => panic!("expected one integer, got {other:?}"),
        },
        other => panic!("expected one row, got {other:?}"),
    }
}

fn user_schema_rows(
    conn: &std::sync::Arc<turso_core::Connection>,
    columns: &str,
) -> Vec<Vec<RValue>> {
    const NOT_INTERNAL: &str = r"name NOT LIKE '\_\_turso%' ESCAPE '\'";
    limbo_exec_rows(
        conn,
        &format!("SELECT {columns} FROM sqlite_schema WHERE {NOT_INTERNAL} ORDER BY name"),
    )
}

fn error_message(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> String {
    conn.execute(sql)
        .expect_err("statement should fail")
        .to_string()
}

#[turso_macros::test(mvcc)]
fn writable_schema_starts_off(db: TempDatabase) {
    let conn = db.connect_limbo();
    assert_eq!(writable_schema_flag(&conn), 0);
}

#[turso_macros::test(mvcc)]
fn writable_schema_accepts_every_boolean_spelling(db: TempDatabase) {
    let conn = db.connect_limbo();
    let cases = [
        ("ON", 1),
        ("OFF", 0),
        ("TRUE", 1),
        ("FALSE", 0),
        ("YES", 1),
        ("NO", 0),
        ("on", 1),
        ("oFf", 0),
        ("1", 1),
        ("0", 0),
        ("2", 1),
        ("00", 0),
        ("0.0", 0),
        ("1.5", 1),
        ("0x1", 1),
        ("0x0", 0),
        ("-1", 0),
        ("+1", 1),
        ("'on'", 1),
        ("'off'", 0),
        ("'1'", 1),
        ("extra", 0),
        ("full", 0),
        ("banana", 0),
    ];
    for (written, expected) in cases {
        conn.execute("PRAGMA writable_schema = ON").unwrap();
        conn.execute(format!("PRAGMA writable_schema = {written}"))
            .unwrap();
        assert_eq!(
            writable_schema_flag(&conn),
            expected,
            "PRAGMA writable_schema = {written}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn writable_schema_accepts_the_call_form(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA writable_schema(1)").unwrap();
    assert_eq!(writable_schema_flag(&conn), 1);
    conn.execute("PRAGMA writable_schema(0)").unwrap();
    assert_eq!(writable_schema_flag(&conn), 0);
}

#[turso_macros::test(mvcc)]
fn writable_schema_accepts_a_schema_name(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA main.writable_schema = ON").unwrap();
    let rows = limbo_exec_rows(&conn, "PRAGMA main.writable_schema");
    assert_eq!(rows, vec![vec![RValue::Integer(1)]]);
}

#[turso_macros::test(mvcc)]
fn writable_schema_appears_in_pragma_list(db: TempDatabase) {
    let conn = db.connect_limbo();
    let rows = limbo_exec_rows(&conn, "PRAGMA pragma_list");
    assert!(
        rows.contains(&vec![RValue::Text("writable_schema".to_string())]),
        "pragma_list must name writable_schema, got {rows:?}"
    );
}

#[turso_macros::test(mvcc)]
fn pragma_writable_schema_table_function_reads_the_flag(db: TempDatabase) {
    let conn = db.connect_limbo();
    let rows = limbo_exec_rows(&conn, "SELECT * FROM pragma_writable_schema");
    assert_eq!(rows, vec![vec![RValue::Integer(0)]]);

    conn.execute("PRAGMA writable_schema = ON").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT * FROM pragma_writable_schema");
    assert_eq!(rows, vec![vec![RValue::Integer(1)]]);
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_blocks_every_write_to_the_schema_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();

    for (sql, table) in [
        (
            "UPDATE sqlite_schema SET name = 'x' WHERE name = 't'",
            "sqlite_schema",
        ),
        (
            "DELETE FROM sqlite_schema WHERE name = 't'",
            "sqlite_schema",
        ),
        (
            "INSERT INTO sqlite_schema VALUES ('table','x','x',0,'CREATE TABLE x(a)')",
            "sqlite_schema",
        ),
        (
            "UPDATE sqlite_master SET name = 'x' WHERE name = 't'",
            "sqlite_master",
        ),
        (
            "DELETE FROM sqlite_master WHERE name = 't'",
            "sqlite_master",
        ),
    ] {
        let message = error_message(&conn, sql);
        assert!(
            message.contains(&format!("table {table} may not be modified")),
            "{sql} gave {message}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn writable_schema_on_allows_writes_to_the_schema_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();

    conn.execute("UPDATE sqlite_schema SET name = 'renamed' WHERE name = 't'")
        .unwrap();
    assert_eq!(
        user_schema_rows(&conn, "type, name, tbl_name"),
        vec![vec![
            RValue::Text("table".to_string()),
            RValue::Text("renamed".to_string()),
            RValue::Text("t".to_string()),
        ]]
    );

    conn.execute(
        "INSERT INTO sqlite_schema VALUES ('table','ghost','ghost',0,'CREATE TABLE ghost(a)')",
    )
    .unwrap();
    assert_eq!(
        user_schema_rows(&conn, "name"),
        vec![
            vec![RValue::Text("ghost".to_string())],
            vec![RValue::Text("renamed".to_string())],
        ]
    );

    conn.execute("DELETE FROM sqlite_schema WHERE name IN ('ghost', 'renamed')")
        .unwrap();
    assert!(user_schema_rows(&conn, "name").is_empty());
}

#[turso_macros::test(mvcc)]
fn writable_schema_on_allows_writes_through_the_sqlite_master_name(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("UPDATE sqlite_master SET name = 'renamed' WHERE name = 't'")
        .unwrap();
    assert_eq!(
        user_schema_rows(&conn, "name"),
        vec![vec![RValue::Text("renamed".to_string())]]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_again_blocks_the_schema_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("DELETE FROM sqlite_schema WHERE name = 'nothing'")
        .unwrap();

    conn.execute("PRAGMA writable_schema = OFF").unwrap();
    let message = error_message(&conn, "DELETE FROM sqlite_schema WHERE name = 't'");
    assert!(
        message.contains("table sqlite_schema may not be modified"),
        "got {message}"
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_on_allows_object_names_that_start_with_sqlite(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA writable_schema = ON").unwrap();

    conn.execute("CREATE TABLE sqlite_new(a)").unwrap();
    conn.execute("CREATE TABLE indexed(a)").unwrap();
    conn.execute("CREATE INDEX sqlite_ix ON indexed(a)")
        .unwrap();
    conn.execute("CREATE VIEW sqlite_v AS SELECT 1").unwrap();
    conn.execute("CREATE TABLE plain(a)").unwrap();
    conn.execute("ALTER TABLE plain RENAME TO sqlite_renamed")
        .unwrap();

    assert_eq!(
        user_schema_rows(&conn, "name"),
        vec![
            vec![RValue::Text("indexed".to_string())],
            vec![RValue::Text("sqlite_ix".to_string())],
            vec![RValue::Text("sqlite_new".to_string())],
            vec![RValue::Text("sqlite_renamed".to_string())],
            vec![RValue::Text("sqlite_v".to_string())],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_still_refuses_an_index_on_a_sqlite_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("CREATE TABLE sqlite_new(a)").unwrap();

    let message = error_message(&conn, "CREATE INDEX ix ON sqlite_new(a)");
    assert!(
        message.contains("reserved for internal use"),
        "got {message}"
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_blocks_object_names_that_start_with_sqlite(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE plain(a)").unwrap();

    for sql in [
        "CREATE TABLE sqlite_new(a)",
        "CREATE INDEX sqlite_ix ON plain(a)",
        "CREATE VIEW sqlite_v AS SELECT 1",
        "ALTER TABLE plain RENAME TO sqlite_renamed",
    ] {
        let message = error_message(&conn, sql);
        assert!(
            message.contains("reserved for internal use"),
            "{sql} gave {message}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn writable_schema_on_still_protects_turso_internal_tables(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA writable_schema = ON").unwrap();

    let message = error_message(&conn, "CREATE TABLE __turso_internal_x(a)");
    assert!(
        message.contains("reserved for internal use"),
        "got {message}"
    );

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = limbo_exec_rows(
        &conn,
        r"SELECT name FROM sqlite_schema WHERE name LIKE '\_\_turso%' ESCAPE '\'",
    );
    let RValue::Text(backing_table) = rows[0][0].clone() else {
        panic!("expected a table name, got {rows:?}");
    };
    let message = error_message(&conn, &format!("DELETE FROM {backing_table}"));
    assert!(message.contains("may not be modified"), "got {message}");
}

#[turso_macros::test(mvcc)]
fn writable_schema_reset_turns_the_flag_off_and_reads_the_schema_again(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a,b)' WHERE name = 't'")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT name FROM pragma_table_info('t')");
    assert_eq!(
        rows,
        vec![vec![RValue::Text("a".to_string())]],
        "the schema in memory must stay as it was until RESET"
    );

    conn.execute("PRAGMA writable_schema = RESET").unwrap();
    assert_eq!(writable_schema_flag(&conn), 0);
    let rows = limbo_exec_rows(&conn, "SELECT name FROM pragma_table_info('t')");
    assert_eq!(
        rows,
        vec![
            vec![RValue::Text("a".to_string())],
            vec![RValue::Text("b".to_string())],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_reset_matches_any_letter_case(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a,b)' WHERE name = 't'")
        .unwrap();
    conn.execute("PRAGMA writable_schema = ReSeT").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT name FROM pragma_table_info('t')");
    assert_eq!(
        rows.len(),
        2,
        "RESET in mixed case must read the schema again"
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_does_not_read_the_schema_again(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a,b)' WHERE name = 't'")
        .unwrap();
    conn.execute("PRAGMA writable_schema = OFF").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT name FROM pragma_table_info('t')");
    assert_eq!(rows, vec![vec![RValue::Text("a".to_string())]]);
}

#[turso_macros::test(mvcc)]
fn writable_schema_reset_fails_inside_a_transaction(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let message = error_message(&conn, "PRAGMA writable_schema = RESET");
    assert!(
        message.contains("Cannot execute PRAGMA writable_schema=RESET inside a transaction"),
        "got {message}"
    );

    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA writable_schema = RESET").unwrap();
}

#[turso_macros::test(mvcc)]
fn writable_schema_is_set_for_one_connection_only(db: TempDatabase) {
    let first = db.connect_limbo();
    let second = db.connect_limbo();
    first.execute("PRAGMA writable_schema = ON").unwrap();

    assert_eq!(writable_schema_flag(&first), 1);
    assert_eq!(writable_schema_flag(&second), 0);

    second.execute("CREATE TABLE t(a)").unwrap();
    let message = error_message(&second, "DELETE FROM sqlite_schema WHERE name = 't'");
    assert!(
        message.contains("table sqlite_schema may not be modified"),
        "got {message}"
    );
}

#[turso_macros::test]
fn writable_schema_is_not_kept_in_the_database_file(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("CREATE TABLE t(a)").unwrap();
    assert_eq!(writable_schema_flag(&conn), 1);
    conn.close().unwrap();

    let reopened = db.connect_limbo();
    assert_eq!(writable_schema_flag(&reopened), 0);
}

#[turso_macros::test]
fn writable_schema_write_is_saved_to_the_database_file(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a,b)' WHERE name = 't'")
        .unwrap();
    conn.close().unwrap();

    let reopened = db.connect_limbo();
    assert_eq!(
        user_schema_rows(&reopened, "sql"),
        vec![vec![RValue::Text("CREATE TABLE t(a,b)".to_string())]]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_stops_a_statement_prepared_while_it_was_on(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();

    let mut stmt = conn
        .prepare("DELETE FROM sqlite_schema WHERE name = 't'")
        .expect("the statement must compile while the flag is on");

    conn.execute("PRAGMA writable_schema = OFF").unwrap();

    let message = stmt
        .step()
        .expect_err("the statement must compile again and fail")
        .to_string();
    assert!(
        message.contains("table sqlite_schema may not be modified"),
        "got {message}"
    );
    assert_eq!(
        user_schema_rows(&conn, "name"),
        vec![vec![RValue::Text("t".to_string())]]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_off_stops_the_statement_from_compiling(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();

    let message = conn
        .prepare("DELETE FROM sqlite_schema WHERE name = 't'")
        .expect_err("a schema-table write must not compile while the flag is off")
        .to_string();
    assert!(
        message.contains("table sqlite_schema may not be modified"),
        "got {message}"
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_does_not_change_reads_of_the_schema_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    assert_eq!(
        user_schema_rows(&conn, "type, name"),
        vec![vec![
            RValue::Text("table".to_string()),
            RValue::Text("t".to_string()),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn writable_schema_can_change_inside_a_transaction(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("BEGIN").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    assert_eq!(writable_schema_flag(&conn), 1);
    conn.execute("COMMIT").unwrap();
    assert_eq!(writable_schema_flag(&conn), 1);
}

#[turso_macros::test(mvcc)]
fn writable_schema_stays_on_after_a_rollback(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("BEGIN").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(writable_schema_flag(&conn), 1);
}

#[turso_macros::test(mvcc)]
fn writable_schema_write_rolls_back_with_its_transaction(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM sqlite_schema WHERE name = 't'")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        user_schema_rows(&conn, "name"),
        vec![vec![RValue::Text("t".to_string())]]
    );
}

#[turso_macros::test(mvcc)]
fn query_only_still_stops_a_write_to_the_schema_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("PRAGMA query_only = 1").unwrap();

    let message = error_message(&conn, "DELETE FROM sqlite_schema WHERE name = 't'");
    assert!(
        message.contains("query_only"),
        "query_only must stop the write, got {message}"
    );
}

#[test]
fn writable_schema_applies_to_an_attached_database() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_attach(true))
        .build();
    let conn = db.connect_limbo();
    let aux_path = db.path.with_extension("writable_schema_attached.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))
        .unwrap();
    conn.execute("CREATE TABLE aux.t(a)").unwrap();

    let message = error_message(&conn, "DELETE FROM aux.sqlite_schema WHERE name = 't'");
    assert!(message.contains("may not be modified"), "got {message}");

    conn.execute("PRAGMA writable_schema = ON").unwrap();
    conn.execute("DELETE FROM aux.sqlite_schema WHERE name = 't'")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM aux.sqlite_schema");
    assert_eq!(rows, vec![vec![RValue::Integer(0)]]);
}
