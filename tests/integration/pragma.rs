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

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_clean_database_returns_no_rows(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1), (11, NULL)")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check");
    assert_eq!(
        rows,
        Vec::<Vec<RValue>>::new(),
        "no violations and a NULL fk column must not be reported"
    );
}

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_detects_rowid_violation(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("PRAGMA foreign_keys=OFF").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1), (11, 99)")
        .unwrap();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();

    let rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check");
    assert_eq!(
        rows,
        vec![vec![
            RValue::Text("child".to_string()),
            RValue::Integer(11),
            RValue::Text("parent".to_string()),
            RValue::Integer(0),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_single_table_form(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child_a(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE child_b(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    conn.execute("PRAGMA foreign_keys=OFF").unwrap();
    conn.execute("INSERT INTO child_a VALUES (1, 99)").unwrap();
    conn.execute("INSERT INTO child_b VALUES (1, 99)").unwrap();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();

    let rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check(child_a)");
    assert_eq!(
        rows,
        vec![vec![
            RValue::Text("child_a".to_string()),
            RValue::Integer(1),
            RValue::Text("parent".to_string()),
            RValue::Integer(0),
        ]],
        "single-table form must not report violations from other tables"
    );
}

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_unique_index_parent(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent(code TEXT UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parent(code))",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES ('a')").unwrap();
    conn.execute("PRAGMA foreign_keys=OFF").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 'a'), (2, 'missing')")
        .unwrap();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();

    let rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check");
    assert_eq!(
        rows,
        vec![vec![
            RValue::Text("child".to_string()),
            RValue::Integer(2),
            RValue::Text("parent".to_string()),
            RValue::Integer(0),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_composite_key(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent(a INTEGER, b INTEGER, UNIQUE(a, b))")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, \
         FOREIGN KEY(pa, pb) REFERENCES parent(a, b))",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 2)").unwrap();
    conn.execute("PRAGMA foreign_keys=OFF").unwrap();
    // (1, NULL): one column NULL means the whole key is not checked.
    // (1, 3): fully non-NULL but no matching parent row -> violation.
    conn.execute("INSERT INTO child VALUES (1, 1, 2), (2, 1, NULL), (3, 1, 3)")
        .unwrap();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();

    let rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check");
    assert_eq!(
        rows,
        vec![vec![
            RValue::Text("child".to_string()),
            RValue::Integer(3),
            RValue::Text("parent".to_string()),
            RValue::Integer(0),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn test_pragma_foreign_key_check_fkid_matches_foreign_key_list(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE parent_a(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE parent_b(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES parent_a(id), \
         b_id INTEGER REFERENCES parent_b(id))",
    )
    .unwrap();
    conn.execute("PRAGMA foreign_keys=OFF").unwrap();
    conn.execute("INSERT INTO child VALUES (1, 99, 1)").unwrap();
    conn.execute("INSERT INTO parent_b VALUES (1)").unwrap();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();

    let list_rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_list(child)");
    let expected_fkid = list_rows
        .iter()
        .find(|row| row[2] == RValue::Text("parent_a".to_string()))
        .map(|row| row[0].clone())
        .expect("foreign_key_list must report the parent_a constraint");

    let check_rows = limbo_exec_rows(&conn, "PRAGMA foreign_key_check(child)");
    assert_eq!(check_rows.len(), 1);
    assert_eq!(
        check_rows[0][3], expected_fkid,
        "foreign_key_check's fkid must match foreign_key_list's id for the same constraint"
    );
}
