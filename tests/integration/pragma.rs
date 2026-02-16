use crate::common::TempDatabase;
#[cfg(not(target_vendor = "apple"))]
use turso_core::LimboError;
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

    // On non-Apple platforms, fullfsync pragma should not exist
    let result = conn.execute("PRAGMA fullfsync=1");
    assert!(
        matches!(
            result,
            Err(LimboError::ParseError(e)) if e.contains("Not a valid pragma name")
        ),
        "fullfsync pragma should not be available on non-Apple platforms"
    );
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
fn test_pragma_cache_size_i64_min(db: TempDatabase) {
    let conn = db.connect_limbo();
    // This value is i64::MIN
    let result = conn.execute("PRAGMA cache_size=-9223372036854775808");
    assert!(result.is_ok());
}
