use crate::common::TempDatabase;
use turso_core::{StepResult, Value};

#[test]
fn test_pragma_module_list_returns_list() {
    let db = TempDatabase::new_empty();
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

#[test]
fn test_pragma_module_list_generate_series() {
    let db = TempDatabase::new_empty();
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
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3),]
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

#[test]
fn test_pragma_page_sizes_without_writes_persists() {
    for test_page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let db = TempDatabase::new_empty();
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
        let Value::Integer(page_size) = row.get_value(0) else {
            panic!("expected integer value");
        };
        assert_eq!(*page_size, test_page_size);

        // Reopen database and verify page size
        let db = TempDatabase::new_with_existent(&db.path);
        let conn = db.connect_limbo();
        let mut rows = conn.query("PRAGMA page_size").unwrap().unwrap();
        let StepResult::Row = rows.step().unwrap() else {
            panic!("expected row");
        };
        let row = rows.row().unwrap();
        let Value::Integer(page_size) = row.get_value(0) else {
            panic!("expected integer value");
        };
        assert_eq!(*page_size, test_page_size);
    }
}

#[test]
fn test_pragma_page_sizes_with_writes_persists() {
    for test_page_size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let db = TempDatabase::new_empty();
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
                let Value::Integer(page_size) = page_size else {
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
            let Value::Integer(page_size) = page_size else {
                panic!("expected integer value");
            };
            assert_eq!(page_size, test_page_size);

            // Verify data can still be read
            let mut rows = conn
                .query("SELECT value FROM test WHERE id = 1")
                .unwrap()
                .unwrap();
            loop {
                if let StepResult::Row = rows.step().unwrap() {
                    let row = rows.row().unwrap();
                    let Value::Text(value) = row.get_value(0) else {
                        panic!("expected text value");
                    };
                    assert_eq!(value.as_str(), "test data");
                    break;
                }
                rows.run_once().unwrap();
            }
        }

        // Drop the db and reopen it, and verify the same
        let db = TempDatabase::new_with_existent(&db.path);
        let conn = db.connect_limbo();
        let mut page_size = conn.pragma_query("page_size").unwrap();
        let mut page_size = page_size.pop().unwrap();
        let page_size = page_size.pop().unwrap();
        let Value::Integer(page_size) = page_size else {
            panic!("expected integer value");
        };
        assert_eq!(page_size, test_page_size);
    }
}
