use crate::common::TempDatabase;
use turso_core::{StepResult, Value};

#[test]
fn test_pragma_module_list_returns_list() {
    let db = TempDatabase::new_empty(false);
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
    let db = TempDatabase::new_empty(false);
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
