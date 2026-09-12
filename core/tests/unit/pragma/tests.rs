use super::*;
use crate::{Database, MemoryIO, SqliteDialect};

#[test]
fn filter_starts_each_pragma_scan_at_rowid_one() {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, crate::util::MEMORY_PATH, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE scan_target(first, second)")
        .unwrap();

    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };
    let mut cursor = pragma_vtab.open(conn).unwrap();

    assert!(cursor
        .filter(crate::alloc::vec![Value::from_text("scan_target")])
        .unwrap());
    assert_eq!(cursor.rowid(), 1);
    assert!(cursor.next().unwrap());
    assert_eq!(cursor.rowid(), 2);
    assert!(!cursor.next().unwrap());

    assert!(cursor
        .filter(crate::alloc::vec![Value::from_text("scan_target")])
        .unwrap());
    assert_eq!(cursor.rowid(), 1);
}

#[test]
fn test_best_index_argv_order_both_hidden_constraints() {
    // Test when both hidden constraints are present (arg and schema)
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };

    let constraints = vec![
        usable_constraint(6), // arg (first hidden column)
        usable_constraint(7), // schema (second hidden column)
    ];

    let index_info = pragma_vtab.best_index(&constraints).unwrap();

    // Verify arg gets argv_index 1, schema gets argv_index 2
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // arg
    assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // schema
    assert!(index_info.constraint_usages[0].omit);
    assert!(index_info.constraint_usages[1].omit);
}

#[test]
fn test_best_index_argv_order_only_arg() {
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };

    let constraints = vec![
        usable_constraint(6), // arg (first hidden column)
    ];

    let index_info = pragma_vtab.best_index(&constraints).unwrap();

    // Verify arg gets argv_index 1
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // arg
    assert!(index_info.constraint_usages[0].omit);
}

#[test]
fn test_best_index_argv_order_only_schema() {
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 1,
        max_arg_count: 1,
        has_pragma_arg: false,
    };

    let constraints = vec![
        usable_constraint(1), // schema (first hidden column after visible columns)
    ];

    let index_info = pragma_vtab.best_index(&constraints).unwrap();

    // Verify schema gets argv_index 1
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // schema
    assert!(index_info.constraint_usages[0].omit);
}

#[test]
fn test_best_index_argv_order_reverse_constraint_order() {
    // Test when constraints are provided in reverse order (schema first, then arg)
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };

    let constraints = vec![
        usable_constraint(7), // schema (second hidden column)
        usable_constraint(6), // arg (first hidden column)
    ];

    let index_info = pragma_vtab.best_index(&constraints).unwrap();

    // Verify arg still gets argv_index 1, schema gets argv_index 2 regardless of constraint order
    assert_eq!(index_info.constraint_usages[0].argv_index, Some(2)); // schema
    assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // arg
    assert!(index_info.constraint_usages[0].omit);
    assert!(index_info.constraint_usages[1].omit);
}

#[test]
fn test_best_index_visible_columns_ignored() {
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };

    let constraints = vec![
        usable_constraint(0), // visible column (cid)
        usable_constraint(6), // arg (hidden)
    ];

    let index_info = pragma_vtab.best_index(&constraints).unwrap();

    // Verify visible column constraint is ignored, arg gets argv_index 1
    assert_eq!(index_info.constraint_usages[0].argv_index, None); // visible column
    assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // arg
    assert!(!index_info.constraint_usages[0].omit); // visible column not omitted
    assert!(index_info.constraint_usages[1].omit); // arg omitted
}

#[test]
fn test_best_index_no_usable_constraints() {
    let pragma_vtab = PragmaVirtualTable {
        pragma_name: "table_info".to_string(),
        visible_column_count: 6,
        max_arg_count: 2,
        has_pragma_arg: true,
    };

    let constraints = vec![ConstraintInfo {
        column_index: 6,
        op: ConstraintOp::Eq,
        usable: false,
        index: 0,
    }];

    let result = pragma_vtab.best_index(&constraints);

    assert!(matches!(result, Err(ResultCode::ConstraintViolation)));
}

fn usable_constraint(column_index: u32) -> ConstraintInfo {
    ConstraintInfo {
        column_index,
        op: ConstraintOp::Eq,
        usable: true,
        index: 0,
    }
}
