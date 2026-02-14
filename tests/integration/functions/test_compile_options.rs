use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test]
fn test_sqlite_compileoption_used(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // Test known option
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT sqlite_compileoption_used('THREADSAFE=1')");
    assert_eq!(rows, vec![(1,)]);

    // Test case insensitivity
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT sqlite_compileoption_used('threadsafe=1')");
    assert_eq!(rows, vec![(1,)]);

    // Test unknown option
    let rows: Vec<(i64,)> =
        conn.exec_rows("SELECT sqlite_compileoption_used('NON_EXISTENT_OPTION')");
    assert_eq!(rows, vec![(0,)]);
}

#[turso_macros::test]
fn test_sqlite_compileoption_get(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // Test getting the first option
    let rows: Vec<(String,)> = conn.exec_rows("SELECT sqlite_compileoption_get(0)");
    assert!(!rows.is_empty());
    let first_option = &rows[0].0;
    assert!(!first_option.is_empty());

    // Verify the retrieved option is considered "used"
    let sql = format!("SELECT sqlite_compileoption_used('{first_option}')");
    let rows: Vec<(i64,)> = conn.exec_rows(&sql);
    assert_eq!(rows, vec![(1,)]);

    // Test out of bounds (should return NULL)
    let rows: Vec<(Option<String>,)> = conn.exec_rows("SELECT sqlite_compileoption_get(1000000)");
    assert_eq!(rows, vec![(None,)]);
}
