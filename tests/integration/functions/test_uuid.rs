use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as RusqliteValue;

/// Test that uuid7_timestamp_ms returns NULL for empty blob input
#[turso_macros::test]
fn uuid7_timestamp_ms_empty_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms(X'')");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms returns NULL for short blob (1 byte)
#[turso_macros::test]
fn uuid7_timestamp_ms_short_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms(X'00')");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms returns NULL for 5-byte blob
#[turso_macros::test]
fn uuid7_timestamp_ms_5_byte_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms(randomblob(5))");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms returns NULL for 10-byte blob
#[turso_macros::test]
fn uuid7_timestamp_ms_10_byte_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms(zeroblob(10))");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms returns NULL for invalid UUID string
#[turso_macros::test]
fn uuid7_timestamp_ms_invalid_string(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms('not-a-uuid')");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms works with valid 16-byte blob from uuid7()
#[turso_macros::test]
fn uuid7_timestamp_ms_valid_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT typeof(uuid7_timestamp_ms(uuid7()))");
    assert_eq!(
        result,
        vec![vec![RusqliteValue::Text("integer".to_string())]]
    );
}

/// Test that uuid7_timestamp_ms correctly parses a known UUID7 string
#[turso_macros::test]
fn uuid7_timestamp_ms_valid_string(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    // This UUID7 has a known timestamp. Dividing by 1000 gives Unix seconds.
    let result = limbo_exec_rows(
        &conn,
        "SELECT uuid7_timestamp_ms('01945ca0-3189-76c0-9a8f-caf310fc8b8e') / 1000",
    );
    assert_eq!(result, vec![vec![RusqliteValue::Integer(1736720789)]]);
}
