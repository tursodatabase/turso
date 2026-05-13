use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as RusqliteValue;

/// Test that uuid7_timestamp_ms returns NULL for empty blob input
#[turso_macros::test]
fn uuid7_timestamp_ms_empty_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let result = limbo_exec_rows(&conn, "SELECT uuid7_timestamp_ms(X'')");
    assert_eq!(result, vec![vec![RusqliteValue::Null]]);
}

/// Test that uuid7_timestamp_ms returns NULL for non 16-byte blob
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

/// The built-in `uuid` type stores UUIDs as blobs, so it must accept the
/// blob-producing UUID helpers directly in STRICT tables.
#[test]
fn uuid_type_accepts_blob_helpers_in_strict_table() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE events (id uuid PRIMARY KEY, name TEXT) STRICT")
        .unwrap();
    conn.execute("INSERT INTO events VALUES (uuid4(), 'launch')")
        .unwrap();
    conn.execute("INSERT INTO events VALUES ('01945ca0-3189-76c0-9a8f-caf310fc8b8e', 'known')")
        .unwrap();

    let result = limbo_exec_rows(
        &conn,
        "SELECT typeof(id), length(id), id, name FROM events ORDER BY name",
    );
    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0], RusqliteValue::Text("text".to_string()));
    assert_eq!(result[0][1], RusqliteValue::Integer(36));
    assert_eq!(
        result[0][2],
        RusqliteValue::Text("01945ca0-3189-76c0-9a8f-caf310fc8b8e".to_string())
    );
    assert_eq!(result[0][3], RusqliteValue::Text("known".to_string()));
    assert_eq!(result[1][0], RusqliteValue::Text("text".to_string()));
    assert_eq!(result[1][1], RusqliteValue::Integer(36));
    assert_eq!(result[1][3], RusqliteValue::Text("launch".to_string()));
    let uuid = match &result[1][2] {
        RusqliteValue::Text(uuid) => uuid,
        other => panic!("expected uuid text, got {other:?}"),
    };
    assert_eq!(uuid.len(), 36);
}
