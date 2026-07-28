use crate::common::{limbo_exec_rows, try_limbo_exec_rows, TempDatabase};
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

/// uuid7_str() rejects timestamps it cannot represent instead of overflowing.
///
/// `uuid::Timestamp::from_unix` takes u64 seconds and multiplies by 1000
/// unchecked, so an unvalidated `int as u64` on a negative or very large
/// argument overflowed inside the uuid crate.
#[turso_macros::test]
fn uuid7_str_rejects_out_of_range_timestamps(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    for arg in [
        "-1",
        "0",
        "9223372036854775807",
        "-9223372036854775808",
        "281474976710656",
        "'-1'",
    ] {
        let query = format!("SELECT uuid7_str({arg})");
        let result = try_limbo_exec_rows(&tmp_db, &conn, &query);
        assert!(
            result.is_err(),
            "uuid7_str({arg}) should be rejected, got {result:?}"
        );
    }
}

/// uuid7() reports an out-of-range timestamp as NULL, the way it reports every
/// other unusable argument.
#[turso_macros::test]
fn uuid7_rejects_out_of_range_timestamps(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    for arg in [
        "-1",
        "0",
        "9223372036854775807",
        "-9223372036854775808",
        "281474976710656",
    ] {
        let result = limbo_exec_rows(&conn, &format!("SELECT uuid7({arg})"));
        assert_eq!(
            result,
            vec![vec![RusqliteValue::Null]],
            "uuid7({arg}) should be NULL"
        );
    }
}

/// Timestamps inside the representable range still round-trip, including the
/// largest value the 48-bit millisecond field can hold.
#[turso_macros::test]
fn uuid7_accepts_in_range_timestamps(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    for unix in [1i64, 1736720789, 281474976710] {
        let result = limbo_exec_rows(&conn, &format!("SELECT uuid7_timestamp_ms(uuid7({unix}))"));
        assert_eq!(
            result,
            vec![vec![RusqliteValue::Integer(unix * 1000)]],
            "uuid7({unix}) should round-trip"
        );
        let result = limbo_exec_rows(
            &conn,
            &format!("SELECT uuid7_timestamp_ms(uuid7_str({unix}))"),
        );
        assert_eq!(
            result,
            vec![vec![RusqliteValue::Integer(unix * 1000)]],
            "uuid7_str({unix}) should round-trip"
        );
    }
}
