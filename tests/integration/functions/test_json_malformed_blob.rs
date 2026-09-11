//! Regression tests for malformed JSONB blobs reaching the json functions.
//!
//! Every field of a BLOB argument's JSONB encoding is caller-controlled, so
//! these must produce an error or NULL rather than an out-of-bounds read,
//! invalid UTF-8 decoded as `&str`, or a stack overflow.
//!
//! Not in the `.sqltest` corpus because SQLite is more permissive here -- it
//! renders an invalid UTF-8 key with replacement characters instead of
//! rejecting the document.

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, TempDatabase};
use rusqlite::types::Value as RusqliteValue;
use std::sync::Arc;

/// OBJECT holding a 7-byte TEXT5 key, then NULL. The key ends in a truncated
/// multi-byte lead byte. TEXT5 because only non-plain-TEXT keys go through
/// `unescape_string`, and a 9-byte payload because anything over 8 bytes used
/// to skip validation.
const TRUNCATED_UTF8_KEY: &str = "x'9C79616161616161F000'";

/// Same shape, but the key bytes decode to an invalid `char` value.
const INVALID_UTF8_KEY: &str = "x'9C79FFFFFFFFFFFFFF00'";

/// Asserts the document is rejected rather than acted upon: the statement
/// either fails, or evaluates to NULL for the opcodes that discard the error.
fn assert_rejected(db: &TempDatabase, conn: &Arc<turso_core::Connection>, sql: &str) {
    match limbo_exec_rows_fallible(db, conn, sql) {
        Err(_) => {}
        Ok(rows) => assert_eq!(
            rows,
            vec![vec![RusqliteValue::Null]],
            "expected `{sql}` to be rejected"
        ),
    }
}

#[turso_macros::test]
fn json_rejects_invalid_utf8_object_key(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    for blob in [TRUNCATED_UTF8_KEY, INVALID_UTF8_KEY] {
        assert_rejected(&tmp_db, &conn, &format!("SELECT json({blob})"));
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_extract({blob}, '$.a')"),
        );
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_set({blob}, '$.a', 1)"),
        );
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_insert({blob}, '$.a', 1)"),
        );
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_replace({blob}, '$.a', 1)"),
        );
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_remove({blob}, '$.a')"),
        );
        assert_rejected(&tmp_db, &conn, &format!("SELECT json_type({blob}, '$.a')"));
        assert_rejected(
            &tmp_db,
            &conn,
            &format!("SELECT json_array_length({blob}, '$.a')"),
        );
    }
}

/// An 8-byte payload size (header marker 15) makes `offset + header + payload`
/// overflow `usize` unless the arithmetic is checked.
#[turso_macros::test]
fn json_rejects_element_size_overflowing_usize(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    // ARRAY with a 9-byte payload: a single ARRAY element whose header uses
    // the 15 marker to declare a payload of 0xFFFF_FFFF_FFFF_FFFF bytes.
    let blob = "x'9BFBFFFFFFFFFFFFFFFF'";
    assert_rejected(&tmp_db, &conn, &format!("SELECT json({blob})"));
    assert_rejected(
        &tmp_db,
        &conn,
        &format!("SELECT json_extract({blob}, '$[0]')"),
    );
    assert_rejected(
        &tmp_db,
        &conn,
        &format!("SELECT json_remove({blob}, '$[0]')"),
    );
}

/// Builds a JSONB blob of `depth` nested arrays as a SQL hex literal.
fn nested_arrays(depth: usize) -> String {
    fn header(element_type: u8, size: usize) -> Vec<u8> {
        match size {
            0..=11 => vec![((size as u8) << 4) | element_type],
            12..=0xFF => vec![(12 << 4) | element_type, size as u8],
            0x100..=0xFFFF => {
                let mut bytes = vec![(13 << 4) | element_type];
                bytes.extend_from_slice(&(size as u16).to_be_bytes());
                bytes
            }
            _ => {
                let mut bytes = vec![(14 << 4) | element_type];
                bytes.extend_from_slice(&(size as u32).to_be_bytes());
                bytes
            }
        }
    }

    const ARRAY: u8 = 11;
    let mut blob = header(ARRAY, 0);
    for _ in 0..depth {
        let mut wrapped = header(ARRAY, blob.len());
        wrapped.extend_from_slice(&blob);
        blob = wrapped;
    }

    let mut literal = String::with_capacity(blob.len() * 2 + 3);
    literal.push_str("x'");
    for byte in blob {
        literal.push_str(&format!("{byte:02X}"));
    }
    literal.push('\'');
    literal
}

/// Validating blob-sourced JSONB must not reject documents Turso itself emits.
#[turso_macros::test]
fn json_accepts_well_formed_blob(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    let result = limbo_exec_rows(
        &conn,
        r#"SELECT json_extract(jsonb('{"a":{"b":[1,2,3]},"c":"é"}'), '$.a.b[1]')"#,
    );
    assert_eq!(result, vec![vec![RusqliteValue::Integer(2)]]);

    let result = limbo_exec_rows(
        &conn,
        r#"SELECT json(jsonb('{"a":{"b":[1,2,3]},"c":"é"}'))"#,
    );
    assert_eq!(
        result,
        vec![vec![RusqliteValue::Text(
            r#"{"a":{"b":[1,2,3]},"c":"é"}"#.to_string()
        )]]
    );

    let result = limbo_exec_rows(
        &conn,
        r#"SELECT json_remove(jsonb('{"a":1,"b":2}'), '$.a')"#,
    );
    assert_eq!(
        result,
        vec![vec![RusqliteValue::Text(r#"{"b":2}"#.to_string())]]
    );

    // Non-ASCII keys and values survive the UTF-8 checks on text payloads.
    let result = limbo_exec_rows(
        &conn,
        r#"SELECT json_extract(jsonb('{"ключ":"значение"}'), '$."ключ"')"#,
    );
    assert_eq!(
        result,
        vec![vec![RusqliteValue::Text("значение".to_string())]]
    );

    // Nesting within the depth limit still round-trips through a blob.
    let result = limbo_exec_rows(&conn, &format!("SELECT json({})", nested_arrays(100)));
    assert_eq!(
        result,
        vec![vec![RusqliteValue::Text(format!(
            "{}[]{}",
            "[".repeat(100),
            "]".repeat(100)
        ))]]
    );
}
