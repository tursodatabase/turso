//! Integration tests for SQLite dbhash compatibility.
//!
//! These tests verify that turso-dbhash produces the same hashes as SQLite's dbhash tool.
//!
//! All tests are marked with `#[ignore]` by default because they require SQLite's dbhash
//! binary to be installed. See tests/README.md for setup instructions.
//!
//! Run with: cargo test -p turso-dbhash --ignored

use std::process::Command;
use tempfile::NamedTempFile;
use turso_dbhash::{hash_database, DbHashOptions};

/// Helper to create a test database with SQLite and return the file path.
fn create_test_db(sql: &str) -> NamedTempFile {
    let file = NamedTempFile::new().expect("Failed to create temp file");
    let path = file.path().to_str().unwrap();

    let status = Command::new("sqlite3")
        .arg(path)
        .arg(sql)
        .status()
        .expect("Failed to execute sqlite3");

    assert!(status.success(), "sqlite3 command failed");
    file
}

/// Helper to get hash from SQLite's dbhash tool.
fn sqlite_dbhash(path: &str, args: &[&str]) -> String {
    let mut cmd = Command::new("dbhash");
    cmd.args(args).arg(path);

    let output = cmd.output().expect("dbhash command failed - is it installed?");
    assert!(output.status.success(), "dbhash returned non-zero status");

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .split_whitespace()
        .next()
        .expect("dbhash produced no output")
        .to_string()
}

#[test]
#[ignore]
fn test_compat_simple_table() {
    let db = create_test_db("CREATE TABLE t(x INT, y TEXT); INSERT INTO t VALUES(1, 'hello');");
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for simple table"
    );
}

#[test]
#[ignore]
fn test_compat_empty_database() {
    let db = create_test_db("SELECT 1;"); // Just initializes the database
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for empty database"
    );
    // Empty database has the SHA1 of empty input
    assert_eq!(
        turso_result.hash,
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
}

#[test]
#[ignore]
fn test_compat_integer_bounds() {
    let db = create_test_db(
        "CREATE TABLE t(v INTEGER);
         INSERT INTO t VALUES(0);
         INSERT INTO t VALUES(1);
         INSERT INTO t VALUES(-1);
         INSERT INTO t VALUES(9223372036854775807);  -- i64::MAX
         INSERT INTO t VALUES(-9223372036854775808); -- i64::MIN
         INSERT INTO t VALUES(2147483647);  -- i32::MAX
         INSERT INTO t VALUES(-2147483648); -- i32::MIN",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for integer bounds"
    );
}

#[test]
#[ignore]
fn test_compat_negative_integers() {
    let db = create_test_db(
        "CREATE TABLE t(v INTEGER);
         INSERT INTO t VALUES(-1);
         INSERT INTO t VALUES(-2);
         INSERT INTO t VALUES(-100);
         INSERT INTO t VALUES(-1000000);
         INSERT INTO t VALUES(-9223372036854775808);",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for negative integers"
    );
}

#[test]
#[ignore]
fn test_compat_float_special_values() {
    let db = create_test_db(
        "CREATE TABLE t(v REAL);
         INSERT INTO t VALUES(0.0);
         INSERT INTO t VALUES(-0.0);
         INSERT INTO t VALUES(1.0);
         INSERT INTO t VALUES(-1.0);
         INSERT INTO t VALUES(3.14159265358979);
         INSERT INTO t VALUES(1e308);  -- Large
         INSERT INTO t VALUES(1e-308); -- Small",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for float special values"
    );
}

#[test]
#[ignore]
fn test_compat_float_precision() {
    // Test floats that might have precision issues
    let db = create_test_db(
        "CREATE TABLE t(v REAL);
         INSERT INTO t VALUES(0.1);
         INSERT INTO t VALUES(0.2);
         INSERT INTO t VALUES(0.3);
         INSERT INTO t VALUES(0.1 + 0.2);", // Famous floating point test
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for float precision"
    );
}

#[test]
#[ignore]
fn test_compat_text_unicode() {
    let db = create_test_db(
        "CREATE TABLE t(v TEXT);
         INSERT INTO t VALUES('hello');
         INSERT INTO t VALUES('');
         INSERT INTO t VALUES('Hello, 世界!');
         INSERT INTO t VALUES('Emoji: 🎉🎊🎁');
         INSERT INTO t VALUES('Ñoño');
         INSERT INTO t VALUES('Ελληνικά');
         INSERT INTO t VALUES('日本語');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for unicode text"
    );
}

#[test]
#[ignore]
fn test_compat_text_special_chars() {
    let db = create_test_db(
        r#"CREATE TABLE t(v TEXT);
           INSERT INTO t VALUES('line1
line2');
           INSERT INTO t VALUES('tab	here');
           INSERT INTO t VALUES('quote''s');
           INSERT INTO t VALUES('backslash\here');"#,
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for special chars in text"
    );
}

#[test]
#[ignore]
fn test_compat_blob_data() {
    let db = create_test_db(
        "CREATE TABLE t(v BLOB);
         INSERT INTO t VALUES(x'');
         INSERT INTO t VALUES(x'00');
         INSERT INTO t VALUES(x'DEADBEEF');
         INSERT INTO t VALUES(x'000102030405060708090A0B0C0D0E0F');
         INSERT INTO t VALUES(x'FFFFFFFFFFFFFFFF');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for blob data"
    );
}

#[test]
#[ignore]
fn test_compat_null_values() {
    let db = create_test_db(
        "CREATE TABLE t(a INT, b TEXT, c REAL, d BLOB);
         INSERT INTO t VALUES(NULL, NULL, NULL, NULL);
         INSERT INTO t VALUES(1, NULL, NULL, NULL);
         INSERT INTO t VALUES(NULL, 'text', NULL, NULL);
         INSERT INTO t VALUES(NULL, NULL, 1.5, NULL);
         INSERT INTO t VALUES(NULL, NULL, NULL, x'00');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for null values"
    );
}

#[test]
#[ignore]
fn test_compat_mixed_types() {
    let db = create_test_db(
        "CREATE TABLE t(v);
         INSERT INTO t VALUES(NULL);
         INSERT INTO t VALUES(0);
         INSERT INTO t VALUES(0.0);
         INSERT INTO t VALUES('0');
         INSERT INTO t VALUES(x'00');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for mixed types (type prefixes)"
    );
}

#[test]
#[ignore]
fn test_compat_table_ordering() {
    // Tables should be hashed in case-insensitive name order
    let db = create_test_db(
        "CREATE TABLE Zebra(x INT);
         CREATE TABLE apple(x INT);
         CREATE TABLE BANANA(x INT);
         INSERT INTO Zebra VALUES(1);
         INSERT INTO apple VALUES(2);
         INSERT INTO BANANA VALUES(3);",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for table ordering"
    );
}

#[test]
#[ignore]
fn test_compat_table_name_with_quotes() {
    // Table name containing double quotes
    let db = create_test_db(
        r#"CREATE TABLE "my""table"(x INT);
           INSERT INTO "my""table" VALUES(42);"#,
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for table name with quotes"
    );
}

#[test]
#[ignore]
fn test_compat_table_name_with_spaces() {
    let db = create_test_db(
        r#"CREATE TABLE "my table"(x INT);
           INSERT INTO "my table" VALUES(42);"#,
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for table name with spaces"
    );
}

#[test]
#[ignore]
fn test_compat_schema_only() {
    let db = create_test_db(
        "CREATE TABLE t(x INT);
         INSERT INTO t VALUES(1);
         INSERT INTO t VALUES(2);",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &["--schema-only"]);
    let turso_result = hash_database(
        path,
        &DbHashOptions {
            schema_only: true,
            ..Default::default()
        },
    )
    .expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for --schema-only"
    );
    assert_eq!(turso_result.tables_hashed, 0);
}

#[test]
#[ignore]
fn test_compat_without_schema() {
    let db = create_test_db(
        "CREATE TABLE t(x INT);
         INSERT INTO t VALUES(1);
         INSERT INTO t VALUES(2);",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &["--without-schema"]);
    let turso_result = hash_database(
        path,
        &DbHashOptions {
            without_schema: true,
            ..Default::default()
        },
    )
    .expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for --without-schema"
    );
}

#[test]
#[ignore]
fn test_compat_like_filter() {
    let db = create_test_db(
        "CREATE TABLE users(id INT);
         CREATE TABLE orders(id INT);
         CREATE TABLE order_items(id INT);
         INSERT INTO users VALUES(1);
         INSERT INTO orders VALUES(2);
         INSERT INTO order_items VALUES(3);",
    );
    let path = db.path().to_str().unwrap();

    // Test filtering to only 'order%' tables
    let sqlite_hash = sqlite_dbhash(path, &["--like", "order%"]);
    let turso_result = hash_database(
        path,
        &DbHashOptions {
            table_filter: Some("order%".to_string()),
            ..Default::default()
        },
    )
    .expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for --like filter"
    );
    assert_eq!(turso_result.tables_hashed, 2); // orders and order_items
}

#[test]
#[ignore]
fn test_compat_excludes_sqlite_sequence() {
    // AUTOINCREMENT creates sqlite_sequence table
    let db = create_test_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
         INSERT INTO t(name) VALUES('first');
         INSERT INTO t(name) VALUES('second');
         INSERT INTO t(name) VALUES('third');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch (sqlite_sequence should be excluded)"
    );
    assert_eq!(turso_result.tables_hashed, 1); // Only 't', not sqlite_sequence
}

#[test]
#[ignore]
fn test_compat_empty_table() {
    let db = create_test_db("CREATE TABLE t(x INT, y TEXT, z BLOB);");
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for empty table"
    );
    assert_eq!(turso_result.tables_hashed, 1);
    assert_eq!(turso_result.rows_hashed, 0);
}

#[test]
#[ignore]
fn test_compat_row_ordering_integer_pk() {
    // Insert in non-PK order, should still hash in PK order
    let db = create_test_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
         INSERT INTO t VALUES(3, 'three');
         INSERT INTO t VALUES(1, 'one');
         INSERT INTO t VALUES(2, 'two');",
    );
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for row ordering"
    );
}

#[test]
#[ignore]
fn test_compat_large_text() {
    // Create a large text value (100KB)
    let large_text = "x".repeat(100_000);
    let sql = format!("CREATE TABLE t(v TEXT); INSERT INTO t VALUES('{large_text}');");
    let db = create_test_db(&sql);
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for large text"
    );
}

#[test]
#[ignore]
fn test_compat_many_rows() {
    // Create table with many rows
    let mut sql = String::from("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);");
    for i in 0..1000 {
        sql.push_str(&format!("INSERT INTO t VALUES({i}, 'row{i}');"));
    }
    let db = create_test_db(&sql);
    let path = db.path().to_str().unwrap();

    let sqlite_hash = sqlite_dbhash(path, &[]);
    let turso_result = hash_database(path, &DbHashOptions::default()).expect("turso-dbhash failed");

    assert_eq!(
        turso_result.hash, sqlite_hash,
        "Hash mismatch for many rows"
    );
    assert_eq!(turso_result.rows_hashed, 1000);
}
