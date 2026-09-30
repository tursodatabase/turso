use crate::common::TempDatabase;
use std::io::Write;
use tempfile::NamedTempFile;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

/// Helper: create a temp file with the given content and return it (keeps it alive).
fn write_temp_file(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

/// Helper: query all rows from a connection.
fn query_all(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                result.push(rows.row().unwrap().get_values().cloned().collect());
            }
            StepResult::Done => break,
            _ => panic!("unexpected step result"),
        }
    }
    result
}

fn assert_text(val: &Value, expected: &str) {
    let Value::Text(t) = val else {
        panic!("expected Text, got: {val:?}");
    };
    assert_eq!(t.value, expected);
}

fn assert_int(val: &Value, expected: i64) {
    let Value::Numeric(Numeric::Integer(i)) = val else {
        panic!("expected Integer, got: {val:?}");
    };
    assert_eq!(*i, expected);
}

fn assert_null(val: &Value) {
    assert!(matches!(val, Value::Null), "expected Null, got: {val:?}");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_basic(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();

    let tsv = write_temp_file("1\tAlice\n2\tBob\n");
    let sql = format!("COPY users FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(rows.len(), 2);

    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
    assert_int(&rows[1][0], 2);
    assert_text(&rows[1][1], "Bob");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_null_values(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let tsv = write_temp_file("1\t\\N\n\\N\thello\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);

    assert_int(&rows[0][0], 1);
    assert_null(&rows[0][1]);
    assert_null(&rows[1][0]);
    assert_text(&rows[1][1], "hello");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_empty_string(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();

    let tsv = write_temp_file("hello\t\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_text(&rows[0][0], "hello");
    // Empty field is an empty string, not NULL
    assert_text(&rows[0][1], "");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_with_columns(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
        .unwrap();

    // Only supply columns a and c
    let tsv = write_temp_file("1\thello\n2\tworld\n");
    let sql = format!("COPY t (a, c) FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b, c FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);

    assert_int(&rows[0][0], 1);
    assert_null(&rows[0][1]); // b not specified, should be NULL
    assert_text(&rows[0][2], "hello");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_file_not_found(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER)").unwrap();

    let result = conn.execute("COPY t FROM '/nonexistent/path/data.tsv'");
    assert!(result.is_err());
}

#[turso_macros::test(mvcc)]
fn test_copy_from_wrong_column_count(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let tsv = write_temp_file("1\thello\textra\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    let result = conn.execute(&sql);
    assert!(result.is_err());
}

#[turso_macros::test(mvcc)]
fn test_copy_from_with_header(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    let tsv = write_temp_file("id\tname\n1\tAlice\n2\tBob\n");
    let sql = format!("COPY t FROM '{}' WITH (HEADER true)", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT id, name FROM t ORDER BY id");
    // Header row should be skipped
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
}

// ---------------------------------------------------------------------------
// Backslash escapes end-to-end
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_backslash_escapes(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();

    // Field with escaped backslash, and field with escaped newline
    let tsv = write_temp_file("hello\\\\world\tline1\\nline2\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_text(&rows[0][0], "hello\\world");
    assert_text(&rows[0][1], "line1\nline2");
}

#[turso_macros::test(mvcc)]
fn test_copy_from_escaped_tab_in_field(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();

    // First field contains a literal \t escape (not a real tab delimiter)
    let tsv = write_temp_file("col\\t1\tvalue\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_text(&rows[0][0], "col\t1");
    assert_text(&rows[0][1], "value");
}

// ---------------------------------------------------------------------------
// Custom DELIMITER option end-to-end
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_custom_delimiter(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    let csv = write_temp_file("1,Alice\n2,Bob\n");
    let sql = format!(
        "COPY t FROM '{}' WITH (DELIMITER ',')",
        csv.path().display()
    );
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
    assert_int(&rows[1][0], 2);
    assert_text(&rows[1][1], "Bob");
}

// ---------------------------------------------------------------------------
// Custom NULL string end-to-end
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_custom_null_string(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let tsv = write_temp_file("1\tNULL\nNULL\thello\n");
    let sql = format!("COPY t FROM '{}' WITH (NULL 'NULL')", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_null(&rows[0][1]);
    assert_null(&rows[1][0]);
    assert_text(&rows[1][1], "hello");
}

// ---------------------------------------------------------------------------
// Empty file
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_empty_file(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let tsv = write_temp_file("");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// File with only end-of-data marker
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_end_of_data_marker_only(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER)").unwrap();

    let tsv = write_temp_file("\\.\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 0);
}

#[turso_macros::test(mvcc)]
fn test_copy_from_data_then_end_marker(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    // Two data rows, then end-of-data marker, then a row that should be ignored
    let tsv = write_temp_file("1\tfirst\n2\tsecond\n\\.\n3\tignored\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "first");
    assert_int(&rows[1][0], 2);
    assert_text(&rows[1][1], "second");
}

// ---------------------------------------------------------------------------
// Table does not exist
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_nonexistent_table(db: TempDatabase) {
    let conn = db.connect_postgres();

    let tsv = write_temp_file("1\thello\n");
    let sql = format!("COPY nonexistent FROM '{}'", tsv.path().display());
    let result = conn.execute(&sql);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Larger dataset (transaction atomicity)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_many_rows(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();

    let mut data = String::new();
    for i in 0..200 {
        data.push_str(&format!("{i}\trow_{i}\n"));
    }
    let tsv = write_temp_file(&data);
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT COUNT(*) FROM t");
    assert_int(&rows[0][0], 200);

    // Spot-check first and last rows
    let rows = query_all(&conn, "SELECT val FROM t WHERE id = 0");
    assert_text(&rows[0][0], "row_0");
    let rows = query_all(&conn, "SELECT val FROM t WHERE id = 199");
    assert_text(&rows[0][0], "row_199");
}

// ---------------------------------------------------------------------------
// Transaction atomicity: malformed row in middle should roll back
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_malformed_row_rolls_back(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    // Row 2 has wrong column count — should cause error
    let tsv = write_temp_file("1\tok\n2\tbad\textra\n3\tok\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    let result = conn.execute(&sql);
    assert!(result.is_err());

    // Table should have zero rows because the transaction should have been rolled back
    let rows = query_all(&conn, "SELECT COUNT(*) FROM t");
    assert_int(&rows[0][0], 0);
}

// ---------------------------------------------------------------------------
// No trailing newline
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_no_trailing_newline(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    // No trailing newline at end of file
    let tsv = write_temp_file("1\thello\n2\tworld");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "hello");
    assert_int(&rows[1][0], 2);
    assert_text(&rows[1][1], "world");
}

// ---------------------------------------------------------------------------
// HEADER with bare keyword (no true/false)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_header_bare_keyword(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    let tsv = write_temp_file("id\tname\n1\tAlice\n");
    let sql = format!("COPY t FROM '{}' WITH (HEADER)", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT id, name FROM t");
    assert_eq!(rows.len(), 1);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
}

// ---------------------------------------------------------------------------
// Multiple options combined
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_combined_options(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    // CSV with custom delimiter, custom null, and header
    let csv = write_temp_file("id|name\n1|Alice\n2|<nil>\n");
    let sql = format!(
        "COPY t FROM '{}' WITH (DELIMITER '|', NULL '<nil>', HEADER true)",
        csv.path().display()
    );
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
    assert_int(&rows[1][0], 2);
    assert_null(&rows[1][1]);
}

// ---------------------------------------------------------------------------
// COPY TO should produce a clear error (unsupported)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_to_unsupported(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER)").unwrap();

    let result = conn.execute("COPY t TO '/tmp/out.tsv'");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Single column table
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_single_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (val TEXT)").unwrap();

    let tsv = write_temp_file("hello\nworld\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT val FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_text(&rows[0][0], "hello");
    assert_text(&rows[1][0], "world");
}

// ---------------------------------------------------------------------------
// All NULLs row
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_all_nulls(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
        .unwrap();

    let tsv = write_temp_file("\\N\t\\N\t\\N\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b, c FROM t");
    assert_eq!(rows.len(), 1);
    assert_null(&rows[0][0]);
    assert_null(&rows[0][1]);
    assert_null(&rows[0][2]);
}

// ---------------------------------------------------------------------------
// COPY FROM with HEADER and empty data (only header line)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_header_only_no_data(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    let tsv = write_temp_file("id\tname\n");
    let sql = format!("COPY t FROM '{}' WITH (HEADER true)", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// Multiple COPY operations on the same table
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_append(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();

    let tsv1 = write_temp_file("1\tAlice\n2\tBob\n");
    let sql = format!("COPY t FROM '{}'", tsv1.path().display());
    conn.execute(&sql).unwrap();

    let tsv2 = write_temp_file("3\tCharlie\n4\tDiana\n");
    let sql = format!("COPY t FROM '{}'", tsv2.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT COUNT(*) FROM t");
    assert_int(&rows[0][0], 4);

    let rows = query_all(&conn, "SELECT name FROM t WHERE id = 3");
    assert_text(&rows[0][0], "Charlie");
}

// ---------------------------------------------------------------------------
// \\N is literal \N, not NULL (PG copy2.sql pattern: \N vs \\N vs \NN)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_escaped_backslash_n_is_not_null(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT, d TEXT)")
        .unwrap();

    // PG copy2.sql pattern: \N=NULL, \\N=literal \N, \NN=not-null
    // Verified against PG expected output (copy2.out row 9999)
    let tsv = write_temp_file("1\t\\N\t\\\\N\t\\NN\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b, c, d FROM t");
    assert_eq!(rows.len(), 1);
    assert_int(&rows[0][0], 1);
    assert_null(&rows[0][1]); // \N → NULL
    assert_text(&rows[0][2], "\\N"); // \\N → \\ unescapes to \, N is literal → "\N"
                                     // \NN: doesn't match null marker (exact \N required), unescape: \N is unknown escape → "N", then "N" → "NN"
    assert_text(&rows[0][3], "NN");
}

// ---------------------------------------------------------------------------
// Empty string as NULL marker (NULL AS '')
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_empty_null_marker(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();

    // With NULL='', empty fields become NULL instead of empty strings
    let tsv = write_temp_file("hello\t\n\tworld\n");
    let sql = format!("COPY t FROM '{}' WITH (NULL '')", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_text(&rows[0][0], "hello");
    assert_null(&rows[0][1]); // empty field → NULL when null marker is ''
    assert_null(&rows[1][0]); // empty field → NULL
    assert_text(&rows[1][1], "world");
}

// ---------------------------------------------------------------------------
// Too few columns (missing data) — should fail
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_too_few_columns(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
        .unwrap();

    // Only 2 fields but table expects 3
    let tsv = write_temp_file("1\thello\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    let result = conn.execute(&sql);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Non-existent column in column list — should fail
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_nonexistent_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let tsv = write_temp_file("1\thello\n");
    let sql = format!("COPY t (a, xyz) FROM '{}'", tsv.path().display());
    let result = conn.execute(&sql);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// CSV format rejection — we only support text format
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_csv_format_unsupported(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let csv = write_temp_file("1,hello\n");
    let sql = format!("COPY t FROM '{}' WITH (FORMAT csv)", csv.path().display());
    let result = conn.execute(&sql);
    // Should fail: CSV format is not supported
    assert!(
        result.is_err(),
        "CSV format should not be silently accepted"
    );
}

// ---------------------------------------------------------------------------
// BINARY format rejection
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_binary_format_unsupported(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER)").unwrap();

    let tsv = write_temp_file("\x00");
    let sql = format!(
        "COPY t FROM '{}' WITH (FORMAT binary)",
        tsv.path().display()
    );
    let result = conn.execute(&sql);
    assert!(
        result.is_err(),
        "BINARY format should not be silently accepted"
    );
}

// ---------------------------------------------------------------------------
// Complex escape interactions from PG copy2.sql:
// \x (unknown escape → literal x), \\x → backslash+x, \\\x → backslash+x
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_complex_escape_sequences(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a TEXT, b TEXT, c TEXT, d TEXT)")
        .unwrap();

    // Custom delimiter ',' and null marker 'x' (from PG copy2.sql row 7)
    // File content: x,\x,\\x,\\\x
    // Verified against PG expected output (copy2.out row 7)
    let csv = write_temp_file("x,\\x,\\\\x,\\\\\\x\n");
    let sql = format!(
        "COPY t FROM '{}' WITH (DELIMITER ',', NULL 'x')",
        csv.path().display()
    );
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b, c, d FROM t");
    assert_eq!(rows.len(), 1);
    assert_null(&rows[0][0]); // "x" = null marker → NULL
    assert_text(&rows[0][1], "x"); // "\x" → unknown escape → "x"
    assert_text(&rows[0][2], "\\x"); // "\\x" → \\ → \, x plain → "\x"
    assert_text(&rows[0][3], "\\x"); // "\\\x" → \\ → \, \x unknown → x → "\x"
}

// ---------------------------------------------------------------------------
// \r\n line endings (Windows format)
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_crlf_line_endings(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    // Windows-style \r\n line endings
    let tsv = write_temp_file("1\tAlice\r\n2\tBob\r\n");
    let sql = format!("COPY t FROM '{}'", tsv.path().display());
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_int(&rows[0][0], 1);
    assert_text(&rows[0][1], "Alice");
    assert_int(&rows[1][0], 2);
    assert_text(&rows[1][1], "Bob");
}

// ---------------------------------------------------------------------------
// Semicolon delimiter (PG copy2.sql: DELIMITER AS ';' NULL AS '')
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_semicolon_delimiter_empty_null(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
        .unwrap();

    // Semicolon delimiter, empty string = null
    let data = write_temp_file("1;;hello\n");
    let sql = format!(
        "COPY t FROM '{}' WITH (DELIMITER ';', NULL '')",
        data.path().display()
    );
    conn.execute(&sql).unwrap();

    let rows = query_all(&conn, "SELECT a, b, c FROM t");
    assert_eq!(rows.len(), 1);
    assert_int(&rows[0][0], 1);
    assert_null(&rows[0][1]); // empty between semicolons → NULL
    assert_text(&rows[0][2], "hello");
}

// ---------------------------------------------------------------------------
// COPY FROM STDIN (not supported for file-based COPY) — should error
// ---------------------------------------------------------------------------

#[turso_macros::test(mvcc)]
fn test_copy_from_stdin_unsupported(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (a INTEGER)").unwrap();

    let result = conn.execute("COPY t FROM STDIN");
    // STDIN is not supported — should produce an error
    assert!(result.is_err());
}
