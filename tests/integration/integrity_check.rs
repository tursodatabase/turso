//! Integration tests for PRAGMA integrity_check corruption detection.
//!
//! These tests create databases, checkpoint them to disk, manually corrupt records,
//! and verify that integrity_check detects the corruption.
//!
//! NOTE: Corruption tests are disabled when the "checksum" feature is enabled,
//! because checksums will detect byte-level corruption before integrity_check runs.

use crate::common::TempDatabase;
#[cfg(not(feature = "checksum"))]
use std::fs::OpenOptions;
#[cfg(not(feature = "checksum"))]
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use turso_core::{Numeric, Value};

/// Default page size
#[cfg(not(feature = "checksum"))]
const PAGE_SIZE: usize = 4096;

/// Helper to run integrity_check and return the result string
fn run_integrity_check(conn: &Arc<turso_core::Connection>) -> String {
    let rows = conn
        .pragma_query("integrity_check")
        .expect("integrity_check should succeed");

    rows.into_iter()
        .filter_map(|row| {
            row.into_iter().next().and_then(|v| {
                if let Value::Text(text) = v {
                    Some(text.as_str().to_string())
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Helper to run quick_check and return the result string
fn run_quick_check(conn: &Arc<turso_core::Connection>) -> String {
    let rows = conn
        .pragma_query("quick_check")
        .expect("quick_check should succeed");

    rows.into_iter()
        .filter_map(|row| {
            row.into_iter().next().and_then(|v| {
                if let Value::Text(text) = v {
                    Some(text.as_str().to_string())
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Helper to checkpoint the database (flush WAL to main db file)
fn checkpoint_database(conn: &Arc<turso_core::Connection>) {
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
}

/// Helper to run integrity_check, catching panics
/// Returns Ok(result_string) or Err(panic_message)
#[cfg(not(feature = "checksum"))]
fn run_integrity_check_catching_panic(
    conn: &Arc<turso_core::Connection>,
) -> Result<String, String> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_integrity_check_or_error(conn)
    }));

    match result {
        Ok(Ok(msg)) => Ok(msg),
        Ok(Err(err_msg)) => Ok(err_msg),
        Err(panic_info) => {
            let panic_msg = panic_info
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_info.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(panic_msg)
        }
    }
}

/// Helper that returns Ok(result_string) or Err(error_string) instead of panicking
#[cfg(not(feature = "checksum"))]
fn run_integrity_check_or_error(conn: &Arc<turso_core::Connection>) -> Result<String, String> {
    match conn.pragma_query("integrity_check") {
        Ok(rows) => {
            let msg = rows
                .into_iter()
                .filter_map(|row| {
                    row.into_iter().next().and_then(|v| {
                        if let Value::Text(text) = v {
                            Some(text.as_str().to_string())
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>()
                .join("\n");
            Ok(msg)
        }
        Err(e) => Err(format!("{e}")),
    }
}

/// Read a 2-byte big-endian integer from a buffer
#[cfg(not(feature = "checksum"))]
fn read_u16_be(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([buf[offset], buf[offset + 1]])
}

/// Write a 2-byte big-endian integer to a buffer
#[cfg(not(feature = "checksum"))]
fn write_u16_be(buf: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_be_bytes();
    buf[offset] = bytes[0];
    buf[offset + 1] = bytes[1];
}

// =============================================================================
// VALID DATABASE TESTS
// =============================================================================

/// Test that integrity_check returns "ok" for a valid database
#[turso_macros::test]
fn test_integrity_check_valid_database(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, value INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t1(name);").unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'alice', 100);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 'bob', 200);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (3, 'charlie', 300);")
        .unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Valid database should pass integrity_check");
}

/// Test that quick_check returns "ok" for a valid database
#[turso_macros::test]
fn test_quick_check_valid_database(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t1(name);").unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'test');").unwrap();

    checkpoint_database(&conn);

    let result = run_quick_check(&conn);
    assert_eq!(result, "ok", "Valid database should pass quick_check");
}

/// Test that database with multiple indexes validates correctly
#[turso_macros::test]
fn test_integrity_check_multiple_indexes(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c REAL, d BLOB);")
        .unwrap();
    conn.execute("CREATE INDEX idx_b ON t1(b);").unwrap();
    conn.execute("CREATE INDEX idx_c ON t1(c);").unwrap();
    conn.execute("CREATE INDEX idx_bc ON t1(b, c);").unwrap();

    for i in 0..50 {
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({i}, 'text{i}', {i}.5, X'DEADBEEF');",
        ))
        .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Database with multiple indexes should pass integrity_check"
    );
}

/// Test that integrity_check with max_errors parameter works
#[turso_macros::test]
fn test_integrity_check_max_errors(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1);").unwrap();

    checkpoint_database(&conn);

    let rows = conn
        .pragma_query("integrity_check(10)")
        .expect("integrity_check(10) should succeed");

    let result: String = rows
        .into_iter()
        .filter_map(|row| {
            row.into_iter().next().and_then(|v| {
                if let Value::Text(text) = v {
                    Some(text.as_str().to_string())
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        result, "ok",
        "Valid database should pass integrity_check(10)"
    );
}

// =============================================================================
// REGRESSION TESTS
// =============================================================================

/// Regression test for issue #5124:
/// PRAGMA freelist_count must not mark page 1 dirty outside a write transaction.
#[turso_macros::test]
fn test_freelist_count_does_not_dirty_read_txn(db: TempDatabase) {
    {
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);")
            .unwrap();
        for i in 0..200 {
            conn.execute(format!(
                "INSERT INTO t VALUES ({i}, '{}');",
                "x".repeat(200)
            ))
            .unwrap();
        }
        checkpoint_database(&conn);
        conn.execute("DELETE FROM t WHERE id > 10;").unwrap();
        checkpoint_database(&conn);
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let rows = conn.pragma_query("freelist_count").unwrap();
    let count = match rows.as_slice() {
        [row] => match row.as_slice() {
            [Value::Numeric(Numeric::Integer(count))] => *count,
            other => panic!("unexpected freelist_count row: {other:?}"),
        },
        other => panic!("unexpected freelist_count result: {other:?}"),
    };
    assert!(count > 0, "expected freelist_count > 0");

    conn.execute("BEGIN;").unwrap();
    conn.execute("ROLLBACK;").unwrap();
}

// =============================================================================
// ERROR CASE TESTS: CellOutOfRange
// =============================================================================

/// Test detection of CellOutOfRange error
/// Corrupts a cell pointer to point before the content area
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_cell_out_of_range(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with enough data to have content in page 2
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..100 {
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({}, '{}');",
            i,
            "x".repeat(30) // Enough to ensure data is on page 2
        ))
        .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Read page 2, find the content area start, then set a cell pointer to before it
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        if file_size < 2 * PAGE_SIZE as u64 {
            panic!("Database should have at least 2 pages");
        }

        // Read page 2 (offset 4096)
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        // Page 2 header (leaf table page = 0x0d):
        // - byte 0: page type
        // - bytes 1-2: first freeblock
        // - bytes 3-4: cell count
        // - bytes 5-6: cell content start (content area begins here)
        // - byte 7: fragmented bytes
        // - bytes 8+: cell pointer array

        let page_type = page2[0];
        let cell_count = read_u16_be(&page2, 3);
        let content_start = read_u16_be(&page2, 5);

        // Page 2 must be a leaf table page with cells
        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count > 0, "Expected at least one cell");
        assert!(content_start > 8, "Expected content area after header");

        // Set first cell pointer to point BEFORE content area (e.g., offset 10)
        // This is invalid because cells must be within content area
        write_u16_be(&mut page2, 8, 10); // Cell pointer at offset 10, before content_start

        // Write corrupted page back
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("out of range"),
                "Expected 'out of range' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return CellOutOfRange error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: FreelistCountMismatch
// =============================================================================

/// Test detection of FreelistCountMismatch error
/// Creates a database with freelist pages, then corrupts the count
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_freelist_count_mismatch(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a table with data
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..100 {
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({}, '{}');",
            i,
            "x".repeat(100)
        ))
        .unwrap();
    }

    checkpoint_database(&conn);

    // Delete rows to create freelist pages
    conn.execute("DELETE FROM t1 WHERE id > 10;").unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Read the current freelist trunk page and count, then corrupt the count
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read current freelist trunk page (offset 32-35) and count (offset 36-39)
        let mut header = [0u8; 100];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut header).unwrap();

        let freelist_trunk = u32::from_be_bytes([header[32], header[33], header[34], header[35]]);
        let freelist_count = u32::from_be_bytes([header[36], header[37], header[38], header[39]]);

        // We deleted 90 rows, so we must have freelist pages
        assert!(freelist_trunk > 0, "Expected freelist trunk page");
        assert!(freelist_count > 0, "Expected freelist count > 0");

        // Increase the freelist count by 10, creating a mismatch
        let corrupted_count = freelist_count + 10;
        file.seek(SeekFrom::Start(36)).unwrap();
        file.write_all(&corrupted_count.to_be_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("Freelist: size is") && msg.contains("should be"),
                "Expected SQLite-style freelist size mismatch error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return FreelistCountMismatch error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: IndexEntryCountMismatch
// =============================================================================

/// Test detection of IndexEntryCountMismatch error
/// Creates a table with an index, then truncates the index page to have fewer cells
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_index_entry_count_mismatch(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a table with an index
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t1(name);").unwrap();

    // Insert exactly 10 rows
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'name{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // The database structure after checkpoint:
    // - Page 1: sqlite_master (schema)
    // - Page 2: t1 table data
    // - Page 3: idx_name index data
    //
    // To create an index entry count mismatch, we corrupt the index page (page 3)
    // by reducing its cell count in the header. This makes the B-tree cursor
    // count fewer index entries than table rows.
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 3 (index page, offset 8192)
        let mut page3 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page3).unwrap();

        let page_type = page3[0];
        let cell_count = read_u16_be(&page3, 3);

        // We inserted 10 rows with an index, so page 3 must be a leaf index page with 10 cells
        assert_eq!(page_type, 0x0a, "Expected leaf index page");
        assert_eq!(cell_count, 10, "Expected 10 index entries");

        // Reduce cell count from 10 to 5, creating a mismatch
        write_u16_be(&mut page3, 3, 5);

        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.write_all(&page3).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("wrong # of entries"),
                "Expected 'wrong # of entries' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return IndexEntryCountMismatch error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: NotNullViolation
// =============================================================================

/// Test detection of NotNullViolation error
/// Creates a NOT NULL column, then corrupts a record's serial type to NULL
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_not_null_violation(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with NOT NULL constraint
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER NOT NULL);")
        .unwrap();
    // Insert a few rows with non-null values
    conn.execute("INSERT INTO t1 VALUES (1, 100);").unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 200);").unwrap();
    conn.execute("INSERT INTO t1 VALUES (3, 300);").unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // To create a NOT NULL violation, we need to change a record's serial type to NULL.
    // SQLite record format:
    // - header_size (varint)
    // - serial types (varints) for each column
    // - column values
    //
    // For (1, 100): header is ~3 bytes, types are [1, 2] (int8, int16)
    // For (2, 200): similar
    // For (3, 300): similar
    //
    // We need to find a cell in page 2 and change the serial type of the second
    // column from a non-zero type to 0 (NULL).
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        if file_size < 2 * PAGE_SIZE as u64 {
            panic!("Database should have at least 2 pages");
        }

        // Read page 2 (table data)
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        let page_type = page2[0];
        let cell_count = read_u16_be(&page2, 3);

        // Page 2 must be a leaf table page with cells
        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count > 0, "Expected at least one cell");

        // Get first cell pointer
        let cell_ptr = read_u16_be(&page2, 8) as usize;
        assert!(cell_ptr + 5 < PAGE_SIZE, "Cell pointer out of bounds");

        // Cell format for leaf table page:
        // - payload_size (varint)
        // - rowid (varint)
        // - payload (record)
        //
        // Record format:
        // - header_size (varint)
        // - serial_type for col 0 (varint)
        // - serial_type for col 1 (varint)  <-- this is the NOT NULL column
        //
        // For small values:
        // cell_ptr + 0: payload_size varint
        // cell_ptr + 1: rowid varint
        // cell_ptr + 2: header_size varint (value 3 means 3 bytes of header)
        // cell_ptr + 3: type for col0
        // cell_ptr + 4: type for col1 <-- change this to 0
        page2[cell_ptr + 4] = 0; // Set type to NULL

        // Write corrupted page back
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("NULL value"),
                "Expected 'NULL value' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return NotNullViolation error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: PageReferencedMultipleTimes
// =============================================================================

/// Test detection of PageReferencedMultipleTimes error
/// Creates a B-tree with multiple pages, then corrupts a child pointer to reference
/// an already-referenced page
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_page_referenced_multiple_times(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create enough data to have multiple pages in the B-tree
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();

    // Insert enough data to create multiple leaf pages and possibly interior pages
    for i in 0..500 {
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({}, '{}');",
            i,
            "x".repeat(100) // Large enough to fill multiple pages
        ))
        .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // With 500 rows of ~100 bytes each, we should have multiple pages.
    // Find an interior page and modify its rightmost pointer to point to
    // a page that's also referenced by a cell pointer.
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        let num_pages = file_size as usize / PAGE_SIZE;

        // Find an interior table page (type 0x05)
        let mut found_interior_page = None;
        for page_num in 2..num_pages {
            let page_offset = (page_num - 1) * PAGE_SIZE;
            let mut page = [0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(page_offset as u64)).unwrap();
            file.read_exact(&mut page).unwrap();

            if page[0] == 0x05 {
                found_interior_page = Some((page_num, page_offset, page));
                break;
            }
        }

        let (page_num, page_offset, mut page) =
            found_interior_page.expect("With 500 rows, should have an interior table page");

        // Interior table page header:
        // type(1) + freeblock(2) + cellcount(2) + contentstart(2) + frag(1) + rightmost(4)
        // Cell pointer array starts at offset 12
        let cell_count = read_u16_be(&page, 3);
        assert!(
            cell_count >= 2,
            "Interior page {page_num} should have at least 2 cells, got {cell_count}"
        );

        // Read first cell pointer
        let first_cell_ptr = read_u16_be(&page, 12) as usize;
        // First 4 bytes of cell is the left child page number
        // Set rightmost pointer (at offset 8) to same value as left_child
        // This creates a duplicate page reference
        page[8] = page[first_cell_ptr];
        page[9] = page[first_cell_ptr + 1];
        page[10] = page[first_cell_ptr + 2];
        page[11] = page[first_cell_ptr + 3];

        file.seek(SeekFrom::Start(page_offset as u64)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("referenced multiple times"),
                "Expected 'referenced multiple times' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return PageReferencedMultipleTimes error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: CellOverlap
// =============================================================================

/// Test detection of CellOverlap error
/// Corrupts cell pointers to make two cells overlap
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_cell_overlap(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    // Insert enough rows to have multiple cells
    for i in 0..20 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'data_{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // To cause cell overlap, we modify a cell pointer to point into another cell's range
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 2
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        let page_type = page2[0];
        let cell_count = read_u16_be(&page2, 3);

        // Page 2 must be a leaf table page with at least 2 cells
        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count >= 2, "Expected at least two cells");

        // Read first cell pointer
        let cell1_ptr = read_u16_be(&page2, 8);

        // Set second cell pointer to overlap with first cell (first_ptr + 2)
        // This creates an overlap because the second cell now starts inside the first
        let overlapping_ptr = cell1_ptr.saturating_add(2);
        write_u16_be(&mut page2, 10, overlapping_ptr);

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("overlap"),
                "Expected 'overlap' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return CellOverlap error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: UnexpectedFragmentation
// =============================================================================

/// Test detection of UnexpectedFragmentation error
/// Corrupts the fragmented bytes count in the page header to not match actual fragmentation
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_unexpected_fragmentation(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'data{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt the fragmented bytes field in page 2's header
    // For leaf pages: offset 7 is the fragmented bytes count (1 byte)
    // Set it to a value that doesn't match the actual fragmentation
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 2
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        // Set fragmented bytes to 50 (should be 0 or very small in a fresh page)
        // The integrity check calculates actual fragmentation by coverage analysis
        // and compares it to this header value
        page2[7] = 50;

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("fragmentation"),
                "Expected 'fragmentation' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return UnexpectedFragmentation error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: PageNeverUsed
// =============================================================================

/// Test detection of PageNeverUsed error
/// Corrupts the database size in header to claim more pages exist than are referenced
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_page_never_used(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1);").unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt the database size in header (offset 28-31) to claim more pages exist
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Set database size to 10 pages when only 2 actually exist
        // This means pages 3-10 are "never used" according to the header
        file.seek(SeekFrom::Start(28)).unwrap();
        file.write_all(&10u32.to_be_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("never used"),
                "Expected 'never used' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return PageNeverUsed error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: UniqueViolation
// =============================================================================

/// Test detection of UniqueViolation error
/// Creates a UNIQUE index, then corrupts it to have duplicate key entries
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_unique_violation(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with UNIQUE constraint on name column
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT UNIQUE);")
        .unwrap();

    // Insert rows with unique values - using same length strings for easier corruption
    // We use 3-char strings so the record format is consistent
    conn.execute("INSERT INTO t1 VALUES (1, 'AAA');").unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 'BBB');").unwrap();
    conn.execute("INSERT INTO t1 VALUES (3, 'CCC');").unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // The database structure after checkpoint:
    // - Page 1: sqlite_master (schema)
    // - Page 2: t1 table data
    // - Page 3: sqlite_autoindex_t1_1 index data (UNIQUE index on name)
    //
    // To create a unique violation, we corrupt the index page (page 3)
    // by modifying one of the index entries to have the same key as another.
    // Index entries are: [name_value, rowid]
    // We change 'BBB' to 'AAA' in the second entry, creating a duplicate.
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 3 (index page, offset 8192)
        let mut page3 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page3).unwrap();

        let page_type = page3[0];
        let cell_count = read_u16_be(&page3, 3);

        // We inserted 3 rows with a UNIQUE index, so page 3 must be a leaf index page with 3 cells
        assert_eq!(page_type, 0x0a, "Expected leaf index page");
        assert_eq!(cell_count, 3, "Expected 3 index entries");

        // Get pointer to second cell (we'll modify it to match first)
        let cell2_ptr = read_u16_be(&page3, 10) as usize;

        // Search for 'BBB' in cell2 and replace with 'AAA' to create a duplicate
        let bbb = b"BBB";
        let aaa = b"AAA";

        let mut found = false;
        for i in cell2_ptr..cell2_ptr + 20 {
            if i + 3 <= PAGE_SIZE && &page3[i..i + 3] == bbb {
                page3[i..i + 3].copy_from_slice(aaa);
                found = true;
                break;
            }
        }
        assert!(found, "Should have found 'BBB' in cell2 to corrupt");

        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.write_all(&page3).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("non-unique entry"),
                "Expected 'non-unique entry' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return UniqueViolation error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: CellOverflowsPage
// =============================================================================

/// Test detection of CellOverflowsPage error
/// Corrupts a cell to extend beyond the page boundary
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_cell_overflows_page(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..20 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'data_{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt a cell pointer to make the cell extend beyond the page
    // We set a cell pointer near the end of the page with a large payload size
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 2 (table data)
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        let page_type = page2[0];
        let cell_count = read_u16_be(&page2, 3);

        // Page 2 must be a leaf table page with cells
        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count > 0, "Expected at least one cell");

        // Get first cell pointer
        let cell_ptr = read_u16_be(&page2, 8) as usize;
        assert!(cell_ptr < PAGE_SIZE - 10, "Cell pointer out of bounds");

        // Modify the cell to have a very large payload size that would overflow
        // Cell format: payload_size (varint), rowid (varint), payload
        // A 2-byte varint can encode values up to 16383
        // This creates a cell that claims to extend past the page boundary
        page2[cell_ptr] = 0xFF; // First byte of varint (continuation bit set)
        page2[cell_ptr + 1] = 0x7F; // Large payload size

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("extends out of page")
                    || msg.contains("out of range")
                    || msg.contains("out of bounds")
                    || msg.contains("corrupt"),
                "Expected cell overflow error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return an error for cell overflow, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: CellPointerOutOfBounds
// =============================================================================

/// Test that corrupt cell pointers pointing beyond the page do not cause a panic.
/// Instead, the error should be returned gracefully as a Corrupt error.
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_corrupt_cell_pointer_beyond_page(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'data_{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt a cell pointer to point beyond the page boundary
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        let page_type = page2[0];
        let cell_count = read_u16_be(&page2, 3);

        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count > 0, "Expected at least one cell");

        // Set first cell pointer to point beyond the page (0xFFFF > 4096)
        write_u16_be(&mut page2, 8, 0xFFFF);

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg != "ok",
                "Corrupt cell pointer should not pass integrity check"
            );
        }
        Err(panic_msg) => {
            panic!("Corrupt cell pointer should return an error, not panic: {panic_msg}");
        }
    }
}

/// Test that corrupt cell pointers on interior pages do not cause a panic.
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_corrupt_cell_pointer_interior_page(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Insert enough data to create interior pages in the B-tree
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..500 {
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({}, '{}');",
            i,
            "x".repeat(100)
        ))
        .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Find an interior table page and corrupt its cell pointer
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        let num_pages = file_size as usize / PAGE_SIZE;

        let mut found_interior_page = None;
        for page_num in 2..num_pages {
            let page_offset = (page_num - 1) * PAGE_SIZE;
            let mut page = [0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(page_offset as u64)).unwrap();
            file.read_exact(&mut page).unwrap();

            // 0x05 = interior table page
            if page[0] == 0x05 {
                let cell_count = read_u16_be(&page, 3);
                if cell_count > 0 {
                    found_interior_page = Some((page_offset, page));
                    break;
                }
            }
        }

        let (page_offset, mut page) =
            found_interior_page.expect("With 500 rows, should have an interior table page");

        // Interior page header is 12 bytes, cell pointer array starts at offset 12
        // Set first cell pointer to point beyond the page
        write_u16_be(&mut page, 12, 0xFFFF);

        file.seek(SeekFrom::Start(page_offset as u64)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg != "ok",
                "Corrupt interior page cell pointer should not pass integrity check"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Corrupt interior page cell pointer should return an error, not panic: {panic_msg}"
            );
        }
    }
}

/// Test that a SELECT on a table with a corrupt cell pointer does not panic.
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_select_with_corrupt_cell_pointer_no_panic(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'hello');").unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 'world');").unwrap();

    checkpoint_database(&conn);
    drop(conn);

    // Corrupt the first cell pointer on page 2 to point beyond the page
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        assert_eq!(page2[0], 0x0d, "Expected leaf table page");

        // Set first cell pointer to point beyond the page
        write_u16_be(&mut page2, 8, 0xFFFF);

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    // A SELECT should return an error, not panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute("SELECT * FROM t1;")
    }));

    match result {
        Ok(exec_result) => {
            // The query should return an error due to corruption
            assert!(
                exec_result.is_err(),
                "SELECT on corrupt page should return an error"
            );
        }
        Err(panic_info) => {
            let panic_msg = panic_info
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_info.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            panic!("SELECT on corrupt page should return an error, not panic: {panic_msg}");
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: CellRowidOutOfRange
// =============================================================================

/// Test detection of CellRowidOutOfRange error
/// Corrupts a cell's rowid to violate the monotonically increasing rowid invariant
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_cell_rowid_out_of_range(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a table with small payloads so payload_size fits in 1 byte
    // This makes the cell format predictable: payload_size (1 byte), rowid (1 byte for small rowids)
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();

    // Insert enough rows to get multiple cells per page
    for i in 1..50 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, {i});"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt the second cell's rowid to be smaller than the first cell's rowid
    // This violates the B-tree invariant that rowids must be monotonically increasing
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 2 (first data page after sqlite_master)
        let mut page = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page).unwrap();

        let page_type = page[0];
        let cell_count = read_u16_be(&page, 3);

        assert_eq!(page_type, 0x0d, "Expected leaf table page");
        assert!(cell_count >= 2, "Expected at least 2 cells");

        // Get second cell pointer (cell2 should have rowid > cell1's rowid)
        let cell2_ptr = read_u16_be(&page, 10) as usize;

        // Cell format for small INTEGER values:
        // - payload_size: 1 byte (value ~3 for header + 2 small ints)
        // - rowid: 1 byte for rowids 1-127
        // - payload: the record

        // Set cell2's rowid (at offset +1) to 0, which is smaller than cell1's rowid
        // This makes cell1's rowid > cell2's rowid, triggering the check
        page[cell2_ptr + 1] = 0;

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            assert!(
                msg.contains("rowid") && msg.contains("wrong order"),
                "Expected 'rowid ... wrong order' error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!(
                "Expected integrity_check to return CellRowidOutOfRange error, but it panicked: {panic_msg}"
            );
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: FreeBlockOutOfRange
// =============================================================================

/// Test detection of FreeBlockOutOfRange error
/// Corrupts the freeblock chain to point outside valid range
///
/// FIXME: The pager may panic on this corruption instead of returning an error.
/// We should handle invalid freeblock pointers gracefully.
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_freeblock_out_of_range(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT);")
        .unwrap();
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'data{i}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    // Delete some rows to potentially create freeblocks
    conn.execute("DELETE FROM t1 WHERE id > 5;").unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt the freeblock pointer in page 2's header
    // Freeblock pointer is at offset 1-2 in the page header
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // Read page 2
        let mut page2 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page2).unwrap();

        // Set first freeblock pointer to an invalid offset near end of page
        // but leave room for the freeblock header (4 bytes)
        // Freeblock: next_freeblock (2 bytes), size (2 bytes)
        let freeblock_offset = PAGE_SIZE - 8; // Safe offset
        write_u16_be(&mut page2, 1, freeblock_offset as u16);

        // Write freeblock structure at that location with invalid size
        write_u16_be(&mut page2, freeblock_offset, 0); // next = 0 (end of chain)
        write_u16_be(&mut page2, freeblock_offset + 2, 100); // size = 100 (extends beyond page)

        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page2).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            // The corruption might trigger different errors depending on how it's detected
            assert!(
                msg.contains("freeblock")
                    || msg.contains("out of range")
                    || msg.contains("fragmentation")
                    || msg.contains("overlap")
                    || msg != "ok",
                "Expected freeblock error or some error, got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!("Expected integrity_check to return an error, but it panicked: {panic_msg}");
        }
    }
}

// =============================================================================
// ERROR CASE TESTS: RowMissingFromIndex
// =============================================================================

/// Test detection of RowMissingFromIndex error
/// Creates a table with index, then corrupts the index to remove an entry
/// while keeping the table row.
#[cfg(not(feature = "checksum"))]
#[turso_macros::test]
fn test_integrity_check_row_missing_from_index(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with an index
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    conn.execute("CREATE INDEX idx_t1_name ON t1(name);")
        .unwrap();

    // Insert rows - using fixed-length strings for predictable layout
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i}, 'name_{i:03}');"))
            .unwrap();
    }

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid before corruption");

    drop(conn);

    // Corrupt the index by reducing the cell count in the index page header
    // This makes the index "forget" about the last entry, while the table still has the row
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db.path)
            .unwrap();

        // The index is on page 3 (page 1 = schema, page 2 = table, page 3 = index)
        let mut page3 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page3).unwrap();

        let page_type = page3[0];
        let cell_count = read_u16_be(&page3, 3);

        // We inserted 10 rows with an index, so page 3 must be a leaf index page with 10 cells
        assert_eq!(page_type, 0x0a, "Expected leaf index page");
        assert_eq!(cell_count, 10, "Expected 10 index entries");

        // Reduce cell count by 1 - this makes the last row "missing" from the index
        write_u16_be(&mut page3, 3, cell_count - 1);

        file.seek(SeekFrom::Start(2 * PAGE_SIZE as u64)).unwrap();
        file.write_all(&page3).unwrap();
        file.sync_all().unwrap();
    }

    let db = TempDatabase::new_with_existent(&db.path);
    let conn = db.connect_limbo();

    let result = run_integrity_check_catching_panic(&conn);
    match result {
        Ok(msg) => {
            // Should detect both: wrong count AND row missing from index
            assert!(
                msg.contains("wrong # of entries"),
                "Expected 'wrong # of entries', got: {msg}"
            );
            assert!(
                msg.contains("missing from index"),
                "Expected 'missing from index', got: {msg}"
            );
        }
        Err(panic_msg) => {
            panic!("Expected integrity_check to return error, but it panicked: {panic_msg}");
        }
    }
}

/// Test integrity_check with index on column added via ALTER TABLE ADD COLUMN with DEFAULT.
///
/// This is a regression test for a bug where integrity_check would report false positives
/// ("row X missing from index") because it used NULL for missing columns instead of the
/// column's DEFAULT value.
///
/// The bug occurs because:
/// 1. Old rows (inserted before ALTER TABLE) don't physically store the new column
/// 2. When CREATE INDEX builds the index, it uses the DEFAULT value for these rows
/// 3. But integrity_check was using NULL when reading these rows to verify the index
/// 4. This caused a seek mismatch: index has (DEFAULT, rowid), but check looked for (NULL, rowid)
#[turso_macros::test]
fn test_integrity_check_index_on_column_with_default(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table and insert rows BEFORE adding the column with DEFAULT
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'charlie');")
        .unwrap();

    // Add column with DEFAULT - old rows don't physically have this column,
    // but its value is implicitly the DEFAULT (42)
    conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 42;")
        .unwrap();

    // Create index on the new column - index entries will contain the DEFAULT value (42)
    conn.execute("CREATE INDEX idx_extra ON t(extra);").unwrap();

    checkpoint_database(&conn);

    // Integrity check must use DEFAULT (42) not NULL when checking old rows
    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Integrity check should pass - must use DEFAULT value for missing columns"
    );
}

/// More complex test: multiple tables, multiple columns with different DEFAULT types,
/// composite indexes, and a mix of rows inserted before and after ALTER TABLE.
#[turso_macros::test]
fn test_integrity_check_multiple_tables_with_defaults(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Table 1: Will have INTEGER and TEXT defaults
    conn.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'alice');")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob');")
        .unwrap();

    // Table 2: Will have REAL default
    conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, title TEXT);")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (100, 'Widget');")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (101, 'Gadget');")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (102, 'Gizmo');")
        .unwrap();

    // Add columns with various DEFAULT types to table 1
    conn.execute("ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0;")
        .unwrap();
    conn.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';")
        .unwrap();

    // Add column with REAL default to table 2
    conn.execute("ALTER TABLE products ADD COLUMN price REAL DEFAULT 9.99;")
        .unwrap();

    // Insert some rows AFTER the ALTER TABLE (these will have the columns physically)
    conn.execute("INSERT INTO users VALUES (3, 'charlie', 30, 'inactive');")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (103, 'Doohickey', 19.99);")
        .unwrap();

    // Create indexes on the new columns - these must use DEFAULT values for old rows
    conn.execute("CREATE INDEX idx_users_age ON users(age);")
        .unwrap();
    conn.execute("CREATE INDEX idx_users_status ON users(status);")
        .unwrap();
    conn.execute("CREATE INDEX idx_products_price ON products(price);")
        .unwrap();

    // Create a composite index spanning original and new columns
    conn.execute("CREATE INDEX idx_users_composite ON users(name, age, status);")
        .unwrap();

    checkpoint_database(&conn);

    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Integrity check should pass with multiple tables and various DEFAULT types"
    );
}

#[turso_macros::test]
fn test_reserved_bytes_set_before_page_allocation(db: TempDatabase) {
    /// bug: previously, `set_reserved_bytes()` only updated the pager's cache, but
    /// `allocate_page1()` read from IOContext which returned 0, causing pages to be
    /// created with wrong `cell_content_area` and triggering integrity_check errors
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    const RESERVED_BYTES: u8 = 48;
    // offset of reserved_space field in SQLite database header
    const RESERVED_SPACE_OFFSET: u64 = 20;

    let conn = db.connect_limbo();

    // set reserved_bytes BEFORE any page allocation
    // This is what the sync engine does for encrypted remotes
    conn.set_reserved_bytes(RESERVED_BYTES).unwrap();

    // trigger page 1 allocation via write transaction
    // the bug was, this would read reserved_space from IOContext (0)
    // instead of using the cached value (48)
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("INSERT INTO test_table VALUES (1, 'yo v!')")
        .unwrap();
    checkpoint_database(&conn);

    assert_eq!(
        conn.get_reserved_bytes(),
        Some(RESERVED_BYTES),
        "Pager should have reserved_bytes={RESERVED_BYTES}"
    );

    // manually, verify reserved_space in the database header file, because why not
    {
        let mut file = File::open(&db.path).expect("Failed to open database file");
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET)).unwrap();
        let mut reserved_space_buf = [0u8; 1];
        file.read_exact(&mut reserved_space_buf).unwrap();

        assert_eq!(
            reserved_space_buf[0], RESERVED_BYTES,
            "Database header should have reserved_space={RESERVED_BYTES}, got {}",
            reserved_space_buf[0]
        );
    }

    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Integrity check should pass when reserved_bytes is set before page allocation"
    );
}

#[turso_macros::test]
fn test_reserved_bytes_default(db: TempDatabase) {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    const RESERVED_SPACE_OFFSET: u64 = 20;

    // checksum feature reserves 8 bytes, otherwise 0
    #[cfg(feature = "checksum")]
    const EXPECTED_RESERVED_BYTES: u8 = 8;
    #[cfg(not(feature = "checksum"))]
    const EXPECTED_RESERVED_BYTES: u8 = 0;

    let conn = db.connect_limbo();
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("INSERT INTO test_table VALUES (1, 'test data')")
        .unwrap();
    checkpoint_database(&conn);

    assert_eq!(
        conn.get_reserved_bytes(),
        Some(EXPECTED_RESERVED_BYTES),
        "Pager should have reserved_bytes={EXPECTED_RESERVED_BYTES} by default"
    );

    {
        let mut file = File::open(&db.path).expect("Failed to open database file");
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET)).unwrap();
        let mut reserved_space_buf = [0u8; 1];
        file.read_exact(&mut reserved_space_buf).unwrap();

        assert_eq!(
            reserved_space_buf[0], EXPECTED_RESERVED_BYTES,
            "Database header should have reserved_space={EXPECTED_RESERVED_BYTES} by default, got {}",
            reserved_space_buf[0]
        );
    }

    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Integrity check should pass with default reserved_bytes"
    );
}

// =============================================================================
// REGRESSION TEST: Freelist trunk page reuse
// =============================================================================

/// Regression test for freelist trunk page reuse bug.
///
/// When a single page is freed into an empty freelist, it becomes the trunk page
/// with no leaf pointers and no next trunk pointer. The trunk page ITSELF should
/// be reusable for allocation.
///
/// The bug was that when allocating a page and finding a trunk with:
/// - no leaf pointers (number_of_freelist_leaves == 0)
/// - no next trunk (next_trunk_page_id == 0)
///
/// The old code would incorrectly treat this as "empty freelist" and allocate a
/// new page at the end of the database file, leaving the trunk page permanently
/// stuck in the freelist (never reused).
///
/// This test verifies that after:
/// 1. Creating a table (allocates data page)
/// 2. Dropping the table (frees data page -> becomes freelist trunk)
/// 3. Creating another table (should reuse the freelist trunk)
///
/// The freelist_count should be 0 (trunk was properly reused), not 1.
#[turso_macros::test]
fn test_freelist_trunk_page_reuse(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create a table with some data to occupy a data page (page 2)
    conn.execute("CREATE TABLE t1(x INTEGER PRIMARY KEY, y TEXT);")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'some data to put in the table');")
        .unwrap();

    checkpoint_database(&conn);

    // Verify initial state
    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid after initial setup");

    // Drop the table - this frees the data page into the freelist.
    // The freed page becomes the trunk page (no leaves, no next trunk).
    conn.execute("DROP TABLE t1;").unwrap();

    checkpoint_database(&conn);

    // After dropping, freelist should have exactly 1 page (the trunk)
    let freelist_after_drop: Vec<Vec<turso_core::Value>> = conn
        .pragma_query("freelist_count")
        .expect("freelist_count should work");
    let freelist_count_after_drop = match &freelist_after_drop[0][0] {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(n)) => *n,
        _ => panic!("Expected integer from freelist_count"),
    };
    assert_eq!(
        freelist_count_after_drop, 1,
        "Freelist should have exactly 1 page after DROP TABLE"
    );

    let result = run_integrity_check(&conn);
    assert_eq!(result, "ok", "Database should be valid after DROP TABLE");

    // Create a new table and insert data.
    // This should allocate a page from the freelist, reusing the trunk.
    // BUG: Old code would skip the trunk (because next_trunk == 0) and allocate
    // a new page at the end of the database, leaving freelist_count = 1.
    conn.execute("CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT);")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 'new data');")
        .unwrap();

    checkpoint_database(&conn);

    // After creating t2, the freelist trunk should have been reused.
    // With the fix: freelist_count = 0 (trunk reused).
    // With the bug: freelist_count = 1 (trunk never reused).
    let freelist_after_create: Vec<Vec<turso_core::Value>> = conn
        .pragma_query("freelist_count")
        .expect("freelist_count should work");
    let freelist_count_after_create = match &freelist_after_create[0][0] {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(n)) => *n,
        _ => panic!("Expected integer from freelist_count"),
    };

    assert_eq!(
        freelist_count_after_create, 0,
        "Freelist trunk page should have been reused for allocation"
    );

    let result = run_integrity_check(&conn);
    assert_eq!(
        result, "ok",
        "Database should pass integrity check after freelist trunk reuse"
    );
}
