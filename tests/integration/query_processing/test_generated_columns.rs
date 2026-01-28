use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use tempfile::TempDir;

/// Test that rows with mixed STORED and VIRTUAL generated columns can be found
/// after database reopen, UPDATE, and SELECT operations.
///
/// This test reproduces a bug found via fault-injection simulation (seed: 15339281454867987339)
/// where rows become unfindable after a sequence of: CREATE TABLE → INSERT → REOPEN → UPDATE → SELECT.
///
/// The bug requires fault injection (DISCONNECT/REOPEN_DATABASE) to manifest.
#[test]
fn test_gencol_row_not_found_after_reopen() {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_gencol_reopen.db");

    // Phase 1: Create table with mixed STORED and VIRTUAL generated columns, insert data
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Create table with interleaved STORED and VIRTUAL generated columns
        // Simplified from the 113-column table in the simulator bug report
        conn.execute(
            "CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                col_a REAL,
                col_b TEXT,
                col_c INTEGER,
                col_d BLOB,
                gen_virtual_1 REAL AS (col_a),
                gen_stored_1 REAL AS (+ col_a) STORED,
                col_e TEXT,
                col_f INTEGER,
                gen_virtual_2 TEXT AS (col_b),
                gen_stored_2 TEXT AS (col_b || col_e) STORED,
                col_g REAL,
                col_h BLOB,
                gen_virtual_3 INTEGER AS (col_c),
                gen_stored_3 REAL AS (col_a * col_a) STORED,
                col_i TEXT,
                col_j INTEGER
            )",
        )
        .unwrap();

        // Insert multiple rows
        conn.execute(
            "INSERT INTO t (id, col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, col_i, col_j)
             VALUES (1, 100.5, 'hello', 42, X'AABB', 'world', 10, 3.14, X'CCDD', 'test1', 100)"
        ).unwrap();

        conn.execute(
            "INSERT INTO t (id, col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, col_i, col_j)
             VALUES (2, -200.25, 'foo', -100, X'1122', 'bar', 20, -6.28, X'3344', 'test2', 200)"
        ).unwrap();

        conn.execute(
            "INSERT INTO t (id, col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, col_i, col_j)
             VALUES (3, 0.0, 'empty', 0, X'0000', '', 0, 0.0, X'FFFF', 'test3', 0)"
        ).unwrap();

        // Verify data is correct before close
        let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
        assert_eq!(
            rows[0][0],
            Value::Integer(3),
            "Should have 3 rows before reopen"
        );
    }
    // Database closes when scope ends

    // Phase 2: Reopen database - this simulates REOPEN_DATABASE fault
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // First verify rows exist
        let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
        assert_eq!(
            rows[0][0],
            Value::Integer(3),
            "Should have 3 rows after first reopen"
        );

        // Update some rows - this is part of the bug trigger sequence
        conn.execute("UPDATE t SET col_a = 999.99, col_b = 'updated' WHERE id = 1")
            .unwrap();
    }
    // Database closes again

    // Phase 3: Reopen again and verify data integrity
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // This is where the bug manifests - rows might not be found
        let rows = limbo_exec_rows(&conn, "SELECT * FROM t WHERE id = 1");

        eprintln!("Rows found after reopens and update: {}", rows.len());
        for (i, row) in rows.iter().enumerate() {
            eprintln!("Row {i}: {row:?}");
        }

        assert_eq!(rows.len(), 1, "Row with id=1 should be found after reopens");

        // Also verify all rows can be found
        let all_rows = limbo_exec_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(all_rows.len(), 3, "All 3 rows should be found");
        assert_eq!(all_rows[0][0], Value::Integer(1));
        assert_eq!(all_rows[1][0], Value::Integer(2));
        assert_eq!(all_rows[2][0], Value::Integer(3));

        // Verify generated columns have correct values after update
        let row1 = limbo_exec_rows(
            &conn,
            "SELECT col_a, gen_virtual_1, gen_stored_1, gen_stored_3 FROM t WHERE id = 1",
        );
        assert_eq!(row1.len(), 1, "Should find row 1");
        // gen_virtual_1 = col_a = 999.99
        // gen_stored_1 = + col_a = 999.99
        // gen_stored_3 = col_a * col_a = 999.99 * 999.99
        assert_eq!(row1[0][0], Value::Real(999.99), "col_a should be 999.99");
        assert_eq!(
            row1[0][1],
            Value::Real(999.99),
            "gen_virtual_1 should be 999.99"
        );
        assert_eq!(
            row1[0][2],
            Value::Real(999.99),
            "gen_stored_1 should be 999.99"
        );
    }
}

/// Test with more complex generated column expressions matching the simulator bug pattern.
/// Uses nested generated columns and more column types.
#[test]
fn test_gencol_complex_expressions_after_reopen() {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_gencol_complex.db");

    // Phase 1: Create complex table
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Table with generated columns that reference other columns
        // Pattern from simulator: gen columns referencing regular columns in complex ways
        conn.execute(
            "CREATE TABLE t (
                base_int INTEGER,
                base_real REAL,
                base_text TEXT,
                base_blob BLOB,
                gen_v1 INTEGER AS (base_int),
                gen_s1 REAL AS (+ base_real) STORED,
                middle_text TEXT,
                gen_v2 TEXT AS (base_text),
                gen_s2 TEXT AS (middle_text || base_text) STORED,
                middle_int INTEGER,
                gen_v3 REAL AS (base_real),
                gen_s3 INTEGER AS (+ middle_int + base_int) STORED,
                trailing_real REAL,
                gen_v4 BLOB AS (base_blob),
                gen_s4 REAL AS (base_real * base_real) STORED
            )",
        )
        .unwrap();

        // Insert rows
        conn.execute(
            "INSERT INTO t (base_int, base_real, base_text, base_blob, middle_text, middle_int, trailing_real)
             VALUES (100, 1.5, 'hello', X'AABB', 'mid_', 50, 9.9)"
        ).unwrap();

        conn.execute(
            "INSERT INTO t (base_int, base_real, base_text, base_blob, middle_text, middle_int, trailing_real)
             VALUES (-200, -2.5, 'world', X'CCDD', 'test_', -100, -8.8)"
        ).unwrap();
    }

    // Phase 2: Multiple reopens with updates
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Update a row
        conn.execute("UPDATE t SET base_int = 999 WHERE base_int = 100")
            .unwrap();
    }

    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Another update
        conn.execute("UPDATE t SET middle_text = 'changed_' WHERE base_int = 999")
            .unwrap();
    }

    // Phase 3: Final verification
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Verify both rows exist and have correct generated values
        let rows = limbo_exec_rows(
            &conn,
            "SELECT base_int, gen_v1, gen_s3 FROM t ORDER BY base_int",
        );

        eprintln!("Rows after multiple reopens:");
        for (i, row) in rows.iter().enumerate() {
            eprintln!("Row {i}: {row:?}");
        }

        assert_eq!(rows.len(), 2, "Both rows should be found");

        // Row 1: base_int=-200, gen_v1=-200, gen_s3 = middle_int + base_int = -100 + -200 = -300
        assert_eq!(rows[0][0], Value::Integer(-200));
        assert_eq!(rows[0][1], Value::Integer(-200));
        assert_eq!(rows[0][2], Value::Integer(-300));

        // Row 2: base_int=999 (updated), gen_v1=999, gen_s3 = middle_int + base_int = 50 + 999 = 1049
        assert_eq!(rows[1][0], Value::Integer(999));
        assert_eq!(rows[1][1], Value::Integer(999));
        assert_eq!(rows[1][2], Value::Integer(1049));

        // Verify STORED concatenation column
        let concat_rows = limbo_exec_rows(&conn, "SELECT gen_s2 FROM t WHERE base_int = 999");
        assert_eq!(concat_rows.len(), 1);
        // gen_s2 = middle_text || base_text = 'changed_' || 'hello' = 'changed_hello'
        assert_eq!(concat_rows[0][0], Value::Text("changed_hello".to_string()));
    }
}

/// Test STORED generated columns survive ADD COLUMN + database reopen + UPDATE.
#[test]
fn test_stored_gencol_corruption_after_add_column_and_reopen() {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Phase 1: Create table with STORED generated column, insert data, add column
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Create table with columns before and after STORED generated column
        conn.execute(
            "CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                col_a INTEGER,
                gen_col TEXT GENERATED ALWAYS AS ('gen_value') STORED,
                col_b INTEGER,
                col_c TEXT
            )",
        )
        .unwrap();

        // Insert a row
        conn.execute("INSERT INTO t (id, col_a, col_b, col_c) VALUES (1, 100, 200, 'text_c')")
            .unwrap();

        // Add a new column AFTER the table already has data
        conn.execute("ALTER TABLE t ADD COLUMN new_col INTEGER")
            .unwrap();

        // Verify data before reopen - should be correct
        let rows = limbo_exec_rows(
            &conn,
            "SELECT id, col_a, gen_col, col_b, col_c, new_col FROM t",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(1)); // id
        assert_eq!(rows[0][1], Value::Integer(100)); // col_a
        assert_eq!(rows[0][2], Value::Text("gen_value".to_string())); // gen_col
        assert_eq!(rows[0][3], Value::Integer(200)); // col_b
        assert_eq!(rows[0][4], Value::Text("text_c".to_string())); // col_c
        assert_eq!(rows[0][5], Value::Null); // new_col
    }
    // Database is closed when tmp_db goes out of scope

    // Phase 2: Reopen database and update a column - this triggers the bug
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Update the new column - this triggers the corruption
        conn.execute("UPDATE t SET new_col = 999 WHERE id = 1")
            .unwrap();

        // Query the data - this should show the corruption
        let rows = limbo_exec_rows(
            &conn,
            "SELECT id, col_a, gen_col, col_b, col_c, new_col FROM t",
        );

        // Print actual values for debugging
        eprintln!("After reopen and UPDATE:");
        for (i, val) in rows[0].iter().enumerate() {
            eprintln!("  Column {i}: {val:?}");
        }

        // Assert expected values - these should pass but currently fail due to bug
        assert_eq!(rows.len(), 1, "Expected exactly 1 row");
        assert_eq!(rows[0][0], Value::Integer(1), "id should be 1");
        assert_eq!(rows[0][1], Value::Integer(100), "col_a should be 100");
        assert_eq!(
            rows[0][2],
            Value::Text("gen_value".to_string()),
            "gen_col should be 'gen_value'"
        );
        assert_eq!(rows[0][3], Value::Integer(200), "col_b should be 200");
        assert_eq!(
            rows[0][4],
            Value::Text("text_c".to_string()),
            "col_c should be 'text_c'"
        );
        assert_eq!(
            rows[0][5],
            Value::Integer(999),
            "new_col should be 999 (the value we just set)"
        );
    }
}

/// Test STORED column that depends on VIRTUAL column which depends on another STORED column.
/// This is a critical dependency chain found in the simulator bug (seed: 15339281454867987339):
///   regular → STORED → VIRTUAL → STORED
///
/// In the original bug:
/// - `glowing_kumper_596 REAL AS (- vivacious_rowan_585 * vivacious_rowan_585) STORED`
/// - `romantic_spies_668 REAL AS (glowing_kumper_596)` (VIRTUAL)
/// - `giving_massimino_660 REAL AS (romantic_spies_668) STORED`
#[test]
fn test_stored_depends_on_virtual_depends_on_stored() {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_chain.db");

    // Phase 1: Create table with STORED → VIRTUAL → STORED chain
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        conn.execute(
            "CREATE TABLE t (
                a REAL,
                s1 REAL AS (- a * a) STORED,
                v1 REAL AS (s1),
                s2 REAL AS (v1) STORED
            )",
        )
        .unwrap();

        // Insert row with value that matches the simulator bug
        conn.execute("INSERT INTO t (a) VALUES (-5950697394.64845)")
            .unwrap();

        // Verify values immediately after insert
        let rows = limbo_exec_rows(&conn, "SELECT a, s1, v1, s2 FROM t");
        eprintln!("After INSERT (before reopen):");
        eprintln!("  a = {:?}", rows[0][0]);
        eprintln!("  s1 = {:?}", rows[0][1]);
        eprintln!("  v1 = {:?}", rows[0][2]);
        eprintln!("  s2 = {:?}", rows[0][3]);

        // Expected: s1 = -(-5950697394.64845)^2 ≈ -3.541e19
        // s2 should equal s1 (via v1)
    }

    // Phase 2: Reopen database (simulates REOPEN_DATABASE fault)
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        let rows = limbo_exec_rows(&conn, "SELECT a, s1, v1, s2 FROM t");
        eprintln!("After REOPEN:");
        eprintln!("  a = {:?}", rows[0][0]);
        eprintln!("  s1 = {:?}", rows[0][1]);
        eprintln!("  v1 = {:?}", rows[0][2]);
        eprintln!("  s2 = {:?}", rows[0][3]);

        assert_eq!(rows.len(), 1, "Should have 1 row");

        // Extract values
        let a = match rows[0][0] {
            Value::Real(v) => v,
            _ => panic!("a should be Real"),
        };
        let s1 = match rows[0][1] {
            Value::Real(v) => v,
            _ => panic!("s1 should be Real"),
        };
        let s2 = match rows[0][3] {
            Value::Real(v) => v,
            _ => panic!("s2 should be Real"),
        };

        // s1 = -a*a
        let expected_s1 = -a * a;
        assert!(
            (s1 - expected_s1).abs() < 1e10,
            "s1 should be -a*a = {expected_s1}, got {s1}"
        );

        // s2 should equal s1 (via v1)
        assert!(
            (s2 - s1).abs() < 1e10,
            "s2 should equal s1 = {s1}, got {s2}"
        );
    }
}

/// Test UPDATE on table with STORED → VIRTUAL → STORED chain after reopen.
#[test]
fn test_stored_chain_update_after_reopen() {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_chain_update.db");

    // Phase 1: Create and insert
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        conn.execute(
            "CREATE TABLE t (
                a REAL,
                s1 REAL AS (- a * a) STORED,
                v1 REAL AS (s1),
                s2 REAL AS (v1) STORED
            )",
        )
        .unwrap();

        conn.execute("INSERT INTO t (a) VALUES (10.0)").unwrap();
    }

    // Phase 2: Reopen and update
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        // Update the base column
        conn.execute("UPDATE t SET a = -5950697394.64845").unwrap();
    }

    // Phase 3: Reopen again and verify
    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();

        let rows = limbo_exec_rows(&conn, "SELECT a, s1, v1, s2 FROM t");
        eprintln!("After UPDATE and second REOPEN:");
        eprintln!("  a = {:?}", rows[0][0]);
        eprintln!("  s1 = {:?}", rows[0][1]);
        eprintln!("  v1 = {:?}", rows[0][2]);
        eprintln!("  s2 = {:?}", rows[0][3]);

        assert_eq!(rows.len(), 1, "Should have 1 row");

        let a = match rows[0][0] {
            Value::Real(v) => v,
            _ => panic!("a should be Real"),
        };
        let s1 = match rows[0][1] {
            Value::Real(v) => v,
            _ => panic!("s1 should be Real"),
        };
        let s2 = match rows[0][3] {
            Value::Real(v) => v,
            _ => panic!("s2 should be Real"),
        };

        // After UPDATE, s1 = -a*a should be recomputed
        let expected_s1 = -a * a;
        assert!(
            (s1 - expected_s1).abs() < 1e10,
            "s1 should be -a*a = {expected_s1}, got {s1}"
        );

        // s2 should equal s1 (via v1)
        assert!(
            (s2 - s1).abs() < 1e10,
            "s2 should equal s1 = {s1}, got {s2}"
        );
    }
}
