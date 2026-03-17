#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use tempfile::TempDir;
    use turso_core::Value;

    /// Custom types must be loaded from __turso_internal_types when reopening
    /// a database. Without this, SELECT returns raw encoded values and PRAGMA
    /// list_types omits user-defined types.
    #[test]
    fn test_custom_types_persist_across_reopen() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_reopen.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        // First session: create a custom type, table, and insert data
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 100)").unwrap();

            // Sanity check: values are decoded in the first session
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 ORDER BY id");
            assert_eq!(rows, vec![(1, 42), (2, 100)]);
            conn.close().unwrap();
        }

        // Second session: reopen and verify decoded values
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            // SELECT must return decoded values, not raw encoded (4200, 10000)
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 ORDER BY id");
            assert_eq!(
                rows,
                vec![(1, 42), (2, 100)],
                "After reopen, SELECT should return decoded values, not raw encoded"
            );

            // INSERT must still apply encoding
            conn.execute("INSERT INTO t1 VALUES (3, 55)").unwrap();
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 WHERE id = 3");
            assert_eq!(rows, vec![(3, 55)]);

            conn.close().unwrap();
        }
    }

    /// After reopening, schema changes (CREATE TABLE) that use a previously
    /// defined custom type must still work. The type must survive the schema
    /// change and new tables must encode/decode correctly.
    #[test]
    fn test_custom_types_survive_schema_change_after_reopen() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_schema_change.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        // First session: create type, table, insert data
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();
            conn.close().unwrap();
        }

        // Second session: reopen, create a new table using the same type
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            // Original table still decodes
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
            assert_eq!(rows, vec![(42,)], "t1 should decode after reopen");

            // Create a second table using the same custom type (schema change)
            conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, price cents) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1, 99)").unwrap();

            // Both tables must decode correctly
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
            assert_eq!(
                rows,
                vec![(42,)],
                "t1 should still decode after schema change"
            );
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT price FROM t2 WHERE id = 1");
            assert_eq!(rows, vec![(99,)], "t2 should decode after schema change");

            conn.close().unwrap();
        }

        // Third session: reopen again, both tables must still work
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
            assert_eq!(rows, vec![(42,)], "t1 should decode after second reopen");
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT price FROM t2 WHERE id = 1");
            assert_eq!(rows, vec![(99,)], "t2 should decode after second reopen");

            conn.close().unwrap();
        }
    }

    /// A new connection on the same database must see custom types that were
    /// created by another connection, even without reopening the database file.
    #[test]
    fn test_new_connection_sees_custom_types() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_new_conn.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn1 = db.connect_limbo();
        conn1
            .execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn1
            .execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();
        conn1.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();

        // Second connection on the same database
        let conn2 = db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT amount FROM t1 WHERE id = 1");
        assert_eq!(
            rows,
            vec![(42,)],
            "New connection should decode custom type values"
        );

        // Second connection should also be able to insert with encoding
        conn2.execute("INSERT INTO t1 VALUES (2, 77)").unwrap();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT amount FROM t1 WHERE id = 2");
        assert_eq!(rows, vec![(77,)]);
    }

    /// UPSERT (INSERT ... ON CONFLICT DO UPDATE) must not double-encode
    /// custom type values. The `excluded.column` pseudo-table must return
    /// user-facing (decoded) values so that the DO UPDATE SET path encodes
    /// them exactly once.
    ///
    /// Also tests that the WHERE clause in DO UPDATE sees decoded values,
    /// and that sequential UPSERTs do not progressively corrupt data.
    #[test]
    fn test_upsert_does_not_double_encode_custom_types() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_upsert.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();

        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();

        // Bug 7: excluded.amount should not be double-encoded
        conn.execute(
            "INSERT INTO t1 VALUES (1, 50) ON CONFLICT(id) DO UPDATE SET amount = excluded.amount",
        )
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
        assert_eq!(
            rows,
            vec![(50,)],
            "UPSERT with excluded.amount should produce 50, not double-encoded value"
        );

        // Bug 15: sequential UPSERTs must not progressively corrupt data
        conn.execute(
            "INSERT INTO t1 VALUES (1, 75) ON CONFLICT(id) DO UPDATE SET amount = excluded.amount",
        )
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
        assert_eq!(
            rows,
            vec![(75,)],
            "Sequential UPSERT should produce 75, not progressively corrupted value"
        );

        // Bug 13: WHERE clause in DO UPDATE must see decoded values
        conn.execute("INSERT INTO t1 VALUES (2, 10)").unwrap();
        conn.execute(
            "INSERT INTO t1 VALUES (2, 99) ON CONFLICT(id) DO UPDATE SET amount = excluded.amount WHERE t1.amount < 20",
        )
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 2");
        assert_eq!(
            rows,
            vec![(99,)],
            "WHERE clause should compare against decoded value (10 < 20 = true)"
        );

        // WHERE clause should block update when condition is false (99 < 20 is false)
        conn.execute(
            "INSERT INTO t1 VALUES (2, 5) ON CONFLICT(id) DO UPDATE SET amount = excluded.amount WHERE t1.amount < 20",
        )
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 2");
        assert_eq!(
            rows,
            vec![(99,)],
            "WHERE clause should block update when decoded value 99 >= 20"
        );

        // Complex expression: excluded.amount + t1.amount
        conn.execute("DELETE FROM t1").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();
        conn.execute(
            "INSERT INTO t1 VALUES (1, 8) ON CONFLICT(id) DO UPDATE SET amount = excluded.amount + t1.amount",
        )
        .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT amount FROM t1 WHERE id = 1");
        assert_eq!(
            rows,
            vec![(50,)],
            "excluded.amount (8) + t1.amount (42) should equal 50"
        );
    }

    /// Multi-row UPDATE must not progressively double-encode custom type
    /// values. Each row updated by a single UPDATE statement must receive
    /// exactly one encode pass, regardless of how many rows are affected.
    ///
    /// Previously, the encode expression wrote its result back to the same
    /// register that held the user's SET constant. Because the constant was
    /// hoisted before the loop, subsequent iterations read the already-encoded
    /// value and encoded it again, causing exponential corruption.
    #[test]
    fn test_multi_row_update_does_not_double_encode() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_multi_update.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();

        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 20)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 30)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (4, 40)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (5, 50)").unwrap();

        // UPDATE all rows with a constant value
        conn.execute("UPDATE t1 SET amount = 99").unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 ORDER BY id");
        assert_eq!(
            rows,
            vec![(1, 99), (2, 99), (3, 99), (4, 99), (5, 99)],
            "All rows must have amount=99 after UPDATE, not progressively double-encoded values"
        );

        // UPDATE with WHERE matching multiple rows
        conn.execute("UPDATE t1 SET amount = 42 WHERE id > 2")
            .unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 ORDER BY id");
        assert_eq!(
            rows,
            vec![(1, 99), (2, 99), (3, 42), (4, 42), (5, 42)],
            "WHERE-filtered multi-row UPDATE must encode each row exactly once"
        );

        // Multi-column UPDATE with different custom types
        conn.execute("CREATE TYPE score BASE integer ENCODE value * 10 DECODE value / 10")
            .unwrap();
        conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, a cents, b score) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 10, 5)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 20, 6)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3, 30, 7)").unwrap();

        conn.execute("UPDATE t2 SET a = 50, b = 8").unwrap();
        let rows: Vec<(i64, i64, i64)> = conn.exec_rows("SELECT id, a, b FROM t2 ORDER BY id");
        assert_eq!(
            rows,
            vec![(1, 50, 8), (2, 50, 8), (3, 50, 8)],
            "Multi-column UPDATE must encode each column independently and correctly"
        );
    }

    /// VACUUM INTO must work when custom types are defined. The destination
    /// database must contain the __turso_internal_types table and its data,
    /// and the vacuumed database must decode/encode correctly when reopened.
    #[test]
    fn test_vacuum_into_with_custom_types() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        // Create source database with custom type and data
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 100)").unwrap();

            // VACUUM INTO destination
            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        // Open the vacuumed database and verify
        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            // Data must be decoded correctly
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 ORDER BY id");
            assert_eq!(
                rows,
                vec![(1, 42), (2, 100)],
                "Vacuumed DB should return decoded values"
            );

            // Encoding must still work for new inserts
            conn.execute("INSERT INTO t1 VALUES (3, 55)").unwrap();
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t1 WHERE id = 3");
            assert_eq!(rows, vec![(3, 55)]);

            conn.close().unwrap();
        }
    }

    /// Regression test for #5898 covering the richer custom and array-backed
    /// types introduced by #5254 and #5729.
    #[test]
    fn test_vacuum_into_with_rich_builtin_and_array_types() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_rich_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_rich_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let source_arrays: Vec<(String, i64, String, i64, String, i64)>;

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute(
                "CREATE TABLE rich_types(
                    id INTEGER PRIMARY KEY,
                    amount cents,
                    flags boolean[],
                    balances cents[],
                    tags text[],
                    profile json,
                    profile_bin jsonb,
                    price numeric(10,2),
                    label varchar(16),
                    happened date,
                    payload bytea
                ) STRICT",
            )
            .unwrap();
            conn.execute(
                r#"INSERT INTO rich_types VALUES (
                    1,
                    42,
                    '{true,false,true}',
                    ARRAY[10, 20, 30],
                    ARRAY['alpha', 'beta'],
                    '{"role":"admin","active":true}',
                    '{"enabled":true,"count":2}',
                    '123.45',
                    'starter',
                    '2025-01-15',
                    X'DEADBEEF'
                )"#,
            )
            .unwrap();
            source_arrays = conn.exec_rows(
                "SELECT \
                    hex(flags), \
                    array_length(flags), \
                    hex(balances), \
                    array_length(balances), \
                    hex(tags), \
                    array_length(tags) \
                 FROM rich_types WHERE id = 1",
            );

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(String, i64, String, i64, String, i64)> = conn.exec_rows(
                "SELECT \
                    hex(flags), \
                    array_length(flags), \
                    hex(balances), \
                    array_length(balances), \
                    hex(tags), \
                    array_length(tags) \
                 FROM rich_types WHERE id = 1",
            );
            assert_eq!(rows, source_arrays);

            let rows: Vec<(i64, String, String, String, String)> = conn.exec_rows(
                "SELECT \
                    amount, \
                    json_extract(profile, '$.role'), \
                    CAST(json_extract(profile_bin, '$.enabled') AS TEXT), \
                    price, \
                    label \
                 FROM rich_types WHERE id = 1",
            );
            assert_eq!(
                rows,
                vec![(
                    42,
                    "admin".to_string(),
                    "1".to_string(),
                    "123.45".to_string(),
                    "starter".to_string(),
                )]
            );
            let rows: Vec<(String, String)> =
                conn.exec_rows("SELECT tags[1], happened FROM rich_types WHERE id = 1");
            assert_eq!(rows, vec![("alpha".to_string(), "2025-01-15".to_string())]);
            let rows: Vec<(String, String, String)> =
                conn.exec_rows("SELECT flags, balances, tags FROM rich_types WHERE id = 1");
            assert_eq!(
                rows,
                vec![(
                    "{1,0,1}".to_string(),
                    "{10,20,30}".to_string(),
                    "{alpha,beta}".to_string(),
                )]
            );
            let rows: Vec<(String, String)> =
                conn.exec_rows("SELECT hex(payload), hex(balances) FROM rich_types WHERE id = 1");
            assert_eq!(
                rows,
                vec![("DEADBEEF".to_string(), source_arrays[0].2.clone())]
            );

            conn.execute(
                r#"INSERT INTO rich_types VALUES (
                    2,
                    55,
                    ARRAY[false, true],
                    '{5, 15}',
                    '{"gamma","delta"}',
                    '{"role":"user"}',
                    '{"enabled":false}',
                    '5.50',
                    'followup',
                    '2026-02-03',
                    X'AA55'
                )"#,
            )
            .unwrap();

            let rows: Vec<(i64, i64, i64, String, String, String, String, String)> = conn
                .exec_rows(
                    "SELECT \
                        amount, \
                        array_length(flags), \
                        array_length(balances), \
                        tags[1], \
                        json_extract(profile, '$.role'), \
                        CAST(json_extract(profile_bin, '$.enabled') AS TEXT), \
                        price, \
                        label \
                     FROM rich_types WHERE id = 2",
                );
            assert_eq!(
                rows,
                vec![(
                    55,
                    2,
                    2,
                    "gamma".to_string(),
                    "user".to_string(),
                    "0".to_string(),
                    "5.50".to_string(),
                    "followup".to_string(),
                )]
            );
            let rows: Vec<(String,)> =
                conn.exec_rows("SELECT hex(payload) FROM rich_types WHERE id = 2");
            assert_eq!(rows, vec![("AA55".to_string(),)]);
            let rows: Vec<(String, String, String)> =
                conn.exec_rows("SELECT flags, balances, tags FROM rich_types WHERE id = 2");
            assert_eq!(
                rows,
                vec![(
                    "{0,1}".to_string(),
                    "{5,15}".to_string(),
                    "{gamma,delta}".to_string(),
                )]
            );
            let rows: Vec<(String, String, String)> = conn.exec_rows(
                "SELECT hex(flags), hex(balances), hex(tags) FROM rich_types WHERE id = 2",
            );
            assert_eq!(rows.len(), 1);
            assert!(!rows[0].0.is_empty());
            assert!(!rows[0].1.is_empty());
            assert!(!rows[0].2.is_empty());

            conn.close().unwrap();
        }
    }

    /// Regression coverage for partial-index predicates on custom types during
    /// VACUUM INTO. The copy path binds stored values directly, so the partial
    /// predicate must still evaluate with decoded custom-type semantics.
    #[test]
    fn test_vacuum_into_with_custom_type_partial_index() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_partial_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_partial_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute(
                "CREATE TYPE cents(value integer) BASE integer \
                 ENCODE value * 100 \
                 DECODE value / 100 \
                 OPERATOR '<'",
            )
            .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, amount cents) STRICT")
                .unwrap();
            conn.execute("CREATE INDEX idx_amount_partial ON t(amount) WHERE amount > 100")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 42), (2, 150), (3, 250)")
                .unwrap();

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(i64, i64)> =
                conn.exec_rows("SELECT id, amount FROM t WHERE amount > 100 ORDER BY amount");
            assert_eq!(rows, vec![(2, 150), (3, 250)]);

            let rows = conn.pragma_query("integrity_check").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0], Value::Text("ok".into()));

            let rows: Vec<(String,)> =
                conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'idx_amount_partial'");
            assert_eq!(rows.len(), 1);
            assert!(rows[0].0.contains("WHERE amount > 100"));

            conn.close().unwrap();
        }
    }

    /// Regression coverage for VACUUM INTO with array edge cases whose element
    /// type goes through the custom-type transform path: empty arrays, NULL
    /// arrays, and arrays containing NULL elements.
    #[test]
    fn test_vacuum_into_with_custom_type_array_edge_cases() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_array_edges_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_array_edges_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let source_rows: Vec<(i64, String, String, String, String, String, String)>;

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute(
                "CREATE TABLE t(
                    id INTEGER PRIMARY KEY,
                    flags boolean[],
                    balances cents[],
                    tags text[]
                ) STRICT",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO t VALUES
                    (1, ARRAY[true, false, true], ARRAY[10, 20, 30], ARRAY['alpha', 'beta']),
                    (2, '{}', '{}', '{}'),
                    (3, ARRAY[true, NULL, false], ARRAY[10, NULL, 30], ARRAY['alpha', NULL, 'omega']),
                    (4, NULL, NULL, NULL)",
            )
            .unwrap();

            source_rows = conn.exec_rows(
                "SELECT
                    id,
                    COALESCE(hex(flags), 'NULL'),
                    COALESCE(CAST(array_length(flags) AS TEXT), 'NULL'),
                    COALESCE(hex(balances), 'NULL'),
                    COALESCE(CAST(array_length(balances) AS TEXT), 'NULL'),
                    COALESCE(hex(tags), 'NULL'),
                    COALESCE(CAST(array_length(tags) AS TEXT), 'NULL')
                 FROM t
                 ORDER BY id",
            );

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(i64, String, String, String, String, String, String)> = conn.exec_rows(
                "SELECT
                    id,
                    COALESCE(hex(flags), 'NULL'),
                    COALESCE(CAST(array_length(flags) AS TEXT), 'NULL'),
                    COALESCE(hex(balances), 'NULL'),
                    COALESCE(CAST(array_length(balances) AS TEXT), 'NULL'),
                    COALESCE(hex(tags), 'NULL'),
                    COALESCE(CAST(array_length(tags) AS TEXT), 'NULL')
                 FROM t
                 ORDER BY id",
            );
            assert_eq!(rows, source_rows);

            let rows: Vec<(String, String, String, String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(CAST(array_length(flags) AS TEXT), 'NULL'),
                    COALESCE(CAST(flags[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(array_length(balances) AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(array_length(tags) AS TEXT), 'NULL'),
                    COALESCE(tags[1], 'NULL')
                 FROM t
                 WHERE id = 2",
            );
            assert_eq!(
                rows,
                vec![(
                    "0".to_string(),
                    "NULL".to_string(),
                    "0".to_string(),
                    "NULL".to_string(),
                    "0".to_string(),
                    "NULL".to_string(),
                )]
            );

            let rows: Vec<(String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(CAST(flags[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(flags[2] AS TEXT), 'NULL'),
                    COALESCE(CAST(flags[3] AS TEXT), 'NULL')
                 FROM t
                 WHERE id = 3",
            );
            assert_eq!(
                rows,
                vec![("1".to_string(), "NULL".to_string(), "0".to_string(),)]
            );

            let rows: Vec<(String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(CAST(balances[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[2] AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[3] AS TEXT), 'NULL')
                 FROM t
                 WHERE id = 3",
            );
            assert_eq!(
                rows,
                vec![("10".to_string(), "NULL".to_string(), "30".to_string(),)]
            );

            let rows: Vec<(String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(tags[1], 'NULL'),
                    COALESCE(tags[2], 'NULL'),
                    COALESCE(tags[3], 'NULL')
                 FROM t
                 WHERE id = 3",
            );
            assert_eq!(
                rows,
                vec![("alpha".to_string(), "NULL".to_string(), "omega".to_string(),)]
            );

            let rows: Vec<(String, String, String, String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(CAST(array_length(flags) AS TEXT), 'NULL'),
                    COALESCE(CAST(flags[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(array_length(balances) AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(array_length(tags) AS TEXT), 'NULL'),
                    COALESCE(tags[1], 'NULL')
                 FROM t
                 WHERE id = 4",
            );
            assert_eq!(
                rows,
                vec![(
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                )]
            );

            conn.execute(
                "INSERT INTO t VALUES (
                    5,
                    ARRAY[false, true],
                    ARRAY[5, NULL, 15],
                    ARRAY['late', NULL, 'entry']
                )",
            )
            .unwrap();
            let rows: Vec<(String, String, String)> =
                conn.exec_rows("SELECT flags, balances, tags FROM t WHERE id = 5");
            assert_eq!(
                rows,
                vec![(
                    "{0,1}".to_string(),
                    "{5,NULL,15}".to_string(),
                    "{late,NULL,entry}".to_string(),
                )]
            );
            let rows: Vec<(String, String, String)> = conn.exec_rows(
                "SELECT
                    COALESCE(CAST(balances[1] AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[2] AS TEXT), 'NULL'),
                    COALESCE(CAST(balances[3] AS TEXT), 'NULL')
                 FROM t
                 WHERE id = 5",
            );
            assert_eq!(
                rows,
                vec![("5".to_string(), "NULL".to_string(), "15".to_string(),)]
            );

            conn.close().unwrap();
        }
    }

    /// Regression coverage for expression indexes on custom type values during
    /// VACUUM INTO. The destination insert path must preserve stored values
    /// while expression-index maintenance still observes decoded semantics.
    #[test]
    fn test_vacuum_into_with_custom_type_expression_index() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_expr_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_expr_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute(
                "CREATE TYPE cents(value integer) BASE integer \
                 ENCODE value * 100 \
                 DECODE value / 100 \
                 OPERATOR '<'",
            )
            .unwrap();
            conn.execute(
                "CREATE TABLE t(
                    id INTEGER PRIMARY KEY,
                    amount cents,
                    label TEXT
                ) STRICT",
            )
            .unwrap();
            conn.execute("CREATE INDEX idx_amount_expr ON t(amount + 5)")
                .unwrap();
            conn.execute(
                "INSERT INTO t VALUES
                    (1, 42, 'starter'),
                    (2, 150, 'mid'),
                    (3, 250, 'large')",
            )
            .unwrap();

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(i64, i64, String)> = conn
                .exec_rows("SELECT id, amount, label FROM t WHERE amount + 5 >= 155 ORDER BY id");
            assert_eq!(
                rows,
                vec![(2, 150, "mid".to_string()), (3, 250, "large".to_string()),]
            );

            let rows = conn.pragma_query("integrity_check").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0], Value::Text("ok".into()));

            let rows: Vec<(String,)> =
                conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'idx_amount_expr'");
            assert_eq!(rows.len(), 1);
            assert!(rows[0].0.contains("amount + 5"));

            conn.execute("INSERT INTO t VALUES (4, 195, 'post-vacuum')")
                .unwrap();
            let rows: Vec<(i64,)> =
                conn.exec_rows("SELECT id FROM t WHERE amount + 5 = 200 ORDER BY id");
            assert_eq!(rows, vec![(4,)]);

            conn.close().unwrap();
        }
    }

    /// Custom-type array element access should decode individual elements the
    /// same way full-array reads decode the whole value.
    #[test]
    fn test_custom_type_array_element_decode_semantics() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_array_element_decode.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();

        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute(
            "CREATE TABLE t(
                id INTEGER PRIMARY KEY,
                flags boolean[],
                balances cents[],
                tags text[]
            ) STRICT",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t VALUES
                (1, ARRAY[true, NULL, false], ARRAY[10, NULL, 30], ARRAY['alpha', NULL, 'omega'])",
        )
        .unwrap();

        let rows: Vec<(String, String, String)> = conn.exec_rows(
            "SELECT
                COALESCE(CAST(flags[1] AS TEXT), 'NULL'),
                COALESCE(CAST(flags[2] AS TEXT), 'NULL'),
                COALESCE(CAST(flags[3] AS TEXT), 'NULL')
             FROM t
             WHERE id = 1",
        );
        assert_eq!(
            rows,
            vec![("1".to_string(), "NULL".to_string(), "0".to_string(),)]
        );

        let rows: Vec<(String, String, String)> = conn.exec_rows(
            "SELECT
                COALESCE(CAST(balances[1] AS TEXT), 'NULL'),
                COALESCE(CAST(balances[2] AS TEXT), 'NULL'),
                COALESCE(CAST(balances[3] AS TEXT), 'NULL')
             FROM t
             WHERE id = 1",
        );
        assert_eq!(
            rows,
            vec![("10".to_string(), "NULL".to_string(), "30".to_string(),)]
        );

        let rows: Vec<(String, String, String)> = conn.exec_rows(
            "SELECT
                COALESCE(tags[1], 'NULL'),
                COALESCE(tags[2], 'NULL'),
                COALESCE(tags[3], 'NULL')
             FROM t
             WHERE id = 1",
        );
        assert_eq!(
            rows,
            vec![("alpha".to_string(), "NULL".to_string(), "omega".to_string(),)]
        );

        let rows: Vec<(String, String, String)> = conn.exec_rows(
            "SELECT
                COALESCE(CAST(array_element(COALESCE(balances, '{}'), 1) AS TEXT), 'NULL'),
                COALESCE(CAST(array_element(COALESCE(balances, '{}'), 2) AS TEXT), 'NULL'),
                COALESCE(CAST(array_element(COALESCE(balances, '{}'), 3) AS TEXT), 'NULL')
             FROM t
             WHERE id = 1",
        );
        assert_eq!(
            rows,
            vec![("10".to_string(), "NULL".to_string(), "30".to_string(),)]
        );
    }

    /// VACUUM INTO must preserve multiple custom types in the destination so
    /// later reopens, extra connections, schema changes, and failed writes all
    /// behave like the source database.
    #[test]
    fn test_vacuum_into_with_multiple_custom_types_reopen_and_negative_writes() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_multi_reopen_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_multi_reopen_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TYPE slug BASE text ENCODE lower(value) DECODE value")
                .unwrap();
            conn.execute(
                "CREATE TABLE catalog(
                    id INTEGER PRIMARY KEY,
                    price cents,
                    code slug
                ) STRICT",
            )
            .unwrap();
            conn.execute("INSERT INTO catalog VALUES (1, 42, 'Starter')")
                .unwrap();

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn1 = dest_db.connect_limbo();

            let rows: Vec<(i64, String)> =
                conn1.exec_rows("SELECT price, code FROM catalog ORDER BY id");
            assert_eq!(rows, vec![(42, "starter".to_string())]);

            assert!(
                conn1
                    .execute("INSERT INTO catalog VALUES (2, 'oops', 'beta')")
                    .is_err(),
                "text into integer-backed custom type should fail after vacuum"
            );
            assert!(
                conn1
                    .execute("INSERT INTO catalog VALUES (2, 55, X'AA')")
                    .is_err(),
                "blob into text-backed custom type should fail after vacuum"
            );

            conn1
                .execute(
                    "CREATE TABLE catalog_extra(
                        id INTEGER PRIMARY KEY,
                        price cents,
                        code slug
                    ) STRICT",
                )
                .unwrap();
            conn1
                .execute("INSERT INTO catalog_extra VALUES (1, 75, 'Extra')")
                .unwrap();

            let conn2 = dest_db.connect_limbo();
            let rows: Vec<(i64, String)> =
                conn2.exec_rows("SELECT price, code FROM catalog_extra ORDER BY id");
            assert_eq!(rows, vec![(75, "extra".to_string())]);
            conn2
                .execute("INSERT INTO catalog VALUES (2, 55, 'FollowUp')")
                .unwrap();

            let rows: Vec<(i64, String)> =
                conn1.exec_rows("SELECT price, code FROM catalog ORDER BY id");
            assert_eq!(
                rows,
                vec![(42, "starter".to_string()), (55, "followup".to_string()),]
            );
            conn1.close().unwrap();
            conn2.close().unwrap();
        }

        {
            let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = dest_db.connect_limbo();

            let rows: Vec<(i64, String)> =
                conn.exec_rows("SELECT price, code FROM catalog ORDER BY id");
            assert_eq!(
                rows,
                vec![(42, "starter".to_string()), (55, "followup".to_string()),]
            );
            let rows: Vec<(i64, String)> =
                conn.exec_rows("SELECT price, code FROM catalog_extra ORDER BY id");
            assert_eq!(rows, vec![(75, "extra".to_string())]);
            conn.close().unwrap();
        }
    }

    /// VACUUM INTO must preserve unique and composite indexes that depend on
    /// custom-type comparison semantics.
    #[test]
    fn test_vacuum_into_with_custom_type_unique_and_composite_indexes() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_unique_composite_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_unique_composite_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute(
                "CREATE TYPE cents(value integer) BASE integer \
                 ENCODE value * 100 \
                 DECODE value / 100 \
                 OPERATOR '<'",
            )
            .unwrap();
            conn.execute(
                "CREATE TABLE sales(
                    id INTEGER PRIMARY KEY,
                    label TEXT,
                    amount cents
                ) STRICT",
            )
            .unwrap();
            conn.execute("CREATE UNIQUE INDEX idx_sales_unique ON sales(amount, label)")
                .unwrap();
            conn.execute("CREATE INDEX idx_sales_label_amount ON sales(label, amount)")
                .unwrap();
            conn.execute(
                "INSERT INTO sales VALUES
                    (1, 'starter', 42),
                    (2, 'mid', 150),
                    (3, 'large', 250)",
            )
            .unwrap();

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(String, i64)> = conn
                .exec_rows("SELECT label, amount FROM sales WHERE amount >= 150 ORDER BY amount");
            assert_eq!(
                rows,
                vec![("mid".to_string(), 150), ("large".to_string(), 250),]
            );

            let rows = conn.pragma_query("integrity_check").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0], Value::Text("ok".into()));

            let rows: Vec<(String,)> = conn.exec_rows(
                "SELECT name FROM sqlite_schema \
                 WHERE type = 'index' AND name LIKE 'idx_sales_%' ORDER BY name",
            );
            assert_eq!(
                rows,
                vec![
                    ("idx_sales_label_amount".to_string(),),
                    ("idx_sales_unique".to_string(),),
                ]
            );

            assert!(
                conn.execute("INSERT INTO sales VALUES (4, 'mid', 150)")
                    .is_err(),
                "unique custom-type index should reject duplicates after vacuum"
            );
            conn.execute("INSERT INTO sales VALUES (4, 'repeat', 150)")
                .unwrap();

            let rows: Vec<(i64,)> = conn.exec_rows(
                "SELECT id FROM sales WHERE label = 'repeat' AND amount = 150 ORDER BY id",
            );
            assert_eq!(rows, vec![(4,)]);
            conn.close().unwrap();
        }
    }

    /// VACUUM INTO recreates views and triggers after the data copy. Custom
    /// type values referenced there must still use decoded semantics.
    #[test]
    fn test_vacuum_into_with_custom_type_views_and_triggers() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_vacuum_views_triggers_src.db");
        let dest_path = path.with_file_name("custom_types_vacuum_views_triggers_dest.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();

            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute(
                "CREATE TABLE invoices(
                    id INTEGER PRIMARY KEY,
                    amount cents,
                    note TEXT
                ) STRICT",
            )
            .unwrap();
            conn.execute("CREATE TABLE audit_log(entry TEXT)").unwrap();
            conn.execute(
                "CREATE VIEW expensive_invoices AS \
                 SELECT id, amount, note FROM invoices WHERE amount > 100",
            )
            .unwrap();
            conn.execute(
                "CREATE TRIGGER log_invoice AFTER INSERT ON invoices BEGIN
                    INSERT INTO audit_log VALUES ('invoice:' || CAST(NEW.amount AS TEXT));
                END",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO invoices VALUES
                    (1, 42, 'starter'),
                    (2, 150, 'mid')",
            )
            .unwrap();

            conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
            let conn = db.connect_limbo();

            let rows: Vec<(i64, i64, String)> =
                conn.exec_rows("SELECT id, amount, note FROM expensive_invoices ORDER BY id");
            assert_eq!(rows, vec![(2, 150, "mid".to_string())]);

            let rows: Vec<(String,)> = conn.exec_rows("SELECT entry FROM audit_log ORDER BY entry");
            assert_eq!(
                rows,
                vec![("invoice:150".to_string(),), ("invoice:42".to_string(),)]
            );

            conn.execute("INSERT INTO invoices VALUES (3, 175, 'after vacuum')")
                .unwrap();
            let rows: Vec<(String,)> = conn.exec_rows("SELECT entry FROM audit_log ORDER BY entry");
            assert_eq!(
                rows,
                vec![
                    ("invoice:150".to_string(),),
                    ("invoice:175".to_string(),),
                    ("invoice:42".to_string(),),
                ]
            );

            let rows: Vec<(String,)> = conn.exec_rows(
                "SELECT name FROM sqlite_schema \
                 WHERE type IN ('view', 'trigger') ORDER BY type, name",
            );
            assert_eq!(
                rows,
                vec![
                    ("log_invoice".to_string(),),
                    ("expensive_invoices".to_string(),),
                ]
            );

            conn.close().unwrap();
        }
    }

    /// Self-joins on custom type columns must return matching rows.
    ///
    /// The optimizer builds an ephemeral auto-index for the inner table.
    /// The auto-index stores raw encoded values, so the seek key built from
    /// the outer table must also be encoded.  Previously, the seek-key
    /// encoder could not find the table metadata because it searched by
    /// alias (e.g. "b") while the index stored the base table name ("t1"),
    /// causing a seek-key / index-key mismatch and returning no rows.
    #[test]
    fn test_self_join_on_custom_type_column() {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("custom_types_self_join.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();

        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 20)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 10)").unwrap();

        // Self-join: rows with equal decoded amounts must match
        let mut rows: Vec<(i64, i64)> =
            conn.exec_rows("SELECT a.id, b.id FROM t1 a, t1 b WHERE a.amount = b.amount");
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, 1), (1, 3), (2, 2), (3, 1), (3, 3)],
            "Self-join on custom type column should return matching rows"
        );

        // LEFT JOIN variant: unmatched rows should produce NULLs
        conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, amount cents) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 20)").unwrap();

        let rows: Vec<(i64, String)> = conn.exec_rows(
            "SELECT t1.id, COALESCE(CAST(t2.id AS TEXT), 'NULL') \
             FROM t1 LEFT JOIN t2 ON t1.amount = t2.amount ORDER BY t1.id",
        );
        assert_eq!(
            rows,
            vec![
                (1, "1".to_string()),
                (2, "2".to_string()),
                (3, "1".to_string()),
            ],
            "LEFT JOIN on custom type column should find matches and produce NULLs for non-matches"
        );
    }
}
