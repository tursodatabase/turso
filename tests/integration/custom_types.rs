#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use asserting::prelude::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_uuid_invalid_update_and_upsert_preserve_rows() {
        for mvcc in [false, true] {
            let opts = turso_core::DatabaseOpts::new()
                .with_custom_types(true)
                .with_encryption(true);
            let db = TempDatabase::builder()
                .with_opts(opts)
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();
            let mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
            assert_eq!(mode, vec![(if mvcc { "mvcc" } else { "wal" }.to_string(),)]);

            let uuid = "01945ca0-3189-76c0-9a8f-caf310fc8b8e";
            conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b uuid) STRICT")
                .unwrap();
            conn.execute(format!("INSERT INTO t1 VALUES (1, '{uuid}')"))
                .unwrap();

            for sql in [
                "UPDATE t1 SET b = 42 WHERE a = 1",
                "INSERT INTO t1 VALUES (1, '01945ca0-3189-76c0-9a8f-caf310fc8b8e') \
                 ON CONFLICT(a) DO UPDATE SET b = 42",
            ] {
                assert_that!(conn.execute(sql))
                    .err()
                    .display_string()
                    .contains("invalid UUID value");
                let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t1 ORDER BY a");
                assert_eq!(rows, vec![(1, uuid.to_string())], "mvcc={mvcc}, {sql}");
            }
            conn.close().unwrap();
        }
    }

    #[test]
    fn test_uuid_invalid_multirow_writes_preserve_transaction() {
        for mvcc in [false, true] {
            let opts = turso_core::DatabaseOpts::new()
                .with_custom_types(true)
                .with_encryption(true);
            let db = TempDatabase::builder()
                .with_opts(opts)
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();
            let mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
            assert_eq!(mode, vec![(if mvcc { "mvcc" } else { "wal" }.to_string(),)]);

            let uuid = "01945ca0-3189-76c0-9a8f-caf310fc8b8e";
            conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b uuid) STRICT")
                .unwrap();
            conn.execute(format!(
                "INSERT INTO t1 VALUES (1, '{uuid}'), (2, '{uuid}')"
            ))
            .unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute(format!("INSERT INTO t1 VALUES (3, '{uuid}')"))
                .unwrap();
            let expected = vec![
                (1, uuid.to_string()),
                (2, uuid.to_string()),
                (3, uuid.to_string()),
            ];

            for sql in [
                "INSERT INTO t1 VALUES (4, '01945ca0-3189-76c0-9a8f-caf310fc8b8e'), (5, 42)",
                "UPDATE t1 SET b = CASE WHEN a = 1 THEN '550e8400-e29b-41d4-a716-446655440000' \
                 ELSE 42 END WHERE a < 3",
            ] {
                assert_that!(conn.execute(sql))
                    .err()
                    .display_string()
                    .contains("invalid UUID value");
                let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t1 ORDER BY a");
                assert_eq!(rows, expected, "mvcc={mvcc}, {sql}");
            }
            conn.execute("COMMIT").unwrap();
            let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t1 ORDER BY a");
            assert_eq!(rows, expected, "mvcc={mvcc}, after COMMIT");
            conn.close().unwrap();
        }
    }

    /// Custom types must be loaded from __turso_internal_types when reopening
    /// a database. Without this, SELECT returns raw encoded values and PRAGMA
    /// list_types omits user-defined types.
    #[test]
    fn test_custom_types_persist_across_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_reopen.db");
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

    #[test]
    fn test_reopen_fails_when_stored_custom_type_does_not_parse() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_bad_type_sql.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TYPE zz BASE text ENCODE upper(value) DECODE lower(value)")
                .unwrap();
            conn.execute("CREATE TABLE t(a cents, b zz) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (5, 'Hi')").unwrap();

            conn.start_nested();
            let stmt = conn.prepare(
                "UPDATE __turso_internal_types SET sql = 'CREATE TYPE cents BAZE integer' \
                 WHERE name = 'cents'",
            );
            conn.end_nested();
            stmt.unwrap().run_ignore_rows().unwrap();

            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.close().unwrap();
        }

        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        let result = turso_core::Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts,
            None,
            Arc::new(turso_core::SqliteDialect),
        );
        let Err(err) = result else {
            panic!("open must fail when a stored type does not parse");
        };
        assert!(
            err.to_string().contains("malformed database schema"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_reparse_fails_when_stored_custom_type_does_not_parse() {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE TABLE t(a cents) STRICT").unwrap();

        conn.start_nested();
        let stmt = conn.prepare(
            "UPDATE __turso_internal_types SET sql = 'CREATE TYPE cents BAZE integer' \
             WHERE name = 'cents'",
        );
        conn.end_nested();
        stmt.unwrap().run_ignore_rows().unwrap();

        assert_that!(conn.execute("VACUUM"))
            .err()
            .display_string()
            .contains("malformed database schema");
    }

    /// After reopening, schema changes (CREATE TABLE) that use a previously
    /// defined custom type must still work. The type must survive the schema
    /// change and new tables must encode/decode correctly.
    #[test]
    fn test_custom_types_survive_schema_change_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_schema_change.db");
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

    #[test]
    fn test_add_column_with_non_literal_type_default_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_add_column_default.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_encryption(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE DOMAIN dm AS text DEFAULT lower('X')")
                .unwrap();
            conn.execute("CREATE TYPE ty BASE text ENCODE value DECODE value DEFAULT lower('Y')")
                .unwrap();
            conn.execute("CREATE TABLE t(a INTEGER) STRICT").unwrap();
            conn.execute("ALTER TABLE t ADD COLUMN b dm").unwrap();
            conn.execute("ALTER TABLE t ADD COLUMN c ty").unwrap();
            let rows: Vec<(String,)> =
                conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
            assert_eq!(
                rows,
                vec![(
                    "CREATE TABLE t (a INTEGER, b dm DEFAULT (lower ('X')), c ty DEFAULT (lower ('Y'))) STRICT"
                        .to_string(),
                )]
            );
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("INSERT INTO t(a) VALUES (1)").unwrap();
            let rows: Vec<(i64, String, String)> = conn.exec_rows("SELECT a, b, c FROM t");
            assert_eq!(rows, vec![(1, "x".to_string(), "y".to_string())]);
            conn.close().unwrap();
        }
    }

    /// A new connection on the same database must see custom types that were
    /// created by another connection, even without reopening the database file.
    #[test]
    fn test_new_connection_sees_custom_types() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_new_conn.db");
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
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_upsert.db");
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
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_multi_update.db");
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
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_vacuum_src.db");
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
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_self_join.db");
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

    #[test]
    fn test_date_plus_and_minus_bound_integer() {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t1(d date) STRICT").unwrap();
        conn.execute("INSERT INTO t1 VALUES ('2025-01-05')")
            .unwrap();

        let mut stmt = conn.prepare("SELECT d + ?1, d - ?1 FROM t1").unwrap();
        stmt.bind_at(1.try_into().unwrap(), turso_core::Value::from_i64(3))
            .unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push((row.get::<String>(0)?, row.get::<String>(1)?));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            rows,
            vec![("2025-01-08".to_string(), "2025-01-02".to_string())]
        );
    }

    /// The fixture was written by tursodb v0.9.0-pre.1, before databases
    /// recorded which version of the custom-type rules built their indexes:
    ///
    /// ```sql
    /// CREATE TABLE t(id INTEGER PRIMARY KEY, val numeric(10,2),
    ///                g ANY AS (val * 3) VIRTUAL) STRICT;
    /// CREATE INDEX t_expr ON t((val * 2));
    /// CREATE INDEX t_partial ON t(id) WHERE val * 2 > 2;
    /// CREATE INDEX t_gen ON t(g);
    /// CREATE INDEX t_plain ON t(val);
    /// CREATE TABLE u(id INTEGER PRIMARY KEY);
    /// CREATE TRIGGER u_deletes_t AFTER INSERT ON u
    ///   BEGIN DELETE FROM t WHERE id = NEW.id; END;
    /// INSERT INTO t(id, val) VALUES (1, 1.50), (2, 0.25), (3, 2.10);
    /// ```
    ///
    /// All four indexes on `t` store values that depend on the custom-type
    /// rules, so they must be rebuilt before use.
    fn open_database_with_indexes_built_before_rules_version(dir: &TempDir) -> TempDatabase {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("integration/fixtures/custom_type_indexes_v0.9.0-pre.1.db");
        let path = dir.path().join("old_indexes.db");
        std::fs::copy(&fixture, &path).unwrap();
        TempDatabase::builder()
            .with_db_path(&path)
            .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
            .build()
    }

    fn prepare_error(conn: &Arc<turso_core::Connection>, sql: &str) -> String {
        match conn.prepare(sql) {
            Ok(_) => panic!("{sql} must fail"),
            Err(err) => err.to_string(),
        }
    }

    const VERSION_TABLES: &str = "SELECT name FROM sqlite_schema \
                                  WHERE name LIKE '__turso_internal_custom_type_rules_v%' \
                                  ORDER BY name";

    #[test]
    fn test_index_built_before_rules_version_is_refused_until_reindex() {
        let dir = TempDir::new().unwrap();
        let db = open_database_with_indexes_built_before_rules_version(&dir);
        let conn = db.connect_limbo();

        // Statements that open no old index work.
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
        assert_eq!(rows, vec![(0,)]);
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t NOT INDEXED");
        assert_eq!(rows, vec![(3,)]);

        let refused = "indexes built by an older version of Turso's experimental custom types \
                       must be rebuilt before use: t_expr, t_gen, t_partial, t_plain. Run REINDEX";
        for sql in [
            "SELECT id FROM t WHERE val * 2 > 1",
            "SELECT id FROM t INDEXED BY t_gen WHERE g > 1",
            "SELECT id FROM t INDEXED BY t_plain WHERE val > 1",
            "DELETE FROM t WHERE id = 1",
            "UPDATE t SET val = 5 WHERE id = 1",
            "INSERT INTO t(id, val) VALUES (4, 1)",
            // The trigger's DELETE opens the old indexes.
            "INSERT INTO u VALUES (1)",
            "PRAGMA integrity_check",
        ] {
            let err = prepare_error(&conn, sql);
            assert!(err.contains(refused), "{sql}: {err}");
        }

        // An index created now is built under the current rules, but the
        // database still holds old ones, so it waits for REINDEX with them.
        conn.execute("CREATE INDEX t_new ON t((val + 1))").unwrap();
        let err = prepare_error(&conn, "SELECT id FROM t INDEXED BY t_new WHERE val + 1 > 0");
        assert!(
            err.contains("t_gen, t_new, t_partial, t_plain. Run REINDEX"),
            "{err}"
        );
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(rows, vec![]);

        // REINDEX of one index rebuilds every old index.
        conn.execute("REINDEX t_expr").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(
            rows,
            vec![("__turso_internal_custom_type_rules_v1".to_string(),)]
        );
        conn.execute("DELETE FROM t WHERE id = 1").unwrap();
        conn.execute("INSERT INTO u VALUES (2)").unwrap();
        conn.execute("UPDATE t SET val = 0.75 WHERE id = 3")
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t INDEXED BY t_expr ORDER BY id");
        assert_eq!(rows, vec![(3,)]);
        let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(rows, vec![("ok".to_string(),)]);
        conn.close().unwrap();

        let conn = db.connect_limbo();
        conn.execute("DELETE FROM t WHERE id = 3").unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(0,)]);
    }

    /// MVCC has no REINDEX, so the old indexes are dropped and created again.
    #[test]
    fn test_index_built_before_rules_version_can_be_created_again_in_mvcc() {
        let dir = TempDir::new().unwrap();
        let db = open_database_with_indexes_built_before_rules_version(&dir);
        let conn = db.connect_limbo();
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();

        let err = prepare_error(&conn, "DELETE FROM t WHERE id = 1");
        assert!(err.contains("must be rebuilt before use"), "{err}");

        for index in ["t_expr", "t_partial", "t_gen", "t_plain"] {
            conn.execute(format!("DROP INDEX {index}")).unwrap();
        }
        conn.execute("CREATE INDEX t_expr ON t((val * 2))").unwrap();
        conn.execute("CREATE INDEX t_partial ON t(id) WHERE val * 2 > 2")
            .unwrap();
        conn.execute("CREATE INDEX t_gen ON t(g)").unwrap();
        conn.execute("DELETE FROM t WHERE id = 1").unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t INDEXED BY t_expr ORDER BY id");
        assert_eq!(rows, vec![(2,), (3,)]);
    }

    /// The statement that gives a database its first index that depends on
    /// the rules marks it with the rules version. Plain tables and indexes
    /// leave the schema as SQLite would have it.
    #[test]
    fn test_new_database_records_rules_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_indexes.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(rows, vec![]);
        conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY, x integer UNIQUE) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX p_x ON p(x + 1)").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(rows, vec![]);

        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val numeric(10,2) UNIQUE, \
             g ANY AS (val * 2) VIRTUAL) STRICT",
        )
        .unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(
            rows,
            vec![("__turso_internal_custom_type_rules_v1".to_string(),)]
        );
        conn.execute("INSERT INTO t(id, val) VALUES (1, 1.5)")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN w numeric(10,2)")
            .unwrap();
        conn.execute("CREATE INDEX t_w ON t((w + 1))").unwrap();
        conn.close().unwrap();

        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO t(id, val, w) VALUES (2, 2.5, 1)")
            .unwrap();
        conn.execute("DELETE FROM t WHERE id = 1").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(rows, vec![("ok".to_string(),)]);
    }

    /// Partial, expression and generated-column indexes over plain columns
    /// store nothing the custom-type rules decide, so a database built by
    /// SQLite is used as it is.
    #[test]
    fn test_plain_indexes_need_no_rules_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("plain.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER,
                                g AS (b * 2) VIRTUAL, UNIQUE(a, b));
                 CREATE INDEX t_partial ON t(a) WHERE b > 1;
                 CREATE INDEX t_expr ON t(lower(a), b + 1);
                 CREATE INDEX t_gen ON t(g);
                 INSERT INTO t(id, a, b) VALUES (1, 'X', 1), (2, 'Y', 2);",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t INDEXED BY t_gen WHERE g > 2");
        assert_eq!(rows, vec![(2,)]);
        conn.execute("DELETE FROM t WHERE lower(a) = 'x' AND b + 1 = 2")
            .unwrap();
        conn.execute("CREATE INDEX t_more ON t(a || 'z') WHERE g > 0")
            .unwrap();
        conn.execute("REINDEX").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(rows, vec![]);
    }

    /// Whether an index depends on the rules is read from the schema's text,
    /// not from the types defined now: a CAST to a name that isn't one of
    /// SQLite's own types may mean a custom type, now or after CREATE TYPE.
    #[test]
    fn test_cast_to_unknown_type_depends_on_rules() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cast_unknown.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE q(id INTEGER PRIMARY KEY, x);
                 CREATE INDEX q_money ON q(CAST(x AS money));
                 CREATE INDEX q_int ON q(CAST(x AS integer));",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let rows: Vec<(i64,)> =
            conn.exec_rows("SELECT id FROM q INDEXED BY q_int WHERE CAST(x AS integer) = 1");
        assert_eq!(rows, vec![]);
        let err = prepare_error(&conn, "INSERT INTO q VALUES (1, 1)");
        assert!(err.contains("q_money") && !err.contains("q_int"), "{err}");
    }

    /// A connection without custom types would compute index keys with
    /// SQLite's rules, so it doesn't touch a database whose indexes depend
    /// on the custom-type rules, and doesn't rebuild them either.
    #[test]
    fn test_connection_without_custom_types_refuses_marked_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flag_off.db");
        {
            let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY, x)")
                .unwrap();
            conn.execute("CREATE INDEX p_cast ON p(CAST(x AS numeric(10,2)))")
                .unwrap();
            conn.close().unwrap();
        }

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        let refused = "depend on Turso's experimental custom types. Enable custom types";
        for sql in [
            "INSERT INTO p VALUES (1, 2.5)",
            "SELECT x FROM p",
            "REINDEX",
        ] {
            let err = prepare_error(&conn, sql);
            assert!(err.contains(refused), "{sql}: {err}");
        }
    }

    /// SQLite doesn't have string_reverse, so an index using it was built by
    /// Turso, and Turso's own functions may follow the custom-type rules.
    #[test]
    fn test_index_on_turso_function_depends_on_rules() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("turso_function.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
                 CREATE INDEX t_rev ON t(upper(a));
                 PRAGMA writable_schema = ON;
                 UPDATE sqlite_schema SET sql = replace(sql, 'upper', 'string_reverse')
                     WHERE name = 't_rev';",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let err = prepare_error(&conn, "INSERT INTO t VALUES (1, 'abc')");
        assert!(err.contains("t_rev"), "{err}");
        conn.execute("REINDEX").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
        let rows: Vec<(i64,)> =
            conn.exec_rows("SELECT id FROM t INDEXED BY t_rev WHERE string_reverse(a) = 'cba'");
        assert_eq!(rows, vec![(1,)]);
    }

    /// REGEXP calls a function that SQLite doesn't have.
    #[test]
    fn test_index_on_regexp_depends_on_rules() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("regexp.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
                 CREATE INDEX t_re ON t(a GLOB 'x*');
                 PRAGMA writable_schema = ON;
                 UPDATE sqlite_schema SET sql = replace(sql, 'GLOB', 'REGEXP')
                     WHERE name = 't_re';",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let err = prepare_error(&conn, "INSERT INTO t VALUES (1, 'abc')");
        assert!(err.contains("t_re"), "{err}");
    }

    /// REINDEX can't rebuild an index on a WITHOUT ROWID table, so it says
    /// to drop the index.
    #[test]
    fn test_old_index_on_without_rowid_table_must_be_dropped() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("without_rowid.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE w(k PRIMARY KEY, v) WITHOUT ROWID;
                 CREATE INDEX w_money ON w(CAST(v AS money));",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let err = prepare_error(&conn, "REINDEX");
        assert!(
            err.contains("index w_money was built")
                && err.contains("REINDEX doesn't support WITHOUT ROWID tables. Drop the index"),
            "{err}"
        );
        conn.execute("DROP INDEX w_money").unwrap();
        conn.execute("INSERT INTO w VALUES (1, 2)").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(rows, vec![]);
    }

    /// ALTER TABLE changes the schema, so in a database built by SQLite it
    /// marks the database before the rebuilt index depends on the rules.
    #[test]
    fn test_alter_column_to_custom_type_records_rules_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("alter.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT) STRICT;
                 CREATE INDEX t_a ON t(a);
                 INSERT INTO t VALUES (1, '1.5');",
            )
            .unwrap();
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        conn.execute("ALTER TABLE t ALTER COLUMN a TO b numeric(10,2)")
            .unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(
            rows,
            vec![("__turso_internal_custom_type_rules_v1".to_string(),)]
        );
        conn.execute("DELETE FROM t WHERE b = 1.5").unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t INDEXED BY t_a");
        assert_eq!(rows, vec![(0,)]);
    }

    /// VACUUM builds every index again, under the current rules.
    #[test]
    fn test_vacuum_records_rules_version() {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val numeric(10,2) UNIQUE) STRICT")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1.5), (2, 2.5)")
            .unwrap();
        conn.execute("VACUUM").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(
            rows,
            vec![("__turso_internal_custom_type_rules_v1".to_string(),)]
        );
        conn.execute("DELETE FROM t WHERE val = 1.5").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(rows, vec![("ok".to_string(),)]);

        let dir = TempDir::new().unwrap();
        let db = open_database_with_indexes_built_before_rules_version(&dir);
        let conn = db.connect_limbo();
        conn.execute("VACUUM").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows(VERSION_TABLES);
        assert_eq!(
            rows,
            vec![("__turso_internal_custom_type_rules_v1".to_string(),)]
        );
        conn.execute("DELETE FROM t WHERE id = 1").unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t INDEXED BY t_expr ORDER BY id");
        assert_eq!(rows, vec![(2,), (3,)]);
    }

    /// A newer version of Turso marks the database with a higher rules
    /// version. This version can't tell which indexes follow the newer rules,
    /// and the newer version would trust any index this one built, so every
    /// statement that reads or changes the database fails.
    #[test]
    fn test_database_changed_by_newer_version_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("newer.db");
        // With custom types, the CAST in q_cast is to the type numeric(10,2).
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER);
                 CREATE TABLE q(x);
                 CREATE INDEX q_cast ON q(CAST(x AS numeric(10,2)));
                 CREATE TABLE __turso_internal_custom_type_rules_v2(unused);",
            )
            .unwrap();

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let newer = "this database's indexes were built by a newer version of Turso's \
                     experimental custom types";
        for sql in [
            "DELETE FROM q WHERE x = 1",
            "CREATE INDEX p_cast ON p(CAST(x AS numeric(10,2)))",
            "CREATE TABLE u(id INTEGER PRIMARY KEY, val numeric(10,2) UNIQUE) STRICT",
            "CREATE INDEX p_x ON p(x)",
            "REINDEX",
            "INSERT INTO p VALUES (1, 1)",
            "SELECT x FROM p",
        ] {
            let err = prepare_error(&conn, sql);
            assert!(err.contains(newer), "{sql}: {err}");
        }
    }
}
