#[cfg(test)]
mod tests {
    use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
    use asserting::prelude::*;
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
    fn test_min_max_of_indexed_custom_type_column_reads_one_index_entry() {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE ev(id INTEGER PRIMARY KEY, ts timestamp, v uuid) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX ev_ts ON ev(ts)").unwrap();
        conn.execute("CREATE INDEX ev_v ON ev(v)").unwrap();

        for query in [
            "SELECT max(ts) FROM ev",
            "SELECT min(ts) FROM ev",
            "SELECT max(v) FROM ev",
            "SELECT min(v) FROM ev",
        ] {
            let opcodes: Vec<String> =
                crate::common::limbo_exec_rows(&conn, &format!("EXPLAIN {query}"))
                    .into_iter()
                    .map(|row| match &row[1] {
                        rusqlite::types::Value::Text(opcode) => opcode.clone(),
                        other => panic!("opcode column holds {other:?}"),
                    })
                    .collect();
            let agg_step = opcodes
                .iter()
                .position(|opcode| opcode == "AggStep")
                .unwrap_or_else(|| panic!("{query} has no AggStep: {opcodes:?}"));
            assert_eq!(
                opcodes[agg_step + 1],
                "Goto",
                "{query} must stop after the first index entry: {opcodes:?}"
            );
        }
    }

    fn open_file(
        path: &std::path::Path,
        custom_types: bool,
    ) -> turso_core::Result<std::sync::Arc<turso_core::Database>> {
        let io: std::sync::Arc<dyn turso_core::IO + Send> =
            std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        turso_core::Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            turso_core::OpenFlags::Create,
            turso_core::DatabaseOpts::new()
                .with_custom_types(custom_types)
                .with_attach(true),
            None,
            std::sync::Arc::new(turso_core::SqliteDialect),
        )
    }

    fn create_file(path: &std::path::Path, mvcc: bool, sql: &str) {
        let db = open_file(path, true).unwrap();
        let conn = db.connect().unwrap();
        if mvcc {
            conn.pragma_update("journal_mode", "'mvcc'").unwrap();
        }
        conn.execute(sql).unwrap();
        conn.close().unwrap();
    }

    /// The MVCC log recovery builds a new schema. The user types must be in
    /// it, also a type that only the log has.
    #[test]
    fn test_custom_types_survive_mvcc_log_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_mvcc_log.db");
        create_file(
            &path,
            true,
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
             CREATE TABLE t(id INTEGER PRIMARY KEY, a cents) STRICT;
             INSERT INTO t VALUES (1, 5);",
        );
        assert!(path.with_extension("db-log").metadata().unwrap().len() > 0);

        for (insert, expected) in [
            ("INSERT INTO t VALUES (2, 7)", vec![(1, 5), (2, 7)]),
            ("SELECT 1", vec![(1, 5), (2, 7)]),
        ] {
            let db = open_file(&path, true).unwrap();
            let conn = db.connect().unwrap();
            conn.execute(insert).unwrap();
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, a FROM t ORDER BY id");
            assert_eq!(rows, expected);
            conn.close().unwrap();
        }
    }

    fn write_table_sql(path: &std::path::Path, sql: &str) {
        let sqlite = rusqlite::Connection::open(path).unwrap();
        sqlite
            .execute_batch("CREATE TABLE t(id bigint PRIMARY KEY, n); PRAGMA writable_schema = ON;")
            .unwrap();
        sqlite
            .execute("UPDATE sqlite_master SET sql = ?1 WHERE name = 't'", [sql])
            .unwrap();
    }

    /// Older versions of the PostgreSQL frontend stored PostgreSQL DDL after a
    /// marker comment. The SQLite parser reads it as a table without STRICT.
    #[test]
    fn test_sqlite_dialect_refuses_postgres_ddl_of_older_versions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("pg_marker.db");
        write_table_sql(
            &path,
            "/* turso_frontend:postgres */ CREATE TABLE t (id bigint PRIMARY KEY, n numeric(10,2))",
        );
        for custom_types in [false, true] {
            let Err(err) = open_file(&path, custom_types) else {
                panic!("the SQLite dialect must refuse PostgreSQL DDL");
            };
            assert_that!(err.to_string()).contains("created by the PostgreSQL frontend");
        }
    }

    /// RENAME of older versions of the PostgreSQL frontend stored the marker
    /// before canonical STRICT SQL. The marker shows that the table is a
    /// table of the PostgreSQL frontend.
    #[test]
    fn test_marker_before_canonical_sql_loads_as_pg_storage_table() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("pg_marker_rename.db");
        write_table_sql(
            &path,
            "/* turso_frontend:postgres */ CREATE TABLE t (id bigint PRIMARY KEY, n numeric (10, 2)) STRICT",
        );
        let Err(err) = open_file(&path, false) else {
            panic!("a table of the PostgreSQL frontend needs custom types");
        };
        assert_that!(err.to_string()).contains("table t was created by the PostgreSQL frontend");

        let db = open_file(&path, true).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("INSERT INTO t VALUES (1, 2.5)").unwrap();
        let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, n FROM t");
        assert_eq!(rows, vec![(1, "2.50".to_string())]);
    }

    /// Run `sql` with SQLite on a file that Turso wrote. SQLite expects an
    /// automatic index for the PRIMARY KEY of the types table, which Turso
    /// does not create, so the table SQL has no PRIMARY KEY during the edit.
    fn edit_with_sqlite(path: &std::path::Path, sql: &str) {
        const TYPES_TABLE_SQL: &str =
            "CREATE TABLE __turso_internal_types(name TEXT PRIMARY KEY, sql TEXT)";
        let set_types_table_sql = |sql: &str| {
            format!("UPDATE sqlite_master SET sql = '{sql}' WHERE name = '__turso_internal_types';")
        };
        rusqlite::Connection::open(path)
            .unwrap()
            .execute_batch(&format!(
                "PRAGMA writable_schema = ON; {}",
                set_types_table_sql("CREATE TABLE __turso_internal_types(name TEXT, sql TEXT)")
            ))
            .unwrap();
        rusqlite::Connection::open(path)
            .unwrap()
            .execute_batch(&format!(
                "PRAGMA writable_schema = ON; {sql}; {}",
                set_types_table_sql(TYPES_TABLE_SQL)
            ))
            .unwrap();
    }

    /// A column type that does not resolve to a primitive type: the stored
    /// values have no meaning for this binary.
    #[test]
    fn test_open_refuses_column_types_that_do_not_resolve() {
        for (change, error) in [
            (
                "UPDATE __turso_internal_types SET sql = 'CREATE DOMAIN d AS pg_later_type' WHERE name = 'd'",
                "column x.a has type \"d\", which this database does not define",
            ),
            (
                "DELETE FROM __turso_internal_types WHERE name = 'd'",
                "column x.a has type \"d\", which this database does not define",
            ),
            (
                "UPDATE sqlite_master SET sql = 'CREATE TABLE x (id INTEGER PRIMARY KEY, a d, b pg_later_type) STRICT' WHERE name = 'x'",
                "column x.b has type \"pg_later_type\", which this database does not define",
            ),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().join("unresolved_type.db");
            create_file(
                &path,
                false,
                "CREATE DOMAIN d AS INTEGER CHECK (value > 0);
                 CREATE TABLE x(id INTEGER PRIMARY KEY, a d) STRICT;
                 INSERT INTO x VALUES (1, 5);",
            );
            edit_with_sqlite(&path, change);
            let Err(err) = open_file(&path, true) else {
                panic!("the open must fail: {change}");
            };
            assert_that!(err.to_string()).contains(error);
        }
    }

    /// One type that does not load must not hide the types after it.
    #[test]
    fn test_types_after_a_type_that_does_not_load_still_load() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bad_type_first.db");
        create_file(
            &path,
            false,
            "CREATE DOMAIN a_unused AS INTEGER;
             CREATE DOMAIN z_used AS INTEGER CHECK (value > 0);
             CREATE TABLE x(id INTEGER PRIMARY KEY, a z_used) STRICT;",
        );
        edit_with_sqlite(
            &path,
            "UPDATE __turso_internal_types SET sql = 'CREATE DOMAIN a_unused AS' WHERE name = 'a_unused'",
        );
        let db = open_file(&path, true).unwrap();
        let conn = db.connect().unwrap();
        assert_that!(conn.execute("INSERT INTO x VALUES (1, -5)"))
            .err()
            .display_string()
            .contains("CHECK constraint failed");
    }

    #[test]
    fn test_pg_storage_option_survives_alter_table_and_reopen() {
        for mvcc in [false, true] {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().join("pg_storage.db");
            create_file(
                &path,
                mvcc,
                "CREATE TABLE t(id INTEGER PRIMARY KEY, n numeric(10,2), gone TEXT) STRICT, PGSTORAGE;
                 INSERT INTO t VALUES (1, 2.5, 'x');
                 ALTER TABLE t ADD COLUMN c TEXT;
                 ALTER TABLE t DROP COLUMN gone;
                 ALTER TABLE t RENAME COLUMN c TO d;
                 ALTER TABLE t RENAME TO t2;",
            );

            let db = open_file(&path, true).unwrap();
            let conn = db.connect().unwrap();
            let sql: Vec<(String,)> =
                conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't2'");
            assert_eq!(
                sql,
                vec![(
                    "CREATE TABLE t2 (id INTEGER PRIMARY KEY, n numeric (10, 2), d TEXT) STRICT, PGSTORAGE"
                        .to_string(),
                )],
                "mvcc={mvcc}"
            );
            conn.execute("INSERT INTO t2 VALUES (2, 3, 'y')").unwrap();
            let rows: Vec<(i64, String, String)> =
                conn.exec_rows("SELECT id, n, coalesce(d, 'NULL') FROM t2 ORDER BY id");
            assert_eq!(
                rows,
                vec![
                    (1, "2.50".to_string(), "NULL".to_string()),
                    (2, "3.00".to_string(), "y".to_string())
                ],
                "mvcc={mvcc}"
            );
            conn.close().unwrap();
        }
    }

    #[test]
    fn test_open_without_custom_types_refuses_tables_that_need_them() {
        for mvcc in [false, true] {
            for (sql, error) in [
                (
                    "CREATE TABLE p(id INTEGER PRIMARY KEY, a TEXT) STRICT, PGSTORAGE",
                    "table p was created by the PostgreSQL frontend",
                ),
                (
                    "CREATE TABLE u(id INTEGER PRIMARY KEY, b uuid) STRICT",
                    "column u.b has type \"uuid\", which needs custom types",
                ),
                (
                    "CREATE TABLE a(id INTEGER PRIMARY KEY, c INTEGER[]) STRICT",
                    "column a.c has type \"INTEGER[]\", which needs custom types",
                ),
            ] {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("needs_custom_types.db");
                create_file(&path, mvcc, sql);
                let Err(err) = open_file(&path, false) else {
                    panic!("open without custom types must fail: mvcc={mvcc}, {sql}");
                };
                assert_that!(err.to_string()).contains(error);
                open_file(&path, true).unwrap();
            }
        }
    }

    #[test]
    fn test_create_pg_storage_table_needs_custom_types() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("pg_storage_create.db");
        let db = open_file(&path, false).unwrap();
        let conn = db.connect().unwrap();
        assert_that!(conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY) STRICT, PGSTORAGE"))
            .err()
            .display_string()
            .contains("PGSTORAGE table p needs custom types");
    }

    /// A reparse finds a column type that does not resolve. The connection
    /// refuses the new schema and keeps the schema that it had.
    #[test]
    fn test_reparse_refuses_column_types_that_do_not_resolve() {
        for (setup, change) in [
            (
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
                 CREATE DOMAIN d AS cents;
                 CREATE TABLE x(id INTEGER PRIMARY KEY, a d) STRICT;",
                "UPDATE __turso_internal_types SET sql = 'CREATE DOMAIN d AS pg_later_type' WHERE name = 'd'",
            ),
            (
                "CREATE TABLE x(id INTEGER PRIMARY KEY, a INTEGER) STRICT;",
                "UPDATE sqlite_schema SET sql = 'CREATE TABLE x (id INTEGER PRIMARY KEY, a pg_later_type) STRICT' WHERE name = 'x'",
            ),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().join("reparse.db");
            let db = open_file(&path, true).unwrap();
            let conn = db.connect().unwrap();
            conn.execute(setup).unwrap();
            conn.execute("INSERT INTO x VALUES (1, 5)").unwrap();
            conn.execute("BEGIN IMMEDIATE").unwrap();
            conn.start_nested();
            conn.execute(change).unwrap();
            conn.end_nested();
            conn.execute("COMMIT").unwrap();

            let Err(err) = conn.force_reparse_schema_without_publish() else {
                panic!("the reparse must fail: {change}");
            };
            assert_that!(err.to_string()).contains("which this database does not define");
            let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, a FROM x");
            assert_eq!(rows, vec![(1, 5)], "{change}");
        }
    }

    /// The values of an attached table were encoded with the type definition
    /// of the attached file, but its tables use the types of the main
    /// database.
    #[test]
    fn test_attach_refuses_a_type_that_the_main_database_defines_differently() {
        let temp_dir = TempDir::new().unwrap();
        let attached = temp_dir.path().join("attached.db");
        create_file(
            &attached,
            false,
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
             CREATE DOMAIN d AS cents;
             CREATE TABLE t(c cents, e d) STRICT;
             INSERT INTO t VALUES (7, 8);",
        );
        let attach = format!("ATTACH '{}' AS a", attached.display());
        for (main_types, error) in [
            (
                "CREATE TYPE cents BASE integer ENCODE value * 1000 DECODE value / 1000;
                 CREATE DOMAIN d AS cents;",
                "column t.c uses type \"cents\", which the attached database and the main database define differently",
            ),
            (
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
                 CREATE DOMAIN d AS integer;",
                "column t.e uses type \"d\", which the attached database and the main database define differently",
            ),
        ] {
            let main = temp_dir.path().join("main.db");
            create_file(&main, false, main_types);
            let db = open_file(&main, true).unwrap();
            let conn = db.connect().unwrap();
            assert_that!(conn.execute(&attach))
                .err()
                .display_string()
                .contains(error);
            conn.close().unwrap();
            drop(db);
            std::fs::remove_file(&main).unwrap();
            let _ = std::fs::remove_file(main.with_extension("db-wal"));
        }

        let main = temp_dir.path().join("same.db");
        create_file(
            &main,
            false,
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100;
             CREATE DOMAIN d AS cents;",
        );
        let db = open_file(&main, true).unwrap();
        let conn = db.connect().unwrap();
        conn.execute(&attach).unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT c, e FROM a.t");
        assert_eq!(rows, vec![(7, 8)]);
    }

    /// A table of a database that ATTACH opened uses the types of the main
    /// database. An open of the same file as the main database must check
    /// the types again, also when the open gets the instance that ATTACH
    /// opened.
    #[test]
    fn test_open_as_main_checks_the_types_of_a_database_that_attach_opened() {
        let temp_dir = TempDir::new().unwrap();
        let main = temp_dir.path().join("main.db");
        let attached = temp_dir.path().join("attached.db");
        let db = open_file(&main, true).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE DOMAIN d AS INTEGER CHECK (value > 0)")
            .unwrap();
        conn.execute(format!("ATTACH '{}' AS a", attached.display()))
            .unwrap();
        conn.execute("CREATE TABLE a.t(id INTEGER PRIMARY KEY, x d) STRICT")
            .unwrap();
        conn.execute("INSERT INTO a.t VALUES (1, 5)").unwrap();

        let Err(err) = open_file(&attached, true) else {
            panic!("the file has no type d");
        };
        assert_that!(err.to_string())
            .contains("column t.x has type \"d\", which this database does not define");
    }

    #[test]
    fn test_attach_without_custom_types_refuses_pg_storage_tables() {
        let temp_dir = TempDir::new().unwrap();
        let attached = temp_dir.path().join("attached.db");
        create_file(
            &attached,
            false,
            "CREATE TABLE p(id INTEGER PRIMARY KEY, a TEXT) STRICT, PGSTORAGE;",
        );
        let db = open_file(&temp_dir.path().join("main.db"), false).unwrap();
        let conn = db.connect().unwrap();
        assert_that!(conn.execute(format!("ATTACH '{}' AS a", attached.display())))
            .err()
            .display_string()
            .contains("table p was created by the PostgreSQL frontend");
    }

    /// A column whose types DECODE only to the stored value reads like a
    /// column of its base type: no IsNull and no DECODE after its Column, so
    /// consecutive reads fuse into one ColumnRange.
    #[test]
    fn test_identity_decode_columns_fuse_into_column_range() {
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE DOMAIN dint AS integer").unwrap();
        conn.execute("CREATE TYPE keeps BASE integer ENCODE (value + 0) DECODE (value)")
            .unwrap();
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, b bigint, v varchar(5), j json, d dint, k keeps) STRICT",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE p(id pg_int4 PRIMARY KEY, a pg_int8, n pg_int4, s text) STRICT, PGSTORAGE",
        )
        .unwrap();
        conn.execute(r#"INSERT INTO t VALUES (1, 5, 'abc', '{"k":1}', 7, 8)"#)
            .unwrap();
        conn.execute("INSERT INTO p VALUES (1, 9000000000, -3, 'x')")
            .unwrap();

        for (sql, columns) in [
            ("SELECT b, v, j, d, k FROM t", 5),
            ("SELECT a, n, s FROM p", 3),
        ] {
            let program = limbo_exec_rows(&conn, &format!("EXPLAIN {sql}"));
            let opcodes: Vec<String> = program
                .iter()
                .map(|insn| match &insn[1] {
                    rusqlite::types::Value::Text(opcode) => opcode.clone(),
                    other => panic!("opcode is text: {other:?}"),
                })
                .collect();
            assert!(
                !opcodes.iter().any(|op| op == "IsNull"),
                "{sql}: {opcodes:?}"
            );
            let ranges: Vec<_> = program
                .iter()
                .zip(&opcodes)
                .filter(|(_, opcode)| *opcode == "ColumnRange")
                .map(|(insn, _)| insn[5].clone())
                .collect();
            assert_eq!(
                ranges,
                vec![rusqlite::types::Value::Integer(columns)],
                "{sql}: {opcodes:?}"
            );
        }

        let rows: Vec<(i64, String, String, i64, i64)> =
            conn.exec_rows("SELECT b, v, j, d, k FROM t");
        assert_eq!(rows, vec![(5, "abc".into(), r#"{"k":1}"#.into(), 7, 8)]);
        let rows: Vec<(i64, i64, String)> = conn.exec_rows("SELECT a, n, s FROM p");
        assert_eq!(rows, vec![(9_000_000_000, -3, "x".into())]);
    }
}
