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

    /// Indexes on custom-type columns must keep their order after reopening:
    /// the stored-value order of a user type, and numeric's own order, both for
    /// column indexes and for expression indexes.
    #[test]
    fn test_index_keeps_type_order_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("type_order_reopen.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE TYPE rev BASE text ENCODE string_reverse(value) \
                 DECODE string_reverse(value) OPERATOR '<'",
            )
            .unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, word rev, n numeric(10,2)) STRICT",
            )
            .unwrap();
            conn.execute("CREATE INDEX t_word ON t(word)").unwrap();
            conn.execute("CREATE INDEX t_n_plus_one ON t((n+1))")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1,'cherry',9), (2,'apple',10.5), (3,'banana',100)")
                .unwrap();
            conn.close().unwrap();
        }

        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".to_string(),)]);
        // A new row goes to its place in the stored (reversed) text order:
        // ananab < elppa < tocirpa < yrrehc.
        conn.execute("INSERT INTO t VALUES (4,'apricot',0)")
            .unwrap();
        let rows: Vec<(String,)> =
            conn.exec_rows("SELECT word FROM t INDEXED BY t_word ORDER BY word");
        let words = ["banana", "apple", "apricot", "cherry"];
        assert_eq!(rows, words.map(|w| (w.to_string(),)));
        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".to_string(),)]);
        let rows: Vec<(i64,)> =
            conn.exec_rows("SELECT id FROM t INDEXED BY t_n_plus_one WHERE n+1 = 11.5");
        assert_eq!(rows, vec![(2,)]);
        let rows: Vec<(i64,)> = conn
            .exec_rows("SELECT id FROM t INDEXED BY t_n_plus_one WHERE n+1 > 10.5 ORDER BY n+1");
        assert_eq!(rows, vec![(2,), (3,)]);
    }

    /// Under MVCC the schema is rebuilt from sqlite_schema rows when the
    /// logical log is replayed on open and when a passive checkpoint runs.
    /// Indexes on columns whose type has its own order must keep that order
    /// in both.
    #[test]
    fn test_mvcc_index_keeps_type_order_after_schema_rebuild() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc_type_order_rebuild.db");
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let open = || {
            TempDatabase::builder()
                .with_db_path(&path)
                .with_opts(opts)
                .with_mvcc(true)
                .build()
        };
        let check = |conn: &Arc<turso_core::Connection>, words: &[&str], ids: &[i64]| {
            let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
            assert_eq!(integrity, vec![("ok".to_string(),)]);
            let rows: Vec<(String,)> =
                conn.exec_rows("SELECT word FROM t INDEXED BY t_word ORDER BY word");
            let words: Vec<(String,)> = words.iter().map(|w| (w.to_string(),)).collect();
            assert_eq!(rows, words);
            let ids: Vec<(i64,)> = ids.iter().map(|id| (*id,)).collect();
            for sql in [
                "SELECT id FROM t INDEXED BY t_n WHERE n > 10 ORDER BY id",
                "SELECT id FROM t NOT INDEXED WHERE n > 10 ORDER BY id",
            ] {
                let rows: Vec<(i64,)> = conn.exec_rows(sql);
                assert_eq!(rows, ids, "{sql}");
            }
        };

        {
            let db = open();
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE TYPE rev BASE text ENCODE string_reverse(value) \
                 DECODE string_reverse(value) OPERATOR '<'",
            )
            .unwrap();
            conn.execute(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, word rev, n numeric(10,2)) STRICT",
            )
            .unwrap();
            conn.execute("CREATE INDEX t_word ON t(word)").unwrap();
            conn.execute("CREATE INDEX t_n ON t(n)").unwrap();
            conn.execute("INSERT INTO t VALUES (1,'cherry',9), (2,'apple',10.5)")
                .unwrap();
            conn.close().unwrap();
        }

        // Reopening replays the logical log, which rebuilds the schema.
        let db = open();
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO t VALUES (3,'banana',100)")
            .unwrap();
        // Stored (reversed) text order: ananab < elppa < yrrehc.
        check(&conn, &["banana", "apple", "cherry"], &[2, 3]);

        // A passive checkpoint builds its own schema to write the index rows.
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        conn.execute("INSERT INTO t VALUES (4,'apricot',50)")
            .unwrap();
        check(&conn, &["banana", "apple", "apricot", "cherry"], &[2, 3, 4]);
    }

    /// Under MVCC, reopening replays the logical log; a domain's DEFAULT and
    /// NOT NULL must still apply afterwards.
    #[test]
    fn test_mvcc_domain_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc_domain_reopen.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let open = || {
            TempDatabase::builder()
                .with_db_path(&path)
                .with_opts(opts)
                .with_mvcc(true)
                .build()
        };

        {
            let db = open();
            let conn = db.connect_limbo();
            conn.execute("CREATE DOMAIN d AS integer DEFAULT 4 NOT NULL")
                .unwrap();
            conn.execute("CREATE TABLE t(a d, b int) STRICT").unwrap();
            conn.close().unwrap();
        }

        let db = open();
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO t(b) VALUES (1)").unwrap();
        assert!(conn.execute("INSERT INTO t VALUES (NULL, 2)").is_err());
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM t");
        assert_eq!(rows, vec![(4, 1)]);
    }

    /// Under MVCC a passive checkpoint builds its own schema to write index
    /// rows. An index on a domain over numeric must keep the numeric order
    /// there, or the checkpointed index rows are out of order.
    #[test]
    fn test_mvcc_passive_checkpoint_keeps_domain_index_order() {
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let db = TempDatabase::builder()
            .with_opts(opts)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();
        conn.execute("CREATE DOMAIN dn AS numeric").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n dn) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX t_n ON t(n)").unwrap();
        for i in 1..=300 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, {i}.5)"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".to_string(),)]);
        for sql in [
            "SELECT id FROM t INDEXED BY t_n WHERE n > 299 ORDER BY id",
            "SELECT id FROM t NOT INDEXED WHERE n > 299 ORDER BY id",
        ] {
            let rows: Vec<(i64,)> = conn.exec_rows(sql);
            assert_eq!(rows, vec![(299,), (300,)], "{sql}");
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

    /// An index on a numeric column added by ALTER TABLE must be built in
    /// numeric order right away, the order the column has after a reopen.
    #[test]
    fn test_index_on_added_numeric_column_keeps_its_order_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_add_column_index.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY) STRICT")
                .unwrap();
            conn.execute("ALTER TABLE t ADD COLUMN n numeric(10,2)")
                .unwrap();
            conn.execute("CREATE INDEX t_n ON t(n)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 9), (2, 10), (3, -3), (4, 100)")
                .unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
            assert_eq!(rows, vec![("ok".to_string(),)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE n > 9 ORDER BY id");
            assert_eq!(rows, vec![(2,), (4,)]);
            conn.close().unwrap();
        }
    }

    /// Without custom types enabled, a database that uses them is refused: its
    /// custom-type columns would read as stored bytes, and an index on them
    /// would be read and written in a different order than it was built in.
    #[test]
    fn test_database_with_custom_types_is_refused_without_the_flag() {
        let temp_dir = TempDir::new().unwrap();
        let cases = [
            (
                "builtin_type.db",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, n numeric(10,2)) STRICT",
                "column t.n has a custom type",
            ),
            (
                "array.db",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a integer[]) STRICT",
                "column t.a has a custom type",
            ),
            (
                "created_type.db",
                "CREATE TYPE neg BASE integer ENCODE -value DECODE -value",
                "the database has a table of custom types",
            ),
        ];
        for (file, sql, expected_error) in cases {
            let path = temp_dir.path().join(file);
            {
                let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
                let db = TempDatabase::new_with_existent_with_opts(&path, opts);
                let conn = db.connect_limbo();
                conn.execute(sql).unwrap();
                conn.close().unwrap();
            }
            let io = Arc::new(turso_core::PlatformIO::new().unwrap());
            let error = turso_core::Database::open_file(
                io,
                path.to_str().unwrap(),
                Arc::new(turso_core::SqliteDialect),
            )
            .and_then(|db| db.connect(turso_core::ConnectOpts::new()))
            .and_then(|conn| conn.execute("SELECT 1"))
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(expected_error),
                "{file}: unexpected error {error}"
            );
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

        // Complex expression over excluded.amount and t1.amount. cents declares
        // no '+', so the values are cast to integers first.
        conn.execute("DELETE FROM t1").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 42)").unwrap();
        conn.execute(
            "INSERT INTO t1 VALUES (1, 8) ON CONFLICT(id) DO UPDATE SET amount = CAST(excluded.amount AS INTEGER) + CAST(t1.amount AS INTEGER)",
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

    /// A failed `CREATE TYPE` must not leave `__turso_internal_types` behind: the types table is
    /// registered in the connection schema before the type SQL is parsed, and the rollback
    /// restores that schema only when the schema cookie was set beforehand.
    #[test]
    fn test_failed_create_type_leaves_no_types_table() {
        for mvcc in [false, true] {
            let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
            let db = TempDatabase::builder()
                .with_opts(opts)
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();

            // 257 variants: the type SQL is persisted first and rejected only when the type
            // itself is added to the schema
            let variants = (0..257)
                .map(|i| format!("v{i} INT"))
                .collect::<Vec<_>>()
                .join(", ");
            assert_that!(conn.execute(format!("CREATE TYPE u AS UNION({variants})")))
                .err()
                .display_string()
                .contains("UNION type cannot have more than 256 variants");

            let tables: Vec<(String, String, String, i64, i64, i64)> =
                conn.exec_rows("PRAGMA table_list");
            assert!(
                !tables
                    .iter()
                    .any(|(_, name, ..)| name == "__turso_internal_types"),
                "failed CREATE TYPE left the types table in the schema (mvcc={mvcc}): {tables:?}"
            );

            // The connection must still be usable: a later CREATE TYPE has to succeed
            conn.execute("CREATE TYPE t2 BASE text").unwrap();
            let tables: Vec<(String, String, String, i64, i64, i64)> =
                conn.exec_rows("PRAGMA table_list");
            assert!(
                tables
                    .iter()
                    .any(|(_, name, ..)| name == "__turso_internal_types"),
                "CREATE TYPE did not register the types table (mvcc={mvcc}): {tables:?}"
            );
            conn.close().unwrap();
        }
    }

    /// The other half of the same invariant: when the types table already exists, a failed
    /// `CREATE TYPE` must leave it in place, and leave the connection usable.
    #[test]
    fn test_failed_create_type_keeps_an_existing_types_table() {
        for mvcc in [false, true] {
            let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
            let db = TempDatabase::builder()
                .with_opts(opts)
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();

            conn.execute("CREATE TYPE type_one BASE text").unwrap();

            let variants = (0..257)
                .map(|i| format!("v{i} INT"))
                .collect::<Vec<_>>()
                .join(", ");
            assert_that!(conn.execute(format!("CREATE TYPE u AS UNION({variants})")))
                .err()
                .display_string()
                .contains("UNION type cannot have more than 256 variants");

            let tables: Vec<(String, String, String, i64, i64, i64)> =
                conn.exec_rows("PRAGMA table_list");
            assert!(
                tables
                    .iter()
                    .any(|(_, name, ..)| name == "__turso_internal_types"),
                "failed CREATE TYPE dropped the existing types table (mvcc={mvcc}): {tables:?}"
            );

            // The failed type must not be persisted, and the connection must still be usable
            conn.execute("CREATE TYPE type_two BASE text").unwrap();
            let types: Vec<(String,)> =
                conn.exec_rows("SELECT name FROM __turso_internal_types ORDER BY name");
            assert_eq!(
                types,
                vec![("type_one".to_string(),), ("type_two".to_string(),)],
                "mvcc={mvcc}"
            );
            conn.close().unwrap();
        }
    }

    /// Arrays compare as in PostgreSQL's `array_cmp`: element by element, a
    /// NULL element is greater than any other element and equal to another
    /// NULL, and when one array is a prefix of the other the shorter is
    /// smaller. Checks every pair of arrays of up to two elements, under every
    /// comparison operator, in WHERE with and without an index, ORDER BY,
    /// GROUP BY and DISTINCT, against that rule computed here.
    #[test]
    fn test_array_comparisons_sort_null_elements_last() {
        /// An element: `None` is NULL, `Some(k)` is the k-th smallest value.
        type Array = Vec<Option<usize>>;

        fn expected_order(left: &Array, right: &Array) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            for (l, r) in left.iter().zip(right) {
                let order = match (l, r) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (Some(l), Some(r)) => l.cmp(r),
                };
                if order.is_ne() {
                    return order;
                }
            }
            left.len().cmp(&right.len())
        }

        let element_types: [(&str, [&str; 2]); 3] = [
            ("integer", ["1", "2"]),
            ("real", ["1.5", "2.5"]),
            ("text", ["'a'", "'b'"]),
        ];
        let symbols = [None, Some(0), Some(1)];
        let mut arrays: Vec<Array> = vec![vec![]];
        arrays.extend(symbols.iter().map(|&s| vec![s]));
        arrays.extend(
            symbols
                .iter()
                .flat_map(|&a| symbols.iter().map(move |&b| vec![a, b])),
        );
        let n = arrays.len() as i64;
        let operators = [
            ("=", std::cmp::Ordering::is_eq as fn(_) -> bool),
            ("<>", std::cmp::Ordering::is_ne),
            ("<", std::cmp::Ordering::is_lt),
            ("<=", std::cmp::Ordering::is_le),
            (">", std::cmp::Ordering::is_gt),
            (">=", std::cmp::Ordering::is_ge),
        ];

        for mvcc in [false, true] {
            let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
            let db = TempDatabase::builder()
                .with_opts(opts)
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();

            for (element_type, values) in element_types {
                // `ARRAY[]` and `ARRAY[NULL]` have no element type to infer, so they
                // get a CAST, and a CAST keeps the planner from seeking the index.
                let untyped = |array: &Array| array.iter().all(Option::is_none);
                let literal = |array: &Array| {
                    let elements: Vec<&str> = array
                        .iter()
                        .map(|e| e.map_or("NULL", |k| values[k]))
                        .collect();
                    let literal = format!("ARRAY[{}]", elements.join(","));
                    if untyped(array) {
                        format!("CAST({literal} AS {element_type}[])")
                    } else {
                        literal
                    }
                };
                let plain = format!("plain_{element_type}");
                let indexed = format!("indexed_{element_type}");
                for table in [&plain, &indexed] {
                    conn.execute(format!(
                        "CREATE TABLE {table}(id INTEGER PRIMARY KEY, a {element_type}[]) STRICT"
                    ))
                    .unwrap();
                    // Each array twice, as rows i and i + n.
                    for (i, array) in arrays.iter().enumerate() {
                        for id in [i as i64, i as i64 + n] {
                            conn.execute(format!(
                                "INSERT INTO {table} VALUES ({id}, {})",
                                literal(array)
                            ))
                            .unwrap();
                        }
                    }
                }
                conn.execute(format!("CREATE INDEX {indexed}_a ON {indexed}(a)"))
                    .unwrap();
                let array_of = |id: i64| &arrays[(id % n) as usize];
                let context = format!("mvcc={mvcc}, {element_type}[]");

                for (op, holds) in operators {
                    let rows: Vec<(i64, i64, i64)> = conn.exec_rows(&format!(
                        "SELECT l.id, r.id, l.a {op} r.a FROM {plain} l, {plain} r \
                         WHERE l.id < {n} AND r.id < {n}"
                    ));
                    assert_eq!(rows.len(), arrays.len() * arrays.len(), "{context}");
                    for (l, r, result) in rows {
                        let (left, right) = (array_of(l), array_of(r));
                        assert_eq!(
                            result,
                            holds(expected_order(left, right)) as i64,
                            "{context}: {left:?} {op} {right:?}"
                        );
                    }

                    for right in &arrays {
                        let expected: Vec<(i64,)> = (0..2 * n)
                            .filter(|&id| holds(expected_order(array_of(id), right)))
                            .map(|id| (id,))
                            .collect();
                        for source in [plain.clone(), format!("{indexed} INDEXED BY {indexed}_a")] {
                            let query = format!(
                                "SELECT id FROM {source} WHERE a {op} {} ORDER BY id",
                                literal(right)
                            );
                            if source != plain && op != "<>" && !untyped(right) {
                                let plan: Vec<(i64, i64, i64, String)> =
                                    conn.exec_rows(&format!("EXPLAIN QUERY PLAN {query}"));
                                assert!(
                                    plan.iter().any(|(.., detail)| detail.starts_with("SEARCH")),
                                    "{context}: {query} does not seek the index: {plan:?}"
                                );
                            }
                            let rows: Vec<(i64,)> = conn.exec_rows(&query);
                            assert_eq!(rows, expected, "{context}: {query}");
                        }
                    }
                }

                for table in [&plain, &indexed] {
                    for direction in ["ASC", "DESC"] {
                        let ids: Vec<(i64,)> = conn
                            .exec_rows(&format!("SELECT id FROM {table} ORDER BY a {direction}"));
                        let sorted: Vec<&Array> = ids.iter().map(|&(id,)| array_of(id)).collect();
                        assert_eq!(sorted.len(), 2 * arrays.len(), "{context}");
                        for pair in sorted.windows(2) {
                            let order = expected_order(pair[0], pair[1]);
                            let in_order = match direction {
                                "ASC" => order.is_le(),
                                _ => order.is_ge(),
                            };
                            assert!(
                                in_order,
                                "{context}: {table} ORDER BY a {direction}: {sorted:?}"
                            );
                        }
                    }

                    let groups: Vec<(i64, i64, i64)> = conn.exec_rows(&format!(
                        "SELECT min(id), max(id), count(*) FROM {table} GROUP BY a ORDER BY a"
                    ));
                    let mut expected: Vec<(i64, i64, i64)> =
                        (0..n).map(|i| (i, i + n, 2)).collect();
                    expected.sort_by(|l, r| expected_order(array_of(l.0), array_of(r.0)));
                    assert_eq!(groups, expected, "{context}: {table} GROUP BY a");

                    let distinct: Vec<(i64,)> = conn.exec_rows(&format!(
                        "SELECT count(*) FROM (SELECT DISTINCT a FROM {table})"
                    ));
                    assert_eq!(distinct, vec![(n,)], "{context}: {table} DISTINCT a");
                }
            }
            conn.close().unwrap();
        }
    }

    /// An array of a custom type holds the values its elements read as, the
    /// same values a column of the type holds. For every kind of element type,
    /// each way of writing or building an array is checked against a column
    /// `w` of the element type that the same statement writes. A type whose
    /// ENCODE and DECODE don't undo each other (`ENCODE value + 1`) changes `w`
    /// on every write, and must change the array's elements the same way.
    #[test]
    fn test_arrays_hold_the_values_of_their_elements() {
        struct Kind {
            setup: String,
            /// The element type as written in a column, such as `varchar(5)`.
            ty: String,
            name: String,
            sample: &'static str,
        }
        let builtin = |ty: &str, sample| Kind {
            setup: String::new(),
            ty: ty.to_string(),
            name: ty.split('(').next().unwrap().to_string(),
            sample,
        };
        let mut kinds = vec![
            builtin("integer", "7"),
            builtin("text", "'x'"),
            builtin("real", "1.5"),
            builtin("blob", "x'01'"),
            builtin("bigint", "7"),
            builtin("smallint", "7"),
            builtin("varchar(5)", "'abc'"),
            builtin("bytea", "x'01'"),
            builtin("cidr", "'10.0.0.0/8'"),
            builtin("macaddr", "'08:00:2b:01:02:03'"),
            builtin("macaddr8", "'08:00:2b:01:02:03:04:05'"),
            builtin("uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
            builtin("boolean", "1"),
            builtin("date", "'2024-01-02'"),
            builtin("time", "'10:00:00'"),
            builtin("timestamp", "'2024-01-02 10:00:00'"),
            builtin("timestamptz", "'2024-01-02 10:00:00'"),
            builtin("json", "'{}'"),
            builtin("jsonb", "'{}'"),
            builtin("inet", "'10.0.0.1'"),
            builtin("numeric(10,2)", "1.5"),
        ];
        // User types with every combination of no, an unchanging and a changing
        // ENCODE and DECODE, and a domain over each.
        let encodes = [None, Some("value"), Some("value + 1")];
        let decodes = [None, Some("value"), Some("value - 1")];
        for (i, encode) in encodes.iter().enumerate() {
            for (j, decode) in decodes.iter().enumerate() {
                let name = format!("t_e{i}_d{j}");
                let mut create = format!("CREATE TYPE {name}(value integer) BASE integer");
                if let Some(encode) = encode {
                    create.push_str(&format!(" ENCODE {encode}"));
                }
                if let Some(decode) = decode {
                    create.push_str(&format!(" DECODE {decode}"));
                }
                kinds.push(Kind {
                    setup: create.clone(),
                    ty: name.clone(),
                    name: name.clone(),
                    sample: "7",
                });
                kinds.push(Kind {
                    setup: format!("{create}; CREATE DOMAIN dom_{name} AS {name}"),
                    ty: format!("dom_{name}"),
                    name: format!("dom_{name}"),
                    sample: "7",
                });
            }
        }
        // Stored forms that sort in the opposite order of the values.
        let negated = "CREATE TYPE t_neg(value integer) BASE integer ENCODE -value DECODE -value";
        kinds.push(Kind {
            setup: negated.to_string(),
            ty: "t_neg".to_string(),
            name: "t_neg".to_string(),
            sample: "7",
        });
        kinds.push(Kind {
            setup: format!("{negated}; CREATE DOMAIN dom_t_neg AS t_neg"),
            ty: "dom_t_neg".to_string(),
            name: "dom_t_neg".to_string(),
            sample: "7",
        });
        for (base, sample) in [
            ("integer", "7"),
            ("uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
            ("date", "'2024-01-02'"),
        ] {
            kinds.push(Kind {
                setup: format!("CREATE DOMAIN dom_{base} AS {base}"),
                ty: format!("dom_{base}"),
                name: format!("dom_{base}"),
                sample,
            });
        }

        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        let registered: Vec<(String,)> = conn.exec_rows("PRAGMA list_types");
        for (signature,) in registered {
            let name = signature.split('(').next().unwrap().to_lowercase();
            assert!(
                name == "any" || kinds.iter().any(|kind| kind.name == name),
                "built-in type {name} is not tried"
            );
        }

        for kind in &kinds {
            let db = TempDatabase::builder().with_opts(opts).build();
            let conn = db.connect_limbo();
            let ty = &kind.ty;
            let s = kind.sample;
            for sql in kind.setup.split(';').filter(|sql| !sql.trim().is_empty()) {
                conn.execute(sql).unwrap();
            }
            let create = format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v {ty}, w {ty}, a {ty}[], m {ty}[][]) STRICT"
            );
            if kind.name == "numeric" {
                // numeric's order is not the order of its stored values.
                assert_that!(conn.execute(&create))
                    .err()
                    .display_string()
                    .contains("arrays of type numeric are not supported");
                continue;
            }
            conn.execute(&create).unwrap();
            let run = |sql: &str| {
                conn.execute(sql)
                    .unwrap_or_else(|e| panic!("{ty}: {sql}: {e}"));
            };
            let all_true = |sql: &str, count: usize| {
                let rows = crate::common::limbo_exec_rows(&conn, sql);
                let expected = vec![vec![rusqlite::types::Value::Integer(1); count]];
                assert_eq!(rows, expected, "{ty}: {sql}");
            };
            let check = |step: &str| {
                all_true(
                    &format!(
                        "SELECT a = ARRAY[w], a[1] = w, m[1] = ARRAY[w], m[1][1] = w, \
                         CAST(a AS {ty}[]) = ARRAY[CAST(w AS {ty})], \
                         (SELECT array_agg(t2.w) = t.a FROM t t2) FROM t -- after {step}"
                    ),
                    6,
                )
            };

            run(&format!(
                "INSERT INTO t VALUES (1, {s}, {s}, ARRAY[{s}], ARRAY[ARRAY[{s}]])"
            ));
            check("INSERT");
            run("UPDATE t SET w = v, a = ARRAY[v], m = ARRAY[ARRAY[v]]");
            check("UPDATE from another column");
            run("UPDATE t SET w = w, a = a, m = m");
            check("UPDATE of each column to itself");
            run(&format!(
                "INSERT INTO t(id, v) VALUES (1, {s}) \
                 ON CONFLICT(id) DO UPDATE SET w = w, a = a, m = m"
            ));
            check("UPSERT of each column to itself");
            run(&format!(
                "INSERT INTO t(id, v) VALUES (1, {s}) ON CONFLICT(id) DO UPDATE SET v = v"
            ));
            check("UPSERT of another column");
            run("VACUUM");
            check("VACUUM");
            all_true("UPDATE t SET w = w, a = a, m = m RETURNING a = ARRAY[w]", 1);
            check("UPDATE with RETURNING");

            run(&format!("CREATE TABLE log(w {ty}, a {ty}[]) STRICT"));
            run("CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (NEW.w, NEW.a); END");
            run("UPDATE t SET v = v");
            all_true("SELECT a = ARRAY[w] FROM log", 1);

            // A key on an array follows the element type's key rule, so only
            // types whose index on a column of the type finds the row are
            // tried. An index on a type whose DECODE changes values, such as
            // `DECODE value - 1`, misses rows for a column of the type too.
            let element_index_works = conn.execute("CREATE INDEX tw ON t(w)").is_ok()
                && crate::common::limbo_exec_rows(
                    &conn,
                    "SELECT count(*) FROM t WHERE w = (SELECT w FROM t)",
                ) == vec![vec![rusqlite::types::Value::Integer(1)]];
            if element_index_works {
                run("CREATE INDEX ta ON t(a)");
                all_true(
                    "SELECT count(*) FROM t WHERE a = ARRAY[(SELECT w FROM t)]",
                    1,
                );
            }

            // Elements sort and compare by their values, with and without an
            // index, for types whose values are numbers: 7 sorts before 8.
            if s == "7" {
                run(&format!(
                    "CREATE TABLE o(id INTEGER PRIMARY KEY, a {ty}[]) STRICT"
                ));
                run("INSERT INTO o VALUES (1, ARRAY[7]), (2, ARRAY[8])");
                let sorts_by_value = |step: &str| {
                    for (sql, expected) in [
                        (
                            "SELECT group_concat(id) FROM (SELECT id FROM o ORDER BY a[1])",
                            "1,2",
                        ),
                        (
                            "SELECT group_concat(o1.id || '<' || o2.id) \
                             FROM o o1, o o2 WHERE o1.a < o2.a",
                            "1<2",
                        ),
                    ] {
                        let rows: Vec<(String,)> = conn.exec_rows(sql);
                        assert_eq!(rows, vec![(expected.to_string(),)], "{ty}: {sql} ({step})");
                    }
                };
                sorts_by_value("without an index");
                if element_index_works {
                    run("CREATE INDEX oa ON o(a)");
                    sorts_by_value("with an index");
                }
            }

            run("UPDATE t SET w = w, a = array_append(a, w)");
            all_true("SELECT a = ARRAY[w, w] FROM t", 1);

            run(&format!(
                "CREATE TABLE d(id INTEGER PRIMARY KEY, dw {ty} DEFAULT ({s}), \
                 da {ty}[] DEFAULT (ARRAY[{s}])) STRICT"
            ));
            run("INSERT INTO d(id) VALUES (1)");
            all_true("SELECT da = ARRAY[dw] FROM d", 1);
            // ADD COLUMN takes only a constant default, so the array is
            // written as text, such as '{"x"}'. Blobs have no text form, and
            // ADD COLUMN checks the default of an integer[] or real[] column
            // as if it were an integer or a real, so it refuses any text.
            let text_array = match s.as_bytes()[0] {
                _ if ty == "integer" || ty == "real" => None,
                b'\'' => Some(format!("'{{\"{}\"}}'", &s[1..s.len() - 1])),
                b'x' => None,
                _ => Some(format!("'{{{s}}}'")),
            };
            if let Some(text_array) = text_array {
                run(&format!(
                    "ALTER TABLE d ADD COLUMN dx {ty}[] DEFAULT {text_array}"
                ));
                all_true("SELECT dx = ARRAY[dw] FROM d", 1);
            }

            run(&format!(
                "CREATE TABLE g(id INTEGER PRIMARY KEY, w {ty}, \
                 gv {ty} AS (w) VIRTUAL, av {ty}[] AS (ARRAY[w]) VIRTUAL) STRICT"
            ));
            run(&format!("INSERT INTO g(id, w) VALUES (1, {s})"));
            all_true("SELECT av = ARRAY[gv], av[1] = gv FROM g", 2);

            let base = match s.as_bytes()[0] {
                b'\'' => "text",
                b'x' => "blob",
                _ if s.contains('.') => "real",
                _ => "integer",
            };
            run(&format!(
                "CREATE TABLE b(id INTEGER PRIMARY KEY, y {base}, x {base}[]) STRICT"
            ));
            run(&format!("INSERT INTO b VALUES (1, {s}, ARRAY[{s}])"));
            run(&format!("ALTER TABLE b ALTER COLUMN y TO y2 {ty}"));
            run(&format!("ALTER TABLE b ALTER COLUMN x TO x2 {ty}[]"));
            all_true("SELECT x2 = ARRAY[y2] FROM b", 1);
        }
    }
}
