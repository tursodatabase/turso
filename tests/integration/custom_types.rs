#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use asserting::prelude::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use turso_sqlite::SqliteDialect;

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
            conn.execute("CREATE TYPE rev BASE text ENCODE string_reverse(value) OPERATOR '<'")
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
        // The type has no DECODE, so words read back as they are stored
        // (reversed). A new row goes to its place in that order.
        conn.execute("INSERT INTO t VALUES (4,'apricot',0)")
            .unwrap();
        let rows: Vec<(String,)> =
            conn.exec_rows("SELECT word FROM t INDEXED BY t_word ORDER BY word");
        let words = ["ananab", "elppa", "tocirpa", "yrrehc"];
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
            conn.execute("CREATE TYPE rev BASE text ENCODE string_reverse(value) OPERATOR '<'")
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
        // Words read back as stored (reversed): ananab < elppa < yrrehc.
        check(&conn, &["ananab", "elppa", "yrrehc"], &[2, 3]);

        // A passive checkpoint builds its own schema to write the index rows.
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        conn.execute("INSERT INTO t VALUES (4,'apricot',50)")
            .unwrap();
        check(&conn, &["ananab", "elppa", "tocirpa", "yrrehc"], &[2, 3, 4]);
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
            Arc::new(SqliteDialect),
        );
        let Err(err) = result else {
            panic!("open must fail when a stored type does not parse");
        };
        assert!(
            err.to_string().contains("malformed database schema"),
            "unexpected error: {err}"
        );
    }

    /// CREATE TYPE refuses a DECODE whose kind can't be worked out, so a
    /// stored type with one must not open and compare by a guessed kind.
    #[test]
    fn test_reopen_fails_when_stored_type_reads_as_no_known_kind() {
        let err = reopen_after_storing_type_sql(
            "CREATE TYPE r(value any) BASE integer ENCODE value DECODE round(value)",
        );
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell what kind of value DECODE of type r returns"),
            "unexpected error: {err}"
        );
    }

    /// CREATE DOMAIN refuses a base type that does not exist yet, so a stored
    /// domain based on itself must not open and use its values as a guessed kind.
    #[test]
    fn test_reopen_fails_when_stored_type_stores_no_known_kind() {
        let err = reopen_after_storing_type_sql("CREATE DOMAIN r AS r");
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell what kind of value type r stores"),
            "unexpected error: {err}"
        );
    }

    /// An earlier version could store a type whose ENCODE casts to itself.
    /// Such a type still opens, and DROP TYPE removes it.
    #[test]
    fn test_drop_type_that_casts_to_itself_stored_by_an_earlier_version() {
        let (_temp_dir, path) = database_with_stored_type_sql(
            "CREATE TYPE r BASE integer ENCODE CAST(value AS r) DECODE value",
        );
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        conn.execute("DROP TABLE t").unwrap();
        conn.execute("DROP TYPE r").unwrap();
        let rows: Vec<(i64,)> =
            conn.exec_rows("SELECT count(*) FROM sqlite_turso_types WHERE name = 'r'");
        assert_eq!(rows, vec![(0,)]);
    }

    /// Returns the error from opening a database storing `type_sql` as the
    /// SQL of type `r` (see database_with_stored_type_sql).
    fn reopen_after_storing_type_sql(type_sql: &str) -> String {
        let (_temp_dir, path) = database_with_stored_type_sql(type_sql);
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        match turso_core::Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        ) {
            Ok(_) => panic!("open must fail for stored type SQL {type_sql}"),
            Err(err) => err.to_string(),
        }
    }

    /// Creates type `r` and a table `t(a r)` holding one row, then replaces
    /// the stored SQL of `r` with `type_sql`, as an earlier version could
    /// have stored it.
    fn database_with_stored_type_sql(type_sql: &str) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_stored_type_sql.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE TYPE r(value any) BASE text ENCODE value DECODE CAST(value AS real)",
            )
            .unwrap();
            conn.execute("CREATE TABLE t(a r) STRICT").unwrap();
            conn.execute("INSERT INTO t VALUES ('10')").unwrap();

            conn.start_nested();
            let stmt = conn.prepare(format!(
                "UPDATE __turso_internal_types SET sql = '{type_sql}' WHERE name = 'r'"
            ));
            conn.end_nested();
            stmt.unwrap().run_ignore_rows().unwrap();

            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.close().unwrap();
        }
        (temp_dir, path)
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
                Arc::new(SqliteDialect),
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

    /// CREATE TYPE refuses a DECODE whose kind of value it can't tell. Every
    /// other type's column compares like a plain column of the kind its DECODE
    /// gives, holding the decoded values: with BASE text and DECODE
    /// CAST(value AS real), '10.0' > 9.6 although the text '10.0' < '9.6'.
    #[test]
    fn test_decode_kind_decides_comparisons() {
        // Each DECODE, and the column type of a plain table that compares the
        // same way, or None when CREATE TYPE must refuse it.
        let shapes: &[(&str, Option<&str>)] = &[
            ("json_extract(value, '$')", None),
            ("CAST(json_extract(value, '$') AS real)", Some("REAL")),
            ("CAST(value AS real)", Some("REAL")),
            ("CAST(value AS integer)", Some("INTEGER")),
            ("CAST(value AS numeric)", Some("NUMERIC")),
            ("CASE WHEN value = '' THEN 0 ELSE value END", None),
            (
                "CASE WHEN value = '' THEN 0.0 ELSE CAST(value AS real) END",
                Some("REAL"),
            ),
            ("coalesce(value, 0)", None),
            ("coalesce(CAST(value AS integer), 0)", Some("INTEGER")),
            ("iif(value = '', NULL, CAST(value AS real))", Some("REAL")),
            ("ifnull(value, 'x')", Some("TEXT")),
            ("lower(value)", Some("TEXT")),
            ("value * 1.0", Some("REAL")),
            ("round(value, 2)", Some("REAL")),
            ("value + 0", Some("NUMERIC")),
            ("NULL", None),
            ("CAST(value AS int_text)", Some("INTEGER")),
            ("CAST(value AS real_domain)", Some("REAL")),
            ("CAST(value AS numeric(10, 2))", None),
        ];
        // Valid JSON, so that json_extract accepts each of them.
        let values = [
            "'10.0'", "'9.5'", "'100'", "'2e1'", "'-3'", "'0.50'", "NULL",
        ];
        let literals = ["9.6", "10", "'10'", "0", "-3", "'9.5'"];

        for mvcc in [false, true] {
            let db = TempDatabase::builder()
                .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
                .with_mvcc(mvcc)
                .build();
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE TYPE int_text(value any) BASE text \
                 ENCODE CAST(CAST(value AS integer) AS text) DECODE CAST(value AS integer)",
            )
            .unwrap();
            conn.execute(
                "CREATE TYPE real_text(value any) BASE text \
                 ENCODE CAST(CAST(value AS real) AS text) DECODE CAST(value AS real)",
            )
            .unwrap();
            conn.execute("CREATE DOMAIN real_domain AS real_text")
                .unwrap();

            for (i, (decode, kind)) in shapes.iter().enumerate() {
                let context = format!("mvcc={mvcc}, DECODE {decode}");
                let create_type = format!(
                    "CREATE TYPE k{i}(value any) BASE text \
                     ENCODE CAST({decode} AS text) DECODE {decode}"
                );
                let Some(kind) = kind else {
                    assert_that!(conn.execute(&create_type))
                        .named(&context)
                        .err()
                        .display_string()
                        .contains("wrap it in CAST(... AS <type>)");
                    continue;
                };
                conn.execute(&create_type)
                    .unwrap_or_else(|e| panic!("{context}: {e}"));
                conn.execute(format!(
                    "CREATE TABLE t{i}(id INTEGER PRIMARY KEY, c k{i}) STRICT"
                ))
                .unwrap();
                for (id, value) in values.iter().enumerate() {
                    conn.execute(format!("INSERT INTO t{i} VALUES ({id}, {value})"))
                        .unwrap_or_else(|e| panic!("{context}: {e}"));
                }
                conn.execute(format!(
                    "CREATE TABLE p{i}(id INTEGER PRIMARY KEY, c {kind})"
                ))
                .unwrap();
                conn.execute(format!("INSERT INTO p{i} SELECT id, c FROM t{i}"))
                    .unwrap();

                let mut conditions = Vec::new();
                for literal in literals {
                    for op in ["<", "=", ">"] {
                        conditions.push(format!("c {op} {literal}"));
                    }
                    conditions.push(format!("c IN ({literal}, 100)"));
                }
                let compare_with_plain_table = || {
                    for condition in &conditions {
                        let got: Vec<(i64,)> = conn.exec_rows(&format!(
                            "SELECT id FROM t{i} WHERE {condition} ORDER BY id"
                        ));
                        let expected: Vec<(i64,)> = conn.exec_rows(&format!(
                            "SELECT id FROM p{i} WHERE {condition} ORDER BY id"
                        ));
                        assert_eq!(got, expected, "{context}: WHERE {condition}");
                    }
                    let got: Vec<(i64, i64)> = conn.exec_rows(&format!(
                        "SELECT a.id, b.id FROM t{i} a, t{i} b WHERE a.c < b.c ORDER BY 1, 2"
                    ));
                    let expected: Vec<(i64, i64)> = conn.exec_rows(&format!(
                        "SELECT a.id, b.id FROM p{i} a, p{i} b WHERE a.c < b.c ORDER BY 1, 2"
                    ));
                    assert_eq!(got, expected, "{context}: a.c < b.c");
                };
                compare_with_plain_table();
            }
            conn.close().unwrap();
        }
    }

    /// A type whose DECODE casts to another type reads as that type's kind:
    /// here `a` reads as a real, because `b`'s DECODE gives a real, although
    /// both store text. This holds after reopening too, when the kinds are
    /// worked out as the types load.
    #[test]
    fn test_decode_kind_follows_the_type_it_casts_to() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("decode_kind_cast_type.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        // As reals, 10.0 > 9.6 > 9.0. As text, '10' < '9' < '9.6'.
        let check = |conn: &Arc<turso_core::Connection>, context: &str| {
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE c < 9.6 ORDER BY id");
            assert_eq!(rows, vec![(2,)], "{context}: WHERE c < 9.6");
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE c > 9 ORDER BY id");
            assert_eq!(rows, vec![(1,)], "{context}: WHERE c > 9");
        };
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE TYPE b(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS real)",
            )
            .unwrap();
            conn.execute(
                "CREATE TYPE a(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS b)",
            )
            .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c a) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, '10'), (2, '9')")
                .unwrap();
            check(&conn, "same session");
            conn.close().unwrap();
        }
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        check(&conn, "after reopen");
        conn.close().unwrap();
    }

    /// Every way of storing a value of a custom type stores what a STRICT column
    /// of the type's BASE stores when given the ENCODE result: the same kind and
    /// value, or an error naming the same kind. CAST gives that value too.
    #[test]
    fn test_stored_value_has_the_kind_of_the_base() {
        // ENCODE results for the input 7, one per kind a value can have.
        let encodes = [
            "value",
            "value + 0.0",
            "value + 0.5",
            "value / 3.0",
            "value + 9007199254740986",
            "CAST(value AS text)",
            "CAST(value AS text) || '.0'",
            "'x' || value",
            "CAST(value AS blob)",
        ];
        let bases = ["integer", "real", "text", "blob"];
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();

        let mut mismatches = vec![];
        for (e, encode) in encodes.iter().enumerate() {
            for base in bases {
                let ty = format!("t_{e}_{base}");
                conn.execute(format!(
                    "CREATE TYPE {ty}(value any) BASE {base} ENCODE {encode} DECODE value"
                ))
                .unwrap();
                let plain_value = encode.replace("value", "7");
                let expected = stored_kind_and_value(
                    &conn,
                    &[
                        &format!("CREATE TABLE plain_{ty}(c {base}) STRICT"),
                        &format!("INSERT INTO plain_{ty} VALUES ({plain_value})"),
                    ],
                    &format!("SELECT typeof(c), quote(c) FROM plain_{ty}"),
                );
                let table_with_c = |prefix: &str, column: &str| {
                    format!("CREATE TABLE {prefix}_{ty}(id INTEGER PRIMARY KEY, {column}) STRICT")
                };
                let select =
                    |prefix: &str| format!("SELECT typeof(c), quote(c) FROM {prefix}_{ty}");
                let paths: Vec<(&str, Vec<String>, String)> = vec![
                    (
                        "INSERT",
                        vec![
                            table_with_c("ins", &format!("c {ty}")),
                            format!("INSERT INTO ins_{ty} VALUES (1, 7)"),
                        ],
                        select("ins"),
                    ),
                    (
                        "UPDATE",
                        vec![
                            table_with_c("upd", &format!("c {ty}")),
                            format!("INSERT INTO upd_{ty} VALUES (1, NULL)"),
                            format!("UPDATE upd_{ty} SET c = 7"),
                        ],
                        select("upd"),
                    ),
                    (
                        "UPSERT",
                        vec![
                            table_with_c("ups", &format!("c {ty}")),
                            format!("INSERT INTO ups_{ty} VALUES (1, NULL)"),
                            format!(
                                "INSERT INTO ups_{ty} VALUES (1, NULL) \
                                 ON CONFLICT(id) DO UPDATE SET c = 7"
                            ),
                        ],
                        select("ups"),
                    ),
                    (
                        "DEFAULT",
                        vec![
                            table_with_c("def", &format!("c {ty} DEFAULT 7")),
                            format!("INSERT INTO def_{ty}(id) VALUES (1)"),
                        ],
                        select("def"),
                    ),
                    (
                        "ADD COLUMN DEFAULT",
                        vec![
                            format!("CREATE TABLE add_{ty}(id INTEGER PRIMARY KEY) STRICT"),
                            format!("INSERT INTO add_{ty} VALUES (1)"),
                            format!("ALTER TABLE add_{ty} ADD COLUMN c {ty} DEFAULT 7"),
                        ],
                        select("add"),
                    ),
                    (
                        "ALTER COLUMN",
                        vec![
                            table_with_c("alt", "c integer"),
                            format!("INSERT INTO alt_{ty} VALUES (1, 7)"),
                            format!("ALTER TABLE alt_{ty} ALTER COLUMN c TO d {ty}"),
                        ],
                        format!("SELECT typeof(d), quote(d) FROM alt_{ty}"),
                    ),
                    (
                        "virtual generated column",
                        vec![
                            table_with_c("gen", &format!("a integer, c {ty} AS (a)")),
                            format!("INSERT INTO gen_{ty}(id, a) VALUES (1, 7)"),
                        ],
                        select("gen"),
                    ),
                    (
                        "domain",
                        vec![
                            format!("CREATE DOMAIN dom_{ty} AS {ty}"),
                            table_with_c("dom", &format!("c dom_{ty}")),
                            format!("INSERT INTO dom_{ty} VALUES (1, 7)"),
                        ],
                        select("dom"),
                    ),
                    (
                        "CAST",
                        vec![],
                        format!("SELECT typeof(CAST(7 AS {ty})), quote(CAST(7 AS {ty}))"),
                    ),
                ];
                for (path, setup, select) in paths {
                    let setup: Vec<&str> = setup.iter().map(String::as_str).collect();
                    let actual = stored_kind_and_value(&conn, &setup, &select);
                    if actual != expected {
                        mismatches.push(format!(
                            "ENCODE {encode}, BASE {base}, {path}: expected {expected:?}, got {actual:?}"
                        ));
                    }
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    /// Runs `setup`, then `select`, which returns the stored kind and value. An
    /// error is reduced to the "cannot store KIND value" part of its message, so
    /// errors about the same kind compare equal whatever column or type they name.
    fn stored_kind_and_value(
        conn: &Arc<turso_core::Connection>,
        setup: &[&str],
        select: &str,
    ) -> Result<(String, String), String> {
        let run = || -> turso_core::Result<(String, String)> {
            for sql in setup {
                conn.execute(sql)?;
            }
            let mut stmt = conn.prepare(select)?;
            let mut rows = vec![];
            stmt.run_with_row_callback(|row| {
                rows.push((row.get::<String>(0)?, row.get::<String>(1)?));
                Ok(())
            })?;
            assert_eq!(rows.len(), 1, "{select}");
            Ok(rows.remove(0))
        };
        run().map_err(|err| {
            let message = err.to_string();
            match message.find("cannot store ") {
                Some(start) => message[start..]
                    .split(" value")
                    .next()
                    .unwrap_or_default()
                    .to_string(),
                None => message,
            }
        })
    }

    /// Only text types take a COLLATE, as in PostgreSQL. Tries every kind of
    /// column type in every place a COLLATE can be written: an array or a
    /// custom type that isn't text refuses it, and every other type behaves
    /// as before.
    #[test]
    fn test_collate_only_on_text_types() {
        enum Collate {
            Accepted,
            /// Refused with an error that names this type.
            Refused(&'static str),
        }
        struct Kind {
            ty: &'static str,
            strict: bool,
            stored: &'static str,
            /// A value that equals `stored` under NOCASE.
            probe: &'static str,
            collate: Collate,
        }
        let kinds = [
            Kind {
                ty: "text",
                strict: true,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "integer",
                strict: true,
                stored: "1",
                probe: "1",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "date",
                strict: false,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "varchar(10)",
                strict: true,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "label",
                strict: true,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "short_text",
                strict: true,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "plain_text",
                strict: true,
                stored: "'ABC'",
                probe: "'abc'",
                collate: Collate::Accepted,
            },
            Kind {
                ty: "date",
                strict: true,
                stored: "'2024-01-02'",
                probe: "'2024-01-02'",
                collate: Collate::Refused("date"),
            },
            Kind {
                ty: "uuid",
                strict: true,
                stored: "'01945ca0-3189-76c0-9a8f-caf310fc8b8e'",
                probe: "'01945ca0-3189-76c0-9a8f-caf310fc8b8e'",
                collate: Collate::Refused("uuid"),
            },
            Kind {
                ty: "cents",
                strict: true,
                stored: "1",
                probe: "1",
                collate: Collate::Refused("cents"),
            },
            Kind {
                ty: "day",
                strict: true,
                stored: "'2024-01-02'",
                probe: "'2024-01-02'",
                collate: Collate::Refused("day"),
            },
            Kind {
                ty: "integer[]",
                strict: true,
                stored: "ARRAY[1]",
                probe: "ARRAY[1]",
                collate: Collate::Refused("integer[]"),
            },
            Kind {
                ty: "text[]",
                strict: true,
                stored: "ARRAY['ABC']",
                probe: "ARRAY['abc']",
                collate: Collate::Refused("text[]"),
            },
            Kind {
                ty: "date[]",
                strict: true,
                stored: "ARRAY['2024-01-02']",
                // A bare ARRAY['2024-01-02'] is a text[].
                probe: "CAST(ARRAY['2024-01-02'] AS date[])",
                collate: Collate::Refused("date[]"),
            },
        ];
        // (statement, whether it returns the count 1 when accepted). `{n}`
        // keeps the names of created objects apart.
        let places = [
            (
                "SELECT count(*) FROM t WHERE c = {probe} COLLATE NOCASE",
                true,
            ),
            (
                "SELECT count(*) FROM t WHERE c COLLATE NOCASE = {probe}",
                true,
            ),
            (
                "SELECT count(*) FROM t WHERE c COLLATE NOCASE IN ({probe})",
                true,
            ),
            // An IN list compares with the collation of its left side only.
            (
                "SELECT count(*) FROM t WHERE c IN ({probe} COLLATE NOCASE)",
                false,
            ),
            (
                "SELECT count(*) FROM t WHERE c IN (SELECT {probe} COLLATE NOCASE)",
                true,
            ),
            ("SELECT c COLLATE NOCASE FROM t", false),
            ("SELECT c FROM t ORDER BY c COLLATE NOCASE", false),
            (
                "SELECT c FROM t UNION SELECT c FROM t ORDER BY c COLLATE NOCASE",
                false,
            ),
            ("SELECT count(*) FROM t GROUP BY c COLLATE NOCASE", true),
            ("SELECT DISTINCT c COLLATE NOCASE FROM t", false),
            ("CREATE TABLE t{n}(c {ty} COLLATE NOCASE){strict}", false),
            (
                "CREATE TABLE t{n}(c {ty}, UNIQUE(c COLLATE NOCASE)){strict}",
                false,
            ),
            // Two key columns, because a key of only an integer[] column is
            // taken for a rowid alias (#11315).
            (
                "CREATE TABLE t{n}(c {ty}, k integer, PRIMARY KEY(c COLLATE NOCASE, k)){strict}",
                false,
            ),
            ("CREATE INDEX i{n} ON t(c COLLATE NOCASE)", false),
            ("ALTER TABLE t ADD COLUMN c{n} {ty} COLLATE NOCASE", false),
        ];
        for kind in &kinds {
            let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
            let db = TempDatabase::builder().with_opts(opts).build();
            let conn = db.connect_limbo();
            let strict = if kind.strict { " STRICT" } else { "" };
            conn.execute(
                "CREATE TYPE label(value text) BASE text ENCODE value DECODE value OPERATOR '<';
                 CREATE TYPE cents(value integer) BASE integer ENCODE value * 100 DECODE value / 100 OPERATOR '<';
                 CREATE DOMAIN short_text AS varchar;
                 CREATE DOMAIN plain_text AS text;
                 CREATE DOMAIN day AS date;",
            )
            .unwrap();
            conn.execute(format!("CREATE TABLE t(c {}){strict}", kind.ty))
                .unwrap();
            conn.execute(format!("INSERT INTO t VALUES ({})", kind.stored))
                .unwrap();
            for (n, (place, counts_rows)) in places.iter().enumerate() {
                let sql = place
                    .replace("{n}", &n.to_string())
                    .replace("{ty}", kind.ty)
                    .replace("{probe}", kind.probe)
                    .replace("{strict}", strict);
                let context = format!("type {} (strict={}): {sql}", kind.ty, kind.strict);
                let refused_by = match kind.collate {
                    Collate::Accepted => None,
                    Collate::Refused(name) => Some(name),
                };
                match refused_by {
                    Some(name) => {
                        let error = match conn.prepare(&sql) {
                            Ok(_) => panic!("{context}: expected an error"),
                            Err(error) => error.to_string(),
                        };
                        assert!(
                            error.contains(&format!("collations are not supported by type {name}")),
                            "{context}: {error}"
                        );
                    }
                    None if *counts_rows => {
                        let rows: Vec<(i64,)> = conn.exec_rows(&sql);
                        assert_eq!(rows, vec![(1,)], "{context}");
                    }
                    None => {
                        if let Err(error) = conn.execute(&sql) {
                            panic!("{context}: {error}");
                        }
                    }
                }
            }
        }
    }

    /// Every statement that copies a row must copy custom-type values as
    /// stored. Running DECODE and then ENCODE again changes the value of a
    /// type whose DECODE does not undo its ENCODE, like `plus1` and `x100`.
    #[test]
    fn test_row_copies_keep_custom_type_values() {
        let mut changed = Vec::new();
        for copied_type in COPIED_TYPES {
            for row_copy in [
                RowCopy::Vacuum,
                RowCopy::VacuumInto,
                RowCopy::DropColumn,
                RowCopy::OnUpdateCascade,
            ] {
                if row_copy == RowCopy::OnUpdateCascade && copied_type.new_key.is_none() {
                    continue;
                }
                let column_type = copied_type.column_type;
                match copy_custom_type_value(copied_type, row_copy) {
                    Ok((before, after)) if before == after => {}
                    Ok((before, after)) => {
                        changed.push(format!("{row_copy:?} {column_type}: {before} -> {after}"))
                    }
                    Err(e) => changed.push(format!("{row_copy:?} {column_type}: {e}")),
                }
            }
        }
        assert!(
            changed.is_empty(),
            "row copies changed values:\n{}",
            changed.join("\n")
        );
    }

    const CREATE_COPIED_TYPES: &[&str] = &[
        "CREATE TYPE plus1 BASE integer ENCODE value + 1 DECODE value",
        "CREATE TYPE x100 BASE integer ENCODE value * 100 DECODE value",
        "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
        "CREATE TYPE shout BASE text ENCODE upper(value) DECODE lower(value)",
    ];

    // Arrays and types with a DECODE other than `value` can't be keys, so
    // they have no ON UPDATE CASCADE copy.
    const COPIED_TYPES: &[CopiedType] = &[
        CopiedType::new("plus1", "5", Some("9")),
        CopiedType::new("x100", "1", Some("2")),
        CopiedType::new("cents", "42", None),
        CopiedType::new("shout", "'Hi'", None),
        CopiedType::new("plus1[]", "ARRAY[1, 2]", None),
        CopiedType::new("shout[]", "ARRAY['a', 'B']", None),
    ];

    /// A custom-type column and the values to write to it.
    struct CopiedType {
        column_type: &'static str,
        value: &'static str,
        /// The parent key's new value in the ON UPDATE CASCADE copy, or
        /// `None` if the type can't be a key.
        new_key: Option<&'static str>,
    }

    impl CopiedType {
        const fn new(
            column_type: &'static str,
            value: &'static str,
            new_key: Option<&'static str>,
        ) -> Self {
            Self {
                column_type,
                value,
                new_key,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RowCopy {
        Vacuum,
        VacuumInto,
        DropColumn,
        OnUpdateCascade,
    }

    /// Writes a value of `copied_type`, copies it with `row_copy` and returns
    /// the value read before and after the copy. For ON UPDATE CASCADE these
    /// are the parent's new key and the child's copy of it.
    fn copy_custom_type_value(
        copied_type: &CopiedType,
        row_copy: RowCopy,
    ) -> turso_core::Result<(String, String)> {
        let opts = turso_core::DatabaseOpts::new()
            .with_custom_types(true)
            .with_vacuum(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        for sql in CREATE_COPIED_TYPES {
            conn.execute(sql)?;
        }
        let CopiedType {
            column_type,
            value,
            new_key,
        } = copied_type;
        match row_copy {
            RowCopy::Vacuum | RowCopy::VacuumInto => {
                conn.execute(format!(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, v {column_type}) STRICT"
                ))?;
                conn.execute(format!("INSERT INTO t VALUES (1, {value})"))?;
                let before = first_value(&conn, "SELECT v FROM t")?;
                if row_copy == RowCopy::Vacuum {
                    conn.execute("VACUUM")?;
                    return Ok((before, first_value(&conn, "SELECT v FROM t")?));
                }
                let dest_dir = TempDir::new().unwrap();
                let dest_path = dest_dir.path().join("copy.db");
                conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
                let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
                let dest_conn = dest_db.connect_limbo();
                Ok((before, first_value(&dest_conn, "SELECT v FROM t")?))
            }
            RowCopy::DropColumn => {
                conn.execute(format!(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, dropped INTEGER, v {column_type}) STRICT"
                ))?;
                conn.execute(format!("INSERT INTO t VALUES (1, 0, {value})"))?;
                let before = first_value(&conn, "SELECT v FROM t")?;
                conn.execute("ALTER TABLE t DROP COLUMN dropped")?;
                Ok((before, first_value(&conn, "SELECT v FROM t")?))
            }
            RowCopy::OnUpdateCascade => {
                conn.execute("PRAGMA foreign_keys = ON")?;
                conn.execute(format!(
                    "CREATE TABLE parent(k {column_type} PRIMARY KEY) STRICT"
                ))?;
                conn.execute(format!(
                    "CREATE TABLE child(id INTEGER PRIMARY KEY, \
                     k {column_type} REFERENCES parent(k) ON UPDATE CASCADE) STRICT"
                ))?;
                conn.execute(format!("INSERT INTO parent VALUES ({value})"))?;
                conn.execute(format!("INSERT INTO child VALUES (1, {value})"))?;
                let new_key = new_key.expect("only key types are cascaded");
                conn.execute(format!("UPDATE parent SET k = {new_key}"))?;
                Ok((
                    first_value(&conn, "SELECT k FROM parent")?,
                    first_value(&conn, "SELECT k FROM child")?,
                ))
            }
        }
    }

    fn first_value(conn: &Arc<turso_core::Connection>, query: &str) -> turso_core::Result<String> {
        let mut stmt = conn.prepare(query)?;
        let mut value = None;
        stmt.run_with_row_callback(|row| {
            value.get_or_insert(row.get::<&turso_core::Value>(0)?.to_string());
            Ok(())
        })?;
        Ok(value.unwrap_or_else(|| panic!("{query} returned no row")))
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
}
