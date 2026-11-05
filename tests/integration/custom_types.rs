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
                && err.contains("cannot tell the kinds of type r: it names itself"),
            "unexpected error: {err}"
        );
    }

    /// CREATE DOMAIN refuses a base type that is neither a STRICT column type
    /// nor a registered type, so a stored one must not open as a guessed kind.
    #[test]
    fn test_reopen_fails_when_stored_domain_is_based_on_an_unknown_name() {
        let err = reopen_after_storing_type_sql("CREATE DOMAIN r AS foo");
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell what kind of value type r stores"),
            "unexpected error: {err}"
        );
    }

    /// An earlier version could store a DECODE that reads a type parameter.
    /// A parameter holds the argument written in the column's type, which
    /// is not converted to the parameter's type, so its kind is not known.
    #[test]
    fn test_reopen_fails_when_stored_decode_reads_a_type_parameter() {
        let err = reopen_after_storing_type_sql(
            "CREATE TYPE r(value any, p integer) BASE text ENCODE value DECODE p",
        );
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell what kind of value DECODE of type r returns"),
            "unexpected error: {err}"
        );
    }

    /// An earlier version could store a type whose ENCODE casts to itself.
    /// Its kinds would depend on themselves, so the database does not open.
    #[test]
    fn test_reopen_fails_when_stored_type_casts_to_itself() {
        let err = reopen_after_storing_type_sql(
            "CREATE TYPE r BASE integer ENCODE CAST(value AS r) DECODE value",
        );
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell the kinds of type r: it names itself"),
            "unexpected error: {err}"
        );
    }

    /// CREATE TYPE refuses a type whose name another type already casts to,
    /// so two types casting to each other can only come from stored SQL.
    #[test]
    fn test_reopen_fails_when_stored_types_cast_to_each_other() {
        let (_temp_dir, path) = database_with_stored_types_a_and_b(
            "CREATE TYPE a(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS b)",
            "CREATE TYPE b(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS a)",
        );
        let err = open_error(&path);
        assert!(
            err.contains("malformed database schema")
                && err.contains("cannot tell the kinds of types a, b: they name each other"),
            "unexpected error: {err}"
        );
    }

    /// The kinds of a type stored before a type its DECODE casts to are
    /// worked out after that type's.
    #[test]
    fn test_reopen_with_type_stored_before_the_type_it_casts_to() {
        let (_temp_dir, path) = database_with_stored_types_a_and_b(
            "CREATE TYPE a(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS b)",
            "CREATE TYPE b(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS integer)",
        );
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        conn.start_nested();
        let names: Vec<(String,)> = conn.exec_rows(
            "SELECT name FROM __turso_internal_types WHERE name IN ('a', 'b') ORDER BY rowid",
        );
        conn.end_nested();
        assert_eq!(names, vec![("a".to_string(),), ("b".to_string(),)]);
        // a reads as b's kind, integer, so 10 > 9 although the text '10' < '9'.
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE c > 9");
        assert_eq!(rows, vec![(1,)]);
    }

    /// A stored expression of an attached database counts when creating a
    /// type it casts to, as it does in main: creating b would change what the
    /// index on CAST(x AS b) gives.
    #[test]
    fn test_index_in_attached_database_that_casts_to_a_new_type() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let main_path = temp_dir.path().join("main.db");
        let custom_types = turso_core::DatabaseOpts::new().with_custom_types(true);
        {
            let db = TempDatabase::new_with_existent_with_opts(&aux_path, custom_types);
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE t(x text)").unwrap();
            conn.execute("CREATE INDEX i ON t(CAST(x AS b))").unwrap();
            conn.close().unwrap();
        }
        let opts = custom_types.with_attach(true);
        let db = TempDatabase::new_with_existent_with_opts(&main_path, opts);
        let conn = db.connect_limbo();
        conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))
            .unwrap();
        let err = conn
            .execute(
                "CREATE TYPE b(value any) BASE text ENCODE CAST(value AS text) DECODE lower(value)",
            )
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot create type b: index i already casts to it"),
            "unexpected error: {err}"
        );
    }

    /// Statements look custom types up in main only. An attached database
    /// with a type of its own would have its values read with main's type of
    /// the same name: aux stores 5 cents as 500, and main's cents would read
    /// 500 back as 71. So ATTACH refuses it.
    #[test]
    fn test_attach_refuses_database_with_its_own_type() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let main_path = temp_dir.path().join("main.db");
        let custom_types = turso_core::DatabaseOpts::new().with_custom_types(true);
        {
            let db = TempDatabase::new_with_existent_with_opts(&aux_path, custom_types);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
                .unwrap();
            conn.execute("CREATE TABLE t(c cents) STRICT").unwrap();
            conn.execute("INSERT INTO t VALUES (5)").unwrap();
            conn.close().unwrap();
        }
        let db =
            TempDatabase::new_with_existent_with_opts(&main_path, custom_types.with_attach(true));
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 7 DECODE value / 7")
            .unwrap();
        let err = conn
            .execute(format!("ATTACH '{}' AS aux", aux_path.display()))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "database aux has type cents: custom types and domains can only be used in the main database"
            ),
            "unexpected error: {err}"
        );
        let err = conn
            .execute("INSERT INTO aux.t VALUES (9)")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no such table: aux.t"),
            "unexpected error: {err}"
        );
    }

    /// A database that gains a type after it was attached is refused by the
    /// next statement that uses it, before a value of the type is read or
    /// written with main's type of the same name.
    #[test]
    fn test_attached_database_that_gains_a_type_is_refused() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let main_path = temp_dir.path().join("main.db");
        let custom_types = turso_core::DatabaseOpts::new().with_custom_types(true);
        let aux_db = TempDatabase::new_with_existent_with_opts(&aux_path, custom_types);
        let aux_conn = aux_db.connect_limbo();
        aux_conn
            .execute("CREATE TABLE plain(x integer) STRICT")
            .unwrap();

        let db =
            TempDatabase::new_with_existent_with_opts(&main_path, custom_types.with_attach(true));
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 7 DECODE value / 7")
            .unwrap();
        conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM aux.plain");
        assert_eq!(rows, vec![(0,)]);

        aux_conn
            .execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        aux_conn.execute("CREATE TABLE t(c cents) STRICT").unwrap();
        aux_conn.execute("INSERT INTO t VALUES (5)").unwrap();

        for sql in ["SELECT c FROM aux.t", "INSERT INTO aux.t VALUES (9)"] {
            let err = conn.execute(sql).unwrap_err().to_string();
            assert!(
                err.contains(
                    "database aux has type cents: custom types and domains can only be used in the main database"
                ),
                "unexpected error for {sql}: {err}"
            );
        }
        let rows: Vec<(i64,)> = aux_conn.exec_rows("SELECT c FROM t");
        assert_eq!(rows, vec![(5,)]);
        conn.execute("DETACH aux").unwrap();
    }

    /// A table of an attached or the temp database can't have a column of a
    /// type of main, as its values would be read with whatever type of that
    /// name main has when the database is used.
    #[test]
    fn test_tables_outside_main_cannot_use_types_of_main() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let main_path = temp_dir.path().join("main.db");
        let custom_types = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db =
            TempDatabase::new_with_existent_with_opts(&main_path, custom_types.with_attach(true));
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
            .unwrap();
        conn.execute("CREATE DOMAIN pos AS integer CHECK (value > 0)")
            .unwrap();
        conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))
            .unwrap();
        conn.execute("CREATE TABLE aux.t(x integer) STRICT")
            .unwrap();
        conn.execute("CREATE TEMP TABLE tt(x integer) STRICT")
            .unwrap();

        for (sql, column, database, ty) in [
            ("CREATE TABLE aux.u(c cents) STRICT", "u.c", "aux", "cents"),
            (
                "CREATE TABLE aux.u(c cents[]) STRICT",
                "u.c",
                "aux",
                "cents",
            ),
            ("CREATE TABLE aux.u(p pos) STRICT", "u.p", "aux", "pos"),
            (
                "CREATE TEMP TABLE u(c cents) STRICT",
                "u.c",
                "temp",
                "cents",
            ),
            (
                "ALTER TABLE aux.t ADD COLUMN c cents",
                "t.c",
                "aux",
                "cents",
            ),
            (
                "ALTER TABLE aux.t ALTER COLUMN x TO y cents",
                "t.y",
                "aux",
                "cents",
            ),
            ("ALTER TABLE tt ADD COLUMN c cents", "tt.c", "temp", "cents"),
        ] {
            let err = conn.execute(sql).unwrap_err().to_string();
            let expected = format!(
                "column {column} of database {database} cannot have type {ty}: custom types and domains can only be used in the main database"
            );
            assert!(err.contains(&expected), "unexpected error for {sql}: {err}");
        }
        let err = conn
            .execute("CREATE TYPE aux.c2 BASE integer ENCODE value DECODE value")
            .unwrap_err()
            .to_string();
        // CREATE TYPE takes a plain name, so a type is always created in main.
        assert!(err.contains("expected BASE keyword"), "{err}");
    }

    /// Built-in types are the same in every database, so tables of attached
    /// and temp databases can use them.
    #[test]
    fn test_builtin_types_work_outside_main() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let main_path = temp_dir.path().join("main.db");
        let custom_types = turso_core::DatabaseOpts::new().with_custom_types(true);
        {
            let db = TempDatabase::new_with_existent_with_opts(&aux_path, custom_types);
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE t(j jsonb) STRICT").unwrap();
            conn.execute(r#"INSERT INTO t VALUES ('{"a":1}')"#).unwrap();
            conn.close().unwrap();
        }
        let db =
            TempDatabase::new_with_existent_with_opts(&main_path, custom_types.with_attach(true));
        let conn = db.connect_limbo();
        conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))
            .unwrap();
        conn.execute(r#"INSERT INTO aux.t VALUES ('{"b":2}')"#)
            .unwrap();
        conn.execute("CREATE TABLE aux.u(j jsonb, n numeric(5, 2)) STRICT")
            .unwrap();
        conn.execute(r#"INSERT INTO aux.u VALUES ('{"c":3}', 1.5)"#)
            .unwrap();
        conn.execute("CREATE TEMP TABLE tt(j jsonb) STRICT")
            .unwrap();
        conn.execute(r#"INSERT INTO tt VALUES ('{"d":4}')"#)
            .unwrap();

        let rows: Vec<(String,)> = conn.exec_rows("SELECT j FROM aux.t ORDER BY rowid");
        assert_eq!(rows, vec![(r#"{"a":1}"#.into(),), (r#"{"b":2}"#.into(),)]);
        let rows: Vec<(String, String)> = conn.exec_rows("SELECT j, n FROM aux.u");
        assert_eq!(rows, vec![(r#"{"c":3}"#.into(), "1.50".into())]);
        let rows: Vec<(String,)> = conn.exec_rows("SELECT j FROM tt");
        assert_eq!(rows, vec![(r#"{"d":4}"#.into(),)]);

        conn.execute("DETACH aux").unwrap();
        conn.close().unwrap();
        let db = TempDatabase::new_with_existent_with_opts(&aux_path, custom_types);
        let conn = db.connect_limbo();
        let rows: Vec<(String,)> = conn.exec_rows("SELECT j FROM t ORDER BY rowid");
        assert_eq!(rows, vec![(r#"{"a":1}"#.into(),), (r#"{"b":2}"#.into(),)]);
    }

    /// Returns the error from opening a database storing `type_sql` as the
    /// SQL of type `r` (see database_with_stored_type_sql).
    fn reopen_after_storing_type_sql(type_sql: &str) -> String {
        let (_temp_dir, path) = database_with_stored_type_sql(type_sql);
        open_error(&path)
    }

    fn open_error(path: &std::path::Path) -> String {
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
            Ok(_) => panic!("open must fail for {}", path.display()),
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
            replace_stored_type_sql(&conn, "r", type_sql);
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.close().unwrap();
        }
        (temp_dir, path)
    }

    /// Creates types `a` and `b`, in that order, and a table
    /// `t(id INTEGER PRIMARY KEY, c a)` holding the text values '10' and '9'.
    /// Then replaces the stored SQL of the types with `a_sql` and `b_sql`.
    fn database_with_stored_types_a_and_b(
        a_sql: &str,
        b_sql: &str,
    ) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("custom_types_stored_types_a_and_b.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            for name in ["a", "b"] {
                conn.execute(format!(
                    "CREATE TYPE {name}(value any) BASE text ENCODE CAST(value AS text) DECODE CAST(value AS text)"
                ))
                .unwrap();
            }
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c a) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, '10'), (2, '9')")
                .unwrap();
            replace_stored_type_sql(&conn, "a", a_sql);
            replace_stored_type_sql(&conn, "b", b_sql);
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.close().unwrap();
        }
        (temp_dir, path)
    }

    fn replace_stored_type_sql(conn: &Arc<turso_core::Connection>, name: &str, type_sql: &str) {
        conn.start_nested();
        let stmt = conn.prepare(format!(
            "UPDATE __turso_internal_types SET sql = '{type_sql}' WHERE name = '{name}'"
        ));
        conn.end_nested();
        stmt.unwrap().run_ignore_rows().unwrap();
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

    /// An array column whose stored value is not an array is a damaged
    /// database. Reading it through DECODE must fail, not loop forever.
    #[test]
    fn test_decode_of_array_column_holding_a_non_array_fails() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("array_column_holding_integer.db");
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute("CREATE TYPE d1(value integer) BASE integer DECODE value - 1")
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b integer) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

            // Turso has no writable_schema pragma. Use the same schema-write
            // bypass as VACUUM during prepare, then execute normally.
            conn.start_nested();
            let stmt = conn.prepare(
                "UPDATE sqlite_schema \
                 SET sql = 'CREATE TABLE t(id INTEGER PRIMARY KEY, b d1[]) STRICT' \
                 WHERE name = 't'",
            );
            conn.end_nested();
            stmt.unwrap().run_ignore_rows().unwrap();
            conn.close().unwrap();
        }

        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let mut stmt = conn.prepare("SELECT b FROM t").unwrap();
        assert_that!(stmt.run_collect_rows())
            .err()
            .display_string()
            .contains("expected an array, but the value is not an array");
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

    /// A RANGE frame with an offset over a custom type takes only the offset
    /// types PostgreSQL's `in_range` functions take: integers over smallint
    /// and bigint, integers and numerics over numeric, and nothing over any
    /// other custom type. Tries every kind of column type with every kind of
    /// offset, as a PRECEDING and a FOLLOWING bound, at the start and end of
    /// the frame, in both orders. An accepted frame counts the same rows as a
    /// subquery that applies the offset with `+` and `-`. SQLite's own types
    /// keep SQLite's rules.
    #[test]
    fn test_range_offsets_only_on_types_postgres_allows() {
        /// What kind of number a type is to PostgreSQL's `in_range` functions.
        #[derive(Clone, Copy, PartialEq)]
        enum Number {
            Integer,
            Numeric,
            None,
        }
        /// The `in_range` pairs PostgreSQL has for numbers: an integer column
        /// takes integer offsets, and a numeric column takes integer and
        /// numeric offsets.
        fn takes(column: Number, offset: Number) -> bool {
            matches!(
                (column, offset),
                (Number::Integer, Number::Integer)
                    | (Number::Numeric, Number::Integer | Number::Numeric)
            )
        }
        enum Offsets {
            /// A SQLite type, or a domain over one: SQLite's rules apply.
            Sqlite,
            /// A custom type of this kind of number. A refused offset gets an
            /// error that names the type `name`.
            Takes { name: &'static str, number: Number },
        }
        struct Kind {
            ty: &'static str,
            strict: bool,
            /// The window's ORDER BY value, written over the column `c`.
            order: &'static str,
            /// Three distinct values, inserted for kinds that take offsets.
            values: [&'static str; 3],
            offsets: Offsets,
        }
        let kind = |ty, values, offsets| Kind {
            ty,
            strict: true,
            order: "c",
            values,
            offsets,
        };
        // An ORDER BY value that isn't a column, over an integer column.
        let sqlite_order = |order| Kind {
            ty: "integer",
            strict: true,
            order,
            values: ["1", "2", "4"],
            offsets: Offsets::Sqlite,
        };
        let takes_kind = |name, number| Offsets::Takes { name, number };
        let none = |name| takes_kind(name, Number::None);
        let no_values = ["NULL", "NULL", "NULL"];
        let kinds = [
            kind("integer", ["1", "2", "4"], Offsets::Sqlite),
            kind("real", ["1.0", "2.5", "4.0"], Offsets::Sqlite),
            kind("text", ["'a'", "'b'", "'c'"], Offsets::Sqlite),
            kind("any", ["1", "'x'", "4"], Offsets::Sqlite),
            kind("blob", ["x'01'", "x'02'", "x'04'"], Offsets::Sqlite),
            kind("pos", ["1", "2", "4"], Offsets::Sqlite),
            Kind {
                ty: "date",
                strict: false,
                order: "c",
                values: ["1", "2", "4"],
                offsets: Offsets::Sqlite,
            },
            // SQLite's CAST to numeric, a decimal, a string, NULL, and a CAST
            // without a type name.
            sqlite_order("CAST(c AS numeric)"),
            sqlite_order("0.5"),
            sqlite_order("'a'"),
            sqlite_order("NULL"),
            sqlite_order("CAST(c AS)"),
            kind(
                "smallint",
                ["1", "2", "4"],
                takes_kind("smallint", Number::Integer),
            ),
            kind(
                "bigint",
                ["1", "2", "4"],
                takes_kind("bigint", Number::Integer),
            ),
            kind(
                "big",
                ["1", "2", "4"],
                takes_kind("bigint", Number::Integer),
            ),
            kind(
                "numeric(10, 2)",
                ["1", "2.5", "4"],
                takes_kind("numeric", Number::Numeric),
            ),
            kind(
                "amount",
                ["1", "2.5", "4"],
                takes_kind("numeric", Number::Numeric),
            ),
            // Types without an order, such as json and inet, are left out:
            // ORDER BY refuses them before the frame is looked at.
            kind("date", no_values, none("date")),
            kind("day", no_values, none("date")),
            kind("time", no_values, none("time")),
            kind("timestamp", no_values, none("timestamp")),
            kind("timestamptz", no_values, none("timestamptz")),
            kind("boolean", no_values, none("boolean")),
            kind("uuid", no_values, none("uuid")),
            kind("varchar(10)", no_values, none("varchar")),
            kind("bytea", no_values, none("bytea")),
            kind("cents", no_values, none("cents")),
            kind("label", no_values, none("label")),
            kind("integer[]", no_values, none("integer[]")),
            // Values built from a custom column with an operator: `date - date`
            // is an integer, and `date + 1` is a date.
            Kind {
                ty: "date",
                strict: true,
                order: "c - c",
                values: ["'2026-01-01'", "'2026-01-02'", "'2026-01-04'"],
                offsets: Offsets::Sqlite,
            },
            Kind {
                order: "c + 1",
                ..kind("date", no_values, none("date"))
            },
            // `+` on an array gives a value whose type nothing declares, which
            // may be a custom type's value.
            Kind {
                order: "c + 1",
                ..kind("integer[]", no_values, none("unknown"))
            },
            // SQLite's CAST without a type name gives a SQLite number, also
            // from a custom type's value.
            Kind {
                ty: "numeric(10, 2)",
                strict: true,
                order: "CAST(c AS)",
                values: ["1", "2.5", "4"],
                offsets: Offsets::Sqlite,
            },
        ];
        enum OffsetType {
            /// Known when the statement is prepared. An error names it `name`.
            Known {
                name: &'static str,
                custom: bool,
                number: Number,
            },
            /// A string or a parameter, read as the column's type when the
            /// statement runs.
            ColumnType { integer: bool },
            /// Accepted wherever the column type takes offsets, and then fails
            /// when the statement runs.
            Null,
        }
        /// What a window over a kind with an offset does.
        enum Expected {
            /// Fails when prepared, with this error.
            Refused(String),
            /// Anything but a refusal, as SQLite's rules decide.
            SqliteRules,
            /// Fails when the statement runs, with this error.
            FailsWhenRun(&'static str),
            /// Counts the same rows as a subquery that applies the offset with
            /// `+` and `-`.
            SameAsSubquery,
        }
        let known = |name, number| OffsetType::Known {
            name,
            custom: false,
            number,
        };
        let custom = |name, number| OffsetType::Known {
            name,
            custom: true,
            number,
        };
        // (offset, its type, the value bound to ?1). A CAST to numeric without
        // arguments is SQLite's CAST, whose result may be an integer or a real,
        // so it is not a number PostgreSQL's numeric takes.
        let offsets = [
            ("1", known("integer", Number::Integer), None),
            ("1.5", known("numeric", Number::Numeric), None),
            ("CAST(1.5 AS REAL)", known("real", Number::None), None),
            ("x'01'", known("blob", Number::None), None),
            ("CAST(1 AS TEXT)", known("text", Number::None), None),
            // SQLite's CAST without a type name, whose type is SQLite's ANY.
            ("CAST(1 AS)", known("any", Number::None), None),
            // `+` on an array, whose type nothing declares.
            (
                "CAST('{1}' AS integer[]) + 1",
                custom("unknown", Number::None),
                None,
            ),
            // A CASE that mixes SQLite types has SQLite's type ANY, and here
            // holds a real.
            (
                "CASE WHEN 1 THEN CAST(0.5 AS REAL) ELSE 'a' END",
                known("any", Number::None),
                None,
            ),
            // An offset that is not a constant is NULL, as in SQLite.
            ("(SELECT 1)", OffsetType::Null, None),
            (
                "CAST('{1}' AS integer[])",
                custom("integer[]", Number::None),
                None,
            ),
            ("CAST(1 AS numeric)", known("numeric", Number::None), None),
            ("'1'", OffsetType::ColumnType { integer: true }, None),
            ("'1.5'", OffsetType::ColumnType { integer: false }, None),
            (
                "?1",
                OffsetType::ColumnType { integer: true },
                Some(turso_core::Value::from_i64(1)),
            ),
            (
                "?1",
                OffsetType::ColumnType { integer: false },
                Some(turso_core::Value::build_text("1.5")),
            ),
            ("NULL", OffsetType::Null, None),
            (
                "CAST(1 AS smallint)",
                custom("smallint", Number::Integer),
                None,
            ),
            ("CAST(1 AS bigint)", custom("bigint", Number::Integer), None),
            (
                "CAST(1 AS numeric(5, 2))",
                custom("numeric", Number::Numeric),
                None,
            ),
            ("CAST(1 AS boolean)", custom("boolean", Number::None), None),
            // A domain offset is checked as the type it is based on.
            ("CAST(1 AS pos)", known("integer", Number::Integer), None),
            ("CAST(1 AS big)", custom("bigint", Number::Integer), None),
            (
                "CAST('2026-01-01' AS day)",
                custom("date", Number::None),
                None,
            ),
        ];
        // (frame, the rows it holds for the row `t` when ordered ascending,
        // and when ordered descending). `{k}` is the offset.
        let frames = [
            (
                "{k} PRECEDING AND CURRENT ROW",
                "u.c >= t.c - {k} AND u.c <= t.c",
                "u.c <= t.c + {k} AND u.c >= t.c",
            ),
            (
                "CURRENT ROW AND {k} FOLLOWING",
                "u.c >= t.c AND u.c <= t.c + {k}",
                "u.c <= t.c AND u.c >= t.c - {k}",
            ),
            (
                "{k} FOLLOWING AND UNBOUNDED FOLLOWING",
                "u.c >= t.c + {k}",
                "u.c <= t.c - {k}",
            ),
            (
                "UNBOUNDED PRECEDING AND {k} PRECEDING",
                "u.c <= t.c - {k}",
                "u.c >= t.c + {k}",
            ),
        ];
        let counts = |conn: &Arc<turso_core::Connection>,
                      sql: &str,
                      param: &Option<turso_core::Value>|
         -> turso_core::Result<Vec<i64>> {
            let mut stmt = conn.prepare(sql)?;
            if let Some(param) = param {
                stmt.bind_at(1.try_into().unwrap(), param.clone())?;
            }
            let mut rows = Vec::new();
            stmt.run_with_row_callback(|row| {
                rows.push(row.get::<i64>(0)?);
                Ok(())
            })?;
            Ok(rows)
        };
        let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
        let db = TempDatabase::builder().with_opts(opts).build();
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TYPE cents(value integer) BASE integer ENCODE value * 100 DECODE value / 100 OPERATOR '<';
             CREATE TYPE label(value text) BASE text ENCODE value DECODE value OPERATOR '<';
             CREATE DOMAIN pos AS integer;
             CREATE DOMAIN big AS bigint;
             CREATE DOMAIN amount AS numeric;
             CREATE DOMAIN day AS date;",
        )
        .unwrap();
        for (n, kind) in kinds.iter().enumerate() {
            let table = format!("t{n}");
            let strict = if kind.strict { " STRICT" } else { "" };
            conn.execute(format!(
                "CREATE TABLE {table}(id INTEGER PRIMARY KEY, c {}){strict}",
                kind.ty
            ))
            .unwrap();
            if !matches!(
                kind.offsets,
                Offsets::Takes {
                    number: Number::None,
                    ..
                }
            ) {
                for value in kind.values {
                    conn.execute(format!("INSERT INTO {table}(c) VALUES ({value})"))
                        .unwrap();
                }
            }
            for (offset, offset_type, param) in &offsets {
                for (frame, asc_rows, desc_rows) in frames {
                    for (order, rows) in [("ASC", asc_rows), ("DESC", desc_rows)] {
                        let window = format!(
                            "SELECT count(*) OVER (ORDER BY {} {order} RANGE BETWEEN {frame}) \
                             FROM {table} t ORDER BY id",
                            kind.order
                        )
                        .replace("{k}", offset);
                        let context =
                            format!("type {} (strict={}): {window}", kind.ty, kind.strict);
                        let expected = match (&kind.offsets, offset_type) {
                            // Over a SQLite value, an offset of a custom type
                            // other than smallint and bigint is refused: adding
                            // it gives a value of that type, which doesn't
                            // compare with SQLite's numbers.
                            (
                                Offsets::Sqlite,
                                OffsetType::Known {
                                    name,
                                    custom: true,
                                    number: Number::None | Number::Numeric,
                                },
                            ) => Expected::Refused(format!(
                                "RANGE with offset PRECEDING/FOLLOWING is not supported for offset type {name} over a value without a custom type"
                            )),
                            (
                                Offsets::Sqlite,
                                OffsetType::Known {
                                    custom: true,
                                    number: Number::Integer,
                                    ..
                                }
                                | OffsetType::Known {
                                    custom: false,
                                    number: Number::None | Number::Integer | Number::Numeric,
                                    ..
                                }
                                | OffsetType::ColumnType { .. }
                                | OffsetType::Null,
                            ) => Expected::SqliteRules,
                            (
                                Offsets::Takes {
                                    name,
                                    number: Number::None,
                                },
                                _,
                            ) => Expected::Refused(format!(
                                "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {name}"
                            )),
                            (
                                Offsets::Takes {
                                    name,
                                    number: number @ (Number::Integer | Number::Numeric),
                                },
                                OffsetType::Known {
                                    name: offset_type,
                                    custom,
                                    number: offset_number,
                                },
                            ) => {
                                if takes(*number, *offset_number) {
                                    Expected::SameAsSubquery
                                } else if !*custom && *offset_number == Number::None {
                                    // SQLite's own types, such as REAL and
                                    // SQLite's NUMERIC.
                                    Expected::Refused(format!(
                                        "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {name} and an offset of SQLite type {}",
                                        offset_type.to_ascii_uppercase()
                                    ))
                                } else {
                                    Expected::Refused(format!(
                                        "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {name} and offset type {offset_type}"
                                    ))
                                }
                            }
                            (
                                Offsets::Takes {
                                    number: Number::Integer | Number::Numeric,
                                    ..
                                },
                                OffsetType::Null,
                            ) => Expected::FailsWhenRun("offset must be a non-negative"),
                            (
                                Offsets::Takes {
                                    number: Number::Integer,
                                    ..
                                },
                                OffsetType::ColumnType { integer: false },
                            ) => Expected::FailsWhenRun("offset must be a non-negative integer"),
                            (
                                Offsets::Takes {
                                    number: Number::Numeric,
                                    ..
                                },
                                OffsetType::ColumnType { integer: false },
                            )
                            | (
                                Offsets::Takes {
                                    number: Number::Integer | Number::Numeric,
                                    ..
                                },
                                OffsetType::ColumnType { integer: true },
                            ) => Expected::SameAsSubquery,
                        };
                        let result = counts(&conn, &window, param);
                        match expected {
                            Expected::Refused(refusal) => {
                                let error = match conn.prepare(&window) {
                                    Ok(_) => panic!("{context}: expected an error"),
                                    Err(error) => error.to_string(),
                                };
                                assert!(error.contains(&refusal), "{context}: {error}");
                            }
                            Expected::SqliteRules => {
                                if let Err(error) = result {
                                    assert!(
                                        !error.to_string().contains("is not supported for"),
                                        "{context}: {error}"
                                    );
                                }
                            }
                            Expected::FailsWhenRun(message) => {
                                let error = result.expect_err(&context).to_string();
                                assert!(error.contains(message), "{context}: {error}");
                            }
                            Expected::SameAsSubquery => {
                                let expected_sql = format!(
                                    "SELECT (SELECT count(*) FROM {table} u WHERE {rows}) \
                                     FROM {table} t ORDER BY id"
                                )
                                .replace("{k}", offset);
                                let expected = counts(&conn, &expected_sql, param)
                                    .unwrap_or_else(|e| panic!("{expected_sql}: {e}"));
                                let actual = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                                assert_eq!(actual, expected, "{context}");
                            }
                        }
                    }
                }
            }
        }
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

            // A key on an array follows the element type's key rule: when an
            // index on the element type is accepted, so is one on the array,
            // and when it is refused because the type's DECODE can read
            // different stored values back as the same value, so is the array.
            // An index on a type with no OPERATOR '<' is refused before that
            // check runs, and one on an array of it, such as cidr[], is accepted
            // because of a bug that is also on main.
            let element_index = conn.execute("CREATE INDEX tw ON t(w)");
            let array_index = conn.execute("CREATE INDEX ta ON t(a)");
            let refused_with = |result: &turso_core::Result<()>, message: &str| matches!(result, Err(e) if e.to_string().contains(message));
            let indexes =
                format!("{ty}: index on {ty}: {element_index:?}, on {ty}[]: {array_index:?}");
            if element_index.is_ok() {
                assert!(array_index.is_ok(), "{indexes}");
            } else if refused_with(&element_index, "cannot be a key") {
                assert!(refused_with(&array_index, "cannot be a key"), "{indexes}");
            } else {
                assert!(refused_with(&element_index, "OPERATOR '<'"), "{indexes}");
            }
            if element_index.is_ok() {
                all_true("SELECT count(*) FROM t WHERE w = (SELECT w FROM t)", 1);
            }
            let element_index_works = array_index.is_ok();
            if element_index_works {
                all_true(
                    "SELECT count(*) FROM t WHERE a = ARRAY[(SELECT w FROM t)]",
                    1,
                );
                // A key that is not an array matches no row, as in a scan.
                all_true("SELECT count(*) = 0 FROM t WHERE a = 'x'", 1);
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
                            "SELECT group_concat(id) FROM (SELECT id FROM o ORDER BY a)",
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
                    // A sort key that is also a result column is read back
                    // from the sorter.
                    assert_eq!(
                        crate::common::limbo_exec_rows(&conn, "SELECT a FROM o ORDER BY a"),
                        crate::common::limbo_exec_rows(&conn, "SELECT a FROM o ORDER BY id"),
                        "{ty}: SELECT a FROM o ORDER BY a ({step})"
                    );
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
            // written as text, such as '{"x"}'. Blobs have no text form.
            // integer[] and real[] are skipped because of a bug that is also on
            // main: ADD COLUMN checks their text default as if it were an
            // integer or a real, and refuses it.
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
