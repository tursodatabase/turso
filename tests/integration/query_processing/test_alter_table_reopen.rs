//! Reopen-DB tests for ALTER TABLE schema rewrites.
//!
//! BTreeTable::to_sql() must preserve schema details so that after ALTER TABLE
//! the stored schema in sqlite_schema still matches the table metadata.
//!
//! For table-level UNIQUE (...), sqlite_schema must still match the sqlite_autoindex_* entries.
//! Otherwise reopen triggers populate_indices panic: "all automatic indexes parsed
//! from sqlite_schema should have been consumed, but N remain".
//! See https://github.com/tursodatabase/turso/issues/5616

use crate::common::{ExecRows, TempDatabase};
use tempfile::TempDir;

/// After ALTER TABLE DROP COLUMN on an AUTOINCREMENT table, reopen must parse
/// the persisted schema as AUTOINCREMENT and avoid reusing deleted rowids.
#[test]
fn test_alter_table_drop_column_preserves_autoincrement_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("alter_drop_col_autoincrement_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doomed INT,
                v TEXT
            )",
        )
        .unwrap();
        conn.execute("INSERT INTO t(doomed, v) VALUES (9, 'a'), (8, 'b')")
            .unwrap();
        conn.execute("ALTER TABLE t DROP COLUMN doomed").unwrap();
        conn.execute("INSERT INTO t(v) VALUES ('c')").unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let schema: Vec<(i64,)> =
            conn.exec_rows("SELECT sql LIKE '%AUTOINCREMENT%' FROM sqlite_schema WHERE name = 't'");
        assert_eq!(schema, vec![(1,)]);

        let seq_before: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
        assert_eq!(seq_before, vec![("t".into(), 3)]);

        conn.execute("INSERT INTO t(v) VALUES ('d')").unwrap();
        conn.execute("DELETE FROM t WHERE v = 'd'").unwrap();
        conn.execute("INSERT INTO t(v) VALUES ('e')").unwrap();

        let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![
                (1, "a".into()),
                (2, "b".into()),
                (3, "c".into()),
                (5, "e".into()),
            ]
        );

        let seq_after: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
        assert_eq!(seq_after, vec![("t".into(), 5)]);
        conn.close().unwrap();
    }
}

/// After ALTER TABLE ADD COLUMN on a table with UNIQUE(stream_id, version), reopen
/// must succeed (no orphan autoindex) and the unique constraint must still be enforced.
#[test]
fn test_alter_table_add_column_preserves_unique_constraint_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_add_col_unique_reopen.db");

    // Session 1: create table with table-level UNIQUE, add column, close
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE events (
                id TEXT,
                stream_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (stream_id, version)
            )",
        )
        .unwrap();
        conn.execute("ALTER TABLE events ADD COLUMN extra TEXT")
            .unwrap();
        conn.close().unwrap();
    }

    // Session 2: reopen must not panic; UNIQUE(stream_id, version) must still be enforced
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("INSERT INTO events VALUES('e1', 's1', 1, 'x')")
            .unwrap();
        let err = conn.execute("INSERT INTO events VALUES('e2', 's1', 1, 'y')");
        assert!(
            err.is_err(),
            "UNIQUE(stream_id, version) must still be enforced after reopen"
        );

        let rows: Vec<(String, String, i64, String)> =
            conn.exec_rows("SELECT id, stream_id, version, extra FROM events ORDER BY id");
        assert_eq!(rows, vec![("e1".into(), "s1".into(), 1, "x".into())]);
        conn.close().unwrap();
    }
}

/// After ALTER TABLE ADD COLUMN on a table with multiple UNIQUE(a,b) and UNIQUE(b,c),
/// reopen must succeed and both constraints must still be enforced.
#[test]
fn test_alter_table_add_column_preserves_multiple_unique_constraints_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_add_col_multi_unique_reopen.db");

    // Session 1: create table with two table-level UNIQUEs, add column, close
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t (
                a TEXT,
                b INTEGER,
                c TEXT,
                UNIQUE (a, b),
                UNIQUE (b, c)
            )",
        )
        .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN d TEXT").unwrap();
        conn.close().unwrap();
    }

    // Session 2: reopen must not panic; both UNIQUEs must still be enforced
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("INSERT INTO t VALUES('x', 1, 'c1', 'd1')")
            .unwrap();
        let err = conn.execute("INSERT INTO t VALUES('x', 1, 'c2', 'd2')");
        assert!(
            err.is_err(),
            "UNIQUE(a, b) must still be enforced after reopen"
        );

        conn.execute("INSERT INTO t VALUES('a2', 2, 'c', 'd2')")
            .unwrap();
        let err = conn.execute("INSERT INTO t VALUES('a3', 2, 'c', 'd3')");
        assert!(
            err.is_err(),
            "UNIQUE(b, c) must still be enforced after reopen"
        );

        let rows: Vec<(String, i64, String, String)> =
            conn.exec_rows("SELECT a, b, c, d FROM t ORDER BY a, b");
        assert_eq!(
            rows,
            vec![
                ("a2".into(), 2, "c".into(), "d2".into()),
                ("x".into(), 1, "c1".into(), "d1".into()),
            ]
        );
        conn.close().unwrap();
    }
}

/// ALTER TABLE ADD COLUMN with a generated expression must be
/// rejected when --experimental-generated-columns is disabled
#[test]
fn test_alter_add_generated_column_rejected_without_flag() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();

    let err = conn
        .execute("ALTER TABLE t ADD COLUMN b AS (a)")
        .expect_err("generated column must be rejected without the flag");
    assert!(
        err.to_string().contains("Generated columns require"),
        "unexpected error: {err}"
    );

    let cols: Vec<(String,)> = conn.exec_rows("SELECT name FROM pragma_table_info('t')");
    assert_eq!(cols, vec![("a".to_string(),)]);
}

/// ALTER TABLE ADD COLUMN with a generated expression must be
/// accepted when --experimental-generated-columns is enabled
#[test]
fn test_alter_add_generated_column_succeeds_with_flag() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_add_generated_column.db");
    let opts = turso_core::DatabaseOpts::new().with_generated_columns(true);

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a)").unwrap();
        conn.execute("INSERT INTO t VALUES (5)").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN b AS (a)").unwrap();

        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM t");
        assert_eq!(rows, vec![(5, 5)]);
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, opts);
        let conn = db.connect_limbo();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM t");
        assert_eq!(rows, vec![(5, 5)]);
        conn.close().unwrap();
    }
}

#[test]
fn test_alter_table_add_column_preserves_collation_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("alter_table_add_col_collate_reopen.db");

    // Session 1: create table with collate nocase column, insert value, then add another column
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE tbl (col1 TEXT COLLATE NOCASE)")
            .unwrap();
        conn.execute("INSERT INTO tbl VALUES ('ABC')").unwrap();
        conn.execute("ALTER TABLE tbl ADD COLUMN extra TEXT")
            .unwrap();
        conn.close().unwrap();
    }

    // Session 2: reopen connection and check if collation property still holds
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let col_schema: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'tbl'");
        assert_eq!(
            col_schema,
            vec![("CREATE TABLE tbl (col1 TEXT COLLATE NOCASE, extra TEXT)".to_string(),)]
        );

        let check_collate_res: Vec<(i64,)> =
            conn.exec_rows("SELECT count(*) FROM tbl WHERE col1 = 'abc'");
        assert_eq!(check_collate_res, vec![(1,)]);
        conn.close().unwrap();
    }
}

#[test]
fn test_alter_table_preserves_desc_primary_key_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_desc_pk_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY DESC, q TEXT)")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN z TEXT").unwrap();
        let schema: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
        assert!(schema[0].0.contains("PRIMARY KEY DESC"), "{schema:?}");
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO t(a, q, z) VALUES (1, 'q', 'z')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t(a, q, z) VALUES (1, 'duplicate', 'z')")
        .is_err());
    conn.close().unwrap();
}

#[test]
fn test_alter_table_rename_quotes_digit_leading_name() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_rename_quoted_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a)").unwrap();
        conn.execute("CREATE INDEX i ON t(a)").unwrap();
        conn.execute("ALTER TABLE t RENAME TO \"1a\"").unwrap();
        let rows: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name IN ('1a', 'i') ORDER BY name");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(sql,)| sql.contains("\"1a\"")), "{rows:?}");
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO \"1a\" VALUES (1)").unwrap();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM \"1a\"");
    assert_eq!(rows, vec![(1,)]);
    conn.close().unwrap();
}

#[test]
fn test_alter_table_omits_empty_foreign_key_parent_column_list() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_fk_parent_columns_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p)")
            .unwrap();
        conn.execute("CREATE TABLE u(a INTEGER REFERENCES p, b INTEGER REFERENCES p(id))")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN c INTEGER").unwrap();
        conn.execute("ALTER TABLE u ADD COLUMN c INTEGER").unwrap();
        let rows: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name IN ('t', 'u') ORDER BY name");
        assert!(
            rows.iter().all(|(sql,)| !sql.contains("REFERENCES p()")),
            "{rows:?}"
        );
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    let t_rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(t_rows, vec![(0,)]);
    let u_rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
    assert_eq!(u_rows, vec![(0,)]);
    conn.close().unwrap();
}

#[test]
fn test_alter_table_preserves_table_constraint_modifiers() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_constraint_modifiers_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a TEXT, UNIQUE(a DESC))")
            .unwrap();
        conn.execute("CREATE TABLE u(a TEXT, UNIQUE(a COLLATE NOCASE))")
            .unwrap();
        conn.execute("CREATE TABLE p(a TEXT, b TEXT, PRIMARY KEY(a DESC, b COLLATE NOCASE))")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN q TEXT").unwrap();
        conn.execute("ALTER TABLE u ADD COLUMN q TEXT").unwrap();
        conn.execute("ALTER TABLE p ADD COLUMN q TEXT").unwrap();
        let rows: Vec<(String, String)> = conn.exec_rows(
            "SELECT name, sql FROM sqlite_schema WHERE name IN ('t', 'u', 'p') ORDER BY name",
        );
        assert!(
            rows.iter()
                .any(|(name, sql)| name == "t" && sql.contains("UNIQUE (a DESC)")),
            "{rows:?}"
        );
        assert!(
            rows.iter()
                .any(|(name, sql)| name == "u" && sql.contains("COLLATE NOCASE")),
            "{rows:?}"
        );
        assert!(
            rows.iter()
                .any(|(name, sql)| name == "p"
                    && sql.contains("PRIMARY KEY (a DESC, b COLLATE NOCASE)")),
            "{rows:?}"
        );
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO t(a, q) VALUES ('a', 'x')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t(a, q) VALUES ('a', 'y')")
        .is_err());
    conn.execute("INSERT INTO u(a, q) VALUES ('ABC', 'x')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO u(a, q) VALUES ('abc', 'y')")
        .is_err());
    conn.close().unwrap();
}

#[test]
fn test_alter_table_preserves_on_conflict_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_conflict_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER, k TEXT UNIQUE ON CONFLICT ROLLBACK, x INTEGER)")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN note TEXT").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
        assert!(rows[0].0.contains("ON CONFLICT ROLLBACK"), "{rows:?}");
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t(id, k) VALUES (1, 'a')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t(id, k) VALUES (2, 'a')")
        .is_err());
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(0,)]);
    conn.close().unwrap();
}

#[test]
fn test_alter_table_preserves_primary_key_and_unique_order() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_constraint_order_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a TEXT, b TEXT, UNIQUE(a), PRIMARY KEY(b))")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN d TEXT").unwrap();
        let rows: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
        let sql = &rows[0].0;
        assert!(
            sql.find("UNIQUE (a)").unwrap() < sql.find("PRIMARY KEY (b)").unwrap(),
            "{sql}"
        );
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO t(a, b, d) VALUES ('a', 'b', 'd')")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t(a, b, d) VALUES ('a', 'c', 'd')")
        .is_err());
    assert!(conn
        .execute("INSERT INTO t(a, b, d) VALUES ('c', 'b', 'd')")
        .is_err());
    conn.close().unwrap();
}
