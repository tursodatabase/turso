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
use asserting::prelude::*;
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

/// ADD COLUMN and DROP COLUMN rewrite the stored CREATE TABLE. After reopen,
/// sqlite_sequence must still use the table name as written in CREATE TABLE.
#[test]
fn test_alter_table_add_drop_column_keeps_mixed_case_sequence_name_reopen() {
    alter_table_add_drop_column_keeps_mixed_case_sequence_name_reopen(false);
}

#[test]
fn test_mvcc_alter_table_add_drop_column_keeps_mixed_case_sequence_name_reopen() {
    alter_table_add_drop_column_keeps_mixed_case_sequence_name_reopen(true);
}

fn alter_table_add_drop_column_keeps_mixed_case_sequence_name_reopen(enable_mvcc: bool) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("alter_mixed_case_autoincrement_reopen.db");
    let open = || {
        TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(enable_mvcc)
            .build()
    };

    {
        let db = open();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE \"MiXeD\"(id INTEGER PRIMARY KEY AUTOINCREMENT, v)")
            .unwrap();
        conn.execute("INSERT INTO mixed(v) VALUES (1)").unwrap();
        conn.execute("ALTER TABLE mixed ADD COLUMN z").unwrap();
        conn.execute("ALTER TABLE mixed DROP COLUMN z").unwrap();
        conn.close().unwrap();
    }

    {
        let db = open();
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO mixed(v) VALUES (2)").unwrap();
        let seq: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
        assert_eq!(seq, vec![("MiXeD".into(), 2)]);

        conn.execute("DELETE FROM sqlite_sequence").unwrap();
        conn.execute("INSERT INTO mixed(v) VALUES (3)").unwrap();
        let seq: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
        assert_eq!(seq, vec![("MiXeD".into(), 3)]);
        conn.close().unwrap();
    }
}

/// Databases written by Turso v0.9.0-pre.1 and earlier keep working. Those
/// versions stored the lowercased table name in `sqlite_sequence`, and ADD
/// COLUMN rewrote the stored CREATE TABLE with the lowercased name. The test
/// builds that state with the current code, then checks that INSERT, RENAME
/// TO and DROP TABLE use the old rows and that ids keep counting from them
/// after a reopen.
#[test]
fn test_autoincrement_tables_from_older_turso_keep_working() {
    autoincrement_tables_from_older_turso_keep_working(false);
}

/// Same as above, but the lowercased `sqlite_sequence` rows are written in
/// MVCC mode and are only in the logical log when the database is reopened.
#[test]
fn test_mvcc_autoincrement_tables_from_older_turso_keep_working() {
    autoincrement_tables_from_older_turso_keep_working(true);
}

fn autoincrement_tables_from_older_turso_keep_working(enable_mvcc: bool) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("autoincrement_from_older_turso.db");
    let open = |enable_mvcc| {
        TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(enable_mvcc)
            .build()
    };

    {
        let db = open(false);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE \"MiXeD Name\"(id INTEGER PRIMARY KEY AUTOINCREMENT, v)")
            .unwrap();
        // ADD COLUMN in older versions rewrote the CREATE TABLE text with the
        // lowercased name. Creating the table with that text gives the same
        // sqlite_schema row.
        conn.execute("CREATE TABLE \"add col\" (id INTEGER PRIMARY KEY AUTOINCREMENT, v, w)")
            .unwrap();
        conn.execute("INSERT INTO \"MiXeD Name\"(v) VALUES ('x'), ('y')")
            .unwrap();
        conn.execute("INSERT INTO \"AdD Col\"(v) VALUES ('a'), ('b'), ('c')")
            .unwrap();
        // Without the sqlite_sequence rows, the next ids would reuse these.
        conn.execute("DELETE FROM \"MiXeD Name\" WHERE id = 2")
            .unwrap();
        conn.execute("DELETE FROM \"AdD Col\" WHERE id = 3")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let db = open(enable_mvcc);
        let conn = db.connect_limbo();
        conn.execute("UPDATE sqlite_sequence SET name = lower(name)")
            .unwrap();
        conn.close().unwrap();
    }
    if enable_mvcc {
        let log_path = format!("{}-log", path.display());
        let log_len = std::fs::metadata(log_path).unwrap().len();
        assert!(
            log_len > 0,
            "the old sqlite_sequence rows must be in the logical log"
        );
    }

    {
        let db = open(enable_mvcc);
        let conn = db.connect_limbo();
        let schema: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'add col'");
        assert_eq!(
            schema,
            vec![("CREATE TABLE \"add col\" (id INTEGER PRIMARY KEY AUTOINCREMENT, v, w)".into(),)]
        );
        let seq: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence ORDER BY name");
        assert_eq!(seq, vec![("add col".into(), 3), ("mixed name".into(), 2)]);

        conn.execute("INSERT INTO \"MiXeD Name\"(v) VALUES ('z')")
            .unwrap();
        conn.execute("INSERT INTO \"AdD Col\"(v) VALUES ('d')")
            .unwrap();
        let seq: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence ORDER BY name");
        assert_eq!(seq, vec![("MiXeD Name".into(), 3), ("add col".into(), 4)]);

        conn.execute("ALTER TABLE \"AdD Col\" RENAME TO \"ReNaMed\"")
            .unwrap();
        conn.execute("DROP TABLE \"MiXeD Name\"").unwrap();
        let seq: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
        assert_eq!(seq, vec![("ReNaMed".into(), 4)]);
        conn.close().unwrap();
    }

    {
        let db = open(enable_mvcc);
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO renamed(v) VALUES ('e')").unwrap();
        let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM renamed ORDER BY id");
        assert_eq!(
            rows,
            vec![
                (1, "a".into()),
                (2, "b".into()),
                (4, "d".into()),
                (5, "e".into()),
            ]
        );
        let seq: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
        assert_eq!(seq, vec![("ReNaMed".into(), 5)]);
        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".into(),)]);
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

/// ALTER TABLE ADD COLUMN with a generated expression works with the
/// default database options, and the column is still there after a reopen.
#[test]
fn test_alter_add_generated_column_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_add_generated_column.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a)").unwrap();
        conn.execute("INSERT INTO t VALUES (5)").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN b AS (a)").unwrap();

        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM t");
        assert_eq!(rows, vec![(5, 5)]);
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
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
fn test_alter_table_add_column_preserves_unique_constraint_collation_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_table_unique_collate_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t1 (a, UNIQUE (a COLLATE NOCASE))")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES ('a')").unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN b").unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        assert_that!(conn.execute("INSERT INTO t1 VALUES ('A', 1)"))
            .is_err()
            .err()
            .display_string()
            .contains("UNIQUE constraint failed: t1.a");
        conn.close().unwrap();
    }
}

#[test]
fn test_alter_table_add_column_preserves_primary_key_constraint_collation_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("alter_table_primary_key_collate_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t1 (a, PRIMARY KEY (a COLLATE NOCASE))")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES ('a')").unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN b").unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        assert_that!(conn.execute("INSERT INTO t1 VALUES ('A', 1)"))
            .is_err()
            .err()
            .display_string()
            .contains("UNIQUE constraint failed: t1.a");
        conn.close().unwrap();
    }
}

#[test]
fn test_alter_table_add_column_preserves_unique_constraint_desc_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_table_unique_desc_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t1 (a, UNIQUE (a DESC))")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES ('x'), ('y'), ('z')")
            .unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN b").unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        assert_that!(conn.execute("INSERT INTO t1 VALUES ('x', 1)"))
            .is_err()
            .err()
            .display_string()
            .contains("UNIQUE constraint failed: t1.a");
        conn.close().unwrap();
    }
}

/// ALTER TABLE rewrites the schema from the table metadata. Type names must be
/// written as the user wrote them: the dequoted declared type of `[x] INT` is
/// `x] IN`, which is not valid SQL.
#[test]
fn test_alter_table_keeps_type_names_as_written_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("alter_table_type_names_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t (z, e [x] INT, d \"a\"\"b\" INT, b \"BLOB\" INTEGER, a BLOB/*INT*/REAL, g \"\")",
        )
        .unwrap();
        conn.execute("ALTER TABLE t DROP COLUMN z").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN f \"c\"\"d\" TEXT")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let schema: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
        assert_eq!(
            schema,
            vec![(
                "CREATE TABLE t (e [x] INT, d \"a\"\"b\" INT, b \"BLOB\" INTEGER, a BLOB/*INT*/REAL, g \"\", f \"c\"\"d\" TEXT)"
                    .to_string(),
            )]
        );

        let declared_types: Vec<(String,)> =
            conn.exec_rows("SELECT type FROM pragma_table_info('t')");
        assert_eq!(
            declared_types,
            vec![
                ("x] IN".to_string(),),
                ("a\"b".to_string(),),
                ("BLOB".to_string(),),
                ("BLOB/*INT*/REAL".to_string(),),
                ("".to_string(),),
                ("c\"d".to_string(),),
            ]
        );

        conn.execute("INSERT INTO t (g) VALUES ('12')").unwrap();
        let stored_types: Vec<(String,)> = conn.exec_rows("SELECT typeof(g) FROM t");
        assert_eq!(stored_types, vec![("integer".to_string(),)]);
        conn.close().unwrap();
    }
}

/// Older Turso versions kept the quotes in `a "INTEGER" PRIMARY KEY`, so the
/// column was not a rowid alias and the table got an automatic index for the
/// primary key. Such a database must still open, and `a` must keep its stored
/// values instead of reading the rowid.
#[test]
fn test_quoted_integer_primary_key_with_automatic_index_reopen() {
    let quoted_types = ["\"INTEGER\"", "'INTEGER'", "`INTEGER`"];
    let column_constraints = ["PRIMARY KEY", "PRIMARY KEY UNIQUE"];
    for (quoted_type, constraints) in quoted_types
        .into_iter()
        .flat_map(|t| column_constraints.map(|c| (t, c)))
    {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("quoted_integer_primary_key.db");

        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite
            .execute_batch(&format!(
                "CREATE TABLE t(a INT {constraints}, b TEXT);
                 INSERT INTO t VALUES (10, 'x'), (20, 'y');
                 PRAGMA writable_schema = ON;
                 UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a {} {constraints}, b TEXT)'
                  WHERE name = 't';",
                quoted_type.replace('\'', "''")
            ))
            .unwrap();
        drop(sqlite);

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let rows: Vec<(i64, i64, String)> = conn.exec_rows("SELECT rowid, a, b FROM t ORDER BY a");
        assert_eq!(rows, vec![(1, 10, "x".into()), (2, 20, "y".into())]);

        assert!(conn.execute("INSERT INTO t VALUES (10, 'z')").is_err());
        conn.execute("INSERT INTO t VALUES (30, 'z')").unwrap();

        let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t WHERE a = 30");
        assert_eq!(rows, vec![(30, "z".into())]);

        conn.execute("ALTER TABLE t ADD COLUMN c").unwrap();
        conn.execute("INSERT INTO t VALUES (40, 'w', 1)").unwrap();
        conn.close().unwrap();
        drop(db);

        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let rows: Vec<(i64, i64, String)> = conn.exec_rows("SELECT rowid, a, b FROM t ORDER BY a");
        assert_eq!(
            rows,
            vec![
                (1, 10, "x".into()),
                (2, 20, "y".into()),
                (3, 30, "z".into()),
                (4, 40, "w".into()),
            ]
        );
        assert!(conn.execute("INSERT INTO t VALUES (40, 'v', 2)").is_err());

        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".to_string(),)]);
        conn.close().unwrap();
    }
}
